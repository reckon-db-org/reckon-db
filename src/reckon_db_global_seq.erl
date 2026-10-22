%% @doc A stable global sequence for ordinary streams.
%%
%% The store's global order used to be `(epoch_us, stream_id, version)'
%% (`reckon_db_index:sort_in_global_order/1'), with `epoch_us' taken from
%% the appending node's clock, and a subscriber's resume point was an
%% OFFSET: a count of events to skip. A scavenge removes events and
%% renumbers nothing, so after one the same offset names a different
%% event: a checkpoint skips events it never processed, or repeats ones it
%% did. A clock that steps reorders events that already exist.
%%
%% This module gives every ordinary-stream event a sequence number assigned
%% inside the append transaction, from one counter bumped in the same Ra
%% command that writes the events. Ra is the store's only total order, so
%% the seq order is the commit order and no clock is consulted. A seq is
%% never reused and never reassigned, which is what makes it usable as a
%% checkpoint.
%%
%% The seq is recorded BESIDE the event, never inside it:
%%
%% <pre>
%% [global_seq, zero-padded-20-digit-seq] -> #{stream_id, version}
%% [global_seq_ref, Type, Id, PaddedVersion] -> Seq
%% </pre>
%%
%% The `#event{}' record belongs to reckon-gater, and its canonical
%% encoding is HMAC'd: a new field would change the MAC bytes of every
%% event in every store that has integrity on. The seq reaches consumers as
%% the `global_position' metadata key instead, which is what evoq and the
%% guides already name.
%%
%% The reverse ref exists because live delivery needs event -> seq: the
%% trigger that fires on an event node hands over the event, not the seq.
%%
%% == The authenticated head ==
%%
%% The seq log is what a checkpoint names and what a consumer resumes
%% from, so it is attacked in its own right: deleting the newest entries
%% (or rewriting one) moves a resume point without touching an event.
%% The per-stream chain (`prev_event_hash') says nothing about it.
%%
%% When the store has integrity enabled, `[metadata, integrity,
%% global_seq_head]' holds `#{from, last, chain, mac}': the first and the
%% last seq the chain covers, a SHA-256 chain over every
%% `[global_seq, N] -> #{stream_id, version}' mapping from `from' to
%% `last', and an HMAC over all of it under the store's integrity key,
%% with the store id inside. `verify/1' recomputes the chain from the tree
%% and answers whether the log is the one the store wrote. Head and refs
%% are written in the same Ra command as the events, so a store cannot
%% commit an event whose seq is outside the authenticated range.
%%
%% A store without integrity has no key to authenticate with, so it has no
%% head; `verify/1' says so rather than pretending. A store whose
%% integrity is switched on later gets its head sealed at the next start
%% (`ensure_head/1'), over the seqs it already holds.
%%
%% == DCB ==
%%
%% DCB events keep their own counter and their own numbering
%% (`reckon_db_dcb'). They are not numbered here and do not appear in the
%% seq log; a DCB consumer reads `reckon_db_dcb:read_log/3'. A subscription
%% is either an ordinary-stream one or a DCB one, and its checkpoint names
%% which numbering it is in.
%%
%% == Durability ==
%%
%% A store is either fully on this numbering or not at all: `backfill/1'
%% runs at store start, before reads are served, and marks the store done.
%% An append on a store that has not been backfilled is refused, so no
%% event can be written without a seq, and a read on one is refused too,
%% so nothing reads a half-numbered store as if it were whole.
%%
%% @end
-module(reckon_db_global_seq).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    path/1,
    ref_path/2,
    seq_for/3,
    prepare/2,
    assign_in_tx/3,
    last_assigned/1,
    read_from/3,
    positioned/2,
    annotate/2,
    backfill/1,
    is_backfilled/1,
    ensure_head/1,
    head/1,
    verify/1
]).

-export_type([seq/0]).

%% A sequence number. Assigned once, never reused.
-type seq() :: non_neg_integer().

%% The authenticated head of the sequence log. See the module doc.
-type head() :: #{from := seq(), last := seq(),
                  chain := binary(), mac := binary()}.

%% How many refs one backfill transaction writes.
-define(BACKFILL_CHUNK, 500).

%% Seq keys are zero-padded to a fixed width so the tree's byte order is
%% the numeric order.
-define(SEQ_KEY_WIDTH, 20).

%% The counter, bumped in the append transaction. `last_assigned' is the
%% highest seq handed out.
-define(GLOBAL_SEQ_COUNTER_PATH, [metadata, global_seq, last_assigned]).

%% Set by backfill/1 and never cleared. An append or a read before it is
%% refused: a store is either on this numbering or not at all.
-define(GLOBAL_SEQ_BACKFILLED_PATH, [metadata, global_seq, backfilled]).

%% [global_seq, <padded seq>] -> #{stream_id, version}
-define(GLOBAL_SEQ_ROOT, [global_seq]).

%% [global_seq_ref, Type, Id, PaddedVersion] -> Seq
-define(GLOBAL_SEQ_REF_ROOT, [global_seq_ref]).

%% The authenticated head: #{from, last, chain, mac}. See the module doc.
-define(GLOBAL_SEQ_HEAD_PATH, [metadata, integrity, global_seq_head]).

%% The chain's first link starts from all-zero bytes, the same genesis
%% value gater's event chain uses, so the chain function has no nil branch.
-define(GENESIS_CHAIN, <<0:256>>).

%%====================================================================
%% Paths
%%====================================================================

%% @doc The tree path a sequence number's reference lives at.
-spec path(seq()) -> ['global_seq' | binary(), ...].
path(Seq) when is_integer(Seq), Seq >= 0 ->
    ?GLOBAL_SEQ_ROOT ++ [seq_key(Seq)].

%% @doc The tree path that maps one event (stream id + version) to its seq.
-spec ref_path(binary(), non_neg_integer()) -> ['global_seq_ref' | 'streams' | binary(), ...].
ref_path(StreamId, Version) ->
    [_Streams, Type, Id] = reckon_db_stream_path:stream_path(StreamId),
    ?GLOBAL_SEQ_REF_ROOT ++ [Type, Id, reckon_db_stream_path:version_key(Version)].

%% @private
-spec seq_key(seq()) -> binary().
seq_key(Seq) ->
    iolist_to_binary(io_lib:format("~20..0B", [Seq])).

%%====================================================================
%% Assignment (append transaction)
%%====================================================================

%% @doc Prepare the head for an append, OUTSIDE the transaction.
%%
%% The chain and its MAC cannot be computed inside a Khepri transaction:
%% horus will not extract a crypto call, which is the same reason the event
%% MACs are computed before `write_batch'. So the head the append will
%% write is built here, from the counter and the head this call reads, and
%% the transaction refuses to write it if either moved in between - the
%% read-then-verify shape the version guard already uses, for the same
%% reason.
%%
%% Answers `none' on a store with no integrity key: there is nothing to
%% authenticate with. Answers `{error, _}' when the log cannot be
%% authenticated at all - a head missing over seqs that exist, a head that
%% does not verify, or a head and counter that disagree - and the append is
%% then refused rather than extended.
-spec prepare(atom(), [{khepri_path:native_path(), event()}]) ->
        none | {head, seq() | none, head()} | {error, term()}.
prepare(StoreId, Writes) ->
    case reckon_db_integrity_key:get(StoreId) of
        undefined -> none;
        Key -> prepare_head(StoreId, Key, Writes)
    end.

%% @private The counter and the head, read in ONE snapshot.
%%
%% They are written in one Ra command, so they only agree with each other
%% inside one: two separate reads let an append commit between them and
%% hand this call a counter from before it and a head from after it.
-spec snapshot(atom()) -> {ok, seq() | none, term()} | {error, term()}.
snapshot(StoreId) ->
    Fun = fun() ->
                  Last = last_assigned_in_tx(),
                  Head = khepri_tx:get(?GLOBAL_SEQ_HEAD_PATH),
                  {Last, in_tx_value(Head)}
          end,
    case khepri:transaction(StoreId, Fun) of
        {ok, {Last, Value}} -> {ok, Last, Value};
        {error, Reason} -> {error, Reason}
    end.

%% @private
in_tx_value({ok, Value}) -> Value;
in_tx_value(_) -> undefined.

%% @private
prepare_head(StoreId, Key, Writes) ->
    case snapshot(StoreId) of
        {ok, Last, HeadValue} ->
            prepare_snapshot(StoreId, Key, Last, HeadValue, Writes);
        {error, _} = Error ->
            Error
    end.

%% @private
prepare_snapshot(StoreId, Key, none, undefined, Writes) ->
    %% Nothing numbered yet: the chain begins at this block.
    build_head(StoreId, Key, 0, ?GENESIS_CHAIN, none, Writes);
prepare_snapshot(StoreId, _Key, _Last, undefined, _Writes) ->
    {error, {global_seq_head_missing, StoreId}};
prepare_snapshot(StoreId, Key, Last, HeadValue, Writes) ->
    case parse_head(HeadValue) of
        {ok, Head} -> prepared_from(StoreId, Key, Head, Last, Writes);
        malformed -> {error, {global_seq_head_unverified, StoreId}}
    end.

%% @private
prepared_from(StoreId, Key, #{last := HeadLast} = Head, Last, Writes) ->
    case {head_mac_matches(StoreId, Key, Head), HeadLast =:= counter(Last)} of
        {true, true} ->
            build_head(StoreId, Key, maps:get(from, Head),
                       maps:get(chain, Head), Last, Writes);
        {true, false} ->
            {error, {global_seq_head_counter_mismatch, StoreId}};
        {false, _} ->
            {error, {global_seq_head_unverified, StoreId}}
    end.

%% @private
counter(none) -> -1;
counter(Last) -> Last.

%% @private The head that covers this block, given the head it extends.
build_head(StoreId, Key, From, Chain0, ExpectedLast, Writes) ->
    Base = base(ExpectedLast),
    Triples = [{Base + I, Event#event.stream_id, Event#event.version}
               || {I, {_Path, Event}} <-
                      lists:zip(lists:seq(0, length(Writes) - 1), Writes)],
    {Last, Chain} = fold_chain(Triples, ExpectedLast, Chain0),
    {head, ExpectedLast, head_record(StoreId, Key, From, Last, Chain)}.

%% @private One link per (seq, stream, version), from the previous one.
-spec fold_chain([{seq(), binary(), non_neg_integer()}], seq() | none,
                 binary()) -> {seq(), binary()}.
fold_chain(Triples, InitialLast, InitialChain) ->
    lists:foldl(fun({Seq, StreamId, Version}, {_Last, Chain}) ->
                        {Seq, link(Chain, Seq, StreamId, Version)}
                end,
                {counter(InitialLast), InitialChain}, Triples).

%% @private
base(none) -> 0;
base(Last) -> Last + 1.

%% @doc Assign a seq to each event of a batch, inside the append transaction.
%%
%% One append is one Ra command, and one batch's seqs are contiguous. The
%% counter and both ref directions are written here, so they commit or not
%% with the events themselves. With a prepared head, the head commits with
%% them too.
%%
%% Refused, by aborting the transaction, when the store has not been
%% backfilled: numbering new events from 0 beside a store whose existing
%% events have no number would make two events share a seq. With a prepared
%% head, also refused when the counter moved after it was prepared: another
%% append took those seqs, and the head built for them no longer describes
%% this batch. That abort is retried by the append path, like a version
%% conflict.
-spec assign_in_tx(atom(), [{khepri_path:native_path(), event()}],
                   none | {head, seq() | none, head()}) ->
        [{seq(), binary(), non_neg_integer()}].
assign_in_tx(_StoreId, [], _Prepared) ->
    [];
assign_in_tx(StoreId, Writes, Prepared) ->
    ok = require_started(),
    ok = counter_unchanged(StoreId, Prepared),
    Base = take_block(length(Writes)),
    Assigned = assign(Writes, Base),
    ok = write_head_in_tx(Prepared),
    Assigned.

%% @private The seqs this block takes, and both directions of each.
-spec assign([{khepri_path:native_path(), event()}], seq()) ->
        [{seq(), binary(), non_neg_integer()}].
assign(Writes, Base) ->
    Assigned = [{Base + I, Event#event.stream_id, Event#event.version}
                || {I, {_EventPath, Event}} <-
                       lists:zip(lists:seq(0, length(Writes) - 1), Writes)],
    lists:foreach(
      fun({{EventPath, Event}, {Seq, _StreamId, _Version}}) ->
              write_ref(Seq, Event, ref_path_of(EventPath))
      end,
      lists:zip(Writes, Assigned)),
    Assigned.

%% @private A prepared head describes one block of seqs. If another append
%% committed first, they are not these seqs any more.
-spec counter_unchanged(atom(), none | {head, seq() | none, head()}) -> ok.
counter_unchanged(_StoreId, none) ->
    ok;
counter_unchanged(StoreId, {head, ExpectedLast, _Head}) ->
    case last_assigned_in_tx() of
        ExpectedLast -> ok;
        _ -> khepri_tx:abort({global_seq_head_moved, StoreId})
    end.

%% @private
-spec write_head_in_tx(none | {head, seq() | none, head()}) -> ok.
write_head_in_tx(none) ->
    ok;
write_head_in_tx({head, _ExpectedLast, Head}) ->
    khepri_tx:put(?GLOBAL_SEQ_HEAD_PATH, Head).

%% @private The reverse-ref path, derived from the event's own path.
%%
%% Derived, not rebuilt: the event path is already
%% `[streams, Type, Id, PaddedVersion]', and splitting a stream id again
%% inside a transaction is denied (the splitter calls binary:split/2,
%% which horus refuses to extract into a tx fun).
-spec ref_path_of(khepri_path:native_path()) -> khepri_path:native_path().
ref_path_of([_Streams | Rest]) ->
    ?GLOBAL_SEQ_REF_ROOT ++ Rest.

%% @private Reserve `N' contiguous seqs and write the new high-water mark.
-spec take_block(pos_integer()) -> seq().
take_block(N) ->
    Base = next_base(last_assigned_in_tx()),
    ok = khepri_tx:put(?GLOBAL_SEQ_COUNTER_PATH, Base + N - 1),
    Base.

%% @private An absent counter is `none', and the first seq a store hands
%% out is 0.
-spec next_base(seq() | none) -> seq().
next_base(none) -> 0;
next_base(Last) -> Last + 1.

%% @private An absent counter is `none', and `none + 1' is 0: the first seq
%% a store hands out.
-spec last_assigned_in_tx() -> seq() | none.
last_assigned_in_tx() ->
    case khepri_tx:get(?GLOBAL_SEQ_COUNTER_PATH) of
        {ok, Last} when is_integer(Last), Last >= 0 -> Last;
        _ -> none
    end.

%% @private A store is either backfilled or has nothing to backfill.
%%
%% A store with events and no backfill is refused: numbering its new events
%% from 0 beside survivors that have no number would make two events share
%% a seq, and a backfill that failed at store start leaves exactly that
%% shape. A store with no events has nothing to number, so the first append
%% starts at 0 and marks the store done in the same transaction.
-spec require_started() -> ok.
require_started() ->
    case {is_backfilled_in_tx(), event_count_in_tx()} of
        {true, _} ->
            ok;
        {false, 0} ->
            %% Nothing here to number, so this append is where the store
            %% starts: the first seq is 0 and the marker goes in with it.
            ok = khepri_tx:put(?GLOBAL_SEQ_BACKFILLED_PATH, true),
            ok;
        {false, _} ->
            khepri_tx:abort(store_not_migrated)
    end.

%% @private
-spec event_count_in_tx() -> non_neg_integer().
event_count_in_tx() ->
    case khepri_tx:get(?GLOBAL_EVENT_COUNT_PATH) of
        {ok, Count} when is_integer(Count), Count > 0 -> Count;
        _ -> 0
    end.

%% @private
-spec is_backfilled_in_tx() -> boolean().
is_backfilled_in_tx() ->
    case khepri_tx:get(?GLOBAL_SEQ_BACKFILLED_PATH) of
        {ok, true} -> true;
        _ -> false
    end.

%% @private Both directions of the same fact, in the same transaction.
-spec write_ref(seq(), event(), khepri_path:native_path()) -> ok.
write_ref(Seq, #event{stream_id = StreamId, version = Version}, RefPath) ->
    ok = khepri_tx:put(path(Seq), #{stream_id => StreamId, version => Version}),
    ok = khepri_tx:put(RefPath, Seq).

%%====================================================================
%% The authenticated head (read side)
%%====================================================================

%% @doc The head as the store holds it, unverified.
%%
%% `verify/1' is what says whether it is the head this store wrote; this
%% answers the shape, for a tool that wants to show it.
-spec head(atom()) -> {ok, head()} | none.
head(StoreId) ->
    case read_head(StoreId) of
        {ok, Head} -> {ok, Head};
        _ -> none
    end.

%% @private The head, or `malformed' for a value that is not one: a write
%% must not extend it and a verify must not read it as absent.
-spec read_head(atom()) -> {ok, head()} | none | malformed.
read_head(StoreId) ->
    case khepri:get(StoreId, ?GLOBAL_SEQ_HEAD_PATH) of
        {ok, Value} -> parse_head(Value);
        _ -> none
    end.

%% @private
-spec parse_head(term()) -> {ok, head()} | malformed.
parse_head(#{from := From, last := Last, chain := Chain, mac := Mac})
        when is_integer(From), From >= 0, is_integer(Last), Last >= From,
             is_binary(Chain), is_binary(Mac) ->
    {ok, #{from => From, last => Last, chain => Chain, mac => Mac}};
parse_head(_) ->
    malformed.

%% @doc Does the seq log still hold what this store wrote?
%%
%% Recomputed from the tree, not from the head: every
%% `[global_seq, N] -> #{stream_id, version}' mapping from `from' to
%% `last', the counter that must agree with `last', and nothing above it.
%% A head that does not verify, a mapping that moved, a missing mapping and
%% a mapping beyond the head are each reported by name, because they mean
%% different things: a resume point that was rewritten, one that was
%% deleted, and a seq this store never authenticated.
%%
%% O(seqs in the store): this is a check an operator or a tool runs, not a
%% per-read cost.
-spec verify(atom()) ->
        ok | {error, {integrity_key_absent | head_absent, atom()} |
                    {head_mac_mismatch | head_malformed, atom()} |
                    {ref_missing | ref_beyond_head, seq()} |
                    {ref_moved, seq()} |
                    {head_counter_mismatch, seq(), seq()}}.
verify(StoreId) ->
    case reckon_db_integrity_key:get(StoreId) of
        undefined -> {error, {integrity_key_absent, StoreId}};
        Key -> verify_head(StoreId, Key)
    end.

%% @private
verify_head(StoreId, Key) ->
    case read_head(StoreId) of
        none -> {error, {head_absent, StoreId}};
        malformed -> {error, {head_malformed, StoreId}};
        {ok, Head} -> verify_head_mac(StoreId, Key, Head)
    end.

%% @private
verify_head_mac(StoreId, Key, #{from := From, last := Last, chain := Chain} = Head) ->
    case head_mac_matches(StoreId, Key, Head) of
        true -> verify_entries(StoreId, From, Last, Chain);
        false -> {error, {head_mac_mismatch, StoreId}}
    end.

%% @private
verify_entries(StoreId, From, Last, Expected) ->
    case entries(StoreId) of
        {ok, Entries} -> verify_entries_counter(StoreId, Entries, From, Last, Expected);
        {error, _} = Error -> Error
    end.

%% @private
verify_entries_counter(StoreId, Entries, From, Last, Expected) ->
    case verify_counter(StoreId, Last) of
        ok -> chain_from(Entries, From, Last, Expected);
        {error, _} = Error -> Error
    end.

%% @private The counter and the head are written in one Ra command, so they
%% disagree only if one of them was changed.
verify_counter(StoreId, Last) ->
    case last_assigned(StoreId) of
        {ok, Last} -> ok;
        none -> {error, {head_counter_mismatch, Last, -1}};
        {ok, Other} -> {error, {head_counter_mismatch, Last, Other}}
    end.

%% @private Recompute the chain over the entries the head covers and check
%% that nothing sits above it.
chain_from(Entries, From, Last, Expected) ->
    case [Seq || {Seq, _, _} <- Entries, Seq > Last] of
        [Above | _] ->
            {error, {ref_beyond_head, Above}};
        [] ->
            chain_over(lists:sort([E || {Seq, _, _} = E <- Entries, Seq >= From]),
                       From, Last, ?GENESIS_CHAIN, Expected)
    end.

%% @private Walk every seq in [From, Last]; a gap is a mapping deleted out
%% of the log, and a chain that lands somewhere else is a mapping rewritten.
chain_over(_Entries, Cursor, Last, _Chain, _Expected) when Cursor > Last ->
    {error, {ref_missing, Last}};
chain_over([], Cursor, _Last, _Chain, _Expected) ->
    {error, {ref_missing, Cursor}};
chain_over([{Seq, _, _} | Rest], Cursor, Last, Chain, Expected) when Seq < Cursor ->
    %% Cannot arise after the filter above; consuming it keeps the walk
    %% linear instead of looping.
    chain_over(Rest, Cursor, Last, Chain, Expected);
chain_over([{Seq, _, _} | _Rest], Cursor, _Last, _Chain, _Expected)
        when Seq > Cursor ->
    {error, {ref_missing, Cursor}};
chain_over([{Cursor, StreamId, Version} | Rest], Cursor, Last, Chain, Expected) ->
    Next = link(Chain, Cursor, StreamId, Version),
    case Cursor =:= Last of
        true -> chain_match(Next, Expected, Cursor);
        false -> chain_over(Rest, Cursor + 1, Last, Next, Expected)
    end.

%% @private
chain_match(Chain, Chain, _Seq) -> ok;
chain_match(_Actual, _Expected, Seq) -> {error, {ref_moved, Seq}}.


%% @doc Seal a head over the seqs the store already holds, when it has
%% integrity and none.
%%
%% Run at store start, after the backfill and before anything is served:
%% the case it exists for is a store numbered while integrity was off, and
%% then started with it on. A store whose head is present but does not
%% verify is not re-sealed - that would launder the tampering - and is
%% reported instead; its appends stay refused.
-spec ensure_head(atom()) -> ok | {error, {head_unverified, atom()}}.
ensure_head(StoreId) ->
    case reckon_db_integrity_key:get(StoreId) of
        undefined -> ok;
        Key -> ensure_head(StoreId, Key)
    end.

%% @private
ensure_head(StoreId, Key) ->
    case read_head(StoreId) of
        none -> seal_existing(StoreId, Key);
        malformed -> report_head_unverified(StoreId);
        {ok, Head} -> verify_existing(StoreId, Key, Head)
    end.

%% @private
verify_existing(StoreId, Key, Head) ->
    case head_mac_matches(StoreId, Key, Head) of
        true -> ok;
        false -> report_head_unverified(StoreId)
    end.

%% @private
report_head_unverified(StoreId) ->
    logger:error("store ~p holds a global sequence head that does not "
                 "verify; it is not re-sealed and its appends stay refused",
                 [StoreId]),
    telemetry:execute([reckon, db, integrity, global_seq_head_unverified],
                      #{system_time => erlang:system_time(millisecond)},
                      #{store_id => StoreId}),
    {error, {head_unverified, StoreId}}.

%% @private Seal over every seq the store holds. Absent seqs mean nothing
%% was ever numbered here, and the first append mints the head instead.
seal_existing(StoreId, Key) ->
    case entries(StoreId) of
        {ok, []} -> ok;
        {ok, Entries} -> seal_entries(StoreId, Key, Entries);
        {error, Reason} -> report_seal_failure(StoreId, Reason)
    end.

%% @private
seal_entries(StoreId, Key, Entries) ->
    {Last, Chain} = fold_chain(lists:sort(Entries), 0, ?GENESIS_CHAIN),
    write_head_directly(StoreId, head_record(StoreId, Key, 0, Last, Chain)).

%% @private
report_seal_failure(StoreId, Reason) ->
    logger:warning("store ~p: could not read its sequence log to seal a "
                   "head: ~p", [StoreId, Reason]),
    {error, Reason}.

%% @private In a transaction, so a concurrent write cannot land between the
%% read above and this write; nothing else writes this node at start.
write_head_directly(StoreId, Head) ->
    Fun = fun() -> put_head_if_absent(Head) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @private
put_head_if_absent(Head) ->
    case khepri_tx:get(?GLOBAL_SEQ_HEAD_PATH) of
        {ok, _} ->
            present;
        _ ->
            ok = khepri_tx:put(?GLOBAL_SEQ_HEAD_PATH, Head),
            sealed
    end.

%% @private Every mapping in the seq log, in seq order.
-spec entries(atom()) -> {ok, [{seq(), binary(), non_neg_integer()}]} |
                         {error, term()}.
entries(StoreId) ->
    Pattern = ?GLOBAL_SEQ_ROOT ++ [?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Pattern) of
        {ok, Results} ->
            {ok, lists:sort([entry(Path, Value)
                             || {Path, Value} <- maps:to_list(Results)])};
        {error, _} = Error ->
            Error
    end.

%% @private
entry(Path, #{stream_id := StreamId, version := Version})
        when is_binary(StreamId), is_integer(Version) ->
    {seq_of_key(lists:last(Path)), StreamId, Version};
entry(Path, _Value) ->
    %% A node that is not a mapping: it keeps the seq its key names, so the
    %% walk reports the seq it broke at instead of silently shifting the
    %% rest of the chain.
    {seq_of_key(lists:last(Path)), <<>>, -1}.

%% @private
seq_of_key(Key) when is_binary(Key) ->
    try binary_to_integer(Key)
    catch _:_ -> -1
    end;
seq_of_key(_) ->
    -1.

%%====================================================================
%% The head record and its chain
%%====================================================================

%% @private
-spec head_record(atom(), binary(), seq(), seq(), binary()) -> head().
head_record(StoreId, Key, From, Last, Chain) ->
    #{from => From,
      last => Last,
      chain => Chain,
      mac => head_mac(StoreId, Key, From, Last, Chain)}.

%% @private
-spec head_mac(atom(), binary(), seq(), seq(), binary()) -> binary().
head_mac(StoreId, Key, From, Last, Chain) ->
    crypto:mac(hmac, sha256, Key,
               term_to_binary({?MODULE, head, StoreId, From, Last, Chain},
                              [deterministic])).

%% @private
-spec head_mac_matches(atom(), binary(), head()) -> boolean().
head_mac_matches(StoreId, Key, #{from := From, last := Last, chain := Chain,
                                 mac := Mac}) ->
    crypto:hash_equals(Mac, head_mac(StoreId, Key, From, Last, Chain)).

%% @private One link of the chain: the previous link, and the mapping this
%% seq names. Domain-separated by the module tag, deterministic.
-spec link(binary(), seq(), binary(), non_neg_integer()) -> binary().
link(Previous, Seq, StreamId, Version) ->
    crypto:hash(sha256,
                [Previous,
                 term_to_binary({?MODULE, link, Seq, StreamId, Version},
                                [deterministic])]).


%%====================================================================
%% Reading
%%====================================================================

%% @doc The highest seq handed out, or `none' when the store has none.
%%
%% A backfill of a store with no events reserves an empty block, which
%% leaves the counter at -1: nothing has been numbered, and this answers
%% `none' rather than a seq below zero.
-spec last_assigned(atom()) -> {ok, seq()} | none.
last_assigned(StoreId) ->
    case khepri:get(StoreId, ?GLOBAL_SEQ_COUNTER_PATH) of
        {ok, Last} when is_integer(Last), Last >= 0 -> {ok, Last};
        _ -> none
    end.

%% @doc Has this store been backfilled?
-spec is_backfilled(atom()) -> boolean().
is_backfilled(StoreId) ->
    case khepri:get(StoreId, ?GLOBAL_SEQ_BACKFILLED_PATH) of
        {ok, true} -> true;
        _ -> false
    end.

%% @doc The seq of one event, which live delivery needs.
-spec seq_for(atom(), binary(), non_neg_integer()) -> {ok, seq()} | not_found.
seq_for(StoreId, StreamId, Version) ->
    case khepri:get(StoreId, ref_path(StreamId, Version)) of
        {ok, Seq} when is_integer(Seq) -> {ok, Seq};
        _ -> not_found
    end.

%% @doc The first `BatchSize' events at or after `Cursor', with their seqs.
%%
%% The cursor is INCLUSIVE, which is what `read_all_global/3''s offset was:
%% a checkpoint therefore keeps its meaning across the upgrade with no
%% translation table - at the moment of the backfill a survivor's old offset
%% equals its new seq. Inclusivity also cannot skip an event if a consumer
%% checkpoints the seq it just processed: it sees that one again
%% (at-least-once) rather than missing the next.
%%
%% Point reads forward from the cursor, so a page costs what the page is
%% worth (plus the holes a scavenge left). A seq whose event has been
%% scavenged resolves to nothing and is skipped, not an error: scavenge
%% deletes event nodes and leaves both refs, so a stale ref is an ordinary
%% shape, and stopping there would strand every subscriber behind one
%% scavenged stream.
-spec read_from(atom(), seq() | none, pos_integer()) ->
        {ok, [{seq(), event()}]} | {error, term()}.
read_from(StoreId, Cursor, BatchSize) when is_integer(BatchSize), BatchSize > 0 ->
    case is_backfilled(StoreId) of
        true ->
            read_forward(StoreId, cursor(Cursor), last_seq(StoreId), BatchSize, []);
        false ->
            {error, {store_not_migrated, StoreId}}
    end.

%% @private
last_seq(StoreId) ->
    case last_assigned(StoreId) of
        {ok, Last} -> Last;
        none -> -1
    end.

%% @private `none' is "nothing consumed yet", which is seq 0.
-spec cursor(seq() | none) -> seq().
cursor(none) -> 0;
cursor(Cursor) when is_integer(Cursor), Cursor >= 0 -> Cursor.

%% @private
read_forward(_StoreId, Cursor, Last, _BatchSize, Acc) when Cursor > Last ->
    {ok, lists:reverse(Acc)};
read_forward(_StoreId, _Cursor, _Last, BatchSize, Acc) when length(Acc) >= BatchSize ->
    {ok, lists:reverse(Acc)};
read_forward(StoreId, Cursor, Last, BatchSize, Acc) ->
    case event_at(StoreId, Cursor) of
        {ok, Event} ->
            read_forward(StoreId, Cursor + 1, Last, BatchSize,
                         [{Cursor, Event} | Acc]);
        hole ->
            read_forward(StoreId, Cursor + 1, Last, BatchSize, Acc)
    end.

%% @private Resolve one seq to its event: the ref names the stream and the
%% version, and the event node is the authority on whether it still exists.
-spec event_at(atom(), seq()) -> {ok, event()} | hole.
event_at(StoreId, Seq) ->
    case khepri:get(StoreId, path(Seq)) of
        {ok, #{stream_id := StreamId, version := Version}}
                when is_binary(StreamId), is_integer(Version) ->
            resolve_event(StoreId, StreamId, Version);
        _ ->
            hole
    end.

%% @private
resolve_event(StoreId, StreamId, Version) ->
    EventPath = reckon_db_stream_path:event_path(
                  StreamId, reckon_db_stream_path:version_key(Version)),
    case khepri:get(StoreId, EventPath) of
        {ok, #event{} = Event} -> {ok, Event};
        _ -> hole
    end.

%% @doc Attach the seq to an event as `global_position' in its metadata.
%%
%% The stored event is never changed; this is the copy handed to a
%% subscriber, and the key is the one evoq and the guides already read.
-spec annotate(atom(), event()) -> event().
annotate(StoreId, #event{stream_id = StreamId, version = Version} = Event) ->
    case seq_for(StoreId, StreamId, Version) of
        {ok, Seq} -> positioned(Seq, Event);
        not_found -> Event
    end.

%% @doc The event as a consumer receives it: its seq in the metadata under
%% `global_position', the key a projection checkpoints on.
%%
%% `global_position' is added to the DELIVERED copy and is therefore not
%% covered by the event's MAC, which is computed over the stored record.
%% The store verifies an event before it delivers it (catch-up does; live
%% delivery hands over what the trigger fired on); a consumer that verifies
%% a delivered event itself must strip `global_position' from the metadata
%% first.
-spec positioned(seq(), event()) -> event().
positioned(Seq, #event{metadata = Metadata} = Event) when is_map(Metadata) ->
    Event#event{metadata = Metadata#{global_position => Seq}};
positioned(Seq, #event{} = Event) ->
    Event#event{metadata = #{global_position => Seq}}.

%%====================================================================
%% Backfill
%%====================================================================

%% @doc Number a store's surviving events once, in the order the reader
%% produced before this release.
%%
%% Numbering survivors in today's reader order is what lets a checkpoint
%% written before the upgrade keep its meaning: at the moment of the
%% upgrade, a survivor's old offset equals its new seq. A store that
%% scavenged before the upgrade has already lost that correspondence (the
%% offset shifted then), so its pre-upgrade checkpoints may point past
%% unprocessed events, and that cannot be reconstructed from what is left;
%% the remedy is one rebuild per affected projection.
%%
%% The counter is reserved in one transaction before anything is written,
%% so an append that lands mid-backfill takes a seq above the reserved
%% block, and a reader cannot see the half-filled range (reads are refused
%% until the done marker is set, and the marker is set last).
-spec backfill(atom()) -> ok | {error, term()}.
backfill(StoreId) ->
    case is_backfilled(StoreId) of
        true ->
            ok;
        false ->
            backfill_fresh(StoreId)
    end.

%% @private
backfill_fresh(StoreId) ->
    case reckon_db_streams:all_events_in_global_order(StoreId) of
        {ok, Events} -> number_survivors(StoreId, Events);
        {error, _} = Error -> Error
    end.

%% @private
number_survivors(StoreId, Events) ->
    case reserve(StoreId, length(Events)) of
        {ok, none} -> ok;
        {ok, Base} -> fill_and_mark(StoreId, Base, Events);
        {error, _} = Error -> Error
    end.

%% @private
fill_and_mark(StoreId, Base, Events) ->
    case fill(StoreId, Base, Events) of
        ok -> mark_backfilled(StoreId);
        {error, _} = Error -> Error
    end.

%% @private Reserve the block before writing it.
reserve(StoreId, Count) ->
    Base = case last_assigned(StoreId) of
               none -> 0;
               {ok, Last} -> Last + 1
           end,
    Fun = fun() -> reserve_in_tx(StoreId, Base, Count) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, reserved} -> {ok, Base};
        {ok, already_backfilled} -> {ok, none};
        {ok, {error, Reason}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

%% @private
reserve_in_tx(StoreId, Base, Count) ->
    case is_backfilled_in_tx() of
        true -> already_backfilled;
        false -> start_reservation(StoreId, Base, Count)
    end.

%% @private
start_reservation(StoreId, Base, Count) ->
    case last_assigned_in_tx() of
        none -> ok;
        _Last -> khepri_tx:abort({global_seq_already_started, StoreId})
    end,
    ok = khepri_tx:put(?GLOBAL_SEQ_COUNTER_PATH, Base + Count - 1),
    reserved.

%% @private
fill(_StoreId, _Base, []) ->
    ok;
fill(StoreId, Base, Events) ->
    {Chunk, Rest} = lists:split(min(?BACKFILL_CHUNK, length(Events)), Events),
    %% The reverse-ref paths are built here, outside the transaction:
    %% deriving them needs the stream id split, and horus refuses to
    %% extract that call into a tx fun.
    Rows = [{Base + I, Event, ref_path_of(event_path(Event))}
            || {I, Event} <- lists:zip(lists:seq(0, length(Chunk) - 1), Chunk)],
    Fun = fun() -> write_chunk(Rows) end,
    case khepri:transaction(StoreId, Fun) of
        {ok, ok} ->
            fill(StoreId, Base + length(Chunk), Rest);
        {ok, {error, Reason}} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
write_chunk(Rows) ->
    lists:foreach(fun({Seq, Event, RefPath}) -> write_ref(Seq, Event, RefPath) end,
                  Rows),
    ok.

%% @private
event_path(#event{stream_id = StreamId, version = Version}) ->
    reckon_db_stream_path:event_path(
      StreamId, reckon_db_stream_path:version_key(Version)).

%% @private
mark_backfilled(StoreId) ->
    Fun = fun() ->
                  ok = khepri_tx:put(?GLOBAL_SEQ_BACKFILLED_PATH, true),
                  ok
          end,
    case khepri:transaction(StoreId, Fun) of
        {ok, ok} -> ok;
        {ok, {error, Reason}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.
