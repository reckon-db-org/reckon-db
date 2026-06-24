%%% @doc DCB conditional-append primitive.
%%%
%%% Implements append_if_no_tag_matches/4 as a khepri:transaction/2
%%% body. The transaction:
%%%
%%%   1. Verifies the tag-filter context ({context_changed, _} on
%%%      conflict).
%%%   2. For integrity-enabled stores: verifies the seq counter and
%%%      chain-tip still match what the caller observed when
%%%      pre-stamping MACs. On mismatch aborts with
%%%      {dcb_state_changed, _} and the outer loop retries with a
%%%      fresh snapshot.
%%%   3. Writes events under ?DCB_STREAM_PATH ++ [SeqKey] plus one
%%%      tag-index entry per tag at ?BY_TAG_PATH ++ [Tag, SeqKey].
%%%   4. Updates the seq counter (and chain-tip if integrity is
%%%      enabled).
%%%
%%% The whole sequence happens inside one Ra log entry; atomic across
%%% the cluster. Either everything commits, or an abort comes back
%%% and nothing changed.
%%%
%%% == Integrity (reckon-db 3.2.0+) ==
%%%
%%% On integrity-enabled stores, DCB events carry prev_event_hash +
%%% mac like any other integrity-bearing event. Because Khepri's
%%% Horus extractor rejects code containing crypto:* calls (even on
%%% unreachable branches; extraction sees the whole function), MAC
%%% chains are pre-computed OUTSIDE the transaction. Inside the
%%% transaction we verify the chain-tip + counter are still what we
%%% saw, then write the pre-stamped records.
%%%
%%% Two retry vectors:
%%%   - {context_changed, _}: tag-filter conflict. Callers (e.g.,
%%%     evoq_decision_runtime) retry with fresh context.
%%%   - {dcb_state_changed, _}: counter or chain-tip moved between
%%%     our pre-stamp read and the transaction. The retry loop in this
%%%     module handles it transparently, bounded by
%%%     ?INTEGRITY_RETRY_BUDGET.
%%% @end
-module(reckon_db_dcb).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    append_if_no_tag_matches/4,
    read_log/3,
    all_tags/1,
    all_event_types/1,
    read_by_payload/4,
    read_by_payload_hash/4
]).

-ifdef(TEST).
%% Exposed for unit tests: payload-entry extraction must accept both
%% atom-keyed (evoq-built) and binary-keyed (JSON-decoded) data maps.
-export([extract_payload_entries/2]).
-endif.

%% Bounded retry budget for the pre-stamp / transaction race in
%% integrity-on stores. Heavy contention escalates rather than spins.
-define(INTEGRITY_RETRY_BUDGET, 5).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Read DCB events in ascending seq order.
%%
%% FromSeq is the first sequence number to include (0 for the beginning).
%% Limit is the maximum number of events to return.
%%
%% Returns {ok, Events, TotalCount} where TotalCount is the total
%% number of events in the DCB log (used for pagination).
-spec read_log(atom(), non_neg_integer(), pos_integer()) ->
    {ok, [#event{}], non_neg_integer()} | {error, term()}.
read_log(StoreId, FromSeq, Limit) when is_integer(FromSeq), is_integer(Limit) ->
    Pattern = ?DCB_STREAM_PATH ++ [?KHEPRI_WILDCARD_STAR],
    FromKey = reckon_db_dcb_paths:seq_key(FromSeq),
    case khepri:get_many(StoreId, Pattern) of
        {ok, NodeMap} when is_map(NodeMap) ->
            TotalCount = maps:size(NodeMap),
            Events = maps:fold(fun
                ([_, _, SeqKey], #event{} = E, Acc) when SeqKey >= FromKey ->
                    [E | Acc];
                (_, _, Acc) ->
                    Acc
            end, [], NodeMap),
            Sorted = lists:sort(fun(A, B) -> A#event.version =< B#event.version end, Events),
            {ok, lists:sublist(Sorted, Limit), TotalCount};
        {error, {khepri, node_not_found, _}} ->
            {ok, [], 0};
        {error, _} = Err ->
            Err
    end.

%% @doc Return all tags present in the DCB log, sorted by event count descending.
%%
%% Each tag is returned as {Tag, EventCount}. Uses the by_tag index
%% so it does not scan DCB events directly.
-spec all_tags(atom()) -> {ok, [{binary(), non_neg_integer()}]} | {error, term()}.
all_tags(StoreId) ->
    Pattern = ?BY_TAG_PATH ++ [?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Pattern) of
        {ok, NodeMap} when is_map(NodeMap) ->
            Counts = maps:fold(fun
                ([_Root, Tag, _SeqKey], _, Acc) when is_binary(Tag) ->
                    maps:update_with(Tag, fun(C) -> C + 1 end, 1, Acc);
                (_, _, Acc) ->
                    Acc
            end, #{}, NodeMap),
            Sorted = lists:sort(fun({_, C1}, {_, C2}) -> C1 >= C2 end,
                                maps:to_list(Counts)),
            {ok, Sorted};
        {error, {khepri, node_not_found, _}} ->
            {ok, []};
        {error, _} = Err ->
            Err
    end.

%% @doc Return all event types present in the DCB log, sorted by count descending.
%%
%% Each type is returned as {EventType, EventCount}. Uses the by_event_type index.
-spec all_event_types(atom()) -> {ok, [{binary(), non_neg_integer()}]} | {error, term()}.
all_event_types(StoreId) ->
    Pattern = ?BY_EVENT_TYPE_PATH ++ [?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Pattern) of
        {ok, NodeMap} when is_map(NodeMap) ->
            Counts = maps:fold(fun
                ([_Root, EventType, _SeqKey], _, Acc) when is_binary(EventType) ->
                    maps:update_with(EventType, fun(C) -> C + 1 end, 1, Acc);
                (_, _, Acc) ->
                    Acc
            end, #{}, NodeMap),
            Sorted = lists:sort(fun({_, C1}, {_, C2}) -> C1 >= C2 end,
                                maps:to_list(Counts)),
            {ok, Sorted};
        {error, {khepri, node_not_found, _}} ->
            {ok, []};
        {error, _} = Err ->
            Err
    end.

-spec append_if_no_tag_matches(
    StoreId   :: atom() | binary(),
    TagFilter :: reckon_gater_types:tag_filter(),
    SeqCutoff :: reckon_gater_types:seq_cutoff(),
    Events    :: [map()]
) ->
      {ok, LastSeq :: non_neg_integer()}
    | {error, {context_changed, non_neg_integer()}}
    | {error, no_events}
    | {error, dcb_concurrent_writer_exhausted}
    | {error, term()}.
append_if_no_tag_matches(_StoreId, _TagFilter, _SeqCutoff, []) ->
    {error, no_events};
append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events)
  when is_list(Events), is_integer(SeqCutoff) ->
    PayloadDecls = reckon_db_index_config:declared_dcb_payload(StoreId),
    ProcessedFilter = reckon_db_ccc_filter:preprocess_filter(TagFilter),
    IntegrityCtx = setup_integrity_ctx(StoreId),
    try_append(StoreId, ProcessedFilter, SeqCutoff, Events, IntegrityCtx,
               PayloadDecls, ?INTEGRITY_RETRY_BUDGET).

%%====================================================================
%% Outer retry loop (chain-tip / counter races on integrity-on stores)
%%====================================================================

try_append(_StoreId, _TF, _SC, _Events, _Ctx, _PD, 0) ->
    {error, dcb_concurrent_writer_exhausted};
try_append(StoreId, TagFilter, SeqCutoff, Events, IntegrityCtx, PayloadDecls, Retries) ->
    Now = erlang:system_time(millisecond),
    EpochUs = erlang:system_time(microsecond),
    Snapshot = take_snapshot(StoreId, IntegrityCtx),
    {Stamped, FinalTip} = stamp_events(Events, Now, EpochUs, Snapshot, PayloadDecls),
    case khepri:transaction(
           StoreId,
           fun() -> tx_body(TagFilter, SeqCutoff, Stamped, Snapshot, FinalTip) end) of
        {ok, LastSeq} when is_integer(LastSeq) ->
            {ok, LastSeq};
        %% Khepri 0.17.x catch-all wraps process_command errors as {ok, Err}
        %% when Ra is not yet ready (e.g. store startup race).
        {ok, {error, E}} ->
            {error, E};
        {error, {context_changed, _} = Reason} ->
            {error, Reason};
        {error, {dcb_state_changed, _}} ->
            try_append(StoreId, TagFilter, SeqCutoff, Events, IntegrityCtx,
                       PayloadDecls, Retries - 1);
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Outside transaction: integrity context + state snapshot
%%====================================================================

setup_integrity_ctx(StoreId) ->
    case reckon_db_integrity_key:is_enabled(StoreId) of
        false -> disabled;
        true  -> {enabled, reckon_db_integrity_key:get(StoreId)}
    end.

%% disabled snapshot:
%%   For integrity-off stores. The transaction picks seqs from the
%%   live counter; no pre-stamp verification needed beyond tag-filter.
%%
%% #{integrity := {enabled, Key}, ...} snapshot:
%%   For integrity-on stores. Carries the seq-counter value and
%%   chain-tip value that the transaction will verify haven't moved.
take_snapshot(_StoreId, disabled) ->
    disabled;
take_snapshot(StoreId, {enabled, Key}) ->
    ExpectedCounter =
        case khepri:get(StoreId, ?DCB_SEQ_COUNTER_PATH) of
            {ok, N} when is_integer(N) -> N;
            _                          -> undefined
        end,
    ExpectedTip =
        case khepri:get(StoreId, ?DCB_CHAIN_TIP_PATH) of
            {ok, T} when is_binary(T) -> T;
            _                         -> reckon_gater_integrity:genesis_prev_hash()
        end,
    NextSeq = case ExpectedCounter of
                  undefined -> 0;
                  N1        -> N1 + 1
              end,
    #{integrity         => {enabled, Key},
      expected_counter  => ExpectedCounter,
      expected_tip      => ExpectedTip,
      next_seq          => NextSeq}.

%%====================================================================
%% Outside transaction: pre-stamp event records
%%====================================================================

%% Returns {[{Seq | undefined, #event{}, [payload_entry()]}], FinalTip}.
%%   - For disabled snapshots, Seq is undefined (the transaction
%%     picks the seq from the live counter) and FinalTip is
%%     undefined (no chain to track).
%%   - For integrity-on snapshots, Seq is pre-assigned, the record
%%     carries prev_event_hash + mac, and FinalTip is the chain-hash
%%     of the last event.
%% PayloadDecls drives extraction of payload index entries from each record;
%% extraction calls crypto (via payload_combo_hash) so it must stay here,
%% outside the transaction body.
stamp_events(EventMaps, Now, EpochUs, disabled, PayloadDecls) ->
    Stamped = [begin
        R = build_event_record(EM, undefined, Now, EpochUs),
        PE = extract_payload_entries(R, PayloadDecls),
        {undefined, R, PE}
    end || EM <- EventMaps],
    {Stamped, undefined};
stamp_events(EventMaps, Now, EpochUs,
             #{integrity := {enabled, Key},
               expected_tip := Tip,
               next_seq := StartSeq},
             PayloadDecls) ->
    stamp_chain(EventMaps, StartSeq, Now, EpochUs, Key, Tip, PayloadDecls, []).

stamp_chain([], _Seq, _Now, _EpochUs, _Key, Tip, _PD, Acc) ->
    {lists:reverse(Acc), Tip};
stamp_chain([EM | Rest], Seq, Now, EpochUs, Key, Tip, PayloadDecls, Acc) ->
    Record0 = build_event_record(EM, Seq, Now, EpochUs),
    Record1 = Record0#event{prev_event_hash = Tip},
    Mac = reckon_gater_integrity:compute_event_mac(Record1, Key),
    Record2 = Record1#event{mac = Mac},
    NextTip = reckon_gater_integrity:compute_chain_hash(Record2, Tip),
    PE = extract_payload_entries(Record2, PayloadDecls),
    stamp_chain(Rest, Seq + 1, Now, EpochUs, Key, NextTip, PayloadDecls,
                [{Seq, Record2, PE} | Acc]).

%%====================================================================
%% Transaction body — strictly Horus-extractable
%%====================================================================
%% NO crypto, NO try/catch, NO message passing. Pre-stamped records
%% arrive ready to write; we verify state pre-conditions and commit.

%% Integrity-OFF path. The transaction picks seqs from the live counter.
tx_body(TagFilter, SeqCutoff, Stamped, disabled, _FinalTip) ->
    case reckon_db_ccc_filter:match_any_above_cutoff(TagFilter, SeqCutoff) of
        {true, MaxSeq} ->
            khepri_tx:abort({context_changed, MaxSeq});
        false ->
            BaseSeq = next_base_seq_in_tx(),
            LastSeq = write_off(Stamped, BaseSeq),
            ok = khepri_tx:put(?DCB_SEQ_COUNTER_PATH, LastSeq),
            LastSeq
    end;
%% Integrity-ON path. Verify counter + chain-tip match snapshot, then
%% write pre-stamped events at their pre-assigned seqs.
tx_body(TagFilter, SeqCutoff, Stamped,
        #{expected_counter := ExpectedCounter,
          expected_tip     := ExpectedTip,
          next_seq         := StartSeq},
        FinalTip) ->
    case verify_counter(ExpectedCounter) of
        {error, Actual} ->
            khepri_tx:abort({dcb_state_changed, {counter, Actual}});
        ok ->
            case verify_tip(ExpectedTip) of
                {error, Actual} ->
                    khepri_tx:abort({dcb_state_changed, {tip, Actual}});
                ok ->
                    case reckon_db_ccc_filter:match_any_above_cutoff(
                           TagFilter, SeqCutoff) of
                        {true, MaxSeq} ->
                            khepri_tx:abort({context_changed, MaxSeq});
                        false ->
                            LastSeq = write_on(Stamped, StartSeq),
                            ok = khepri_tx:put(?DCB_SEQ_COUNTER_PATH, LastSeq),
                            ok = khepri_tx:put(?DCB_CHAIN_TIP_PATH, FinalTip),
                            LastSeq
                    end
            end
    end.

%% Integrity-off write: live counter assigns seqs, stamp the seq into
%% the record and write at the right path.
write_off([], LastSeq) ->
    LastSeq;
write_off([{undefined, Record0, PayloadEntries} | Rest], Seq) ->
    Record = Record0#event{version = Seq},
    write_one_record(Record, Seq, PayloadEntries),
    case Rest of
        [] -> Seq;
        _  -> write_off(Rest, Seq + 1)
    end.

%% Integrity-on write: records already carry the right version (Seq).
write_on([], LastSeq) ->
    LastSeq;
write_on([{Seq, Record, PayloadEntries} | Rest], ExpectedSeq) ->
    %% Pre-assigned seq must equal the expected position in the chain.
    %% If these diverge, our snapshot was inconsistent — but the
    %% counter+tip verification already prevents that.
    case Seq =:= ExpectedSeq of
        true ->
            write_one_record(Record, Seq, PayloadEntries),
            case Rest of
                [] -> Seq;
                _  -> write_on(Rest, ExpectedSeq + 1)
            end;
        false ->
            khepri_tx:abort({dcb_state_changed, {seq_skew, Seq, ExpectedSeq}})
    end.

write_one_record(#event{event_type = EventType, tags = Tags} = Record, Seq,
                 PayloadEntries) ->
    ok = khepri_tx:put(reckon_db_dcb_paths:event_path(Seq), Record),
    TagList = case Tags of
                  undefined     -> [];
                  L when is_list(L) -> L
              end,
    lists:foreach(
        fun(Tag) when is_binary(Tag) ->
            ok = khepri_tx:put(reckon_db_dcb_paths:by_tag_path(Tag, Seq), #{})
        end,
        TagList),
    ok = khepri_tx:put(reckon_db_dcb_paths:by_event_type_path(EventType, Seq), #{}),
    lists:foreach(
        fun({single, Key, Value}) ->
            ok = khepri_tx:put(
                   reckon_db_ccc_paths:by_payload_path(Key, Value, Seq), #{});
           ({combo, Hash}) ->
            ok = khepri_tx:put(
                   reckon_db_ccc_paths:by_payload_hash_path(Hash, Seq), #{})
        end,
        PayloadEntries),
    ok.

verify_counter(ExpectedCounter) ->
    Actual = case khepri_tx:get(?DCB_SEQ_COUNTER_PATH) of
                 {ok, N} when is_integer(N) -> N;
                 _                          -> undefined
             end,
    case Actual =:= ExpectedCounter of
        true  -> ok;
        false -> {error, Actual}
    end.

verify_tip(ExpectedTip) ->
    Actual = case khepri_tx:get(?DCB_CHAIN_TIP_PATH) of
                 {ok, T} when is_binary(T) -> T;
                 %% Absent path means "no integrity-bearing DCB events
                 %% yet"; the genesis hash IS what we expected if the
                 %% snapshot also saw none.
                 _                         -> reckon_gater_integrity:genesis_prev_hash()
             end,
    case Actual =:= ExpectedTip of
        true  -> ok;
        false -> {error, Actual}
    end.

next_base_seq_in_tx() ->
    case khepri_tx:get(?DCB_SEQ_COUNTER_PATH) of
        {ok, LastAssigned} when is_integer(LastAssigned), LastAssigned >= 0 ->
            LastAssigned + 1;
        _ ->
            0
    end.

%%====================================================================
%% Event record builder
%%====================================================================

%% Builds a #event{} record. Seq may be undefined for the
%% integrity-off path (the transaction stamps version at write time);
%% otherwise it's the pre-assigned global DCB seq.
build_event_record(EventMap, Seq, Now, EpochUs) ->
    EventId = maps:get(event_id, EventMap, generate_event_id(Seq, EpochUs)),
    EventType = maps:get(event_type, EventMap),
    Data = maps:get(data, EventMap),
    Metadata = maps:get(metadata, EventMap, #{}),
    Tags = case maps:get(tags, EventMap, undefined) of
               undefined         -> undefined;
               []                -> undefined;
               L when is_list(L) -> L
           end,
    #event{
        event_id   = EventId,
        event_type = EventType,
        stream_id  = ?DCB_STREAM,
        version    = case Seq of undefined -> 0; _ -> Seq end,
        data       = Data,
        metadata   = Metadata,
        tags       = Tags,
        timestamp  = Now,
        epoch_us   = EpochUs
        %% prev_event_hash + mac stamped later by stamp_chain/7 on
        %% integrity-on stores; default undefined otherwise.
    }.

generate_event_id(undefined, EpochUs) ->
    iolist_to_binary(io_lib:format("dcb-~p-pre", [EpochUs]));
generate_event_id(Seq, EpochUs) ->
    iolist_to_binary(io_lib:format("dcb-~p-~p", [EpochUs, Seq])).

%%====================================================================
%% Payload entry extraction (outside transaction)
%%====================================================================

%% @doc Extract payload index entries from an event record for the given
%% declarations. Returns [{single, Key, Value}] for payload declarations
%% and [{combo, Hash}] for payload_hash declarations where all field values
%% are present as binaries in the event data JSON.
%%
%% Non-binary values and absent fields produce no entry (event invisible
%% to that filter). Calls crypto:hash/2 for combo hashes — must NOT be
%% called from inside a Khepri transaction.
-spec extract_payload_entries(#event{}, [index_decl()]) ->
    [{single, binary(), binary()} | {combo, binary()}].
extract_payload_entries(_Event, []) ->
    [];
extract_payload_entries(#event{data = Data}, PayloadDecls) ->
    JsonMap = decode_json_map(Data),
    lists:flatmap(fun(D) -> entry_for_decl(D, JsonMap) end, PayloadDecls).

decode_json_map(Bin) when is_binary(Bin) ->
    try json:decode(Bin) of
        M when is_map(M) -> M;
        _                -> #{}
    catch
        _:_ -> #{}
    end;
decode_json_map(M) when is_map(M) ->
    normalize_payload_keys(M);
decode_json_map(_) ->
    #{}.

%% Event data maps may carry atom keys — evoq-built domain events look like
%% #{lot_id => <<"...">>}, with atom keys and binary values — while payload
%% index declarations use binary keys (<<"lot_id">>). Normalise atom keys to
%% binary so the declared binary key matches via maps:get/3 regardless of how
%% the producer keyed the map. JSON-decoded maps already have binary keys and
%% pass through unchanged. (The producer owns event content; the store must
%% index it as given.)
normalize_payload_keys(M) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K) -> Acc#{atom_to_binary(K, utf8) => V};
           (K, V, Acc)                 -> Acc#{K => V}
        end, #{}, M).

-spec entry_for_decl(index_decl(), map()) ->
    [{single, binary(), binary()} | {combo, binary()}].
entry_for_decl({payload, Key}, JsonMap) ->
    case maps:get(Key, JsonMap, undefined) of
        V when is_binary(V) -> [{single, Key, V}];
        _                   -> []
    end;
entry_for_decl({payload_hash, Keys}, JsonMap) ->
    Values = [maps:get(K, JsonMap, undefined) || K <- Keys],
    case lists:all(fun is_binary/1, Values) of
        true ->
            Hash = reckon_db_ccc_paths:payload_combo_hash(Keys, Values),
            [{combo, Hash}];
        false ->
            []
    end;
entry_for_decl(_, _) ->
    [].

%%====================================================================
%% Payload-indexed reads
%%====================================================================

%% @doc Return DCB events where data[Key] = Value, up to Limit events
%% in ascending seq order.
%%
%% Requires {payload, Key} declared in the store's index config.
-spec read_by_payload(atom(), binary(), binary(), pos_integer()) ->
    {ok, [#event{}]} | {error, term()}.
read_by_payload(StoreId, Key, Value, Limit) ->
    Pattern = reckon_db_ccc_paths:by_payload_pattern(Key, Value),
    case khepri:get_many(StoreId, Pattern) of
        {ok, NodeMap} when is_map(NodeMap) ->
            SeqKeys = [SK || [_, _, _, SK] <- maps:keys(NodeMap)],
            resolve_seqkeys(StoreId, SeqKeys, Limit);
        {error, {khepri, node_not_found, _}} ->
            {ok, []};
        {error, _} = Err ->
            Err
    end.

%% @doc Return DCB events matching the composite payload field combination,
%% up to Limit events in ascending seq order.
%%
%% Requires {payload_hash, Keys} declared in the store's index config.
%% Field order in Keys/Values is ignored (hash is order-independent).
-spec read_by_payload_hash(atom(), [binary()], [binary()], pos_integer()) ->
    {ok, [#event{}]} | {error, term()}.
read_by_payload_hash(StoreId, Keys, Values, Limit) ->
    Hash = reckon_db_ccc_paths:payload_combo_hash(Keys, Values),
    Pattern = reckon_db_ccc_paths:by_payload_hash_pattern(Hash),
    case khepri:get_many(StoreId, Pattern) of
        {ok, NodeMap} when is_map(NodeMap) ->
            SeqKeys = [SK || [_, _, SK] <- maps:keys(NodeMap)],
            resolve_seqkeys(StoreId, SeqKeys, Limit);
        {error, {khepri, node_not_found, _}} ->
            {ok, []};
        {error, _} = Err ->
            Err
    end.

resolve_seqkeys(StoreId, SeqKeys, Limit) ->
    Sorted = lists:sort(SeqKeys),
    Limited = lists:sublist(Sorted, Limit),
    Events = lists:filtermap(
        fun(SK) ->
            case khepri:get(StoreId, ?DCB_STREAM_PATH ++ [SK]) of
                {ok, #event{} = E} -> {true, E};
                _                  -> false
            end
        end,
        Limited),
    {ok, Events}.
