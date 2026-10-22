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
%% ```
%% [global_seq, <zero-padded 20-digit seq>] -> #{stream_id, version}
%% [global_seq_ref, Type, Id, PaddedVersion] -> Seq
%% ```
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
    assign_in_tx/1,
    last_assigned/1,
    read_from/3,
    annotate/2,
    backfill/1,
    is_backfilled/1
]).

-export_type([seq/0]).

%% A sequence number. Assigned once, never reused.
-type seq() :: non_neg_integer().

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

%%====================================================================
%% Paths
%%====================================================================

%% @doc The tree path a sequence number's reference lives at.
-spec path(seq()) -> [atom() | binary()].
path(Seq) when is_integer(Seq), Seq >= 0 ->
    ?GLOBAL_SEQ_ROOT ++ [seq_key(Seq)].

%% @doc The tree path that maps one event (stream id + version) to its seq.
-spec ref_path(binary(), non_neg_integer()) -> [atom() | binary()].
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

%% @doc Assign a seq to each event of a batch, inside the append transaction.
%%
%% One append is one Ra command, and one batch's seqs are contiguous. The
%% counter and both ref directions are written here, so they commit or not
%% with the events themselves.
%%
%% Refused, by aborting the transaction, when the store has not been
%% backfilled: numbering new events from 0 beside a store whose existing
%% events have no number would make two events share a seq.
-spec assign_in_tx([{khepri_path:native_path(), event()}]) ->
        [{seq(), binary(), non_neg_integer()}].
assign_in_tx([]) ->
    [];
assign_in_tx(Writes) ->
    ok = require_started(),
    Base = take_block(length(Writes)),
    Assigned = [{Base + I, Event#event.stream_id, Event#event.version}
                || {I, {_EventPath, Event}} <-
                       lists:zip(lists:seq(0, length(Writes) - 1), Writes)],
    lists:foreach(
      fun({{EventPath, Event}, {Seq, _StreamId, _Version}}) ->
              write_ref(Seq, Event, ref_path_of(EventPath))
      end,
      lists:zip(Writes, Assigned)),
    Assigned.

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
        {ok, Last} when is_integer(Last) -> Last;
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
    case is_backfilled_in_tx() of
        true ->
            ok;
        false ->
            case event_count_in_tx() of
                0 ->
                    ok = khepri_tx:put(?GLOBAL_SEQ_BACKFILLED_PATH, true),
                    ok;
                _ ->
                    khepri_tx:abort(global_seq_not_backfilled)
            end
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
%% Reading
%%====================================================================

%% @doc The highest seq handed out, or `none' when the store has none.
-spec last_assigned(atom()) -> {ok, seq()} | none.
last_assigned(StoreId) ->
    case khepri:get(StoreId, ?GLOBAL_SEQ_COUNTER_PATH) of
        {ok, Last} when is_integer(Last) -> {ok, Last};
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
            {error, global_seq_not_backfilled}
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
            EventPath = reckon_db_stream_path:event_path(
                          StreamId, reckon_db_stream_path:version_key(Version)),
            case khepri:get(StoreId, EventPath) of
                {ok, #event{} = Event} -> {ok, Event};
                _ -> hole
            end;
        _ ->
            hole
    end.

%% @doc Attach the seq to an event as `global_position' in its metadata.
%%
%% The stored event is never changed; this is the copy handed to a
%% subscriber, and the key is the one evoq and the guides already read.
-spec annotate(atom(), event()) -> event().
annotate(StoreId, #event{stream_id = StreamId, version = Version} = Event) ->
    case seq_for(StoreId, StreamId, Version) of
        {ok, Seq} ->
            Metadata = Event#event.metadata,
            Event#event{metadata = Metadata#{global_position => Seq}};
        not_found ->
            Event
    end.

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
        {ok, Events} ->
            case reserve(StoreId, length(Events)) of
                {ok, none} ->
                    ok;
                {ok, Base} ->
                    case fill(StoreId, Base, Events) of
                        ok -> mark_backfilled(StoreId);
                        {error, _} = Error -> Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @private Reserve the block before writing it.
reserve(StoreId, Count) ->
    Base = case last_assigned(StoreId) of
               none -> 0;
               {ok, Last} -> Last + 1
           end,
    Fun = fun() ->
                  case is_backfilled_in_tx() of
                      true ->
                          already_backfilled;
                      false ->
                          case last_assigned_in_tx() of
                              none -> ok;
                              _Last -> khepri_tx:abort({global_seq_already_started, StoreId})
                          end,
                          ok = khepri_tx:put(?GLOBAL_SEQ_COUNTER_PATH,
                                             Base + Count - 1),
                          reserved
                  end
          end,
    case khepri:transaction(StoreId, Fun) of
        {ok, reserved} -> {ok, Base};
        {ok, already_backfilled} -> {ok, none};
        {ok, {error, Reason}} -> {error, Reason};
        {error, Reason} -> {error, Reason}
    end.

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
