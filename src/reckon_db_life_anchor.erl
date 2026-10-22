%% @doc The node-local record of every stream's life generation.
%%
%% A MAC keyed inside the store cannot tell a whole-life rollback from the
%% real thing: an older life's seal, life token and events all verify against
%% each other once restored together. What a tree writer cannot reach is a
%% record kept OUTSIDE the tree, beside the store's data directory, naming the
%% life each stream carries now. That is this module, and it is what a targeted
%% read consults in O(1) instead of walking version nodes.
%%
%% The record is a GENERATION, not the life token alone. Lives are random, so
%% a token cannot be ordered: a node that has never seen a life could not tell
%% a legitimate re-create from a rollback. The generation is a per-stream
%% monotonic counter allocated inside the mint transaction and bound into the
%% seal's MAC, so the ordering is the store's own claim, not this file's.
%%
%% The file is append-only: one length-framed, MAC'd record per write, each
%% record's MAC covering the one before it. A break localises to the TAIL, so a
%% damaged or tampered log loses the records after the break and keeps the rest.
%%
%% Residual, named in #18: with the file lost, the start's life pass records
%% every stream the tree can vouch for, once and loudly - what it cannot see is
%% a life restored BEFORE that pass, nor a stream on another node that has not
%% read the stream yet; a stream deleted on ANOTHER node is not tombstoned
%% here; and a rollback WITHIN one life is truncation, which no generation can
%% see.
-module(reckon_db_life_anchor).

-behaviour(gen_server).

-include("reckon_db.hrl").

-export([start_link/2,
         highest_gen/2,
         entry/2,
         check/4,
         record_mint/4,
         record_delete_intent/3,
         record_delete_undone/4,
         record_deleted/3,
         life_pass/1,
         has_records/1,
         log_path/1,
         rebuild/1,
         stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TABLE_PREFIX, "reckon_db_life_anchor_").
-define(LOG_FILE, "reckon_db_lives.log").
-define(LOG_VERSION, 1).
-define(COMPACT_FACTOR, 2).
-define(FSYNC_INTERVAL_MS, 5).

-record(state, {
    store_id :: atom(),
    path :: file:filename_all(),
    fd :: file:io_device() | undefined,
    entries = 0 :: non_neg_integer(),
    records = 0 :: non_neg_integer(),
    last_mac = <<>> :: binary(),
    timer :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(atom(), file:filename_all()) -> {ok, pid()} | {error, term()}.
start_link(StoreId, DataDir) ->
    gen_server:start_link({local, name(StoreId)}, ?MODULE, {StoreId, DataDir}, []).

-spec stop(atom()) -> ok.
stop(StoreId) ->
    case whereis(name(StoreId)) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end.

%% @doc Where this store's anchor log lives, so an operator message names the
%% file the module actually writes.
-spec log_path(file:filename_all()) -> file:filename_all().
log_path(DataDir) ->
    filename:join(DataDir, ?LOG_FILE).

%% @doc The highest generation this node has recorded for a stream, 0 when it
%% has none. Answers 0 when the anchor is not running, so a mint falls back to
%% the tree's counter alone - the named residual.
-spec highest_gen(atom(), binary()) -> non_neg_integer().
highest_gen(StoreId, StreamId) ->
    case lookup(StoreId, StreamId) of
        {Gen, _Value} -> Gen;
        undefined -> 0
    end.

%% @doc The recorded entry: `{Gen, Life}', `{Gen, delete_intent}' or
%% `{Gen, deleted}'.
-spec entry(atom(), binary()) ->
        {non_neg_integer(), binary() | deleted | delete_intent} | undefined.
entry(StoreId, StreamId) ->
    lookup(StoreId, StreamId).

%% @doc The read-side rule, one ETS lookup.
%%
%% `TreeGen' and `TreeLife' are what the store's own seal says the stream
%% carries now; `undefined' means the tree has no life at all - no seal, or a
%% stream stripped to chain-less.
%%
%% An anchor that is NOT RUNNING refuses, before it reads the tree at all.
%% Answering `ok' there served an integrity-enabled store unguarded, which is
%% the whole failure this module exists to stop. A store does not start without
%% it, so this is a fault and not a mode: it is loud.
-spec check(atom(), binary(), non_neg_integer(), binary() | undefined) ->
        ok | {refuse, map()}.
check(StoreId, StreamId, TreeGen, TreeLife) ->
    case running(StoreId) of
        false ->
            {refuse, refusal(StreamId, TreeGen, life_anchor_unavailable, 0)};
        true ->
            check_recorded(StoreId, StreamId, TreeGen, TreeLife)
    end.

%% @private
%%
%% An INTENT (delete_intent) is a delete that has not been recorded as
%% committed, so it refuses nothing: the stream it names may still be there,
%% and a failed transaction has to leave an honest stream serving. What it
%% cannot do is excuse a rollback - a tree below the generation the intent
%% names is refused like any other.
check_recorded(StoreId, StreamId, TreeGen, TreeLife) ->
    case lookup(StoreId, StreamId) of
        undefined ->
            adopt(StoreId, StreamId, TreeGen, TreeLife);
        {Gen, delete_intent} when TreeLife =:= undefined, TreeGen =< Gen ->
            ok;
        {Gen, delete_intent} when TreeGen > Gen ->
            adopt(StoreId, StreamId, TreeGen, TreeLife);
        {Gen, delete_intent} when TreeGen =:= Gen ->
            ok;
        {Gen, delete_intent} ->
            {refuse, refusal(StreamId, TreeGen, life_gen_rolled_back, Gen)};
        {Gen, deleted} when TreeLife =:= undefined, TreeGen =< Gen ->
            ok;
        {Gen, deleted} when TreeGen > Gen ->
            adopt(StoreId, StreamId, TreeGen, TreeLife);
        {Gen, deleted} ->
            {refuse, refusal(StreamId, TreeGen, life_gen_deleted, Gen)};
        {_Gen, _Recorded} when TreeLife =:= undefined ->
            {refuse, refusal(StreamId, TreeGen, life_absent_in_tree, TreeGen)};
        {Gen, _Recorded} when TreeGen > Gen ->
            adopt(StoreId, StreamId, TreeGen, TreeLife);
        {Gen, Recorded} when TreeGen =:= Gen, Recorded =:= TreeLife ->
            ok;
        {Gen, _Recorded} when TreeGen =:= Gen ->
            {refuse, refusal(StreamId, TreeGen, life_gen_mismatch, Gen)};
        {Gen, _Recorded} ->
            {refuse, refusal(StreamId, TreeGen, life_gen_rolled_back, Gen)}
    end.

%% @doc Record a life this node has just committed, read back from the tree
%% after the transaction - never the caller's own candidate.
%%
%% Recording at the SAME generation as the entry already there is how a life
%% becomes a tombstone (a delete names the generation the stream has now, not
%% the next one) and how a dropped intent becomes a life again. The table never
%% moves DOWN; at one generation, the most recent record is the one that holds.
-spec record_mint(atom(), binary(), non_neg_integer(), binary()) -> ok.
record_mint(StoreId, StreamId, Gen, Life) ->
    write(StoreId, StreamId, Gen, Life).

%% @doc The tombstone INTENT, fsynced BEFORE the delete commits, so a crash
%% between the two is resolvable at the next start against the tree.
%%
%% It is NOT the tombstone: an intent and a committed delete that shared one
%% value were indistinguishable at the next start, so every restart took each
%% committed tombstone for an uncommitted intent, read the life back into the
%% table, and served a stream the store had deleted (Jupiter J21-1, Terra
%% T21-1 - every delete ever made, resurrected once per restart). The intent
%% carries its own value and only an intent is ever resolved.
%%
%% Named and not covered: a delete that commits before the intent reached the
%% disk, and an intent resolved after a tree writer restored the life - both
%% drop the intent and leave the stream readable.
-spec record_delete_intent(atom(), binary(), non_neg_integer()) -> ok.
record_delete_intent(StoreId, StreamId, Gen) ->
    sync_write(StoreId, StreamId, tombstone_gen(StoreId, StreamId, Gen),
               delete_intent).

%% @doc Undo an intent whose delete transaction did not commit.
%%
%% A transaction can fail for honest reasons - a Ra timeout, a leader change -
%% and the intent is already on the disk by then. Left standing, it refuses the
%% honest stream (life_gen_deleted) until the next restart (Jupiter J21-2).
%% The life goes back at the generation the intent names, which is the same
%% generation it was recorded at, so the entry is the one the mint wrote.
-spec record_delete_undone(atom(), binary(), non_neg_integer(), binary()) -> ok.
record_delete_undone(StoreId, StreamId, Gen, Life) ->
    sync_write(StoreId, StreamId, tombstone_gen(StoreId, StreamId, Gen), Life).

%% @doc The tombstone, written AFTER the delete commits.
-spec record_deleted(atom(), binary(), non_neg_integer()) -> ok.
record_deleted(StoreId, StreamId, Gen) ->
    write(StoreId, StreamId, tombstone_gen(StoreId, StreamId, Gen), deleted).

%% @private The generation a tombstone is recorded at: never below what this
%% node has already recorded for the stream.
%%
%% The caller reads the stream's generation from the tree, and the tree is
%% what an attacker writes. Lower the counter to 0, delete the stream, and a
%% tombstone at 0 is one the table refuses (it never moves down) - so nothing
%% is recorded, the entry keeps the life that was deleted, and restoring that
%% life and its events is served. The floor is this node's own record.
-spec tombstone_gen(atom(), binary(), non_neg_integer()) -> non_neg_integer().
tombstone_gen(StoreId, StreamId, Gen) ->
    max(Gen, highest_gen(StoreId, StreamId)).

%% @doc The operator act: discard what this node recorded and take the tree's
%% word for every stream (trust on first use, named).
%%
%% This is the recovery for an honest restore from backup - every seal's
%% generation is older than what this node recorded - and for a log whose chain
%% broke. The old log is kept aside, never deleted. It must be run on EVERY
%% member: one rebuilt node leaves the others holding higher entries that
%% refuse honest streams.
-spec rebuild(atom()) -> {ok, non_neg_integer()}.
rebuild(StoreId) ->
    gen_server:call(name(StoreId), rebuild, 120000).

%% @doc Whether this node has recorded anything at all.
-spec has_records(atom()) -> boolean().
has_records(StoreId) ->
    case ets:info(table(StoreId), size) of
        undefined -> false;
        Size -> Size > 0
    end.

%% @doc Record every stream the tree can vouch for that this node has no entry
%% for, and say so loudly.
%%
%% This is the UPGRADE PASS, run at start by a store whose migration marker is
%% already there but whose anchor carries nothing: every pre-5.12 stream would
%% otherwise be unknown to this node, and the first read of each would adopt it
%% - including a restore of an older life that happened before anyone read it.
%% A marker means the streams were sealed by 5.12 or later, so their seals carry
%% the generation this pass records. It runs again on every member at upgrade,
%% because the marker arrives on one write and the anchor is per node.
%%
%% It never overwrites an entry this node already holds: a pass after a real
%% restart would otherwise adopt a restored life for every stream at once.
-spec life_pass(atom()) -> ok | {error, term()}.
life_pass(StoreId) ->
    case whereis(name(StoreId)) of
        undefined -> {error, not_running};
        _Pid -> gen_server:call(name(StoreId), life_pass, 300000)
    end.

%%====================================================================
%% gen_server
%%====================================================================

init({StoreId, DataDir}) ->
    Path = filename:join(DataDir, ?LOG_FILE),
    _ = ets:new(table(StoreId), [named_table, public, set,
                                 {read_concurrency, true}]),
    State0 = #state{store_id = StoreId, path = Path},
    State1 = load(State0),
    State2 = resolve_intents(State1),
    {ok, maybe_compact(State2)}.

%% The delete INTENT. It skips the adopt coalescing and it answers only after
%% the record is on the disk: the delete that follows compares against this
%% record at the next start, so a reply that outran the fsync would leave a
%% delete nothing can resolve.
handle_call({append_sync, StreamId, Gen, Value}, _From, State) ->
    case insert(State#state.store_id, StreamId, Gen, Value) of
        true ->
            Written = flush(append(State, StreamId, Gen, Value)),
            {reply, ok, maybe_compact(Written)};
        false ->
            {reply, ok, State}
    end;
handle_call(life_pass, _From, State) ->
    {reply, ok, run_life_pass(State)};
handle_call(rebuild, _From, State) ->
    {Count, State1} = do_rebuild(State),
    {reply, {ok, Count}, State1};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({adopt, StreamId, Gen, Value}, State) ->
    case insert(State#state.store_id, StreamId, Gen, Value) of
        true ->
            {noreply, schedule_flush(
                        maybe_compact(append(State, StreamId, Gen, Value)))};
        false ->
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(flush, State) ->
    {noreply, maybe_compact(flush(State#state{timer = undefined}))};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{fd = undefined}) ->
    ok;
terminate(_Reason, #state{fd = Fd}) ->
    _ = file:sync(Fd),
    _ = file:close(Fd),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% The read rule
%%====================================================================

%% @private A generation above everything this node has seen: a life minted
%% after this node last looked, here or on another member. The read does not
%% wait for the fsync; the record is queued.
adopt(StoreId, StreamId, TreeGen, TreeLife) ->
    case {TreeLife, whereis(name(StoreId))} of
        {undefined, _} ->
            ok;
        {_, undefined} ->
            _ = insert(StoreId, StreamId, TreeGen, TreeLife),
            ok;
        {_, _Pid} ->
            gen_server:cast(name(StoreId),
                            {adopt, StreamId, TreeGen, TreeLife}),
            ok
    end.

%% @private
refusal(StreamId, TreeGen, Detail, RecordedGen) ->
    #{layer => storage,
      stream_id => StreamId,
      version => TreeGen,
      kind => missing_integrity,
      context => #{detail => Detail, recorded_gen => RecordedGen}}.

%%====================================================================
%% Writes
%%====================================================================

%% @private Coalesced: the table carries the answer for the next read at once,
%% the record is made durable by the writer within one fsync window.
%%
%% The cast goes out even when this process believes the entry already says the
%% same thing: the writer is the one place that decides, and a second writer
%% racing this one must not be able to leave the table and the log disagreeing
%% about which record came last.
write(StoreId, StreamId, Gen, Value) ->
    case whereis(name(StoreId)) of
        undefined ->
            _ = insert(StoreId, StreamId, Gen, Value),
            ok;
        _Pid ->
            gen_server:cast(name(StoreId), {adopt, StreamId, Gen, Value})
    end.

%% @private The delete intent: durable BEFORE it answers.
sync_write(StoreId, StreamId, Gen, Value) ->
    case whereis(name(StoreId)) of
        undefined -> _ = insert(StoreId, StreamId, Gen, Value), ok;
        _Pid -> gen_server:call(name(StoreId), {append_sync, StreamId, Gen, Value},
                                30000)
    end.

%% @private The table only ever moves up, and at one generation the LAST record
%% is the one that holds - which is what makes a tombstone, and a dropped
%% intent, expressible at all. A delete names the generation the stream has
%% now, so its tombstone lands on top of that generation's life; and a delete
%% that never committed is undone by writing the life back at the same one.
%%
%% Answers whether anything changed, which is what decides whether a record is
%% appended: an entry that already says this writes nothing.
insert(StoreId, StreamId, Gen, Value) ->
    case ets:info(table(StoreId), size) of
        undefined -> false;
        _ -> replace_row(table(StoreId), StreamId, Gen, Value)
    end.

%% @private
replace_row(Tab, StreamId, Gen, Value) ->
    case ets:lookup(Tab, StreamId) of
        [{_, Existing, Value}] when Existing >= Gen ->
            false;
        [{_, Existing, _}] when Existing > Gen ->
            false;
        _ ->
            ets:insert(Tab, {StreamId, Gen, Value}),
            true
    end.

%% @private Whether this node's anchor is up. The table is created by the
%% writer in init/1 and dies with it, so its existence answers the question
%% without a call.
running(StoreId) ->
    ets:info(table(StoreId), size) =/= undefined.

%% @private
lookup(StoreId, StreamId) ->
    case ets:info(table(StoreId), size) of
        undefined -> undefined;
        _ -> row(table(StoreId), StreamId)
    end.

%% @private
row(Tab, StreamId) ->
    case ets:lookup(Tab, StreamId) of
        [{_, Gen, Value}] -> {Gen, Value};
        [] -> undefined
    end.

%%====================================================================
%% The log
%%====================================================================

%% @private Replay into ETS, then open the file for appending at its end. A
%% record that does not parse or does not verify ends the replay: what is
%% before it is kept, what is after it is dropped.
load(#state{store_id = StoreId, path = Path} = State) ->
    {Entries, Records, LastMac, Good} =
        case file:read_file(Path) of
            {error, enoent} -> {0, 0, <<>>, 0};
            {ok, Bin} -> replay(StoreId, Bin, 0, 0, <<>>, 0, byte_size(Bin), Path);
            {error, Reason} ->
                logger:error("life anchor ~ts unreadable: ~p", [Path, Reason]),
                {0, 0, <<>>, 0}
        end,
    Fd = open_at_end(Path, Good),
    State#state{fd = Fd, entries = Entries, records = Records, last_mac = LastMac}.

%% @private
replay(_StoreId, <<>>, Entries, Records, LastMac, Good, _Size, _Path) ->
    {Entries, Records, LastMac, Good};
replay(StoreId, <<Size:32, Rest/binary>>, Entries, Records, LastMac, Good,
       Total, Path) when byte_size(Rest) >= Size ->
    <<Bin:Size/binary, Tail/binary>> = Rest,
    case decode(StoreId, Bin, LastMac) of
        {ok, #{stream := StreamId, gen := Gen, value := Value, mac := Mac}} ->
            ok = put_entry(StoreId, StreamId, Gen, Value),
            replay(StoreId, Tail, Entries + delta(Value), Records + 1, Mac,
                   Good + 32 + Size, Total, Path);
        error ->
            dropped(Path, Total - Good),
            {Entries, Records, LastMac, Good}
    end;
replay(_StoreId, _Torn, Entries, Records, LastMac, Good, Total, Path) ->
    dropped(Path, Total - Good),
    {Entries, Records, LastMac, Good}.

%% @private
put_entry(StoreId, StreamId, Gen, Value) ->
    _ = insert(StoreId, StreamId, Gen, Value),
    ok.

%% @private
delta(Life) when is_binary(Life) -> 1;
delta(delete_intent) -> 0;
delta(deleted) -> 0.

%% @private
dropped(_Path, 0) ->
    ok;
dropped(Path, Bytes) ->
    logger:warning("life anchor ~ts: ~p unverifiable bytes dropped from the "
                   "tail; if a stream then refuses with life_absent_in_tree, "
                   "run reckon_db_life_anchor:rebuild/1", [Path, Bytes]),
    telemetry:execute([reckon, db, life_anchor, tail_dropped], #{bytes => Bytes},
                      #{path => Path}).

%% @private
decode(StoreId, Bin, PrevMac) ->
    case safe_term(Bin) of
        #{version := ?LOG_VERSION, stream := StreamId, gen := Gen,
          value := Value, prev_mac := PrevMac, mac := Mac}
                when is_binary(StreamId), is_integer(Gen), is_binary(Mac) ->
            decode_verified(StoreId, StreamId, Gen, Value, PrevMac, Mac);
        _ ->
            error
    end.

%% @private
decode_verified(StoreId, StreamId, Gen, Value, PrevMac, Mac) ->
    Expected = record_mac(StoreId, StreamId, Gen, Value, PrevMac),
    case mac_matches(Mac, Expected) of
        true -> {ok, #{stream => StreamId, gen => Gen, value => Value,
                       mac => Mac}};
        false -> error
    end.

%% @private
safe_term(Bin) ->
    try binary_to_term(Bin, [safe]) of
        Term -> Term
    catch
        _:_ -> undefined
    end.

%% @private One length-framed record.
append(#state{store_id = StoreId, fd = Fd, last_mac = PrevMac} = State,
       StreamId, Gen, Value) ->
    Mac = record_mac(StoreId, StreamId, Gen, Value, PrevMac),
    Bin = term_to_binary(#{version => ?LOG_VERSION, stream => StreamId,
                           gen => Gen, value => Value, prev_mac => PrevMac,
                           mac => Mac}, [deterministic]),
    case write_framed(Fd, Bin) of
        ok ->
            State#state{last_mac = Mac, records = State#state.records + 1,
                        entries = State#state.entries + delta(Value)};
        {error, Reason} ->
            %% A log that cannot be written is loud, never silent: the store
            %% keeps serving, and the operator has to know that adoptions and
            %% tombstones are not reaching the disk.
            logger:error("life anchor ~ts: append failed: ~p",
                         [State#state.path, Reason]),
            telemetry:execute([reckon, db, life_anchor, append_failed],
                              #{reason => Reason}, #{path => State#state.path}),
            State
    end.

%% @private
write_framed(undefined, _Bin) ->
    {error, no_file};
write_framed(Fd, Bin) ->
    file:write(Fd, <<(byte_size(Bin)):32, Bin/binary>>).

%% @private
schedule_flush(#state{timer = undefined} = State) ->
    State#state{timer = erlang:send_after(?FSYNC_INTERVAL_MS, self(), flush)};
schedule_flush(State) ->
    State.

%% @private
flush(#state{fd = undefined} = State) ->
    State;
flush(#state{fd = Fd} = State) ->
    _ = file:sync(Fd),
    State.

%% @private The log is created here when it is not there yet - a fresh store,
%% or the first start after a rebuild set the old one aside. `write' is what
%% creates it: there is no `create' mode in file:open, and a mode list that
%% carries one is not a call dialyzer can give a return to.
open_at_end(Path, Good) ->
    ok = filelib:ensure_dir(Path),
    case file:open(Path, [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, _} = file:position(Fd, Good),
            ok = file:truncate(Fd),
            Fd;
        {error, _} ->
            undefined
    end.

%% @private
record_mac(StoreId, StreamId, Gen, Value, PrevMac) ->
    case reckon_db_integrity_key:get(StoreId) of
        Key when is_binary(Key) ->
            crypto:mac(hmac, sha256, Key,
                       term_to_binary({?MODULE, record, StoreId, StreamId, Gen,
                                       Value, PrevMac}, [deterministic]));
        _ ->
            <<>>
    end.

%% @private
mac_matches(Mac, Expected) ->
    byte_size(Mac) =:= byte_size(Expected) andalso
        crypto:hash_equals(Mac, Expected).

%%====================================================================
%% Start-time passes
%%====================================================================

%% @private An INTENT whose life is still in the tree is a delete that never
%% committed - or a delete whose life a tree writer put back. Both drop the
%% intent; the second is named in #18 beside the delete crash window. An intent
%% whose life is gone is the delete that committed, and it becomes the
%% tombstone it was standing in for.
%%
%% Only an intent is resolved. A committed tombstone carries its own value
%% (`deleted'), so a restart leaves it exactly where the delete put it.
%%
%% Resolving has to be a WRITE, not the absence of one: the intent is in the
%% table and in the log, so a restart that only declined to act would keep
%% refusing a stream that is still there. The life goes back at the generation
%% the intent carries, and a superseding record goes into the log, so the next
%% replay reaches the same answer.
resolve_intents(#state{store_id = StoreId} = State) ->
    Intents = ets:tab2list(table(StoreId)),
    lists:foldl(fun(Entry, Acc) -> resolve_intent(StoreId, Acc, Entry) end,
                State, Intents).

%% @private
resolve_intent(StoreId, Acc, {StreamId, Gen, delete_intent}) ->
    case reckon_db_chain_watermark:life(StoreId, StreamId) of
        {ok, undefined} -> commit_intent(Acc, StreamId, Gen);
        {ok, Life} when is_binary(Life) -> drop_intent(Acc, StreamId, Gen, Life)
    end;
resolve_intent(_StoreId, Acc, _Entry) ->
    Acc.

%% @private
commit_intent(#state{store_id = StoreId} = State, StreamId, Gen) ->
    _ = insert(StoreId, StreamId, Gen, deleted),
    append(State, StreamId, Gen, deleted).

%% @private
drop_intent(#state{store_id = StoreId, path = Path} = State, StreamId, Gen,
            Life) ->
    _ = insert(StoreId, StreamId, Gen, Life),
    logger:warning("life anchor ~ts: the delete of stream ~p never committed "
                   "(its life is still in the tree); the intent is dropped",
                   [Path, StreamId]),
    telemetry:execute([reckon, db, life_anchor, intent_dropped], #{},
                      #{path => Path, stream_id => StreamId}),
    append(State, StreamId, Gen, Life).

%% @private Compaction: more than twice as many records as lives means the log
%% carries history nobody will read again. The writer writes a fresh log
%% (a snapshot record per stream), fsyncs, and renames over the old one.
maybe_compact(#state{records = Records, entries = Entries} = State)
        when Entries > 0, Records > ?COMPACT_FACTOR * Entries ->
    compact(State);
maybe_compact(State) ->
    State.

%% @private
compact(#state{path = Path, fd = OldFd} = State) ->
    Temp = Path ++ ".new",
    case file:open(Temp, [write, raw, binary]) of
        {ok, Fd} -> compact_into(State, Fd, OldFd, Temp);
        {error, Reason} ->
            logger:warning("life anchor compaction failed: ~p", [Reason]),
            State
    end.

%% @private
compact_into(#state{store_id = StoreId, path = Path} = State, Fd, OldFd, Temp) ->
    Fresh = State#state{fd = Fd, entries = 0, records = 0, last_mac = <<>>},
    State1 = lists:foldl(fun compact_entry/2, Fresh,
                         ets:tab2list(table(StoreId))),
    _ = file:sync(Fd),
    _ = file:close(Fd),
    _ = file:close(OldFd),
    ok = file:rename(Temp, Path),
    State1#state{path = Path, fd = open_at_end(Path, file_size(Path))}.

%% @private
compact_entry({StreamId, Gen, Value}, Acc) ->
    append(Acc, StreamId, Gen, Value).

%% @private The upgrade pass, in the writer.
%%
%% Loud on purpose: a node that had an anchor and finds none is either the
%% first start after an upgrade or a log somebody removed, and an operator has
%% to be able to tell the two apart. It records only streams this node has no
%% entry for, so it can run on every start without ever taking a newer
%% generation down.
run_life_pass(#state{store_id = StoreId} = State) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, StreamIds} ->
            {Recorded, State1} = lists:foldl(fun life_pass_one/2, {0, State},
                                             StreamIds),
            announce_life_pass(State1, length(StreamIds), Recorded),
            State1;
        {error, Reason} ->
            logger:error("life anchor ~ts: the life pass could not list the "
                         "store's streams: ~p", [State#state.path, Reason]),
            State
    end.

%% @private
life_pass_one(StreamId, {N, Acc}) ->
    StoreId = Acc#state.store_id,
    case entry(StoreId, StreamId) of
        undefined -> record_sealed_life(StoreId, StreamId, N, Acc);
        _Known -> {N, Acc}
    end.

%% @private
record_sealed_life(StoreId, StreamId, N,
                   #state{store_id = StoreId} = Acc) ->
    case reckon_db_chain_watermark:binding(StoreId, StreamId) of
        {ok, #{life := Life, gen := Gen}} when is_binary(Life) ->
            _ = insert(StoreId, StreamId, Gen, Life),
            {N + 1, append(Acc, StreamId, Gen, Life)};
        _ ->
            {N, Acc}
    end.

%% @private
announce_life_pass(_State, 0, _Recorded) ->
    ok;
announce_life_pass(#state{path = Path}, Streams, Recorded) ->
    logger:warning("life anchor ~ts: the life pass recorded ~p of ~p streams "
                   "(the log carried nothing for them). If this node's anchor "
                   "was removed rather than newly created, an older life "
                   "restored before this start has been adopted: the anchor "
                   "cannot tell the two apart.", [Path, Recorded, Streams]),
    telemetry:execute([reckon, db, life_anchor, life_pass],
                      #{streams => Streams, recorded => Recorded},
                      #{path => Path}).

%% @private Discard the log and take the tree's word for every stream.
%%
%% The file has to be REOPENED, not just renamed: the old descriptor points at
%% the inode that is now called `.discarded.<ts>', so leaving it open made every
%% later record land in the file the rebuild set aside, and the next start read
%% an empty log - which is every stream adopting on first sight, silently.
do_rebuild(#state{store_id = StoreId, path = Path, fd = OldFd} = State) ->
    Aside = Path ++ ".discarded." ++ integer_to_list(erlang:system_time(second)),
    _ = file:rename(Path, Aside),
    _ = file:close(OldFd),
    ets:delete_all_objects(table(StoreId)),
    Fd = open_at_end(Path, 0),
    logger:warning("life anchor rebuilt: old log kept at ~ts; taking the tree's "
                   "word for every stream (trust on first use)", [Aside]),
    telemetry:execute([reckon, db, life_anchor, rebuilt], #{},
                      #{path => Path, aside => Aside}),
    State1 = State#state{fd = Fd, entries = 0, records = 0, last_mac = <<>>},
    {ok, Streams} = reckon_db_streams:list_streams(StoreId),
    {Count, State2} = lists:foldl(fun rebuild_one/2, {0, State1}, Streams),
    {Count, State2#state{entries = Count, records = Count}}.

%% @private
rebuild_one(StreamId, {N, Acc}) ->
    case rebuild_stream(Acc, StreamId) of
        {ok, Acc1} -> {N + 1, Acc1};
        none -> {N, Acc}
    end.

%% @private `rebuild' has to leave a log behind, not only a table: the whole
%% point of the act is that the NEXT start agrees with this one.
rebuild_stream(#state{store_id = StoreId} = State, StreamId) ->
    case reckon_db_chain_watermark:binding(StoreId, StreamId) of
        {ok, #{life := Life, gen := Gen}} when is_binary(Life) ->
            _ = insert(StoreId, StreamId, Gen, Life),
            {ok, append(State, StreamId, Gen, Life)};
        _ ->
            none
    end.

%% @private
file_size(Path) ->
    filelib:file_size(Path).

%%====================================================================
%% Names
%%====================================================================

-spec name(atom()) -> atom().
name(StoreId) ->
    list_to_atom(?MODULE_STRING ++ "_" ++ atom_to_list(StoreId)).

-spec table(atom()) -> atom().
table(StoreId) ->
    list_to_atom(?TABLE_PREFIX ++ atom_to_list(StoreId)).
