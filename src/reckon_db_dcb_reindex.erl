%% @doc One-time re-index of DCB events written before they got secondary
%% index entries (reckon-db #2).
%%
%% Up to 5.11.10 the DCB append wrote its own `by_tag' / `by_event_type' /
%% payload entries but never the `[idx]' entries of `reckon_db_index', so on
%% a store declaring `tags', `event_type' or `{meta, Key}' the indexed
%% `read_by_tags' / `read_by_event_types' / `read_by_metadata' never returned
%% a DCB event. A DCB-context decision on such a store read an empty
%% context and could not commit into a non-empty boundary. The append now
%% writes those entries; this gives the events written before it theirs.
%%
%% == Once ==
%%
%% A marker at `[metadata, index, dcb_indexed]' lists the kinds whose entries
%% the DCB log already has. A run re-indexes only the declared kinds missing
%% from it, and writes the marker after every batch has landed: a run that
%% dies half way leaves no marker and the next one redoes it (the entries are
%% keyed by path, so writing one twice is harmless). With the marker in
%% place a run is one read. A kind declared later, never covered before, is
%% re-indexed on its own. The marker trusts that every DCB append since it
%% was written came from 5.11.11 or later; when that is not so (a rolling
%% upgrade where an old member appended after the marker, or a kind that was
%% undeclared and declared again), `run(StoreId, #{force => true})' ignores
%% it and redoes every declared kind.
%%
%% == Through the Ra leader ==
%%
%% Every member of a cluster opens the store, so `run/1' works only on the
%% Ra leader and answers `not_leader' elsewhere; `reckon_db_leader' runs it,
%% in a process of its own, on activation, which the node monitor does on
%% the leader after a start and after every leadership change. Two runs can still overlap across a
%% leadership change; they stay correct (same entries, same paths, the
%% marker is a union) and each scans the log once.
-module(reckon_db_dcb_reindex).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([run/1, run/2]).

-define(MARKER_PATH, [metadata, index, dcb_indexed]).
%% DCB records re-indexed per transaction: one Ra command each, bounded.
-define(BATCH, 500).

-type outcome() :: #{reindexed := non_neg_integer(),
                     kinds := [index_decl()],
                     duration_us := non_neg_integer()}.

%% @doc Re-index the DCB log for the declared kinds it has no entries for
%% yet. `{ok, not_leader}' on a member that is not the Ra leader.
-spec run(atom()) -> {ok, outcome() | not_leader} | {error, term()}.
run(StoreId) ->
    run(StoreId, #{}).

%% @doc As run/1. With `#{force => true}' the marker is ignored and every
%% declared kind is re-indexed: the way to redo it when DCB events got in
%% without entries after the marker was written, e.g. appended by a member
%% still on 5.11.10 during a rolling upgrade, or while a kind was undeclared
%% and then declared again.
-spec run(atom(), #{force => boolean()}) -> {ok, outcome() | not_leader} | {error, term()}.
run(StoreId, Opts) ->
    run(reckon_db_store_coordinator:is_leader(StoreId), StoreId, maps:get(force, Opts, false)).

run(false, _StoreId, _Force) ->
    {ok, not_leader};
run(true, StoreId, Force) ->
    Start = erlang:monotonic_time(microsecond),
    Declared = reckon_db_index:entry_kinds(reckon_db_index_config:declared(StoreId)),
    Result = reindex(StoreId, missing(Declared, covered(Force, StoreId))),
    report(StoreId, Result, erlang:monotonic_time(microsecond) - Start).

covered(true, _StoreId) -> {ok, []};
covered(false, StoreId) -> marker(StoreId).

%%====================================================================
%% Internal
%%====================================================================

missing(Declared, {ok, Covered}) ->
    {ok, lists:usort(Declared -- Covered), Covered};
missing(_Declared, {error, _} = Error) ->
    Error.

marker(StoreId) ->
    case khepri:get(StoreId, ?MARKER_PATH) of
        {ok, Kinds} when is_list(Kinds) -> {ok, Kinds};
        {ok, _NoMarkerData} -> {ok, []};
        {error, {khepri, node_not_found, _}} -> {ok, []};
        {error, _} = Error -> Error
    end.

reindex(_StoreId, {ok, [], _Covered}) ->
    {ok, 0, []};
reindex(StoreId, {ok, Kinds, Covered}) ->
    case dcb_records(StoreId) of
        {ok, Records} ->
            write_batches(StoreId, Records, Kinds, Covered);
        {error, _} = Error ->
            Error
    end;
reindex(_StoreId, {error, _} = Error) ->
    Error.

dcb_records(StoreId) ->
    %% Consistent, not the default local read: a leader just elected could
    %% otherwise read its log short of the last committed events.
    event_records(khepri:get_many(StoreId, ?DCB_STREAM_PATH ++ [?KHEPRI_WILDCARD_STAR],
                                  #{favor => consistency})).

event_records({ok, Map}) -> {ok, [R || #event{} = R <- maps:values(Map)]};
event_records({error, _} = Error) -> Error.

write_batches(StoreId, Records, Kinds, Covered) ->
    case write_batch(StoreId, Records, Kinds) of
        ok ->
            mark(StoreId, lists:usort(Covered ++ Kinds), length(Records), Kinds);
        {error, _} = Error ->
            Error
    end.

write_batch(_StoreId, [], _Kinds) ->
    ok;
write_batch(StoreId, Records, Kinds) ->
    {Batch, Rest} = split(Records),
    Entries = lists:flatmap(fun(R) -> reckon_db_index:entries(R, Kinds) end, Batch),
    Put = fun() -> put_all(Entries) end,
    next_batch(transaction(StoreId, Put), StoreId, Rest, Kinds).

next_batch(ok, StoreId, Rest, Kinds) -> write_batch(StoreId, Rest, Kinds);
next_batch({error, _} = Error, _StoreId, _Rest, _Kinds) -> Error.

%% Up to ?BATCH records off the front, without measuring the rest.
split(Records) -> take(?BATCH, Records, []).

take(0, Rest, Acc) -> {lists:reverse(Acc), Rest};
take(_N, [], Acc) -> {lists:reverse(Acc), []};
take(N, [R | Rest], Acc) -> take(N - 1, Rest, [R | Acc]).

put_all(Entries) ->
    lists:foreach(fun({Path, Ref}) -> ok = khepri_tx:put(Path, Ref) end, Entries).

%% The marker is a union with whatever another run already recorded.
mark(StoreId, Kinds, Count, Reindexed) ->
    Union = fun() -> union_marker(Kinds) end,
    marked(transaction(StoreId, Union), Count, Reindexed).

marked(ok, Count, Reindexed) -> {ok, Count, Reindexed};
marked({error, _} = Error, _Count, _Reindexed) -> Error.

union_marker(Kinds) ->
    ok = khepri_tx:put(?MARKER_PATH, lists:usort(covered_in_tx(khepri_tx:get(?MARKER_PATH)) ++ Kinds)).

covered_in_tx({ok, Kinds}) when is_list(Kinds) -> Kinds;
covered_in_tx(_) -> [].

transaction(StoreId, Fun) ->
    case khepri:transaction(StoreId, Fun) of
        {ok, ok} -> ok;
        {ok, {error, _} = Error} -> Error;
        {error, _} = Error -> Error
    end.

report(_StoreId, {ok, 0, []}, _DurationUs) ->
    {ok, #{reindexed => 0, kinds => [], duration_us => 0}};
report(StoreId, {ok, Count, Kinds}, DurationUs) ->
    logger:notice("[reckon_db] DCB re-index for store ~p: ~b DCB events given index "
                  "entries for ~p in ~b us (once; marker ~p)",
                  [StoreId, Count, Kinds, DurationUs, ?MARKER_PATH]),
    {ok, #{reindexed => Count, kinds => Kinds, duration_us => DurationUs}};
report(StoreId, {error, Reason} = Error, DurationUs) ->
    logger:error("[reckon_db] DCB re-index for store ~p FAILED after ~b us: ~p. Indexed "
                 "read_by_tags / read_by_event_types / read_by_metadata miss DCB events "
                 "written before 5.11.11 until it succeeds.",
                 [StoreId, DurationUs, Reason]),
    Error.
