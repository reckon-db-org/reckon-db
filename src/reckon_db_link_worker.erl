%% @doc The process that runs one link.
%%
%% A link has two ways to receive events - the backfill over what is already
%% in the source streams, and the live delivery while it runs - and they must
%% meet exactly, with nothing missing and nothing written twice (the link
%% stream is appended with `-2', so nothing downstream dedupes for us).
%%
%% The order that makes them meet is:
%%
%%   1. subscribe, with this process as the subscriber, so the store's own
%%      delivery path (trigger -> emitter pool -> subscriber) starts before
%%      the fence is taken;
%%   2. take each source stream's newest version - its FENCE - so everything
%%      appended from now on is above it;
%%   3. backfill to the fence;
%%   4. from then on, skip every live event at or below its stream's fence.
%%
%% Anything appended during 1..3 is at or below the fence and is linked by the
%% backfill; anything appended after it is above the fence and is linked
%% live. Both are covered by exactly one of the two paths. Events that arrive
%% while init/1 is backfilling wait in this process's mailbox, which is why a
%% process is needed at all: a handler fun kept in a subscription is
%% deserialised afresh on every call and cannot remember a fence.
%%
%% == Delivery ==
%%
%% The live path is the store's real one: this process holds the
%% subscription's subscriber pid, so the trigger broadcasts to the emitter
%% pool and the pool delivers `{events, [...]}' here. Joining the emitter
%% group by hand would put this process in competition with the emitters for
%% the same broadcasts, and would deliver nothing at all unless the group key
%% matched the one the trigger uses.
%%
%% Catch-up is switched off on the subscription: the backfill in step 3 is the
%% catch-up, and it is the exact one the fence arithmetic is written against.
%%
%% == Durability ==
%%
%% The fence is written to the store after every batch it covers, so a restart
%% - the supervisor's after a crash, or an operator's stop and start - resumes
%% the backfill at the last batch it completed instead of appending the whole
%% link stream again. A batch whose append did not land does not move the
%% fence: the worker stops instead, and resumes there.
-module(reckon_db_link_worker).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([start_link/2, start/2, stop/2]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(BATCH, 10000).
-define(LINKS_PATH, [links]).

-record(state, {
    store_id :: atom(),
    name :: binary(),
    link :: term(),
    key :: binary() | undefined,
    %% The version each source stream is linked up to. Events at or below it
    %% are done, whether the backfill or the live path did them.
    fences = #{} :: #{binary() => integer()}
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a link worker as a supervised child.
-spec start_link(atom(), binary()) -> {ok, pid()} | {error, term()}.
start_link(StoreId, Name) ->
    gen_server:start_link({local, reckon_db_naming:link_worker_name(StoreId, Name)},
                          ?MODULE, {StoreId, Name}, []).

%% @doc Start a link worker that no supervisor owns.
%%
%% For a bare Khepri store, which is how the suites and the operator tools
%% drive reckon-db without the application's supervision tree.
-spec start(atom(), binary()) -> {ok, pid()} | {error, term()}.
start(StoreId, Name) ->
    gen_server:start({local, reckon_db_naming:link_worker_name(StoreId, Name)},
                     ?MODULE, {StoreId, Name}, []).

%% @doc Stop the worker for a link, wherever it was started.
%%
%% Called by `reckon_db_links:stop/2' when no supervisor owns the worker.
-spec stop(atom(), binary()) -> ok.
stop(StoreId, Name) ->
    Worker = reckon_db_naming:link_worker_name(StoreId, Name),
    case whereis(Worker) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid, normal, 30000)
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init({StoreId, Name}) ->
    case reckon_db_links:get_link(StoreId, Name) of
        {ok, Link} ->
            init_link(StoreId, Name, Link);
        {error, _} = Error ->
            {stop, Error}
    end.

%% @private
init_link(StoreId, Name, Link) ->
    %% Subscribing first is what closes the window between "the store starts
    %% delivering" and "the fence is known". Everything delivered from here
    %% until the fences are taken waits in the mailbox and is at or below them.
    case subscribe(StoreId, Link, Name) of
        {ok, Key} -> start_subscribed(StoreId, Name, Link, Key);
        {error, Reason} -> fail(StoreId, Name, {link_subscribe_failed, Reason})
    end.

%% @private A link that cannot do its job stops, and says so in the store.
%%
%% `{shutdown, _}' on purpose: the child is `transient', so an abnormal exit
%% is restarted at once, and a link whose link stream refuses appends would
%% restart, re-backfill, fail again, and take the links supervisor past its
%% intensity with it. The fence is where it was, so an operator starting the
%% link again resumes rather than repeats.
fail(StoreId, Name, Reason) ->
    ok = record_failure(StoreId, Name, Reason),
    {stop, {shutdown, Reason}}.

%% @private
record_failure(StoreId, Name, Reason) ->
    logger:error("link ~p on store ~p stopped: ~p", [Name, StoreId, Reason]),
    _ = reckon_db_links:mark_failed(StoreId, Name),
    ok.


%% @private Deliveries are already stacking up in the mailbox; the fences are
%% taken now and the backfill runs behind them.
start_subscribed(StoreId, Name, Link, Key) ->
    Fences = take_fences(StoreId, Link),
    State = #state{store_id = StoreId, name = Name, link = Link,
                   key = Key, fences = Fences},
    case backfill(State) of
        {ok, State1} -> {ok, State1};
        {error, Reason} -> fail(StoreId, Name, Reason)
    end.

%% @private
handle_call(_Request, _From, State) -> {reply, ok, State}.

%% @private
handle_cast(_Request, State) -> {noreply, State}.

%% @private The live path: what the store's emitter delivers, above the fence.
handle_info({events, Events}, State) ->
    case link_live(Events, State) of
        {ok, State1} ->
            {noreply, State1};
        {error, Reason} ->
            #state{store_id = StoreId, name = Name} = State,
            ok = record_failure(StoreId, Name, Reason),
            {stop, {shutdown, Reason}, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #state{store_id = StoreId, key = Key}) when is_binary(Key) ->
    %% A stopped link leaves no subscription behind: its trigger, its emitter
    %% pool and its entry in the subscription table are all this link's, and a
    %% link that is deleted must not keep writing to a stream nobody reads.
    %% A store that is already gone has nothing to clean up, and terminate has
    %% no business raising on the way out of a shutdown.
    try reckon_db_subscriptions:unsubscribe(StoreId, Key) of
        _ -> ok
    catch
        _:_ -> ok
    end;
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%====================================================================
%% Subscription and fences
%%====================================================================

%% @private Create the subscription the store's own delivery runs for.
%%
%% This process is the subscriber, so the emitter pool delivers to it; catch-up
%% is off because the backfill below is the catch-up.
-spec subscribe(atom(), #link{}, binary()) -> {ok, binary()} | {error, term()}.
subscribe(StoreId, Link, Name) ->
    {Type, Selector} = source_selector(Link),
    case reckon_db_subscriptions:subscribe(
           StoreId, Type, Selector, subscription_name(Name),
           #{subscriber => self(), catchup => false}) of
        {ok, Key} -> {ok, Key};
        {error, _} = Error -> Error
    end.

%% @private
subscription_name(Name) ->
    <<"$link-sub:", Name/binary>>.

%% @private
%%
%% A stream link selects its own stream. A pattern link, and an all link, take
%% the whole store: the pattern is applied here, to the events that arrive,
%% because a subscription filter matches event content, not stream-id shape.
source_selector(#link{source = #{type := stream, stream_id := StreamId}}) ->
    {stream, StreamId};
source_selector(#link{source = #{type := stream_pattern}}) ->
    {stream, <<"$all">>};
source_selector(#link{source = #{type := all}}) ->
    {stream, <<"$all">>}.

%% @private The version up to which each source stream has been linked.
%%
%% This is the run's fence AND the value the live path compares against, so it
%% has to be taken after the subscription exists and before anything is read.
take_fences(StoreId, Link) ->
    Streams = reckon_db_links:source_streams(StoreId, Link),
    maps:from_list([{StreamId, newest(StoreId, StreamId)} || StreamId <- Streams]).

%% @private
newest(StoreId, StreamId) ->
    case reckon_db_streams:newest_event_version(StoreId, StreamId) of
        {ok, Version} -> Version;
        {error, _} -> -1
    end.

%%====================================================================
%% Backfill
%%====================================================================

%% @private Link everything each source stream holds from the last recorded
%% fence up to its run fence.
%%
%% With no recorded fence - the link has never linked this stream - `backfill'
%% decides where the stream starts: from 0 when it is on, from the run fence
%% when it is off (the events it already holds are deliberately not linked,
%% and the fence is what keeps the live path from linking them either).
backfill(#state{fences = Fences} = State) ->
    backfill_streams(maps:keys(Fences), State).

%% @private
backfill_streams([], State) ->
    {ok, State};
backfill_streams([StreamId | Rest], State) ->
    From = resume_from(State, StreamId),
    To = maps:get(StreamId, State#state.fences),
    case link_range(State, StreamId, From, To) of
        ok -> backfill_streams(Rest, State);
        {error, _} = Error -> Error
    end.

%% @private The first source version this run has to link.
resume_from(#state{store_id = StoreId, name = Name, link = Link}, StreamId) ->
    case stored_fence(StoreId, Name, StreamId) of
        {ok, Fence} -> Fence + 1;
        undefined when Link#link.backfill =:= true -> 0;
        undefined -> unlimited
    end.

%% @private Link (From, To] of a source stream, then record the fence.
%%
%% The fold has no early exit and no lower bound, so both ends are applied to
%% each batch here. What is above To is the live path's, and what is below
%% From was linked by an earlier run.
link_range(State, StreamId, unlimited, To) ->
    %% Nothing to link; the fence still moves, so a later run does not revisit
    %% the events this link was created to ignore.
    store_fence(State#state.store_id, State#state.name, StreamId, To);
link_range(#state{store_id = StoreId, name = Name} = State,
           StreamId, From, To) ->
    Feed = fun(Events, ok) ->
                   link_batch(State, StreamId, From, To, Events);
              (_Events, {error, _} = Error) ->
                   Error
           end,
    case reckon_db_streams:fold_event_batches(StoreId, StreamId, ?BATCH,
                                              forward, Feed, ok) of
        {ok, ok} ->
            store_fence(StoreId, Name, StreamId, To);
        {ok, {error, Reason}} ->
            {error, {link_append_failed, StreamId, Reason}};
        {error, Reason} ->
            %% A fold that could not read the source is not a link that
            %% reached its fence: leave the fence where it is.
            {error, {link_backfill_failed, StreamId, Reason}}
    end.

%% @private Link the part of a batch between From and To.
%%
%% The fence is recorded per batch, with the batch's highest source version,
%% so a crash between two batches resumes at the second one. It is recorded
%% only after the append landed.
link_batch(#state{store_id = StoreId, link = Link, name = Name},
           StreamId, From, To, Events) ->
    InRange = [E || #event{version = V} = E <- Events, V >= From, V =< To],
    case append_events(StoreId, Link, Name, InRange) of
        ok -> store_batch_fence(StoreId, Name, StreamId, InRange);
        {error, _} = Error -> Error
    end.

%% @private A batch with nothing to link in it links nothing and records
%% nothing: the fence it would record is the one an earlier batch already has.
store_batch_fence(_StoreId, _Name, _StreamId, []) ->
    ok;
store_batch_fence(StoreId, Name, StreamId, InRange) ->
    Highest = lists:max([E#event.version || E <- InRange]),
    store_fence(StoreId, Name, StreamId, Highest).

%%====================================================================
%% Live delivery
%%====================================================================

%% @private Link what arrived live, each event at most once.
link_live(Events, State) ->
    Selected = [E || E <- Events, source_matches(E, State), above_fence(E, State)],
    link_live_by_stream(Selected, State).

%% @private Only the streams this link's source covers. A pattern link, and an
%% all link, subscribed to everything in the store and decide here.
source_matches(#event{stream_id = StreamId}, #state{link = Link}) ->
    reckon_db_links:source_matches(Link, StreamId).

%% @private
link_live_by_stream([], State) ->
    {ok, State};
link_live_by_stream(Events, #state{store_id = StoreId, link = Link,
                                   name = Name, fences = Fences} = State) ->
    ByStream = group_by_stream(Events),
    Fold = fun(StreamId, StreamEvents, Acc) ->
                   link_stream_live(StoreId, Link, Name, StreamId, StreamEvents,
                                    Acc)
           end,
    case maps:fold(Fold, {ok, Fences}, ByStream) of
        {ok, Fences1} -> {ok, State#state{fences = Fences1}};
        {error, _} = Error -> Error
    end.

%% @private One stream's share of a delivery, appended in arrival order.
%%
%% group_by_stream/1 builds its lists by prepending, so they arrive here in
%% the reverse of the order they were delivered in. The fence is recorded only
%% once the append has landed.
link_stream_live(_StoreId, _Link, _Name, _StreamId, _Events, {error, _} = Error) ->
    Error;
link_stream_live(StoreId, Link, Name, StreamId, StreamEvents, {ok, Acc}) ->
    Ordered = lists:reverse(StreamEvents),
    case append_events(StoreId, Link, Name, Ordered) of
        ok ->
            Highest = lists:max([E#event.version || E <- StreamEvents]),
            ok = store_fence(StoreId, Name, StreamId, Highest),
            {ok, Acc#{StreamId => Highest}};
        {error, _} = Error ->
            Error
    end.

%% @private An event is linked live when its stream's fence does not cover it.
above_fence(#event{stream_id = StreamId, version = Version},
            #state{fences = Fences}) ->
    case maps:get(StreamId, Fences, undefined) of
        undefined -> true;
        Fence -> Version > Fence
    end.

%% @private
group_by_stream(Events) ->
    lists:foldl(fun group_one/2, #{}, Events).

%% @private Newest first, which link_live_by_stream/2 reverses.
group_one(#event{stream_id = StreamId} = Event, Acc) ->
    maps:update_with(StreamId, fun(Events) -> [Event | Events] end, [Event], Acc).

%%====================================================================
%% Appending
%%====================================================================

%% @private
append_events(_StoreId, _Link, _Name, []) ->
    ok;
append_events(StoreId, Link, Name, Events) ->
    Processed = reckon_db_links:process_events(Link, Events),
    case reckon_db_links:append_to_link(StoreId, Name, Processed) of
        ok ->
            ok;
        {error, Reason} ->
            %% Not swallowed, and not skipped over either: the caller leaves
            %% the fence where it is and stops the worker, so the events are
            %% still to be linked when it comes back.
            logger:error("link ~p: appending to ~p failed: ~p",
                         [Name, reckon_db_links:link_stream_id(Name), Reason]),
            {error, Reason}
    end.

%%====================================================================
%% Fences in the store
%%====================================================================

%% @private The fence a previous run recorded, or none.
stored_fence(StoreId, Name, StreamId) ->
    case khepri:get(StoreId, fence_path(StoreId, Name, StreamId)) of
        {ok, Version} when is_integer(Version) -> {ok, Version};
        _ -> undefined
    end.

%% @private
store_fence(StoreId, Name, StreamId, Version) ->
    Path = fence_path(StoreId, Name, StreamId),
    case khepri:put(StoreId, Path, Version) of
        ok ->
            ok;
        {error, Reason} ->
            %% A fence that could not be recorded is not a reason to stop
            %% linking: the worst it costs is linking those events again after
            %% a restart, once.
            logger:warning("link ~p: could not record the fence for ~p: ~p",
                           [Name, StreamId, Reason]),
            ok
    end.

%% @private
fence_path(StoreId, Name, StreamId) ->
    ?LINKS_PATH ++ [StoreId, Name, fences, StreamId].
