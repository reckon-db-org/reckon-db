%% @doc The process that runs one link.
%%
%% A link has two ways to receive events - the backfill over what is already
%% in the source streams, and the live delivery while it runs - and they must
%% meet exactly, with nothing missing and nothing written twice (the link
%% stream is appended with `-2', so nothing downstream dedupes for us).
%%
%% The order that makes them meet is:
%%
%%   1. subscribe, so live delivery starts before the fence is taken;
%%   2. take each source stream's newest version - its FENCE - so everything
%%      appended from now on is above it;
%%   3. backfill to the fence;
%%   4. from then on, skip every live event at or below its stream's fence.
%%
%% Anything appended during 1..3 is at or below the fence and is linked by the
%% backfill; anything appended after it is above the fence and is linked
%% live. Both are skipped by exactly one of the two paths. Events that arrive
%% while init/1 is backfilling wait in this process's mailbox, which is why a
%% process is needed at all: a handler fun kept in a subscription is
%% deserialised afresh on every call and cannot remember a fence.
%%
%% The fence is written to the store after each batch it covers, so a restart
%% resumes the backfill where it stopped instead of appending the whole link
%% stream again.
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
    subscription :: binary(),
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
%% Called by `reckon_db_links:stop/2'.
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
    Subscription = subscription_name(Name),
    ok = subscribe(StoreId, Link, Subscription),
    ok = reckon_db_emitter_group:join(StoreId, Subscription, self()),

    %% The fence is taken AFTER the subscription exists, so no event can be
    %% appended between the two and land in neither path.
    Fences = take_fences(StoreId, Link),
    State = #state{store_id = StoreId, name = Name, link = Link,
                   subscription = Subscription, fences = Fences},
    ok = backfill(State, maps:keys(Fences)),
    {ok, State}.

%% @private
handle_call(_Request, _From, State) -> {reply, ok, State}.

%% @private
handle_cast(_Request, State) -> {noreply, State}.

%% @private The live path: what the emitter broadcasts, above the fence.
handle_info({events, Events}, State) ->
    {noreply, link_live(Events, State)};
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #state{store_id = StoreId, subscription = Subscription}) ->
    _ = reckon_db_emitter_group:leave(StoreId, Subscription, self()),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%====================================================================
%% Subscription and fences
%%====================================================================

%% @private
subscription_name(Name) ->
    <<"$link-sub:", Name/binary>>.

%% @private Create the subscription the emitter broadcasts for.
%%
%% No subscriber pid on purpose: a pid makes the store replay the whole
%% global log to it as catch-up, and this link does its own backfill.
subscribe(StoreId, Link, Subscription) ->
    case reckon_db_subscriptions:exists(StoreId, Subscription) of
        true -> ok;
        false -> create_subscription(StoreId, Link, Subscription)
    end.

%% @private
create_subscription(StoreId, Link, Subscription) ->
    {Type, Selector} = source_selector(Link),
    subscribe_once(reckon_db_subscriptions:subscribe(StoreId, Type, Selector,
                                                     Subscription, #{})).

%% @private
subscribe_once({ok, _Key}) -> ok;
subscribe_once({error, already_exists}) -> ok;
subscribe_once({error, _} = Error) -> Error.

%% @private
source_selector(#link{source = #{type := stream, stream_id := StreamId}}) ->
    {stream, StreamId};
source_selector(#link{source = #{type := stream_pattern, pattern := Pattern}}) ->
    {event_pattern, Pattern};
source_selector(#link{source = #{type := all}}) ->
    {all, <<"*">>}.

%% @private The newest version of every source stream, now.
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

%% @private Link everything each source stream held up to its fence.
%%
%% `backfill => false' still stores the fence: the events the stream already
%% holds are deliberately not linked, and the fence is what keeps the live
%% path from linking them either. Without it, a link created with no backfill
%% would quietly link whatever a live event happened to arrive next to.
backfill(#state{store_id = StoreId, link = #link{backfill = false} = Link,
                fences = Fences}, StreamIds) ->
    lists:foreach(fun(StreamId) ->
                          ok = store_fence(StoreId, Link#link.name, StreamId,
                                           maps:get(StreamId, Fences))
                  end, StreamIds),
    ok;
backfill(_State, []) ->
    ok;
backfill(#state{link = Link, fences = Fences} = State, [StreamId | Rest]) ->
    Fence = maps:get(StreamId, Fences),
    ok = link_stream_to(State, StreamId, Fence),
    backfill(State#state{link = Link}, Rest).

%% @private
%%
%% The fold is asked for the whole stream and the batches above the fence are
%% dropped: the fold has no early exit, and a pass that stops at the fence
%% would need a second cursor. What is above the fence is delivered live, and
%% the live path skips what is at or below it, so nothing is linked twice.
link_stream_to(#state{store_id = StoreId, link = Link, name = Name}, StreamId,
               Fence) ->
    Feed = fun(Events, ok) ->
                   link_batch(StoreId, Link, Name, StreamId, Fence, Events)
           end,
    case reckon_db_streams:fold_event_batches(StoreId, StreamId, ?BATCH,
                                              forward, Feed, ok) of
        {ok, ok} ->
            ok;
        {error, Reason} ->
            logger:warning("link ~p: backfill of stream ~p stopped at ~p; the "
                           "events after that point are not linked",
                           [Name, StreamId, Reason]),
            ok
    end.

%% @private Link the part of a batch that is at or below the fence.
link_batch(StoreId, Link, Name, StreamId, Fence, Events) ->
    Below = [E || #event{version = V} = E <- Events, V =< Fence],
    ok = append_events(StoreId, Link, Name, Below),
    ok = store_fence(StoreId, Name, StreamId, Fence),
    ok.

%%====================================================================
%% Live delivery
%%====================================================================

%% @private Link what arrived live, each event at most once.
link_live(Events, State) ->
    Above = [E || E <- Events, above_fence(E, State)],
    link_live_by_stream(Above, State).

%% @private
link_live_by_stream([], State) ->
    State;
link_live_by_stream(Events, #state{store_id = StoreId, link = Link,
                                   name = Name, fences = Fences} = State) ->
    ByStream = group_by_stream(Events),
    Fences1 =
        maps:fold(
          fun(StreamId, StreamEvents, Acc) ->
                  %% group_by_stream/1 builds its lists by prepending, so they
                  %% are in the reverse of the order the events arrived in.
                  Ordered = lists:reverse(StreamEvents),
                  ok = append_events(StoreId, Link, Name, Ordered),
                  Highest = lists:max([E#event.version || E <- StreamEvents]),
                  ok = store_fence(StoreId, Name, StreamId, Highest),
                  Acc#{StreamId => Highest}
          end, Fences, ByStream),
    State#state{fences = Fences1}.

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
        ok -> ok;
        {error, Reason} ->
            %% Not swallowed: the fence is only advanced when the append
            %% landed, so a failed append leaves the events to be linked by
            %% the next backfill rather than lost behind an advanced fence.
            logger:error("link ~p: appending to ~p failed: ~p",
                         [Name, reckon_db_links:link_stream_id(Name), Reason]),
            ok
    end.

%%====================================================================
%% Fences in the store
%%====================================================================

%% @private
store_fence(StoreId, Name, StreamId, Version) ->
    Path = fence_path(StoreId, Name, StreamId),
    case khepri:put(StoreId, Path, Version) of
        ok -> ok;
        {error, Reason} ->
            logger:warning("link ~p: could not record the fence for ~p: ~p",
                           [Name, StreamId, Reason]),
            ok
    end.

%% @private
fence_path(StoreId, Name, StreamId) ->
    ?LINKS_PATH ++ [StoreId, Name, fences, StreamId].
