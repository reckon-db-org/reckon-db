%% @doc The process that keeps one running link up to date (reckon-db#24).
%%
%% One consumer per running link, cluster-wide: it registers under a
%% global name, so a second start anywhere answers already_started.
%%
%% == What it follows ==
%%
%% It subscribes as the subscriber of a `stream' subscription: the source
%% stream itself for a `stream' source, `$all' for `stream_pattern' and
%% `all', where it applies the source match to each event's stream id. A
%% link never follows its own stream, and a pattern or `all' link never
%% follows link streams (two `all' links would feed each other without
%% end), unless its pattern itself names `$link:' streams.
%%
%% == Positions ==
%%
%% Per source stream it keeps the last version it has handled, stored
%% with the link (see reckon_db_links:record_progress/5). An event at or
%% below that version is passed over, so a re-delivered event is never
%% written twice; an event past the next one first has the gap read from
%% its stream, so none is skipped. On start:
%%
%% <ul>
%% <li>a link that never ran starts at version -1 for every stream with
%%     `backfill => true', and at each matching stream's current version
%%     without it, so only later events arrive;</li>
%% <li>every event in the link stream past the link-stream version stored
%%     with the positions raises its source stream's position, which covers
%%     a crash between an append and the progress write that follows it,
%%     also when that append held events from several source streams;</li>
%% <li>after subscribing, every matching stream is read from its position
%%     to its end: that is the backfill, and on a restart it is what the
%%     link missed while it was down.</li>
%% </ul>
%%
%% Each source event that passes the filter reaches the link stream once.
%% A failed append or progress write crashes the consumer; its supervisor
%% restarts it, and the restart reads again from the stored positions.
%%
%% == A link that cannot progress ==
%%
%% A transform that crashes or returns something other than an event, and
%% an event in the link stream that does not name its source position,
%% would fail again on every restart. The link stops instead: it writes
%% what it took before that event, nothing for the event itself (never the
%% untransformed event), removes its subscription, stores status `error'
%% with the reason, and ends normally, so its supervisor leaves it down and
%% every other link keeps running. A filter that crashes selects nothing.
%%
%% @author rgfaber

-module(reckon_db_link_consumer).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_links.hrl").

-export([start_link/2, name/2]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2]).

-define(READ_BATCH, 500).

-record(state, {
    store_id :: atom(),
    link :: #link{},
    link_stream :: binary(),
    positions :: #{binary() => integer()},
    link_version :: integer(),
    written = [] :: [{event(), event()}],
    moved = false :: boolean(),
    halted :: term()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc The global name of a link's consumer.
-spec name(atom(), binary()) -> {reckon_db_link, atom(), binary()}.
name(StoreId, Name) ->
    {reckon_db_link, StoreId, Name}.

%% @doc Start the consumer of a stored link. It answers the subscribe
%% error when the link cannot subscribe.
-spec start_link(atom(), binary()) -> {ok, pid()} | {error, term()}.
start_link(StoreId, Name) ->
    gen_server:start_link({global, name(StoreId, Name)}, ?MODULE, {StoreId, Name}, []).

%%====================================================================
%% gen_server
%%====================================================================

%% @private
init({StoreId, Name}) ->
    case reckon_db_links:link_record(StoreId, Name) of
        {ok, Link} -> init_link(StoreId, Link);
        {error, Reason} -> {stop, Reason}
    end.

%% The link stream is read in handle_continue, not here: a link that
%% cannot read it stops there without its supervisor restarting it.
init_link(StoreId, #link{name = Name, source = Source, link_version = LinkVersion} = Link) ->
    LinkStream = reckon_db_links:link_stream_id(Name),
    Positions = starting_positions(StoreId, Link, LinkStream),
    {ok, GlobalCount} = reckon_db_streams:global_event_count(StoreId),
    Subscribed = reckon_db_subscriptions:subscribe(
                   StoreId, stream, selector(Source), subscription_name(Name),
                   #{subscriber => self(), start_from => GlobalCount}),
    State = #state{store_id = StoreId, link = Link, link_stream = LinkStream,
                   positions = Positions, link_version = LinkVersion},
    subscribed(Subscribed, State).

subscribed({ok, _Key}, #state{store_id = StoreId, link = #link{name = Name}} = State) ->
    ok = reckon_db_links:record_running(StoreId, Name, State#state.positions,
                                        State#state.link_version),
    {ok, State, {continue, sweep}};
subscribed({error, Reason}, _State) ->
    {stop, Reason}.

%% @private Raise the positions from what the link stream already holds,
%% then read every matching stream from its position to its end.
handle_continue(sweep, State) ->
    progressed(flush(sweep(after_last_written(State)))).

%% @private
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private Live events from the subscription, and catch-up batches.
handle_info({events, Events}, State) ->
    progressed(flush(lists:foldl(fun follow/2, State, Events)));
handle_info({subscription_error, Violation}, #state{link = #link{name = Name}} = State) ->
    logger:error("Link ~s: its subscription reported ~p", [Name, Violation]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%====================================================================
%% Positions on start
%%====================================================================

starting_positions(_StoreId, #link{positions = Positions}, _LinkStream)
  when is_map(Positions) ->
    Positions;
starting_positions(_StoreId, #link{backfill = true}, _LinkStream) ->
    #{};
starting_positions(StoreId, #link{source = Source}, LinkStream) ->
    maps:from_list([{S, reckon_db_streams:get_version(StoreId, S)}
                    || S <- source_streams(StoreId, Source, LinkStream)]).

%% Each event written to the link stream after the stored link-stream
%% version names the source position it came from; the progress write
%% after that append may not have happened.
after_last_written(#state{store_id = StoreId, link_stream = LinkStream} = State) ->
    after_last_written(reckon_db_streams:get_version(StoreId, LinkStream), State).

after_last_written(Last, #state{link_version = Read} = State) when Last =< Read ->
    State;
after_last_written(Last, #state{store_id = StoreId, link_stream = LinkStream,
                                link_version = Read} = State) ->
    {ok, Events} = reckon_db_streams:read(StoreId, LinkStream, Read + 1,
                                          min(?READ_BATCH, Last - Read), forward),
    raised(lists:foldl(fun raise/2, State, Events), Last).

raised(#state{halted = undefined} = State, Last) -> after_last_written(Last, State);
raised(State, _Last) -> State.

raise(_Event, #state{halted = Reason} = State) when Reason =/= undefined ->
    State;
raise(#event{version = LV, metadata = #{source_stream := S, source_version := V}},
      #state{positions = Positions} = State) when is_binary(S), is_integer(V) ->
    State#state{positions = Positions#{S => max(V, maps:get(S, Positions, -1))},
                link_version = LV};
raise(#event{version = LV}, State) ->
    State#state{halted = {unreadable_link_event, LV}}.

%%====================================================================
%% Following
%%====================================================================

sweep(#state{store_id = StoreId, link = Link, link_stream = LinkStream} = State) ->
    lists:foldl(fun sweep_stream/2, State, source_streams(StoreId, Link#link.source, LinkStream)).

sweep_stream(StreamId, #state{store_id = StoreId} = State) ->
    fill(StreamId, position(StreamId, State) + 1,
         reckon_db_streams:get_version(StoreId, StreamId), State).

follow(_Event, #state{halted = Reason} = State) when Reason =/= undefined ->
    State;
follow(#event{stream_id = S} = Event, #state{link = Link, link_stream = LinkStream} = State) ->
    follow(is_source(Link#link.source, LinkStream, S), Event, State).

follow(false, _Event, State) ->
    State;
follow(true, #event{stream_id = S, version = V} = Event, State) ->
    follow_at(V, position(S, State), Event, State).

follow_at(V, Position, _Event, State) when V =< Position ->
    State;
follow_at(V, Position, Event, State) when V =:= Position + 1 ->
    take(Event, State);
follow_at(V, Position, #event{stream_id = S} = Event, State) ->
    take(Event, fill(S, Position + 1, V - 1, State)).

%% Read [From, To] of one source stream and take each event in order.
fill(_StreamId, _From, _To, #state{halted = Reason} = State) when Reason =/= undefined ->
    State;
fill(_StreamId, From, To, State) when From > To ->
    State;
fill(StreamId, From, To, #state{store_id = StoreId} = State) ->
    {ok, Events} = reckon_db_streams:read(StoreId, StreamId, From,
                                          min(?READ_BATCH, To - From + 1), forward),
    fill_next(Events, StreamId, To, State).

fill_next([], _StreamId, _To, State) ->
    State;
fill_next(Events, StreamId, To, State) ->
    Taken = lists:foldl(fun take/2, State, Events),
    fill(StreamId, position(StreamId, Taken) + 1, To, Taken).

take(_Event, #state{halted = Reason} = State) when Reason =/= undefined ->
    State;
take(#event{} = Event, #state{link = Link} = State) ->
    taken(derive(apply_filter(Link#link.filter, Event), Link#link.transform, Event),
          Event, State).

taken({halt, Reason}, _Event, State) ->
    State#state{halted = Reason};
taken(skip, Event, State) ->
    moved(Event, State);
taken({write, Derived}, Event, #state{written = Written} = State) ->
    moved(Event, State#state{written = [{Event, Derived} | Written]}).

moved(#event{stream_id = S, version = V}, #state{positions = Positions} = State) ->
    State#state{positions = Positions#{S => V}, moved = true}.

position(StreamId, #state{positions = Positions}) ->
    maps:get(StreamId, Positions, -1).

%% Append what was taken, then store the positions it was taken up to and
%% the link-stream version that append reached.
flush(#state{moved = false} = State) ->
    State;
flush(#state{store_id = StoreId, link = #link{name = Name}, link_stream = LinkStream,
             positions = Positions, written = Written} = State) ->
    Pairs = lists:reverse(Written),
    LinkVersion = append(StoreId, LinkStream, Pairs, State#state.link_version),
    Derived = [D || {_Source, D} <- Pairs],
    ok = reckon_db_links:record_progress(StoreId, Name, Positions, LinkVersion,
                                         Derived, last_id(Derived)),
    State#state{written = [], moved = false, link_version = LinkVersion}.

append(_StoreId, _LinkStream, [], LinkVersion) ->
    LinkVersion;
append(StoreId, LinkStream, Pairs, _LinkVersion) ->
    {ok, LinkVersion} = reckon_db_streams:append(
                          StoreId, LinkStream, -2,
                          [event_to_map(Source, Derived) || {Source, Derived} <- Pairs]),
    LinkVersion.

last_id([]) -> undefined;
last_id(Events) -> (lists:last(Events))#event.event_id.

%% A link that cannot progress stops; see the moduledoc.
progressed(#state{halted = undefined} = State) ->
    {noreply, State};
progressed(#state{store_id = StoreId, link = #link{name = Name}, halted = Reason} = State) ->
    logger:error("Link ~s stopped, it cannot progress: ~p (store: ~p)", [Name, Reason, StoreId]),
    _ = reckon_db_subscriptions:unsubscribe(StoreId, stream, subscription_name(Name)),
    ok = reckon_db_links:record_halt(StoreId, Name, Reason),
    {stop, normal, State}.

%%====================================================================
%% Source
%%====================================================================

selector(#{type := stream, stream_id := StreamId}) -> StreamId;
selector(_PatternOrAll) -> <<"$all">>.

subscription_name(Name) ->
    <<"$link-sub:", Name/binary>>.

%% The source streams that exist now.
source_streams(_StoreId, #{type := stream, stream_id := StreamId}, _LinkStream) ->
    [StreamId];
source_streams(StoreId, Source, LinkStream) ->
    {ok, Streams} = reckon_db_streams:list_streams(StoreId),
    [S || S <- Streams, is_source(Source, LinkStream, S)].

is_source(_Source, LinkStream, LinkStream) ->
    false;
is_source(#{type := stream, stream_id := StreamId}, _LinkStream, S) ->
    S =:= StreamId;
is_source(#{type := stream_pattern, pattern := Pattern}, _LinkStream, S) ->
    (not is_link_stream(S) orelse follows_links(Pattern)) andalso matches_pattern(Pattern, S);
is_source(#{type := all}, _LinkStream, S) ->
    not is_link_stream(S).

%% A pattern follows link streams only when it names them itself.
follows_links(Pattern) ->
    is_link_stream(Pattern).

is_link_stream(<<"$link:", _/binary>>) -> true;
is_link_stream(_) -> false.

matches_pattern(Pattern, S) ->
    re:run(S, wildcard_to_regex(Pattern)) =/= nomatch.

wildcard_to_regex(Pattern) ->
    Escaped = re:replace(Pattern, <<"[.^$+?{}\\[\\]\\\\|()]">>, <<"\\\\&">>,
                         [global, {return, binary}]),
    Converted = binary:replace(Escaped, <<"*">>, <<".*">>, [global]),
    <<"^", Converted/binary, "$">>.

%%====================================================================
%% Filter and transform
%%====================================================================

apply_filter(undefined, _Event) ->
    true;
apply_filter(Filter, Event) ->
    try Filter(Event) =:= true
    catch _:_ -> false
    end.

%% What a selected event becomes. The halt reason names the source
%% position only: a crash reason can carry the event's data, which a
%% redacting transform exists to keep out of the store.
derive(false, _Transform, _Event) ->
    skip;
derive(true, undefined, Event) ->
    {write, Event};
derive(true, Transform, #event{stream_id = S, version = V} = Event) ->
    try Transform(Event) of
        #event{metadata = M} = Derived when is_map(M) -> {write, Derived};
        _NotAnEvent -> {halt, {transform_returned_no_event, S, V}}
    catch Class:_ -> {halt, {transform_crashed, S, V, Class}}
    end.

%% The derived event keeps the id and type the transform left it and names
%% the source position it came from, whatever the transform did to the
%% event's own stream id and version.
event_to_map(#event{stream_id = S, version = V}, Derived) ->
    #{
        event_id => Derived#event.event_id,
        event_type => Derived#event.event_type,
        data => Derived#event.data,
        metadata => maps:merge(Derived#event.metadata, #{
            source_stream => S,
            source_version => V
        })
    }.
