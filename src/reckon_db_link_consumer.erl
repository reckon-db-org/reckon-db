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
%% <li>the last event in the link stream raises its source stream's
%%     position, which covers a crash between an append and the progress
%%     write that follows it;</li>
%% <li>after subscribing, every matching stream is read from its position
%%     to its end: that is the backfill, and on a restart it is what the
%%     link missed while it was down.</li>
%% </ul>
%%
%% Each source event that passes the filter reaches the link stream once.
%% A failed append or progress write crashes the consumer; its supervisor
%% restarts it, and the restart reads again from the stored positions.
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
    written = [] :: [event()],
    moved = false :: boolean()
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

init_link(StoreId, #link{name = Name, source = Source} = Link) ->
    LinkStream = reckon_db_links:link_stream_id(Name),
    Positions = after_last_written(StoreId, LinkStream,
                                   starting_positions(StoreId, Link, LinkStream)),
    ok = reckon_db_links:record_progress(StoreId, Name, Positions, [], undefined),
    {ok, GlobalCount} = reckon_db_streams:global_event_count(StoreId),
    Subscribed = reckon_db_subscriptions:subscribe(
                   StoreId, stream, selector(Source), subscription_name(Name),
                   #{subscriber => self(), start_from => GlobalCount}),
    State = #state{store_id = StoreId, link = Link, link_stream = LinkStream,
                   positions = Positions},
    subscribed(Subscribed, State).

subscribed({ok, _Key}, State) -> {ok, State, {continue, sweep}};
subscribed({error, Reason}, _State) -> {stop, Reason}.

%% @private Read every matching stream from its position to its end.
handle_continue(sweep, #state{store_id = StoreId, link = Link, link_stream = LinkStream} = State) ->
    Streams = source_streams(StoreId, Link#link.source, LinkStream),
    {noreply, flush(lists:foldl(fun sweep_stream/2, State, Streams))}.

%% @private
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private Live events from the subscription, and catch-up batches.
handle_info({events, Events}, State) ->
    {noreply, flush(lists:foldl(fun follow/2, State, Events))};
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

%% The last event written to the link stream names the source position it
%% came from; the progress write after that append may not have happened.
after_last_written(StoreId, LinkStream, Positions) ->
    after_last_written(reckon_db_streams:get_version(StoreId, LinkStream),
                       StoreId, LinkStream, Positions).

after_last_written(Last, _StoreId, _LinkStream, Positions) when Last < 0 ->
    Positions;
after_last_written(Last, StoreId, LinkStream, Positions) ->
    {ok, [#event{metadata = #{source_stream := S, source_version := V}}]} =
        reckon_db_streams:read(StoreId, LinkStream, Last, 1, forward),
    Positions#{S => max(V, maps:get(S, Positions, -1))}.

%%====================================================================
%% Following
%%====================================================================

sweep_stream(StreamId, #state{store_id = StoreId} = State) ->
    fill(StreamId, position(StreamId, State) + 1,
         reckon_db_streams:get_version(StoreId, StreamId), State).

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

take(#event{stream_id = S, version = V} = Event,
     #state{link = Link, positions = Positions, written = Written} = State) ->
    Moved = State#state{positions = Positions#{S => V}, moved = true},
    take(apply_filter(Link#link.filter, Event), Link, Event, Moved, Written).

take(true, Link, Event, State, Written) ->
    State#state{written = [apply_transform(Link#link.transform, Event) | Written]};
take(false, _Link, _Event, State, _Written) ->
    State.

position(StreamId, #state{positions = Positions}) ->
    maps:get(StreamId, Positions, -1).

%% Append what was taken, then store the positions it was taken up to.
flush(#state{moved = false} = State) ->
    State;
flush(#state{store_id = StoreId, link = #link{name = Name}, link_stream = LinkStream,
             positions = Positions, written = Written} = State) ->
    Events = lists:reverse(Written),
    ok = append(StoreId, LinkStream, Events),
    ok = reckon_db_links:record_progress(StoreId, Name, Positions, Events, last_id(Events)),
    State#state{written = [], moved = false}.

append(_StoreId, _LinkStream, []) ->
    ok;
append(StoreId, LinkStream, Events) ->
    {ok, _} = reckon_db_streams:append(StoreId, LinkStream, -2, [event_to_map(E) || E <- Events]),
    ok.

last_id([]) -> undefined;
last_id(Events) -> (lists:last(Events))#event.event_id.

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

apply_transform(undefined, Event) ->
    Event;
apply_transform(Transform, Event) ->
    try Transform(Event)
    catch _:_ -> Event
    end.

%% The derived event keeps the source event's id and type and names the
%% source position it came from.
event_to_map(Event) ->
    #{
        event_id => Event#event.event_id,
        event_type => Event#event.event_type,
        data => Event#event.data,
        metadata => maps:merge(Event#event.metadata, #{
            source_stream => Event#event.stream_id,
            source_version => Event#event.version
        })
    }.
