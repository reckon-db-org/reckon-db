%% @doc Causation and correlation tracking for reckon-db
%%
%% Provides functionality to trace event lineage:
%% - Causation ID: Links an event to its direct cause
%% - Correlation ID: Groups related events in a business process/saga
%% - Actor ID: Identifies who/what triggered the event
%%
%% Standard metadata fields (convention):
%% ```
%% #{
%%     causation_id => binary(),     %% ID of event/command that caused this
%%     correlation_id => binary(),   %% Business process/saga ID
%%     actor_id => binary()          %% Who/what triggered this
%% }
%% '''
%%
%% Use cases:
%% - Debugging distributed event flows
%% - Audit trails
%% - Saga/process manager state reconstruction
%% - Dependency analysis
%%
%% @author rgfaber

-module(reckon_db_causation).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

%% API
-export([
    get_effects/2,
    get_cause/2,
    get_chain/2,
    get_correlated/2,
    build_graph/2,
    to_dot/1
]).

%%====================================================================
%% Types
%%====================================================================

-type causation_graph() :: #{
    nodes := [event()],
    edges := [{binary(), binary()}],  %% {cause_event_id, effect_event_id}
    root := binary() | undefined
}.

-export_type([causation_graph/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get all events caused by the given event.
%%
%% Returns events whose causation_id matches the given event_id.
-spec get_effects(atom(), binary()) -> {ok, [event()]} | {error, term()}.
get_effects(StoreId, EventId) ->
    StartTime = erlang:monotonic_time(),

    Result = scan_for_metadata(StoreId, causation_id, EventId),

    Duration = erlang:monotonic_time() - StartTime,
    emit_telemetry(StoreId, EventId, causation_effects, Duration, Result),

    Result.

%% @doc Get the event that caused the given event.
%%
%% Finds the event whose event_id matches this event's causation_id.
-spec get_cause(atom(), binary()) -> {ok, event()} | {error, not_found | term()}.
get_cause(StoreId, EventId) ->
    StartTime = erlang:monotonic_time(),

    Result = case find_event_by_id(StoreId, EventId) of
        {ok, Event} ->
            case maps:get(causation_id, Event#event.metadata, undefined) of
                undefined ->
                    {error, no_cause};
                CausationId ->
                    find_event_by_id(StoreId, CausationId)
            end;
        Error ->
            Error
    end,

    Duration = erlang:monotonic_time() - StartTime,
    emit_telemetry(StoreId, EventId, causation_cause, Duration, Result),

    Result.

%% @doc Get the full causation chain from root to the given event.
%%
%% Walks backward through causation_id links until reaching
%% an event with no cause. Returns events in order from root to target.
-spec get_chain(atom(), binary()) -> {ok, [event()]} | {error, term()}.
get_chain(StoreId, EventId) ->
    StartTime = erlang:monotonic_time(),

    Result = case find_event_by_id(StoreId, EventId) of
        {ok, Event} ->
            Chain = build_chain_backward(StoreId, Event, [Event]),
            {ok, lists:reverse(Chain)};
        Error ->
            Error
    end,

    Duration = erlang:monotonic_time() - StartTime,
    emit_telemetry(StoreId, EventId, causation_chain, Duration, Result),

    Result.

%% @doc Get all events sharing the same correlation ID.
%%
%% Useful for finding all events in a saga or business process.
-spec get_correlated(atom(), binary()) -> {ok, [event()]} | {error, term()}.
get_correlated(StoreId, CorrelationId) ->
    StartTime = erlang:monotonic_time(),

    Result = scan_for_metadata(StoreId, correlation_id, CorrelationId),

    Duration = erlang:monotonic_time() - StartTime,
    emit_telemetry(StoreId, CorrelationId, causation_correlated, Duration, Result),

    Result.

%% @doc Build a causation graph for visualization.
%%
%% Accepts either an event_id (builds graph from that event) or
%% a correlation_id (builds graph from all correlated events).
%% Returns nodes and edges suitable for graph rendering.
-spec build_graph(atom(), binary()) -> {ok, causation_graph()} | {error, term()}.
build_graph(StoreId, Id) ->
    %% Try as event_id first, then as correlation_id
    case find_event_by_id(StoreId, Id) of
        {ok, RootEvent} ->
            build_graph_from_event(StoreId, RootEvent);
        {error, not_found} ->
            %% Try as correlation_id
            case get_correlated(StoreId, Id) of
                {ok, Events} when Events =/= [] ->
                    build_graph_from_correlation(Events);
                {ok, []} ->
                    {error, not_found};
                Error ->
                    Error
            end
    end.

%% @doc Export a causation graph as DOT format for Graphviz.
%%
%% Usage: dot -Tpng -o graph.png with the output.
-spec to_dot(causation_graph()) -> binary().
to_dot(#{nodes := Nodes, edges := Edges}) ->
    Header = <<"digraph causation {\n">>,
    Indent = <<"  ">>,

    %% Node definitions
    NodeLines = lists:map(
        fun(Event) ->
            Id = Event#event.event_id,
            Type = Event#event.event_type,
            Label = io_lib:format("~s [label=\"~s\\n~s\"];",
                                  [escape_dot(Id), escape_dot(Id), escape_dot(Type)]),
            [Indent, list_to_binary(Label), <<"\n">>]
        end,
        Nodes
    ),

    %% Edge definitions
    EdgeLines = lists:map(
        fun({From, To}) ->
            Line = io_lib:format("~s -> ~s;", [escape_dot(From), escape_dot(To)]),
            [Indent, list_to_binary(Line), <<"\n">>]
        end,
        Edges
    ),

    Footer = <<"}\n">>,

    iolist_to_binary([Header, NodeLines, EdgeLines, Footer]).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Scan all streams for events matching metadata field
-spec scan_for_metadata(atom(), atom(), binary()) -> {ok, [event()]} | {error, term()}.
scan_for_metadata(StoreId, Field, Value) ->
    %% Get all streams
    case reckon_db_streams:list_streams(StoreId) of
        {ok, StreamIds} ->
            Events = lists:foldl(
                fun(StreamId, Acc) ->
                    MatchingEvents = scan_stream_for_metadata(StoreId, StreamId, Field, Value),
                    Acc ++ MatchingEvents
                end,
                [],
                StreamIds
            ),
            %% Sort by epoch_us for consistent ordering
            Sorted = lists:sort(
                fun(A, B) -> A#event.epoch_us =< B#event.epoch_us end,
                Events
            ),
            {ok, Sorted};
        Error ->
            Error
    end.

%% @private Scan a single stream for matching events
-spec scan_stream_for_metadata(atom(), binary(), atom(), binary()) -> [event()].
scan_stream_for_metadata(StoreId, StreamId, Field, Value) ->
    case reckon_db_streams:read(StoreId, StreamId, 0, 10000, forward) of
        {ok, Events} ->
            lists:filter(
                fun(Event) ->
                    case maps:get(Field, Event#event.metadata, undefined) of
                        Value -> true;
                        _ -> false
                    end
                end,
                Events
            );
        {error, _} ->
            []
    end.

%% @private Find an event by its ID across all streams
-spec find_event_by_id(atom(), binary()) -> {ok, event()} | {error, not_found | term()}.
find_event_by_id(StoreId, EventId) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, StreamIds} ->
            find_event_in_streams(StoreId, StreamIds, EventId);
        Error ->
            Error
    end.

%% @private Search streams for an event
-spec find_event_in_streams(atom(), [binary()], binary()) -> {ok, event()} | {error, not_found}.
find_event_in_streams(_StoreId, [], _EventId) ->
    {error, not_found};
find_event_in_streams(StoreId, [StreamId | Rest], EventId) ->
    case scan_stream_for_event(StoreId, StreamId, EventId) of
        {ok, Event} ->
            {ok, Event};
        not_found ->
            find_event_in_streams(StoreId, Rest, EventId)
    end.

%% @private Scan a stream for an event by ID
-spec scan_stream_for_event(atom(), binary(), binary()) -> {ok, event()} | not_found.
scan_stream_for_event(StoreId, StreamId, EventId) ->
    case reckon_db_streams:read(StoreId, StreamId, 0, 10000, forward) of
        {ok, Events} ->
            case lists:search(fun(E) -> E#event.event_id =:= EventId end, Events) of
                {value, Event} -> {ok, Event};
                false -> not_found
            end;
        {error, _} ->
            not_found
    end.

%% @private Build causation chain by walking backward
-spec build_chain_backward(atom(), event(), [event()]) -> [event()].
build_chain_backward(StoreId, Event, Acc) ->
    case maps:get(causation_id, Event#event.metadata, undefined) of
        undefined ->
            Acc;
        CausationId ->
            case find_event_by_id(StoreId, CausationId) of
                {ok, CauseEvent} ->
                    build_chain_backward(StoreId, CauseEvent, [CauseEvent | Acc]);
                {error, _} ->
                    Acc
            end
    end.

%% @private Build graph starting from a single event
-spec build_graph_from_event(atom(), event()) -> {ok, causation_graph()}.
build_graph_from_event(StoreId, RootEvent) ->
    %% First, get the full chain to find the true root
    {ok, Chain} = get_chain(StoreId, RootEvent#event.event_id),
    TrueRoot = hd(Chain),

    %% Then, build graph by finding all effects recursively
    {AllNodes, AllEdges} = collect_effects(StoreId, TrueRoot, [TrueRoot], []),

    {ok, #{
        nodes => AllNodes,
        edges => AllEdges,
        root => TrueRoot#event.event_id
    }}.

%% @private Build graph from correlated events
-spec build_graph_from_correlation([event()]) -> {ok, causation_graph()}.
build_graph_from_correlation(Events) ->
    %% Build edges from causation_id relationships
    EventMap = maps:from_list([{E#event.event_id, E} || E <- Events]),

    Edges = lists:filtermap(
        fun(Event) ->
            case maps:get(causation_id, Event#event.metadata, undefined) of
                undefined ->
                    false;
                CausationId ->
                    case maps:is_key(CausationId, EventMap) of
                        true -> {true, {CausationId, Event#event.event_id}};
                        false -> false
                    end
            end
        end,
        Events
    ),

    %% Find roots (events with no incoming edges)
    AllTargets = [To || {_, To} <- Edges],
    Roots = [E || E <- Events, not lists:member(E#event.event_id, AllTargets)],

    RootId = case Roots of
        [First | _] -> First#event.event_id;
        [] -> undefined
    end,

    {ok, #{
        nodes => Events,
        edges => Edges,
        root => RootId
    }}.

%% @private Recursively collect effects
-spec collect_effects(atom(), event(), [event()], [{binary(), binary()}]) ->
    {[event()], [{binary(), binary()}]}.
collect_effects(StoreId, Event, AccNodes, AccEdges) ->
    EventId = Event#event.event_id,

    case get_effects(StoreId, EventId) of
        {ok, Effects} ->
            %% Add edges from this event to its effects
            NewEdges = [{EventId, E#event.event_id} || E <- Effects],

            %% Filter out already visited events to prevent cycles
            VisitedIds = [E#event.event_id || E <- AccNodes],
            NewEffects = [E || E <- Effects,
                          not lists:member(E#event.event_id, VisitedIds)],

            %% Recursively collect from new effects
            lists:foldl(
                fun(EffectEvent, {Nodes, Edges}) ->
                    collect_effects(StoreId, EffectEvent,
                                   [EffectEvent | Nodes],
                                   NewEdges ++ Edges)
                end,
                {AccNodes, AccEdges ++ NewEdges},
                NewEffects
            );
        {error, _} ->
            {AccNodes, AccEdges}
    end.

%% @private Escape string for DOT format
-spec escape_dot(binary()) -> binary().
escape_dot(Bin) ->
    %% Replace quotes and backslashes
    Escaped = binary:replace(Bin, <<"\\">>, <<"\\\\">>, [global]),
    binary:replace(Escaped, <<"\"">>, <<"\\\"">>, [global]).

%% @private Emit telemetry for causation queries
-spec emit_telemetry(atom(), binary(), atom(), integer(), term()) -> ok.
emit_telemetry(StoreId, Id, QueryType, Duration, Result) ->
    EventCount = case Result of
        {ok, Events} when is_list(Events) -> length(Events);
        {ok, _} -> 1;
        {error, _} -> 0
    end,

    telemetry:execute(
        [reckon_db, causation, query],
        #{duration => Duration, event_count => EventCount},
        #{store_id => StoreId, id => Id, query_type => QueryType}
    ),
    ok.
