%% @doc Optimized graph operations for causation analysis in reckon-db.
%%
%% This module provides high-performance graph algorithms:
%%
%% <ul>
%% <li><b>Graph building</b>: Build edges from event causation relationships</li>
%% <li><b>Topological sort</b>: Order events by causation (causes before effects)</li>
%% <li><b>Path finding</b>: Check paths, find ancestors/descendants</li>
%% <li><b>DOT export</b>: Generate Graphviz visualization</li>
%% </ul>
%%
%% The mode is automatically detected at startup based on whether the NIF
%% library is available. Community edition users (hex.pm) will always use
%% the Erlang fallbacks, which provide identical functionality.
%%
%% == Usage ==
%%
%% ```
%% %% Build edges from events (event_id, causation_id pairs)
%% Nodes = [{<<"evt-1">>, undefined}, {<<"evt-2">>, <<"evt-1">>}],
%% Edges = esdb_graph_nif:build_edges(Nodes).
%%
%% %% Topological sort
%% {ok, Sorted} = esdb_graph_nif:topo_sort([<<"evt-1">>, <<"evt-2">>], Edges).
%%
%% %% Check which mode is active
%% nif = esdb_graph_nif:implementation().  %% Enterprise
%% erlang = esdb_graph_nif:implementation(). %% Community
%% '''
%%
%% @author rgfaber

-module(esdb_graph_nif).

%% Public API - Graph Building
-export([
    build_edges/1,
    find_roots/2,
    find_leaves/2
]).

%% Public API - Topological Sort
-export([
    topo_sort/2,
    has_cycle/2
]).

%% Public API - Graph Metrics
-export([
    graph_stats/2
]).

%% Public API - DOT Export
-export([
    to_dot/2,
    to_dot_simple/2
]).

%% Public API - Path Finding
-export([
    has_path/4,
    get_ancestors/3,
    get_descendants/3
]).

%% Introspection
-export([
    is_nif_loaded/0,
    implementation/0
]).

%% For testing - expose both implementations
-export([
    nif_build_edges/1,
    nif_find_roots/2,
    nif_find_leaves/2,
    nif_topo_sort/2,
    nif_has_cycle/2,
    nif_graph_stats/2,
    nif_to_dot/2,
    nif_to_dot_simple/2,
    nif_has_path/4,
    nif_get_ancestors/3,
    nif_get_descendants/3,
    erlang_build_edges/1,
    erlang_find_roots/2,
    erlang_find_leaves/2,
    erlang_topo_sort/2,
    erlang_has_cycle/2,
    erlang_graph_stats/2,
    erlang_to_dot/2,
    erlang_to_dot_simple/2,
    erlang_has_path/4,
    erlang_get_ancestors/3,
    erlang_get_descendants/3
]).

%% NIF loading
-on_load(init/0).

%% Persistent term key for NIF status
-define(NIF_LOADED_KEY, esdb_graph_nif_loaded).

%%====================================================================
%% NIF Loading
%%====================================================================

%% @private
%% Try to load NIF from multiple locations:
%% 1. reckon_nifs priv/ (enterprise addon package)
%% 2. reckon_db priv/ (standalone enterprise build)
-spec init() -> ok.
init() ->
    NifName = "esdb_graph_nif",
    Paths = nif_search_paths(NifName),
    case try_load_nif(Paths) of
        ok ->
            persistent_term:put(?NIF_LOADED_KEY, true),
            logger:info("[esdb_graph_nif] NIF loaded - Enterprise mode"),
            ok;
        {error, Reason} ->
            persistent_term:put(?NIF_LOADED_KEY, false),
            logger:info("[esdb_graph_nif] NIF not available (~p), using pure Erlang - Community mode",
                       [Reason]),
            ok
    end.

%% @private
nif_search_paths(NifName) ->
    Paths = [
        case code:priv_dir(reckon_nifs) of
            {error, _} -> undefined;
            NifsDir -> filename:join(NifsDir, NifName)
        end,
        case code:priv_dir(reckon_db) of
            {error, _} -> filename:join("priv", NifName);
            Dir -> filename:join(Dir, NifName)
        end
    ],
    [P || P <- Paths, P =/= undefined].

%% @private
try_load_nif([]) ->
    {error, no_nif_found};
try_load_nif([Path | Rest]) ->
    case erlang:load_nif(Path, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, _} -> try_load_nif(Rest)
    end.

%%====================================================================
%% Introspection API
%%====================================================================

%% @doc Check if the NIF is loaded (Enterprise mode).
-spec is_nif_loaded() -> boolean().
is_nif_loaded() ->
    persistent_term:get(?NIF_LOADED_KEY, false).

%% @doc Get the current implementation mode.
-spec implementation() -> nif | erlang.
implementation() ->
    case is_nif_loaded() of
        true -> nif;
        false -> erlang
    end.

%%====================================================================
%% Public API - Graph Building
%%====================================================================

%% @doc Build edges from a list of event_id, causation_id tuples.
%%
%% Causation ID can be undefined for root events.
%% Returns list of from_id, to_id edges.
-spec build_edges([{EventId :: binary(), CausationId :: binary() | undefined}]) ->
    [{binary(), binary()}].
build_edges(Nodes) ->
    case is_nif_loaded() of
        true -> nif_build_edges(Nodes);
        false -> erlang_build_edges(Nodes)
    end.

%% @doc Find root nodes (nodes with no incoming edges).
-spec find_roots(Nodes :: [binary()], Edges :: [{binary(), binary()}]) -> [binary()].
find_roots(Nodes, Edges) ->
    case is_nif_loaded() of
        true -> nif_find_roots(Nodes, Edges);
        false -> erlang_find_roots(Nodes, Edges)
    end.

%% @doc Find leaf nodes (nodes with no outgoing edges).
-spec find_leaves(Nodes :: [binary()], Edges :: [{binary(), binary()}]) -> [binary()].
find_leaves(Nodes, Edges) ->
    case is_nif_loaded() of
        true -> nif_find_leaves(Nodes, Edges);
        false -> erlang_find_leaves(Nodes, Edges)
    end.

%%====================================================================
%% Public API - Topological Sort
%%====================================================================

%% @doc Perform topological sort on the graph.
%%
%% Returns nodes in dependency order (causes before effects).
-spec topo_sort(Nodes :: [binary()], Edges :: [{binary(), binary()}]) ->
    {ok, [binary()]} | {error, cycle_detected}.
topo_sort(Nodes, Edges) ->
    case is_nif_loaded() of
        true -> nif_topo_sort(Nodes, Edges);
        false -> erlang_topo_sort(Nodes, Edges)
    end.

%% @doc Check if the graph contains cycles.
-spec has_cycle(Nodes :: [binary()], Edges :: [{binary(), binary()}]) -> boolean().
has_cycle(Nodes, Edges) ->
    case is_nif_loaded() of
        true -> nif_has_cycle(Nodes, Edges);
        false -> erlang_has_cycle(Nodes, Edges)
    end.

%%====================================================================
%% Public API - Graph Metrics
%%====================================================================

%% @doc Get graph statistics.
%%
%% Returns a map with node_count, edge_count, root_count, leaf_count, max_depth.
-spec graph_stats(Nodes :: [binary()], Edges :: [{binary(), binary()}]) -> map().
graph_stats(Nodes, Edges) ->
    case is_nif_loaded() of
        true -> nif_graph_stats(Nodes, Edges);
        false -> erlang_graph_stats(Nodes, Edges)
    end.

%%====================================================================
%% Public API - DOT Export
%%====================================================================

%% @doc Generate DOT format for Graphviz visualization.
%%
%% Nodes are tuples of {event_id, event_type, label}.
-spec to_dot(Nodes :: [{binary(), binary(), binary()}],
             Edges :: [{binary(), binary()}]) -> binary().
to_dot(Nodes, Edges) ->
    case is_nif_loaded() of
        true ->
            Result = nif_to_dot(Nodes, Edges),
            ensure_binary(Result);
        false -> erlang_to_dot(Nodes, Edges)
    end.

%% @doc Generate DOT format with simplified labels (just type).
%%
%% Nodes are tuples of {event_id, event_type}.
-spec to_dot_simple(Nodes :: [{binary(), binary()}],
                    Edges :: [{binary(), binary()}]) -> binary().
to_dot_simple(Nodes, Edges) ->
    case is_nif_loaded() of
        true ->
            Result = nif_to_dot_simple(Nodes, Edges),
            ensure_binary(Result);
        false -> erlang_to_dot_simple(Nodes, Edges)
    end.

%%====================================================================
%% Public API - Path Finding
%%====================================================================

%% @doc Check if there's a path between two nodes.
-spec has_path(Nodes :: [binary()], Edges :: [{binary(), binary()}],
               From :: binary(), To :: binary()) -> boolean().
has_path(Nodes, Edges, From, To) ->
    case is_nif_loaded() of
        true -> nif_has_path(Nodes, Edges, From, To);
        false -> erlang_has_path(Nodes, Edges, From, To)
    end.

%% @doc Get all ancestors of a node (nodes that can reach it).
-spec get_ancestors(Nodes :: [binary()], Edges :: [{binary(), binary()}],
                    Target :: binary()) -> [binary()].
get_ancestors(Nodes, Edges, Target) ->
    case is_nif_loaded() of
        true -> nif_get_ancestors(Nodes, Edges, Target);
        false -> erlang_get_ancestors(Nodes, Edges, Target)
    end.

%% @doc Get all descendants of a node (nodes reachable from it).
-spec get_descendants(Nodes :: [binary()], Edges :: [{binary(), binary()}],
                      Source :: binary()) -> [binary()].
get_descendants(Nodes, Edges, Source) ->
    case is_nif_loaded() of
        true -> nif_get_descendants(Nodes, Edges, Source);
        false -> erlang_get_descendants(Nodes, Edges, Source)
    end.

%%====================================================================
%% NIF Stubs (replaced when NIF loads)
%%====================================================================

%% @private
nif_build_edges(_Nodes) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_find_roots(_Nodes, _Edges) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_find_leaves(_Nodes, _Edges) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_topo_sort(_Nodes, _Edges) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_has_cycle(_Nodes, _Edges) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_graph_stats(_Nodes, _Edges) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_to_dot(_Nodes, _Edges) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_to_dot_simple(_Nodes, _Edges) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_has_path(_Nodes, _Edges, _From, _To) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_get_ancestors(_Nodes, _Edges, _Target) ->
    erlang:nif_error(nif_not_loaded).

%% @private
nif_get_descendants(_Nodes, _Edges, _Source) ->
    erlang:nif_error(nif_not_loaded).

%%====================================================================
%% Pure Erlang Implementations (Always Available)
%%====================================================================

%% @private
-spec erlang_build_edges([{binary(), binary() | undefined}]) -> [{binary(), binary()}].
erlang_build_edges(Nodes) ->
    NodeIds = sets:from_list([Id || {Id, _} <- Nodes]),
    lists:filtermap(
        fun({EventId, CausationId}) ->
            case CausationId of
                undefined -> false;
                _ ->
                    case sets:is_element(CausationId, NodeIds) of
                        true -> {true, {CausationId, EventId}};
                        false -> false
                    end
            end
        end,
        Nodes
    ).

%% @private
-spec erlang_find_roots([binary()], [{binary(), binary()}]) -> [binary()].
erlang_find_roots(Nodes, Edges) ->
    Targets = sets:from_list([To || {_, To} <- Edges]),
    [Node || Node <- Nodes, not sets:is_element(Node, Targets)].

%% @private
-spec erlang_find_leaves([binary()], [{binary(), binary()}]) -> [binary()].
erlang_find_leaves(Nodes, Edges) ->
    Sources = sets:from_list([From || {From, _} <- Edges]),
    [Node || Node <- Nodes, not sets:is_element(Node, Sources)].

%% @private Kahn's algorithm for topological sort
-spec erlang_topo_sort([binary()], [{binary(), binary()}]) ->
    {ok, [binary()]} | {error, cycle_detected}.
erlang_topo_sort(Nodes, Edges) ->
    %% Build in-degree map and adjacency list
    InDegree0 = maps:from_list([{N, 0} || N <- Nodes]),
    Adjacency0 = maps:from_list([{N, []} || N <- Nodes]),

    {InDegree, Adjacency} = lists:foldl(
        fun({From, To}, {InD, Adj}) ->
            NewInD = maps:update_with(To, fun(V) -> V + 1 end, 1, InD),
            NewAdj = maps:update_with(From, fun(V) -> [To | V] end, [To], Adj),
            {NewInD, NewAdj}
        end,
        {InDegree0, Adjacency0},
        Edges
    ),

    %% Start with nodes that have no incoming edges
    Queue = [N || N <- Nodes, maps:get(N, InDegree, 0) =:= 0],

    kahn_loop(Queue, InDegree, Adjacency, [], length(Nodes)).

kahn_loop([], _InDegree, _Adjacency, Result, ExpectedCount) ->
    case length(Result) =:= ExpectedCount of
        true -> {ok, lists:reverse(Result)};
        false -> {error, cycle_detected}
    end;
kahn_loop([Node | Rest], InDegree, Adjacency, Result, ExpectedCount) ->
    Children = maps:get(Node, Adjacency, []),
    {NewQueue, NewInDegree} = lists:foldl(
        fun(Child, {Q, InD}) ->
            NewDegree = maps:get(Child, InD, 1) - 1,
            UpdatedInD = maps:put(Child, NewDegree, InD),
            case NewDegree of
                0 -> {Q ++ [Child], UpdatedInD};
                _ -> {Q, UpdatedInD}
            end
        end,
        {Rest, InDegree},
        Children
    ),
    kahn_loop(NewQueue, NewInDegree, Adjacency, [Node | Result], ExpectedCount).

%% @private
-spec erlang_has_cycle([binary()], [{binary(), binary()}]) -> boolean().
erlang_has_cycle(Nodes, Edges) ->
    case erlang_topo_sort(Nodes, Edges) of
        {ok, _} -> false;
        {error, cycle_detected} -> true
    end.

%% @private
-spec erlang_graph_stats([binary()], [{binary(), binary()}]) -> map().
erlang_graph_stats(Nodes, Edges) ->
    NodeCount = length(Nodes),
    EdgeCount = length(Edges),
    Roots = erlang_find_roots(Nodes, Edges),
    Leaves = erlang_find_leaves(Nodes, Edges),
    MaxDepth = erlang_calculate_max_depth(Nodes, Edges, Roots),

    #{
        node_count => NodeCount,
        edge_count => EdgeCount,
        root_count => length(Roots),
        leaf_count => length(Leaves),
        max_depth => MaxDepth
    }.

%% @private Calculate max depth via BFS from roots
-spec erlang_calculate_max_depth([binary()], [{binary(), binary()}], [binary()]) ->
    non_neg_integer().
erlang_calculate_max_depth([], _, _) -> 0;
erlang_calculate_max_depth(_, _, []) -> 0;
erlang_calculate_max_depth(_Nodes, Edges, Roots) ->
    %% Build adjacency list
    Adjacency = lists:foldl(
        fun({From, To}, Acc) ->
            maps:update_with(From, fun(V) -> [To | V] end, [To], Acc)
        end,
        #{},
        Edges
    ),

    %% BFS with depth tracking
    Queue = [{R, 0} || R <- Roots],
    bfs_max_depth(Queue, Adjacency, sets:new(), 0).

bfs_max_depth([], _Adjacency, _Visited, MaxDepth) ->
    MaxDepth;
bfs_max_depth([{Node, Depth} | Rest], Adjacency, Visited, MaxDepth) ->
    case sets:is_element(Node, Visited) of
        true ->
            bfs_max_depth(Rest, Adjacency, Visited, MaxDepth);
        false ->
            NewVisited = sets:add_element(Node, Visited),
            NewMaxDepth = max(MaxDepth, Depth),
            Children = maps:get(Node, Adjacency, []),
            ChildQueue = [{C, Depth + 1} || C <- Children,
                          not sets:is_element(C, NewVisited)],
            bfs_max_depth(Rest ++ ChildQueue, Adjacency, NewVisited, NewMaxDepth)
    end.

%% @private
-spec erlang_to_dot([{binary(), binary(), binary()}], [{binary(), binary()}]) -> binary().
erlang_to_dot(Nodes, Edges) ->
    Header = <<"digraph causation {\n  rankdir=TB;\n  node [shape=box, style=rounded];\n\n">>,

    NodeLines = lists:map(
        fun({EventId, EventType, Label}) ->
            EscapedId = escape_dot(EventId),
            EscapedType = escape_dot(EventType),
            EscapedLabel = escape_dot(Label),
            io_lib:format("  \"~s\" [label=\"~s\\n~s\"];\n",
                         [EscapedId, EscapedLabel, EscapedType])
        end,
        Nodes
    ),

    EdgeLines = lists:map(
        fun({From, To}) ->
            io_lib:format("  \"~s\" -> \"~s\";\n",
                         [escape_dot(From), escape_dot(To)])
        end,
        Edges
    ),

    Footer = <<"}\n">>,
    iolist_to_binary([Header, NodeLines, <<"\n">>, EdgeLines, Footer]).

%% @private
-spec erlang_to_dot_simple([{binary(), binary()}], [{binary(), binary()}]) -> binary().
erlang_to_dot_simple(Nodes, Edges) ->
    Header = <<"digraph causation {\n  rankdir=TB;\n  node [shape=box, style=rounded];\n\n">>,

    NodeLines = lists:map(
        fun({EventId, EventType}) ->
            EscapedId = escape_dot(EventId),
            EscapedType = escape_dot(EventType),
            io_lib:format("  \"~s\" [label=\"~s\"];\n",
                         [EscapedId, EscapedType])
        end,
        Nodes
    ),

    EdgeLines = lists:map(
        fun({From, To}) ->
            io_lib:format("  \"~s\" -> \"~s\";\n",
                         [escape_dot(From), escape_dot(To)])
        end,
        Edges
    ),

    Footer = <<"}\n">>,
    iolist_to_binary([Header, NodeLines, <<"\n">>, EdgeLines, Footer]).

%% @private
-spec erlang_has_path([binary()], [{binary(), binary()}], binary(), binary()) -> boolean().
erlang_has_path(_Nodes, Edges, From, To) ->
    %% Build adjacency list
    Adjacency = lists:foldl(
        fun({F, T}, Acc) ->
            maps:update_with(F, fun(V) -> [T | V] end, [T], Acc)
        end,
        #{},
        Edges
    ),

    %% BFS from source
    bfs_find_path([From], Adjacency, sets:new(), To).

bfs_find_path([], _Adjacency, _Visited, _Target) ->
    false;
bfs_find_path([Target | _], _Adjacency, _Visited, Target) ->
    true;
bfs_find_path([Node | Rest], Adjacency, Visited, Target) ->
    case sets:is_element(Node, Visited) of
        true ->
            bfs_find_path(Rest, Adjacency, Visited, Target);
        false ->
            NewVisited = sets:add_element(Node, Visited),
            Children = maps:get(Node, Adjacency, []),
            bfs_find_path(Rest ++ Children, Adjacency, NewVisited, Target)
    end.

%% @private
-spec erlang_get_ancestors([binary()], [{binary(), binary()}], binary()) -> [binary()].
erlang_get_ancestors(_Nodes, Edges, Target) ->
    %% Build reverse adjacency list
    ReverseAdj = lists:foldl(
        fun({From, To}, Acc) ->
            maps:update_with(To, fun(V) -> [From | V] end, [From], Acc)
        end,
        #{},
        Edges
    ),

    %% BFS from target following reverse edges
    bfs_collect([Target], ReverseAdj, sets:from_list([Target]), []).

%% @private
-spec erlang_get_descendants([binary()], [{binary(), binary()}], binary()) -> [binary()].
erlang_get_descendants(_Nodes, Edges, Source) ->
    %% Build adjacency list
    Adjacency = lists:foldl(
        fun({From, To}, Acc) ->
            maps:update_with(From, fun(V) -> [To | V] end, [To], Acc)
        end,
        #{},
        Edges
    ),

    %% BFS from source
    bfs_collect([Source], Adjacency, sets:from_list([Source]), []).

%% @private Generic BFS to collect reachable nodes
bfs_collect([], _Adjacency, _Visited, Collected) ->
    lists:reverse(Collected);
bfs_collect([Node | Rest], Adjacency, Visited, Collected) ->
    Children = maps:get(Node, Adjacency, []),
    {NewQueue, NewVisited, NewCollected} = lists:foldl(
        fun(Child, {Q, V, C}) ->
            case sets:is_element(Child, V) of
                true -> {Q, V, C};
                false -> {Q ++ [Child], sets:add_element(Child, V), [Child | C]}
            end
        end,
        {Rest, Visited, Collected},
        Children
    ),
    bfs_collect(NewQueue, Adjacency, NewVisited, NewCollected).

%%====================================================================
%% Internal Helpers
%%====================================================================

%% @private Escape string for DOT format
-spec escape_dot(binary()) -> binary().
escape_dot(Bin) ->
    Escaped = binary:replace(Bin, <<"\\">>, <<"\\\\">>, [global]),
    binary:replace(Escaped, <<"\"">>, <<"\\\"">>, [global]).

%% @private Ensure result is binary
-spec ensure_binary(binary() | list()) -> binary().
ensure_binary(Result) when is_binary(Result) -> Result;
ensure_binary(Result) when is_list(Result) -> list_to_binary(Result);
ensure_binary(Result) -> Result.
