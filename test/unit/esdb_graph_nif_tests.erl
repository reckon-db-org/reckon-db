%% @doc Unit tests for esdb_graph_nif module.
%%
%% Tests both the pure Erlang implementations (always tested) and the NIF
%% implementations (tested only when available in Enterprise builds).
%%
%% @author R. Lefever

-module(esdb_graph_nif_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Data
%%====================================================================

%% Simple linear causation chain: A -> B -> C -> D
linear_chain_nodes() ->
    [
        {<<"evt-a">>, undefined},
        {<<"evt-b">>, <<"evt-a">>},
        {<<"evt-c">>, <<"evt-b">>},
        {<<"evt-d">>, <<"evt-c">>}
    ].

%% Diamond pattern: A -> B, A -> C, B -> D, C -> D
diamond_nodes() ->
    [
        {<<"evt-a">>, undefined},
        {<<"evt-b">>, <<"evt-a">>},
        {<<"evt-c">>, <<"evt-a">>},
        {<<"evt-d">>, <<"evt-b">>}  %% D is also caused by C but only one causation_id
    ].

%% Multiple roots: A1 -> B1, A2 -> B2
multi_root_nodes() ->
    [
        {<<"root-1">>, undefined},
        {<<"root-2">>, undefined},
        {<<"child-1">>, <<"root-1">>},
        {<<"child-2">>, <<"root-2">>}
    ].

%%====================================================================
%% Pure Erlang Implementation Tests (Always Run)
%%====================================================================

erlang_build_edges_linear_test() ->
    Nodes = linear_chain_nodes(),
    Edges = esdb_graph_nif:erlang_build_edges(Nodes),
    ?assertEqual(3, length(Edges)),
    ?assert(lists:member({<<"evt-a">>, <<"evt-b">>}, Edges)),
    ?assert(lists:member({<<"evt-b">>, <<"evt-c">>}, Edges)),
    ?assert(lists:member({<<"evt-c">>, <<"evt-d">>}, Edges)).

erlang_build_edges_empty_test() ->
    Edges = esdb_graph_nif:erlang_build_edges([]),
    ?assertEqual([], Edges).

erlang_build_edges_single_root_test() ->
    Edges = esdb_graph_nif:erlang_build_edges([{<<"only">>, undefined}]),
    ?assertEqual([], Edges).

erlang_find_roots_linear_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    Roots = esdb_graph_nif:erlang_find_roots(NodeIds, Edges),
    ?assertEqual([<<"evt-a">>], Roots).

erlang_find_roots_multi_test() ->
    NodeIds = [Id || {Id, _} <- multi_root_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(multi_root_nodes()),
    Roots = lists:sort(esdb_graph_nif:erlang_find_roots(NodeIds, Edges)),
    ?assertEqual([<<"root-1">>, <<"root-2">>], Roots).

erlang_find_leaves_linear_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    Leaves = esdb_graph_nif:erlang_find_leaves(NodeIds, Edges),
    ?assertEqual([<<"evt-d">>], Leaves).

erlang_find_leaves_multi_test() ->
    NodeIds = [Id || {Id, _} <- multi_root_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(multi_root_nodes()),
    Leaves = lists:sort(esdb_graph_nif:erlang_find_leaves(NodeIds, Edges)),
    ?assertEqual([<<"child-1">>, <<"child-2">>], Leaves).

erlang_topo_sort_linear_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    {ok, Sorted} = esdb_graph_nif:erlang_topo_sort(NodeIds, Edges),
    %% Verify order: each node appears before its effects
    AIdx = index_of(<<"evt-a">>, Sorted),
    BIdx = index_of(<<"evt-b">>, Sorted),
    CIdx = index_of(<<"evt-c">>, Sorted),
    DIdx = index_of(<<"evt-d">>, Sorted),
    ?assert(AIdx < BIdx),
    ?assert(BIdx < CIdx),
    ?assert(CIdx < DIdx).

erlang_topo_sort_empty_test() ->
    {ok, Sorted} = esdb_graph_nif:erlang_topo_sort([], []),
    ?assertEqual([], Sorted).

erlang_has_cycle_false_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    ?assertNot(esdb_graph_nif:erlang_has_cycle(NodeIds, Edges)).

erlang_has_cycle_true_test() ->
    %% Create a cycle: A -> B -> C -> A
    Nodes = [<<"a">>, <<"b">>, <<"c">>],
    Edges = [{<<"a">>, <<"b">>}, {<<"b">>, <<"c">>}, {<<"c">>, <<"a">>}],
    ?assert(esdb_graph_nif:erlang_has_cycle(Nodes, Edges)).

erlang_graph_stats_linear_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    Stats = esdb_graph_nif:erlang_graph_stats(NodeIds, Edges),
    ?assertEqual(4, maps:get(node_count, Stats)),
    ?assertEqual(3, maps:get(edge_count, Stats)),
    ?assertEqual(1, maps:get(root_count, Stats)),
    ?assertEqual(1, maps:get(leaf_count, Stats)),
    ?assertEqual(3, maps:get(max_depth, Stats)).

erlang_graph_stats_multi_root_test() ->
    NodeIds = [Id || {Id, _} <- multi_root_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(multi_root_nodes()),
    Stats = esdb_graph_nif:erlang_graph_stats(NodeIds, Edges),
    ?assertEqual(4, maps:get(node_count, Stats)),
    ?assertEqual(2, maps:get(edge_count, Stats)),
    ?assertEqual(2, maps:get(root_count, Stats)),
    ?assertEqual(2, maps:get(leaf_count, Stats)),
    ?assertEqual(1, maps:get(max_depth, Stats)).

erlang_to_dot_simple_test() ->
    Nodes = [{<<"evt-a">>, <<"Created">>}, {<<"evt-b">>, <<"Updated">>}],
    Edges = [{<<"evt-a">>, <<"evt-b">>}],
    Dot = esdb_graph_nif:erlang_to_dot_simple(Nodes, Edges),
    ?assert(is_binary(Dot)),
    ?assert(binary:match(Dot, <<"digraph causation">>) =/= nomatch),
    ?assert(binary:match(Dot, <<"evt-a">>)  =/= nomatch),
    ?assert(binary:match(Dot, <<"evt-b">>)  =/= nomatch),
    ?assert(binary:match(Dot, <<"->">>) =/= nomatch).

erlang_to_dot_full_test() ->
    Nodes = [{<<"evt-a">>, <<"OrderCreated">>, <<"Order 123">>}],
    Edges = [],
    Dot = esdb_graph_nif:erlang_to_dot(Nodes, Edges),
    ?assert(is_binary(Dot)),
    ?assert(binary:match(Dot, <<"OrderCreated">>) =/= nomatch),
    ?assert(binary:match(Dot, <<"Order 123">>) =/= nomatch).

erlang_has_path_true_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    ?assert(esdb_graph_nif:erlang_has_path(NodeIds, Edges, <<"evt-a">>, <<"evt-d">>)),
    ?assert(esdb_graph_nif:erlang_has_path(NodeIds, Edges, <<"evt-a">>, <<"evt-b">>)),
    ?assert(esdb_graph_nif:erlang_has_path(NodeIds, Edges, <<"evt-b">>, <<"evt-d">>)).

erlang_has_path_false_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    %% Reverse direction should not have path
    ?assertNot(esdb_graph_nif:erlang_has_path(NodeIds, Edges, <<"evt-d">>, <<"evt-a">>)),
    ?assertNot(esdb_graph_nif:erlang_has_path(NodeIds, Edges, <<"evt-c">>, <<"evt-a">>)).

erlang_get_ancestors_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    Ancestors = lists:sort(esdb_graph_nif:erlang_get_ancestors(NodeIds, Edges, <<"evt-d">>)),
    ?assertEqual([<<"evt-a">>, <<"evt-b">>, <<"evt-c">>], Ancestors).

erlang_get_ancestors_root_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    Ancestors = esdb_graph_nif:erlang_get_ancestors(NodeIds, Edges, <<"evt-a">>),
    ?assertEqual([], Ancestors).

erlang_get_descendants_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    Descendants = lists:sort(esdb_graph_nif:erlang_get_descendants(NodeIds, Edges, <<"evt-a">>)),
    ?assertEqual([<<"evt-b">>, <<"evt-c">>, <<"evt-d">>], Descendants).

erlang_get_descendants_leaf_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),
    Descendants = esdb_graph_nif:erlang_get_descendants(NodeIds, Edges, <<"evt-d">>),
    ?assertEqual([], Descendants).

%%====================================================================
%% Public API Tests (Uses Dispatch)
%%====================================================================

api_build_edges_test() ->
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    ?assertEqual(3, length(Edges)).

api_find_roots_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    Roots = esdb_graph_nif:find_roots(NodeIds, Edges),
    ?assertEqual([<<"evt-a">>], Roots).

api_find_leaves_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    Leaves = esdb_graph_nif:find_leaves(NodeIds, Edges),
    ?assertEqual([<<"evt-d">>], Leaves).

api_topo_sort_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    {ok, Sorted} = esdb_graph_nif:topo_sort(NodeIds, Edges),
    ?assertEqual(4, length(Sorted)).

api_has_cycle_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    ?assertNot(esdb_graph_nif:has_cycle(NodeIds, Edges)).

api_graph_stats_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    Stats = esdb_graph_nif:graph_stats(NodeIds, Edges),
    ?assert(is_map(Stats)),
    ?assertEqual(4, maps:get(node_count, Stats)).

api_to_dot_simple_test() ->
    Nodes = [{<<"evt-a">>, <<"Type">>}],
    Edges = [],
    Dot = esdb_graph_nif:to_dot_simple(Nodes, Edges),
    ?assert(is_binary(Dot)).

api_to_dot_test() ->
    Nodes = [{<<"evt-a">>, <<"Type">>, <<"Label">>}],
    Edges = [],
    Dot = esdb_graph_nif:to_dot(Nodes, Edges),
    ?assert(is_binary(Dot)).

api_has_path_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    ?assert(esdb_graph_nif:has_path(NodeIds, Edges, <<"evt-a">>, <<"evt-d">>)).

api_get_ancestors_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    Ancestors = esdb_graph_nif:get_ancestors(NodeIds, Edges, <<"evt-d">>),
    ?assertEqual(3, length(Ancestors)).

api_get_descendants_test() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:build_edges(linear_chain_nodes()),
    Descendants = esdb_graph_nif:get_descendants(NodeIds, Edges, <<"evt-a">>),
    ?assertEqual(3, length(Descendants)).

%%====================================================================
%% Introspection Tests
%%====================================================================

implementation_returns_atom_test() ->
    Impl = esdb_graph_nif:implementation(),
    ?assert(Impl =:= nif orelse Impl =:= erlang).

is_nif_loaded_returns_boolean_test() ->
    Loaded = esdb_graph_nif:is_nif_loaded(),
    ?assert(is_boolean(Loaded)).

%%====================================================================
%% NIF Implementation Tests (Only When Available)
%%====================================================================

nif_tests_when_available_test_() ->
    case esdb_graph_nif:is_nif_loaded() of
        true ->
            [
                {"NIF build_edges works", fun nif_build_edges_works/0},
                {"NIF find_roots works", fun nif_find_roots_works/0},
                {"NIF find_leaves works", fun nif_find_leaves_works/0},
                {"NIF topo_sort works", fun nif_topo_sort_works/0},
                {"NIF has_cycle works", fun nif_has_cycle_works/0},
                {"NIF graph_stats works", fun nif_graph_stats_works/0},
                {"NIF to_dot_simple works", fun nif_to_dot_simple_works/0},
                {"NIF has_path works", fun nif_has_path_works/0},
                {"NIF get_ancestors works", fun nif_get_ancestors_works/0},
                {"NIF get_descendants works", fun nif_get_descendants_works/0}
            ];
        false ->
            []
    end.

nif_build_edges_works() ->
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    ?assertEqual(3, length(Edges)).

nif_find_roots_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    Roots = esdb_graph_nif:nif_find_roots(NodeIds, Edges),
    ?assertEqual([<<"evt-a">>], Roots).

nif_find_leaves_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    Leaves = esdb_graph_nif:nif_find_leaves(NodeIds, Edges),
    ?assertEqual([<<"evt-d">>], Leaves).

nif_topo_sort_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    {ok, Sorted} = esdb_graph_nif:nif_topo_sort(NodeIds, Edges),
    ?assertEqual(4, length(Sorted)).

nif_has_cycle_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    ?assertNot(esdb_graph_nif:nif_has_cycle(NodeIds, Edges)).

nif_graph_stats_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    Stats = esdb_graph_nif:nif_graph_stats(NodeIds, Edges),
    ?assert(is_map(Stats)),
    ?assertEqual(4, maps:get(node_count, Stats)).

nif_to_dot_simple_works() ->
    Nodes = [{<<"evt-a">>, <<"Type">>}],
    Edges = [],
    Dot = esdb_graph_nif:nif_to_dot_simple(Nodes, Edges),
    ?assert(is_list(Dot) orelse is_binary(Dot)).

nif_has_path_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    ?assert(esdb_graph_nif:nif_has_path(NodeIds, Edges, <<"evt-a">>, <<"evt-d">>)).

nif_get_ancestors_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    Ancestors = esdb_graph_nif:nif_get_ancestors(NodeIds, Edges, <<"evt-d">>),
    ?assertEqual(3, length(Ancestors)).

nif_get_descendants_works() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:nif_build_edges(linear_chain_nodes()),
    Descendants = esdb_graph_nif:nif_get_descendants(NodeIds, Edges, <<"evt-a">>),
    ?assertEqual(3, length(Descendants)).

%%====================================================================
%% Equivalence Tests (Enterprise builds only)
%%====================================================================

equivalence_tests_when_available_test_() ->
    case esdb_graph_nif:is_nif_loaded() of
        true ->
            [
                {"Equivalence: build_edges", fun equivalence_build_edges/0},
                {"Equivalence: topo_sort", fun equivalence_topo_sort/0},
                {"Equivalence: graph_stats", fun equivalence_graph_stats/0},
                {"Equivalence: has_path", fun equivalence_has_path/0}
            ];
        false ->
            []
    end.

equivalence_build_edges() ->
    TestCases = [
        linear_chain_nodes(),
        diamond_nodes(),
        multi_root_nodes(),
        []
    ],
    lists:foreach(fun(Nodes) ->
        ErlEdges = lists:sort(esdb_graph_nif:erlang_build_edges(Nodes)),
        NifEdges = lists:sort(esdb_graph_nif:nif_build_edges(Nodes)),
        ?assertEqual(ErlEdges, NifEdges)
    end, TestCases).

equivalence_topo_sort() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),

    {ok, ErlSorted} = esdb_graph_nif:erlang_topo_sort(NodeIds, Edges),
    {ok, NifSorted} = esdb_graph_nif:nif_topo_sort(NodeIds, Edges),

    %% Both should have same elements (order may differ for nodes at same level)
    ?assertEqual(lists:sort(ErlSorted), lists:sort(NifSorted)),
    %% Both should have correct length
    ?assertEqual(length(ErlSorted), length(NifSorted)).

equivalence_graph_stats() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),

    ErlStats = esdb_graph_nif:erlang_graph_stats(NodeIds, Edges),
    NifStats = esdb_graph_nif:nif_graph_stats(NodeIds, Edges),

    ?assertEqual(maps:get(node_count, ErlStats), maps:get(node_count, NifStats)),
    ?assertEqual(maps:get(edge_count, ErlStats), maps:get(edge_count, NifStats)),
    ?assertEqual(maps:get(root_count, ErlStats), maps:get(root_count, NifStats)),
    ?assertEqual(maps:get(leaf_count, ErlStats), maps:get(leaf_count, NifStats)).

equivalence_has_path() ->
    NodeIds = [Id || {Id, _} <- linear_chain_nodes()],
    Edges = esdb_graph_nif:erlang_build_edges(linear_chain_nodes()),

    TestCases = [
        {<<"evt-a">>, <<"evt-d">>},
        {<<"evt-a">>, <<"evt-b">>},
        {<<"evt-d">>, <<"evt-a">>},
        {<<"evt-b">>, <<"evt-a">>}
    ],

    lists:foreach(fun({From, To}) ->
        ErlResult = esdb_graph_nif:erlang_has_path(NodeIds, Edges, From, To),
        NifResult = esdb_graph_nif:nif_has_path(NodeIds, Edges, From, To),
        ?assertEqual(ErlResult, NifResult)
    end, TestCases).

%%====================================================================
%% Benchmark Tests (Only When NIF Available)
%%====================================================================

benchmark_test_() ->
    case esdb_graph_nif:is_nif_loaded() of
        true ->
            [{"Benchmark topo_sort", fun benchmark_topo_sort/0}];
        false ->
            []
    end.

benchmark_topo_sort() ->
    %% Create a larger dataset for benchmarking
    NumNodes = 500,
    Nodes = [{list_to_binary(io_lib:format("evt-~4..0B", [I])),
              case I of
                  0 -> undefined;
                  _ -> list_to_binary(io_lib:format("evt-~4..0B", [I - 1]))
              end}
             || I <- lists:seq(0, NumNodes - 1)],

    NodeIds = [Id || {Id, _} <- Nodes],
    Edges = esdb_graph_nif:erlang_build_edges(Nodes),

    Iterations = 50,

    %% Benchmark Erlang path
    {TimeErl, _} = timer:tc(fun() ->
        [esdb_graph_nif:erlang_topo_sort(NodeIds, Edges)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Benchmark NIF path
    {TimeNif, _} = timer:tc(fun() ->
        [esdb_graph_nif:nif_topo_sort(NodeIds, Edges)
         || _ <- lists:seq(1, Iterations)]
    end),

    %% Log results
    logger:info("Topo sort benchmark (~p nodes, ~p iterations): Erlang=~pus, NIF=~pus, Speedup=~.2fx",
               [NumNodes, Iterations, TimeErl, TimeNif, TimeErl / max(TimeNif, 1)]),

    %% NIF should be competitive
    ?assert(TimeNif =< TimeErl * 3).

%%====================================================================
%% Helper Functions
%%====================================================================

index_of(Element, List) ->
    index_of(Element, List, 0).

index_of(_, [], _) ->
    -1;
index_of(Element, [Element | _], Index) ->
    Index;
index_of(Element, [_ | Rest], Index) ->
    index_of(Element, Rest, Index + 1).
