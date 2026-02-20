%% @doc Process naming utilities for reckon-db
%%
%% Provides consistent naming conventions for all processes and
%% registered names throughout the system.
%%
%% @author rgfaber

-module(reckon_db_naming).

%% API
-export([
    %% Supervisor names
    system_sup_name/1,
    core_sup_name/1,
    persistence_sup_name/1,
    notification_sup_name/1,
    cluster_sup_name/1,
    gateway_sup_name/1,
    streams_sup_name/1,
    emitter_sup_name/1,
    leader_sup_name/1,

    %% Worker names
    store_name/1,
    store_worker_name/1,
    store_mgr_name/1,
    subscriptions_store_name/1,
    snapshots_store_name/1,
    persistence_worker_name/1,
    leader_name/1,
    leader_tracker_name/1,
    discovery_name/1,
    coordinator_name/1,
    node_monitor_name/1,
    gateway_worker_name/1,
    health_monitor_name/1,

    %% Pool names
    writer_pool_name/1,
    reader_pool_name/1,
    emitter_pool_name/2,

    %% Group names
    pg_group_name/2,
    tracker_group_key/2,
    emitter_group_key/2
]).

%%====================================================================
%% Supervisor Names
%%====================================================================

%% @doc System supervisor name for a store
-spec system_sup_name(atom()) -> atom().
system_sup_name(StoreId) ->
    make_name("reckon_db_system_", StoreId).

%% @doc Core supervisor name for a store
-spec core_sup_name(atom()) -> atom().
core_sup_name(StoreId) ->
    make_name("reckon_db_core_", StoreId).

%% @doc Persistence supervisor name for a store
-spec persistence_sup_name(atom()) -> atom().
persistence_sup_name(StoreId) ->
    make_name("reckon_db_persistence_", StoreId).

%% @doc Notification supervisor name for a store
-spec notification_sup_name(atom()) -> atom().
notification_sup_name(StoreId) ->
    make_name("reckon_db_notification_", StoreId).

%% @doc Cluster supervisor name for a store
-spec cluster_sup_name(atom()) -> atom().
cluster_sup_name(StoreId) ->
    make_name("reckon_db_cluster_", StoreId).

%% @doc Gateway supervisor name for a store
-spec gateway_sup_name(atom()) -> atom().
gateway_sup_name(StoreId) ->
    make_name("reckon_db_gateway_", StoreId).

%% @doc Streams supervisor name for a store
-spec streams_sup_name(atom()) -> atom().
streams_sup_name(StoreId) ->
    make_name("reckon_db_streams_", StoreId).

%% @doc Emitter supervisor name for a store
-spec emitter_sup_name(atom()) -> atom().
emitter_sup_name(StoreId) ->
    make_name("reckon_db_emitter_", StoreId).

%% @doc Leader supervisor name for a store
-spec leader_sup_name(atom()) -> atom().
leader_sup_name(StoreId) ->
    make_name("reckon_db_leader_", StoreId).

%%====================================================================
%% Worker Names
%%====================================================================

%% @doc Khepri store name (this is the atom used for khepri operations)
-spec store_name(atom()) -> atom().
store_name(StoreId) ->
    StoreId.

%% @doc Store worker name (the gen_server that starts Khepri)
%% NOTE: This is different from store_name/1 (Khepri operations) and
%% store_mgr_name/1 (store lifecycle coordination).
-spec store_worker_name(atom()) -> atom().
store_worker_name(StoreId) ->
    make_name("reckon_db_store_", StoreId).

%% @doc Store manager worker name
-spec store_mgr_name(atom()) -> atom().
store_mgr_name(StoreId) ->
    make_name("reckon_db_store_mgr_", StoreId).

%% @doc Subscriptions store worker name
-spec subscriptions_store_name(atom()) -> atom().
subscriptions_store_name(StoreId) ->
    make_name("reckon_db_subscriptions_", StoreId).

%% @doc Snapshots store worker name
-spec snapshots_store_name(atom()) -> atom().
snapshots_store_name(StoreId) ->
    make_name("reckon_db_snapshots_", StoreId).

%% @doc Persistence worker name
-spec persistence_worker_name(atom()) -> atom().
persistence_worker_name(StoreId) ->
    make_name("reckon_db_persistence_worker_", StoreId).

%% @doc Leader worker name
-spec leader_name(atom()) -> atom().
leader_name(StoreId) ->
    make_name("reckon_db_leader_worker_", StoreId).

%% @doc Leader tracker worker name
-spec leader_tracker_name(atom()) -> atom().
leader_tracker_name(StoreId) ->
    make_name("reckon_db_leader_tracker_", StoreId).

%% @doc Discovery worker name
-spec discovery_name(atom()) -> atom().
discovery_name(StoreId) ->
    make_name("reckon_db_discovery_", StoreId).

%% @doc Store coordinator worker name
-spec coordinator_name(atom()) -> atom().
coordinator_name(StoreId) ->
    make_name("reckon_db_coordinator_", StoreId).

%% @doc Node monitor worker name
-spec node_monitor_name(atom()) -> atom().
node_monitor_name(StoreId) ->
    make_name("reckon_db_node_monitor_", StoreId).

%% @doc Gateway worker name for a store
-spec gateway_worker_name(atom()) -> atom().
gateway_worker_name(StoreId) ->
    make_name("reckon_db_gateway_worker_", StoreId).

%% @doc Subscription health monitor name for a store
-spec health_monitor_name(atom()) -> atom().
health_monitor_name(StoreId) ->
    make_name("reckon_db_health_monitor_", StoreId).

%%====================================================================
%% Pool Names
%%====================================================================

%% @doc Writer pool name for a store
-spec writer_pool_name(atom()) -> atom().
writer_pool_name(StoreId) ->
    make_name("reckon_db_writer_pool_", StoreId).

%% @doc Reader pool name for a store
-spec reader_pool_name(atom()) -> atom().
reader_pool_name(StoreId) ->
    make_name("reckon_db_reader_pool_", StoreId).

%% @doc Emitter pool name for a subscription
-spec emitter_pool_name(atom(), binary()) -> atom().
emitter_pool_name(StoreId, SubscriptionId) ->
    Name = io_lib:format("reckon_db_emitter_pool_~s_~s", [StoreId, SubscriptionId]),
    list_to_atom(lists:flatten(Name)).

%%====================================================================
%% Group Names
%%====================================================================

%% @doc Process group name for a store feature
-spec pg_group_name(atom(), atom()) -> term().
pg_group_name(StoreId, Feature) ->
    {StoreId, Feature}.

%% @doc Tracker group key (for subscription tracking via pg)
-spec tracker_group_key(atom(), atom()) -> integer().
tracker_group_key(StoreId, Feature) ->
    erlang:phash2({StoreId, Feature, trackers}).

%% @doc Emitter group key (for event distribution via pg)
-spec emitter_group_key(atom(), binary()) -> term().
emitter_group_key(StoreId, SubscriptionId) ->
    {StoreId, SubscriptionId, emitters}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec make_name(string(), atom()) -> atom().
make_name(Prefix, StoreId) ->
    list_to_atom(Prefix ++ atom_to_list(StoreId)).
