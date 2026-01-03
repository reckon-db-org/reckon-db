%% @doc Tracker group management for reckon-db
%%
%% Uses pg (process groups) for managing tracker processes.
%% Trackers receive notifications about subscription lifecycle events.
%%
%% This module provides:
%% - Process group management for tracker processes
%% - Notification broadcasting for created/deleted/updated events
%%
%% Features that can be tracked:
%% - subscriptions: Subscription lifecycle
%% - streams: Stream lifecycle
%% - snapshots: Snapshot lifecycle
%%
%% @author rgfaber

-module(reckon_db_tracker_group).

-include("reckon_db.hrl").

-export([
    join/3,
    leave/3,
    members/2,
    group_key/2,
    notify_created/3,
    notify_deleted/3,
    notify_updated/3
]).

%%====================================================================
%% Types
%%====================================================================

-type store_id() :: atom().
-type feature() :: subscriptions | streams | snapshots | atom().

%%====================================================================
%% API
%%====================================================================

%% @doc Generate the group key for a feature's trackers
-spec group_key(store_id(), feature()) -> integer().
group_key(StoreId, Feature) ->
    erlang:phash2({StoreId, Feature, trackers}).

%% @doc Join one or more processes to the tracker group for a feature
-spec join(store_id(), feature(), pid() | [pid()]) -> ok.
join(StoreId, Feature, PidOrPids) ->
    Group = group_key(StoreId, Feature),
    ok = pg:join(?RECKON_DB_PG_SCOPE, Group, PidOrPids),
    ok.

%% @doc Remove one or more processes from the tracker group
-spec leave(store_id(), feature(), pid() | [pid()]) -> ok.
leave(StoreId, Feature, PidOrPids) ->
    Group = group_key(StoreId, Feature),
    ok = pg:leave(?RECKON_DB_PG_SCOPE, Group, PidOrPids),
    ok.

%% @doc Get all member processes tracking a feature
-spec members(store_id(), feature()) -> [pid()].
members(StoreId, Feature) ->
    Group = group_key(StoreId, Feature),
    pg:get_members(?RECKON_DB_PG_SCOPE, Group).

%% @doc Notify all trackers that a feature instance was created
-spec notify_created(store_id(), feature(), term()) -> ok.
notify_created(StoreId, Feature, Data) ->
    Msg = created(Feature, Data),
    Pids = members(StoreId, Feature),
    lists:foreach(fun(Pid) -> Pid ! Msg end, Pids),
    ok.

%% @doc Notify all trackers that a feature instance was deleted
-spec notify_deleted(store_id(), feature(), term()) -> ok.
notify_deleted(StoreId, Feature, Data) ->
    Msg = deleted(Feature, Data),
    Pids = members(StoreId, Feature),
    lists:foreach(fun(Pid) -> Pid ! Msg end, Pids),
    ok.

%% @doc Notify all trackers that a feature instance was updated
-spec notify_updated(store_id(), feature(), term()) -> ok.
notify_updated(StoreId, Feature, Data) ->
    Msg = updated(Feature, Data),
    Pids = members(StoreId, Feature),
    lists:foreach(fun(Pid) -> Pid ! Msg end, Pids),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Create a feature_created message
-spec created(feature(), term()) -> {feature_created, feature(), term()}.
created(Feature, Data) ->
    {feature_created, Feature, Data}.

%% @private Create a feature_deleted message
-spec deleted(feature(), term()) -> {feature_deleted, feature(), term()}.
deleted(Feature, Data) ->
    {feature_deleted, Feature, Data}.

%% @private Create a feature_updated message
-spec updated(feature(), term()) -> {feature_updated, feature(), term()}.
updated(Feature, Data) ->
    {feature_updated, Feature, Data}.
