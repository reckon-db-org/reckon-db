%% @doc Emitter group management for reckon-db
%%
%% Uses pg (process groups) for managing emitter workers.
%% Emitters are responsible for broadcasting events to subscribers.
%%
%% This module provides:
%% - Process group management for emitter workers
%% - Random emitter selection for load distribution
%% - Topic generation for pub/sub
%% - Emitter name generation for registration
%%
%% @author rgfaber

-module(reckon_db_emitter_group).

-include("reckon_db.hrl").

-export([
    join/3,
    leave/3,
    members/2,
    broadcast/3,
    group_key/2,
    topic/2,
    emitter_name/2,
    emitter_name/3,
    persist_emitters/3,
    retrieve_emitters/2
]).

%%====================================================================
%% Types
%%====================================================================

-type store_id() :: atom().
-type subscription_id() :: binary().

%%====================================================================
%% API
%%====================================================================

%% @doc Join one or more processes to the emitter group
-spec join(store_id(), subscription_id(), pid() | [pid()]) -> ok.
join(StoreId, SubscriptionId, PidOrPids) when is_atom(StoreId) ->
    Group = group_key(StoreId, SubscriptionId),
    ok = pg:join(?RECKON_DB_PG_SCOPE, Group, PidOrPids),
    ok.

%% @doc Remove one or more processes from the emitter group
-spec leave(store_id(), subscription_id(), pid() | [pid()]) -> ok.
leave(StoreId, SubscriptionId, PidOrPids) when is_atom(StoreId) ->
    Group = group_key(StoreId, SubscriptionId),
    ok = pg:leave(?RECKON_DB_PG_SCOPE, Group, PidOrPids),
    ok.

%% @doc Get all member processes in the emitter group
-spec members(store_id(), subscription_id()) -> [pid()].
members(StoreId, SubscriptionId) when is_atom(StoreId) ->
    Group = group_key(StoreId, SubscriptionId),
    pg:get_members(?RECKON_DB_PG_SCOPE, Group).

%% @doc Broadcast an event to a random emitter in the group
%%
%% If the selected emitter is on the local node, sends a forward_to_local
%% message for optimized local delivery. Otherwise sends a broadcast message.
-spec broadcast(store_id(), subscription_id(), event()) -> ok | {error, no_emitters}.
broadcast(StoreId, SubscriptionId, Event) when is_atom(StoreId) ->
    Topic = topic(StoreId, SubscriptionId),
    Members = members(StoreId, SubscriptionId),
    case Members of
        [] ->
            logger:warning("No emitters for [~p]~n", [Topic]),
            {error, no_emitters};
        _ ->
            EmitterPid = random_emitter(Members),
            Message = case node(EmitterPid) =:= node() of
                true -> forward_to_local_msg(Topic, Event);
                false -> broadcast_msg(Topic, Event)
            end,
            EmitterPid ! Message,
            ok
    end.

%% @doc Generate the group key for a subscription's emitters
-spec group_key(store_id(), subscription_id()) -> {atom(), subscription_id(), emitters}.
group_key(StoreId, SubscriptionId) ->
    {StoreId, SubscriptionId, emitters}.

%% @doc Generate the topic name for a subscription
%%
%% Special case: the binary &lt;&lt;"$all"&gt;&gt; creates a topic for all events
-spec topic(store_id(), subscription_id()) -> binary().
topic(StoreId, <<"$all">>) ->
    iolist_to_binary(io_lib:format("~s:$all", [StoreId]));
topic(StoreId, SubscriptionId) ->
    iolist_to_binary(io_lib:format("~s:~s", [StoreId, SubscriptionId])).

%% @doc Generate the base emitter name for a subscription
-spec emitter_name(store_id(), subscription_id()) -> atom().
emitter_name(StoreId, SubscriptionId) ->
    list_to_atom(lists:flatten(
        io_lib:format("~s_~s_emitter", [StoreId, SubscriptionId]))).

%% @doc Generate a numbered emitter name for a subscription
-spec emitter_name(store_id(), subscription_id(), pos_integer()) -> atom().
emitter_name(StoreId, SubscriptionId, Number) ->
    list_to_atom(lists:flatten(
        io_lib:format("~s_~s_emitter_~p", [StoreId, SubscriptionId, Number]))).

%% @doc Persist emitter names to persistent_term for fast retrieval
-spec persist_emitters(store_id(), subscription_id(), pos_integer()) -> [atom()].
persist_emitters(StoreId, SubscriptionId, PoolSize) ->
    %% Generate list of emitter names
    EmitterList = [emitter_name(StoreId, SubscriptionId, N) || N <- lists:seq(2, PoolSize)],
    Emitters = [emitter_name(StoreId, SubscriptionId) | EmitterList],
    Key = group_key(StoreId, SubscriptionId),
    ok = persistent_term:put(Key, list_to_tuple(Emitters)),
    Emitters.

%% @doc Retrieve previously persisted emitter names
-spec retrieve_emitters(store_id(), subscription_id()) -> [atom()].
retrieve_emitters(StoreId, SubscriptionId) ->
    Key = group_key(StoreId, SubscriptionId),
    case persistent_term:get(Key, undefined) of
        undefined -> [];
        EmitterTuple -> tuple_to_list(EmitterTuple)
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Select a random emitter from the list
-spec random_emitter([pid()]) -> pid().
random_emitter(Emitters) ->
    EmittersTuple = list_to_tuple(Emitters),
    Size = tuple_size(EmittersTuple),
    Random = rand:uniform(Size),
    erlang:element(Random, EmittersTuple).

%% @private Create a forward_to_local message for local emitters
-spec forward_to_local_msg(binary(), event()) -> {forward_to_local, binary(), event()}.
forward_to_local_msg(Topic, Event) ->
    {forward_to_local, Topic, Event}.

%% @private Create a broadcast message for remote emitters
-spec broadcast_msg(binary(), event()) -> {broadcast, binary(), event()}.
broadcast_msg(Topic, Event) ->
    {broadcast, Topic, Event}.
