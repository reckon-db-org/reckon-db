%% @doc Subscriptions store for reckon-db
%%
%% Manages subscription persistence and retrieval directly via Khepri.
%% This is a facade module that provides direct access to the subscription
%% storage without going through a gen_server, since Khepri/Ra handles
%% concurrency internally.
%%
%% @author rgfaber

-module(reckon_db_subscriptions_store).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([
    put/2,
    get/2,
    delete/2,
    exists/2,
    exists/3,
    list/1,
    key/1,
    key/3,
    update_checkpoint/3,
    find_by_name/2
]).

%%====================================================================
%% Types
%%====================================================================

-type store_id() :: atom().

%%====================================================================
%% API
%%====================================================================

%% @doc Generate a unique key for a subscription
%%
%% The key is a phash2 hash of {type, selector, subscription_name}
-spec key(subscription()) -> binary().
key(#subscription{type = Type, selector = Selector, subscription_name = Name}) ->
    key(Type, Selector, Name).

%% @doc Generate a unique key for a subscription from components
-spec key(subscription_type(), binary() | map(), binary()) -> binary().
key(Type, Selector, SubscriptionName) ->
    integer_to_binary(erlang:phash2({Type, Selector, SubscriptionName})).

%% @doc Store a subscription
-spec put(store_id(), subscription()) -> ok | {error, term()}.
put(StoreId, #subscription{} = Subscription) ->
    Key = key(Subscription),
    Path = ?SUBSCRIPTIONS_PATH ++ [Key],
    %% Add the id to the subscription if not set
    SubscriptionWithId = case Subscription#subscription.id of
        undefined -> Subscription#subscription{id = Key};
        _ -> Subscription
    end,
    case khepri:put(StoreId, Path, SubscriptionWithId) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%% @doc Get a subscription by key
-spec get(store_id(), binary()) -> subscription() | undefined.
get(StoreId, Key) when is_binary(Key) ->
    Path = ?SUBSCRIPTIONS_PATH ++ [Key],
    case khepri:get(StoreId, Path) of
        {ok, Subscription} when is_record(Subscription, subscription) ->
            Subscription;
        {ok, SubscriptionMap} when is_map(SubscriptionMap) ->
            map_to_subscription(SubscriptionMap);
        _ ->
            undefined
    end.

%% @doc Delete a subscription by key
-spec delete(store_id(), binary()) -> ok | {error, term()}.
delete(StoreId, Key) when is_binary(Key) ->
    Path = ?SUBSCRIPTIONS_PATH ++ [Key],
    case khepri:delete(StoreId, Path) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%% @doc Check if a subscription exists by key
-spec exists(store_id(), binary()) -> boolean().
exists(StoreId, Key) when is_binary(Key) ->
    Path = ?SUBSCRIPTIONS_PATH ++ [Key],
    case khepri:exists(StoreId, Path) of
        true -> true;
        false -> false;
        {error, _} -> false
    end.

%% @doc Check if a subscription exists by record
-spec exists(store_id(), subscription_type(), binary()) -> boolean().
exists(StoreId, Type, SubscriptionName) ->
    %% We need to search through subscriptions since we don't have
    %% the selector to generate the key
    case list(StoreId) of
        {ok, Subs} ->
            lists:any(
                fun(#subscription{type = T, subscription_name = N}) ->
                    T =:= Type andalso N =:= SubscriptionName
                end,
                Subs
            );
        _ ->
            false
    end.

%% @doc List all subscriptions in the store
-spec list(store_id()) -> {ok, [subscription()]} | {error, term()}.
list(StoreId) ->
    Path = ?SUBSCRIPTIONS_PATH ++ [?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Results} when is_map(Results) ->
            Subs = [convert_to_subscription(V) || {_, V} <- maps:to_list(Results)],
            {ok, [S || S <- Subs, S =/= undefined]};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @doc Find a subscription by name
%%
%% Searches all subscriptions for one matching the given name.
%% Returns the subscription key and record if found.
-spec find_by_name(store_id(), binary()) -> {ok, binary(), subscription()} | {error, not_found}.
find_by_name(StoreId, SubscriptionName) ->
    case list(StoreId) of
        {ok, Subscriptions} ->
            find_matching_subscription(Subscriptions, SubscriptionName);
        {error, _} ->
            {error, not_found}
    end.

%% @doc Update the checkpoint for a subscription
%%
%% Parameters:
%%   StoreId - The store identifier
%%   Key     - The subscription key
%%   Position - The new checkpoint position
-spec update_checkpoint(store_id(), binary(), non_neg_integer()) -> ok | {error, term()}.
update_checkpoint(StoreId, Key, Position) ->
    case ?MODULE:get(StoreId, Key) of
        undefined ->
            {error, not_found};
        Subscription ->
            UpdatedSubscription = Subscription#subscription{checkpoint = Position},
            ?MODULE:put(StoreId, UpdatedSubscription)
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Find a subscription by name in a list
-spec find_matching_subscription([subscription()], binary()) ->
    {ok, binary(), subscription()} | {error, not_found}.
find_matching_subscription([], _Name) ->
    {error, not_found};
find_matching_subscription([#subscription{subscription_name = Name} = Sub | _], Name) ->
    {ok, key(Sub), Sub};
find_matching_subscription([_ | Rest], Name) ->
    find_matching_subscription(Rest, Name).

%% @private Convert value to subscription record
-spec convert_to_subscription(term()) -> subscription() | undefined.
convert_to_subscription(V) when is_record(V, subscription) ->
    V;
convert_to_subscription(V) when is_map(V) ->
    map_to_subscription(V);
convert_to_subscription(_) ->
    undefined.

%% @private Convert map to subscription record
-spec map_to_subscription(map()) -> subscription().
map_to_subscription(Map) ->
    #subscription{
        id = maps:get(id, Map, undefined),
        type = maps:get(type, Map, stream),
        selector = maps:get(selector, Map, <<>>),
        subscription_name = maps:get(subscription_name, Map, <<>>),
        subscriber_pid = maps:get(subscriber_pid, Map, undefined),
        created_at = maps:get(created_at, Map, 0),
        pool_size = maps:get(pool_size, Map, 1),
        checkpoint = maps:get(checkpoint, Map, undefined),
        options = maps:get(options, Map, #{})
    }.
