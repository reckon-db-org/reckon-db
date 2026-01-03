%% @doc Configuration management for reckon-db
%%
%% Handles reading and validating store configurations from the
%% application environment.
%%
%% @author rgfaber

-module(reckon_db_config).

-include("reckon_db.hrl").

%% API
-export([
    get_store_config/1,
    get_all_store_configs/0,
    get_env/2,
    get_env/3
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get configuration for a specific store
-spec get_store_config(atom()) -> {ok, store_config()} | {error, not_found}.
get_store_config(StoreId) ->
    case get_env(stores, []) of
        [] ->
            {error, not_found};
        Stores ->
            case proplists:get_value(StoreId, Stores) of
                undefined ->
                    {error, not_found};
                StoreOpts when is_list(StoreOpts) ->
                    {ok, parse_store_config(StoreId, StoreOpts)}
            end
    end.

%% @doc Get all configured store configurations
-spec get_all_store_configs() -> [store_config()].
get_all_store_configs() ->
    case get_env(stores, []) of
        [] ->
            [];
        Stores ->
            [parse_store_config(StoreId, StoreOpts) || {StoreId, StoreOpts} <- Stores]
    end.

%% @doc Get an application environment value
-spec get_env(atom(), term()) -> term().
get_env(Key, Default) ->
    application:get_env(reckon_db, Key, Default).

%% @doc Get an application environment value with a specific app
-spec get_env(atom(), atom(), term()) -> term().
get_env(App, Key, Default) ->
    application:get_env(App, Key, Default).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec parse_store_config(atom(), proplists:proplist()) -> store_config().
parse_store_config(StoreId, Opts) ->
    DataDir = proplists:get_value(data_dir, Opts, default_data_dir(StoreId)),
    Mode = proplists:get_value(mode, Opts, single),
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    WriterPoolSize = proplists:get_value(
        writer_pool_size, Opts,
        get_env(writer_pool_size, ?DEFAULT_POOL_SIZE)
    ),
    ReaderPoolSize = proplists:get_value(
        reader_pool_size, Opts,
        get_env(reader_pool_size, ?DEFAULT_POOL_SIZE)
    ),
    GatewayPoolSize = proplists:get_value(
        gateway_pool_size, Opts,
        get_env(gateway_pool_size, ?DEFAULT_GATEWAY_POOL_SIZE)
    ),

    #store_config{
        store_id = StoreId,
        data_dir = DataDir,
        mode = validate_mode(Mode),
        timeout = Timeout,
        writer_pool_size = WriterPoolSize,
        reader_pool_size = ReaderPoolSize,
        gateway_pool_size = GatewayPoolSize,
        options = maps:from_list(
            proplists:delete(data_dir,
            proplists:delete(mode,
            proplists:delete(timeout,
            proplists:delete(writer_pool_size,
            proplists:delete(reader_pool_size,
            proplists:delete(gateway_pool_size, Opts))))))
        )
    }.

%% @private
-spec default_data_dir(atom()) -> string().
default_data_dir(StoreId) ->
    filename:join(["/var/lib/reckon_db", atom_to_list(StoreId)]).

%% @private
-spec validate_mode(atom()) -> single | cluster.
validate_mode(single) -> single;
validate_mode(cluster) -> cluster;
validate_mode(Other) ->
    logger:warning("Invalid mode ~p, defaulting to single", [Other]),
    single.
