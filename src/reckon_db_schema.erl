%% @doc Schema registry and upcasting for reckon-db
%%
%% Provides schema versioning and automatic event transformation:
%% - Register schemas for event types with version numbers
%% - Define upcast functions to transform old versions to new
%% - Auto-upcast events when reading from streams
%%
%% Event schema evolution strategies:
%% - Weak schema: No validation, just track versions
%% - Strong schema: Validate against JSON Schema or custom validator
%% - Tolerant reader: Accept old versions, upcast on demand
%%
%% Usage:
%% ```
%% %% Register schema
%% reckon_db_schema:register(my_store, <<"OrderPlaced">>, #{
%%     version => 2,
%%     upcast_from => #{
%%         1 => fun(V1Data) -> V1Data#{new_field => default} end
%%     }
%% }).
%%
%% %% Read with upcasting
%% {ok, Events} = reckon_db_streams:read(my_store, stream, 0, 100, forward),
%% UpcastedEvents = reckon_db_schema:upcast(my_store, Events).
%% '''
%%
%% @author rgfaber

-module(reckon_db_schema).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([
    register/3,
    unregister/2,
    get/2,
    list/1,
    get_version/2,
    upcast/2,
    upcast_event/2,
    validate/2
]).

%%====================================================================
%% Types
%%====================================================================

-type version() :: pos_integer().
-type upcast_fun() :: fun((map()) -> map()).
-type validator_fun() :: fun((map()) -> ok | {error, term()}).

-type schema() :: #{
    event_type := binary(),
    version := version(),
    upcast_from => #{version() => upcast_fun()},
    validator => validator_fun(),
    description => binary()
}.

-type schema_info() :: #{
    event_type := binary(),
    version := version(),
    registered_at := integer()
}.

-export_type([schema/0, schema_info/0, version/0, upcast_fun/0]).

%%====================================================================
%% Khepri Paths
%%====================================================================

-define(SCHEMAS_PATH, [schemas]).

%%====================================================================
%% API
%%====================================================================

%% @doc Register a schema for an event type.
%%
%% Options:
%% - version: Schema version (required, positive integer)
%% - upcast_from: Map of OldVersion to UpcastFun for transformations
%% - validator: Fun to validate event data (returns ok or error tuple)
%% - description: Human-readable description
-spec register(atom(), binary(), schema()) -> ok | {error, term()}.
register(StoreId, EventType, Schema) when is_map(Schema) ->
    Version = maps:get(version, Schema),

    %% Validate schema
    case is_integer(Version) andalso Version > 0 of
        true ->
            SchemaRecord = Schema#{
                event_type => EventType,
                registered_at => erlang:system_time(millisecond)
            },
            Path = ?SCHEMAS_PATH ++ [StoreId, EventType],
            case khepri:put(StoreId, Path, SchemaRecord) of
                ok ->
                    emit_telemetry(StoreId, EventType, registered, Version),
                    ok;
                {error, _} = Error ->
                    Error
            end;
        false ->
            {error, {invalid_version, Version}}
    end.

%% @doc Unregister a schema.
-spec unregister(atom(), binary()) -> ok | {error, term()}.
unregister(StoreId, EventType) ->
    Path = ?SCHEMAS_PATH ++ [StoreId, EventType],
    case khepri:delete(StoreId, Path) of
        ok ->
            emit_telemetry(StoreId, EventType, unregistered, 0),
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Get a schema by event type.
-spec get(atom(), binary()) -> {ok, schema()} | {error, not_found}.
get(StoreId, EventType) ->
    Path = ?SCHEMAS_PATH ++ [StoreId, EventType],
    case khepri:get(StoreId, Path) of
        {ok, Schema} -> {ok, Schema};
        {error, {khepri, node_not_found, _}} -> {error, not_found};
        {error, _} = Error -> Error
    end.

%% @doc List all registered schemas.
-spec list(atom()) -> {ok, [schema_info()]} | {error, term()}.
list(StoreId) ->
    Path = ?SCHEMAS_PATH ++ [StoreId, ?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Schemas} ->
            Infos = [to_info(S) || {_, S} <- maps:to_list(Schemas)],
            {ok, Infos};
        {error, _} = Error ->
            Error
    end.

%% @doc Get current version for an event type.
-spec get_version(atom(), binary()) -> {ok, version()} | {error, not_found}.
get_version(StoreId, EventType) ->
    case get(StoreId, EventType) of
        {ok, #{version := Version}} -> {ok, Version};
        Error -> Error
    end.

%% @doc Upcast a list of events to their current schema versions.
%%
%% Events without registered schemas are returned unchanged.
%% Events already at current version are returned unchanged.
-spec upcast(atom(), [event()]) -> [event()].
upcast(StoreId, Events) ->
    [upcast_event(StoreId, E) || E <- Events].

%% @doc Upcast a single event to current schema version.
-spec upcast_event(atom(), event()) -> event().
upcast_event(StoreId, Event) ->
    EventType = Event#event.event_type,
    case get(StoreId, EventType) of
        {ok, Schema} ->
            CurrentVersion = maps:get(version, Schema),
            EventVersion = get_event_version(Event),

            case EventVersion < CurrentVersion of
                true ->
                    upcast_to_version(Event, Schema, EventVersion, CurrentVersion, StoreId);
                false ->
                    Event
            end;
        {error, not_found} ->
            %% No schema registered, return unchanged
            Event
    end.

%% @doc Validate an event against its registered schema.
-spec validate(atom(), event()) -> ok | {error, term()}.
validate(StoreId, Event) ->
    EventType = Event#event.event_type,
    case get(StoreId, EventType) of
        {ok, Schema} ->
            case maps:get(validator, Schema, undefined) of
                undefined ->
                    %% No validator, always valid
                    ok;
                ValidatorFun when is_function(ValidatorFun, 1) ->
                    ValidatorFun(Event#event.data)
            end;
        {error, not_found} ->
            %% No schema, implicitly valid
            ok
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get event version from metadata
-spec get_event_version(event()) -> version().
get_event_version(Event) ->
    maps:get(schema_version, Event#event.metadata, 1).

%% @private Upcast event through version chain
-spec upcast_to_version(event(), schema(), version(), version(), atom()) -> event().
upcast_to_version(Event, Schema, FromVersion, ToVersion, StoreId) when FromVersion < ToVersion ->
    UpcastFuns = maps:get(upcast_from, Schema, #{}),

    case maps:get(FromVersion, UpcastFuns, undefined) of
        undefined ->
            %% No upcast function for this version, skip to next
            upcast_to_version(Event, Schema, FromVersion + 1, ToVersion, StoreId);
        UpcastFun when is_function(UpcastFun, 1) ->
            StartTime = erlang:monotonic_time(),

            %% Apply upcast function to event data
            NewData = UpcastFun(Event#event.data),

            %% Update metadata with new version
            NewMetadata = maps:put(schema_version, FromVersion + 1, Event#event.metadata),
            NewEvent = Event#event{data = NewData, metadata = NewMetadata},

            Duration = erlang:monotonic_time() - StartTime,
            emit_upcast_telemetry(StoreId, Event#event.event_type,
                                  FromVersion, FromVersion + 1, Duration),

            %% Continue upcasting if more versions to go
            upcast_to_version(NewEvent, Schema, FromVersion + 1, ToVersion, StoreId)
    end;
upcast_to_version(Event, _Schema, _FromVersion, _ToVersion, _StoreId) ->
    Event.

%% @private Convert schema to info
-spec to_info(schema()) -> schema_info().
to_info(Schema) ->
    #{
        event_type => maps:get(event_type, Schema),
        version => maps:get(version, Schema),
        registered_at => maps:get(registered_at, Schema, 0)
    }.

%% @private Emit telemetry for schema operations
-spec emit_telemetry(atom(), binary(), atom(), version()) -> ok.
emit_telemetry(StoreId, EventType, Operation, Version) ->
    telemetry:execute(
        [reckon_db, schema, Operation],
        #{version => Version},
        #{store_id => StoreId, event_type => EventType}
    ),
    ok.

%% @private Emit telemetry for upcast operations
-spec emit_upcast_telemetry(atom(), binary(), version(), version(), integer()) -> ok.
emit_upcast_telemetry(StoreId, EventType, FromVersion, ToVersion, Duration) ->
    telemetry:execute(
        [reckon_db, schema, upcasted],
        #{duration => Duration},
        #{store_id => StoreId, event_type => EventType,
          from_version => FromVersion, to_version => ToVersion}
    ),
    ok.
