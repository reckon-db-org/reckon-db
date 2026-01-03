%% @doc Optimized event aggregation operations for reckon-db.
%%
%% This module provides high-performance event aggregation implementations:
%%
%% <ul>
%% <li><b>aggregate_events</b>: Bulk fold with tagged value semantics</li>
%% <li><b>sum_field</b>: Vectorized sum accumulation for numeric fields</li>
%% <li><b>count_where</b>: Count events matching a condition</li>
%% <li><b>merge_tagged_batch</b>: Batch map merge with tagged values</li>
%% </ul>
%%
%% The mode is automatically detected at startup based on whether the NIF
%% library is available. Community edition users (hex.pm) will always use
%% the Erlang fallbacks, which provide identical functionality.
%%
%% == Tagged Value Semantics ==
%%
%% Tagged values control how fields are aggregated:
%% <ul>
%% <li>`{sum, N}': Add N to the current value (numeric accumulation)</li>
%% <li>`{overwrite, V}': Replace current value with V</li>
%% <li>Plain value: Replace current value (default behavior)</li>
%% </ul>
%%
%% == Usage ==
%%
%% ```
%% %% Aggregate events with tagged value semantics
%% Events = [#{amount => {sum, 100}}, #{amount => {sum, 50}}],
%% Result = esdb_aggregate_nif:aggregate_events(Events, #{}, true),
%% #{amount := 150} = Result.
%%
%% %% Sum a specific field across all events
%% Total = esdb_aggregate_nif:sum_field(Events, amount).
%%
%% %% Check which mode is active
%% nif = esdb_aggregate_nif:implementation().  %% Enterprise
%% erlang = esdb_aggregate_nif:implementation(). %% Community
%% '''
%%
%% @author rgfaber

-module(esdb_aggregate_nif).

%% Public API
-export([
    aggregate_events/3,
    sum_field/2,
    count_where/3,
    merge_tagged_batch/2,
    finalize/1,
    aggregation_stats/1
]).

%% Introspection
-export([
    is_nif_loaded/0,
    implementation/0
]).

%% For testing - expose both implementations
-export([
    nif_aggregate_events/3,
    nif_sum_field/2,
    nif_count_where/3,
    nif_merge_tagged_batch/2,
    nif_finalize/1,
    nif_aggregation_stats/1,
    erlang_aggregate_events/3,
    erlang_sum_field/2,
    erlang_count_where/3,
    erlang_merge_tagged_batch/2,
    erlang_finalize/1,
    erlang_aggregation_stats/1
]).

%% NIF loading
-on_load(init/0).

%% Persistent term key for NIF status
-define(NIF_LOADED_KEY, esdb_aggregate_nif_loaded).

%%====================================================================
%% NIF Loading
%%====================================================================

%% @private
%% Try to load NIF from multiple locations:
%% 1. reckon_nifs priv/ (enterprise addon package)
%% 2. reckon_db priv/ (standalone enterprise build)
-spec init() -> ok.
init() ->
    NifName = "esdb_aggregate_nif",
    Paths = nif_search_paths(NifName),
    case try_load_nif(Paths) of
        ok ->
            persistent_term:put(?NIF_LOADED_KEY, true),
            logger:info("[esdb_aggregate_nif] NIF loaded - Enterprise mode"),
            ok;
        {error, Reason} ->
            persistent_term:put(?NIF_LOADED_KEY, false),
            logger:info("[esdb_aggregate_nif] NIF not available (~p), using pure Erlang - Community mode",
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
%% Public API
%%====================================================================

%% @doc Aggregate a list of events with tagged value semantics.
%%
%% Processes events in order, applying tagged value rules:
%% - `{sum, N}' adds N to the current value
%% - `{overwrite, V}' replaces the current value with V
%% - Plain values replace the current value
%%
%% If Finalize is true, the result will have tagged values unwrapped.
-spec aggregate_events(Events :: [map()], InitialState :: map(), Finalize :: boolean()) -> map().
aggregate_events(Events, InitialState, Finalize) ->
    case is_nif_loaded() of
        true -> nif_aggregate_events(Events, InitialState, Finalize);
        false -> erlang_aggregate_events(Events, InitialState, Finalize)
    end.

%% @doc Sum a specific field across all events.
%%
%% Efficiently accumulates numeric values from a named field.
%% Handles tagged values ({sum, N}) and plain numeric values.
-spec sum_field(Events :: [map()], Field :: atom() | binary()) -> number().
sum_field(Events, Field) ->
    case is_nif_loaded() of
        true -> nif_sum_field(Events, Field);
        false -> erlang_sum_field(Events, Field)
    end.

%% @doc Count events matching a simple field condition.
%%
%% Returns the number of events where the specified field equals the expected value.
-spec count_where(Events :: [map()], Field :: atom() | binary(), Value :: term()) -> non_neg_integer().
count_where(Events, Field, Value) ->
    case is_nif_loaded() of
        true -> nif_count_where(Events, Field, Value);
        false -> erlang_count_where(Events, Field, Value)
    end.

%% @doc Merge a batch of key-value pairs into a state map.
%%
%% Applies tagged value semantics to each pair.
-spec merge_tagged_batch(Pairs :: [{term(), term()}], State :: map()) -> map().
merge_tagged_batch(Pairs, State) ->
    case is_nif_loaded() of
        true -> nif_merge_tagged_batch(Pairs, State);
        false -> erlang_merge_tagged_batch(Pairs, State)
    end.

%% @doc Finalize a tagged map by unwrapping all tagged values.
%%
%% Converts {sum, N} to N and {overwrite, V} to V.
-spec finalize(TaggedMap :: map()) -> map().
finalize(TaggedMap) ->
    case is_nif_loaded() of
        true -> nif_finalize(TaggedMap);
        false -> erlang_finalize(TaggedMap)
    end.

%% @doc Get statistics about event aggregation.
%%
%% Returns counts and basic metrics about the event list.
-spec aggregation_stats(Events :: [map()]) -> map().
aggregation_stats(Events) ->
    case is_nif_loaded() of
        true -> nif_aggregation_stats(Events);
        false -> erlang_aggregation_stats(Events)
    end.

%%====================================================================
%% NIF Stubs (replaced when NIF loads)
%%====================================================================

%% @private
-spec nif_aggregate_events([map()], map(), boolean()) -> map().
nif_aggregate_events(_Events, _InitialState, _Finalize) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_sum_field([map()], atom() | binary()) -> number().
nif_sum_field(_Events, _Field) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_count_where([map()], atom() | binary(), term()) -> non_neg_integer().
nif_count_where(_Events, _Field, _Value) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_merge_tagged_batch([{term(), term()}], map()) -> map().
nif_merge_tagged_batch(_Pairs, _State) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_finalize(map()) -> map().
nif_finalize(_TaggedMap) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_aggregation_stats([map()]) -> map().
nif_aggregation_stats(_Events) ->
    erlang:nif_error(nif_not_loaded).

%%====================================================================
%% Pure Erlang Implementations (Always Available)
%%====================================================================

%% @private
-spec erlang_aggregate_events([map()], map(), boolean()) -> map().
erlang_aggregate_events(Events, InitialState, Finalize) ->
    Result = lists:foldl(fun apply_event/2, InitialState, Events),
    case Finalize of
        true -> erlang_finalize(Result);
        false -> Result
    end.

%% @private
-spec erlang_sum_field([map()], atom() | binary()) -> number().
erlang_sum_field(Events, Field) ->
    lists:foldl(fun(Event, Acc) ->
        Data = get_event_data(Event),
        case maps:get(Field, Data, undefined) of
            {sum, N} when is_number(N) -> Acc + N;
            N when is_number(N) -> Acc + N;
            _ -> Acc
        end
    end, 0, Events).

%% @private
-spec erlang_count_where([map()], atom() | binary(), term()) -> non_neg_integer().
erlang_count_where(Events, Field, Expected) ->
    lists:foldl(fun(Event, Acc) ->
        Data = get_event_data(Event),
        case maps:get(Field, Data, undefined) of
            Expected -> Acc + 1;
            _ -> Acc
        end
    end, 0, Events).

%% @private
-spec erlang_merge_tagged_batch([{term(), term()}], map()) -> map().
erlang_merge_tagged_batch(Pairs, State) ->
    lists:foldl(fun({Key, Value}, Acc) ->
        apply_field(Key, Value, Acc)
    end, State, Pairs).

%% @private
-spec erlang_finalize(map()) -> map().
erlang_finalize(TaggedMap) ->
    maps:map(fun(_Key, Value) -> finalize_value(Value) end, TaggedMap).

%% @private
-spec erlang_aggregation_stats([map()]) -> map().
erlang_aggregation_stats(Events) ->
    {EventsWithData, Fields} = lists:foldl(fun(Event, {Count, FieldSet}) ->
        Data = get_event_data(Event),
        case map_size(Data) > 0 of
            true ->
                NewFields = maps:fold(fun(K, _, Acc) ->
                    sets:add_element(K, Acc)
                end, FieldSet, Data),
                {Count + 1, NewFields};
            false ->
                {Count, FieldSet}
        end
    end, {0, sets:new()}, Events),
    #{
        total_events => length(Events),
        events_with_data => EventsWithData,
        unique_fields => sets:size(Fields)
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Get data from an event (handles both map and record-like formats)
-spec get_event_data(map()) -> map().
get_event_data(#{data := Data}) when is_map(Data) ->
    Data;
get_event_data(Event) when is_map(Event) ->
    Event;
get_event_data(_) ->
    #{}.

%% @private Apply a single event to the accumulator
-spec apply_event(map(), map()) -> map().
apply_event(Event, Acc) ->
    Data = get_event_data(Event),
    maps:fold(fun apply_field/3, Acc, Data).

%% @private Apply a single field to the accumulator
-spec apply_field(term(), term(), map()) -> map().
apply_field(Key, {sum, Num}, Acc) when is_number(Num) ->
    Current = get_current_number(maps:get(Key, Acc, 0)),
    maps:put(Key, {sum, Current + Num}, Acc);
apply_field(Key, {overwrite, Value}, Acc) ->
    maps:put(Key, Value, Acc);
apply_field(Key, Value, Acc) ->
    maps:put(Key, Value, Acc).

%% @private Extract the numeric value from a potentially tagged value
-spec get_current_number(term()) -> number().
get_current_number({sum, Value}) when is_number(Value) ->
    Value;
get_current_number(Value) when is_number(Value) ->
    Value;
get_current_number(_) ->
    0.

%% @private Finalize a single value
-spec finalize_value(term()) -> term().
finalize_value({sum, Value}) ->
    Value;
finalize_value({overwrite, Value}) ->
    Value;
finalize_value(Value) ->
    Value.
