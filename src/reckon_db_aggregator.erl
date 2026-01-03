%% @doc Event aggregator for reckon-db
%%
%% Aggregates events from an event stream using tagged rules.
%% Supports special value tags for custom aggregation behavior:
%%
%% Tagged value types:
%%   {sum, N}         - Add N to current value (starts at 0)
%%   {overwrite, V}   - Replace current value with V
%%   plain value      - Replace current value (default behavior)
%%
%% @author rgfaber

-module(reckon_db_aggregator).

-include("reckon_db.hrl").

-export([
    foldl/1,
    foldl/2,
    foldr/1,
    foldr/2,
    finalize/1,
    aggregate/3
]).

%%====================================================================
%% Types
%%====================================================================

-type tagged_value() :: {sum, number()} | {overwrite, term()} | term().
-type tagged_map() :: #{atom() | binary() => tagged_value()}.

-export_type([tagged_value/0, tagged_map/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Fold a list of events from left to right (chronological order)
%%
%% Events should be sorted by version in ascending order.
%% Returns a tagged map that can be finalized with finalize/1.
-spec foldl([event() | map()]) -> tagged_map().
foldl(Events) ->
    foldl(Events, #{}).

%% @doc Fold events with an initial state
-spec foldl([event() | map()], tagged_map()) -> tagged_map().
foldl(Events, InitialState) ->
    lists:foldl(fun apply_event/2, InitialState, Events).

%% @doc Fold a list of events from right to left
%%
%% Events should be sorted by version in ascending order.
%% This will process them in reverse (newest first).
-spec foldr([event() | map()]) -> tagged_map().
foldr(Events) ->
    foldr(Events, #{}).

%% @doc Fold events from right with an initial state
-spec foldr([event() | map()], tagged_map()) -> tagged_map().
foldr(Events, InitialState) ->
    lists:foldr(fun apply_event/2, InitialState, Events).

%% @doc Finalize a tagged map by unwrapping all tagged values
%%
%% Converts {sum, N} to N and {overwrite, V} to V.
-spec finalize(tagged_map()) -> map().
finalize(TaggedMap) ->
    maps:map(fun(_Key, Value) -> finalize_value(Value) end, TaggedMap).

%% @doc Aggregate events from a stream with optional snapshot
%%
%% This is a convenience function that:
%% 1. Starts from a snapshot's data (if provided) or empty map
%% 2. Applies events in order
%% 3. Returns the finalized aggregate state
-spec aggregate([event() | map()], snapshot() | undefined, map()) -> map().
aggregate(Events, undefined, Opts) ->
    InitialState = maps:get(initial_state, Opts, #{}),
    Finalize = maps:get(finalize, Opts, true),
    Result = foldl(Events, InitialState),
    case Finalize of
        true -> finalize(Result);
        false -> Result
    end;
aggregate(Events, #snapshot{data = SnapshotData}, Opts) ->
    %% Start from snapshot data
    InitialState = case is_map(SnapshotData) of
        true -> SnapshotData;
        false -> #{}
    end,
    Finalize = maps:get(finalize, Opts, true),
    Result = foldl(Events, InitialState),
    case Finalize of
        true -> finalize(Result);
        false -> Result
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Apply a single event to the accumulator
-spec apply_event(event() | map(), tagged_map()) -> tagged_map().
apply_event(#event{data = Data}, Acc) when is_map(Data) ->
    apply_data(Data, Acc);
apply_event(#{data := Data}, Acc) when is_map(Data) ->
    apply_data(Data, Acc);
apply_event(Event, Acc) when is_map(Event) ->
    %% Treat the entire map as data
    apply_data(Event, Acc);
apply_event(_, Acc) ->
    Acc.

%% @private Apply data map to accumulator
-spec apply_data(map(), tagged_map()) -> tagged_map().
apply_data(Data, Acc) ->
    maps:fold(fun apply_field/3, Acc, Data).

%% @private Apply a single field to the accumulator
-spec apply_field(term(), tagged_value(), tagged_map()) -> tagged_map().
apply_field(Key, {sum, Num}, Acc) when is_number(Num) ->
    Current = get_current_number(maps:get(Key, Acc, 0)),
    maps:put(Key, {sum, Current + Num}, Acc);
apply_field(Key, {overwrite, Value}, Acc) ->
    maps:put(Key, Value, Acc);
apply_field(Key, Value, Acc) ->
    %% Default: just put the value (overwrite semantics)
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
-spec finalize_value(tagged_value()) -> term().
finalize_value({sum, Value}) ->
    Value;
finalize_value({overwrite, Value}) ->
    Value;
finalize_value(Value) ->
    Value.
