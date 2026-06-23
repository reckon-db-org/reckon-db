%% @doc Generic write-maintained secondary index (opt-in per store).
%%
%% Turns cross-cutting lookups — "all events with tag X", "all events of
%% type Y", "all events whose metadata key K = V" — from O(total events)
%% scans into O(matches) subtree reads, by maintaining reference entries
%% keyed by the indexed value. This module owns the index layout; nothing
%% else constructs `idx' paths.
%%
%% == Layout ==
%%
%% ```
%% [idx, tag,        Tag,       OrderKey] -> EventRef
%% [idx, event_type, EventType, OrderKey] -> EventRef
%% [idx, meta, Key,  Value,     OrderKey] -> EventRef
%% '''
%%
%% `OrderKey = pad(epoch_us) | stream_id | pad(version)' — globally ordered
%% (subtree iteration yields event/time order) and unique (no two events
%% share stream+version). `EventRef = #{stream_id := binary(),
%% version := non_neg_integer()}' points at the primary event under the
%% Model C layout; resolution is a point `khepri:get' via
%% {@link reckon_db_stream_path:event_path/2}.
%%
%% == Maintenance ==
%%
%% `entries/2' returns the `{Path, EventRef}' pairs an event produces under
%% a store's declared indexes; the append path writes them transactionally
%% with the event (so the index is never partially populated). This module
%% performs no writes itself — it builds paths and reads.
%%
%% == Reads ==
%%
%% `lookup_*' read the relevant subtree, resolve refs to events (dropping
%% refs whose event has since been removed, e.g. by scavenge), and sort by
%% `epoch_us'. Compound tag `all' intersects ref sets across tags.
%%
%% See plans/DESIGN_SECONDARY_INDEX.md.
-module(reckon_db_index).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    entries/2,
    order_key/1,
    event_ref/1,
    lookup_tags/3,
    lookup_event_types/2,
    lookup_meta/3
]).

-type event_ref() :: #{stream_id := binary(), version := non_neg_integer()}.
-export_type([event_ref/0]).

%%====================================================================
%% Write-path entry construction
%%====================================================================

%% @doc The index entries an event produces under a store's declared
%% indexes. Each entry is a `{Path, EventRef}' the caller writes
%% transactionally with the event. Empty when no declared index applies.
-spec entries(event(), [index_decl()]) -> [{khepri_path:native_path(), event_ref()}].
entries(#event{} = Event, Declared) ->
    OrderKey = order_key(Event),
    Ref = event_ref(Event),
    lists:flatmap(fun(Decl) -> entries_for(Decl, Event, OrderKey, Ref) end,
                  Declared).

-spec entries_for(index_decl(), event(), binary(), event_ref()) ->
    [{khepri_path:native_path(), event_ref()}].
entries_for(tags, #event{tags = Tags}, OrderKey, Ref) when is_list(Tags) ->
    [{tag_path(T, OrderKey), Ref} || T <- Tags, is_binary(T)];
entries_for(tags, _Event, _OrderKey, _Ref) ->
    [];
entries_for(event_type, #event{event_type = ET}, OrderKey, Ref)
        when is_binary(ET) ->
    [{event_type_path(ET, OrderKey), Ref}];
entries_for(event_type, _Event, _OrderKey, _Ref) ->
    [];
entries_for({meta, Key}, #event{metadata = M}, OrderKey, Ref) when is_map(M) ->
    case maps:get(Key, M, undefined) of
        V when is_binary(V) -> [{meta_path(Key, V, OrderKey), Ref}];
        %% Absent, or a non-binary value (only binary values are path-keyable
        %% and round-trippable against read_by_metadata/3's Value argument).
        _ -> []
    end;
entries_for({meta, _Key}, _Event, _OrderKey, _Ref) ->
    [];
%% Payload indexes are DCB-scoped (under ?BY_PAYLOAD_PATH / ?BY_PAYLOAD_HASH_PATH)
%% and written by reckon_db_dcb, not by the secondary index mechanism.
entries_for({payload, _Key}, _Event, _OrderKey, _Ref) ->
    [];
entries_for({payload_hash, _Keys}, _Event, _OrderKey, _Ref) ->
    [].

%% @doc The unique, time-orderable leaf key for an event's index entries.
-spec order_key(event()) -> nonempty_binary().
order_key(#event{epoch_us = EpochUs, stream_id = StreamId, version = Version}) ->
    Epoch = pad(EpochUs, ?INDEX_ORDER_KEY_WIDTH),
    Ver = pad(Version, ?VERSION_PADDING),
    <<Epoch/binary, "|", StreamId/binary, "|", Ver/binary>>.

%% @doc The reference stored at an index leaf — enough to point-get the
%% primary event under the Model C layout.
-spec event_ref(event()) -> event_ref().
event_ref(#event{stream_id = StreamId, version = Version}) ->
    #{stream_id => StreamId, version => Version}.

%%====================================================================
%% Read-path lookups
%%====================================================================

%% @doc Events carrying the given tags. `any' = union, `all' = intersection.
%% Empty tag list returns [] (no criteria, no results — matches the
%% pre-index scan semantics).
-spec lookup_tags(atom(), [binary()], any | all) ->
    {ok, [event()]} | {error, term()}.
lookup_tags(_StoreId, [], _Match) ->
    {ok, []};
lookup_tags(StoreId, Tags, any) ->
    case refs_for_patterns(StoreId, [tag_subtree(T) || T <- Tags]) of
        {ok, RefLists} ->
            {ok, resolve_sorted(StoreId, dedup_refs(lists:append(RefLists)))};
        {error, _} = Error ->
            Error
    end;
lookup_tags(StoreId, Tags, all) ->
    case refs_for_patterns(StoreId, [tag_subtree(T) || T <- Tags]) of
        {ok, RefLists} ->
            Common = intersect_all([refkey_set(R) || R <- RefLists]),
            Refs = [#{stream_id => S, version => V} || {S, V} <- sets:to_list(Common)],
            {ok, resolve_sorted(StoreId, Refs)};
        {error, _} = Error ->
            Error
    end.

%% @doc Events of any of the given types (union).
-spec lookup_event_types(atom(), [binary()]) -> {ok, [event()]} | {error, term()}.
lookup_event_types(_StoreId, []) ->
    {ok, []};
lookup_event_types(StoreId, Types) ->
    case refs_for_patterns(StoreId, [event_type_subtree(ET) || ET <- Types]) of
        {ok, RefLists} ->
            {ok, resolve_sorted(StoreId, dedup_refs(lists:append(RefLists)))};
        {error, _} = Error ->
            Error
    end.

%% @doc Events whose metadata key = value. The sanctioned primitive apps
%% build causation/correlation/saga read models on; the store does not
%% interpret the key.
-spec lookup_meta(atom(), binary(), binary()) ->
    {ok, [event()]} | {error, term()}.
lookup_meta(StoreId, Key, Value) when is_binary(Key), is_binary(Value) ->
    case subtree_refs(StoreId, meta_subtree(Key, Value)) of
        {ok, Refs} -> {ok, resolve_sorted(StoreId, dedup_refs(Refs))};
        {error, _} = Error -> Error
    end.

%%====================================================================
%% Path builders
%%====================================================================

-spec tag_path(binary(), binary()) -> khepri_path:native_path().
tag_path(Tag, OrderKey) -> ?INDEX_PATH ++ [tag, Tag, OrderKey].

-spec event_type_path(binary(), binary()) -> khepri_path:native_path().
event_type_path(EventType, OrderKey) ->
    ?INDEX_PATH ++ [event_type, EventType, OrderKey].

-spec meta_path(binary(), binary(), binary()) -> khepri_path:native_path().
meta_path(Key, Value, OrderKey) ->
    ?INDEX_PATH ++ [meta, Key, Value, OrderKey].

tag_subtree(Tag) -> ?INDEX_PATH ++ [tag, Tag, ?KHEPRI_WILDCARD_STAR].
event_type_subtree(ET) -> ?INDEX_PATH ++ [event_type, ET, ?KHEPRI_WILDCARD_STAR].
meta_subtree(Key, Value) -> ?INDEX_PATH ++ [meta, Key, Value, ?KHEPRI_WILDCARD_STAR].

%%====================================================================
%% Internal — ref collection / resolution
%%====================================================================

%% @private All EventRefs under an index subtree pattern. A real Khepri
%% error propagates (a not-ready store must surface as a retriable error,
%% NOT a silently-empty result — that would be a silent-incomplete read);
%% an absent subtree (no events for that value) is `{ok, []}'.
-spec subtree_refs(atom(), khepri_path:native_pattern()) ->
    {ok, [event_ref()]} | {error, term()}.
subtree_refs(StoreId, Pattern) ->
    case khepri:get_many(StoreId, Pattern) of
        {ok, Results} when is_map(Results) ->
            {ok, [Ref || Ref <- maps:values(Results), is_map(Ref)]};
        {ok, _} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @private Collect ref lists for several subtree patterns, short-circuiting
%% on the first Khepri error. Returns one ref list per pattern, in order.
-spec refs_for_patterns(atom(), [khepri_path:native_pattern()]) ->
    {ok, [[event_ref()]]} | {error, term()}.
refs_for_patterns(StoreId, Patterns) ->
    lists:foldr(
        fun(_Pattern, {error, _} = Error) ->
                Error;
           (Pattern, {ok, Acc}) ->
                case subtree_refs(StoreId, Pattern) of
                    {ok, Refs} -> {ok, [Refs | Acc]};
                    {error, _} = Error -> Error
                end
        end,
        {ok, []},
        Patterns).

%% @private De-duplicate refs by {stream_id, version}.
-spec dedup_refs([event_ref()]) -> [event_ref()].
dedup_refs(Refs) ->
    Map = lists:foldl(
        fun(#{stream_id := S, version := V} = R, Acc) -> Acc#{{S, V} => R} end,
        #{}, Refs),
    maps:values(Map).

%% @private A set of {stream_id, version} ref keys.
-spec refkey_set([event_ref()]) -> sets:set().
refkey_set(Refs) ->
    sets:from_list([{S, V} || #{stream_id := S, version := V} <- Refs]).

-spec intersect_all([sets:set()]) -> sets:set().
intersect_all([]) -> sets:new();
intersect_all([S | Rest]) ->
    lists:foldl(fun sets:intersection/2, S, Rest).

%% @private Resolve refs to events (dropping refs whose event is gone),
%% sorted by epoch_us — consistent with the pre-index read_by_* ordering.
-spec resolve_sorted(atom(), [event_ref()]) -> [event()].
resolve_sorted(StoreId, Refs) ->
    Events = lists:filtermap(fun(Ref) -> resolve(StoreId, Ref) end, Refs),
    lists:sort(
        fun(#event{epoch_us = E1}, #event{epoch_us = E2}) -> E1 =< E2 end,
        Events).

-spec resolve(atom(), event_ref()) -> {true, event()} | false.
resolve(StoreId, #{stream_id := StreamId, version := Version}) ->
    Path = reckon_db_stream_path:event_path(StreamId, pad(Version, ?VERSION_PADDING)),
    case khepri:get(StoreId, Path) of
        {ok, #event{} = Event} -> {true, Event#event{stream_id = StreamId}};
        _ -> false
    end.

%%====================================================================
%% Internal — padding
%%====================================================================

-spec pad(non_neg_integer(), pos_integer()) -> binary().
pad(N, Width) ->
    Str = integer_to_list(N),
    Padding = Width - length(Str),
    list_to_binary(lists:duplicate(max(0, Padding), $0) ++ Str).
