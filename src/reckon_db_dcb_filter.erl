%%% @doc DCB tag-filter evaluation inside Khepri transactions.
%%%
%%% Evaluates a tag_filter() against the tag index (/by_tag/Tag/SeqKey
%%% subtree) and event-type index (/by_event_type/Type/SeqKey subtree),
%%% returning either the set of matching seqs OR an answer to
%%% "does any matching event have seq above Cutoff?".
%%%
%%% Two layers:
%%%   - match_seqs/2: pure-by-construction set algebra over seqs. The
%%%     only side effect is the seqs-provider function, which the caller
%%%     supplies. Test with a hardcoded mock provider.
%%%   - match_any_above_cutoff/2: wraps match_seqs with the cutoff
%%%     comparison and the production seqs-provider that reads from
%%%     Khepri via khepri_tx:get_many/1. Must be called from inside a
%%%     khepri:transaction/2 body.
%%%
%%% The tag_filter() type is canonical in reckon_gater_types.hrl
%%% (reckon-gater 3.4.0+). Use it via the include; do NOT redefine here.
%%% @end
-module(reckon_db_dcb_filter).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    match_seqs/2,
    match_any_above_cutoff/2,
    match_any_above_cutoff/3,
    preprocess_filter/1,
    seqs_for_tag/1,
    seqs_for_event_type/1,
    seqs_for_payload/2,
    seqs_for_payload_hash_pre/1
]).

-export_type([seqs_provider/0, match_result/0]).

%%====================================================================
%% Types
%%====================================================================

%% tag_filter() and seq_cutoff() are CANONICAL in reckon_gater_types.hrl
%% (reckon-gater 3.4.0+). Use them via the include; do NOT redefine here.

%% Fetches the seqs indexed under one lookup key. In production this hits
%% Khepri via khepri_tx:get_many/1. In tests, supply a mock.
%%
%%   binary()                         — look up by tag
%%   {event_type, binary()}           — look up by event_type field
%%   {payload, Key, Value}            — look up by single-field payload
%%   {payload_hash_pre, Hash}         — look up by pre-computed composite hash
%%
%% The {payload_hash_pre, Hash} key receives an already-computed SHA-256 hash.
%% The hash is pre-computed OUTSIDE the transaction by preprocess_filter/1 so
%% that no crypto calls happen inside the Horus-extracted transaction body.
-type seqs_provider() :: fun((
    binary() |
    {event_type, binary()} |
    {payload, Key :: binary(), Value :: binary()} |
    {payload_hash_pre, Hash :: binary()}
) -> [non_neg_integer()]).

-type match_result() :: false | {true, MaxSeq :: non_neg_integer()}.

%%====================================================================
%% Public API
%%====================================================================

%% @doc Set of seqs whose events match the filter, per-event semantics.
%%
%% Algebra:
%%   any_of(Tags)       = union(seqs_for_tag(T) | T in Tags)
%%   all_of(Tags)       = intersection(seqs_for_tag(T) | T in Tags)
%%   event_type(Type)   = seqs_for_event_type(Type)
%%   or_(Filters)       = union(match_seqs(F) | F in Filters)
%%   and_(Filters)      = intersection(match_seqs(F) | F in Filters)
%%
%% Empty tag/filter lists yield the empty set (no event matches "nothing").
-spec match_seqs(reckon_gater_types:tag_filter(), seqs_provider()) ->
    sets:set(non_neg_integer()).
match_seqs({any_of, []}, _Provider) ->
    sets:new();
match_seqs({any_of, Tags}, Provider) when is_list(Tags) ->
    lists:foldl(
        fun(Tag, Acc) ->
            sets:union(Acc, sets:from_list(Provider(Tag)))
        end,
        sets:new(), Tags);
match_seqs({all_of, []}, _Provider) ->
    sets:new();
match_seqs({all_of, [First | Rest]}, Provider) ->
    InitSet = sets:from_list(Provider(First)),
    lists:foldl(
        fun(Tag, Acc) ->
            sets:intersection(Acc, sets:from_list(Provider(Tag)))
        end,
        InitSet, Rest);
match_seqs({event_type, Type}, Provider) when is_binary(Type) ->
    sets:from_list(Provider({event_type, Type}));
match_seqs({or_, []}, _Provider) ->
    sets:new();
match_seqs({or_, Filters}, Provider) when is_list(Filters) ->
    lists:foldl(
        fun(SubFilter, Acc) ->
            sets:union(Acc, match_seqs(SubFilter, Provider))
        end,
        sets:new(), Filters);
match_seqs({and_, []}, _Provider) ->
    sets:new();
match_seqs({and_, [First | Rest]}, Provider) ->
    InitSet = match_seqs(First, Provider),
    lists:foldl(
        fun(SubFilter, Acc) ->
            sets:intersection(Acc, match_seqs(SubFilter, Provider))
        end,
        InitSet, Rest);
match_seqs({payload_match, Key, Value}, Provider)
        when is_binary(Key), is_binary(Value) ->
    sets:from_list(Provider({payload, Key, Value}));
%% Internal variant: payload_hash_match with pre-computed hash.
%% Created by preprocess_filter/1; never appears in raw user-supplied filters.
match_seqs({payload_hash_match_pre, Hash}, Provider) when is_binary(Hash) ->
    sets:from_list(Provider({payload_hash_pre, Hash})).

%% @doc Rewrite a tag_filter to replace {payload_hash_match, Keys, Values}
%% with {payload_hash_match_pre, Hash}, where Hash is the pre-computed
%% SHA-256 of the sorted [{Key,Value}] pair list.
%%
%% MUST be called OUTSIDE a Khepri transaction. The hash computation calls
%% crypto:hash/2 (a NIF); Horus cannot extract NIF calls into the portable
%% transaction form that Ra replicates to all cluster members.
%%
%% Filters without payload_hash_match are returned unchanged.
-spec preprocess_filter(reckon_gater_types:tag_filter()) ->
    reckon_gater_types:tag_filter().
preprocess_filter({payload_hash_match, Keys, Values}) ->
    Hash = reckon_db_dcb_paths:payload_combo_hash(Keys, Values),
    {payload_hash_match_pre, Hash};
preprocess_filter({and_, Filters}) ->
    {and_, [preprocess_filter(F) || F <- Filters]};
preprocess_filter({or_, Filters}) ->
    {or_, [preprocess_filter(F) || F <- Filters]};
preprocess_filter(Other) ->
    Other.

%% @doc "Does any matching event have seq > Cutoff?" using the production
%% Khepri-backed seqs provider. Call from inside khepri:transaction/2.
%%
%% Cutoff = -1 means "I saw nothing yet" — ANY matching event triggers a
%% conflict. Cutoff = N (>= 0) means "I saw events through seq N" —
%% only seqs > N trigger a conflict.
-spec match_any_above_cutoff(reckon_gater_types:tag_filter(),
                             reckon_gater_types:seq_cutoff()) ->
    match_result().
match_any_above_cutoff(Filter, Cutoff) ->
    match_any_above_cutoff(Filter, Cutoff, fun default_provider/1).

%% @doc Pure-testable variant: the caller supplies the seqs provider.
-spec match_any_above_cutoff(reckon_gater_types:tag_filter(),
                             reckon_gater_types:seq_cutoff(),
                             seqs_provider()) -> match_result().
match_any_above_cutoff(Filter, Cutoff, Provider)
  when is_integer(Cutoff) ->
    MatchingSet = match_seqs(Filter, Provider),
    Above = [S || S <- sets:to_list(MatchingSet), S > Cutoff],
    case Above of
        []   -> false;
        _    -> {true, lists:max(Above)}
    end.

%% @doc The production seqs provider. Dispatches to the appropriate index
%% depending on the key shape. MUST be called from inside khepri:transaction/2.
-spec default_provider(
    binary() |
    {event_type, binary()} |
    {payload, binary(), binary()} |
    {payload_hash_pre, binary()}
) -> [non_neg_integer()].
default_provider({event_type, Type}) ->
    seqs_for_event_type(Type);
default_provider({payload, Key, Value}) ->
    seqs_for_payload(Key, Value);
default_provider({payload_hash_pre, Hash}) ->
    seqs_for_payload_hash_pre(Hash);
default_provider(Tag) when is_binary(Tag) ->
    seqs_for_tag(Tag).

%% @doc Look up seqs by tag. Reads the tag index inside a transaction.
%% Returns the list of seqs indexed under Tag. Empty list if no entries.
-spec seqs_for_tag(binary()) -> [non_neg_integer()].
seqs_for_tag(Tag) when is_binary(Tag) ->
    Pattern = reckon_db_dcb_paths:by_tag_pattern(Tag),
    case khepri_tx:get_many(Pattern) of
        {ok, Map} ->
            [reckon_db_dcb_paths:seq_from_key(SeqKey)
             || Path <- maps:keys(Map),
                [_, _, SeqKey] <- [Path]];
        {error, _} ->
            []
    end.

%% @doc Look up seqs by event type. Reads the event-type index inside a
%% transaction. Returns the list of seqs for EventType. Empty list if none.
-spec seqs_for_event_type(binary()) -> [non_neg_integer()].
seqs_for_event_type(EventType) when is_binary(EventType) ->
    Pattern = reckon_db_dcb_paths:by_event_type_pattern(EventType),
    case khepri_tx:get_many(Pattern) of
        {ok, Map} ->
            [reckon_db_dcb_paths:seq_from_key(SeqKey)
             || Path <- maps:keys(Map),
                [_, _, SeqKey] <- [Path]];
        {error, _} ->
            []
    end.

%% @doc Look up seqs by single payload field value. Reads the payload
%% index [by_payload, Key, Value, *] inside a transaction.
-spec seqs_for_payload(binary(), binary()) -> [non_neg_integer()].
seqs_for_payload(Key, Value) when is_binary(Key), is_binary(Value) ->
    Pattern = reckon_db_dcb_paths:by_payload_pattern(Key, Value),
    case khepri_tx:get_many(Pattern) of
        {ok, Map} ->
            [reckon_db_dcb_paths:seq_from_key(SeqKey)
             || Path <- maps:keys(Map),
                [_, _, _, SeqKey] <- [Path]];
        {error, _} ->
            []
    end.

%% @doc Look up seqs by pre-computed composite payload hash. Reads the
%% payload hash index [by_payload_hash, Hash, *] inside a transaction.
%%
%% The Hash must be pre-computed OUTSIDE the transaction via
%% payload_combo_hash/2 — this function is called inside the transaction
%% and must not perform any crypto calls.
-spec seqs_for_payload_hash_pre(binary()) -> [non_neg_integer()].
seqs_for_payload_hash_pre(Hash) when is_binary(Hash) ->
    Pattern = reckon_db_dcb_paths:by_payload_hash_pattern(Hash),
    case khepri_tx:get_many(Pattern) of
        {ok, Map} ->
            [reckon_db_dcb_paths:seq_from_key(SeqKey)
             || Path <- maps:keys(Map),
                [_, _, SeqKey] <- [Path]];
        {error, _} ->
            []
    end.
