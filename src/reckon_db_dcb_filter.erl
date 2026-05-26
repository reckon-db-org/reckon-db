%%% @doc DCB tag-filter evaluation inside Khepri transactions.
%%%
%%% Evaluates a `tag_filter()` against the tag index (`/by_tag/Tag/SeqKey`
%%% subtree) and returns either the set of matching seqs OR an answer to
%%% "does any matching event have seq > Cutoff?".
%%%
%%% Two layers:
%%%   - `match_seqs/2` — pure-by-construction set algebra over seqs. The
%%%     only side effect is the seqs-provider function, which the caller
%%%     supplies. Test with a hardcoded mock provider.
%%%   - `match_any_above_cutoff/2` — wraps match_seqs with the cutoff
%%%     comparison and the production seqs-provider that reads from
%%%     Khepri via `khepri_tx:get_many/1`. Must be called from inside a
%%%     `khepri:transaction/2` body.
%%%
%%% The `tag_filter()` type is defined here for now. P3.3 will move it
%%% to `reckon-gater/include/reckon_gater_types.hrl` as the canonical
%%% home; this module will then re-export from there.
%%% @end
-module(reckon_db_dcb_filter).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    match_seqs/2,
    match_any_above_cutoff/2,
    match_any_above_cutoff/3,
    seqs_for_tag/1
]).

-export_type([seqs_provider/0, match_result/0]).

%%====================================================================
%% Types
%%====================================================================

%% `tag_filter()` and `seq_cutoff()` are CANONICAL in reckon_gater_types.hrl
%% (reckon-gater 2.3.0+). Use them via the include; do NOT redefine here.

%% Fetches the seqs indexed under one tag. In production this hits
%% Khepri via khepri_tx:get_many/1. In tests, supply a mock.
-type seqs_provider() :: fun((binary()) -> [non_neg_integer()]).

-type match_result() :: false | {true, MaxSeq :: non_neg_integer()}.

%%====================================================================
%% Public API
%%====================================================================

%% @doc Set of seqs whose events match the filter, per-event semantics.
%%
%% Algebra:
%%   any_of(Tags)    = union(seqs_for_tag(T) | T in Tags)
%%   all_of(Tags)    = intersection(seqs_for_tag(T) | T in Tags)
%%   or_(Filters)    = union(match_seqs(F) | F in Filters)
%%   and_(Filters)   = intersection(match_seqs(F) | F in Filters)
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
        InitSet, Rest).

%% @doc "Does any matching event have seq > Cutoff?" using the production
%% Khepri-backed seqs provider. Call from inside `khepri:transaction/2`.
%%
%% Cutoff = -1 means "I saw nothing yet" — ANY matching event triggers a
%% conflict. Cutoff = N (>= 0) means "I saw events through seq N" —
%% only seqs > N trigger a conflict.
-spec match_any_above_cutoff(reckon_gater_types:tag_filter(),
                             reckon_gater_types:seq_cutoff()) ->
    match_result().
match_any_above_cutoff(Filter, Cutoff) ->
    match_any_above_cutoff(Filter, Cutoff, fun seqs_for_tag/1).

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

%% @doc The production seqs provider. Reads the tag index inside a
%% transaction. MUST be called from inside `khepri:transaction/2`.
%%
%% Returns the list of seqs indexed under `Tag`. Empty list if the tag
%% has no entries (the path doesn't exist).
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
