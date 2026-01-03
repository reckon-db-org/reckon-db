%% @doc Optimized pattern matching and filtering operations for reckon-db.
%%
%% This module provides high-performance pattern matching implementations:
%%
%% <ul>
%% <li><b>Wildcard matching</b>: Fast * and ? pattern matching</li>
%% <li><b>Regex matching</b>: Compiled regex pattern matching</li>
%% <li><b>Prefix/Suffix matching</b>: Optimized start/end checks</li>
%% <li><b>Batch filtering</b>: Filter lists by patterns efficiently</li>
%% </ul>
%%
%% The mode is automatically detected at startup based on whether the NIF
%% library is available. Community edition users (hex.pm) will always use
%% the Erlang fallbacks, which provide identical functionality.
%%
%% == Wildcard Patterns ==
%%
%% Wildcards supported:
%% <ul>
%% <li>`*' matches any sequence of characters (including empty)</li>
%% <li>`?' matches any single character</li>
%% </ul>
%%
%% == Usage ==
%%
%% ```
%% %% Check if a stream matches a pattern
%% true = esdb_filter_nif:wildcard_match(<<"orders-123">>, <<"orders-*">>).
%%
%% %% Filter streams by pattern
%% Matching = esdb_filter_nif:filter_by_wildcard(Streams, <<"user-*">>).
%%
%% %% Check which mode is active
%% nif = esdb_filter_nif:implementation().  %% Enterprise
%% erlang = esdb_filter_nif:implementation(). %% Community
%% '''
%%
%% @author rgfaber

-module(esdb_filter_nif).

%% Public API - Pattern matching
-export([
    wildcard_to_regex/1,
    wildcard_match/2,
    regex_match/2,
    has_prefix/2,
    has_suffix/2,
    is_valid_regex/1
]).

%% Public API - Batch filtering
-export([
    filter_by_wildcard/2,
    filter_by_regex/2,
    filter_by_prefix/2,
    filter_by_suffix/2,
    match_indices/2,
    count_matches/2
]).

%% Introspection
-export([
    is_nif_loaded/0,
    implementation/0
]).

%% For testing - expose both implementations
-export([
    nif_wildcard_to_regex/1,
    nif_wildcard_match/2,
    nif_regex_match/2,
    nif_filter_by_wildcard/2,
    nif_filter_by_regex/2,
    nif_has_prefix/2,
    nif_has_suffix/2,
    nif_filter_by_prefix/2,
    nif_filter_by_suffix/2,
    nif_match_indices/2,
    nif_count_matches/2,
    nif_is_valid_regex/1,
    erlang_wildcard_to_regex/1,
    erlang_wildcard_match/2,
    erlang_regex_match/2,
    erlang_filter_by_wildcard/2,
    erlang_filter_by_regex/2,
    erlang_has_prefix/2,
    erlang_has_suffix/2,
    erlang_filter_by_prefix/2,
    erlang_filter_by_suffix/2,
    erlang_match_indices/2,
    erlang_count_matches/2,
    erlang_is_valid_regex/1
]).

%% NIF loading
-on_load(init/0).

%% Persistent term key for NIF status
-define(NIF_LOADED_KEY, esdb_filter_nif_loaded).

%%====================================================================
%% NIF Loading
%%====================================================================

%% @private
%% Try to load NIF from multiple locations:
%% 1. reckon_nifs priv/ (enterprise addon package)
%% 2. reckon_db priv/ (standalone enterprise build)
-spec init() -> ok.
init() ->
    NifName = "esdb_filter_nif",
    Paths = nif_search_paths(NifName),
    case try_load_nif(Paths) of
        ok ->
            persistent_term:put(?NIF_LOADED_KEY, true),
            logger:info("[esdb_filter_nif] NIF loaded - Enterprise mode"),
            ok;
        {error, Reason} ->
            persistent_term:put(?NIF_LOADED_KEY, false),
            logger:info("[esdb_filter_nif] NIF not available (~p), using pure Erlang - Community mode",
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
%% Public API - Pattern Matching
%%====================================================================

%% @doc Convert a wildcard pattern to a regex pattern.
%%
%% Wildcards:
%% - `*' matches any sequence of characters
%% - `?' matches any single character
-spec wildcard_to_regex(Pattern :: binary()) -> binary().
wildcard_to_regex(Pattern) ->
    case is_nif_loaded() of
        true ->
            %% NIF returns a Rust String which becomes an Erlang binary
            Result = nif_wildcard_to_regex(Pattern),
            if
                is_binary(Result) -> Result;
                is_list(Result) -> list_to_binary(Result);
                true -> Result
            end;
        false -> erlang_wildcard_to_regex(Pattern)
    end.

%% @doc Check if a string matches a wildcard pattern.
-spec wildcard_match(Text :: binary(), Pattern :: binary()) -> boolean().
wildcard_match(Text, Pattern) ->
    case is_nif_loaded() of
        true -> nif_wildcard_match(Text, Pattern);
        false -> erlang_wildcard_match(Text, Pattern)
    end.

%% @doc Check if a string matches a regex pattern.
-spec regex_match(Text :: binary(), RegexPattern :: binary()) -> boolean().
regex_match(Text, RegexPattern) ->
    case is_nif_loaded() of
        true -> nif_regex_match(Text, RegexPattern);
        false -> erlang_regex_match(Text, RegexPattern)
    end.

%% @doc Check if a string has a specific prefix.
-spec has_prefix(Text :: binary(), Prefix :: binary()) -> boolean().
has_prefix(Text, Prefix) ->
    case is_nif_loaded() of
        true -> nif_has_prefix(Text, Prefix);
        false -> erlang_has_prefix(Text, Prefix)
    end.

%% @doc Check if a string has a specific suffix.
-spec has_suffix(Text :: binary(), Suffix :: binary()) -> boolean().
has_suffix(Text, Suffix) ->
    case is_nif_loaded() of
        true -> nif_has_suffix(Text, Suffix);
        false -> erlang_has_suffix(Text, Suffix)
    end.

%% @doc Validate that a pattern is a valid regex.
-spec is_valid_regex(Pattern :: binary()) -> boolean().
is_valid_regex(Pattern) ->
    case is_nif_loaded() of
        true -> nif_is_valid_regex(Pattern);
        false -> erlang_is_valid_regex(Pattern)
    end.

%%====================================================================
%% Public API - Batch Filtering
%%====================================================================

%% @doc Filter a list of binaries by wildcard pattern.
-spec filter_by_wildcard(Items :: [binary()], Pattern :: binary()) -> [binary()].
filter_by_wildcard(Items, Pattern) ->
    case is_nif_loaded() of
        true -> nif_filter_by_wildcard(Items, Pattern);
        false -> erlang_filter_by_wildcard(Items, Pattern)
    end.

%% @doc Filter a list of binaries by regex pattern.
-spec filter_by_regex(Items :: [binary()], RegexPattern :: binary()) -> [binary()].
filter_by_regex(Items, RegexPattern) ->
    case is_nif_loaded() of
        true -> nif_filter_by_regex(Items, RegexPattern);
        false -> erlang_filter_by_regex(Items, RegexPattern)
    end.

%% @doc Filter a list of binaries by prefix.
-spec filter_by_prefix(Items :: [binary()], Prefix :: binary()) -> [binary()].
filter_by_prefix(Items, Prefix) ->
    case is_nif_loaded() of
        true -> nif_filter_by_prefix(Items, Prefix);
        false -> erlang_filter_by_prefix(Items, Prefix)
    end.

%% @doc Filter a list of binaries by suffix.
-spec filter_by_suffix(Items :: [binary()], Suffix :: binary()) -> [binary()].
filter_by_suffix(Items, Suffix) ->
    case is_nif_loaded() of
        true -> nif_filter_by_suffix(Items, Suffix);
        false -> erlang_filter_by_suffix(Items, Suffix)
    end.

%% @doc Return indices of items matching a wildcard pattern.
-spec match_indices(Items :: [binary()], Pattern :: binary()) -> [non_neg_integer()].
match_indices(Items, Pattern) ->
    case is_nif_loaded() of
        true -> nif_match_indices(Items, Pattern);
        false -> erlang_match_indices(Items, Pattern)
    end.

%% @doc Count items matching a wildcard pattern.
-spec count_matches(Items :: [binary()], Pattern :: binary()) -> non_neg_integer().
count_matches(Items, Pattern) ->
    case is_nif_loaded() of
        true -> nif_count_matches(Items, Pattern);
        false -> erlang_count_matches(Items, Pattern)
    end.

%%====================================================================
%% NIF Stubs (replaced when NIF loads)
%%====================================================================

%% @private
-spec nif_wildcard_to_regex(binary()) -> binary().
nif_wildcard_to_regex(_Pattern) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_wildcard_match(binary(), binary()) -> boolean().
nif_wildcard_match(_Text, _Pattern) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_regex_match(binary(), binary()) -> boolean().
nif_regex_match(_Text, _RegexPattern) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_filter_by_wildcard([binary()], binary()) -> [binary()].
nif_filter_by_wildcard(_Items, _Pattern) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_filter_by_regex([binary()], binary()) -> [binary()].
nif_filter_by_regex(_Items, _RegexPattern) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_has_prefix(binary(), binary()) -> boolean().
nif_has_prefix(_Text, _Prefix) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_has_suffix(binary(), binary()) -> boolean().
nif_has_suffix(_Text, _Suffix) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_filter_by_prefix([binary()], binary()) -> [binary()].
nif_filter_by_prefix(_Items, _Prefix) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_filter_by_suffix([binary()], binary()) -> [binary()].
nif_filter_by_suffix(_Items, _Suffix) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_match_indices([binary()], binary()) -> [non_neg_integer()].
nif_match_indices(_Items, _Pattern) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_count_matches([binary()], binary()) -> non_neg_integer().
nif_count_matches(_Items, _Pattern) ->
    erlang:nif_error(nif_not_loaded).

%% @private
-spec nif_is_valid_regex(binary()) -> boolean().
nif_is_valid_regex(_Pattern) ->
    erlang:nif_error(nif_not_loaded).

%%====================================================================
%% Pure Erlang Implementations (Always Available)
%%====================================================================

%% @private
-spec erlang_wildcard_to_regex(binary()) -> binary().
erlang_wildcard_to_regex(Pattern) ->
    %% Escape regex special characters, then convert * and ? to regex equivalents
    Escaped = re:replace(Pattern, <<"[.^$+{}\\[\\]\\\\|()]">>, <<"\\\\&">>, [global, {return, binary}]),
    %% Convert * to .* and ? to .
    WithStar = binary:replace(Escaped, <<"*">>, <<".*">>, [global]),
    WithQuestion = binary:replace(WithStar, <<"?">>, <<".">>, [global]),
    <<"^", WithQuestion/binary, "$">>.

%% @private
-spec erlang_wildcard_match(binary(), binary()) -> boolean().
erlang_wildcard_match(Text, Pattern) ->
    RegexPattern = erlang_wildcard_to_regex(Pattern),
    erlang_regex_match(Text, RegexPattern).

%% @private
-spec erlang_regex_match(binary(), binary()) -> boolean().
erlang_regex_match(Text, RegexPattern) ->
    case re:run(Text, RegexPattern) of
        {match, _} -> true;
        nomatch -> false
    end.

%% @private
-spec erlang_filter_by_wildcard([binary()], binary()) -> [binary()].
erlang_filter_by_wildcard(Items, Pattern) ->
    RegexPattern = erlang_wildcard_to_regex(Pattern),
    [Item || Item <- Items, erlang_regex_match(Item, RegexPattern)].

%% @private
-spec erlang_filter_by_regex([binary()], binary()) -> [binary()].
erlang_filter_by_regex(Items, RegexPattern) ->
    [Item || Item <- Items, erlang_regex_match(Item, RegexPattern)].

%% @private
-spec erlang_has_prefix(binary(), binary()) -> boolean().
erlang_has_prefix(Text, Prefix) ->
    PrefixSize = byte_size(Prefix),
    case Text of
        <<Prefix:PrefixSize/binary, _/binary>> -> true;
        _ -> false
    end.

%% @private
-spec erlang_has_suffix(binary(), binary()) -> boolean().
erlang_has_suffix(Text, Suffix) ->
    SuffixSize = byte_size(Suffix),
    TextSize = byte_size(Text),
    case TextSize >= SuffixSize of
        true ->
            PrefixSize = TextSize - SuffixSize,
            case Text of
                <<_:PrefixSize/binary, Suffix:SuffixSize/binary>> -> true;
                _ -> false
            end;
        false ->
            false
    end.

%% @private
-spec erlang_filter_by_prefix([binary()], binary()) -> [binary()].
erlang_filter_by_prefix(Items, Prefix) ->
    [Item || Item <- Items, erlang_has_prefix(Item, Prefix)].

%% @private
-spec erlang_filter_by_suffix([binary()], binary()) -> [binary()].
erlang_filter_by_suffix(Items, Suffix) ->
    [Item || Item <- Items, erlang_has_suffix(Item, Suffix)].

%% @private
-spec erlang_match_indices([binary()], binary()) -> [non_neg_integer()].
erlang_match_indices(Items, Pattern) ->
    RegexPattern = erlang_wildcard_to_regex(Pattern),
    {Indices, _} = lists:foldl(fun(Item, {Acc, Idx}) ->
        case erlang_regex_match(Item, RegexPattern) of
            true -> {[Idx | Acc], Idx + 1};
            false -> {Acc, Idx + 1}
        end
    end, {[], 0}, Items),
    lists:reverse(Indices).

%% @private
-spec erlang_count_matches([binary()], binary()) -> non_neg_integer().
erlang_count_matches(Items, Pattern) ->
    length(erlang_filter_by_wildcard(Items, Pattern)).

%% @private
-spec erlang_is_valid_regex(binary()) -> boolean().
erlang_is_valid_regex(Pattern) ->
    case re:compile(Pattern) of
        {ok, _} -> true;
        {error, _} -> false
    end.
