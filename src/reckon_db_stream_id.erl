%% @doc Stream-id format validator.
%%
%% Single source of truth for what "a valid stream id" means in
%% reckon-db. Used by {@link reckon_db_streams:append/4} to reject
%% malformed ids at write time so the store never accumulates
%% polluted paths.
%%
%% == Accepted formats ==
%%
%% <ul>
%% <li>**User stream:** `&lt;prefix&gt;-&lt;hex&gt;', where:
%%     <ul>
%%     <li>`&lt;prefix&gt;' is `[A-Za-z]+' — one or more ASCII letters,
%%         no digits, no hyphens, no `$'.</li>
%%     <li>`-' is a single mandatory separator.</li>
%%     <li>`&lt;hex&gt;' is `[A-Fa-f0-9]+' — one or more hex digits,
%%         typically a UUIDv7 with dashes stripped.</li>
%%     </ul>
%%     Examples: `account-018f6a7b8c9d4abc8901234567890abc',
%%     `order-deadbeef'.</li>
%% <li>**System stream:** `$&lt;namespace&gt;:&lt;name&gt;', where:
%%     <ul>
%%     <li>`$' is a mandatory prefix.</li>
%%     <li>`&lt;namespace&gt;' is `[a-z][a-z0-9-]*' — lowercase
%%         identifier, may contain hyphens (e.g. `link-sub').</li>
%%     <li>`:' is a single mandatory separator.</li>
%%     <li>`&lt;name&gt;' is `[A-Za-z0-9][A-Za-z0-9_.-]*' — intentionally
%%         human-readable; the whole point of system streams is
%%         operational legibility.</li>
%%     </ul>
%%     Examples: `$link:high-value-orders', `$link-sub:revenue'.</li>
%% </ul>
%%
%% See `guides/system_streams.md' for the rationale.
%%
%% == Rejected (returns `{error, Reason}') ==
%%
%% <ul>
%% <li>Empty binary — `empty'</li>
%% <li>Not a binary — `not_binary'</li>
%% <li>Anything starting with `$' that isn't a well-formed system
%%     id — `malformed_system_id'</li>
%% <li>Anything not starting with `$' that isn't a well-formed
%%     user id — `malformed_user_id'</li>
%% </ul>
%%
%% Rejected examples: `test$basic-stream' (mid-string `$' in a
%% user id), `partition$XYZ' (ditto), `$weird' (no `:'),
%% `MYAPP-abc' (prefix is fine but `abc' starts compliant — and
%% `xyz' isn't hex; `g' would be rejected), `account-' (empty
%% hex), `-deadbeef' (empty prefix).

-module(reckon_db_stream_id).

-export([
    validate/1,
    is_valid/1,
    is_system/1
]).

-export_type([validation_error/0]).

-type validation_error() ::
    empty
    | not_binary
    | malformed_user_id
    | malformed_system_id.

%% Compile the regexes once at module load via persistent_term so
%% the hot path doesn't pay for re:compile per call.
-define(USER_RE,
    {?MODULE, user_re}).
-define(SYSTEM_RE,
    {?MODULE, system_re}).

%% @doc Validate StreamId. Returns `ok' if it matches either
%% accepted format, or `{error, Reason}'.
-spec validate(term()) -> ok | {error, validation_error()}.
validate(<<>>) ->
    {error, empty};
validate(<<"$", _/binary>> = Id) ->
    case re:run(Id, system_re(), [{capture, none}]) of
        match -> ok;
        nomatch -> {error, malformed_system_id}
    end;
validate(Id) when is_binary(Id) ->
    case re:run(Id, user_re(), [{capture, none}]) of
        match -> ok;
        nomatch -> {error, malformed_user_id}
    end;
validate(_) ->
    {error, not_binary}.

%% @doc Boolean wrapper for use in guards / list-comprehensions.
-spec is_valid(term()) -> boolean().
is_valid(StreamId) ->
    validate(StreamId) =:= ok.

%% @doc True if StreamId is in the system namespace (starts with
%% `$' and is well-formed). Note: `$all' is NOT a valid stream id
%% — it's a subscription-selector sentinel only.
-spec is_system(binary()) -> boolean().
is_system(<<"$", _/binary>> = Id) ->
    validate(Id) =:= ok;
is_system(_) ->
    false.

%%====================================================================
%% Internal — regex caching
%%====================================================================

user_re() ->
    case persistent_term:get(?USER_RE, undefined) of
        undefined ->
            {ok, MP} = re:compile(<<"^[A-Za-z]+-[A-Fa-f0-9]+$">>),
            persistent_term:put(?USER_RE, MP),
            MP;
        MP ->
            MP
    end.

system_re() ->
    case persistent_term:get(?SYSTEM_RE, undefined) of
        undefined ->
            {ok, MP} = re:compile(
                <<"^\\$[a-z][a-z0-9-]*:[A-Za-z0-9][A-Za-z0-9_.-]*$">>),
            persistent_term:put(?SYSTEM_RE, MP),
            MP;
        MP ->
            MP
    end.
