%% @doc Token revocation management for reckon-db.
%%
%% Manages revocation of capability tokens. Tokens can be revoked before their
%% expiration when:
%% <ul>
%% <li>A key is compromised</li>
%% <li>An identity is removed from the system</li>
%% <li>Permissions need to be immediately revoked</li>
%% </ul>
%%
%% This module supports multiple revocation strategies:
%% <ul>
%% <li>Local ETS (current): Fast local lookups, no distribution</li>
%% <li>Gossip (planned): Eventually consistent, partition tolerant</li>
%% <li>Epoch-based (planned): Revoke all tokens before a timestamp</li>
%% </ul>
%%
%% @author rgfaber

-module(esdb_revocation).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([revoke/1, revoke/2]).
-export([revoke_issuer/1]).
-export([is_revoked/1]).
-export([is_issuer_revoked/1]).
-export([get_revocations/0]).
-export([clear/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(TOKEN_TABLE, esdb_revoked_tokens).
-define(ISSUER_TABLE, esdb_revoked_issuers).

%% Default TTL for revocation entries (7 days)
%% After this, the token would have expired anyway
-define(DEFAULT_REVOCATION_TTL_SECS, 604800).

-record(revocation, {
    cid :: binary(),                 %% Token CID or Issuer DID
    revoked_at :: integer(),         %% Unix timestamp
    reason :: binary() | undefined,  %% Optional reason
    expires_at :: integer()          %% When to clean up this entry
}).

-record(state, {
    cleanup_timer :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the revocation server
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Revoke a token by its CID
-spec revoke(binary()) -> ok.
revoke(TokenCID) ->
    revoke(TokenCID, undefined).

%% @doc Revoke a token with a reason
-spec revoke(binary(), binary() | undefined) -> ok.
revoke(TokenCID, Reason) ->
    gen_server:call(?SERVER, {revoke_token, TokenCID, Reason}).

%% @doc Revoke all tokens from an issuer
%%
%% This is useful when an identity is compromised or removed.
%% All tokens with this issuer DID will be considered revoked.
-spec revoke_issuer(binary()) -> ok.
revoke_issuer(IssuerDID) ->
    gen_server:call(?SERVER, {revoke_issuer, IssuerDID}).

%% @doc Check if a token CID is revoked
-spec is_revoked(binary()) -> boolean().
is_revoked(TokenCID) ->
    case ets:lookup(?TOKEN_TABLE, TokenCID) of
        [#revocation{expires_at = ExpiresAt}] ->
            %% Check if the revocation entry is still valid
            erlang:system_time(second) < ExpiresAt;
        [] ->
            false
    end.

%% @doc Check if an issuer DID is revoked
-spec is_issuer_revoked(binary()) -> boolean().
is_issuer_revoked(IssuerDID) ->
    case ets:lookup(?ISSUER_TABLE, IssuerDID) of
        [#revocation{}] -> true;
        [] -> false
    end.

%% @doc Get all active revocations (for debugging/monitoring)
-spec get_revocations() -> #{tokens := [binary()], issuers := [binary()]}.
get_revocations() ->
    Now = erlang:system_time(second),
    Tokens = [CID || #revocation{cid = CID, expires_at = E} <- ets:tab2list(?TOKEN_TABLE), E > Now],
    Issuers = [DID || #revocation{cid = DID} <- ets:tab2list(?ISSUER_TABLE)],
    #{tokens => Tokens, issuers => Issuers}.

%% @doc Clear all revocations (for testing)
-spec clear() -> ok.
clear() ->
    gen_server:call(?SERVER, clear).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @private
init([]) ->
    %% Create ETS tables for revocations
    ets:new(?TOKEN_TABLE, [
        named_table,
        public,
        set,
        {keypos, #revocation.cid},
        {read_concurrency, true}
    ]),
    ets:new(?ISSUER_TABLE, [
        named_table,
        public,
        set,
        {keypos, #revocation.cid},
        {read_concurrency, true}
    ]),

    %% Schedule periodic cleanup
    Timer = schedule_cleanup(),

    {ok, #state{cleanup_timer = Timer}}.

%% @private
handle_call({revoke_token, TokenCID, Reason}, _From, State) ->
    Now = erlang:system_time(second),
    ExpiresAt = Now + ?DEFAULT_REVOCATION_TTL_SECS,
    Revocation = #revocation{
        cid = TokenCID,
        revoked_at = Now,
        reason = Reason,
        expires_at = ExpiresAt
    },
    ets:insert(?TOKEN_TABLE, Revocation),

    logger:info("Token revoked: ~s (reason: ~p)", [TokenCID, Reason]),

    {reply, ok, State};

handle_call({revoke_issuer, IssuerDID}, _From, State) ->
    Now = erlang:system_time(second),
    %% Issuer revocations don't expire (permanent until cleared)
    Revocation = #revocation{
        cid = IssuerDID,
        revoked_at = Now,
        reason = <<"issuer_revoked">>,
        expires_at = Now + (365 * 24 * 3600)  %% 1 year
    },
    ets:insert(?ISSUER_TABLE, Revocation),

    logger:warning("Issuer revoked: ~s", [IssuerDID]),

    {reply, ok, State};

handle_call(clear, _From, State) ->
    ets:delete_all_objects(?TOKEN_TABLE),
    ets:delete_all_objects(?ISSUER_TABLE),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info(cleanup, #state{} = State) ->
    cleanup_expired(),
    Timer = schedule_cleanup(),
    {noreply, State#state{cleanup_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec schedule_cleanup() -> reference().
schedule_cleanup() ->
    %% Cleanup every hour
    erlang:send_after(3600 * 1000, self(), cleanup).

%% @private
-spec cleanup_expired() -> ok.
cleanup_expired() ->
    Now = erlang:system_time(second),

    %% Clean up expired token revocations
    TokenMs = ets:fun2ms(fun(#revocation{expires_at = E}) when E < Now -> true end),
    NumTokens = ets:select_delete(?TOKEN_TABLE, TokenMs),

    %% Clean up expired issuer revocations
    IssuerMs = ets:fun2ms(fun(#revocation{expires_at = E}) when E < Now -> true end),
    NumIssuers = ets:select_delete(?ISSUER_TABLE, IssuerMs),

    case NumTokens + NumIssuers of
        0 -> ok;
        N -> logger:debug("Cleaned up ~p expired revocation entries", [N])
    end,
    ok.

%%====================================================================
%% Tests
%%====================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Tests are in test/unit/esdb_revocation_tests.erl

-endif.
