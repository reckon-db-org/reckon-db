%% @doc Behaviour for pluggable subscription-state backends.
%%
%% Subscription checkpoints track WHERE each consumer is in a stream
%% (or global log). They are written frequently — every ack — but are
%% small and not latency-critical. Most storage backends can handle
%% them fine; the behaviour exists so that a backend can choose
%% whether to keep checkpoints in its own store or delegate to the
%% Khepri control plane.
%%
%% The default implementation (`reckon_db_khepri_subscription_backend')
%% stores checkpoints in Khepri. This is the natural fit — subscription
%% state IS a control-plane concern, and Ra consensus on checkpoints
%% is exactly what you want for "don't replay already-acknowledged
%% events on leader failover."
%%
%% Fast log backends (RocksDB, custom) will typically implement
%% `reckon_db_log_backend' but NOT this — they defer to the Khepri
%% implementation. Having the behaviour explicit makes the split clean.
%%
%% @author rgfaber
-module(reckon_db_subscription_backend).

-type state()        :: term().
-type subscription() :: #{
    name         := binary(),
    selector     := term(),
    checkpoint   := non_neg_integer(),
    consumer_pid => pid() | undefined,
    metadata     => map()
}.

-export_type([state/0, subscription/0]).

-callback init(Opts :: map()) ->
    {ok, state()} | {error, term()}.

-callback close(state()) -> ok.

-callback save(
    state(),
    Selector :: term(),
    Name :: binary(),
    Data :: map()
) -> ok | {error, term()}.

-callback remove(state(), Selector :: term(), Name :: binary()) ->
    ok | {error, term()}.

-callback get(state(), Name :: binary()) ->
    {ok, subscription()} | {error, not_found} | {error, term()}.

-callback list(state()) ->
    {ok, [subscription()]} | {error, term()}.

-callback exists(state(), Name :: binary()) -> boolean().

-callback ack(
    state(),
    Name :: binary(),
    Selector :: term(),
    Checkpoint :: non_neg_integer()
) -> ok | {error, term()}.
