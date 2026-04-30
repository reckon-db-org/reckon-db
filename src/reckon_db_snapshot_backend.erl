%% @doc Behaviour for pluggable snapshot backends.
%%
%% Aggregate snapshots are a COLD PATH — written periodically by evoq's
%% `maybe_snapshot/1' trigger, read only during aggregate rehydration.
%% Not the bottleneck that drove the log-backend split, but equally
%% worth isolating so a backend can specialise (e.g., a RocksDB log
%% backend could either reuse its column families for snapshots, or
%% delegate to the Khepri control plane).
%%
%% The default implementation (`reckon_db_khepri_snapshot_backend')
%% stores snapshots in Khepri — completely fine for cold-path use.
%% Alternative implementations exist primarily to avoid the Khepri
%% dependency when a backend wants to be self-contained.
%%
%% @author rgfaber
-module(reckon_db_snapshot_backend).

-type state()        :: term().
-type source_id()    :: binary().  %% aggregate id
-type stream_id()    :: binary().
-type version()      :: non_neg_integer().
-type snapshot_data() :: map() | binary().

-export_type([state/0, source_id/0, stream_id/0, version/0, snapshot_data/0]).

-callback init(Opts :: map()) ->
    {ok, state()} | {error, term()}.

-callback close(state()) -> ok.

-callback save(state(), source_id(), stream_id(), version(), snapshot_data()) ->
    ok | {error, term()}.

-callback load(state(), source_id(), stream_id()) ->
    {ok, #{version := version(), data := snapshot_data()}}
    | {error, not_found}
    | {error, term()}.

-callback load_at(state(), source_id(), stream_id(), version()) ->
    {ok, snapshot_data()} | {error, not_found} | {error, term()}.

-callback delete(state(), source_id(), stream_id()) ->
    ok | {error, term()}.

-callback list(state(), stream_id()) ->
    {ok, [#{version := version(), source_id := source_id()}]}
    | {error, term()}.

-callback exists(state(), source_id(), stream_id()) -> boolean().
