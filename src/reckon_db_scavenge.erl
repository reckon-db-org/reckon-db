%% @doc Scavenging and archival for reckon-db
%%
%% Provides functionality to:
%% - Remove old events beyond a retention period
%% - Optionally archive events before deletion
%% - Maintain stream integrity by requiring snapshots
%%
%% Use cases:
%% - Reduce storage costs by removing old events
%% - Comply with data retention policies
%% - Archive events to cold storage
%%
%% Safety guarantees:
%% - By default, requires a snapshot before scavenging
%% - Supports dry-run mode for previewing changes
%% - Telemetry events for monitoring
%%
%% @author rgfaber

-module(reckon_db_scavenge).

-include("reckon_db.hrl").

%% API
-export([
    scavenge/3,
    scavenge_matching/3,
    archive_and_scavenge/4,
    dry_run/3
]).

%%====================================================================
%% Types
%%====================================================================

-type scavenge_opts() :: #{
    before => integer(),           %% Delete events before this timestamp (epoch_us)
    before_version => integer(),   %% Delete events before this version
    keep_versions => pos_integer(),%% Keep at least N latest versions
    require_snapshot => boolean(), %% Require snapshot exists before scavenging (default: true)
    dry_run => boolean()           %% Preview only, don't delete (default: false)
}.

-type scavenge_result() :: #{
    stream_id := binary(),
    deleted_count := non_neg_integer(),
    deleted_versions := {non_neg_integer(), non_neg_integer()}, %% {from, to}
    archived := boolean(),
    archive_key => binary()
}.

-export_type([scavenge_opts/0, scavenge_result/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Scavenge a single stream.
%%
%% Deletes old events based on the provided options.
%% By default, requires a snapshot to exist (safety measure).
%%
%% Options:
%% - `before`: Delete events with epoch_us before this timestamp
%% - `before_version`: Delete events before this version (alternative to `before`)
%% - `keep_versions`: Always keep at least N latest versions (default: 0)
%% - `require_snapshot`: Require snapshot exists (default: true)
%% - `dry_run`: Preview only, don't delete (default: false)
%%
%% Example:
%% ```
%% %% Delete events older than 1 year, keep last 10 versions
%% OneYearAgo = erlang:system_time(microsecond) - (365 * 24 * 60 * 60 * 1000000),
%% {ok, Result} = reckon_db_scavenge:scavenge(my_store, <<"orders-123">>, #{
%%     before => OneYearAgo,
%%     keep_versions => 10
%% }).
%% '''
-spec scavenge(atom(), binary(), scavenge_opts()) ->
    {ok, scavenge_result()} | {error, term()}.
scavenge(StoreId, StreamId, Opts) ->
    StartTime = erlang:monotonic_time(),

    Result = do_scavenge(StoreId, StreamId, Opts),

    Duration = erlang:monotonic_time() - StartTime,
    emit_telemetry(StoreId, StreamId, Result, Duration),

    Result.

%% @doc Scavenge all streams matching a pattern.
%%
%% Pattern uses shell-like wildcards (e.g., "orders-*").
%% Applies the same options to all matching streams.
-spec scavenge_matching(atom(), binary(), scavenge_opts()) ->
    {ok, [scavenge_result()]} | {error, term()}.
scavenge_matching(StoreId, Pattern, Opts) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, AllStreams} ->
            MatchingStreams = filter_by_pattern(AllStreams, Pattern),
            Results = [scavenge(StoreId, StreamId, Opts) || StreamId <- MatchingStreams],
            SuccessResults = [R || {ok, R} <- Results],
            {ok, SuccessResults};
        {error, _} = Error ->
            Error
    end.

%% @doc Archive events to a backend, then scavenge.
%%
%% First archives events to the specified backend, then deletes them.
%% This ensures events are preserved before removal.
-spec archive_and_scavenge(atom(), binary(), {module(), term()}, scavenge_opts()) ->
    {ok, scavenge_result()} | {error, term()}.
archive_and_scavenge(StoreId, StreamId, {BackendMod, BackendState}, Opts) ->
    %% First, find events that would be scavenged
    case find_scavenge_candidates(StoreId, StreamId, Opts) of
        {ok, [], _} ->
            {ok, #{
                stream_id => StreamId,
                deleted_count => 0,
                deleted_versions => {0, 0},
                archived => false
            }};
        {ok, Events, {FromVersion, ToVersion}} ->
            %% Archive the events
            ArchiveKey = reckon_db_archive_backend:make_key(StoreId, StreamId, FromVersion, ToVersion),
            case BackendMod:archive(BackendState, ArchiveKey, Events) of
                {ok, _NewBackendState} ->
                    %% Now scavenge (with archive flag)
                    ScavengeOpts = Opts#{archived_key => ArchiveKey},
                    do_scavenge(StoreId, StreamId, ScavengeOpts);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Preview what would be scavenged without making changes.
-spec dry_run(atom(), binary(), scavenge_opts()) ->
    {ok, scavenge_result()} | {error, term()}.
dry_run(StoreId, StreamId, Opts) ->
    scavenge(StoreId, StreamId, Opts#{dry_run => true}).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
-spec do_scavenge(atom(), binary(), scavenge_opts()) ->
    {ok, scavenge_result()} | {error, term()}.
do_scavenge(StoreId, StreamId, Opts) ->
    RequireSnapshot = maps:get(require_snapshot, Opts, true),
    DryRun = maps:get(dry_run, Opts, false),

    %% Safety check: require snapshot unless explicitly disabled
    case RequireSnapshot of
        true ->
            case reckon_db_snapshots:exists(StoreId, StreamId) of
                true ->
                    do_scavenge_with_snapshot_check(StoreId, StreamId, Opts, DryRun);
                false ->
                    {error, {no_snapshot, StreamId}}
            end;
        false ->
            do_scavenge_with_snapshot_check(StoreId, StreamId, Opts, DryRun)
    end.

%% @private
-spec do_scavenge_with_snapshot_check(atom(), binary(), scavenge_opts(), boolean()) ->
    {ok, scavenge_result()} | {error, term()}.
do_scavenge_with_snapshot_check(StoreId, StreamId, Opts, DryRun) ->
    case find_scavenge_candidates(StoreId, StreamId, Opts) of
        {ok, [], _} ->
            {ok, #{
                stream_id => StreamId,
                deleted_count => 0,
                deleted_versions => {0, 0},
                archived => false
            }};
        {ok, Events, {FromVersion, ToVersion}} ->
            case DryRun of
                true ->
                    {ok, #{
                        stream_id => StreamId,
                        deleted_count => length(Events),
                        deleted_versions => {FromVersion, ToVersion},
                        archived => false,
                        dry_run => true
                    }};
                false ->
                    %% Actually delete the events
                    delete_event_versions(StoreId, StreamId, FromVersion, ToVersion),
                    Archived = maps:get(archived_key, Opts, undefined) =/= undefined,
                    Result = #{
                        stream_id => StreamId,
                        deleted_count => length(Events),
                        deleted_versions => {FromVersion, ToVersion},
                        archived => Archived
                    },
                    FinalResult = case maps:get(archived_key, Opts, undefined) of
                        undefined -> Result;
                        Key -> Result#{archive_key => Key}
                    end,
                    {ok, FinalResult}
            end;
        {error, _} = Error ->
            Error
    end.

%% @private Find events that should be scavenged
-spec find_scavenge_candidates(atom(), binary(), scavenge_opts()) ->
    {ok, [event()], {non_neg_integer(), non_neg_integer()}} | {error, term()}.
find_scavenge_candidates(StoreId, StreamId, Opts) ->
    case reckon_db_streams:exists(StoreId, StreamId) of
        false ->
            {error, {stream_not_found, StreamId}};
        true ->
            CurrentVersion = reckon_db_streams:get_version(StoreId, StreamId),
            KeepVersions = maps:get(keep_versions, Opts, 0),

            %% Determine cutoff version
            CutoffVersion = determine_cutoff_version(StoreId, StreamId, Opts, CurrentVersion),

            %% Ensure we keep the minimum number of versions
            SafeCutoff = min(CutoffVersion, CurrentVersion - KeepVersions),

            case SafeCutoff < 0 of
                true ->
                    {ok, [], {0, 0}};
                false ->
                    %% Read events up to the cutoff
                    case reckon_db_streams:read(StoreId, StreamId, 0, SafeCutoff + 1, forward) of
                        {ok, Events} ->
                            FromVersion = 0,
                            ToVersion = SafeCutoff,
                            {ok, Events, {FromVersion, ToVersion}};
                        {error, _} = Error ->
                            Error
                    end
            end
    end.

%% @private Determine the cutoff version based on options
-spec determine_cutoff_version(atom(), binary(), scavenge_opts(), integer()) -> integer().
determine_cutoff_version(StoreId, StreamId, Opts, _CurrentVersion) ->
    case maps:find(before_version, Opts) of
        {ok, Version} ->
            Version - 1;
        error ->
            case maps:find(before, Opts) of
                {ok, Timestamp} ->
                    %% Find version at timestamp
                    case reckon_db_temporal:version_at(StoreId, StreamId, Timestamp) of
                        {ok, Version} when Version >= 0 ->
                            Version;
                        _ ->
                            -1
                    end;
                error ->
                    %% No cutoff specified, don't delete anything
                    -1
            end
    end.

%% @private Delete events from version range
-spec delete_event_versions(atom(), binary(), non_neg_integer(), non_neg_integer()) -> ok.
delete_event_versions(StoreId, StreamId, FromVersion, ToVersion) ->
    %% Delete each version's event from Khepri
    lists:foreach(
        fun(Version) ->
            PaddedVersion = pad_version(Version, ?VERSION_PADDING),
            Path = ?STREAMS_PATH ++ [StreamId, PaddedVersion],
            khepri:delete(StoreId, Path)
        end,
        lists:seq(FromVersion, ToVersion)
    ),
    ok.

%% @private Pad version to fixed length
-spec pad_version(non_neg_integer(), pos_integer()) -> binary().
pad_version(Version, Length) ->
    VersionStr = integer_to_list(Version),
    Padding = Length - length(VersionStr),
    PaddedStr = lists:duplicate(Padding, $0) ++ VersionStr,
    list_to_binary(PaddedStr).

%% @private Filter streams by pattern (simple wildcard matching)
-spec filter_by_pattern([binary()], binary()) -> [binary()].
filter_by_pattern(Streams, Pattern) ->
    RegexPattern = wildcard_to_regex(Pattern),
    [S || S <- Streams, re:run(S, RegexPattern) =/= nomatch].

%% @private Convert wildcard pattern to regex
-spec wildcard_to_regex(binary()) -> binary().
wildcard_to_regex(Pattern) ->
    %% Escape special regex characters, convert * to .*
    Escaped = re:replace(Pattern, <<"[.^$+?{}\\[\\]\\\\|()]">>, <<"\\\\&">>, [global, {return, binary}]),
    Converted = binary:replace(Escaped, <<"*">>, <<".*">>, [global]),
    <<"^", Converted/binary, "$">>.

%% @private Emit telemetry for scavenge operations
-spec emit_telemetry(atom(), binary(), term(), integer()) -> ok.
emit_telemetry(StoreId, StreamId, Result, Duration) ->
    {DeletedCount, Archived} = case Result of
        {ok, #{deleted_count := DC, archived := A}} -> {DC, A};
        _ -> {0, false}
    end,

    telemetry:execute(
        [reckon_db, scavenge, complete],
        #{duration => Duration, deleted_count => DeletedCount},
        #{store_id => StoreId, stream_id => StreamId, archived => Archived}
    ),
    ok.
