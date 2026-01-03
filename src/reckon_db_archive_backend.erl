%% @doc Archive backend behaviour for reckon-db
%%
%% Defines the interface for archive storage backends.
%% Implementations can store archived events in various formats:
%% - Local files
%% - S3/object storage
%% - Network storage
%%
%% @author rgfaber

-module(reckon_db_archive_backend).

-include("reckon_db.hrl").

%% API
-export([make_key/4, parse_key/1]).

%%====================================================================
%% Callbacks
%%====================================================================

%% Initialize the archive backend with options.
-callback init(Opts :: map()) ->
    {ok, State :: term()} | {error, Reason :: term()}.

%% Archive a list of events.
%% The archive key is typically based on store_id, stream_id, and timestamp range.
-callback archive(State :: term(), ArchiveKey :: binary(), Events :: [event()]) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

%% Read archived events by key.
-callback read(State :: term(), ArchiveKey :: binary()) ->
    {ok, [event()], NewState :: term()} | {error, Reason :: term()}.

%% List all archive keys for a stream.
-callback list(State :: term(), StoreId :: atom(), StreamId :: binary()) ->
    {ok, [binary()], NewState :: term()} | {error, Reason :: term()}.

%% Delete an archive.
-callback delete(State :: term(), ArchiveKey :: binary()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

%% Check if an archive exists.
-callback exists(State :: term(), ArchiveKey :: binary()) ->
    {boolean(), NewState :: term()}.

%%====================================================================
%% Utility functions
%%====================================================================

%% @doc Generate a standard archive key.
-spec make_key(atom(), binary(), integer(), integer()) -> binary().
make_key(StoreId, StreamId, FromVersion, ToVersion) ->
    iolist_to_binary([
        atom_to_binary(StoreId, utf8), <<"/">>,
        StreamId, <<"/">>,
        integer_to_binary(FromVersion), <<"-">>,
        integer_to_binary(ToVersion), <<".archive">>
    ]).

%% @doc Parse an archive key to extract metadata.
-spec parse_key(binary()) -> {ok, map()} | {error, invalid_key}.
parse_key(Key) ->
    case binary:split(Key, <<"/">>, [global]) of
        [StoreIdBin, StreamId, VersionRange] ->
            case parse_version_range(VersionRange) of
                {ok, FromVersion, ToVersion} ->
                    {ok, #{
                        store_id => binary_to_atom(StoreIdBin, utf8),
                        stream_id => StreamId,
                        from_version => FromVersion,
                        to_version => ToVersion
                    }};
                error ->
                    {error, invalid_key}
            end;
        _ ->
            {error, invalid_key}
    end.

%% @private Parse version range from "N-M.archive" format
-spec parse_version_range(binary()) -> {ok, integer(), integer()} | error.
parse_version_range(VersionRange) ->
    case binary:split(VersionRange, <<".archive">>) of
        [Range, <<>>] ->
            case binary:split(Range, <<"-">>) of
                [FromBin, ToBin] ->
                    try
                        {ok, binary_to_integer(FromBin), binary_to_integer(ToBin)}
                    catch
                        _:_ -> error
                    end;
                _ ->
                    error
            end;
        _ ->
            error
    end.
