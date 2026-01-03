%% @doc File-based archive backend for reckon-db
%%
%% Stores archived events as Erlang term files on the local filesystem.
%% Archives are organized in directories by store and stream.
%%
%% Directory structure:
%% ```
%% {base_dir}/
%%   {store_id}/
%%     {stream_id}/
%%       0-99.archive
%%       100-199.archive
%% '''
%%
%% @author rgfaber

-module(reckon_db_archive_file).
-behaviour(reckon_db_archive_backend).

-include("reckon_db.hrl").

%% Behaviour callbacks
-export([
    init/1,
    archive/3,
    read/2,
    list/3,
    delete/2,
    exists/2
]).

-record(state, {
    base_dir :: string()
}).

%%====================================================================
%% Behaviour callbacks
%%====================================================================

%% @doc Initialize the file archive backend.
%%
%% Options:
%% - base_dir: Directory where archives are stored (required)
-spec init(map()) -> {ok, #state{}} | {error, term()}.
init(Opts) ->
    BaseDir = maps:get(base_dir, Opts),
    case filelib:ensure_dir(BaseDir ++ "/") of
        ok ->
            {ok, #state{base_dir = BaseDir}};
        {error, Reason} ->
            {error, {failed_to_create_dir, Reason}}
    end.

%% @doc Archive events to a file.
-spec archive(#state{}, binary(), [event()]) -> {ok, #state{}} | {error, term()}.
archive(#state{base_dir = BaseDir} = State, ArchiveKey, Events) ->
    FilePath = key_to_path(BaseDir, ArchiveKey),

    %% Ensure directory exists
    case filelib:ensure_dir(FilePath) of
        ok ->
            %% Write events as Erlang term
            Data = #{
                version => 1,
                created_at => erlang:system_time(millisecond),
                event_count => length(Events),
                events => Events
            },
            case file:write_file(FilePath, term_to_binary(Data, [compressed])) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, {write_failed, Reason}}
            end;
        {error, Reason} ->
            {error, {failed_to_create_dir, Reason}}
    end.

%% @doc Read events from an archive file.
-spec read(#state{}, binary()) -> {ok, [event()], #state{}} | {error, term()}.
read(#state{base_dir = BaseDir} = State, ArchiveKey) ->
    FilePath = key_to_path(BaseDir, ArchiveKey),
    case file:read_file(FilePath) of
        {ok, Binary} ->
            try
                #{events := Events} = binary_to_term(Binary),
                {ok, Events, State}
            catch
                _:_ ->
                    {error, {corrupted_archive, ArchiveKey}}
            end;
        {error, enoent} ->
            {error, {archive_not_found, ArchiveKey}};
        {error, Reason} ->
            {error, {read_failed, Reason}}
    end.

%% @doc List all archive keys for a stream.
-spec list(#state{}, atom(), binary()) -> {ok, [binary()], #state{}} | {error, term()}.
list(#state{base_dir = BaseDir} = State, StoreId, StreamId) ->
    StreamDir = filename:join([BaseDir, atom_to_list(StoreId), binary_to_list(StreamId)]),
    case file:list_dir(StreamDir) of
        {ok, Files} ->
            %% Filter for .archive files and convert to keys
            ArchiveFiles = [F || F <- Files, filename:extension(F) =:= ".archive"],
            Keys = [path_to_key(StoreId, StreamId, F) || F <- ArchiveFiles],
            {ok, Keys, State};
        {error, enoent} ->
            {ok, [], State};
        {error, Reason} ->
            {error, {list_failed, Reason}}
    end.

%% @doc Delete an archive file.
-spec delete(#state{}, binary()) -> {ok, #state{}} | {error, term()}.
delete(#state{base_dir = BaseDir} = State, ArchiveKey) ->
    FilePath = key_to_path(BaseDir, ArchiveKey),
    case file:delete(FilePath) of
        ok ->
            {ok, State};
        {error, enoent} ->
            {ok, State}; %% Already deleted
        {error, Reason} ->
            {error, {delete_failed, Reason}}
    end.

%% @doc Check if an archive exists.
-spec exists(#state{}, binary()) -> {boolean(), #state{}}.
exists(#state{base_dir = BaseDir} = State, ArchiveKey) ->
    FilePath = key_to_path(BaseDir, ArchiveKey),
    {filelib:is_regular(FilePath), State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Convert archive key to file path
-spec key_to_path(string(), binary()) -> string().
key_to_path(BaseDir, ArchiveKey) ->
    %% Key format: "store_id/stream_id/N-M.archive"
    filename:join(BaseDir, binary_to_list(ArchiveKey)).

%% @private Convert file name to archive key
-spec path_to_key(atom(), binary(), string()) -> binary().
path_to_key(StoreId, StreamId, FileName) ->
    iolist_to_binary([
        atom_to_binary(StoreId, utf8), <<"/">>,
        StreamId, <<"/">>,
        list_to_binary(FileName)
    ]).
