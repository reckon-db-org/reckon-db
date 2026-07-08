%% @doc System resource sampling for reckon-db (CPU + disk).
%%
%% A node-wide singleton gen_server that samples host CPU and disk usage on a
%% fixed interval and emits telemetry gauges — unlike `reckon_db_memory', which
%% fires only when a pressure LEVEL changes, this emits a fresh sample every
%% tick so consumers can graph a live series.
%%
%% Telemetry (see reckon_db_telemetry.hrl):
%% <ul>
%%   <li>`[reckon_db, cpu, sample]' — measurements `busy_percent', `load1',
%%       `load5', `load15'; metadata `cores'.</li>
%%   <li>`[reckon_db, disk, sample]' — one per mount; measurements
%%       `used_percent', `total_kb', `available_kb'; metadata `mount',
%%       `data_dir_mount'.</li>
%% </ul>
%%
%% CPU/disk come from `cpu_sup'/`disksup' (os_mon). The monitor tries to start
%% os_mon on init and DEGRADES GRACEFULLY if it can't: `get_stats/0' reports
%% `os_mon => false' and no CPU/disk telemetry is emitted (there is no
%% pure-BEAM fallback for host CPU% or disk usage). Started from
%% `reckon_db_sup' when `resource_monitoring' is enabled (the default).
%%
%% @author rgfaber
-module(reckon_db_resource_monitor).
-behaviour(gen_server).

-include("reckon_db.hrl").
-include("reckon_db_telemetry.hrl").

-export([start_link/0, start_link/1, get_stats/0, sample_now/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_INTERVAL, 10000).  %% 10s

-record(state, {
    interval   :: pos_integer(),
    data_dir   :: string() | undefined,
    os_mon     :: boolean(),
    timer      :: reference() | undefined,
    cpu        :: map() | undefined,
    disk = []  :: [map()],
    last_sample :: integer() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> start_link(#{}).

%% @doc Start the monitor. Config keys: `interval' (ms, default 10000),
%% `data_dir' (string; the store data root, to flag its mount — resolved from
%% the configured stores when omitted).
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

%% @doc Latest sampled resource snapshot:
%% `#{os_mon => boolean(), cpu => map()|undefined, disk => [map()],
%%    last_sample => ms}'.
-spec get_stats() -> map().
get_stats() -> gen_server:call(?SERVER, get_stats).

%% @doc Force an immediate sample and return the fresh snapshot.
-spec sample_now() -> map().
sample_now() -> gen_server:call(?SERVER, sample_now).

%%====================================================================
%% gen_server
%%====================================================================

init(Config) ->
    Interval = maps:get(interval, Config, ?DEFAULT_INTERVAL),
    OsMon = ensure_os_mon(),
    DataDir = resolve_data_dir(Config),
    %% Prime cpu_sup: its first util/0 reading is the since-boot baseline;
    %% discard it so the first real sample reflects the live interval.
    _ = (catch cpu_sup:util()),
    S0 = #state{interval = Interval, data_dir = DataDir, os_mon = OsMon},
    S1 = do_sample(S0),
    {ok, S1#state{timer = schedule(Interval)}}.

handle_call(get_stats, _From, State) ->
    {reply, stats_map(State), State};
handle_call(sample_now, _From, State) ->
    S1 = do_sample(State),
    {reply, stats_map(S1), S1};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(sample, State) ->
    S1 = do_sample(State),
    {noreply, S1#state{timer = schedule(S1#state.interval)}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{timer = Timer}) ->
    cancel(Timer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal
%%====================================================================

-spec ensure_os_mon() -> boolean().
ensure_os_mon() ->
    case application:ensure_all_started(os_mon) of
        {ok, _} -> true;
        _Error  -> false
    end.

resolve_data_dir(Config) ->
    case maps:get(data_dir, Config, undefined) of
        undefined -> data_dir_from_stores();
        Dir       -> Dir
    end.

%% @private Best-effort: the first configured store's data_dir, so we can flag
%% which mount actually holds event data.
data_dir_from_stores() ->
    try reckon_db_config:get_all_store_configs() of
        [#store_config{data_dir = D} | _] when is_list(D), D =/= [] -> D;
        _ -> undefined
    catch _:_ -> undefined end.

-spec do_sample(#state{}) -> #state{}.
do_sample(#state{data_dir = DD0} = State) ->
    %% Resolve the data dir lazily: at init the consuming app may not have
    %% configured its stores yet, so keep retrying until we can identify the
    %% mount that actually holds event data.
    DataDir = case DD0 of undefined -> resolve_data_dir(#{}); _ -> DD0 end,
    Cpu  = sample_cpu(),
    Disk = sample_disk(DataDir),
    emit_cpu(Cpu),
    emit_disk(Disk),
    State#state{data_dir = DataDir, cpu = Cpu, disk = Disk, last_sample = now_ms()}.

%% --- CPU ---

sample_cpu() ->
    Busy = case (catch cpu_sup:util()) of
               U when is_number(U) -> round_2(U);
               _ -> undefined
           end,
    #{busy_percent => Busy,
      load1  => load_avg(fun cpu_sup:avg1/0),
      load5  => load_avg(fun cpu_sup:avg5/0),
      load15 => load_avg(fun cpu_sup:avg15/0),
      cores  => cores()}.

%% cpu_sup:avgN/0 returns the load average * 256 (integer); normalise to a float.
load_avg(F) ->
    case (catch F()) of
        N when is_integer(N) -> round_2(N / 256);
        _ -> undefined
    end.

cores() ->
    case erlang:system_info(logical_processors_available) of
        N when is_integer(N) -> N;
        _ -> erlang:system_info(schedulers_online)
    end.

emit_cpu(#{busy_percent := undefined}) ->
    ok;  %% os_mon/cpu_sup unavailable — emit nothing
emit_cpu(#{busy_percent := Busy, load1 := L1, load5 := L5,
           load15 := L15, cores := Cores}) ->
    telemetry:execute(?CPU_SAMPLE,
        #{busy_percent => Busy, load1 => num(L1),
          load5 => num(L5), load15 => num(L15)},
        #{cores => Cores}).

%% --- Disk ---

%% Disk usage per mount. Primary source is `df' — it works in containers, where
%% disksup returns [] because it skips overlay/virtual filesystems (the only
%% root a container overlay has). Falls back to disksup on the rare host without
%% a `df' (df is POSIX and present on essentially every Unix).
sample_disk(DataDir) ->
    Rows = case parse_df(os:cmd("df -kP 2>/dev/null")) of
               []   -> disksup_rows();
               Real -> Real
           end,
    with_data_dir(DataDir, Rows).

%% Bare-metal fallback: disksup gives {MountId, TotalKb, UsedPct}.
disksup_rows() ->
    case (catch disksup:get_disk_data()) of
        Data when is_list(Data), Data =/= [] ->
            [#{mount => to_bin(Id), total_kb => T, used_percent => U,
               available_kb => round(T * (100 - U) / 100)}
             || {Id, T, U} <- Data, is_integer(T), T > 0];
        _ -> []
    end.

%% Parse `df -kP' (POSIX format = 6 columns, one line per mount, no wrapping):
%% Filesystem 1024-blocks Used Available Capacity Mounted-on
parse_df(Output) ->
    case string:split(Output, "\n", all) of
        [_Header | Lines] -> lists:filtermap(fun parse_df_line/1, Lines);
        _ -> []
    end.

parse_df_line(Line) ->
    case string:lexemes(Line, " ") of
        [Fs, Blocks, _Used, Avail, Cap | MountParts] when MountParts =/= [] ->
            Mount = lists:flatten(lists:join(" ", MountParts)),
            case real_fs(Fs) andalso parse_cols(Blocks, Avail, Cap) of
                {T, A, P} ->
                    {true, #{mount => list_to_binary(Mount), total_kb => T,
                             available_kb => A, used_percent => P}};
                _ -> false
            end;
        _ -> false
    end.

parse_cols(Blocks, Avail, Cap) ->
    case {to_int(Blocks), to_int(Avail), to_int(string:trim(Cap, trailing, "%"))} of
        {T, A, P} when is_integer(T), T > 0, is_integer(A), is_integer(P) -> {T, A, P};
        _ -> false
    end.

%% Keep only real filesystems, dropping the pseudo mounts df lists inside a
%% container (tmpfs, proc, sysfs, cgroup, mqueue, shm, devtmpfs, none, ...).
%% A real filesystem's device is either a path (`/dev/sda1', `/dev/mapper/...')
%% or the container root `overlay'; pseudo devices are bare names.
real_fs("overlay") -> true;
real_fs([$/ | _])  -> true;
real_fs(_)         -> false.

%% Add the data-dir mount as a distinct, flagged entry. Prefix-matching the
%% general `df' mounts is unreliable inside containers (busybox mislabels bind
%% mounts), so we `df' the data dir directly for its true usage, label it with
%% the data dir path, and drop any general row that is the SAME filesystem
%% (same total + available) to avoid a duplicate under the garbled label.
with_data_dir(undefined, Rows) ->
    [R#{data_dir_mount => false} || R <- Rows];
with_data_dir(DataDir, Rows) ->
    case df_path(DataDir) of
        undefined ->
            [R#{data_dir_mount => false} || R <- Rows];
        DEntry ->
            DT = maps:get(total_kb, DEntry),
            DA = maps:get(available_kb, DEntry),
            Same = fun(R) -> maps:get(total_kb, R) =:= DT
                             andalso maps:get(available_kb, R) =:= DA end,
            Others = [R#{data_dir_mount => false} || R <- Rows, not Same(R)],
            [DEntry#{data_dir_mount => true} | Others]
    end.

%% `df' one path -> the filesystem containing it. The usage IS the data disk's;
%% label it with the data dir path (df's mount-point column is unreliable for
%% bind mounts in containers).
df_path(DataDir) ->
    case parse_df(os:cmd("df -kP '" ++ DataDir ++ "' 2>/dev/null")) of
        [Row | _] -> Row#{mount => list_to_binary(DataDir)};
        _         -> undefined
    end.

to_int(S) ->
    case string:to_integer(S) of
        {N, _} when is_integer(N) -> N;
        _ -> undefined
    end.

to_bin(X) -> unicode:characters_to_binary(X).

emit_disk(Entries) ->
    lists:foreach(
        fun(#{mount := M, total_kb := T, used_percent := U,
              available_kb := A, data_dir_mount := DDM}) ->
            telemetry:execute(?DISK_SAMPLE,
                #{used_percent => U, total_kb => T, available_kb => A},
                #{mount => M, data_dir_mount => DDM})
        end, Entries).

%% --- helpers ---

stats_map(#state{os_mon = OsMon, cpu = Cpu, disk = Disk, last_sample = LS}) ->
    #{os_mon => OsMon, cpu => Cpu, disk => Disk, last_sample => LS}.

num(undefined) -> 0;
num(N) -> N.

round_2(F) -> round(F * 100) / 100.

now_ms() -> erlang:system_time(millisecond).

schedule(Interval) -> erlang:send_after(Interval, self(), sample).

cancel(undefined) -> ok;
cancel(Ref) -> erlang:cancel_timer(Ref), ok.
