%% Diagnostic NIF loader — mirrors reckon_db_hash_nif's init/0 but
%% surfaces the load_nif error directly via io:format instead of
%% via logger:info (which gets filtered in rebar3 shell). Used to
%% triage NIF load failures during the embedded-NIF migration.
-module(diagnose_nif).
-export([try_load/1]).
-on_load(init/0).

init() ->
    %% Don't actually try to load anything at module load time —
    %% we'll do that explicitly via try_load/1.
    ok.

try_load(NifBaseName) when is_list(NifBaseName) ->
    PrivDir = code:priv_dir(reckon_db),
    Path = filename:join(PrivDir, NifBaseName),
    io:format("[diag] attempting load_nif(~p, 0)~n", [Path]),
    case erlang:load_nif(Path, 0) of
        ok ->
            io:format("[diag] load_nif: OK~n");
        {error, Reason} ->
            io:format("[diag] load_nif: ERROR ~p~n", [Reason])
    end.
