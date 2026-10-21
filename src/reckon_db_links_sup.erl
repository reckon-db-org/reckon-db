%% @doc Supervisor for the running links of one store.
%%
%% A link owns one process while it is running: `reckon_db_link_worker', which
%% subscribes, fences each source stream, backfills to that fence and then
%% writes what arrives live. It is a process and not a handler fun because the
%% fence has to survive between events - a link must link an event appended
%% during its own start exactly once, and a fun stored in a subscription is
%% deserialised fresh on every call, so it cannot remember one.
%%
%% Children are started by `reckon_db_links:start/2' and stopped by
%% `reckon_db_links:stop/2'; this supervisor only owns them.
-module(reckon_db_links_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% @doc Start the links supervisor for a store.
-spec start_link(atom()) -> {ok, pid()} | {error, term()}.
start_link(StoreId) ->
    Name = reckon_db_naming:links_sup_name(StoreId),
    supervisor:start_link({local, Name}, ?MODULE, StoreId).

%% @private
-spec init(atom()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_StoreId) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    {ok, {SupFlags, []}}.
