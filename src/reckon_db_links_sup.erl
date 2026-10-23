%% @doc Supervises a store's running link consumers (reckon-db#24).
%%
%% One reckon_db_link_consumer per running link, started by
%% reckon_db_links:start/2 and by leader activation for links stored as
%% running. A consumer is transient: reckon_db_links:stop/2 ends it
%% normally and it stays down; a crash restarts it, and it resumes from
%% the positions stored with its link.
%%
%% @author rgfaber

-module(reckon_db_links_sup).
-behaviour(supervisor).

-include("reckon_db.hrl").

-export([start_link/1, start_consumer/2]).
-export([init/1]).

%% @doc Start the links supervisor of a store.
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(#store_config{store_id = StoreId}) ->
    supervisor:start_link({local, reckon_db_naming:links_sup_name(StoreId)}, ?MODULE, StoreId).

%% @doc Start a link's consumer. A consumer already running anywhere in the
%% cluster counts as started.
-spec start_consumer(atom(), binary()) -> ok | {error, term()}.
start_consumer(StoreId, Name) ->
    started(start_child(whereis(reckon_db_naming:links_sup_name(StoreId)), Name), StoreId).

start_child(undefined, _Name) ->
    {error, links_sup_not_running};
start_child(Sup, Name) ->
    supervisor:start_child(Sup, [Name]).

started({ok, _Pid}, _StoreId) -> ok;
started({error, {already_started, _Pid}}, _StoreId) -> ok;
started({error, links_sup_not_running}, StoreId) -> {error, {links_sup_not_running, StoreId}};
started({error, _} = Error, _StoreId) -> Error.

%% @private
-spec init(atom()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(StoreId) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 60},
    Consumer = #{
        id => reckon_db_link_consumer,
        start => {reckon_db_link_consumer, start_link, [StoreId]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [reckon_db_link_consumer]
    },
    {ok, {SupFlags, [Consumer]}}.
