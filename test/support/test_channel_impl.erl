%% @doc Test channel implementation for authorization tests
%%
%% Minimal channel implementation used by esdb_channel_auth_tests.
%%
%% @author Macula.io

-module(test_channel_impl).
-behaviour(esdb_channel).

-export([init/1, handle_publish/3, handle_subscribe/3, handle_unsubscribe/3,
         handle_message/2, terminate/2]).

init(_Opts) -> {ok, #{}}.
handle_publish(_Topic, _Msg, State) -> {ok, State}.
handle_subscribe(_Topic, _Pid, State) -> {ok, State}.
handle_unsubscribe(_Topic, _Pid, State) -> {ok, State}.
handle_message(_Msg, State) -> {ok, State}.
terminate(_Reason, _State) -> ok.
