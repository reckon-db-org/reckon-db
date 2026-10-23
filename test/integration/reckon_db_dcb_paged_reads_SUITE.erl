%%% @doc Paged DCB reads by type, tag and payload, and acknowledged
%%% snapshot writes, answered by the gateway worker (reckon-gater 3.15.0's
%%% requests; evoq#6 part two, reckon-evoq#1).
%%%
%%% The limit-only DCB reads return the first Limit matches and cannot go
%%% on, so an evoq decision over more than one page of context could not be
%%% read. These requests page through every matching DCB event in DCB
%%% sequence order with a stateless cursor. And record_snapshot/delete_snapshot
%%% are casts, so a checkpoint store built on them never learns whether a
%%% write happened or in which order two landed: the _sync requests are calls.
%%%
%%% Requests go through reckon_db_gateway_worker:handle_call/3 itself, with
%%% the two-field state dispatch reads (see reckon_db_streams_SUITE), so the
%%% wire contract is what is tested. A real Khepri store per test.
%%% @end
-module(reckon_db_dcb_paged_reads_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("../../include/reckon_db.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([pages_by_event_type_in_sequence_order/1,
         pages_by_tags_any_and_all/1,
         pages_by_payload/1,
         pages_by_payload_hash/1,
         an_append_after_a_page_lands_after_its_cursor/1,
         a_cursor_is_bound_to_its_read/1,
         a_bad_request_is_refused/1,
         an_empty_read_is_done/1,
         a_sync_snapshot_write_is_acknowledged/1,
         an_integrity_refusal_reaches_the_caller/1,
         a_bare_snapshot_record_is_saved/1,
         a_bare_snapshot_record_cast_is_saved/1]).

all() ->
    [pages_by_event_type_in_sequence_order,
     pages_by_tags_any_and_all,
     pages_by_payload,
     pages_by_payload_hash,
     an_append_after_a_page_lands_after_its_cursor,
     a_cursor_is_bound_to_its_read,
     a_bad_request_is_refused,
     an_empty_read_is_done,
     a_sync_snapshot_write_is_acknowledged,
     an_integrity_refusal_reaches_the_caller,
     a_bare_snapshot_record_is_saved,
     a_bare_snapshot_record_cast_is_saved].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),
    [{base_data_dir, "/tmp/reckon_db_dcb_paged_reads_SUITE"} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    %% Per-store ra_system pattern, matches reckon_db_store production
    %% setup. Sufficient for everything except subscription-delivery
    %% tests, which require manual emitter wiring (see the
    %% reckon_db_subscription_delivery_SUITE pattern). Phase 2 interop
    %% test deferred to P3.2 follow-up.
    Rand = integer_to_list(erlang:unique_integer([positive])),
    Base = proplists:get_value(base_data_dir, Config),
    RaDataDir = Base ++ "_ra_" ++ atom_to_list(Case) ++ "_" ++ Rand,
    StoreDataDir = Base ++ "_store_" ++ atom_to_list(Case) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ RaDataDir),
    os:cmd("rm -rf " ++ StoreDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    ok = filelib:ensure_dir(filename:join(StoreDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),
    {ok, _} = ra:start([{data_dir, RaDataDir}]),
    StoreId = list_to_atom("dcb_paged_" ++ Rand),
    RaSystemName = list_to_atom("ra_" ++ atom_to_list(StoreId)),
    RaSystemConfig = (ra_system:default_config())#{
        name => RaSystemName,
        data_dir => StoreDataDir,
        wal_data_dir => StoreDataDir,
        names => ra_system:derive_names(RaSystemName)
    },
    case ra_system:start(RaSystemConfig) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok
    end,
    {ok, _} = application:ensure_all_started(khepri),
    {ok, _} = khepri:start(RaSystemName, StoreId, 5000),
    [{store_id, StoreId},
     {ra_data_dir, RaDataDir},
     {store_data_dir, StoreDataDir} | Config].

end_per_testcase(_Case, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    catch khepri:stop(StoreId),
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    os:cmd("rm -rf " ++ proplists:get_value(store_data_dir, Config)),
    ok.

%%====================================================================
%% Paged reads
%%====================================================================

%% 26 events, 24 of two types and 2 of a third: paging the two in pages of
%% 10 returns all 24 once, in sequence order, in pages of 10, 10 and 4, the
%% last with done.
pages_by_event_type_in_sequence_order(Config) ->
    StoreId = ?config(store_id, Config),
    [append(StoreId, type_of(N), [], #{n => N}) || N <- lists:seq(1, 26)],
    Pages = all_pages(StoreId, fun(After) ->
                {dcb_read_by_event_types_page, StoreId, [<<"a_v1">>, <<"b_v1">>], After, 10}
            end),
    ?assertEqual([10, 10, 4], [length(Es) || {Es, _} <- Pages]),
    ?assertEqual(done, element(2, lists:last(Pages))),
    Ns = [n(E) || {Es, _} <- Pages, E <- Es],
    ?assertEqual([N || N <- lists:seq(1, 26), type_of(N) =/= <<"c_v1">>], Ns).

pages_by_tags_any_and_all(Config) ->
    StoreId = ?config(store_id, Config),
    [append(StoreId, <<"a_v1">>, tags_of(N), #{n => N}) || N <- lists:seq(1, 12)],
    Any = [n(E) || {Es, _} <- all_pages(StoreId, fun(After) ->
                       {dcb_read_by_tags_page, StoreId, [<<"two">>, <<"three">>], any, After, 3}
                   end), E <- Es],
    ?assertEqual([N || N <- lists:seq(1, 12), N rem 2 =:= 0 orelse N rem 3 =:= 0], Any),
    All = [n(E) || {Es, _} <- all_pages(StoreId, fun(After) ->
                       {dcb_read_by_tags_page, StoreId, [<<"two">>, <<"three">>], all, After, 1}
                   end), E <- Es],
    ?assertEqual([6, 12], All).

pages_by_payload(Config) ->
    StoreId = ?config(store_id, Config),
    declare(StoreId, [{payload, <<"order">>}]),
    [append(StoreId, <<"a_v1">>, [], #{<<"order">> => order_of(N), n => N})
     || N <- lists:seq(1, 9)],
    Ns = [n(E) || {Es, _} <- all_pages(StoreId, fun(After) ->
                      {dcb_read_by_payload_page, StoreId, <<"order">>, <<"o1">>, After, 2}
                  end), E <- Es],
    ?assertEqual([1, 4, 7], Ns).

pages_by_payload_hash(Config) ->
    StoreId = ?config(store_id, Config),
    declare(StoreId, [{payload_hash, [<<"order">>, <<"line">>]}]),
    [append(StoreId, <<"a_v1">>, [],
            #{<<"order">> => order_of(N), <<"line">> => <<"l">>, n => N})
     || N <- lists:seq(1, 9)],
    Ns = [n(E) || {Es, _} <- all_pages(StoreId, fun(After) ->
                      {dcb_read_by_payload_hash_page, StoreId, [<<"order">>, <<"line">>],
                       [<<"o2">>, <<"l">>], After, 2}
                  end), E <- Es],
    ?assertEqual([2, 5, 8], Ns).

%% The sequence number is assigned inside the append transaction, so an
%% event appended after a page was read sorts after that page's cursor:
%% the next page has it.
an_append_after_a_page_lands_after_its_cursor(Config) ->
    StoreId = ?config(store_id, Config),
    [append(StoreId, <<"a_v1">>, [], #{n => N}) || N <- lists:seq(1, 3)],
    {ok, {First, Cursor}} = call(StoreId, {dcb_read_by_event_types_page, StoreId,
                                           [<<"a_v1">>], start, 3}),
    ?assertEqual([1, 2, 3], [n(E) || E <- First]),
    ?assertEqual(done, Cursor),
    {ok, {Two, Next}} = call(StoreId, {dcb_read_by_event_types_page, StoreId,
                                       [<<"a_v1">>], start, 2}),
    ?assertEqual([1, 2], [n(E) || E <- Two]),
    append(StoreId, <<"a_v1">>, [], #{n => 4}),
    {ok, {Rest, done}} = call(StoreId, {dcb_read_by_event_types_page, StoreId,
                                        [<<"a_v1">>], Next, 10}),
    ?assertEqual([3, 4], [n(E) || E <- Rest]).

%% A cursor is valid only for the read and arguments that returned it.
a_cursor_is_bound_to_its_read(Config) ->
    StoreId = ?config(store_id, Config),
    [append(StoreId, <<"a_v1">>, [<<"two">>], #{n => N}) || N <- lists:seq(1, 3)],
    {ok, {_, Cursor}} = call(StoreId, {dcb_read_by_event_types_page, StoreId,
                                       [<<"a_v1">>], start, 1}),
    ?assertEqual({error, {invalid_cursor, Cursor}},
                 call(StoreId, {dcb_read_by_event_types_page, StoreId, [<<"b_v1">>], Cursor, 1})),
    ?assertEqual({error, {invalid_cursor, Cursor}},
                 call(StoreId, {dcb_read_by_tags_page, StoreId, [<<"two">>], any, Cursor, 1})),
    ?assertEqual({error, {invalid_cursor, <<"junk">>}},
                 call(StoreId, {dcb_read_by_event_types_page, StoreId, [<<"a_v1">>],
                                <<"junk">>, 1})).

a_bad_request_is_refused(Config) ->
    StoreId = ?config(store_id, Config),
    ?assertEqual({error, {invalid_page_request, {limit, 0}}},
                 call(StoreId, {dcb_read_by_event_types_page, StoreId, [<<"a_v1">>], start, 0})),
    ?assertEqual({error, {invalid_page_request, {match, some}}},
                 call(StoreId, {dcb_read_by_tags_page, StoreId, [<<"t">>], some, start, 5})).

an_empty_read_is_done(Config) ->
    StoreId = ?config(store_id, Config),
    ?assertEqual({ok, {[], done}},
                 call(StoreId, {dcb_read_by_event_types_page, StoreId, [<<"a_v1">>], start, 5})).

%%====================================================================
%% Acknowledged snapshot writes
%%====================================================================

a_sync_snapshot_write_is_acknowledged(Config) ->
    StoreId = ?config(store_id, Config),
    Stream = <<"$evoq-checkpoint:order_summary">>,
    ?assertEqual(ok, call(StoreId, {record_snapshot_sync, StoreId, Stream, Stream, 41,
                                    #{data => #{checkpoint => 41}, metadata => #{}}})),
    ?assertMatch({ok, #snapshot{version = 41, data = #{checkpoint := 41}}},
                 reckon_db_snapshots:load(StoreId, Stream)),
    ?assertEqual(ok, call(StoreId, {delete_snapshot_sync, StoreId, Stream, Stream, 41})),
    ?assertEqual({error, not_found}, reckon_db_snapshots:load(StoreId, Stream)).

%% A store with integrity on seals a snapshot with an anchor into its
%% stream's event chain at that version, and refuses one it cannot anchor.
%% The cast drops that refusal with a log line; the call tells the caller.
an_integrity_refusal_reaches_the_caller(Config) ->
    StoreId = ?config(store_id, Config),
    %% A user stream with no event at version 41: nothing to anchor to, so
    %% the save is refused (a "$" stream with no events is sealed instead,
    %% reckon-db#25).
    Stream = <<"order-018f6a7b8c9d4e5f60718293a4b5c6d7">>,
    enable_integrity(StoreId),
    try
        ?assertMatch({error, _},
                     call(StoreId, {record_snapshot_sync, StoreId, Stream, Stream, 41,
                                    #{data => #{checkpoint => 41}}})),
        ?assertEqual({error, not_found}, reckon_db_snapshots:load(StoreId, Stream))
    after
        persistent_term:erase({reckon_db, integrity_enabled, StoreId}),
        persistent_term:erase({reckon_db, integrity_key, StoreId})
    end.

%% Any gateway caller can send a record that is not a map. maps:get on it
%% raised badmap and took the worker down with every call in flight on it
%% (the reckon-db#4 class). A bare term is the snapshot's data.
a_bare_snapshot_record_is_saved(Config) ->
    StoreId = ?config(store_id, Config),
    Stream = <<"$evoq-checkpoint:order_summary">>,
    ?assertEqual(ok, call(StoreId, {record_snapshot_sync, StoreId, Stream, Stream, 3,
                                    <<"bare state">>})),
    ?assertMatch({ok, #snapshot{data = <<"bare state">>, metadata = #{}}},
                 reckon_db_snapshots:load(StoreId, Stream)).

%% The cast twin unwraps the record the same way.
a_bare_snapshot_record_cast_is_saved(Config) ->
    StoreId = ?config(store_id, Config),
    Stream = <<"$evoq-checkpoint:order_summary">>,
    State = {state, StoreId, undefined},
    ?assertMatch({noreply, _},
                 reckon_db_gateway_worker:handle_cast(
                   {record_snapshot, StoreId, Stream, Stream, 3, <<"bare state">>}, State)),
    ?assertMatch({ok, #snapshot{data = <<"bare state">>}},
                 reckon_db_snapshots:load(StoreId, Stream)).

enable_integrity(StoreId) ->
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),
    persistent_term:put({reckon_db, integrity_key, StoreId}, crypto:strong_rand_bytes(32)).

%%====================================================================
%% Helpers
%%====================================================================

call(StoreId, Request) ->
    State = {state, StoreId, undefined},
    {reply, Reply, State} = reckon_db_gateway_worker:handle_call(Request, self(), State),
    Reply.

%% Every page of a read, following the cursor until done.
all_pages(StoreId, Request) ->
    all_pages(StoreId, Request, start, []).

all_pages(StoreId, Request, After, Acc) ->
    {ok, {Events, Next}} = call(StoreId, Request(After)),
    next_pages(Next, StoreId, Request, [{Events, Next} | Acc]).

next_pages(done, _StoreId, _Request, Acc) -> lists:reverse(Acc);
next_pages(Cursor, StoreId, Request, Acc) -> all_pages(StoreId, Request, Cursor, Acc).

append(StoreId, Type, Tags, Data) ->
    {ok, _} = reckon_db_dcb:append_if_no_tag_matches(
                StoreId, {any_of, [<<"never-used">>]}, -1,
                [#{event_type => Type, tags => Tags, data => Data}]).

declare(StoreId, Indexes) ->
    ok = reckon_db_index_config:load(#store_config{store_id = StoreId, indexes = Indexes}).

n(#event{data = Data}) -> maps:get(n, Data).

type_of(N) when N rem 9 =:= 0 -> <<"c_v1">>;
type_of(N) when N rem 2 =:= 0 -> <<"b_v1">>;
type_of(_) -> <<"a_v1">>.

tags_of(N) ->
    [T || {T, D} <- [{<<"two">>, 2}, {<<"three">>, 3}], N rem D =:= 0].

order_of(N) -> <<"o", (integer_to_binary((N - 1) rem 3 + 1))/binary>>.
