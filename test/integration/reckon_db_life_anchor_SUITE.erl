%% @doc Common Test suite for the node-local life anchor.
%%
%% The anchor is the one witness outside the tree: a MAC'd, MAC-chained,
%% append-only log beside the store's data directory naming each stream's
%% current life generation. A tree writer can restore an older life's seal,
%% life token and events together, and every one of them verifies against the
%% others; what they cannot do is raise a generation, because the generation is
%% bound into the seal's MAC under a key they do not hold.
%%
%% What this suite pins, claim by claim (r21 design F1-F9):
%%
%% - the read rule, one ETS lookup: a lower generation refuses, a higher one is
%%   adopted, the same generation with a different life refuses, a recorded life
%%   the tree no longer has refuses, and an absent anchor refuses everything;
%% - the tombstone: a delete leaves the generation behind, so a deleted stream
%%   cannot be resurrected, and the re-create that follows rises above it;
%% - the floor: a counter a tree writer lowered cannot pull the next generation
%%   down to where an entry or tombstone already sits (F5/F6);
%% - the intent: fsynced before the delete commits, and resolved against the
%%   tree at the next start (F2/F3/F4);
%% - the CAS: a mint that keeps losing the counter race refuses BY NAME (F9);
%% - rebuild: discards the log, takes the tree's word for every stream, and
%%   leaves a log behind that the next start agrees with (F1/F2);
%% - the log itself: what is recorded survives a restart, and a damaged tail
%%   costs the tail, never the head.
%% @end
-module(reckon_db_life_anchor_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("reckon_db.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    %% The read rule
    an_absent_anchor_refuses/1,
    a_restored_older_life_refuses/1,
    a_different_life_at_the_recorded_generation_refuses/1,
    a_higher_generation_is_adopted/1,
    a_recorded_life_the_tree_no_longer_has_refuses/1,
    a_tombstone_refuses_the_life_it_replaced/1,

    %% Generations across create/delete/re-create
    a_delete_keeps_the_generation_and_the_re_create_rises_above_it/1,
    a_lowered_counter_cannot_lower_the_next_generation/1,
    a_held_counter_race_refuses_by_name/1,

    %% Intents and resolution
    an_intent_whose_delete_committed_stays_a_tombstone/1,
    an_intent_whose_delete_never_committed_is_dropped/1,

    %% Rebuild
    rebuild_takes_the_tree_word_and_leaves_a_log_behind/1,

    %% The log
    what_was_recorded_survives_a_restart/1,
    a_damaged_tail_costs_the_tail_not_the_head/1
]).

all() ->
    [
        an_absent_anchor_refuses,
        a_restored_older_life_refuses,
        a_different_life_at_the_recorded_generation_refuses,
        a_higher_generation_is_adopted,
        a_recorded_life_the_tree_no_longer_has_refuses,
        a_tombstone_refuses_the_life_it_replaced,
        a_delete_keeps_the_generation_and_the_re_create_rises_above_it,
        a_lowered_counter_cannot_lower_the_next_generation,
        a_held_counter_race_refuses_by_name,
        an_intent_whose_delete_committed_stays_a_tombstone,
        an_intent_whose_delete_never_committed_is_dropped,
        rebuild_takes_the_tree_word_and_leaves_a_log_behind,
        what_was_recorded_survives_a_restart,
        a_damaged_tail_costs_the_tail_not_the_head
    ].

suite() ->
    [{timetrap, {seconds, 120}}].

%%====================================================================
%% Setup
%%====================================================================

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(crypto),
    {ok, _} = application:ensure_all_started(telemetry),

    RaDataDir = "/tmp/reckon_db_life_anchor_test_ra",
    os:cmd("rm -rf " ++ RaDataDir),
    ok = filelib:ensure_dir(filename:join(RaDataDir, "dummy")),
    application:set_env(ra, data_dir, RaDataDir),

    {ok, _} = application:ensure_all_started(ra),
    ok = ra:start(),
    {ok, _} = application:ensure_all_started(khepri),

    [{ra_data_dir, RaDataDir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ proplists:get_value(ra_data_dir, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    Rand = integer_to_list(erlang:unique_integer([positive])),
    DataDir = "/tmp/reckon_db_life_anchor_" ++ atom_to_list(TestCase) ++ "_" ++ Rand,
    os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    StoreId = list_to_atom("life_anchor_test_" ++ atom_to_list(TestCase) ++ "_" ++ Rand),

    {ok, _} = khepri:start(DataDir, StoreId),
    khepri:put(StoreId, [streams], #{}),
    khepri:put(StoreId, [metadata], #{}),

    Key = crypto:strong_rand_bytes(32),
    persistent_term:put({reckon_db, integrity_key, StoreId}, Key),
    persistent_term:put({reckon_db, integrity_enabled, StoreId}, true),

    [{data_dir, DataDir}, {store_id, StoreId}, {key, Key} | Config].

end_per_testcase(_TestCase, Config) ->
    StoreId = proplists:get_value(store_id, Config),
    DataDir = proplists:get_value(data_dir, Config),
    reckon_db_life_anchor:stop(StoreId),
    catch khepri:stop(StoreId),
    persistent_term:erase({reckon_db, integrity_key, StoreId}),
    persistent_term:erase({reckon_db, integrity_enabled, StoreId}),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% The read rule
%%====================================================================

%% An integrity-enabled read path with no anchor is a fault, and it must refuse
%% rather than serve: answering ok there is the whole failure the anchor exists
%% to stop.
an_absent_anchor_refuses(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"no-anchor">>),
    {refuse, Violation} = reckon_db_life_anchor:check(StoreId, StreamId, 1, <<"l">>),
    ?assertEqual(life_anchor_unavailable, detail(Violation)),
    ok.

%% A life restored from an older copy carries an older generation. Scheme-2
%% seals carry no generation at all and read as generation 0; every stream this
%% line creates is minted at 1 or above.
a_restored_older_life_refuses(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"older-life">>),
    start_anchor(Config),
    Life = crypto:strong_rand_bytes(16),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 1, Life),
    settle(),
    {refuse, Violation} = reckon_db_life_anchor:check(StoreId, StreamId, 0, Life),
    ?assertEqual(life_gen_rolled_back, detail(Violation)),
    ?assertEqual(1, recorded_gen(Violation)),
    ok.

%% Same generation, different life: a token that was never this generation's,
%% or one minted elsewhere that this node has not seen rise. Refused, never
%% adopted - adoption is only ever upward.
a_different_life_at_the_recorded_generation_refuses(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"same-gen-other-life">>),
    start_anchor(Config),
    Life = crypto:strong_rand_bytes(16),
    Other = crypto:strong_rand_bytes(16),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 1, Life),
    settle(),
    {refuse, Violation} = reckon_db_life_anchor:check(StoreId, StreamId, 1, Other),
    ?assertEqual(life_gen_mismatch, detail(Violation)),
    ?assertEqual({1, Life}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok.

%% A life minted after this node last looked - on another member, or by a
%% re-create - is ADOPTED, and the entry moves up.
a_higher_generation_is_adopted(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"adopt-up">>),
    start_anchor(Config),
    Life = crypto:strong_rand_bytes(16),
    NewLife = crypto:strong_rand_bytes(16),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 1, Life),
    settle(),
    ok = reckon_db_life_anchor:check(StoreId, StreamId, 2, NewLife),
    settle(),
    ?assertEqual({2, NewLife}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ?assertEqual(2, reckon_db_life_anchor:highest_gen(StoreId, StreamId)),
    ok.

%% The anchor knows a life and the tree holds none: a stream stripped to
%% chain-less (no seal, no life node) is refused, never read as a legacy
%% stream. That shape is the tree writer's cheapest way to hide one.
a_recorded_life_the_tree_no_longer_has_refuses(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"life-gone-from-tree">>),
    start_anchor(Config),
    Life = crypto:strong_rand_bytes(16),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 1, Life),
    settle(),
    {refuse, Violation} = reckon_db_life_anchor:check(StoreId, StreamId, 0, undefined),
    ?assertEqual(life_absent_in_tree, detail(Violation)),
    ok.

%% A tombstone is recorded at the generation the deleted stream had. Putting
%% that life back - the whole resurrection shape - is refused.
a_tombstone_refuses_the_life_it_replaced(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"tombstoned">>),
    start_anchor(Config),
    Life = crypto:strong_rand_bytes(16),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 1, Life),
    ok = reckon_db_life_anchor:record_delete_intent(StoreId, StreamId, 1),
    ok = reckon_db_life_anchor:record_deleted(StoreId, StreamId, 1),
    settle(),
    ?assertEqual({1, deleted}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    {refuse, Violation} = reckon_db_life_anchor:check(StoreId, StreamId, 1, Life),
    ?assertEqual(life_gen_deleted, detail(Violation)),
    %% The tree with no life at all, at or below the tombstone, is the deleted
    %% stream as the delete left it: nothing to read, nothing to refuse.
    ok = reckon_db_life_anchor:check(StoreId, StreamId, 0, undefined),
    ok.

%%====================================================================
%% Generations across create / delete / re-create
%%====================================================================

%% F8. A delete keeps the generation counter, so a re-create under the same id
%% rises above the deleted life instead of starting where it did.
a_delete_keeps_the_generation_and_the_re_create_rises_above_it(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"delete-then-create">>),
    start_anchor(Config),

    First = create(StoreId, StreamId),
    ok = reckon_db_streams:delete(StoreId, StreamId),
    settle(),
    ?assertEqual({1, deleted}, reckon_db_life_anchor:entry(StoreId, StreamId)),

    Second = create(StoreId, StreamId),
    settle(),
    ?assertEqual({2, Second}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ?assert(Second =/= First),
    ?assertEqual(2, gen_on_seal(StoreId, StreamId)),
    ok.

%% F5/F6. The generation is allocated as max(tree counter, this node's floor)
%% + 1, so lowering the counter node - which any tree writer can do - cannot
%% make an honest re-create land at or below an entry or tombstone this node
%% holds. Two creates in a row still rise.
a_lowered_counter_cannot_lower_the_next_generation(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"lowered-counter">>),
    start_anchor(Config),

    _First = create(StoreId, StreamId),
    ok = reckon_db_streams:delete(StoreId, StreamId),
    settle(),

    %% The tree writer's move: put the per-stream counter back to 0.
    ok = khepri:put(StoreId, reckon_db_stream_path:life_gen_path(StreamId), 0),

    Second = create(StoreId, StreamId),
    settle(),
    ?assertEqual(2, gen_on_seal(StoreId, StreamId)),
    ?assertEqual({2, Second}, reckon_db_life_anchor:entry(StoreId, StreamId)),

    %% Again: a second life minted after another lowering still rises, so the
    %% floor is the ANCHOR, not a value the counter happens to hold.
    ok = khepri:put(StoreId, reckon_db_stream_path:life_gen_path(StreamId), 0),
    ok = reckon_db_streams:delete(StoreId, StreamId),
    settle(),
    Third = create(StoreId, StreamId),
    settle(),
    ?assertEqual(3, gen_on_seal(StoreId, StreamId)),
    ?assertEqual({3, Third}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok.

%% F9. The mint reads the counter, MACs the seal outside the transaction, and
%% writes only if the counter still holds that value. Moving the counter after
%% every read loses every attempt, and the append is refused BY NAME - not
%% crashed, and not retried forever.
a_held_counter_race_refuses_by_name(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"held-race">>),
    start_anchor(Config),
    CounterPath = reckon_db_stream_path:life_gen_path(StreamId),

    %% Every read of the counter is followed by somebody else writing it, so
    %% the value the seal was MAC'd with is never the value the transaction
    %% finds. Both arities are intercepted: life_gen/2 reads with get/2.
    meck:new(khepri, [passthrough, no_link]),
    try
        meck:expect(khepri, get,
                    fun(S, P) ->
                            Result = meck:passthrough([S, P]),
                            _ = (P =:= CounterPath) andalso
                                khepri:put(S, P, 100 + rand:uniform(50)),
                            Result
                    end),
        meck:expect(khepri, get,
                    fun(S, P, Opts) ->
                            Result = meck:passthrough([S, P, Opts]),
                            _ = (P =:= CounterPath) andalso
                                khepri:put(S, P, 100 + rand:uniform(50)),
                            Result
                    end),
        ?assertEqual({error, {life_gen_contended, StreamId}},
                     reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM,
                                              events(1)))
    after
        meck:unload(khepri)
    end,

    %% Nothing was written: the stream is still absent, so the refusal left no
    %% half-minted seal behind.
    ?assertEqual({ok, undefined},
                 reckon_db_chain_watermark:lookup(StoreId, StreamId)),
    ?assertEqual(undefined, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok.

%%====================================================================
%% Intents and their resolution
%%====================================================================

%% The intent is fsynced before the delete commits. If the delete then
%% committed, the next start finds no life in the tree and the tombstone stays.
an_intent_whose_delete_committed_stays_a_tombstone(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"intent-committed">>),
    DataDir = proplists:get_value(data_dir, Config),
    start_anchor(Config),

    _Life = create(StoreId, StreamId),
    ok = reckon_db_life_anchor:record_delete_intent(StoreId, StreamId, 1),
    ok = reckon_db_streams:delete(StoreId, StreamId),
    restart_anchor(StoreId, DataDir),

    ?assertEqual({1, deleted}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok.

%% The other side of the same crash: the intent reached the disk, the delete
%% never committed. The life is still in the tree, so the intent is dropped -
%% and dropping it must be a WRITE, or the stream stays refused for good on
%% this node.
an_intent_whose_delete_never_committed_is_dropped(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"intent-uncommitted">>),
    DataDir = proplists:get_value(data_dir, Config),
    start_anchor(Config),

    Life = create(StoreId, StreamId),
    ok = reckon_db_life_anchor:record_delete_intent(StoreId, StreamId, 1),
    %% No delete. The anchor goes down with the intent on disk.
    restart_anchor(StoreId, DataDir),

    ?assertEqual({1, Life}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok = reckon_db_life_anchor:check(StoreId, StreamId, 1, Life),

    %% And the correction is in the log, not only in the table: a second
    %% restart reaches the same answer.
    restart_anchor(StoreId, DataDir),
    ?assertEqual({1, Life}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok.

%%====================================================================
%% Rebuild
%%====================================================================

%% F1/F2. Rebuild discards what this node recorded and takes the tree's word
%% for every stream - the recovery for an honest restore from backup, or for a
%% log whose chain broke. The old log is kept aside, and the NEW one has to be
%% written, opened and readable by the next start: a rebuild that only fixes
%% this process leaves the next start believing the discarded log.
rebuild_takes_the_tree_word_and_leaves_a_log_behind(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"rebuilt">>),
    DataDir = proplists:get_value(data_dir, Config),
    start_anchor(Config),

    Late = create(StoreId, StreamId),
    %% Model a node that has seen the newer life while the tree was rolled
    %% back to an older one: the tree says generation 1, this node says 5.
    OlderLife = crypto:strong_rand_bytes(16),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 5, Late),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 1, OlderLife),
    settle(),
    ?assertEqual({5, Late}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    {refuse, _} = reckon_db_life_anchor:check(StoreId, StreamId, 1, OlderLife),

    ok = put_older_life_in_the_tree(StoreId, StreamId, OlderLife, 1),
    {ok, 1} = reckon_db_life_anchor:rebuild(StoreId),
    ?assertEqual({1, OlderLife}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok = reckon_db_life_anchor:check(StoreId, StreamId, 1, OlderLife),

    %% The rebuild left a log the next start agrees with.
    restart_anchor(StoreId, DataDir),
    ?assertEqual({1, OlderLife}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok.

%%====================================================================
%% The log
%%====================================================================

%% What was recorded is on disk, not only in the table.
what_was_recorded_survives_a_restart(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"survives-restart">>),
    DataDir = proplists:get_value(data_dir, Config),
    start_anchor(Config),

    Life = create(StoreId, StreamId),
    %% A tombstone at the current generation and a later life: both go to disk,
    %% and the later one is what a restart must find.
    ok = reckon_db_life_anchor:record_delete_intent(StoreId, StreamId, 1),
    ok = reckon_db_life_anchor:record_mint(StoreId, StreamId, 2, <<"next">>),
    settle(),
    ?assertEqual({2, <<"next">>}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    restart_anchor(StoreId, DataDir),
    ?assertEqual({2, <<"next">>}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ?assert(Life =/= <<"next">>),
    ok.

%% A damaged tail costs the tail: the records before the break still load, so
%% a torn write never costs the store its whole history.
a_damaged_tail_costs_the_tail_not_the_head(Config) ->
    StoreId = proplists:get_value(store_id, Config),
    StreamId = sid(<<"damaged-tail">>),
    DataDir = proplists:get_value(data_dir, Config),
    start_anchor(Config),

    Life = create(StoreId, StreamId),
    settle(),
    reckon_db_life_anchor:stop(StoreId),
    Path = filename:join(DataDir, "reckon_db_lives.log"),
    {ok, Bin} = file:read_file(Path),
    ok = file:write_file(Path, <<Bin/binary, "junkjunkjunk">>),
    {ok, _} = reckon_db_life_anchor:start_link(StoreId, DataDir),

    ?assertEqual({1, Life}, reckon_db_life_anchor:entry(StoreId, StreamId)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

sid(Label) ->
    reckon_db_test_helpers:sid(Label).

start_anchor(Config) ->
    {ok, _} = reckon_db_life_anchor:start_link(
                proplists:get_value(store_id, Config),
                proplists:get_value(data_dir, Config)),
    ok.

restart_anchor(StoreId, DataDir) ->
    reckon_db_life_anchor:stop(StoreId),
    {ok, _} = reckon_db_life_anchor:start_link(StoreId, DataDir),
    ok.

%% The writer coalesces, so a record is durable a moment after the call. The
%% tests read the table, which is the same answer one step earlier; this makes
%% the ordering explicit rather than accidental.
settle() ->
    timer:sleep(20),
    ok.

create(StoreId, StreamId) ->
    {ok, _} = reckon_db_streams:append(StoreId, StreamId, ?NO_STREAM, events(1)),
    settle(),
    {ok, Life} = reckon_db_chain_watermark:life(StoreId, StreamId),
    Life.

gen_on_seal(StoreId, StreamId) ->
    {ok, Binding} = reckon_db_chain_watermark:binding(StoreId, StreamId),
    maps:get(gen, Binding).

%% A tree writer putting an older life and generation back where the seal goes.
put_older_life_in_the_tree(StoreId, StreamId, Life, Gen) ->
    Key = reckon_db_integrity_key:get(StoreId),
    Mac = crypto:mac(hmac, sha256, Key,
                     term_to_binary({reckon_db_chain_watermark, seal, 3, StoreId,
                                     StreamId, Life, Gen, 0, 0},
                                    [deterministic])),
    Record = #{scheme => 3, version => 0, v2_from => 0, life => Life, gen => Gen,
               mac => Mac},
    ok = khepri:put(StoreId, reckon_db_chain_watermark:life_path(StreamId), Life),
    ok = khepri:put(StoreId, reckon_db_chain_watermark:path(StreamId), Record),
    ok = khepri:put(StoreId, reckon_db_stream_path:life_gen_path(StreamId), Gen),
    ok.

events(N) ->
    [#{event_type => <<"e">>, data => #{n => I}} || I <- lists:seq(1, N)].

detail(#{context := #{detail := Detail}}) -> Detail.

recorded_gen(#{context := #{recorded_gen := Gen}}) -> Gen.
