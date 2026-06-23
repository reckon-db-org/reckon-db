%%% @doc CCC (Command Context Consistency) Khepri path helpers.
%%%
%%% Payload index paths used by the CCC extensions to the DCB conditional-
%%% append primitive. Two structures live here:
%%%
%%%   [by_payload, Key, Value, SeqKey]  → #{}   (single-field payload index)
%%%   [by_payload_hash, Hash, SeqKey]   → #{}   (composite SHA-256 hash index)
%%%
%%% These are DCB-scoped (SeqKey-ordered, written atomically with each DCB
%%% event in the same Khepri transaction) and entirely separate from the
%%% secondary index under [idx, ...] which is OrderKey-keyed and optional.
%%%
%%% === Horus constraint ===
%%%
%%% All functions here EXCEPT payload_combo_hash/2 are PURE and safe to call
%%% from inside a khepri:transaction/2 body.
%%%
%%% payload_combo_hash/2 calls crypto:hash/2 — a NIF that Horus cannot
%%% extract into a portable Raft log entry. It MUST be called outside any
%%% transaction (in the stamp phase or via preprocess_filter/1).
%%%
%%% @end
-module(reckon_db_ccc_paths).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    by_payload_path/3,
    by_payload_pattern/2,
    by_payload_hash_path/2,
    by_payload_hash_pattern/1,
    payload_combo_hash/2
]).

%% @doc Path to a single-field payload index entry:
%%   [by_payload, Key, Value, SeqKey]
-spec by_payload_path(binary(), binary(), non_neg_integer()) -> [term()].
by_payload_path(Key, Value, Seq)
  when is_binary(Key), is_binary(Value), is_integer(Seq), Seq >= 0 ->
    ?BY_PAYLOAD_PATH ++ [Key, Value, reckon_db_dcb_paths:seq_key(Seq)].

%% @doc Pattern matching every seq under a payload field value (subtree wildcard).
%% Use with khepri_tx:get_many/1 inside a transaction.
-spec by_payload_pattern(binary(), binary()) -> [term()].
by_payload_pattern(Key, Value) when is_binary(Key), is_binary(Value) ->
    ?BY_PAYLOAD_PATH ++ [Key, Value, ?KHEPRI_WILDCARD_STAR].

%% @doc Path to a composite payload hash index entry:
%%   [by_payload_hash, Hash, SeqKey]
-spec by_payload_hash_path(binary(), non_neg_integer()) -> [term()].
by_payload_hash_path(Hash, Seq)
  when is_binary(Hash), is_integer(Seq), Seq >= 0 ->
    ?BY_PAYLOAD_HASH_PATH ++ [Hash, reckon_db_dcb_paths:seq_key(Seq)].

%% @doc Pattern matching every seq under a composite payload hash.
%% Use with khepri_tx:get_many/1 inside a transaction.
-spec by_payload_hash_pattern(binary()) -> [term()].
by_payload_hash_pattern(Hash) when is_binary(Hash) ->
    ?BY_PAYLOAD_HASH_PATH ++ [Hash, ?KHEPRI_WILDCARD_STAR].

%% @doc SHA-256 of the sorted [{Key, Value}] pair list.
%%
%% Sort ensures field-order independence: the same set of fields produces
%% the same hash regardless of the order Keys and Values are supplied.
%%
%% Example: payload_combo_hash([A, B], [V1, V2]) ==
%%          payload_combo_hash([B, A], [V2, V1])
%%
%% MUST NOT be called from inside a Khepri transaction — crypto:hash/2 is a
%% NIF that Horus cannot extract. Pre-compute the hash in the stamp phase or
%% via reckon_db_ccc_filter:preprocess_filter/1 before entering the
%% transaction.
-spec payload_combo_hash([binary()], [binary()]) -> binary().
payload_combo_hash(Keys, Values)
  when is_list(Keys), is_list(Values), length(Keys) =:= length(Values) ->
    Pairs = lists:sort(lists:zip(Keys, Values)),
    crypto:hash(sha256, term_to_binary(Pairs)).
