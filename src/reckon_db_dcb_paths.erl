%%% @doc DCB Khepri path helpers.
%%%
%%% DCB events live under [events, ?DCB_STREAM] and are mirrored under
%%% [by_tag, Tag, SeqKey] for every tag they carry, and under
%%% [by_event_type, EventType, SeqKey] for their event_type field.
%%% Both mirrors are the consistency index: a subtree iteration on
%%% [by_tag, Tag, *] or [by_event_type, Type, *] returns all seq keys
%%% for that tag or type, lexicographically (numerically) sorted.
%%%
%%% Seq keys are fixed-width zero-padded decimal binaries (see
%%% ?DCB_SEQ_KEY_WIDTH), so lists:max/1 on a list of seq keys returns
%%% the highest, and Key > Cutoff is a lex comparison that matches the
%%% numeric comparison.
%%%
%%% All functions here are PURE; safe to call from inside a
%%% khepri:transaction/2 body.
%%%
%%% Payload index paths (single-field and composite hash) were extracted
%%% to reckon_db_ccc_paths in 5.4.0 to reflect their CCC scope.
%%%
%%% @end
-module(reckon_db_dcb_paths).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([
    event_path/1,
    by_tag_path/2,
    by_tag_pattern/1,
    by_event_type_path/2,
    by_event_type_pattern/1,
    seq_key/1,
    seq_from_key/1
]).

%% @doc Path to the event payload at a given seq:
%%   [events, &lt;&lt;"_dcb"&gt;&gt;, &lt;&lt;"00000000000000000042"&gt;&gt;]
-spec event_path(non_neg_integer()) -> [term()].
event_path(Seq) when is_integer(Seq), Seq >= 0 ->
    ?DCB_STREAM_PATH ++ [seq_key(Seq)].

%% @doc Path to a tag-index entry:
%%   [by_tag, &lt;&lt;"email:foo@bar"&gt;&gt;, &lt;&lt;"00000000000000000042"&gt;&gt;]
-spec by_tag_path(binary(), non_neg_integer()) -> [term()].
by_tag_path(Tag, Seq) when is_binary(Tag), is_integer(Seq), Seq >= 0 ->
    ?BY_TAG_PATH ++ [Tag, seq_key(Seq)].

%% @doc Pattern matching every seq under a tag (subtree wildcard).
%% Use with khepri_tx:get_many/1 inside a transaction.
-spec by_tag_pattern(binary()) -> [term()].
by_tag_pattern(Tag) when is_binary(Tag) ->
    ?BY_TAG_PATH ++ [Tag, ?KHEPRI_WILDCARD_STAR].

%% @doc Path to an event-type index entry for a given seq.
%% Example key: [by_event_type, &lt;&lt;"user_registered_v1"&gt;&gt;, &lt;&lt;"00000000000000000042"&gt;&gt;]
-spec by_event_type_path(binary(), non_neg_integer()) -> [term()].
by_event_type_path(EventType, Seq)
  when is_binary(EventType), is_integer(Seq), Seq >= 0 ->
    ?BY_EVENT_TYPE_PATH ++ [EventType, seq_key(Seq)].

%% @doc Pattern matching every seq under an event type (subtree wildcard).
%% Use with khepri_tx:get_many/1 inside a transaction.
-spec by_event_type_pattern(binary()) -> [term()].
by_event_type_pattern(EventType) when is_binary(EventType) ->
    ?BY_EVENT_TYPE_PATH ++ [EventType, ?KHEPRI_WILDCARD_STAR].

%% @doc Convert a non-negative integer to a fixed-width zero-padded
%% decimal binary (?DCB_SEQ_KEY_WIDTH digits).
%%
%% Invariants:
%%   - byte_size(seq_key(N)) == ?DCB_SEQ_KEY_WIDTH for all valid N
%%   - seq_key(A) =&lt; seq_key(B) iff A =&lt; B (lex == numeric)
%%   - seq_from_key(seq_key(N)) == N (roundtrip)
-spec seq_key(non_neg_integer()) -> binary().
seq_key(Seq) when is_integer(Seq), Seq >= 0 ->
    Str = integer_to_list(Seq),
    Padding = ?DCB_SEQ_KEY_WIDTH - length(Str),
    case Padding < 0 of
        true  -> erlang:error({seq_overflow, Seq, ?DCB_SEQ_KEY_WIDTH});
        false -> list_to_binary(lists:duplicate(Padding, $0) ++ Str)
    end.

%% @doc Decode a seq_key binary back to its integer.
-spec seq_from_key(binary()) -> non_neg_integer().
seq_from_key(Bin) when is_binary(Bin) ->
    binary_to_integer(Bin).
