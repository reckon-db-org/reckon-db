%%% @doc DCB conditional-append primitive.
%%%
%%% Implements `append_if_no_tag_matches/4` as a `khepri:transaction/2`
%%% body. The transaction:
%%%
%%%   1. Reads all tag-index subtrees referenced by the filter,
%%%      computes the matching seq set, and checks whether any matching
%%%      seq exceeds the caller's `SeqCutoff`. If yes, aborts with
%%%      `{context_changed, MaxSeq}`.
%%%   2. Otherwise reads the global DCB seq counter, assigns sequential
%%%      seqs to the new events, writes each event under
%%%      `?DCB_STREAM_PATH ++ [SeqKey]` plus one tag-index entry per
%%%      tag at `?BY_TAG_PATH ++ [Tag, SeqKey]`, and updates the
%%%      counter.
%%%
%%% The whole sequence happens inside one Ra log entry — atomic across
%%% the cluster. Either everything commits, or `{context_changed, _}`
%%% comes back and nothing changed.
%%%
%%% v1 scope: DCB events skip integrity (HMAC chain). Integrity is a
%%% v2 concern, tracked in PLAN_DCB_IMPLEMENTATION.md.
%%% @end
-module(reckon_db_dcb).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([append_if_no_tag_matches/4]).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Conditionally append events to the DCB pseudo-stream.
%%
%% Returns:
%%   - `{ok, LastSeq}` on commit (`LastSeq` is the seq of the last
%%     appended event; multi-event batches commit contiguously)
%%   - `{error, {context_changed, MaxSeq}}` when a matching event with
%%     seq > `SeqCutoff` exists; nothing was written
%%   - `{error, no_events}` if `Events` is empty
%%   - `{error, Other}` on backend failure
-spec append_if_no_tag_matches(
    StoreId   :: atom() | binary(),
    TagFilter :: reckon_gater_types:tag_filter(),
    SeqCutoff :: reckon_gater_types:seq_cutoff(),
    Events    :: [map()]
) ->
      {ok, LastSeq :: non_neg_integer()}
    | {error, {context_changed, non_neg_integer()}}
    | {error, no_events}
    | {error, term()}.
append_if_no_tag_matches(_StoreId, _TagFilter, _SeqCutoff, []) ->
    {error, no_events};
append_if_no_tag_matches(StoreId, TagFilter, SeqCutoff, Events)
  when is_list(Events), is_integer(SeqCutoff) ->
    %% v1 safety check. Tamper-resistance (HMAC chain) is not yet
    %% implemented for DCB events. To prevent silent tamper exposure
    %% on integrity-enabled stores, fail closed. Real integrity support
    %% lands in a v2 follow-up; this check goes away then.
    case reckon_db_integrity_key:is_enabled(StoreId) of
        true ->
            {error, integrity_not_supported_in_dcb_v1};
        false ->
            Now = erlang:system_time(millisecond),
            EpochUs = erlang:system_time(microsecond),
            case khepri:transaction(
                   StoreId,
                   fun() ->
                       tx_body(TagFilter, SeqCutoff, Events, Now, EpochUs)
                   end) of
                {ok, LastSeq} ->
                    {ok, LastSeq};
                {error, {context_changed, _MaxSeq} = Reason} ->
                    {error, Reason};
                {error, _} = Error ->
                    Error
            end
    end.

%%====================================================================
%% Transaction body (runs inside khepri:transaction)
%%====================================================================

%% @private
%% Must be pure / transaction-safe: only `khepri_tx:*` calls + whitelisted BIFs.
tx_body(TagFilter, SeqCutoff, Events, Now, EpochUs) ->
    case reckon_db_dcb_filter:match_any_above_cutoff(TagFilter, SeqCutoff) of
        {true, MaxSeq} ->
            khepri_tx:abort({context_changed, MaxSeq});
        false ->
            BaseSeq = next_base_seq_in_tx(),
            LastSeq = write_events(Events, BaseSeq, Now, EpochUs),
            ok = khepri_tx:put(?DCB_SEQ_COUNTER_PATH, LastSeq),
            LastSeq
    end.

%% @private Read the global DCB counter inside the tx. Absent counter
%% means no DCB events yet; first seq is 0.
next_base_seq_in_tx() ->
    case khepri_tx:get(?DCB_SEQ_COUNTER_PATH) of
        {ok, LastAssigned} when is_integer(LastAssigned), LastAssigned >= 0 ->
            LastAssigned + 1;
        _ ->
            0
    end.

%% @private Write each event + its tag-index mirrors. Returns the last
%% assigned seq.
write_events([Event | Rest], CurSeq, Now, EpochUs) ->
    ok = write_one_event(Event, CurSeq, Now, EpochUs),
    case Rest of
        [] -> CurSeq;
        _  -> write_events(Rest, CurSeq + 1, Now, EpochUs)
    end.

%% @private Build the #event{} record, write it under the DCB stream
%% path, write the tag-index mirror entries.
write_one_event(EventMap, Seq, Now, EpochUs) when is_map(EventMap) ->
    EventRecord = build_event_record(EventMap, Seq, Now, EpochUs),
    ok = khepri_tx:put(reckon_db_dcb_paths:event_path(Seq), EventRecord),
    Tags = case maps:get(tags, EventMap, []) of
               undefined -> [];
               TagList when is_list(TagList) -> TagList
           end,
    lists:foreach(
        fun(Tag) when is_binary(Tag) ->
            ok = khepri_tx:put(
                reckon_db_dcb_paths:by_tag_path(Tag, Seq), #{})
        end,
        Tags),
    ok.

%% @private Build the #event{} record from a new_event() map. DCB v1
%% skips integrity fields (prev_event_hash, mac stay undefined). The
%% event lives at version = Seq under the ?DCB_STREAM pseudo-stream.
build_event_record(EventMap, Seq, Now, EpochUs) ->
    EventId = maps:get(event_id, EventMap, generate_event_id_in_tx(Seq, EpochUs)),
    EventType = maps:get(event_type, EventMap),
    Data = maps:get(data, EventMap),
    Metadata = maps:get(metadata, EventMap, #{}),
    Tags = case maps:get(tags, EventMap, undefined) of
               undefined -> undefined;
               []        -> undefined;
               L when is_list(L) -> L
           end,
    #event{
        event_id   = EventId,
        event_type = EventType,
        stream_id  = ?DCB_STREAM,
        version    = Seq,
        data       = Data,
        metadata   = Metadata,
        tags       = Tags,
        timestamp  = Now,
        epoch_us   = EpochUs
        %% Integrity fields (prev_event_hash, mac, ...) stay default-undefined.
        %% DCB integrity is a v2 concern.
    }.

%% @private Generate a deterministic event_id when the caller didn't
%% provide one. Pure: same inputs → same id. Inside transactions we
%% can't call crypto:strong_rand_bytes (not on the whitelist), so we
%% derive a stable id from seq + epoch_us.
generate_event_id_in_tx(Seq, EpochUs) ->
    %% Format: "dcb-<epoch_us>-<seq>". Stable, sortable, debuggable.
    iolist_to_binary(io_lib:format("dcb-~p-~p", [EpochUs, Seq])).
