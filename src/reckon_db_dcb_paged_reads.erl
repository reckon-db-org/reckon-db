%%% @doc Paged DCB reads by event type, tag and payload, in DCB sequence
%%% order (reckon-gater 3.15.0's requests; evoq#6 part two).
%%%
%%% The limit-only DCB reads return the first Limit matching events and
%%% cannot go on, so a reader of more than one page (an evoq decision over a
%%% large context) could not see the rest. Each read here returns one page
%%% of DCB events after a cursor, and the cursor for the next page or
%%% `done'.
%%%
%%% == Why sequence order ==
%%%
%%% A DCB event's sequence number is assigned inside its append transaction,
%%% so every event committed after a page was read sorts after that page:
%%% nothing lands behind a cursor, and the highest sequence number over the
%%% pages is a sound cutoff for append_if_no_tag_matches/4. The indexes the
%%% DCB append writes (by_event_type, by_tag, by_payload, by_payload_hash)
%%% are keyed by the zero-padded sequence key, so their keys sort in that
%%% order.
%%%
%%% == The cursor ==
%%%
%%% `<<"rdc1", Digest:16/binary, SeqKey/binary>>': the sequence key of the
%%% page's last event, and the first 16 bytes of the SHA-256 of the read and
%%% its arguments. The store keeps no state for it, so it is valid on any
%%% worker and node; a cursor from another read, or one that does not
%%% decode, is `{invalid_cursor, Cursor}'.
%%%
%%% == Cost ==
%%%
%%% Khepri has no range query on a path, so each page reads the whole index
%%% branch of its filter and keeps the keys after the cursor. Paging N
%%% matching events costs N per page; that is the price of the limit-only
%%% reads too, once per page.
%%% @end
-module(reckon_db_dcb_paged_reads).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

-export([by_event_types/4, by_tags/5, by_payload/5, by_payload_hash/5]).

-type after_() :: start | binary().
-type page() :: {ok, {[#event{}], done | binary()}} | {error, term()}.

-define(CURSOR_TAG, "rdc1").

%%====================================================================
%% API
%%====================================================================

%% @doc One page of DCB events of any of EventTypes, after After.
-spec by_event_types(atom(), [binary()], after_(), integer()) -> page().
by_event_types(StoreId, EventTypes, After, Limit) ->
    Read = {by_event_types, EventTypes},
    page(StoreId, Read, After, Limit, fun() ->
        union([branch_keys(StoreId, reckon_db_dcb_paths:by_event_type_pattern(T))
               || T <- EventTypes])
    end).

%% @doc One page of DCB events carrying any (Match = any) or all
%% (Match = all) of Tags, after After.
-spec by_tags(atom(), [binary()], term(), after_(), integer()) -> page().
by_tags(_StoreId, _Tags, Match, _After, _Limit) when Match =/= any, Match =/= all ->
    {error, {invalid_page_request, {match, Match}}};
by_tags(StoreId, Tags, Match, After, Limit) ->
    Read = {by_tags, Tags, Match},
    page(StoreId, Read, After, Limit, fun() ->
        combine(Match, [branch_keys(StoreId, reckon_db_dcb_paths:by_tag_pattern(T))
                        || T <- Tags])
    end).

%% @doc One page of DCB events whose payload field Key equals Value, after
%% After. Needs the store's {payload, Key} index.
-spec by_payload(atom(), binary(), binary(), after_(), integer()) -> page().
by_payload(StoreId, Key, Value, After, Limit) ->
    Read = {by_payload, Key, Value},
    page(StoreId, Read, After, Limit, fun() ->
        branch_keys(StoreId, reckon_db_ccc_paths:by_payload_pattern(Key, Value))
    end).

%% @doc One page of DCB events matching a composite payload field set,
%% after After. Needs the store's {payload_hash, Keys} index.
-spec by_payload_hash(atom(), [binary()], [binary()], after_(), integer()) -> page().
by_payload_hash(StoreId, Keys, Values, After, Limit) ->
    Read = {by_payload_hash, Keys, Values},
    Hash = reckon_db_ccc_paths:payload_combo_hash(Keys, Values),
    page(StoreId, Read, After, Limit, fun() ->
        branch_keys(StoreId, reckon_db_ccc_paths:by_payload_hash_pattern(Hash))
    end).

%%====================================================================
%% Internal
%%====================================================================

%% @private Validate the request, read the matching sequence keys, and cut
%% the page after the cursor. One key past the page decides `done'.
page(_StoreId, _Read, _After, Limit, _Keys) when not is_integer(Limit); Limit < 1 ->
    {error, {invalid_page_request, {limit, Limit}}};
page(StoreId, Read, After, Limit, Keys) ->
    paged(after_key(Read, After), StoreId, Read, Limit, Keys).

paged({error, _} = Error, _StoreId, _Read, _Limit, _Keys) ->
    Error;
paged({ok, AfterKey}, StoreId, Read, Limit, Keys) ->
    cut(Keys(), AfterKey, StoreId, Read, Limit).

cut({error, _} = Error, _AfterKey, _StoreId, _Read, _Limit) ->
    Error;
cut({ok, SeqKeys}, AfterKey, StoreId, Read, Limit) ->
    Following = [K || K <- SeqKeys, K > AfterKey],
    {PageKeys, Rest} = lists:split(min(Limit, length(Following)), Following),
    {ok, {resolve(StoreId, PageKeys), next(Rest, Read, PageKeys)}}.

next([], _Read, _PageKeys) -> done;
next(_Rest, Read, PageKeys) -> cursor(Read, lists:last(PageKeys)).

%% @private The sequence keys under an index branch, sorted.
branch_keys(StoreId, Pattern) ->
    keys_of(khepri:get_many(StoreId, Pattern)).

keys_of({ok, NodeMap}) when is_map(NodeMap) ->
    {ok, lists:usort([lists:last(Path) || Path <- maps:keys(NodeMap)])};
keys_of({error, {khepri, node_not_found, _}}) ->
    {ok, []};
keys_of({error, _} = Error) ->
    Error.

%% @private Union of several sorted key lists, or the first error.
union(Results) ->
    first_error(Results, fun(Lists) -> lists:umerge(Lists) end).

combine(any, Results) ->
    union(Results);
combine(all, []) ->
    {ok, []};
combine(all, Results) ->
    first_error(Results, fun ordsets:intersection/1).

first_error(Results, Combine) ->
    combined([E || {error, _} = E <- Results], Results, Combine).

combined([Error | _], _Results, _Combine) -> Error;
combined([], Results, Combine) -> {ok, Combine([L || {ok, L} <- Results])}.

%% @private The events at the page's keys. An index entry whose event is
%% gone is skipped: a page may hold fewer than Limit events.
resolve(StoreId, SeqKeys) ->
    lists:filtermap(fun(K) -> event_at(khepri:get(StoreId, ?DCB_STREAM_PATH ++ [K])) end,
                    SeqKeys).

event_at({ok, #event{} = E}) -> {true, E};
event_at(_) -> false.

%% @private The sequence key a cursor names, checked against its read.
after_key(_Read, start) ->
    {ok, <<>>};
after_key(Read, <<?CURSOR_TAG, Digest:16/binary, SeqKey/binary>> = Cursor) ->
    checked_key(Digest =:= digest(Read) andalso is_seq_key(SeqKey), SeqKey, Cursor);
after_key(_Read, Cursor) ->
    {error, {invalid_cursor, Cursor}}.

checked_key(true, SeqKey, _Cursor) -> {ok, SeqKey};
checked_key(false, _SeqKey, Cursor) -> {error, {invalid_cursor, Cursor}}.

is_seq_key(Key) ->
    byte_size(Key) =:= ?DCB_SEQ_KEY_WIDTH andalso
        lists:all(fun(C) -> C >= $0 andalso C =< $9 end, binary_to_list(Key)).

cursor(Read, SeqKey) ->
    <<?CURSOR_TAG, (digest(Read))/binary, SeqKey/binary>>.

digest(Read) ->
    binary:part(crypto:hash(sha256, term_to_binary(Read)), 0, 16).
