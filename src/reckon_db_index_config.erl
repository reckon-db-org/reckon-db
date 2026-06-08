%% @doc Per-store secondary-index declaration registry.
%%
%% A store declares which secondary indexes it maintains in
%% `#store_config.indexes' (none by default). This module installs that
%% declared list into persistent_term at store startup so the append hot
%% path can read it without process-state coupling — exactly the pattern
%% {@link reckon_db_integrity_key} uses for the HMAC key.
%%
%% == Storage ==
%%
%% The declared index list is placed in persistent_term under
%% `{reckon_db, indexes, StoreId}'. Callers read it via `declared/1' on
%% every append; the lookup is sub-microsecond.
%%
%% == Index kinds ==
%%
%% <ul>
%%   <li>`tags' — index every tag in `#event.tags'.</li>
%%   <li>`event_type' — index `#event.event_type'.</li>
%%   <li>`{meta, Key}' — index `maps:get(Key, metadata)' when present.</li>
%% </ul>
%%
%% See plans/DESIGN_SECONDARY_INDEX.md.
-module(reckon_db_index_config).

-include("reckon_db.hrl").

-export([
    load/1,
    declared/1,
    is_indexed/2,
    clear/1
]).

%%====================================================================
%% Public API
%%====================================================================

%% @doc Install a store's declared index list into persistent_term at
%% startup. Idempotent: re-loading overwrites the previous declaration.
-spec load(store_config()) -> ok.
load(#store_config{store_id = StoreId, indexes = Indexes})
        when is_list(Indexes) ->
    persistent_term:put({reckon_db, indexes, StoreId}, normalize(Indexes)),
    ok.

%% @doc The declared index list for a store ([] if none / not loaded).
-spec declared(StoreId :: atom()) -> [index_decl()].
declared(StoreId) ->
    persistent_term:get({reckon_db, indexes, StoreId}, []).

%% @doc Whether a given index kind is declared for a store. `Kind' is an
%% `index_decl()' (`tags', `event_type', or `{meta, Key}'). Drives the
%% read-path "use index vs scan-and-warn" decision.
-spec is_indexed(StoreId :: atom(), Kind :: index_decl()) -> boolean().
is_indexed(StoreId, Kind) ->
    lists:member(Kind, declared(StoreId)).

%% @doc Remove a store's index declaration from persistent_term (shutdown
%% / test isolation).
-spec clear(StoreId :: atom()) -> ok.
clear(StoreId) ->
    persistent_term:erase({reckon_db, indexes, StoreId}),
    ok.

%%====================================================================
%% Internal
%%====================================================================

%% @private Drop anything that isn't a recognised index declaration and
%% de-duplicate, so the hot path can trust the list shape.
-spec normalize([term()]) -> [index_decl()].
normalize(Indexes) ->
    lists:usort([D || D <- Indexes, is_valid_decl(D)]).

-spec is_valid_decl(term()) -> boolean().
is_valid_decl(tags) -> true;
is_valid_decl(event_type) -> true;
is_valid_decl({meta, Key}) when is_binary(Key) -> true;
is_valid_decl(_) -> false.
