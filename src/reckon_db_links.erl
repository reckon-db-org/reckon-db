%% @doc Server-side stream linking and projections for reckon-db.
%%
%% A *link* is a derived stream computed from one or more source
%% streams by applying an optional filter predicate and an optional
%% transform function. The result is persisted under a stable name
%% and exposed as just-another-subscribable-stream — consumers
%% read it like any user stream.
%%
%% This is the same primitive Greg Young's EventStoreDB exposes
%% as *projections*: `$ce-account' (category events for
%% `account-*' streams), `$et-UserCreated' (all events of one
%% type), or arbitrary user-defined JavaScript projections that
%% emit into custom destination streams. reckon-db's links cover
%% the same conceptual ground with native Erlang funs instead of a
%% scripting layer.
%%
%% == Why links exist ==
%%
%% Reckon-db already has rich typed subscription filters
%% (`by_event_type', `by_event_pattern', `by_event_payload',
%% `by_tags') that filter the event stream on the fly. So why a
%% separate "link" mechanism?
%%
%% <ul>
%% <li>**Transform.** Typed subscriptions can only filter. Links
%%     can rewrite events (add fields, re-key, redact, denormalise)
%%     before delivery. The transform is server-side, deterministic,
%%     and replayable.</li>
%% <li>**Materialisation.** Typed subscriptions are recomputed for
%%     every consumer. A link computes once, persists the derived
%%     events on disk, and serves them like any other stream — N
%%     consumers cost N stream-reads, not N×(source-scans + filter +
%%     transform).</li>
%% <li>**Stable named subscription targets.** Dynamically-created
%%     source streams (`order-018f6a...', `order-018f6b...', ...)
%%     can't be subscribed to as a group via the bare-id selector;
%%     you'd need a wildcard pattern in the consumer. A link
%%     (`$link:orders' filtered to source pattern `order-*') gives
%%     you one durable, named, listable target.</li>
%% <li>**Replayability.** A new consumer attaching to a link
%%     replays the persisted derived events from the start, in
%%     order, deterministically — same shape every time, regardless
%%     of how the source streams have grown. Typed subscriptions
%%     replay by re-scanning the entire global log and re-applying
%%     the predicate, which is correct but expensive at scale.</li>
%% <li>**Composability.** Links can source from other links —
%%     `$link:high-value-orders' built on top of `$link:orders'.
%%     Useful for incremental refinement of a view.</li>
%% <li>**Operational visibility.** Named derived streams show up
%%     in store listings (lazyreckon's `streams' pane, for
%%     example) — operators can see "what derived views does this
%%     store maintain" at a glance. Typed subscriptions are
%%     ephemeral client state and invisible to tooling.</li>
%% </ul>
%%
%% == Stream-id namespace ==
%%
%% Link streams live in the `$' system namespace:
%%
%%   `$link:&lt;human-readable-name&gt;'
%%
%% e.g. `$link:high-value-orders'. The `$' prefix is the
%% reckon-db convention for "system / projected stream, not user
%% data" — same role as EventStoreDB's `$ce-' / `$et-' /
%% `$by_category' / `$stats-' prefixes. User streams follow the
%% `&lt;prefix&gt;-&lt;hex&gt;' format (e.g. `account-018f6a7b8c9d4abc...').
%% Both forms are accepted as subscription selectors via
%% `reckon_db_filters:by_stream/1'.
%%
%% Link subscriptions themselves get a related namespace:
%%
%%   `$link-sub:&lt;link-name&gt;'
%%
%% so subscriptions managed by the link engine are
%% distinguishable from user subscriptions in operational tooling.
%%
%% == Use cases (worked examples) ==
%%
%% <strong>1. Materialised category view.</strong>
%% Collate every event from all `order-&lt;hex&gt;' streams into one
%% subscribable stream:
%% ```
%% reckon_db_links:create(my_store, #{
%%     name   => <<"orders">>,
%%     source => #{type => stream_pattern, pattern => <<"order-*">>}
%% }).
%% reckon_db_subscriptions:subscribe(
%%     my_store, stream, <<"$link:orders">>, <<"orders-projector">>).
%% '''
%%
%% <strong>2. Filtered derived stream.</strong>
%% Just the high-value subset:
%% ```
%% reckon_db_links:create(my_store, #{
%%     name   => <<"high-value-orders">>,
%%     source => #{type => stream_pattern, pattern => <<"order-*">>},
%%     filter => fun(E) ->
%%         maps:get(total, E#event.data, 0) > 1000
%%     end
%% }).
%% '''
%%
%% <strong>3. Transform / re-shape.</strong>
%% Strip PII from outbound webhook events:
%% ```
%% reckon_db_links:create(my_store, #{
%%     name      => <<"webhook-feed">>,
%%     source    => #{type => stream_pattern, pattern => <<"order-*">>},
%%     transform => fun(E) ->
%%         D2 = maps:without([customer_email, customer_phone],
%%                           E#event.data),
%%         E#event{data = D2}
%%     end
%% }).
%% '''
%%
%% <strong>4. Composition.</strong>
%% Layer a filter on top of an existing link:
%% ```
%% reckon_db_links:create(my_store, #{
%%     name   => <<"high-value-webhook">>,
%%     source => #{type => stream, stream_id => <<"$link:webhook-feed">>},
%%     filter => fun(E) ->
%%         maps:get(total, E#event.data, 0) > 1000
%%     end
%% }).
%% '''
%%
%% == When NOT to use a link ==
%%
%% <ul>
%% <li>Single-consumer one-off filter — a `by_event_type' or
%%     `by_tags' subscription is cheaper and avoids persistent
%%     derived data.</li>
%% <li>Read model that lives in a database (postgres, sqlite, etc)
%%     — subscribe directly with the appropriate typed filter and
%%     project to the database. The link layer adds no value when
%%     the materialised target is elsewhere.</li>
%% <li>Aggregation across many sources where the math is
%%     non-trivial — links emit one derived event per source event;
%%     they don't fold. For aggregation, use a process manager.</li>
%% </ul>
%%
%% == Running a link ==
%%
%% start/2 starts the link's consumer (reckon_db_link_consumer), which
%% follows the source live and, with `backfill => true', writes what the
%% source already held first. Each source event that passes the filter
%% reaches the link stream once, also across a consumer crash or a store
%% restart: the consumer stores, per source stream, the last version it
%% handled. A link stored as running is resumed when the store's leader
%% activates. stop/2 ends the consumer and removes its subscription; a
%% later start/2 picks up from where it stopped, so the link also gets
%% what the source received while it was stopped.
%%
%% A link that cannot progress (a transform that crashes or returns
%% something other than an event) stops with status `error' and the
%% reason, and writes nothing for that event; the other links keep
%% running. Fix the cause, then start/2 it again.
%%
%% == Status ==
%%
%% Live delivery works for every source type since reckon-db#24; before
%% that a link only ever held its backfill. Treat links as preview while
%% we accumulate real usage.
%%
%% @author rgfaber

-module(reckon_db_links).

-include("reckon_db.hrl").
-include("reckon_db_links.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([
    create/2,
    delete/2,
    get/2,
    list/1,
    start/2,
    stop/2,
    info/2,
    resume/1
]).

%% For reckon_db_link_consumer
-export([
    link_record/2,
    link_stream_id/1,
    record_running/4,
    record_progress/6,
    record_halt/3
]).

%%====================================================================
%% Types
%%====================================================================

-type source_spec() :: #{
    type := stream | stream_pattern | all,
    stream_id => binary(),
    pattern => binary()
}.

-type filter_fun() :: fun((event()) -> boolean()).
-type transform_fun() :: fun((event()) -> event()).

-type link_spec() :: #{
    name := binary(),
    source := source_spec(),
    filter => filter_fun(),
    transform => transform_fun(),
    backfill => boolean()  %% Process existing events (default: false)
}.

-type link_info() :: #{
    name := binary(),
    source := source_spec(),
    status := running | stopped | error,
    processed := non_neg_integer(),
    last_event => binary(),
    reason => term()
}.

-export_type([link_spec/0, link_info/0, source_spec/0, filter_fun/0, transform_fun/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new link.
%%
%% Options:
%% - name: Link name (required, will create stream with $link prefix)
%% - source: Source specification (stream, stream_pattern, or all)
%% - filter: Predicate function to filter events
%% - transform: Function to transform events
%% - backfill: Process existing events (default: false)
%%
%% A source the link cannot follow is refused with
%% `{error, {invalid_source, Source}}' and nothing is stored: a `stream'
%% source needs a non-empty binary `stream_id' other than the link's own
%% stream, a `stream_pattern' source a non-empty binary `pattern'.
-spec create(atom(), link_spec()) -> ok | {error, term()}.
create(StoreId, Spec) ->
    Name = maps:get(name, Spec),
    Source = maps:get(source, Spec),
    create(valid_source(Source, Name), StoreId, Name, Source, Spec).

create(false, _StoreId, _Name, Source, _Spec) ->
    {error, {invalid_source, Source}};
create(true, StoreId, Name, Source, Spec) ->
    Link = #link{
        name = Name,
        source = Source,
        filter = maps:get(filter, Spec, undefined),
        transform = maps:get(transform, Spec, undefined),
        backfill = maps:get(backfill, Spec, false),
        created_at = erlang:system_time(millisecond),
        status = stopped,
        processed = 0
    },
    case khepri:put(StoreId, link_path(StoreId, Name), Link) of
        ok ->
            emit_telemetry(StoreId, Name, created),
            ok;
        {error, _} = Error ->
            Error
    end.

valid_source(#{type := stream, stream_id := StreamId}, Name)
  when is_binary(StreamId), StreamId =/= <<>> ->
    StreamId =/= link_stream_id(Name);
valid_source(#{type := stream_pattern, pattern := Pattern}, _Name)
  when is_binary(Pattern), Pattern =/= <<>> ->
    true;
valid_source(#{type := all}, _Name) ->
    true;
valid_source(_Source, _Name) ->
    false.

%% @doc Delete a link.
-spec delete(atom(), binary()) -> ok | {error, term()}.
delete(StoreId, Name) ->
    %% Stop the link first if running
    stop(StoreId, Name),

    case khepri:delete(StoreId, link_path(StoreId, Name)) of
        ok ->
            emit_telemetry(StoreId, Name, deleted),
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Get a link by name.
-spec get(atom(), binary()) -> {ok, link_info()} | {error, not_found}.
get(StoreId, Name) ->
    case link_record(StoreId, Name) of
        {ok, Link} -> {ok, link_to_info(Link)};
        {error, _} = Error -> Error
    end.

%% @doc List all links.
-spec list(atom()) -> {ok, [link_info()]} | {error, term()}.
list(StoreId) ->
    case khepri:get_many(StoreId, ?LINKS_PATH ++ [StoreId, ?KHEPRI_WILDCARD_STAR]) of
        {ok, Links} when is_map(Links) ->
            {ok, [link_to_info(L) || L <- maps:values(Links)]};
        {error, _} = Error ->
            Error
    end.

%% @doc Start processing a link.
%%
%% Starts the link's consumer, which subscribes to the source, sets the
%% status to running, writes the backfill if the link asks for it (or, on
%% a later start, what the source received while the link was stopped),
%% and then follows the source live. A link that cannot subscribe answers
%% the subscribe error and keeps its status.
-spec start(atom(), binary()) -> ok | {error, term()}.
start(StoreId, Name) ->
    case link_record(StoreId, Name) of
        {ok, _Link} -> started(reckon_db_links_sup:start_consumer(StoreId, Name), StoreId, Name);
        {error, _} = Error -> Error
    end.

started(ok, StoreId, Name) ->
    emit_telemetry(StoreId, Name, started),
    ok;
started({error, _} = Error, _StoreId, _Name) ->
    Error.

%% @doc Stop processing a link: its consumer ends and its subscription is
%% removed.
-spec stop(atom(), binary()) -> ok.
stop(StoreId, Name) ->
    ok = stop_consumer(global:whereis_name(reckon_db_link_consumer:name(StoreId, Name))),
    _ = reckon_db_subscriptions:unsubscribe(StoreId, stream, <<"$link-sub:", Name/binary>>),
    _ = set_status(StoreId, Name, stopped),
    emit_telemetry(StoreId, Name, stopped),
    ok.

stop_consumer(undefined) ->
    ok;
stop_consumer(Pid) ->
    try gen_server:stop(Pid)
    catch exit:noproc -> ok
    end.

%% @doc Get detailed link info.
-spec info(atom(), binary()) -> {ok, map()} | {error, not_found}.
info(StoreId, Name) ->
    case link_record(StoreId, Name) of
        {ok, Link} ->
            LinkStreamId = link_stream_id(Name),
            Version = reckon_db_streams:get_version(StoreId, LinkStreamId),

            Info = #{
                name => Link#link.name,
                source => Link#link.source,
                status => Link#link.status,
                processed => Link#link.processed,
                link_stream => LinkStreamId,
                link_stream_version => Version,
                created_at => Link#link.created_at,
                last_event => Link#link.last_event
            },
            {ok, with_reason(Link, Info)};
        Error ->
            Error
    end.

%% @doc Start the consumer of every link stored as running. Leader
%% activation calls this, so running links come back after a restart.
%% A link that cannot resume is logged and left as it is.
-spec resume(atom()) -> ok.
resume(StoreId) ->
    {ok, Links} = list(StoreId),
    lists:foreach(fun(#{name := Name}) -> resume(StoreId, Name) end,
                  [L || #{status := running} = L <- Links]).

resume(StoreId, Name) ->
    case reckon_db_links_sup:start_consumer(StoreId, Name) of
        ok -> ok;
        {error, Reason} ->
            logger:warning("Link ~s did not resume: ~p (store: ~p)", [Name, Reason, StoreId])
    end.

%%====================================================================
%% For reckon_db_link_consumer
%%====================================================================

%% @doc The stored link record.
-spec link_record(atom(), binary()) -> {ok, #link{}} | {error, term()}.
link_record(StoreId, Name) ->
    case khepri:get(StoreId, link_path(StoreId, Name)) of
        {ok, #link{} = Link} ->
            {ok, Link};
        {error, {khepri, node_not_found, _}} ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.

%% @doc The stream a link writes to.
-spec link_stream_id(binary()) -> binary().
link_stream_id(Name) ->
    <<?LINK_STREAM_PREFIX/binary, Name/binary>>.

%% @doc Store that the link's consumer has subscribed: status running, no
%% reason, and the positions it starts from. The consumer writes this
%% before it reads anything, so a stop it comes to is never overwritten
%% by start/2.
-spec record_running(atom(), binary(), #{binary() => integer()}, integer()) -> ok.
record_running(StoreId, Name, Positions, LinkVersion) ->
    in_link_tx(StoreId, Name,
               fun(Link) -> Link#link{status = running, reason = undefined,
                                      positions = Positions, link_version = LinkVersion}
               end).

%% @doc Store the consumer's progress: the positions it has handled up to,
%% the link-stream version its last append reached, and the events it has
%% just written. One transaction, so a concurrent start/2 or stop/2 status
%% write is never lost.
-spec record_progress(atom(), binary(), #{binary() => integer()}, integer(), [event()],
                      binary() | undefined) -> ok.
record_progress(StoreId, Name, Positions, LinkVersion, Written, LastEvent) ->
    Count = length(Written),
    in_link_tx(StoreId, Name,
               fun(Link) -> Link#link{positions = Positions, link_version = LinkVersion,
                                      processed = Link#link.processed + Count,
                                      last_event = last_event(LastEvent, Link)}
               end).

%% @doc Store that the link stopped because it cannot progress: status
%% error, and the reason.
-spec record_halt(atom(), binary(), term()) -> ok.
record_halt(StoreId, Name, Reason) ->
    in_link_tx(StoreId, Name, fun(Link) -> Link#link{status = error, reason = Reason} end).

in_link_tx(StoreId, Name, Change) ->
    Path = link_path(StoreId, Name),
    {ok, ok} = khepri:transaction(StoreId, fun() -> change_tx(Path, Change) end),
    ok.

change_tx(Path, Change) ->
    {ok, Link} = khepri_tx:get(Path),
    khepri_tx:put(Path, Change(Link)).

last_event(undefined, #link{last_event = Last}) -> Last;
last_event(LastEvent, _Link) -> LastEvent.

%%====================================================================
%% Internal functions
%%====================================================================

link_path(StoreId, Name) ->
    ?LINKS_PATH ++ [StoreId, Name].

%% @private Convert link record to info map
-spec link_to_info(#link{}) -> link_info().
link_to_info(Link) ->
    Info = #{
        name => Link#link.name,
        source => Link#link.source,
        status => Link#link.status,
        processed => Link#link.processed
    },
    with_reason(Link, case Link#link.last_event of
                          undefined -> Info;
                          LastEvent -> Info#{last_event => LastEvent}
                      end).

with_reason(#link{status = error, reason = Reason}, Info) -> Info#{reason => Reason};
with_reason(_Link, Info) -> Info.

%% @private Set the link's status; a link that is gone has none to set.
-spec set_status(atom(), binary(), stopped) -> ok | {error, term()}.
set_status(StoreId, Name, Status) ->
    Path = link_path(StoreId, Name),
    Tx = fun() -> status_tx(Path, Status) end,
    case khepri:transaction(StoreId, Tx) of
        {ok, ok} -> ok;
        {ok, {error, _} = Error} -> Error;
        {error, _} = Error -> Error
    end.

status_tx(Path, Status) ->
    case khepri_tx:get(Path) of
        {ok, Link} -> khepri_tx:put(Path, Link#link{status = Status});
        {error, _} = Error -> Error
    end.

%% @private Emit telemetry
-spec emit_telemetry(atom(), binary(), created | deleted | started | stopped) -> ok.
emit_telemetry(StoreId, LinkName, Operation) ->
    telemetry:execute(
        [reckon_db, link, Operation],
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, link_name => LinkName}
    ),
    ok.
