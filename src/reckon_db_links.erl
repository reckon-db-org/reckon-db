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
%%     source => #{type => stream, name => <<"$link:webhook-feed">>},
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
%% == Status ==
%%
%% This module ships with the initial reckon-db release. The
%% feature is wired but **not yet exercised in production**;
%% treat as preview while we accumulate real usage.
%%
%% @author rgfaber

-module(reckon_db_links).

-include("reckon_db.hrl").
-include_lib("khepri/include/khepri.hrl").

%% API
-export([
    create/2,
    delete/2,
    get/2,
    list/1,
    start/2,
    stop/2,
    info/2
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
    last_event => binary()
}.

-export_type([link_spec/0, link_info/0, source_spec/0, filter_fun/0, transform_fun/0]).

%%====================================================================
%% Khepri Paths
%%====================================================================

-define(LINKS_PATH, [links]).
-define(LINK_STREAM_PREFIX, <<"$link:">>).

%%====================================================================
%% Link Record
%%====================================================================

-record(link, {
    name :: binary(),
    source :: source_spec(),
    filter :: filter_fun() | undefined,
    transform :: transform_fun() | undefined,
    backfill :: boolean(),
    created_at :: integer(),
    status = stopped :: running | stopped | error,
    processed = 0 :: non_neg_integer(),
    last_event :: binary() | undefined
}).

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
-spec create(atom(), link_spec()) -> ok | {error, term()}.
create(StoreId, Spec) ->
    Name = maps:get(name, Spec),
    Source = maps:get(source, Spec),
    Filter = maps:get(filter, Spec, undefined),
    Transform = maps:get(transform, Spec, undefined),
    Backfill = maps:get(backfill, Spec, false),

    Link = #link{
        name = Name,
        source = Source,
        filter = Filter,
        transform = Transform,
        backfill = Backfill,
        created_at = erlang:system_time(millisecond),
        status = stopped,
        processed = 0
    },

    Path = ?LINKS_PATH ++ [StoreId, Name],
    case khepri:put(StoreId, Path, Link) of
        ok ->
            emit_telemetry(StoreId, Name, created),
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Delete a link.
-spec delete(atom(), binary()) -> ok | {error, term()}.
delete(StoreId, Name) ->
    %% Stop the link first if running
    stop(StoreId, Name),

    Path = ?LINKS_PATH ++ [StoreId, Name],
    case khepri:delete(StoreId, Path) of
        ok ->
            emit_telemetry(StoreId, Name, deleted),
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Get a link by name.
-spec get(atom(), binary()) -> {ok, link_info()} | {error, not_found}.
get(StoreId, Name) ->
    Path = ?LINKS_PATH ++ [StoreId, Name],
    case khepri:get(StoreId, Path) of
        {ok, Link} ->
            {ok, link_to_info(Link)};
        {error, {khepri, node_not_found, _}} ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.

%% @doc List all links.
-spec list(atom()) -> {ok, [link_info()]} | {error, term()}.
list(StoreId) ->
    Path = ?LINKS_PATH ++ [StoreId, ?KHEPRI_WILDCARD_STAR],
    case khepri:get_many(StoreId, Path) of
        {ok, Links} when is_map(Links) ->
            Infos = [link_to_info(L) || {_, L} <- maps:to_list(Links)],
            {ok, Infos};
        {error, _} = Error ->
            Error
    end.

%% @doc Start processing a link.
%%
%% This will:
%% 1. Subscribe to source stream(s)
%% 2. Optionally backfill existing events
%% 3. Apply filter and transform to each event
%% 4. Write matching events to the link stream
-spec start(atom(), binary()) -> ok | {error, term()}.
start(StoreId, Name) ->
    case get_link(StoreId, Name) of
        {ok, Link} ->
            update_status(StoreId, Name, running),
            maybe_backfill_link(StoreId, Link),
            start_link_subscription(StoreId, Link),
            emit_telemetry(StoreId, Name, started),
            ok;
        {error, _} = Error ->
            Error
    end.

maybe_backfill_link(StoreId, #link{backfill = true} = Link) ->
    backfill_link(StoreId, Link);
maybe_backfill_link(_StoreId, _Link) ->
    ok.

%% @doc Stop processing a link.
-spec stop(atom(), binary()) -> ok | {error, term()}.
stop(StoreId, Name) ->
    %% Stop subscription
    stop_link_subscription(StoreId, Name),

    %% Update status
    update_status(StoreId, Name, stopped),

    emit_telemetry(StoreId, Name, stopped),
    ok.

%% @doc Get detailed link info.
-spec info(atom(), binary()) -> {ok, map()} | {error, not_found}.
info(StoreId, Name) ->
    case get_link(StoreId, Name) of
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
            {ok, Info};
        Error ->
            Error
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get raw link record
-spec get_link(atom(), binary()) -> {ok, #link{}} | {error, not_found}.
get_link(StoreId, Name) ->
    Path = ?LINKS_PATH ++ [StoreId, Name],
    case khepri:get(StoreId, Path) of
        {ok, Link} ->
            {ok, Link};
        {error, {khepri, node_not_found, _}} ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.

%% @private Convert link record to info map
-spec link_to_info(#link{}) -> link_info().
link_to_info(Link) ->
    Info = #{
        name => Link#link.name,
        source => Link#link.source,
        status => Link#link.status,
        processed => Link#link.processed
    },
    case Link#link.last_event of
        undefined -> Info;
        LastEvent -> Info#{last_event => LastEvent}
    end.

%% @private Update link status
-spec update_status(atom(), binary(), running | stopped | error) -> ok.
update_status(StoreId, Name, Status) ->
    Path = ?LINKS_PATH ++ [StoreId, Name],
    case khepri:get(StoreId, Path) of
        {ok, Link} ->
            khepri:put(StoreId, Path, Link#link{status = Status}),
            ok;
        _ ->
            ok
    end.

%% @private Get link stream ID
-spec link_stream_id(binary()) -> binary().
link_stream_id(Name) ->
    <<?LINK_STREAM_PREFIX/binary, Name/binary>>.

%% @private Backfill link with existing events
-spec backfill_link(atom(), #link{}) -> ok.
backfill_link(StoreId, Link) ->
    SourceStreams = get_source_streams(StoreId, Link#link.source),

    lists:foreach(fun(StreamId) -> process_link_stream(StoreId, Link, StreamId) end,
                  SourceStreams),
    ok.

%% @private Read one source stream and feed its events into the link.
process_link_stream(StoreId, Link, StreamId) ->
    case reckon_db_streams:read(StoreId, StreamId, 0, 10000, forward) of
        {ok, Events} -> process_events_for_link(StoreId, Link, Events);
        {error, _} -> ok
    end.

%% @private Get source stream IDs based on source spec
-spec get_source_streams(atom(), source_spec()) -> [binary()].
get_source_streams(_StoreId, #{type := stream, stream_id := StreamId}) ->
    [StreamId];
get_source_streams(StoreId, #{type := stream_pattern, pattern := Pattern}) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, Streams} ->
            filter_by_pattern(Streams, Pattern);
        {error, _} ->
            []
    end;
get_source_streams(StoreId, #{type := all}) ->
    case reckon_db_streams:list_streams(StoreId) of
        {ok, Streams} ->
            %% Exclude link streams
            [S || S <- Streams, not is_link_stream(S)];
        {error, _} ->
            []
    end.

%% @private Check if stream is a link stream
-spec is_link_stream(binary()) -> boolean().
is_link_stream(StreamId) ->
    Prefix = ?LINK_STREAM_PREFIX,
    PrefixSize = byte_size(Prefix),
    case StreamId of
        <<Prefix:PrefixSize/binary, _/binary>> -> true;
        _ -> false
    end.

%% @private Filter streams by pattern
-spec filter_by_pattern([binary()], binary()) -> [binary()].
filter_by_pattern(Streams, Pattern) ->
    RegexPattern = wildcard_to_regex(Pattern),
    [S || S <- Streams, re:run(S, RegexPattern) =/= nomatch].

%% @private Convert wildcard to regex
-spec wildcard_to_regex(binary()) -> binary().
wildcard_to_regex(Pattern) ->
    Escaped = re:replace(Pattern, <<"[.^$+?{}\\[\\]\\\\|()]">>, <<"\\\\&">>, [global, {return, binary}]),
    Converted = binary:replace(Escaped, <<"*">>, <<".*">>, [global]),
    <<"^", Converted/binary, "$">>.

%% @private Process events through link filter/transform
-spec process_events_for_link(atom(), #link{}, [event()]) -> ok.
process_events_for_link(StoreId, Link, Events) ->
    Filter = Link#link.filter,
    Transform = Link#link.transform,
    LinkStreamId = link_stream_id(Link#link.name),

    ProcessedEvents = lists:filtermap(
                        fun(Event) -> filter_and_transform(Filter, Transform, Event) end,
                        Events),

    append_processed_events(StoreId, Link#link.name, LinkStreamId, ProcessedEvents).

%% @private Keep + transform events that pass the link filter.
filter_and_transform(Filter, Transform, Event) ->
    case apply_filter(Filter, Event) of
        true -> {true, apply_transform(Transform, Event)};
        false -> false
    end.

append_processed_events(_StoreId, _LinkName, _LinkStreamId, []) ->
    ok;
append_processed_events(StoreId, LinkName, LinkStreamId, ProcessedEvents) ->
    EventMaps = [event_to_map(E) || E <- ProcessedEvents],
    case reckon_db_streams:append(StoreId, LinkStreamId, -2, EventMaps) of
        {ok, _} ->
            update_processed_count(StoreId, LinkName, length(ProcessedEvents)),
            ok;
        {error, _} ->
            ok
    end.

%% @private Apply filter function
-spec apply_filter(filter_fun() | undefined, event()) -> boolean().
apply_filter(undefined, _Event) ->
    true;
apply_filter(Filter, Event) when is_function(Filter, 1) ->
    try
        Filter(Event)
    catch
        _:_ -> false
    end.

%% @private Apply transform function
-spec apply_transform(transform_fun() | undefined, event()) -> event().
apply_transform(undefined, Event) ->
    Event;
apply_transform(Transform, Event) when is_function(Transform, 1) ->
    try
        Transform(Event)
    catch
        _:_ -> Event
    end.

%% @private Convert event record to map for appending
-spec event_to_map(event()) -> map().
event_to_map(Event) ->
    #{
        event_id => Event#event.event_id,
        event_type => Event#event.event_type,
        data => Event#event.data,
        metadata => maps:merge(Event#event.metadata, #{
            source_stream => Event#event.stream_id,
            source_version => Event#event.version
        })
    }.

%% @private Update processed count
-spec update_processed_count(atom(), binary(), pos_integer()) -> ok.
update_processed_count(StoreId, Name, Count) ->
    Path = ?LINKS_PATH ++ [StoreId, Name],
    case khepri:get(StoreId, Path) of
        {ok, Link} ->
            khepri:put(StoreId, Path, Link#link{
                processed = Link#link.processed + Count
            }),
            ok;
        _ ->
            ok
    end.

%% @private Start subscription for link
-spec start_link_subscription(atom(), #link{}) -> ok.
start_link_subscription(StoreId, Link) ->
    %% Create subscription based on source type
    SubscriptionName = <<"$link-sub:", (Link#link.name)/binary>>,

    case Link#link.source of
        #{type := stream, stream_id := StreamId} ->
            reckon_db_subscriptions:subscribe(
                StoreId, stream, StreamId, SubscriptionName,
                #{handler => fun(Events) ->
                    process_events_for_link(StoreId, Link, Events)
                end}
            );
        #{type := stream_pattern, pattern := Pattern} ->
            reckon_db_subscriptions:subscribe(
                StoreId, event_pattern, Pattern, SubscriptionName,
                #{handler => fun(Events) ->
                    process_events_for_link(StoreId, Link, Events)
                end}
            );
        #{type := all} ->
            reckon_db_subscriptions:subscribe(
                StoreId, all, <<"*">>, SubscriptionName,
                #{handler => fun(Events) ->
                    process_events_for_link(StoreId, Link, Events)
                end}
            )
    end,
    ok.

%% @private Stop subscription for link
-spec stop_link_subscription(atom(), binary()) -> ok.
stop_link_subscription(StoreId, Name) ->
    SubscriptionName = <<"$link-sub:", Name/binary>>,
    reckon_db_subscriptions:unsubscribe(StoreId, SubscriptionName),
    ok.

%% @private Emit telemetry
-spec emit_telemetry(atom(), binary(), atom()) -> ok.
emit_telemetry(StoreId, LinkName, Operation) ->
    telemetry:execute(
        [reckon_db, link, Operation],
        #{system_time => erlang:system_time(millisecond)},
        #{store_id => StoreId, link_name => LinkName}
    ),
    ok.
