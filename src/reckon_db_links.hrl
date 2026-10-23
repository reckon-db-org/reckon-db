%% The stored link: what reckon_db_links writes at create and
%% reckon_db_link_consumer advances while the link runs.

-define(LINKS_PATH, [links]).
-define(LINK_STREAM_PREFIX, <<"$link:">>).

%% positions: per source stream, the last version the link has handled
%% (written to its stream, or passed over by its filter). undefined until
%% the link first starts; see reckon_db_link_consumer.
%% link_version: the link-stream version those positions account for; a
%% link-stream event past it raises its source position on start.
%% reason: why a link with status error stopped.
-record(link, {
    name :: binary(),
    source :: reckon_db_links:source_spec(),
    filter :: reckon_db_links:filter_fun() | undefined,
    %% The caller's fun: transform_fun() is its contract, nothing holds it
    %% to it, so the consumer checks what it returns.
    transform :: fun((event()) -> term()) | undefined,
    backfill :: boolean(),
    created_at :: integer(),
    status = stopped :: running | stopped | error,
    processed = 0 :: non_neg_integer(),
    last_event :: binary() | undefined,
    positions :: #{binary() => integer()} | undefined,
    link_version = -1 :: integer(),
    reason :: term()
}).
