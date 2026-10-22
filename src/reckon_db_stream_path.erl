%% @doc The single source of truth for the Khepri stream layout.
%%
%% reckon-db stores events under a **structural aggregate-type subtree**
%% (DESIGN_STREAM_NAMESPACE.md, Model C):
%%
%% ```
%% USER:    [streams, <<"ride">>, <<"abc…hex">>, PaddedVersion] -> #event{}
%% SYSTEM:  [streams, <<"$link">>, <<"name">>,    PaddedVersion] -> #event{}
%% DCB:     [streams, <<"_dcb">>,  SeqKey]                        -> #event{}
%% '''
%%
%% The aggregate **type** (the user-id prefix, or the `$'-namespace for
%% system streams) is a real Khepri path level rather than a leading
%% substring of an opaque id. This makes type-scoped operations
%% (`list_streams', replay, links) navigate a subtree instead of scanning
%% the whole store.
%%
%% This module is the ONLY place that knows the 4-level shape. Every other
%% module builds stream paths through these functions, so the layout lives
%% in exactly one file. The opaque round-trip — a stream id written under
%% `event_path/2' must reconstruct byte-identical via `stream_id_from_path/1'
%% — is the load-bearing correctness property and is exercised directly by
%% `reckon_db_stream_path_tests'.
%%
%% The DCB pseudo-stream (`_dcb') keeps its existing 2-level
%% `[streams, _dcb, SeqKey]' shape (a flat seq-keyed log, not an aggregate);
%% its paths are built by {@link reckon_db_dcb_paths}, but the readers here
%% (`stream_id_from_path/1') tolerate it because global reads span both
%% depths.
%%
%% @author rgfaber

-module(reckon_db_stream_path).

-include("reckon_db.hrl").

-type validation_error() ::
    {invalid_stream_id, reckon_gater_stream_id:validation_error(), term()}.
-include_lib("khepri/include/khepri.hrl").

-export([
    validate/1,
    event_path/2,
    stream_path/1,
    version_counter_path/1,
    life_gen_path/1,
    versions_pattern/1,
    type_streams_pattern/1,
    all_events_pattern/0,
    stream_id_from_path/1,
    type_of/1
]).

-export_type([validation_error/0]).

%%====================================================================
%% The boundary check
%%====================================================================

%% @doc Does this stream id decompose into path segments?
%%
%% Call this BEFORE any builder below. The builders raise
%% `{invalid_stream_id, StreamId}' (they have no error channel of their
%% own), and that raise used to fire inside a gateway worker: the worker
%% died, the retry layer read the exit as transient, and the caller
%% waited out the backoff for an id the store could never serve
%% (reckon-db#4). A caller that validates first turns a malformed id
%% into an answer.
%%
%% The reason is `reckon_gater_stream_id:validate/1''s own:
%% `empty', `not_binary', `malformed_user_id', `malformed_system_id'.
%% so the shape matches the one `reckon_db_streams:append/5' already
%% returns, which the gater's retry layer classifies as permanent.
-spec validate(term()) -> ok | {error, validation_error()}.
validate(StreamId) ->
    case reckon_gater_stream_id:validate(StreamId) of
        ok -> ok;
        {error, Reason} -> {error, {invalid_stream_id, Reason, StreamId}}
    end.

%%====================================================================
%% Path builders
%%====================================================================

%% @doc Khepri path of a single event:
%% `[streams, Type, Id, PaddedVersion]'.
%%
%% Raises `{invalid_stream_id, StreamId}' for ids that don't decompose
%% (including the DCB pseudo-stream, whose paths are built elsewhere).
-spec event_path(binary(), binary()) -> khepri_path:native_path().
event_path(StreamId, PaddedVersion) ->
    {Type, Id} = id_nodes(StreamId),
    [streams, Type, Id, PaddedVersion].

%% @doc Khepri path of the aggregate node (all versions live beneath it):
%% `[streams, Type, Id]'. Used by exists/delete and version counting.
-spec stream_path(binary()) -> khepri_path:native_path().
stream_path(StreamId) ->
    {Type, Id} = id_nodes(StreamId),
    [streams, Type, Id].

%% @doc Pattern matching every version node of one stream:
%% `[streams, Type, Id, *]'. Used by `get_version' (count).
-spec versions_pattern(binary()) -> khepri_path:native_pattern().
versions_pattern(StreamId) ->
    {Type, Id} = id_nodes(StreamId),
    [streams, Type, Id, ?KHEPRI_WILDCARD_STAR].

%% @doc Khepri path of a stream's version counter:
%% `[metadata, stream_versions, Type, Id]' -> integer.
%%
%% Deliberately NOT a child of the stream node: a stream's latest version
%% must not be derived from how many version nodes it has (scavenging
%% deletes nodes without renumbering), and a node under `[streams, Type,
%% Id]' would be counted as a version and read as an event.
-spec version_counter_path(binary()) -> khepri_path:native_path().
version_counter_path(StreamId) ->
    {Type, Id} = id_nodes(StreamId),
    ?STREAM_VERSION_COUNTERS_PATH ++ [Type, Id].

%% @doc Khepri path of a stream's life generation:
%% `[metadata, integrity, life_gen, Type, Id]' -> integer.
%%
%% A sibling of the version counter, and deliberately NOT removed by a stream
%% delete: a re-created stream must mint a generation above the one it had, so
%% a node holding the deleted life's entry refuses a restore of it.
-spec life_gen_path(binary()) -> khepri_path:native_path().
life_gen_path(StreamId) ->
    {Type, Id} = id_nodes(StreamId),
    ?STREAM_LIFE_GENS_PATH ++ [Type, Id].

%% @doc Pattern matching every aggregate-id node of one type:
%% `[streams, Type, *]'. Used by links to navigate a type subtree
%% (`order-*') instead of scanning the whole store.
-spec type_streams_pattern(binary()) -> khepri_path:native_pattern().
type_streams_pattern(Type) when is_binary(Type) ->
    [streams, Type, ?KHEPRI_WILDCARD_STAR].

%% @doc Pattern matching every regular (user/system) event leaf across
%% the store: `[streams, *, *, leaf-with-data]'. This is the 4-level
%% regular shape ONLY — the 2-level DCB log is at a different depth and
%% must be queried separately (see `reckon_db_streams' global readers).
-spec all_events_pattern() -> khepri_path:native_pattern().
all_events_pattern() ->
    [streams,
     ?KHEPRI_WILDCARD_STAR,
     ?KHEPRI_WILDCARD_STAR,
     #if_all{conditions = [
         ?KHEPRI_WILDCARD_STAR,
         #if_has_data{has_data = true}
     ]}].

%%====================================================================
%% Path readers (the inverse — reconstruct the opaque id)
%%====================================================================

%% @doc Reconstruct the opaque stream id from a Khepri path. The inverse
%% of `event_path/2' / `stream_path/1'. Tolerates the 2-level DCB node
%% (returns the reserved `_dcb' id) so global readers spanning both
%% depths can label every result.
-spec stream_id_from_path(khepri_path:native_path()) -> binary().
stream_id_from_path([streams, ?DCB_STREAM | _]) ->
    ?DCB_STREAM;
stream_id_from_path([streams, Type, Id | _]) when is_binary(Type), is_binary(Id) ->
    recombine(Type, Id);
stream_id_from_path(_) ->
    <<>>.

%% @doc The aggregate-type node for a stream id (`<<"ride">>',
%% `<<"$link">>'). Raises for ids that don't decompose.
-spec type_of(binary()) -> binary().
type_of(StreamId) ->
    {Type, _Id} = id_nodes(StreamId),
    Type.

%%====================================================================
%% Internal
%%====================================================================

%% @private Split a stream id into its {TypeNode, IdNode} Khepri path
%% segments. System streams keep their leading `$' on the type node so
%% the `$'-namespace stays structurally reserved and distinct from any
%% user type of the same name.
-spec id_nodes(binary()) -> {binary(), binary()}.
id_nodes(StreamId) ->
    case reckon_gater_stream_id:parts(StreamId) of
        {user, Type, Id} ->
            {Type, Id};
        {system, Ns, Name} ->
            {<<"$", Ns/binary>>, Name};
        {error, malformed} ->
            erlang:error({invalid_stream_id, StreamId})
    end.

%% @private Rejoin a {TypeNode, IdNode} pair into the opaque id. System
%% type nodes start with `$' and rejoin with `:'; user type nodes rejoin
%% with `-'.
-spec recombine(binary(), binary()) -> nonempty_binary().
recombine(<<"$", _/binary>> = Type, Name) ->
    <<Type/binary, ":", Name/binary>>;
recombine(Type, Id) ->
    <<Type/binary, "-", Id/binary>>.
