%% @doc Telemetry events for reckon-db
%% @author rgfaber

-ifndef(RECKON_DB_TELEMETRY_HRL).
-define(RECKON_DB_TELEMETRY_HRL, true).

%%====================================================================
%% Stream Telemetry Events
%%====================================================================

%% Emitted when starting to write events to a stream
%% Measurements: system_time
%% Metadata: store_id, stream_id, event_count, expected_version
-define(STREAM_WRITE_START, [reckon_db, stream, write, start]).

%% Emitted after successfully writing events to a stream
%% Measurements: duration, event_count
%% Metadata: store_id, stream_id, new_version
-define(STREAM_WRITE_STOP, [reckon_db, stream, write, stop]).

%% Emitted when a stream write fails
%% Measurements: duration
%% Metadata: store_id, stream_id, reason
-define(STREAM_WRITE_ERROR, [reckon_db, stream, write, error]).

%% Emitted when starting to read events from a stream
%% Measurements: system_time
%% Metadata: store_id, stream_id, start_version, count, direction
-define(STREAM_READ_START, [reckon_db, stream, read, start]).

%% Emitted after successfully reading events from a stream
%% Measurements: duration, event_count
%% Metadata: store_id, stream_id
-define(STREAM_READ_STOP, [reckon_db, stream, read, stop]).

%%====================================================================
%% Subscription Telemetry Events
%%====================================================================

%% Emitted when a subscription is created
%% Measurements: system_time
%% Metadata: store_id, subscription_id, type, selector
-define(SUBSCRIPTION_CREATED, [reckon_db, subscription, created]).

%% Emitted when a subscription is deleted
%% Measurements: system_time
%% Metadata: store_id, subscription_id
-define(SUBSCRIPTION_DELETED, [reckon_db, subscription, deleted]).

%% Emitted when an event is delivered to a subscriber
%% Measurements: duration
%% Metadata: store_id, subscription_id, event_id, event_type
-define(SUBSCRIPTION_EVENT_DELIVERED, [reckon_db, subscription, event_delivered]).

%% Emitted when a subscription checkpoint is updated
%% Measurements: position
%% Metadata: store_id, subscription_name
-define(SUBSCRIPTION_CHECKPOINT, [reckon_db, subscription, checkpoint]).

%%====================================================================
%% Snapshot Telemetry Events
%%====================================================================

%% Emitted when a snapshot is created
%% Measurements: system_time, size_bytes
%% Metadata: store_id, stream_id, version
-define(SNAPSHOT_CREATED, [reckon_db, snapshot, created]).

%% Emitted when a snapshot is read
%% Measurements: duration, size_bytes
%% Metadata: store_id, stream_id, version
-define(SNAPSHOT_READ, [reckon_db, snapshot, read]).

%%====================================================================
%% Cluster Telemetry Events
%%====================================================================

%% Emitted when a node joins the cluster
%% Measurements: system_time
%% Metadata: store_id, node, member_count
-define(CLUSTER_NODE_UP, [reckon_db, cluster, node, up]).

%% Emitted when a node leaves the cluster
%% Measurements: system_time
%% Metadata: store_id, node, member_count, reason
-define(CLUSTER_NODE_DOWN, [reckon_db, cluster, node, down]).

%% Emitted when a new leader is elected
%% Measurements: system_time
%% Metadata: store_id, leader_node, previous_leader
-define(CLUSTER_LEADER_ELECTED, [reckon_db, cluster, leader, elected]).

%%====================================================================
%% Store Telemetry Events
%%====================================================================

%% Emitted when a store is started
%% Measurements: system_time
%% Metadata: store_id, mode, data_dir
-define(STORE_STARTED, [reckon_db, store, started]).

%% Emitted when a store is stopped
%% Measurements: system_time, uptime_ms
%% Metadata: store_id, reason
-define(STORE_STOPPED, [reckon_db, store, stopped]).

%%====================================================================
%% Emitter Telemetry Events
%%====================================================================

%% Emitted when an emitter broadcasts an event
%% Measurements: duration
%% Metadata: store_id, subscription_id, event_id, recipient_count
-define(EMITTER_BROADCAST, [reckon_db, emitter, broadcast]).

%% Emitted when an emitter pool is created
%% Measurements: system_time
%% Metadata: store_id, subscription_id, pool_size
-define(EMITTER_POOL_CREATED, [reckon_db, emitter, pool, created]).

%%====================================================================
%% Temporal Query Telemetry Events
%%====================================================================

%% Emitted when a read_until query completes
%% Measurements: duration, event_count
%% Metadata: store_id, stream_id, timestamp
-define(TEMPORAL_READ_UNTIL, [reckon_db, temporal, read_until]).

%% Emitted when a read_range query completes
%% Measurements: duration, event_count
%% Metadata: store_id, stream_id, timestamp (tuple of {from, to})
-define(TEMPORAL_READ_RANGE, [reckon_db, temporal, read_range]).

%%====================================================================
%% Scavenge Telemetry Events
%%====================================================================

%% Emitted when a scavenge operation completes
%% Measurements: duration, deleted_count
%% Metadata: store_id, stream_id, archived
-define(SCAVENGE_COMPLETE, [reckon_db, scavenge, complete]).

%%====================================================================
%% Causation Telemetry Events
%%====================================================================

%% Emitted when a causation query completes
%% Measurements: duration, event_count
%% Metadata: store_id, id, query_type (causation_effects, causation_cause, etc.)
-define(CAUSATION_QUERY, [reckon_db, causation, query]).

%%====================================================================
%% Backpressure Telemetry Events
%%====================================================================

%% Emitted when subscription queue reaches warning threshold
%% Measurements: queue_size, max_size
%% Metadata: store_id, subscription_key
-define(BACKPRESSURE_WARNING, [reckon_db, subscription, backpressure, warning]).

%% Emitted when events are dropped due to backpressure
%% Measurements: count
%% Metadata: store_id, subscription_key, strategy
-define(BACKPRESSURE_DROPPED, [reckon_db, subscription, backpressure, dropped]).

%%====================================================================
%% Schema Telemetry Events
%%====================================================================

%% Emitted when a schema is registered
%% Measurements: version
%% Metadata: store_id, event_type
-define(SCHEMA_REGISTERED, [reckon_db, schema, registered]).

%% Emitted when a schema is unregistered
%% Measurements: version (0)
%% Metadata: store_id, event_type
-define(SCHEMA_UNREGISTERED, [reckon_db, schema, unregistered]).

%% Emitted when an event is upcasted
%% Measurements: duration
%% Metadata: store_id, event_type, from_version, to_version
-define(SCHEMA_UPCASTED, [reckon_db, schema, upcasted]).

%%====================================================================
%% Memory Pressure Telemetry Events
%%====================================================================

%% Emitted when memory pressure level changes
%% Measurements: usage_ratio
%% Metadata: old_level, new_level
-define(MEMORY_PRESSURE_CHANGED, [reckon_db, memory, pressure_changed]).

%%====================================================================
%% Consistency Checker Telemetry Events
%%====================================================================

%% Emitted when a consistency check completes
%% Measurements: duration_us
%% Metadata: store_id, status, checks
-define(CONSISTENCY_CHECK_COMPLETE, [reckon_db, consistency, check, complete]).

%% Emitted when consistency status changes
%% Measurements: system_time
%% Metadata: store_id, old_status, new_status
-define(CONSISTENCY_STATUS_CHANGED, [reckon_db, consistency, status, changed]).

%% Emitted when split-brain is detected
%% Measurements: system_time
%% Metadata: store_id, result
-define(SPLIT_BRAIN_DETECTED, [reckon_db, consistency, split_brain, detected]).

%% Emitted when quorum is lost
%% Measurements: system_time
%% Metadata: store_id, available_nodes, required_quorum
-define(QUORUM_LOST, [reckon_db, consistency, quorum, lost]).

%% Emitted when quorum is restored
%% Measurements: system_time
%% Metadata: store_id, available_nodes, required_quorum
-define(QUORUM_RESTORED, [reckon_db, consistency, quorum, restored]).

%%====================================================================
%% Health Prober Telemetry Events
%%====================================================================

%% Emitted when a health probe completes
%% Measurements: duration_us, success_count, failure_count
%% Metadata: store_id
-define(HEALTH_PROBE_COMPLETE, [reckon_db, health, probe, complete]).

%% Emitted when a node is declared failed after threshold
%% Measurements: system_time, consecutive_failures
%% Metadata: store_id, node
-define(HEALTH_NODE_FAILED, [reckon_db, health, node, failed]).

%% Emitted when a failed node recovers
%% Measurements: system_time
%% Metadata: store_id, node
-define(HEALTH_NODE_RECOVERED, [reckon_db, health, node, recovered]).

%%====================================================================
%% Link Telemetry Events
%%====================================================================

%% Emitted when a link is created
%% Measurements: system_time
%% Metadata: store_id, link_name
-define(LINK_CREATED, [reckon_db, link, created]).

%% Emitted when a link is deleted
%% Measurements: system_time
%% Metadata: store_id, link_name
-define(LINK_DELETED, [reckon_db, link, deleted]).

%% Emitted when a link is started
%% Measurements: system_time
%% Metadata: store_id, link_name
-define(LINK_STARTED, [reckon_db, link, started]).

%% Emitted when a link is stopped
%% Measurements: system_time
%% Metadata: store_id, link_name
-define(LINK_STOPPED, [reckon_db, link, stopped]).

-endif. %% RECKON_DB_TELEMETRY_HRL
