//! NIF optimizations for reckon-db causation graph operations.
//!
//! This module provides high-performance graph algorithms for causation analysis:
//! - Building directed graphs from event causation relationships
//! - Topological sorting for event ordering
//! - DOT format export for visualization
//! - Cycle detection and graph validation
//!
//! These NIFs are optional - the Erlang wrapper falls back to pure Erlang
//! implementations when the NIF is not available (community edition).

use petgraph::algo::{has_path_connecting, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use rustler::{Atom, Binary, Encoder, Env, NewBinary, NifResult, Term};
use std::collections::HashMap;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        cycle_detected,
        invalid_graph,
        node_count,
        edge_count,
        root_count,
        leaf_count,
        max_depth,
        undefined
    }
}

// ============================================================================
// Graph Building
// ============================================================================

/// Build edges from a list of event nodes.
///
/// Each node is a tuple: (event_id, causation_id | undefined)
/// Returns a list of edges: [{from_id, to_id}]
///
/// Arguments:
/// - nodes: List of {event_id, causation_id} tuples
///
/// Returns:
/// - List of {cause_id, effect_id} edges
#[rustler::nif]
fn nif_build_edges<'a>(env: Env<'a>, nodes: Term<'a>) -> NifResult<Vec<Term<'a>>> {
    let mut edges = Vec::new();
    let mut event_ids: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut parsed_nodes: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

    // Parse nodes and collect event IDs
    let node_list: Vec<Term<'a>> = nodes.decode()?;
    for node_term in node_list {
        let tuple: (Binary<'a>, Term<'a>) = node_term.decode()?;
        let event_id = tuple.0.as_slice().to_vec();
        event_ids.insert(event_id.clone());

        // Check if causation_id is undefined atom or binary
        let causation_id = if let Ok(atom) = tuple.1.decode::<Atom>() {
            if atom == atoms::undefined() {
                None
            } else {
                None // Any other atom is treated as None
            }
        } else if let Ok(binary) = tuple.1.decode::<Binary>() {
            Some(binary.as_slice().to_vec())
        } else {
            None
        };

        parsed_nodes.push((event_id, causation_id));
    }

    // Build edges
    for (event_id, causation_id) in parsed_nodes {
        if let Some(cause_id) = causation_id {
            if event_ids.contains(&cause_id) {
                // Create the edge tuple
                let mut from_bin = NewBinary::new(env, cause_id.len());
                from_bin.as_mut_slice().copy_from_slice(&cause_id);
                let mut to_bin = NewBinary::new(env, event_id.len());
                to_bin.as_mut_slice().copy_from_slice(&event_id);
                let edge: Term<'a> =
                    (Term::from(from_bin), Term::from(to_bin)).encode(env);
                edges.push(edge);
            }
        }
    }

    Ok(edges)
}

/// Find root nodes (nodes with no incoming edges).
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
///
/// Returns:
/// - List of root event_ids
#[rustler::nif]
fn nif_find_roots<'a>(
    env: Env<'a>,
    nodes: Vec<Binary<'a>>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
) -> Vec<Term<'a>> {
    // Collect all targets (nodes with incoming edges)
    let targets: std::collections::HashSet<Vec<u8>> =
        edges.iter().map(|(_, to)| to.as_slice().to_vec()).collect();

    // Roots are nodes not in the targets set
    nodes
        .into_iter()
        .filter(|node| !targets.contains(node.as_slice()))
        .map(|node| node.encode(env))
        .collect()
}

/// Find leaf nodes (nodes with no outgoing edges).
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
///
/// Returns:
/// - List of leaf event_ids
#[rustler::nif]
fn nif_find_leaves<'a>(
    env: Env<'a>,
    nodes: Vec<Binary<'a>>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
) -> Vec<Term<'a>> {
    // Collect all sources (nodes with outgoing edges)
    let sources: std::collections::HashSet<Vec<u8>> = edges
        .iter()
        .map(|(from, _)| from.as_slice().to_vec())
        .collect();

    // Leaves are nodes not in the sources set
    nodes
        .into_iter()
        .filter(|node| !sources.contains(node.as_slice()))
        .map(|node| node.encode(env))
        .collect()
}

// ============================================================================
// Topological Sort
// ============================================================================

/// Perform topological sort on the graph.
///
/// Returns nodes in dependency order (causes before effects).
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
///
/// Returns:
/// - {ok, [event_id]} on success (sorted order)
/// - {error, cycle_detected} if the graph has cycles
#[rustler::nif]
fn nif_topo_sort<'a>(
    env: Env<'a>,
    nodes: Vec<Binary<'a>>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
) -> Term<'a> {
    // Build petgraph DiGraph
    let mut graph: DiGraph<Vec<u8>, ()> = DiGraph::new();
    let mut id_to_index: HashMap<Vec<u8>, NodeIndex> = HashMap::new();

    // Add nodes
    for node in &nodes {
        let id = node.as_slice().to_vec();
        let idx = graph.add_node(id.clone());
        id_to_index.insert(id, idx);
    }

    // Add edges
    for (from, to) in edges {
        let from_id = from.as_slice().to_vec();
        let to_id = to.as_slice().to_vec();

        if let (Some(&from_idx), Some(&to_idx)) =
            (id_to_index.get(&from_id), id_to_index.get(&to_id))
        {
            graph.add_edge(from_idx, to_idx, ());
        }
    }

    // Perform topological sort
    match toposort(&graph, None) {
        Ok(sorted_indices) => {
            let sorted: Vec<Term<'a>> = sorted_indices
                .iter()
                .map(|&idx| {
                    let id = &graph[idx];
                    let mut binary = NewBinary::new(env, id.len());
                    binary.as_mut_slice().copy_from_slice(id);
                    binary.into()
                })
                .collect();
            (atoms::ok(), sorted).encode(env)
        }
        Err(_cycle) => (atoms::error(), atoms::cycle_detected()).encode(env),
    }
}

/// Check if the graph contains cycles.
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
///
/// Returns:
/// - true if graph has cycles, false otherwise
#[rustler::nif]
fn nif_has_cycle(nodes: Vec<Binary>, edges: Vec<(Binary, Binary)>) -> bool {
    // Build petgraph DiGraph
    let mut graph: DiGraph<(), ()> = DiGraph::new();
    let mut id_to_index: HashMap<Vec<u8>, NodeIndex> = HashMap::new();

    // Add nodes
    for node in &nodes {
        let id = node.as_slice().to_vec();
        let idx = graph.add_node(());
        id_to_index.insert(id, idx);
    }

    // Add edges
    for (from, to) in edges {
        let from_id = from.as_slice().to_vec();
        let to_id = to.as_slice().to_vec();

        if let (Some(&from_idx), Some(&to_idx)) =
            (id_to_index.get(&from_id), id_to_index.get(&to_id))
        {
            graph.add_edge(from_idx, to_idx, ());
        }
    }

    // Try topological sort - if it fails, there's a cycle
    toposort(&graph, None).is_err()
}

// ============================================================================
// Graph Metrics
// ============================================================================

/// Get graph statistics.
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
///
/// Returns:
/// - #{node_count, edge_count, root_count, leaf_count, max_depth}
#[rustler::nif]
fn nif_graph_stats<'a>(
    env: Env<'a>,
    nodes: Vec<Binary<'a>>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
) -> Term<'a> {
    let node_count = nodes.len();
    let edge_count = edges.len();

    // Find roots and leaves
    let targets: std::collections::HashSet<Vec<u8>> =
        edges.iter().map(|(_, to)| to.as_slice().to_vec()).collect();

    let sources: std::collections::HashSet<Vec<u8>> = edges
        .iter()
        .map(|(from, _)| from.as_slice().to_vec())
        .collect();

    let node_ids: std::collections::HashSet<Vec<u8>> =
        nodes.iter().map(|n| n.as_slice().to_vec()).collect();

    let root_count = node_ids.iter().filter(|n| !targets.contains(*n)).count();
    let leaf_count = node_ids.iter().filter(|n| !sources.contains(*n)).count();

    // Calculate max depth via BFS from roots
    let max_depth = calculate_max_depth(&nodes, &edges);

    // Build result map
    let result = vec![
        (atoms::node_count().encode(env), node_count.encode(env)),
        (atoms::edge_count().encode(env), edge_count.encode(env)),
        (atoms::root_count().encode(env), root_count.encode(env)),
        (atoms::leaf_count().encode(env), leaf_count.encode(env)),
        (atoms::max_depth().encode(env), max_depth.encode(env)),
    ];

    Term::map_from_pairs(env, &result).unwrap()
}

fn calculate_max_depth(nodes: &[Binary], edges: &[(Binary, Binary)]) -> u32 {
    if nodes.is_empty() {
        return 0;
    }

    // Build adjacency list
    let mut adjacency: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
    for node in nodes {
        adjacency.insert(node.as_slice().to_vec(), Vec::new());
    }
    for (from, to) in edges {
        if let Some(children) = adjacency.get_mut(from.as_slice()) {
            children.push(to.as_slice().to_vec());
        }
    }

    // Find roots
    let targets: std::collections::HashSet<Vec<u8>> =
        edges.iter().map(|(_, to)| to.as_slice().to_vec()).collect();

    let roots: Vec<Vec<u8>> = nodes
        .iter()
        .map(|n| n.as_slice().to_vec())
        .filter(|n| !targets.contains(n))
        .collect();

    if roots.is_empty() {
        return 0;
    }

    // BFS to find max depth
    let mut max_depth = 0;
    let mut visited: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<(Vec<u8>, u32)> =
        roots.into_iter().map(|r| (r, 0)).collect();

    while let Some((node, depth)) = queue.pop_front() {
        if visited.contains(&node) {
            continue;
        }
        visited.insert(node.clone());
        max_depth = max_depth.max(depth);

        if let Some(children) = adjacency.get(&node) {
            for child in children {
                if !visited.contains(child) {
                    queue.push_back((child.clone(), depth + 1));
                }
            }
        }
    }

    max_depth
}

// ============================================================================
// DOT Format Export
// ============================================================================

/// Generate DOT format for Graphviz visualization.
///
/// Arguments:
/// - nodes: List of {event_id, event_type, label} tuples
/// - edges: List of {from_id, to_id} tuples
///
/// Returns:
/// - DOT format as binary
#[rustler::nif]
fn nif_to_dot<'a>(
    nodes: Vec<(Binary<'a>, Binary<'a>, Binary<'a>)>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
) -> NifResult<String> {
    let mut dot = String::with_capacity(nodes.len() * 100 + edges.len() * 50);

    dot.push_str("digraph causation {\n");
    dot.push_str("  rankdir=TB;\n");
    dot.push_str("  node [shape=box, style=rounded];\n\n");

    // Node definitions
    for (event_id, event_type, label) in &nodes {
        let id_str = std::str::from_utf8(event_id.as_slice()).unwrap_or("?");
        let type_str = std::str::from_utf8(event_type.as_slice()).unwrap_or("?");
        let label_str = std::str::from_utf8(label.as_slice()).unwrap_or("?");

        // Escape for DOT format
        let escaped_id = escape_dot_string(id_str);
        let escaped_type = escape_dot_string(type_str);
        let escaped_label = escape_dot_string(label_str);

        dot.push_str(&format!(
            "  \"{}\" [label=\"{}\\n{}\"];\n",
            escaped_id, escaped_label, escaped_type
        ));
    }

    dot.push('\n');

    // Edge definitions
    for (from, to) in edges {
        let from_str = std::str::from_utf8(from.as_slice()).unwrap_or("?");
        let to_str = std::str::from_utf8(to.as_slice()).unwrap_or("?");

        dot.push_str(&format!(
            "  \"{}\" -> \"{}\";\n",
            escape_dot_string(from_str),
            escape_dot_string(to_str)
        ));
    }

    dot.push_str("}\n");

    Ok(dot)
}

/// Generate DOT format with simplified node labels (just type).
///
/// Arguments:
/// - nodes: List of {event_id, event_type} tuples
/// - edges: List of {from_id, to_id} tuples
///
/// Returns:
/// - DOT format as binary
#[rustler::nif]
fn nif_to_dot_simple<'a>(
    nodes: Vec<(Binary<'a>, Binary<'a>)>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
) -> NifResult<String> {
    let mut dot = String::with_capacity(nodes.len() * 80 + edges.len() * 50);

    dot.push_str("digraph causation {\n");
    dot.push_str("  rankdir=TB;\n");
    dot.push_str("  node [shape=box, style=rounded];\n\n");

    // Node definitions
    for (event_id, event_type) in &nodes {
        let id_str = std::str::from_utf8(event_id.as_slice()).unwrap_or("?");
        let type_str = std::str::from_utf8(event_type.as_slice()).unwrap_or("?");

        let escaped_id = escape_dot_string(id_str);
        let escaped_type = escape_dot_string(type_str);

        dot.push_str(&format!(
            "  \"{}\" [label=\"{}\"];\n",
            escaped_id, escaped_type
        ));
    }

    dot.push('\n');

    // Edge definitions
    for (from, to) in edges {
        let from_str = std::str::from_utf8(from.as_slice()).unwrap_or("?");
        let to_str = std::str::from_utf8(to.as_slice()).unwrap_or("?");

        dot.push_str(&format!(
            "  \"{}\" -> \"{}\";\n",
            escape_dot_string(from_str),
            escape_dot_string(to_str)
        ));
    }

    dot.push_str("}\n");

    Ok(dot)
}

fn escape_dot_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

// ============================================================================
// Path Finding
// ============================================================================

/// Check if there's a path between two nodes.
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
/// - from: Source event_id
/// - to: Target event_id
///
/// Returns:
/// - true if path exists, false otherwise
#[rustler::nif]
fn nif_has_path(
    nodes: Vec<Binary>,
    edges: Vec<(Binary, Binary)>,
    from: Binary,
    to: Binary,
) -> bool {
    // Build petgraph DiGraph
    let mut graph: DiGraph<(), ()> = DiGraph::new();
    let mut id_to_index: HashMap<Vec<u8>, NodeIndex> = HashMap::new();

    // Add nodes
    for node in &nodes {
        let id = node.as_slice().to_vec();
        let idx = graph.add_node(());
        id_to_index.insert(id, idx);
    }

    // Add edges
    for (edge_from, edge_to) in edges {
        let from_id = edge_from.as_slice().to_vec();
        let to_id = edge_to.as_slice().to_vec();

        if let (Some(&from_idx), Some(&to_idx)) =
            (id_to_index.get(&from_id), id_to_index.get(&to_id))
        {
            graph.add_edge(from_idx, to_idx, ());
        }
    }

    // Check path
    let from_id = from.as_slice().to_vec();
    let to_id = to.as_slice().to_vec();

    match (id_to_index.get(&from_id), id_to_index.get(&to_id)) {
        (Some(&from_idx), Some(&to_idx)) => has_path_connecting(&graph, from_idx, to_idx, None),
        _ => false,
    }
}

/// Get all ancestors of a node (nodes that can reach it).
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
/// - target: Target event_id
///
/// Returns:
/// - List of ancestor event_ids
#[rustler::nif]
fn nif_get_ancestors<'a>(
    env: Env<'a>,
    nodes: Vec<Binary<'a>>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
    target: Binary<'a>,
) -> Vec<Term<'a>> {
    // Build reverse adjacency list
    let mut reverse_adj: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
    for node in &nodes {
        reverse_adj.insert(node.as_slice().to_vec(), Vec::new());
    }
    for (from, to) in &edges {
        if let Some(parents) = reverse_adj.get_mut(to.as_slice()) {
            parents.push(from.as_slice().to_vec());
        }
    }

    // BFS from target, following reverse edges
    let target_id = target.as_slice().to_vec();
    let mut visited: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<Vec<u8>> =
        std::collections::VecDeque::from([target_id.clone()]);
    let mut ancestors: Vec<Vec<u8>> = Vec::new();

    while let Some(node) = queue.pop_front() {
        if visited.contains(&node) {
            continue;
        }
        visited.insert(node.clone());

        if let Some(parents) = reverse_adj.get(&node) {
            for parent in parents {
                if !visited.contains(parent) {
                    ancestors.push(parent.clone());
                    queue.push_back(parent.clone());
                }
            }
        }
    }

    ancestors
        .into_iter()
        .map(|id| {
            let mut binary = NewBinary::new(env, id.len());
            binary.as_mut_slice().copy_from_slice(&id);
            let term: Term<'a> = binary.into();
            term
        })
        .collect()
}

/// Get all descendants of a node (nodes reachable from it).
///
/// Arguments:
/// - nodes: List of event_id binaries
/// - edges: List of {from_id, to_id} tuples
/// - source: Source event_id
///
/// Returns:
/// - List of descendant event_ids
#[rustler::nif]
fn nif_get_descendants<'a>(
    env: Env<'a>,
    nodes: Vec<Binary<'a>>,
    edges: Vec<(Binary<'a>, Binary<'a>)>,
    source: Binary<'a>,
) -> Vec<Term<'a>> {
    // Build adjacency list
    let mut adjacency: HashMap<Vec<u8>, Vec<Vec<u8>>> = HashMap::new();
    for node in &nodes {
        adjacency.insert(node.as_slice().to_vec(), Vec::new());
    }
    for (from, to) in &edges {
        if let Some(children) = adjacency.get_mut(from.as_slice()) {
            children.push(to.as_slice().to_vec());
        }
    }

    // BFS from source
    let source_id = source.as_slice().to_vec();
    let mut visited: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut queue: std::collections::VecDeque<Vec<u8>> =
        std::collections::VecDeque::from([source_id.clone()]);
    let mut descendants: Vec<Vec<u8>> = Vec::new();

    while let Some(node) = queue.pop_front() {
        if visited.contains(&node) {
            continue;
        }
        visited.insert(node.clone());

        if let Some(children) = adjacency.get(&node) {
            for child in children {
                if !visited.contains(child) {
                    descendants.push(child.clone());
                    queue.push_back(child.clone());
                }
            }
        }
    }

    descendants
        .into_iter()
        .map(|id| {
            let mut binary = NewBinary::new(env, id.len());
            binary.as_mut_slice().copy_from_slice(&id);
            let term: Term<'a> = binary.into();
            term
        })
        .collect()
}

rustler::init!("reckon_db_graph_nif");
