//! NIF optimizations for reckon-db event aggregation operations.
//!
//! This module provides high-performance aggregation for event streams:
//! - Bulk fold with tagged value semantics ({sum, N}, {overwrite, V})
//! - Vectorized sum accumulation for numeric fields
//! - Batch map merge operations
//!
//! These NIFs are optional - the Erlang wrapper falls back to pure Erlang
//! implementations when the NIF is not available (community edition).

use rustler::{Atom, Encoder, Env, NifResult, Term};
use std::collections::HashMap;

mod atoms {
    rustler::atoms! {
        sum,
        overwrite,
        data,
        ok,
        error
    }
}

/// Represents a tagged value in the aggregation system.
#[derive(Clone, Debug)]
enum TaggedValue<'a> {
    Sum(f64),
    Overwrite(Term<'a>),
    Plain(Term<'a>),
}

/// Extract the numeric value from a term.
fn term_to_f64(term: Term) -> Option<f64> {
    if let Ok(i) = term.decode::<i64>() {
        Some(i as f64)
    } else if let Ok(f) = term.decode::<f64>() {
        Some(f)
    } else {
        None
    }
}

/// Parse a term into a TaggedValue.
fn parse_tagged_value<'a>(term: Term<'a>) -> TaggedValue<'a> {
    // Check if it's a tuple like {sum, N} or {overwrite, V}
    if let Ok(tuple) = term.decode::<(Atom, Term<'a>)>() {
        let (tag, value) = tuple;
        if tag == atoms::sum() {
            if let Some(num) = term_to_f64(value) {
                return TaggedValue::Sum(num);
            }
        } else if tag == atoms::overwrite() {
            return TaggedValue::Overwrite(value);
        }
    }
    TaggedValue::Plain(term)
}

/// Get the current numeric value from an existing tagged value.
fn get_current_number(value: &TaggedValue) -> f64 {
    match value {
        TaggedValue::Sum(n) => *n,
        TaggedValue::Plain(term) => term_to_f64(*term).unwrap_or(0.0),
        TaggedValue::Overwrite(term) => term_to_f64(*term).unwrap_or(0.0),
    }
}

/// Apply a single field update to the state.
fn apply_field<'a>(
    state: &mut HashMap<Term<'a>, TaggedValue<'a>>,
    key: Term<'a>,
    value: TaggedValue<'a>,
) {
    match value {
        TaggedValue::Sum(num) => {
            let current = state.get(&key).map(get_current_number).unwrap_or(0.0);
            state.insert(key, TaggedValue::Sum(current + num));
        }
        TaggedValue::Overwrite(v) => {
            state.insert(key, TaggedValue::Plain(v));
        }
        TaggedValue::Plain(v) => {
            state.insert(key, TaggedValue::Plain(v));
        }
    }
}

/// Apply a data map to the state.
fn apply_data<'a>(
    state: &mut HashMap<Term<'a>, TaggedValue<'a>>,
    data: Term<'a>,
) -> NifResult<()> {
    // data should be a map
    let iter = data.decode::<rustler::MapIterator>()?;
    for (key, value) in iter {
        let tagged = parse_tagged_value(value);
        apply_field(state, key, tagged);
    }
    Ok(())
}

/// Extract data from an event (handles both map and record formats).
fn get_event_data<'a>(event: Term<'a>) -> Option<Term<'a>> {
    // Try to get 'data' key from map
    if let Ok(iter) = event.decode::<rustler::MapIterator>() {
        for (key, value) in iter {
            if let Ok(atom) = key.decode::<Atom>() {
                if atom == atoms::data() {
                    return Some(value);
                }
            }
        }
        // If no 'data' key, treat the entire map as data
        return Some(event);
    }
    None
}

/// Convert state back to Erlang terms.
fn state_to_term<'a>(
    env: Env<'a>,
    state: &HashMap<Term<'a>, TaggedValue<'a>>,
    finalize: bool,
) -> Term<'a> {
    let pairs: Vec<(Term<'a>, Term<'a>)> = state
        .iter()
        .map(|(k, v)| {
            let value = if finalize {
                match v {
                    TaggedValue::Sum(n) => {
                        // Return as integer if whole number, else float
                        if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                            (*n as i64).encode(env)
                        } else {
                            n.encode(env)
                        }
                    }
                    TaggedValue::Overwrite(t) | TaggedValue::Plain(t) => *t,
                }
            } else {
                match v {
                    TaggedValue::Sum(n) => {
                        let num = if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                            (*n as i64).encode(env)
                        } else {
                            n.encode(env)
                        };
                        (atoms::sum(), num).encode(env)
                    }
                    TaggedValue::Overwrite(t) => (atoms::overwrite(), *t).encode(env),
                    TaggedValue::Plain(t) => *t,
                }
            };
            (*k, value)
        })
        .collect();

    Term::map_from_pairs(env, &pairs).unwrap_or_else(|_| rustler::types::atom::nil().encode(env))
}

// ============================================================================
// NIF Functions
// ============================================================================

/// Aggregate a list of events with tagged value semantics.
///
/// This is the main aggregation function that processes events in order,
/// applying tagged value rules ({sum, N} adds, {overwrite, V} replaces).
///
/// Arguments:
/// - events: List of event maps (each with optional 'data' key)
/// - initial_state: Starting state map
/// - finalize: If true, unwrap tagged values in result
///
/// Returns:
/// - Aggregated state map
#[rustler::nif]
fn nif_aggregate_events<'a>(
    env: Env<'a>,
    events: Term<'a>,
    initial_state: Term<'a>,
    finalize: bool,
) -> NifResult<Term<'a>> {
    let mut state: HashMap<Term<'a>, TaggedValue<'a>> = HashMap::new();

    // Load initial state
    if let Ok(iter) = initial_state.decode::<rustler::MapIterator>() {
        for (key, value) in iter {
            state.insert(key, parse_tagged_value(value));
        }
    }

    // Process events
    let event_list = events.decode::<Vec<Term<'a>>>()?;
    for event in event_list {
        if let Some(data) = get_event_data(event) {
            if data.is_map() {
                apply_data(&mut state, data)?;
            }
        }
    }

    Ok(state_to_term(env, &state, finalize))
}

/// Sum a specific field across all events.
///
/// Efficiently accumulates numeric values from a named field.
///
/// Arguments:
/// - events: List of event maps
/// - field: The field name (atom or binary) to sum
///
/// Returns:
/// - Sum of all values for that field
#[rustler::nif]
fn nif_sum_field<'a>(events: Term<'a>, field: Term<'a>) -> NifResult<f64> {
    let event_list = events.decode::<Vec<Term<'a>>>()?;
    let mut sum = 0.0;

    for event in event_list {
        if let Some(data) = get_event_data(event) {
            if let Ok(iter) = data.decode::<rustler::MapIterator>() {
                for (key, value) in iter {
                    // Compare keys (handles both atom and binary)
                    if rustler::Term::eq(&key, &field) {
                        // Handle tagged values
                        match parse_tagged_value(value) {
                            TaggedValue::Sum(n) => sum += n,
                            TaggedValue::Plain(v) | TaggedValue::Overwrite(v) => {
                                if let Some(n) = term_to_f64(v) {
                                    sum += n;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(sum)
}

/// Count events matching a simple field condition.
///
/// Arguments:
/// - events: List of event maps
/// - field: The field name to check
/// - value: The value to match
///
/// Returns:
/// - Count of matching events
#[rustler::nif]
fn nif_count_where<'a>(events: Term<'a>, field: Term<'a>, expected: Term<'a>) -> NifResult<u64> {
    let event_list = events.decode::<Vec<Term<'a>>>()?;
    let mut count = 0u64;

    for event in event_list {
        if let Some(data) = get_event_data(event) {
            if let Ok(iter) = data.decode::<rustler::MapIterator>() {
                for (key, value) in iter {
                    if rustler::Term::eq(&key, &field) && rustler::Term::eq(&value, &expected) {
                        count += 1;
                        break;
                    }
                }
            }
        }
    }

    Ok(count)
}

/// Merge a batch of key-value pairs into a state map.
///
/// Applies tagged value semantics to each pair.
///
/// Arguments:
/// - pairs: List of {Key, TaggedValue} tuples
/// - state: Current state map
///
/// Returns:
/// - Updated state map
#[rustler::nif]
fn nif_merge_tagged_batch<'a>(
    env: Env<'a>,
    pairs: Term<'a>,
    state: Term<'a>,
) -> NifResult<Term<'a>> {
    let mut result: HashMap<Term<'a>, TaggedValue<'a>> = HashMap::new();

    // Load current state
    if let Ok(iter) = state.decode::<rustler::MapIterator>() {
        for (key, value) in iter {
            result.insert(key, parse_tagged_value(value));
        }
    }

    // Apply pairs
    let pair_list = pairs.decode::<Vec<(Term<'a>, Term<'a>)>>()?;
    for (key, value) in pair_list {
        let tagged = parse_tagged_value(value);
        apply_field(&mut result, key, tagged);
    }

    Ok(state_to_term(env, &result, false))
}

/// Finalize a tagged map by unwrapping all tagged values.
///
/// Converts {sum, N} to N and {overwrite, V} to V.
///
/// Arguments:
/// - tagged_map: Map with potentially tagged values
///
/// Returns:
/// - Map with all values unwrapped
#[rustler::nif]
fn nif_finalize<'a>(env: Env<'a>, tagged_map: Term<'a>) -> NifResult<Term<'a>> {
    let mut state: HashMap<Term<'a>, TaggedValue<'a>> = HashMap::new();

    if let Ok(iter) = tagged_map.decode::<rustler::MapIterator>() {
        for (key, value) in iter {
            state.insert(key, parse_tagged_value(value));
        }
    }

    Ok(state_to_term(env, &state, true))
}

/// Get statistics about event aggregation.
///
/// Returns counts and basic metrics about the event list.
///
/// Arguments:
/// - events: List of event maps
///
/// Returns:
/// - Map with statistics: total_events, events_with_data, unique_fields
#[rustler::nif]
fn nif_aggregation_stats<'a>(env: Env<'a>, events: Term<'a>) -> NifResult<Term<'a>> {
    let event_list = events.decode::<Vec<Term<'a>>>()?;
    let mut events_with_data = 0u64;
    let mut field_counts: HashMap<String, u64> = HashMap::new();

    for event in &event_list {
        if let Some(data) = get_event_data(*event) {
            if let Ok(iter) = data.decode::<rustler::MapIterator>() {
                events_with_data += 1;
                for (key, _) in iter {
                    // Try to get a string representation of the key
                    let key_str = if let Ok(atom) = key.decode::<Atom>() {
                        format!("{:?}", atom)
                    } else if let Ok(s) = key.decode::<String>() {
                        s
                    } else {
                        format!("{:?}", key)
                    };
                    *field_counts.entry(key_str).or_insert(0) += 1;
                }
            }
        }
    }

    let total_events = event_list.len() as u64;
    let unique_fields = field_counts.len() as u64;

    let pairs: Vec<(Atom, u64)> = vec![
        (Atom::from_str(env, "total_events").unwrap(), total_events),
        (Atom::from_str(env, "events_with_data").unwrap(), events_with_data),
        (Atom::from_str(env, "unique_fields").unwrap(), unique_fields),
    ];

    Ok(Term::map_from_pairs(env, &pairs).unwrap())
}

rustler::init!("reckon_db_aggregate_nif");
