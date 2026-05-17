//! NIF optimizations for reckon-db pattern matching and filtering operations.
//!
//! This module provides high-performance pattern matching:
//! - Compiled regex patterns for stream ID matching
//! - Batch filtering of stream IDs
//! - Wildcard to regex conversion
//! - Glob pattern matching
//!
//! These NIFs are optional - the Erlang wrapper falls back to pure Erlang
//! implementations when the NIF is not available (community edition).

use regex::Regex;
use rustler::{Binary, Encoder, Env, NifResult, Term};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nomatch,
        match_found
    }
}

// ============================================================================
// Regex Pattern Matching
// ============================================================================

/// Convert a wildcard pattern to a regex pattern.
///
/// Wildcards:
/// - `*` matches any sequence of characters
/// - `?` matches any single character
///
/// All other regex special characters are escaped.
///
/// Arguments:
/// - pattern: Wildcard pattern as binary
///
/// Returns:
/// - Regex pattern as binary
#[rustler::nif]
fn nif_wildcard_to_regex(pattern: Binary) -> NifResult<String> {
    let pattern_str = std::str::from_utf8(pattern.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8")))?;

    let mut result = String::with_capacity(pattern_str.len() * 2 + 2);
    result.push('^');

    for c in pattern_str.chars() {
        match c {
            '*' => result.push_str(".*"),
            '?' => result.push('.'),
            // Escape regex special characters
            '.' | '^' | '$' | '+' | '{' | '}' | '[' | ']' | '\\' | '|' | '(' | ')' => {
                result.push('\\');
                result.push(c);
            }
            _ => result.push(c),
        }
    }

    result.push('$');
    Ok(result)
}

/// Check if a string matches a wildcard pattern.
///
/// Arguments:
/// - text: The string to match
/// - pattern: Wildcard pattern (*, ? supported)
///
/// Returns:
/// - true if matches, false otherwise
#[rustler::nif]
fn nif_wildcard_match(text: Binary, pattern: Binary) -> NifResult<bool> {
    let text_str = std::str::from_utf8(text.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in text")))?;
    let pattern_str = std::str::from_utf8(pattern.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in pattern")))?;

    // Convert wildcard to regex
    let mut regex_pattern = String::with_capacity(pattern_str.len() * 2 + 2);
    regex_pattern.push('^');

    for c in pattern_str.chars() {
        match c {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '.' | '^' | '$' | '+' | '{' | '}' | '[' | ']' | '\\' | '|' | '(' | ')' => {
                regex_pattern.push('\\');
                regex_pattern.push(c);
            }
            _ => regex_pattern.push(c),
        }
    }
    regex_pattern.push('$');

    // Compile and match
    match Regex::new(&regex_pattern) {
        Ok(re) => Ok(re.is_match(text_str)),
        Err(_) => Err(rustler::Error::Term(Box::new("invalid regex pattern"))),
    }
}

/// Check if a string matches a regex pattern.
///
/// Arguments:
/// - text: The string to match
/// - regex_pattern: Regex pattern
///
/// Returns:
/// - true if matches, false otherwise
#[rustler::nif]
fn nif_regex_match(text: Binary, regex_pattern: Binary) -> NifResult<bool> {
    let text_str = std::str::from_utf8(text.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in text")))?;
    let pattern_str = std::str::from_utf8(regex_pattern.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in pattern")))?;

    match Regex::new(pattern_str) {
        Ok(re) => Ok(re.is_match(text_str)),
        Err(_) => Err(rustler::Error::Term(Box::new("invalid regex pattern"))),
    }
}

/// Filter a list of strings by wildcard pattern.
///
/// Arguments:
/// - items: List of binaries to filter
/// - pattern: Wildcard pattern
///
/// Returns:
/// - List of matching items
#[rustler::nif]
fn nif_filter_by_wildcard<'a>(
    env: Env<'a>,
    items: Vec<Binary<'a>>,
    pattern: Binary,
) -> NifResult<Vec<Term<'a>>> {
    let pattern_str = std::str::from_utf8(pattern.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in pattern")))?;

    // Convert wildcard to regex
    let mut regex_pattern = String::with_capacity(pattern_str.len() * 2 + 2);
    regex_pattern.push('^');

    for c in pattern_str.chars() {
        match c {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '.' | '^' | '$' | '+' | '{' | '}' | '[' | ']' | '\\' | '|' | '(' | ')' => {
                regex_pattern.push('\\');
                regex_pattern.push(c);
            }
            _ => regex_pattern.push(c),
        }
    }
    regex_pattern.push('$');

    let re = Regex::new(&regex_pattern)
        .map_err(|_| rustler::Error::Term(Box::new("invalid regex pattern")))?;

    let results: Vec<Term<'a>> = items
        .into_iter()
        .filter(|item| {
            if let Ok(s) = std::str::from_utf8(item.as_slice()) {
                re.is_match(s)
            } else {
                false
            }
        })
        .map(|item| item.encode(env))
        .collect();

    Ok(results)
}

/// Filter a list of strings by regex pattern.
///
/// Arguments:
/// - items: List of binaries to filter
/// - regex_pattern: Regex pattern
///
/// Returns:
/// - List of matching items
#[rustler::nif]
fn nif_filter_by_regex<'a>(
    env: Env<'a>,
    items: Vec<Binary<'a>>,
    regex_pattern: Binary,
) -> NifResult<Vec<Term<'a>>> {
    let pattern_str = std::str::from_utf8(regex_pattern.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in pattern")))?;

    let re = Regex::new(pattern_str)
        .map_err(|_| rustler::Error::Term(Box::new("invalid regex pattern")))?;

    let results: Vec<Term<'a>> = items
        .into_iter()
        .filter(|item| {
            if let Ok(s) = std::str::from_utf8(item.as_slice()) {
                re.is_match(s)
            } else {
                false
            }
        })
        .map(|item| item.encode(env))
        .collect();

    Ok(results)
}

// ============================================================================
// Prefix Matching (Optimized)
// ============================================================================

/// Check if a string has a specific prefix.
///
/// More efficient than regex for simple prefix checks.
///
/// Arguments:
/// - text: The string to check
/// - prefix: The prefix to match
///
/// Returns:
/// - true if text starts with prefix, false otherwise
#[rustler::nif]
fn nif_has_prefix(text: Binary, prefix: Binary) -> bool {
    if prefix.len() > text.len() {
        return false;
    }
    text.as_slice()[..prefix.len()] == *prefix.as_slice()
}

/// Check if a string has a specific suffix.
///
/// Arguments:
/// - text: The string to check
/// - suffix: The suffix to match
///
/// Returns:
/// - true if text ends with suffix, false otherwise
#[rustler::nif]
fn nif_has_suffix(text: Binary, suffix: Binary) -> bool {
    if suffix.len() > text.len() {
        return false;
    }
    text.as_slice()[text.len() - suffix.len()..] == *suffix.as_slice()
}

/// Filter items by prefix.
///
/// Arguments:
/// - items: List of binaries to filter
/// - prefix: Prefix to match
///
/// Returns:
/// - List of items starting with prefix
#[rustler::nif]
fn nif_filter_by_prefix<'a>(
    env: Env<'a>,
    items: Vec<Binary<'a>>,
    prefix: Binary,
) -> Vec<Term<'a>> {
    let prefix_slice = prefix.as_slice();
    let prefix_len = prefix_slice.len();

    items
        .into_iter()
        .filter(|item| {
            item.len() >= prefix_len && item.as_slice()[..prefix_len] == *prefix_slice
        })
        .map(|item| item.encode(env))
        .collect()
}

/// Filter items by suffix.
///
/// Arguments:
/// - items: List of binaries to filter
/// - suffix: Suffix to match
///
/// Returns:
/// - List of items ending with suffix
#[rustler::nif]
fn nif_filter_by_suffix<'a>(
    env: Env<'a>,
    items: Vec<Binary<'a>>,
    suffix: Binary,
) -> Vec<Term<'a>> {
    let suffix_slice = suffix.as_slice();
    let suffix_len = suffix_slice.len();

    items
        .into_iter()
        .filter(|item| {
            item.len() >= suffix_len
                && item.as_slice()[item.len() - suffix_len..] == *suffix_slice
        })
        .map(|item| item.encode(env))
        .collect()
}

// ============================================================================
// Batch Operations
// ============================================================================

/// Check multiple strings against a wildcard pattern.
///
/// Returns indices of matching items (more efficient than returning items).
///
/// Arguments:
/// - items: List of binaries to check
/// - pattern: Wildcard pattern
///
/// Returns:
/// - List of indices (0-based) of matching items
#[rustler::nif]
fn nif_match_indices(items: Vec<Binary>, pattern: Binary) -> NifResult<Vec<u32>> {
    let pattern_str = std::str::from_utf8(pattern.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in pattern")))?;

    // Convert wildcard to regex
    let mut regex_pattern = String::with_capacity(pattern_str.len() * 2 + 2);
    regex_pattern.push('^');

    for c in pattern_str.chars() {
        match c {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '.' | '^' | '$' | '+' | '{' | '}' | '[' | ']' | '\\' | '|' | '(' | ')' => {
                regex_pattern.push('\\');
                regex_pattern.push(c);
            }
            _ => regex_pattern.push(c),
        }
    }
    regex_pattern.push('$');

    let re = Regex::new(&regex_pattern)
        .map_err(|_| rustler::Error::Term(Box::new("invalid regex pattern")))?;

    let indices: Vec<u32> = items
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            if let Ok(s) = std::str::from_utf8(item.as_slice()) {
                if re.is_match(s) {
                    Some(idx as u32)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    Ok(indices)
}

/// Count items matching a wildcard pattern.
///
/// Arguments:
/// - items: List of binaries to check
/// - pattern: Wildcard pattern
///
/// Returns:
/// - Count of matching items
#[rustler::nif]
fn nif_count_matches(items: Vec<Binary>, pattern: Binary) -> NifResult<u64> {
    let pattern_str = std::str::from_utf8(pattern.as_slice())
        .map_err(|_| rustler::Error::Term(Box::new("invalid utf8 in pattern")))?;

    // Convert wildcard to regex
    let mut regex_pattern = String::with_capacity(pattern_str.len() * 2 + 2);
    regex_pattern.push('^');

    for c in pattern_str.chars() {
        match c {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '.' | '^' | '$' | '+' | '{' | '}' | '[' | ']' | '\\' | '|' | '(' | ')' => {
                regex_pattern.push('\\');
                regex_pattern.push(c);
            }
            _ => regex_pattern.push(c),
        }
    }
    regex_pattern.push('$');

    let re = Regex::new(&regex_pattern)
        .map_err(|_| rustler::Error::Term(Box::new("invalid regex pattern")))?;

    let count = items
        .iter()
        .filter(|item| {
            if let Ok(s) = std::str::from_utf8(item.as_slice()) {
                re.is_match(s)
            } else {
                false
            }
        })
        .count() as u64;

    Ok(count)
}

/// Validate that a pattern is a valid regex.
///
/// Arguments:
/// - pattern: Regex pattern to validate
///
/// Returns:
/// - true if valid, false otherwise
#[rustler::nif]
fn nif_is_valid_regex(pattern: Binary) -> bool {
    if let Ok(s) = std::str::from_utf8(pattern.as_slice()) {
        Regex::new(s).is_ok()
    } else {
        false
    }
}

rustler::init!("reckon_db_filter_nif");
