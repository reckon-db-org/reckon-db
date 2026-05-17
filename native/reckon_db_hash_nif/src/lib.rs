//! NIF optimizations for reckon-db hashing operations.
//!
//! This module provides high-performance hash implementations:
//! - xxHash64: Extremely fast 64-bit hash
//! - xxHash3: Even faster, modern 64-bit hash
//! - Partition hash: For consistent stream/subscription routing
//!
//! These NIFs are optional - the Erlang wrapper falls back to pure Erlang
//! implementations when the NIF is not available (community edition).

use rustler::{Binary, NifResult};
use xxhash_rust::xxh3::xxh3_64;
use xxhash_rust::xxh64::xxh64;

// ============================================================================
// xxHash64 - Fast 64-bit hash
// ============================================================================

/// Compute xxHash64 of binary data.
///
/// xxHash64 is an extremely fast non-cryptographic hash function.
/// Ideal for hash tables, checksums, and data fingerprinting.
///
/// Arguments:
/// - data: The data to hash (binary)
///
/// Returns:
/// - 64-bit hash value as unsigned integer
#[rustler::nif]
fn nif_xxhash64(data: Binary) -> u64 {
    xxh64(data.as_slice(), 0)
}

/// Compute xxHash64 with a seed.
///
/// Arguments:
/// - data: The data to hash (binary)
/// - seed: Seed value for the hash
///
/// Returns:
/// - 64-bit hash value as unsigned integer
#[rustler::nif]
fn nif_xxhash64_seed(data: Binary, seed: u64) -> u64 {
    xxh64(data.as_slice(), seed)
}

// ============================================================================
// xxHash3 - Modern, faster 64-bit hash
// ============================================================================

/// Compute xxHash3 (64-bit) of binary data.
///
/// xxHash3 is the latest generation of xxHash, even faster than xxHash64
/// especially for small inputs. Uses SIMD when available.
///
/// Arguments:
/// - data: The data to hash (binary)
///
/// Returns:
/// - 64-bit hash value as unsigned integer
#[rustler::nif]
fn nif_xxhash3(data: Binary) -> u64 {
    xxh3_64(data.as_slice())
}

// ============================================================================
// Partition Hashing - For consistent routing
// ============================================================================

/// Hash data and map to a partition number.
///
/// This is used for consistent routing of streams/subscriptions to workers.
/// Uses xxHash3 for speed, then modulo for partition mapping.
///
/// Arguments:
/// - data: The data to hash (binary)
/// - partitions: Number of partitions (must be > 0)
///
/// Returns:
/// - Partition number (0 to partitions-1)
#[rustler::nif]
fn nif_partition_hash(data: Binary, partitions: u32) -> NifResult<u32> {
    if partitions == 0 {
        return Err(rustler::Error::Term(Box::new("partitions must be > 0")));
    }
    let hash = xxh3_64(data.as_slice());
    Ok((hash % partitions as u64) as u32)
}

/// Hash a tuple of {StoreId, StreamId} for stream routing.
///
/// Optimized for the common case of routing streams to workers.
/// Combines both IDs efficiently before hashing.
///
/// Arguments:
/// - store_id: The store identifier (binary)
/// - stream_id: The stream identifier (binary)
/// - partitions: Number of partitions
///
/// Returns:
/// - Partition number (0 to partitions-1)
#[rustler::nif]
fn nif_stream_partition(store_id: Binary, stream_id: Binary, partitions: u32) -> NifResult<u32> {
    if partitions == 0 {
        return Err(rustler::Error::Term(Box::new("partitions must be > 0")));
    }

    // Combine store_id and stream_id with a separator
    let mut combined = Vec::with_capacity(store_id.len() + stream_id.len() + 1);
    combined.extend_from_slice(store_id.as_slice());
    combined.push(0); // Separator byte
    combined.extend_from_slice(stream_id.as_slice());

    let hash = xxh3_64(&combined);
    Ok((hash % partitions as u64) as u32)
}

// ============================================================================
// Batch Hashing - For bulk operations
// ============================================================================

/// Hash multiple binaries and return their partition assignments.
///
/// More efficient than calling partition_hash repeatedly due to
/// reduced NIF call overhead.
///
/// Arguments:
/// - items: List of binaries to hash
/// - partitions: Number of partitions
///
/// Returns:
/// - List of partition numbers in same order as input
#[rustler::nif]
fn nif_partition_hash_batch(items: Vec<Binary>, partitions: u32) -> NifResult<Vec<u32>> {
    if partitions == 0 {
        return Err(rustler::Error::Term(Box::new("partitions must be > 0")));
    }

    let results: Vec<u32> = items
        .iter()
        .map(|item| {
            let hash = xxh3_64(item.as_slice());
            (hash % partitions as u64) as u32
        })
        .collect();

    Ok(results)
}

// ============================================================================
// FNV Hash - Fast for small keys
// ============================================================================

/// Compute FNV-1a hash of binary data.
///
/// FNV-1a is particularly fast for small keys (< 32 bytes).
/// Good for hash tables with small keys like atoms or short strings.
///
/// Arguments:
/// - data: The data to hash (binary)
///
/// Returns:
/// - 64-bit hash value
#[rustler::nif]
fn nif_fnv1a(data: Binary) -> u64 {
    use std::hash::Hasher;
    let mut hasher = fnv::FnvHasher::default();
    hasher.write(data.as_slice());
    hasher.finish()
}

// ============================================================================
// Comparison with phash2
// ============================================================================

/// Hash data similar to erlang:phash2/2 but faster.
///
/// This provides a drop-in replacement for phash2 with better performance.
/// Uses xxHash3 internally but maps to the same range as phash2.
///
/// Arguments:
/// - data: The data to hash (binary)
/// - range: Maximum value (exclusive), like phash2's second argument
///
/// Returns:
/// - Hash value in range [0, range)
#[rustler::nif]
fn nif_fast_phash(data: Binary, range: u32) -> NifResult<u32> {
    if range == 0 {
        return Err(rustler::Error::Term(Box::new("range must be > 0")));
    }
    let hash = xxh3_64(data.as_slice());
    Ok((hash % range as u64) as u32)
}

rustler::init!("reckon_db_hash_nif");
