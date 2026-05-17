//! NIF optimizations for reckon-db archive compression.
//!
//! This module provides high-performance compression implementations:
//! - LZ4: Fastest compression/decompression (real-time use)
//! - Zstd: Best compression ratio (cold storage)
//! - Zlib: Standard compatibility
//!
//! These NIFs are optional - the Erlang wrapper falls back to pure Erlang
//! implementations when the NIF is not available (community edition).

use flate2::read::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use rustler::{Atom, Binary, Env, NifResult, OwnedBinary};
use std::io::Read;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        lz4,
        zstd,
        zlib,
        unknown_algorithm,
        compression_error,
        decompression_error,
    }
}

// ============================================================================
// LZ4 Compression (Fastest)
// ============================================================================

/// Compress data using LZ4 algorithm.
///
/// LZ4 is optimized for speed - ideal for real-time compression where
/// latency matters more than compression ratio.
///
/// Returns compressed binary on success.
#[rustler::nif]
fn nif_compress_lz4<'a>(env: Env<'a>, data: Binary) -> NifResult<Binary<'a>> {
    let compressed = lz4_flex::compress_prepend_size(data.as_slice());

    let mut output = OwnedBinary::new(compressed.len()).ok_or(rustler::Error::Term(Box::new(
        "Failed to allocate binary",
    )))?;
    output.as_mut_slice().copy_from_slice(&compressed);

    Ok(output.release(env))
}

/// Decompress LZ4-compressed data.
///
/// Returns decompressed binary on success, error tuple on failure.
#[rustler::nif]
fn nif_decompress_lz4<'a>(env: Env<'a>, data: Binary) -> NifResult<(Atom, Binary<'a>)> {
    match lz4_flex::decompress_size_prepended(data.as_slice()) {
        Ok(decompressed) => {
            let mut output =
                OwnedBinary::new(decompressed.len()).ok_or(rustler::Error::Term(Box::new(
                    "Failed to allocate binary",
                )))?;
            output.as_mut_slice().copy_from_slice(&decompressed);
            Ok((atoms::ok(), output.release(env)))
        }
        Err(_) => {
            let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
                "Failed to allocate binary",
            )))?;
            Ok((atoms::error(), output.release(env)))
        }
    }
}

// ============================================================================
// Zstandard Compression (Best Ratio)
// ============================================================================

/// Compress data using Zstandard algorithm.
///
/// Zstd provides excellent compression ratios while maintaining good speed.
/// Ideal for cold storage and archival.
///
/// Level parameter: 1-22 (default 3, higher = better ratio but slower)
#[rustler::nif]
fn nif_compress_zstd<'a>(env: Env<'a>, data: Binary, level: i32) -> NifResult<Binary<'a>> {
    let compressed = zstd::encode_all(data.as_slice(), level).map_err(|_| {
        rustler::Error::Term(Box::new("Zstd compression failed"))
    })?;

    let mut output = OwnedBinary::new(compressed.len()).ok_or(rustler::Error::Term(Box::new(
        "Failed to allocate binary",
    )))?;
    output.as_mut_slice().copy_from_slice(&compressed);

    Ok(output.release(env))
}

/// Decompress Zstd-compressed data.
///
/// Returns decompressed binary on success, error tuple on failure.
#[rustler::nif]
fn nif_decompress_zstd<'a>(env: Env<'a>, data: Binary) -> NifResult<(Atom, Binary<'a>)> {
    match zstd::decode_all(data.as_slice()) {
        Ok(decompressed) => {
            let mut output =
                OwnedBinary::new(decompressed.len()).ok_or(rustler::Error::Term(Box::new(
                    "Failed to allocate binary",
                )))?;
            output.as_mut_slice().copy_from_slice(&decompressed);
            Ok((atoms::ok(), output.release(env)))
        }
        Err(_) => {
            let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
                "Failed to allocate binary",
            )))?;
            Ok((atoms::error(), output.release(env)))
        }
    }
}

// ============================================================================
// Zlib Compression (Compatibility)
// ============================================================================

/// Compress data using Zlib algorithm.
///
/// Zlib is the standard compression used by Erlang's term_to_binary/2.
/// Provided for compatibility when exchanging data with pure Erlang systems.
///
/// Level parameter: 0-9 (0 = no compression, 9 = best compression)
#[rustler::nif]
fn nif_compress_zlib<'a>(env: Env<'a>, data: Binary, level: u32) -> NifResult<Binary<'a>> {
    let compression = Compression::new(level.min(9));
    let mut encoder = ZlibEncoder::new(data.as_slice(), compression);
    let mut compressed = Vec::new();

    encoder.read_to_end(&mut compressed).map_err(|_| {
        rustler::Error::Term(Box::new("Zlib compression failed"))
    })?;

    let mut output = OwnedBinary::new(compressed.len()).ok_or(rustler::Error::Term(Box::new(
        "Failed to allocate binary",
    )))?;
    output.as_mut_slice().copy_from_slice(&compressed);

    Ok(output.release(env))
}

/// Decompress Zlib-compressed data.
///
/// Returns decompressed binary on success, error tuple on failure.
#[rustler::nif]
fn nif_decompress_zlib<'a>(env: Env<'a>, data: Binary) -> NifResult<(Atom, Binary<'a>)> {
    let mut decoder = ZlibDecoder::new(data.as_slice());
    let mut decompressed = Vec::new();

    match decoder.read_to_end(&mut decompressed) {
        Ok(_) => {
            let mut output =
                OwnedBinary::new(decompressed.len()).ok_or(rustler::Error::Term(Box::new(
                    "Failed to allocate binary",
                )))?;
            output.as_mut_slice().copy_from_slice(&decompressed);
            Ok((atoms::ok(), output.release(env)))
        }
        Err(_) => {
            let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
                "Failed to allocate binary",
            )))?;
            Ok((atoms::error(), output.release(env)))
        }
    }
}

// ============================================================================
// Unified API
// ============================================================================

/// Compress data using specified algorithm.
///
/// Algorithm atoms: lz4, zstd, zlib
/// Returns {ok, CompressedData} or {error, Reason}
#[rustler::nif]
fn nif_compress<'a>(
    env: Env<'a>,
    data: Binary,
    algorithm: Atom,
    level: i32,
) -> NifResult<(Atom, Binary<'a>)> {
    if algorithm == atoms::lz4() {
        // LZ4 compression
        let compressed = lz4_flex::compress_prepend_size(data.as_slice());
        let mut output = OwnedBinary::new(compressed.len()).ok_or(rustler::Error::Term(Box::new(
            "Failed to allocate binary",
        )))?;
        output.as_mut_slice().copy_from_slice(&compressed);
        Ok((atoms::ok(), output.release(env)))
    } else if algorithm == atoms::zstd() {
        // Zstd compression
        let compressed = zstd::encode_all(data.as_slice(), level).map_err(|_| {
            rustler::Error::Term(Box::new("Zstd compression failed"))
        })?;
        let mut output = OwnedBinary::new(compressed.len()).ok_or(rustler::Error::Term(Box::new(
            "Failed to allocate binary",
        )))?;
        output.as_mut_slice().copy_from_slice(&compressed);
        Ok((atoms::ok(), output.release(env)))
    } else if algorithm == atoms::zlib() {
        // Zlib compression
        let compression = Compression::new((level as u32).min(9));
        let mut encoder = ZlibEncoder::new(data.as_slice(), compression);
        let mut compressed = Vec::new();
        encoder.read_to_end(&mut compressed).map_err(|_| {
            rustler::Error::Term(Box::new("Zlib compression failed"))
        })?;
        let mut output = OwnedBinary::new(compressed.len()).ok_or(rustler::Error::Term(Box::new(
            "Failed to allocate binary",
        )))?;
        output.as_mut_slice().copy_from_slice(&compressed);
        Ok((atoms::ok(), output.release(env)))
    } else {
        let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
            "Failed to allocate binary",
        )))?;
        Ok((atoms::unknown_algorithm(), output.release(env)))
    }
}

/// Decompress data using specified algorithm.
///
/// Algorithm atoms: lz4, zstd, zlib
/// Returns {ok, DecompressedData} or {error, Reason}
#[rustler::nif]
fn nif_decompress<'a>(
    env: Env<'a>,
    data: Binary,
    algorithm: Atom,
) -> NifResult<(Atom, Binary<'a>)> {
    if algorithm == atoms::lz4() {
        // LZ4 decompression
        match lz4_flex::decompress_size_prepended(data.as_slice()) {
            Ok(decompressed) => {
                let mut output =
                    OwnedBinary::new(decompressed.len()).ok_or(rustler::Error::Term(Box::new(
                        "Failed to allocate binary",
                    )))?;
                output.as_mut_slice().copy_from_slice(&decompressed);
                Ok((atoms::ok(), output.release(env)))
            }
            Err(_) => {
                let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
                    "Failed to allocate binary",
                )))?;
                Ok((atoms::error(), output.release(env)))
            }
        }
    } else if algorithm == atoms::zstd() {
        // Zstd decompression
        match zstd::decode_all(data.as_slice()) {
            Ok(decompressed) => {
                let mut output =
                    OwnedBinary::new(decompressed.len()).ok_or(rustler::Error::Term(Box::new(
                        "Failed to allocate binary",
                    )))?;
                output.as_mut_slice().copy_from_slice(&decompressed);
                Ok((atoms::ok(), output.release(env)))
            }
            Err(_) => {
                let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
                    "Failed to allocate binary",
                )))?;
                Ok((atoms::error(), output.release(env)))
            }
        }
    } else if algorithm == atoms::zlib() {
        // Zlib decompression
        let mut decoder = ZlibDecoder::new(data.as_slice());
        let mut decompressed = Vec::new();

        match decoder.read_to_end(&mut decompressed) {
            Ok(_) => {
                let mut output =
                    OwnedBinary::new(decompressed.len()).ok_or(rustler::Error::Term(Box::new(
                        "Failed to allocate binary",
                    )))?;
                output.as_mut_slice().copy_from_slice(&decompressed);
                Ok((atoms::ok(), output.release(env)))
            }
            Err(_) => {
                let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
                    "Failed to allocate binary",
                )))?;
                Ok((atoms::error(), output.release(env)))
            }
        }
    } else {
        let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
            "Failed to allocate binary",
        )))?;
        Ok((atoms::unknown_algorithm(), output.release(env)))
    }
}

// ============================================================================
// Benchmarking Helpers
// ============================================================================

/// Get compression ratio for data with specified algorithm.
/// Returns {OriginalSize, CompressedSize, Ratio}
#[rustler::nif]
fn nif_compression_stats(
    data: Binary,
    algorithm: Atom,
    level: i32,
) -> NifResult<(usize, usize, f64)> {
    let original_size = data.len();

    let compressed_size = if algorithm == atoms::lz4() {
        let compressed = lz4_flex::compress_prepend_size(data.as_slice());
        compressed.len()
    } else if algorithm == atoms::zstd() {
        let compressed = zstd::encode_all(data.as_slice(), level).map_err(|_| {
            rustler::Error::Term(Box::new("Compression failed"))
        })?;
        compressed.len()
    } else if algorithm == atoms::zlib() {
        let compression = Compression::new((level as u32).min(9));
        let mut encoder = ZlibEncoder::new(data.as_slice(), compression);
        let mut compressed = Vec::new();
        encoder.read_to_end(&mut compressed).map_err(|_| {
            rustler::Error::Term(Box::new("Compression failed"))
        })?;
        compressed.len()
    } else {
        return Err(rustler::Error::Term(Box::new("Unknown algorithm")));
    };

    let ratio = if compressed_size > 0 {
        original_size as f64 / compressed_size as f64
    } else {
        0.0
    };

    Ok((original_size, compressed_size, ratio))
}

rustler::init!("reckon_db_archive_nif");
