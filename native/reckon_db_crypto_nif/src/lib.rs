//! NIF optimizations for reckon-db cryptographic operations.
//!
//! This module provides high-performance implementations of:
//! - Ed25519 signature verification
//! - SHA-256 hashing with base64 encoding (for token CIDs)
//! - Canonical serialization for deterministic signing
//!
//! These NIFs are optional - the Erlang wrapper falls back to pure Erlang
//! implementations when the NIF is not available (community edition).

use ed25519_dalek::{Signature, VerifyingKey};
use rustler::{Atom, Binary, Env, NifResult, OwnedBinary};
use sha2::{Digest, Sha256};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        invalid_signature,
        invalid_public_key,
        invalid_key_length,
    }
}

/// Verify an Ed25519 signature.
///
/// Arguments:
/// - message: The message that was signed (binary)
/// - signature: The 64-byte Ed25519 signature (binary)
/// - public_key: The 32-byte Ed25519 public key (binary)
///
/// Returns:
/// - `true` if signature is valid
/// - `false` if signature is invalid
#[rustler::nif]
fn nif_verify_ed25519(message: Binary, signature: Binary, public_key: Binary) -> bool {
    // Validate input lengths
    if signature.len() != 64 {
        return false;
    }
    if public_key.len() != 32 {
        return false;
    }

    // Parse public key
    let pk_bytes: [u8; 32] = match public_key.as_slice().try_into() {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };

    let verifying_key = match VerifyingKey::from_bytes(&pk_bytes) {
        Ok(key) => key,
        Err(_) => return false,
    };

    // Parse signature
    let sig_bytes: [u8; 64] = match signature.as_slice().try_into() {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };

    let sig = Signature::from_bytes(&sig_bytes);

    // Verify signature
    use ed25519_dalek::Verifier;
    verifying_key.verify(message.as_slice(), &sig).is_ok()
}

/// Compute SHA-256 hash of data.
///
/// Arguments:
/// - data: The data to hash (binary)
///
/// Returns:
/// - 32-byte SHA-256 hash (binary)
#[rustler::nif]
fn nif_hash_sha256<'a>(env: Env<'a>, data: Binary) -> NifResult<Binary<'a>> {
    let mut hasher = Sha256::new();
    hasher.update(data.as_slice());
    let result = hasher.finalize();

    let mut output = OwnedBinary::new(32).ok_or(rustler::Error::Term(Box::new(
        "Failed to allocate binary",
    )))?;
    output.as_mut_slice().copy_from_slice(&result);

    Ok(output.release(env))
}

/// Compute SHA-256 hash and encode as URL-safe base64 (no padding).
///
/// This is optimized for token CID generation - combines hash + encode
/// in a single NIF call to avoid intermediate allocations.
///
/// Arguments:
/// - data: The data to hash (binary)
///
/// Returns:
/// - URL-safe base64-encoded SHA-256 hash (binary string)
#[rustler::nif]
fn nif_hash_sha256_base64<'a>(env: Env<'a>, data: Binary) -> NifResult<Binary<'a>> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    // Hash the data
    let mut hasher = Sha256::new();
    hasher.update(data.as_slice());
    let hash = hasher.finalize();

    // Encode as URL-safe base64 (no padding)
    let encoded = URL_SAFE_NO_PAD.encode(hash);

    // Create output binary
    let mut output = OwnedBinary::new(encoded.len()).ok_or(rustler::Error::Term(Box::new(
        "Failed to allocate binary",
    )))?;
    output.as_mut_slice().copy_from_slice(encoded.as_bytes());

    Ok(output.release(env))
}

/// Base64 URL-safe encode without padding.
///
/// Arguments:
/// - data: The data to encode (binary)
///
/// Returns:
/// - URL-safe base64-encoded data (binary string)
#[rustler::nif]
fn nif_base64_encode_urlsafe<'a>(env: Env<'a>, data: Binary) -> NifResult<Binary<'a>> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    let encoded = URL_SAFE_NO_PAD.encode(data.as_slice());

    let mut output = OwnedBinary::new(encoded.len()).ok_or(rustler::Error::Term(Box::new(
        "Failed to allocate binary",
    )))?;
    output.as_mut_slice().copy_from_slice(encoded.as_bytes());

    Ok(output.release(env))
}

/// Base64 URL-safe decode.
///
/// Arguments:
/// - data: The base64-encoded data (binary string)
///
/// Returns:
/// - `{ok, Binary}` on success
/// - `{error, invalid_base64}` on failure
#[rustler::nif]
fn nif_base64_decode_urlsafe<'a>(env: Env<'a>, data: Binary) -> NifResult<(Atom, Binary<'a>)> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    match URL_SAFE_NO_PAD.decode(data.as_slice()) {
        Ok(decoded) => {
            let mut output =
                OwnedBinary::new(decoded.len()).ok_or(rustler::Error::Term(Box::new(
                    "Failed to allocate binary",
                )))?;
            output.as_mut_slice().copy_from_slice(&decoded);
            Ok((atoms::ok(), output.release(env)))
        }
        Err(_) => {
            // Return empty binary for error case
            let output = OwnedBinary::new(0).ok_or(rustler::Error::Term(Box::new(
                "Failed to allocate binary",
            )))?;
            Ok((atoms::error(), output.release(env)))
        }
    }
}

/// Constant-time comparison of two binaries.
///
/// This is important for security - prevents timing attacks when comparing
/// signatures, hashes, or tokens.
///
/// Arguments:
/// - a: First binary
/// - b: Second binary
///
/// Returns:
/// - `true` if equal (constant time)
/// - `false` if not equal (constant time)
#[rustler::nif]
fn nif_secure_compare(a: Binary, b: Binary) -> bool {
    if a.len() != b.len() {
        return false;
    }

    // Constant-time comparison
    let mut result: u8 = 0;
    for (x, y) in a.as_slice().iter().zip(b.as_slice().iter()) {
        result |= x ^ y;
    }
    result == 0
}

rustler::init!("reckon_db_crypto_nif");
