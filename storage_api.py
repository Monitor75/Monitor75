"""
Pre-signed URL generation and validation.

A pre-signed URL is a time-limited, HMAC-signed token that authorises a
specific action (PUT or GET) on a specific bucket/key without requiring
the caller to hold a session token.  The flow mirrors S3 presigned URLs:

  1. Caller hits  GET /storage/{bucket}/{key}/presign?ttl=3600
  2. Server returns { url, expires, method }
  3. Caller sends  PUT <url>  (or GET <url>) — no other auth needed
  4. Server validates the embedded token before processing

The signing secret is loaded from the PRESIGN_SECRET env-var, or a
random one is generated at process startup (survives restarts via env).
"""
import hmac
import hashlib
import time
import os

SIGNING_SECRET: bytes = os.environ.get(
    "PRESIGN_SECRET", os.urandom(32).hex()
).encode()


def _signature(bucket: str, key: str, method: str, expires: int) -> str:
    msg = f"{method}:{bucket}:{key}:{expires}".encode()
    return hmac.new(SIGNING_SECRET, msg, hashlib.sha256).hexdigest()


def generate(
    base_url: str,
    bucket: str,
    key: str,
    method: str = "PUT",
    ttl: int = 3600,
) -> dict:
    """Return a presigned URL descriptor."""
    expires = int(time.time()) + ttl
    token = _signature(bucket, key, method, expires)
    url = (
        f"{base_url}/storage/{bucket}/{key}"
        f"?token={token}&expires={expires}&method={method}"
    )
    return {
        "url": url,
        "method": method,
        "expires": expires,
        "ttl_seconds": ttl,
    }


def validate(
    bucket: str,
    key: str,
    method: str,
    token: str,
    expires: int,
) -> bool:
    """Return True iff the token is valid and not expired."""
    if time.time() > expires:
        return False
    expected = _signature(bucket, key, method, expires)
    return hmac.compare_digest(expected, token)
