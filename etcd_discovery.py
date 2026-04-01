"""
Async background processing pipeline.

After every successful upload the pipeline runs three tasks concurrently:

  1. File-type detection  — magic-byte sniffing + UTF-8 probe
  2. Content analysis     — byte entropy, line/word count for text
  3. Pattern scan         — lightweight bad-pattern check (EICAR-style)

Results are stored in an in-memory dict keyed by "bucket/key" and served
via  GET /storage/{bucket}/{key}/analysis.
"""
import asyncio
import math
from collections import Counter
from typing import Any, Dict, Optional

_store: Dict[str, Dict[str, Any]] = {}
_lock = asyncio.Lock()

MAGIC = {
    b"\x89PNG":     "image/png",
    b"\xff\xd8\xff": "image/jpeg",
    b"GIF8":        "image/gif",
    b"RIFF":        "audio/wav",
    b"PK\x03\x04":  "application/zip",
    b"%PDF":        "application/pdf",
    b"\x7fELF":     "application/x-elf",
    b"MZ":          "application/x-executable",
}

BAD_PATTERNS = [
    b"EICAR-STANDARD-ANTIVIRUS-TEST-FILE",
    b"X5O!P%@AP[4",
]


def _detect_type(data: bytes) -> str:
    for sig, mime in MAGIC.items():
        if data[: len(sig)] == sig:
            return mime
    try:
        data.decode("utf-8")
        return "text/plain"
    except Exception:
        return "application/octet-stream"


def _entropy(data: bytes) -> float:
    if not data:
        return 0.0
    counts = Counter(data)
    n = len(data)
    return round(-sum((c / n) * math.log2(c / n) for c in counts.values()), 4)


def _scan(data: bytes) -> bool:
    return any(pat in data for pat in BAD_PATTERNS)


async def enqueue(bucket: str, key: str, enc_data: bytes) -> None:
    """Schedule background analysis without blocking the upload response."""
    asyncio.get_event_loop().create_task(_run(bucket, key, enc_data))


async def _run(bucket: str, key: str, enc_data: bytes) -> None:
    raw = bytes(b ^ ord("k") for b in enc_data)
    detected_type = _detect_type(raw)
    entropy_val   = _entropy(raw)
    flagged       = _scan(raw)

    result: Dict[str, Any] = {
        "bucket":        bucket,
        "key":           key,
        "detected_type": detected_type,
        "entropy":       entropy_val,
        "size_bytes":    len(raw),
        "flagged":       flagged,
        "flag_reason":   "suspicious pattern detected" if flagged else None,
    }

    if detected_type == "text/plain":
        try:
            text = raw.decode("utf-8", errors="replace")
            result["line_count"] = text.count("\n")
            result["word_count"] = len(text.split())
            result["char_count"] = len(text)
        except Exception:
            pass

    status = "FLAGGED" if flagged else "OK"
    print(f"[worker] {bucket}/{key}: type={detected_type}, "
          f"entropy={entropy_val}, scan={status}")

    async with _lock:
        _store[f"{bucket}/{key}"] = result


async def get_result(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    async with _lock:
        return _store.get(f"{bucket}/{key}")


async def remove_result(bucket: str, key: str) -> None:
    async with _lock:
        _store.pop(f"{bucket}/{key}", None)
