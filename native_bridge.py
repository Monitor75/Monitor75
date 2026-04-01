"""
native_bridge.py — Python interface to our C/Assembly library
=============================================================

This file teaches you how ctypes works:
  - How to load a compiled shared library (.so file)
  - How to declare function signatures (argument types, return types)
  - How to mirror a C struct in Python
  - How to call native code from Python

WHY CTYPES?
  ctypes is Python's built-in way to call C functions without
  writing any extra glue code. It's in the standard library — no pip needed.
  For more performance-critical work, people use cffi or pybind11,
  but ctypes is the best learning starting point.
"""

import ctypes
import os
import platform

# -------------------------------------------------------
# Step 1: Find and load the shared library
# -------------------------------------------------------

# Get the directory this file lives in
_dir = os.path.dirname(os.path.abspath(__file__))

# The library file name differs by OS
if platform.system() == "Windows":
    _lib_name = "fileops.dll"
else:
    _lib_name = "libfileops.so" # Linux / macOS

_lib_path = os.path.join(_dir, "native", _lib_name)

# Try to load the library
try:
    _lib = ctypes.CDLL(_lib_path)
    NATIVE_AVAILABLE = True
except OSError:
    _lib = None
    NATIVE_AVAILABLE = False
    print(f"⚠️ Native library not found at {_lib_path}")
    print(" Run: cd native && make")
    print(" Falling back to pure Python implementations.")


# -------------------------------------------------------
# Step 2: Mirror the C struct in Python
#
# In C we have:
# typedef struct {
# uint32_t adler32;
# uint64_t file_size;
# uint64_t null_bytes;
# uint64_t newline_bytes;
# uint64_t printable_bytes;
# uint8_t min_byte;
# uint8_t max_byte;
# uint8_t is_text;
# uint8_t _pad[1];
# } FileStats;
#
# ctypes.Structure mirrors this exactly.
# The order and types MUST match the C definition.
# -------------------------------------------------------

class FileStats(ctypes.Structure):
    _fields_ = [
        ("adler32", ctypes.c_uint32),
        ("file_size", ctypes.c_uint64),
        ("null_bytes", ctypes.c_uint64),
        ("newline_bytes", ctypes.c_uint64),
        ("printable_bytes", ctypes.c_uint64),
        ("min_byte", ctypes.c_uint8),
        ("max_byte", ctypes.c_uint8),
        ("is_text", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 1),
    ]


# -------------------------------------------------------
# Step 3: Declare function signatures
#
# ctypes needs to know what types go in and come out.
# Without this, Python might pass the wrong types and crash.
# -------------------------------------------------------

if NATIVE_AVAILABLE:
    # int inspect_file(const char *path, FileStats *out)
    _lib.inspect_file.argtypes = [ctypes.c_char_p, ctypes.POINTER(FileStats)]
    _lib.inspect_file.restype = ctypes.c_int

    # uint32_t get_file_checksum(const char *path)
    _lib.get_file_checksum.argtypes = [ctypes.c_char_p]
    _lib.get_file_checksum.restype = ctypes.c_uint32


# -------------------------------------------------------
# Step 4: Python-friendly wrapper functions
# -------------------------------------------------------

def inspect_file(path: str) -> dict:
    """
    Inspect a file using our C/Assembly native library.
    Returns a dict with checksum, size, byte statistics.

    Falls back to pure Python if the native library isn't built yet.
    """
    if not NATIVE_AVAILABLE:
        return _python_fallback_inspect(path)

    stats = FileStats()
    result = _lib.inspect_file(path.encode("utf-8"), ctypes.byref(stats))
    # ^ strings must be bytes in ctypes
    # ^ byref = pass a pointer to stats

    if result == -1:
        raise FileNotFoundError(f"Cannot open file: {path}")
    if result == -2:
        raise MemoryError("C layer failed to allocate memory")

    return {
        "adler32": f"0x{stats.adler32:08X}", # hex string like "0x1A2B3C4D"
        "adler32_int": stats.adler32,
        "file_size": stats.file_size,
        "null_bytes": stats.null_bytes,
        "newline_bytes": stats.newline_bytes,
        "printable_bytes": stats.printable_bytes,
        "min_byte": stats.min_byte,
        "max_byte": stats.max_byte,
        "is_text": bool(stats.is_text),
        "engine": "assembly+c",
    }


def get_checksum(path: str) -> str:
    """Return just the Adler-32 checksum as a hex string."""
    if not NATIVE_AVAILABLE:
        return _python_fallback_adler32(path)
    checksum = _lib.get_file_checksum(path.encode("utf-8"))
    return f"0x{checksum:08X}"


# -------------------------------------------------------
# Pure Python fallbacks (used when .so isn't built yet)
# These help you understand what the assembly is doing!
# -------------------------------------------------------

def _python_fallback_adler32(data_or_path) -> str:
    """Pure Python implementation of Adler-32 — mirrors the assembly logic."""
    MOD = 65521
    A, B = 1, 0

    if isinstance(data_or_path, str):
        with open(data_or_path, "rb") as f:
            data = f.read()
    else:
        data = data_or_path

    for byte in data:
        A = (A + byte) % MOD
        B = (B + A) % MOD

    return f"0x{((B << 16) | A):08X}"


def _python_fallback_inspect(path: str) -> dict:
    """Pure Python file inspector — same logic as the C/ASM version."""
    with open(path, "rb") as f:
        data = f.read()

    size = len(data)
    if size == 0:
        return {"adler32": "0x00000001", "file_size": 0, "engine": "python-fallback"}

    MOD = 65521
    A, B = 1, 0
    null_bytes = 0
    newline_bytes = 0
    printable_bytes = 0
    min_b = 255
    max_b = 0

    for byte in data:
        A = (A + byte) % MOD
        B = (B + A) % MOD
        if byte == 0x00: null_bytes += 1
        if byte == 0x0A: newline_bytes += 1
        if 0x20 <= byte <= 0x7E: printable_bytes += 1
        if byte < min_b: min_b = byte
        if byte > max_b: max_b = byte

    return {
        "adler32": f"0x{((B << 16) | A):08X}",
        "adler32_int": (B << 16) | A,
        "file_size": size,
        "null_bytes": null_bytes,
        "newline_bytes": newline_bytes,
        "printable_bytes": printable_bytes,
        "min_byte": min_b,
        "max_byte": max_b,
        "is_text": (printable_bytes * 100 // size) >= 90,
        "engine": "python-fallback",
    }
