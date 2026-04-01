import aiosqlite
import os
from datetime import datetime

DB_PATH = "storage_metadata.db"


async def init_db():
    """
    Creates the database and the 'files' table if they don't exist yet.
    Adds storage_hash column for the content-addressed engine path.
    This runs once when the app starts.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL UNIQUE,
                file_size INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                storage_hash TEXT,
                uploaded_at TEXT NOT NULL
            )
        """)
        # Add storage_hash column if upgrading from an older schema
        try:
            await db.execute("ALTER TABLE files ADD COLUMN storage_hash TEXT")
        except Exception:
            pass  # Column already exists
        await db.commit()
    print("✅ Database initialized.")


async def add_file_record(filename: str, file_size: int, content_type: str, storage_hash: str | None = None):
    """
    Insert a new file record into the database.
    storage_hash is the SHA256 returned by the C++ engine — used to locate
    the file on disk at storage/<hash[0:2]>/<hash[2:4]>/<hash>.
    If the file already exists (same name), replace the old record.
    """
    uploaded_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO files (filename, file_size, content_type, storage_hash, uploaded_at)
            VALUES (?, ?, ?, ?, ?)
        """, (filename, file_size, content_type, storage_hash, uploaded_at))
        await db.commit()


async def get_all_files():
    """
    Return all file records from the database as a list of dicts.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files ORDER BY uploaded_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_file_record(filename: str):
    """
    Find a single file record by filename.
    Returns a dict if found, or None if not found.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM files WHERE filename = ?", (filename,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def delete_file_record(filename: str):
    """
    Delete a file record from the database by filename.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM files WHERE filename = ?", (filename,))
        await db.commit()
