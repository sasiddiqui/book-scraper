"""
Local SQLite-backed embedding cache.

Replaces the on-cluster `embedding_cache` MongoDB collection. Its only job is to
avoid re-paying the Voyage API for text we've already embedded, so it never
needs to live in the production Atlas cluster (the frontend doesn't read it).

Vectors are stored as packed little-endian float32 bytes (~4 KB for a 1024-dim
vector) and handed back as float32 BSON `Binary` values, byte-for-byte identical
to what `upload.py`/`backfill_embeddings.py` write into `books.embedding`.

The key already bakes in the model (sha256("<model>|<text>")), but we also store
and filter on `model` so a model switch can't return stale vectors.
"""

from __future__ import annotations

import os
import sqlite3
import struct
from datetime import datetime, timezone

from bson.binary import Binary, BinaryVectorDtype

DEFAULT_CACHE_PATH = os.getenv(
    "EMBEDDING_CACHE_PATH",
    os.path.join(os.path.dirname(__file__), "embedding_cache.sqlite3"),
)

# SQLite caps host parameters per statement (999 on older builds); stay well under.
_IN_CHUNK = 900


def _to_floats(vec) -> list[float]:
    """Normalize a stored/incoming vector (BSON Binary or list) to a float list."""
    if isinstance(vec, Binary):
        return list(vec.as_vector().data)
    return [float(x) for x in vec]


def _pack(vec) -> bytes:
    floats = _to_floats(vec)
    return struct.pack(f"<{len(floats)}f", *floats)


def _unpack_to_binary(blob: bytes) -> Binary:
    n = len(blob) // 4
    floats = list(struct.unpack(f"<{n}f", blob))
    return Binary.from_vector(floats, BinaryVectorDtype.FLOAT32)


class EmbeddingCacheStore:
    """
    Minimal cache API mirroring how the code used the Mongo collection:
      - get_many(keys) -> {key: Binary}   (only rows for this store's model)
      - put_many({key: vector})           (vector = list[float] or Binary)
    """

    def __init__(self, model: str, path: str = DEFAULT_CACHE_PATH):
        self.model = model
        self.path = path
        self._conn = sqlite3.connect(path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS embedding_cache (
                key        TEXT PRIMARY KEY,
                model      TEXT NOT NULL,
                vector     BLOB NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def get_many(self, keys: list[str]) -> dict[str, Binary]:
        out: dict[str, Binary] = {}
        if not keys:
            return out
        cur = self._conn.cursor()
        for i in range(0, len(keys), _IN_CHUNK):
            chunk = keys[i : i + _IN_CHUNK]
            placeholders = ",".join("?" * len(chunk))
            cur.execute(
                f"SELECT key, vector FROM embedding_cache "
                f"WHERE model = ? AND key IN ({placeholders})",
                [self.model, *chunk],
            )
            for key, blob in cur.fetchall():
                out[key] = _unpack_to_binary(blob)
        return out

    def put_many(self, items: dict[str, object]) -> int:
        if not items:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        rows = [(key, self.model, _pack(vec), now) for key, vec in items.items()]
        self._conn.executemany(
            "INSERT OR REPLACE INTO embedding_cache (key, model, vector, created_at) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
        self._conn.commit()
        return len(rows)

    def count(self) -> int:
        cur = self._conn.execute("SELECT COUNT(*) FROM embedding_cache")
        return int(cur.fetchone()[0])

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "EmbeddingCacheStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
