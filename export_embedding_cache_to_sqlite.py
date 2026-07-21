"""
One-time export: copy the on-cluster `embedding_cache` MongoDB collection into
the local SQLite cache used by `embedding_cache_store.py`.

Run this ONCE on the machine that runs the scraper (e.g. the Raspberry Pi) before
the first scrape after switching to the SQLite cache. Otherwise the local cache
starts empty and the next scrape re-pays Voyage to re-embed everything (correct,
just wasteful).

It only READS from MongoDB — it never writes to or drops anything in the cluster.
Safe to re-run; rows are upserted by key.

Usage:
    python export_embedding_cache_to_sqlite.py
    python export_embedding_cache_to_sqlite.py --batch-size 2000
"""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv

from embedding_cache_store import DEFAULT_CACHE_PATH, EmbeddingCacheStore
from upload import db

logger = logging.getLogger("export_embedding_cache")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def export(batch_size: int) -> None:
    src = db["embedding_cache"]
    total = src.estimated_document_count()
    logger.info(f"exporting ~{total} cache rows to {DEFAULT_CACHE_PATH}")

    # Group by model so each SQLite write carries the correct model tag. The
    # collection has historically used a single model, but this keeps it correct
    # if that ever changes.
    stores: dict[str, EmbeddingCacheStore] = {}
    buffers: dict[str, dict[str, object]] = {}
    exported = 0

    def flush(model: str) -> None:
        nonlocal exported
        buf = buffers.get(model)
        if not buf:
            return
        if model not in stores:
            stores[model] = EmbeddingCacheStore(model)
        stores[model].put_many(buf)
        exported += len(buf)
        buffers[model] = {}
        logger.info(f"exported {exported}/{total}")

    try:
        for row in src.find({}, {"_id": 1, "model": 1, "vector": 1}):
            vec = row.get("vector")
            if not vec:
                continue
            model = row.get("model") or "unknown"
            buffers.setdefault(model, {})[row["_id"]] = vec
            if len(buffers[model]) >= batch_size:
                flush(model)

        for model in list(buffers.keys()):
            flush(model)

        for model, store in stores.items():
            logger.info(f"model={model}: local cache now holds {store.count()} rows")
        logger.info(f"export done. total exported = {exported}")
    finally:
        for store in stores.values():
            store.close()


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Export Mongo embedding_cache into local SQLite."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Rows to accumulate per SQLite write.",
    )
    args = parser.parse_args()

    export(batch_size=args.batch_size)
