"""
One-off backfill: attach Voyage embeddings to every book that doesn't already
have one (or has one from a different model).

Reuses the same cache + input format as `upload.py` so subsequent scheduled
crawls are essentially free. Safe to re-run.

Usage:
    python backfill_embeddings.py                  # backfill everything missing
    python backfill_embeddings.py --limit 500      # small test run
    python backfill_embeddings.py --batch-size 256 # override DB write batch
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import UpdateOne

import voyage_embed
from upload import _embedding_cache_key, _embedding_input, db


logger = logging.getLogger("backfill_embeddings")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _needs_embedding_query(model: str) -> dict:
    """Books without an embedding, or whose embedding came from a different model."""
    return {
        "$or": [
            {"embedding": {"$exists": False}},
            {"embedding": None},
            {"embeddingModel": {"$ne": model}},
        ]
    }


def backfill(limit: int | None, batch_size: int) -> None:
    model = voyage_embed.DEFAULT_MODEL
    books_col = db["books"]
    cache_col = db["embedding_cache"]

    query = _needs_embedding_query(model)
    total = books_col.count_documents(query)
    logger.info(
        f"backfill: {total} books need embeddings (model={model}, limit={limit})"
    )
    if not total:
        return

    projection = {
        "_id": 1,
        "title": 1,
        "author": 1,
    }

    cursor = books_col.find(query, projection)
    if limit:
        cursor = cursor.limit(limit)

    # Accumulate until we hit batch_size, then embed + write in one round-trip.
    pending: list[tuple[object, str, str]] = []  # (_id, cache_key, text)
    processed = 0

    def flush() -> None:
        nonlocal pending, processed
        if not pending:
            return

        unique_keys = list({k for _, k, _ in pending})
        cached: dict[str, list[float]] = {}
        for row in cache_col.find(
            {"_id": {"$in": unique_keys}, "model": model}, {"vector": 1}
        ):
            vec = row.get("vector")
            if vec:
                cached[row["_id"]] = vec

        # Dedupe miss texts so identical title+author across stores = 1 API call.
        misses: dict[str, str] = {}
        for _, key, text in pending:
            if key in cached or key in misses:
                continue
            misses[key] = text

        if misses:
            miss_keys = list(misses.keys())
            miss_texts = [misses[k] for k in miss_keys]
            vectors = voyage_embed.embed_texts(
                miss_texts, model=model, input_type="document"
            )
            if vectors is None:
                raise RuntimeError(
                    "VOYAGE_API_KEY is not set; backfill cannot proceed"
                )
            now = datetime.now(timezone.utc)
            cache_ops = [
                UpdateOne(
                    {"_id": key},
                    {
                        "$set": {
                            "model": model,
                            "vector": vec,
                            "created_at": now,
                        }
                    },
                    upsert=True,
                )
                for key, vec in zip(miss_keys, vectors)
            ]
            cached.update(dict(zip(miss_keys, vectors)))
            cache_col.bulk_write(cache_ops, ordered=False)

        book_ops: list[UpdateOne] = []
        for _id, key, _ in pending:
            vec = cached.get(key)
            if not vec:
                continue
            book_ops.append(
                UpdateOne(
                    {"_id": _id},
                    {"$set": {"embedding": vec, "embeddingModel": model}},
                )
            )
        if book_ops:
            books_col.bulk_write(book_ops, ordered=False)

        processed += len(pending)
        logger.info(
            f"backfill: processed {processed}/{total if not limit else min(limit, total)}"
        )
        pending = []

    for doc in cursor:
        text = _embedding_input(doc)
        if not text:
            continue
        key = _embedding_cache_key(text, model)
        pending.append((doc["_id"], key, text))
        if len(pending) >= batch_size:
            flush()

    flush()
    logger.info(f"backfill: done. total processed = {processed}")


if __name__ == "__main__":
    load_dotenv()

    parser = argparse.ArgumentParser(description="Backfill book embeddings.")
    parser.add_argument(
        "--limit", type=int, default=None, help="Process at most N books."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=256,
        help="How many books to accumulate before flushing to Voyage + Mongo.",
    )
    args = parser.parse_args()

    backfill(limit=args.limit, batch_size=args.batch_size)
