"""
One-time migration: convert embedding vectors stored as plain BSON arrays into
compact float32 BinData vectors.

Why: a 1024-dim vector stored as a BSON array is ~13 KB/doc (every element
carries a string index key). The same vector as float32 BinData is ~4 KB and is
the encoding Atlas Vector Search recommends. Converting `books.embedding` and
`embedding_cache.vector` cuts those two collections from ~2.5 GB to ~0.8 GB.

The migration is:
  - Idempotent / resumable: it only touches docs whose vector is still an array
    ($type: "array"), so re-running (or resuming after an interruption) is safe.
  - Non-destructive to search: the `vector_index` definition is unchanged; Atlas
    re-indexes the field in place as documents are updated.

Usage:
    python migrate_embeddings_to_binvector.py            # migrate both collections
    python migrate_embeddings_to_binvector.py --dry-run  # just report counts
    python migrate_embeddings_to_binvector.py --batch-size 1000

After it finishes, the freed space becomes WiredTiger "free" space inside the
files. To return it to the OS (and shrink your Atlas storage footprint) run a
compact per collection, e.g. in mongosh:
    db.runCommand({ compact: "books" })
    db.runCommand({ compact: "embedding_cache" })
"""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv
from pymongo import UpdateOne

from upload import db, to_bson_vector

logger = logging.getLogger("migrate_embeddings")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _migrate_collection(collection_name: str, field: str, batch_size: int, dry_run: bool) -> None:
    col = db[collection_name]

    # $type "array" matches only the not-yet-migrated docs; float32 BinData is
    # "binData", so converted docs drop out of this filter automatically.
    array_filter = {field: {"$type": "array"}}
    remaining = col.count_documents(array_filter)
    total_docs = col.estimated_document_count()
    logger.info(
        f"{collection_name}.{field}: {remaining} array vectors to convert "
        f"(collection has ~{total_docs} docs)"
    )

    if dry_run or not remaining:
        return

    migrated = 0
    while True:
        # Re-query each pass: converted docs no longer match the filter, so a
        # plain limited find walks steadily through the backlog without a
        # long-lived cursor (avoids cursor timeouts on big collections).
        batch = list(
            col.find(array_filter, {field: 1}).limit(batch_size)
        )
        if not batch:
            break

        ops = [
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {field: to_bson_vector(doc[field])}},
            )
            for doc in batch
            if doc.get(field)
        ]
        if not ops:
            break

        col.bulk_write(ops, ordered=False)
        migrated += len(ops)
        logger.info(f"{collection_name}.{field}: converted {migrated}/{remaining}")

    logger.info(f"{collection_name}.{field}: done, {migrated} vectors converted")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert array embeddings to float32 BinData vectors."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many array vectors exist without modifying anything.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Documents to convert per bulk_write round-trip.",
    )
    args = parser.parse_args()

    load_dotenv()

    _migrate_collection("books", "embedding", args.batch_size, args.dry_run)
    _migrate_collection("embedding_cache", "vector", args.batch_size, args.dry_run)


if __name__ == "__main__":
    main()
