"""
One-off backfill: recompute the searchable normalized fields for every book
with the current normalizer (`normalize.py`, a port of the frontend's
`normalizeText`).

Recomputes `titleNormalized`, `authorNormalized`, `authorArabicNormalized` and
`publisherNormalized` from `title`, `author`, `authorArabic` and `publisher`.
Rows whose stored values already match are skipped, so it is idempotent and
safe to re-run. It walks the collection in `_id` order and logs the last `_id`
of each batch, so an interrupted run can be resumed with `--start-after`.

Embeddings are NOT touched: they are built from the raw `title`/`author`, not
from the normalized fields.

Usage:
    python backfill_normalized.py                       # everything
    python backfill_normalized.py --dry-run             # count what would change
    python backfill_normalized.py --start-after 65f0...  # resume after an _id
    python backfill_normalized.py --batch-size 500
"""

from __future__ import annotations

import argparse
import logging

from bson import ObjectId
from pymongo import UpdateOne

from normalize import FIELD_SOURCES, compute_updates
from upload import db

logger = logging.getLogger("backfill_normalized")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DEFAULT_BATCH_SIZE = 1000

PROJECTION = {
    "_id": 1,
    **{f: 1 for f in FIELD_SOURCES},
    **{f: 1 for f in FIELD_SOURCES.values()},
}


def backfill(batch_size: int, start_after: ObjectId | None, dry_run: bool) -> None:
    books = db["books"]
    last_id = start_after
    scanned = changed = 0

    while True:
        query = {"_id": {"$gt": last_id}} if last_id is not None else {}
        batch = list(books.find(query, PROJECTION).sort("_id", 1).limit(batch_size))
        if not batch:
            break

        ops = []
        for doc in batch:
            changes = compute_updates(doc)
            if changes:
                ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": changes}))

        scanned += len(batch)
        changed += len(ops)
        if ops and not dry_run:
            books.bulk_write(ops, ordered=False)

        last_id = batch[-1]["_id"]
        logger.info(
            f"backfill: scanned={scanned} {'would change' if dry_run else 'updated'}={changed} "
            f"last_id={last_id}"
        )

    logger.info(
        f"backfill: done. scanned={scanned} "
        f"{'would change' if dry_run else 'updated'}={changed}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--start-after",
        type=ObjectId,
        default=None,
        help="resume after this _id (the last_id from a previous run's log)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="report how many rows would change"
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    backfill(args.batch_size, args.start_after, args.dry_run)


if __name__ == "__main__":
    main()
