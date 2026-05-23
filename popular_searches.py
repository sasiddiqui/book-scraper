#!/usr/bin/env python3
"""
Read all-time title search counts from MongoDB (same DB/collection as usage_graph.py).
Writes a sorted text file: one line per unique search string with occurrence count.
"""

import argparse
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


def connect_to_db():
    """Same connection logic as usage_graph.py"""
    mongodb_uri = os.getenv("MONGODB_URI")

    if not mongodb_uri:
        mongourl_path = os.path.join(os.path.dirname(__file__), "mongourl.txt")
        if os.path.exists(mongourl_path):
            with open(mongourl_path, "r") as f:
                mongodb_uri = f.read().strip()
        else:
            raise ValueError("MONGODB_URI not set and mongourl.txt not found")

    client = MongoClient(mongodb_uri)
    return client["data"]


def main():
    parser = argparse.ArgumentParser(
        description="Export unique title search strings with counts (all time)."
    )
    parser.add_argument(
        "-o",
        "--output",
        default=os.path.join(os.path.dirname(__file__), "popular_searches_all_time.txt"),
        help="Output text file path (default: popular_searches_all_time.txt next to this script)",
    )
    args = parser.parse_args()

    db = connect_to_db()
    coll = db["usage"]

    pipeline = [
        {
            "$match": {
                "type": "search",
                "search": {"$exists": True, "$type": "string", "$ne": ""},
            }
        },
        {"$group": {"_id": "$search", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]

    rows = list(coll.aggregate(pipeline))
    total_events = sum(r["count"] for r in rows)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines = [
        f"# Popular title searches (all time)",
        f"# generated_utc: {generated}",
        f"# unique_queries: {len(rows)}",
        f"# total_search_events: {total_events}",
        "# format: count<TAB>query",
        "",
    ]
    for r in rows:
        title = r["_id"].replace("\n", " ").replace("\r", " ")
        lines.append(f"{r['count']}\t{title}")

    out_path = os.path.abspath(args.output)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Wrote {len(rows)} unique searches ({total_events} total events) to {out_path}")


if __name__ == "__main__":
    main()
