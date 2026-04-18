"""
Post-scrape author enrichment using Claude Haiku 3.5 via Anthropic's Batch API.

Pipeline:
    1. Collect books from MongoDB that are missing `author`.
    2. Deduplicate by a normalized `titleKey` (see `title_key.py`).
    3. Look up the `author_cache` collection; any hit short-circuits the LLM call.
    4. For remaining titles, submit a single Message Batch to Anthropic (one request
       per title). System prompt is marked `cache_control: ephemeral` so the Batch
       API's 50% discount stacks with the ~90% prompt-cache discount.
    5. Poll the batch, parse JSONL results, upsert into `author_cache` (including
       explicit `unknown` rows so unanswerable titles don't keep re-billing).
    6. Backfill `books` via bulk updates keyed on `titleNormalized`.

The cache shape (`titleKey` -> result) is deliberately generic so a future
cover-image enrichment pass can reuse it with an extra `coverHash` dimension.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable

from pymongo import UpdateMany, UpdateOne

from title_key import title_key
from upload import db, sanitize_arabic_text

logger = logging.getLogger("scraper")


MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 512  # leaves room for the tool_use block; Haiku 4.5 sometimes thinks before calling
BATCH_POLL_INTERVAL_S = 30
BATCH_MAX_WAIT_S = 24 * 60 * 60  # Anthropic Batch SLA
UNKNOWN_RETRY_AFTER = timedelta(days=30)

# Keep per-batch size under Anthropic's 100k-request / 256MB limits with plenty
# of headroom. If we ever exceed this we split into multiple batches.
MAX_REQUESTS_PER_BATCH = 50_000


SYSTEM_PROMPT = """You identify the author of Islamic / Arabic / English books from their title.

Rules:
- If you are not highly confident, pass null for both fields.
- authorEn: the commonly-used English transliteration (e.g. "Abu Hamid al-Ghazali").
- authorAr: the author's name in Arabic script, but ONLY if the author's name is of
  Arabic origin (e.g. Ghazali, Ibn Taymiyyah, Abu Bakr). Pass null for authors like
  "John Sterling" or "Yasir Qadhi" who do not have an Arabic-script name.
- Never invent. Never guess from publisher, series, or cover art
-You should have high confidence in famous books and authors, especially classical ones. Modern books may be more difficult.
- The title alone is the only evidence. If the title is generic (e.g. "Prayer",
  "The Sealed Nectar" alone) and you cannot identify a specific author with high
  confidence, pass null for both.

Respond by calling the submit_author tool exactly once.
"""


AUTHOR_TOOL = {
    "name": "submit_author",
    "description": "Submit the identified author of the book whose title was provided.",
    "input_schema": {
        "type": "object",
        "properties": {
            "authorEn": {
                "type": ["string", "null"],
                "description": (
                    "English transliteration of the author's name, or null if unknown."
                ),
            },
            "authorAr": {
                "type": ["string", "null"],
                "description": (
                    "Author's name in Arabic script, or null if the author's name is "
                    "not of Arabic origin or is unknown."
                ),
            },
        },
        "required": ["authorEn", "authorAr"],
        "additionalProperties": False,
    },
}


def _cache_collection():
    coll = db["author_cache"]
    coll.create_index("titleKey", unique=True)
    return coll


def _books_collection():
    return db["books"]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _collect_missing_titles() -> dict[str, list[str]]:
    """
    Scan `books` for rows missing `author` and group original titles by `titleKey`.
    Returns {titleKey: [original_title, ...]} - we keep a sample title per key
    so the LLM sees real input rather than the lossy normalized form.
    """
    query = {
        "$and": [
            {"title": {"$exists": True, "$ne": None, "$ne": ""}},
            {"$or": [
                {"author": None},
                {"author": ""},
                {"author": {"$exists": False}},
            ]},
        ]
    }
    buckets: dict[str, list[str]] = {}
    for doc in _books_collection().find(query, {"title": 1}):
        title = doc.get("title")
        key = title_key(title)
        if not key:
            continue
        buckets.setdefault(key, []).append(title)
    return buckets


def _load_cache(keys: Iterable[str]) -> dict[str, dict]:
    keys = list(keys)
    if not keys:
        return {}
    cursor = _cache_collection().find({"titleKey": {"$in": keys}})
    return {row["titleKey"]: row for row in cursor}


def _should_reuse_cache(row: dict) -> bool:
    """Reuse any enriched row; retry `unknown` rows only after UNKNOWN_RETRY_AFTER."""
    if row.get("status") == "enriched":
        return True
    updated = row.get("updatedAt")
    if not isinstance(updated, datetime):
        return False
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    return (_utcnow() - updated) < UNKNOWN_RETRY_AFTER


def _custom_id_for(title_key_value: str) -> str:
    """
    Anthropic requires custom_id to match ^[a-zA-Z0-9_-]{1,64}$ but our titleKey
    can contain spaces and non-ASCII (Arabic) characters. A short sha256 hex
    digest is safe, collision-resistant enough for this use, and well under 64.
    """
    return hashlib.sha256(title_key_value.encode("utf-8")).hexdigest()[:32]


def _build_batch_requests(to_query: dict[str, str]) -> tuple[list[dict], dict[str, str]]:
    """
    Build one request per titleKey, returning (requests, id_to_key) so results
    can be mapped back to the original titleKey.
    """
    requests = []
    id_to_key: dict[str, str] = {}
    for key, sample_title in to_query.items():
        cid = _custom_id_for(key)
        id_to_key[cid] = key
        requests.append({
            "custom_id": cid,
            "params": {
                "model": MODEL,
                "max_tokens": MAX_TOKENS,
                "system": [
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "tools": [AUTHOR_TOOL],
                "tool_choice": {"type": "tool", "name": "submit_author"},
                "messages": [
                    {
                        "role": "user",
                        "content": sample_title,
                    }
                ],
            },
        })
    return requests, id_to_key


_JSON_OBJ_RE = re.compile(r"\{[^{}]*\}")


def _coerce_author_fields(raw: dict) -> tuple[str | None, str | None]:
    en = raw.get("authorEn")
    ar = raw.get("authorAr")
    if isinstance(en, str):
        en = en.strip() or None
    else:
        en = None
    if isinstance(ar, str):
        ar = ar.strip() or None
    else:
        ar = None
    return en, ar


def _extract_author_from_message(message) -> tuple[str | None, str | None]:
    """
    Pull authorEn/authorAr out of a Claude message.

    Primary path: a `tool_use` block named `submit_author` with structured args.
    Fallback: scan text blocks for the LAST JSON object matching the schema -
    Haiku 4.5 sometimes "thinks" before emitting JSON and occasionally revises
    its answer, so taking the last match is the safest heuristic.
    """
    content = getattr(message, "content", []) or []

    for block in content:
        if getattr(block, "type", None) != "tool_use":
            continue
        if getattr(block, "name", None) != "submit_author":
            continue
        data = getattr(block, "input", None) or {}
        if isinstance(data, dict):
            return _coerce_author_fields(data)

    for block in content:
        if getattr(block, "type", None) != "text":
            continue
        text = getattr(block, "text", "") or ""
        candidates = _JSON_OBJ_RE.findall(text)
        for raw in reversed(candidates):
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict) and ("authorEn" in data or "authorAr" in data):
                logger.info(
                    "enrich_authors: fell back to text-parse (model did not emit tool_use)"
                )
                return _coerce_author_fields(data)

    logger.warning(
        f"enrich_authors: could not extract author from message content={content!r}"
    )
    return None, None


def _upsert_cache_rows(rows: list[dict]) -> None:
    if not rows:
        return
    ops = []
    for row in rows:
        ops.append(UpdateOne(
            {"titleKey": row["titleKey"]},
            {"$set": row},
            upsert=True,
        ))
    _cache_collection().bulk_write(ops, ordered=False)


def _backfill_books(cache_rows: list[dict]) -> int:
    """Push enriched author data back onto the `books` collection."""
    enriched = [r for r in cache_rows if r.get("status") == "enriched"]
    if not enriched:
        return 0

    ops = []
    for row in enriched:
        author_en = row.get("authorEn")
        author_ar = row.get("authorAr")
        if not author_en and not author_ar:
            continue

        set_fields: dict = {}
        if author_en:
            set_fields["author"] = author_en
            set_fields["authorNormalized"] = sanitize_arabic_text(author_en)
        if author_ar:
            set_fields["authorArabic"] = author_ar
            set_fields["authorArabicNormalized"] = sanitize_arabic_text(author_ar)
        set_fields["authorSource"] = "llm"

        # We filter on the ORIGINAL-title bucket: any book whose title normalizes
        # to this key and still has no author gets the update. We can't query on
        # `titleKey` server-side (not stored on books), so we match on the titles
        # we actually saw plus a safety `author` null guard.
        titles = row.get("_sourceTitles", [])
        if not titles:
            continue

        ops.append(UpdateMany(
            {
                "title": {"$in": titles},
                "$or": [
                    {"author": None},
                    {"author": ""},
                    {"author": {"$exists": False}},
                ],
            },
            {"$set": set_fields},
        ))

    if not ops:
        return 0

    result = _books_collection().bulk_write(ops, ordered=False)
    return result.modified_count


def _batches_resource(client):
    """
    Return the `batches` resource, transparently handling the stable vs beta path.

    Older SDK versions (<= 0.42.x) only expose it under `client.beta.messages.batches`;
    newer versions graduate it to `client.messages.batches`. We try stable first.
    """
    messages = getattr(client, "messages", None)
    if messages is not None and hasattr(messages, "batches"):
        return messages.batches
    beta_messages = getattr(getattr(client, "beta", None), "messages", None)
    if beta_messages is not None and hasattr(beta_messages, "batches"):
        return beta_messages.batches
    raise RuntimeError(
        "Installed anthropic SDK has no `messages.batches` resource. "
        "Upgrade `anthropic` in requirements.txt."
    )


def _submit_and_wait(client, requests: list[dict]) -> list:
    """Submit a single batch and block until it ends, returning the results list."""
    batches = _batches_resource(client)
    logger.info(f"enrich_authors: submitting batch of {len(requests)} requests")
    batch = batches.create(requests=requests)
    batch_id = batch.id

    deadline = time.time() + BATCH_MAX_WAIT_S
    while True:
        status = batches.retrieve(batch_id)
        if status.processing_status == "ended":
            logger.info(f"enrich_authors: batch {batch_id} ended: {status.request_counts}")
            break
        if time.time() > deadline:
            raise TimeoutError(f"Batch {batch_id} did not finish within SLA")
        logger.info(
            f"enrich_authors: batch {batch_id} status={status.processing_status} "
            f"counts={status.request_counts}"
        )
        time.sleep(BATCH_POLL_INTERVAL_S)

    return list(batches.results(batch_id))


def _process_results(
    results,
    to_query: dict[str, list[str]],
    id_to_key: dict[str, str],
) -> list[dict]:
    """Convert batch results + original title buckets into cache row dicts."""
    now = _utcnow()
    rows: list[dict] = []
    seen: set[str] = set()

    for item in results:
        key = id_to_key.get(item.custom_id)
        if key is None:
            logger.warning(f"enrich_authors: unknown custom_id in results: {item.custom_id!r}")
            continue
        seen.add(key)
        source_titles = to_query.get(key, [])
        base = {
            "titleKey": key,
            "model": MODEL,
            "updatedAt": now,
            "_sourceTitles": source_titles,
        }

        result_type = getattr(item.result, "type", None)
        if result_type != "succeeded":
            logger.warning(
                f"enrich_authors: {key} result_type={result_type} "
                f"error={getattr(item.result, 'error', None)}"
            )
            continue

        message = item.result.message
        en, ar = _extract_author_from_message(message)
        status = "enriched" if (en or ar) else "unknown"
        rows.append({
            **base,
            "authorEn": en,
            "authorAr": ar,
            "status": status,
            "createdAt": now,
        })

    missing = set(to_query) - seen
    if missing:
        logger.warning(f"enrich_authors: {len(missing)} custom_ids missing from results")

    return rows


def _chunked(items: dict[str, str], size: int):
    it = iter(items.items())
    while True:
        chunk = {}
        for _ in range(size):
            try:
                k, v = next(it)
            except StopIteration:
                break
            chunk[k] = v
        if not chunk:
            return
        yield chunk


def run(dry_run: bool = False) -> dict:
    """
    Main entrypoint. Returns a summary dict suitable for logging.
    Safe to call even if ANTHROPIC_API_KEY is unset (logs a warning and returns).
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("enrich_authors: ANTHROPIC_API_KEY not set; skipping enrichment")
        return {"skipped": True}

    try:
        from anthropic import Anthropic
    except ImportError:
        logger.error("enrich_authors: `anthropic` package not installed; skipping")
        return {"skipped": True}

    buckets = _collect_missing_titles()
    logger.info(f"enrich_authors: {len(buckets)} unique missing-author titleKeys")
    if not buckets:
        return {"missing_titles": 0, "llm_calls": 0, "enriched": 0, "backfilled": 0}

    cached = _load_cache(buckets.keys())
    reusable = {k: v for k, v in cached.items() if _should_reuse_cache(v)}

    # Seed cache rows from reusable entries so we can backfill them in the same pass.
    preloaded_rows = []
    for key, row in reusable.items():
        preloaded_rows.append({
            **row,
            "_sourceTitles": buckets[key],
        })

    to_query = {k: v[0] for k, v in buckets.items() if k not in reusable}
    logger.info(
        f"enrich_authors: {len(reusable)} cache hits, {len(to_query)} to query LLM"
    )

    new_rows: list[dict] = []
    if to_query and not dry_run:
        client = Anthropic(api_key=api_key)

        for chunk in _chunked(to_query, MAX_REQUESTS_PER_BATCH):
            requests, id_to_key = _build_batch_requests(chunk)
            results = _submit_and_wait(client, requests)
            chunk_rows = _process_results(
                results,
                {k: buckets[k] for k in chunk},
                id_to_key,
            )
            _upsert_cache_rows([
                {k: v for k, v in r.items() if not k.startswith("_")}
                for r in chunk_rows
            ])
            new_rows.extend(chunk_rows)

    all_rows = preloaded_rows + new_rows
    backfilled = 0 if dry_run else _backfill_books(all_rows)

    summary = {
        "missing_titles": len(buckets),
        "cache_hits": len(reusable),
        "llm_calls": len(to_query),
        "enriched": sum(1 for r in all_rows if r.get("status") == "enriched"),
        "unknown": sum(1 for r in all_rows if r.get("status") == "unknown"),
        "backfilled": backfilled,
    }
    logger.info(f"enrich_authors: done {summary}")
    return summary


async def run_async(dry_run: bool = False) -> dict:
    """Async shim - `run` is CPU/IO bound but we keep it off the event loop."""
    return await asyncio.to_thread(run, dry_run)


if __name__ == "__main__":
    import argparse
    import logging.config
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Enrich missing book authors via Claude")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; no LLM calls, no writes")
    args = parser.parse_args()

    run(dry_run=args.dry_run)
