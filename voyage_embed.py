"""
Thin client over Voyage AI's embeddings REST API.

Used both for ingest-time document embeddings (from `upload.py`) and for one-off
backfills. Query-side embeddings live in the frontend so prompt caching and
latency stay close to the user.

Design notes:
- We deliberately avoid adding the `voyageai` SDK as a dependency; `requests` is
  already pulled in for every scraper, and the REST contract is tiny.
- All calls go through `embed_texts`; batching logic lives there so callers just
  hand us a list of strings.
- If `VOYAGE_API_KEY` is missing we return `None`. Callers treat this as
  "embeddings disabled" rather than crashing, so local/dev scraping still works
  without a key.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

import requests

logger = logging.getLogger("scraper")


VOYAGE_ENDPOINT = "https://api.voyageai.com/v1/embeddings"
DEFAULT_MODEL = os.getenv("VOYAGE_MODEL", "voyage-4-large")

# Voyage accepts up to 1000 inputs per call, but smaller batches keep payloads
# under a few MB and make retries cheap.
BATCH_SIZE = 128

# Retry a couple of times on transient failures (5xx, 429). Each retry backs off
# exponentially; past that we let the caller decide what to do.
MAX_RETRIES = 4
INITIAL_BACKOFF_S = 2.0


class VoyageError(RuntimeError):
    pass


def _api_key() -> Optional[str]:
    return os.getenv("VOYAGE_API_KEY")


def _post_batch(texts: list[str], model: str, input_type: str) -> list[list[float]]:
    key = _api_key()
    if not key:
        raise VoyageError("VOYAGE_API_KEY not set")

    payload = {
        "input": texts,
        "model": model,
        "input_type": input_type,
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    backoff = INITIAL_BACKOFF_S
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                VOYAGE_ENDPOINT, json=payload, headers=headers, timeout=60
            )
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(f"voyage_embed: request error (attempt {attempt + 1}): {exc}")
        else:
            if resp.status_code == 200:
                data = resp.json().get("data") or []
                # Voyage returns {index, embedding} per item; preserve input order.
                ordered = sorted(data, key=lambda d: d.get("index", 0))
                return [row["embedding"] for row in ordered]

            if resp.status_code in (429, 500, 502, 503, 504):
                last_exc = VoyageError(
                    f"voyage {resp.status_code}: {resp.text[:200]}"
                )
                logger.warning(
                    f"voyage_embed: retryable {resp.status_code} (attempt {attempt + 1})"
                )
            else:
                raise VoyageError(
                    f"voyage {resp.status_code}: {resp.text[:500]}"
                )

        time.sleep(backoff)
        backoff *= 2

    raise VoyageError(f"voyage_embed: exhausted retries ({last_exc})")


def embed_texts(
    texts: list[str],
    model: str | None = None,
    input_type: str = "document",
) -> Optional[list[list[float]]]:
    """
    Embed a list of texts. Returns vectors in the same order as the input.

    Returns None if `VOYAGE_API_KEY` is not configured so callers can skip
    embedding gracefully. Raises VoyageError on API failure.
    """
    if not texts:
        return []

    if not _api_key():
        logger.warning("voyage_embed: VOYAGE_API_KEY not set; skipping embeddings")
        return None

    model = model or DEFAULT_MODEL
    out: list[list[float]] = []
    for i in range(0, len(texts), BATCH_SIZE):
        chunk = texts[i : i + BATCH_SIZE]
        vectors = _post_batch(chunk, model=model, input_type=input_type)
        if len(vectors) != len(chunk):
            raise VoyageError(
                f"voyage_embed: expected {len(chunk)} vectors, got {len(vectors)}"
            )
        out.extend(vectors)

    return out
