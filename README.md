# book-scraper

Scrapers for Islamic bookstores, plus post-scrape author enrichment (Claude Haiku) and semantic-search embeddings (Voyage).

## Setup

```bash
pip install -r requirements.txt
```

Required env (see `.env`):

- `MONGODB_URI` (via `mongourl.txt` in this repo's legacy path).
- `ANTHROPIC_API_KEY` — for the optional `--enrich` author pass.
- `VOYAGE_API_KEY` — for semantic-search embeddings. If unset, crawls still work; books just get uploaded without vectors and the frontend falls back to keyword-only search.
- `VOYAGE_MODEL` — optional override (default `voyage-4-large`, 1024-dim).

## Running

```bash
python main.py                                 # all scrapers
python main.py --store maktabahalhidayah       # one store
python main.py --store ... --enrich            # + author enrichment batch
```

## Semantic search embeddings

Every book gets a Voyage embedding over `title` (or `title\nauthor` when an author is known) during upload. Vectors are cached in a MongoDB `embedding_cache` collection keyed by `sha256(model|text)` so identical titles across stores and re-crawls cost zero API calls.

One-off backfill for existing books:

```bash
python backfill_embeddings.py                  # all books missing vectors
python backfill_embeddings.py --limit 500      # smoke test
```

The matching Atlas Vector Search index definition (`vector_index`, 1024-dim cosine, filter fields `source` + `instock`) is documented in `Islamic-Book-Compare/README.md`.
