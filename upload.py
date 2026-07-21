from book import Book
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import UpdateOne
from bson.binary import Binary, BinaryVectorDtype
import hashlib
import logging
import os
import re

from title_key import title_key
import voyage_embed
from embedding_cache_store import EmbeddingCacheStore

logger = logging.getLogger("scraper")

# How many upsert ops to send per bulk_write round-trip. Keeps peak memory
# bounded on small hosts (e.g. the Raspberry Pi) during full crawls.
UPSERT_CHUNK = 500

# Safety valve for full-crawl deletion: if a full crawl would delete more than
# this fraction of a source's existing books, assume the crawl was partial/broken
# and skip the deletes rather than nuke the catalog.
DELETION_SAFETY_RATIO = 0.5

uri = open("mongourl.txt").read().strip()

client = MongoClient(uri, server_api=ServerApi("1"))
client.admin.command("ping")
print("Pinged your deployment. You successfully connected to MongoDB!")

db = client["data"]


def sanitize_arabic_text(text: str | None) -> str | None:
    """
    Normalizes Arabic text

    Replaces:
    - أ (alif with hamza above) → ا
    - إ (alif with hamza below) → ا
    - آ (alif with madda) → ا
    - ؤ (waw with hamza above) → و
    - ال (when at the beginning of a word) → null
    - remove harkaat
    """
    if not text:
        return text

    # Replace hamza variants with regular alif
    normalized = text.replace("أ", "ا")  # alif with hamza above
    normalized = normalized.replace("إ", "ا")  # alif with hamza below
    normalized = normalized.replace("آ", "ا")  # alif with madda
    normalized = normalized.replace("ؤ", "و")  # waw with hamza above
    normalized = normalized.replace("ئ", "ي")  # ya with hamza
    normalized = normalized.replace("ٱ", "ا")  # hamzah al wasl
    normalized = re.sub(
        r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED]", "", normalized
    )

    # remove ال when at the beggining of a word using a regex
    normalized = re.sub(r"\bال", "", normalized)

    return normalized.strip()


class StatusManager:
    """
    Manages the status of the scrapers and inserts into db for frontend to display
    """

    def __init__(self, scrapers):
        # get the status doc
        self.status = db["status"].find_one() or {"status": "idle"}

        # take each scraper and add it to the status
        for scraper in scrapers:
            # if new scraper, add it to the status
            if (name := scraper().name) not in self.status:
                self.status[name] = {
                    "last_crawled": None,
                    "error": None,
                    "time_to_crawl": None,
                    "total_books": db["books"].count_documents({"source": name}),
                    "last_crawl_success": None,
                }
            elif "last_crawl_success" not in self.status[name]:
                self.status[name]["last_crawl_success"] = None

        self._save()

    def _save(self):
        db["status"].update_one({}, {"$set": self.status}, upsert=True)

    def set_status(self, status: str):
        self.status["status"] = status
        self._save()

    def update_status(
        self,
        scraper_name,
        last_crawled,
        error: str,
        time_to_crawl: int,
        new_books: int | None = None,
        last_crawl_success=None,
    ):
        if scraper_name not in self.status:
            raise ValueError(f"Scraper {scraper_name} not found in status")

        self.status[scraper_name]["last_crawled"] = last_crawled
        self.status[scraper_name]["error"] = error
        self.status[scraper_name]["time_to_crawl"] = time_to_crawl
        self.status[scraper_name]["total_books"] = db["books"].count_documents({"source": scraper_name})

        if new_books is not None:
            self.status[scraper_name]["new_books"] = new_books

        if last_crawl_success is not None:
            self.status[scraper_name]["last_crawl_success"] = last_crawl_success

        self._save()


def _embedding_input(book: dict) -> str | None:
    """
    Build the text we embed for a book.

    Just title on its own, or "title\\nauthor" when the book has an author.
    Deliberately skips authorArabic / publisher / description - see the plan.
    Returns None if there's no usable title (shouldn't happen, but defensive).
    """
    title = (book.get("title") or "").strip()
    if not title:
        return None
    author = (book.get("author") or "").strip()
    if author:
        return f"{title}\n{author}"
    return title


def _embedding_cache_key(text: str, model: str) -> str:
    digest = hashlib.sha256(f"{model}|{text}".encode("utf-8")).hexdigest()
    return digest


def to_bson_vector(vec):
    """
    Pack an embedding into a compact BSON float32 vector (BinData).

    Storing 1024-dim vectors as a plain BSON array costs ~13 KB/doc because every
    element carries a string index key; the float32 BinData encoding is ~4 KB and
    is the format Atlas Vector Search recommends. Accepts either a raw list of
    floats or an already-encoded Binary (returned unchanged) so callers can stay
    format-agnostic during the migration window.
    """
    if vec is None:
        return None
    if isinstance(vec, Binary):
        return vec
    return Binary.from_vector([float(x) for x in vec], BinaryVectorDtype.FLOAT32)


class BookManager:
    def __init__(self):
        self.books = db["books"]
        self.author_cache = db["author_cache"]
        self.embedding_model = os.getenv("VOYAGE_MODEL", voyage_embed.DEFAULT_MODEL)
        # Embedding cache now lives in a local SQLite file, not the Atlas cluster.
        self.embedding_cache = EmbeddingCacheStore(self.embedding_model)
        # Backs the per-source upsert filter and the count_documents({"source"}) calls
        # in StatusManager. Idempotent — a no-op once the index exists.
        self.books.create_index([("source", 1), ("url", 1)], name="source_1_url_1")

    def _apply_author_cache(self, books: list[dict]) -> int:
        """
        For books missing `author`, look up their normalized title in `author_cache`
        and fill in authorEn / authorAr if we have an `enriched` row. Returns the
        number of books that got an author applied.
        """
        missing = [
            (i, title_key(b.get("title")))
            for i, b in enumerate(books)
            if not b.get("author")
        ]
        keys = {k for _, k in missing if k}
        if not keys:
            return 0

        cache_rows = {
            r["titleKey"]: r
            for r in self.author_cache.find(
                {"titleKey": {"$in": list(keys)}, "status": "enriched"}
            )
        }
        if not cache_rows:
            return 0

        applied = 0
        for idx, key in missing:
            if not key:
                continue
            row = cache_rows.get(key)
            if not row:
                continue
            book = books[idx]
            author_en = row.get("authorEn")
            author_ar = row.get("authorAr")
            if author_en:
                book["author"] = author_en
            if author_ar:
                book["authorArabic"] = author_ar
            if author_en or author_ar:
                book["authorSource"] = "llm"
                applied += 1
        return applied

    def _apply_embeddings(self, books: list[dict]) -> int:
        """
        Attach `embedding` + `embeddingModel` to each book using `embedding_cache`
        to avoid re-billing identical text across re-crawls.

        Returns the number of books that got an embedding attached (for logging).
        If VOYAGE_API_KEY is missing, logs once and returns 0 - books are uploaded
        without embeddings and the frontend falls back to keyword-only search.
        """
        # Build (index, cache_key, text) for every book that has usable text.
        entries: list[tuple[int, str, str]] = []
        for i, book in enumerate(books):
            text = _embedding_input(book)
            if not text:
                continue
            key = _embedding_cache_key(text, self.embedding_model)
            entries.append((i, key, text))

        if not entries:
            return 0

        # Pull existing vectors from the local SQLite cache. It hands them back as
        # float32 BinData, ready to drop straight into books.
        unique_keys = list({k for _, k, _ in entries})
        cached: dict[str, Binary] = self.embedding_cache.get_many(unique_keys)

        # Figure out which unique texts still need an API call. We dedupe so
        # identical texts across books only cost one embedding.
        misses: dict[str, str] = {}
        for _, key, text in entries:
            if key in cached or key in misses:
                continue
            misses[key] = text

        if misses:
            miss_keys = list(misses.keys())
            miss_texts = [misses[k] for k in miss_keys]
            try:
                vectors = voyage_embed.embed_texts(
                    miss_texts, model=self.embedding_model, input_type="document"
                )
            except voyage_embed.VoyageError as exc:
                logger.error(f"upload_books: embedding failed, skipping: {exc}")
                vectors = None

            if vectors is None:
                # Either no API key or an API failure - continue without embeddings.
                # We still use whatever was in cache.
                pass
            else:
                new_vectors = dict(zip(miss_keys, vectors))
                self.embedding_cache.put_many(new_vectors)
                for key, vec in new_vectors.items():
                    cached[key] = to_bson_vector(vec)

        applied = 0
        for i, key, _ in entries:
            vec = cached.get(key)
            if not vec:
                continue
            books[i]["embedding"] = vec
            books[i]["embeddingModel"] = self.embedding_model
            applied += 1
        return applied

    def upload_books(self, source: str, books: list[dict], full_crawl: bool = False) -> int:
        """
        Upsert freshly-scraped books into the catalog, keyed on (source, url).

        Only the books scraped this run are written, so unchanged books are never
        touched — no oplog entries and no search/vector index churn for them. This
        replaces the old delete-all + insert-all pattern that rewrote every book on
        every run (and regenerated their _ids).

        Adds normalized fields (titleNormalized, authorNormalized) for hamza-agnostic search.

        When `full_crawl` is True (the scraper saw the whole catalog, e.g. no
        incremental watermark), books that exist in the DB but were absent from this
        crawl are treated as delisted and deleted — unless that would remove more than
        DELETION_SAFETY_RATIO of the source (guards against a partial/broken crawl).
        Incremental crawls never delete, since they only see a slice of the catalog.
        """

        # applied = self._apply_author_cache(books)
        # if applied:
            # logger.info(f"upload_books: applied cached authors to {applied}/{len(books)} books for {source}")

        books_dicts = []
        for book in books:

            book["titleNormalized"] = sanitize_arabic_text(book["title"])

            book["authorNormalized"] = None
            book["authorArabicNormalized"] = None
            book["publisherNormalized"] = None

            if "author" in book and book["author"]:
                book["authorNormalized"] = sanitize_arabic_text(book["author"])

            if "authorArabic" in book and book["authorArabic"]:
                book["authorArabicNormalized"] = sanitize_arabic_text(book["authorArabic"])

            if "publisher" in book and book["publisher"]:
                book["publisherNormalized"] = sanitize_arabic_text(book["publisher"])

            books_dicts.append(book)

        embedded = self._apply_embeddings(books_dicts)
        if embedded:
            logger.info(
                f"upload_books: attached embeddings to {embedded}/{len(books_dicts)} books for {source}"
            )

        # One upsert per scraped book, keyed on (source, url). Chunked to bound
        # peak memory when a full crawl produces tens of thousands of ops.
        ops: list[UpdateOne] = []
        scraped_urls: set[str] = set()
        for book in books_dicts:
            url = book.get("url")
            if not url:
                continue
            scraped_urls.add(url)
            ops.append(
                UpdateOne(
                    {"source": source, "url": url},
                    {"$set": book},
                    upsert=True,
                )
            )

        new_count = 0
        updated_count = 0
        for i in range(0, len(ops), UPSERT_CHUNK):
            result = self.books.bulk_write(ops[i : i + UPSERT_CHUNK], ordered=False)
            new_count += result.upserted_count
            updated_count += result.matched_count

        deleted_count = 0
        if full_crawl:
            # Covered read (url only, straight from the {source,url} index — no docs
            # or embeddings fetched) to find books that disappeared from the store.
            existing_urls = {
                doc["url"]
                for doc in self.books.find(
                    {"source": source}, {"url": 1, "_id": 0}
                )
                if doc.get("url")
            }
            to_delete = existing_urls - scraped_urls
            if (
                existing_urls
                and len(to_delete) / len(existing_urls) > DELETION_SAFETY_RATIO
            ):
                logger.warning(
                    f"upload_books: {source} — refusing to delete "
                    f"{len(to_delete)}/{len(existing_urls)} books "
                    f"(> {DELETION_SAFETY_RATIO:.0%}); likely a partial crawl, "
                    f"skipping deletion"
                )
            elif to_delete:
                result = self.books.delete_many(
                    {"source": source, "url": {"$in": list(to_delete)}}
                )
                deleted_count = result.deleted_count

        logger.info(
            f"upload_books: {source} — {len(scraped_urls)} scraped "
            f"({new_count} new, {updated_count} updated, {deleted_count} deleted), "
            f"full_crawl={full_crawl}"
        )

        return new_count
