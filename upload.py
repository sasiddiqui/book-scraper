from book import Book
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import UpdateOne
import hashlib
import logging
import os
import re
from datetime import datetime, timezone

from title_key import title_key
import voyage_embed

logger = logging.getLogger("scraper")

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
        total_books: int | None = None,
        last_crawl_success=None,
    ):
        if scraper_name not in self.status:
            raise ValueError(f"Scraper {scraper_name} not found in status")

        self.status[scraper_name]["last_crawled"] = last_crawled
        self.status[scraper_name]["error"] = error
        self.status[scraper_name]["time_to_crawl"] = time_to_crawl

        if total_books is None:
            total_books = db["books"].count_documents({"source": scraper_name})

        self.status[scraper_name]["total_books"] = total_books

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


class BookManager:
    def __init__(self):
        self.books = db["books"]
        self.author_cache = db["author_cache"]
        self.embedding_cache = db["embedding_cache"]
        self.embedding_model = os.getenv("VOYAGE_MODEL", voyage_embed.DEFAULT_MODEL)

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

        # Pull existing vectors from the cache in one query.
        unique_keys = list({k for _, k, _ in entries})
        cached: dict[str, list[float]] = {}
        for row in self.embedding_cache.find(
            {"_id": {"$in": unique_keys}}, {"vector": 1}
        ):
            vec = row.get("vector")
            if vec:
                cached[row["_id"]] = vec

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
                now = datetime.now(timezone.utc)
                ops: list[UpdateOne] = []
                for key, vec in zip(miss_keys, vectors):
                    cached[key] = vec
                    ops.append(
                        UpdateOne(
                            {"_id": key},
                            {
                                "$set": {
                                    "model": self.embedding_model,
                                    "vector": vec,
                                    "created_at": now,
                                }
                            },
                            upsert=True,
                        )
                    )
                if ops:
                    self.embedding_cache.bulk_write(ops, ordered=False)

        applied = 0
        for i, key, _ in entries:
            vec = cached.get(key)
            if not vec:
                continue
            books[i]["embedding"] = vec
            books[i]["embeddingModel"] = self.embedding_model
            applied += 1
        return applied

    def upload_books(self, source: str, books: list[dict]) -> None:
        """
        Delete all books for this source then insert new ones.
        Adds normalized fields (titleNormalized, authorNormalized) for hamza-agnostic search.
        Fills in author/authorArabic from `author_cache` when the scraper couldn't find one.
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

        self.books.delete_many({"source": source})
        self.books.insert_many(books_dicts)
