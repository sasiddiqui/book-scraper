from book import Book
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import logging
import re

from title_key import title_key

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


class BookManager:
    def __init__(self):
        self.books = db["books"]
        self.author_cache = db["author_cache"]

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

    def upload_books(self, source: str, books: list[dict]) -> None:
        """
        Delete all books for this source then insert new ones.
        Adds normalized fields (titleNormalized, authorNormalized) for hamza-agnostic search.
        Fills in author/authorArabic from `author_cache` when the scraper couldn't find one.
        """

        applied = self._apply_author_cache(books)
        if applied:
            logger.info(f"upload_books: applied cached authors to {applied}/{len(books)} books for {source}")

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

        self.books.delete_many({"source": source})
        self.books.insert_many(books_dicts)
