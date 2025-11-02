from book import Book
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = open("mongourl.txt").read().strip()

client = MongoClient(uri, server_api=ServerApi('1'))
client.admin.command('ping')
print("Pinged your deployment. You successfully connected to MongoDB!")

db = client["data"]


def sanitize_arabic_text(text: str | None) -> str | None:
    """
    Normalizes Arabic text by replacing hamza variants with regular alif (ا).
    This helps with search matching when users don't type hamzas correctly.
    
    Replaces:
    - أ (alif with hamza above) → ا
    - إ (alif with hamza below) → ا
    - آ (alif with madda) → ا
    """
    if not text:
        return text
    
    # Replace hamza variants with regular alif
    normalized = text.replace('أ', 'ا')  # alif with hamza above
    normalized = normalized.replace('إ', 'ا')  # alif with hamza below
    normalized = normalized.replace('آ', 'ا')  # alif with madda

    if normalized != text:
        print(f"Normalized {text} to {normalized}")
    
    
    return normalized



class StatusManager:
    """
    Manages the status of the scrapers and inserts into db for frontend to display
    """
    def __init__(self, scrapers):
        # get the status doc
        self.status = db["status"].find_one() or {
            "status": "idle"
        }

        # take each scraper and add it to the status
        for scraper in scrapers:
            # if new scraper, add it to the status
            if (name := scraper().name) not in self.status:
                self.status[name] = {
                    "last_crawled": None,
                    "error": None,
                    "time_to_crawl": None,
                    "total_books": db["books"].count_documents({"source": name})
                }

        self._save()

    def _save(self):
        db["status"].update_one({}, {"$set": self.status}, upsert=True)
    
    def set_status(self, status: str):
        self.status["status"] = status
        self._save()
    
    def update_status(self, scraper_name, last_crawled, error: str, time_to_crawl: int, total_books: int | None = None):
        if scraper_name not in self.status:
            raise ValueError(f"Scraper {scraper_name} not found in status")
        
        self.status[scraper_name]["last_crawled"] = last_crawled
        self.status[scraper_name]["error"] = error
        self.status[scraper_name]["time_to_crawl"] = time_to_crawl

        if total_books is None:
            total_books = db["books"].count_documents({"source": scraper_name})

        self.status[scraper_name]["total_books"] = total_books
        
        self._save()

class BookManager:
    def __init__(self):
        self.books = db["books"]

    def upload_books(self, source: str, books: list[dict]) -> None:
        """
        Delete all books for this source then insert new ones.
        Adds normalized fields (titleNormalized, authorNormalized) for hamza-agnostic search.
        """
        
        # Convert books to dicts and add normalized fields
        books_dicts = []
        for book in books:
            
            # Add normalized fields for search (without hamzas)
            book['titleNormalized'] = sanitize_arabic_text(book['title'])

            book['authorNormalized'] = None
            book['publisherNormalized'] = None

            if "author" in book and book['author']:
                book['authorNormalized'] = sanitize_arabic_text(book['author'])
            
            if "publisher" in book and book['publisher']:
                book['publisherNormalized'] = sanitize_arabic_text(book['publisher'])
            
            books_dicts.append(book)
        
        self.books.delete_many({"source": source})
        self.books.insert_many(books_dicts)