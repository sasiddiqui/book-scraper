from book import Book
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = open("mongourl.txt").read().strip()

client = MongoClient(uri, server_api=ServerApi('1'))
client.admin.command('ping')
print("Pinged your deployment. You successfully connected to MongoDB!")

db = client["data"]



class StatusManager:
    """
    Manages the status of the scrapers and inserts into db for frontend to display
    """
    def __init__(self, scrapers):
        # get the status doc
        self.status = db["status"].find_one() or {}

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

    def upload_books(self, source: str, books: list[Book]) -> None:
        """delete all books for this source then insert new ones"""

        self.books.delete_many({"source": source})
        self.books.insert_many(books)