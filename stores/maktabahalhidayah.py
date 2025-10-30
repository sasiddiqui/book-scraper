import os, requests
from book import Book
from stores.wooscraper import WooScraper

# This is an integration with woo, not a scraper
class MaktabahAlHidayah(WooScraper):
    def __init__(self) -> None:
        super().__init__("Maktabah Al-Hidayah", "https://maktabahalhidayah.com", os.getenv("HIDAYAH_CK"), os.getenv("HIDAYAH_CS"))
    
    
