import bs4
from ..scraper import AbstractBookScraper

class SafinatulNajat(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://safinatulnajat.com/", "Safinatul Najat", convert_rate=1.3)

    def extract_book_info(self, soup: bs4.BeautifulSoup, url: str) -> dict | None:
        pass