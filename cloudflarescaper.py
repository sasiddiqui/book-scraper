# scraper that bypasses cloudflare turnstile
from scraper import AbstractBookScraper

class CloudflareScraper(AbstractBookScraper):
    def __init__(self, base_url, name, convert_rate=1):
        super().__init__(base_url, name, convert_rate)
    
    def ignore_url(self, url):
        return False

    def is_product_url(self, url):
        return False

    def extract_book_info(self, soup, url):
        return None
    
    def test_base_url(self):
        pass
    
    def crawl_product_pages(self):
        

