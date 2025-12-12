# this scraper takes advantage of the sitemap

import asyncio
import aiohttp
from bs4 import BeautifulSoup, SoupStrainer
import requests
from scraper import ScraperError, AbstractBookScraper
import time
from book import Book


class Irfan(AbstractBookScraper):
    def __init__(self):
        super().__init__("https://www.irfanbooks.org/", "Irfan Books")
        self.strainer = SoupStrainer("meta")
    
    def is_product_url(self, url):
        return True
    
    def extract_book_info(self, soup: BeautifulSoup, url) -> dict | None:
        book_info = {}

        book_info['url'] = url
        book_info["source"] = self.name

        try: 
            book_info["title"] = soup.find("meta", property="og:title")["content"].replace("| Irfan Books", "").strip()
        except AttributeError:
            self.logger.warning(f"Could not find title for {url}")
            return None
        
        try: 
            book_info["price"] = float(soup.find("meta", property="product:price:amount")["content"].strip())
        except AttributeError:
            self.logger.warning(f"Could not find price for {url}")
            return None
        
        try: 
            book_info["image"] = soup.find("meta", property="og:image")["content"].strip()
        except AttributeError:
            self.logger.info(f"Could not find image for {url}")

        try:
            book_info["instock"] = soup.find("meta", property="og:availability")["content"].strip() == "InStock"
        except AttributeError:
            self.logger.info(f"Could not find instock for {url}")

        return book_info
    
    async def crawl_product_pages(self) -> list[dict]:

        self.test_base_url()

        sitemap = requests.get(self.base_url + "store-products-sitemap.xml").text

        soup = BeautifulSoup(sitemap, "xml")
        urls = [url.text for url in soup.find_all("loc") if "file" not in url.text] 
        


        async with aiohttp.ClientSession() as session:
            all_res = []
            while len(urls) > 0:
                batch_urls = urls[:self.batch_size]
                tasks = []
                for url in batch_urls:
                    tasks.append(asyncio.create_task(self.fetch_page(session, url)))
                    urls.remove(url)
                
                all_res.extend(await asyncio.gather(*tasks))
                time.sleep(self.batch_delay)

            for url, response in all_res:
                if response:
                    soup = BeautifulSoup(response, "lxml", parse_only=self.strainer)
                    book_info = self.extract_book_info(soup, url)
                    if book_info is not None:
                        import pprint
                        book_info = Book(**book_info)
                        pprint.pprint(book_info)
                        self.add_book(book_info)


        return self.all_books
