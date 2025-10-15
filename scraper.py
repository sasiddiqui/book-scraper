from pydantic import ValidationError
from abc import ABC, abstractmethod
from book import Book
import json
import time
from bs4 import BeautifulSoup, SoupStrainer
import csv
from urllib.parse import urlparse, urljoin
import logging
import datetime
import pickle


class ScraperError(Exception):
    pass

start_timestamp = str(datetime.datetime.now())



logger = logging.getLogger('scraper')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')



file_handler = logging.FileHandler(f'logs/scraper-{start_timestamp}.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


def remove_tld(url):
    # Parse the URL
    parsed_url = urlparse(url)
    
    # Split the hostname to remove the TLD
    domain_parts = parsed_url.hostname.split('.')
    
    return domain_parts[1]

import asyncio
import aiohttp
from aiohttp import ClientSession

class AbstractBookScraper(ABC):
    def __init__(self, base_url, name, convert_rate=1):
        self.base_url = base_url
        self.name = name
        self.logger = logger
        self.urls_to_visit = set()
        self.visited_urls = set()
        self.all_books = []
        self.count = 0
        self.batch_size = 20
        self.strainer = SoupStrainer()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        self.error_count = 0
        self.ERROR_THRESHOLD = 20

        # converting from GBP to USD
        self.convert_rate = convert_rate

    @abstractmethod
    def extract_book_info(self, soup, url) -> Book | None:
        pass

    @abstractmethod
    def is_product_url(self, url):
        pass


    @abstractmethod
    def ignore_url(self, url) -> bool:
        pass

    def url_in_domain(self, url):
        return url.startswith(self.base_url)

    def write_to_csv(self, book_list):
        with open('csvs/' + self.name, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['URL', 'Title', 'Author', 'Price', "In Stock", "Image", "Editor", "Edition", "Year Published", "Volumes", "Pages", "Binding", "Weight"])
            writer.writeheader()
            for book in book_list:
                writer.writerow(book)

    def write_to_json(self):
        json.dump(self.all_books, open("jsons/" + self.name + ".json", 'w'), indent=4)

    def save_lines_to_file(self, links, filename):
        with open(f"{filename}.txt", 'w') as file:
            for link in links:
                file.write(str(link))
                file.write('\n')
    
    # in a method so that it can be overridden by some scrapers
    def add_book(self, book_info: Book) -> None:
        if book_info:
            self.all_books.append(book_info.model_dump(exclude_none=True))


    async def fetch_page(self, session: ClientSession, url: str) -> tuple[str, str]:
        try:
            async with session.get(url, headers=self.headers, timeout=20) as response:
                if response.status == 200:
                    content = await response.content.read()
                    return url, content
                else:
                    self.logger.error(f'Failed to retrieve the page. Status code: {response.status}')
                    self.error_count += 1


                    if response.status in [503, 429]:
                        raise ScraperError(f'{response.status} error on {url}')
                    

        except asyncio.TimeoutError:
            self.logger.error(f'Timeout on {url}')
        except Exception as e:
            self.logger.error(f'Unexpected error on {url}: {e}')
            self.error_count += 1
            if isinstance(e, ScraperError):
                raise e
        
        if self.error_count > self.ERROR_THRESHOLD:
            raise ScraperError(f'Error threshold reached for {url}')
        
        return url, None

    def get_cache(self, use_cached_links) -> None:
        with open(f"saved_progress/urls_to_visit_{use_cached_links}.txt", 'r') as file:
            self.urls_to_visit = file.readlines()
            self.urls_to_visit = set([url.strip() for url in self.urls_to_visit])

        with open(f"saved_progress/visited_urls_{use_cached_links}.txt", 'r') as file:
            self.visited_urls = file.readlines()
            self.visited_urls = set([url.strip() for url in self.visited_urls])

        with open(f"saved_progress/all_books_{use_cached_links}", 'rb') as file:
            self.all_books = pickle.load(file)

    async def crawl_product_pages(self, initial_urls=list(), use_cached_links=None) -> list[dict]:
        print(f"Crawling {self.name}")

        start = time.time()
        if use_cached_links:
            self.get_cache(use_cached_links)
        else:
            self.urls_to_visit = [self.base_url] + initial_urls

        self.count = 0

        async with aiohttp.ClientSession() as session:
            while self.urls_to_visit:
                tasks = []
                # Process URLs in batches
                # create a batch of URLs to visit
                for _ in range(min(self.batch_size, len(self.urls_to_visit))):
                    url = self.urls_to_visit.pop()
                    if url in self.visited_urls:
                        continue
                    self.visited_urls.add(url)
                    task = asyncio.create_task(self.fetch_page(session, url))
                    tasks.append(task)

                # wait for all tasks to complete
                responses = await asyncio.gather(*tasks)
                for url, response in responses:
                    if response:
                        soup = None
                        # If it is a product page, extract book information
                        if self.is_product_url(url):
                            soup = BeautifulSoup(response, 'lxml', parse_only=self.strainer)
                            logger.debug(f'Parsing {url}')
                            try:
                                book_info = self.extract_book_info(soup, url)

                                if book_info is not None:
                                    try:
                                        book_info = Book(**book_info)
                                        book_info.price *= self.convert_rate

                                    except ValidationError as e:
                                        logger.warning(f'Could not validate book info on {url}: {e}')
                                        continue


                            except AttributeError:
                                logger.warning(f'Could not find essential book details on {url}')
                                continue 

                            self.add_book(book_info)
                            logger.log(f'SUCCESS - Added {book_info["title"]} to all books')

                        soup = BeautifulSoup(response, 'lxml', parse_only=SoupStrainer('a'))
                        
                        # Find all links on the page and add product links to the queue
                        new_links = [link['href'] for link in soup.find_all('a', href=True)]
                        for link in new_links:
                            absolute_link = urljoin(url, link)
                            if self.url_in_domain(absolute_link) and not self.ignore_url(absolute_link) and absolute_link not in self.visited_urls and absolute_link not in self.urls_to_visit:
                                self.urls_to_visit.append(absolute_link)
                                logger.debug(f'Adding {absolute_link} to urls to visit')
                        
                    self.count += 1
                    # if self.count % 25 == 0:
                        # self.save_lines_to_file(self.urls_to_visit, f"saved_progress/urls_to_visit_{start_timestamp}")
                        # self.save_lines_to_file(list(self.visited_urls), f"saved_progress/visited_urls_{start_timestamp}")
                        # logger.info(f'Saved progress at {self.count} links')
                            
                        # self.write_to_json()

            # Write all extracted book information to a CSV file
            self.write_to_json()
            print(f"Finished crawling {self.name} in {time.time() - start} seconds")
            return self.all_books

