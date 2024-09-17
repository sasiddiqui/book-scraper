from abc import ABC, abstractmethod
import requests
import json
import time
import requests_cache
from bs4 import BeautifulSoup, SoupStrainer
import csv
from urllib.parse import urlparse, urljoin
import logging
import datetime
import pickle


start_timestamp = str(datetime.datetime.now())


requests_cache.install_cache('scraper_cache', backend='filesystem')

logger = logging.getLogger('scraper')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')



file_handler = logging.FileHandler(f'logs/scraper-{start_timestamp}.log')
file_handler.setLevel(logging.DEBUG)
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
    def __init__(self, base_url):
        self.base_url = base_url
        self.name = remove_tld(base_url)
        self.logger = logger
        self.urls_to_visit = set()
        self.visited_urls = set()
        self.all_books = []

    @abstractmethod
    def extract_book_info(self, soup, url):
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

    async def fetch_page(self, session: ClientSession, url: str) -> tuple[str, str]:

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        try:
            async with session.get(url, headers=headers, timeout=20) as response:
                if response.status == 200:
                    content = await response.text()
                    print(f'Fetched {url}')
                    return url, content
                else:
                    self.logger.error(f'Failed to retrieve the page. Status code: {response.status}')
        except asyncio.TimeoutError:
            self.logger.error(f'Timeout on {url}')
        except Exception as e:
            self.logger.error(f'Unexpected error on {url}: {e}')
        
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

    async def crawl_product_pages(self, initial_urls=list(), use_cached_links=None):

        start = time.time()
        if use_cached_links:
            self.get_cache(use_cached_links)
        else:
            self.urls_to_visit = [self.base_url] + initial_urls

        logger.debug(f'Starting to crawl {self.urls_to_visit}')

        count = 0
        batch_size = 150

        async with aiohttp.ClientSession() as session:
            while self.urls_to_visit:
                tasks = []
                # Process URLs in batches
                # create a batch of URLs to visit
                for _ in range(min(batch_size, len(self.urls_to_visit))):
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
                            soup = BeautifulSoup(response, 'lxml')
                            logger.debug(f'Parsing {url}')
                            try:
                                book_info = self.extract_book_info(soup, url)

                                if book_info:
                                    self.all_books.append(book_info)
                                    logger.debug(f'Got book info {book_info}')

                            except AttributeError:
                                logger.warning(f'Could not find essential book details on {url}')

                        if not soup:
                            # only parse the <a> tags to optimize
                            soup = BeautifulSoup(response, 'lxml', parse_only=SoupStrainer('a'))
                        
                        # Find all links on the page and add product links to the queue
                        new_links = [link['href'] for link in soup.find_all('a', href=True)]
                        for link in new_links:
                            absolute_link = urljoin(url, link)
                            if self.url_in_domain(absolute_link) and not self.ignore_url(absolute_link) and absolute_link not in self.visited_urls and absolute_link not in self.urls_to_visit:
                                self.urls_to_visit.append(absolute_link)
                                logger.debug(f'Adding {absolute_link} to urls to visit')
                        
                    count += 1
                    print(count)
                    if count % 25 == 0:
                        self.save_lines_to_file(self.urls_to_visit, f"saved_progress/urls_to_visit_{start_timestamp}")
                        self.save_lines_to_file(list(self.visited_urls), f"saved_progress/visited_urls_{start_timestamp}")
                        # with open(f"saved_progress/all_books_{start_timestamp}", 'wb') as file:
                            
                        #     pickle.dump(self.all_books, file)
                        logger.info(f'Saved progress at {count} links')
                            
                        self.write_to_json()

            # Write all extracted book information to a CSV file
            self.write_to_json()
            logger.info(f"Finished crawling in {time.time() - start} seconds")

