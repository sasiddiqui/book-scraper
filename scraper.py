from abc import ABC, abstractmethod
import requests
import requests_cache
from bs4 import BeautifulSoup
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
    
    return domain_parts[-1]


class AbstractBookScraper(ABC):
    def __init__(self, base_url):
        self.base_url = base_url
        self.name = remove_tld(base_url)
        self.logger = logger

    @abstractmethod
    def extract_book_info(self, soup, url):
        pass

    @abstractmethod
    def is_product_url(self, soup, url):
        pass

    @abstractmethod
    def find_product_links(self, soup):
        pass


    @abstractmethod
    def ignore_url(self, url) -> bool:
        # for urls that should not be visited such as image uploads. True if should be ignored
        pass

    def url_in_domain(self, url):
        return url.startswith(self.base_url)

    def write_to_csv(self, book_list):
        with open('csvs/' + self.name, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['URL', 'Title', 'Author', 'Price'])
            writer.writeheader()
            for book in book_list:
                writer.writerow(book)
    
    def save_lines_to_file(self, links, filename):
        with open(f"{filename}.txt", 'w') as file:
            for link in links:
                file.write(str(link))
                file.write('\n')


    def crawl_product_pages(self, initial_urls=list(), use_cached_links=None):

        if use_cached_links:
            with open(f"saved_progress/urls_to_visit_{use_cached_links}.txt", 'r') as file:
                urls_to_visit = file.readlines()
                urls_to_visit = [url.strip() for url in urls_to_visit]

            with open(f"saved_progress/visited_urls_{use_cached_links}.txt", 'r') as file:
                visited_urls = file.readlines()
                visited_urls = [url.strip() for url in visited_urls]

            with open(f"saved_progress/all_books_{use_cached_links}.txt", 'r') as file:
                db = pickle.load(open('examplePickle', 'rb'))
        else:
            visited_urls = set()
            urls_to_visit = [self.base_url] + initial_urls
            all_books = []

        logger.debug(f'Starting to crawl {urls_to_visit}')

        count = 0

        while urls_to_visit:
            # Every 100 links, save our progress.
            count += 1
            if count % 100 == 0:
                self.save_lines_to_file(urls_to_visit, f"saved_progress/urls_to_visit_{start_timestamp}")
                self.save_lines_to_file(visited_urls, f"saved_progress/visited_urls_{start_timestamp}")
                pickle.dump(all_books, open(f"saved_progress/all_books_{start_timestamp}", 'wb'))                    
                logger.info(f'Saved progress at {count} links')

            url = urls_to_visit.pop(0)
            if url in visited_urls:
                continue

            visited_urls.add(url)

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            try:
                response = requests.get(url, headers=headers, timeout=10)
            except requests.exceptions.Timeout:
                logger.error(f'Timeout on {url}')
                continue
            except Exception as e:
                logger.error(f'Unexpected error on {url}: {e}')
                continue
            
            if response.status_code == 200:
                try:
                    soup = BeautifulSoup(response.content, 'html.parser')
                except:
                    logger.error(f'Failed to parse the page {url}')
                    continue

                # If it is a product page, extract book information
                if self.is_product_url(url):
                    logger.debug(f'Parsing {url}')
                    try:
                        book_info = self.extract_book_info(soup, url)

                        if book_info:
                            all_books.append(book_info)
                            logger.debug(f'Got book info {book_info}')
                    except AttributeError:
                        logger.warning(f'Could not find essential book details on {url}')

                # Find all links on the page and add product links to the queue
                new_links = [link['href'] for link in soup.find_all('a', href=True)]
                for link in new_links:
                    if self.url_in_domain(link) and link not in visited_urls and link not in urls_to_visit and link.startswith(self.base_url) and not self.ignore_url(link):# link.startswith(('http://', 'https://')):
                        urls_to_visit.append(link)
                        logger.debug(f'Adding {link} to urls to visit')

            else:
                logger.error(f'Failed to retrieve the page. Status code: {response.status_code}')

        # Write all extracted book information to a CSV file
        self.write_to_csv(all_books)
