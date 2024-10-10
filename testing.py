import bs4
import pprint
import requests
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa
from stores.kitaabun import Kitaabun

url = "https://kitaabun.com/shopping3/products_new.php?page=39"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}

response = requests.get(url, headers=headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser')

# new_links = [link['href'] for link in soup.find_all('a', href=True)]
# pprint.pprint(new_links)

pprint.pprint(Kitaabun().extract_book_info(soup, url), indent=4)
