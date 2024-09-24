import bs4
import pprint
import requests
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa

url = "https://www.sifatusafwa.com/en/ramadaan-siyaam-and-aid/48-questions-on-fasting-by-shaykh-al-uthaymeen.html"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',

}

response = requests.get(url, headers=headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser')

pprint.pprint(SifatuSafwa().extract_book_info(soup, url))
