import bs4
import pprint
import requests
from stores.albalagh import AlBalagh    
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa
from stores.kitaabun import Kitaabun

url = "https://www.albalaghbooks.com/fiqh/principles-of-jurisprudence-usul/ghayat-al-wusul-ila-sharh-lubb-al-usul/"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}

response = requests.get(url, headers=headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser', parse_only=AlBalagh().strainer)

# new_links = [link['href'] for link in soup.find_all('a', href=True)]
# pprint.pprint(new_links)

pprint.pprint(AlBalagh().extract_book_info(soup, url), indent=4)
