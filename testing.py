import bs4
import pprint
import requests
from stores.albalagh import AlBalagh    
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa
from stores.kitaabun import Kitaabun
from stores.kunuz import Kunuz

url = "https://www.alkunuz.co.uk/product-page/%D8%A7%D9%84%D8%BA%D8%B1%D8%A9-%D8%A7%D9%84%D9%85%D9%86%D9%8A%D9%81%D8%A9-%D9%81%D9%8A-%D8%AA%D8%B1%D8%AC%D9%8A%D8%AD-%D9%85%D8%B0%D9%87%D8%A8-%D8%A7%D8%A8%D9%8A-%D8%AD%D9%86%D9%8A%D9%81%D8%A9"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}

response = requests.get(url, headers=headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser')

# new_links = [link['href'] for link in soup.find_all('a', href=True)]
# pprint.pprint(new_links)

pprint.pprint(Kunuz().extract_book_info(soup, url), indent=4)
