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
from stores.buraq import Buraq                  

url = "https://buraqbooks.com/products/%D8%A7%D9%84%D9%81%D8%AA%D8%A7%D9%88%D9%89-%D8%A7%D9%84%D8%AA%D8%A7%D8%AA%D8%B1%D8%AE%D8%A7%D9%86%D9%8A%D8%A9"

scraper = Buraq()


response = requests.get(url, headers=scraper.headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser', parse_only=scraper.strainer)

# new_links = [link['href'] for link in soup.find_all('a', href=True)]
# pprint.pprint(new_links)

pprint.pprint(scraper.extract_book_info(soup, url), indent=4)
