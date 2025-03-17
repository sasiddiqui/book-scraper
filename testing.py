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
from stores.ismaeel import IsmaeelScraper
from stores.safinatulnajat import SafinatulNajat

url = ""

scraper = SifatuSafwa()


response = requests.get(url, headers=scraper.headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser', parse_only=scraper.strainer)

# new_links = [link['href'] for link in soup.find_all('a', href=True)]
# pprint.pprint(new_links)

pprint.pprint(scraper.extract_book_info(soup, url), indent=4)
