import bs4
import pprint
import requests
from stores.albadr import AlBadrBooksScraper
url = "https://albadr.co.uk/product/durus-fi-sharh-nawaqid-al-islam-shaykh-abdullah-bin-salih-al-fawzan/"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
response = requests.get(url, headers=headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser')

# pprint.pprint([i["href"] for i in soup.find_all("a")])

pprint.pprint(AlBadrBooksScraper().extract_book_info(soup, url))
