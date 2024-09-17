import bs4
import pprint
import requests
from stores.ismaeel import IsmaeelScapper
url = "https://ismaeelbooks.co.uk/product/beware-of-oppression/"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
response = requests.get(url, headers=headers)
soup = bs4.BeautifulSoup(response.content, 'html.parser')

# pprint.pprint([i["href"] for i in soup.find_all("a")])

pprint.pprint(IsmaeelScapper().extract_book_info(soup, url))
