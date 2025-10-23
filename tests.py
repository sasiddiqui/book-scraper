import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pytest
from stores.salafi import Salafi
import pprint

@pytest.mark.asyncio
async def test_salafi():
    scraper = Salafi()
    if not scraper.test_base_url():
        pytest.skip("Could not reach base URL")

    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in scraper.test_urls:
            tasks.append(asyncio.create_task(scraper.fetch_page(session, url)))
        responses = await asyncio.gather(*tasks)
    
    for url, response in responses:
        if response:
            soup = BeautifulSoup(response, 'html.parser', parse_only=scraper.strainer)

            new_links = [link['href'] for link in soup.find_all('a', href=True)]
            pprint.pprint(new_links, indent=4)
            book = scraper.extract_book_info(soup, url)
            pprint.pprint(book, indent=4)
            print("-" * 100)
            assert book is not None
            assert book["title"] is not None
            assert book["price"] is not None
            assert book["url"] is not None
            assert book["source"] is not None
    
    assert True
    
    
