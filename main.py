import asyncio
import logging
from datetime import datetime
from urllib.parse import urljoin
from scraper import ScraperError
from stores.zakariyya import ZakariyyaBooksScraper
from stores.ismaeel import IsmaeelScraper
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa
from stores.kitaabun import Kitaabun
from stores.albalagh import AlBalagh
from stores.kunuz import Kunuz
from stores.buraq import Buraq
from upload import BookManager, StatusManager

async def main():

    # await AlBadrBooksScraper().crawl_product_pages()
    # await IsmaeelScraper().crawl_product_pages()
    # await ZakariyyaBooksScraper().crawl_product_pages()
    # await Kunuz().crawl_product_pages()
    # await Qurtuba().crawl_product_pages()
    # await SifatuSafwa().crawl_product_pages()
    # await AlHidayaah().crawl_product_pages()
    # await AlBalagh().crawl_product_pages()
    # await Kitaabun().crawl_product_pages()
    # await Buraq().crawl_product_pages()

    scrapers = [
        Qurtuba,
        IsmaeelScraper,
        SifatuSafwa,
        AlBadrBooksScraper,
        Buraq,
        AlHidayaah,
        AlBalagh,
        Kunuz,      
        ZakariyyaBooksScraper,
    ]

    db = BookManager()
    status = StatusManager(scrapers)
    logger = logging.getLogger()

    while True:
        for scraper in scrapers:
            try:
                scrape = scraper()
                status.set_status(scrape.name)

                start_time = datetime.now()
                books = await scrape.crawl_product_pages()
                time_to_crawl = datetime.now() - start_time
                # ensures that some books were actually found 
                if books:
                    db.upload_books(scrape.name, books)

            except Exception as e:

                time_to_crawl = datetime.now() - start_time
                logger.error(e)
                status.update_status(scrape.name, error=e.__str__(), last_crawled=datetime.now(), time_to_crawl=time_to_crawl.seconds/60)

            else:
                status.update_status(scrape.name, error=None, last_crawled=datetime.now(), time_to_crawl=time_to_crawl.seconds/60, total_books=len(books))

            logger.info(f"Finished {scrape.name}")
        
        logger.info("Sleeping for 1 hour")
        status.set_status("idle")
        await asyncio.sleep(3600)

if __name__ == '__main__':
    # call main
    asyncio.run(main())