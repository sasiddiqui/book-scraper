import asyncio
import argparse
from datetime import datetime
from stores.maktabahalhidayah import MaktabahAlHidayah
from stores.zakariyya import ZakariyyaBooksScraper
from stores.ismaeel import IsmaeelScraper
from stores.albadr import AlBadrBooksScraper
from stores.alhidaayah import AlHidayaah
from stores.qurtuba import Qurtuba
from stores.sifatusafwa import SifatuSafwa
from stores.albalagh import AlBalagh
from stores.kunuz import Kunuz
from stores.buraq import Buraq
from stores.salafi import Salafi
from upload import BookManager, StatusManager
import logging
from dotenv import load_dotenv


logger = logging.getLogger('scraper')

# Store name mapping to scraper classes
STORE_MAPPING = {
    'zakariyya': ZakariyyaBooksScraper,
    'ismaeel': IsmaeelScraper,
    'albadr': AlBadrBooksScraper,
    'alhidaayah': AlHidayaah,
    'qurtuba': Qurtuba,
    'sifatusafwa': SifatuSafwa,
    'albalagh': AlBalagh,
    'kunuz': Kunuz,
    'buraq': Buraq,
    'salafi': Salafi,  
    'maktabahalhidayah': MaktabahAlHidayah,
}

async def main(store_name=None, no_save=False):

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

    # Select scrapers based on store_name parameter
    if store_name:
        if store_name not in STORE_MAPPING:
            logger.error(f"Unknown store: {store_name}. Available stores: {', '.join(STORE_MAPPING.keys())}")
            return
        scrapers = [STORE_MAPPING[store_name]]
        logger.info(f"Running scraper for store: {store_name}")
    else:
        scrapers = [
            ZakariyyaBooksScraper,
            AlHidayaah,
            Qurtuba,
            IsmaeelScraper,
            SifatuSafwa,
            AlBadrBooksScraper,
            Buraq,
            AlBalagh,
            Kunuz,      
            Salafi,
            MaktabahAlHidayah,
        ]
        logger.info("Running all scrapers")

    db = BookManager()
    status = StatusManager(scrapers)

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
                    if not no_save:
                        db.upload_books(scrape.name, books)

            except Exception as e:

                time_to_crawl = datetime.now() - start_time
                logger.error(f"Critical Error in {scrape.name}: {e}")
                status.update_status(scrape.name, error=e.__str__(), last_crawled=datetime.now(), time_to_crawl=time_to_crawl.seconds/60)

            else:
                status.update_status(scrape.name, error=None, last_crawled=datetime.now(), time_to_crawl=time_to_crawl.seconds/60, total_books=len(books))

            logger.info(f"Finished {scrape.name} in {time_to_crawl.seconds/60} minutes")
        
        logger.info("Sleeping for 1 day")
        status.set_status("idle")
        await asyncio.sleep(86400)

if __name__ == '__main__':
    if not load_dotenv():
        raise ValueError("No .env file found")

    parser = argparse.ArgumentParser(description='Book scraper with optional store selection')
    parser.add_argument('--store', type=str, help='Run scraper for specific store only')
    parser.add_argument('--no-save', action='store_true', help='Do not save books to database')
    args = parser.parse_args()
    
    # call main with store argument
    asyncio.run(main(args.store, args.no_save))