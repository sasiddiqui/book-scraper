# script to find the rate limiting for a given website
import argparse

import aiohttp
import asyncio

async def main(url) -> int:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                return True
            else:
                return False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Find the batch size for a given website')
    parser.add_argument('--url', type=str, help='URL to test')

    args = parser.parse_args()
