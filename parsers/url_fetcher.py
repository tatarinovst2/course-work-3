""""""
import time

import aiohttp
from aiohttp import client_exceptions

from headers_generation import get_headers


async def fetch(url: str) -> str:
    """Fetch the page."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=get_headers(), allow_redirects=False) as response:
                if response.status != 200:
                    print(f"Error for {url}: ", response.status)
                    return ""

                return await response.text(encoding="utf-8")
        except aiohttp.client_exceptions.ClientConnectorError:
            print(f"Error for {url}: ", "Connection refused")
            time.sleep(15)
            return ""
