import aiohttp
from typing import Any

WB_URL = "https://suppliers-api.wildberries.ru"

class WBAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._headers = {
            "Authorization": api_key,
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{WB_URL}{path}"
        async with aiohttp.ClientSession(headers=self._headers) as sess:
            async with sess.request(method, url, **kwargs) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def ping(self) -> bool:
        try:
            await self._request("GET", "/api/v3/orders?limit=1")
            return True
        except Exception:
            return False
