import aiohttp
from typing import Any

OZON_URL = "https://api-seller.ozon.ru"

class OzonAPI:
    def __init__(self, client_id: str, api_key: str):
        self.client_id = client_id
        self.api_key = api_key
        self._headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{OZON_URL}{path}"
        async with aiohttp.ClientSession(headers=self._headers) as sess:
            async with sess.request(method, url, **kwargs) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def ping(self) -> bool:
        try:
            await self._request("POST", "/v1/posting/fbs/list", json={"limit": 1})
            return True
        except Exception:
            return False
