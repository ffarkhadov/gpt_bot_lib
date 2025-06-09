"""
SACache — выбирает сервис-аккаунт за O(1) из кэша.
Чтение таблицы = 1 раз в TTL секунд, инкремент used_count
пушится в Sheets в фоне, поэтому пользователь не ждёт I/O.
"""
from __future__ import annotations
import asyncio, time, logging
from typing import Any

from core.services.sheets import SheetsClient
from config import settings

log = logging.getLogger(__name__)
TTL = 300  # секунд

class SACache:
    def __init__(self):
        self._data: list[list[str]] = []
        self._stamp: float = 0
        self._lock = asyncio.Lock()
        self.sheets = SheetsClient(settings.ADMIN_SA_JSON)

    async def _refresh(self):
        ws = await self.sheets.get_worksheet(settings.TECH_SHEET_ID, "service_acc")
        self._data = (await self.sheets.read_all(ws))[1:]     # без заголовка
        self._stamp = time.time()

    async def pick(self) -> dict[str, str]:
        async with self._lock:
            if time.time() - self._stamp > TTL or not self._data:
                await self._refresh()

            # минимальный used_count (колонка 2)
            idx, row = min(
                enumerate(self._data),
                key=lambda t: int(t[1][2])
            )
            row[2] = str(int(row[2]) + 1)        # инкремент локально
            path, email = row[0], row[3]

            # фоновая синхронизация
            asyncio.create_task(self._push(idx + 2, row[2]))  # +2: заголовок+1-based
            return {"path": path, "email": email}

    async def _push(self, row_idx: int, new_val: str):
        """Отправляем PATCH в Sheets без ожидания."""
        try:
            ws = await self.sheets.get_worksheet(settings.TECH_SHEET_ID, "service_acc")
            await ws.update(f"C{row_idx}", [[new_val]])
        except Exception as e:
            log.warning("SA push failed: %s", e)


# глобальный singleton
sa_cache = SACache()
