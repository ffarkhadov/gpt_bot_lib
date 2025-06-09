from __future__ import annotations
import json
from typing import Any

from core.services.sheets import SheetsClient
from config import settings

TECH_SHEET = settings.TECH_SHEET_ID


class GsDB:
    """Мини-ORM для Users / Stores"""

    def __init__(self):
        self.sheets = SheetsClient(settings.ADMIN_SA_JSON)

    # ---------- helpers ----------
    async def _ws(self, title: str):
        return await self.sheets.get_worksheet(TECH_SHEET, title)

    # ---------- USERS ----------
    async def ensure_user(self, tg_id: int, username: str | None,
                          full_name: str | None):
        ws = await self._ws("Users")
        rows = await self.sheets.read_all(ws)[1:]          # без заголовка
        ids = [r[0] for r in rows]
        if str(tg_id) in ids:
            return  # уже есть
        await self.sheets.append_rows(ws, [[tg_id, username, full_name]])

    # ---------- STORES ----------
    async def get_stores_by_owner(self, tg_id: int) -> list[dict[str, Any]]:
        ws = await self._ws("Stores")
        rows = await self.sheets.read_all(ws)
        stores = []
        for r in rows[1:]:
            if r[1] == str(tg_id):
                stores.append(
                    dict(store_id=r[0], marketplace=r[2], name=r[3])
                )
        return stores

    async def add_store(self, *, store_id: str, owner_id: int,
                        marketplace: str, name: str,
                        credentials_json: str, sheet_id: str):
        ws = await self._ws("Stores")
        await self.sheets.append_rows(
            ws, [[store_id, owner_id, marketplace,
                  name, credentials_json, sheet_id]]
        )
