from __future__ import annotations
from typing import Any

from core.services.sheets import SheetsClient
from core.services.sa_cache import sa_cache
from config import settings

TECH_SHEET = settings.TECH_SHEET_ID


class GsDB:
    """Операции Users / Stores на Google Sheets."""

    def __init__(self):
        self.sheets = SheetsClient(settings.ADMIN_SA_JSON)

    # ─── helpers ───
    async def _ws(self, title: str):
        return await self.sheets.get_worksheet(TECH_SHEET, title)

    # ─── Users ───
    async def ensure_user(self, tg_id: int, username: str | None,
                          full_name: str | None):
        ws = await self._ws("Users")
        rows = (await self.sheets.read_all(ws))[1:]
        if str(tg_id) not in [r[0] for r in rows]:
            await self.sheets.append_rows(ws, [[tg_id, username, full_name]])

    # ─── Stores ───
    async def get_stores_by_owner(self, tg_id: int) -> list[dict[str, Any]]:
        ws = await self._ws("Stores")
        rows = (await self.sheets.read_all(ws))[1:]
        return [
            dict(store_id=r[0], marketplace=r[2], name=r[3])
            for r in rows if r[1] == str(tg_id)
        ]

    async def add_store(self, *, store_id: str, owner_id: int, marketplace: str,
                        name: str, credentials_json: str,
                        sheet_id: str, sa_path: str):
        ws = await self._ws("Stores")
        await self.sheets.append_rows(
            ws,
            [[store_id, owner_id, marketplace, name,
              credentials_json, sheet_id, sa_path]]
        )

    # ─── сервис-аккаунт ───
    async def pick_service_account(self) -> dict[str, str]:
        sa = await sa_cache.pick()
        if not sa:
            raise RuntimeError("В service_acc нет доступных аккаунтов")
        return sa
