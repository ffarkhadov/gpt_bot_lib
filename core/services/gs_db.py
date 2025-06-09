from __future__ import annotations
from typing import Any
from json import dumps

from core.services.sheets import SheetsClient
from config import settings

TECH_SHEET = settings.TECH_SHEET_ID

-from core.services.sheets import SheetsClient
+from core.services.sheets import SheetsClient
+from core.services.sa_cache import sa_cache
@@
-    async def pick_service_account(self) -> dict[str, str]:
-        ...
+    async def pick_service_account(self) -> dict[str, str]:
+        """
+        Берём из RAM-кэша; запись used_count делается там же.
+        """
+        sa = await sa_cache.pick()
+        if not sa:
+            raise RuntimeError("Нет доступных сервис-аккаунтов")
+        return sa


class GsDB:
    """
    Users / Stores / service_acc
    """

    def __init__(self):
        self.sheets = SheetsClient(settings.ADMIN_SA_JSON)

    # ───────────────── helpers ─────────────────
    async def _ws(self, title: str):
        return await self.sheets.get_worksheet(TECH_SHEET, title)

    # ───────────────── USERS ─────────────────
    async def ensure_user(self, tg_id: int, username: str | None,
                          full_name: str | None):
        ws = await self._ws("Users")
        rows = (await self.sheets.read_all(ws))[1:]   # без заголовка
        if str(tg_id) not in [r[0] for r in rows]:
            await self.sheets.append_rows(ws, [[tg_id, username, full_name]])

    # ───────────────── STORES ─────────────────
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

    # ─────────────── service_acc ───────────────
    async def pick_service_account(self) -> dict[str, str]:
        """
        Возвращает dict(path=…, email=…), увеличивая used_count на 1.
        """
        ws = await self._ws("service_acc")
        rows = (await self.sheets.read_all(ws))[1:]
        # ищем минимальный used_count (C = index 2)
        idx_row, row = min(
            enumerate(rows, start=2),
            key=lambda t: int(t[1][2])
        )
        path, email, used = row[0], row[3], int(row[2])

        # +1 к used_count
        ws.update(f"C{idx_row}", [[used + 1]])
        return {"path": path, "email": email}
