"""
run_report(store_cfg) — запускает отчётный скрипт и передаёт в него
ключи/токены динамически через monkey-patch переменных.
"""
from importlib import import_module
import asyncio, logging, json

from core.services.sheets import SheetsClient

log = logging.getLogger(__name__)

async def run_report(store_cfg: dict):
    """
    store_cfg =
      {
        store_id, marketplace, credentials_json, sheet_id, sa_path
      }
    """
    creds = json.loads(store_cfg["credentials_json"])
    api_key = creds["api_key"]
    client_id = creds.get("client_id", "")

    # авторизация в GS под выделенным SA
    sheets = SheetsClient(store_cfg["sa_path"])
    ws = await sheets.get_worksheet(store_cfg["sheet_id"], "unit-day")

    # импортируем скрипт
    mod = import_module("report_scripts.unit_day_5")

    # monkey-patch переменных
    setattr(mod, "TOKEN_OZ", api_key)
    setattr(mod, "CLIENT_ID", client_id)
    setattr(mod, "GS_CRED",  store_cfg["sa_path"])
    setattr(mod, "SPREAD_ID", store_cfg["sheet_id"])

    # запускаем в отдельном потоке, чтобы не блокировать event-loop
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, mod.run)
    log.info("unit_day report done for %s", store_cfg["store_id"])
