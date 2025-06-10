"""
Запускает отчётные скрипты, передавая им ключи магазина.

store_cfg =
{
  "store_id": ...,
  "marketplace": "ozon" | "wb",
  "credentials_json": '{"client_id":"…","api_key":"…"}',
  "sheet_id": "1AbC…",
  "sa_path":  "/path/to/sa.json"
}
"""
from __future__ import annotations
import json, asyncio, inspect, logging
from importlib import import_module
from functools import partial

log = logging.getLogger(__name__)


async def run_report(store_cfg: dict):
    creds = json.loads(store_cfg["credentials_json"])
    kwargs = dict(
        token_oz=creds["api_key"],
        client_id=creds.get("client_id", ""),
        gs_cred=store_cfg["sa_path"],
        spread_id=store_cfg["sheet_id"],
    )

    mod = import_module("report_scripts.unit_day_5")
    func = getattr(mod, "run", None)
    if func is None:
        log.error("unit_day_5.run not found")
        return

    # если функция асинхронная — просто await;
    # если синхронная — отправляем в пул потоков
    if inspect.iscoroutinefunction(func):
        await func(**kwargs)
    else:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, partial(func, **kwargs))

    log.info("unit-day report done for %s", store_cfg["store_id"])
