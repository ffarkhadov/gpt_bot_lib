"""
core/tasks/report_runner.py
---------------------------
Фоновый запуск unit_day_5 с детальным логом и сообщением
пользователю об успехе / ошибке.

store_cfg =
{
  store_id, marketplace,
  credentials_json,   # '{"client_id":"…","api_key":"…"}'
  sheet_id,
  sa_path,
  chat_id,
}
"""
from __future__ import annotations
import asyncio, json, inspect, logging, traceback
from functools import partial
from importlib import import_module

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from config import settings

log = logging.getLogger(__name__)


async def run_report(cfg: dict):
    # ↓ правильный способ задать parse_mode
    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML")
    )
    chat_id = cfg["chat_id"]

    # индикатор «идёт обработка»
    prog = await bot.send_message(chat_id, "⏳ Строю отчёт…")

    try:
        creds = json.loads(cfg["credentials_json"])
        kwargs = dict(
            token_oz=creds["api_key"],
            client_id=creds.get("client_id", ""),
            gs_cred=cfg["sa_path"],
            spread_id=cfg["sheet_id"],
        )

        mod = import_module("report_scripts.unit_day_5")
        func = getattr(mod, "run")

        if inspect.iscoroutinefunction(func):
            await func(**kwargs)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(func, **kwargs))

        await bot.edit_message_text(chat_id, prog.message_id,
                                    "✅ Отчёт unit-day обновлён.")
        log.info("unit-day OK for %s", cfg["store_id"])

    except Exception:
        err = traceback.format_exc()
        await bot.edit_message_text(
            chat_id, prog.message_id,
            f"❌ unit-day ERROR:\n<code>{err.splitlines()[-1]}</code>"
        )
        log.error("unit-day FAIL for %s\n%s", cfg["store_id"], err)
