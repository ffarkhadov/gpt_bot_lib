"""
Фоновый запуск отчёта unit_day_5.

store_cfg =
{
  "store_id": ...,
  "marketplace": ...,
  "credentials_json": '{"client_id":"…","api_key":"…"}',
  "sheet_id": "1AbC...",
  "sa_path":  "/path/sa.json",
  "chat_id": <int>,               # кому отвечаем
  "menu_message_id": <int | None> # источник кнопки 📝
}
"""
from __future__ import annotations
import asyncio, json, inspect, logging
from functools import partial
from importlib import import_module

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from config import settings

log = logging.getLogger(__name__)


async def run_report(store_cfg: dict):
    # создаём Bot локально, чтобы контекст всегда был
    bot = Bot(settings.BOT_TOKEN, parse_mode="HTML")

    chat_id = store_cfg["chat_id"]
    menu_mid = store_cfg.get("menu_message_id")

    # всплывающий индикатор
    prog_msg_id: int | None = None
    try:
        prog = await bot.send_message(chat_id, "⏳ Строю отчёт, подождите…")
        prog_msg_id = prog.message_id
    except TelegramBadRequest:
        pass

    # подготовка аргументов
    creds = json.loads(store_cfg["credentials_json"])
    kwargs = dict(
        token_oz=creds["api_key"],
        client_id=creds.get("client_id", ""),
        gs_cred=store_cfg["sa_path"],
        spread_id=store_cfg["sheet_id"],
    )

    try:
        # импорт и запуск
        mod = import_module("report_scripts.unit_day_5")
        func = getattr(mod, "run")

        if inspect.iscoroutinefunction(func):
            await func(**kwargs)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(func, **kwargs))

        # успех
        if prog_msg_id:
            await bot.delete_message(chat_id, prog_msg_id)
        await bot.send_message(chat_id, "✅ Отчёт unit-day обновлён.")
        log.info("unit-day OK for store %s", store_cfg["store_id"])

    except Exception as e:
        # ошибка
        if prog_msg_id:
            await bot.delete_message(chat_id, prog_msg_id)
        await bot.send_message(chat_id, f"❌ Отчёт не построен:\n<code>{e}</code>")
        log.exception("unit-day FAIL for store %s", store_cfg["store_id"])
