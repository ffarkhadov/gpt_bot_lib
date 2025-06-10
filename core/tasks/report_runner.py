"""
Запускает отчёт unit_day_5 и уведомляет пользователя
об успехе или ошибке.

store_cfg =
{
  store_id, marketplace, credentials_json, sheet_id, sa_path,
  chat_id,           # кому слать результат
  menu_message_id    # id сообщения, где ждёт кнопка 📝
}
"""
from __future__ import annotations
import json, asyncio, inspect, logging
from importlib import import_module
from functools import partial
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

log = logging.getLogger(__name__)


async def run_report(store_cfg: dict):
    bot: Bot = Bot.get_current()

    creds = json.loads(store_cfg["credentials_json"])
    kwargs = dict(
        token_oz=creds["api_key"],
        client_id=creds.get("client_id", ""),
        gs_cred=store_cfg["sa_path"],
        spread_id=store_cfg["sheet_id"],
    )

    chat_id = store_cfg["chat_id"]
    menu_mid = store_cfg.get("menu_message_id")  # может быть None

    # заменяем подпись кнопки на «⏳…»
    try:
        if menu_mid:
            await bot.edit_message_reply_markup(
                chat_id, menu_mid,
                reply_markup=None
            )
            await bot.send_message(chat_id, "⏳ Строю отчёт, подождите…")
    except TelegramBadRequest:
        pass  # сообщение уже изменено или удалено

    try:
        mod = import_module("report_scripts.unit_day_5")
        func = getattr(mod, "run")

        if inspect.iscoroutinefunction(func):
            await func(**kwargs)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(func, **kwargs))

        await bot.send_message(
            chat_id,
            "✅ Отчёт unit-day обновлён.",
        )
        log.info("unit-day OK for store %s", store_cfg["store_id"])

    except Exception as e:
        await bot.send_message(
            chat_id,
            f"❌ Отчёт не построен: {e}",
        )
        log.exception("unit-day FAIL for store %s", store_cfg["store_id"])
