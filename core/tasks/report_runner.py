"""
core/tasks/report_runner.py
───────────────────────────
Фоновый запуск отчётных скриптов.

cfg =
{
  store_id, marketplace,
  credentials_json,  # '{"client_id":"…","api_key":"…"}'
  sheet_id, sa_path,
  chat_id,
  script: "unit_day_5" | "balans_1" | …
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
    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML")
    )
    chat_id = cfg["chat_id"]
    script  = cfg.get("script", "unit_day_5")       # ← по умолчанию unit_day_5
    nice    = "balans" if "balans" in script else "unit-day"

    prog = await bot.send_message(chat_id, f"⏳ Строю отчёт <b>{nice}</b>…")

    try:
        creds = json.loads(cfg["credentials_json"])
        kwargs = dict(
            token_oz=creds["api_key"],
            client_id=creds.get("client_id", ""),
            gs_cred=cfg["sa_path"],
            spread_id=cfg["sheet_id"],
        )

        mod = import_module(f"report_scripts.{script}")
        func = getattr(mod, "run")

        if inspect.iscoroutinefunction(func):
            await func(**kwargs)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(func, **kwargs))

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=prog.message_id,
            text=f"✅ Отчёт <b>{nice}</b> обновлён."
        )
        log.info("%s OK for %s (%s)", nice, cfg["store_id"], script)

    except Exception:
        err = traceback.format_exc()
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=prog.message_id,
            text=f"❌ <b>{nice}</b> ERROR:\n<code>{err.splitlines()[-1]}</code>"
        )
        log.error("%s FAIL for %s (%s)\n%s",
                  nice, cfg["store_id"], script, err)
    finally:
        await bot.session.close()
