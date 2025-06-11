from __future__ import annotations
import asyncio, json, inspect, logging, time, traceback
from functools import partial
from importlib import import_module
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from config import settings

log = logging.getLogger(__name__)


async def run_report(cfg: dict):
    bot: Bot = cfg["bot"]
    chat_id  = cfg["chat_id"]
    script   = cfg["script"]
    nice     = cfg.get("human", script)
    step     = cfg.get("step", "?")

    header = f"⏳ Шаг {step} <b>{nice}</b>…"
    if "p_campain_fin" in script:
        header += " (до 1 ч)"
    msg = await bot.send_message(chat_id, header)

    try:
        creds = json.loads(cfg["credentials_json"])
        raw_kwargs = dict(
            token_oz           = creds.get("api_key", ""),
            client_id          = creds.get("client_id", ""),
            perf_client_id     = creds.get("perf_client_id", ""),
            perf_client_secret = creds.get("perf_client_secret", ""),
            gs_cred            = cfg["sa_path"],
            spread_id          = cfg["sheet_id"],
        )

        mod  = import_module(f"report_scripts.{script}")
        func = getattr(mod, "run")
        sig  = inspect.signature(func)
        kwargs = {k: v for k, v in raw_kwargs.items()
                  if k in sig.parameters}

        t0 = time.time()
        if inspect.iscoroutinefunction(func):
            await func(**kwargs)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, partial(func, **kwargs))
        m, s = divmod(round(time.time() - t0), 60)

        await bot.edit_message_text(
            text=f"✅ {nice} готов ({m} м {s} с).",
            chat_id=chat_id,
            message_id=msg.message_id
        )
        log.info("%s OK for %s", script, cfg["store_id"])

    except Exception:
        err = traceback.format_exc()
        await bot.edit_message_text(
            text=f"❌ {nice} ERROR:\n<code>{err.splitlines()[-1]}</code>",
            chat_id=chat_id,
            message_id=msg.message_id
        )
        log.error("%s FAIL for %s\n%s", script, cfg["store_id"], err)

    finally:
        ev = cfg.get("step_event")
        if isinstance(ev, asyncio.Event):
            ev.set()
