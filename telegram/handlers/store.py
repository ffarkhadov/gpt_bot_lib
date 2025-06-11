from __future__ import annotations
import asyncio, logging
from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from telegram.keyboards import kb_store_menu, kb_del_confirm, kb_main
from core.services.gs_db import GsDB
from core.tasks.store_queue import get_worker, _workers   # очередь

log = logging.getLogger(__name__)
router = Router(name="store")


# ───────────── FSM переименование ─────────────
class Rename(StatesGroup):
    waiting_name = State()


# ───── открыть меню ─────
@router.callback_query(F.data.startswith("store_"))
async def open_store(cb: CallbackQuery):
    sid = cb.data.removeprefix("store_")
    db = GsDB()
    stores = await db.get_stores_by_owner(cb.from_user.id)
    if sid not in [s["store_id"] for s in stores]:
        await cb.answer("Магазин не найден", show_alert=True); return
    await cb.message.edit_text(
        f"<b>Меню магазина</b> <code>{sid}</code>",
        reply_markup=kb_store_menu(sid))
    await cb.answer()


# ───── переименование ─────
@router.callback_query(F.data.startswith("rename_"))
async def rename_ask(cb: CallbackQuery, state: FSMContext):
    sid = cb.data.removeprefix("rename_")
    await state.update_data(rename_sid=sid, prev_mid=cb.message.message_id)
    await cb.message.answer("Введите новое название магазина:")
    await state.set_state(Rename.waiting_name)
    await cb.answer()


@router.message(Rename.waiting_name)
async def rename_save(msg: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    sid, prev_mid = data["rename_sid"], data["prev_mid"]
    db = GsDB()
    ws = await db._ws("Stores")
    rows = (await db.sheets.read_all(ws))[1:]
    for idx, r in enumerate(rows, start=2):
        if r[0] == sid:
            ws.update(f"D{idx}", [[msg.text.strip()]])
            break
    await bot.delete_message(msg.chat.id, msg.message_id)
    await bot.edit_message_text(msg.chat.id, prev_mid,
                                "✅ Название обновлено.")
    await state.clear()


# ───── удаление ─────
@router.callback_query(F.data.startswith("delask_"))
async def delete_ask(cb: CallbackQuery):
    sid = cb.data.removeprefix("delask_")
    await cb.message.edit_reply_markup(reply_markup=kb_del_confirm(sid))
    await cb.answer()


@router.callback_query(F.data.startswith("dellater_"))
async def delete_cancel(cb: CallbackQuery):
    sid = cb.data.removeprefix("dellater_")
    await cb.message.edit_text("Удаление отменено.",
                               reply_markup=kb_store_menu(sid))
    await cb.answer()


@router.callback_query(F.data.startswith("delok_"))
async def delete_ok(cb: CallbackQuery, bot: Bot):
    sid = cb.data.removeprefix("delok_")
    db = GsDB()
    ws = await db._ws("Stores")
    rows = (await db.sheets.read_all(ws))[1:]
    for idx, r in enumerate(rows, start=2):
        if r[0] == sid:
            ws.delete_rows(idx); break
    await cb.message.edit_text("🗑 Магазин удалён.")
    stores = await db.get_stores_by_owner(cb.from_user.id)
    await cb.message.answer("Главное меню:",
        reply_markup=kb_main([(s["store_id"], s["name"], s["marketplace"])
                              for s in stores]))
    await cb.answer()


# ───── запуск цепочки (unit→ads) ─────
@router.callback_query(F.data.startswith("update_"))
async def update_chain(cb: CallbackQuery):
    sid = cb.data.removeprefix("update_")
    db = GsDB()
    row = next(r for r in (await db.sheets.read_all(await db._ws("Stores")))[1:]
               if r[0] == sid)

    base_cfg = {
        "store_id": sid,
        "credentials_json": row[4],
        "sheet_id": row[5],
        "sa_path": row[6],
        "chat_id": cb.from_user.id,
        "bot": cb.bot,
    }
    worker = await get_worker(sid, base_cfg)
    await worker.enqueue_chain(manual=True)
    await cb.answer()


# ───── остановить цепочку ─────
@router.callback_query(F.data.startswith("stop_"))
async def stop_chain(cb: CallbackQuery):
    sid = cb.data.removeprefix("stop_")
    w = _workers.get(sid)
    if not w:
        await cb.answer("Нет активного обновления"); return
    w.cancel.set(); w.queue = asyncio.Queue()
    await cb.answer("⏹ Остановлено.")
    await cb.message.answer("🛑 Обновление прервано.")
