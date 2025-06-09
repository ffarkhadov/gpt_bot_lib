from __future__ import annotations

from json import dumps
from uuid import uuid4
import logging

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext

from telegram.states import AddStore
from telegram.keyboards import kb_step, kb_main
from core.services.gs_db import GsDB
from core.services.ozon_api import OzonAPI
from core.services.wb_api import WBAPI

log = logging.getLogger(__name__)
router = Router(name="add_store")


# ─────────────────────────────────────────────────────────────
#  старт регистрации (кнопки «➕ Добавить …»)
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "add_ozon")
@router.callback_query(F.data == "add_wb")
async def add_store_intro(cb: CallbackQuery, state: FSMContext):
    log.info("Got CB: %s", cb.data)     # в журнале увидите коллбэк
    mp = "ozon" if cb.data == "add_ozon" else "wb"
    await state.update_data(mp=mp)

    await cb.message.answer(
        "<b>Подключение магазина</b>\n"
        "Всего 3 шага: ключи → таблица → подтверждение.\n"
        "Нажмите «Далее».",
        reply_markup=kb_step("step1"),   # ← клавиатура!
    )
    await cb.answer()


# ─────────────────────────────────────────────────────────────
# «Далее» → начинаем спрашивать ключи
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "step1")
async def ask_first_key(cb: CallbackQuery, state: FSMContext):
    log.info("Got CB: %s", cb.data)
    data = await state.get_data()
    if data["mp"] == "ozon":
        await cb.message.answer("Введите <b>Client-ID</b> Ozon:")
        await state.set_state(AddStore.client_id)
    else:
        await cb.message.answer("Введите <b>API-Key</b> Wildberries:")
        await state.set_state(AddStore.api_key)
    await cb.answer()


# ─────────────────────────────────────────────────────────────
# Client-ID  →  дальше API-Key
# ─────────────────────────────────────────────────────────────
@router.message(AddStore.client_id)
async def save_client_id(msg: Message, state: FSMContext):
    await state.update_data(client_id=msg.text.strip())
    await msg.answer("Теперь введите <b>API-Key</b> Ozon:")
    await state.set_state(AddStore.api_key)


# ─────────────────────────────────────────────────────────────
# API-Key  →  проверка → sheet_id
# ─────────────────────────────────────────────────────────────
@router.message(AddStore.api_key)
async def save_api_key(msg: Message, state: FSMContext):
    await state.update_data(api_key=msg.text.strip())

    data = await state.get_data()
    ok = False
    if data["mp"] == "ozon":
        ok = await OzonAPI(data["client_id"], data["api_key"]).ping()
    else:
        ok = await WBAPI(data["api_key"]).ping()

    if not ok:
        await msg.answer("❌ Ключи не прошли проверку. Попробуйте ещё раз.")
        return

    await msg.answer(
        "Укажите <b>ID Google-таблицы</b> "
        "(строка между «/d/» и «/edit» в URL):"
    )
    await state.set_state(AddStore.sheet_id)


# ─────────────────────────────────────────────────────────────
# Sheet ID  →  подтверждение
# ─────────────────────────────────────────────────────────────
@router.message(AddStore.sheet_id)
async def save_sheet(msg: Message, state: FSMContext):
    await state.update_data(sheet_id=msg.text.strip())
    await msg.answer(
        "Проверьте данные и напишите «Готово», если всё верно."
    )
    await state.set_state(AddStore.confirm)


# ─────────────────────────────────────────────────────────────
# Финал: записываем магазин, обновляем меню
# ─────────────────────────────────────────────────────────────
@router.message(AddStore.confirm, F.text.lower().in_({"готово", "done"}))
async def finish(msg: Message, state: FSMContext):
    data = await state.get_data()
    db = GsDB()

    store_id = data.get("client_id") or str(uuid4())
    credentials = dumps(
        {"client_id": data.get("client_id"), "api_key": data["api_key"]}
    )
    await db.add_store(
        store_id=store_id,
        owner_id=msg.from_user.id,
        marketplace=data["mp"],
        name=f"{data['mp'].upper()}-{store_id[:6]}",
        credentials_json=credentials,
        sheet_id=data["sheet_id"],
    )

    await msg.answer("✅ Магазин сохранён. Возвращаюсь в меню.")
    await state.clear()

    stores = await db.get_stores_by_owner(msg.from_user.id)
    await msg.answer(
        "Главное меню:",
        reply_markup=kb_main(
            [(s["store_id"], s["name"], s["marketplace"]) for s in stores]
        ),
    )
