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

log = logging.getLogger(__name__)
router = Router(name="add_store")


# ─────────────────────────────────────────────────────────────
#  «➕ Добавить Ozon / WB магазин»
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "add_ozon")
@router.callback_query(F.data == "add_wb")
async def add_store_intro(cb: CallbackQuery, state: FSMContext):
    mp = "ozon" if cb.data == "add_ozon" else "wb"
    await state.update_data(mp=mp)

    await cb.message.answer(
        "<b>Подключение магазина</b>\n"
        "Всего 3 шага: ключи → таблица → подтверждение.\n"
        "Нажмите «Далее».",
        reply_markup=kb_step("step1"),
    )
    await cb.answer()


# ─────────────────────────────────────────────────────────────
# «Далее» → спрашиваем Client-ID (Ozon) или API-Key (WB)
# ─────────────────────────────────────────────────────────────
@router.callback_query(F.data == "step1")
async def ask_first_key(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if data["mp"] == "ozon":
        await cb.message.answer("Введите <b>Client-ID</b> Ozon:")
        await state.set_state(AddStore.client_id)
    else:
        await cb.message.answer("Введите <b>API-Key</b> Wildberries:")
        await state.set_state(AddStore.api_key)
    await cb.answer()


# ─────────────────────────────────────────────────────────────
# Client-ID (Ozon) → дальше API-Key
# ─────────────────────────────────────────────────────────────
@router.message(AddStore.client_id)
async def save_client_id(msg: Message, state: FSMContext):
    await state.update_data(client_id=msg.text.strip())
    await msg.answer("Теперь введите <b>API-Key</b> Ozon:")
    await state.set_state(AddStore.api_key)


# ─────────────────────────────────────────────────────────────
# API-Key получен  →  сразу спрашиваем ID таблицы (без проверки)
# ─────────────────────────────────────────────────────────────
@router.message(AddStore.api_key)
async def save_api_key(msg: Message, state: FSMContext):
    await state.update_data(api_key=msg.text.strip())

    await msg.answer(
        "Укажите <b>ID Google-таблицы</b> "
        "(строка между «/d/» и «/edit» в URL):"
    )
    await state.set_state(AddStore.sheet_id)


# ─────────────────────────────────────────────────────────────
# Sheet ID  →  подтверждение «Готово»
# ─────────────────────────────────────────────────────────────
@router.message(AddStore.sheet_id)
async def save_sheet(msg: Message, state: FSMContext):
    await state.update_data(sheet_id=msg.text.strip())
    await msg.answer("Проверьте данные и напишите «Готово», если всё верно.")
    await state.set_state(AddStore.confirm)


# ─────────────────────────────────────────────────────────────
# Завершаем: пишем в тех-таблицу и возвращаем меню
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

    # Обновлённое меню
    stores = await db.get_stores_by_owner(msg.from_user.id)
    await msg.answer(
        "Главное меню:",
        reply_markup=kb_main(
            [(s["store_id"], s["name"], s["marketplace"]) for s in stores]
        ),
    )
