from __future__ import annotations

import logging
from json import dumps
from uuid import uuid4

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext

from telegram.states import AddStore
from telegram.keyboards import kb_step, kb_main, kb_confirm
from core.services.gs_db import GsDB

log = logging.getLogger(__name__)
router = Router(name="add_store")


def _mask(token: str) -> str:
    """102e••••••6ce11d — показываем первые/последние 4 символа."""
    return token[:4] + "•" * max(0, len(token) - 8) + token[-4:]


# ───────────────────────── Старт ─────────────────────────
@router.callback_query(F.data.in_({"add_ozon", "add_wb"}))
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


# ─────────────────── Шаг 1: ключи ───────────────────
@router.callback_query(F.data == "step1")
async def ask_first_key(cb: CallbackQuery, state: FSMContext):
    mp = (await state.get_data())["mp"]
    if mp == "ozon":
        await cb.message.answer("Введите <b>Client-ID</b> Ozon:")
        await state.set_state(AddStore.client_id)
    else:
        await cb.message.answer("Введите <b>API-Key</b> Wildberries:")
        await state.set_state(AddStore.api_key)
    await cb.answer()


@router.message(AddStore.client_id)
async def save_client(msg: Message, state: FSMContext):
    await state.update_data(client_id=msg.text.strip())
    await msg.answer("Теперь введите <b>API-Key</b> Ozon:")
    await state.set_state(AddStore.api_key)


@router.message(AddStore.api_key)
async def save_api(msg: Message, state: FSMContext):
    await state.update_data(api_key=msg.text.strip())
    await msg.answer(
        "Укажите <b>ID Google-таблицы</b> "
        "(строка между «/d/» и «/edit» в URL):"
    )
    await state.set_state(AddStore.sheet_id)


# ──────────────── Шаг 2: Sheet ID → подтверждение ────────────────
@router.message(AddStore.sheet_id)
async def confirm_data(msg: Message, state: FSMContext):
    await state.update_data(sheet_id=msg.text.strip())
    d = await state.get_data()

    text = (
        "<b>Проверьте введённые данные:</b>\n"
        f"Маркетплейс: <code>{d['mp'].upper()}</code>\n"
    )
    if d["mp"] == "ozon":
        text += f"Client-ID: <code>{d['client_id']}</code>\n"
    text += (
        f"API-Key: <code>{_mask(d['api_key'])}</code>\n"
        f"Sheet ID: <code>{d['sheet_id']}</code>\n\n"
        "Нажмите «✅ Сохранить магазин» или «❌ Отмена»."
    )

    await msg.answer(text, reply_markup=kb_confirm())
    await state.set_state(AddStore.confirm)


# ──────────── Кнопка «Отмена» ────────────
@router.callback_query(F.data == "cancel_store")
async def cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("🚫 Подключение прервано.")
    await cb.answer()


# ──────────── Кнопка «Сохранить магазин» ────────────
@router.callback_query(F.data == "save_store")
async def save(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    db = GsDB()

    store_id = data.get("client_id") or str(uuid4())
    creds_json = dumps(
        {"client_id": data.get("client_id"), "api_key": data["api_key"]}
    )
    await db.add_store(
        store_id=store_id,
        owner_id=cb.from_user.id,
        marketplace=data["mp"],
        name=f"{data['mp'].upper()}-{store_id[:6]}",
        credentials_json=creds_json,
        sheet_id=data["sheet_id"],
    )

    await cb.message.edit_text("✅ Магазин сохранён. Возвращаюсь в меню.")
    await state.clear()

    # обновляем меню
    stores = await db.get_stores_by_owner(cb.from_user.id)
    await cb.message.answer(
        "Главное меню:",
        reply_markup=kb_main(
            [(s["store_id"], s["name"], s["marketplace"]) for s in stores]
        ),
    )
    await cb.answer()
