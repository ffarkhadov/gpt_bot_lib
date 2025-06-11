from __future__ import annotations

import asyncio, logging
from json import dumps
from uuid import uuid4

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext

from telegram.states import AddStore
from telegram.keyboards import kb_step, kb_main, kb_confirm
from core.services.gs_db import GsDB
from telegram.handlers.start import cmd_start

log = logging.getLogger(__name__)
router = Router(name="add_store")


def _mask(s: str) -> str:
    return s[:4] + "•" * max(0, len(s) - 8) + s[-4:]


# ───────────────────── старт ─────────────────────
@router.callback_query(F.data.in_({"add_ozon", "add_wb"}))
async def add_store_intro(cb: CallbackQuery, state: FSMContext):
    mp = "ozon" if cb.data == "add_ozon" else "wb"
    await state.update_data(mp=mp)
    await cb.message.answer(
        "<b>Подключение магазина</b>\n"
        "Шаги: ключи API → таблица → сервисный аккаунт.\n"
        "Нажмите «Далее».",
        reply_markup=kb_step("step1"),
    )
    await cb.answer()


# ───────────────────── ключи ─────────────────────
@router.callback_query(F.data == "step1")
async def ask_first_key(cb: CallbackQuery, state: FSMContext):
    mp = (await state.get_data())["mp"]
    if mp == "ozon":
        await cb.message.answer("Введите <b>Client-ID</b> Ozon (Seller API):")
        await state.set_state(AddStore.client_id)
    else:
        await cb.message.answer("Введите <b>API-Key</b> Wildberries:")
        await state.set_state(AddStore.api_key)
    await cb.answer()


@router.message(AddStore.client_id)
async def save_client_id(msg: Message, state: FSMContext):
    await state.update_data(client_id=msg.text.strip())
    await msg.answer("Теперь введите <b>API-Key</b> Ozon (Seller API):")
    await state.set_state(AddStore.api_key)


@router.message(AddStore.api_key)
async def save_api_key(msg: Message, state: FSMContext):
    await state.update_data(api_key=msg.text.strip())
    data = await state.get_data()

    if data["mp"] == "ozon":
        await msg.answer("Введите <b>Client-ID</b> Performance API:")
        await state.set_state(AddStore.perf_id)
    else:  # WB
        await msg.answer("Укажите <b>ID Google-таблицы</b>:")
        await state.set_state(AddStore.sheet_id)


@router.message(AddStore.perf_id)
async def save_perf_id(msg: Message, state: FSMContext):
    await state.update_data(perf_client_id=msg.text.strip())
    await msg.answer("Введите <b>Client-Secret</b> Performance API:")
    await state.set_state(AddStore.perf_secret)


@router.message(AddStore.perf_secret)
async def save_perf_secret(msg: Message, state: FSMContext):
    await state.update_data(perf_client_secret=msg.text.strip())
    await msg.answer("Укажите <b>ID Google-таблицы</b>:")
    await state.set_state(AddStore.sheet_id)


# ───────────────────── выбор SA, подтверждение ─────────────────────
@router.message(AddStore.sheet_id)
async def confirm_data(msg: Message, state: FSMContext):
    await state.update_data(sheet_id=msg.text.strip())

    db = GsDB()
    sa = await db.pick_service_account()
    await state.update_data(sa_path=sa["path"], sa_email=sa["email"])

    d = await state.get_data()
    txt = (
        "<b>Проверьте введённые данные:</b>\n"
        f"Маркетплейс: <code>{d['mp'].upper()}</code>\n"
    )
    if d["mp"] == "ozon":
        txt += (
            f"Seller Client-ID: <code>{d['client_id']}</code>\n"
            f"Perf Client-ID  : <code>{d['perf_client_id']}</code>\n"
            f"Perf Secret     : <code>{_mask(d['perf_client_secret'])}</code>\n"
        )
    txt += (
        f"API-Key: <code>{_mask(d['api_key'])}</code>\n"
        f"Sheet ID: <code>{d['sheet_id']}</code>\n\n"
        f"<b>Сервисный аккаунт:</b> <code>{d['sa_email']}</code>\n"
        "Добавьте этот email как редактора в таблицу.\n\n"
        "Нажмите «✅ Сохранить магазин»."
    )
    await msg.answer(txt, reply_markup=kb_confirm())
    await state.set_state(AddStore.confirm)


# ─────────────────── отмена ───────────────────
@router.callback_query(F.data == "cancel_store")
async def cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("🚫 Подключение прервано.")
    await cmd_start(cb.message, cb.bot)
    await cb.answer()


# ─────────────────── сохранение ───────────────────
@router.callback_query(F.data == "save_store")
async def save(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    db = GsDB()
    bot: Bot = cb.bot

    await cb.message.edit_reply_markup()
    wait = await cb.message.answer("⏳ Сохраняю магазин…")
    await cb.answer()

    async def _background():
        try:
            store_id = data.get("client_id") or str(uuid4())
            creds_json = {
                "client_id": data.get("client_id"),
                "api_key": data["api_key"],
                "perf_client_id": data.get("perf_client_id", ""),
                "perf_client_secret": data.get("perf_client_secret", "")
            }
            await db.add_store(
                store_id=store_id,
                owner_id=cb.from_user.id,
                marketplace=data["mp"],
                name=f"{data['mp'].upper()}-{store_id[:6]}",
                credentials_json=dumps(creds_json),
                sheet_id=data["sheet_id"],
                sa_path=data["sa_path"],
            )

            stores = await db.get_stores_by_owner(cb.from_user.id)
            await bot.delete_message(wait.chat.id, wait.message_id)
            await bot.send_message(
                cb.from_user.id,
                "✅ Магазин сохранён. Главное меню:",
                reply_markup=kb_main(
                    [(s["store_id"], s["name"], s["marketplace"]) for s in stores]
                ),
            )
        except Exception as e:
            await bot.edit_message_text(
                chat_id=wait.chat.id, message_id=wait.message_id,
                text=f"❌ Не удалось сохранить: {e}"
            )
        finally:
            await state.clear()

    asyncio.create_task(_background())
