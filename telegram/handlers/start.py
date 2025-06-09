from __future__ import annotations

from aiogram import Router, F
from aiogram.types import Message
from aiogram import Bot

from telegram.keyboards import kb_main
from core.services.gs_db import GsDB

router = Router(name="start")


@router.message(F.text.in_({"/start", "🏠 Меню"}))
async def cmd_start(msg: Message, bot: Bot):
    # мгновенный индикатор
    wait_msg = await msg.answer("⏳ Проверяем данные, секунду…")

    db = GsDB()
    await db.ensure_user(msg.from_user.id, msg.from_user.username,
                         msg.from_user.full_name)
    stores = await db.get_stores_by_owner(msg.from_user.id)

    await bot.delete_message(chat_id=wait_msg.chat.id, message_id=wait_msg.message_id)

    await msg.answer(
        "Привет! Я бот-отчётник для Ozon/WB.\nВыберите действие:",
        reply_markup=kb_main(
            [(s["store_id"], s["name"], s["marketplace"]) for s in stores]
        ),
    )
