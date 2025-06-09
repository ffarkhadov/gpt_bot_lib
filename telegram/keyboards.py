from __future__ import annotations
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# ───────────────────── Главное меню ─────────────────────
def kb_main(stores: list[tuple[str, str, str]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(
            text=f"{name} · {mp.upper()}",
            callback_data=f"store_{sid}"
        )]
        for sid, name, mp in stores
    ]
    rows += [
        [InlineKeyboardButton("➕ Добавить Ozon магазин", callback_data="add_ozon")],
        [InlineKeyboardButton("➕ Добавить Wildberries магазин", callback_data="add_wb")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ───────────────────── Кнопка «Далее» ─────────────────────
def kb_step(cb: str, text: str = "➡️ Далее") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text, callback_data=cb)]]
    )


# ───────────────────── Подтвердить / Отмена ─────────────────────
def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton("✅ Сохранить магазин", callback_data="save_store"),
            InlineKeyboardButton("❌ Отмена", callback_data="cancel_store"),
        ]]
    )


# ───────────────────── Меню магазина ─────────────────────
def kb_store_menu(sid: str) -> InlineKeyboardMarkup:
    """
    sid — store_id
    """
    rows = [
        [
            InlineKeyboardButton("✏ Переименовать", callback_data=f"rename_{sid}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"delask_{sid}"),
        ],
        [InlineKeyboardButton("📝 Обновить отчёт (unit-day)", callback_data=f"unit_{sid}")],
        # добавляйте новые скрипты по аналогии:
        # [InlineKeyboardButton("📦 Скрипт X", callback_data=f"x_{sid}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_del_confirm(sid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"delok_{sid}"),
            InlineKeyboardButton("❌ Отмена",   callback_data=f"dellater_{sid}"),
        ]]
    )
