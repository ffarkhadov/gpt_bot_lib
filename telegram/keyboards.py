from __future__ import annotations
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# ───────────────────── Главное меню ─────────────────────
def kb_main(stores: list[tuple[str, str, str]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(
            text=f"{name} · {mp.upper()}",
            callback_data=f"store_{sid}"
        )] for sid, name, mp in stores
    ]
    rows += [
        [InlineKeyboardButton(text="➕ Добавить Ozon магазин",
                              callback_data="add_ozon")],
        [InlineKeyboardButton(text="➕ Добавить Wildberries магазин",
                              callback_data="add_wb")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ───────────────────── «Далее» ─────────────────────
def kb_step(cb: str, text: str = "➡️ Далее") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=cb)]]
    )


# ─────────────── Сохранить / Отмена ───────────────
def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Сохранить магазин",
                             callback_data="save_store"),
        InlineKeyboardButton(text="❌ Отмена",
                             callback_data="cancel_store"),
    ]])


# ───────────────────── Меню магазина ─────────────────────
def kb_store_menu(sid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✏ Переименовать",
                                 callback_data=f"rename_{sid}"),
            InlineKeyboardButton(text="🗑 Удалить",
                                 callback_data=f"delask_{sid}"),
        ],
        [InlineKeyboardButton(text="📝 Обновить отчёт",
                              callback_data=f"update_{sid}")],
        [InlineKeyboardButton(text="⏹ Остановить обновление",
                              callback_data=f"stop_{sid}")],
    ])


def kb_del_confirm(sid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да, удалить",
                             callback_data=f"delok_{sid}"),
        InlineKeyboardButton(text="❌ Отмена",
                             callback_data=f"dellater_{sid}"),
    ]])
