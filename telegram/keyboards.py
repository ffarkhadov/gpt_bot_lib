from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# ───────────────────── Главное меню ─────────────────────
def kb_main(stores: list[tuple[str, str, str]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=f"{n} · {mp.upper()}",
                              callback_data=f"store_{sid}")]
        for sid, n, mp in stores
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


# ──────────────── Кнопки «Сохранить / Отмена» ─────────────
def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton("✅ Сохранить магазин", callback_data="save_store"),
                InlineKeyboardButton("❌ Отмена", callback_data="cancel_store"),
            ]
        ]
    )
