from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# ───────────────────── Главное меню ─────────────────────
def kb_main(stores: list[tuple[str, str, str]]) -> InlineKeyboardMarkup:
    """
    :param stores: [(store_id, name, marketplace)]
    """
    rows: list[list[InlineKeyboardButton]] = []

    # Магазины пользователя
    for sid, n, mp in stores:
        rows.append([
            InlineKeyboardButton(
                text=f"{n} · {mp.upper()}",
                callback_data=f"store_{sid}",
            )
        ])

    # Кнопки добавления
    rows.append([InlineKeyboardButton(
        text="➕ Добавить Ozon магазин", callback_data="add_ozon"
    )])
    rows.append([InlineKeyboardButton(
        text="➕ Добавить Wildberries магазин", callback_data="add_wb"
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ───────────────────── Кнопка «Далее» ─────────────────────
def kb_step(cb: str, text: str = "➡️ Далее") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data=cb)]
        ]
    )


# ──────────────── Кнопки «Сохранить / Отмена» ─────────────
def kb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Сохранить магазин", callback_data="save_store"
                ),
                InlineKeyboardButton(
                    text="❌ Отмена", callback_data="cancel_store"
                ),
            ]
        ]
    )
