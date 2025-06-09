from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup

def kb_main(
    stores: list[tuple[str, str, str]],   # [(store_id, name, marketplace), ...]
    show_next: bool = False,              # Показать кнопку "Далее"?
    next_cb: str = "next"                 # callback для "Далее"
) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    # Кнопки магазинов (если есть)
    for store_id, name, mp in stores:
        kb.button(
            text=f"{name} · {mp.upper()}",
            callback_data=f"store_{store_id}",
        )

    # Кнопки «Добавить …»
    kb.button(text="➕ Добавить Ozon магазин", callback_data="add_ozon")
    kb.button(text="➕ Добавить Wildberries магазин", callback_data="add_wb")

    # Кнопка "Далее"
    if show_next:
        kb.button(text="➡️ Далее", callback_data=next_cb)

    kb.adjust(1)
    return kb.as_markup()
