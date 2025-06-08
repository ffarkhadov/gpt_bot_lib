"""
Заглушка-обёртка: реальный код берём из gpt_bot_lib.
Чтобы проект запустился, обеспечиваем API совместимый метод run().
"""
import asyncio
from random import randint
from typing import Any

async def run(store_cfg: dict[str, Any], worksheet):
    # псевдо-обработка (5-10 сек) → пишем случайное число
    await asyncio.sleep(randint(5, 10))
    worksheet.append_row(
        [f"OK for {store_cfg['marketplace']}", randint(1, 100)],
        value_input_option="USER_ENTERED",
    )
