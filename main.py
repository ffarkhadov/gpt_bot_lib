import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

TOKEN = "7570856507:AAHAZX7bm7zk8otWg50ad9EHxbYA1QEOV68"

async def main():
    bot = Bot(
        token=TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    @dp.message()
    async def echo_handler(message: types.Message):
        await message.answer(f"Вы сказали: {message.text}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

