import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart

# Завантажуємо змінні з .env
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Помилка: BOT_TOKEN не знайдено в файлі .env!")

# Налаштовуємо логування
logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Обробник команды /start
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        f"Привіт, {message.from_user.first_name}! 👋\n\n"
        f"Вітаємо у боті знайомств **Нирчик** 🇺🇦!\n"
        f"Скоро тут можна буде створити анкету та шукати нові знайомства. 🚀"
    )

async def main():
    logging.info("Запуск бота...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
