import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiohttp import web

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Помилка: BOT_TOKEN не знайдено!")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Хендлер для команди /start
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(
        f"Привіт, {message.from_user.first_name}! 👋\n\n"
        f"Вітаємо у боті знайомств **Нирчик** 🇺🇦!\n"
        f"Скоро тут можна буде створити анкету та шукати нові знайомства. 🚀"
    )

# Простий веб-сервер для Render (щоб не падав безкоштовний Web Service)
async def handle_healthcheck(request):
    return web.Response(text="Bot is alive!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Веб-сервер запущено на порту {port}")

async def main():
    logging.info("Запуск бота та веб-сервера...")
    # Запускаємо веб-сервер та бот паралельно
    await asyncio.gather(
        start_web_server(),
        dp.start_polling(bot)
    )

if __name__ == "__main__":
    asyncio.run(main())
