import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiohttp import web

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Помилка: BOT_TOKEN не знайдено!")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# тимчасове сховище анкет у пам'яті (до підключення PostgreSQL)
user_profiles = {}

# --- СТАНИ РЕЄСТРАЦІЇ (FSM) ---
class ProfileRegistration(StatesGroup):
    name = State()
    age = State()
    gender = State()
    target_gender = State()
    city = State()
    bio = State()
    photo = State()

# --- КЛАВІАТУРИ ---
def gender_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Хлопець 👨"), KeyboardButton(text="Дівчина 👩")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def target_gender_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Дівчат 👩"), KeyboardButton(text="Хлопців 👨")],
            [KeyboardButton(text="Усіх 🌈")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# --- ХЕНДЛЕРИ ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id in user_profiles:
        profile = user_profiles[user_id]
        caption = (
            f"Твоя анкета:\n\n"
            f"📌 **{profile['name']}**, {profile['age']}, {profile['city']}\n"
            f"📝 {profile['bio']}"
        )
        await message.answer_photo(
            photo=profile['photo'],
            caption=caption,
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            f"Привіт, {message.from_user.first_name}! 👋\n"
            f"Ласкаво просимо до **Нирчик UA** 🇺🇦!\n\n"
            f"Давай створимо твою анкету. Як тебе звати?"
        )
        await state.set_state(ProfileRegistration.name)

# 1. Ім'я
@dp.message(ProfileRegistration.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("Скільки тобі років?")
    await state.set_state(ProfileRegistration.age)

# 2. Вік
@dp.message(ProfileRegistration.age)
async def process_age(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or not (12 <= int(message.text) <= 99):
        await message.answer("Будь ласка, вкажи реальний вік числом (наприклад, 19):")
        return
    await state.update_data(age=int(message.text))
    await message.answer("Вкажи свою стать:", reply_markup=gender_keyboard())
    await state.set_state(ProfileRegistration.gender)

# 3. Стать
@dp.message(ProfileRegistration.gender)
async def process_gender(message: types.Message, state: FSMContext):
    if message.text not in ["Хлопець 👨", "Дівчина 👩"]:
        await message.answer("Будь ласка, обриваючи варіант з кнопок нижче:", reply_markup=gender_keyboard())
        return
    await state.update_data(gender=message.text)
    await message.answer("Кого ти шукаєш?", reply_markup=target_gender_keyboard())
    await state.set_state(ProfileRegistration.target_gender)

# 4. Кого шукає
@dp.message(ProfileRegistration.target_gender)
async def process_target_gender(message: types.Message, state: FSMContext):
    await state.update_data(target_gender=message.text)
    await message.answer("З якого ти міста?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(ProfileRegistration.city)

# 5. Місто
@dp.message(ProfileRegistration.city)
async def process_city(message: types.Message, state: FSMContext):
    await state.update_data(city=message.text)
    await message.answer("Напиши короткий опис про себе (хто ти, чим захоплюєшся, кого шукаєш):")
    await state.set_state(ProfileRegistration.bio)

# 6. Опис
@dp.message(ProfileRegistration.bio)
async def process_bio(message: types.Message, state: FSMContext):
    await state.update_data(bio=message.text)
    await message.answer("Надішли своє фото для анкети 📸:")
    await state.set_state(ProfileRegistration.photo)

# 7. Фото та фініш
@dp.message(ProfileRegistration.photo, F.photo)
async def process_photo(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    data['photo'] = photo_id
    
    # Зберігаємо в пам'ять
    user_profiles[message.from_user.id] = data
    await state.clear()
    
    caption = (
        f"🎉 **Анкету успішно створено!**\n\n"
        f"📌 **{data['name']}**, {data['age']}, {data['city']}\n"
        f"📝 {data['bio']}"
    )
    await message.answer_photo(photo=photo_id, caption=caption, parse_mode="Markdown")

@dp.message(ProfileRegistration.photo)
async def process_photo_invalid(message: types.Message):
    await message.answer("Будь ласка, надішли саме **світлину** (не файл і не текст):")

# --- ВЕБ-СЕРВЕР ДЛЯ RENDER ---
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

async def main():
    await asyncio.gather(
        start_web_server(),
        dp.start_polling(bot)
    )

if __name__ == "__main__":
    asyncio.run(main())
