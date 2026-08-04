import asyncio
import logging
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiohttp import web

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Помилка: BOT_TOKEN не знайдено!")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Сховища у пам'яті (до підключення PostgreSQL)
user_profiles = {}  # user_id -> profile data
likes_queue = {}    # target_user_id -> list of user_ids who liked them
seen_profiles = {}  # user_id -> set of viewed user_ids

# --- СТАНИ РЕЄСТРАЦІЇ ТА ЛАЙКІВ ---
class ProfileRegistration(StatesGroup):
    name = State()
    age = State()
    gender = State()
    target_gender = State()
    city = State()
    bio = State()
    photo = State()

class FeedState(StatesGroup):
    viewing = State()

# --- КЛАВІАТУРИ ---
def gender_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Хлопець 👨"), KeyboardButton(text="Дівчина 👩")]],
        resize_keyboard=True, one_time_keyboard=True
    )

def target_gender_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Дівчат 👩"), KeyboardButton(text="Хлопців 👨")],
            [KeyboardButton(text="Усіх 🌈")]
        ],
        resize_keyboard=True, one_time_keyboard=True
    )

def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 Дивитися анкети")],
            [KeyboardButton(text="👤 Моя анкета")]
        ],
        resize_keyboard=True
    )

def feed_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❤️"), KeyboardButton(text="👎"), KeyboardButton(text="🛑")],
            [KeyboardButton(text="🏠 Головне меню")]
        ],
        resize_keyboard=True
    )

# --- ДОПОМІЖНІ ФУНКЦІЇ ---
def get_next_profile(current_user_id):
    """Шукає наступну анкету для показу"""
    if current_user_id not in seen_profiles:
        seen_profiles[current_user_id] = set()
    
    for uid, profile in user_profiles.items():
        if uid != current_user_id and uid not in seen_profiles[current_user_id]:
            return uid, profile
    return None, None

async def show_profile(message: types.Message, target_uid, profile):
    caption = (
        f"📌 **{profile['name']}**, {profile['age']}, {profile['city']}\n"
        f"📝 {profile['bio']}"
    )
    await message.answer_photo(
        photo=profile['photo'],
        caption=caption,
        parse_mode="Markdown",
        reply_markup=feed_keyboard()
    )

# --- ХЕНДЛЕРИ СТАРТУ ТА РЕЄСТРАЦІЇ ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if user_id in user_profiles:
        await message.answer("З поверненням!", reply_markup=main_menu_keyboard())
    else:
        await message.answer(
            f"Привіт, {message.from_user.first_name}! 👋\n"
            f"Вітаємо у **Нирчик UA** 🇺🇦!\n\nДавай створимо твою анкету. Як тебе звати?"
        )
        await state.set_state(ProfileRegistration.name)

@dp.message(ProfileRegistration.name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text, username=message.from_user.username)
    await message.answer("Скільки тобі років?")
    await state.set_state(ProfileRegistration.age)

@dp.message(ProfileRegistration.age)
async def process_age(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or not (12 <= int(message.text) <= 99):
        await message.answer("Будь ласка, вкажи реальний вік числом (наприклад, 19):")
        return
    await state.update_data(age=int(message.text))
    await message.answer("Вкажи свою стать:", reply_markup=gender_keyboard())
    await state.set_state(ProfileRegistration.gender)

@dp.message(ProfileRegistration.gender)
async def process_gender(message: types.Message, state: FSMContext):
    if message.text not in ["Хлопець 👨", "Дівчина 👩"]:
        await message.answer("Обери варіант з кнопок нижче:", reply_markup=gender_keyboard())
        return
    await state.update_data(gender=message.text)
    await message.answer("Кого ти шукаєш?", reply_markup=target_gender_keyboard())
    await state.set_state(ProfileRegistration.target_gender)

@dp.message(ProfileRegistration.target_gender)
async def process_target_gender(message: types.Message, state: FSMContext):
    await state.update_data(target_gender=message.text)
    await message.answer("З якого ти міста?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(ProfileRegistration.city)

@dp.message(ProfileRegistration.city)
async def process_city(message: types.Message, state: FSMContext):
    await state.update_data(city=message.text)
    await message.answer("Напиши короткий опис про себе:")
    await state.set_state(ProfileRegistration.bio)

@dp.message(ProfileRegistration.bio)
async def process_bio(message: types.Message, state: FSMContext):
    await state.update_data(bio=message.text)
    await message.answer("Надішли своє фото 📸:")
    await state.set_state(ProfileRegistration.photo)

@dp.message(ProfileRegistration.photo, F.photo)
async def process_photo(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    data['photo'] = photo_id
    
    user_profiles[message.from_user.id] = data
    await state.clear()
    
    caption = (
        f"🎉 **Анкету створено!**\n\n"
        f"📌 **{data['name']}**, {data['age']}, {data['city']}\n"
        f"📝 {data['bio']}"
    )
    await message.answer_photo(photo=photo_id, caption=caption, parse_mode="Markdown", reply_markup=main_menu_keyboard())

# --- ГОРТАННЯ АНКЕТ (ФІД) ---

@dp.message(F.text == "👤 Моя анкета")
async def show_my_profile(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_profiles:
        p = user_profiles[user_id]
        caption = f"Твоя анкета:\n\n📌 **{p['name']}**, {p['age']}, {p['city']}\n📝 {p['bio']}"
        await message.answer_photo(photo=p['photo'], caption=caption, parse_mode="Markdown", reply_markup=main_menu_keyboard())

@dp.message(F.text == "🚀 Дивитися анкети")
async def start_feed(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Перевірка чи є лайки від когось
    if user_id in likes_queue and likes_queue[user_id]:
        liker_id = likes_queue[user_id].pop(0)
        liker_profile = user_profiles.get(liker_id)
        if liker_profile:
            await state.update_data(current_target=liker_id, is_like_mode=True)
            await message.answer("Комусь сподобалась твоя анкета! 🚀", reply_markup=feed_keyboard())
            await show_profile(message, liker_id, liker_profile)
            await state.set_state(FeedState.viewing)
            return

    target_uid, profile = get_next_profile(user_id)
    if not profile:
        await message.answer("Поки що немає нових анкет у твоєму місті/фільтрах. Завітай пізніше! 😉", reply_markup=main_menu_keyboard())
        return

    await state.update_data(current_target=target_uid, is_like_mode=False)
    await show_profile(message, target_uid, profile)
    await state.set_state(FeedState.viewing)

@dp.message(FeedState.viewing, F.text.in_(["❤️", "👎", "🛑"]))
async def process_reaction(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    target_uid = data.get("current_target")
    is_like_mode = data.get("is_like_mode", False)
    
    if target_uid:
        seen_profiles.setdefault(user_id, set()).add(target_uid)

    reaction = message.text

    if reaction == "❤️":
        if is_like_mode:
            # ВЗАЄМНИЙ МЕТЧ!
            my_prof = user_profiles.get(user_id)
            target_prof = user_profiles.get(target_uid)
            
            my_link = f"@{my_prof.get('username')}" if my_prof.get('username') else f"[Користувач](tg://user?id={user_id})"
            target_link = f"@{target_prof.get('username')}" if target_prof.get('username') else f"[Користувач](tg://user?id={target_uid})"

            await message.answer(f"🎉 **Це МЕТЧ!**\nТи сподобався(лась) {target_prof['name']}!\nНапиши першим(ою): {target_link}", parse_mode="Markdown")
            try:
                await bot.send_message(target_uid, f"🎉 **Це МЕТЧ!**\nТобі відповіли взаємністю! Напиши: {my_link}", parse_mode="Markdown")
            except Exception:
                pass
        else:
            # Звичайний лайк
            likes_queue.setdefault(target_uid, []).append(user_id)
            try:
                await bot.send_message(target_uid, "Твоєю анкетою хтось зацікавився! Натисни «🚀 Дивитися анкети», щоб дізнатися хто. 😉")
            except Exception:
                pass

    # Переходимо до наступної анкети
    await start_feed(message, state)

@dp.message(FeedState.viewing, F.text == "🏠 Головне меню")
async def exit_feed(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Повертаємось у головне меню.", reply_markup=main_menu_keyboard())

# --- ВЕБ-СЕРВЕР ---
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
