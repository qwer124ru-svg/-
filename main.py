import asyncio
import logging
import os
import psycopg2
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
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
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise ValueError("Помилка: BOT_TOKEN не знайдено!")
if not DATABASE_URL:
    raise ValueError("Помилка: DATABASE_URL не знайдено!")

ADMIN_ID = 5512316636

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- РОБОТА З БАЗОЮ ДАНИХ POSTGRESQL (SUPABASE) ---

def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS profiles (
            user_id BIGINT PRIMARY KEY,
            name TEXT,
            age INTEGER,
            gender TEXT,
            target_gender TEXT,
            target_age_min INTEGER DEFAULT 12,
            target_age_max INTEGER DEFAULT 99,
            city TEXT,
            bio TEXT,
            photo TEXT,
            username TEXT,
            active INTEGER DEFAULT 1
        )
    ''')
    # Додаємо міграцію на випадок, якщо таблиця вже існувала без цих колонок
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS target_age_min INTEGER DEFAULT 12;')
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS target_age_max INTEGER DEFAULT 99;')
    conn.commit()
    cursor.close()
    conn.close()

init_db()

def db_save_profile(user_id, data):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO profiles (user_id, name, age, gender, target_gender, target_age_min, target_age_max, city, bio, photo, username, active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            target_gender = EXCLUDED.target_gender,
            target_age_min = EXCLUDED.target_age_min,
            target_age_max = EXCLUDED.target_age_max,
            city = EXCLUDED.city,
            bio = EXCLUDED.bio,
            photo = EXCLUDED.photo,
            username = EXCLUDED.username,
            active = EXCLUDED.active
    ''', (
        user_id,
        data.get('name'),
        data.get('age'),
        data.get('gender'),
        data.get('target_gender'),
        data.get('target_age_min', 12),
        data.get('target_age_max', 99),
        data.get('city'),
        data.get('bio'),
        data.get('photo'),
        data.get('username'),
        1 if data.get('active', True) else 0
    ))
    conn.commit()
    cursor.close()
    conn.close()

def db_get_profile(user_id):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('SELECT user_id, name, age, gender, target_gender, target_age_min, target_age_max, city, bio, photo, username, active FROM profiles WHERE user_id = %s', (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row:
        return {
            'user_id': row[0],
            'name': row[1],
            'age': row[2],
            'gender': row[3],
            'target_gender': row[4],
            'target_age_min': row[5],
            'target_age_max': row[6],
            'city': row[7],
            'bio': row[8],
            'photo': row[9],
            'username': row[10],
            'active': bool(row[11])
        }
    return None

def db_get_next_profile(current_user_id, seen_set, target_city=None):
    current_profile = db_get_profile(current_user_id)
    if not current_profile:
        return None, None
        
    min_age = current_profile.get('target_age_min', 12)
    max_age = current_profile.get('target_age_max', 99)
    target_gender = current_profile.get('target_gender', 'Усіх 🌈')

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    query = 'SELECT user_id, name, age, gender, target_gender, target_age_min, target_age_max, city, bio, photo, username, active FROM profiles WHERE user_id != %s AND active = 1 AND age BETWEEN %s AND %s'
    params = [current_user_id, min_age, max_age]
    
    # Фільтрація за статтю, яку шукає користувач
    if target_gender == "Дівчат 👩":
        query += " AND gender = 'Дівчина 👩'"
    elif target_gender == "Хлопців 👨":
        query += " AND gender = 'Хлопець 👨'"
    
    if target_city:
        query += ' AND LOWER(city) = LOWER(%s)'
        params.append(target_city)
        
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    for row in rows:
        uid = row[0]
        if uid not in seen_set:
            return uid, {
                'user_id': row[0],
                'name': row[1],
                'age': row[2],
                'gender': row[3],
                'target_gender': row[4],
                'target_age_min': row[5],
                'target_age_max': row[6],
                'city': row[7],
                'bio': row[8],
                'photo': row[9],
                'username': row[10],
                'active': bool(row[11])
            }
    return None, None

def db_get_profiles_count():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM profiles')
    total_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM profiles WHERE active = 1')
    active_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return total_count, active_count

# Тимчасова оперативка
likes_queue = {}
seen_profiles = {}
search_filters = {}

# --- СТАНИ FSM ---
class ProfileRegistration(StatesGroup):
    name = State()
    age = State()
    gender = State()
    target_gender = State()
    city = State()
    bio = State()
    photo = State()

class EditProfileState(StatesGroup):
    new_name = State()
    new_age = State()
    new_city = State()
    new_bio = State()
    new_photo = State()

class SearchFilterState(StatesGroup):
    filter_city = State()

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
            [KeyboardButton(text="🚀 Дивитися анкети"), KeyboardButton(text="🔍 Пошук")],
            [KeyboardButton(text="👤 Моя анкета"), KeyboardButton(text="⚙️ Налаштування")],
            [KeyboardButton(text="❓ Допомога")]
        ],
        resize_keyboard=True
    )

def my_profile_keyboard(is_active: bool):
    toggle_text = "❌ Приховати анкету" if is_active else "✅ Увімкнути анкету"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Редагувати анкету", callback_data="edit_profile")],
            [InlineKeyboardButton(text=toggle_text, callback_data="toggle_active")],
            [InlineKeyboardButton(text="🔄 Перебудувати анкету з нуля", callback_data="recreate_profile")]
        ]
    )

def edit_fields_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Змінити ім'я", callback_data="edit_name"), InlineKeyboardButton(text="🎂 Змінити вік", callback_data="edit_age")],
            [InlineKeyboardButton(text="🏙 Змінити місто", callback_data="edit_city"), InlineKeyboardButton(text="📖 Змінити опис", callback_data="edit_bio")],
            [InlineKeyboardButton(text="📸 Оновити фото", callback_data="edit_photo")],
            [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_profile")]
        ]
    )

def search_options_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏙 Пошук за містом", callback_data="search_by_city")],
            [InlineKeyboardButton(text="🔄 Скинути фільтри пошуку", callback_data="reset_search_filters")]
        ]
    )

def feed_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❤️"), KeyboardButton(text="👎"), KeyboardButton(text="🛑 Скарга")],
            [KeyboardButton(text="🏠 Головне меню")]
        ],
        resize_keyboard=True
    )

# --- ДОПОМІЖНІ ФУНКЦІЇ ---

def format_profile(profile: dict) -> str:
    status = "🟢 Активна" if profile.get('active', True) else "🔴 Прихована з пошуку"
    return (
        f"📌 **{profile['name']}**, {profile['age']}, {profile['city']}\n"
        f"📝 {profile['bio']}\n\n"
        f"Статус анкети: {status}"
    )

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

# --- ХЕНДЛЕР СКАСУВАННЯ / СКИДАННЯ СТАНУ ---
@dp.message(Command("cancel"))
@dp.message(F.text == "🚫 Скасувати")
async def cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return
    await state.clear()
    await message.answer("Реєстрацію або дію скасовано.", reply_markup=main_menu_keyboard())

# --- ХЕНДЛЕРИ СТАРТУ ТА РЕЄСТРАЦІЇ ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    profile = db_get_profile(user_id)
    if profile:
        await message.answer("З поверненням у **Дайвінчик UA** 🇺🇦!", reply_markup=main_menu_keyboard())
    else:
        await message.answer(
            f"Привіт, {message.from_user.first_name}! 👋\n"
            f"Вітаємо у **Дайвінчик UA** 🇺🇦!\n\nДавай створимо твою анкету. Як тебе звати?"
        )
        await state.set_state(ProfileRegistration.name)

@dp.message(ProfileRegistration.name)
async def process_name(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("/") or message.text in ["🚀 Дивитися анкети", "🔍 Пошук", "👤 Моя анкета", "⚙️ Налаштування", "❓ Допомога"]):
        await state.clear()
        await message.answer("Реєстрацію перервано.", reply_markup=main_menu_keyboard())
        return
    await state.update_data(name=message.text, username=message.from_user.username)
    await message.answer("Скільки тобі років?")
    await state.set_state(ProfileRegistration.age)

@dp.message(ProfileRegistration.age)
async def process_age(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("/") or message.text in ["🚀 Дивитися анкети", "🔍 Пошук", "👤 Моя анкета", "⚙️ Налаштування", "❓ Допомога"]):
        await state.clear()
        await message.answer("Реєстрацію перервано.", reply_markup=main_menu_keyboard())
        return

    if not message.text or not message.text.isdigit() or not (12 <= int(message.text) <= 99):
        await message.answer("Вкажи реальний вік числом (наприклад, 19):")
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
    await state.update_data(target_gender=message.text, target_age_min=12, target_age_max=99)
    await message.answer("З якого ти міста?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(ProfileRegistration.city)

@dp.message(ProfileRegistration.city)
async def process_city(message: types.Message, state: FSMContext):
    await state.update_data(city=message.text)
    await message.answer("Напиши короткий опис про себе (хто ти, чим захоплюєшся):")
    await state.set_state(ProfileRegistration.bio)

@dp.message(ProfileRegistration.bio)
async def process_bio(message: types.Message, state: FSMContext):
    await state.update_data(bio=message.text)
    await message.answer("Надішли своє фото для анкети 📸:")
    await state.set_state(ProfileRegistration.photo)

@dp.message(ProfileRegistration.photo, F.photo)
async def process_photo(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    data['photo'] = photo_id
    data['active'] = True
    
    db_save_profile(message.from_user.id, data)
    await state.clear()
    
    await message.answer("🎉 **Анкету створено успішно!**", parse_mode="Markdown", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

# --- РЕЖИМ ПОШУКУ ТА ФІЛЬТРІВ ---

@dp.message(F.text == "🔍 Пошук")
@dp.message(Command("search"))
async def search_menu(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if not db_get_profile(user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    current_filter = search_filters.get(user_id, {})
    city_filter = current_filter.get('city', 'Усі міста')
    
    await message.answer(
        f"🔍 **Налаштування пошуку**\n\n"
        f"Поточний фільтр міста: **{city_filter}**\n"
        f"Обери параметр для пошуку або скинь фільтри:",
        parse_mode="Markdown",
        reply_markup=search_options_keyboard()
    )

@dp.callback_query(F.data == "search_by_city")
async def ask_search_city(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи назву міста, в якому хочеш шукати анкети:")
    await state.set_state(SearchFilterState.filter_city)

@dp.message(SearchFilterState.filter_city)
async def set_search_city(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    target_city = message.text.strip()
    
    search_filters.setdefault(user_id, {})['city'] = target_city
    await state.clear()
    
    await message.answer(
        f"✅ Фільтр встановлено: шукаємо анкети в місті **{target_city}**!\n"
        f"Натисни «🚀 Дивитися анкети», щоб розпочати перегляд.",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )

@dp.callback_query(F.data == "reset_search_filters")
async def reset_search_filters(call: types.CallbackQuery):
    user_id = call.from_user.id
    if user_id in search_filters:
        search_filters[user_id].clear()
    await call.answer("Фільтри скинуто! Шукаємо по всіх містах.", show_alert=True)
    await call.message.edit_text(
        "🔍 **Налаштування пошуку**\n\nПоточний фільтр міста: **Усі міста**",
        reply_markup=search_options_keyboard()
    )

# --- МЕНЮ "МОЯ АНКЕТА" ТА РЕДАКТУВАННЯ ---

async def show_my_profile_logic(message: types.Message):
    user_id = message.from_user.id
    p = db_get_profile(user_id)
    if not p:
        await message.answer("У тебе ще немає анкети. Напиши /start для реєстрації.")
        return
    
    caption = f"Твоя анкета:\n\n{format_profile(p)}"
    await message.answer_photo(
        photo=p['photo'],
        caption=caption,
        parse_mode="Markdown",
        reply_markup=my_profile_keyboard(p.get('active', True))
    )

@dp.message(F.text == "👤 Моя анкета")
@dp.message(Command("myprofile"))
async def show_my_profile_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "toggle_active")
async def toggle_active(call: types.CallbackQuery):
    user_id = call.from_user.id
    p = db_get_profile(user_id)
    if p:
        p['active'] = not p['active']
        db_save_profile(user_id, p)
        new_status = "активовано" if p['active'] else "приховано з пошуку"
        await call.answer(f"Анкету {new_status}!", show_alert=True)
        await call.message.delete()
        await show_my_profile_logic(call.message)

@dp.callback_query(F.data == "edit_profile")
async def edit_profile_menu(call: types.CallbackQuery):
    await call.message.edit_caption(
        caption="Обери, який пункт ти хочеш змінити:",
        reply_markup=edit_fields_keyboard()
    )

@dp.callback_query(F.data == "back_to_profile")
async def back_to_profile(call: types.CallbackQuery):
    await call.message.delete()
    await show_my_profile_logic(call.message)

@dp.callback_query(F.data == "recreate_profile")
async def recreate_profile(call: types.CallbackQuery, state: FSMContext):
    await call.message.delete()
    await call.message.answer("Розпочнемо заново! Як тебе звати?")
    await state.set_state(ProfileRegistration.name)

# --- РЕДАКТУВАННЯ ОКРЕМИХ ПОЛІВ ---

@dp.callback_query(F.data == "edit_name")
async def edit_name(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи нове ім'я:")
    await state.set_state(EditProfileState.new_name)

@dp.message(EditProfileState.new_name)
async def process_new_name(message: types.Message, state: FSMContext):
    p = db_get_profile(message.from_user.id)
    if p:
        p['name'] = message.text
        db_save_profile(message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Ім'я успішно оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_age")
async def edit_age(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи новий вік:")
    await state.set_state(EditProfileState.new_age)

@dp.message(EditProfileState.new_age)
async def process_new_age(message: types.Message, state: FSMContext):
    if not message.text or not message.text.isdigit() or not (12 <= int(message.text) <= 99):
        await message.answer("Вкажи реальний вік числом:")
        return
    p = db_get_profile(message.from_user.id)
    if p:
        p['age'] = int(message.text)
        db_save_profile(message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Вік оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_city")
async def edit_city(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи нове місто:")
    await state.set_state(EditProfileState.new_city)

@dp.message(EditProfileState.new_city)
async def process_new_city(message: types.Message, state: FSMContext):
    p = db_get_profile(message.from_user.id)
    if p:
        p['city'] = message.text
        db_save_profile(message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Місто оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_bio")
async def edit_bio(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Напиши новий опис про себе:")
    await state.set_state(EditProfileState.new_bio)

@dp.message(EditProfileState.new_bio)
async def process_new_bio(message: types.Message, state: FSMContext):
    p = db_get_profile(message.from_user.id)
    if p:
        p['bio'] = message.text
        db_save_profile(message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Опис оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_photo")
async def edit_photo(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Надішли нову світлину 📸:")
    await state.set_state(EditProfileState.new_photo)

@dp.message(EditProfileState.new_photo, F.photo)
async def process_new_photo(message: types.Message, state: FSMContext):
    p = db_get_profile(message.from_user.id)
    if p:
        p['photo'] = message.photo[-1].file_id
        db_save_profile(message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Фото оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

# --- ГОРТАННЯ АНКЕТ (ФІД) ---

@dp.message(F.text == "🚀 Дивитися анкети")
@dp.message(Command("feed"))
async def start_feed(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    if not db_get_profile(user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    if user_id in likes_queue and likes_queue[user_id]:
        liker_id = likes_queue[user_id].pop(0)
        liker_profile = db_get_profile(liker_id)
        if liker_profile and liker_profile.get('active', True):
            await state.update_data(current_target=liker_id, is_like_mode=True)
            await message.answer("Комусь сподобалась твоя анкета! 🚀", reply_markup=feed_keyboard())
            await show_profile(message, liker_id, liker_profile)
            await state.set_state(FeedState.viewing)
            return

    filters = search_filters.get(user_id, {})
    target_city = filters.get('city')
    seen_set = seen_profiles.get(user_id, set())

    target_uid, profile = db_get_next_profile(user_id, seen_set, target_city)
    if not profile:
        city_info = f" у місті **{target_city}**" if target_city else ""
        await message.answer(f"Поки що немає нових анкет{city_info}. Спробуй скинути фільтри або завітай трохи пізніше! 😉", parse_mode="Markdown", reply_markup=main_menu_keyboard())
        return

    await state.update_data(current_target=target_uid, is_like_mode=False)
    await show_profile(message, target_uid, profile)
    await state.set_state(FeedState.viewing)

@dp.message(FeedState.viewing, F.text == "🏠 Головне меню")
async def exit_feed(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Повертаємось у головне меню.", reply_markup=main_menu_keyboard())

@dp.message(FeedState.viewing, F.text.in_(["❤️", "👎", "🛑 Скарга"]))
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
            my_prof = db_get_profile(user_id)
            target_prof = db_get_profile(target_uid)
            
            my_link = f"@{my_prof.get('username')}" if my_prof.get('username') else f"[Користувач](tg://user?id={user_id})"
            target_link = f"@{target_prof.get('username')}" if target_prof.get('username') else f"[Користувач](tg://user?id={target_uid})"

            await message.answer(f"🎉 **Це МЕТЧ!**\nТи сподобався(лась) {target_prof['name']}!\nКонтакт для зв'язку: {target_link}", parse_mode="Markdown")
            try:
                await bot.send_message(target_uid, f"🎉 **Це МЕТЧ!**\nТобі відповіли взаємністю! Контакт: {my_link}", parse_mode="Markdown")
            except Exception:
                pass
        else:
            likes_queue.setdefault(target_uid, []).append(user_id)
            try:
                await bot.send_message(target_uid, "Твоєю анкетою хтось зацікавився! Натисни «🚀 Дивитися анкети», щоб переглянути. 😉")
            except Exception:
                pass

    elif reaction == "🛑 Скарга":
        await message.answer("Скаргу прийнято. Дякуємо, що робите сервіс безпечнішим!")

    await start_feed(message, state)

# --- БЛОКУВАННЯ КРУЖКІВ ТА МЕДІА ПІД ЧАС ПЕРЕГЛЯДУ АНКЕТ ---
@dp.message(FeedState.viewing, F.video_note | F.voice | F.sticker | F.video | F.photo | F.document)
async def block_media_in_feed(message: types.Message):
    await message.answer(
        "⚠️ У режимі перегляду анкет відправка «кружків» та медіа вимкнена.\n"
        "Користуйся кнопками нижче: ❤️, 👎, 🛑 Скарга або 🏠 Головне меню."
    )

# --- ІНШІ КОМАНДИ ТА МЕНЮ ---

@dp.message(F.text == "⚙️ Налаштування")
async def settings_menu(message: types.Message, state: FSMContext):
    await state.clear()
    
    if message.from_user.id == ADMIN_ID:
        total, active = db_get_profiles_count()
        await message.answer(
            f"⚙️ **Налаштування та статистика**\n\n"
            f"👥 Усього зареєстровано анкет: **{total}**\n"
            f"🟢 Активних у пошуку: **{active}**\n"
            f"🔴 Прихованих анкет: **{total - active}**\n\n"
            f"Панель адміністратора активна!",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
    else:
        await message.answer(
            "⚙️ **Налаштування бота**\n\nТут ти можеш налаштувати сповіщення та мову інтерфейсу. (В розробці)",
            reply_markup=main_menu_keyboard()
        )

@dp.message(F.text == "❓ Допомога")
@dp.message(Command("help"))
async def help_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❓ **Як користуватися ботом Дайвінчик UA:**\n\n"
        "• **🚀 Дивитися анкети** — починає гортання користувачів.\n"
        "• **🔍 Пошук** — встановлення фільтру за містом.\n"
        "• **❤️** — поставити лайк.\n"
        "• **👎** — пропустити анкету.\n"
        "• **👤 Моя анкета** — перегляд, редагування або приховання своєї анкети з пошуку.\n\n"
        "Приємного спілкування! 🇺🇦"
    )

@dp.message(Command("stats"))
async def admin_stats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    total, active = db_get_profiles_count()
    await message.answer(
        f"📊 **Статистика бота:**\n\n"
        f"• Всього користувачів: **{total}**\n"
        f"• Активних анкет: **{active}**\n"
        f"• Прихованих анкет: **{total - active}**",
        parse_mode="Markdown"
    )

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
