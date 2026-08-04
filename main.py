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

# Реквізити для розділу "Підтримати бота" — заміни на свої.
SUPPORT_CARD_NUMBER = os.getenv("SUPPORT_CARD_NUMBER", "0000 0000 0000 0000")
SUPPORT_JAR_URL = os.getenv("SUPPORT_JAR_URL", "https://send.monobank.ua/jar/приклад")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

@dp.message.outer_middleware()
async def ban_check_middleware(handler, event: types.Message, data):
    """Блокує будь-яку дію забаненого користувача (крім адміна)."""
    user_id = event.from_user.id if event.from_user else None
    if user_id and user_id != ADMIN_ID and db_is_banned(user_id):
        await event.answer("⛔ Твій акаунт заблоковано адміністрацією бота.")
        return
    return await handler(event, data)

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
            target_age_min INTEGER DEFAULT 18,
            target_age_max INTEGER DEFAULT 99,
            city TEXT,
            bio TEXT,
            photo TEXT,
            username TEXT,
            active INTEGER DEFAULT 1,
            banned INTEGER DEFAULT 0
        )
    ''')
    # Додаємо міграцію на випадок, якщо таблиця вже існувала без цих колонок
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS banned INTEGER DEFAULT 0;')
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS target_age_min INTEGER DEFAULT 18;')
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS target_age_max INTEGER DEFAULT 99;')

    # Лайки: хто кого лайкнув. Метч = запис в обидва боки.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS likes (
            from_user_id BIGINT NOT NULL,
            to_user_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (from_user_id, to_user_id)
        )
    ''')
    # Коментар до лайка (опційно). Видно лише тому, кого лайкнули, коли він відкриє анкету лайкера.
    cursor.execute('ALTER TABLE likes ADD COLUMN IF NOT EXISTS comment TEXT;')

    # Перегляди: які анкети користувач вже бачив (лайк/дизлайк/скарга) — щоб не показувати повторно.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS seen (
            user_id BIGINT NOT NULL,
            target_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (user_id, target_id)
        )
    ''')

    # Фільтри пошуку користувача (перевизначають налаштування з анкети). Зберігаються постійно.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_filters (
            user_id BIGINT PRIMARY KEY,
            city TEXT,
            age_min INTEGER,
            age_max INTEGER,
            gender TEXT
        )
    ''')
    # Захист: піднімаємо всі наявні анкети з віком/цільовим віком нижче 18 до мінімуму 18,
    # і ховаємо з пошуку будь-які анкети з віком нижче 18 (на випадок, якщо такі
    # з'явилися до підняття мінімального віку реєстрації).
    cursor.execute('UPDATE profiles SET target_age_min = 18 WHERE target_age_min < 18;')
    cursor.execute('UPDATE profiles SET active = 0 WHERE age < 18;')
    cursor.execute('UPDATE search_filters SET age_min = 18 WHERE age_min IS NOT NULL AND age_min < 18;')

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
        data.get('target_age_min', 18),
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

def db_get_search_filters(user_id):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('SELECT city, age_min, age_max, gender FROM search_filters WHERE user_id = %s', (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if row:
        return {'city': row[0], 'age_min': row[1], 'age_max': row[2], 'gender': row[3]}
    return {'city': None, 'age_min': None, 'age_max': None, 'gender': None}

def db_set_search_filter(user_id, **fields):
    """Оновлює один чи декілька фільтрів пошуку (city, age_min, age_max, gender)."""
    current = db_get_search_filters(user_id)
    current.update(fields)
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO search_filters (user_id, city, age_min, age_max, gender)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            city = EXCLUDED.city,
            age_min = EXCLUDED.age_min,
            age_max = EXCLUDED.age_max,
            gender = EXCLUDED.gender
    ''', (user_id, current['city'], current['age_min'], current['age_max'], current['gender']))
    conn.commit()
    cursor.close()
    conn.close()

def db_reset_search_filters(user_id):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM search_filters WHERE user_id = %s', (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

def db_get_next_profile(current_user_id):
    current_profile = db_get_profile(current_user_id)
    if not current_profile:
        return None, None

    filters = db_get_search_filters(current_user_id)
    min_age = filters.get('age_min') or current_profile.get('target_age_min', 18)
    max_age = filters.get('age_max') or current_profile.get('target_age_max', 99)
    target_gender = filters.get('gender') or current_profile.get('target_gender', 'Усіх 🌈')
    target_city = filters.get('city')

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    query = '''
        SELECT user_id, name, age, gender, target_gender, target_age_min, target_age_max, city, bio, photo, username, active
        FROM profiles
        WHERE user_id != %s AND active = 1 AND age BETWEEN %s AND %s
          AND NOT EXISTS (
              SELECT 1 FROM seen s WHERE s.user_id = %s AND s.target_id = profiles.user_id
          )
    '''
    params = [current_user_id, min_age, max_age, current_user_id]
    
    # Фільтрація за статтю, яку шукає користувач
    if target_gender == "Дівчат 👩":
        query += " AND gender = 'Дівчина 👩'"
    elif target_gender == "Хлопців 👨":
        query += " AND gender = 'Хлопець 👨'"
    
    if target_city:
        query += ' AND LOWER(city) = LOWER(%s)'
        params.append(target_city)

    query += ' ORDER BY RANDOM() LIMIT 1'

    cursor.execute(query, params)
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return None, None

    return row[0], {
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

def db_add_like(from_user_id, to_user_id, comment=None):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO likes (from_user_id, to_user_id, comment) VALUES (%s, %s, %s)
        ON CONFLICT (from_user_id, to_user_id) DO UPDATE SET
            comment = COALESCE(EXCLUDED.comment, likes.comment)
        ''',
        (from_user_id, to_user_id, comment)
    )
    conn.commit()
    cursor.close()
    conn.close()

def db_get_like_comment(from_user_id, to_user_id):
    """Коментар, який from_user_id залишив(ла) до лайка на анкету to_user_id (якщо є)."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT comment FROM likes WHERE from_user_id = %s AND to_user_id = %s',
        (from_user_id, to_user_id)
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row and row[0] else None

def db_check_mutual_like(user_a, user_b):
    """Чи user_b вже лайкнув user_a раніше (для миттєвого визначення метчу)."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT 1 FROM likes WHERE from_user_id = %s AND to_user_id = %s',
        (user_b, user_a)
    )
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row is not None

def db_add_seen(user_id, target_id):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO seen (user_id, target_id) VALUES (%s, %s) ON CONFLICT DO NOTHING',
        (user_id, target_id)
    )
    conn.commit()
    cursor.close()
    conn.close()

def db_get_pending_like(user_id):
    """ID користувача, який лайкнув user_id і якого user_id ще не бачив (черга 'тебе лайкнули')."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT l.from_user_id
        FROM likes l
        JOIN profiles p ON p.user_id = l.from_user_id
        WHERE l.to_user_id = %s
          AND p.active = 1
          AND NOT EXISTS (
              SELECT 1 FROM seen s WHERE s.user_id = %s AND s.target_id = l.from_user_id
          )
        ORDER BY l.created_at ASC
        LIMIT 1
    ''', (user_id, user_id))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None

def db_count_pending_likes(user_id):
    """Скільки людей лайкнули user_id і ще не були переглянуті (черга 'тебе лайкнули')."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*)
        FROM likes l
        JOIN profiles p ON p.user_id = l.from_user_id
        WHERE l.to_user_id = %s
          AND p.active = 1
          AND NOT EXISTS (
              SELECT 1 FROM seen s WHERE s.user_id = %s AND s.target_id = l.from_user_id
          )
    ''', (user_id, user_id))
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count

def db_get_matches(user_id):
    """Список user_id, з якими є взаємний лайк (метч)."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT l1.to_user_id
        FROM likes l1
        JOIN likes l2 ON l1.to_user_id = l2.from_user_id AND l1.from_user_id = l2.to_user_id
        WHERE l1.from_user_id = %s
        ORDER BY l1.created_at DESC
    ''', (user_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [r[0] for r in rows]

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

# --- АДМІН-ФУНКЦІЇ ---

def db_get_detailed_stats():
    """Розширена статистика для адмін-панелі: анкети, лайки, метчі, топ міст."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM profiles')
    total = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM profiles WHERE active = 1')
    active = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM profiles WHERE banned = 1')
    banned = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM likes')
    likes_total = cursor.fetchone()[0]
    cursor.execute('''
        SELECT COUNT(*) FROM likes l1
        JOIN likes l2 ON l1.to_user_id = l2.from_user_id AND l1.from_user_id = l2.to_user_id
        WHERE l1.from_user_id < l1.to_user_id
    ''')
    matches_total = cursor.fetchone()[0]
    cursor.execute("SELECT gender, COUNT(*) FROM profiles GROUP BY gender")
    by_gender = cursor.fetchall()
    cursor.execute('''
        SELECT city, COUNT(*) c FROM profiles
        WHERE city IS NOT NULL AND city != ''
        GROUP BY city ORDER BY c DESC LIMIT 5
    ''')
    top_cities = cursor.fetchall()
    cursor.close()
    conn.close()
    return {
        'total': total, 'active': active, 'banned': banned,
        'likes_total': likes_total, 'matches_total': matches_total,
        'by_gender': by_gender, 'top_cities': top_cities,
    }

def db_get_all_user_ids():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('SELECT user_id FROM profiles')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [r[0] for r in rows]

def db_is_banned(user_id):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('SELECT banned FROM profiles WHERE user_id = %s', (user_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return bool(row[0]) if row else False

def db_set_banned(user_id, banned: bool):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    if banned:
        cursor.execute('UPDATE profiles SET banned = 1, active = 0 WHERE user_id = %s', (user_id,))
    else:
        cursor.execute('UPDATE profiles SET banned = 0 WHERE user_id = %s', (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

def db_delete_profile(user_id):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM likes WHERE from_user_id = %s OR to_user_id = %s', (user_id, user_id))
    cursor.execute('DELETE FROM seen WHERE user_id = %s OR target_id = %s', (user_id, user_id))
    cursor.execute('DELETE FROM search_filters WHERE user_id = %s', (user_id,))
    cursor.execute('DELETE FROM profiles WHERE user_id = %s', (user_id,))
    conn.commit()
    cursor.close()
    conn.close()

def db_admin_get_random_profile(admin_id, gender_filter=None):
    """Випадкова анкета для адмін-перегляду. Ігнорує 'seen' (можна бачити навіть уже лайкані)
    та 'active' (адмін бачить і приховані анкети). gender_filter: 'Хлопець 👨' / 'Дівчина 👩' / None (усі)."""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    query = '''
        SELECT user_id, name, age, gender, target_gender, target_age_min, target_age_max,
               city, bio, photo, username, active
        FROM profiles
        WHERE user_id != %s
    '''
    params = [admin_id]
    if gender_filter:
        query += ' AND gender = %s'
        params.append(gender_filter)
    query += ' ORDER BY RANDOM() LIMIT 1'

    cursor.execute(query, params)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        return None
    return {
        'user_id': row[0], 'name': row[1], 'age': row[2], 'gender': row[3],
        'target_gender': row[4], 'target_age_min': row[5], 'target_age_max': row[6],
        'city': row[7], 'bio': row[8], 'photo': row[9], 'username': row[10], 'active': bool(row[11])
    }

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
    filter_age = State()

class FeedState(StatesGroup):
    viewing = State()

class MessageToProfileState(StatesGroup):
    text = State()

class LikeCommentState(StatesGroup):
    text = State()

class AdminBroadcastState(StatesGroup):
    text = State()

class AdminLookupState(StatesGroup):
    user_id = State()

class AdminBrowseState(StatesGroup):
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
            [KeyboardButton(text="💞 Мої метчі"), KeyboardButton(text="❤️ Хто мене лайкнув")],
            [KeyboardButton(text="👤 Моя анкета"), KeyboardButton(text="⚙️ Налаштування")],
            [KeyboardButton(text="💙 Підтримати бота")]
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
            [InlineKeyboardButton(text="🎂 Віковий діапазон", callback_data="search_by_age")],
            [InlineKeyboardButton(text="🚻 Кого шукати", callback_data="search_by_gender")],
            [InlineKeyboardButton(text="🔄 Скинути фільтри пошуку", callback_data="reset_search_filters")]
        ]
    )

def search_gender_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Дівчат 👩", callback_data="search_gender_girls")],
            [InlineKeyboardButton(text="Хлопців 👨", callback_data="search_gender_boys")],
            [InlineKeyboardButton(text="Усіх 🌈", callback_data="search_gender_all")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_back")]
        ]
    )

def feed_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❤️"), KeyboardButton(text="👎"), KeyboardButton(text="🛑 Скарга")],
            [KeyboardButton(text="💌 Лайк з коментарем"), KeyboardButton(text="✉️ Написати")],
            [KeyboardButton(text="🏠 Головне меню")]
        ],
        resize_keyboard=True
    )

def admin_panel_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Детальна статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="📢 Розсилка всім користувачам", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="🔍 Знайти анкету за ID", callback_data="admin_lookup")],
            [InlineKeyboardButton(text="👀 Переглянути всі анкети", callback_data="admin_browse")],
        ]
    )

def admin_broadcast_confirm_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Надіслати всім", callback_data="admin_broadcast_confirm")],
            [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_broadcast_cancel")],
        ]
    )

def admin_lookup_actions_keyboard(target_id: int, is_banned: bool):
    ban_btn = (
        InlineKeyboardButton(text="✅ Розбанити", callback_data=f"admin_unban_{target_id}")
        if is_banned else
        InlineKeyboardButton(text="🚫 Забанити", callback_data=f"admin_ban_{target_id}")
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [ban_btn],
            [InlineKeyboardButton(text="🗑 Видалити анкету", callback_data=f"admin_delete_{target_id}")],
            [InlineKeyboardButton(text="⬅️ Назад в адмін-панель", callback_data="admin_back")],
        ]
    )

def admin_delete_confirm_keyboard(target_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❗️ Так, видалити назавжди", callback_data=f"admin_delete_confirm_{target_id}")],
            [InlineKeyboardButton(text="⬅️ Скасувати", callback_data="admin_back")],
        ]
    )

def admin_browse_gender_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👨 Хлопців", callback_data="admin_browse_g_m")],
            [InlineKeyboardButton(text="👩 Дівчат", callback_data="admin_browse_g_f")],
            [InlineKeyboardButton(text="🌈 Усіх", callback_data="admin_browse_g_all")],
            [InlineKeyboardButton(text="⬅️ Назад в адмін-панель", callback_data="admin_back")],
        ]
    )

def admin_browse_feed_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➡️ Наступна анкета")],
            [KeyboardButton(text="🔄 Змінити фільтр"), KeyboardButton(text="🏠 Головне меню")],
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

async def show_profile(message: types.Message, target_uid, profile, like_comment=None):
    caption = (
        f"📌 **{profile['name']}**, {profile['age']}, {profile['city']}\n"
        f"📝 {profile['bio']}"
    )
    if like_comment:
        caption += f"\n\n💌 **Коментар до лайка:**\n{like_comment}"
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
    if message.text and (message.text.startswith("/") or message.text in ["🚀 Дивитися анкети", "🔍 Пошук", "💞 Мої метчі", "❤️ Хто мене лайкнув", "👤 Моя анкета", "⚙️ Налаштування", "💙 Підтримати бота"]):
        await state.clear()
        await message.answer("Реєстрацію перервано.", reply_markup=main_menu_keyboard())
        return
    await state.update_data(name=message.text, username=message.from_user.username)
    await message.answer("Скільки тобі років?")
    await state.set_state(ProfileRegistration.age)

@dp.message(ProfileRegistration.age)
async def process_age(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("/") or message.text in ["🚀 Дивитися анкети", "🔍 Пошук", "💞 Мої метчі", "❤️ Хто мене лайкнув", "👤 Моя анкета", "⚙️ Налаштування", "💙 Підтримати бота"]):
        await state.clear()
        await message.answer("Реєстрацію перервано.", reply_markup=main_menu_keyboard())
        return

    if not message.text or not message.text.isdigit() or not (18 <= int(message.text) <= 99):
        await message.answer("Реєстрація доступна лише з 18 років. Вкажи реальний вік числом (наприклад, 19):")
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
    await state.update_data(target_gender=message.text, target_age_min=18, target_age_max=99)
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

def format_search_filters_text(filters: dict) -> str:
    city = filters.get('city') or 'Усі міста'
    if filters.get('age_min') and filters.get('age_max'):
        age = f"{filters['age_min']}–{filters['age_max']}"
    else:
        age = "як в анкеті"
    gender = filters.get('gender') or "як в анкеті"
    return (
        f"🔍 **Налаштування пошуку**\n\n"
        f"🏙 Місто: **{city}**\n"
        f"🎂 Вік: **{age}**\n"
        f"🚻 Кого шукати: **{gender}**\n\n"
        f"Обери параметр, щоб змінити, або скинь фільтри:"
    )

@dp.message(F.text == "🔍 Пошук")
@dp.message(Command("search"))
async def search_menu(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if not db_get_profile(user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    filters = db_get_search_filters(user_id)
    await message.answer(
        format_search_filters_text(filters),
        parse_mode="Markdown",
        reply_markup=search_options_keyboard()
    )

@dp.callback_query(F.data == "search_by_city")
async def ask_search_city(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи назву міста, в якому хочеш шукати анкети (або /cancel):")
    await state.set_state(SearchFilterState.filter_city)
    await call.answer()

@dp.message(SearchFilterState.filter_city)
async def set_search_city(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    target_city = message.text.strip()

    db_set_search_filter(user_id, city=target_city)
    await state.clear()

    await message.answer(
        f"✅ Фільтр встановлено: шукаємо анкети в місті **{target_city}**!\n"
        f"Натисни «🚀 Дивитися анкети», щоб розпочати перегляд.",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )

@dp.callback_query(F.data == "search_by_age")
async def ask_search_age(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer(
        "Введи бажаний віковий діапазон у форматі **мін-макс**, наприклад: 18-25 (або /cancel):",
        parse_mode="Markdown"
    )
    await state.set_state(SearchFilterState.filter_age)
    await call.answer()

@dp.message(SearchFilterState.filter_age)
async def set_search_age(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text.strip().replace(" ", "")
    parts = text.split("-")

    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        await message.answer("Невірний формат. Введи діапазон так: 18-25")
        return

    age_min, age_max = int(parts[0]), int(parts[1])
    if not (18 <= age_min <= 99) or not (18 <= age_max <= 99) or age_min > age_max:
        await message.answer("Вкажи реальний діапазон від 18 до 99, де мінімум не більший за максимум. Приклад: 20-30")
        return

    db_set_search_filter(user_id, age_min=age_min, age_max=age_max)
    await state.clear()

    await message.answer(
        f"✅ Фільтр встановлено: шукаємо анкети віком **{age_min}–{age_max}**!\n"
        f"Натисни «🚀 Дивитися анкети», щоб розпочати перегляд.",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )

@dp.callback_query(F.data == "search_by_gender")
async def ask_search_gender(call: types.CallbackQuery):
    await call.message.edit_text(
        "🚻 Кого шукати в анкетах?",
        reply_markup=search_gender_keyboard()
    )
    await call.answer()

@dp.callback_query(F.data == "search_back")
async def search_back(call: types.CallbackQuery):
    user_id = call.from_user.id
    filters = db_get_search_filters(user_id)
    await call.message.edit_text(
        format_search_filters_text(filters),
        reply_markup=search_options_keyboard()
    )
    await call.answer()

@dp.callback_query(F.data.startswith("search_gender_"))
async def set_search_gender(call: types.CallbackQuery):
    user_id = call.from_user.id
    code = call.data.replace("search_gender_", "")
    mapping = {"girls": "Дівчат 👩", "boys": "Хлопців 👨", "all": "Усіх 🌈"}
    gender = mapping.get(code)
    if not gender:
        await call.answer()
        return

    db_set_search_filter(user_id, gender=gender)
    filters = db_get_search_filters(user_id)
    await call.answer(f"Обрано: {gender}", show_alert=True)
    await call.message.edit_text(
        format_search_filters_text(filters),
        reply_markup=search_options_keyboard()
    )

@dp.callback_query(F.data == "reset_search_filters")
async def reset_search_filters(call: types.CallbackQuery):
    user_id = call.from_user.id
    db_reset_search_filters(user_id)
    await call.answer("Фільтри скинуто! Шукаємо за налаштуваннями анкети.", show_alert=True)
    filters = db_get_search_filters(user_id)
    await call.message.edit_text(
        format_search_filters_text(filters),
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
    if not message.text or not message.text.isdigit() or not (18 <= int(message.text) <= 99):
        await message.answer("Мінімальний вік на анкеті — 18. Вкажи реальний вік числом:")
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

# --- МЕТЧІ ТА ВХІДНІ ЛАЙКИ ---

def match_card_keyboard(target_uid):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✉️ Написати", callback_data=f"match_msg_{target_uid}")]]
    )

@dp.message(F.text == "💞 Мої метчі")
@dp.message(Command("matches"))
async def show_matches(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if not db_get_profile(user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    matches = db_get_matches(user_id)
    if not matches:
        await message.answer(
            "У тебе поки немає метчів 💔\nПродовжуй переглядати анкети — і хтось обов'язково відповість взаємністю!",
            reply_markup=main_menu_keyboard()
        )
        return

    await message.answer(f"💞 У тебе {len(matches)} метч(ів)!", reply_markup=main_menu_keyboard())
    for target_uid in matches[:20]:
        prof = db_get_profile(target_uid)
        if not prof:
            continue
        caption = f"📌 **{prof['name']}**, {prof['age']}, {prof['city']}\n📝 {prof['bio']}"
        await message.answer_photo(
            photo=prof['photo'],
            caption=caption,
            parse_mode="Markdown",
            reply_markup=match_card_keyboard(target_uid)
        )

@dp.callback_query(F.data.startswith("match_msg_"))
async def match_message_start(call: types.CallbackQuery, state: FSMContext):
    target_uid = int(call.data.replace("match_msg_", ""))
    await state.update_data(current_target=target_uid)
    await call.message.answer("Напиши своє повідомлення (або /cancel, щоб скасувати):", reply_markup=ReplyKeyboardRemove())
    await state.set_state(MessageToProfileState.text)
    await call.answer()

@dp.message(F.text == "❤️ Хто мене лайкнув")
async def who_liked_me(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if not db_get_profile(user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    count = db_count_pending_likes(user_id)
    if count == 0:
        await message.answer("Поки що ніхто новий тебе не лайкнув 😉 Продовжуй переглядати анкети!", reply_markup=main_menu_keyboard())
        return

    await message.answer(f"🔥 Тебе лайкнуло {count} людей! Дивимось, хто саме 👇")
    await start_feed(message, state)

# --- ГОРТАННЯ АНКЕТ (ФІД) ---

@dp.message(F.text == "🚀 Дивитися анкети")
@dp.message(Command("feed"))
async def start_feed(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    if not db_get_profile(user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    pending_liker_id = db_get_pending_like(user_id)
    if pending_liker_id:
        liker_profile = db_get_profile(pending_liker_id)
        if liker_profile and liker_profile.get('active', True):
            like_comment = db_get_like_comment(pending_liker_id, user_id)
            await state.update_data(current_target=pending_liker_id, is_like_mode=True)
            await message.answer("Комусь сподобалась твоя анкета! 🚀", reply_markup=feed_keyboard())
            await show_profile(message, pending_liker_id, liker_profile, like_comment=like_comment)
            await state.set_state(FeedState.viewing)
            return

    filters = db_get_search_filters(user_id)
    target_city = filters.get('city')

    target_uid, profile = db_get_next_profile(user_id)
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
        db_add_seen(user_id, target_uid)

    reaction = message.text

    if reaction == "❤️":
        db_add_like(user_id, target_uid)

        # is_like_mode означає, що target_uid вже лайкнув нас раніше — це гарантований метч.
        # Інакше перевіряємо, чи не лайкнув target_uid нас раніше незалежно (миттєвий метч).
        is_match = is_like_mode or db_check_mutual_like(user_id, target_uid)

        if is_match:
            my_prof = db_get_profile(user_id)
            target_prof = db_get_profile(target_uid)
            
            my_link = f"@{my_prof.get('username')}" if my_prof.get('username') else f"<a href='tg://user?id={user_id}'>Користувач</a>"
            target_link = f"@{target_prof.get('username')}" if target_prof.get('username') else f"<a href='tg://user?id={target_uid}'>Користувач</a>"

            await message.answer(f"🎉 <b>Це МЕТЧ!</b>\nТи сподобався(лась) {target_prof['name']}!\nКонтакт для зв'язку: {target_link}", parse_mode="HTML")
            try:
                await bot.send_message(target_uid, f"🎉 <b>Це МЕТЧ!</b>\nТобі відповіли взаємністю! Контакт: {my_link}", parse_mode="HTML")
            except Exception:
                pass
        else:
            try:
                await bot.send_message(target_uid, "Твоєю анкетою хтось зацікавився! Натисни «🚀 Дивитися анкети», щоб переглянути. 😉")
            except Exception:
                pass

    elif reaction == "🛑 Скарга":
        await message.answer("Скаргу прийнято. Дякуємо, що робите сервіс безпечнішим!")

    await start_feed(message, state)

# --- ЛАЙК З КОМЕНТАРЕМ (анонімно, видно лише при відкритті анкети лайкера) ---

@dp.message(FeedState.viewing, F.text == "💌 Лайк з коментарем")
async def ask_like_comment(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("current_target"):
        return
    await message.answer(
        "Напиши короткий коментар до лайка 💌\n"
        "Його побачить тільки ця людина, коли відкриє твою анкету. "
        "Твої контакти залишаться анонімними, поки не станеться метч.\n\n"
        "(або /cancel, щоб скасувати):",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(LikeCommentState.text)

@dp.message(LikeCommentState.text, F.text)
async def process_like_comment(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    target_uid = data.get("current_target")
    is_like_mode = data.get("is_like_mode", False)

    if not target_uid:
        await state.clear()
        await message.answer("Щось пішло не так, спробуй ще раз.", reply_markup=main_menu_keyboard())
        return

    comment_text = message.text.strip()[:500]

    db_add_seen(user_id, target_uid)
    db_add_like(user_id, target_uid, comment=comment_text)

    # is_like_mode означає, що target_uid вже лайкнув нас раніше — це гарантований метч.
    is_match = is_like_mode or db_check_mutual_like(user_id, target_uid)

    if is_match:
        my_prof = db_get_profile(user_id)
        target_prof = db_get_profile(target_uid)

        my_link = f"@{my_prof.get('username')}" if my_prof.get('username') else f"<a href='tg://user?id={user_id}'>Користувач</a>"
        target_link = f"@{target_prof.get('username')}" if target_prof.get('username') else f"<a href='tg://user?id={target_uid}'>Користувач</a>"

        await message.answer(f"🎉 <b>Це МЕТЧ!</b>\nТи сподобався(лась) {target_prof['name']}!\nКонтакт для зв'язку: {target_link}", parse_mode="HTML")
        try:
            await bot.send_message(target_uid, f"🎉 <b>Це МЕТЧ!</b>\nТобі відповіли взаємністю! Контакт: {my_link}", parse_mode="HTML")
        except Exception:
            pass
    else:
        await message.answer("💌 Лайк із коментарем надіслано!")
        try:
            await bot.send_message(target_uid, "Твоєю анкетою хтось зацікавився і залишив коментар до лайка! Натисни «🚀 Дивитися анкети», щоб переглянути. 😉")
        except Exception:
            pass

    await state.clear()
    await start_feed(message, state)

@dp.message(LikeCommentState.text)
async def block_media_in_like_comment(message: types.Message):
    await message.answer("⚠️ Коментар до лайка може бути лише текстом. Напиши текст або /cancel.")

# --- ПОВІДОМЛЕННЯ НА АНКЕТУ ЗАМІСТЬ ЛАЙКА ---

@dp.message(FeedState.viewing, F.text == "✉️ Написати")
async def ask_message_to_profile(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("current_target"):
        return
    await message.answer(
        "Напиши своє повідомлення (або /cancel, щоб скасувати):",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(MessageToProfileState.text)

@dp.message(MessageToProfileState.text, F.text)
async def send_message_to_profile(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    target_uid = data.get("current_target")

    if not target_uid:
        await state.clear()
        await message.answer("Щось пішло не так, спробуй ще раз.", reply_markup=main_menu_keyboard())
        return

    my_prof = db_get_profile(user_id)
    my_link = f"@{my_prof.get('username')}" if my_prof and my_prof.get('username') else f"<a href='tg://user?id={user_id}'>Користувач</a>"
    my_name = my_prof.get('name') if my_prof else "Хтось"

    try:
        await bot.send_message(
            target_uid,
            f"✉️ <b>Тобі повідомлення від {my_name}!</b>\nКонтакт: {my_link}\n\n{message.text}",
            parse_mode="HTML"
        )
        await message.answer("✅ Повідомлення надіслано!")
    except Exception:
        await message.answer("⚠️ Не вдалося надіслати повідомлення (можливо, користувач заблокував бота).")

    db_add_seen(user_id, target_uid)
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
            f"Панель адміністратора активна! Обери дію нижче 👇",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
        await message.answer("🛠 **Адмін-панель**", parse_mode="Markdown", reply_markup=admin_panel_keyboard())
    else:
        await message.answer(
            "⚙️ **Налаштування бота**\n\nТут ти можеш налаштувати сповіщення та мову інтерфейсу. (В розробці)",
            reply_markup=main_menu_keyboard()
        )

# --- АДМІН-ПАНЕЛЬ: КНОПКИ ТА ДІЇ (тільки для ADMIN_ID) ---

@dp.callback_query(F.data == "admin_back")
async def admin_back(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    await state.clear()
    await call.message.edit_text("🛠 **Адмін-панель**", parse_mode="Markdown", reply_markup=admin_panel_keyboard())
    await call.answer()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats_cb(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    s = db_get_detailed_stats()
    gender_lines = "\n".join(f"   • {g or 'не вказано'}: {c}" for g, c in s['by_gender']) or "   • немає даних"
    city_lines = "\n".join(f"   {i+1}. {c} — {n}" for i, (c, n) in enumerate(s['top_cities'])) or "   • немає даних"
    text = (
        "📊 **Детальна статистика бота**\n\n"
        f"👥 Всього анкет: **{s['total']}**\n"
        f"🟢 Активні: **{s['active']}**\n"
        f"🔴 Приховані: **{s['total'] - s['active']}**\n"
        f"🚫 Забанені: **{s['banned']}**\n\n"
        f"❤️ Всього лайків: **{s['likes_total']}**\n"
        f"🎉 Всього метчів: **{s['matches_total']}**\n\n"
        f"🚻 За статтю:\n{gender_lines}\n\n"
        f"🏙 Топ-5 міст:\n{city_lines}"
    )
    await call.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]])
    )
    await call.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    await call.message.edit_text(
        "📢 Надішли текст повідомлення, яке піде **всім** зареєстрованим користувачам бота.\n\n"
        "Або /cancel, щоб скасувати.",
        parse_mode="Markdown"
    )
    await state.set_state(AdminBroadcastState.text)
    await call.answer()

@dp.message(AdminBroadcastState.text, F.text)
async def admin_broadcast_preview(message: types.Message, state: FSMContext):
    await state.update_data(broadcast_text=message.text)
    total_users = len(db_get_all_user_ids())
    await message.answer(
        f"Ось що піде **{total_users}** користувачам:\n\n{message.text}\n\n"
        "Підтверджуєш розсилку?",
        parse_mode="Markdown",
        reply_markup=admin_broadcast_confirm_keyboard()
    )

@dp.message(AdminBroadcastState.text)
async def admin_broadcast_block_media(message: types.Message):
    await message.answer("⚠️ Розсилка підтримує лише текст. Напиши текст або /cancel.")

@dp.callback_query(F.data == "admin_broadcast_cancel")
async def admin_broadcast_cancel(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    await state.clear()
    await call.message.edit_text("❌ Розсилку скасовано.")
    await call.answer()

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def admin_broadcast_confirm(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    data = await state.get_data()
    text = data.get("broadcast_text")
    await state.clear()
    if not text:
        await call.message.edit_text("⚠️ Текст розсилки загублено, спробуй ще раз.")
        return await call.answer()

    await call.message.edit_text("⏳ Розсилаю...")
    sent, failed = 0, 0
    for uid in db_get_all_user_ids():
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # захист від рейт-лімітів Telegram

    await call.message.answer(
        f"✅ Розсилку завершено.\nНадіслано: **{sent}**\nНе вдалося: **{failed}**",
        parse_mode="Markdown",
        reply_markup=admin_panel_keyboard()
    )
    await call.answer()

@dp.callback_query(F.data == "admin_lookup")
async def admin_lookup_start(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    await call.message.edit_text(
        "🔍 Надішли Telegram ID користувача, якого хочеш знайти.\n\nАбо /cancel."
    )
    await state.set_state(AdminLookupState.user_id)
    await call.answer()

@dp.message(AdminLookupState.user_id, F.text)
async def admin_lookup_result(message: types.Message, state: FSMContext):
    await state.clear()
    if not message.text.strip().isdigit():
        await message.answer("⚠️ ID має бути числом. Спробуй ще раз через меню адмін-панелі.")
        return

    target_id = int(message.text.strip())
    profile = db_get_profile(target_id)
    if not profile:
        await message.answer(
            "😕 Анкету з таким ID не знайдено.",
            reply_markup=admin_panel_keyboard()
        )
        return

    is_banned = db_is_banned(target_id)
    status = "🚫 Забанений" if is_banned else ("🟢 Активна" if profile['active'] else "🔴 Прихована")
    username_line = f"@{profile['username']}" if profile.get('username') else "(немає юзернейму)"
    text = (
        f"👤 **Анкета #{target_id}**\n\n"
        f"Ім'я: **{profile['name']}**, {profile['age']} років\n"
        f"Стать: {profile['gender']}\n"
        f"Місто: {profile.get('city') or '—'}\n"
        f"Юзернейм: {username_line}\n"
        f"Статус: {status}\n\n"
        f"Опис: {profile.get('bio') or '—'}"
    )
    await message.answer(text, parse_mode="Markdown", reply_markup=admin_lookup_actions_keyboard(target_id, is_banned))

@dp.callback_query(F.data.startswith("admin_ban_"))
async def admin_ban_user(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    target_id = int(call.data.replace("admin_ban_", ""))
    db_set_banned(target_id, True)
    try:
        await bot.send_message(target_id, "⛔ Твій акаунт заблоковано адміністрацією бота.")
    except Exception:
        pass
    await call.message.edit_reply_markup(reply_markup=admin_lookup_actions_keyboard(target_id, True))
    await call.answer("Користувача забанено.")

@dp.callback_query(F.data.startswith("admin_unban_"))
async def admin_unban_user(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    target_id = int(call.data.replace("admin_unban_", ""))
    db_set_banned(target_id, False)
    try:
        await bot.send_message(target_id, "✅ Твій акаунт розблоковано. Ласкаво просимо назад!")
    except Exception:
        pass
    await call.message.edit_reply_markup(reply_markup=admin_lookup_actions_keyboard(target_id, False))
    await call.answer("Користувача розбанено.")

@dp.callback_query(F.data.startswith("admin_delete_confirm_"))
async def admin_delete_user_confirmed(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    target_id = int(call.data.replace("admin_delete_confirm_", ""))
    db_delete_profile(target_id)
    await call.message.edit_text(f"🗑 Анкету #{target_id} видалено назавжди.", reply_markup=admin_panel_keyboard())
    await call.answer("Видалено.")

@dp.callback_query(F.data.startswith("admin_delete_"))
async def admin_delete_user_ask(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    target_id = int(call.data.replace("admin_delete_", ""))
    await call.message.edit_text(
        f"❗️ Точно видалити анкету #{target_id} назавжди? Це незворотньо (лайки, метчі, фільтри теж зникнуть).",
        reply_markup=admin_delete_confirm_keyboard(target_id)
    )
    await call.answer()

# --- АДМІН-ПЕРЕГЛЯД УСІХ АНКЕТ (без урахування "seen", навіть уже лайканих) ---

ADMIN_GENDER_CODES = {
    "m": ("Хлопець 👨", "👨 Хлопці"),
    "f": ("Дівчина 👩", "👩 Дівчата"),
    "all": (None, "🌈 Усі"),
}

async def admin_show_next_profile(message: types.Message, state: FSMContext):
    data = await state.get_data()
    gender_code = data.get("admin_gender_code", "all")
    gender_filter, _ = ADMIN_GENDER_CODES.get(gender_code, (None, ""))

    profile = db_admin_get_random_profile(ADMIN_ID, gender_filter=gender_filter)
    if not profile:
        await message.answer("😕 Анкет за цим фільтром не знайдено в базі.", reply_markup=admin_browse_feed_keyboard())
        return

    await state.update_data(admin_current_target=profile['user_id'])
    is_banned = db_is_banned(profile['user_id'])
    status = "🚫 Забанений" if is_banned else ("🟢 Активна" if profile['active'] else "🔴 Прихована")
    username_line = f"@{profile['username']}" if profile.get('username') else "(немає юзернейму)"
    caption = (
        f"👤 **#{profile['user_id']}** — {profile['name']}, {profile['age']}, {profile.get('city') or '—'}\n"
        f"Стать: {profile['gender']} | Статус: {status}\n"
        f"Юзернейм: {username_line}\n\n"
        f"📝 {profile.get('bio') or '—'}"
    )
    if profile.get('photo'):
        await message.answer_photo(
            photo=profile['photo'], caption=caption, parse_mode="Markdown",
            reply_markup=admin_browse_feed_keyboard()
        )
    else:
        await message.answer(caption, parse_mode="Markdown", reply_markup=admin_browse_feed_keyboard())

@dp.callback_query(F.data == "admin_browse")
async def admin_browse_start(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    await call.message.edit_text(
        "👀 **Перегляд усіх анкет**\n\nКого показувати? (Тут видно навіть уже лайкані та приховані анкети.)",
        parse_mode="Markdown",
        reply_markup=admin_browse_gender_keyboard()
    )
    await call.answer()

@dp.callback_query(F.data.startswith("admin_browse_g_"))
async def admin_browse_pick_gender(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    gender_code = call.data.replace("admin_browse_g_", "")
    await state.update_data(admin_gender_code=gender_code)
    await state.set_state(AdminBrowseState.viewing)
    _, label = ADMIN_GENDER_CODES.get(gender_code, (None, "🌈 Усі"))
    await call.message.answer(f"👀 Перегляд анкет: **{label}**", parse_mode="Markdown")
    await admin_show_next_profile(call.message, state)
    await call.answer()

@dp.message(AdminBrowseState.viewing, F.text == "➡️ Наступна анкета")
async def admin_browse_next(message: types.Message, state: FSMContext):
    await admin_show_next_profile(message, state)

@dp.message(AdminBrowseState.viewing, F.text == "🔄 Змінити фільтр")
async def admin_browse_change_filter(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👀 Кого показувати?",
        reply_markup=ReplyKeyboardRemove()
    )
    await message.answer("Обери фільтр:", reply_markup=admin_browse_gender_keyboard())

@dp.message(AdminBrowseState.viewing, F.text == "🏠 Головне меню")
async def admin_browse_exit(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Головне меню.", reply_markup=main_menu_keyboard())

@dp.message(Command("help"))
async def help_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❓ **Як користуватися ботом Дайвінчик UA:**\n\n"
        "• **🚀 Дивитися анкети** — починає гортання користувачів.\n"
        "• **🔍 Пошук** — фільтри за містом, віком і статтю (зберігаються назавжди, поки не скинеш).\n"
        "• **❤️** — поставити лайк.\n"
        "• **💌 Лайк з коментарем** — лайк з повідомленням, яке людина побачить анонімно, коли відкриє твою анкету; контакти з'являться лише після метчу.\n"
        "• **👎** — пропустити анкету.\n"
        "• **✉️ Написати** — надіслати повідомлення на анкету без лайка.\n"
        "• **💞 Мої метчі** — список тих, з ким у вас взаємний лайк.\n"
        "• **❤️ Хто мене лайкнув** — скільки людей тебе лайкнули і перегляд їхніх анкет.\n"
        "• **👤 Моя анкета** — перегляд, редагування або приховання своєї анкети з пошуку.\n\n"
        "Приємного спілкування! 🇺🇦"
    )

@dp.message(F.text == "💙 Підтримати бота")
async def support_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "💙 **Підтримати Дайвінчик UA**\n\n"
        "Бот працює на ентузіазмі та на хостингу, за який треба платити щомісяця 🙂\n"
        "Якщо тобі подобається сервіс і хочеш допомогти йому жити — будемо дуже вдячні за будь-яку суму!\n\n"
        f"💳 Картка для донату: `{SUPPORT_CARD_NUMBER}`\n"
        f"🏦 Або через банку: {SUPPORT_JAR_URL}\n\n"
        "Кожен донат допомагає тримати бота онлайн і додавати нові можливості. Дякуємо! 🇺🇦❤️",
        parse_mode="Markdown"
    )
    await message.answer(
        "Питання чи інструкція по користуванню? Напиши /help.",
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
