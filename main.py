import asyncio
import functools
import hashlib
import hmac
import html
import json
import logging
import os
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qsl
import psycopg2
import psycopg2.pool
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo,
    LabeledPrice
)
from aiohttp import web

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")  # напр. redis://default:password@host:port/0 (Render/Upstash)

if not BOT_TOKEN:
    raise ValueError("Помилка: BOT_TOKEN не знайдено!")
if not DATABASE_URL:
    raise ValueError("Помилка: DATABASE_URL не знайдено!")

ADMIN_ID = 5512316636

# Реквізити для розділу "Підтримати бота" — заміни на свої.
SUPPORT_CARD_NUMBER = os.getenv("SUPPORT_CARD_NUMBER", "0000 0000 0000 0000")
SUPPORT_JAR_URL = os.getenv("SUPPORT_JAR_URL", "https://send.monobank.ua/jar/приклад")

# --- ПРЕМІУМ-ФУНКЦІЇ (оплата Telegram Stars, без сторонніх платіжних систем) ---
# Буст: анкета стає пріоритетною в пошуку (показується першою) на STARS_BOOST_MINUTES хвилин.
STARS_BOOST_PRICE = int(os.getenv("STARS_BOOST_PRICE", "50"))
STARS_BOOST_MINUTES = int(os.getenv("STARS_BOOST_MINUTES", "30"))
# Повний список "Хто мене лайкнув": бачиш одразу всіх лайкерів на STARS_PREMIUM_LIKES_DAYS днів
# (замість перегляду по одному через звичайну стрічку).
STARS_PREMIUM_LIKES_PRICE = int(os.getenv("STARS_PREMIUM_LIKES_PRICE", "100"))
STARS_PREMIUM_LIKES_DAYS = int(os.getenv("STARS_PREMIUM_LIKES_DAYS", "7"))
# Денний ліміт лайків для безкоштовних користувачів. Преміум (premium_likes_active) знімає обмеження.
FREE_DAILY_LIKE_LIMIT = int(os.getenv("FREE_DAILY_LIKE_LIMIT", "15"))

# Публічний HTTPS-URL твого сервісу на Render (без слеша в кінці), напр. https://my-bot.onrender.com
# Потрібен, щоб кнопка "Адмін-сайт" відкривала міні-апп.
WEBAPP_BASE_URL = os.getenv("WEBAPP_BASE_URL", "").rstrip("/")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)

# --- ЗБЕРІГАННЯ СТАНУ FSM ---
# MemoryStorage тримає стан реєстрації/редагування анкети тільки в оперативній
# пам'яті процесу. Render free-інстанс засинає і перезапускається при простої —
# і весь прогрес користувача посеред діалогу зникає без помилки. Якщо є
# REDIS_URL — використовуємо RedisStorage, стан переживає рестарт процесу.
# Якщо ні — падаємо назад на MemoryStorage (з попередженням у логах), щоб бот
# не переставав запускатися там, де Redis ще не підключений.
if REDIS_URL:
    from aiogram.fsm.storage.redis import RedisStorage
    storage = RedisStorage.from_url(REDIS_URL)
    logging.info("FSM-стан: RedisStorage (%s)", REDIS_URL.split('@')[-1])
else:
    storage = MemoryStorage()
    logging.warning(
        "REDIS_URL не задано — FSM-стан живе тільки в пам'яті процесу. "
        "На Render free-інстанс перезапуск при засинанні зітре прогрес користувачів "
        "посеред реєстрації/редагування анкети. Додай Redis і REDIS_URL, щоб це виправити."
    )

dp = Dispatcher(storage=storage)

# --- НЕБЛОКУЮЧИЙ ДОСТУП ДО БД ---
# psycopg2 синхронний. Якщо викликати його напряму з async-хендлера, ВЕСЬ бот
# зависає для ВСІХ користувачів, поки триває один запит до бази. Тому кожен
# виклик db_* з асинхронного коду йде через run_db(), який виконує його в
# окремому потоці і не блокує основний цикл подій бота.
DB_EXECUTOR = ThreadPoolExecutor(max_workers=10)

async def run_db(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(DB_EXECUTOR, functools.partial(func, *args, **kwargs))

# --- ПУЛ З'ЄДНАНЬ З БД ---
# Раніше кожна db_* функція відкривала нове TCP-з'єднання (psycopg2.connect)
# і закривала його вручну лінійним кодом без try/finally. Якщо запит падав
# (constraint violation, обрив мережі тощо) — з'єднання лишалося відкритим
# назавжди. З ThreadPoolExecutor(max_workers=10) це могло дати до 10 одночасних
# "живих" з'єднань, що витікають, і врешті впертися в ліміт з'єднань Supabase.
# Пул тримає фіксовану кількість готових з'єднань і перевикористовує їх;
# кожна db_* функція тепер бере з'єднання з пулу (getconn) і завжди повертає
# його назад (putconn) у finally, навіть якщо запит впав з помилкою.
db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, DATABASE_URL)

# --- АНТИ-ФЛУД ---
# Проста пам'ятна анти-флуд заглушка: якщо юзер шле апдейти частіше, ніж раз
# на THROTTLE_SECONDS, зайві апдейти просто відкидаються (хендлер не викликається),
# а попередження показуємо не частіше, ніж раз на WARN_COOLDOWN, щоб саме
# попередження не перетворилось на спам.
THROTTLE_SECONDS = 0.6
WARN_COOLDOWN = 3.0
_last_seen: dict[int, float] = defaultdict(float)
_last_warned: dict[int, float] = defaultdict(float)

def _is_throttled(user_id: int) -> bool:
    now = time.monotonic()
    was_throttled = (now - _last_seen[user_id]) < THROTTLE_SECONDS
    _last_seen[user_id] = now
    return was_throttled

def _should_warn(user_id: int) -> bool:
    now = time.monotonic()
    if now - _last_warned[user_id] > WARN_COOLDOWN:
        _last_warned[user_id] = now
        return True
    return False

@dp.message.outer_middleware()
async def private_chat_only_middleware(handler, event: types.Message, data):
    """Бот — суто приватний (1-на-1) дейтинг-бот, у групах йому робити нічого.
    Якщо його все ж додали в групу і хтось написав команду, звичайна
    ReplyKeyboardMarkup (головне меню, кнопки реєстрації тощо) без
    selective=True показується Telegram-ом УСІМ учасникам групи, а не лише
    тому, хто написав — саме звідси "панель бота" з'являється у чужих людей.
    Тому в групах бот взагалі не обробляє звичайні повідомлення; на /start
    у групі відповідає один раз і просить писати в приват."""
    if event.chat.type != "private":
        if event.text and event.text.startswith("/start"):
            try:
                await event.answer(
                    "Привіт! Я працюю лише в особистих повідомленнях — напиши мені в приваті 🙂",
                    reply_markup=types.ReplyKeyboardRemove()
                )
            except Exception:
                pass
        return
    return await handler(event, data)

@dp.callback_query.outer_middleware()
async def private_chat_only_callback_middleware(handler, event: types.CallbackQuery, data):
    if event.message and event.message.chat.type != "private":
        try:
            await event.answer("Цей бот працює лише в приваті.", show_alert=True)
        except Exception:
            pass
        return
    return await handler(event, data)

@dp.my_chat_member()
async def leave_groups(event: types.ChatMemberUpdated):
    """Якщо бота додали в групу/канал — одразу виходимо. Це найнадійніший
    фікс: панель бота фізично не встигає "протекти" в чужі акаунти, бо бот
    не встигає нічого туди надіслати перед виходом."""
    if event.chat.type == "private":
        return
    if event.new_chat_member.user.id != bot.id:
        return
    if event.new_chat_member.status in ("member", "administrator"):
        try:
            await bot.send_message(
                event.chat.id,
                "Я особистий бот знайомств і працюю лише в приватних повідомленнях, тому залишаю цю групу 🙂 Пиши мені напряму!"
            )
        except Exception:
            pass
        try:
            await bot.leave_chat(event.chat.id)
        except Exception:
            pass

@dp.message.outer_middleware()
async def throttle_middleware(handler, event: types.Message, data):
    user_id = event.from_user.id if event.from_user else None
    if user_id and user_id != ADMIN_ID and _is_throttled(user_id):
        if _should_warn(user_id):
            try:
                await event.answer("⏳ Занадто швидко! Зачекай секунду.")
            except Exception:
                pass
        return
    return await handler(event, data)

@dp.callback_query.outer_middleware()
async def throttle_callback_middleware(handler, event: types.CallbackQuery, data):
    user_id = event.from_user.id if event.from_user else None
    if user_id and user_id != ADMIN_ID and _is_throttled(user_id):
        if _should_warn(user_id):
            try:
                await event.answer("⏳ Занадто швидко!", show_alert=False)
            except Exception:
                pass
        return
    return await handler(event, data)

@dp.message.outer_middleware()
async def ban_check_middleware(handler, event: types.Message, data):
    """Блокує будь-яку дію забаненого користувача (крім адміна)."""
    user_id = event.from_user.id if event.from_user else None
    if user_id and user_id != ADMIN_ID and await run_db(db_is_banned, user_id):
        await event.answer("⛔ Твій акаунт заблоковано адміністрацією бота.")
        return
    return await handler(event, data)

@dp.callback_query.outer_middleware()
async def ban_check_callback_middleware(handler, event: types.CallbackQuery, data):
    """Той самий бан-чек, але для інлайн-кнопок (лайки, гортання анкет тощо).
    Раніше middleware стояв тільки на message, тому забанений користувач не міг
    писати текстом, але міг вільно тиснути кнопки — це і закриваємо тут."""
    user_id = event.from_user.id if event.from_user else None
    if user_id and user_id != ADMIN_ID and await run_db(db_is_banned, user_id):
        await event.answer("⛔ Твій акаунт заблоковано адміністрацією бота.", show_alert=True)
        return
    return await handler(event, data)

# --- МОНІТОРИНГ ЗБОЇВ ---
# При необробленій помилці адмін отримує сповіщення в бота (не частіше, ніж раз
# на ADMIN_ALERT_COOLDOWN, щоб масовий збій не закидав адміна сотнями однакових
# повідомлень).
ADMIN_ALERT_COOLDOWN = 30.0
_last_admin_alert = 0.0

async def _notify_admin_of_crash(exc: Exception):
    global _last_admin_alert
    now = time.monotonic()
    if now - _last_admin_alert < ADMIN_ALERT_COOLDOWN:
        return
    _last_admin_alert = now
    try:
        text = f"🚨 <b>Помилка в боті</b>\n<code>{html.escape(f'{type(exc).__name__}: {exc}')[:500]}</code>"
        await bot.send_message(ADMIN_ID, text, parse_mode="HTML")
    except Exception:
        logging.exception("Не вдалося надіслати адміну сповіщення про збій")

@dp.errors()
async def global_error_handler(event: types.ErrorEvent):
    """Ловить будь-яку необроблену помилку в хендлерах (обрив з'єднання з БД,
    тимчасова недоступність Telegram API тощо), щоб бот не 'зависав' мовчки
    для користувача, а показував зрозуміле повідомлення."""
    logging.exception("Необроблена помилка під час обробки апдейту", exc_info=event.exception)
    asyncio.create_task(_notify_admin_of_crash(event.exception))

    # Флуд-ліміт Telegram (429) — це не "поломка", а сигнал почекати; окремо
    # користувачу нема сенсу казати "технічна помилка", досить просто не спамити далі.
    if isinstance(event.exception, TelegramRetryAfter):
        return True

    try:
        update = event.update
        if update.message:
            await update.message.answer(
                "😔 Ой, щось пішло не так на нашому боці. Ми вже отримали сповіщення "
                "про це. Спробуй ще раз за хвилину або напиши /start."
            )
        elif update.callback_query:
            await update.callback_query.answer(
                "Сталася технічна помилка, спробуй ще раз.", show_alert=True
            )
    except Exception:
        logging.exception("Не вдалося повідомити користувача про помилку")
    return True

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
    # Геолокація анкети — потрібна для пошуку "поруч зі мною".
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;')
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;')
    # Перемикач "Налаштування → 🔔 Сповіщення про лайки". Метч-сповіщення не вимикаємо
    # свідомо: це критична інформація (з'явився новий контакт), її не можна пропустити.
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS notify_likes INTEGER DEFAULT 1;')
    # Преміум-фічі (оплата Telegram Stars): буст анкети та повний список "хто лайкнув".
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS boost_until TIMESTAMP;')
    cursor.execute('ALTER TABLE profiles ADD COLUMN IF NOT EXISTS premium_likes_until TIMESTAMP;')

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
    # is_like — прод-база вже мала цю колонку як NOT NULL до цієї версії коду; тримаємо її
    # й тут з дефолтом TRUE, щоб CREATE TABLE з нуля (напр. на новому інстансі) теж працював.
    cursor.execute('ALTER TABLE likes ADD COLUMN IF NOT EXISTS is_like BOOLEAN NOT NULL DEFAULT TRUE;')

    # Індекс для геопошуку — прискорює вибірку анкет з відомими координатами.
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_profiles_location ON profiles (latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;')

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
            gender TEXT,
            radius_km INTEGER
        )
    ''')
    cursor.execute('ALTER TABLE search_filters ADD COLUMN IF NOT EXISTS radius_km INTEGER;')
    # Захист: піднімаємо всі наявні анкети з віком/цільовим віком нижче 18 до мінімуму 18,
    # і ховаємо з пошуку будь-які анкети з віком нижче 18 (на випадок, якщо такі
    # з'явилися до підняття мінімального віку реєстрації).
    cursor.execute('UPDATE profiles SET target_age_min = 18 WHERE target_age_min < 18;')
    cursor.execute('UPDATE profiles SET active = 0 WHERE age < 18;')
    cursor.execute('UPDATE search_filters SET age_min = 18 WHERE age_min IS NOT NULL AND age_min < 18;')

    # Скарги на анкети — раніше кнопка "🛑 Скарга" нічого не зберігала.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY,
            from_user_id BIGINT NOT NULL,
            target_user_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            reviewed INTEGER DEFAULT 0
        )
    ''')

    # Індекси для найчастіших запитів — без них стрічка й лайки з часом сповільнюються.
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_likes_to_user ON likes (to_user_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_likes_from_user ON likes (from_user_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_seen_user ON seen (user_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_profiles_active_age ON profiles (active, age);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_profiles_city ON profiles (LOWER(city));')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_reports_target ON reports (target_user_id);')

    conn.commit()
    cursor.close()
    conn.close()

init_db()

def db_save_profile(user_id, data):
    conn = db_pool.getconn()
    try:
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
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_profile(user_id):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, name, age, gender, target_gender, target_age_min, target_age_max, city, bio, photo, username, active, latitude, longitude, notify_likes FROM profiles WHERE user_id = %s', (user_id,))
        row = cursor.fetchone()
        cursor.close()
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
                'active': bool(row[11]),
                'latitude': row[12],
                'longitude': row[13],
                'notify_likes': bool(row[14]) if row[14] is not None else True
            }
        return None
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_update_location(user_id, latitude, longitude):
    """Зберігає геолокацію анкети (для пошуку 'поруч зі мною')."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('UPDATE profiles SET latitude = %s, longitude = %s WHERE user_id = %s', (latitude, longitude, user_id))
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_set_notify_likes(user_id, enabled: bool):
    """Вмикає/вимикає пуш-повідомлення 'твоєю анкетою хтось зацікавився'.
    Окрема функція (а не через db_save_profile), щоб редагування інших полів
    анкети випадково не скидало це налаштування назад."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
    finally:
        db_pool.putconn(conn)

def db_set_boost(user_id, minutes: int):
    """Активує/продовжує буст анкети (пріоритет у пошуку). Якщо буст вже активний,
    новий час додається до наявного залишку, а не перезаписує його."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE profiles
            SET boost_until = GREATEST(COALESCE(boost_until, NOW()), NOW()) + (%s || ' minutes')::interval
            WHERE user_id = %s
            """,
            (minutes, user_id)
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_set_premium_likes(user_id, days: int):
    """Активує/продовжує преміум-доступ до повного списку 'Хто мене лайкнув'."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE profiles
            SET premium_likes_until = GREATEST(COALESCE(premium_likes_until, NOW()), NOW()) + (%s || ' days')::interval
            WHERE user_id = %s
            """,
            (days, user_id)
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_premium_status(user_id):
    """Поточний статус преміум-фіч: чи активний буст і чи активний повний список лайків."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT boost_until, boost_until > NOW(), premium_likes_until, premium_likes_until > NOW()
            FROM profiles WHERE user_id = %s
            ''',
            (user_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        if not row:
            return {'boost_active': False, 'boost_until': None, 'premium_likes_active': False, 'premium_likes_until': None}
        return {
            'boost_until': row[0],
            'boost_active': bool(row[1]),
            'premium_likes_until': row[2],
            'premium_likes_active': bool(row[3]),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_all_pending_likes(user_id, limit=20):
    """Усі, хто лайкнув user_id і ще не був переглянутий — повний список одразу (Premium),
    на відміну від db_get_pending_like, яка віддає по одному через звичайну стрічку."""
    conn = db_pool.getconn()
    try:
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
            LIMIT %s
        ''', (user_id, user_id, limit))
        rows = cursor.fetchall()
        cursor.close()
        return [r[0] for r in rows]
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_count_likes_today(user_id):
    """Скільки лайків user_id вже поставив(ла) сьогодні (за UTC-добу сервера БД) —
    для денного ліміту безкоштовних користувачів."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM likes WHERE from_user_id = %s AND created_at >= date_trunc('day', NOW())",
            (user_id,)
        )
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_search_filters(user_id):
    """Повертає збережені фільтри пошуку користувача (місто, вік, стать, радіус)."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT city, age_min, age_max, gender, radius_km FROM search_filters WHERE user_id = %s', (user_id,))
        row = cursor.fetchone()
        cursor.close()
        if row:
            return {'city': row[0], 'age_min': row[1], 'age_max': row[2], 'gender': row[3], 'radius_km': row[4]}
        return {'city': None, 'age_min': None, 'age_max': None, 'gender': None, 'radius_km': None}
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_set_search_filter(user_id, **fields):
    """Оновлює один чи декілька фільтрів пошуку (city, age_min, age_max, gender, radius_km)."""
    current = db_get_search_filters(user_id)
    current.update(fields)
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO search_filters (user_id, city, age_min, age_max, gender, radius_km)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                city = EXCLUDED.city,
                age_min = EXCLUDED.age_min,
                age_max = EXCLUDED.age_max,
                gender = EXCLUDED.gender,
                radius_km = EXCLUDED.radius_km
        ''', (user_id, current['city'], current['age_min'], current['age_max'], current['gender'], current['radius_km']))
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_reset_search_filters(user_id):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM search_filters WHERE user_id = %s', (user_id,))
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_next_profile(current_user_id):
    current_profile = db_get_profile(current_user_id)
    if not current_profile:
        return None, None

    filters = db_get_search_filters(current_user_id)
    min_age = filters.get('age_min') or current_profile.get('target_age_min', 18)
    max_age = filters.get('age_max') or current_profile.get('target_age_max', 99)
    target_gender = filters.get('gender') or current_profile.get('target_gender', 'Усіх 🌈')
    target_city = filters.get('city')
    radius_km = filters.get('radius_km')

    my_lat = current_profile.get('latitude')
    my_lon = current_profile.get('longitude')
    use_location = bool(radius_km) and my_lat is not None and my_lon is not None

    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()

        gender_clause = ""
        if target_gender == "Дівчат 👩":
            gender_clause = " AND gender = 'Дівчина 👩'"
        elif target_gender == "Хлопців 👨":
            gender_clause = " AND gender = 'Хлопець 👨'"

        if use_location:
            # Формула гаверсинуса — рахує відстань між координатами (у км) прямо в SQL,
            # щоб можна було відсортувати анкети від найближчої до найдальшої.
            distance_expr = '''
                (6371 * acos(LEAST(1.0, GREATEST(-1.0,
                    cos(radians(%s)) * cos(radians(latitude)) * cos(radians(longitude) - radians(%s))
                    + sin(radians(%s)) * sin(radians(latitude))
                ))))
            '''
            query = f'''
                SELECT * FROM (
                    SELECT user_id, name, age, gender, target_gender, target_age_min, target_age_max,
                           city, bio, photo, username, active, latitude, longitude,
                           {distance_expr} AS distance_km,
                           (boost_until IS NOT NULL AND boost_until > NOW()) AS is_boosted
                    FROM profiles
                    WHERE user_id != %s AND active = 1 AND age BETWEEN %s AND %s
                      AND latitude IS NOT NULL AND longitude IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM seen s WHERE s.user_id = %s AND s.target_id = profiles.user_id
                      )
                      {gender_clause}
                ) sub
                WHERE distance_km <= %s
                ORDER BY is_boosted DESC, distance_km ASC
                LIMIT 1
            '''
            params = [my_lat, my_lon, my_lat, current_user_id, min_age, max_age, current_user_id, radius_km]
        else:
            query = f'''
                SELECT user_id, name, age, gender, target_gender, target_age_min, target_age_max,
                       city, bio, photo, username, active, latitude, longitude, NULL AS distance_km
                FROM profiles
                WHERE user_id != %s AND active = 1 AND age BETWEEN %s AND %s
                  AND NOT EXISTS (
                      SELECT 1 FROM seen s WHERE s.user_id = %s AND s.target_id = profiles.user_id
                  )
                  {gender_clause}
            '''
            params = [current_user_id, min_age, max_age, current_user_id]

            if target_city:
                query += ' AND LOWER(city) = LOWER(%s)'
                params.append(target_city)

            query += ' ORDER BY (boost_until IS NOT NULL AND boost_until > NOW()) DESC, RANDOM() LIMIT 1'

        cursor.execute(query, params)
        row = cursor.fetchone()
        cursor.close()

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
            'active': bool(row[11]),
            'latitude': row[12],
            'longitude': row[13],
            'distance_km': row[14]
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_add_like(from_user_id, to_user_id, comment=None):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO likes (from_user_id, to_user_id, is_like, comment)
            VALUES (%s, %s, TRUE, %s)
            ON CONFLICT (from_user_id, to_user_id)
            DO UPDATE SET is_like = TRUE, comment = EXCLUDED.comment;
            """,
            (from_user_id, to_user_id, comment)
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_like_comment(from_user_id, to_user_id):
    """Коментар, який from_user_id залишив(ла) до лайка на анкету to_user_id (якщо є)."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT comment FROM likes WHERE from_user_id = %s AND to_user_id = %s',
            (from_user_id, to_user_id)
        )
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row and row[0] else None
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_check_mutual_like(user_a, user_b):
    """Чи user_b вже лайкнув user_a раніше (для миттєвого визначення метчу)."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT 1 FROM likes WHERE from_user_id = %s AND to_user_id = %s',
            (user_b, user_a)
        )
        row = cursor.fetchone()
        cursor.close()
        return row is not None
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_add_seen(user_id, target_id):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO seen (user_id, target_id) VALUES (%s, %s) ON CONFLICT DO NOTHING',
            (user_id, target_id)
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_remove_seen(user_id, target_id):
    """Прибирає позначку 'переглянуто' — потрібно для відкату останнього свайпу."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM seen WHERE user_id = %s AND target_id = %s', (user_id, target_id))
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_remove_like(from_user_id, to_user_id):
    """Прибирає лайк — потрібно для відкату останнього свайпу (якщо це був лайк без метчу)."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM likes WHERE from_user_id = %s AND to_user_id = %s', (from_user_id, to_user_id))
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_add_report(from_user_id, target_user_id):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO reports (from_user_id, target_user_id) VALUES (%s, %s)',
            (from_user_id, target_user_id)
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_count_open_reports():
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM reports WHERE reviewed = 0')
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_pending_like(user_id):
    """ID користувача, який лайкнув user_id і якого user_id ще не бачив (черга 'тебе лайкнули')."""
    conn = db_pool.getconn()
    try:
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
        return row[0] if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_count_pending_likes(user_id):
    """Скільки людей лайкнули user_id і ще не були переглянуті (черга 'тебе лайкнули')."""
    conn = db_pool.getconn()
    try:
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
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_matches(user_id):
    """Список user_id, з якими є взаємний лайк (метч)."""
    conn = db_pool.getconn()
    try:
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
        return [r[0] for r in rows]
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_profiles_count():
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM profiles')
        total_count = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM profiles WHERE active = 1')
        active_count = cursor.fetchone()[0]
        cursor.close()
        return total_count, active_count
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

# --- АДМІН-ФУНКЦІЇ ---

def db_get_detailed_stats():
    """Розширена статистика для адмін-панелі: анкети, лайки, метчі, топ міст."""
    conn = db_pool.getconn()
    try:
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
        cursor.execute('SELECT COUNT(*) FROM reports WHERE reviewed = 0')
        open_reports = cursor.fetchone()[0]
        cursor.close()
        return {
            'total': total, 'active': active, 'banned': banned,
            'likes_total': likes_total, 'matches_total': matches_total,
            'by_gender': by_gender, 'top_cities': top_cities,
            'open_reports': open_reports,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_get_all_user_ids():
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM profiles')
        rows = cursor.fetchall()
        cursor.close()
        return [r[0] for r in rows]
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_is_banned(user_id):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT banned FROM profiles WHERE user_id = %s', (user_id,))
        row = cursor.fetchone()
        cursor.close()
        return bool(row[0]) if row else False
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_set_banned(user_id, banned: bool):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        if banned:
            cursor.execute('UPDATE profiles SET banned = 1, active = 0 WHERE user_id = %s', (user_id,))
        else:
            cursor.execute('UPDATE profiles SET banned = 0 WHERE user_id = %s', (user_id,))
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_delete_profile(user_id):
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM likes WHERE from_user_id = %s OR to_user_id = %s', (user_id, user_id))
        cursor.execute('DELETE FROM seen WHERE user_id = %s OR target_id = %s', (user_id, user_id))
        cursor.execute('DELETE FROM search_filters WHERE user_id = %s', (user_id,))
        cursor.execute('DELETE FROM profiles WHERE user_id = %s', (user_id,))
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_admin_get_random_profile(admin_id, gender_filter=None):
    """Випадкова анкета для адмін-перегляду. Ігнорує 'seen' (можна бачити навіть уже лайкані)
    та 'active' (адмін бачить і приховані анкети). gender_filter: 'Хлопець 👨' / 'Дівчина 👩' / None (усі)."""
    conn = db_pool.getconn()
    try:
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
        if not row:
            return None
        return {
            'user_id': row[0], 'name': row[1], 'age': row[2], 'gender': row[3],
            'target_gender': row[4], 'target_age_min': row[5], 'target_age_max': row[6],
            'city': row[7], 'bio': row[8], 'photo': row[9], 'username': row[10], 'active': bool(row[11])
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_list_profiles(search="", gender=None, limit=30, offset=0):
    """Список анкет для веб-адмінки: пошук по імені/місту/юзернейму, фільтр за статтю, пагінація."""
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        query = '''
            SELECT user_id, name, age, gender, city, username, active, banned
            FROM profiles
            WHERE 1=1
        '''
        params = []
        if search:
            query += " AND (name ILIKE %s OR city ILIKE %s OR username ILIKE %s OR CAST(user_id AS TEXT) = %s)"
            like = f"%{search}%"
            params += [like, like, like, search]
        if gender:
            query += " AND gender = %s"
            params.append(gender)
        query += " ORDER BY user_id DESC LIMIT %s OFFSET %s"
        params += [limit, offset]

        cursor.execute(query, params)
        rows = cursor.fetchall()

        count_query = "SELECT COUNT(*) FROM profiles WHERE 1=1"
        count_params = []
        if search:
            count_query += " AND (name ILIKE %s OR city ILIKE %s OR username ILIKE %s OR CAST(user_id AS TEXT) = %s)"
            count_params += [like, like, like, search]
        if gender:
            count_query += " AND gender = %s"
            count_params.append(gender)
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()[0]

        cursor.close()
        items = [
            {
                'user_id': r[0], 'name': r[1], 'age': r[2], 'gender': r[3],
                'city': r[4], 'username': r[5], 'active': bool(r[6]), 'banned': bool(r[7]),
            }
            for r in rows
        ]
        return items, total
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

def db_update_profile_fields(user_id, **fields):
    """Часткове оновлення анкети (name/age/city/bio) з веб-адмінки."""
    allowed = {'name', 'age', 'city', 'bio'}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        set_clause = ", ".join(f"{k} = %s" for k in updates)
        cursor.execute(
            f"UPDATE profiles SET {set_clause} WHERE user_id = %s",
            list(updates.values()) + [user_id]
        )
        conn.commit()
        cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)

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
    new_location = State()

class SearchFilterState(StatesGroup):
    filter_city = State()
    filter_age = State()
    filter_location = State()

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
            [KeyboardButton(text="💎 Преміум"), KeyboardButton(text="💙 Підтримати бота")]
        ],
        resize_keyboard=True
    )

def premium_menu_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🚀 Буст анкети ({STARS_BOOST_MINUTES} хв) — {STARS_BOOST_PRICE}⭐", callback_data="buy_boost")],
            [InlineKeyboardButton(text=f"📋 Список лайків + безліміт ({STARS_PREMIUM_LIKES_DAYS} дн.) — {STARS_PREMIUM_LIKES_PRICE}⭐", callback_data="buy_premium_likes")]
        ]
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
            [InlineKeyboardButton(text="📸 Оновити фото", callback_data="edit_photo"), InlineKeyboardButton(text="📍 Оновити геолокацію", callback_data="edit_location")],
            [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_profile")]
        ]
    )

def search_options_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏙 Пошук за містом", callback_data="search_by_city")],
            [InlineKeyboardButton(text="📍 Пошук за геолокацією", callback_data="search_by_location")],
            [InlineKeyboardButton(text="🎂 Віковий діапазон", callback_data="search_by_age")],
            [InlineKeyboardButton(text="🚻 Кого шукати", callback_data="search_by_gender")],
            [InlineKeyboardButton(text="🔄 Скинути фільтри пошуку", callback_data="reset_search_filters")]
        ]
    )

def location_request_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Надіслати мою геолокацію", request_location=True)],
            [KeyboardButton(text="🚫 Скасувати")]
        ],
        resize_keyboard=True, one_time_keyboard=True
    )

def search_radius_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="5 км", callback_data="search_radius_5"), InlineKeyboardButton(text="10 км", callback_data="search_radius_10")],
            [InlineKeyboardButton(text="25 км", callback_data="search_radius_25"), InlineKeyboardButton(text="50 км", callback_data="search_radius_50")],
            [InlineKeyboardButton(text="100 км", callback_data="search_radius_100")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="search_back")]
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

def feed_inline_keyboard(target_uid):
    """Кнопки дій прикріплені прямо під фото анкети — так дію видно поруч з
    об'єктом дії, і не треба шукати кнопку десь під полем вводу."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="👎", callback_data=f"swipe_no_{target_uid}"),
                InlineKeyboardButton(text="❤️", callback_data=f"swipe_yes_{target_uid}"),
            ],
            [
                InlineKeyboardButton(text="💌 Лайк з коментарем", callback_data=f"swipe_comment_{target_uid}"),
            ],
            [
                InlineKeyboardButton(text="⏪ Відкат", callback_data="swipe_undo"),
                InlineKeyboardButton(text="🛑 Скарга", callback_data=f"swipe_report_{target_uid}"),
                InlineKeyboardButton(text="🏠 Меню", callback_data="swipe_exit"),
            ],
        ]
    )

def settings_keyboard(notify_likes: bool):
    toggle_text = "🔕 Вимкнути сповіщення про лайки" if notify_likes else "🔔 Увімкнути сповіщення про лайки"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text, callback_data="toggle_notify_likes")]
        ]
    )

def admin_panel_keyboard():
    buttons = [
        [InlineKeyboardButton(text="📊 Детальна статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📢 Розсилка всім користувачам", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🔍 Знайти анкету за ID", callback_data="admin_lookup")],
        [InlineKeyboardButton(text="👀 Переглянути всі анкети", callback_data="admin_browse")],
    ]
    if WEBAPP_BASE_URL:
        buttons.append([InlineKeyboardButton(
            text="🖥 Відкрити адмін-сайт",
            web_app=WebAppInfo(url=f"{WEBAPP_BASE_URL}/admin")
        )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

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

def escape_md(text) -> str:
    """Escape Telegram legacy-Markdown special chars in user-supplied text
    so a stray *, _, ` or [ in a name/bio/comment can't break formatting
    or throw TelegramBadRequest."""
    if text is None:
        return ""
    return re.sub(r'([_*`\[])', r'\\\1', str(text))

def format_profile(profile: dict) -> str:
    status = "🟢 Активна" if profile.get('active', True) else "🔴 Прихована з пошуку"
    return (
        f"📌 **{escape_md(profile['name'])}**, 🎂 {profile['age']}, 🏙 {escape_md(profile['city'])}\n"
        f"➖➖➖➖➖➖➖➖\n"
        f"📝 {escape_md(profile['bio'])}\n\n"
        f"Статус анкети: {status}"
    )

async def show_profile(user_id: int, target_uid, profile, like_comment=None):
    """Показує картку анкети конкретному user_id (не прив'язано до вхідного
    Message — картку треба вміти надіслати як у відповідь на текст, так і у
    відповідь на натискання inline-кнопки)."""
    caption = (
        f"📌 <b>{html.escape(profile['name'])}</b>, 🎂 {profile['age']}, 🏙 {html.escape(profile['city'])}\n"
        f"➖➖➖➖➖➖➖➖\n"
        f"📝 {html.escape(profile['bio'])}"
    )
    distance_km = profile.get('distance_km')
    if distance_km is not None:
        caption += f"\n📍 ~{round(distance_km)} км від тебе"
    if like_comment:
        caption += f"\n\n💌 <b>Коментар до лайка:</b>\n{html.escape(like_comment)}"

    kb = feed_inline_keyboard(target_uid)

    if profile.get('photo'):
        try:
            await bot.send_photo(
                chat_id=user_id,
                photo=profile['photo'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=kb
            )
            return
        except TelegramBadRequest:
            pass

    await bot.send_message(user_id, caption, parse_mode="HTML", reply_markup=kb)

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
    profile = await run_db(db_get_profile, user_id)
    if profile:
        await message.answer("З поверненням у **Дайвінчик UA** 🇺🇦!", parse_mode="Markdown", reply_markup=main_menu_keyboard())
    else:
        await message.answer(
            f"Привіт, {message.from_user.first_name}! 👋\n"
            f"Вітаємо у **Дайвінчик UA** 🇺🇦!\n\n{reg_step(1)} · Давай створимо твою анкету. Як тебе звати?",
            parse_mode="Markdown"
        )
        await state.set_state(ProfileRegistration.name)

REG_TOTAL_STEPS = 7

def reg_step(n: int) -> str:
    return f"Крок {n}/{REG_TOTAL_STEPS}"

@dp.message(ProfileRegistration.name)
async def process_name(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("/") or message.text in ["🚀 Дивитися анкети", "🔍 Пошук", "💞 Мої метчі", "❤️ Хто мене лайкнув", "👤 Моя анкета", "⚙️ Налаштування", "💎 Преміум", "💙 Підтримати бота"]):
        await state.clear()
        await message.answer("Реєстрацію перервано.", reply_markup=main_menu_keyboard())
        return
    await state.update_data(name=message.text, username=message.from_user.username)
    await message.answer(f"{reg_step(2)} · Скільки тобі років?")
    await state.set_state(ProfileRegistration.age)

@dp.message(ProfileRegistration.age)
async def process_age(message: types.Message, state: FSMContext):
    if message.text and (message.text.startswith("/") or message.text in ["🚀 Дивитися анкети", "🔍 Пошук", "💞 Мої метчі", "❤️ Хто мене лайкнув", "👤 Моя анкета", "⚙️ Налаштування", "💎 Преміум", "💙 Підтримати бота"]):
        await state.clear()
        await message.answer("Реєстрацію перервано.", reply_markup=main_menu_keyboard())
        return

    if not message.text or not message.text.isdigit() or not (18 <= int(message.text) <= 99):
        await message.answer("Реєстрація доступна лише з 18 років. Вкажи реальний вік числом (наприклад, 19):")
        return
        
    await state.update_data(age=int(message.text))
    await message.answer(f"{reg_step(3)} · Вкажи свою стать:", reply_markup=gender_keyboard())
    await state.set_state(ProfileRegistration.gender)

@dp.message(ProfileRegistration.gender)
async def process_gender(message: types.Message, state: FSMContext):
    if message.text not in ["Хлопець 👨", "Дівчина 👩"]:
        await message.answer("Обери варіант з кнопок нижче:", reply_markup=gender_keyboard())
        return
    await state.update_data(gender=message.text)
    await message.answer(f"{reg_step(4)} · Кого ти шукаєш?", reply_markup=target_gender_keyboard())
    await state.set_state(ProfileRegistration.target_gender)

@dp.message(ProfileRegistration.target_gender)
async def process_target_gender(message: types.Message, state: FSMContext):
    await state.update_data(target_gender=message.text, target_age_min=18, target_age_max=99)
    await message.answer(f"{reg_step(5)} · З якого ти міста?", reply_markup=ReplyKeyboardRemove())
    await state.set_state(ProfileRegistration.city)

@dp.message(ProfileRegistration.city)
async def process_city(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Напиши назву міста текстом, будь ласка:")
        return
    await state.update_data(city=message.text)
    await message.answer(f"{reg_step(6)} · Напиши короткий опис про себе (хто ти, чим захоплюєшся):")
    await state.set_state(ProfileRegistration.bio)

@dp.message(ProfileRegistration.bio)
async def process_bio(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Напиши короткий опис про себе текстом, будь ласка:")
        return
    await state.update_data(bio=message.text)
    await message.answer(f"{reg_step(7)} · Надішли своє фото для анкети 📸:")
    await state.set_state(ProfileRegistration.photo)

@dp.message(ProfileRegistration.photo, F.photo)
async def process_photo(message: types.Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    data['photo'] = photo_id
    data['active'] = True
    
    await run_db(db_save_profile, message.from_user.id, data)
    await state.clear()
    
    await message.answer("🎉 **Анкету створено успішно!**", parse_mode="Markdown", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

# --- РЕЖИМ ПОШУКУ ТА ФІЛЬТРІВ ---

def format_search_filters_text(filters: dict) -> str:
    city = escape_md(filters.get('city')) or 'Усі міста'
    if filters.get('age_min') and filters.get('age_max'):
        age = f"{filters['age_min']}–{filters['age_max']}"
    else:
        age = "як в анкеті"
    gender = filters.get('gender') or "як в анкеті"
    radius_km = filters.get('radius_km')
    location_line = f"📍 Радіус пошуку: **{radius_km} км** від твоєї геолокації\n" if radius_km else ""
    return (
        f"🔍 **Налаштування пошуку**\n\n"
        f"🏙 Місто: **{city}**\n"
        f"{location_line}"
        f"🎂 Вік: **{age}**\n"
        f"🚻 Кого шукати: **{gender}**\n\n"
        f"Обери параметр, щоб змінити, або скинь фільтри:"
    )

@dp.message(F.text == "🔍 Пошук")
@dp.message(Command("search"))
async def search_menu(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if not await run_db(db_get_profile, user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    filters = await run_db(db_get_search_filters, user_id)
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

    await run_db(db_set_search_filter, user_id, city=target_city)
    await state.clear()

    await message.answer(
        f"✅ Фільтр встановлено: шукаємо анкети в місті **{escape_md(target_city)}**!\n"
        f"Натисни «🚀 Дивитися анкети», щоб розпочати перегляд.",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )

@dp.callback_query(F.data == "search_by_location")
async def ask_search_location(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer(
        "📍 Надішли свою геолокацію кнопкою нижче, щоб шукати анкети поруч із тобою "
        "(або натисни «🚫 Скасувати»):",
        reply_markup=location_request_keyboard()
    )
    await state.set_state(SearchFilterState.filter_location)
    await call.answer()

@dp.message(SearchFilterState.filter_location, F.location)
async def set_search_location(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    lat, lon = message.location.latitude, message.location.longitude
    await run_db(db_update_location, user_id, lat, lon)
    await state.clear()
    await message.answer("✅ Геолокацію збережено!", reply_markup=ReplyKeyboardRemove())
    await message.answer("Тепер обери радіус пошуку:", reply_markup=search_radius_keyboard())

@dp.message(SearchFilterState.filter_location)
async def invalid_search_location(message: types.Message):
    await message.answer(
        "Будь ласка, надішли геолокацію кнопкою «📍 Надіслати мою геолокацію» нижче, "
        "або натисни «🚫 Скасувати»."
    )

@dp.callback_query(F.data.startswith("search_radius_"))
async def set_search_radius(call: types.CallbackQuery):
    user_id = call.from_user.id
    try:
        radius = int(call.data.replace("search_radius_", ""))
    except ValueError:
        await call.answer()
        return

    await run_db(db_set_search_filter, user_id, radius_km=radius)
    filters = await run_db(db_get_search_filters, user_id)
    await call.answer(f"Радіус пошуку: {radius} км", show_alert=True)
    await call.message.edit_text(
        format_search_filters_text(filters),
        parse_mode="Markdown",
        reply_markup=search_options_keyboard()
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

    await run_db(db_set_search_filter, user_id, age_min=age_min, age_max=age_max)
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
    filters = await run_db(db_get_search_filters, user_id)
    await call.message.edit_text(
        format_search_filters_text(filters),
        parse_mode="Markdown",
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

    await run_db(db_set_search_filter, user_id, gender=gender)
    filters = await run_db(db_get_search_filters, user_id)
    await call.answer(f"Обрано: {gender}", show_alert=True)
    await call.message.edit_text(
        format_search_filters_text(filters),
        reply_markup=search_options_keyboard()
    )

@dp.callback_query(F.data == "reset_search_filters")
async def reset_search_filters(call: types.CallbackQuery):
    user_id = call.from_user.id
    await run_db(db_reset_search_filters, user_id)
    await call.answer("Фільтри скинуто! Шукаємо за налаштуваннями анкети.", show_alert=True)
    filters = await run_db(db_get_search_filters, user_id)
    await call.message.edit_text(
        format_search_filters_text(filters),
        reply_markup=search_options_keyboard()
    )

# --- МЕНЮ "МОЯ АНКЕТА" ТА РЕДАКТУВАННЯ ---

async def show_my_profile_logic(message: types.Message):
    user_id = message.from_user.id
    p = await run_db(db_get_profile, user_id)
    if not p:
        await message.answer("У тебе ще немає анкети. Напиши /start для реєстрації.")
        return
    
    caption = f"Твоя анкета:\n\n{format_profile(p)}"

    if p.get('photo'):
        try:
            await message.answer_photo(
                photo=p['photo'],
                caption=caption,
                parse_mode="Markdown",
                reply_markup=my_profile_keyboard(p.get('active', True))
            )
            return
        except TelegramBadRequest:
            # Збережене фото більше не валідне — очищуємо, щоб не падати знову
            p['photo'] = None
            await run_db(db_save_profile, user_id, p)

    await message.answer(
        caption + "\n\n⚠️ Твоє фото пошкоджене або застаріле, онови його.",
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
    p = await run_db(db_get_profile, user_id)
    if p:
        p['active'] = not p['active']
        await run_db(db_save_profile, user_id, p)
        new_status = "активовано" if p['active'] else "приховано з пошуку"
        await call.answer(f"Анкету {new_status}!", show_alert=True)
        await call.message.delete()
        await show_my_profile_logic(call.message)

@dp.callback_query(F.data == "toggle_notify_likes")
async def toggle_notify_likes_cb(call: types.CallbackQuery):
    user_id = call.from_user.id
    profile = await run_db(db_get_profile, user_id)
    if not profile:
        await call.answer()
        return
    new_value = not profile.get('notify_likes', True)
    await run_db(db_set_notify_likes, user_id, new_value)
    status_text = "Сповіщення про лайки увімкнено 🔔" if new_value else "Сповіщення про лайки вимкнено 🔕"
    await call.answer(status_text, show_alert=True)
    try:
        await call.message.edit_reply_markup(reply_markup=settings_keyboard(new_value))
    except TelegramBadRequest:
        pass

@dp.callback_query(F.data == "edit_profile")
async def edit_profile_menu(call: types.CallbackQuery):
    try:
        await call.message.edit_caption(
            caption="Обери, який пункт ти хочеш змінити:",
            reply_markup=edit_fields_keyboard()
        )
    except TelegramBadRequest:
        # Повідомлення текстове (без фото), а не фото з підписом
        await call.message.edit_text(
            "Обери, який пункт ти хочеш змінити:",
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
    p = await run_db(db_get_profile, message.from_user.id)
    if p:
        p['name'] = message.text
        await run_db(db_save_profile, message.from_user.id, p)
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
    p = await run_db(db_get_profile, message.from_user.id)
    if p:
        p['age'] = int(message.text)
        await run_db(db_save_profile, message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Вік оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_city")
async def edit_city(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи нове місто:")
    await state.set_state(EditProfileState.new_city)

@dp.message(EditProfileState.new_city)
async def process_new_city(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Напиши назву міста текстом, будь ласка:")
        return
    p = await run_db(db_get_profile, message.from_user.id)
    if p:
        p['city'] = message.text
        await run_db(db_save_profile, message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Місто оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_bio")
async def edit_bio(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Напиши новий опис про себе:")
    await state.set_state(EditProfileState.new_bio)

@dp.message(EditProfileState.new_bio)
async def process_new_bio(message: types.Message, state: FSMContext):
    if not message.text:
        await message.answer("Напиши опис текстом, будь ласка:")
        return
    p = await run_db(db_get_profile, message.from_user.id)
    if p:
        p['bio'] = message.text
        await run_db(db_save_profile, message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Опис оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_photo")
async def edit_photo(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Надішли нову світлину 📸:")
    await state.set_state(EditProfileState.new_photo)

@dp.message(EditProfileState.new_photo, F.photo)
async def process_new_photo(message: types.Message, state: FSMContext):
    p = await run_db(db_get_profile, message.from_user.id)
    if p:
        p['photo'] = message.photo[-1].file_id
        await run_db(db_save_profile, message.from_user.id, p)
    await state.clear()
    await message.answer("✅ Фото оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.callback_query(F.data == "edit_location")
async def edit_location(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer(
        "📍 Надішли свою геолокацію кнопкою нижче (або натисни «🚫 Скасувати»):",
        reply_markup=location_request_keyboard()
    )
    await state.set_state(EditProfileState.new_location)

@dp.message(EditProfileState.new_location, F.location)
async def process_new_location(message: types.Message, state: FSMContext):
    lat, lon = message.location.latitude, message.location.longitude
    await run_db(db_update_location, message.from_user.id, lat, lon)
    await state.clear()
    await message.answer("✅ Геолокацію оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile_logic(message)

@dp.message(EditProfileState.new_location)
async def invalid_new_location(message: types.Message):
    await message.answer(
        "Будь ласка, надішли геолокацію кнопкою «📍 Надіслати мою геолокацію» нижче, "
        "або натисни «🚫 Скасувати»."
    )

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
    if not await run_db(db_get_profile, user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    matches = await run_db(db_get_matches, user_id)
    if not matches:
        await message.answer(
            "У тебе поки немає метчів 💔\nПродовжуй переглядати анкети — і хтось обов'язково відповість взаємністю!",
            reply_markup=main_menu_keyboard()
        )
        return

    await message.answer(f"💞 У тебе {len(matches)} метч(ів)!", reply_markup=main_menu_keyboard())
    for target_uid in matches[:20]:
        prof = await run_db(db_get_profile, target_uid)
        if not prof:
            continue
        caption = f"📌 **{escape_md(prof['name'])}**, {prof['age']}, {escape_md(prof['city'])}\n📝 {escape_md(prof['bio'])}"

        sent = False
        if prof.get('photo'):
            try:
                await message.answer_photo(
                    photo=prof['photo'],
                    caption=caption,
                    parse_mode="Markdown",
                    reply_markup=match_card_keyboard(target_uid)
                )
                sent = True
            except TelegramBadRequest:
                pass

        if not sent:
            await message.answer(caption, parse_mode="Markdown", reply_markup=match_card_keyboard(target_uid))

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
    if not await run_db(db_get_profile, user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    count = await run_db(db_count_pending_likes, user_id)
    if count == 0:
        await message.answer("Поки що ніхто новий тебе не лайкнув 😉 Продовжуй переглядати анкети!", reply_markup=main_menu_keyboard())
        return

    status = await run_db(db_get_premium_status, user_id)
    if status['premium_likes_active']:
        liker_ids = await run_db(db_get_all_pending_likes, user_id, 20)
        await message.answer(f"🔥 Тебе лайкнуло {count} людей! Ось усі одразу 👇", reply_markup=main_menu_keyboard())
        for liker_id in liker_ids:
            prof = await run_db(db_get_profile, liker_id)
            if not prof:
                continue
            like_comment = await run_db(db_get_like_comment, liker_id, user_id)
            caption = f"📌 **{escape_md(prof['name'])}**, {prof['age']}, {escape_md(prof['city'])}\n📝 {escape_md(prof['bio'])}"
            if like_comment:
                caption += f"\n\n💌 Коментар до лайка: _{escape_md(like_comment)}_"
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="❤️ Лайкнути у відповідь", callback_data=f"premlike_yes_{liker_id}"),
                InlineKeyboardButton(text="👎 Пропустити", callback_data=f"premlike_no_{liker_id}")
            ]])
            sent = False
            if prof.get('photo'):
                try:
                    await message.answer_photo(photo=prof['photo'], caption=caption, parse_mode="Markdown", reply_markup=kb)
                    sent = True
                except TelegramBadRequest:
                    pass
            if not sent:
                await message.answer(caption, parse_mode="Markdown", reply_markup=kb)
        return

    await message.answer(
        f"🔥 Тебе лайкнуло {count} людей! Дивимось по одному, хто саме 👇\n\n"
        f"💎 Хочеш бачити всіх одразу? Оформи повний список у розділі «💎 Преміум»."
    )
    await start_feed(message, state)

@dp.callback_query(F.data.startswith("premlike_yes_"))
async def premlike_yes_cb(call: types.CallbackQuery):
    user_id = call.from_user.id
    liker_id = int(call.data.replace("premlike_yes_", ""))
    await run_db(db_add_seen, user_id, liker_id)
    await run_db(db_add_like, user_id, liker_id)
    await call.answer("❤️")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await notify_match(user_id, liker_id)

@dp.callback_query(F.data.startswith("premlike_no_"))
async def premlike_no_cb(call: types.CallbackQuery):
    user_id = call.from_user.id
    liker_id = int(call.data.replace("premlike_no_", ""))
    await run_db(db_add_seen, user_id, liker_id)
    await call.answer("Пропущено")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

# --- ПРЕМІУМ: БУСТ АНКЕТИ ТА ПОВНИЙ СПИСОК ЛАЙКІВ (Telegram Stars) ---

@dp.message(F.text == "💎 Преміум")
async def premium_menu(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if not await run_db(db_get_profile, user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    status = await run_db(db_get_premium_status, user_id)
    lines = ["💎 **Преміум-можливості**"]

    if status['boost_active']:
        lines.append(f"\n🚀 Буст **активний** до {status['boost_until'].strftime('%H:%M %d.%m')}. Можеш продовжити його нижче.")
    else:
        lines.append(
            f"\n🚀 **Буст анкети** — твоя анкета показується першою всім, хто підходить "
            f"під критерії пошуку, протягом {STARS_BOOST_MINUTES} хв."
        )

    if status['premium_likes_active']:
        lines.append(
            f"\n📋 Преміум **активний** до {status['premium_likes_until'].strftime('%d.%m.%Y')} — "
            f"повний список \"хто лайкнув\" і безлімітні лайки. Можеш продовжити нижче."
        )
    else:
        lines.append(
            f"\n📋 **Повний список \"Хто мене лайкнув\" + безлімітні лайки** — бачиш одразу всіх, хто тебе лайкнув "
            f"(замість перегляду по одному), і знімається денний ліміт лайків ({FREE_DAILY_LIKE_LIMIT}/день без преміуму), "
            f"на {STARS_PREMIUM_LIKES_DAYS} днів."
        )

    lines.append("\nОплата — через Telegram Stars ⭐, прямо в боті.")

    await message.answer("\n".join(lines), parse_mode="Markdown", reply_markup=premium_menu_keyboard())

@dp.callback_query(F.data == "buy_boost")
async def buy_boost_cb(call: types.CallbackQuery):
    await call.answer()
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title="🚀 Буст анкети",
        description=f"Твоя анкета стане пріоритетною в пошуку на {STARS_BOOST_MINUTES} хвилин.",
        payload="boost",
        currency="XTR",
        prices=[LabeledPrice(label="Буст анкети", amount=STARS_BOOST_PRICE)],
        provider_token=""
    )

@dp.callback_query(F.data == "buy_premium_likes")
async def buy_premium_likes_cb(call: types.CallbackQuery):
    await call.answer()
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title="📋 Повний список лайків",
        description=f"Бач одразу всіх, хто тебе лайкнув, протягом {STARS_PREMIUM_LIKES_DAYS} днів.",
        payload="premium_likes",
        currency="XTR",
        prices=[LabeledPrice(label="Преміум-доступ до лайків", amount=STARS_PREMIUM_LIKES_PRICE)],
        provider_token=""
    )

@dp.pre_checkout_query()
async def process_pre_checkout(pre_checkout_q: types.PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)

@dp.message(F.successful_payment)
async def process_successful_payment(message: types.Message):
    payload = message.successful_payment.invoice_payload
    user_id = message.from_user.id
    if payload == "boost":
        await run_db(db_set_boost, user_id, STARS_BOOST_MINUTES)
        await message.answer(
            f"🚀 Буст активовано на {STARS_BOOST_MINUTES} хвилин! Твоя анкета тепер пріоритетна в пошуку.",
            reply_markup=main_menu_keyboard()
        )
    elif payload == "premium_likes":
        await run_db(db_set_premium_likes, user_id, STARS_PREMIUM_LIKES_DAYS)
        await message.answer(
            f"📋 Преміум-доступ активовано на {STARS_PREMIUM_LIKES_DAYS} днів! Тепер відкрий "
            f"«❤️ Хто мене лайкнув», щоб побачити всіх одразу.",
            reply_markup=main_menu_keyboard()
        )
    else:
        logging.warning("Невідомий payload у successful_payment: %s", payload)

# --- ГОРТАННЯ АНКЕТ (ФІД) ---
# Дії (лайк/дизлайк/скарга/коментар) прикріплені inline-кнопками прямо під
# фото анкети (див. feed_inline_keyboard) — тому вся ця логіка працює через
# user_id/chat_id напряму, а не через конкретний вхідний types.Message: її
# однаково викликають і з тексту ("🚀 Дивитися анкети"), і з callback_query
# (натискання кнопки під карткою).

async def _enter_feed(user_id: int, state: FSMContext):
    """Показує користувачу наступну анкету: спершу — тих, хто вже лайкнув
    його (гарантований метч), інакше — наступну підходящу за фільтрами."""
    await state.clear()

    pending_liker_id = await run_db(db_get_pending_like, user_id)
    if pending_liker_id:
        liker_profile = await run_db(db_get_profile, pending_liker_id)
        if liker_profile and liker_profile.get('active', True):
            like_comment = await run_db(db_get_like_comment, pending_liker_id, user_id)
            await state.update_data(current_target=pending_liker_id, is_like_mode=True)
            await bot.send_message(user_id, "Комусь сподобалась твоя анкета! 🚀")
            await show_profile(user_id, pending_liker_id, liker_profile, like_comment=like_comment)
            await state.set_state(FeedState.viewing)
            return

    filters = await run_db(db_get_search_filters, user_id)
    target_city = filters.get('city')
    radius_km = filters.get('radius_km')

    target_uid, profile = await run_db(db_get_next_profile, user_id)
    if not profile:
        if radius_km:
            extra_info = f" у радіусі <b>{radius_km} км</b>"
        elif target_city:
            extra_info = f" у місті <b>{html.escape(target_city)}</b>"
        else:
            extra_info = ""
        await bot.send_message(
            user_id,
            f"Поки що немає нових анкет{extra_info}. Спробуй скинути фільтри або завітай трохи пізніше! 😉",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard()
        )
        return

    await state.update_data(current_target=target_uid, is_like_mode=False)
    await show_profile(user_id, target_uid, profile)
    await state.set_state(FeedState.viewing)

@dp.message(F.text == "🚀 Дивитися анкети")
@dp.message(Command("feed"))
async def start_feed(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not await run_db(db_get_profile, user_id):
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return
    await _enter_feed(user_id, state)

@dp.callback_query(F.data == "swipe_exit")
async def exit_feed_cb(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.answer()
    await call.message.answer("Повертаємось у головне меню.", reply_markup=main_menu_keyboard())

async def send_match_card(chat_id: int, prof: dict, target_uid: int):
    """Надсилає повну картку анкети (фото + ім'я/вік/місто/опис) з кнопкою
    "✉️ Написати" — та сама картка, що й у розділі "Мої метчі", але тепер ще
    й одразу в момент самого метчу."""
    caption = f"📌 <b>{html.escape(prof['name'])}</b>, {prof['age']}, {html.escape(prof['city'])}\n📝 {html.escape(prof['bio'])}"

    if prof.get('photo'):
        try:
            await bot.send_photo(
                chat_id=chat_id,
                photo=prof['photo'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=match_card_keyboard(target_uid)
            )
            return
        except TelegramBadRequest:
            pass
        except Exception:
            return

    try:
        await bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=match_card_keyboard(target_uid))
    except Exception:
        pass

async def notify_match(user_id: int, target_uid: int):
    """Повідомляє обох користувачів про метч. Спільна логіка для лайка й лайка з коментарем."""
    my_prof = await run_db(db_get_profile, user_id)
    target_prof = await run_db(db_get_profile, target_uid)
    if not my_prof or not target_prof:
        return
    my_link = f"@{my_prof.get('username')}" if my_prof.get('username') else f"<a href='tg://user?id={user_id}'>Користувач</a>"
    target_link = f"@{target_prof.get('username')}" if target_prof.get('username') else f"<a href='tg://user?id={target_uid}'>Користувач</a>"

    await bot.send_message(user_id, f"🎉 <b>Це МЕТЧ!</b>\nТи сподобався(лась) {html.escape(target_prof['name'])}!\nКонтакт для зв'язку: {target_link}", parse_mode="HTML")
    await send_match_card(user_id, target_prof, target_uid)

    try:
        await bot.send_message(target_uid, f"🎉 <b>Це МЕТЧ!</b>\nТобі відповіли взаємністю! Контакт: {my_link}", parse_mode="HTML")
    except Exception:
        pass
    await send_match_card(target_uid, my_prof, user_id)

async def _record_swipe_and_advance(user_id: int, state: FSMContext, target_uid, reaction: str, matched: bool):
    """Запам'ятовує останній свайп для можливості відкату (метч відкатати не
    можна — контакти вже надіслані обом сторонам) і одразу показує наступну
    анкету."""
    last_swipe_data = {"target_uid": target_uid, "reaction": reaction, "matched": matched} if target_uid else None
    await _enter_feed(user_id, state)
    # _enter_feed() починається з state.clear(), тому last_swipe можна класти
    # в стан лише ПІСЛЯ її виклику — інакше він стирається одразу після запису.
    if last_swipe_data:
        await state.update_data(last_swipe=last_swipe_data)

@dp.callback_query(FeedState.viewing, F.data.startswith("swipe_yes_") | F.data.startswith("swipe_no_"))
async def swipe_like_dislike_cb(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    is_like = call.data.startswith("swipe_yes_")
    target_uid = int(call.data.rsplit("_", 1)[-1])

    data = await state.get_data()
    if data.get("current_target") != target_uid:
        await call.answer("Ця анкета вже неактуальна, дивись наступну 🙂", show_alert=True)
        return
    is_like_mode = data.get("is_like_mode", False)

    if is_like:
        status = await run_db(db_get_premium_status, user_id)
        if not status['premium_likes_active']:
            likes_today = await run_db(db_count_likes_today, user_id)
            if likes_today >= FREE_DAILY_LIKE_LIMIT:
                await call.answer(
                    f"😔 Денний ліміт лайків вичерпано ({FREE_DAILY_LIKE_LIMIT}/день). "
                    f"Онови завтра або оформи «💎 Преміум».",
                    show_alert=True
                )
                return

    await call.answer("❤️ Лайк!" if is_like else "👎")
    await run_db(db_add_seen, user_id, target_uid)

    matched = False
    if is_like:
        await run_db(db_add_like, user_id, target_uid)
        # is_like_mode означає, що target_uid вже лайкнув нас раніше — це гарантований метч.
        # Інакше перевіряємо, чи не лайкнув target_uid нас раніше незалежно (миттєвий метч).
        matched = is_like_mode or await run_db(db_check_mutual_like, user_id, target_uid)
        if matched:
            await notify_match(user_id, target_uid)
        else:
            target_prof = await run_db(db_get_profile, target_uid)
            if not target_prof or target_prof.get('notify_likes', True):
                try:
                    await bot.send_message(target_uid, "Твоєю анкетою хтось зацікавився! Натисни «🚀 Дивитися анкети», щоб переглянути. 😉")
                except Exception:
                    pass

    await _record_swipe_and_advance(user_id, state, target_uid, "yes" if is_like else "no", matched)

@dp.callback_query(FeedState.viewing, F.data.startswith("swipe_report_"))
async def swipe_report_cb(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    target_uid = int(call.data.rsplit("_", 1)[-1])

    data = await state.get_data()
    if data.get("current_target") != target_uid:
        await call.answer("Ця анкета вже неактуальна, дивись наступну 🙂", show_alert=True)
        return

    await run_db(db_add_seen, user_id, target_uid)
    await run_db(db_add_report, user_id, target_uid)
    try:
        await bot.send_message(ADMIN_ID, f"🛑 Нова скарга: користувач #{user_id} поскаржився на анкету #{target_uid}.")
    except Exception:
        pass
    await call.answer("Скаргу прийнято і передано модератору 🙏", show_alert=True)

    await _record_swipe_and_advance(user_id, state, target_uid, "report", matched=False)

@dp.callback_query(FeedState.viewing, F.data == "swipe_undo")
async def undo_last_swipe_cb(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    data = await state.get_data()
    last_swipe = data.get("last_swipe")

    if not last_swipe:
        await call.answer("Немає жодного свайпу для відкату.", show_alert=True)
        return
    if last_swipe.get("matched"):
        await call.answer("Цей свайп призвів до метчу, тому відкат неможливий 💞", show_alert=True)
        return

    target_uid = last_swipe["target_uid"]
    reaction = last_swipe["reaction"]

    await run_db(db_remove_seen, user_id, target_uid)
    if reaction == "yes":
        await run_db(db_remove_like, user_id, target_uid)

    # Одноразовий відкат: щоб не можна було скасувати той самий свайп двічі.
    await state.update_data(last_swipe=None)
    await call.answer("⏪ Відкатано!")

    profile = await run_db(db_get_profile, target_uid)
    if not profile:
        await bot.send_message(user_id, "Цю анкету вже не вдалося відновити.")
        await _enter_feed(user_id, state)
        return

    await state.update_data(current_target=target_uid, is_like_mode=False)
    await bot.send_message(user_id, "⏪ Свайп відкатано! Ось анкета знову:")
    await show_profile(user_id, target_uid, profile)
    await state.set_state(FeedState.viewing)

# --- ЛАЙК З КОМЕНТАРЕМ (анонімно, видно лише при відкритті анкети лайкера) ---

@dp.callback_query(FeedState.viewing, F.data.startswith("swipe_comment_"))
async def ask_like_comment_cb(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    target_uid = int(call.data.rsplit("_", 1)[-1])
    data = await state.get_data()
    if data.get("current_target") != target_uid:
        await call.answer("Ця анкета вже неактуальна, дивись наступну 🙂", show_alert=True)
        return
    await call.answer()
    await bot.send_message(
        user_id,
        "Напиши короткий коментар до лайка 💌\n"
        "Його побачить тільки ця людина, коли відкриє твою анкету. "
        "Твої контакти залишаться анонімними, поки не станеться метч.\n\n"
        "(або /cancel, щоб скасувати):"
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
        await message.answer(
            "😅 Схоже, ця анкета вже застаріла (можливо, бот перезапускався). "
            "Натисни «🚀 Дивитися анкети» — і продовжимо.",
            reply_markup=main_menu_keyboard()
        )
        return

    comment_text = message.text.strip()[:500]

    status = await run_db(db_get_premium_status, user_id)
    if not status['premium_likes_active']:
        likes_today = await run_db(db_count_likes_today, user_id)
        if likes_today >= FREE_DAILY_LIKE_LIMIT:
            await state.clear()
            await message.answer(
                f"😔 Ти вичерпав(ла) денний ліміт лайків ({FREE_DAILY_LIKE_LIMIT}/день). "
                f"Ліміт оновиться завтра, або оформи «💎 Преміум», щоб зняти обмеження назавжди.",
                reply_markup=main_menu_keyboard()
            )
            return

    await run_db(db_add_seen, user_id, target_uid)
    await run_db(db_add_like, user_id, target_uid, comment=comment_text)

    # is_like_mode означає, що target_uid вже лайкнув нас раніше — це гарантований метч.
    is_match = is_like_mode or await run_db(db_check_mutual_like, user_id, target_uid)

    if is_match:
        await notify_match(user_id, target_uid)
    else:
        await message.answer("💌 Лайк із коментарем надіслано!")
        target_prof = await run_db(db_get_profile, target_uid)
        if not target_prof or target_prof.get('notify_likes', True):
            try:
                await bot.send_message(target_uid, "Твоєю анкетою хтось зацікавився і залишив коментар до лайка! Натисни «🚀 Дивитися анкети», щоб переглянути. 😉")
            except Exception:
                pass

    await state.clear()
    await _enter_feed(user_id, state)

@dp.message(LikeCommentState.text)
async def block_media_in_like_comment(message: types.Message):
    await message.answer("⚠️ Коментар до лайка може бути лише текстом. Напиши текст або /cancel.")

@dp.message(MessageToProfileState.text, F.text)
async def send_message_to_profile(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    target_uid = data.get("current_target")

    if not target_uid:
        await state.clear()
        await message.answer(
            "😅 Схоже, ця анкета вже застаріла (можливо, бот перезапускався). "
            "Натисни «🚀 Дивитися анкети» — і продовжимо.",
            reply_markup=main_menu_keyboard()
        )
        return

    my_prof = await run_db(db_get_profile, user_id)
    my_link = f"@{my_prof.get('username')}" if my_prof and my_prof.get('username') else f"<a href='tg://user?id={user_id}'>Користувач</a>"
    my_name = html.escape(my_prof.get('name')) if my_prof else "Хтось"
    safe_text = html.escape(message.text or "")

    try:
        await bot.send_message(
            target_uid,
            f"✉️ <b>Тобі повідомлення від {my_name}!</b>\nКонтакт: {my_link}\n\n{safe_text}",
            parse_mode="HTML"
        )
        await message.answer("✅ Повідомлення надіслано!")
    except Exception:
        await message.answer("⚠️ Не вдалося надіслати повідомлення (можливо, користувач заблокував бота).")

    await run_db(db_add_seen, user_id, target_uid)
    await start_feed(message, state)

# --- БЛОКУВАННЯ КРУЖКІВ ТА МЕДІА ПІД ЧАС ПЕРЕГЛЯДУ АНКЕТ ---
@dp.message(FeedState.viewing, F.video_note | F.voice | F.sticker | F.video | F.photo | F.document)
async def block_media_in_feed(message: types.Message):
    await message.answer(
        "⚠️ У режимі перегляду анкет відправка «кружків» та медіа вимкнена.\n"
        "Користуйся кнопками під анкетою: ❤️, 👎, 🛑 Скарга або 🏠 Меню."
    )

# --- ІНШІ КОМАНДИ ТА МЕНЮ ---

@dp.message(F.text == "⚙️ Налаштування")
async def settings_menu(message: types.Message, state: FSMContext):
    await state.clear()
    
    if message.from_user.id == ADMIN_ID:
        total, active = await run_db(db_get_profiles_count, )
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
        profile = await run_db(db_get_profile, message.from_user.id)
        notify_likes = profile.get('notify_likes', True) if profile else True
        await message.answer(
            "⚙️ **Налаштування**\n\n"
            "🎉 Сповіщення про метчі завжди увімкнені — це найважливіше.\n"
            "🔔 А сповіщення про звичайні лайки (без метчу) можна вимкнути нижче:",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )
        await message.answer("Керування сповіщеннями:", reply_markup=settings_keyboard(notify_likes))

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
    s = await run_db(db_get_detailed_stats, )
    gender_lines = "\n".join(f"   • {g or 'не вказано'}: {c}" for g, c in s['by_gender']) or "   • немає даних"
    city_lines = "\n".join(f"   {i+1}. {escape_md(c)} — {n}" for i, (c, n) in enumerate(s['top_cities'])) or "   • немає даних"
    text = (
        "📊 **Детальна статистика бота**\n\n"
        f"👥 Всього анкет: **{s['total']}**\n"
        f"🟢 Активні: **{s['active']}**\n"
        f"🔴 Приховані: **{s['total'] - s['active']}**\n"
        f"🚫 Забанені: **{s['banned']}**\n\n"
        f"❤️ Всього лайків: **{s['likes_total']}**\n"
        f"🎉 Всього метчів: **{s['matches_total']}**\n\n"
        f"🚻 За статтю:\n{gender_lines}\n\n"
        f"🏙 Топ-5 міст:\n{city_lines}\n\n"
        f"🛑 Неопрацьованих скарг: **{s.get('open_reports', 0)}**"
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
    total_users = len(await run_db(db_get_all_user_ids, ))
    await message.answer(
        f"Ось що піде **{total_users}** користувачам:\n\n{escape_md(message.text)}\n\n"
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
    for uid in await run_db(db_get_all_user_ids, ):
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
    profile = await run_db(db_get_profile, target_id)
    if not profile:
        await message.answer(
            "😕 Анкету з таким ID не знайдено.",
            reply_markup=admin_panel_keyboard()
        )
        return

    is_banned = await run_db(db_is_banned, target_id)
    status = "🚫 Забанений" if is_banned else ("🟢 Активна" if profile['active'] else "🔴 Прихована")
    username_line = escape_md(f"@{profile['username']}") if profile.get('username') else "(немає юзернейму)"
    text = (
        f"👤 **Анкета #{target_id}**\n\n"
        f"Ім'я: **{escape_md(profile['name'])}**, {profile['age']} років\n"
        f"Стать: {profile['gender']}\n"
        f"Місто: {escape_md(profile.get('city')) or '—'}\n"
        f"Юзернейм: {username_line}\n"
        f"Статус: {status}\n\n"
        f"Опис: {escape_md(profile.get('bio')) or '—'}"
    )
    await message.answer(text, parse_mode="Markdown", reply_markup=admin_lookup_actions_keyboard(target_id, is_banned))

@dp.callback_query(F.data.startswith("admin_ban_"))
async def admin_ban_user(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer()
    target_id = int(call.data.replace("admin_ban_", ""))
    await run_db(db_set_banned, target_id, True)
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
    await run_db(db_set_banned, target_id, False)
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
    await run_db(db_delete_profile, target_id)
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

    profile = await run_db(db_admin_get_random_profile, ADMIN_ID, gender_filter=gender_filter)
    if not profile:
        await message.answer("😕 Анкет за цим фільтром не знайдено в базі.", reply_markup=admin_browse_feed_keyboard())
        return

    await state.update_data(admin_current_target=profile['user_id'])
    is_banned = await run_db(db_is_banned, profile['user_id'])
    status = "🚫 Забанений" if is_banned else ("🟢 Активна" if profile['active'] else "🔴 Прихована")
    username_line = escape_md(f"@{profile['username']}") if profile.get('username') else "(немає юзернейму)"
    caption = (
        f"👤 **#{profile['user_id']}** — {escape_md(profile['name'])}, {profile['age']}, {escape_md(profile.get('city')) or '—'}\n"
        f"Стать: {profile['gender']} | Статус: {status}\n"
        f"Юзернейм: {username_line}\n\n"
        f"📝 {escape_md(profile.get('bio')) or '—'}"
    )
    sent = False
    if profile.get('photo'):
        try:
            await message.answer_photo(
                photo=profile['photo'], caption=caption, parse_mode="Markdown",
                reply_markup=admin_browse_feed_keyboard()
            )
            sent = True
        except TelegramBadRequest:
            pass

    if not sent:
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
        "• **💞 Мої метчі** — список тих, з ким у вас взаємний лайк.\n"
        "• **❤️ Хто мене лайкнув** — скільки людей тебе лайкнули і перегляд їхніх анкет.\n"
        "• **👤 Моя анкета** — перегляд, редагування або приховання своєї анкети з пошуку.\n\n"
        "Приємного спілкування! 🇺🇦",
        parse_mode="Markdown"
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

    total, active = await run_db(db_get_profiles_count, )
    await message.answer(
        f"📊 **Статистика бота:**\n\n"
        f"• Всього користувачів: **{total}**\n"
        f"• Активних анкет: **{active}**\n"
        f"• Прихованих анкет: **{total - active}**",
        parse_mode="Markdown"
    )

# --- ВЕБ-СЕРВЕР ДЛЯ RENDER + АДМІН-МІНІАПП ---

async def handle_healthcheck(request):
    return web.Response(text="Bot is alive!")

def validate_webapp_init_data(init_data: str):
    """Перевіряє криптографічний підпис Telegram WebApp initData (за офіційним алгоритмом
    Telegram) і повертає дані користувача, ЛИШЕ якщо підпис коректний і це саме ADMIN_ID.
    Це замінює логін/пароль — підробити initData без токена бота неможливо."""
    if not init_data:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(computed_hash, received_hash):
            return None
        user = json.loads(parsed.get("user", "{}"))
        if user.get("id") != ADMIN_ID:
            return None
        return user
    except Exception:
        return None

def get_admin_or_none(request):
    init_data = request.headers.get("X-Telegram-Init-Data") or request.query.get("initData", "")
    return validate_webapp_init_data(init_data)

def forbidden():
    return web.json_response({"error": "forbidden"}, status=403)

async def handle_admin_page(request):
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "admin.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html")
    except FileNotFoundError:
        return web.Response(
            text="admin.html не знайдено. Поклади файл admin.html поруч з main.py.",
            status=500
        )

async def api_stats(request):
    if not get_admin_or_none(request):
        return forbidden()
    return web.json_response(await run_db(db_get_detailed_stats, ))

async def api_profiles(request):
    if not get_admin_or_none(request):
        return forbidden()
    search = request.query.get("search", "").strip()
    gender = request.query.get("gender") or None
    try:
        page = max(1, int(request.query.get("page", "1") or 1))
    except (TypeError, ValueError):
        page = 1
    limit = 30
    items, total = await run_db(db_list_profiles, search=search, gender=gender, limit=limit, offset=(page - 1) * limit)
    return web.json_response({"items": items, "total": total, "page": page, "limit": limit})

async def api_profile_detail(request):
    if not get_admin_or_none(request):
        return forbidden()
    user_id = int(request.match_info["user_id"])
    profile = await run_db(db_get_profile, user_id)
    if not profile:
        return web.json_response({"error": "not_found"}, status=404)
    profile["banned"] = await run_db(db_is_banned, user_id)
    return web.json_response(profile)

# Простий кеш в пам'яті: file_id Telegram-фото не змінюється, тож нема сенсу
# щоразу ходити в Telegram API за тим самим фото при кожному відкритті анкети.
_photo_cache: dict[str, bytes] = {}
_PHOTO_CACHE_LIMIT = 300

async def api_profile_photo(request):
    if not get_admin_or_none(request):
        return forbidden()
    user_id = int(request.match_info["user_id"])
    profile = await run_db(db_get_profile, user_id)
    photo_id = profile.get("photo") if profile else None
    if not photo_id:
        return web.json_response({"error": "no_photo"}, status=404)

    cached = _photo_cache.get(photo_id)
    if cached is not None:
        return web.Response(body=cached, content_type="image/jpeg")

    try:
        file = await bot.get_file(photo_id)
        buf = await bot.download_file(file.file_path)
        data = buf.read()
    except Exception:
        logging.exception("Не вдалося завантажити фото анкети #%s з Telegram", user_id)
        return web.json_response({"error": "fetch_failed"}, status=502)

    if len(_photo_cache) >= _PHOTO_CACHE_LIMIT:
        _photo_cache.clear()
    _photo_cache[photo_id] = data
    return web.Response(body=data, content_type="image/jpeg")

async def api_profile_update(request):
    if not get_admin_or_none(request):
        return forbidden()
    user_id = int(request.match_info["user_id"])
    data = await request.json()
    await run_db(db_update_profile_fields, user_id, **{k: v for k, v in data.items() if k in ("name", "age", "city", "bio")})
    return web.json_response({"ok": True})

async def api_profile_ban(request):
    if not get_admin_or_none(request):
        return forbidden()
    user_id = int(request.match_info["user_id"])
    await run_db(db_set_banned, user_id, True)
    try:
        await bot.send_message(user_id, "⛔ Твій акаунт заблоковано адміністрацією бота.")
    except Exception:
        pass
    return web.json_response({"ok": True})

async def api_profile_unban(request):
    if not get_admin_or_none(request):
        return forbidden()
    user_id = int(request.match_info["user_id"])
    await run_db(db_set_banned, user_id, False)
    try:
        await bot.send_message(user_id, "✅ Твій акаунт розблоковано. Ласкаво просимо назад!")
    except Exception:
        pass
    return web.json_response({"ok": True})

async def api_profile_delete(request):
    if not get_admin_or_none(request):
        return forbidden()
    user_id = int(request.match_info["user_id"])
    await run_db(db_delete_profile, user_id)
    return web.json_response({"ok": True})

broadcast_status = {"running": False, "sent": 0, "failed": 0, "total": 0}

async def _run_broadcast(text: str):
    global broadcast_status
    user_ids = await run_db(db_get_all_user_ids)
    broadcast_status = {"running": True, "sent": 0, "failed": 0, "total": len(user_ids)}
    for uid in user_ids:
        try:
            await bot.send_message(uid, text)
            broadcast_status["sent"] += 1
        except TelegramRetryAfter as e:
            # Telegram сам каже, скільки чекати, — чекаємо і пробуємо саме цього юзера ще раз,
            # інакше при масовій розсилці частина людей просто не отримає повідомлення.
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                await bot.send_message(uid, text)
                broadcast_status["sent"] += 1
            except Exception:
                broadcast_status["failed"] += 1
        except Exception:
            broadcast_status["failed"] += 1
        await asyncio.sleep(0.05)  # захист від рейт-лімітів Telegram
    broadcast_status["running"] = False

async def api_broadcast(request):
    # Розсилка може йти хвилинами при великій кількості користувачів — якщо робити
    # це прямо в тілі HTTP-запиту, адмін-панель у браузері просто "зависне" й,
    # ймовірно, отримає таймаут від Render. Тому запускаємо як фонову задачу і
    # одразу повертаємо відповідь, а прогрес адмінка опитує через /broadcast/status.
    if not get_admin_or_none(request):
        return forbidden()
    if broadcast_status.get("running"):
        return web.json_response({"error": "already_running"}, status=409)
    data = await request.json()
    text = (data.get("text") or "").strip()
    if not text:
        return web.json_response({"error": "empty_text"}, status=400)
    asyncio.create_task(_run_broadcast(text))
    return web.json_response({"started": True})

async def api_broadcast_status(request):
    if not get_admin_or_none(request):
        return forbidden()
    return web.json_response(broadcast_status)

@web.middleware
async def api_error_middleware(request, handler):
    """Ловить необроблені помилки в адмін-API (напр. обрив з'єднання з БД) і
    повертає акуратний JSON 500 замість того, щоб зронити з'єднання без відповіді."""
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception:
        logging.exception("Помилка в адмін API: %s", request.path)
        return web.json_response({"error": "server_error"}, status=500)

async def start_web_server():
    app = web.Application(middlewares=[api_error_middleware])
    app.router.add_get("/", handle_healthcheck)
    app.router.add_get("/admin", handle_admin_page)
    app.router.add_get("/admin/api/stats", api_stats)
    app.router.add_get("/admin/api/profiles", api_profiles)
    app.router.add_get("/admin/api/profile/{user_id}", api_profile_detail)
    app.router.add_get("/admin/api/profile/{user_id}/photo", api_profile_photo)
    app.router.add_put("/admin/api/profile/{user_id}", api_profile_update)
    app.router.add_post("/admin/api/profile/{user_id}/ban", api_profile_ban)
    app.router.add_post("/admin/api/profile/{user_id}/unban", api_profile_unban)
    app.router.add_delete("/admin/api/profile/{user_id}", api_profile_delete)
    app.router.add_post("/admin/api/broadcast", api_broadcast)
    app.router.add_get("/admin/api/broadcast/status", api_broadcast_status)
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
