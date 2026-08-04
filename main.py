import asyncio
import logging
import os
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
if not BOT_TOKEN:
    raise ValueError("Помилка: BOT_TOKEN не знайдено!")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- ТИМЧАСОВА БАЗА ДАНИХ У ПАМ'ЯТІ ---
user_profiles = {}  # user_id -> profile data
likes_queue = {}    # target_user_id -> list of user_ids who liked them
seen_profiles = {}  # user_id -> set of viewed user_ids

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
    choose_field = State()
    new_name = State()
    new_age = State()
    new_city = State()
    new_bio = State()
    new_photo = State()

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

def get_next_profile(current_user_id):
    if current_user_id not in seen_profiles:
        seen_profiles[current_user_id] = set()
    
    current_user_prof = user_profiles.get(current_user_id)
    
    for uid, profile in user_profiles.items():
        if uid != current_user_id and uid not in seen_profiles[current_user_id]:
            # Перевіряємо чи анкета активна
            if not profile.get('active', True):
                continue
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

# --- ХЕНДЛЕРИ СТАРТУ ТА ПОЧАТКОВОЇ РЕЄСТРАЦІЇ ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if user_id in user_profiles:
        await message.answer("З поверненням у **Нирчик UA** 🇺🇦!", reply_markup=main_menu_keyboard())
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
    await state.update_data(target_gender=message.text)
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
    
    user_profiles[message.from_user.id] = data
    await state.clear()
    
    await message.answer("🎉 **Анкету створено успішно!**", parse_mode="Markdown")
    await show_my_profile(message)

# --- МЕНЮ "МОЯ АНКЕТА" ТА РЕДАКТУВАННЯ ---

@dp.message(F.text == "👤 Моя анкета")
async def show_my_profile(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_profiles:
        await message.answer("У тебе ще немає анкети. Напиши /start для реєстрації.")
        return
    
    p = user_profiles[user_id]
    caption = f"Твоя анкета:\n\n{format_profile(p)}"
    await message.answer_photo(
        photo=p['photo'],
        caption=caption,
        parse_mode="Markdown",
        reply_markup=my_profile_keyboard(p.get('active', True))
    )

@dp.callback_query(F.data == "toggle_active")
async def toggle_active(call: types.CallbackQuery):
    user_id = call.from_user.id
    if user_id in user_profiles:
        current_status = user_profiles[user_id].get('active', True)
        user_profiles[user_id]['active'] = not current_status
        new_status = "активовано" if not current_status else "приховано з пошуку"
        await call.answer(f"Анкету {new_status}!", show_alert=True)
        await call.message.delete()
        await show_my_profile(call.message)

@dp.callback_query(F.data == "edit_profile")
async def edit_profile_menu(call: types.CallbackQuery):
    await call.message.edit_caption(
        caption="Обери, який пункт ти хочеш змінити:",
        reply_markup=edit_fields_keyboard()
    )

@dp.callback_query(F.data == "back_to_profile")
async def back_to_profile(call: types.CallbackQuery):
    await call.message.delete()
    await show_my_profile(call.message)

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
    user_profiles[message.from_user.id]['name'] = message.text
    await state.clear()
    await message.answer("✅ Ім'я успішно оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile(message)

@dp.callback_query(F.data == "edit_age")
async def edit_age(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи новий вік:")
    await state.set_state(EditProfileState.new_age)

@dp.message(EditProfileState.new_age)
async def process_new_age(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or not (12 <= int(message.text) <= 99):
        await message.answer("Вкажи реальний вік числом:")
        return
    user_profiles[message.from_user.id]['age'] = int(message.text)
    await state.clear()
    await message.answer("✅ Вік оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile(message)

@dp.callback_query(F.data == "edit_city")
async def edit_city(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи нове місто:")
    await state.set_state(EditProfileState.new_city)

@dp.message(EditProfileState.new_city)
async def process_new_city(message: types.Message, state: FSMContext):
    user_profiles[message.from_user.id]['city'] = message.text
    await state.clear()
    await message.answer("✅ Місто оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile(message)

@dp.callback_query(F.data == "edit_bio")
async def edit_bio(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Напиши новий опис про себе:")
    await state.set_state(EditProfileState.new_bio)

@dp.message(EditProfileState.new_bio)
async def process_new_bio(message: types.Message, state: FSMContext):
    user_profiles[message.from_user.id]['bio'] = message.text
    await state.clear()
    await message.answer("✅ Опис оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile(message)

@dp.callback_query(F.data == "edit_photo")
async def edit_photo(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Надішли нову світлину 📸:")
    await state.set_state(EditProfileState.new_photo)

@dp.message(EditProfileState.new_photo, F.photo)
async def process_new_photo(message: types.Message, state: FSMContext):
    user_profiles[message.from_user.id]['photo'] = message.photo[-1].file_id
    await state.clear()
    await message.answer("✅ Фото оновлено!", reply_markup=main_menu_keyboard())
    await show_my_profile(message)

# --- ГОРТАННЯ АНКЕТ (ФІД) ---

@dp.message(F.text == "🚀 Дивитися анкети")
async def start_feed(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id not in user_profiles:
        await message.answer("Спочатку створи анкету за допомогою /start!")
        return

    # Перевіряємо чергу лайків
    if user_id in likes_queue and likes_queue[user_id]:
        liker_id = likes_queue[user_id].pop(0)
        liker_profile = user_profiles.get(liker_id)
        if liker_profile and liker_profile.get('active', True):
            await state.update_data(current_target=liker_id, is_like_mode=True)
            await message.answer("Комусь сподобалась твоя анкета! 🚀", reply_markup=feed_keyboard())
            await show_profile(message, liker_id, liker_profile)
            await state.set_state(FeedState.viewing)
            return

    target_uid, profile = get_next_profile(user_id)
    if not profile:
        await message.answer("Поки що немає нових анкет у твоєму регіоні. Завітай трохи пізніше! 😉", reply_markup=main_menu_keyboard())
        return

    await state.update_data(current_target=target_uid, is_like_mode=False)
    await show_profile(message, target_uid, profile)
    await state.set_state(FeedState.viewing)

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
            my_prof = user_profiles.get(user_id)
            target_prof = user_profiles.get(target_uid)
            
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

@dp.message(F.text == "⚙️ Налаштування")
async def settings_menu(message: types.Message):
    await message.answer("⚙️ **Налаштування бота**\n\nТут ти можеш налаштувати пошукові фільтри та конфіденційність. (В розробці)", reply_markup=main_menu_keyboard())

@dp.message(F.text == "❓ Допомога")
async def help_menu(message: types.Message):
    await message.answer(
        "❓ **Як користуватися ботом Нирчик UA:**\n\n"
        "• **🚀 Дивитися анкети** — починає гортання користувачів.\n"
        "• **❤️** — поставити лайк.\n"
        "• **👎** — пропустити анкету.\n"
        "• **👤 Моя анкета** — перегляд, редагування або приховання своєї анкети з пошуку.\n\n"
        "Приємного спілкування! 🇺🇦"
    )

@dp.message(FeedState.viewing, F.text == "🏠 Головне меню")
async def exit_feed(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Повертаємось у головне меню.", reply_markup=main_menu_keyboard())

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
