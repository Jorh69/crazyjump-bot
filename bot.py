import os
import sqlite3
import logging
from datetime import datetime, timedelta
from uuid import uuid4
from functools import wraps
import telebot
from telebot import types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
import pytz
import threading
import time
import tempfile
import requests
import sys
import csv
import io
from typing import Optional
import calendar
from flask import Flask, request

# Инициализация Flask приложения
app = Flask(__name__)

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log')
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger('telebot').setLevel(logging.WARNING)

# Загрузка конфигурации
load_dotenv()
TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID'))
BACKUP_CHAT_ID = os.getenv('BACKUP_CHAT_ID')
TIMEZONE = pytz.timezone(os.getenv('TIMEZONE', 'Europe/Moscow'))
PORT = int(os.getenv('PORT', 10000))
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')

# Банковские реквизиты
BANK_DETAILS = {
    'bank_name': os.getenv('BANK_NAME'),
    'recipient': os.getenv('BANK_RECIPIENT'),
    'phone': os.getenv('BANK_PHONE')
}

# Локации для занятий
LOCATIONS = {
    "Мулино": {"address": "ул. Спортивная, 10"},
    "Сормово": {"address": "ул. Коминтерна, 15"},
    "Автозавод": {"address": "ул. Ленина, 25"}
}

# Конфигурация подписок
SUBSCRIPTION_PLANS = {
    "4 дня": {"sessions": 4, "price": 2400, "days_valid": 30},
    "6 дней": {"sessions": 6, "price": 3200, "days_valid": 45},
    "8 дней": {"sessions": 8, "price": 3400, "days_valid": 60},
    "12 дней": {"sessions": 12, "price": 3800, "days_valid": 90}
}

# Инициализация бота
bot = telebot.TeleBot(TOKEN, parse_mode='HTML', threaded=True, num_threads=5)

class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.init_db()
        return cls._instance

    def init_db(self):
        try:
            self.conn = sqlite3.connect('crazyjump.db', check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            cursor = self.conn.cursor()
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA busy_timeout=5000')

            # Создаем таблицы, если их нет
            tables = [
                """CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    join_date TEXT,
                    last_activity TEXT,
                    notifications_enabled INTEGER DEFAULT 1,
                    is_trainer INTEGER DEFAULT 0,
                    reminders_enabled INTEGER DEFAULT 1
                )""",
                """CREATE TABLE IF NOT EXISTS payments (
                    payment_id TEXT PRIMARY KEY,
                    user_id INTEGER,
                    plan_name TEXT,
                    amount INTEGER,
                    status TEXT,
                    created_at TEXT,
                    confirmed_at TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )""",
                """CREATE TABLE IF NOT EXISTS subscriptions (
                    subscription_id TEXT PRIMARY KEY,
                    user_id INTEGER,
                    plan_name TEXT,
                    sessions_total INTEGER,
                    sessions_used INTEGER DEFAULT 0,
                    price INTEGER,
                    status TEXT,
                    created_at TEXT,
                    activated_at TEXT,
                    expires_at TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )""",
                """CREATE TABLE IF NOT EXISTS trainers (
                    trainer_id INTEGER PRIMARY KEY,
                    full_name TEXT,
                    specialization TEXT,
                    bio TEXT,
                    photo_id TEXT,
                    FOREIGN KEY(trainer_id) REFERENCES users(user_id)
                )""",
                """CREATE TABLE IF NOT EXISTS schedule (
                    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trainer_id INTEGER,
                    location TEXT,
                    date TEXT,
                    time TEXT,
                    max_participants INTEGER DEFAULT 10,
                    current_participants INTEGER DEFAULT 0,
                    FOREIGN KEY(trainer_id) REFERENCES trainers(trainer_id)
                )""",
                """CREATE TABLE IF NOT EXISTS bookings (
                    booking_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    schedule_id INTEGER,
                    booking_date TEXT,
                    status TEXT DEFAULT 'active',
                    FOREIGN KEY(user_id) REFERENCES users(user_id),
                    FOREIGN KEY(schedule_id) REFERENCES schedule(schedule_id)
                )"""
            ]

            for table in tables:
                cursor.execute(table)

            # Проверяем существование колонки перед добавлением
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'reminders_enabled' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN reminders_enabled INTEGER DEFAULT 1")
                logger.info("Added missing column 'reminders_enabled' to users table")
            
            self.conn.commit()
            logger.info("Database initialized successfully")
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during DB init: {e}")
            raise

    def execute(self, query: str, params=(), fetchone=False, fetchall=False):
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchone() if fetchone else cursor.fetchall() if fetchall else None
            self.conn.commit()
            return result
        except sqlite3.Error as e:
            self.conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Unexpected DB error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def backup_to_file(self, filename: str) -> bool:
        try:
            with sqlite3.connect(filename) as new_conn:
                self.conn.backup(new_conn)
            return True
        except sqlite3.Error as e:
            logger.error(f"Backup error: {e}")
            return False

db = Database()

class TelegramBackup:
    def __init__(self, bot: telebot.TeleBot, chat_id: str):
        self.bot = bot
        self.chat_id = chat_id

    def send_backup(self) -> bool:
        try:
            with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
                tmp.close()
                if db.backup_to_file(tmp.name):
                    with open(tmp.name, 'rb') as f:
                        self.bot.send_document(
                            self.chat_id,
                            f,
                            caption=f"Backup {datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M')}"
                        )
                    os.unlink(tmp.name)
                    return True
            return False
        except Exception as e:
            logger.error(f"Backup error: {e}")
            return False

backup = TelegramBackup(bot, BACKUP_CHAT_ID)

def admin_required(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            if isinstance(args[0], types.Message):
                user_id = args[0].from_user.id
                message = args[0]
            elif isinstance(args[0], types.CallbackQuery):
                user_id = args[0].from_user.id
                message = args[0].message
            else:
                raise ValueError("Invalid argument type")

            if user_id == ADMIN_ID:
                return func(*args, **kwargs)
            else:
                bot.reply_to(message, "⛔ Доступ запрещен")
        except Exception as e:
            logger.error(f"Admin check error: {e}")
    return wrapped

def trainer_required(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            if isinstance(args[0], types.Message):
                user_id = args[0].from_user.id
                message = args[0]
            elif isinstance(args[0], types.CallbackQuery):
                user_id = args[0].from_user.id
                message = args[0].message
            else:
                raise ValueError("Invalid argument type")

            if db.execute("SELECT 1 FROM trainers WHERE trainer_id = ?", (user_id,), fetchone=True):
                return func(*args, **kwargs)
            else:
                bot.reply_to(message, "⛔ Доступ разрешен только тренерам")
        except Exception as e:
            logger.error(f"Trainer check error: {e}")
    return wrapped

def marquee_text(text, width=20):
    """Создает бегущую строку для длинного текста"""
    if len(text) <= width:
        return text
    return (text + " " + text)[:width]

def format_date(date_str: str) -> str:
    try:
        return datetime.fromisoformat(date_str).strftime('%d.%m.%Y')
    except:
        return date_str[:10]

def get_main_menu(user_id: Optional[int] = None) -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("💳 Купить абонемент")
    markup.row("📋 Мои абонементы", "🏋️ Записаться")
    markup.row("⚙️ Настройки", "❓ Помощь")
    
    if user_id == ADMIN_ID:
        markup.row("👑 Админ-панель")
    elif db.execute("SELECT 1 FROM trainers WHERE trainer_id = ?", (user_id,), fetchone=True):
        markup.row("🏋️ Панель тренера")
        
    return markup

def get_admin_menu() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("📊 Платежи", "👥 Пользователи")
    markup.row("🎫 Абонементы", "📦 Экспорт")
    markup.row("🏋️ Управление тренерами", "📅 Расписание")
    markup.row("💾 Создать бэкап", "🔄 Восстановить")
    markup.row("📊 Статистика", "⬅️ Главное меню")
    return markup

def get_trainer_menu() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("📅 Мои занятия", "👥 Мои клиенты")
    markup.row("📅 Просмотр расписания", "⬅️ Главное меню")
    return markup

def get_settings_menu() -> types.ReplyKeyboardMarkup:
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("🔔 Уведомления Вкл/Выкл")
    markup.row("📅 Напоминания о занятиях")
    markup.row("⬅️ Главное меню")
    return markup

def get_subscription_plans_keyboard() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    for plan_name, details in SUBSCRIPTION_PLANS.items():
        markup.add(InlineKeyboardButton(
            text=f"{plan_name} - {details['price']}₽",
            callback_data=f"plan_{plan_name}")
        )
    markup.add(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_subscription"))
    return markup

def get_locations_keyboard() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=1)
    for location in LOCATIONS.keys():
        display_text = f"{marquee_text(location)} ({LOCATIONS[location]['address']})"
        markup.add(InlineKeyboardButton(
            display_text, 
            callback_data=f"location_{location}"))
    markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_menu"))
    return markup

def get_dates_keyboard(location: str) -> InlineKeyboardMarkup:
    today = datetime.now(TIMEZONE)
    year = today.year
    month = today.month
    
    first_day = today.replace(day=1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    
    schedule = db.execute(
        "SELECT date, time FROM schedule WHERE location = ? AND date BETWEEN ? AND ?",
        (location, first_day.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d')),
        fetchall=True
    )
    
    schedule_dict = {}
    for item in schedule:
        date = item['date']
        if date not in schedule_dict:
            schedule_dict[date] = []
        schedule_dict[date].append(item['time'])

    markup = InlineKeyboardMarkup(row_width=7)
    
    month_name = first_day.strftime('%B %Y')
    markup.add(InlineKeyboardButton(month_name, callback_data="ignore"))
    
    week_days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    markup.row(*[InlineKeyboardButton(day, callback_data="ignore") for day in week_days])
    
    cal = calendar.monthcalendar(year, month)
    
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                day_str = f"{year}-{month:02d}-{day:02d}"
                if day_str in schedule_dict:
                    times = "\n".join(schedule_dict[day_str])
                    btn_text = f"{day}⏰\n{times}"
                    row.append(InlineKeyboardButton(
                        btn_text,
                        callback_data=f"date_{day_str}_{location}"
                    ))
                else:
                    row.append(InlineKeyboardButton(
                        str(day),
                        callback_data=f"date_{day_str}_{location}"
                    ))
        markup.row(*row)
    
    markup.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to_locations_{location}"))
    return markup

def get_schedule_actions_keyboard(schedule_id: int, location: str) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit_schedule_{schedule_id}"),
        InlineKeyboardButton("❌ Удалить", callback_data=f"delete_schedule_{schedule_id}")
    )
    markup.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to_dates_{location}"))
    return markup

# Flask endpoint для вебхука
@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return ''
    return 'Bad request', 400

# Основные обработчики команд бота
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message: types.Message):
    try:
        user_id = message.from_user.id
        now = datetime.now(TIMEZONE).isoformat()

        if not db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,), fetchone=True):
            db.execute(
                """INSERT INTO users 
                (user_id, username, first_name, last_name, join_date, last_activity, reminders_enabled) 
                VALUES (?, ?, ?, ?, ?, ?, 1)""",
                (user_id, message.from_user.username, message.from_user.first_name,
                 message.from_user.last_name or "", now, now)
            )

        bot.send_message(
            message.chat.id,
            "<b>🏆 Добро пожаловать в CrazyJump!</b>\n\nВыберите действие:",
            reply_markup=get_main_menu(user_id)
        )
    except Exception as e:
        logger.error(f"Welcome error: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

# ... (все остальные обработчики сообщений и callback'ов остаются без изменений)

def run_polling():
    """Запуск бота в режиме polling"""
    try:
        logger.info("Starting bot in polling mode...")
        bot.remove_webhook()
        bot.polling(none_stop=True, interval=2, timeout=30)
    except Exception as e:
        logger.error(f"Polling error: {e}")
        time.sleep(15)
        run_polling()

def run_webhook():
    """Запуск бота в режиме webhook"""
    try:
        logger.info(f"Setting webhook to {WEBHOOK_URL}/webhook")
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
    except Exception as e:
        logger.error(f"Webhook setup error: {e}")

def start_background_tasks():
    """Запуск фоновых задач"""
    threading.Thread(target=check_subscriptions, daemon=True).start()
    threading.Thread(target=send_reminders, daemon=True).start()
    logger.info("Background tasks started")

if __name__ == '__main__':
    if WEBHOOK_URL:
        run_webhook()
        app.run(host='0.0.0.0', port=PORT)
    else:
        run_polling()
