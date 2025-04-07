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
TIMEZONE = pytz.timezone('Europe/Moscow')
REPLIT_PROJECT_NAME = os.getenv('REPLIT_PROJECT_NAME', 'your-replit-name')

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

            # Проверяем существование таблиц и столбцов
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            
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

            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_users_join_date ON users(join_date)",
                "CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)",
                "CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status)",
                "CREATE INDEX IF NOT EXISTS idx_subscriptions_expires ON subscriptions(expires_at)",
                "CREATE INDEX IF NOT EXISTS idx_schedule_date ON schedule(date)",
                "CREATE INDEX IF NOT EXISTS idx_schedule_location ON schedule(location)"
            ]

            for table in tables:
                cursor.execute(table)
            
            if 'reminders_enabled' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN reminders_enabled INTEGER DEFAULT 1")
                logger.info("Added missing column 'reminders_enabled' to users table")
            
            for index in indexes:
                cursor.execute(index)

            self.conn.commit()
            self.last_backup_time = None
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
            self.last_backup_time = datetime.now(TIMEZONE)
            return True
        except sqlite3.Error as e:
            logger.error(f"Backup error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected backup error: {e}")
            return False

    def check_integrity(self):
        try:
            result = self.execute("PRAGMA integrity_check", fetchone=True)
            if result and result[0] == 'ok':
                logger.info("Database integrity check passed")
                return True
            logger.error(f"Database integrity check failed: {result}")
            return False
        except Exception as e:
            logger.error(f"Integrity check failed: {e}")
            return False

    def reconnect(self):
        try:
            if self.conn:
                self.conn.close()
            self.init_db()
            return True
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
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

@bot.message_handler(func=lambda m: m.text == "💳 Купить абонемент")
def show_subscription_plans_handler(message: types.Message):
    try:
        bot.send_message(
            message.chat.id,
            "Выберите абонемент:",
            reply_markup=get_subscription_plans_keyboard()
        )
    except Exception as e:
        logger.error(f"Error showing subscription plans: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("plan_"))
def process_subscription_plan(call: types.CallbackQuery):
    try:
        plan_name = call.data.split("_")[1]
        plan_details = SUBSCRIPTION_PLANS.get(plan_name)
        
        if not plan_details:
            bot.answer_callback_query(call.id, "Абонемент не найден")
            return
            
        payment_id = str(uuid4())
        user_id = call.from_user.id
        
        db.execute(
            """INSERT INTO payments 
            (payment_id, user_id, plan_name, amount, status, created_at) 
            VALUES (?, ?, ?, ?, 'pending', ?)""",
            (payment_id, user_id, plan_name, plan_details['price'], datetime.now(TIMEZONE).isoformat())
        )
        
        payment_text = (
            f"💳 Оплата абонемента «{plan_name}»\n\n"
            f"💰 Сумма: {plan_details['price']}₽\n"
            f"📅 Срок действия: {plan_details['days_valid']} дней\n"
            f"🏋️ Количество занятий: {plan_details['sessions']}\n\n"
            f"Для оплаты переведите {plan_details['price']}₽ на:\n"
            f"🏦 Банк: {BANK_DETAILS['bank_name']}\n"
            f"👤 Получатель: {BANK_DETAILS['recipient']}\n"
            f"📱 Телефон: {BANK_DETAILS['phone']}\n\n"
            f"В комментарии укажите: {payment_id[:8]}\n\n"
            f"После оплаты нажмите кнопку «Подтвердить оплату»"
        )
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Подтвердить оплату", callback_data=f"confirm_payment_{payment_id}"))
        markup.add(InlineKeyboardButton("❌ Отменить", callback_data="cancel_subscription"))
        
        bot.edit_message_text(
            payment_text,
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=markup
        )
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error processing subscription plan: {e}")
        bot.answer_callback_query(call.id, "Ошибка при выборе абонемента")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data == "cancel_subscription")
def cancel_subscription(call: types.CallbackQuery):
    try:
        bot.edit_message_text(
            "Выберите действие:",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=get_main_menu(call.from_user.id)
        )
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error canceling subscription: {e}")
        bot.answer_callback_query(call.id, "Ошибка при отмене")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "📋 Мои абонементы")
def show_user_subscriptions(message: types.Message):
    try:
        user_id = message.from_user.id
        subscriptions = db.execute(
            "SELECT * FROM subscriptions WHERE user_id = ? AND status = 'active'",
            (user_id,),
            fetchall=True
        )

        if not subscriptions:
            bot.send_message(message.chat.id, "У вас нет активных абонементов.")
            return

        for sub in subscriptions:
            try:
                sub_id = sub['subscription_id']
                plan_name = sub['plan_name']
                total = sub['sessions_total']
                used = sub['sessions_used']
                price = sub['price']
                created_at = format_date(sub['created_at'])
                expires_at = format_date(sub['expires_at'])
                remaining = total - used

                msg_text = (
                    f"📋 Абонемент #{sub_id[:8]}\n"
                    f"📌 Тариф: {plan_name}\n"
                    f"🔢 Занятий: {used}/{total} (осталось {remaining})\n"
                    f"💰 Стоимость: {price}₽\n"
                    f"📅 Дата активации: {created_at}\n"
                    f"⏳ Срок действия до: {expires_at}"
                )

                bot.send_message(message.chat.id, msg_text)
            except Exception as e:
                logger.error(f"Error showing subscription: {e}")
                continue

    except Exception as e:
        logger.error(f"Subscriptions error: {e}")
        bot.send_message(
            message.chat.id,
            "Произошла ошибка при загрузке ваших абонементов."
        )

@bot.message_handler(func=lambda m: m.text == "🏋️ Записаться")
def start_booking(message: types.Message):
    try:
        bot.send_message(
            message.chat.id,
            "Выберите локацию для занятия:",
            reply_markup=get_locations_keyboard()
        )
    except Exception as e:
        logger.error(f"Error starting booking: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "⚙️ Настройки")
def show_settings(message: types.Message):
    try:
        user_id = message.from_user.id
        user = db.execute(
            "SELECT notifications_enabled, reminders_enabled FROM users WHERE user_id = ?",
            (user_id,),
            fetchone=True
        )

        if not user:
            bot.send_message(message.chat.id, "Сначала зарегистрируйтесь через /start")
            return

        notif_status = "🔔 Включены" if user['notifications_enabled'] else "🔕 Выключены"
        reminders_status = "⏰ Включены" if user.get('reminders_enabled', 1) else "⏳ Выключены"

        msg_text = (
            f"⚙️ <b>Ваши настройки</b>\n\n"
            f"{notif_status} - уведомления об абонементах\n"
            f"{reminders_status} - напоминания о занятиях\n\n"
            f"Выберите параметр для изменения:"
        )

        bot.send_message(
            message.chat.id,
            msg_text,
            reply_markup=get_settings_menu()
        )
    except Exception as e:
        logger.error(f"Settings error: {e}")
        bot.send_message(
            message.chat.id,
            "⚠️ Произошла ошибка при загрузке настроек"
        )

@bot.message_handler(func=lambda m: m.text == "❓ Помощь")
def show_help(message: types.Message):
    try:
        help_text = """
<b>🤔 Помощь по боту CrazyJump</b>

💳 <b>Купить абонемент</b> - выбор и оплата абонемента
📋 <b>Мои абонементы</b> - просмотр активных абонементов
🏋️ <b>Записаться</b> - запись на конкретное занятие
⚙️ <b>Настройки</b> - управление уведомлениями

Для начала работы просто выберите нужный пункт в меню.
"""
        bot.send_message(message.chat.id, help_text)
    except Exception as e:
        logger.error(f"Error showing help: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "🔔 Уведомления Вкл/Выкл")
def toggle_notifications(message: types.Message):
    try:
        user_id = message.from_user.id
        user = db.execute(
            "SELECT notifications_enabled FROM users WHERE user_id = ?",
            (user_id,),
            fetchone=True
        )

        if not user:
            bot.send_message(message.chat.id, "Сначала зарегистрируйтесь через /start")
            return

        new_status = not user['notifications_enabled']
        db.execute(
            "UPDATE users SET notifications_enabled = ? WHERE user_id = ?",
            (new_status, user_id)
        )

        status_msg = "включены" if new_status else "выключены"
        bot.send_message(
            message.chat.id,
            f"Уведомления теперь {status_msg}",
            reply_markup=get_settings_menu()
        )
    except Exception as e:
        logger.error(f"Toggle notifications error: {e}")
        bot.send_message(
            message.chat.id,
            "⚠️ Не удалось изменить настройки уведомлений",
            reply_markup=get_main_menu(message.from_user.id)
        )

@bot.message_handler(func=lambda m: m.text == "📅 Напоминания о занятиях")
def toggle_reminders(message: types.Message):
    try:
        user_id = message.from_user.id
        user = db.execute(
            "SELECT reminders_enabled FROM users WHERE user_id = ?",
            (user_id,),
            fetchone=True
        )

        if not user:
            bot.send_message(message.chat.id, "Сначала зарегистрируйтесь через /start")
            return

        current_status = user.get('reminders_enabled', 1)
        new_status = 0 if current_status else 1
        db.execute(
            "UPDATE users SET reminders_enabled = ? WHERE user_id = ?",
            (new_status, user_id)
        )

        status_msg = "включены" if new_status else "выключены"
        bot.send_message(
            message.chat.id,
            f"Напоминания о занятиях теперь {status_msg}",
            reply_markup=get_settings_menu()
        )
    except Exception as e:
        logger.error(f"Toggle reminders error: {e}")
        bot.send_message(
            message.chat.id,
            "⚠️ Не удалось изменить настройки напоминаний",
            reply_markup=get_main_menu(message.from_user.id)
        )

@bot.message_handler(func=lambda m: m.text == "👑 Админ-панель")
@admin_required
def admin_panel(message: types.Message):
    try:
        bot.send_message(message.chat.id, "Админ-панель:", reply_markup=get_admin_menu())
    except Exception as e:
        logger.error(f"Error showing admin panel: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "🏋️ Панель тренера")
@trainer_required
def trainer_panel(message: types.Message):
    try:
        bot.send_message(message.chat.id, "Панель тренера:", reply_markup=get_trainer_menu())
    except Exception as e:
        logger.error(f"Error showing trainer panel: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "⬅️ Главное меню")
def return_to_main_menu(message: types.Message):
    try:
        bot.send_message(
            message.chat.id,
            "Главное меню:",
            reply_markup=get_main_menu(message.from_user.id)
        )
    except Exception as e:
        logger.error(f"Error returning to main menu: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "📊 Платежи")
@admin_required
def list_payments(message: types.Message):
    try:
        payments = db.execute(
            "SELECT * FROM payments ORDER BY created_at DESC LIMIT 10",
            fetchall=True
        )

        if not payments:
            bot.send_message(message.chat.id, "Нет данных о платежах")
            return

        payments_text = "\n".join(
            f"{i+1}. ID: {p['payment_id'][:8]} - {p['plan_name']} ({p['amount']}₽) - {p['status']}"
            for i, p in enumerate(payments)
        )

        bot.send_message(message.chat.id, f"Последние платежи:\n\n{payments_text}")
    except Exception as e:
        logger.error(f"Error listing payments: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "👥 Пользователи")
@admin_required
def list_users(message: types.Message):
    try:
        users = db.execute(
            "SELECT user_id, username, first_name, last_name, join_date FROM users ORDER BY join_date DESC LIMIT 10",
            fetchall=True
        )

        if not users:
            bot.send_message(message.chat.id, "Нет данных о пользователях")
            return

        users_text = "\n".join(
            f"{i+1}. ID: {u['user_id']} - @{u['username'] or 'нет'} ({u['first_name']} {u['last_name']}) - {format_date(u['join_date'])}"
            for i, u in enumerate(users)
        )

        bot.send_message(message.chat.id, f"Последние пользователи:\n\n{users_text}")
    except Exception as e:
        logger.error(f"Error listing users: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "🎫 Абонементы")
@admin_required
def list_subscriptions(message: types.Message):
    try:
        subs = db.execute(
            """SELECT s.subscription_id, s.user_id, s.plan_name, s.status, 
            u.username, u.first_name, u.last_name 
            FROM subscriptions s
            JOIN users u ON s.user_id = u.user_id
            ORDER BY s.created_at DESC LIMIT 10""",
            fetchall=True
        )

        if not subs:
            bot.send_message(message.chat.id, "Нет данных об абонементах")
            return

        subs_text = "\n".join(
            f"{i+1}. ID: {s['subscription_id'][:8]} - {s['plan_name']} ({s['status']})\n"
            f"Пользователь: @{s['username'] or 'нет'} ({s['first_name']} {s['last_name']})"
            for i, s in enumerate(subs)
        )

        bot.send_message(message.chat.id, f"Последние абонементы:\n\n{subs_text}")
    except Exception as e:
        logger.error(f"Error listing subscriptions: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "📦 Экспорт")
@admin_required
def export_data(message: types.Message):
    try:
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("📝 Пользователи", callback_data="export_users"),
            types.InlineKeyboardButton("💳 Платежи", callback_data="export_payments"),
            types.InlineKeyboardButton("🎫 Абонементы", callback_data="export_subs"),
            types.InlineKeyboardButton("🏋️ Тренеры", callback_data="export_trainers"),
            types.InlineKeyboardButton("📅 Расписание", callback_data="export_schedule"),
            types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_menu")
        )
        bot.send_message(message.chat.id, "Выберите данные для экспорта:", reply_markup=markup)
    except Exception as e:
        logger.error(f"Error showing export menu: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("export_"))
@admin_required
def handle_export(call: types.CallbackQuery):
    try:
        data_type = call.data[7:]
        filename = f"{data_type}_export.csv"

        if data_type == "users":
            data = db.execute("SELECT * FROM users", fetchall=True)
            header = "user_id,username,first_name,last_name,join_date,last_activity,notifications_enabled,is_trainer,reminders_enabled\n"
            rows = [
                f"{u['user_id']},{u['username'] or ''},{u['first_name'] or ''},"
                f"{u['last_name'] or ''},{u['join_date']},{u['last_activity']},"
                f"{u['notifications_enabled']},{u['is_trainer']},{u.get('reminders_enabled', 1)}\n"
                for u in data
            ]

        elif data_type == "payments":
            data = db.execute("SELECT * FROM payments", fetchall=True)
            header = "payment_id,user_id,plan_name,amount,status,created_at,confirmed_at\n"
            rows = [
                f"{p['payment_id']},{p['user_id']},{p['plan_name']},{p['amount']},"
                f"{p['status']},{p['created_at']},{p['confirmed_at'] or ''}\n"
                for p in data
            ]

        elif data_type == "subs":
            data = db.execute("SELECT * FROM subscriptions", fetchall=True)
            header = "subscription_id,user_id,plan_name,sessions_total,sessions_used,price,status,created_at,activated_at,expires_at\n"
            rows = [
                f"{s['subscription_id']},{s['user_id']},{s['plan_name']},{s['sessions_total']},"
                f"{s['sessions_used']},{s['price']},{s['status']},{s['created_at']},"
                f"{s['activated_at'] or ''},{s['expires_at'] or ''}\n"
                for s in data
            ]

        elif data_type == "trainers":
            data = db.execute("SELECT * FROM trainers", fetchall=True)
            header = "trainer_id,full_name,specialization,bio,photo_id\n"
            rows = [
                f"{t['trainer_id']},{t['full_name']},{t['specialization']},"
                f"{t['bio']},{t['photo_id']}\n"
                for t in data
            ]

        elif data_type == "schedule":
            data = db.execute("SELECT * FROM schedule", fetchall=True)
            header = "schedule_id,trainer_id,location,date,time,max_participants,current_participants\n"
            rows = [
                f"{s['schedule_id']},{s['trainer_id']},{s['location']},"
                f"{s['date']},{s['time']},{s['max_participants']},{s['current_participants']}\n"
                for s in data
            ]

        else:
            bot.answer_callback_query(call.id, "Неизвестный тип экспорта")
            return

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([h.strip() for h in header.split(',')])
        for row in rows:
            writer.writerow([f.strip() for f in row.split(',')])

        bot.send_document(
            call.message.chat.id,
            ('export.csv', io.BytesIO(output.getvalue().encode())),
            caption=f"Экспорт {data_type} ({len(data)} записей)"
        )
        bot.answer_callback_query(call.id, "Экспорт завершен")

    except Exception as e:
        logger.error(f"Export error: {e}")
        bot.answer_callback_query(call.id, "Ошибка при экспорте")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка при экспорте данных")

@bot.message_handler(func=lambda m: m.text == "🏋️ Управление тренерами")
@admin_required
def manage_trainers(message: types.Message):
    try:
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("➕ Добавить тренера", callback_data="add_trainer"),
            types.InlineKeyboardButton("✏️ Редактировать тренера", callback_data="edit_trainer"),
            types.InlineKeyboardButton("❌ Удалить тренера", callback_data="delete_trainer"),
            types.InlineKeyboardButton("👀 Список тренеров", callback_data="list_trainers"),
            types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_menu")
        )
        bot.send_message(message.chat.id, "Управление тренерами:", reply_markup=markup)
    except Exception as e:
        logger.error(f"Error managing trainers: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data == "add_trainer")
@admin_required
def add_trainer(call: types.CallbackQuery):
    try:
        bot.answer_callback_query(call.id)
        msg = bot.send_message(call.message.chat.id, "Введите ID пользователя, которого хотите сделать тренером:")
        bot.register_next_step_handler(msg, process_add_trainer)
    except Exception as e:
        logger.error(f"Error starting add trainer: {e}")
        bot.answer_callback_query(call.id, "Ошибка при добавлении тренера")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_add_trainer(message: types.Message):
    try:
        user_id = int(message.text)
        user = db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,), fetchone=True)
        if not user:
            bot.send_message(message.chat.id, "Пользователь с таким ID не найден")
            return

        db.execute("UPDATE users SET is_trainer = 1 WHERE user_id = ?", (user_id,))
        msg = bot.send_message(message.chat.id, "Введите полное имя тренера:")
        bot.register_next_step_handler(msg, lambda m: process_trainer_name(m, user_id))
    except ValueError:
        bot.send_message(message.chat.id, "Неверный ID пользователя")
    except Exception as e:
        logger.error(f"Error in process_add_trainer: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_trainer_name(message: types.Message, user_id: int):
    try:
        full_name = message.text
        msg = bot.send_message(message.chat.id, "Введите специализацию тренера:")
        bot.register_next_step_handler(msg, lambda m: process_trainer_specialization(m, user_id, full_name))
    except Exception as e:
        logger.error(f"Error in process_trainer_name: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_trainer_specialization(message: types.Message, user_id: int, full_name: str):
    try:
        specialization = message.text
        msg = bot.send_message(message.chat.id, "Введите описание тренера:")
        bot.register_next_step_handler(msg, lambda m: process_trainer_bio(m, user_id, full_name, specialization))
    except Exception as e:
        logger.error(f"Error in process_trainer_specialization: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_trainer_bio(message: types.Message, user_id: int, full_name: str, specialization: str):
    try:
        bio = message.text
        msg = bot.send_message(message.chat.id, "Отправьте фото тренера:")
        bot.register_next_step_handler(msg, lambda m: process_trainer_photo(m, user_id, full_name, specialization, bio))
    except Exception as e:
        logger.error(f"Error in process_trainer_bio: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_trainer_photo(message: types.Message, user_id: int, full_name: str, specialization: str, bio: str):
    try:
        if message.photo:
            photo_id = message.photo[-1].file_id
            db.execute(
                """INSERT INTO trainers 
                (trainer_id, full_name, specialization, bio, photo_id) 
                VALUES (?, ?, ?, ?, ?)""",
                (user_id, full_name, specialization, bio, photo_id)
            )
            bot.send_message(message.chat.id, "✅ Тренер успешно добавлен!", reply_markup=get_admin_menu())
        else:
            bot.send_message(message.chat.id, "Пожалуйста, отправьте фото")
    except Exception as e:
        logger.error(f"Error in process_trainer_photo: {e}")
        bot.send_message(message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data == "list_trainers")
@admin_required
def list_trainers(call: types.CallbackQuery):
    try:
        trainers = db.execute(
            "SELECT t.*, u.username FROM trainers t JOIN users u ON t.trainer_id = u.user_id",
            fetchall=True
        )

        if not trainers:
            bot.send_message(call.message.chat.id, "Нет зарегистрированных тренеров")
            return

        for trainer in trainers:
            try:
                msg_text = (
                    f"🏋️ Тренер: {trainer['full_name']}\n"
                    f"👤 Username: @{trainer['username']}\n"
                    f"📌 Специализация: {trainer['specialization']}\n"
                    f"📝 О себе: {trainer['bio']}"
                )

                if trainer['photo_id']:
                    bot.send_photo(call.message.chat.id, trainer['photo_id'], caption=msg_text)
                else:
                    bot.send_message(call.message.chat.id, msg_text)
            except Exception as e:
                logger.error(f"Error showing trainer info: {e}")
                continue

        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error listing trainers: {e}")
        bot.answer_callback_query(call.id, "Ошибка при получении списка тренеров")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "📅 Расписание")
@admin_required
def manage_schedule(message: types.Message):
    try:
        bot.send_message(
            message.chat.id,
            "Выберите локацию:",
            reply_markup=get_locations_keyboard()
        )
    except Exception as e:
        logger.error(f"Error managing schedule: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data == "back_to_admin_menu")
@admin_required
def back_to_admin_menu(call: types.CallbackQuery):
    try:
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(
                "Админ-панель:",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=get_admin_menu()
            )
        except Exception as e:
            logger.warning(f"Couldn't edit message, sending new: {e}")
            bot.send_message(
                call.message.chat.id,
                "Админ-панель:",
                reply_markup=get_admin_menu()
            )
    except Exception as e:
        logger.error(f"Error returning to admin menu: {e}")
        bot.answer_callback_query(call.id, "Ошибка при возврате")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("location_"))
def process_location(call: types.CallbackQuery):
    try:
        location = call.data.split("_")[1]
        bot.answer_callback_query(call.id)
        
        try:
            bot.edit_message_text(
                f"Выбрана локация: {location}\nВыберите дату:",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=get_dates_keyboard(location)
            )
        except Exception as e:
            logger.warning(f"Couldn't edit message, sending new: {e}")
            bot.send_message(
                call.message.chat.id,
                f"Выбрана локация: {location}\nВыберите дату:",
                reply_markup=get_dates_keyboard(location)
            )
    except Exception as e:
        logger.error(f"Error processing location: {e}")
        bot.answer_callback_query(call.id, "Ошибка при выборе локации")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("back_to_locations_"))
def back_to_locations(call: types.CallbackQuery):
    try:
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(
                "Выберите локацию:",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=get_locations_keyboard()
            )
        except Exception as e:
            logger.warning(f"Couldn't edit message, sending new: {e}")
            bot.send_message(
                call.message.chat.id,
                "Выберите локацию:",
                reply_markup=get_locations_keyboard()
            )
    except Exception as e:
        logger.error(f"Error returning to locations: {e}")
        bot.answer_callback_query(call.id, "Ошибка при возврате")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("date_"))
def process_date(call: types.CallbackQuery):
    try:
        parts = call.data.split("_")
        date = parts[1]
        location = parts[2]
        
        bot.answer_callback_query(call.id)
        
        sessions = db.execute(
            """SELECT s.schedule_id, s.time, t.full_name, 
            s.max_participants, s.current_participants
            FROM schedule s
            LEFT JOIN trainers t ON s.trainer_id = t.trainer_id
            WHERE s.date = ? AND s.location = ?
            ORDER BY s.time""",
            (date, location),
            fetchall=True
        )
        
        if not sessions:
            msg = bot.send_message(
                call.message.chat.id,
                f"На {format_date(date)} в локации {location} занятий нет.\n"
                "Введите время занятия в формате HH.MM (например, 18.30):"
            )
            bot.register_next_step_handler(msg, lambda m: process_time(m, date, location))
            return
            
        markup = InlineKeyboardMarkup()
        for session in sessions:
            markup.add(InlineKeyboardButton(
                f"{session['time']} - {session['full_name'] or 'Без тренера'}",
                callback_data=f"session_{session['schedule_id']}"
            ))
        
        markup.add(InlineKeyboardButton("➕ Добавить время", callback_data=f"add_time_{date}_{location}"))
        markup.add(InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to_locations_{location}"))
        
        try:
            bot.edit_message_text(
                f"📅 Расписание на {format_date(date)} в {location}:\n\nВыберите занятие для редактирования:",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=markup
            )
        except Exception as e:
            logger.warning(f"Couldn't edit message, sending new: {e}")
            bot.send_message(
                call.message.chat.id,
                f"📅 Расписание на {format_date(date)} в {location}:\n\nВыберите занятие для редактирования:",
                reply_markup=markup
            )
    except Exception as e:
        logger.error(f"Error processing date: {e}")
        bot.answer_callback_query(call.id, "Ошибка при выборе даты")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("add_time_"))
@admin_required
def add_time(call: types.CallbackQuery):
    try:
        parts = call.data.split("_")
        date = parts[2]
        location = parts[3]
        
        bot.answer_callback_query(call.id)
        msg = bot.send_message(
            call.message.chat.id,
            f"Введите время занятия в формате HH.MM (например, 18.30) для {format_date(date)}:"
        )
        bot.register_next_step_handler(msg, lambda m: process_time(m, date, location))
    except Exception as e:
        logger.error(f"Error adding time: {e}")
        bot.answer_callback_query(call.id, "Ошибка при добавлении времени")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_time(message: types.Message, date: str, location: str):
    try:
        time_str = message.text.replace(':', '.')  # Заменяем двоеточия на точки
        try:
            # Проверяем корректность формата времени
            hours, minutes = map(int, time_str.split('.'))
            if not (0 <= hours < 24 and 0 <= minutes < 60):
                raise ValueError
            time_str = f"{hours:02d}.{minutes:02d}"  # Форматируем время
        except ValueError:
            bot.send_message(message.chat.id, "Неверный формат времени. Используйте HH.MM (например, 18.30)")
            return

        existing = db.execute(
            "SELECT 1 FROM schedule WHERE date = ? AND location = ? AND time = ?",
            (date, location, time_str.replace('.', ':')),
            fetchone=True
        )
        
        if existing:
            bot.send_message(message.chat.id, "Занятие на это время уже существует!")
            return

        db.execute(
            """INSERT INTO schedule 
            (location, date, time, max_participants, current_participants) 
            VALUES (?, ?, ?, 10, 0)""",
            (location, date, time_str.replace('.', ':'))
        )
        
        bot.send_message(
            message.chat.id,
            f"✅ Занятие успешно добавлено!\n\n"
            f"📅 Дата: {format_date(date)}\n"
            f"⏰ Время: {time_str}\n"
            f"📍 Локация: {location}",
            reply_markup=get_admin_menu()
        )
        
        bot.send_message(
            message.chat.id,
            f"Выберите дату для добавления еще одного занятия в {location}:",
            reply_markup=get_dates_keyboard(location)
        )
    except Exception as e:
        logger.error(f"Error processing time: {e}")
        bot.send_message(message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("session_"))
@admin_required
def manage_session(call: types.CallbackQuery):
    try:
        schedule_id = int(call.data.split("_")[1])
        session = db.execute(
            """SELECT s.*, t.full_name 
            FROM schedule s
            LEFT JOIN trainers t ON s.trainer_id = t.trainer_id
            WHERE s.schedule_id = ?""",
            (schedule_id,),
            fetchone=True
        )
        
        if not session:
            bot.answer_callback_query(call.id, "Занятие не найдено")
            return
            
        session_text = (
            f"📅 Дата: {format_date(session['date'])}\n"
            f"⏰ Время: {session['time']}\n"
            f"📍 Локация: {session['location']}\n"
            f"🏋️ Тренер: {session['full_name'] or 'Не назначен'}\n"
            f"👥 Участники: {session['current_participants']}/{session['max_participants']}"
        )
        
        try:
            bot.edit_message_text(
                session_text,
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=get_schedule_actions_keyboard(schedule_id, session['location'])
            )
        except Exception as e:
            logger.warning(f"Couldn't edit message, sending new: {e}")
            bot.send_message(
                call.message.chat.id,
                session_text,
                reply_markup=get_schedule_actions_keyboard(schedule_id, session['location'])
            )
    except Exception as e:
        logger.error(f"Error managing session: {e}")
        bot.answer_callback_query(call.id, "Ошибка при загрузке занятия")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_schedule_"))
@admin_required
def edit_schedule(call: types.CallbackQuery):
    try:
        schedule_id = int(call.data.split("_")[2])
        session = db.execute(
            "SELECT * FROM schedule WHERE schedule_id = ?",
            (schedule_id,),
            fetchone=True
        )
        
        if not session:
            bot.answer_callback_query(call.id, "Занятие не найдено")
            return
            
        msg = bot.send_message(
            call.message.chat.id,
            f"Текущее время: {session['time']}\nВведите новое время в формате HH.MM:"
        )
        bot.register_next_step_handler(msg, lambda m: process_edit_time(m, schedule_id))
    except Exception as e:
        logger.error(f"Error editing schedule: {e}")
        bot.answer_callback_query(call.id, "Ошибка при редактировании")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_edit_time(message: types.Message, schedule_id: int):
    try:
        time_str = message.text.replace(':', '.')  # Заменяем двоеточия на точки
        try:
            hours, minutes = map(int, time_str.split('.'))
            if not (0 <= hours < 24 and 0 <= minutes < 60):
                raise ValueError
            time_str = f"{hours:02d}.{minutes:02d}"
        except ValueError:
            bot.send_message(message.chat.id, "Неверный формат времени. Используйте HH.MM (например, 18.30)")
            return

        db.execute(
            "UPDATE schedule SET time = ? WHERE schedule_id = ?",
            (time_str.replace('.', ':'), schedule_id)
        )
        
        bot.send_message(
            message.chat.id,
            f"✅ Время занятия успешно изменено на {time_str}",
            reply_markup=get_admin_menu()
        )
    except Exception as e:
        logger.error(f"Error processing edit time: {e}")
        bot.send_message(message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("delete_schedule_"))
@admin_required
def delete_schedule(call: types.CallbackQuery):
    try:
        schedule_id = int(call.data.split("_")[2])
        
        db.execute(
            "DELETE FROM schedule WHERE schedule_id = ?",
            (schedule_id,)
        )
        
        bot.answer_callback_query(call.id, "✅ Занятие удалено!")
        try:
            bot.edit_message_text(
                "Занятие успешно удалено",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )
        except:
            pass
        
        bot.send_message(
            call.message.chat.id,
            "Выберите действие:",
            reply_markup=get_admin_menu()
        )
    except Exception as e:
        logger.error(f"Error deleting schedule: {e}")
        bot.answer_callback_query(call.id, "Ошибка при удалении")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("back_to_dates_"))
@admin_required
def back_to_dates(call: types.CallbackQuery):
    try:
        location = call.data.split("_")[3]
        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(
                f"Выберите дату для локации {location}:",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=get_dates_keyboard(location)
            )
        except Exception as e:
            logger.warning(f"Couldn't edit message, sending new: {e}")
            bot.send_message(
                call.message.chat.id,
                f"Выберите дату для локации {location}:",
                reply_markup=get_dates_keyboard(location)
            )
    except Exception as e:
        logger.error(f"Error returning to dates: {e}")
        bot.answer_callback_query(call.id, "Ошибка при возврате")
        bot.send_message(call.message.chat.id, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "💾 Создать бэкап")
@admin_required
def create_backup(message: types.Message):
    try:
        if backup.send_backup():
            bot.send_message(message.chat.id, "✅ Бэкап успешно создан и отправлен!")
        else:
            bot.send_message(message.chat.id, "❌ Не удалось создать бэкап")
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

@bot.message_handler(func=lambda m: m.text == "🔄 Восстановить")
@admin_required
def restore_backup(message: types.Message):
    try:
        msg = bot.send_message(message.chat.id, "Пожалуйста, отправьте файл базы данных для восстановления:")
        bot.register_next_step_handler(msg, process_backup_file)
    except Exception as e:
        logger.error(f"Error starting restore: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

def process_backup_file(message: types.Message):
    try:
        if message.document:
            file_info = bot.get_file(message.document.file_id)
            downloaded_file = bot.download_file(file_info.file_path)
            
            with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
                tmp.write(downloaded_file)
                tmp.close()
                
                # Проверяем целостность файла
                try:
                    test_conn = sqlite3.connect(tmp.name)
                    test_conn.execute("SELECT 1 FROM sqlite_master")
                    test_conn.close()
                except sqlite3.Error as e:
                    os.unlink(tmp.name)
                    bot.send_message(message.chat.id, "❌ Файл поврежден или не является базой данных")
                    return
                
                # Заменяем текущую базу данных
                os.replace(tmp.name, 'crazyjump.db')
                
                # Переподключаем базу данных
                db.reconnect()
                
                bot.send_message(message.chat.id, "✅ База данных успешно восстановлена!")
        else:
            bot.send_message(message.chat.id, "Пожалуйста, отправьте файл базы данных")
    except Exception as e:
        logger.error(f"Error processing backup file: {e}")
        bot.send_message(message.chat.id, "⚠️ Произошла ошибка при восстановлении базы данных")

@bot.message_handler(func=lambda m: m.text == "📊 Статистика")
@admin_required
def show_stats(message: types.Message):
    try:
        stats = {
            "users": db.execute("SELECT COUNT(*) FROM users", fetchone=True)[0],
            "active_subs": db.execute("SELECT COUNT(*) FROM subscriptions WHERE status = 'active'", fetchone=True)[0],
            "trainers": db.execute("SELECT COUNT(*) FROM trainers", fetchone=True)[0],
            "upcoming_sessions": db.execute("SELECT COUNT(*) FROM schedule WHERE date >= date('now')", fetchone=True)[0]
        }

        stats_text = (
            "📊 Статистика системы:\n\n"
            f"👥 Пользователей: {stats['users']}\n"
            f"🎫 Активных абонементов: {stats['active_subs']}\n"
            f"🏋️ Тренеров: {stats['trainers']}\n"
            f"📅 Предстоящих занятий: {stats['upcoming_sessions']}"
        )

        bot.send_message(message.chat.id, stats_text)
    except Exception as e:
        logger.error(f"Error showing stats: {e}")
        bot.reply_to(message, "⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")

def ping_server():
    while True:
        try:
            if "REPLIT_ENV" in os.environ:
                requests.get(f"https://{REPLIT_PROJECT_NAME}.replit.app", timeout=10)
            time.sleep(300)
        except Exception as e:
            logger.error(f"Ping error: {e}")
            time.sleep(60)

def check_subscriptions():
    while True:
        try:
            if not db.conn or not db.check_integrity():
                if not db.reconnect():
                    time.sleep(3600)
                    continue

            soon = (datetime.now(TIMEZONE) + timedelta(days=3)).isoformat()
            subs = db.execute(
                """SELECT s.subscription_id, s.user_id, s.plan_name, s.expires_at, 
                u.username, u.notifications_enabled 
                FROM subscriptions s JOIN users u ON s.user_id = u.user_id 
                WHERE s.expires_at <= ? AND s.status = 'active'""",
                (soon,), 
                fetchall=True
            )

            if subs is None:
                logger.error("Failed to fetch subscriptions from database")
                time.sleep(3600)
                continue

            for sub in subs:
                if sub['notifications_enabled']:
                    try:
                        expires_date = datetime.fromisoformat(sub['expires_at']).strftime('%d.%m.%Y')
                        bot.send_message(
                            sub['user_id'],
                            f"⚠️ Ваш абонемент «{sub['plan_name']}» истекает {expires_date}!\n\n"
                            "Продлите его, чтобы продолжить занятия без перерывов.",
                            reply_markup=get_main_menu(sub['user_id'])
                        )
                        logger.info(f"Sent subscription reminder to user {sub['user_id']}")
                    except Exception as e:
                        logger.error(f"Failed to notify user {sub['user_id']}: {e}")

            time.sleep(86400)
        except Exception as e:
            logger.error(f"Subscription check error: {e}")
            time.sleep(3600)

def send_reminders():
    while True:
        try:
            if not db.conn or not db.check_integrity():
                if not db.reconnect():
                    time.sleep(3600)
                    continue

            tomorrow = (datetime.now(TIMEZONE) + timedelta(days=1)).strftime('%Y-%m-%d')

            sessions = db.execute(
                """SELECT s.schedule_id, s.date, s.time, s.location, t.full_name, 
                b.user_id, u.username
                FROM schedule s
                JOIN bookings b ON s.schedule_id = b.schedule_id
                JOIN users u ON b.user_id = u.user_id
                LEFT JOIN trainers t ON s.trainer_id = t.trainer_id
                WHERE s.date = ? AND b.status = 'active' 
                AND (u.reminders_enabled IS NULL OR u.reminders_enabled = 1)""",
                (tomorrow,),
                fetchall=True
            )

            if sessions is None:
                logger.error("Failed to fetch sessions from database")
                time.sleep(3600)
                continue

            for session in sessions:
                try:
                    bot.send_message(
                        session['user_id'],
                        f"⏰ Напоминание о занятии!\n\n"
                        f"📍 Локация: {session['location']}\n"
                        f"📅 Дата: {format_date(session['date'])}\n"
                        f"⏰ Время: {session['time']}\n"
                        f"🏋️ Тренер: {session['full_name'] or 'Не указан'}\n\n"
                        f"Не забудьте прийти!",
                        reply_markup=get_main_menu(session['user_id'])
                    )
                    logger.info(f"Sent reminder to user {session['user_id']}")
                except Exception as e:
                    logger.error(f"Failed to send reminder to {session['user_id']}: {e}")

            time.sleep(86400)
        except Exception as e:
            logger.error(f"Reminders error: {e}")
            time.sleep(3600)

def start_background_tasks():
    threading.Thread(target=ping_server, daemon=True).start()
    threading.Thread(target=check_subscriptions, daemon=True).start()
    threading.Thread(target=send_reminders, daemon=True).start()
    logger.info("Background tasks started")

if __name__ == '__main__':
    try:
        start_background_tasks()
        logger.info("Starting bot...")
        bot.infinity_polling()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
