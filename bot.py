import os
import logging
from flask import Flask, request
from dotenv import load_dotenv
import telebot
from telebot import types

# Загружаем переменные окружения
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 10000))

# Логгирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(name)

# Flask-приложение
app = Flask(name)

# Инициализация бота
bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

# Обработчик команды /ping
@bot.message_handler(commands=["ping"])
def handle_ping(message):
    bot.reply_to(message, "🏓 Понг от тестового бота!")

# Обработка webhook-запросов
@app.route("/webhook", methods=["POST"])
def webhook():
    if request.headers.get("content-type") == "application/json":
        json_str = request.get_data().decode("utf-8")
        logger.info(f"[WEBHOOK] Update: {json_str}")
        update = types.Update.de_json(json_str)
        bot.process_new_updates([update])
    return '', 200

# Проверка сервера
@app.route("/", methods=["GET", "HEAD"])
def index():
    return "Бот работает", 200

# Установка webhook при старте
def set_webhook():
    bot.remove_webhook()
    success = bot.set_webhook(f"{WEBHOOK_URL}/webhook")
    logger.info(f"Webhook установлен: {success}")

if name == "main":
    if not TOKEN or not WEBHOOK_URL:
        logger.error("Не указаны TELEGRAM_TOKEN или WEBHOOK_URL")
        exit(1)

    logger.info("Запуск бота...")
    set_webhook()
    app.run(host="0.0.0.0", port=PORT, debug=False)
