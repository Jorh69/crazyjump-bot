import os
import logging
from flask import Flask, request
from telebot import TeleBot, types
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 10000))

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(name)
bot = TeleBot(TOKEN, parse_mode='HTML')

@app.route('/')
def index():
    return 'Бот работает!', 200

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    return 'Invalid request', 400

@bot.message_handler(commands=['start'])
def handle_start(message):
    bot.send_message(message.chat.id, "✅ Вебхук работает! Бот запущен.")

if name == 'main':
    bot.remove_webhook()
    bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
    logger.info(f"Webhook установлен: {WEBHOOK_URL}/webhook")
    app.run(host='0.0.0.0', port=PORT)
