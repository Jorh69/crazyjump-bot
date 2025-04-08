import os
import logging
from flask import Flask, request
from dotenv import load_dotenv
import telebot
from telebot import types

load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 10000))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

app = Flask(name)
bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

@bot.message_handler(commands=["start"])
def start_handler(message):
    bot.reply_to(message, "Привет! Я жив!")

@bot.message_handler(commands=["ping"])
def ping_handler(message):
    bot.reply_to(message, "🏓 Понг!")

@app.route("/webhook", methods=["POST"])
def webhook():
    if request.headers.get("content-type") == "application/json":
        update = types.Update.de_json(request.get_data().decode("utf-8"))
        logger.info(f"[WEBHOOK] Update: {update}")
        bot.process_new_updates([update])
    return '', 200

@app.route("/", methods=["GET"])
def index():
    return "OK", 200

def set_webhook():
    bot.remove_webhook()
    url = f"{WEBHOOK_URL}/webhook"
    bot.set_webhook(url)
    logger.info(f"Webhook установлен на {url}")

if name == "main":
    set_webhook()
    app.run(host="0.0.0.0", port=PORT)
