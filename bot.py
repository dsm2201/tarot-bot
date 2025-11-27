import os
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ===== НАСТРОЙКИ =====

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")       # например: https://tarot-bot-1.onrender.com
WEBHOOK_PATH = "/webhook"             # путь вебхука

CHANNEL_USERNAME = "@YourChannelUsername"
CHANNEL_LINK = "https://t.me/YourChannelUsername"

CARDS = {
    "Sun": (
        "🌞 Солнце\n\n"
        "Карта радости, успеха и ясности. "
        "Благоприятный период, всё складывается в вашу пользу."
    ),
    "Moon": (
        "🌙 Луна\n\n"
        "Интуиция обострена, возможны иллюзии и самообман. "
        "Слушайте внутренний голос, но проверяйте факты."
    ),
    "Star": (
        "⭐ Звезда\n\n"
        "Надежда, вдохновение и восстановление. "
        "Верьте в своё будущее — сейчас закладывается хороший фундамент."
    ),
    "Tower": (
        "⚡ Башня\n\n"
        "Резкие перемены, слом старого. "
        "Через кризис приходит освобождение от того, что больше не нужно."
    ),
    "Death": (
        "💀 Смерть\n\n"
        "Завершение этапа и трансформация. "
        "Что-то уходит, чтобы освободить место для нового."
    ),
    "Lovers": (
        "💞 Влюблённые\n\n"
        "Выбор сердцем, тема отношений и союза. "
        "Важно определиться, чего вы действительно хотите."
    ),
}

# ===== Flask-приложение =====

flask_app = Flask(__name__)
application: Application | None = None


# ===== ОБРАБОТЧИК /start =====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(">>> /start handler called, update_id:", update.update_id)

    args = context.args

    if args:
        card_key = args[0]
        text = CARDS.get(
            card_key,
            "Карта по этой ссылке не найдена 🤔\nПопробуйте другой QR-код или ссылку."
        )
    else:
        text = (
            "Привет! Это бот с таро‑мини‑раскладами по QR‑коду.\n\n"
            "Отсканируйте QR на карте или перейдите по ссылке из поста, "
            "чтобы получить расшифровку."
        )

    if update.message:
        await update.message.reply_text(text)

        keyboard = [
            [InlineKeyboardButton("📢 Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🔔 Получать рассылки в ЛС", callback_data="subscribe")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        info_text = (
            f"Если откликается расклад — можете подписаться на канал {CHANNEL_USERNAME} "
            "и/или получать персональные расклады и полезные подсказки в личку."
        )

        await update.message.reply_text(info_text, reply_markup=reply_markup)
    else:
        print(">>> WARNING: update.message is None в /start")


# ===== ОБРАБОТЧИК КНОПОК =====

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    print(">>> button handler called, data:", data, "user_id:", user_id)

    await query.answer()

    if data == "subscribe":
        try:
            with open("subs.txt", "a", encoding="utf-8") as f:
                f.write(f"{user_id}\n")
        except Exception as e:
            print(f"Ошибка записи в subs.txt: {e}")

        await query.edit_message_text(
            "✅ Вы добавлены в список рассылки.\n"
            "Буду время от времени присылать вам расклады и подсказки в личку."
        )


# ===== МАРШРУТЫ FLASK =====

@flask_app.route("/", methods=["GET"])
def index():
    return "Bot is running."


@flask_app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    """Эндпоинт, куда Telegram шлёт апдейты."""
    global application

    if application is None:
        print(">>> ERROR: application is None в webhook")
        return "Application not ready", 500

    data = request.get_json(force=True)
    print(">>> Got update JSON:", data)

    try:
        update = Update.de_json(data, application.bot)
        application.update_queue.put_nowait(update)
    except Exception as e:
        print(">>> ERROR while handling update:", e)
        return "Error", 500

    return "OK"


# ===== ИНИЦИАЛИЗАЦИЯ TELEGRAM APP =====

async def init_telegram_app():
    global application

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")

    if not BASE_URL:
        raise RuntimeError("BASE_URL не задан")

    print(">>> Initializing Application")
    application = Application.builder().token(BOT_TOKEN).updater(None).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))

    webhook_url = BASE_URL.rstrip("/") + WEBHOOK_PATH
    print(">>> Setting webhook to:", webhook_url)
    await application.bot.set_webhook(url=webhook_url)

    await application.initialize()
    await application.start()
    print(f">>> Bot started with webhook {webhook_url}")


def main():
    import asyncio

    asyncio.run(init_telegram_app())

    port = int(os.getenv("PORT", "10000"))
    print(">>> Starting Flask app on port", port)
    flask_app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
