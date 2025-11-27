import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))  # Render сам прокидывает порт


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
            "и/или получать персональные раскладки и полезные подсказки в личку."
        )

        await update.message.reply_text(info_text, reply_markup=reply_markup)
    else:
        print(">>> WARNING: update.message is None в /start")


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


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button))

    print(">>> Starting bot with built‑in webhook server")

    # Встроенный веб‑сервер ptb: сам слушает порт и обрабатывает webhook’и.
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="",           # путь, можно оставить пустым
        allowed_updates=None,  # все типы апдейтов
    )


if __name__ == "__main__":
    main()
