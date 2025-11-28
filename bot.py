import os
import csv
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))

# Админы
ADMIN_IDS = {457388809, 8089136347}

CHANNEL_USERNAME = "@tatiataro"
CHANNEL_LINK = "https://t.me/tatiataro"

USERS_CSV = "users.csv"

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


# ===== утилиты для CSV =====

def ensure_csv_exists():
    if not os.path.exists(USERS_CSV):
        with open(USERS_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "user_id",
                "username",
                "first_name",
                "card_key",
                "date_iso",
                "subscribed",
            ])


def log_start(user_id: int, username: str | None,
              first_name: str | None, card_key: str | None):
    ensure_csv_exists()
    # Можно оставить так, предупреждение игнорируем
    date_iso = datetime.utcnow().isoformat(timespec="seconds")
    row = [
        user_id,
        username or "",
        first_name or "",
        card_key or "",
        date_iso,
        "unsub",
    ]
    with open(USERS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def mark_subscribed(user_id: int):
    if not os.path.exists(USERS_CSV):
        return

    rows = []
    with open(USERS_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append(r)

    # первая строка — хедер
    for i in range(1, len(rows)):
        if str(rows[i][0]) == str(user_id):
            rows[i][5] = "sub"

    with open(USERS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def load_users():
    if not os.path.exists(USERS_CSV):
        return []

    users = []
    with open(USERS_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            users.append(row)
    return users


def esc_md2(text: str) -> str:
    """Экранирование под MarkdownV2."""
    chars = r'_*[]()~`>#+-=|{}.!'
    for ch in chars:
        text = text.replace(ch, "\\" + ch)
    return text


# ===== хендлеры =====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(">>> /start handler called, update_id:", update.update_id)

    user = update.effective_user
    args = context.args

    card_key = args[0] if args else ""
    if card_key:
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

    # логируем переход
    log_start(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        card_key=card_key,
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


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id

    print(">>> button handler called, data:", data, "user_id:", user_id)

    await query.answer()

    if data == "subscribe":
        mark_subscribed(user_id)

        await query.edit_message_text(
            "✅ Вы добавлены в список рассылки.\n"
            "Буду время от времени присылать вам расклады и подсказки в личку."
        )


async def qr_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Эта команда только для администратора.")
        return

    users = load_users()
    if not users:
        await update.message.reply_text("Пока нет данных по переходам.")
        return

    lines = []
    for row in users:
        uid = row["user_id"]
        username = row["username"]
        first_name = row["first_name"]
        card_key = row["card_key"]
        date_iso = row["date_iso"]
        status = row["subscribed"]  # sub / unsub

        if username:
            link = esc_md2("@" + username)
        else:
            name = esc_md2(first_name or "user")
            link = f"[{name}](tg://user?id={uid})"

        line = (
            f"{link} — {esc_md2(card_key or '-')}"
            f" — {esc_md2(date_iso)} — {esc_md2(status)}"
        )
        lines.append(line)

    text = "Отчёт по переходам:\n\n" + "\n".join(lines)

    await update.message.reply_text(
        esc_md2("Отчёт по переходам:") + "\n\n" + "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN_V2,
        disable_web_page_preview=True,
    )


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(CommandHandler("qr_stats", qr_stats))

    print(">>> Starting bot with built‑in webhook server")

    base_url = os.getenv("BASE_URL")  # например https://tarot-bot-1-i003.onrender.com
    if not base_url:
        raise RuntimeError("BASE_URL не задан")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="",
        webhook_url=base_url,
        allowed_updates=None,
    )


if __name__ == "__main__":
    main()
