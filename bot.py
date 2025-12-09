import os
import random
import csv
import json
from datetime import datetime, UTC, timedelta, time
from collections import defaultdict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.constants import ParseMode

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))

# Админы бота
ADMIN_IDS = {457388809, 8089136347}

# Канал
CHANNEL_USERNAME = "@tatiataro"
CHANNEL_LINK = "https://t.me/tatiataro"

USERS_CSV = "users.csv"
LAST_REPORT_FILE = "last_report_ts.txt"
NURTURE_LOG_CSV = "nurture_log.csv"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEXTS_DIR = os.path.join(BASE_DIR, "texts")
META_CARDS_DIR = os.path.join(BASE_DIR, "meta_cards")
DICE_DIR = os.path.join(BASE_DIR, "dice")

def load_json(name):
    path = os.path.join(TEXTS_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

CARDS = load_json("cards.json")
NURTURE_UNSUB = load_json("nurture_unsub.json")
NURTURE_SUB = load_json("nurture_sub.json")

CARD_KEYS = list(CARDS.keys())

# ===== утилиты CSV и дат =====

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
                "subscribed"
            ])

def ensure_nurture_log_exists():
    if not os.path.exists(NURTURE_LOG_CSV):
        with open(NURTURE_LOG_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "user_id",
                "card_key",
                "segment",          # unsub / sub
                "day_num",          # 1,3,7,14...
                "sent_at",
                "status",           # ok / error
                "error_msg",
                "subscribed_after"  # yes / no / ""
            ])


def log_start(user_id: int, username: str | None,
              first_name: str | None, card_key: str | None):
    ensure_csv_exists()
    date_iso = datetime.now(UTC).isoformat(timespec="seconds")
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


def update_subscribed_flag(user_id: int, is_sub: bool):
    if not os.path.exists(USERS_CSV):
        return

    rows = []
    with open(USERS_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        return

    for i in range(1, len(rows)):
        if str(rows[i][0]) == str(user_id):
            rows[i][5] = "sub" if is_sub else "unsub"

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
    if text is None:
        return ""
    chars = r'_*[]()~`>#+-=|{}.!'
    for ch in chars:
        text = text.replace(ch, "\\" + ch)
    return text


def parse_iso(dt_str: str) -> datetime | None:
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def load_last_report_ts() -> datetime:
    if not os.path.exists(LAST_REPORT_FILE):
        return datetime.now(UTC) - timedelta(hours=1)
    try:
        with open(LAST_REPORT_FILE, "r", encoding="utf-8") as f:
            s = f.read().strip()
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.now(UTC) - timedelta(hours=1)


def save_last_report_ts(ts: datetime):
    with open(LAST_REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(ts.isoformat(timespec="seconds"))

# ===== лимиты попыток на день =====

def _normalize_daily_counters(user_data: dict):
    """Сбрасывает счётчики на новый день и гарантирует наличие ключей."""
    today = datetime.now(UTC).date()

    last_meta_date = user_data.get("last_meta_date")
    last_dice_date = user_data.get("last_dice_date")

    # если дата не сегодняшняя — обнуляем счётчики
    if last_meta_date != today:
        user_data["last_meta_date"] = today
        user_data["meta_used"] = 0
    if last_dice_date != today:
        user_data["last_dice_date"] = today
        user_data["dice_used"] = 0

    # на всякий случай, если ключей нет
    user_data.setdefault("meta_used", 0)
    user_data.setdefault("dice_used", 0)


def get_meta_left(user_data: dict) -> int:
    _normalize_daily_counters(user_data)
    used = user_data.get("meta_used", 0)
    return max(0, 3 - used)


def get_dice_left(user_data: dict) -> int:
    _normalize_daily_counters(user_data)
    used = user_data.get("dice_used", 0)
    return max(0, 3 - used)


def build_main_keyboard(user_data: dict) -> InlineKeyboardMarkup:
    """Клавиатура с учётом количества оставшихся попыток."""
    meta_left = get_meta_left(user_data)
    dice_left = get_dice_left(user_data)

    meta_text = f"🃏 Метафорическая карта ({meta_left})"
    dice_text = f"🎲 Кубик выбора ({dice_left})"

    keyboard = [
        [InlineKeyboardButton("📢 Перейти в канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton("🔔 Получать подсказки в ЛС", callback_data="subscribe")],
        [InlineKeyboardButton(meta_text, callback_data="meta_card_today")],
        [InlineKeyboardButton(dice_text, callback_data="dice_today")],
    ]
    return InlineKeyboardMarkup(keyboard)

async def send_random_meta_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # находим чат (учитываем, что это может быть callback)
    chat = update.effective_chat
    if chat is None and update.callback_query:
        chat = update.callback_query.message.chat

    if chat is None:
        return

    # собираем список jpg/jpeg в папке meta_cards
    files = []
    for name in os.listdir(META_CARDS_DIR):
        lower = name.lower()
        if lower.endswith(".jpg") or lower.endswith(".jpeg"):
            files.append(os.path.join(META_CARDS_DIR, name))

    if not files:
        await chat.send_message("Пока нет ни одной карты в папке meta_cards.")
        return

    import random
    path = random.choice(files)

    with open(path, "rb") as f:
        await chat.send_photo(
            photo=f,
            caption="🃏 Ваша метафорическая карта на сегодня",
        )

async def send_random_dice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # находим чат (учитываем, что это может быть callback)
    chat = update.effective_chat
    if chat is None and update.callback_query:
        chat = update.callback_query.message.chat

    if chat is None:
        return

    # собираем список jpg/jpeg в папке dice
    files = []
    for name in os.listdir(DICE_DIR):
        lower = name.lower()
        if lower.endswith(".jpg") or lower.endswith(".jpeg"):
            files.append(os.path.join(DICE_DIR, name))

    if not files:
        await chat.send_message("Кубик пока не положили в папку dice.")
        return

    import random
    path = random.choice(files)

    with open(path, "rb") as f:
        await chat.send_photo(
            photo=f,
            caption="🎲 Кубик выбор",
        )
# ===== nurture‑лог =====

def log_nurture_event(user_id: int, card_key: str, segment: str,
                      day_num: int, status: str, error_msg: str = ""):
    ensure_nurture_log_exists()
    sent_at = datetime.now(UTC).isoformat(timespec="seconds")
    row = [
        str(user_id),
        card_key,
        segment,
        str(day_num),
        sent_at,
        status,
        error_msg,
        ""  # subscribed_after
    ]
    with open(NURTURE_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def update_nurture_subscribed_after():
    """Обновляет поле subscribed_after в nurture_log.csv на основе текущего статуса."""
    if not os.path.exists(NURTURE_LOG_CSV):
        return
    if not os.path.exists(USERS_CSV):
        return

    users = load_users()
    sub_map = {row["user_id"]: row["subscribed"] for row in users}

    rows = []
    with open(NURTURE_LOG_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        return

    header = rows[0]
    idx_user = header.index("user_id")
    idx_sub_after = header.index("subscribed_after")

    for i in range(1, len(rows)):
        uid = rows[i][idx_user]
        if rows[i][idx_sub_after]:
            continue
        status = sub_map.get(uid, "unsub")
        rows[i][idx_sub_after] = "yes" if status == "sub" else "no"

    with open(NURTURE_LOG_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


# ===== клиентские хендлеры =====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(">>> /start handler called, update_id:", update.update_id)

    user = update.effective_user
    args = context.args

    card_key = args[0] if args else ""
    if card_key and card_key in CARDS:
        card = CARDS[card_key]
        text = f"{card['title']}\n\n" + card["body"].format(channel=CHANNEL_USERNAME)
    elif card_key:
        text = (
            "Для этой карты пока нет расшифровки, но вы можете заглянуть в канал {channel} "
            "и найти подсказки для своей ситуации там."
        ).format(channel=CHANNEL_USERNAME)
    else:
        text = (
            "Привет! Это бот с таро‑мини‑раскладами по QR‑коду.\n\n"
            "Отсканируйте QR на карте или перейдите по ссылке из поста, "
            "чтобы получить свою расшифровку и дальнейшие подсказки в канале {channel}."
        ).format(channel=CHANNEL_USERNAME)

    log_start(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        card_key=card_key,
    )

    if update.message:
        await update.message.reply_text(text)

        # клавиатура с лимитами на день
        reply_markup = build_main_keyboard(context.user_data)

        info_text = (
            f"Если откликается эта карта — загляните в {CHANNEL_USERNAME}.\n"
            "Там больше раскладов, разборов и примеров, как такие состояния "
            "проигрываются в реальной жизни."
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

    user_data = context.user_data
    _normalize_daily_counters(user_data)

    if data == "subscribe":
        await query.edit_message_text(
            "✅ Откройте канал и убедитесь, что вы на него подписаны.\n"
            "Когда вы вернётесь к боту, он уже будет видеть вас как подписчика "
            "в статистике (если подписка оформлена)."
        )

    elif data == "meta_card_today":
        meta_used = user_data.get("meta_used", 0)
        if meta_used >= 3:
            await query.answer("Сегодня попытки метафорических карт закончились.", show_alert=True)
        else:
            user_data["meta_used"] = meta_used + 1
            await send_random_meta_card(update, context)

        # обновляем кнопки с новым количеством
        await query.edit_message_reply_markup(reply_markup=build_main_keyboard(user_data))

    elif data == "dice_today":
        dice_used = user_data.get("dice_used", 0)
        if dice_used >= 3:
            await query.answer("Сегодня попытки кубика выбора закончились.", show_alert=True)
        else:
            user_data["dice_used"] = dice_used + 1
            await send_random_dice(update, context)

        # обновляем кнопки с новым количеством
        await query.edit_message_reply_markup(reply_markup=build_main_keyboard(user_data))

    elif data == "st:menu":
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("Эта функция только для администратора.")
            return
        keyboard = [
            [InlineKeyboardButton("📊 Сегодня: все карты", callback_data="st:today:all")],
            [InlineKeyboardButton("📊 Сегодня: по карте", callback_data="st:today:cards")],
            [InlineKeyboardButton("📅 Вчера: все карты", callback_data="st:yesterday:all")],
            [InlineKeyboardButton("📈 7 дней: все карты", callback_data="st:7days:all")],
            [InlineKeyboardButton("📆 Всё время: все карты", callback_data="st:alltime:all")],
            [InlineKeyboardButton("📁 Скачать CSV", callback_data="st:export:csv")],
            [InlineKeyboardButton("📬 Воронка: 7 дней", callback_data="st:nurture:7days")]
        ]
        await query.edit_message_text(
            "Админ‑меню:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data.startswith("st:"):
        await handle_stats_callback(update, context, data)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатываем обычные текстовые сообщения от пользователя."""
    if not update.message:
        return

    text = (update.message.text or "").strip()
    lower = text.lower()

    # триггер на слово "расклад" в любом виде
    if "расклад" in lower:
        user = update.effective_user
        user_id = user.id
        username = user.username or ""
        first_name = user.first_name or ""

        # ответ пользователю
        reply = (
            "Поймала твой запрос на индивидуальный расклад. 💫\n\n"
            "Напиши, пожалуйста, про какую ситуацию хочешь посмотреть:\n"
            "– в чём сейчас вопрос/запрос;\n"
            "– какой формат тебе комфортнее (голосом, текстом, поэтапно).\n\n"
            "Я отвечу и предложу варианты по формату и стоимости."
        )
        await update.message.reply_text(reply)

        # уведомление админам
        admin_msg = (
            f"🔔 Запрос на РАСКЛАД\n"
            f"id: {user_id}\n"
            f"username: @{username if username else '—'}\n"
            f"имя: {first_name}\n"
            f"текст: {text}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(chat_id=admin_id, text=admin_msg)
            except Exception as e:
                print(f"send RASKLAD notify error to {admin_id}: {e}")
# ===== админ‑меню и статистика =====

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Эта команда только для администратора.")
        return

    keyboard = [
        [InlineKeyboardButton("📊 Сегодня: все карты", callback_data="st:today:all")],
        [InlineKeyboardButton("📊 Сегодня: по карте", callback_data="st:today:cards")],
        [InlineKeyboardButton("📅 Вчера: все карты", callback_data="st:yesterday:all")],
        [InlineKeyboardButton("📈 7 дней: все карты", callback_data="st:7days:all")],
        [InlineKeyboardButton("📆 Всё время: все карты", callback_data="st:alltime:all")],
        [InlineKeyboardButton("📁 Скачать CSV", callback_data="st:export:csv")],
        [InlineKeyboardButton("📬 Воронка: 7 дней", callback_data="st:nurture:7days")]
    ]
    await update.message.reply_text(
        "Админ‑меню:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

    entry_keyboard = [[InlineKeyboardButton("⚙ Открыть админ‑панель", callback_data="st:menu")]]
    await update.message.reply_text(
        "Кнопка для быстрого входа в админ‑панель:",
        reply_markup=InlineKeyboardMarkup(entry_keyboard),
    )


async def handle_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    query = update.callback_query
    user = query.from_user
    if user.id not in ADMIN_IDS:
        await query.edit_message_text("Эта функция только для администратора.")
        return

    parts = data.split(":")
    action = parts[1]

    if action == "export":
        await send_csv_file(query)
        return

    if action == "nurture":
        text = build_nurture_stats(days=7)
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True,
        )
        return

    if action == "today" and parts[2] == "cards":
        keyboard = []
        for key in CARD_KEYS:
            keyboard.append(
                [InlineKeyboardButton(key, callback_data=f"st:today:{key}")]
            )
        await query.edit_message_text(
            "Выберите карту для статистики за сегодня:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    now = datetime.now(UTC)
    if action == "today":
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = now
    elif action == "yesterday":
        y = now - timedelta(days=1)
        start_dt = y.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = y.replace(hour=23, minute=59, second=59, microsecond=0)
    elif action == "7days":
        start_dt = now - timedelta(days=7)
        end_dt = now
    elif action == "alltime":
        # очень ранняя дата как начало
        start_dt = datetime(2000, 1, 1, tzinfo=UTC)
        end_dt = now
    else:
        await query.edit_message_text("Неизвестное действие.")
        return

    card_filter = parts[2] if len(parts) > 2 else "all"

    text = await build_stats_text(context, start_dt, end_dt, card_filter)
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN_V2,
        disable_web_page_preview=True,
    )


async def build_stats_text(context: ContextTypes.DEFAULT_TYPE,
                           start_dt: datetime,
                           end_dt: datetime,
                           card_filter: str) -> str:
    bot = context.bot
    users = load_users()
    if not users:
        return esc_md2("Пока нет данных по переходам.")

    channel_id = CHANNEL_USERNAME

    unique_ids = {row["user_id"] for row in users}
    real_status: dict[str, str] = {}
    for uid in unique_ids:
        try:
            cm = await bot.get_chat_member(chat_id=channel_id, user_id=int(uid))
            if cm.status in ("creator", "administrator", "member"):
                real_status[uid] = "sub"
                update_subscribed_flag(int(uid), True)
            else:
                real_status[uid] = "unsub"
                update_subscribed_flag(int(uid), False)
        except Exception as e:
            print(f"get_chat_member error for {uid}: {e}")
            real_status[uid] = "unsub"
            update_subscribed_flag(int(uid), False)

    filtered = []
    for row in users:
        dt = parse_iso(row["date_iso"])
        if dt is None:
            continue
        if not (start_dt <= dt <= end_dt):
            continue
        if card_filter != "all" and row["card_key"] != card_filter:
            continue
        filtered.append(row)

    if not filtered:
        return esc_md2("В выбранный период переходов не было.")

    total_clicks = len(filtered)
    unique_users = {r["user_id"] for r in filtered}

    sub_users = {uid for uid in unique_users if real_status.get(uid) == "sub"}
    unsub_users = unique_users - sub_users

    per_card_clicks = defaultdict(int)
    per_card_subs = defaultdict(int)
    for row in filtered:
        ck = row["card_key"] or "-"
        per_card_clicks[ck] += 1
        if real_status.get(row["user_id"]) == "sub":
            per_card_subs[ck] += 1

    period_str = f"{start_dt.date()} — {end_dt.date()}"
    if start_dt.date() == end_dt.date():
        period_str = f"{start_dt.date()}"

    header = esc_md2(f"Статистика за {period_str}")
    if card_filter != "all":
        header += f" по карте {card_filter}"

    lines = []
    lines.append(header)
    lines.append("")
    lines.append(esc_md2(f"Всего переходов: {total_clicks}"))
    lines.append(esc_md2(f"Уникальных людей: {len(unique_users)}"))
    lines.append(esc_md2(f"Подписчиков среди них: {len(sub_users)}"))
    lines.append(esc_md2(f"Не подписаны: {len(unsub_users)}"))

    if total_clicks > 0:
        conv = round(len(sub_users) / total_clicks * 100, 1)
        lines.append(esc_md2(f"Общая конверсия: {conv}%"))

    lines.append("")
    lines.append(esc_md2("По картам:"))

    for ck in sorted(per_card_clicks.keys()):
        c = per_card_clicks[ck]
        s = per_card_subs.get(ck, 0)
        conv = round(s / c * 100, 1) if c > 0 else 0
        lines.append(esc_md2(f"{ck}: переходов {c}, подписчиков {s}, конверсия {conv}%"))

    # детальный список пользователей
    lines.append("")
    lines.append(esc_md2("Список пользователей:"))

    # сортируем по времени
    filtered_sorted = sorted(filtered, key=lambda r: r["date_iso"])

    for row in filtered_sorted:
        uid = row["user_id"]
        username = row["username"] or ""
        card = row["card_key"] or "-"
        date_iso = row["date_iso"]
        status = real_status.get(uid, row.get("subscribed", "unsub"))

        if username:
            name_part = f"@{username}"
        else:
            name_part = f"id{uid}"

        line = f"{name_part} — {card} — {date_iso} — {status}"
        lines.append(esc_md2(line))

    return "\n".join(lines)


def build_nurture_stats(days: int = 7) -> str:
    """Краткий отчёт по nurture за последние N дней на основе nurture_log.csv."""
    if not os.path.exists(NURTURE_LOG_CSV):
        return esc_md2("Лог автоворонки пока пуст.")

    now = datetime.now(UTC)
    since = now - timedelta(days=days)

    with open(NURTURE_LOG_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader]

    if not rows:
        return esc_md2("Лог автоворонки пока пуст.")

    total_sent = 0
    by_segment = defaultdict(int)
    by_segment_conv = defaultdict(int)
    by_day_segment = defaultdict(int)

    for r in rows:
        sent_at = parse_iso(r["sent_at"])
        if sent_at is None or sent_at < since:
            continue
        total_sent += 1
        seg = r["segment"]
        day_num = r["day_num"]
        by_segment[seg] += 1
        key = f"{seg}_day_{day_num}"
        by_day_segment[key] += 1
        if r.get("subscribed_after") == "yes":
            by_segment_conv[seg] += 1

    if total_sent == 0:
        return esc_md2(f"За последние {days} дней nurture‑сообщений не отправлялось.")

    lines = []
    lines.append(esc_md2(f"Автоворонка за последние {days} дней"))
    lines.append("")
    lines.append(esc_md2(f"Всего отправлено сообщений: {total_sent}"))
    for seg in ("unsub", "sub"):
        if by_segment[seg]:
            conv = round(by_segment_conv[seg] / by_segment[seg] * 100, 1) if by_segment[seg] > 0 else 0
            lines.append(esc_md2(f"{seg}: отправлено {by_segment[seg]}, подписалось после: {by_segment_conv[seg]} ({conv}%)"))

    lines.append("")
    lines.append(esc_md2("По шагам воронки:"))
    for key in sorted(by_day_segment.keys()):
        lines.append(esc_md2(f"{key}: {by_day_segment[key]}"))

    return "\n".join(lines)


async def send_csv_file(query):
    if not os.path.exists(USERS_CSV):
        await query.edit_message_text("Файл статистики пока не создан.")
        return

    with open(USERS_CSV, "rb") as f:
        await query.message.reply_document(
            document=InputFile(f, filename="users.csv"),
            caption="Файл со всеми переходами.",
        )
    await query.edit_message_reply_markup(reply_markup=None)


# ===== авто‑уведомления для админа =====

async def notify_admins_once(context: ContextTypes.DEFAULT_TYPE, force: bool = False):
    now = datetime.now(UTC)
    last_ts = load_last_report_ts()
    users = load_users()
    if not users:
        if force:
            text = "🔔 Проверка автоуведомления.\nНовых переходов и подписчиков нет."
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=text)
                except Exception as e:
                    print(f"notify_admins_once send error to {admin_id}: {e}")
        save_last_report_ts(now)
        return

    new_rows = []
    for row in users:
        dt = parse_iso(row["date_iso"])
        if dt is None:
            continue
        if dt > last_ts:
            new_rows.append(row)

    if not new_rows and not force:
        save_last_report_ts(now)
        return

    new_clicks = len(new_rows)
    per_card_clicks = defaultdict(int)
    for r in new_rows:
        per_card_clicks[r["card_key"] or "-"] += 1

    bot = context.bot
    channel_id = CHANNEL_USERNAME
    unique_ids = {r["user_id"] for r in new_rows}
    new_subs = set()

    for uid in unique_ids:
        try:
            cm = await bot.get_chat_member(chat_id=channel_id, user_id=int(uid))
            if cm.status in ("creator", "administrator", "member"):
                new_subs.add(uid)
                update_subscribed_flag(int(uid), True)
        except Exception as e:
            print(f"notify get_chat_member error for {uid}: {e}")

    if not new_rows and force:
        text = (
            "🔔 Проверка автоуведомления.\n"
            "За выбранный период новых переходов и подписчиков нет."
        )
    else:
        lines = []
        lines.append("🔔 Новые переходы по QR:")
        lines.append(f"Всего новых переходов: {new_clicks}")
        lines.append(f"Новых подписчиков (по факту в канале): {len(new_subs)}")
        lines.append("")
        lines.append("По картам за период:")
        for ck in sorted(per_card_clicks.keys()):
            lines.append(f"{ck}: {per_card_clicks[ck]}")
        text = "\n".join(lines)

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text)
        except Exception as e:
            print(f"notify_admins_once send error to {admin_id}: {e}")

    save_last_report_ts(now)


async def notify_admins(context: ContextTypes.DEFAULT_TYPE):
    await notify_admins_once(context, force=False)


async def debug_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Эта команда только для администратора.")
        return

    await update.message.reply_text("Запускаю тестовое автоуведомление...")
    await notify_admins_once(context, force=True)


# ===== автоворонка nurture (sub / unsub) =====

async def nurture_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Ежедневная джоба: проходит по пользователям,
    считает дни с момента первого захода и шлёт сообщения
    из NURTURE_UNSUB / NURTURE_SUB для нужных дней.
    Плюс обновляет subscribed_after в логе.
    """
    users = load_users()
    if not users:
        return

    now = datetime.now(UTC)
    bot = context.bot
    channel_id = CHANNEL_USERNAME

    by_user = {}
    for row in users:
        uid = row["user_id"]
        dt = parse_iso(row["date_iso"])
        if dt is None:
            continue
        if uid not in by_user:
            by_user[uid] = {
                "first_dt": dt,
                "last_row": row,
            }
        else:
            if dt < by_user[uid]["first_dt"]:
                by_user[uid]["first_dt"] = dt
            if dt > parse_iso(by_user[uid]["last_row"]["date_iso"]):
                by_user[uid]["last_row"] = row

    for uid, info in by_user.items():
        first_dt = info["first_dt"]
        row = info["last_row"]
        card_key = row["card_key"]
        if not card_key or card_key not in CARD_KEYS:
            continue

        days = (now.date() - first_dt.date()).days

        try:
            cm = await bot.get_chat_member(chat_id=channel_id, user_id=int(uid))
            is_sub = cm.status in ("creator", "administrator", "member")
            update_subscribed_flag(int(uid), is_sub)
        except Exception as e:
            print(f"nurture get_chat_member error for {uid}: {e}")
            is_sub = False
            update_subscribed_flag(int(uid), False)

        # unsub: дни 1, 3, 7
        if not is_sub and days in (1, 3, 7):
            day_num = days
            day_key = f"day_{days}"
            texts = NURTURE_UNSUB.get(card_key, {})
            msg_template = texts.get(day_key)
            if msg_template:
                text = msg_template.format(channel=CHANNEL_USERNAME)
                try:
                    await bot.send_message(chat_id=int(uid), text=text)
                    log_nurture_event(int(uid), card_key, "unsub", day_num, "ok")
                except Exception as e:
                    print(f"nurture unsub send error to {uid}: {e}")
                    log_nurture_event(int(uid), card_key, "unsub", day_num, "error", str(e))

        # sub: дни 3, 7, 14
        if is_sub and days in (3, 7, 14):
            day_num = days
            day_key = f"day_{days}"
            texts = NURTURE_SUB.get(card_key, {})
            msg_template = texts.get(day_key)
            if msg_template:
                text = msg_template.format(channel=CHANNEL_USERNAME)
                try:
                    await bot.send_message(chat_id=int(uid), text=text)
                    log_nurture_event(int(uid), card_key, "sub", day_num, "ok")
                except Exception as e:
                    print(f"nurture sub send error to {uid}: {e}")
                    log_nurture_event(int(uid), card_key, "sub", day_num, "error", str(e))

    update_nurture_subscribed_after()

# ===== ежедневное напоминание пользователям =====

async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Раз в день напоминает пользователям, что снова доступны
    3 попытки метафорической карты и 3 броска кубика.
    """
    users = load_users()
    if not users:
        return

    bot = context.bot
    # уникальные user_id из лога
    unique_ids = {int(row["user_id"]) for row in users if row.get("user_id")}

    text = (
        "Доброе утро! 🌅\n\n"
        "На сегодня снова доступны:\n"
        "🃏 3 попытки вытянуть метафорическую карту\n"
        "🎲 3 броска кубика выбора\n\n"
        "Нажми /start, чтобы начать свой день с подсказки и задать вопрос.\n"
        "Если чувствуешь, что ситуация повторяется — можно сделать индивидуальный развернутый расклад, "
        "просто напиши «РАСКЛАД» в ответ на сообщение бота."
    )

    for uid in unique_ids:
        try:
            await bot.send_message(chat_id=uid, text=text)
        except Exception as e:
            print(f"daily_reminder_job send error to {uid}: {e}")
# ===== входная точка =====

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CommandHandler("debug_notify", debug_notify))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print(">>> Starting bot with built‑in webhook server")

    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise RuntimeError("BASE_URL не задан")

    job_queue = app.job_queue
    job_queue.run_repeating(
        notify_admins,
        interval=1800,
        first=300,
    )
    job_queue.run_repeating(
        nurture_job,
        interval=24 * 3600,
        first=600,
    )
    # новая джоба: ежедневное напоминание
    job_queue.run_daily(
        daily_reminder_job,
        time=time(5, 0),   # 05:00 UTC ≈ 08:00 по Москве
        name="daily_reminder",
    )

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="",
        webhook_url=base_url,
        allowed_updates=None,
    )


if __name__ == "__main__":
    main()




