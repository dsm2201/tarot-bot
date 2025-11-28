import os
import csv
from datetime import datetime, UTC, timedelta
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

# ==== 6 карт под воронку новых клиентов ====
CARDS = {
    "Magician": (
        "🪄 Маг\n\n"
        "Сейчас перед вами открывается окно возможностей, которое бывает нечасто. "
        "Маг указывает, что у вас уже есть всё, чтобы сдвинуть важную тему с мёртвой точки — "
        "нужно лишь собрать волю, знания и ресурсы в одну линию.\n\n"
        "Эта карта часто выпадает тем, кто стоит на пороге нового этапа: смена работы, запуск дела, "
        "личная трансформация или выход из затянувшегося застоя. "
        "Если вы чувствуете, что \"давно пора\", но всё никак не начинается — это прямой знак во Вселенную.\n\n"
        "В канале {channel} разбираются такие состояния подробнее: как не слить импульс Мага в прокрастинацию, "
        "и во что именно сейчас лучше вложить свою энергию, чтобы не пожалеть о выборе."
    ).format(channel=CHANNEL_USERNAME),

    "HighPriestess": (
        "🌙 Верховная Жрица\n\n"
        "Сейчас снаружи может быть мало ясности, но внутри у вас уже есть ответы. "
        "Жрица приходит, когда разуму не хватает данных, а интуиция шепчет своё — и часто оказывается права.\n\n"
        "Карта говорит о скрытых процессах, тайных мотивах людей и ситуациях, где нельзя действовать в лоб. "
        "Это период, когда главное — настроиться на себя, ловить знаки и не разбрасываться своей энергией.\n\n"
        "В канале {channel} есть практики и разборы, которые помогают лучше слышать себя, "
        "отделять истинное чувство от тревожных фантазий и выбирать путь без ощущения, что \"иду вслепую\"."
    ).format(channel=CHANNEL_USERNAME),

    "Empress": (
        "🌿 Императрица\n\n"
        "Императрица — символ изобилия, роста и здоровой самоценности. "
        "Она появляется там, где важно наконец-то позволить себе больше: внимания, денег, удовольствий, "
        "заботы о теле и красоте жизни.\n\n"
        "Эта карта часто указывает на плодородную почву: идеи, отношения или проекты, которые при правильном "
        "уходе могут дать очень щедрый урожай. Вопрос только в том, позволите ли вы себе принять это.\n\n"
        "В канале {channel} много про то, как выходить из сценариев \"мне нельзя\", \"я недостойна\" "
        "и перестраивать реальность под себя, а не под чужие ожидания."
    ).format(channel=CHANNEL_USERNAME),

    "Lovers": (
        "💞 Влюблённые\n\n"
        "Карта Влюблённых почти никогда не про простой выбор — она про выбор, который влияет на вашу линию судьбы. "
        "Здесь переплетены темы отношений, партнёрства, доверия и верности себе.\n\n"
        "Сейчас может обостряться вопрос: с кем я иду дальше, во что вкладываю сердце и время, "
        "и где я предаю себя ради чужого спокойствия. Эта карта мягко, но настойчиво подталкивает к честности.\n\n"
        "В канале {channel} разбираются истории про выбор в любви и не только: как не застрять в старых связях, "
        "узнавать \"своих\" людей и не терять себя, даже если очень тянет в отношения."
    ).format(channel=CHANNEL_USERNAME),

    "Star": (
        "⭐ Звезда\n\n"
        "Звезда приходит тогда, когда внутри уже было непросто — и показывает, что полоса начинает меняться. "
        "Это карта тихой надежды, восстановления и медленного, но верного выхода к своему пути.\n\n"
        "Сейчас важно не гнать события, а настроиться на тот вектор, который действительно ваш. "
        "Звезда часто указывает на долгосрочные мечты, которые вы давно откладывали \"на потом\", "
        "и даёт знак: время осторожно, по шагам, возвращаться к ним.\n\n"
        "В канале {channel} вы найдёте расклады и подсказки для тех, кто выбирается из выгорания, "
        "ищет своё дело или просто хочет снова почувствовать, что жизнь не ограничивается выживанием."
    ).format(channel=CHANNEL_USERNAME),

    "Sun": (
        "🌞 Солнце\n\n"
        "Солнце — одна из самых сильных карт ясности и жизненной энергии. "
        "Оно высвечивает правду, усиливает ваши сильные стороны и помогает выйти из режима сомнений в режим действия.\n\n"
        "Сейчас может складываться ситуация, где вы наконец-то получаете подтверждение: вы на верном пути, "
        "и можно смелее заявлять о себе, своих талантах и желаниях. Главное — не спрятаться обратно в тень.\n\n"
        "В канале {channel} есть расклады про личную силу, самореализацию и то, как не обесценивать свои успехи, "
        "даже если кажется, что \"этого всё ещё мало\"."
    ).format(channel=CHANNEL_USERNAME),
}

CARD_KEYS = list(CARDS.keys())

# ===== утилиты =====

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


# ===== клиентские хендлеры =====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(">>> /start handler called, update_id:", update.update_id)

    user = update.effective_user
    args = context.args

    card_key = args[0] if args else ""
    if card_key:
        text = CARDS.get(
            card_key,
            "Для этой карты пока нет расшифровки, но вы можете заглянуть в канал {channel} "
            "и найти подсказки для своей ситуации там."
            .format(channel=CHANNEL_USERNAME)
        )
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

        keyboard = [
            [InlineKeyboardButton("📢 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🔔 Получать подсказки в ЛС", callback_data="subscribe")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

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

    if data == "subscribe":
        await query.edit_message_text(
            "✅ Откройте канал и убедитесь, что вы на него подписаны.\n"
            "Когда вы вернётесь к боту, он уже будет видеть вас как подписчика "
            "в статистике (если подписка оформлена)."
        )
    elif data == "st:menu":
        # открыть админ‑меню по кнопке
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("Эта функция только для администратора.")
            return
        keyboard = [
            [InlineKeyboardButton("📊 Сегодня: все карты", callback_data="st:today:all")],
            [InlineKeyboardButton("📊 Сегодня: по карте", callback_data="st:today:cards")],
            [InlineKeyboardButton("📅 Вчера: все карты", callback_data="st:yesterday:all")],
            [InlineKeyboardButton("📈 7 дней: все карты", callback_data="st:7days:all")],
            [InlineKeyboardButton("📁 Скачать CSV", callback_data="st:export:csv")],
        ]
        await query.edit_message_text(
            "Админ‑меню:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    elif data.startswith("st:"):
        await handle_stats_callback(update, context, data)


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
        [InlineKeyboardButton("📁 Скачать CSV", callback_data="st:export:csv")],
    ]
    await update.message.reply_text(
        "Админ‑меню:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

    # отдельная «кнопка входа» в админ‑панель
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

    parts = data.split(":")  # st:...
    action = parts[1]

    if action == "export":
        await send_csv_file(query)
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


# ===== авто‑уведомления =====

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
            await bot.send_message(chat_id=admin_id, text=text)
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


# ===== входная точка =====

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CommandHandler("debug_notify", debug_notify))
    app.add_handler(CallbackQueryHandler(button))

    print(">>> Starting bot with built‑in webhook server")

    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise RuntimeError("BASE_URL не задан")

    job_queue = app.job_queue
    job_queue.run_repeating(
        notify_admins,
        interval=1800,  # 30 минут
        first=300,
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
