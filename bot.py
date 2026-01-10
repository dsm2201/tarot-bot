import os
import random
import csv
import json
from datetime import datetime, UTC, timedelta, time
from collections import defaultdict
from telegram.error import TimedOut

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

# ===== импорты для Google Sheets =====
import gspread
from gspread.auth import service_account_from_dict

BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))

# Админы бота
ADMIN_IDS = {457388809, 8089136347}

# Канал
CHANNEL_USERNAME = "@tatiataro"
CHANNEL_LINK = "https://t.me/tatiataro"

# Локальные файлы, которые ещё используем
LAST_REPORT_FILE = "last_report_ts.txt"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEXTS_DIR = os.path.join(BASE_DIR, "texts")
META_CARDS_DIR = os.path.join(BASE_DIR, "meta_cards")
DICE_DIR = os.path.join(BASE_DIR, "dice")
PACKS_DIR = os.path.join(BASE_DIR, "packs_images")
CARD_OF_DAY_DIR = os.path.join(BASE_DIR, "card_of_day_images")

# Статус карты дня: True = авто, False = ручная
CARD_OF_DAY_ENABLED = True
CARD_OF_DAY_STATUS = {}  # {"enabled": True/False}

# ===== настройки Google Sheets =====
GS_SERVICE_JSON = os.getenv("GS_SERVICE_JSON")
GS_SHEET_ID = os.getenv("GS_SHEET_ID")
USERS_SHEET_NAME = "users"
ACTIONS_SHEET_NAME = "actions"
NURTURE_SHEET_NAME = "nurture"
CARD_OF_DAY_SHEET_NAME = "card_of_day"

GS_CLIENT = None
GS_SHEET = None
GS_USERS_WS = None
GS_ACTIONS_WS = None
GS_NURTURE_WS = None
GS_CARD_OF_DAY_WS = None


def init_gs_client():
    """Инициализация клиента gspread из JSON в переменной окружения."""
    global GS_CLIENT, GS_SHEET, GS_USERS_WS, GS_ACTIONS_WS, GS_NURTURE_WS, GS_CARD_OF_DAY_WS

    if not GS_SERVICE_JSON or not GS_SHEET_ID:
        print(">>> Google Sheets: переменные GS_SERVICE_JSON / GS_SHEET_ID не заданы.")
        return

    try:
        info = json.loads(GS_SERVICE_JSON)
        client = service_account_from_dict(info)
        sheet = client.open_by_key(GS_SHEET_ID)
        users_ws = sheet.worksheet(USERS_SHEET_NAME)
        actions_ws = sheet.worksheet(ACTIONS_SHEET_NAME)
        try:
            nurture_ws = sheet.worksheet(NURTURE_SHEET_NAME)
        except Exception:
            nurture_ws = None

        try:
            card_of_day_ws = sheet.worksheet(CARD_OF_DAY_SHEET_NAME)
        except Exception:
            card_of_day_ws = None

        GS_CLIENT = client
        GS_SHEET = sheet
        GS_USERS_WS = users_ws
        GS_ACTIONS_WS = actions_ws
        GS_NURTURE_WS = nurture_ws
        GS_CARD_OF_DAY_WS = card_of_day_ws
        print(">>> Google Sheets: успешно подключено к tatiataro_log.")
    except Exception as e:
        print(f">>> Google Sheets init error: {e}")
        GS_CLIENT = None
        GS_SHEET = None
        GS_USERS_WS = None
        GS_ACTIONS_WS = None
        GS_NURTURE_WS = None
        GS_CARD_OF_DAY_WS = None


def load_json(name):
    path = os.path.join(TEXTS_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


CARDS = load_json("cards.json")
NURTURE_UNSUB = load_json("nurture_unsub.json")
NURTURE_SUB = load_json("nurture_sub.json")

CARD_KEYS = list(CARDS.keys())

# ===== утилиты дат и текста =====


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

# ===== логирование в Google Sheets =====


def log_start_to_sheet(user, card_key: str | None):
    """Лог входа пользователя в лист users."""
    if GS_USERS_WS is None:
        return
    date_iso = datetime.now(UTC).isoformat(timespec="seconds")
    row = [
        str(user.id),
        user.username or "",
        user.first_name or "",
        card_key or "",
        date_iso,
        "unsub",
    ]
    try:
        GS_USERS_WS.append_row(row, value_input_option="RAW")
    except Exception as e:
        print(f">>> log_start_to_sheet error: {e}")


def log_action_to_sheet(user, action: str, source: str = "unknown"):
    """Лог действия пользователя в лист actions."""
    if GS_ACTIONS_WS is None:
        return
    ts_iso = datetime.now(UTC).isoformat(timespec="seconds")
    row = [
        str(user.id),
        user.username or "",
        user.first_name or "",
        action,
        source,
        ts_iso,
    ]
    try:
        GS_ACTIONS_WS.append_row(row, value_input_option="RAW")
    except Exception as e:
        print(f">>> log_action_to_sheet error: {e}")


def log_nurture_to_sheet(user_id: int, card_key: str, segment: str,
                         day_num: int, status: str, error_msg: str = ""):
    """Лог nurture-сообщения в лист nurture."""
    if GS_NURTURE_WS is None:
        return
    sent_at = datetime.now(UTC).isoformat(timespec="seconds")
    row = [
        str(user_id),
        card_key,
        segment,
        str(day_num),
        sent_at,
        status,
        error_msg,
        "",  # subscribed_after
    ]
    try:
        GS_NURTURE_WS.append_row(row, value_input_option="RAW")
    except Exception as e:
        print(f">>> log_nurture_to_sheet error: {e}")

# ===== чтение из Google Sheets =====

def log_card_of_day_publish(card_name: str, mode: str = "auto"):
    """Логируем публикацию карты дня в Google Sheets."""
    if GS_ACTIONS_WS is None:
        return
    ts_iso = datetime.now(UTC).isoformat(timespec="seconds")
    row = [
        "0",  # system
        "bot",
        "card_of_day",
        f"card_of_day_publish_{card_name}",
        mode,
        ts_iso,
    ]
    try:
        GS_ACTIONS_WS.append_row(row, value_input_option="RAW")
    except Exception as e:
        print(f">>> log_card_of_day_publish error: {e}")

def get_card_of_day_stats(days: int = 7) -> str:
    """Статистика по карте дня за последние N дней."""
    rows = load_actions()
    if not rows:
        return esc_md2("Статистика карты дня пока пуста.")
    
    now = datetime.now(UTC)
    since = now - timedelta(days=days)
    
    card_publishes = []
    for r in rows:
        ts_iso = r.get("ts_iso", "").strip()
        action = r.get("action", "").strip()
        
        if "card_of_day" not in action:
            continue
        
        ts = parse_iso(ts_iso)
        if ts is None or ts < since:
            continue
        
        card_publishes.append(r)
    
    if not card_publishes:
        return esc_md2(f"За последние {days} дней карта дня не публиковалась.")
    
    total = len(card_publishes)
    auto_count = sum(1 for r in card_publishes if r.get("source") == "auto")
    manual_count = sum(1 for r in card_publishes if r.get("source") == "manual")
    
    lines = []
    lines.append(esc_md2(f"Статистика карты дня за {days} дней"))
    lines.append("")
    lines.append(esc_md2(f"Всего публикаций: {total}"))
    lines.append(esc_md2(f"Автоматических (🤖): {auto_count}"))
    lines.append(esc_md2(f"Ручных (👋): {manual_count}"))
    
    return "\n".join(lines)

def load_users() -> list[dict]:
    """Читаем всех пользователей из листа users."""
    if GS_USERS_WS is None:
        return []
    try:
        records = GS_USERS_WS.get_all_records()
        # гарантируем строковые user_id
        for r in records:
            r["user_id"] = str(r.get("user_id", "")).strip()
            r["card_key"] = (r.get("card_key") or "").strip()
            r["date_iso"] = (r.get("date_iso") or "").strip()
            r["subscribed"] = (r.get("subscribed") or "").strip()
        return records
    except Exception as e:
        print(f">>> load_users (Sheets) error: {e}")
        return []


def load_actions() -> list[dict]:
    """Читаем лог действий из листа actions."""
    if GS_ACTIONS_WS is None:
        return []
    try:
        records = GS_ACTIONS_WS.get_all_records()
        for r in records:
            r["user_id"] = str(r.get("user_id", "")).strip()
            r["action"] = (r.get("action") or "").strip()
            r["source"] = (r.get("source") or "").strip()
            r["ts_iso"] = (r.get("ts_iso") or "").strip()
            r["username"] = (r.get("username") or "").strip()
            r["first_name"] = (r.get("first_name") or "").strip()
        return records
    except Exception as e:
        print(f">>> load_actions (Sheets) error: {e}")
        return []


def load_nurture_rows() -> list[dict]:
    """Читаем nurture-лог из листа nurture."""
    if GS_NURTURE_WS is None:
        return []
    try:
        records = GS_NURTURE_WS.get_all_records()
        for r in records:
            r["user_id"] = str(r.get("user_id", "")).strip()
            r["card_key"] = (r.get("card_key") or "").strip()
            r["segment"] = (r.get("segment") or "").strip()
            r["day_num"] = str(r.get("day_num", "")).strip()
            r["sent_at"] = (r.get("sent_at") or "").strip()
            r["status"] = (r.get("status") or "").strip()
            r["error_msg"] = (r.get("error_msg") or "").strip()
            r["subscribed_after"] = (r.get("subscribed_after") or "").strip()
        return records
    except Exception as e:
        print(f">>> load_nurture_rows (Sheets) error: {e}")
        return []

# ===== обновление статуса подписки в Sheets =====


def update_subscribed_flag(user_id: int, is_sub: bool):
    """Обновляем поле subscribed для всех строк этого user_id в листе users."""
    if GS_USERS_WS is None:
        return
    try:
        all_values = GS_USERS_WS.get_all_values()
        if not all_values:
            return

        header = all_values[0]
        try:
            idx_id = header.index("user_id")
            idx_sub = header.index("subscribed")
        except ValueError:
            print(">>> update_subscribed_flag: нет нужных столбцов в users")
            return

        target_id = str(user_id)
        for i in range(1, len(all_values)):
            row = all_values[i]
            if len(row) <= max(idx_id, idx_sub):
                continue
            if row[idx_id].strip() == target_id:
                row[idx_sub] = "sub" if is_sub else "unsub"
                GS_USERS_WS.update_cell(i + 1, idx_sub + 1, row[idx_sub])
    except Exception as e:
        print(f">>> update_subscribed_flag (Sheets) error: {e}")

# ===== лимиты попыток на день =====


def _normalize_daily_counters(user_data: dict):
    today = datetime.now(UTC).date()

    last_meta_date = user_data.get("last_meta_date")
    last_dice_date = user_data.get("last_dice_date")

    if last_meta_date != today:
        user_data["last_meta_date"] = today
        user_data["meta_used"] = 0
    if last_dice_date != today:
        user_data["last_dice_date"] = today
        user_data["dice_used"] = 0

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
    meta_left = get_meta_left(user_data)
    dice_left = get_dice_left(user_data)

    meta_text = f"🃏 Метафорическая карта ({meta_left})"
    dice_text = f"🎲 Кубик выбора ({dice_left})"

    keyboard = [
        [InlineKeyboardButton("📢 Перейти в канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton("🔔 Получать подсказки в ЛС", callback_data="subscribe")],
        [InlineKeyboardButton(meta_text, callback_data="meta_card_today")],
        [InlineKeyboardButton(dice_text, callback_data="dice_today")],
        [InlineKeyboardButton("📚 Расклады", callback_data="packs_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_pack_description(code: str) -> tuple[str, str, str]:
    """Название, описание и имя файла расклада по коду."""
    if code == "grapes12":
        title = "🍇 «12 виноградин» — Новогодний ритуал"
        desc = (
            "12 виноградин — 12 желаний на новый год.\n\n"
            "Мы смотрим, какие темы года просятся в твою жизнь, где важно загадать желание, "
            "а где — отпустить ожидания и освободить место под новое."
        )
        filename = "grapes12.jpg"
    elif code == "bye_year":
        title = "👋 «Прощай, уходящий год»"
        desc = (
            "Мягкий разбор уходящего года: что забрать с собой как ресурс, "
            "что оставить, и какие уроки уже пройдены.\n\n"
            "Подходит, если хочется закрыть хвосты, перестать вариться в прошлом "
            "и перейти в новый год легче."
        )
        filename = "bye_year.jpg"
    elif code == "mission":
        title = "🌟 «Луч миссии» — Предназначение"
        desc = (
            "Расклад про твоё внутреннее направление: в чём твой смысл, "
            "через что ты естественно проявляешься и где теряется опора.\n\n"
            "Помогает поймать ориентир, если кажется, что живёшь не своей жизнью."
        )
        filename = "mission.jpg"
    elif code == "anchor":
        title = "🪨 «Точка опоры» — Состояние и ресурс"
        desc = (
            "Смотрим, на что ты сейчас опираешься внутри и снаружи, "
            "какие ресурсы уже есть, а какие проседают.\n\n"
            "Подходит, когда шатает, накрывают качели и хочется устойчивости."
        )
        filename = "anchor.jpg"
    elif code == "money":
        title = "💰 «Финансовый ключ» — Деньги и благополучие"
        desc = (
            "Расклад про деньги: твои установки, сценарии и точки роста.\n\n"
            "Помогает увидеть, где ты сам себе перекрываешь поток, а где есть реальные "
            "возможности для увеличения дохода."
        )
        filename = "money.jpg"
    elif code == "choice":
        title = "🧭 «Компас выбора» — Выбор и развилки"
        desc = (
            "Когда стоишь на развилке и не понимаешь, куда свернуть.\n\n"
            "Смотрим, что стоит за каждым вариантом, какие последствия у выбора "
            "и где больше жизни и ресурса для тебя."
        )
        filename = "choice.jpg"
    elif code == "career":
        title = "🚀 «Разворот в работе» — Карьера и успех"
        desc = (
            "Расклад про работу, карьеру и самореализацию.\n\n"
            "Подходит, если хочется перемен в профессии, перехода в новое дело "
            "или ясности, в какую сторону разворачиваться."
        )
        filename = "career.jpg"
    elif code == "love":
        title = "💞 «Точка притяжения» — Любовь и отношения"
        desc = (
            "Расклад про твою точку притяжения в отношениях: каких партнёров ты притягиваешь, "
            "какой динамике склонна пара и где твоя зона влияния.\n\n"
            "Подходит и для текущих отношений, и для запроса «почему не складывается»."
        )
        filename = "love.jpg"
    else:
        title = "Расклад"
        desc = "Описание этого расклада появится чуть позже."
        filename = ""

    return title, desc, filename

# ===== отправка картинок =====


async def send_random_meta_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat is None and update.callback_query:
        chat = update.callback_query.message.chat

    if chat is None:
        return

    files = []
    for name in os.listdir(META_CARDS_DIR):
        lower = name.lower()
        if lower.endswith(".jpg") or lower.endswith(".jpeg"):
            files.append(os.path.join(META_CARDS_DIR, name))

    if not files:
        await chat.send_message("Пока нет ни одной карты в папке meta_cards.")
        return

    path = random.choice(files)

    with open(path, "rb") as f:
        try:
            await chat.send_photo(
                photo=f,
                caption="🃏 Ваша метафорическая карта на сегодня",
            )
        except TimedOut:
            await chat.send_message(
                "Сейчас не получилось отправить карту (таймаут Telegram).\n"
                "Попробуй, пожалуйста, ещё раз чуть позже."
            )
        except Exception as e:
            print(f"send_random_meta_card error: {e}")
            await chat.send_message(
                "Произошла ошибка при отправке карты. Попробуй ещё раз позже."
            )


async def send_random_dice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat is None and update.callback_query:
        chat = update.callback_query.message.chat

    if chat is None:
        return

    files = []
    for name in os.listdir(DICE_DIR):
        lower = name.lower()
        if lower.endswith(".jpg") or lower.endswith(".jpeg"):
            files.append(os.path.join(DICE_DIR, name))

    if not files:
        await chat.send_message("Кубик пока не положили в папку dice.")
        return

    path = random.choice(files)

    with open(path, "rb") as f:
        try:
            await chat.send_photo(
                photo=f,
                caption="🎲 Кубик выбор",
            )
        except TimedOut:
            await chat.send_message(
                "Сейчас не получилось отправить картинку кубика (таймаут Telegram).\n"
                "Попробуй, пожалуйста, ещё раз чуть позже."
            )
        except Exception as e:
            print(f"send_random_dice error: {e}")
            await chat.send_message(
                "Произошла ошибка при отправке кубика. Попробуй ещё раз позже."
            )

# ===== nurture: подсчёт subscribed_after в Sheets =====

def load_card_of_the_day() -> dict | None:
    """Загружаем случайную карту дня из Google Sheets."""
    if GS_CARD_OF_DAY_WS is None:
        return None
    try:
        records = GS_CARD_OF_DAY_WS.get_all_records()
        if not records:
            return None
# Получаем веса (по умолчанию 1 если не указано)
        weights = []
        for record in records:
            weight = record.get("weight", 1)
            try:
                weight = float(weight) if weight else 1
                if weight < 0:
                    weight = 1
            except (ValueError, TypeError):
                weight = 1
            weights.append(weight)
        
        # Выбираем карту с учетом весов
        selected = random.choices(records, weights=weights, k=1)[0]
        return selected    except Exception as e:
        print(f">>> load_card_of_the_day error: {e}")
        return None

async def send_card_of_the_day_to_channel(context: ContextTypes.DEFAULT_TYPE):
    """Отправляет карту дня в канал если карта дня включена."""
    # Проверяем статус
    if not CARD_OF_DAY_STATUS.get("enabled", True):
        print(">>> Карта дня отключена (ручной режим)")
        return
    
    card_data = load_card_of_the_day()
    if card_data is None:
        print(">>> send_card_of_the_day_to_channel: нет данных")
        return
    
    file_name = card_data.get("file_name", "").strip()
    card_title = card_data.get("card_title", "").strip()
    text = card_data.get("text", "").strip()
    
    if not file_name or not text:
        print(">>> send_card_of_the_day_to_channel: неполные данные в Sheets")
        return
    
    image_path = os.path.join(CARD_OF_DAY_DIR, file_name)
    if not os.path.exists(image_path):
        print(f">>> send_card_of_the_day_to_channel: файл не найден {image_path}")
        return
    
    try:
        with open(image_path, "rb") as f:
            await context.bot.send_photo(
                chat_id=CHANNEL_USERNAME,
                photo=f,
                caption=text,
                parse_mode=ParseMode.HTML,
            )
        print(f">>> Карта дня опубликована: {card_title}")
        # Логируем публикацию
        log_card_of_day_publish(card_title, "auto")
    except Exception as e:
        print(f">>> send_card_of_the_day_to_channel error: {e}")

def update_nurture_subscribed_after():
    """Проставляем subscribed_after в nurture по актуальному статусу подписки из users."""
    if GS_NURTURE_WS is None or GS_USERS_WS is None:
        return

    users = load_users()
    if not users:
        return

    sub_map = {row["user_id"]: row.get("subscribed", "unsub") for row in users}

    try:
        all_values = GS_NURTURE_WS.get_all_values()
        if not all_values:
            return
        header = all_values[0]
        try:
            idx_user = header.index("user_id")
            idx_sub_after = header.index("subscribed_after")
        except ValueError:
            print(">>> nurture sheet: нет нужных столбцов")
            return

        for i in range(1, len(all_values)):
            row = all_values[i]
            if len(row) <= max(idx_user, idx_sub_after):
                continue
            if row[idx_sub_after]:
                continue
            uid = row[idx_user].strip()
            status = sub_map.get(uid, "unsub")
            val = "yes" if status == "sub" else "no"
            GS_NURTURE_WS.update_cell(i + 1, idx_sub_after + 1, val)
    except Exception as e:
        print(f">>> update_nurture_subscribed_after (Sheets) error: {e}")

# ===== клиентские хендлеры =====


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(">>> /start handler called, update_id:", update.update_id)

    user = update.effective_user
    args = context.args

    # разбираем аргументы /start
    source = "direct"
    card_key = ""
    special_start = ""  # channel / rasklad / другое спец-значение

       if args:
        arg0 = args[0]
        if arg0 == "channel":
            source = "channel"
            special_start = "channel"
        elif arg0 == "rasklad":
            source = "channel"
            special_start = "rasklad"
        elif arg0 == "day_card":  # ДОбавляем эту строку
            source = "card_of_day"
            special_start = "day_card"
        elif arg0 in CARDS:
            source = "qr"
            card_key = arg0
        else:
            source = "direct"

    # текстовая логика под разные сценарии
    if card_key and card_key in CARDS:
        # заход по конкретной карте (QR)
        card = CARDS[card_key]
        text = f"{card['title']}\n\n" + card["body"].format(channel=CHANNEL_USERNAME)
        info_text = (
            f"Если откликается эта карта — загляните в {CHANNEL_USERNAME}.\n"
            "Там больше раскладов, разборов и примеров, как такие состояния "
            "проигрываются в реальной жизни."
        )

    elif special_start == "rasklad":
        # заход из поста «хочу личный расклад»
        text = (
            "Вижу, что ты пришёл за личным раскладом. 💫\n\n"
            "Напиши, пожалуйста, пару слов про свою ситуацию:\n"
            "– про что хочешь посмотреть (отношения, деньги, выбор, путь и т.п.);\n"
            "– как тебе комфортно получать разбор (голосом, текстом, поэтапно).\n\n"
            "Я посмотрю запрос и предложу несколько форматов по глубине и стоимости."
        )
        info_text = (
            f"Если по ходу переписки захочешь ещё подумать — в {CHANNEL_USERNAME} "
            "много бесплатных раскладов и примеров разборов."
        )

        elif special_start == "day_card":
        text = (
            "Вижу, что ты пришёл из карты дня! 🃏\\n\\n"
            "Если эта карта откликается — можешь вернуться в основное меню и вытянуть ещё одну, "
            "или сделать индивидуальный расклад для более глубокого разбора."
        )
        info_text = (
            f"Подписывайся на {CHANNEL_USERNAME}, чтобы не пропускать карты дня. "
            "Здесь же можешь использовать метафорические карты и кубик выбора."
        )

    elif card_key:
        # неизвестный card_key (на всякий случай)
        text = (
            "Для этой карты пока нет расшифровки, но вы можете заглянуть в канал {channel} "
            "и найти подсказки для своей ситуации там."
        ).format(channel=CHANNEL_USERNAME)
        info_text = (
            f"Загляните в {CHANNEL_USERNAME} — там больше раскладов и разборов, "
            "можно найти подсказки под свою ситуацию."
        )

    else:
        # обычный /start без параметров
        text = (
            "Привет! Это бот с таро‑мини‑раскладами.\n\n"
            "Здесь можно каждый день вытягивать метафорическую карту и бросать кубик выбора, "
            "а ещё оставить запрос на личный расклад."
        )
        info_text = (
            f"Подписывайся на {CHANNEL_USERNAME}, чтобы не пропускать расклады и подсказки, "
            "а здесь жми кнопки ниже — начнём с карты и кубика."
        )

    # лог в Google Sheets
    log_start_to_sheet(user, card_key)

    # лог действия (вход)
    action_name = "enter_from_channel" if source == "channel" else "enter_bot"
    log_action_to_sheet(user, action_name, source)

    if update.message:
        await update.message.reply_text(text)

        reply_markup = build_main_keyboard(context.user_data)

        await update.message.reply_text(info_text, reply_markup=reply_markup)
    else:
        print(">>> WARNING: update.message is None в /start")


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user = query.from_user
    user_id = user.id

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
            # лог действия
            log_action_to_sheet(user, "meta_card", "bot")

        await query.edit_message_reply_markup(reply_markup=build_main_keyboard(user_data))

    elif data == "dice_today":
        dice_used = user_data.get("dice_used", 0)
        if dice_used >= 3:
            await query.answer("Сегодня попытки кубика выбора закончились.", show_alert=True)
        else:
            user_data["dice_used"] = dice_used + 1
            await send_random_dice(update, context)
            # лог действия
            log_action_to_sheet(user, "dice", "bot")

        await query.edit_message_reply_markup(reply_markup=build_main_keyboard(user_data))

    elif data == "st:menu":
        if user_id not in ADMIN_IDS:
            await query.edit_message_text("Эта функция только для администратора.")
            return
    
        # Статус карты дня
        cod_status = "🤖 Авто" if CARD_OF_DAY_STATUS.get("enabled", True) else "👋 Ручная"
    
        keyboard = [
            [InlineKeyboardButton(f"📅 Карта дня: {cod_status}", callback_data="st:cod_status")],
            [InlineKeyboardButton("📊 Сегодня: все карты", callback_data="st:today:all")],
            [InlineKeyboardButton("📊 Сегодня: по карте", callback_data="st:today:cards")],
            [InlineKeyboardButton("📅 Вчера: все карты", callback_data="st:yesterday:all")],
            [InlineKeyboardButton("📈 7 дней: все карты", callback_data="st:7days:all")],
            [InlineKeyboardButton("📆 Всё время: все карты", callback_data="st:alltime:all")],
            [InlineKeyboardButton("📬 Воронка: 7 дней", callback_data="st:nurture:7days")],
            [InlineKeyboardButton("🧭 Действия: сегодня", callback_data="st:actions:today")],
            [InlineKeyboardButton("🧭 Действия: вчера", callback_data="st:actions:yesterday")],
            [InlineKeyboardButton("🧭 Действия: 7 дней", callback_data="st:actions:7days")],
            [InlineKeyboardButton("🔄 Обновить попытки", callback_data="st:reset_attempts")],
        ]
        await query.edit_message_text(
            "Админ‑меню:",
            reply_markup=InlineKeyboardMarkup(keyboard),
    )

    elif data == "packs_menu":
        # подменю с раскладами
        packs_keyboard = [
            [InlineKeyboardButton("🍇 12 виноградин", callback_data="pack:grapes12")],
            [InlineKeyboardButton("👋 Прощай, уходящий год", callback_data="pack:bye_year")],
            [InlineKeyboardButton("🌟 Луч миссии", callback_data="pack:mission")],
            [InlineKeyboardButton("🪨 Точка опоры", callback_data="pack:anchor")],
            [InlineKeyboardButton("💰 Финансовый ключ", callback_data="pack:money")],
            [InlineKeyboardButton("🧭 Компас выбора", callback_data="pack:choice")],
            [InlineKeyboardButton("🚀 Разворот в работе", callback_data="pack:career")],
            [InlineKeyboardButton("💞 Точка притяжения", callback_data="pack:love")],
        ]
        await query.edit_message_text(
            "Выбери расклад, который откликается:",
            reply_markup=InlineKeyboardMarkup(packs_keyboard),
        )

    elif data.startswith("pack:"):
        # показать описание выбранного расклада и кнопку "выбрать"
        code = data.split(":", 1)[1]
        title, desc, filename = get_pack_description(code)

        text = f"{title}\n\n{desc}"

        select_keyboard = [
            [InlineKeyboardButton("✅ Выбрать этот расклад", callback_data=f"pack_select:{code}")],
            [InlineKeyboardButton("⬅️ Назад к списку", callback_data="packs_menu")],
        ]

        if filename:
            image_path = os.path.join(PACKS_DIR, filename)
            try:
                with open(image_path, "rb") as f:
                    await query.message.reply_photo(
                        photo=f,
                        caption=text,
                        reply_markup=InlineKeyboardMarkup(select_keyboard),
                    )
                await query.edit_message_reply_markup(reply_markup=None)
            except FileNotFoundError:
                print(f"pack image not found: {image_path}")
                await query.edit_message_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(select_keyboard),
                )
        else:
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(select_keyboard),
            )

    elif data.startswith("pack_select:"):
        # человек нажал "выбрать расклад"
        code = data.split(":", 1)[1]
        title, _ = get_pack_description(code)

        # ответ пользователю
        reply = (
            f"Поймала твой запрос на расклад «{title}». 💫\n\n"
            "Напиши пару слов про свою ситуацию и что хочешь понять этим раскладом.\n"
            "Я посмотрю и предложу формат по глубине и стоимости."
        )
        await query.message.reply_text(reply)

        # уведомление админам
        user = query.from_user
        username = user.username or ""
        first_name = user.first_name or ""
        user_id = user.id

        admin_msg = (
            f"🔔 Выбор расклада через кнопку\n"
            f"Расклад: {title} ({code})\n"
            f"id: {user_id}\n"
            f"username: @{username if username else '—'}\n"
            f"имя: {first_name}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(chat_id=admin_id, text=admin_msg)
            except Exception as e:
                print(f"send pack_select notify error to {admin_id}: {e}")

        # лог выбора расклада как действия
        log_action_to_sheet(user, f"pack_select_{code}", "bot")

        # вернуть пользователя к главному меню
        await query.edit_message_reply_markup(
            reply_markup=build_main_keyboard(context.user_data)
        )

    elif data.startswith("st:"):
        await handle_stats_callback(update, context, data)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = (update.message.text or "").strip()
    lower = text.lower()

    if "расклад" in lower:
        user = update.effective_user
        user_id = user.id
        username = user.username or ""
        first_name = user.first_name or ""

        reply = (
            "Поймала твой запрос на индивидуальный расклад. 💫\n\n"
            "Напиши, пожалуйста, про какую ситуацию хочешь посмотреть:\n"
            "– в чём сейчас вопрос/запрос;\n"
            "– какой формат тебе комфортнее (голосом, текстом, поэтапно).\n\n"
            "Я отвечу и предложу варианты по формату и стоимости."
        )
        await update.message.reply_text(reply)

        admin_msg = (
            "🔔 Запрос на РАСКЛАД\n"
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
    
    # Статус карты дня
    cod_status = "🤖 Авто" if CARD_OF_DAY_STATUS.get("enabled", True) else "👋 Ручная"
    
    keyboard = [
        [InlineKeyboardButton(f"📅 Карта дня: {cod_status}", callback_data="st:cod_status")],
        [InlineKeyboardButton("📊 Сегодня: все карты", callback_data="st:today:all")],
        [InlineKeyboardButton("📊 Сегодня: по карте", callback_data="st:today:cards")],
        [InlineKeyboardButton("📅 Вчера: все карты", callback_data="st:yesterday:all")],
        [InlineKeyboardButton("📈 7 дней: все карты", callback_data="st:7days:all")],
        [InlineKeyboardButton("📆 Всё время: все карты", callback_data="st:alltime:all")],
        [InlineKeyboardButton("📬 Воронка: 7 дней", callback_data="st:nurture:7days")],
        [InlineKeyboardButton("🧭 Действия: сегодня", callback_data="st:actions:today")],
        [InlineKeyboardButton("🧭 Действия: вчера", callback_data="st:actions:yesterday")],
        [InlineKeyboardButton("🧭 Действия: 7 дней", callback_data="st:actions:7days")],
        [InlineKeyboardButton("🔄 Обновить попытки", callback_data="st:reset_attempts")],
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

    # ===== cod_status =====
    if action == "cod_status":
        current = CARD_OF_DAY_STATUS.get("enabled", True)
        CARD_OF_DAY_STATUS["enabled"] = not current
        new_status = "🤖 Авто" if CARD_OF_DAY_STATUS["enabled"] else "👋 Ручная"
        
        await query.answer(f"Карта дня переведена в режим: {new_status}", show_alert=True)
        
        keyboard = [
            [InlineKeyboardButton(f"📅 Карта дня: {new_status}", callback_data="st:cod_status")],
            [InlineKeyboardButton("📊 Сегодня: все карты", callback_data="st:today:all")],
            [InlineKeyboardButton("📊 Сегодня: по карте", callback_data="st:today:cards")],
            [InlineKeyboardButton("📅 Вчера: все карты", callback_data="st:yesterday:all")],
            [InlineKeyboardButton("📈 7 дней: все карты", callback_data="st:7days:all")],
            [InlineKeyboardButton("📆 Всё время: все карты", callback_data="st:alltime:all")],
            [InlineKeyboardButton("📬 Воронка: 7 дней", callback_data="st:nurture:7days")],
            [InlineKeyboardButton("🧭 Действия: сегодня", callback_data="st:actions:today")],
            [InlineKeyboardButton("🧭 Действия: вчера", callback_data="st:actions:yesterday")],
            [InlineKeyboardButton("🧭 Действия: 7 дней", callback_data="st:actions:7days")],
            [InlineKeyboardButton("🔄 Обновить попытки", callback_data="st:reset_attempts")],
        ]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # ===== reset_attempts =====
    if action == "reset_attempts":
        user_data = context.user_data
        user_data["meta_used"] = 0
        user_data["dice_used"] = 0
        today = datetime.now(UTC).date()
        user_data["last_meta_date"] = today
        user_data["last_dice_date"] = today
        await query.edit_message_reply_markup(
            reply_markup=build_main_keyboard(user_data)
        )
        await query.answer("Попытки обновлены до 3/3 для этого аккаунта.", show_alert=True)
        return

    # ===== nurture =====
    if action == "nurture":
        text = build_nurture_stats(days=7)
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True,
        )
        return

    # ===== actions =====
    if action == "actions":
        period = parts[2] if len(parts) > 2 else "today"
        text = build_actions_stats(period)
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True,
        )
        return

    # ===== today + cards =====
    if action == "today" and len(parts) > 2 and parts[2] == "cards":
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

    # ===== период и фильтр =====
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


def build_actions_stats(period: str) -> str:
    rows = load_actions()
    if not rows:
        return esc_md2("Лог действий пока пуст.")

    now = datetime.now(UTC)

    if period == "today":
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = now
        period_str = f"{start_dt.date()}"
    elif period == "yesterday":
        y = now - timedelta(days=1)
        start_dt = y.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = y.replace(hour=23, minute=59, second=59, microsecond=0)
        period_str = f"{start_dt.date()}"
    elif period == "7days":
        start_dt = now - timedelta(days=7)
        end_dt = now
        period_str = f"{start_dt.date()} — {end_dt.date()}"
    else:
        start_dt = datetime(2000, 1, 1, tzinfo=UTC)
        end_dt = now
        period_str = "за всё время"

    filtered = []
    for r in rows:
        dt = parse_iso(r["ts_iso"])
        if dt is None:
            continue
        if not (start_dt <= dt <= end_dt):
            continue
        filtered.append(r)

    if not filtered:
        return esc_md2(f"В период {period_str} действий не было.")

    total = len(filtered)
    by_action = defaultdict(int)
    for r in filtered:
        by_action[r["action"]] += 1

    header = esc_md2(f"Действия пользователей за {period_str}")
    lines = [header, ""]
    lines.append(esc_md2(f"Всего действий: {total}"))
    for act, cnt in by_action.items():
        lines.append(esc_md2(f"{act}: {cnt}"))

    lines.append("")
    lines.append(esc_md2("Пользователи и их действия:"))

    filtered_sorted = sorted(filtered, key=lambda r: r["ts_iso"])
    for r in filtered_sorted:
        uid = r["user_id"]
        username = r["username"]
        first_name = r["first_name"]
        act = r["action"]
        src = r["source"]
        ts_iso = r["ts_iso"]

        if username:
            who = f"@{username}"
        elif first_name:
            who = f"{first_name} (id{uid})"
        else:
            who = f"id{uid}"

        line = f"{who} — {act} ({src}) — {ts_iso}"
        lines.append(esc_md2(line))

    return "\n".join(lines)


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
        if not uid:
            continue
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

    lines.append("")
    lines.append(esc_md2("Список пользователей:"))

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
    rows = load_nurture_rows()
    if not rows:
        return esc_md2("Лог автоворонки пока пуст.")

    now = datetime.now(UTC)
    since = now - timedelta(days=days)

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
        if not uid:
            continue
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
        if dt is None or not uid:
            continue
        if uid not in by_user:
            by_user[uid] = {
                "first_dt": dt,
                "last_row": row,
            }
        else:
            if dt < by_user[uid]["first_dt"]:
                by_user[uid]["first_dt"] = dt
            last_dt = parse_iso(by_user[uid]["last_row"]["date_iso"])
            if last_dt is None or dt > last_dt:
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

        if not is_sub and days in (1, 3, 7):
            day_num = days
            day_key = f"day_{days}"
            texts = NURTURE_UNSUB.get(card_key, {})
            msg_template = texts.get(day_key)
            if msg_template:
                text = msg_template.format(channel=CHANNEL_USERNAME)
                try:
                    await bot.send_message(chat_id=int(uid), text=text)
                    log_nurture_to_sheet(int(uid), card_key, "unsub", day_num, "ok")
                except Exception as e:
                    print(f"nurture unsub send error to {uid}: {e}")
                    log_nurture_to_sheet(int(uid), card_key, "unsub", day_num, "error", str(e))

        if is_sub and days in (3, 7, 14):
            day_num = days
            day_key = f"day_{days}"
            texts = NURTURE_SUB.get(card_key, {})
            msg_template = texts.get(day_key)
            if msg_template:
                text = msg_template.format(channel=CHANNEL_USERNAME)
                try:
                    await bot.send_message(chat_id=int(uid), text=text)
                    log_nurture_to_sheet(int(uid), card_key, "sub", day_num, "ok")
                except Exception as e:
                    print(f"nurture sub send error to {uid}: {e}")
                    log_nurture_to_sheet(int(uid), card_key, "sub", day_num, "error", str(e))

    update_nurture_subscribed_after()

# ===== ежедневное напоминание пользователям =====


async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    users = load_users()
    if not users:
        return

    bot = context.bot
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

    # инициализируем Google Sheets
    init_gs_client()

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
    job_queue.run_daily(
    send_card_of_the_day_to_channel,
    time=time(5, 30),  # 05:30 UTC ≈ 08:30 по Москве (раньше, чем напоминание)
    name="card_of_day",
    )
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



