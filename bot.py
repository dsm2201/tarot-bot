import os
import random
import csv
import json
from datetime import datetime, UTC, timedelta, time as dt_time
import time as time_module
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

import time

USERS_CACHE = {'data': None, 'timestamp': 0}
ACTIONS_CACHE = {'data': None, 'timestamp': 0}
CACHE_TTL = 300

# ===== Render Environment ===== Админы
ADMIN_IDS = {
    int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",")
    if id.strip()
}

#Канал
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
CHANNEL_LINK = os.getenv("CHANNEL_LINK")


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
AUTO_NURTURE_SHEET_NAME = "auto_nurture" # <-- НОВАЯ СТРОКА

GS_CLIENT = None
GS_SHEET = None
GS_USERS_WS = None
GS_ACTIONS_WS = None
GS_NURTURE_WS = None
GS_CARD_OF_DAY_WS = None
GS_PACKS_WS = None
GS_AUTO_NURTURE_WS = None # <-- НОВАЯ СТРОКА
PACKS_DATA = {}  # словарь: {code: {title, emoji, description, filename}}

def get_admin_keyboard():
    """ЕДИНАЯ клавиатура админки"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Карта дня →", callback_data="st:card_menu")],
        [InlineKeyboardButton("📢 Рассылка пользователям", callback_data="st:broadcast_menu")],
        [InlineKeyboardButton("🔄 Обновить расклады", callback_data="st:reload_packs")],
        [InlineKeyboardButton("📊 Статистика →", callback_data="st:stats_menu")],
        [InlineKeyboardButton("👥 Список пользователей →", callback_data="st:users_menu")],
        [InlineKeyboardButton("🔄 Обновить попытки", callback_data="st:reset_attempts")],
        [InlineKeyboardButton("📤 Авторассылка →", callback_data="st:auto_nurture_menu")], # <-- НОВАЯ КНОПКА
    ])

def init_gs_client():
    global GS_CLIENT, GS_SHEET, GS_USERS_WS, GS_ACTIONS_WS, GS_NURTURE_WS, GS_CARD_OF_DAY_WS, GS_PACKS_WS, GS_AUTO_NURTURE_WS # <-- Добавлено GS_AUTO_NURTURE_WS
    if not GS_SERVICE_JSON or not GS_SHEET_ID:
        print(">>> Google Sheets: переменные GS_SERVICE_JSON / GS_SHEET_ID не заданы.")
        return
    try:
        info = json.loads(GS_SERVICE_JSON)
        client = service_account_from_dict(info)
        sheet = client.open_by_key(GS_SHEET_ID)
        users_ws = sheet.worksheet(USERS_SHEET_NAME)
        actions_ws = sheet.worksheet(ACTIONS_SHEET_NAME)

        # Обработка вкладок, которые могут отсутствовать
        try:
            nurture_ws = sheet.worksheet(NURTURE_SHEET_NAME)
        except Exception:
            nurture_ws = None
        try:
            card_of_day_ws = sheet.worksheet(CARD_OF_DAY_SHEET_NAME)
        except Exception:
            card_of_day_ws = None
        try:
            packs_ws = sheet.worksheet("packs")  # <- Старый код
        except Exception:
            packs_ws = None

        # --- НОВЫЙ КОД ДЛЯ auto_nurture ---
        try:
            auto_nurture_ws = sheet.worksheet(AUTO_NURTURE_SHEET_NAME) # <- Получаем объект вкладки auto_nurture
            print(f">>> init_gs_client: вкладка '{AUTO_NURTURE_SHEET_NAME}' успешно подключена.")
        except gspread.exceptions.WorksheetNotFound:
            # Конкретно на случай, если вкладка не найдена
            print(f">>> init_gs_client: вкладка '{AUTO_NURTURE_SHEET_NAME}' не найдена. Она будет создана при первой необходимости.")
            auto_nurture_ws = None
        except Exception as e: # Обрабатываем другие возможные ошибки (редко)
            print(f">>> init_gs_client: ошибка при получении вкладки '{AUTO_NURTURE_SHEET_NAME}': {e}")
            auto_nurture_ws = None
        # --- КОНЕЦ НОВОГО КОДА ---

        # --- ОСНОВНОЕ ПРИСВАИВАНИЕ ПЕРЕМЕННЫХ ---
        # Эти строки выполняются ТОЛЬКО если основной try (до этого места) завершился успешно
        GS_CLIENT = client
        GS_SHEET = sheet
        GS_USERS_WS = users_ws
        GS_ACTIONS_WS = actions_ws
        GS_NURTURE_WS = nurture_ws
        GS_CARD_OF_DAY_WS = card_of_day_ws
        GS_PACKS_WS = packs_ws
        GS_AUTO_NURTURE_WS = auto_nurture_ws # <-- Присваиваем, даже если None
        print(">>> Google Sheets: успешно подключено к tatiataro_log.")
        # --- КОНЕЦ ПРИСВАИВАНИЯ ---

    except Exception as e:
        # Этот except срабатывает, если произошла ошибка ДО присваивания переменных (например, ошибка открытия таблицы)
        print(f">>> Google Sheets init error: {e}")
        GS_CLIENT = None
        GS_SHEET = None
        GS_USERS_WS = None
        GS_ACTIONS_WS = None
        GS_NURTURE_WS = None
        GS_CARD_OF_DAY_WS = None
        GS_PACKS_WS = None
        GS_AUTO_NURTURE_WS = None # <-- Важно: сбросить и новую переменную тоже
    
def load_json(name):
    path = os.path.join(TEXTS_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_packs_from_sheets():
    """Загружаем расклады из листа 'packs' в Google Sheets."""
    global PACKS_DATA
    if GS_PACKS_WS is None:
        print(">>> load_packs_from_sheets: лист 'packs' не найден")
        return
    try:
        records = GS_PACKS_WS.get_all_records()
        PACKS_DATA = {}
        for row in records:
            code = row.get("code", "").strip()
            if not code:
                continue
            PACKS_DATA[code] = {
                "emoji": row.get("emoji", "").strip(),
                "title": row.get("title", "").strip(),
                "description": row.get("description", "").strip(),
                "filename": row.get("filename", "").strip(),
            }
        print(f">>> load_packs_from_sheets: загружено {len(PACKS_DATA)} раскладов")
    except Exception as e:
        print(f">>> load_packs_from_sheets error: {e}")

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

# ===== КЭШ RAM =====

def get_cached_users():
    import time as t  # ФИКС конфликта!
    now = t.time()
    if now - USERS_CACHE['timestamp'] > CACHE_TTL:
        print("🔄 Кэш users обновлён")
        USERS_CACHE['data'] = load_users()
        USERS_CACHE['timestamp'] = now
    return USERS_CACHE['data']

def get_cached_actions():
    import time as t  # ФИКС конфликта!
    now = t.time()
    if now - ACTIONS_CACHE['timestamp'] > CACHE_TTL:
        print("🔄 Кэш actions обновлён")
        ACTIONS_CACHE['data'] = load_actions()
        ACTIONS_CACHE['timestamp'] = now
    return ACTIONS_CACHE['data']

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
    return max(0, 1 - used)


def get_dice_left(user_data: dict) -> int:
    _normalize_daily_counters(user_data)
    used = user_data.get("dice_used", 0)
    return max(0, 1 - used)


def build_main_keyboard(user_data: dict) -> InlineKeyboardMarkup:
    meta_left = get_meta_left(user_data)
    dice_left = get_dice_left(user_data)

    meta_text = f"🃏 Метафорическая карта ({meta_left})"
    dice_text = f"🎲 Помощь кубика ({dice_left})"

    keyboard = [
        [InlineKeyboardButton("📢 Перейти в канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton("🔔 Получать подсказки в ЛС", callback_data="subscribe")],
        [InlineKeyboardButton(meta_text, callback_data="meta_card_today")],
        [InlineKeyboardButton(dice_text, callback_data="dice_today")],
        [InlineKeyboardButton("📚 Запись на расклад", callback_data="packs_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_pack_description(code: str) -> tuple[str, str, str]:
    """Получаем описание расклада из загруженных данных."""
    if code in PACKS_DATA:
        pack = PACKS_DATA[code]
        return pack["title"], pack["description"], pack["filename"]
    else:
        return "Расклад", "Описание этого расклада появится чуть позже.", ""

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
                "Для связи пиши мне в ЛС @Tatiataro18"
            )
        except Exception as e:
            print(f"send_random_meta_card error: {e}")
            await chat.send_message(
                "Произошла ошибка при отправке карты. Попробуй ещё раз позже."
                "Для связи пиши мне в ЛС @Tatiataro18"
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
                caption="🎲 Ответ кубика:",
            )
        except TimedOut:
            await chat.send_message(
                "Сейчас не получилось отправить картинку кубика (таймаут Telegram).\n"
                "Попробуй, пожалуйста, ещё раз чуть позже."
                "Для связи пиши мне в ЛС @Tatiataro18"
            )
        except Exception as e:
            print(f"send_random_dice error: {e}")
            await chat.send_message(
                "Произошла ошибка при отправке кубика. Попробуй ещё раз позже."
                "Для связи пиши мне в ЛС @Tatiataro18"
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
        
        # Получаем веса
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
        
        # Выбираем карту
        selected = random.choices(records, weights=weights, k=1)[0]
        return selected
    except Exception as e:
        print(f">>> load_card_of_the_day error: {e}")
        return None

# --- НОВАЯ ФУНКЦИЯ ДЛЯ РАССЫЛКИ ---
async def broadcast_message_to_users(bot, user_list, message_text):
    """
    Выполняет рассылку сообщения указанному списку пользователей.

    Args:
        bot: Экземпляр бота telegram.ext.Application.
        user_list (list): Список словарей пользователей (например, из load_users).
                           Должен содержать ключ 'user_id'.
        message_text (str): Текст сообщения для рассылки.

    Returns:
        str: Отформатированный отчет о результатах рассылки.
    """
    success_count = 0
    failure_count = 0
    failure_details = []

    unique_user_ids = set()
    for user_data in user_list:
        user_id_str = user_data.get("user_id", "").strip()
        if user_id_str:
            try:
                user_id_int = int(user_id_str)
                unique_user_ids.add(user_id_int)
            except ValueError:
                print(f"⚠️ Неверный ID пользователя: '{user_id_str}', пропущен.")

    total_recipients = len(unique_user_ids)

    if total_recipients == 0:
        return "❌ Не найдено корректных ID пользователей для рассылки."

    print(f"📢 Начинаю рассылку {message_text[:50]}... (всего {total_recipients} уникальных пользователей)")

    for user_id in unique_user_ids:
        try:
            await bot.send_message(chat_id=user_id, text=message_text)
            success_count += 1
            print(f"✅ Сообщение успешно отправлено пользователю {user_id}")
        except Exception as e:
            failure_count += 1
            error_detail = f"user_id: {user_id}, error: {type(e).__name__} - {str(e)}"
            failure_details.append(error_detail)
            print(f"❌ Ошибка при отправке пользователю {user_id}: {e}")

    report_lines = []
    report_lines.append(f"📢 *РЕЗУЛЬТАТЫ РАССЫЛКИ*")
    report_lines.append(f"Всего пользователей для рассылки: *{total_recipients}*")
    report_lines.append(f"✅ Успешно доставлено: *{success_count}*")
    report_lines.append(f"❌ Не доставлено: *{failure_count}*")
    report_lines.append("---")

    if failure_details:
        report_lines.append("*Детали ошибок:*")
        for detail in failure_details:
            # Экранируем символы Markdown в деталях ошибки, так как они могут содержать _
            escaped_detail = esc_md2(detail)
            report_lines.append(f"`{escaped_detail}`")
        report_lines.append("---")

    return "\n".join(report_lines)

# --- НОВЫЙ ОБРАБОТЧИК ДЛЯ ЗАПРОСА РАССЫЛКИ (исправленный для HTML)---
import html

async def handle_broadcast_request(update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
    """Обрабатывает запрос на рассылку от администратора."""
    query = update.callback_query
    user = query.from_user
    print(f"📤 Администратор {user.id} запрашивает рассылку: {message_text[:100]}...")

    # Загрузка списка пользователей
    users = get_cached_users() # Используем кэшированную функцию, если доступна

    if not users:
        error_msg = "❌ Не удалось получить список пользователей для рассылки."
        await query.edit_message_text(error_msg)
        print(error_msg)
        return

    # Вызов функции рассылки
    report = await broadcast_message_to_users_html(context.bot, users, message_text) # Вызываем новую версию

    # Отправка отчета администратору
    escaped_message_text = html.escape(message_text) # Экранируем текст рассылки
    report_html = f"<b>📤 ЗАПРОС НА РАССЫЛКУ ОТПРАВЛЕН</b>\n\n<b>Сообщение:</b>\n<pre>{escaped_message_text}</pre>\n\n---\n\n{report}"
    try:
        # Попробуем отредактировать сообщение с запросом
        await query.edit_message_text(
            text=report_html,
            parse_mode='HTML'
        )
    except Exception:
        # Если невозможно отредактировать (например, слишком длинное), отправим новое
        await query.message.reply_text(
            text=report_html,
            parse_mode='HTML'
        )

    print(f"📤 Рассылка завершена. Отчет отправлен администратору {user.id}.")

# --- НОВАЯ ВЕРСИЯ broadcast_message_to_users для генерации HTML отчета ---
async def broadcast_message_to_users_html(bot, user_list, message_text):
    """
    Выполняет рассылку сообщения указанному списку пользователей.

    Args:
        bot: Экземпляр бота telegram.ext.Application.
        user_list (list): Список словарей пользователей (например, из load_users).
                           Должен содержать ключ 'user_id'.
        message_text (str): Текст сообщения для рассылки.

    Returns:
        str: Отформатированный HTML отчет о результатах рассылки.
    """
    success_count = 0
    failure_count = 0
    failure_details = []

    unique_user_ids = set()
    for user_data in user_list:
        user_id_str = user_data.get("user_id", "").strip()
        if user_id_str:
            try:
                user_id_int = int(user_id_str)
                unique_user_ids.add(user_id_int)
            except ValueError:
                print(f"⚠️ Неверный ID пользователя: '{user_id_str}', пропущен.")

    total_recipients = len(unique_user_ids)

    if total_recipients == 0:
        return "❌ Не найдено корректных ID пользователей для рассылки."

    print(f"📢 Начинаю рассылку {message_text[:50]}... (всего {total_recipients} уникальных пользователей)")

    for user_id in unique_user_ids:
        try:
            await bot.send_message(chat_id=user_id, text=message_text)
            success_count += 1
            print(f"✅ Сообщение успешно отправлено пользователю {user_id}")
        except Exception as e:
            failure_count += 1
            error_detail = f"user_id: {user_id}, error: {type(e).__name__} - {str(e)}"
            failure_details.append(error_detail)
            print(f"❌ Ошибка при отправке пользователю {user_id}: {e}")

    # Формируем HTML-отчет
    import html
    report_parts = []
    report_parts.append(f"<b>📢 РЕЗУЛЬТАТЫ РАССЫЛКИ</b>")
    report_parts.append(f"Всего пользователей для рассылки: <b>{total_recipients}</b>")
    report_parts.append(f"✅ Успешно доставлено: <b>{success_count}</b>")
    report_parts.append(f"❌ Не доставлено: <b>{failure_count}</b>")
    report_parts.append("---")

    if failure_details:
        report_parts.append("<b>Детали ошибок:</b>")
        for detail in failure_details:
            # Экранируем детали ошибки для HTML
            escaped_detail = html.escape(detail)
            report_parts.append(f"<pre>{escaped_detail}</pre>")
        report_parts.append("---")

    return "\n".join(report_parts)

# --- ОБНОВЛЁННАЯ ФУНКЦИЯ ДЛЯ АВТОМАТИЧЕСКОЙ РАССЫЛКИ С ИСПОЛЬЗОВАНИЕМ ОТДЕЛЬНОЙ ВКЛАДКИ ---
import asyncio
from datetime import datetime, timedelta, date
from pytz import UTC
import gspread # Убедитесь, что gspread установлен

# --- ОБНОВЛЁННАЯ ФУНКЦИЯ ДЛЯ АВТОМАТИЧЕСКОЙ РАССЫЛКИ С ИСПОЛЬЗОВАНИЕМ ОТДЕЛЬНОЙ ВКЛАДКИ И ГЛОБАЛЬНОЙ ПЕРЕМЕННОЙ GS_SHEET ---
import asyncio
from datetime import datetime, timedelta, date, time # time уже импортирован
from pytz import UTC
import gspread # Убедитесь, что gspread установлен

# --- ОБНОВЛЁННАЯ ФУНКЦИЯ ДЛЯ АВТОМАТИЧЕСКОЙ РАССЫЛКИ С ИСПОЛЬЗОВАНИЕМ ОТДЕЛЬНОЙ ВКЛАДКИ И ГЛОБАЛЬНОЙ ПЕРЕМЕННОЙ GS_AUTO_NURTURE_WS ---
import asyncio
from datetime import datetime, timedelta, date, time # time уже импортирован
from pytz import UTC
import gspread # Убедитесь, что gspread установлен

async def auto_nurture_broadcast(context: ContextTypes.DEFAULT_TYPE):
    """
    Функция автоматической рассылки по воронке.
    Вызывается JobQueue по расписанию.
    Читает настройки из вкладки 'auto_nurture', строка 1.
    Проверяет историю отправок в той же вкладке.
    Использует глобальную переменную GS_AUTO_NURTURE_WS.
    """
    print("🔄 Запуск автоматической воронки из вкладки 'auto_nurture'...")
    bot = context.bot

    # Проверяем, инициализирована ли глобальная переменная GS_AUTO_NURTURE_WS
    global GS_AUTO_NURTURE_WS # Убедимся, что используем глобальную
    if GS_AUTO_NURTURE_WS is None:
        print("❌ Ошибка: GS_AUTO_NURTURE_WS не инициализирована или вкладка не найдена при старте.")
        return

    try:
        worksheet = GS_AUTO_NURTURE_WS # <-- ИСПОЛЬЗУЕМ ГЛОБАЛЬНУЮ ПЕРЕМЕННУЮ
    except Exception as e:
        print(f"❌ Ошибка доступа к вкладке 'auto_nurture': {e}")
        return

    # 1. Чтение настроек из строки 1
    try:
        settings_row = worksheet.row_values(1) # Получаем первую строку
        if len(settings_row) < 8: # Проверяем, достаточно ли колонок (user_id, username, first_name, action, sent_date, status, error_msg, text, period)
            print("❌ Недостаточно данных в строке 1 вкладки 'auto_nurture'. Ожидаемые колонки: user_id, username, first_name, action, sent_date, status, error_msg, text, period")
            return

        # Сопоставляем значения с колонками
        # Предполагаем порядок: A, B, C, D, E, F, G, H, I
        #                       user_id, username, first_name, action, sent_date, status, error_msg, text, period
        # Индексация в Python с 0: 0      1         2            3       4          5       6          7     8
        stored_text = settings_row[7].strip() if len(settings_row) > 7 else "" # Колонка H (индекс 7)
        stored_period_str = settings_row[8].strip() if len(settings_row) > 8 else "" # Колонка I (индекс 8)

        if not stored_text:
            print("❌ Текст для рассылки не задан (колонка 'text' в строке 1 пуста).")
            return
        try:
            stored_period_days = int(stored_period_str)
            if stored_period_days <= 0:
                print("❌ Период рассылки должен быть положительным числом.")
                return
        except ValueError:
            print(f"❌ Неверный формат периода: '{stored_period_str}'. Должно быть целое число дней.")
            return

    except Exception as e:
        print(f"❌ Ошибка чтения настроек из строки 1: {e}")
        return

    print(f"📋 Найдены настройки: период = {stored_period_days} дней, текст = '{stored_text[:30]}...'")

    # 2. Загрузка истории отправок (все строки, кроме первой)
    try:
        all_rows = worksheet.get_all_values()
        if len(all_rows) <= 1:
            history_rows = [] # Нет истории, только настройки
        else:
            history_rows = all_rows[1:] # Берём все строки, кроме первой (настройки)
    except Exception as e:
        print(f"❌ Ошибка загрузки истории из вкладки 'auto_nurture': {e}")
        return

    # 3. Формирование словаря last_sent_date_per_user_id
    last_sent_date_per_user_id = {}
    for row in history_rows:
        if len(row) > 4: # Проверяем, что есть user_id и sent_date
            user_id_str = row[0].strip() # Колонка A
            sent_date_str = row[4].strip() # Колонка E (sent_date)
            if user_id_str and sent_date_str:
                try:
                    # Предполагаем формат YYYY-MM-DD
                    sent_date_obj = datetime.strptime(sent_date_str, "%Y-%m-%d").date()
                    # Если для одного user_id есть несколько записей, берём самую последнюю
                    existing_date = last_sent_date_per_user_id.get(user_id_str)
                    if existing_date is None or sent_date_obj > existing_date:
                        last_sent_date_per_user_id[user_id_str] = sent_date_obj
                except ValueError:
                    print(f"⚠️ Неверный формат даты в истории для user_id {user_id_str}: '{sent_date_str}'")

    # 4. Загрузка *всех* пользователей из основной вкладки (предположим, это USERS_SHEET_NAME, к которому у нас есть доступ через GS_USERS_WS, но проще использовать get_cached_users/load_users)
    # Используем вашу существующую функцию для получения пользователей
    users = get_cached_users() # или load_users(), если у вас нет кэша

    if not users:
        print("❌ Не удалось получить список пользователей для автоматической воронки из основной вкладки.")
        return

    # 5. Определение даты "N дней назад"
    cutoff_date = (datetime.now(UTC).date()) - timedelta(days=stored_period_days)
    print(f"📅 Пороговая дата (до которой НЕ отправляем): {cutoff_date}")

    # 6. Фильтрация и отправка
    users_to_notify = []
    for user_data in users:
        user_id_str = user_data.get("user_id", "").strip()
        if not user_id_str:
            continue

        last_sent_date = last_sent_date_per_user_id.get(user_id_str)
        # Отправляем, если:
        # a) Пользователь никогда не получал это сообщение (last_sent_date is None)
        # b) Последнее получение было раньше cutoff_date (т.е. прошло >= stored_period_days дней)
        if last_sent_date is None or last_sent_date <= cutoff_date:
            users_to_notify.append(user_id_str)

    total_to_notify = len(users_to_notify)
    print(f"📢 Найдено {total_to_notify} пользователей для отправки.")

    if total_to_notify == 0:
        return

    success_count = 0
    failure_count = 0
    successful_notifications = []

    for user_id_str in users_to_notify:
        try:
            await bot.send_message(chat_id=int(user_id_str), text=stored_text)
            success_count += 1
            print(f"✅ Авто-воронка ({stored_period_days} дней) успешно отправлена пользователю {user_id_str}")
            successful_notifications.append(user_id_str)
        except Exception as e:
            failure_count += 1
            error_type = type(e).__name__
            print(f"❌ Ошибка при отправке авто-воронки пользователю {user_id_str}: {error_type} - {e}")
            # Здесь можно логировать ошибку в отдельную колонку, если нужно

    # 7. Запись результатов в вкладку 'auto_nurture'
    if successful_notifications:
        try:
            current_date_str = datetime.now(UTC).strftime("%Y-%m-%d")
            rows_to_append = []
            for user_id_str in successful_notifications:
                # Попробуем получить username и first_name из основного списка
                user_info = next((u for u in users if u.get("user_id") == user_id_str), {})
                username = user_info.get("username", "")
                first_name = user_info.get("first_name", "")

                new_row = [
                    user_id_str,  # A - user_id
                    username,     # B - username
                    first_name,   # C - first_name
                    "auto_nurture", # D - action (тип рассылки)
                    current_date_str, # E - sent_date
                    "sent",       # F - status
                    "",           # G - error_msg (пусто при успехе)
                    stored_text,  # H - text (для истории, можно сократить)
                    str(stored_period_days) # I - period (для истории)
                ]
                rows_to_append.append(new_row)

            if rows_to_append:
                worksheet.append_rows(rows_to_append) # Добавляем строки в конец
                print(f"✅ Записано {len(rows_to_append)} строк об отправке в вкладку 'auto_nurture'.")
        except Exception as write_e:
            print(f"❌ Ошибка записи результата в Google Sheets: {write_e}")

    print(f"🏁 Автоматическая воронка завершена. Успешно: {success_count}, Ошибки: {failure_count}")


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

    # Генерируем заголовок с датой и днём
    now = datetime.now(UTC)
    
    months_ru = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля",
        5: "мая", 6: "июня", 7: "июля", 8: "августа",
        9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"
    }
    
    day = now.day
    month = months_ru[now.month]
    days_ru = {
        0: "Понедельник", 1: "Вторник", 2: "Среда", 3: "Четверг",
        4: "Пятница", 5: "Суббота", 6: "Воскресенье"
    }
    weekday = days_ru[now.weekday()]
    
    header = f"{day} {month} 🔔 {weekday}\n\n"
    text = header + text
    
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

async def test_day_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск карты дня только для админа."""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Эта команда только для администратора.")
        return

    await update.message.reply_text("Пробую отправить карту дня в канал...")
    await send_card_of_the_day_to_channel(context)
    await update.message.reply_text("Готово (если в логах нет ошибок).")

async def reload_packs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("🚀 reload_packs НАЧАЛАСЬ!")
    print(f"🔍 query={update.callback_query is not None}")
    print(f"🔍 ADMIN_IDS={ADMIN_IDS}")
    user = update.effective_user
    query = update.callback_query
    
    if user.id not in ADMIN_IDS:
        if query:
            await query.answer("❌ Только админ!")
        return
    
    # 🔄 ЛОГИКА ДЛЯ КНОПКИ
    if query:
        await query.answer("🔄 Обновляю...")
        await query.message.edit_text("⏳ Перезагружаю...")  # ✅ query.message!
        
    # 🔄 ЛОГИКА ДЛЯ КОМАНДЫ  
    else:
        await update.message.reply_text("⏳ Перезагружаю...")
    
    load_packs_from_sheets()
    count = len(PACKS_DATA)
    result = f"✅ Загружено **{count}** раскладов!" if count else "❌ Ошибка!"
    
    if query:
        await query.message.edit_text(result, parse_mode=ParseMode.MARKDOWN_V2)  # ✅
    else:
        await update.message.reply_text(result)

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
        elif arg0 == "day_card":  # добавили этот случай
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
            "Для связи пиши мне в ЛС @Tatiataro18"
        )

    elif special_start == "rasklad":
        # заход из поста «хочу личный расклад»
        text = (
            "Вижу, что ты пришёл за личным раскладом. 💫\n\n"
            "Напиши, пожалуйста, пару слов про свою ситуацию:\n"
            "– про что хочешь посмотреть (отношения, деньги, выбор, путь и т.п.);\n"
            "– как тебе комфортно получать разбор (голосом, текстом, поэтапно).\n\n"
            "Я посмотрю запрос и предложу несколько форматов по глубине и стоимости."
            "Или сразу пиши мне в ЛС @Tatiataro18"
        )
        info_text = (
            f"Если по ходу переписки захочешь ещё подумать — в {CHANNEL_USERNAME} "
            "много бесплатных раскладов и примеров разборов."
        )

    elif special_start == "day_card":
        text = (
            "Вижу, что ты пришёл из карты дня! 🃏\n\n"
            "Если эта карта откликается — можешь вернуться в основное меню и вытянуть ещё одну, "
            "или сделать индивидуальный расклад для более глубокого разбора."
            "Для связи пиши мне в ЛС @Tatiataro18"
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
            "Для связи пиши мне в ЛС @Tatiataro18"
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
    print(f"🔥 CLICK data='{data}'")  # ← ОБЯЗАТЕЛЬНО!
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

    elif data == "main_menu":
        await query.message.reply_text(
            "Главное меню:",
            reply_markup=build_main_keyboard(context.user_data),
        )

    elif data == "meta_card_today":
        meta_used = user_data.get("meta_used", 0)
        if meta_used >= 1:
            await query.message.reply_text(
            "❌ Только 1 карта в день!\n\n"
            "Приходите за картой завтра",
            reply_markup=build_main_keyboard(user_data)
            )
        else:
            user_data["meta_used"] = meta_used + 1
            await send_random_meta_card(update, context)
            # лог действия
            log_action_to_sheet(user, "meta_card", "bot")

        await query.edit_message_reply_markup(reply_markup=build_main_keyboard(user_data))

    elif data == "dice_today":
        _normalize_daily_counters(user_data)  # ← КЛЮЧЕВОЙ ФИКС!
        dice_used = user_data.get('dice_used', 0)
        if dice_used >= 1:
            await query.message.reply_text(
            "❌ Только 1 кубик в день!\n\n"
            "Приходите за кубиком завтра 🎲",
            reply_markup=build_main_keyboard(user_data)
            )
        else:
            instr_text = """
    🎲 ПОМОЩЬ КУБИКА
    
    1. Сформулируйте вопрос ДА/НЕТ в голове:
    • Получу ли я повышение?
    • Стоит ли сегодня ехать?
    
    2. Нажмите кнопку для ответа!
            """
            keyboard = [[InlineKeyboardButton("🎲 Получить ответ", callback_data="dice_today_confirm")]]
            await query.edit_message_text(instr_text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    elif data == "dice_today_confirm":
        _normalize_daily_counters(user_data)  # ← ЕЩЁ РАЗ для точности
        dice_used = user_data.get('dice_used', 0)
        if dice_used >= 1:
            await query.message.reply_text(
                "❌ Только 1 кубик в день!\n\n"
                "Приходите завтра 🎲",
                reply_markup=build_main_keyboard(user_data)
            )
            await query.answer()
        else:
            user_data['dice_used'] = 1
            today = datetime.now(UTC).date()
            user_data['last_dice_date'] = today
            
            await send_random_dice(update, context)
            log_action_to_sheet(user, "dice", "bot")
            
            await query.edit_message_text(
                "🎲 *Ответ получен!*\n\n"
                "(Смотрите на кубик!)",
                reply_markup=build_main_keyboard(user_data),
                parse_mode=ParseMode.MARKDOWN
            )
        
    elif data == "st:menu":
        if user_id not in ADMIN_IDS:
            await query.answer("❌ Только админ!", show_alert=True)
            return
        await query.message.reply_text("⚙️ Админ-панель:",
    reply_markup=get_admin_keyboard())

    elif data == "st:reload_packs":
        load_packs_from_sheets()
        count = len(PACKS_DATA)
        await query.message.reply_text(f"✅ Загружено {count} раскладов!")
        return
        
    elif data == "packs_menu":
        # подменю с раскладами (генерируем из PACKS_DATA)
        packs_keyboard = [
            [InlineKeyboardButton("📝 Свой запрос", callback_data="pack:other")],
        ]
        # Добавляем кнопки раскладов из Google Sheets
        for code in PACKS_DATA.keys():
            pack = PACKS_DATA[code]
            emoji = pack.get("emoji", "")
            title = pack.get("title", "").split(" — ")[0]  # берём только до " — "
            button_text = f"{emoji} {title}"
            packs_keyboard.append(
                [InlineKeyboardButton(button_text, callback_data=f"pack:{code}")]
            )
        
        await query.message.reply_text(
            "Выбери расклад, который откликается или нажми «Свой вопрос»:",
            reply_markup=InlineKeyboardMarkup(packs_keyboard),
    )
        
    elif data == "pack:other":
        # Свой запрос — ЭТОТ БЛОК ДОЛЖЕН БЫТЬ ЗДЕСЬ, ДО startswith!
        reply = (
            "Поймала твой запрос на расклад «Расклад». 💫\n\n"
            "Напиши пару слов про свою ситуацию и что хочешь понять этим раскладом.\n"
            "Я посмотрю и предложу формат по глубине и стоимости.\n\n"
            "Для связи пиши мне в ЛС @Tatiataro18"
        )
        await query.message.reply_text(reply)
        
        # уведомление админам
        user = query.from_user
        username = user.username or ""
        first_name = user.first_name or ""
        user_id = user.id
        admin_msg = (
            f"🔔 Выбор расклада через кнопку\n"
            f"Расклад: Расклад (other)\n"
            f"id: {user_id}\n"
            f"username: @{username if username else '—'}\n"
            f"имя: {first_name}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(chat_id=admin_id, text=admin_msg)
            except Exception as e:
                print(f"send pack_select notify error to {admin_id}: {e}")
        
        # лог выбора расклада
        log_action_to_sheet(user, "pack_select_other", "bot")
        
        # вернуть пользователя к главному меню
        await query.edit_message_reply_markup(
            reply_markup=build_main_keyboard(context.user_data)
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
            # Проверяем, это URL или локальный файл
            if filename.startswith(("http://", "https://")):
                # Это URL - отправляем фото по ссылке
                await query.message.reply_photo(
                    photo=filename,
                    caption=text,
                    reply_markup=InlineKeyboardMarkup(select_keyboard),
                )
            else:
                # Это локальный файл в папке packs_images
                image_path = os.path.join(PACKS_DIR, filename)
                try:
                    with open(image_path, "rb") as f:
                        await query.message.reply_photo(
                            photo=f,
                            caption=text,
                            reply_markup=InlineKeyboardMarkup(select_keyboard),
                        )
                except FileNotFoundError:
                    print(f"pack image not found: {image_path}")
                    await query.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(select_keyboard),
                    )
        else:
            await query.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(select_keyboard),
            )

    elif data.startswith("pack_select:"):
        # человек нажал "выбрать расклад"
        code = data.split(":", 1)[1]
        title, _, _ = get_pack_description(code)

        # ответ пользователю
        reply = (
            f"Поймала твой запрос на расклад «{title}». 💫\n\n"
            "Напиши пару слов про свою ситуацию и что хочешь понять этим раскладом.\n"
            "Я посмотрю и предложу формат по глубине и стоимости."
            "Для связи пиши мне в ЛС @Tatiataro18"
        )
        await query.message.reply_text(reply)

        # уведомление админам
        user = query.from_user
        username = user.username or ""
        first_name = user.first_name or ""
        user_id = user.id
        admin_msg = (
            f"🔔 Выбор расклада через кнопку\n"
            f"Расклад: Расклад (other)\n"
            f"id: {user_id}\n"
            f"username: @{username if username else '—'}\n"
            f"имя: {first_name}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(chat_id=admin_id, text=admin_msg)
            except Exception as e:
                print(f"send pack_select notify error to {admin_id}: {e}")
        
        # лог выбора расклада
        log_action_to_sheet(user, "pack_select_other", "bot")
        
        # вернуть пользователя к главному меню
        await query.edit_message_reply_markup(
            reply_markup=build_main_keyboard(context.user_data)
        )

    elif data.startswith("st:"):
        await handle_stats_callback(update, context, data)

# --- ОБНОВЛЁННЫЙ ОБРАБОТЧИК handle_text ---
# Теперь он должен учитывать временный ввод текста для рассылки админом.
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # --- ОБЯЗАТЕЛЬНО В НАЧАЛЕ ФУНКЦИИ ---
    global GS_SHEET, GS_AUTO_NURTURE_WS
    # ------------------------------------

    if not update.message:
        return

    user = update.effective_user
    admin_id = user.id

    # --- НОВЫЙ БЛОК: Обработка ввода текста/периода авторассылки админом ---
    if admin_id in ADMIN_IDS and update.message.text:
        text_input = update.message.text.strip()

                # Проверяем, является ли ввод числом (период)
        try:
            input_as_int = int(text_input)
            if input_as_int > 0:
                # Это период
                if GS_SHEET is None:
                    print(f"❌ Ошибка: GS_SHEET не инициализирована для обновления периода.")
                    await update.message.reply_text(f"❌ Ошибка обновления периода: подключение к таблице не готово.")
                    return
                try:
                    worksheet = GS_AUTO_NURTURE_WS # <-- ИСПОЛЬЗУЕМ ГЛОБАЛЬНУЮ ПЕРЕМЕННУЮ
                    # Обновляем только ячейку с периодом (I1) - строка 1, используя правильный формат
                    worksheet.update('I1', [[input_as_int]]) # <-- ИСПРАВЛЕНО: передаём как список списков
                    await update.message.reply_text(f"✅ Период авторассылки обновлён на: *{input_as_int}* дней.", parse_mode=ParseMode.MARKDOWN_V2)
                    print(f"✅ Админ {admin_id} обновил период авторассылки до {input_as_int} дней.")
                    return # Завершаем обработку для админа
                except gspread.exceptions.WorksheetNotFound:
                    print("❌ Вкладка 'auto_nurture' не найдена для обновления периода.")
                    await update.message.reply_text(f"❌ Ошибка обновления периода: вкладка 'auto_nurture' не найдена.")
                    return
                except Exception as e:
                    print(f"❌ Ошибка обновления периода: {e}")
                    await update.message.reply_text(f"❌ Ошибка обновления периода: {e}")
                    return
        except ValueError:
            # Не число, значит, это текст
            pass
        # Если не число, значит, это текст
        # (проверка числа выше, если прошла успешно, функция завершится return)
        # Поэтому если мы здесь, text_input - это текст
        if GS_SHEET is None:
            print(f"❌ Ошибка: GS_SHEET не инициализирована для обновления текста.")
            await update.message.reply_text(f"❌ Ошибка обновления текста: подключение к таблице не готово.")
            return
        try:
            worksheet = GS_AUTO_NURTURE_WS # <-- ИСПОЛЬЗУЕМ ГЛОБАЛЬНУЮ ПЕРЕМЕННУЮ
            # Обновляем только ячейку с текстом (H1) - строка 1, используя правильный формат
            worksheet.update('H1', [[text_input]]) # <-- ИСПРАВЛЕНО: передаём как список списков
            await update.message.reply_text(f"✅ Текст авторассылки обновлён:\n`{esc_md2(text_input)}`", parse_mode=ParseMode.MARKDOWN_V2)
            print(f"✅ Админ {admin_id} обновил текст авторассылки.")
            return # Завершаем обработку для админа
        except gspread.exceptions.WorksheetNotFound:
            print("❌ Вкладка 'auto_nurture' не найдена для обновления текста.")
            await update.message.reply_text(f"❌ Ошибка обновления текста: вкладка 'auto_nurture' не найдена.")
            return
        except Exception as e:
            print(f"❌ Ошибка обновления текста: {e}")
            await update.message.reply_text(f"❌ Ошибка обновления текста: {e}")
            return
    # --- Оригинальная логика для обычных пользователей (и остальная для админов) ---
    text = (update.message.text or "").strip()
    lower = text.lower()
    if "расклад" in lower:
        # ... (ваш старый код для обработки "расклад" начинается здесь)
        user = update.effective_user
        user_id = user.id
        username = user.username or ""
        first_name = user.first_name or ""
        reply = (
            "Поймала твой запрос на индивидуальный расклад. 💫\n"
            "Напиши, пожалуйста, про какую ситуацию хочешь посмотреть:\n"
            "– в чём сейчас вопрос/запрос;\n"
            "– какой формат тебе комфортнее (голосом, текстом, поэтапно).\n"
            "Я отвечу и предложу варианты по формату и стоимости."
            "Или сразу пиши мне в ЛС @Tatiataro18"
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
        # ... (остальной старый код для "расклад" продолжается)

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Только админ!")
        return
    
    await update.message.reply_text("⚙️ Админ-панель:", reply_markup=get_admin_keyboard())

async def handle_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    # --- ОБЪЯВЛЯЕМ ВСЕ GLOBAL ПЕРЕМЕННЫЕ В НАЧАЛЕ ФУНКЦИИ ---
    global GS_SHEET, GS_AUTO_NURTURE_WS
    # ------------------------------

    query = update.callback_query
    user = query.from_user
    if user.id not in ADMIN_IDS:
        await query.edit_message_text("Эта функция только для администратора.")
        return

    parts = data.split(":")
    action = parts[1]

    # --- НОВОЕ ДЕЙСТВИЕ ДЛЯ АВТОРАССЫЛКИ (исправлено для настроек в строке 1 и HTML) ---
    if action == "auto_nurture_menu":
        # Открываем меню управления авторассылкой
        if GS_AUTO_NURTURE_WS is None:
            print("❌ Ошибка: GS_AUTO_NURTURE_WS не инициализирована или вкладка не найдена для меню.")
            current_text = "Ошибка подключения/Вкладка не найдена"
            current_period = "Ошибка подключения/Вкладка не найдена"
        else:
            try:
                worksheet = GS_AUTO_NURTURE_WS
                settings_row = worksheet.row_values(1) # <-- ПРАВИЛЬНО: строка 1
                current_text = settings_row[7] if len(settings_row) > 7 else "" # <-- ПРАВИЛЬНО: колонка H (индекс 7)
                current_period = settings_row[8] if len(settings_row) > 8 else "" # <-- ПРАВИЛЬНО: колонка I (индекс 8)
            except gspread.exceptions.WorksheetNotFound:
                print("❌ Вкладка 'auto_nurture' не найдена для меню (но переменная была инициализирована как None).")
                current_text = "Вкладка не найдена"
                current_period = "Вкладка не найдена"
            except Exception as e:
                print(f"❌ Ошибка чтения настроек авторассылки в меню: {e}")
                current_text = "Ошибка загрузки"
                current_period = "Ошибка загрузки"

        # Экранируем полученные значения перед вставкой в HTML
        import html
        escaped_current_text = html.escape(current_text) # <-- Экранируем для HTML
        escaped_current_period = html.escape(str(current_period)) # <-- Экранируем для HTML

        instruction_html = (
            "<b>📤 МЕНЮ АВТОРАССЫЛКИ</b>\n\n"
            "Здесь можно настроить автоматическую рассылку.\n\n"
            f"<b>Текущий текст:</b>\n<pre>{escaped_current_text}</pre>\n\n" # <-- Используем HTML
            f"<b>Текущий период (дней):</b> <code>{escaped_current_period}</code>\n\n" # <-- Используем HTML
            "Для изменения:\n"
            "1. Отправьте <i>новый текст</i> в этот чат.\n"
            "2. Отправьте <i>новый период</i> (число дней) в этот чат.\n"
            "3. Изменения сохранятся в таблицу.\n\n"
            "Джоба проверяет каждые 24ч, пора ли отправлять."
        )

        keyboard = [
            [InlineKeyboardButton("❌ Закрыть", callback_data="st:menu")], # Возврат в меню
        ]
        # print(f"DEBUG: Sending HTML: {instruction_html}") # <-- Можно раскомментировать для отладки
        await query.edit_message_text(
            text=instruction_html,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML' # <-- Указываем HTML
        )
        return
    
    if action == "broadcast_menu":
        # Показываем меню для ввода сообщения рассылки
        # Так как бот не может просто так запросить ввод, мы дадим инструкции и кнопку подтверждения
        # Используем временное хранилище в bot_data для текста рассылки от конкретного админа
        admin_id = user.id
        temp_key = f"temp_broadcast_text_{admin_id}"

        # Получаем текущий текст, если он был введен ранее
        current_text = context.bot_data.get(temp_key, "")

        # Подготовим HTML-разметку
        instruction_html = (
            "<b>📢 МЕНЮ РАССЫЛКИ</b>\n\n"
            "Для выполнения рассылки:\n"
            "1. Отправьте <i>текст сообщения</i> для рассылки в этот чат.\n"
            "2. После отправки текста нажмите кнопку '✅ Подтвердить рассылку'.\n\n"
            "<b>Текущий введенный текст:</b> \n"
        )
        # Экранируем HTML-специфичные символы в введенном тексте
        import html
        escaped_current_text = html.escape(current_text)
        full_instruction_html = instruction_html + (f"<pre>{escaped_current_text}</pre>" if current_text else "_Пока не введено_")

        keyboard = [
            [InlineKeyboardButton("✅ Подтвердить рассылку", callback_data="st:broadcast_start")],
            [InlineKeyboardButton("❌ Отменить", callback_data="st:menu")], # Возврат в меню
        ]
        await query.edit_message_text(
            text=full_instruction_html,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML' # <-- Используем HTML
        )
        return

    elif action == "broadcast_start":
        admin_id = user.id
        temp_key = f"temp_broadcast_text_{admin_id}"
        message_to_send = context.bot_data.get(temp_key, "")

        if not message_to_send:
             await query.answer("❌ Нет текста для рассылки. Сначала введите текст.", show_alert=True)
             return

        # Запускаем рассылку
        await handle_broadcast_request(update, context, message_to_send)
        # Очищаем временный текст после отправки
        context.bot_data.pop(temp_key, None)
        return

    if action == "cod_status":
        current = CARD_OF_DAY_STATUS.get("enabled", True)
        CARD_OF_DAY_STATUS["enabled"] = not current
        new_status = "🤖 Авто" if CARD_OF_DAY_STATUS["enabled"] else "👋 Ручная"
        
        await query.answer(f"Карта дня переведена в режим: {new_status}", show_alert=True)
        
        # Возвращаем в подменю карты дня
        keyboard = [
            [InlineKeyboardButton(f"⚙️ Режим: {new_status}", callback_data="st:cod_status")],
            [InlineKeyboardButton("🧪 Отправить карту дня", callback_data="st:test_card")],
            [InlineKeyboardButton("⬅️ Назад в админ-меню", callback_data="st:menu")],
        ]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))
        return

        # ===== test_card =====
    if action == "test_card":
        await query.answer("Отправляю карту дня в канал...", show_alert=True)
        await send_card_of_the_day_to_channel(context)
        return
    
    # ===== reload_packs =====
    if action == "reload_packs":
        load_packs_from_sheets()
        count = len(PACKS_DATA)
        await query.answer(f"✅ Загружено {count} раскладов!", show_alert=True)
        return
    
    # ===== card_menu =====
    if action == "card_menu":
        cod_status = "🤖 Авто" if CARD_OF_DAY_STATUS.get("enabled", True) else "👋 Ручная"
        keyboard = [
            [InlineKeyboardButton(f"⚙️ Режим: {cod_status}", callback_data="st:cod_status")],
            [InlineKeyboardButton("🧪 Отправить карту дня в канал", callback_data="st:test_card")],
            [InlineKeyboardButton("⬅️ Назад в админ-меню", callback_data="st:menu")],
        ]
        await query.edit_message_text(
            "📅 Управление картой дня:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return
    
    # ===== stats_menu =====
    if action == "stats_menu":
        keyboard = [
            [InlineKeyboardButton("📊 Сегодня: все карты", callback_data="st:today:all")],
            [InlineKeyboardButton("📊 Сегодня: по карте", callback_data="st:today:cards")],
            [InlineKeyboardButton("📅 Вчера: все карты", callback_data="st:yesterday:all")],
            [InlineKeyboardButton("📈 7 дней: все карты", callback_data="st:7days:all")],
            [InlineKeyboardButton("📬 Воронка: 7 дней", callback_data="st:nurture:7days")],
            [InlineKeyboardButton("🧭 Действия: сегодня", callback_data="st:actions:today")],
            [InlineKeyboardButton("🧭 Действия: вчера", callback_data="st:actions:yesterday")],
            [InlineKeyboardButton("🧭 Действия: 7 дней", callback_data="st:actions:7days")],
            [InlineKeyboardButton("⬅️ Назад в админ-меню", callback_data="st:menu")],
        ]
        await query.edit_message_text(
            "📊 Статистика:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
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
        await query.answer("Попытки обновлены до 1/1 для этого аккаунта.", show_alert=True)
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

        # ===== users_menu =====
    if action == "users_menu":
        keyboard = [
            [InlineKeyboardButton("🆕 По последнему входу", callback_data="st:users_last")],
            [InlineKeyboardButton("📅 По первому входу", callback_data="st:users_first")],
            [InlineKeyboardButton("⬅️ Назад в админ-меню", callback_data="st:menu")],
        ]
        await query.edit_message_text(
            "👥 Список пользователей:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return
    
    # ===== users_last =====
    if action == "users_last":
        text = build_users_list(sort_by="last")
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True,
        )
        return
    
    # ===== users_first =====
    if action == "users_first":
        text = build_users_list(sort_by="first")
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
    rows = get_cached_actions()  # ← БЫЛО: load_actions()
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

def build_users_list(sort_by="last") -> str:
    """Список пользователей с первым и последним входом."""
    users = get_cached_users()  # ← БЫЛО: load_users()
    if not users:
        return esc_md2("Пока нет пользователей в боте.")
    
    # Группируем по user_id, берём первый и последний вход
    by_user = {}
    for row in users:
        uid = row["user_id"].strip()
        if not uid:
            continue
        
        dt = parse_iso(row["date_iso"])
        if dt is None:
            continue
        
        username = row.get("username", "").strip()
        first_name = row.get("first_name", "").strip()
        
        if uid not in by_user:
            by_user[uid] = {
                "username": username,
                "first_name": first_name,
                "first_dt": dt,
                "last_dt": dt,
            }
        else:
            if dt < by_user[uid]["first_dt"]:
                by_user[uid]["first_dt"] = dt
            if dt > by_user[uid]["last_dt"]:
                by_user[uid]["last_dt"] = dt
    
    if not by_user:
        return esc_md2("Нет корректных данных о пользователях.")
    
    lines = []
    lines.append(esc_md2(f"Всего уникальных пользователей: {len(by_user)}"))
    lines.append("")
    
    # Сортировка
    if sort_by == "first":
        lines.append(esc_md2("Сортировка: по первому входу (старые сверху)"))
        sorted_users = sorted(by_user.items(), key=lambda x: x[1]["first_dt"])
    else:
        lines.append(esc_md2("Сортировка: по последнему входу (свежие сверху)"))
        sorted_users = sorted(by_user.items(), key=lambda x: x[1]["last_dt"], reverse=True)
    
    lines.append("")
    lines.append(esc_md2("Первый вход | Последний вход | Пользователь"))
    lines.append("")
    
    for uid, info in sorted_users:
        first = info["first_dt"].strftime("%Y-%m-%d %H:%M")
        last = info["last_dt"].strftime("%Y-%m-%d %H:%M")
        
        username = info["username"]
        first_name = info["first_name"]
        
        if username:
            name_part = f"@{username}"
        elif first_name:
            name_part = f"{first_name} (id{uid})"
        else:
            name_part = f"id{uid}"
        
        line = f"{first} | {last} | {name_part}"
        lines.append(esc_md2(line))
    
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
        "🃏 1 попытка вытянуть метафорическую карту\n"
        "🎲 1 бросок кубика выбора\n\n"
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
    load_packs_from_sheets()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CommandHandler("test_day_card", test_day_card))
    app.add_handler(CommandHandler("debug_notify", debug_notify))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CommandHandler("reload_packs", reload_packs))

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
        time=dt_time(4, 5),  # В Москве на 3 часа больше
    name="card_of_day",
    )
    job_queue.run_daily(
        daily_reminder_job,
        time=dt_time(4, 5),  # В Москве на 3 часа больше
        name="daily_reminder",
    )

    # --- ПЛАНИРОВАНИЕ ДЖОБЫ ---
    job_queue = app.job_queue
    job_queue.run_daily(
        callback=auto_nurture_broadcast, # Наша ОБНОВЛЁННАЯ функция
        time=dt_time(hour=10, tzinfo=UTC), # <-- Используем dt_time # Время в UTC (например, 10:00 UTC). Выберите удобное.
        name="auto_nurture_job" # Имя для идентификации
    )
    print("✅ Джоба 'auto_nurture_job' запланирована (ежедневно в 10:00 UTC). Читает настройки из вкладки 'auto_nurture'.")

    # app.run_polling(...) или app.run_webhook(...)    
# тут как раз запуск веб‑сервиса на Render
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="",
        webhook_url=base_url,
        allowed_updates=None,
    )


if __name__ == "__main__":
    main()








































































