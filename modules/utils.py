import time
import os # Добавлен импорт os, так как он используется в load_last_report_ts
from datetime import datetime, UTC, timedelta
from typing import Dict, Any, List, Optional
from constants import CACHE_TTL # Предполагается, что CACHE_TTL определена в constants.py

# Кэши для RAM
USERS_CACHE = {'data': None, 'timestamp': 0, 'lock': False}
ACTIONS_CACHE = {'data': None, 'timestamp': 0, 'lock': False}

# ===== утилиты дат и текста =====
def esc_md2(text: str) -> str:
    """
    Экранирует специальные символы MarkdownV2.
    """
    if text is None:
        return ""
    # Символы, требующие экранирования в MarkdownV2
    chars = r'_*[]()~`>#+-=|{}.!'
    for ch in chars:
        text = text.replace(ch, "\\" + ch)
    return text

def parse_iso(dt_str: str) -> Optional[datetime]:
    """
    Парсит ISO-формат даты в объект datetime.
    Возвращает None в случае ошибки.
    """
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None

def load_last_report_ts(filepath: str) -> datetime: # Изменено: принимает путь к файлу
    """
    Загружает последнюю отметку времени из файла.
    Если файл не существует или ошибка, возвращает время 1 час назад.
    """
    if not os.path.exists(filepath): # Используется переданный путь
        return datetime.now(UTC) - timedelta(hours=1)
    try:
        with open(filepath, "r", encoding="utf-8") as f: # Используется переданный путь
            s = f.read().strip()
            return datetime.fromisoformat(s)
    except Exception:
        return datetime.now(UTC) - timedelta(hours=1)

def save_last_report_ts(filepath: str, ts: datetime): # Изменено: принимает путь к файлу
    """
    Сохраняет отметку времени в файл.
    """
    with open(filepath, "w", encoding="utf-8") as f: # Используется переданный путь
        f.write(ts.isoformat(timespec="seconds"))

# --- Лимиты попыток ---
def _normalize_daily_counters(user_ Dict[str, Any]):
    """
    Приводит счётчики попыток пользователя к актуальности текущего дня.
    """
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

def get_meta_left(user_ Dict[str, Any]) -> int:
    """
    Возвращает количество оставшихся попыток на мета-карту сегодня.
    """
    _normalize_daily_counters(user_data)
    used = user_data.get("meta_used", 0)
    return max(0, 1 - used)

def get_dice_left(user_ Dict[str, Any]) -> int:
    """
    Возвращает количество оставшихся попыток на кубик сегодня.
    """
    _normalize_daily_counters(user_data)
    used = user_data.get("dice_used", 0)
    return max(0, 1 - used)

# ===== КЭШ RAM =====
def get_cached_users(load_func):
    """Загружает пользователей из кэша или обновляет его."""
    now = time.time()
    if now - USERS_CACHE['timestamp'] > CACHE_TTL:
        print("🔄 Кэш users обновлён")
        USERS_CACHE['data'] = load_func() # load_func должна быть функцией загрузки
        USERS_CACHE['timestamp'] = now
    return USERS_CACHE['data']

def get_cached_actions(load_func):
    """Загружает действия из кэша или обновляет его."""
    now = time.time()
    if now - ACTIONS_CACHE['timestamp'] > CACHE_TTL:
        print("🔄 Кэш actions обновлён")
        ACTIONS_CACHE['data'] = load_func() # load_func должна быть функцией загрузки
        ACTIONS_CACHE['timestamp'] = now
    return ACTIONS_CACHE['data']

# Конец файла