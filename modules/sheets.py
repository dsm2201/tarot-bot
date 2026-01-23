import json
import gspread
from gspread.auth import service_account_from_dict
from config import GS_SERVICE_JSON, GS_SHEET_ID, USERS_SHEET_NAME, ACTIONS_SHEET_NAME, NURTURE_SHEET_NAME, CARD_OF_DAY_SHEET_NAME
from constants import CACHE_TTL
import time
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, UTC

# ----- Глобальные переменные для Google Sheets -----
GS_CLIENT = None
GS_SHEET = None
GS_USERS_WS = None
GS_ACTIONS_WS = None
GS_NURTURE_WS = None
GS_CARD_OF_DAY_WS = None
GS_PACKS_WS = None
PACKS_DATA = {}  # словарь: {code: {title, emoji, description, filename}}

# Кэши для RAM
USERS_CACHE = {'data': None, 'timestamp': 0}
ACTIONS_CACHE = {'data': None, 'timestamp': 0}

def init_gs_client():
    global GS_CLIENT, GS_SHEET, GS_USERS_WS, GS_ACTIONS_WS, GS_NURTURE_WS, GS_CARD_OF_DAY_WS, GS_PACKS_WS
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

        try:
            packs_ws = sheet.worksheet("packs")  # <- ЭТОТ БЛОК
        except Exception:
            packs_ws = None

        GS_CLIENT = client
        GS_SHEET = sheet
        GS_USERS_WS = users_ws
        GS_ACTIONS_WS = actions_ws
        GS_NURTURE_WS = nurture_ws
        GS_CARD_OF_DAY_WS = card_of_day_ws
        GS_PACKS_WS = packs_ws  # <- И ПРИСВАИВАНИЕ
        print(">>> Google Sheets: успешно подключено к tatiataro_log.")
    except Exception as e:
        print(f">>> Google Sheets init error: {e}")
        GS_CLIENT = None
        GS_SHEET = None
        GS_USERS_WS = None
        GS_ACTIONS_WS = None
        GS_NURTURE_WS = None
        GS_CARD_OF_DAY_WS = None
        GS_PACKS_WS = None


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
