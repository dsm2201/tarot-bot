import os
import json
from telegram.constants import ParseMode

# --- Токены и ID ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))
ADMIN_IDS = {
    int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()
}
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
CHANNEL_LINK = os.getenv("CHANNEL_LINK")

# --- Google Sheets ---
GS_SERVICE_JSON = os.getenv("GS_SERVICE_JSON")
GS_SHEET_ID = os.getenv("GS_SHEET_ID")
USERS_SHEET_NAME = "users"
ACTIONS_SHEET_NAME = "actions"
NURTURE_SHEET_NAME = "nurture"
CARD_OF_DAY_SHEET_NAME = "card_of_day"

# --- Пути ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEXTS_DIR = os.path.join(BASE_DIR, "texts")
IMAGES_DIR = os.path.join(BASE_DIR, "images")
META_CARDS_DIR = os.path.join(IMAGES_DIR, "meta_cards")
DICE_DIR = os.path.join(IMAGES_DIR, "dice")
PACKS_DIR = os.path.join(IMAGES_DIR, "packs_images")
CARD_OF_DAY_DIR = os.path.join(IMAGES_DIR, "card_of_day_images")

LAST_REPORT_FILE = os.path.join(BASE_DIR, "data", "last_report_ts.txt")

# --- Конфигурация ---
CARD_OF_DAY_ENABLED = True