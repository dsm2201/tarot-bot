from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from modules.utils import get_meta_left, get_dice_left
from config import CHANNEL_LINK

def get_admin_keyboard():
    """ЕДИНАЯ клавиатура админки"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Карта дня →", callback_data="st:card_menu")],
        [InlineKeyboardButton("🔄 Обновить расклады", callback_data="st:reload_packs")],
        [InlineKeyboardButton("📊 Статистика →", callback_data="st:stats_menu")],
        [InlineKeyboardButton("👥 Список пользователей →", callback_data="st:users_menu")],
        [InlineKeyboardButton("🔄 Обновить попытки", callback_data="st:reset_attempts")],
    ])

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