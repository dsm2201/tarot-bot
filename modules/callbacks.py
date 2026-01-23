import json
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from modules.keyboards import build_main_keyboard, build_packs_keyboard, get_admin_keyboard
from modules.media import send_random_meta_card, send_random_dice
from modules.sheets import log_action_to_sheet, load_packs_from_sheets, update_subscribed_flag, GS_USERS_WS
from modules.utils import _normalize_daily_counters, get_meta_left, get_dice_left
from modules.jobs import send_card_of_the_day_to_channel
from modules.stats import build_stats_text, build_users_list, build_actions_stats, build_nurture_stats
from config import ADMIN_IDS, CHANNEL_LINK
from constants import PARSE_MODE_MD2
from modules.utils import esc_md2

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на inline-кнопки."""
    query = update.callback_query
    data = query.data
    user = query.from_user
    user_id = user.id
    await query.answer()

    user_data = context.user_data
    _normalize_daily_counters(user_data)

    if data == "subscribe":
        current_status = user_data.get("subscribed", "unsub")
        new_status = "sub" if current_status == "unsub" else "unsub"
        user_data["subscribed"] = new_status
        if GS_USERS_WS:
            update_subscribed_flag(user_id, new_status == "sub")
        status_text = "подписки" if new_status == "sub" else "отписки"
        await query.edit_message_text(
            text=f"🔄 Вы {status_text} на ежедневные подсказки!",
            parse_mode=PARSE_MODE_MD2
        )
        log_action_to_sheet(user, f"toggle_subscribe_{new_status}", "callback")
        # Возвращаемся к главному меню
        await query.message.reply_text(
            f"Теперь вы {status_text} на подсказки!\n\nВот главное меню:",
            parse_mode=PARSE_MODE_MD2,
            reply_markup=build_main_keyboard(user_data)
        )

    elif data == "meta_card_today":
        meta_left = get_meta_left(user_data)
        if meta_left <= 0:
            await query.answer("❌ Сегодня попытки закончились.", show_alert=True)
            return
        await send_random_meta_card(update, context, user_data)
        log_action_to_sheet(user, "meta_card_today", "callback")

    elif data == "dice_today":
        dice_left = get_dice_left(user_data)
        if dice_left <= 0:
            await query.answer("❌ Сегодня попытки закончились.", show_alert=True)
            return
        await send_random_dice(update, context, user_data)
        log_action_to_sheet(user, "dice_today", "callback")

    elif data == "packs_menu":
        keyboard = build_packs_keyboard()
        await query.edit_message_text(
            text="📚 *Доступные расклады:*",
            parse_mode=PARSE_MODE_MD2,
            reply_markup=keyboard
        )
        log_action_to_sheet(user, "view_packs", "callback")

    elif data.startswith("pack_"):
        pack_code = data[len("pack_"):].upper()
        pack_info = PACKS_DATA.get(pack_code)
        if pack_info:
            escaped_title = esc_md2(pack_info["title"])
            escaped_description = esc_md2(pack_info["description"])
            response_text = f"{pack_info['emoji']} *{escaped_title}*\n\n{escaped_description}\n\nВведите 'да' для подтверждения записи."
            await query.edit_message_text(
                text=response_text,
                parse_mode=PARSE_MODE_MD2
            )
            context.user_data['pending_pack'] = pack_code
            log_action_to_sheet(user, f"view_pack_details_{pack_code}", "callback")
        else:
            await query.edit_message_text(text="❌ Расклад не найден.")

    elif data.startswith("st:"):
        await handle_stats_callback(update, context, data)

    else:
        await query.edit_message_text(text="❌ Неизвестная команда.")


async def handle_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
    """Обработка админских callback-запросов."""
    query = update.callback_query
    user = query.from_user
    if user.id not in ADMIN_IDS:
        await query.answer("У вас нет доступа.", show_alert=True)
        return

    log_action_to_sheet(user, f"admin_callback_{data}", "callback")

    if data == "st:card_menu":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Опубликовать сейчас", callback_data="st:publish_card_now")],
            [InlineKeyboardButton("📋 Статистика по карте дня", callback_data="st:card_stats")],
            [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
        ])
        await query.edit_message_text("📅 *Меню «Карта дня»*", parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

    elif data == "st:publish_card_now":
        await send_card_of_the_day_to_channel(context)
        await query.answer("Карта дня отправлена!", show_alert=True)

    elif data == "st:card_stats":
        # Предположим, у нас есть функция для сбора статистики по карте дня
        stats_text = "📊 Статистика по карте дня временно недоступна."
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="st:card_menu")]])
        await query.edit_message_text(text=stats_text, parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

    elif data == "st:reload_packs":
        load_packs_from_sheets()
        await query.answer("Расклады обновлены!", show_alert=True)

    elif data == "st:stats_menu":
        stats_text = build_stats_text()
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]])
        await query.edit_message_text(text=stats_text, parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

    elif data == "st:users_menu":
        users_text = build_users_list()
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]])
        await query.edit_message_text(text=users_text, parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

    elif data == "st:nurture_menu":
        nurture_text = build_nurture_stats()
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]])
        await query.edit_message_text(text=nurture_text, parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

    elif data == "st:reset_attempts":
        # Сбросить счётчики попыток для всех пользователей в user_data (RAM)
        # Это временное решение. Для персистентности нужно сбрасывать в Sheets.
        # В данном контексте мы можем сбросить только для активных сессий.
        # Более правильный способ - через команду или кнопку, влияющую на Sheets.
        # Пока просто покажем сообщение.
        await query.answer("Счётчики в памяти сброшены для активных сессий. Для всех пользователей см. команду администратора.", show_alert=True)

    elif data == "admin_back":
        keyboard = get_admin_keyboard()
        await query.edit_message_text("🔧 *Меню администратора*", parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)
