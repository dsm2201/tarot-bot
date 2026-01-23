import json
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from modules.sheets import log_start_to_sheet, log_action_to_sheet, PACKS_DATA
from modules.utils import load_json, _normalize_daily_counters, get_meta_left, get_dice_left
from modules.keyboards import build_main_keyboard, build_packs_keyboard
from config import CHANNEL_USERNAME, TEXTS_DIR, BASE_DIR
from constants import PARSE_MODE_MD2
from modules.utils import esc_md2

CARDS = load_json(os.path.join(TEXTS_DIR, "cards.json"))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start."""
    user = update.effective_user
    log_start_to_sheet(user, card_key=None)
    log_action_to_sheet(user, "/start", "command")

    welcome_text = (
        f"Привет, {esc_md2(user.full_name)}! 👋\n\n"
        "Я — ваш проводник в мир метафорических карт и интуитивных решений.\n\n"
        "Здесь вы можете:\n"
        "• Получить *метафорическую карту* дня для вдохновения\n"
        "• Использовать *помощь кубика*, чтобы подбросить идею\n"
        "• Записаться на персональный расклад\n"
        "• Подписаться на ежедневные подсказки\n\n"
        "*Выберите действие:*"
    )

    keyboard = build_main_keyboard(context.user_data)
    await update.message.reply_text(welcome_text, parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений."""
    user = update.effective_user
    message_text = update.message.text

    # Проверяем, может ли текст быть кодом расклада
    pack_info = PACKS_DATA.get(message_text.upper())
    if pack_info:
        log_action_to_sheet(user, f"pack_request_{message_text}", "text")
        escaped_title = esc_md2(pack_info["title"])
        escaped_description = esc_md2(pack_info["description"])

        response_text = f"{pack_info['emoji']} *{escaped_title}*\n\n{escaped_description}\n\nВведите 'да' для подтверждения записи."
        await update.message.reply_text(response_text, parse_mode=PARSE_MODE_MD2)
        # Сохраняем информацию о выбранном раскладе в user_data
        context.user_data['pending_pack'] = message_text
        return

    # Обработка подтверждения записи
    if message_text.lower() in ['да', 'yes']:
        pending_pack_code = context.user_data.get('pending_pack')
        if pending_pack_code:
            pack_info = PACKS_DATA.get(pending_pack_code)
            if pack_info:
                # Здесь должна быть логика подтверждения записи, например, отправка уведомления админу
                log_action_to_sheet(user, f"confirmed_pack_{pending_pack_code}", "text")
                escaped_title = esc_md2(pack_info["title"])
                await update.message.reply_text(
                    f"✅ Вы записаны на расклад: *{escaped_title}*!\n"
                    f"Скоро я с вами свяжусь.",
                    parse_mode=PARSE_MODE_MD2
                )
                # Сбрасываем информацию о pending_pack
                context.user_data.pop('pending_pack', None)
                return
        else:
            # Если нет pending_pack, но пришло 'да', возможно, это просто подтверждение чего-то другого
            pass

    # Если текст не подходит ни под одну команду, отправляем главное меню
    log_action_to_sheet(user, "text_fallback_to_main_menu", "text")
    fallback_text = "Я не понимаю эту команду. Вот главное меню:"
    keyboard = build_main_keyboard(context.user_data)
    await update.message.reply_text(fallback_text, parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Открывает админское меню."""
    user = update.effective_user
    from config import ADMIN_IDS
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет доступа к этой команде.")
        return

    log_action_to_sheet(user, "/admin", "command")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Карта дня →", callback_data="st:card_menu")],
        [InlineKeyboardButton("🔄 Обновить расклады", callback_data="st:reload_packs")],
        [InlineKeyboardButton("📊 Статистика →", callback_data="st:stats_menu")],
        [InlineKeyboardButton("👥 Список пользователей →", callback_data="st:users_menu")],
        [InlineKeyboardButton("🔄 Обновить попытки", callback_data="st:reset_attempts")],
    ])
    await update.message.reply_text("🔧 *Меню администратора*", parse_mode=PARSE_MODE_MD2, reply_markup=keyboard)

# --- Команды для админов ---

async def test_day_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестирует отправку карты дня в текущий чат."""
    user = update.effective_user
    from config import ADMIN_IDS
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет доступа к этой команде.")
        return
    log_action_to_sheet(user, "/test_day_card", "command")
    await send_card_of_the_day_image(context, update.effective_chat.id)
    await update.message.reply_text("Тестовая карта дня отправлена в этот чат.")

async def debug_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестирует уведомление админов."""
    user = update.effective_user
    from config import ADMIN_IDS
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("У вас нет доступа к этой команде.")
        return
    log_action_to_sheet(user, "/debug_notify", "command")
    await update.message.reply_text("Команда debug_notify выполнена. Уведомление админов запланировано.")
