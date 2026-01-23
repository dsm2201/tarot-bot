import os
import random
from telegram import Update
from telegram.error import TimedOut
from config import META_CARDS_DIR, DICE_DIR, CARD_OF_DAY_DIR
from modules.utils import esc_md2
import logging

logger = logging.getLogger(__name__)

async def send_random_meta_card(update: Update, context, user_data: dict):
    """Отправляет случайную мета-карту пользователю."""
    user = update.effective_user
    if not os.path.exists(META_CARDS_DIR):
        await update.message.reply_text("❌ Папка с мета-картами не найдена.")
        return

    files = os.listdir(META_CARDS_DIR)
    image_files = [f for f in files if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        await update.message.reply_text("❌ Нет доступных мета-карт.")
        return

    random_file = random.choice(image_files)
    file_path = os.path.join(META_CARDS_DIR, random_file)

    try:
        with open(file_path, 'rb') as photo:
            await update.message.reply_photo(
                photo=photo,
                caption=f"🃏 *Метафорическая карта* для {esc_md2(user.full_name)}\n\n_Позвольте образу говорить с вами._",
                parse_mode="MarkdownV2"
            )
        user_data["meta_used"] = user_data.get("meta_used", 0) + 1
        print(f">>> Отправлена мета-карта пользователю {user.id} ({user.full_name})")
    except TimedOut:
        print(">>> Ошибка таймаута при отправке мета-карты.")
        await update.message.reply_text("⏳ Время ожидания истекло. Попробуйте снова.")
    except Exception as e:
        print(f">>> Ошибка при отправке мета-карты: {e}")
        await update.message.reply_text("❌ Произошла ошибка при отправке карты.")

async def send_random_dice(update: Update, context, user_data: dict):
    """Отправляет случайный кубик пользователю."""
    user = update.effective_user
    if not os.path.exists(DICE_DIR):
        await update.message.reply_text("❌ Папка с кубиками не найдена.")
        return

    files = os.listdir(DICE_DIR)
    image_files = [f for f in files if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        await update.message.reply_text("❌ Нет доступных кубиков.")
        return

    random_file = random.choice(image_files)
    file_path = os.path.join(DICE_DIR, random_file)

    try:
        with open(file_path, 'rb') as photo:
            await update.message.reply_photo(
                photo=photo,
                caption=f"🎲 *Помощь кубика* для {esc_md2(user.full_name)}\n\n_Бросьте кубик и позвольте ему подсказать вам шаг._",
                parse_mode="MarkdownV2"
            )
        user_data["dice_used"] = user_data.get("dice_used", 0) + 1
        print(f">>> Отправлен кубик пользователю {user.id} ({user.full_name})")
    except TimedOut:
        print(">>> Ошибка таймаута при отправке кубика.")
        await update.message.reply_text("⏳ Время ожидания истекло. Попробуйте снова.")
    except Exception as e:
        print(f">>> Ошибка при отправке кубика: {e}")
        await update.message.reply_text("❌ Произошла ошибка при отправке кубика.")

async def send_card_of_the_day_image(context, chat_id: int):
    """Отправляет карту дня в указанный чат (канал или пользователю)."""
    if not os.path.exists(CARD_OF_DAY_DIR):
        print("❌ Папка с картами дня не найдена.")
        return

    files = os.listdir(CARD_OF_DAY_DIR)
    image_files = [f for f in files if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        print("❌ Нет доступных карт дня.")
        return

    random_file = random.choice(image_files)
    file_path = os.path.join(CARD_OF_DAY_DIR, random_file)

    try:
        with open(file_path, 'rb') as photo:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption="✨ *Карта дня*\n\nПусть она осветит ваш путь сегодня.",
                parse_mode="MarkdownV2"
            )
        print(f">>> Карта дня отправлена в чат {chat_id}. Выбран файл: {random_file}")
    except TimedOut:
        print(">>> Ошибка таймаута при отправке карты дня.")
    except Exception as e:
        print(f">>> Ошибка при отправке карты дня: {e}")
