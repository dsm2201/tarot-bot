import asyncio
import json
import os
from datetime import datetime, UTC, timedelta, time as dt_time
from telegram import Update
from telegram.ext import ContextTypes
from modules.sheets import (
    load_users, update_subscribed_flag, log_nurture_to_sheet, GS_NURTURE_WS, GS_USERS_WS
)
from modules.utils import load_json, parse_iso
from config import CHANNEL_USERNAME, TEXTS_DIR, CARD_OF_DAY_ENABLED, CARD_OF_DAY_DIR
from modules.media import send_card_of_the_day_image
from constants import PARSE_MODE_MD2
from modules.utils import esc_md2

NURTURE_UNSUB = load_json(os.path.join(TEXTS_DIR, "nurture_unsub.json"))
NURTURE_SUB = load_json(os.path.join(TEXTS_DIR, "nurture_sub.json"))

async def send_card_of_the_day_to_channel(context: ContextTypes.DEFAULT_TYPE):
    """Отправляет карту дня в канал."""
    if not CARD_OF_DAY_ENABLED:
        print(">>> Отправка карты дня отключена через конфиг.")
        return

    if not CHANNEL_USERNAME:
        print(">>> CHANNEL_USERNAME не задан.")
        return

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
                chat_id=CHANNEL_USERNAME,
                photo=photo,
                caption="✨ *Карта дня*\n\nПусть она осветит ваш путь сегодня.",
                parse_mode=PARSE_MODE_MD2
            )
        print(f">>> Карта дня отправлена в канал @{CHANNEL_USERNAME}. Выбран файл: {random_file}")
        # Лог в Google Sheets
        from modules.sheets import log_card_of_day_publish
        log_card_of_day_publish(random_file, mode="auto")
    except Exception as e:
        print(f">>> Ошибка при отправке карты дня в канал: {e}")

async def notify_admins(context: ContextTypes.DEFAULT_TYPE):
    """Уведомляет администраторов о количестве пользователей и активности."""
    # Этот код предполагает, что ADMIN_IDS определены в config
    from config import ADMIN_IDS
    users = load_users()
    total_users = len(users)
    sub_users = sum(1 for u in users if u.get("subscribed") == "sub")

    message = f"📊 *Статистика бота*\n\nВсего пользователей: `{total_users}`\nПодписчиков: `{sub_users}`"

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=message, parse_mode=PARSE_MODE_MD2)
        except Exception as e:
            print(f">>> Ошибка уведомления администратора {admin_id}: {e}")

async def nurture_job(context: ContextTypes.DEFAULT_TYPE):
    """Отправляет nurture-сообщения пользователям."""
    if GS_NURTURE_WS is None:
        print(">>> nurture_job: лист nurture не найден, пропуск.")
        return
    if GS_USERS_WS is None:
        print(">>> nurture_job: лист users не найден, пропуск.")
        return

    users = load_users()
    nurture_rows = []  # Для отслеживания отправленных сообщений

    for user_rec in users:
        user_id_str = user_rec.get("user_id", "")
        if not user_id_str.isdigit():
            continue
        user_id = int(user_id_str)
        subscribed_status = user_rec.get("subscribed", "unsub")
        date_iso = user_rec.get("date_iso")

        if not date_iso:
            continue

        try:
            start_date = datetime.fromisoformat(date_iso)
        except Exception:
            continue

        now = datetime.now(UTC)
        days_since_start = (now - start_date).days

        if subscribed_status == "sub":
            messages = NURTURE_SUB
        else:
            messages = NURTURE_UNSUB

        # Находим подходящее сообщение для дня
        message_for_day = messages.get(str(days_since_start))

        if not message_for_day:
            continue

        # Проверяем, отправлялось ли уже сообщение для этого дня пользователю
        already_sent = any(
            nr.get("user_id") == str(user_id) and
            nr.get("day_num") == str(days_since_start)
            for nr in nurture_rows
        )
        if already_sent:
            continue

        # Отправляем сообщение
        try:
            escaped_text = esc_md2(message_for_day)
            await context.bot.send_message(
                chat_id=user_id,
                text=f"`[Подсказка дня]`\n\n{escaped_text}",
                parse_mode=PARSE_MODE_MD2
            )
            print(f">>> Nurture сообщение (день {days_since_start}, статус {subscribed_status}) отправлено пользователю {user_id}")

            # Логгируем успешную отправку
            log_nurture_to_sheet(user_id, "", subscribed_status, days_since_start, "sent")

            # Проверяем, изменился ли статус подписки после отправки (например, пользователь подписался)
            updated_users = load_users()
            current_sub_status = next((u.get("subscribed", "unsub") for u in updated_users if u.get("user_id") == str(user_id)), "unsub")
            if subscribed_status == "unsub" and current_sub_status == "sub":
                # Обновляем статус в логе nurture, если он изменился
                # Это требует поиска последней строки для этого юзера и обновления поля 'subscribed_after'
                # Упрощённый вариант: просто логгируем факт изменения
                print(f">>> Статус подписки пользователя {user_id} изменился на 'sub' после получения nurture-сообщения.")

        except Exception as e:
            error_msg = str(e)
            print(f">>> Ошибка отправки nurture сообщения пользователю {user_id} (день {days_since_start}): {e}")
            log_nurture_to_sheet(user_id, "", subscribed_status, days_since_start, "failed", error_msg)

async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    """Напоминает подписчикам зайти в бот и получить карту/кубик."""
    # Этот код также зависит от ADMIN_IDS и CHANNEL_USERNAME из config
    from config import ADMIN_IDS, CHANNEL_LINK
    users = load_users()
    sub_users = [u for u in users if u.get("subscribed") == "sub"]

    reminder_text = f"Привет! 🌟\nНе забудьте заглянуть в @{context.bot.username}, чтобы получить свою *метафорическую карту* или *помощь кубика* на день, а также новости в нашем канале: {CHANNEL_LINK}"

    for user_rec in sub_users:
        user_id_str = user_rec.get("user_id", "")
        if not user_id_str.isdigit():
            continue
        user_id = int(user_id_str)

        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=reminder_text,
                parse_mode=PARSE_MODE_MD2
            )
            print(f">>> Напоминание отправлено подписчику {user_id}")
        except Exception as e:
            print(f">>> Ошибка отправки напоминания пользователю {user_id}: {e}")

    # Также можно отправить напоминание админам
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"🤖 *Daily Reminder Job*\nОтправлено напоминаний: {len(sub_users)}",
                parse_mode=PARSE_MODE_MD2
            )
        except Exception as e:
            print(f">>> Ошибка уведомления администратора {admin_id} о напоминании: {e}")
