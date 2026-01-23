from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from modules.handlers import start, handle_text, admin_menu
from modules.callbacks import button
from modules.jobs import send_card_of_the_day_to_channel, notify_admins, nurture_job, daily_reminder_job
from modules.sheets import init_gs_client, load_packs_from_sheets
from config import BOT_TOKEN, BASE_DIR, PORT
from datetime import time as dt_time
import os

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")

    # инициализируем Google Sheets
    init_gs_client()
    load_packs_from_sheets()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CommandHandler("test_day_card", test_day_card)) # нужно импортировать
    app.add_handler(CommandHandler("debug_notify", debug_notify)) # нужно импортировать
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CommandHandler("reload_packs", reload_packs)) # нужно импортировать

    job_queue = app.job_queue
    job_queue.run_repeating(notify_admins, interval=1800, first=300)
    job_queue.run_repeating(nurture_job, interval=24 * 3600, first=600)
    job_queue.run_daily(send_card_of_the_day_to_channel, time=dt_time(4, 5), name="card_of_day")
    job_queue.run_daily(daily_reminder_job, time=dt_time(4, 5), name="daily_reminder")

    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise RuntimeError("BASE_URL не задан")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path="",
        webhook_url=base_url,
        allowed_updates=None,
    )

if __name__ == "__main__":
    main()