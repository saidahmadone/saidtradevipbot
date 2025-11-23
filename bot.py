import os
import json
import time
import threading
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# -----------------------------
# НАСТРОЙКИ
# -----------------------------

ADMIN_ID = 5633585199   # Твой Telegram ID
CHANNEL_IDS = [2733453915]  # Тестовый канал (можешь добавлять ещё)

DATA_FILE = "users.json"

TOKEN = os.getenv("BOT_TOKEN")  # Ты добавишь токен в Render


# -----------------------------
# ФУНКЦИИ ДЛЯ РАБОТЫ С ФАЙЛОМ
# -----------------------------

def load_users():
    if not os.path.exists(DATA_FILE):
        return {}
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def save_users(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=4)


# -----------------------------
# КОМАНДА /adduser
# -----------------------------
async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
    except:
        await update.message.reply_text("Формат: /adduser USER_ID Дни")
        return

    data = load_users()
    end_date = (datetime.now() + timedelta(days=days)).timestamp()

    data[str(target_id)] = end_date
    save_users(data)

    await update.message.reply_text(
        f"Пользователь {target_id} добавлен на {days} дней."
    )


# -----------------------------
# КОМАНДА /extend
# -----------------------------
async def extend_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
    except:
        await update.message.reply_text("Формат: /extend USER_ID Дни")
        return

    data = load_users()

    if str(target_id) not in data:
        await update.message.reply_text("Этого пользователя нет в базе.")
        return

    data[str(target_id)] += days * 86400
    save_users(data)

    await update.message.reply_text(f"Продлил {target_id} на {days} дней.")


# -----------------------------
# КОМАНДА /check
# -----------------------------
async def check_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    data = load_users()
    now = time.time()

    msg = "Список пользователей:\n\n"
    for uid, end in data.items():
        days_left = int((end - now) / 86400)
        msg += f"ID {uid}: осталось {days_left} дней\n"

    await update.message.reply_text(msg)


# -----------------------------
# ФОНОВАЯ ПРОВЕРКА ПОЛЬЗОВАТЕЛЕЙ
# -----------------------------
async def background_checker(app):
    while True:
        data = load_users()
        now = time.time()

        for uid, end in list(data.items()):
            remaining = end - now

            # 3 дня осталось
            if 0 < remaining < 3 * 86400:
                try:
                    await app.bot.send_message(
                        ADMIN_ID,
                        f"У пользователя {uid} осталось меньше 3 дней."
                    )
                except:
                    pass

            # время истекло
            if remaining <= 0:
                # удалить из канала
                for channel in CHANNEL_IDS:
                    try:
                        await app.bot.ban_chat_member(channel, int(uid))
                        await app.bot.unban_chat_member(channel, int(uid))
                    except:
                        pass

                del data[uid]
                save_users(data)

                try:
                    await app.bot.send_message(
                        ADMIN_ID,
                        f"Пользователь {uid} удалён из канала."
                    )
                except:
                    pass

        await asyncio.sleep(60)


# -----------------------------
# ЗАПУСК БОТА
# -----------------------------
import asyncio

async def main():
    app = ApplicationBuilder().token(TOKEN).build()

    # команды
    app.add_handler(CommandHandler("adduser", add_user))
    app.add_handler(CommandHandler("extend", extend_user))
    app.add_handler(CommandHandler("check", check_users))

    # запуск фоновой проверки
    asyncio.create_task(background_checker(app))

    await app.run_polling()


if __name__ == "__main__":
    asyncio.run(main())
