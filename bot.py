import os
import json
import asyncio
import logging
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ====================
# НАСТРОЙКИ
# ====================
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "5633585199"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-1002593053252"))

# Настройка логов
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Файлы для хранения данных
DATA_FILE = "users.json"
HISTORY_FILE = "history.json"

# ====================
# БАЗА ДАННЫХ
# ====================
def load_data(filename):
    """Загружает данные из JSON файла"""
    try:
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Ошибка загрузки {filename}: {e}")
        return {}

def save_data(filename, data):
    """Сохраняет данные в JSON файл"""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка сохранения {filename}: {e}")

def load_users():
    return load_data(DATA_FILE)

def save_users(data):
    save_data(DATA_FILE, data)

def add_to_history(action):
    """Добавляет действие в историю"""
    history = load_data(HISTORY_FILE)
    timestamp = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    
    if "actions" not in history:
        history["actions"] = []
    
    history["actions"].insert(0, {
        "timestamp": timestamp,
        "action": action
    })
    
    # Сохраняем только последние 100 действий
    history["actions"] = history["actions"][:100]
    
    save_data(HISTORY_FILE, history)

# ====================
# ПОЛУЧЕНИЕ ИНФОРМАЦИИ О ПОЛЬЗОВАТЕЛЕ
# ====================
async def get_user_info(bot, user_id):
    """Получает информацию о пользователе"""
    try:
        user = await bot.get_chat(user_id)
        name_parts = []
        if user.first_name:
            name_parts.append(user.first_name)
        if user.last_name:
            name_parts.append(user.last_name)
        
        return {
            "name": " ".join(name_parts) if name_parts else "Неизвестно",
            "username": f"@{user.username}" if user.username else "нет username",
            "id": user_id,
            "profile_link": f"[Профиль](tg://user?id={user_id})"
        }
    except Exception as e:
        logger.error(f"Ошибка получения информации о пользователе {user_id}: {e}")
        return {
            "name": "Неизвестно",
            "username": "нет username",
            "id": user_id,
            "profile_link": f"[Профиль](tg://user?id={user_id})"
        }

# ====================
# ПРОВЕРКА АДМИНА
# ====================
async def is_admin(update: Update):
    """Проверяет является ли пользователь админом"""
    return update.effective_user.id == ADMIN_ID

async def admin_only(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет сообщение если не админ"""
    if not await is_admin(update):
        await update.message.reply_text("❌ Эта команда только для администратора!")
        return False
    return True

# ====================
# КОМАНДЫ
# ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    if not await admin_only(update, context):
        return
    
    users_count = len(load_users())
    
    await update.message.reply_text(
        f"🤖 **БОТ ДЛЯ УПРАВЛЕНИЯ ДОСТУПОМ К КАНАЛУ**\n\n"
        f"📊 В базе: {users_count} пользователей\n"
        f"👑 Админ: {ADMIN_ID}\n\n"
        f"📋 **КОМАНДЫ:**\n"
        f"• /start - эта информация\n"
        f"• /adduser ID ДНИ - добавить пользователя\n"
        f"• /addall ДНИ - добавить ВСЕХ участников канала\n"
        f"• /extend ID ДНИ - продлить подписку\n"
        f"• /remove ID - удалить пользователя\n"
        f"• /check - список всех пользователей\n"
        f"• /getids - ID всех участников канала\n"
        f"• /history - история действий\n"
        f"• /stats - статистика\n"
        f"• /ignore ID - игнорировать нового участника",
        parse_mode='Markdown'
    )

async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавить пользователя /adduser ID ДНИ"""
    if not await admin_only(update, context):
        return
    
    try:
        user_id = int(context.args[0])
        days = int(context.args[1])
    except (IndexError, ValueError):
        await update.message.reply_text(
            "❌ **НЕПРАВИЛЬНЫЙ ФОРМАТ!**\n\n"
            "📝 **Правильно:**\n"
            "`/adduser 123456789 30`\n\n"
            "• 123456789 - ID пользователя\n"
            "• 30 - количество дней",
            parse_mode='Markdown'
        )
        return
    
    data = load_users()
    user_key = str(user_id)
    
    # Получаем информацию о пользователе
    user_info = await get_user_info(context.bot, user_id)
    
    if user_key in data:
        # Пользователь уже есть - обновляем
        current_end = data[user_key]
        new_end = current_end + (days * 86400)
        data[user_key] = new_end
        
        action = f"📅 Обновлён пользователь {user_id} (+{days} дней)"
    else:
        # Новый пользователь
        end_date = datetime.now() + timedelta(days=days)
        data[user_key] = end_date.timestamp()
        
        action = f"✅ Добавлен пользователь {user_id} ({days} дней)"
    
    save_users(data)
    add_to_history(action)
    
    end_date = datetime.fromtimestamp(data[user_key])
    
    await update.message.reply_text(
        f"✅ **ГОТОВО!**\n\n"
        f"👤 **{user_info['name']}**\n"
        f"📱 {user_info['profile_link']}\n"
        f"🆔 ID: `{user_id}`\n"
        f"🔗 {user_info['username']}\n\n"
        f"⏳ **Срок доступа:**\n"
        f"• Дней: {days}\n"
        f"• До: {end_date.strftime('%d.%m.%Y %H:%M')}",
        parse_mode='Markdown'
    )

async def add_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавить всех участников канала /addall ДНИ"""
    if not await admin_only(update, context):
        return
    
    try:
        days = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text(
            "❌ **НЕПРАВИЛЬНЫЙ ФОРМАТ!**\n\n"
            "📝 **Правильно:**\n"
            "`/addall 30`\n\n"
            "• 30 - количество дней для ВСЕХ участников",
            parse_mode='Markdown'
        )
        return
    
    await update.message.reply_text(f"⏳ Начинаю добавление {days} дней для ВСЕХ участников...")
    
    try:
        data = load_users()
        added_count = 0
        updated_count = 0
        errors = []
        
        async for member in context.bot.get_chat_members(CHANNEL_ID):
            try:
                user_id = member.user.id
                
                # Пропускаем самого бота
                if user_id == context.bot.id:
                    continue
                
                user_key = str(user_id)
                end_date = datetime.now() + timedelta(days=days)
                
                if user_key in data:
                    data[user_key] = end_date.timestamp()
                    updated_count += 1
                else:
                    data[user_key] = end_date.timestamp()
                    added_count += 1
                    
            except Exception as e:
                errors.append(f"Ошибка с пользователем {user_id}: {str(e)}")
                continue
        
        save_users(data)
        add_to_history(f"📊 Массовое добавление: +{days} дней для {added_count + updated_count} пользователей")
        
        result_message = (
            f"✅ **МАССОВОЕ ДОБАВЛЕНИЕ ЗАВЕРШЕНО!**\n\n"
            f"📊 **Результат:**\n"
            f"• Добавлено новых: {added_count}\n"
            f"• Обновлено существующих: {updated_count}\n"
            f"• Всего обработано: {added_count + updated_count}\n"
            f"• Срок: {days} дней\n\n"
            f"⏳ **Новый срок для всех:**\n"
            f"До: {(datetime.now() + timedelta(days=days)).strftime('%d.%m.%Y')}"
        )
        
        if errors:
            result_message += f"\n\n⚠️ **Были ошибки:** {len(errors)}"
            if len(errors) <= 5:
                for error in errors[:5]:
                    result_message += f"\n• {error}"
        
        await update.message.reply_text(result_message, parse_mode='Markdown')
        
    except Exception as e:
        await update.message.reply_text(f"❌ **ОШИБКА:** {str(e)}")

async def extend_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Продлить подписку /extend ID ДНИ"""
    if not await admin_only(update, context):
        return
    
    try:
        user_id = int(context.args[0])
        days = int(context.args[1])
    except (IndexError, ValueError):
        await update.message.reply_text(
            "❌ **НЕПРАВИЛЬНЫЙ ФОРМАТ!**\n\n"
            "📝 **Правильно:**\n"
            "`/extend 123456789 30`\n\n"
            "• 123456789 - ID пользователя\n"
            "• 30 - количество дней для продления",
            parse_mode='Markdown'
        )
        return
    
    data = load_users()
    user_key = str(user_id)
    
    if user_key not in data:
        await update.message.reply_text(
            f"❌ **ПОЛЬЗОВАТЕЛЬ НЕ НАЙДЕН!**\n\n"
            f"Пользователь `{user_id}` не найден в базе.\n"
            f"💡 Используйте `/adduser {user_id} {days}` чтобы добавить.",
            parse_mode='Markdown'
        )
        return
    
    # Получаем информацию о пользователе
    user_info = await get_user_info(context.bot, user_id)
    
    # Продлеваем
    current_end = data[user_key]
    new_end = current_end + (days * 86400)
    data[user_key] = new_end
    save_users(data)
    
    add_to_history(f"📈 Продлён пользователь {user_id} (+{days} дней)")
    
    old_date = datetime.fromtimestamp(current_end)
    new_date = datetime.fromtimestamp(new_end)
    
    await update.message.reply_text(
        f"✅ **ПОДПИСКА ПРОДЛЕНА!**\n\n"
        f"👤 **{user_info['name']}**\n"
        f"📱 {user_info['profile_link']}\n"
        f"🆔 ID: `{user_id}`\n\n"
        f"📅 **Было:** {old_date.strftime('%d.%m.%Y %H:%M')}\n"
        f"📅 **Стало:** {new_date.strftime('%d.%m.%Y %H:%M')}\n"
        f"⏳ **Добавлено дней:** {days}",
        parse_mode='Markdown'
    )

async def remove_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удалить пользователя /remove ID"""
    if not await admin_only(update, context):
        return
    
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text(
            "❌ **НЕПРАВИЛЬНЫЙ ФОРМАТ!**\n\n"
            "📝 **Правильно:**\n"
            "`/remove 123456789`\n\n"
            "• 123456789 - ID пользователя для удаления",
            parse_mode='Markdown'
        )
        return
    
    data = load_users()
    user_key = str(user_id)
    
    if user_key not in data:
        await update.message.reply_text(
            f"❌ **ПОЛЬЗОВАТЕЛЬ НЕ НАЙДЕН!**\n\n"
            f"Пользователь `{user_id}` не найден в базе.",
            parse_mode='Markdown'
        )
        return
    
    # Получаем информацию о пользователе
    user_info = await get_user_info(context.bot, user_id)
    
    # Удаляем из канала
    try:
        await context.bot.ban_chat_member(CHANNEL_ID, user_id)
        await context.bot.unban_chat_member(CHANNEL_ID, user_id)
        channel_action = "✅ Удалён из канала"
    except Exception as e:
        channel_action = f"⚠️ Не удалён из канала: {str(e)}"
    
    # Удаляем из базы
    del data[user_key]
    save_users(data)
    
    add_to_history(f"🗑️ Удалён пользователь {user_id}")
    
    await update.message.reply_text(
        f"✅ **ПОЛЬЗОВАТЕЛЬ УДАЛЁН!**\n\n"
        f"👤 **{user_info['name']}**\n"
        f"📱 {user_info['profile_link']}\n"
        f"🆔 ID: `{user_id}`\n\n"
        f"📊 **Результат:**\n"
        f"• База данных: ❌ Удалён\n"
        f"• Канал: {channel_action}",
        parse_mode='Markdown'
    )

async def check_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать всех пользователей /check"""
    if not await admin_only(update, context):
        return
    
    data = load_users()
    
    if not data:
        await update.message.reply_text("📭 **База пользователей пуста!**")
        return
    
    await update.message.reply_text("⏳ Получаю информацию о пользователях...")
    
    now = datetime.now().timestamp()
    active_users = []
    expired_users = []
    
    # Сортируем по дате окончания (сначала те, у кого скоро истекает)
    sorted_users = sorted(data.items(), key=lambda x: x[1])
    
    for user_id_str, end_time in sorted_users:
        user_id = int(user_id_str)
        days_left = int((end_time - now) / 86400)
        end_date = datetime.fromtimestamp(end_time)
        
        try:
            user_info = await get_user_info(context.bot, user_id)
        except:
            user_info = {
                "name": "Неизвестно",
                "profile_link": f"[Профиль](tg://user?id={user_id})",
                "username": ""
            }
        
        user_data = {
            "id": user_id,
            "info": user_info,
            "days_left": days_left,
            "end_date": end_date
        }
        
        if days_left > 0:
            active_users.append(user_data)
        else:
            expired_users.append(user_data)
    
    # Показываем активных пользователей
    if active_users:
        message = "🟢 **АКТИВНЫЕ ПОЛЬЗОВАТЕЛИ:**\n\n"
        
        for i, user in enumerate(active_users[:50], 1):
            status_icon = "🟡" if user["days_left"] <= 1 else "🟢"
            
            message += f"{i}. {status_icon} **{user['info']['name']}**\n"
            message += f"   📱 {user['info']['profile_link']}\n"
            message += f"   🆔 ID: `{user['id']}`\n"
            if user['info']['username'] and user['info']['username'] != "нет username":
                message += f"   🔗 {user['info']['username']}\n"
            message += f"   ⏳ Осталось: {user['days_left']} дней\n"
            message += f"   📅 До: {user['end_date'].strftime('%d.%m.%Y %H:%M')}\n\n"
            
            if i % 5 == 0:
                await update.message.reply_text(message, parse_mode='Markdown')
                message = ""
                await asyncio.sleep(0.5)
        
        if message:
            await update.message.reply_text(message, parse_mode='Markdown')
    
    # Показываем истекших пользователей
    if expired_users:
        message = "🔴 **ИСТЕКШИЕ ПОДПИСКИ:**\n\n"
        
        for i, user in enumerate(expired_users[:20], 1):
            message += f"{i}. 🔴 **{user['info']['name']}**\n"
            message += f"   📱 {user['info']['profile_link']}\n"
            message += f"   🆔 ID: `{user['id']}`\n"
            if user['info']['username'] and user['info']['username'] != "нет username":
                message += f"   🔗 {user['info']['username']}\n"
            message += f"   ⏰ Истек: {user['end_date'].strftime('%d.%m.%Y')}\n\n"
            
            if i % 5 == 0:
                await update.message.reply_text(message, parse_mode='Markdown')
                message = ""
                await asyncio.sleep(0.5)
        
        if message:
            await update.message.reply_text(message, parse_mode='Markdown')
    
    # Статистика
    stats_message = (
        f"📊 **СТАТИСТИКА:**\n"
        f"• Всего в базе: {len(data)}\n"
        f"• Активных: {len(active_users)}\n"
        f"• Истекших: {len(expired_users)}"
    )
    
    await update.message.reply_text(stats_message, parse_mode='Markdown')

async def get_ids(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получить ID всех участников канала /getids"""
    if not await admin_only(update, context):
        return
    
    # Проверяем что бот админ
    try:
        chat_member = await context.bot.get_chat_member(CHANNEL_ID, context.bot.id)
        if chat_member.status not in ["administrator", "creator"]:
            await update.message.reply_text(
                "❌ **БОТ НЕ ЯВЛЯЕТСЯ АДМИНИСТРАТОРОМ!**\n\n"
                "📋 Что сделать:\n"
                "1. Зайдите в настройки канала\n"
                "2. Выберите 'Администраторы'\n"
                "3. Добавьте этого бота\n"
                "4. Дайте права:\n"
                "   • Исключение участников\n"
                "   • Просмотр участников",
                parse_mode='Markdown'
            )
            return
    except Exception as e:
        await update.message.reply_text(f"❌ **ОШИБКА ПРОВЕРКИ ПРАВ:** {str(e)}")
        return
    
    await update.message.reply_text("⏳ Получаю список участников канала...")
    
    try:
        message = "🆔 **УЧАСТНИКИ КАНАЛА:**\n\n"
        count = 0
        
        async for member in context.bot.get_chat_members(CHANNEL_ID):
            user = member.user
            
            # Пропускаем самого бота
            if user.id == context.bot.id:
                continue
            
            count += 1
            
            name_parts = []
            if user.first_name:
                name_parts.append(user.first_name)
            if user.last_name:
                name_parts.append(user.last_name)
            
            name = " ".join(name_parts) if name_parts else "Неизвестно"
            username = f"@{user.username}" if user.username else "нет username"
            
            message += f"{count}. **{name}**\n"
            message += f"   📱 [Профиль](tg://user?id={user.id})\n"
            message += f"   🆔 ID: `{user.id}`\n"
            message += f"   🔗 {username}\n\n"
            
            if count % 5 == 0:
                await update.message.reply_text(message, parse_mode='Markdown')
                message = ""
                await asyncio.sleep(0.5)
        
        if message:
            await update.message.reply_text(message, parse_mode='Markdown')
        
        await update.message.reply_text(
            f"✅ **ГОТОВО!**\n\n"
            f"📊 Всего участников: {count}\n\n"
            f"💡 **КАК ДОБАВИТЬ:**\n"
            f"Используйте команду:\n"
            f"`/adduser ID ДНИ`\n\n"
            f"📝 **Пример:**\n"
            f"`/adduser 123456789 90`",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        await update.message.reply_text(f"❌ **ОШИБКА:** {str(e)}")

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать историю действий /history"""
    if not await admin_only(update, context):
        return
    
    try:
        count = 50
        if context.args:
            try:
                count = int(context.args[0])
                count = min(count, 100)
            except:
                pass
    except:
        count = 50
    
    history = load_data(HISTORY_FILE)
    
    if not history or "actions" not in history or not history["actions"]:
        await update.message.reply_text("📭 **История действий пуста!**")
        return
    
    await update.message.reply_text(f"📜 **ИСТОРИЯ ДЕЙСТВИЙ (последние {min(count, len(history['actions']))}):**\n")
    
    message = ""
    for i, action in enumerate(history["actions"][:count], 1):
        message += f"{i}. **{action['timestamp']}** - {action['action']}\n\n"
        
        if i % 10 == 0:
            await update.message.reply_text(message, parse_mode='Markdown')
            message = ""
            await asyncio.sleep(0.5)
    
    if message:
        await update.message.reply_text(message, parse_mode='Markdown')

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать статистику /stats"""
    if not await admin_only(update, context):
        return
    
    data = load_users()
    now = datetime.now().timestamp()
    
    active_count = 0
    expiring_soon = 0
    expired_count = 0
    
    for end_time in data.values():
        days_left = (end_time - now) / 86400
        
        if days_left > 0:
            active_count += 1
            if days_left <= 3:
                expiring_soon += 1
        else:
            expired_count += 1
    
    channel_stats = "❓ Неизвестно"
    try:
        chat = await context.bot.get_chat(CHANNEL_ID)
        channel_stats = f"{chat.title}"
    except:
        pass
    
    await update.message.reply_text(
        f"📊 **СТАТИСТИКА СИСТЕМЫ**\n\n"
        f"👥 **ПОЛЬЗОВАТЕЛИ:**\n"
        f"• Всего в базе: {len(data)}\n"
        f"• Активных: {active_count}\n"
        f"• Скоро истекает (<3 дней): {expiring_soon}\n"
        f"• Истекших: {expired_count}\n\n"
        f"📺 **КАНАЛ:**\n"
        f"• Название: {channel_stats}\n"
        f"• ID: `{CHANNEL_ID}`\n\n"
        f"🤖 **БОТ:**\n"
        f"• Админ ID: `{ADMIN_ID}`\n"
        f"• Статус: 🟢 Работает",
        parse_mode='Markdown'
    )

async def ignore_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Игнорировать пользователя /ignore ID"""
    if not await admin_only(update, context):
        return
    
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text(
            "❌ **НЕПРАВИЛЬНЫЙ ФОРМАТ!**\n\n"
            "📝 **Правильно:**\n"
            "`/ignore 123456789`\n\n"
            "• 123456789 - ID пользователя для игнорирования",
            parse_mode='Markdown'
        )
        return
    
    user_info = await get_user_info(context.bot, user_id)
    
    await update.message.reply_text(
        f"👌 **ПОЛЬЗОВАТЕЛЬ ИГНОРИРУЕТСЯ**\n\n"
        f"👤 **{user_info['name']}**\n"
        f"📱 {user_info['profile_link']}\n"
        f"🆔 ID: `{user_id}`\n\n"
        f"💡 **Что это значит:**\n"
        f"• Пользователь не будет добавлен в базу\n"
        f"• Не будет получать уведомления\n"
        f"• Не будет автоматически удалён",
        parse_mode='Markdown'
    )
    
    add_to_history(f"👌 Игнорирован пользователь {user_id}")

# ====================
# ФОНОВЫЕ ПРОВЕРКИ
# ====================
async def background_checker(app):
    """Фоновая проверка подписок"""
    notified_users = {}
    
    while True:
        try:
            data = load_users()
            now = datetime.now().timestamp()
            
            for user_id_str, end_time in data.items():
                user_id = int(user_id_str)
                remaining = end_time - now
                
                # Уведомление за 1 день (24 часа)
                if 0 < remaining < 86400:
                    last_notified = notified_users.get(user_id_str)
                    
                    if not last_notified or (now - last_notified) > 43200:
                        try:
                            user_info = await get_user_info(app.bot, user_id)
                            
                            await app.bot.send_message(
                                ADMIN_ID,
                                f"⚠️ **СКОРО ИСТЕКАЕТ ПОДПИСКА!**\n\n"
                                f"👤 **{user_info['name']}**\n"
                                f"📱 {user_info['profile_link']}\n"
                                f"🆔 ID: `{user_id}`\n"
                                f"🔗 {user_info['username']}\n\n"
                                f"⏳ **Осталось менее 1 дня!**\n"
                                f"📅 Истекает: {datetime.fromtimestamp(end_time).strftime('%d.%m.%Y %H:%M')}\n\n"
                                f"💡 **Действие:**\n"
                                f"Используйте: `/extend {user_id} ДНИ`",
                                parse_mode='Markdown'
                            )
                            
                            notified_users[user_id_str] = now
                            add_to_history(f"⏰ Уведомление: у {user_id} остался 1 день")
                            
                        except Exception as e:
                            logger.error(f"Ошибка уведомления для {user_id}: {e}")
                
                # Удаление при истечении
                if remaining <= 0:
                    try:
                        # Удаляем из канала
                        await app.bot.ban_chat_member(CHANNEL_ID, user_id)
                        await app.bot.unban_chat_member(CHANNEL_ID, user_id)
                        
                        if user_id_str in notified_users:
                            del notified_users[user_id_str]
                        
                        # Удаляем из базы
                        del data[user_id_str]
                        save_users(data)
                        
                        user_info = await get_user_info(app.bot, user_id)
                        
                        await app.bot.send_message(
                            ADMIN_ID,
                            f"🗑️ **ПОДПИСКА ИСТЕКЛА!**\n\n"
                            f"👤 **{user_info['name']}**\n"
                            f"📱 {user_info['profile_link']}\n"
                            f"🆔 ID: `{user_id}`\n"
                            f"🔗 {user_info['username']}\n\n"
                            f"⏰ **Автоматически удалён из канала**\n"
                            f"🕐 Время: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
                            parse_mode='Markdown'
                        )
                        
                        add_to_history(f"🗑️ Авто-удаление: истек срок у {user_id}")
                        
                    except Exception as e:
                        logger.error(f"Ошибка удаления {user_id}: {e}")
        
        except Exception as e:
            logger.error(f"Ошибка в фоновой проверке: {e}")
        
        await asyncio.sleep(300)

# ====================
# ЗАПУСК БОТА
# ====================
async def main():
    """Основная функция запуска"""
    if not TOKEN:
        logger.error("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("❌ ОШИБКА: Установите переменную окружения BOT_TOKEN в Render.com")
        return
    
    logger.info(f"🚀 Запуск бота для админа {ADMIN_ID}...")
    
    # Создаем приложение
    app = Application.builder().token(TOKEN).build()
    
    # Добавляем обработчики команд
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("adduser", add_user))
    app.add_handler(CommandHandler("addall", add_all))
    app.add_handler(CommandHandler("extend", extend_user))
    app.add_handler(CommandHandler("remove", remove_user))
    app.add_handler(CommandHandler("check", check_users))
    app.add_handler(CommandHandler("getids", get_ids))
    app.add_handler(CommandHandler("history", show_history))
    app.add_handler(CommandHandler("stats", show_stats))
    app.add_handler(CommandHandler("ignore", ignore_user))
    
    # Запускаем фоновую проверку
    asyncio.create_task(background_checker(app))
    
    logger.info("✅ Бот запущен! Доступен только админу.")
    print("✅ Бот запущен и готов к работе!")
    
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
