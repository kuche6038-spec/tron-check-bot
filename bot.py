import logging
import asyncio
import re
import os
import json
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
import gspread
from google.oauth2.service_account import Credentials

# ========================
# НАСТРОЙКИ — читаем из переменных окружения
# ========================
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
ADMIN_ID        = int(os.environ["ADMIN_ID"])
SPREADSHEET_ID  = os.environ["SPREADSHEET_ID"]
CHECK_DELAY_HOURS = 12

# Колонки (считаем с 1)
COL_DATE   = 11  # K
COL_AMOUNT = 12  # L
COL_HASH   = 13  # M
COL_STATUS = 14  # N

# Google credentials читаем из переменной окружения как JSON-строку
GOOGLE_CREDS = json.loads(os.environ["GOOGLE_CREDENTIALS"])

# ========================
# ЛОГИРОВАНИЕ
# ========================
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========================
# GOOGLE SHEETS
# ========================
def get_spreadsheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(GOOGLE_CREDS, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)

def find_hash_in_all_sheets(tx_hash: str):
    """
    Ищет хеш в колонке M по всем листам с названием оканчивающимся на 'сбив'.
    Возвращает (sheet, row_index, row_data) или (None, None, None).
    """
    spreadsheet = get_spreadsheet()
    for sheet in spreadsheet.worksheets():
        logger.info(f"Проверяю лист: '{sheet.title}'")
        for i, row in enumerate(sheet.get_all_values()):
            for j, cell in enumerate(row):
                if cell.strip().lower() == tx_hash.lower():
                    logger.info(f"Найден на листе '{sheet.title}', строка {i + 1}, столбец {j + 1}")
                    return sheet, i + 1, row
    return None, None, None

def mark_as_processed(sheet, row_index: int):
    sheet.update_cell(row_index, COL_STATUS, "✅ обработано")

def check_hash(tx_hash: str) -> bool:
    try:
        sheet, row_index, _ = find_hash_in_all_sheets(tx_hash)
        if sheet and row_index:
            mark_as_processed(sheet, row_index)
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке хеша {tx_hash}: {e}")
        return False

# ========================
# ОЧЕРЕДЬ ОТЛОЖЕННЫХ ХЕШЕЙ
# ========================
pending_checks: dict = {}

# ========================
# ФОНОВЫЙ ЦИКЛ
# ========================
async def delayed_check_loop(application):
    while True:
        await asyncio.sleep(300)
        now = datetime.now()
        to_remove = []

        for tx_hash, data in list(pending_checks.items()):
            if now < data["check_at"]:
                continue
            logger.info(f"Отложенная проверка: {tx_hash}")
            found = check_hash(tx_hash)
            if not found:
                try:
                    await application.bot.send_message(
                        chat_id=data["user_id"],
                        text=(
                            f"⚠️ <b>Транзакция не найдена</b>\n\n"
                            f"Хеш: <code>{tx_hash}</code>\n\n"
                            f"Транзакция отсутствует в таблице после повторной проверки. "
                            f"Свяжитесь с администратором."
                        ),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить личку {data['user_id']}: {e}")
            to_remove.append(tx_hash)

        for tx_hash in to_remove:
            pending_checks.pop(tx_hash, None)

# ========================
# ОБРАБОТЧИК СООБЩЕНИЙ
# ========================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    # Ищем hex-строку от 63 до 66 символов (TRON хеши бывают разной длины)
    match = re.search(r'\b([0-9a-fA-F]{63,66})\b', update.message.text.strip())
    if not match:
        return

    tx_hash = match.group(1)
    # Убираем префикс 0x если есть
    if tx_hash.startswith(('0x', '0X')):
        tx_hash = tx_hash[2:]
    user_id = update.message.from_user.id
    logger.info(f"Хеш от юзера {user_id}: {tx_hash}")

    if not check_hash(tx_hash):
        logger.info(f"Не найден, очередь на {CHECK_DELAY_HOURS}ч")
        pending_checks[tx_hash] = {
            "user_id": user_id,
            "check_at": datetime.now() + timedelta(hours=CHECK_DELAY_HOURS)
        }

# ========================
# КОМАНДЫ АДМИНА
# ========================
async def recheck_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not pending_checks:
        await update.message.reply_text("📋 Очередь пуста.")
        return

    await update.message.reply_text(f"🔄 Проверяю {len(pending_checks)} хешей...")
    found_count, not_found_list = 0, []

    for tx_hash, data in list(pending_checks.items()):
        if check_hash(tx_hash):
            found_count += 1
            pending_checks.pop(tx_hash, None)
        else:
            not_found_list.append((tx_hash, data))

    await update.message.reply_text(
        f"✅ Готово!\n"
        f"Найдено и обработано: {found_count}\n"
        f"Не найдено (остались в очереди): {len(not_found_list)}"
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    if not pending_checks:
        await update.message.reply_text("📋 Очередь пуста.")
        return

    lines = [f"📋 В очереди: <b>{len(pending_checks)}</b> хешей\n"]
    for tx_hash, data in pending_checks.items():
        sec = max(0, int((data["check_at"] - datetime.now()).total_seconds()))
        lines.append(f"• <code>{tx_hash[:20]}...</code> — через {sec//3600}ч {(sec%3600)//60}м")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

# ========================
# ЗАПУСК
# ========================
async def post_init(application):
    asyncio.create_task(delayed_check_loop(application))

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("recheck", recheck_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Бот запущен!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
