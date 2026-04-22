import logging
import asyncio
import re
import os
import json
import aiohttp
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes
import gspread
from google.oauth2.service_account import Credentials

# ========================
# НАСТРОЙКИ — читаем из переменных окружения
# ========================
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
ADMIN_ID        = int(os.environ["ADMIN_ID"])
SPREADSHEET_ID  = os.environ["SPREADSHEET_ID"]
CHECK_DELAY_HOURS = 1
TRON_API_KEY = "3a47f76f-f6aa-412c-9651-824df43c2d09"
TRON_WALLET = "TX6z5khTbArfSSV4b2yioUxhMytyWBNjC8"

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

async def check_hash_with_tron(tx_hash: str) -> bool:
    """Проверяет хеш в таблице + верифицирует через TRON API"""
    try:
        sheet, row_index, _ = find_hash_in_all_sheets(tx_hash)
        if sheet and row_index:
            mark_as_processed(sheet, row_index)
            result = await verify_and_write_tron_data(sheet, row_index, tx_hash)
            logger.info(f"TRON проверка хеша {tx_hash[:20]}: {result}")
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка при проверке хеша {tx_hash}: {e}")
        return False


# ========================
# TRON API
# ========================
async def get_tron_transaction(tx_hash: str) -> dict:
    """Получает данные транзакции из Tronscan API"""
    url = f"https://apilist.tronscanapi.com/api/transaction-info?hash={tx_hash}"
    headers = {"TRON-PRO-API-KEY": TRON_API_KEY}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception as e:
        logger.error(f"Ошибка TRON API: {e}")
    return {}

async def verify_and_write_tron_data(sheet, row_index: int, tx_hash: str) -> str:
    """
    Проверяет транзакцию через Tronscan.
    Записывает сумму в колонку O, результат проверки адреса в P.
    Возвращает строку с результатом для логов.
    """
    data = await get_tron_transaction(tx_hash)
    if not data or data.get("contractRet") == "FAILED":
        sheet.update_cell(row_index, 15, "⚠️ не найдена в сети")  # O
        sheet.update_cell(row_index, 16, "—")                      # P
        return "не найдена в сети"

    # Получаем сумму — ищем USDT трансфер
    amount = ""
    to_address = ""

    trc20_transfers = data.get("trc20TransferInfo", [])
    if trc20_transfers:
        transfer = trc20_transfers[0]
        raw_amount = transfer.get("amount_str", transfer.get("amount", "0"))
        decimals = int(transfer.get("decimals", 6))
        try:
            amount = str(round(int(raw_amount) / (10 ** decimals), 2))
        except:
            amount = str(raw_amount)
        to_address = transfer.get("to_address", "")
    else:
        # TRX транзакция
        raw_amount = data.get("amount", 0)
        try:
            amount = str(round(int(raw_amount) / 1_000_000, 2))
        except:
            amount = str(raw_amount)
        contract_data = data.get("contractData", {})
        to_address = contract_data.get("to_address", "")

    # Записываем сумму в O
    sheet.update_cell(row_index, 15, amount)

    # Проверяем адрес и записываем результат в P
    if to_address.lower() == TRON_WALLET.lower():
        sheet.update_cell(row_index, 16, "✅ Адрес верный")
        addr_result = "адрес верный"
    else:
        sheet.update_cell(row_index, 16, f"❌ Адрес неверный: {to_address}")
        addr_result = f"адрес неверный ({to_address})"

    return f"сумма: {amount}, {addr_result}"

# ========================
# ОЧЕРЕДЬ ОТЛОЖЕННЫХ ХЕШЕЙ
# ========================
pending_checks: dict = {}
not_found_total: int = 0  # счётчик всех не найденных за всё время

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
            found = await check_hash_with_tron(tx_hash)
            if not found:
                global not_found_total
                not_found_total += 1
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
    # Убираем все пробелы и переносы строк из текста перед поиском
    clean_text = re.sub(r'\s+', '', update.message.text.strip())
    # Ищем hex-строку от 63 до 66 символов
    match = re.search(r'([0-9a-fA-F]{63,66})', clean_text)
    if not match:
        return

    tx_hash = match.group(1)
    # Убираем префикс 0x если есть
    if tx_hash.startswith(('0x', '0X')):
        tx_hash = tx_hash[2:]
    user_id = update.message.from_user.id
    logger.info(f"Хеш от юзера {user_id}: {tx_hash}")

    found = await check_hash_with_tron(tx_hash)
    if not found:
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
        if await check_hash_with_tron(tx_hash):
            found_count += 1
            pending_checks.pop(tx_hash, None)
        else:
            not_found_list.append((tx_hash, data))

    await update.message.reply_text(
        f"✅ Готово!\n"
        f"Найдено и обработано: {found_count}\n"
        f"Не найдено (остались в очереди): {len(not_found_list)}"
    )





async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    keyboard = [
        [KeyboardButton("🔄 Перепроверить"), KeyboardButton("📊 Статистика")],
        [KeyboardButton("📋 Статус очереди"), KeyboardButton("🔍 Список листов")],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("✅ Панель управления активна!", reply_markup=reply_markup)


async def keyboard_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    text = update.message.text

    if text == "🔄 Перепроверить":
        if not pending_checks:
            await update.message.reply_text("📋 Очередь пуста — нечего перепроверять.")
            return
        await update.message.reply_text(f"🔄 Проверяю {len(pending_checks)} хешей...")
        found_count, not_found_list = 0, []
        for tx_hash, data in list(pending_checks.items()):
            if await check_hash_with_tron(tx_hash):
                found_count += 1
                pending_checks.pop(tx_hash, None)
            else:
                not_found_list.append((tx_hash, data))
        await update.message.reply_text(
            f"✅ Готово!\nНайдено и обработано: {found_count}\nНе найдено (остались в очереди): {len(not_found_list)}"
        )

    elif text == "📊 Статистика":
        text_msg = (
            f"📊 <b>Статистика</b>\n\n"
            f"❌ Не найдено за всё время: <b>{not_found_total}</b>\n"
            f"⏳ Сейчас в очереди: <b>{len(pending_checks)}</b>"
        )
        await update.message.reply_text(text_msg, parse_mode="HTML")

    elif text == "📋 Статус очереди":
        if not pending_checks:
            await update.message.reply_text("📋 Очередь пуста.")
            return
        lines = [f"📋 В очереди: <b>{len(pending_checks)}</b> хешей\n"]
        for tx_hash, data in pending_checks.items():
            sec = max(0, int((data["check_at"] - datetime.now()).total_seconds()))
            lines.append(f"• <code>{tx_hash[:20]}...</code> — через {sec//3600}ч {(sec%3600)//60}м")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    elif text == "🔍 Список листов":
        try:
            spreadsheet = get_spreadsheet()
            sheets = spreadsheet.worksheets()
            lines = [f"📊 Листов: <b>{len(sheets)}</b>\n"]
            for sheet in sheets:
                rows = sheet.get_all_values()
                lines.append(f"• <b>{sheet.title}</b> — {len(rows)} строк")
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}")

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    keyboard = [
        [
            InlineKeyboardButton("🔄 Перепроверить очередь", callback_data="recheck"),
        ],
        [
            InlineKeyboardButton("📋 Статус очереди", callback_data="status"),
            InlineKeyboardButton("📊 Статистика", callback_data="stats"),
        ],
        [
            InlineKeyboardButton("🔍 Список листов", callback_data="debug"),
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("🤖 Панель управления ботом:", reply_markup=reply_markup)


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID:
        await query.answer("Нет доступа")
        return

    await query.answer()
    action = query.data

    if action == "recheck":
        if not pending_checks:
            await query.edit_message_text("📋 Очередь пуста — нечего перепроверять.")
            return
        await query.edit_message_text(f"🔄 Проверяю {len(pending_checks)} хешей...")
        found_count, not_found_list = 0, []
        for tx_hash, data in list(pending_checks.items()):
            if await check_hash_with_tron(tx_hash):
                found_count += 1
                pending_checks.pop(tx_hash, None)
            else:
                not_found_list.append((tx_hash, data))
        await query.edit_message_text(
            f"✅ Готово!\nНайдено и обработано: {found_count}\nНе найдено (остались в очереди): {len(not_found_list)}"
        )

    elif action == "status":
        if not pending_checks:
            await query.edit_message_text("📋 Очередь пуста.")
            return
        lines = [f"📋 В очереди: <b>{len(pending_checks)}</b> хешей\n"]
        for tx_hash, data in pending_checks.items():
            sec = max(0, int((data["check_at"] - datetime.now()).total_seconds()))
            lines.append(f"• <code>{tx_hash[:20]}...</code> — через {sec//3600}ч {(sec%3600)//60}м")
        await query.edit_message_text("\n".join(lines), parse_mode="HTML")

    elif action == "stats":
        total_pending = len(pending_checks)
        text = (
            f"📊 <b>Статистика</b>\n\n"
            f"❌ Не найдено за всё время: <b>{not_found_total}</b>\n"
            f"⏳ Сейчас в очереди: <b>{total_pending}</b>"
        )
        await query.edit_message_text(text, parse_mode="HTML")

    elif action == "debug":
        try:
            spreadsheet = get_spreadsheet()
            sheets = spreadsheet.worksheets()
            lines = [f"📊 Листов: <b>{len(sheets)}</b>\n"]
            for sheet in sheets:
                rows = sheet.get_all_values()
                lines.append(f"• <b>{sheet.title}</b> — {len(rows)} строк")
            await query.edit_message_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")


async def checkall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/checkall — проверить список хешей из файла hashes.txt"""
    if update.message.from_user.id != ADMIN_ID:
        return

    try:
        with open('hashes.txt', 'r') as f:
            hashes = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        await update.message.reply_text("❌ Файл hashes.txt не найден.")
        return

    total = len(hashes)
    await update.message.reply_text(f"🔄 Начинаю проверку {total} хешей из истории чата...\nЭто займёт несколько минут.")

    found_count = 0
    not_found = []
    errors = []

    for i, tx_hash in enumerate(hashes):
        try:
            sheet, row_index, _ = find_hash_in_all_sheets(tx_hash)
            if sheet and row_index:
                row = sheet.row_values(row_index)
                status = row[13] if len(row) > 13 else ""
                if not status:
                    mark_as_processed(sheet, row_index)
                    result = await verify_and_write_tron_data(sheet, row_index, tx_hash)
                    found_count += 1
                    logger.info(f"[{i+1}/{total}] Найден и обработан: {tx_hash[:20]}... — {result}")
                else:
                    found_count += 1
                    logger.info(f"[{i+1}/{total}] Уже обработан: {tx_hash[:20]}...")
            else:
                not_found.append(tx_hash)
        except Exception as e:
            errors.append(tx_hash)
            logger.error(f"Ошибка при проверке {tx_hash[:20]}: {e}")

        # Пауза между запросами — защита от лимитов API
        await asyncio.sleep(1.5)

        # Отправляем промежуточный отчёт каждые 100 хешей
        if (i + 1) % 100 == 0:
            await update.message.reply_text(
                f"⏳ Прогресс: {i+1}/{total}\n"
                f"✅ Найдено: {found_count}\n"
                f"❌ Не найдено: {len(not_found)}"
            )

    await update.message.reply_text(
        f"✅ <b>Проверка завершена!</b>\n\n"
        f"Всего хешей: {total}\n"
        f"Найдено и обработано: {found_count}\n"
        f"Не найдено в таблице: {len(not_found)}\n"
        f"Ошибок: {len(errors)}",
        parse_mode="HTML"
    )

async def find_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /find <хеш>")
        return
    tx_hash = args[0].strip()
    await update.message.reply_text(f"🔍 Ищу: {tx_hash[:20]}...\nДлина: {len(tx_hash)} символов")
    try:
        spreadsheet = get_spreadsheet()
        for sheet in spreadsheet.worksheets():
            rows = sheet.get_all_values()
            for i, row in enumerate(rows):
                for j, cell in enumerate(row):
                    if cell.strip().lower() == tx_hash.lower():
                        await update.message.reply_text(
                            f"✅ Найден!\nЛист: {sheet.title}\nСтрока: {i+1}, Столбец: {j+1}\nЗначение ячейки: <code>{cell}</code>",
                            parse_mode="HTML"
                        )
                        return
        await update.message.reply_text("❌ Не найден ни в одном листе")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    try:
        spreadsheet = get_spreadsheet()
        sheets = spreadsheet.worksheets()
        lines = [f"📊 Таблица открыта. Листов: {len(sheets)}\n"]
        for sheet in sheets:
            rows = sheet.get_all_values()
            lines.append(f"• <b>{sheet.title}</b> — {len(rows)} строк")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

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
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("menu", menu_command))
    app.add_handler(MessageHandler(filters.TEXT & filters.Regex("^(🔄 Перепроверить|📊 Статистика|📋 Статус очереди|🔍 Список листов)$") & ~filters.COMMAND, keyboard_handler))
    app.add_handler(CommandHandler("checkall", checkall_command))
    app.add_handler(CommandHandler("find", find_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(CommandHandler("debug", debug_command))
    app.add_handler(CommandHandler("recheck", recheck_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info("Бот запущен!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
