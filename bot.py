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
TRON_WALLETS = [
    "TX6z5khTbArfSSV4b2yioUxhMytyWBNjC8",
    "TXZrknLXgXciqFK5seMiiTpH4DNwBydo9G",
]

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
# БАЗА ИСПОЛЬЗОВАННЫХ ХЕШЕЙ (защита от дублей)
# ========================
# Хранится в Google Sheets на отдельном листе "used_hashes"
# В памяти держим set для быстрой проверки
used_hashes_cache: set = set()

def get_used_hashes_sheet(spreadsheet):
    """Получает или создаёт лист used_hashes"""
    try:
        return spreadsheet.worksheet("used_hashes")
    except gspread.WorksheetNotFound:
        logger.info("Создаю лист used_hashes...")
        sheet = spreadsheet.add_worksheet(title="used_hashes", rows=10000, cols=3)
        sheet.append_row(["hash", "user_id", "timestamp"])
        return sheet

def load_used_hashes(spreadsheet) -> set:
    """Загружает все использованные хеши из листа в память"""
    try:
        sheet = get_used_hashes_sheet(spreadsheet)
        all_rows = sheet.get_all_values()
        hashes = set()
        for row in all_rows[1:]:  # пропускаем заголовок
            if row and row[0].strip():
                hashes.add(row[0].strip().lower())
        logger.info(f"Загружено {len(hashes)} использованных хешей")
        return hashes
    except Exception as e:
        logger.error(f"Ошибка загрузки used_hashes: {e}")
        return set()

def save_used_hash(spreadsheet, tx_hash: str, user_id: int):
    """Сохраняет хеш в лист used_hashes"""
    try:
        sheet = get_used_hashes_sheet(spreadsheet)
        sheet.append_row([tx_hash.lower(), str(user_id), datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
        used_hashes_cache.add(tx_hash.lower())
    except Exception as e:
        logger.error(f"Ошибка сохранения хеша в used_hashes: {e}")

def is_duplicate_hash(tx_hash: str) -> bool:
    """Проверяет дубль по кешу в памяти"""
    return tx_hash.lower() in used_hashes_cache

# ========================
# ЛИСТ ОТКЛОНЁННЫХ ХЕШЕЙ
# ========================
def get_rejected_hashes_sheet(spreadsheet):
    """Получает или создаёт лист rejected_hashes"""
    try:
        return spreadsheet.worksheet("rejected_hashes")
    except gspread.WorksheetNotFound:
        logger.info("Создаю лист rejected_hashes...")
        sheet = spreadsheet.add_worksheet(title="rejected_hashes", rows=10000, cols=4)
        sheet.append_row(["хеш", "причина", "username", "дата"])
        return sheet

def save_rejected_hash(spreadsheet, tx_hash: str, reason: str, user: object):
    """Сохраняет отклонённый хеш в лист rejected_hashes"""
    try:
        username = f"@{user.username}" if user.username else f"id:{user.id}"
        sheet = get_rejected_hashes_sheet(spreadsheet)
        sheet.append_row([
            tx_hash.lower(),
            reason,
            username,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])
        logger.info(f"Отклонённый хеш записан: {tx_hash[:20]}... | {reason} | {username}")
    except Exception as e:
        logger.error(f"Ошибка сохранения в rejected_hashes: {e}")

# ========================
# ОЧЕРЕДЬ В GOOGLE SHEETS (персистентность между рестартами)
# ========================
def get_pending_queue_sheet(spreadsheet):
    """Получает или создаёт лист pending_queue"""
    try:
        return spreadsheet.worksheet("pending_queue")
    except gspread.WorksheetNotFound:
        logger.info("Создаю лист pending_queue...")
        sheet = spreadsheet.add_worksheet(title="pending_queue", rows=1000, cols=4)
        sheet.append_row(["hash", "user_id", "username", "check_at"])
        return sheet

def save_pending_queue(spreadsheet):
    """Перезаписывает лист pending_queue актуальным состоянием очереди"""
    try:
        sheet = get_pending_queue_sheet(spreadsheet)
        sheet.clear()
        sheet.append_row(["hash", "user_id", "username", "check_at"])
        for tx_hash, data in pending_checks.items():
            user = data.get("user")
            username = f"@{user.username}" if user and user.username else f"id:{data['user_id']}"
            sheet.append_row([
                tx_hash,
                str(data["user_id"]),
                username,
                data["check_at"].strftime("%Y-%m-%d %H:%M:%S")
            ])
        logger.info(f"Очередь сохранена: {len(pending_checks)} хешей")
    except Exception as e:
        logger.error(f"Ошибка сохранения очереди: {e}")

def load_pending_queue(spreadsheet) -> dict:
    """Загружает очередь из Google Sheets при старте"""
    try:
        sheet = get_pending_queue_sheet(spreadsheet)
        all_rows = sheet.get_all_values()
        queue = {}
        now = datetime.now()
        for row in all_rows[1:]:
            if not row or not row[0].strip():
                continue
            tx_hash = row[0].strip()
            user_id = int(row[1]) if row[1].isdigit() else 0
            check_at_str = row[3] if len(row) > 3 else ""
            try:
                check_at = datetime.strptime(check_at_str, "%Y-%m-%d %H:%M:%S")
            except:
                check_at = now
            queue[tx_hash] = {
                "user_id": user_id,
                "check_at": check_at,
                "user": None
            }
        logger.info(f"Очередь восстановлена: {len(queue)} хешей")
        return queue
    except Exception as e:
        logger.error(f"Ошибка загрузки очереди: {e}")
        return {}


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

def load_all_sheets_data(spreadsheet) -> dict:
    """
    Загружает ВСЮ таблицу один раз в память.
    Возвращает dict: {sheet_title: {'sheet': obj, 'rows': [...]}}
    Делает паузы между листами чтобы не словить 429.
    """
    import time
    result = {}
    sheets = spreadsheet.worksheets()
    # Пропускаем служебный лист
    sheets = [s for s in sheets if s.title != "used_hashes"]

    for sheet in sheets:
        logger.info(f"Загружаю лист: '{sheet.title}'")
        for attempt in range(3):
            try:
                rows = sheet.get_all_values()
                result[sheet.title] = {'sheet': sheet, 'rows': rows}
                break
            except Exception as e:
                if '429' in str(e) or 'RATE_LIMIT' in str(e) or 'Quota' in str(e):
                    wait = 60 * (attempt + 1)
                    logger.warning(f"Лимит API на листе '{sheet.title}', жду {wait}с...")
                    time.sleep(wait)
                else:
                    logger.error(f"Ошибка загрузки листа '{sheet.title}': {e}")
                    break
        # Пауза между листами
        time.sleep(1)

    logger.info(f"Загружено листов: {len(result)}")
    return result

def find_hash_in_loaded_data(tx_hash: str, sheets_data: dict):
    """
    Ищет хеш в уже загруженных данных (без запросов к API).
    Возвращает (sheet, row_index, row_data) или (None, None, None).
    """
    for title, data in sheets_data.items():
        sheet = data['sheet']
        rows = data['rows']
        for i, row in enumerate(rows):
            for j, cell in enumerate(row):
                if cell.strip().lower() == tx_hash.lower():
                    logger.info(f"Найден на листе '{title}', строка {i + 1}, столбец {j + 1}")
                    return sheet, i + 1, row
    return None, None, None

def find_hash_in_all_sheets(tx_hash: str):
    """
    Обычный поиск хеша (для одиночных проверок из чата).
    Возвращает (sheet, row_index, row_data) или (None, None, None).
    """
    import time
    spreadsheet = get_spreadsheet()
    sheets = [s for s in spreadsheet.worksheets() if s.title != "used_hashes"]
    for sheet in sheets:
        logger.info(f"Проверяю лист: '{sheet.title}'")
        for attempt in range(3):
            try:
                all_rows = sheet.get_all_values()
                break
            except Exception as e:
                if '429' in str(e) or 'RATE_LIMIT' in str(e) or 'Quota' in str(e):
                    wait = 60 * (attempt + 1)
                    logger.warning(f"Лимит API, жду {wait}с...")
                    time.sleep(wait)
                else:
                    raise
        else:
            continue
        for i, row in enumerate(all_rows):
            for j, cell in enumerate(row):
                if cell.strip().lower() == tx_hash.lower():
                    logger.info(f"Найден на листе '{sheet.title}', строка {i + 1}, столбец {j + 1}")
                    return sheet, i + 1, row
    return None, None, None

def mark_as_processed(sheet, row_index: int):
    sheet.update_cell(row_index, COL_STATUS, "✅ обработано")

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
    if any(to_address.lower() == w.lower() for w in TRON_WALLETS):
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
        queue_changed = False

        for tx_hash, data in list(pending_checks.items()):
            if now < data["check_at"]:
                continue
            logger.info(f"Отложенная проверка: {tx_hash}")
            found = await check_hash_with_tron(tx_hash)
            if not found:
                global not_found_total
                not_found_total += 1
                try:
                    # Уведомляем админа, не юзера
                    user = data.get("user")
                    username = f"@{user.username}" if user and user.username else f"id:{data['user_id']}"
                    await application.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=(
                            f"⚠️ <b>Хеш не найден в таблице</b>\n\n"
                            f"Хеш: <code>{tx_hash}</code>\n"
                            f"Юзер: {username}\n\n"
                            f"Транзакция отсутствует после повторной проверки."
                        ),
                        parse_mode="HTML"
                    )
                    # Записываем в rejected_hashes
                    spreadsheet = get_spreadsheet()
                    if user:
                        save_rejected_hash(spreadsheet, tx_hash, "не найден в таблице", user)
                    else:
                        class FakeUser:
                            username = None
                            id = data["user_id"]
                        save_rejected_hash(spreadsheet, tx_hash, "не найден в таблице", FakeUser())
                except Exception as e:
                    logger.error(f"Ошибка уведомления админа: {e}")
            to_remove.append(tx_hash)
            queue_changed = True

        for tx_hash in to_remove:
            pending_checks.pop(tx_hash, None)

        # Сохраняем очередь в Sheets если были изменения
        if queue_changed:
            try:
                spreadsheet = get_spreadsheet()
                save_pending_queue(spreadsheet)
            except Exception as e:
                logger.error(f"Ошибка сохранения очереди: {e}")

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

    user = update.message.from_user

    # Проверка на дубль
    if is_duplicate_hash(tx_hash):
        logger.warning(f"Дубль хеша от юзера {user_id}: {tx_hash[:20]}...")
        try:
            await update.message.reply_text(
                f"⛔ <b>Этот хеш уже был использован ранее!</b>\n\n"
                f"Хеш: <code>{tx_hash}</code>\n\n"
                f"Повторное использование транзакции невозможно. "
                f"Свяжитесь с администратором.",
                parse_mode="HTML"
            )
            # Записываем в rejected_hashes
            spreadsheet = get_spreadsheet()
            save_rejected_hash(spreadsheet, tx_hash, "дубль хеша", user)
        except Exception as e:
            logger.error(f"Не удалось обработать дубль хеша: {e}")
        return

    found = await check_hash_with_tron(tx_hash)
    if found:
        # Сохраняем хеш в базу использованных
        try:
            spreadsheet = get_spreadsheet()
            save_used_hash(spreadsheet, tx_hash, user_id)
        except Exception as e:
            logger.error(f"Не удалось сохранить хеш в used_hashes: {e}")
    else:
        logger.info(f"Не найден, очередь на {CHECK_DELAY_HOURS}ч")
        pending_checks[tx_hash] = {
            "user_id": user_id,
            "check_at": datetime.now() + timedelta(hours=CHECK_DELAY_HOURS),
            "user": user
        }
        # Сохраняем очередь в Sheets чтобы пережить рестарт
        try:
            spreadsheet = get_spreadsheet()
            save_pending_queue(spreadsheet)
        except Exception as e:
            logger.error(f"Ошибка сохранения очереди: {e}")

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
            # Сохраняем в базу использованных
            try:
                spreadsheet = get_spreadsheet()
                save_used_hash(spreadsheet, tx_hash, data["user_id"])
            except Exception as e:
                logger.error(f"Ошибка сохранения хеша при recheck: {e}")
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
                try:
                    spreadsheet = get_spreadsheet()
                    save_used_hash(spreadsheet, tx_hash, data["user_id"])
                except Exception as e:
                    logger.error(f"Ошибка сохранения хеша при recheck: {e}")
            else:
                not_found_list.append((tx_hash, data))
        await update.message.reply_text(
            f"✅ Готово!\nНайдено и обработано: {found_count}\nНе найдено (остались в очереди): {len(not_found_list)}"
        )

    elif text == "📊 Статистика":
        text_msg = (
            f"📊 <b>Статистика</b>\n\n"
            f"❌ Не найдено за всё время: <b>{not_found_total}</b>\n"
            f"⏳ Сейчас в очереди: <b>{len(pending_checks)}</b>\n"
            f"🔒 Использованных хешей: <b>{len(used_hashes_cache)}</b>"
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
            f"⏳ Сейчас в очереди: <b>{total_pending}</b>\n"
            f"🔒 Использованных хешей: <b>{len(used_hashes_cache)}</b>"
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

    # Загружаем прогресс если был прерван
    progress_file = 'checkall_progress.json'
    try:
        with open(progress_file, 'r') as f:
            progress = json.load(f)
        start_index = progress.get('last_index', 0)
        found_count = progress.get('found_count', 0)
        not_found = progress.get('not_found', [])
        errors = progress.get('errors', [])
        if start_index > 0:
            await update.message.reply_text(f"⏩ Продолжаю с места остановки (хеш #{start_index + 1})...")
    except (FileNotFoundError, json.JSONDecodeError):
        start_index = 0
        found_count = 0
        not_found = []
        errors = []

    total = len(hashes)
    await update.message.reply_text(
        f"🔄 Загружаю таблицу в память...\n"
        f"Всего хешей для проверки: {total - start_index}"
    )

    # ===== КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: загружаем таблицу ОДИН РАЗ =====
    try:
        spreadsheet = get_spreadsheet()
        sheets_data = load_all_sheets_data(spreadsheet)
        await update.message.reply_text(
            f"✅ Таблица загружена ({len(sheets_data)} листов)\n"
            f"🔄 Начинаю проверку хешей..."
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка загрузки таблицы: {e}")
        return

    # Множество уже виденных хешей в этом запуске — защита от дублей внутри файла
    seen_in_run = set()

    for i, tx_hash in enumerate(hashes):
        if i < start_index:
            continue
        try:
            # Пропускаем дубли внутри самого файла hashes.txt
            if tx_hash.lower() in seen_in_run:
                logger.info(f"[{i+1}/{total}] Дубль внутри файла, пропускаю: {tx_hash[:20]}...")
                not_found.append(tx_hash)
            else:
                seen_in_run.add(tx_hash.lower())
                sheet, row_index, row = find_hash_in_loaded_data(tx_hash, sheets_data)
                if sheet and row_index:
                    status = row[13] if len(row) > 13 else ""
                    if not status:
                        mark_as_processed(sheet, row_index)
                        result = await verify_and_write_tron_data(sheet, row_index, tx_hash)
                        found_count += 1
                        logger.info(f"[{i+1}/{total}] Найден и обработан: {tx_hash[:20]}... — {result}")
                    else:
                        found_count += 1
                        logger.info(f"[{i+1}/{total}] Уже обработан: {tx_hash[:20]}...")
                    # Добавляем в used_hashes только если ещё не там
                    if not is_duplicate_hash(tx_hash):
                        save_used_hash(spreadsheet, tx_hash, ADMIN_ID)
                else:
                    # Не найден в таблице — сразу в rejected_hashes
                    not_found.append(tx_hash)
                    logger.info(f"[{i+1}/{total}] Не найден: {tx_hash[:20]}...")
                    class AdminUser:
                        username = "checkall"
                        id = ADMIN_ID
                    save_rejected_hash(spreadsheet, tx_hash, "не найден в таблице (/checkall)", AdminUser())
        except Exception as e:
            errors.append(tx_hash)
            logger.error(f"Ошибка при проверке {tx_hash[:20]}: {e}")

        # Сохраняем прогресс после каждого хеша
        progress = {
            'last_index': i + 1,
            'found_count': found_count,
            'not_found': not_found,
            'errors': errors
        }
        with open(progress_file, 'w') as pf:
            json.dump(progress, pf)

        # Минимальная пауза — только для записи в Sheets
        await asyncio.sleep(0.5)

        # Промежуточный отчёт каждые 100 хешей
        if (i + 1) % 100 == 0:
            await update.message.reply_text(
                f"⏳ Прогресс: {i+1}/{total}\n"
                f"✅ Найдено: {found_count}\n"
                f"❌ Не найдено: {len(not_found)}\n"
                f"⚠️ Ошибок: {len(errors)}"
            )

    # Удаляем файл прогресса — задача завершена
    if os.path.exists(progress_file):
        os.remove(progress_file)

    try:
        summary = (
            f"✅ <b>Проверка завершена!</b>\n\n"
            f"Всего хешей: {total}\n"
            f"Найдено и обработано: {found_count}\n"
            f"Не найдено в таблице: {len(not_found)}\n"
            f"Ошибок: {len(errors)}"
        )
        await update.message.reply_text(summary, parse_mode="HTML")

        # Отправляем список не найденных хешей
        if not_found:
            chunk_size = 50
            for idx in range(0, len(not_found), chunk_size):
                chunk = not_found[idx:idx + chunk_size]
                lines = [f"❌ <b>Не найдено ({idx+1}-{idx+len(chunk)} из {len(not_found)}):</b>\n"]
                for h in chunk:
                    lines.append(f"<code>{h}</code>")
                await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка отправки финального отчёта: {e}")

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
            if sheet.title == "used_hashes":
                continue
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
    global used_hashes_cache, pending_checks
    try:
        spreadsheet = get_spreadsheet()
        # Загружаем базу использованных хешей
        used_hashes_cache = load_used_hashes(spreadsheet)
        logger.info(f"База хешей загружена: {len(used_hashes_cache)} записей")
        # Восстанавливаем очередь после рестарта
        pending_checks = load_pending_queue(spreadsheet)
        if pending_checks:
            logger.info(f"Очередь восстановлена: {len(pending_checks)} хешей")
    except Exception as e:
        logger.error(f"Ошибка инициализации: {e}")
        used_hashes_cache = set()
        pending_checks = {}

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
    app.run_polling(drop_pending_updates=False)

if __name__ == "__main__":
    main()
