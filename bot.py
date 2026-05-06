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
# НАСТРОЙКИ
# ========================
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
ADMIN_ID          = int(os.environ["ADMIN_ID"])
SPREADSHEET_ID    = os.environ["SPREADSHEET_ID"]
TRON_API_KEY      = os.environ.get("TRON_API_KEY", "3a47f76f-f6aa-412c-9651-824df43c2d09")
CHECK_DELAY_HOURS = 1

TRON_WALLETS = [
    "TX6z5khTbArfSSV4b2yioUxhMytyWBNjC8",
    "TXZrknLXgXciqFK5seMiiTpH4DNwBydo9G",
]

GOOGLE_CREDS = json.loads(os.environ["GOOGLE_CREDENTIALS"])

# Колонки в основных листах (считаем с 1)
COL_HASH   = 13  # M
COL_STATUS = 14  # N
COL_AMOUNT = 15  # O
COL_ADDR   = 16  # P

# Служебные листы (префикс _ означает технический лист)
SYSTEM_SHEETS = {
    "_использованные_хеши",
    "_очередь",
    "_не_найденные",
    "_дубли",
    "_ошибки",
    "_хеши_для_проверки",
    "_прогресс_проверки",
}

# Адаптивная пауза для /checkall
MIN_PAUSE = 1.0
MAX_PAUSE = 10.0

# ========================
# ЛОГИРОВАНИЕ
# ========================
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========================
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ========================
used_hashes_cache: set = set()
pending_checks: dict   = {}
processing_hashes: set = set()
not_found_total: int   = 0
_spreadsheet_cache     = None

# ========================
# GOOGLE SHEETS — СОЕДИНЕНИЕ
# ========================
def get_spreadsheet():
    global _spreadsheet_cache
    if _spreadsheet_cache is None:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(GOOGLE_CREDS, scopes=scopes)
        client = gspread.authorize(creds)
        _spreadsheet_cache = client.open_by_key(SPREADSHEET_ID)
        logger.info("Соединение с Google Sheets установлено")
    return _spreadsheet_cache

def reset_spreadsheet_cache():
    global _spreadsheet_cache
    _spreadsheet_cache = None

def get_or_create_sheet(spreadsheet, title: str, rows: int = 10000, cols: int = 5):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        logger.info(f"Создаю лист '{title}'...")
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

# ========================
# RETRY WRAPPER ДЛЯ ЗАПИСИ
# ========================
async def sheets_write_with_retry(func, *args, max_attempts: int = 5, **kwargs):
    """
    Обёртка для любой операции записи в Sheets.
    При 429 ошибке ждёт и повторяет.
    """
    for attempt in range(max_attempts):
        try:
            result = await asyncio.get_event_loop().run_in_executor(None, lambda: func(*args, **kwargs))
            return result
        except Exception as e:
            if "429" in str(e) or "RATE_LIMIT" in str(e) or "Quota" in str(e):
                wait = 60 * (attempt + 1)
                logger.warning(f"Write 429, жду {wait}с (попытка {attempt+1}/{max_attempts})...")
                await asyncio.sleep(wait)
            else:
                logger.error(f"Ошибка записи: {e}")
                raise
    logger.error(f"Превышено количество попыток записи")
    return None

# ========================
# USED HASHES
# Структура: хеш | user_id | дата_время
# ========================
def load_used_hashes(spreadsheet) -> set:
    sheet = get_or_create_sheet(spreadsheet, "_использованные_хеши", cols=3)
    all_rows = sheet.get_all_values()
    if not all_rows:
        # Лист пустой — добавляем заголовок
        sheet.append_row(["хеш", "user_id", "дата_время"])
        return set()
    hashes = set()
    # Пропускаем первую строку если это заголовок (любой вариант)
    start = 1 if all_rows[0] and not re.match(r"[0-9a-fA-F]{32,}", all_rows[0][0]) else 0
    for row in all_rows[start:]:
        if row and row[0].strip():
            hashes.add(row[0].strip().lower())
    logger.info(f"Загружено {len(hashes)} использованных хешей")
    return hashes

async def save_used_hash(spreadsheet, tx_hash: str, user_id: int):
    if tx_hash.lower() in used_hashes_cache:
        return
    try:
        sheet = get_or_create_sheet(spreadsheet, "_использованные_хеши", cols=3)
        await sheets_write_with_retry(
            sheet.append_row,
            [tx_hash.lower(), str(user_id), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        )
        used_hashes_cache.add(tx_hash.lower())
    except Exception as e:
        logger.error(f"Ошибка сохранения в использованные_хеши: {e}")

def is_duplicate_hash(tx_hash: str) -> bool:
    return tx_hash.lower() in used_hashes_cache

def is_in_queue(tx_hash: str) -> bool:
    return tx_hash in pending_checks

# ========================
# ТЕХНИЧЕСКИЕ ЛИСТЫ — не найденные, дубли, ошибки
# ========================
async def _write_to_sheet(spreadsheet, sheet_name: str, headers: list, row: list):
    """Универсальная запись в технический лист с созданием заголовка."""
    try:
        sheet = get_or_create_sheet(spreadsheet, sheet_name, cols=len(headers))
        all_rows = sheet.get_all_values()
        if not all_rows:
            await sheets_write_with_retry(sheet.append_row, headers)
        await sheets_write_with_retry(sheet.append_row, row)
    except Exception as e:
        logger.error(f"Ошибка записи в {sheet_name}: {e}")

async def save_not_found(spreadsheet, tx_hash: str, user, reason: str = "не найден в таблице"):
    """Хеш не найден в таблице."""
    username = f"@{user.username}" if getattr(user, "username", None) else f"id:{getattr(user, 'id', '?')}"
    await _write_to_sheet(
        spreadsheet, "_не_найденные",
        ["хеш", "причина", "пользователь", "дата_время"],
        [tx_hash.lower(), reason, username, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    )
    logger.info(f"_не_найденные: {tx_hash[:20]}... | {reason} | {username}")

async def save_duplicate(spreadsheet, tx_hash: str, user):
    """Хеш уже использовался — дубль."""
    username = f"@{user.username}" if getattr(user, "username", None) else f"id:{getattr(user, 'id', '?')}"
    await _write_to_sheet(
        spreadsheet, "_дубли",
        ["хеш", "пользователь", "дата_время"],
        [tx_hash.lower(), username, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    )
    logger.info(f"_дубли: {tx_hash[:20]}... | {username}")

async def save_error(spreadsheet, tx_hash: str, reason: str, user):
    """Ошибка при обработке хеша (API, FAILED, write error)."""
    username = f"@{user.username}" if getattr(user, "username", None) else f"id:{getattr(user, 'id', '?')}"
    await _write_to_sheet(
        spreadsheet, "_ошибки",
        ["хеш", "причина", "пользователь", "дата_время"],
        [tx_hash.lower(), reason, username, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    )
    logger.info(f"_ошибки: {tx_hash[:20]}... | {reason} | {username}")

# ========================
# PENDING QUEUE
# Структура: хеш | user_id | пользователь | проверить_в
# ========================
def load_pending_queue(spreadsheet) -> dict:
    try:
        sheet = get_or_create_sheet(spreadsheet, "_очередь", rows=1000, cols=4)
        all_rows = sheet.get_all_values()
        if not all_rows or all_rows[0] != ["хеш", "user_id", "пользователь", "проверить_в"]:
            sheet.clear()
            sheet.append_row(["хеш", "user_id", "пользователь", "проверить_в"])
            return {}
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
            except Exception:
                check_at = now
            queue[tx_hash] = {"user_id": user_id, "check_at": check_at, "user": None}
        logger.info(f"Очередь восстановлена: {len(queue)} хешей")
        return queue
    except Exception as e:
        logger.error(f"Ошибка загрузки очереди: {e}")
        return {}

async def save_pending_queue(spreadsheet):
    try:
        sheet = get_or_create_sheet(spreadsheet, "_очередь", rows=1000, cols=4)
        rows = [["хеш", "user_id", "пользователь", "проверить_в"]]
        for tx_hash, data in pending_checks.items():
            user = data.get("user")
            username = f"@{user.username}" if user and getattr(user, "username", None) else f"id:{data['user_id']}"
            rows.append([
                tx_hash,
                str(data["user_id"]),
                username,
                data["check_at"].strftime("%Y-%m-%d %H:%M:%S")
            ])
        await sheets_write_with_retry(sheet.clear)
        if len(rows) > 1:
            await sheets_write_with_retry(sheet.update, f"A1:D{len(rows)}", rows)
        else:
            await sheets_write_with_retry(sheet.append_row, rows[0])
        logger.info(f"Очередь сохранена: {len(pending_checks)} хешей")
    except Exception as e:
        logger.error(f"Ошибка сохранения очереди: {e}")

# ========================
# HASHES TO CHECK
# Структура: хеш
# ========================
def load_hashes_to_check(spreadsheet) -> list:
    try:
        sheet = get_or_create_sheet(spreadsheet, "_хеши_для_проверки", rows=5000, cols=1)
        all_rows = sheet.get_all_values()
        if not all_rows:
            sheet.append_row(["хеш"])
            return []
        start = 1 if all_rows[0] == ["хеш"] else 0
        hashes = [row[0].strip() for row in all_rows[start:] if row and row[0].strip()]
        logger.info(f"Загружено {len(hashes)} хешей для checkall")
        return hashes
    except Exception as e:
        logger.error(f"Ошибка загрузки хешей_для_проверки: {e}")
        return []

# ========================
# CHECKALL PROGRESS
# Структура: последний_индекс | найдено | не_найдено | ошибки | дублей | пауза
# ========================
def save_checkall_progress(spreadsheet, last_index: int, found_count: int,
                            not_found: list, errors: list, duplicates: list, current_pause: float):
    try:
        sheet = get_or_create_sheet(spreadsheet, "_прогресс_проверки", rows=3, cols=6)
        sheet.clear()
        sheet.update("A1:F1", [["последний_индекс", "найдено", "не_найдено_json", "ошибки_json", "дублей_json", "пауза"]])
        sheet.update("A2:F2", [[
            last_index,
            found_count,
            json.dumps(not_found),
            json.dumps(errors),
            json.dumps(duplicates),
            current_pause
        ]])
    except Exception as e:
        logger.error(f"Ошибка сохранения прогресса: {e}")

def load_checkall_progress(spreadsheet) -> dict | None:
    try:
        sheet = get_or_create_sheet(spreadsheet, "_прогресс_проверки", rows=3, cols=6)
        all_rows = sheet.get_all_values()
        if len(all_rows) < 2 or not all_rows[1][0]:
            return None
        row = all_rows[1]
        return {
            "last_index":  int(row[0]) if row[0] else 0,
            "found_count": int(row[1]) if row[1] else 0,
            "not_found":   json.loads(row[2]) if row[2] else [],
            "errors":      json.loads(row[3]) if row[3] else [],
            "duplicates":  json.loads(row[4]) if row[4] else [],
            "current_pause": float(row[5]) if len(row) > 5 and row[5] else MIN_PAUSE,
        }
    except Exception:
        return None

def clear_checkall_progress(spreadsheet):
    try:
        sheet = get_or_create_sheet(spreadsheet, "_прогресс_проверки", rows=3, cols=6)
        sheet.clear()
    except Exception as e:
        logger.error(f"Ошибка очистки прогресса: {e}")

# ========================
# GOOGLE SHEETS — ПОИСК
# ========================
async def load_all_sheets_data(spreadsheet) -> dict:
    result = {}
    sheets = [s for s in spreadsheet.worksheets() if s.title not in SYSTEM_SHEETS]
    for sheet in sheets:
        logger.info(f"Загружаю лист: '{sheet.title}'")
        for attempt in range(3):
            try:
                rows = await asyncio.get_event_loop().run_in_executor(None, sheet.get_all_values)
                result[sheet.title] = {"sheet": sheet, "rows": rows}
                break
            except Exception as e:
                if "429" in str(e) or "RATE_LIMIT" in str(e) or "Quota" in str(e):
                    wait = 60 * (attempt + 1)
                    logger.warning(f"Read 429 на листе '{sheet.title}', жду {wait}с...")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Ошибка загрузки листа '{sheet.title}': {e}")
                    break
        await asyncio.sleep(1)
    logger.info(f"Загружено листов: {len(result)}")
    return result

def find_hash_in_loaded_data(tx_hash: str, sheets_data: dict):
    for title, data in sheets_data.items():
        for i, row in enumerate(data["rows"]):
            if len(row) > 12 and row[12].strip().lower() == tx_hash.lower():
                logger.info(f"Найден на листе '{title}', строка {i + 1}")
                return data["sheet"], i + 1, row
    return None, None, None

async def find_hash_in_all_sheets(tx_hash: str):
    spreadsheet = get_spreadsheet()
    sheets = [s for s in spreadsheet.worksheets() if s.title not in SYSTEM_SHEETS]
    for sheet in sheets:
        for attempt in range(3):
            try:
                all_rows = await asyncio.get_event_loop().run_in_executor(None, sheet.get_all_values)
                break
            except Exception as e:
                if "429" in str(e) or "RATE_LIMIT" in str(e) or "Quota" in str(e):
                    wait = 60 * (attempt + 1)
                    logger.warning(f"Read 429, жду {wait}с...")
                    await asyncio.sleep(wait)
                else:
                    raise
        else:
            continue
        for i, row in enumerate(all_rows):
            if len(row) > 12 and row[12].strip().lower() == tx_hash.lower():
                logger.info(f"Найден на листе '{sheet.title}', строка {i + 1}")
                return sheet, i + 1, row
    return None, None, None

# ========================
# BATCH ЗАПИСЬ В ОСНОВНУЮ ТАБЛИЦУ
# ========================
async def mark_and_write_batch(sheet, row_index: int, status: str, amount: str, addr_result: str):
    """
    Записывает статус, сумму и адрес ОДНИМ batch запросом вместо 3 отдельных.
    """
    try:
        updates = [
            {"range": f"N{row_index}", "values": [[status]]},
            {"range": f"O{row_index}", "values": [[amount]]},
            {"range": f"P{row_index}", "values": [[addr_result]]},
        ]
        await sheets_write_with_retry(sheet.batch_update, updates)
    except Exception as e:
        logger.error(f"Ошибка batch записи строки {row_index}: {e}")
        raise

# ========================
# TRON API
# ========================
async def get_tron_transaction(tx_hash: str) -> dict:
    url = f"https://apilist.tronscanapi.com/api/transaction-info?hash={tx_hash}"
    headers = {"TRON-PRO-API-KEY": TRON_API_KEY}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    try:
                        return await resp.json()
                    except Exception:
                        logger.error(f"Tronscan невалидный JSON для {tx_hash[:20]}")
                        return {}
                else:
                    logger.error(f"Tronscan HTTP {resp.status} для {tx_hash[:20]}")
    except asyncio.TimeoutError:
        logger.error(f"Tronscan timeout для {tx_hash[:20]}")
    except Exception as e:
        logger.error(f"Ошибка TRON API: {e}")
    return {}

async def verify_and_write_tron_data(sheet, row_index: int, tx_hash: str) -> str:
    """Проверяет транзакцию и записывает результат ОДНИМ batch запросом."""
    data = await get_tron_transaction(tx_hash)

    if not data:
        await mark_and_write_batch(sheet, row_index, "✅ обработано", "⚠️ API недоступен", "—")
        return "API недоступен"

    if data.get("contractRet") == "FAILED":
        await mark_and_write_batch(sheet, row_index, "✅ обработано", "⚠️ транзакция FAILED", "—")
        return "транзакция FAILED"

    if not data.get("trc20TransferInfo") and not data.get("contractData"):
        await mark_and_write_batch(sheet, row_index, "✅ обработано", "⚠️ нет данных", "—")
        return "нет данных транзакции"

    amount = ""
    to_address = ""

    trc20_transfers = data.get("trc20TransferInfo", [])
    if trc20_transfers:
        transfer = trc20_transfers[0]
        raw_amount = transfer.get("amount_str", transfer.get("amount", "0"))
        decimals = int(transfer.get("decimals", 6))
        try:
            amount = str(round(int(raw_amount) / (10 ** decimals), 2))
        except Exception:
            amount = str(raw_amount)
        to_address = transfer.get("to_address", "")
    else:
        raw_amount = data.get("amount", 0)
        try:
            amount = str(round(int(raw_amount) / 1_000_000, 2))
        except Exception:
            amount = str(raw_amount)
        contract_data = data.get("contractData", {})
        to_address = contract_data.get("to_address", "")

    if any(to_address.lower() == w.lower() for w in TRON_WALLETS):
        addr_result = "✅ Адрес верный"
    else:
        addr_result = f"❌ Адрес неверный: {to_address}"

    await mark_and_write_batch(sheet, row_index, "✅ обработано", amount, addr_result)
    return f"сумма: {amount}, {addr_result}"

# ========================
# ОСНОВНАЯ ЛОГИКА ПРОВЕРКИ
# ========================
async def check_hash_with_tron(tx_hash: str) -> bool:
    try:
        sheet, row_index, _ = await find_hash_in_all_sheets(tx_hash)
        if sheet and row_index:
            result = await verify_and_write_tron_data(sheet, row_index, tx_hash)
            logger.info(f"TRON проверка {tx_hash[:20]}: {result}")
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки хеша {tx_hash[:20]}: {e}")
        return False

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
            logger.info(f"Отложенная проверка: {tx_hash[:20]}")
            found = await check_hash_with_tron(tx_hash)

            if found:
                try:
                    spreadsheet = get_spreadsheet()
                    await save_used_hash(spreadsheet, tx_hash, data["user_id"])
                except Exception as e:
                    logger.error(f"Ошибка сохранения в использованные_хеши: {e}")
            else:
                global not_found_total
                not_found_total += 1
                try:
                    user = data.get("user")
                    username = f"@{user.username}" if user and getattr(user, "username", None) else f"id:{data['user_id']}"
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
                    spreadsheet = get_spreadsheet()
                    if user:
                        await save_not_found(spreadsheet, tx_hash, user)
                    else:
                        class FakeUser:
                            username = None
                            id = data["user_id"]
                        await save_not_found(spreadsheet, tx_hash, FakeUser())
                except Exception as e:
                    logger.error(f"Ошибка уведомления админа: {e}")

            to_remove.append(tx_hash)
            queue_changed = True

        for tx_hash in to_remove:
            pending_checks.pop(tx_hash, None)
            processing_hashes.discard(tx_hash.lower())

        if queue_changed:
            try:
                spreadsheet = get_spreadsheet()
                await save_pending_queue(spreadsheet)
            except Exception as e:
                logger.error(f"Ошибка сохранения очереди: {e}")

# ========================
# ОБРАБОТЧИК СООБЩЕНИЙ
# ========================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    clean_text = re.sub(r"\s+", "", update.message.text.strip())
    match = re.search(r"([0-9a-fA-F]{63,66})", clean_text)
    if not match:
        return

    tx_hash = match.group(1)
    if tx_hash.startswith(("0x", "0X")):
        tx_hash = tx_hash[2:]

    user = update.message.from_user
    user_id = user.id
    logger.info(f"Хеш от юзера {user_id}: {tx_hash[:20]}")

    if is_duplicate_hash(tx_hash):
        logger.warning(f"Дубль от {user_id}: {tx_hash[:20]}")
        try:
            await update.message.reply_text(
                f"⛔ <b>Этот хеш уже был использован ранее!</b>\n\n"
                f"Хеш: <code>{tx_hash}</code>\n\n"
                f"Повторное использование транзакции невозможно. "
                f"Свяжитесь с администратором.",
                parse_mode="HTML"
            )
            spreadsheet = get_spreadsheet()
            await save_duplicate(spreadsheet, tx_hash, user)
        except Exception as e:
            logger.error(f"Ошибка обработки дубля: {e}")
        return

    if is_in_queue(tx_hash):
        await update.message.reply_text(
            f"⏳ <b>Хеш уже находится в очереди на проверку</b>\n\n"
            f"Хеш: <code>{tx_hash}</code>\n\n"
            f"Повторная проверка будет выполнена автоматически.",
            parse_mode="HTML"
        )
        return

    if tx_hash.lower() in processing_hashes:
        return

    processing_hashes.add(tx_hash.lower())
    try:
        found = await check_hash_with_tron(tx_hash)
        if found:
            try:
                spreadsheet = get_spreadsheet()
                await save_used_hash(spreadsheet, tx_hash, user_id)
            except Exception as e:
                logger.error(f"Ошибка сохранения в использованные_хеши: {e}")
        else:
            pending_checks[tx_hash] = {
                "user_id": user_id,
                "check_at": datetime.now() + timedelta(hours=CHECK_DELAY_HOURS),
                "user": user
            }
            try:
                spreadsheet = get_spreadsheet()
                await save_pending_queue(spreadsheet)
            except Exception as e:
                logger.error(f"Ошибка сохранения очереди: {e}")
    finally:
        if tx_hash not in pending_checks:
            processing_hashes.discard(tx_hash.lower())

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
            processing_hashes.discard(tx_hash.lower())
            try:
                spreadsheet = get_spreadsheet()
                await save_used_hash(spreadsheet, tx_hash, data["user_id"])
            except Exception as e:
                logger.error(f"Ошибка сохранения при recheck: {e}")
        else:
            not_found_list.append(tx_hash)

    try:
        spreadsheet = get_spreadsheet()
        await save_pending_queue(spreadsheet)
    except Exception as e:
        logger.error(f"Ошибка сохранения очереди после recheck: {e}")

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
    await update.message.reply_text(
        "✅ Панель управления активна!",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

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
                processing_hashes.discard(tx_hash.lower())
                try:
                    spreadsheet = get_spreadsheet()
                    await save_used_hash(spreadsheet, tx_hash, data["user_id"])
                except Exception as e:
                    logger.error(f"Ошибка сохранения при recheck: {e}")
            else:
                not_found_list.append(tx_hash)
        try:
            spreadsheet = get_spreadsheet()
            await save_pending_queue(spreadsheet)
        except Exception as e:
            logger.error(f"Ошибка сохранения очереди: {e}")
        await update.message.reply_text(
            f"✅ Готово!\nНайдено: {found_count}\nНе найдено: {len(not_found_list)}"
        )

    elif text == "📊 Статистика":
        await update.message.reply_text(
            f"📊 <b>Статистика</b>\n\n"
            f"❌ Не найдено за всё время: <b>{not_found_total}</b>\n"
            f"⏳ Сейчас в очереди: <b>{len(pending_checks)}</b>\n"
            f"🔒 Использованных хешей: <b>{len(used_hashes_cache)}</b>",
            parse_mode="HTML"
        )

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
                tag = " ⚙️" if sheet.title in SYSTEM_SHEETS else ""
                lines.append(f"• <b>{sheet.title}</b>{tag} — {len(rows)} строк")
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}")

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return
    keyboard = [
        [InlineKeyboardButton("🔄 Перепроверить очередь", callback_data="recheck")],
        [
            InlineKeyboardButton("📋 Статус очереди", callback_data="status"),
            InlineKeyboardButton("📊 Статистика", callback_data="stats"),
        ],
        [InlineKeyboardButton("🔍 Список листов", callback_data="debug")],
    ]
    await update.message.reply_text(
        "🤖 Панель управления ботом:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID:
        await query.answer("Нет доступа")
        return
    await query.answer()
    action = query.data

    if action == "recheck":
        if not pending_checks:
            await query.edit_message_text("📋 Очередь пуста.")
            return
        await query.edit_message_text(f"🔄 Проверяю {len(pending_checks)} хешей...")
        found_count, not_found_list = 0, []
        for tx_hash, data in list(pending_checks.items()):
            if await check_hash_with_tron(tx_hash):
                found_count += 1
                pending_checks.pop(tx_hash, None)
                processing_hashes.discard(tx_hash.lower())
                try:
                    spreadsheet = get_spreadsheet()
                    await save_used_hash(spreadsheet, tx_hash, data["user_id"])
                except Exception as e:
                    logger.error(f"Ошибка сохранения при recheck: {e}")
            else:
                not_found_list.append(tx_hash)
        try:
            spreadsheet = get_spreadsheet()
            await save_pending_queue(spreadsheet)
        except Exception as e:
            logger.error(f"Ошибка сохранения очереди: {e}")
        await query.edit_message_text(
            f"✅ Готово!\nНайдено: {found_count}\nНе найдено: {len(not_found_list)}"
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
        await query.edit_message_text(
            f"📊 <b>Статистика</b>\n\n"
            f"❌ Не найдено за всё время: <b>{not_found_total}</b>\n"
            f"⏳ Сейчас в очереди: <b>{len(pending_checks)}</b>\n"
            f"🔒 Использованных хешей: <b>{len(used_hashes_cache)}</b>",
            parse_mode="HTML"
        )

    elif action == "debug":
        try:
            spreadsheet = get_spreadsheet()
            sheets = spreadsheet.worksheets()
            lines = [f"📊 Листов: <b>{len(sheets)}</b>\n"]
            for sheet in sheets:
                rows = sheet.get_all_values()
                tag = " ⚙️" if sheet.title in SYSTEM_SHEETS else ""
                lines.append(f"• <b>{sheet.title}</b>{tag} — {len(rows)} строк")
            await query.edit_message_text("\n".join(lines), parse_mode="HTML")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")

# ========================
# /checkall — с адаптивной паузой
# ========================
async def checkall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id != ADMIN_ID:
        return

    spreadsheet = get_spreadsheet()
    hashes = load_hashes_to_check(spreadsheet)
    if not hashes:
        await update.message.reply_text(
            "❌ Лист <b>хеши_для_проверки</b> пуст.\n\n"
            "Добавь хеши в колонку A листа <b>хеши_для_проверки</b> и запусти снова.",
            parse_mode="HTML"
        )
        return

    progress = load_checkall_progress(spreadsheet)
    if progress:
        start_index   = progress.get("last_index", 0)
        found_count   = progress.get("found_count", 0)
        not_found     = progress.get("not_found", [])
        errors        = progress.get("errors", [])
        duplicates    = progress.get("duplicates", [])
        current_pause = progress.get("current_pause", MIN_PAUSE)
        await update.message.reply_text(f"⏩ Продолжаю с места остановки (хеш #{start_index + 1})...")
    else:
        start_index   = 0
        found_count   = 0
        not_found     = []
        errors        = []
        duplicates    = []
        current_pause = MIN_PAUSE

    total = len(hashes)
    await update.message.reply_text(
        f"🔄 Загружаю таблицу в память...\n"
        f"Всего хешей: {total}, осталось: {total - start_index}\n"
        f"Начальная пауза: {current_pause}с"
    )

    try:
        sheets_data = await load_all_sheets_data(spreadsheet)
        await update.message.reply_text(
            f"✅ Таблица загружена ({len(sheets_data)} листов)\n🔄 Начинаю проверку..."
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка загрузки таблицы: {e}")
        return

    seen_in_run = set()

    for i, tx_hash in enumerate(hashes):
        if i < start_index:
            continue
        had_error = False
        try:
            if tx_hash.lower() in seen_in_run:
                duplicates.append(tx_hash)
                logger.info(f"[{i+1}/{total}] Дубль в файле: {tx_hash[:20]}")
                try:
                    class AdminUser:
                        username = "checkall"
                        id = ADMIN_ID
                    await save_duplicate(spreadsheet, tx_hash, AdminUser())
                except Exception as e:
                    logger.error(f"Ошибка записи дубля: {e}")

            elif is_duplicate_hash(tx_hash):
                duplicates.append(tx_hash)
                found_count += 1
                logger.info(f"[{i+1}/{total}] Уже в использованных: {tx_hash[:20]}")
                try:
                    class AdminUser:
                        username = "checkall"
                        id = ADMIN_ID
                    await save_duplicate(spreadsheet, tx_hash, AdminUser())
                except Exception as e:
                    logger.error(f"Ошибка записи дубля: {e}")

            else:
                seen_in_run.add(tx_hash.lower())
                sheet, row_index, row = find_hash_in_loaded_data(tx_hash, sheets_data)
                if sheet and row_index:
                    status = row[13] if len(row) > 13 else ""
                    if not status:
                        result = await verify_and_write_tron_data(sheet, row_index, tx_hash)
                        logger.info(f"[{i+1}/{total}] Обработан: {tx_hash[:20]} — {result}")
                    else:
                        logger.info(f"[{i+1}/{total}] Уже обработан: {tx_hash[:20]}")
                    found_count += 1
                    await save_used_hash(spreadsheet, tx_hash, ADMIN_ID)
                else:
                    not_found.append(tx_hash)
                    logger.info(f"[{i+1}/{total}] Не найден: {tx_hash[:20]}")

                    class AdminUser:
                        username = "checkall"
                        id = ADMIN_ID
                    await save_not_found(spreadsheet, tx_hash, AdminUser(), reason="не найден (/checkall)")

        except Exception as e:
            had_error = True
            errors.append(tx_hash)
            logger.error(f"Ошибка при проверке {tx_hash[:20]}: {e}")
            try:
                class AdminUser:
                    username = "checkall"
                    id = ADMIN_ID
                await save_error(spreadsheet, tx_hash, str(e)[:100], AdminUser())
            except Exception:
                pass

        # Адаптивная пауза
        if had_error and "429" in str(errors[-1] if errors else ""):
            current_pause = min(current_pause * 1.5, MAX_PAUSE)
            logger.info(f"Пауза увеличена до {current_pause:.1f}с")
        elif not had_error and current_pause > MIN_PAUSE:
            current_pause = max(current_pause * 0.9, MIN_PAUSE)

        # Сохраняем прогресс каждые 10 хешей чтобы не превышать write лимит
        if (i + 1) % 10 == 0:
            save_checkall_progress(spreadsheet, i + 1, found_count, not_found, errors, duplicates, current_pause)

        await asyncio.sleep(current_pause)

        if (i + 1) % 100 == 0:
            await update.message.reply_text(
                f"⏳ Прогресс: {i+1}/{total}\n"
                f"✅ Найдено: {found_count}\n"
                f"❌ Не найдено: {len(not_found)}\n"
                f"♻️ Дублей: {len(duplicates)}\n"
                f"⚠️ Ошибок: {len(errors)}\n"
                f"⏱ Пауза: {current_pause:.1f}с"
            )

    clear_checkall_progress(spreadsheet)

    try:
        await update.message.reply_text(
            f"✅ <b>Проверка завершена!</b>\n\n"
            f"Всего хешей: {total}\n"
            f"Найдено и обработано: {found_count}\n"
            f"Не найдено в таблице: {len(not_found)}\n"
            f"Дублей пропущено: {len(duplicates)}\n"
            f"Ошибок: {len(errors)}",
            parse_mode="HTML"
        )
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
        sheets = [s for s in spreadsheet.worksheets() if s.title not in SYSTEM_SHEETS]
        for sheet in sheets:
            rows = sheet.get_all_values()
            for i, row in enumerate(rows):
                if len(row) > 12 and row[12].strip().lower() == tx_hash.lower():
                    await update.message.reply_text(
                        f"✅ Найден!\nЛист: {sheet.title}\nСтрока: {i+1}\n"
                        f"Значение: <code>{row[12]}</code>",
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
            tag = " ⚙️" if sheet.title in SYSTEM_SHEETS else ""
            lines.append(f"• <b>{sheet.title}</b>{tag} — {len(rows)} строк")
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
        used_hashes_cache = load_used_hashes(spreadsheet)
        pending_checks = load_pending_queue(spreadsheet)
        if pending_checks:
            for tx_hash in pending_checks:
                processing_hashes.add(tx_hash.lower())
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
    app.add_handler(MessageHandler(
        filters.TEXT &
        filters.Regex("^(🔄 Перепроверить|📊 Статистика|📋 Статус очереди|🔍 Список листов)$") &
        ~filters.COMMAND,
        keyboard_handler
    ))
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
