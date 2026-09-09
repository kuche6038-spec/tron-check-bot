import logging
import asyncio
import re
import os
import json
import aiohttp
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes
import gspread
from gspread.utils import absolute_range_name
from google.oauth2.service_account import Credentials

# ========================
# НАСТРОЙКИ
# ========================
TELEGRAM_TOKEN    = os.environ["TELEGRAM_TOKEN"]
ADMIN_ID          = int(os.environ["ADMIN_ID"])  # главный админ (для обратной совместимости)
ADMIN_IDS         = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", os.environ["ADMIN_ID"]).split(",")]
SPREADSHEET_ID    = os.environ["SPREADSHEET_ID"]
TRON_API_KEY      = os.environ["TRON_API_KEY"]
CHECK_DELAY_HOURS = 1

TRON_WALLETS = [
    "TX6z5khTbArfSSV4b2yioUxhMytyWBNjC8",
    "TXZrknLXgXciqFK5seMiiTpH4DNwBydo9G",
    "TGfJMbySkZQKc68Rc6cHWZ7ohCEVbeyY41",
    "TN43HtnfYDxdj4b9ML4gin4S2816yJQArA",
]

GOOGLE_CREDS = json.loads(os.environ["GOOGLE_CREDENTIALS"])

# ========================
# ПРИВЯЗКА К КОЛОНКАМ ПО ЗАГОЛОВКУ
# Номера колонок в коде не фиксируются: бот читает строку 1 листа,
# находит заголовок с "хеш"/"хэш" и отсчитывает от него свои четыре колонки.
# ========================
SHEET_MARKER     = "реестр"           # обрабатываются только листы с этим словом в названии
AMOUNT_TOLERANCE = Decimal("0.01")    # допустимое расхождение сумм, USDT

# Заголовки бота — четыре колонки сразу за колонкой хеша
BOT_HEADERS = ["статус бота", "сумма в сети", "проверка адреса", "сверка суммы"]


def col_letter(idx: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA."""
    letters, n = "", idx + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(ord("A") + rem) + letters
    return letters


def _norm(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def is_register_sheet(title: str) -> bool:
    t = title or ""
    return not t.startswith("_") and SHEET_MARKER in t.lower()


def parse_amount(value):
    """'17826,7' -> Decimal('17826.7'). Пусто или мусор -> None."""
    raw = str(value or "").strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9,.\-]", "", raw).replace(",", ".")
    if cleaned.count(".") > 1:                      # разделители тысяч: 1.234.56
        head, _, tail = cleaned.rpartition(".")
        cleaned = head.replace(".", "") + "." + tail
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def format_amount(value) -> str:
    """Наружу число уходит с десятичной ЗАПЯТОЙ — таблица в русской локали."""
    if value is None:
        return ""
    try:
        return format(value.normalize(), "f").replace(".", ",")
    except (InvalidOperation, ValueError):
        return str(value)


def safe_cell(value: str) -> str:
    """
    Ячейки пишутся с value_input_option=USER_ENTERED, чтобы число стало числом.
    Побочный эффект: строка, начинающаяся с = + - @, была бы понята как формула.
    Такую строку экранируем апострофом.
    """
    text = "" if value is None else str(value)
    return "'" + text if text[:1] in ("=", "+", "-", "@") else text


def bind_columns(rows: list):
    """
    Ищет в строке 1 колонку хеша и проверяет, что четыре колонки справа
    свободны или уже принадлежат боту.
    Возвращает (привязка, причина_отказа). Привязка None -> лист не трогаем.
    """
    if not rows:
        return None, "лист пуст"
    header = rows[0]

    idx_hash = None
    for i, cell in enumerate(header):
        c = _norm(cell)
        if "хеш" in c or "хэш" in c:
            idx_hash = i
            break
    if idx_hash is None:
        return None, "в строке 1 нет заголовка с 'хеш'"

    # заявленная оператором сумма — ближайший слева заголовок со словом 'сумма'
    idx_declared = None
    for i in range(idx_hash - 1, -1, -1):
        if "сумма" in _norm(header[i]):
            idx_declared = i
            break

    targets = [idx_hash + 1 + k for k in range(len(BOT_HEADERS))]
    for k, t in enumerate(targets):
        current = _norm(header[t]) if t < len(header) else ""
        if current and current != BOT_HEADERS[k]:
            return None, (f"колонка {col_letter(t)} занята чужим заголовком "
                          f"'{header[t]}'")

    headers_present = all(
        t < len(header) and _norm(header[t]) == BOT_HEADERS[k]
        for k, t in enumerate(targets)
    )
    return {
        "hash":            idx_hash,
        "declared":        idx_declared,
        "status":          targets[0],
        "amount":          targets[1],
        "addr":            targets[2],
        "recon":           targets[3],
        "headers_present": headers_present,
    }, ""


async def ensure_bot_headers(sheet, binding: dict):
    """Проставляет заголовки бота в строку 1, если их там ещё нет."""
    if binding.get("headers_present"):
        return
    rng = f"{col_letter(binding['status'])}1:{col_letter(binding['recon'])}1"
    await sheets_write_with_retry(sheet.update, range_name=rng, values=[BOT_HEADERS])
    binding["headers_present"] = True
    logger.info(f"Заголовки бота проставлены в {rng}")

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
# httpx на уровне INFO печатает URL целиком, а токен бота — часть URL.
# Без этих двух строк весь лог Railway состоит из строк с токеном.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# ========================
# РАССЫЛКА АДМИНАМ
# ========================
async def notify_admins(bot, text: str, parse_mode: str = "HTML"):
    """Отправляет сообщение всем админам в личку."""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(chat_id=admin_id, text=text, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение админу {admin_id}: {e}")

# ========================
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ========================
used_hashes_cache: set = set()
pending_checks: dict   = {}
processing_hashes: set = set()
not_found_total: int   = 0
_spreadsheet_cache     = None
skipped_sheets: dict   = {}   # лист -> причина, по которой бот его не трогает

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

async def sheets_read_with_retry(func, *args, max_attempts: int = 4, **kwargs):
    """Обёртка для чтения. При 429 ждёт окно квоты и повторяет."""
    last = None
    for attempt in range(max_attempts):
        try:
            return await asyncio.get_event_loop().run_in_executor(
                None, lambda: func(*args, **kwargs)
            )
        except Exception as e:
            last = e
            if "429" in str(e) or "RATE_LIMIT" in str(e) or "Quota" in str(e):
                wait = 60 * (attempt + 1)
                logger.warning(f"Read 429, жду {wait}с (попытка {attempt+1}/{max_attempts})...")
                await asyncio.sleep(wait)
            else:
                raise
    raise last


async def batch_get_ranges(spreadsheet, ranges: list, group: int = 80) -> list:
    """
    Забирает много диапазонов ЗА ОДИН запрос вместо запроса на лист.
    Квота Google — 60 чтений в минуту, а листов у нас под сотню.
    """
    out = []
    for i in range(0, len(ranges), group):
        part = ranges[i:i + group]
        resp = await sheets_read_with_retry(spreadsheet.values_batch_get, part)
        out.extend(resp.get("valueRanges", []))
    return out


# ========================
# USED HASHES
# Структура: хеш | user_id | дата_время
# ========================
def load_used_hashes(spreadsheet) -> set:
    sheet = get_or_create_sheet(spreadsheet, "_использованные_хеши", cols=5)
    all_rows = sheet.get_all_values()
    if not all_rows:
        sheet.append_row(["хеш", "user_id", "ник", "сумма", "дата_время"])
        return set()
    hashes = set()
    # Пропускаем первую строку если это заголовок
    start = 1 if all_rows[0] and not re.match(r"[0-9a-fA-F]{32,}", all_rows[0][0]) else 0
    for row in all_rows[start:]:
        if row and row[0].strip():
            hashes.add(row[0].strip().lower())
    logger.info(f"Загружено {len(hashes)} использованных хешей")
    return hashes

async def save_used_hash(spreadsheet, tx_hash: str, user_id: int, username: str = "", amount: str = ""):
    if tx_hash.lower() in used_hashes_cache:
        return
    try:
        sheet = get_or_create_sheet(spreadsheet, "_использованные_хеши", cols=5)
        await sheets_write_with_retry(
            sheet.append_row,
            [
                tx_hash.lower(),
                str(user_id),
                username,
                amount,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ]
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
    sheets = [s for s in spreadsheet.worksheets() if is_register_sheet(s.title)]
    for sheet in sheets:
        logger.info(f"Загружаю лист: '{sheet.title}'")
        rows = None
        for attempt in range(3):
            try:
                rows = await asyncio.get_event_loop().run_in_executor(None, sheet.get_all_values)
                break
            except Exception as e:
                if "429" in str(e) or "RATE_LIMIT" in str(e) or "Quota" in str(e):
                    wait = 60 * (attempt + 1)
                    logger.warning(f"Read 429 на листе '{sheet.title}', жду {wait}с...")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"Ошибка загрузки листа '{sheet.title}': {e}")
                    break
        if rows is None:
            await asyncio.sleep(1)
            continue
        binding, reason = bind_columns(rows)
        if binding is None:
            skipped_sheets[sheet.title] = reason
            logger.warning(f"Лист '{sheet.title}' пропущен: {reason}")
        else:
            skipped_sheets.pop(sheet.title, None)
            result[sheet.title] = {"sheet": sheet, "rows": rows, "binding": binding}
        await asyncio.sleep(1)
    logger.info(f"Загружено листов: {len(result)}, пропущено: {len(skipped_sheets)}")
    return result

def find_hash_in_loaded_data(tx_hash: str, sheets_data: dict):
    target = tx_hash.lower()
    for title, data in sheets_data.items():
        binding = data["binding"]
        col = binding["hash"]
        for i, row in enumerate(data["rows"]):
            if i == 0:
                continue                      # строка заголовков
            if len(row) > col and row[col].strip().lower() == target:
                logger.info(f"Найден на листе '{title}', строка {i + 1}")
                return data["sheet"], i + 1, row, binding
    return None, None, None, None

async def find_hash_in_all_sheets(tx_hash: str):
    spreadsheet = get_spreadsheet()
    target = tx_hash.lower()
    sheets = [s for s in spreadsheet.worksheets() if is_register_sheet(s.title)]
    for sheet in sheets:
        all_rows = None
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
        if all_rows is None:
            continue
        binding, reason = bind_columns(all_rows)
        if binding is None:
            skipped_sheets[sheet.title] = reason
            logger.warning(f"Лист '{sheet.title}' пропущен: {reason}")
            continue
        skipped_sheets.pop(sheet.title, None)
        col = binding["hash"]
        for i, row in enumerate(all_rows):
            if i == 0:
                continue
            if len(row) > col and row[col].strip().lower() == target:
                logger.info(f"Найден на листе '{sheet.title}', строка {i + 1}")
                return sheet, i + 1, row, binding
    return None, None, None, None

# ========================
# BATCH ЗАПИСЬ В ОСНОВНУЮ ТАБЛИЦУ
# ========================
async def mark_and_write_batch(sheet, binding: dict, row_index: int,
                               status: str, amount: str, addr_result: str, recon: str):
    """
    Записывает статус, сумму, адрес и сверку ОДНИМ batch запросом.
    Колонки берутся из привязки, а не из констант.
    """
    try:
        await ensure_bot_headers(sheet, binding)
        updates = [
            {"range": f"{col_letter(binding['status'])}{row_index}", "values": [[safe_cell(status)]]},
            {"range": f"{col_letter(binding['amount'])}{row_index}", "values": [[safe_cell(amount)]]},
            {"range": f"{col_letter(binding['addr'])}{row_index}",   "values": [[safe_cell(addr_result)]]},
            {"range": f"{col_letter(binding['recon'])}{row_index}",  "values": [[safe_cell(recon)]]},
        ]
        # USER_ENTERED: без него Google Sheets положит "8018,5" как ТЕКСТ,
        # и колонка перестанет суммироваться
        result = await sheets_write_with_retry(
            sheet.batch_update, updates, value_input_option="USER_ENTERED"
        )
        if result is None:
            # sheets_write_with_retry исчерпал попытки и вернул None:
            # без этой проверки строка молча осталась бы незаписанной
            raise RuntimeError(f"запись строки {row_index} не подтверждена Google Sheets")
        return result
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

async def verify_and_write_tron_data(sheet, binding: dict, row_index: int,
                                     tx_hash: str, row: list = None) -> tuple[str, str, str]:
    """
    Проверяет транзакцию и записывает результат ОДНИМ batch запросом.
    Возвращает (result_str, amount, recon).
    """
    data = await get_tron_transaction(tx_hash)

    if not data:
        await mark_and_write_batch(sheet, binding, row_index,
                                   "✅ обработано", "⚠️ API недоступен", "—", "—")
        return "API недоступен", "", "—"

    if data.get("contractRet") == "FAILED":
        await mark_and_write_batch(sheet, binding, row_index,
                                   "✅ обработано", "⚠️ транзакция FAILED", "—", "—")
        return "транзакция FAILED", "", "—"

    if not data.get("trc20TransferInfo") and not data.get("contractData"):
        await mark_and_write_batch(sheet, binding, row_index,
                                   "✅ обработано", "⚠️ нет данных", "—", "—")
        return "нет данных транзакции", "", "—"

    amount_dec = None
    to_address = ""

    trc20_transfers = data.get("trc20TransferInfo", [])
    if trc20_transfers:
        transfer = trc20_transfers[0]
        raw_amount = transfer.get("amount_str", transfer.get("amount", "0"))
        try:
            decimals = int(transfer.get("decimals", 6))
        except (TypeError, ValueError):
            decimals = 6
        try:
            amount_dec = Decimal(str(raw_amount)) / (Decimal(10) ** decimals)
        except (InvalidOperation, ValueError):
            amount_dec = None
        to_address = transfer.get("to_address", "") or ""
    else:
        raw_amount = data.get("amount", 0)
        try:
            amount_dec = Decimal(str(raw_amount)) / Decimal(1_000_000)
        except (InvalidOperation, ValueError):
            amount_dec = None
        contract_data = data.get("contractData") or {}
        to_address = contract_data.get("to_address", "") or ""

    amount = format_amount(amount_dec) if amount_dec is not None else str(raw_amount)

    if any(to_address.lower() == w.lower() for w in TRON_WALLETS):
        addr_result = "✅ Адрес верный"
    else:
        addr_result = f"❌ Адрес неверный: {to_address}"

    # сверка суммы: что заявил оператор против того, что в блокчейне
    declared = None
    if row is not None and binding.get("declared") is not None:
        di = binding["declared"]
        if di < len(row):
            declared = parse_amount(row[di])

    if amount_dec is None or declared is None:
        recon = "—"
    elif abs(declared - amount_dec) <= AMOUNT_TOLERANCE:
        recon = "✅ сумма сходится"
    else:
        recon = (f"⚠️ заявлено {format_amount(declared)}, "
                 f"в сети {format_amount(amount_dec)}")

    await mark_and_write_batch(sheet, binding, row_index,
                               "✅ обработано", amount, addr_result, recon)
    return f"сумма: {amount}, {addr_result}, {recon}", amount, recon

# ========================
# ОСНОВНАЯ ЛОГИКА ПРОВЕРКИ
# ========================
async def check_hash_with_tron(tx_hash: str) -> tuple[bool, str, str]:
    """Возвращает (найден, сумма, сверка)."""
    try:
        sheet, row_index, row, binding = await find_hash_in_all_sheets(tx_hash)
        if sheet and row_index:
            result, amount, recon = await verify_and_write_tron_data(
                sheet, binding, row_index, tx_hash, row
            )
            logger.info(f"TRON проверка {tx_hash[:20]}: {result}")
            return True, amount, recon
        return False, "", ""
    except Exception as e:
        logger.error(f"Ошибка проверки хеша {tx_hash[:20]}: {e}")
        return False, "", ""


async def notify_amount_mismatch(bot, tx_hash: str, recon: str, who: str):
    """Расхождение заявленной и фактической суммы — сигнал админам."""
    if not recon or not recon.startswith("⚠️"):
        return
    await notify_admins(
        bot,
        f"⚠️ <b>Расхождение суммы</b>\n\n"
        f"Хеш: <code>{tx_hash}</code>\n"
        f"{recon}\n"
        f"Юзер: {who}"
    )

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
            found, amount, recon = await check_hash_with_tron(tx_hash)

            if found:
                try:
                    spreadsheet = get_spreadsheet()
                    user = data.get("user")
                    username = f"@{user.username}" if user and getattr(user, "username", None) else f"id:{data['user_id']}"
                    await save_used_hash(spreadsheet, tx_hash, data["user_id"], username, amount)
                    await notify_amount_mismatch(application.bot, tx_hash, recon, username)
                except Exception as e:
                    logger.error(f"Ошибка сохранения в использованные_хеши: {e}")
            else:
                global not_found_total
                not_found_total += 1
                try:
                    user = data.get("user")
                    username = f"@{user.username}" if user and getattr(user, "username", None) else f"id:{data['user_id']}"
                    await notify_admins(
                        application.bot,
                        f"⚠️ <b>Хеш не найден в таблице</b>\n\n"
                        f"Хеш: <code>{tx_hash}</code>\n"
                        f"Юзер: {username}\n\n"
                        f"Транзакция отсутствует после повторной проверки."
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
# ДИАГНОСТИКА ЛИСТОВ
# ========================
def chunk_lines(lines: list, limit: int = 3500) -> list:
    """Режет список строк на куски, влезающие в одно сообщение Telegram."""
    chunks, buf, size = [], [], 0
    for line in lines:
        if buf and size + len(line) + 1 > limit:
            chunks.append("\n".join(buf))
            buf, size = [], 0
        buf.append(line)
        size += len(line) + 1
    if buf:
        chunks.append("\n".join(buf))
    return chunks or [""]


async def build_sheets_report(spreadsheet) -> list:
    """
    Что бот видит, куда привязался, что пропускает и почему.
    Читает ТОЛЬКО строку 1 каждого листа — одним batch-запросом, а не
    запросом на лист: иначе на 68 листах мы упираемся в квоту 60 чтений/мин.
    Возвращает СПИСОК сообщений: список листов длинный и в одно не влезает.
    """
    sheets = spreadsheet.worksheets()
    titles = [sh.title for sh in sheets]

    # запрос 1: строка заголовков со всех листов
    header_ranges = [absolute_range_name(t, "1:1") for t in titles]
    headers_raw = await batch_get_ranges(spreadsheet, header_ranges)
    headers = {}
    for t, vr in zip(titles, headers_raw):
        vals = vr.get("values") or []
        headers[t] = vals[0] if vals else []

    lines = []
    n_ok = n_blocked = n_skip = n_sys = 0
    blocked, bound = [], {}

    for title in titles:
        if title.startswith("_") or title in SYSTEM_SHEETS:
            n_sys += 1
            lines.append(f"• <b>{title}</b> ⚙️ служебный")
            continue
        if not is_register_sheet(title):
            n_skip += 1
            lines.append(f"• <b>{title}</b> — не реестр, пропускается")
            continue
        binding, reason = bind_columns([headers[title]])
        if binding is None:
            n_blocked += 1
            blocked.append((title, reason))
            skipped_sheets[title] = reason
            lines.append(f"• <b>{title}</b> ⛔ {reason}")
        else:
            n_ok += 1
            skipped_sheets.pop(title, None)
            bound[title] = binding

    # запрос 2: колонка хеша у привязанных листов — чтобы знать объём работ
    counts = {}
    if bound:
        hash_ranges = [
            absolute_range_name(t, f"{col_letter(b['hash'])}:{col_letter(b['hash'])}")
            for t, b in bound.items()
        ]
        hashes_raw = await batch_get_ranges(spreadsheet, hash_ranges)
        for t, vr in zip(bound.keys(), hashes_raw):
            vals = vr.get("values") or []
            counts[t] = sum(1 for r in vals[1:] if r and str(r[0]).strip())

    # строки собираем заново — теперь уже с количеством хешей
    lines = []
    for title in titles:
        if title.startswith("_") or title in SYSTEM_SHEETS:
            lines.append(f"• <b>{title}</b> ⚙️ служебный")
        elif not is_register_sheet(title):
            lines.append(f"• <b>{title}</b> — не реестр, пропускается")
        elif title in bound:
            b = bound[title]
            declared = (col_letter(b["declared"])
                        if b["declared"] is not None else "нет")
            lines.append(
                f"• <b>{title}</b> — {counts.get(title, 0)} хешей · "
                f"хеш {col_letter(b['hash'])} · "
                f"пишу в {col_letter(b['status'])}–{col_letter(b['recon'])} · "
                f"заявленная сумма {declared}"
            )
        else:
            lines.append(f"• <b>{title}</b> ⛔ {skipped_sheets.get(title, '')}")

    total_hashes = sum(counts.values())
    head = (f"📊 Листов всего: <b>{len(sheets)}</b>\n"
            f"✅ готовы к работе: <b>{n_ok}</b> ({total_hashes} хешей) · "
            f"⛔ не готовы: <b>{n_blocked}</b> · "
            f"не реестры: {n_skip} · служебных: {n_sys}\n")

    tail = []
    if blocked:
        tail.append(f"\n⛔ <b>Требуют подготовки ({len(blocked)}):</b>")
        for title, reason in blocked:
            tail.append(f"• <b>{title}</b> — {reason}")

    chunks = chunk_lines([head] + lines + tail)
    if len(chunks) > 1:
        chunks = [f"<i>часть {i+1}/{len(chunks)}</i>\n{c}"
                  for i, c in enumerate(chunks)]
    return chunks


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
            username = f"@{user.username}" if user.username else f"id:{user_id}"
            # Уведомляем всех админов в личку
            await notify_admins(
                context.bot,
                f"⛔ <b>Попытка использовать дубль хеша</b>\n\n"
                f"Хеш: <code>{tx_hash}</code>\n"
                f"Юзер: {username}\n\n"
                f"Транзакция уже была использована ранее."
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
        found, amount, recon = await check_hash_with_tron(tx_hash)
        if found:
            try:
                spreadsheet = get_spreadsheet()
                username = f"@{user.username}" if user.username else f"id:{user_id}"
                await save_used_hash(spreadsheet, tx_hash, user_id, username, amount)
                await notify_amount_mismatch(context.bot, tx_hash, recon, username)
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
    if update.message.from_user.id not in ADMIN_IDS:
        return
    if not pending_checks:
        await update.message.reply_text("📋 Очередь пуста.")
        return

    await update.message.reply_text(f"🔄 Проверяю {len(pending_checks)} хешей...")
    found_count, not_found_list = 0, []

    for tx_hash, data in list(pending_checks.items()):
        found, amount, recon = await check_hash_with_tron(tx_hash)
        if found:
            found_count += 1
            pending_checks.pop(tx_hash, None)
            processing_hashes.discard(tx_hash.lower())
            try:
                spreadsheet = get_spreadsheet()
                user = data.get("user")
                username = f"@{user.username}" if user and getattr(user, "username", None) else f"id:{data['user_id']}"
                await save_used_hash(spreadsheet, tx_hash, data["user_id"], username, amount)
                await notify_amount_mismatch(context.bot, tx_hash, recon, username)
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
    if update.message.from_user.id not in ADMIN_IDS:
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
    if update.message.from_user.id not in ADMIN_IDS:
        return
    text = update.message.text

    if text == "🔄 Перепроверить":
        if not pending_checks:
            await update.message.reply_text("📋 Очередь пуста — нечего перепроверять.")
            return
        await update.message.reply_text(f"🔄 Проверяю {len(pending_checks)} хешей...")
        found_count, not_found_list = 0, []
        for tx_hash, data in list(pending_checks.items()):
            found, amount, recon = await check_hash_with_tron(tx_hash)
            if found:
                found_count += 1
                pending_checks.pop(tx_hash, None)
                processing_hashes.discard(tx_hash.lower())
                try:
                    spreadsheet = get_spreadsheet()
                    user = data.get("user")
                    username = f"@{user.username}" if user and getattr(user, "username", None) else f"id:{data['user_id']}"
                    await save_used_hash(spreadsheet, tx_hash, data["user_id"], username, amount)
                    await notify_amount_mismatch(context.bot, tx_hash, recon, username)
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
            for part in await build_sheets_report(spreadsheet):
                await update.message.reply_text(part, parse_mode="HTML")
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}")

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id not in ADMIN_IDS:
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
    if query.from_user.id not in ADMIN_IDS:
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
            found, amount, recon = await check_hash_with_tron(tx_hash)
            if found:
                found_count += 1
                pending_checks.pop(tx_hash, None)
                processing_hashes.discard(tx_hash.lower())
                try:
                    spreadsheet = get_spreadsheet()
                    user = data.get("user")
                    username = f"@{user.username}" if user and getattr(user, "username", None) else f"id:{data['user_id']}"
                    await save_used_hash(spreadsheet, tx_hash, data["user_id"], username, amount)
                    await notify_amount_mismatch(context.bot, tx_hash, recon, username)
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
            parts = await build_sheets_report(spreadsheet)
            await query.edit_message_text(parts[0], parse_mode="HTML")
            for part in parts[1:]:
                await query.message.reply_text(part, parse_mode="HTML")
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")

# ========================
# /checkall — с адаптивной паузой
# ========================
async def checkall_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id not in ADMIN_IDS:
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
    mismatches = []

    if skipped_sheets:
        lines = ["⚠️ <b>Листы пропущены (бот их не трогает):</b>\n"]
        for title, reason in skipped_sheets.items():
            lines.append(f"• <b>{title}</b> — {reason}")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

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
                sheet, row_index, row, binding = find_hash_in_loaded_data(tx_hash, sheets_data)
                if sheet and row_index:
                    si = binding["status"]
                    status = row[si] if len(row) > si else ""
                    hash_amount = ""
                    if not status:
                        result, hash_amount, recon = await verify_and_write_tron_data(
                            sheet, binding, row_index, tx_hash, row
                        )
                        logger.info(f"[{i+1}/{total}] Обработан: {tx_hash[:20]} — {result}")
                        if recon.startswith("⚠️"):
                            mismatches.append(f"{tx_hash} — {recon}")
                    else:
                        logger.info(f"[{i+1}/{total}] Уже обработан: {tx_hash[:20]}")
                    found_count += 1
                    await save_used_hash(spreadsheet, tx_hash, ADMIN_ID, "@checkall", hash_amount)
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
            f"Ошибок: {len(errors)}\n"
            f"Расхождений по сумме: {len(mismatches)}",
            parse_mode="HTML"
        )
        if mismatches:
            chunk = 30
            for idx in range(0, len(mismatches), chunk):
                part = mismatches[idx:idx + chunk]
                lines = [f"⚠️ <b>Расхождения по сумме "
                         f"({idx+1}-{idx+len(part)} из {len(mismatches)}):</b>\n"]
                for m in part:
                    lines.append(f"<code>{m}</code>")
                await update.message.reply_text("\n".join(lines), parse_mode="HTML")
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
    if update.message.from_user.id not in ADMIN_IDS:
        return
    args = context.args
    if not args:
        await update.message.reply_text("Использование: /find <хеш>")
        return
    tx_hash = args[0].strip()
    await update.message.reply_text(f"🔍 Ищу: {tx_hash[:20]}...\nДлина: {len(tx_hash)} символов")
    try:
        spreadsheet = get_spreadsheet()
        sheets = [s for s in spreadsheet.worksheets() if is_register_sheet(s.title)]
        target = tx_hash.lower()
        skipped = []
        for sheet in sheets:
            rows = sheet.get_all_values()
            binding, reason = bind_columns(rows)
            if binding is None:
                skipped.append(f"{sheet.title} — {reason}")
                continue
            col = binding["hash"]
            for i, row in enumerate(rows):
                if i == 0:
                    continue
                if len(row) > col and row[col].strip().lower() == target:
                    await update.message.reply_text(
                        f"✅ Найден!\nЛист: {sheet.title}\n"
                        f"Строка: {i+1}, колонка: {col_letter(col)}\n"
                        f"Значение: <code>{row[col]}</code>",
                        parse_mode="HTML"
                    )
                    return
        msg = "❌ Не найден ни в одном листе-реестре"
        if skipped:
            msg += "\n\nПропущены листы:\n" + "\n".join(f"• {x}" for x in skipped)
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def debug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id not in ADMIN_IDS:
        return
    try:
        spreadsheet = get_spreadsheet()
        for part in await build_sheets_report(spreadsheet):
            await update.message.reply_text(part, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id not in ADMIN_IDS:
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