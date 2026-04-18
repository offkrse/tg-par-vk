#!/usr/bin/env python3
"""
Модуль max_checker.py
Формирует файлы для проверки номеров через API promouser.com
и обрабатывает результаты.
"""
import os
import asyncio
import logging
import aiohttp
import requests
import csv
from datetime import datetime
from typing import Optional, Set, List, Tuple
from dotenv import load_dotenv

load_dotenv("/opt/bot/.env")

VERSION_MAX_CHECKER = "1.34"

# === Настройки ===
PROMO_CHECKER_KEY = os.getenv("PROMO_CHECKER_KEY", "")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHECKER_CHAT_ID = "1550242935"  # Чат для отправки результатов

# Директории
TXTS_DIR = "/opt/bot/max_checker/txts"
SOURCE_TXT_DIR = "/opt/bot/txt"
RESULTS_DIR = "/opt/bot/max_checker/results"

# API endpoints
API_BASE = "https://promouser.com/api"

# Курс доллара к рублю
USD_TO_RUB = 79

# Курс USD/RUB (fallback если API недоступен)
DEFAULT_USD_RUB_RATE = 90.0

# Фильтр по активности (дней с последнего логина)
MAX_ACTIVE_DAYS_AGO = 20

# Список паков для запуска. Допустимые значения: "pack1", "pack2"
# Примеры:
#   ALLOWED_PACKS = ["pack1", "pack2"]  — запустить оба пака
#   ALLOWED_PACKS = ["pack1"]           — только pack1
#   ALLOWED_PACKS = ["pack2"]           — только pack2
ALLOWED_PACKS: List[str] = ["pack1","pack2"]

# Если True  — паки отправляются в Promouser для проверки, результат приходит в ТГ.
# Если False — паки НЕ отправляются в Promouser, файлы отправляются сразу в ТГ.
SEND_TO_PROMOUSER: bool = False

# === Прокси для Telegram ===
_TG_PROXY_URL    = os.getenv("TG_PROXY_URL", "").rstrip("/")
_TG_PROXY_SECRET = os.getenv("TG_PROXY_SECRET", "")
_TG_SOCKS5_HOST  = os.getenv("TG_SOCKS5_HOST", "")
_TG_SOCKS5_PORT  = int(os.getenv("TG_SOCKS5_PORT", "1080"))
_TG_SOCKS5_USER  = os.getenv("TG_SOCKS5_USER", "")
_TG_SOCKS5_PASS  = os.getenv("TG_SOCKS5_PASS", "")


def _bot_api_url(token: str, method: str) -> str:
    if _TG_PROXY_URL:
        return f"{_TG_PROXY_URL}/bot{token}/{method}"
    return f"https://api.telegram.org/bot{token}/{method}"


def _proxy_headers() -> dict:
    if _TG_PROXY_SECRET:
        return {"X-Proxy-Secret": _TG_PROXY_SECRET}
    return {}


def _aiohttp_connector():
    """SOCKS5 коннектор для aiohttp — использует те же настройки что bot_master."""
    if not _TG_SOCKS5_HOST:
        return None
    try:
        from aiohttp_socks import ProxyConnector, ProxyType
        if _TG_SOCKS5_USER:
            return ProxyConnector(
                proxy_type=ProxyType.SOCKS5,
                host=_TG_SOCKS5_HOST, port=_TG_SOCKS5_PORT,
                username=_TG_SOCKS5_USER, password=_TG_SOCKS5_PASS,
                rdns=True,
            )
        return ProxyConnector(
            proxy_type=ProxyType.SOCKS5,
            host=_TG_SOCKS5_HOST, port=_TG_SOCKS5_PORT,
            rdns=True,
        )
    except ImportError:
        logging.warning("aiohttp-socks не установлен, SOCKS5 недоступен")
        return None

def get_usd_rub_rate() -> float:
    """Получает текущий курс USD/RUB"""
    try:
        # ЦБ РФ API
        resp = requests.get(
            "https://www.cbr-xml-daily.ru/daily_json.js",
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            rate = data.get("Valute", {}).get("USD", {}).get("Value", DEFAULT_USD_RUB_RATE)
            logger.info(f"Курс USD/RUB: {rate}")
            return float(rate)
    except Exception as e:
        logger.warning(f"Не удалось получить курс USD/RUB: {e}, используем {DEFAULT_USD_RUB_RATE}")
    
    return DEFAULT_USD_RUB_RATE

# === Логирование ===
logging.basicConfig(
    filename="/opt/bot/max_checker.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("max_checker")

# Флаги ожидания подтверждения оплаты (pack1 и pack2 раздельно)
_waiting_for_payment_confirmation = False
_pending_order_id: Optional[int] = None
_pending_file_path: Optional[str] = None
_pending_original_count: int = 0
_pending_phones_for_ac: List[str] = []
_pending_pack_name: str = ""
_pending_result_filename: str = ""


def get_today_date_str() -> str:
    """Возвращает сегодняшнюю дату в формате DD_MM_YYYY"""
    return datetime.today().strftime("%d_%m_%Y")


def collect_phones_by_prefixes(allowed_prefixes: Tuple[str, ...], priority_prefix: str = "") -> Tuple[Set[str], Set[str]]:
    """
    Универсальная функция сбора номеров из TXT файлов по списку префиксов.

    allowed_prefixes  — какие файлы брать, например ("Б1", "Б0")
    priority_prefix   — префикс с наивысшим приоритетом (идёт первым в итоговом списке)

    Возвращает (all_phones, priority_phones).
    """
    all_phones: Set[str] = set()
    priority_phones: Set[str] = set()

    if not os.path.exists(SOURCE_TXT_DIR):
        logger.warning(f"Директория {SOURCE_TXT_DIR} не существует")
        return all_phones, priority_phones

    for filename in os.listdir(SOURCE_TXT_DIR):
        if not filename.endswith(".txt"):
            continue

        # "Б1 (294).txt" -> "Б1",  "КР ДОП_5 (294).txt" -> "КР ДОП_5"
        base_name = filename.rsplit(" (", 1)[0] if " (" in filename else filename.replace(".txt", "")

        if base_name not in allowed_prefixes:
            continue

        filepath = os.path.join(SOURCE_TXT_DIR, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                phones = {line.strip() for line in f if line.strip()}

            all_phones.update(phones)

            if priority_prefix and base_name == priority_prefix:
                priority_phones.update(phones)

            logger.info(f"Файл [{base_name}]: {filename}, номеров: {len(phones)}")
        except Exception as e:
            logger.exception(f"Ошибка при чтении {filepath}: {e}")

    return all_phones, priority_phones


# --- Pack 1: Б1, Б0 ---
def collect_phones_pack1() -> Tuple[Set[str], Set[str]]:
    """Возвращает (all_phones, b1_phones) для pack1."""
    return collect_phones_by_prefixes(("Б1", "Б0"), priority_prefix="Б1")


# --- Pack 2: ББ ДОП_2, ББ ДОП_3, КР 1, КР 2, КР ДОП_3..КР ДОП_9 ---
PACK2_PREFIXES: Tuple[str, ...] = (
    "ББ ДОП_2", "ББ ДОП_3",
    "КР 1", "КР 2",
    "КР ДОП_3", "КР ДОП_4", "КР ДОП_5",
    "КР ДОП_6", "КР ДОП_7", "КР ДОП_8", "КР ДОП_9",
)


def collect_phones_pack2() -> Tuple[Set[str], Set[str]]:
    """Возвращает (all_phones, priority_phones) для pack2.
    Приоритет — ББ ДОП_2 (идёт первым).
    """
    return collect_phones_by_prefixes(PACK2_PREFIXES, priority_prefix="ББ ДОП_2")


# Оставляем старое имя как алиас для обратной совместимости
def collect_phones_from_txt_files() -> Tuple[Set[str], Set[str]]:
    return collect_phones_pack1()


ALREADY_CHECKED_MAX_LINES = 200000  # Максимум строк в одном файле


def get_already_checked_files() -> List[str]:
    """Возвращает список всех файлов already_checked в правильном порядке"""
    if not os.path.exists(TXTS_DIR):
        return []
    
    files = []
    # Основной файл
    main_file = os.path.join(TXTS_DIR, "already_checked.txt")
    if os.path.exists(main_file):
        files.append(main_file)
    
    # Дополнительные файлы already_checked_1.txt, already_checked_2.txt, ...
    i = 1
    while True:
        numbered_file = os.path.join(TXTS_DIR, f"already_checked_{i}.txt")
        if os.path.exists(numbered_file):
            files.append(numbered_file)
            i += 1
        else:
            break
    
    return files


def check_phone_in_already_checked(phone: str) -> bool:
    """
    Проверяет, есть ли номер в файлах already_checked.
    Читает файлы построчно для экономии памяти.
    """
    for filepath in get_already_checked_files():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip() == phone:
                        return True
        except Exception as e:
            logger.exception(f"Ошибка при чтении {filepath}: {e}")
    return False


def filter_already_checked(phones: List[str]) -> List[str]:
    """
    Фильтрует список номеров, убирая уже проверенные.
    Загружает файлы по одному для экономии памяти.
    """
    # Собираем все проверенные номера из всех файлов
    checked = set()
    for filepath in get_already_checked_files():
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        checked.add(stripped)
            logger.info(f"Загружен {filepath}, всего в памяти: {len(checked)} номеров")
        except Exception as e:
            logger.exception(f"Ошибка при чтении {filepath}: {e}")
    
    # Фильтруем
    result = [p for p in phones if p not in checked]
    logger.info(f"Отфильтровано: {len(phones)} -> {len(result)} номеров")
    
    # Освобождаем память
    del checked
    
    return result


def get_last_already_checked_file() -> Tuple[str, int]:
    """
    Возвращает путь к последнему файлу already_checked и количество строк в нём.
    Если файлов нет, возвращает путь к основному файлу и 0.
    """
    files = get_already_checked_files()
    
    if not files:
        return os.path.join(TXTS_DIR, "already_checked.txt"), 0
    
    last_file = files[-1]
    try:
        with open(last_file, "r", encoding="utf-8") as f:
            line_count = sum(1 for line in f if line.strip())
        return last_file, line_count
    except Exception as e:
        logger.exception(f"Ошибка при подсчёте строк в {last_file}: {e}")
        return last_file, 0


def save_already_checked(phones: Set[str]):
    """
    Добавляет номера в файлы already_checked.
    Разбивает на файлы по ALREADY_CHECKED_MAX_LINES строк.
    """
    if not phones:
        return
    
    os.makedirs(TXTS_DIR, exist_ok=True)
    phones_list = list(phones)
    
    try:
        last_file, current_count = get_last_already_checked_file()
        
        idx = 0
        while idx < len(phones_list):
            # Сколько можно добавить в текущий файл
            space_left = ALREADY_CHECKED_MAX_LINES - current_count
            
            if space_left <= 0:
                # Текущий файл заполнен, создаём новый
                files = get_already_checked_files()
                if not files or files[-1] == os.path.join(TXTS_DIR, "already_checked.txt"):
                    new_file_num = 1
                else:
                    # Извлекаем номер из последнего файла
                    last_name = os.path.basename(files[-1])
                    # already_checked_N.txt -> N
                    try:
                        new_file_num = int(last_name.replace("already_checked_", "").replace(".txt", "")) + 1
                    except ValueError:
                        new_file_num = 1
                
                last_file = os.path.join(TXTS_DIR, f"already_checked_{new_file_num}.txt")
                current_count = 0
                space_left = ALREADY_CHECKED_MAX_LINES
                logger.info(f"Создаётся новый файл: {last_file}")
            
            # Добавляем номера в текущий файл
            to_add = phones_list[idx:idx + space_left]
            
            with open(last_file, "a", encoding="utf-8") as f:
                for phone in to_add:
                    f.write(phone + "\n")
            
            logger.info(f"Добавлено {len(to_add)} номеров в {os.path.basename(last_file)}")
            
            idx += len(to_add)
            current_count += len(to_add)
        
        logger.info(f"Всего сохранено {len(phones_list)} новых номеров в already_checked")
        
    except Exception as e:
        logger.exception(f"Ошибка при сохранении already_checked: {e}")


def prepare_pack_file(
    pack_name: str,
    all_phones: Set[str],
    priority_phones: Set[str],
    max_phones: int = 50000,
) -> Tuple[Optional[str], int, List[str]]:
    """
    Универсальная подготовка файла для отправки в promouser.

    1. Формирует упорядоченный список (priority_phones первыми).
    2. Сохраняет полный список non_check_{pack_name}_{date}.txt.
    3. Фильтрует already_checked.
    4. Обрезает до max_phones.
    5. Сохраняет итоговый файл non_check_wd_{pack_name}_{date}.txt.

    ВАЖНО: в already_checked НЕ записывает — это делается снаружи,
    только после успешной отправки заказа в API.

    Возвращает (путь_к_файлу, кол-во строк, список_телефонов_для_записи_в_ac).
    """
    os.makedirs(TXTS_DIR, exist_ok=True)
    date_str = get_today_date_str()

    if not all_phones:
        logger.warning(f"[{pack_name}] Нет номеров для обработки")
        return None, 0, []

    # Приоритетные идут первыми
    other_phones = all_phones - priority_phones
    ordered_phones: List[str] = sorted(priority_phones) + sorted(other_phones)

    # Полный список (до фильтрации)
    non_check_path = os.path.join(TXTS_DIR, f"non_check_{pack_name}_{date_str}.txt")
    with open(non_check_path, "w", encoding="utf-8") as f:
        f.write("\n".join(ordered_phones))
    logger.info(f"[{pack_name}] Создан {non_check_path}: {len(ordered_phones)} номеров")

    # Убираем уже проверенные
    phones_to_check = filter_already_checked(ordered_phones)

    # Обрезаем до лимита
    original_count = len(phones_to_check)
    if len(phones_to_check) > max_phones:
        phones_to_check = phones_to_check[:max_phones]
        logger.info(f"[{pack_name}] Обрезано до {max_phones} (было {original_count})")

    if not phones_to_check:
        logger.warning(f"[{pack_name}] Все номера уже проверены")
        return None, 0, []

    # Файл для отправки в API
    non_check_wd_path = os.path.join(TXTS_DIR, f"non_check_wd_{pack_name}_{date_str}.txt")
    with open(non_check_wd_path, "w", encoding="utf-8") as f:
        f.write("\n".join(phones_to_check))
    logger.info(f"[{pack_name}] Создан {non_check_wd_path}: {len(phones_to_check)} номеров")

    return non_check_wd_path, len(phones_to_check), phones_to_check


def create_non_check_files() -> Tuple[Optional[str], int, List[str]]:
    """Pack1 (Б1 + Б0). Возвращает (путь, кол-во, phones_for_ac)."""
    all_phones, b1_phones = collect_phones_pack1()
    return prepare_pack_file("pack1", all_phones, b1_phones)


def create_non_check_files_pack2() -> Tuple[Optional[str], int, List[str]]:
    """Pack2 (ББ ДОП_2, ББ ДОП_3, КР 1..9). Возвращает (путь, кол-во, phones_for_ac)."""
    all_phones, priority_phones = collect_phones_pack2()
    return prepare_pack_file("pack2", all_phones, priority_phones)


# === API функции ===

def check_balance() -> Optional[float]:
    """Проверяет баланс через API"""
    try:
        resp = requests.get(
            f"{API_BASE}/balance",
            headers={"key": PROMO_CHECKER_KEY},
            timeout=30
        )
        if resp.status_code == 200:
            data = resp.json()
            balance = float(data.get("balance", 0))
            logger.info(f"Баланс: {balance}")
            return balance
        else:
            logger.error(f"Ошибка проверки баланса: {resp.status_code} {resp.text}")
            return None
    except Exception as e:
        logger.exception(f"Ошибка при проверке баланса: {e}")
        return None


def send_order(file_path: str, service_type: int = 19, force: int = 1) -> Optional[int]:
    """
    Отправляет заказ на проверку.
    Возвращает order_id или None при ошибке.
    """
    try:
        with open(file_path, "rb") as f:
            files = {"file": f}
            data = {"type": str(service_type), "force": str(force)}
            
            resp = requests.post(
                f"{API_BASE}/order",
                headers={"key": PROMO_CHECKER_KEY},
                files=files,
                data=data,
                timeout=120
            )
        
        if resp.status_code == 200:
            result = resp.json()
            order_id = result.get("order_id")
            logger.info(f"Заказ создан: order_id={order_id}")
            return order_id
        else:
            logger.error(f"Ошибка создания заказа: {resp.status_code} {resp.text}")
            return None
    except Exception as e:
        logger.exception(f"Ошибка при отправке заказа: {e}")
        return None


def check_order_status(order_id: int) -> dict:
    """
    Проверяет статус заказа.
    Возвращает dict со статусом и другими данными.
    """
    try:
        resp = requests.get(
            f"{API_BASE}/status",
            headers={"key": PROMO_CHECKER_KEY},
            params={"order_id": order_id},
            timeout=30
        )
        
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.error(f"Ошибка проверки статуса: {resp.status_code} {resp.text}")
            return {"error": resp.text}
    except Exception as e:
        logger.exception(f"Ошибка при проверке статуса: {e}")
        return {"error": str(e)}


def pay_order(order_id: int) -> dict:
    """
    Оплачивает заказ.
    Возвращает dict с результатом.
    """
    try:
        resp = requests.post(
            f"{API_BASE}/pay",
            headers={
                "key": PROMO_CHECKER_KEY,
                "Content-Type": "application/json"
            },
            json={"order_id": order_id},
            timeout=30
        )
        
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.error(f"Ошибка оплаты: {resp.status_code} {resp.text}")
            return {"error": resp.text}
    except Exception as e:
        logger.exception(f"Ошибка при оплате: {e}")
        return {"error": str(e)}


def download_result(url: str, save_path: str) -> bool:
    """Скачивает результат по URL"""
    try:
        resp = requests.get(url, timeout=120)
        if resp.status_code == 200:
            with open(save_path, "wb") as f:
                f.write(resp.content)
            logger.info(f"Результат скачан: {save_path}")
            return True
        else:
            logger.error(f"Ошибка скачивания результата: {resp.status_code}")
            return False
    except Exception as e:
        logger.exception(f"Ошибка при скачивании результата: {e}")
        return False


def filter_and_extract_ids(raw_file_path: str, output_path: str) -> Tuple[int, int]:
    """
    Читает CSV файл с результатами, фильтрует по Active_days_ago <= MAX_ACTIVE_DAYS_AGO,
    извлекает только ID_MAX и сохраняет в output_path.
    
    Формат входного файла (CSV):
    Phone_MAX,ID_MAX,First_name,Last_name,Last_login_time,Active_days_ago,Gender,Avatar_link
    
    Возвращает (количество отфильтрованных, количество исходных).
    """
    filtered_ids = []
    total_count = 0
    
    try:
        with open(raw_file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                total_count += 1
                
                # Получаем Active_days_ago
                active_days_str = row.get("Active_days_ago", "").strip()
                
                # Пропускаем если пустое или не число
                if not active_days_str:
                    continue
                
                try:
                    active_days = int(active_days_str)
                except ValueError:
                    logger.warning(f"Некорректное значение Active_days_ago: {active_days_str}")
                    continue
                
                # Фильтруем по активности
                if active_days <= MAX_ACTIVE_DAYS_AGO:
                    id_max = row.get("ID_MAX", "").strip()
                    if id_max:
                        filtered_ids.append(id_max)
        
        # Сохраняем отфильтрованные ID
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(filtered_ids))
        
        logger.info(f"Отфильтровано {len(filtered_ids)} из {total_count} записей (Active_days_ago <= {MAX_ACTIVE_DAYS_AGO})")
        
        return len(filtered_ids), total_count
        
    except Exception as e:
        logger.exception(f"Ошибка при фильтрации результатов: {e}")
        return 0, 0


async def send_telegram_message(text: str, chat_id: str = CHECKER_CHAT_ID):
    """Отправляет сообщение в Telegram (через прокси если задан)."""
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN не настроен")
        return

    url = _bot_api_url(BOT_TOKEN, "sendMessage")
    connector = _aiohttp_connector()
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(
                url,
                data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                headers=_proxy_headers()
            ) as resp:
                if resp.status != 200:
                    text_resp = await resp.text()
                    logger.error(f"Ошибка отправки сообщения: {resp.status} {text_resp}")
    except Exception as e:
        logger.exception(f"Ошибка при отправке сообщения в Telegram: {e}")


async def send_telegram_file(file_path: str, caption: str = "", chat_id: str = CHECKER_CHAT_ID, custom_filename: str = None):
    """Отправляет файл в Telegram (через прокси если задан)."""
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN не настроен")
        return

    url = _bot_api_url(BOT_TOKEN, "sendDocument")
    connector = _aiohttp_connector()
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            with open(file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("chat_id", chat_id)

                filename = custom_filename or os.path.basename(file_path)
                form.add_field("document", f, filename=filename)

                if caption:
                    form.add_field("caption", caption)

                async with session.post(
                    url, data=form, headers=_proxy_headers()
                ) as resp:
                    if resp.status != 200:
                        text_resp = await resp.text()
                        logger.error(f"Ошибка отправки файла: {resp.status} {text_resp}")
    except Exception as e:
        logger.exception(f"Ошибка при отправке файла в Telegram: {e}")


def count_lines(file_path: str) -> int:
    """Подсчитывает количество строк в файле"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


async def wait_for_order_completion(order_id: int, lines_count: int, check_interval: int = 60) -> dict:
    """
    Ожидает завершения заказа.
    1. Первая проверка через 1 минуту (для выявления ошибок, например нехватки средств)
    2. Если нет ошибки - ждём время в зависимости от количества строк (1000 строк ~ 2 минуты)
    3. Далее проверяем каждую минуту
    """
    # Первая проверка через 1 минуту - для выявления ошибок
    logger.info("Ожидание 60 сек перед первой проверкой статуса (проверка ошибок)...")
    await asyncio.sleep(60)
    
    status_data = check_order_status(order_id)
    
    if "error" in status_data:
        error_msg = str(status_data.get("error", ""))
        # Если ошибка нехватки средств - сразу возвращаем
        if "Insufficient funds" in error_msg:
            logger.error(f"Недостаточно средств: {status_data}")
            return status_data
    
    status = status_data.get("status", "")
    logger.info(f"Статус заказа {order_id} после первой проверки: {status}")
    
    # Если уже готов - возвращаем
    if status == "ready_paid":
        return status_data
    
    # Рассчитываем время ожидания: 1000 строк ~ 2 минуты
    # Формула: (lines_count / 1000) * 2 минуты = (lines_count / 500) секунд
    estimated_wait = int((lines_count / 1000) * 2 * 60)  # в секундах
    # Минимум 2 минуты, вычитаем уже прошедшую 1 минуту
    remaining_wait = max(estimated_wait - 60, 60)
    
    logger.info(f"Ожидание {remaining_wait} сек до следующей проверки (расчёт по {lines_count} строк)...")
    await asyncio.sleep(remaining_wait)
    
    # Далее проверяем каждую минуту
    while True:
        status_data = check_order_status(order_id)
        
        if "error" in status_data:
            logger.error(f"Ошибка при проверке статуса: {status_data}")
            return status_data
        
        status = status_data.get("status", "")
        logger.info(f"Статус заказа {order_id}: {status}")
        
        # ready_paid - заказ готов и оплачен (force=1 автоматически оплачивает)
        if status == "ready_paid":
            return status_data
        
        # Если не готов - ждём
        if status in ("not_ready_unpaid", "not_ready_paid", "processing"):
            await asyncio.sleep(check_interval)
            continue
        
        # Неизвестный статус - тоже ждём
        logger.warning(f"Неизвестный статус: {status}")
        await asyncio.sleep(check_interval)


async def submit_order(
    file_path: str,
    phones_for_ac: List[str],
    pack_name: str,
) -> Optional[int]:
    """
    Шаг 1: отправляет файл в promouser, получает order_id.
    После успешной отправки сразу записывает номера в already_checked.
    Возвращает order_id или None при ошибке.
    """
    if not PROMO_CHECKER_KEY:
        logger.error("PROMO_CHECKER_KEY не настроен в .env")
        return None

    order_id = send_order(file_path, service_type=19, force=1)
    if order_id is None:
        await send_telegram_message(f"❌ [{pack_name}] Не удалось создать заказ")
        return None

    logger.info(f"[{pack_name}] Заказ {order_id} отправлен успешно")

    # ✅ Заказ принят — записываем в already_checked
    if phones_for_ac:
        save_already_checked(set(phones_for_ac))
        logger.info(f"[{pack_name}] Записано в already_checked: {len(phones_for_ac)} номеров")

    return order_id


async def collect_and_send_result(
    order_id: int,
    lines_count: int,
    pack_name: str,
    result_filename: str,
    balance_before: float,
) -> None:
    """
    Шаг 2: ждёт готовности заказа, скачивает результат, фильтрует, отправляет в ТГ.
    Запускается параллельно для обоих паков через asyncio.gather.
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)
    date_str = get_today_date_str()

    result = await wait_for_order_completion(order_id, lines_count=lines_count, check_interval=60)

    if "error" in result:
        error_msg = result.get("error", "Unknown error")
        if "Insufficient funds" in str(error_msg):
            await send_telegram_message(f"⚠️ [{pack_name}] Недостаточно средств — результат недоступен")
        else:
            await send_telegram_message(f"❌ [{pack_name}] Ошибка ожидания: {error_msg}")
        return

    result_url = result.get("result")
    cost = result.get("cost", "0")

    if not result_url:
        await send_telegram_message(f"❌ [{pack_name}] Не получен URL результата")
        return

    original_filename = result_url.split("/")[-1]
    downloaded_path = os.path.join(RESULTS_DIR, original_filename)

    if not download_result(result_url, downloaded_path):
        await send_telegram_message(f"❌ [{pack_name}] Не удалось скачать результат")
        return

    if not result_filename:
        result_filename = f"max_ids_{pack_name}_{date_str}.txt"
    final_path = os.path.join(RESULTS_DIR, result_filename)

    filtered_count, total_from_api = filter_and_extract_ids(downloaded_path, final_path)

    balance_after_usd = check_balance()
    if balance_after_usd is None:
        balance_after_usd = balance_before - float(cost)

    charged_usd = float(cost)
    balance_rub = int(balance_after_usd * USD_TO_RUB)
    charged_rub = int(charged_usd * USD_TO_RUB)

    message = (
        f"💵Баланс: {balance_rub}р (-{charged_rub}р)\n"
        f"🧾Строк: {filtered_count:,} (активных ≤{MAX_ACTIVE_DAYS_AGO}д из {total_from_api:,})".replace(",", ".")
    )

    await send_telegram_message(message)
    await send_telegram_file(final_path, custom_filename=result_filename)

    logger.info(f"[{pack_name}] Завершено: {filtered_count} активных ID из {total_from_api}")


# Оставляем для обратной совместимости с handle_user_confirmation
async def process_checker_order(
    file_path: str,
    original_lines_count: int,
    phones_for_ac: List[str],
    pack_name: str = "pack1",
    result_filename: str = "",
):
    """Совмещённый вариант submit + collect для повторной отправки при нехватке средств."""
    balance_before = check_balance() or 0.0
    order_id = await submit_order(file_path, phones_for_ac, pack_name)
    if order_id is None:
        return
    date_str = get_today_date_str()
    if not result_filename:
        result_filename = f"max_ids_{pack_name}_{date_str}.txt"
    await collect_and_send_result(order_id, original_lines_count, pack_name, result_filename, balance_before)


async def handle_user_confirmation(user_message: str) -> bool:
    """
    Обрабатывает подтверждение пользователя на повторную отправку.
    Возвращает True если сообщение обработано.
    """
    global _waiting_for_payment_confirmation, _pending_order_id, _pending_file_path
    global _pending_original_count, _pending_phones_for_ac, _pending_pack_name, _pending_result_filename

    if not _waiting_for_payment_confirmation:
        return False

    if user_message.lower().strip() == "да":
        _waiting_for_payment_confirmation = False

        if _pending_file_path and _pending_original_count > 0:
            await process_checker_order(
                _pending_file_path,
                _pending_original_count,
                phones_for_ac=_pending_phones_for_ac,
                pack_name=_pending_pack_name,
                result_filename=_pending_result_filename,
            )

        _pending_order_id = None
        _pending_file_path = None
        _pending_original_count = 0
        _pending_phones_for_ac = []
        _pending_pack_name = ""
        _pending_result_filename = ""
        return True

    return False


async def run_max_checker():
    """
    Главная функция модуля.

    Порядок работы:
      1. Подготовить файл pack1 (Б1 + Б0).
      2. Загрузить pack1 в promouser → получить order_id1.
         Сразу после успешной загрузки записать номера pack1 в already_checked.
      3. Подготовить файл pack2 (ББ ДОП_2/3, КР 1..КР ДОП_9).
      4. Загрузить pack2 в promouser → получить order_id2.
         Сразу после успешной загрузки записать номера pack2 в already_checked.
      5. Ждать готовности pack1 и pack2 параллельно (asyncio.gather).
         Как только каждый готов — сразу отправлять результат в ТГ.
    """
    logger.info("=== Запуск max_checker ===")
    date_str = get_today_date_str()

    if not PROMO_CHECKER_KEY:
        logger.error("PROMO_CHECKER_KEY не настроен в .env")
        return

    balance_before = check_balance() or 0.0

    logger.info(f"Активные паки: {ALLOWED_PACKS}, SEND_TO_PROMOUSER={SEND_TO_PROMOUSER}")

    if not SEND_TO_PROMOUSER:
        # ── Режим прямой отправки в ТГ (без Promouser) ──────────────────────
        logger.info("SEND_TO_PROMOUSER=False — файлы отправляются сразу в ТГ")
        for pack_name, create_fn, tg_filename in [
            ("pack1", create_non_check_files,       f"1max_ids_pack1_{date_str}.txt"),
            ("pack2", create_non_check_files_pack2, f"2max_ids_pack2_{date_str}.txt"),
        ]:
            if pack_name not in ALLOWED_PACKS:
                logger.info(f"[{pack_name}] Пропущен (не в ALLOWED_PACKS)")
                continue
            logger.info(f"[{pack_name}] Подготовка файла...")
            file_path, lines_count, phones_ac = create_fn()
            if file_path and lines_count > 0:
                save_already_checked(set(phones_ac))
                logger.info(f"[{pack_name}] Записано в already_checked: {lines_count} номеров")
                await send_telegram_message(f"📦 [{pack_name}] Готов файл: {lines_count} номеров (без Promouser)")
                await send_telegram_file(file_path, custom_filename=tg_filename)
                logger.info(f"[{pack_name}] Файл отправлен в ТГ напрямую: {file_path}")
            else:
                logger.info(f"[{pack_name}] Нет номеров для отправки")
        logger.info("=== max_checker завершён (без Promouser) ===")
        return

    # ── Шаг 1: подготовка и загрузка pack1 ──────────────────────────────────
    order_id1: Optional[int] = None
    lines_count1: int = 0
    if "pack1" in ALLOWED_PACKS:
        logger.info("[pack1] Подготовка файла...")
        file_path1, lines_count1, phones_ac1 = create_non_check_files()
        if file_path1 and lines_count1 > 0:
            order_id1 = await submit_order(file_path1, phones_ac1, "pack1")
        else:
            logger.info("[pack1] Нет номеров для проверки")
    else:
        logger.info("[pack1] Пропущен (не в ALLOWED_PACKS)")

    # ── Шаг 2: подготовка и загрузка pack2 ──────────────────────────────────
    # Запускается сразу после отправки pack1, не ожидая результата от promouser
    order_id2: Optional[int] = None
    lines_count2: int = 0
    if "pack2" in ALLOWED_PACKS:
        logger.info("[pack2] Подготовка файла...")
        file_path2, lines_count2, phones_ac2 = create_non_check_files_pack2()
        if file_path2 and lines_count2 > 0:
            order_id2 = await submit_order(file_path2, phones_ac2, "pack2")
        else:
            logger.info("[pack2] Нет номеров для проверки")
    else:
        logger.info("[pack2] Пропущен (не в ALLOWED_PACKS)")

    # ── Шаг 3: параллельное ожидание и отправка результатов в ТГ ────────────
    # Каждый pack отправляется в ТГ как только готов, независимо от другого
    tasks = []
    if order_id1 is not None:
        tasks.append(collect_and_send_result(
            order_id1, lines_count1, "pack1",
            f"1max_ids_pack1_{date_str}.txt", balance_before,
        ))
    if order_id2 is not None:
        tasks.append(collect_and_send_result(
            order_id2, lines_count2, "pack2",
            f"2max_ids_pack2_{date_str}.txt", balance_before,
        ))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("=== max_checker завершён ===")


# Для интеграции с bot_master - запуск в отдельной задаче
def start_checker_task():
    """
    Запускает проверку в фоновой задаче.
    Можно вызвать из bot_master после формирования TXT файлов.
    """
    return asyncio.create_task(run_max_checker())


if __name__ == "__main__":
    # Для тестирования
    asyncio.run(run_max_checker())
