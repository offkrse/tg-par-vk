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
from datetime import datetime
from typing import Optional, Set, List, Tuple
from dotenv import load_dotenv

load_dotenv("/opt/bot/.env")

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

# Исключаемые файлы (по префиксу)
EXCLUDED_PREFIXES = ("ББ", "КР ДОП_10", "leads_sub_6", "new_subs")

# === Логирование ===
logging.basicConfig(
    filename="/opt/bot/max_checker.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("max_checker")

# Флаг ожидания подтверждения оплаты
_waiting_for_payment_confirmation = False
_pending_order_id: Optional[int] = None
_pending_file_path: Optional[str] = None
_pending_original_count: int = 0


def get_today_date_str() -> str:
    """Возвращает сегодняшнюю дату в формате DD_MM_YYYY"""
    return datetime.today().strftime("%d_%m_%Y")


def collect_phones_from_txt_files() -> Tuple[Set[str], Set[str]]:
    """
    Собирает номера из TXT файлов.
    Возвращает (все номера, номера из Б1).
    Исключает файлы с префиксами из EXCLUDED_PREFIXES.
    """
    all_phones: Set[str] = set()
    b1_phones: Set[str] = set()
    
    if not os.path.exists(SOURCE_TXT_DIR):
        logger.warning(f"Директория {SOURCE_TXT_DIR} не существует")
        return all_phones, b1_phones
    
    for filename in os.listdir(SOURCE_TXT_DIR):
        if not filename.endswith(".txt"):
            continue
            
        # Проверяем исключения
        skip = False
        for prefix in EXCLUDED_PREFIXES:
            if filename.startswith(prefix):
                skip = True
                logger.info(f"Пропускаем файл (в исключениях): {filename}")
                break
        
        if skip:
            continue
        
        filepath = os.path.join(SOURCE_TXT_DIR, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                phones = {line.strip() for line in f if line.strip()}
            
            all_phones.update(phones)
            
            # Проверяем, является ли файл Б1
            if filename.startswith("Б1"):
                b1_phones.update(phones)
                logger.info(f"Файл Б1: {filename}, номеров: {len(phones)}")
            else:
                logger.info(f"Обработан файл: {filename}, номеров: {len(phones)}")
                
        except Exception as e:
            logger.exception(f"Ошибка при чтении {filepath}: {e}")
    
    return all_phones, b1_phones


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


def create_non_check_files() -> Tuple[Optional[str], int]:
    """
    Создаёт файлы non_check_DD_MM_YYYY.txt и non_check_wd_DD_MM_YYYY.txt
    Возвращает (путь к non_check_wd файлу, количество строк в нём).
    """
    os.makedirs(TXTS_DIR, exist_ok=True)
    date_str = get_today_date_str()
    
    # Собираем номера
    all_phones, b1_phones = collect_phones_from_txt_files()
    
    if not all_phones:
        logger.warning("Нет номеров для обработки")
        return None, 0
    
    # Формируем список: сначала Б1, потом остальные
    other_phones = all_phones - b1_phones
    ordered_phones: List[str] = sorted(b1_phones) + sorted(other_phones)
    
    # Сохраняем non_check_DD_MM_YYYY.txt
    non_check_path = os.path.join(TXTS_DIR, f"non_check_{date_str}.txt")
    with open(non_check_path, "w", encoding="utf-8") as f:
        f.write("\n".join(ordered_phones))
    logger.info(f"Создан {non_check_path}: {len(ordered_phones)} номеров")
    
    # Убираем уже проверенные (с экономией памяти)
    phones_to_check = filter_already_checked(ordered_phones)
    
    # Обрезаем до 50000
    original_count = len(phones_to_check)
    if len(phones_to_check) > 50000:
        phones_to_check = phones_to_check[:50000]
        logger.info(f"Обрезано до 50000 номеров (было {original_count})")
    
    if not phones_to_check:
        logger.warning("Все номера уже проверены")
        return None, 0
    
    # Сохраняем non_check_wd_DD_MM_YYYY.txt
    non_check_wd_path = os.path.join(TXTS_DIR, f"non_check_wd_{date_str}.txt")
    with open(non_check_wd_path, "w", encoding="utf-8") as f:
        f.write("\n".join(phones_to_check))
    logger.info(f"Создан {non_check_wd_path}: {len(phones_to_check)} номеров")
    
    # Добавляем в already_checked (с разбиением на файлы)
    save_already_checked(set(phones_to_check))
    
    return non_check_wd_path, len(phones_to_check)


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


async def send_telegram_message(text: str, chat_id: str = CHECKER_CHAT_ID):
    """Отправляет сообщение в Telegram"""
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN не настроен")
        return
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
            ) as resp:
                if resp.status != 200:
                    text_resp = await resp.text()
                    logger.error(f"Ошибка отправки сообщения: {resp.status} {text_resp}")
    except Exception as e:
        logger.exception(f"Ошибка при отправке сообщения в Telegram: {e}")


async def send_telegram_file(file_path: str, caption: str = "", chat_id: str = CHECKER_CHAT_ID, custom_filename: str = None):
    """Отправляет файл в Telegram"""
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN не настроен")
        return
    
    try:
        async with aiohttp.ClientSession() as session:
            with open(file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("chat_id", chat_id)
                
                filename = custom_filename or os.path.basename(file_path)
                form.add_field("document", f, filename=filename)
                
                if caption:
                    form.add_field("caption", caption)
                
                async with session.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                    data=form
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


async def wait_for_order_completion(order_id: int, initial_delay: int = 3600, check_interval: int = 60) -> dict:
    """
    Ожидает завершения заказа.
    Начинает проверять через initial_delay секунд (60 минут),
    затем проверяет каждые check_interval секунд (1 минута).
    """
    logger.info(f"Ожидание {initial_delay} сек перед первой проверкой статуса...")
    await asyncio.sleep(initial_delay)
    
    while True:
        status_data = check_order_status(order_id)
        
        if "error" in status_data:
            logger.error(f"Ошибка при проверке статуса: {status_data}")
            await asyncio.sleep(check_interval)
            continue
        
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


async def process_checker_order(file_path: str, original_lines_count: int):
    """
    Основной процесс: отправка заказа, ожидание, получение результата.
    """
    global _waiting_for_payment_confirmation, _pending_order_id, _pending_file_path, _pending_original_count
    
    if not PROMO_CHECKER_KEY:
        logger.error("PROMO_CHECKER_KEY не настроен в .env")
        return
    
    # Проверяем баланс перед отправкой
    balance_before = check_balance()
    if balance_before is None:
        await send_telegram_message("❌ Не удалось проверить баланс")
        return
    
    # Отправляем заказ
    order_id = send_order(file_path, service_type=19, force=1)
    if order_id is None:
        await send_telegram_message("❌ Не удалось создать заказ")
        return
    
    logger.info(f"Заказ {order_id} отправлен, ожидаем выполнения...")
    
    # Ожидаем завершения (60 минут до первой проверки, затем каждую минуту)
    result = await wait_for_order_completion(order_id, initial_delay=3600, check_interval=60)
    
    if "error" in result:
        error_msg = result.get("error", "Unknown error")
        if "Insufficient funds" in str(error_msg):
            # Недостаточно средств
            _waiting_for_payment_confirmation = True
            _pending_order_id = order_id
            _pending_file_path = file_path
            _pending_original_count = original_lines_count
            await send_telegram_message("⚠️ Недостаточно средств на балансе, повторить?")
            return
        else:
            await send_telegram_message(f"❌ Ошибка: {error_msg}")
            return
    
    # Получаем URL результата
    result_url = result.get("result")
    cost = result.get("cost", "0")
    
    if not result_url:
        await send_telegram_message("❌ Не получен URL результата")
        return
    
    # Скачиваем результат
    os.makedirs(RESULTS_DIR, exist_ok=True)
    date_str = get_today_date_str()
    
    # Извлекаем оригинальное имя файла из URL
    original_filename = result_url.split("/")[-1]
    downloaded_path = os.path.join(RESULTS_DIR, original_filename)
    
    if not download_result(result_url, downloaded_path):
        await send_telegram_message("❌ Не удалось скачать результат")
        return
    
    # Считаем строки в результате
    result_lines = count_lines(downloaded_path)
    
    # Получаем баланс после (в долларах)
    balance_after_usd = check_balance()
    if balance_after_usd is None:
        balance_after_usd = balance_before - float(cost)
    
    charged_usd = float(cost)
    
    # Конвертируем в рубли (целые числа)
    balance_rub = int(balance_after_usd * USD_TO_RUB)
    charged_rub = int(charged_usd * USD_TO_RUB)
    
    # Формируем сообщение
    message = (
        f"💵Баланс: {balance_rub}р (-{charged_rub}р)\n"
        f"🧾Строк: {result_lines:,} (из {original_lines_count:,})".replace(",", ".")
    )
    
    # Отправляем сообщение
    await send_telegram_message(message)
    
    # Сохраняем и отправляем файл под именем max_ids_DD_MM_YYYY.txt
    final_filename = f"max_ids_{date_str}.txt"
    final_path = os.path.join(RESULTS_DIR, final_filename)
    
    # Копируем файл под новым именем (если это не тот же файл)
    if downloaded_path != final_path:
        import shutil
        shutil.copy2(downloaded_path, final_path)
    
    # Отправляем файл
    await send_telegram_file(final_path, custom_filename=final_filename)
    
    logger.info(f"Проверка завершена: {result_lines} строк из {original_lines_count}")


async def handle_user_confirmation(user_message: str) -> bool:
    """
    Обрабатывает подтверждение пользователя на повторную отправку.
    Возвращает True если сообщение обработано.
    """
    global _waiting_for_payment_confirmation, _pending_order_id, _pending_file_path, _pending_original_count
    
    if not _waiting_for_payment_confirmation:
        return False
    
    if user_message.lower().strip() == "да":
        _waiting_for_payment_confirmation = False
        
        if _pending_file_path and _pending_original_count > 0:
            # Повторно отправляем заказ
            await process_checker_order(_pending_file_path, _pending_original_count)
        
        _pending_order_id = None
        _pending_file_path = None
        _pending_original_count = 0
        return True
    
    return False


async def run_max_checker():
    """
    Главная функция модуля.
    Вызывается после формирования TXT файлов в bot_master.
    """
    logger.info("=== Запуск max_checker ===")
    
    # Создаём файлы для проверки
    file_path, lines_count = create_non_check_files()
    
    if not file_path or lines_count == 0:
        logger.info("Нет номеров для проверки")
        return
    
    # Запускаем процесс проверки
    await process_checker_order(file_path, lines_count)
    
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
