#!/usr/bin/env python3
import os
import asyncio
import logging
import random
import pandas as pd
import requests
import boto3
import aiohttp
import time
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from datetime import datetime, timedelta
from telethon import TelegramClient
from collections import defaultdict

# Импорт модуля проверки номеров
try:
    from max_checker import start_checker_task
    MAX_CHECKER_AVAILABLE = True
except ImportError:
    MAX_CHECKER_AVAILABLE = False

load_dotenv()

VersionBotMaster = "2.8"
# === Настройки ===
DOWNLOAD_FROM_TG = True  # Если True — скачиваем CSV из Telegram, если False — берём TXT из /opt/bot/txt/
SEND_FILES_TO_TELEGRAM = True  # Если True — файлы отправляются в Telegram
VK_UPLOAD = True  # Если True — файлы загружаются в VK кабинеты
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
CHANNEL_NAME = os.getenv("CHANNEL_NAME")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

# Второй (резервный/отдельный) бакет, из которого читаем new_subs_...txt из корня
NEW_S3_BUCKET = os.getenv("NEW_S3_BUCKET")
NEW_S3_ENDPOINT = os.getenv("NEW_S3_ENDPOINT")
NEW_S3_ACCESS_KEY = os.getenv("NEW_S3_ACCESS_KEY")
NEW_S3_SECRET_KEY = os.getenv("NEW_S3_SECRET_KEY")

# поддержка нескольких токенов через CSV-вход в .env
# VK_ACCESS_TOKENS = token1,token2,token3
VK_ACCESS_TOKENS = [t.strip() for t in os.getenv("VK_ACCESS_TOKEN", "").split(",") if t.strip()]

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# error bot
ERROR_BOT_TOKEN = os.getenv("ERROR_BOT_TOKEN")
ERROR_CHAT_ID = os.getenv("ERROR_CHAT_ID")

BASE_DATE = datetime(2025, 7, 14)
BASE_NUMBER = 53

# === Логирование ===
logging.basicConfig(
    filename="/opt/bot/bot_master.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("bot_master")
# === S3 клиент ===
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT if S3_ENDPOINT else None,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# === VK API URLs ===
BASE_URL_V3 = "https://ads.vk.com/api/v3"
BASE_URL_V2 = "https://ads.vk.com/api/v2"

MAX_UPLOADS_PER_TOKEN = 20  # максимум загрузок на один кабинет
VK_UPLOAD_COUNTERS = {}

# === HTTP с повторами и обработкой rate limit ===
RETRY_COUNT = 3
RETRY_BACKOFF = 2
RATE_LIMIT_SLEEP = (10, 30)

def req_with_retry(
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    files: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    timeout: int = 60
) -> requests.Response:
    last_exc: Optional[Exception] = None

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.request(
                method, url,
                headers=headers,
                params=params,
                json=json_body,
                data=data,
                files=files,
                timeout=timeout
            )

            # === Обработка rate limit ===
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                logger.warning(f"⚠️ VK API rate limit (429). Пауза {retry_after}s перед повтором...")
                time.sleep(retry_after)
                continue

            try:
                result = resp.json()
                vk_error = result.get("error", {})
                if isinstance(vk_error, dict) and vk_error.get("error_code") in (9, 29):
                    sleep_for = random.uniform(*RATE_LIMIT_SLEEP)
                    logger.warning(f"⚠️ VK flood control ({vk_error.get('error_code')}). "
                                   f"Пауза {sleep_for:.1f}s перед повтором...")
                    time.sleep(sleep_for)
                    continue
            except Exception:
                pass

            if resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} {resp.text}")

            return resp

        except Exception as e:
            last_exc = e
            sleep_for = RETRY_BACKOFF ** (attempt - 1)
            logger.warning(f"{method} {url} попытка {attempt}/{RETRY_COUNT} не удалась: {e}. "
                           f"Повтор через {sleep_for:.1f}s")
            time.sleep(sleep_for)

    assert last_exc is not None
    raise last_exc


# === Утилиты ===
def send_error_sync(message: str):
    """Синхронная отправка ошибки error-ботом (используется в sync коде).
       Отправка без звука (disable_notification)."""
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logging.warning(f"ERROR BOT not configured, would send: {message}")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
            data={"chat_id": ERROR_CHAT_ID, "text": f"ERROR /bot_master.py : {message}", "disable_notification": True}
        )
        if resp.status_code != 200:
            logging.error(f"Не удалось отправить ошибку в error-bot: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.exception(f"Ошибка при отправке ошибки в error-bot: {e}")


async def send_error_async(message: str):
    """Асинхронная отправка ошибки (используется в async коде).
       Отправка без звука (disable_notification)."""
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logging.warning(f"ERROR BOT not configured, would send: {message}")
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
                data={"chat_id": ERROR_CHAT_ID, "text": f"ERROR /bot_master.py : {message}", "disable_notification": "true"}
            )
    except Exception:
        logging.exception("Ошибка при send_error_async")
        # fallback to sync
        send_error_sync(message)


def get_day_number(today: datetime) -> int:
    delta = (today - BASE_DATE).days
    return BASE_NUMBER + delta


def get_output_filename(file_name: str, day_number: int):
    if "MFO5" in file_name:
        return f"Б0 ({day_number}).txt", "Б0"
    elif "6_web" in file_name:
        return None, "6_web"
    elif "broker" in file_name:
        return None, "broker"
    elif any(x in file_name for x in ["253", "345"]):
        return f"Б1 ({day_number}).txt", "Б1"
    else:
        return None, None


async def download_latest_csv(to_folder="/opt/bot/csv"):
    """Скачивает CSV из Telegram в папку to_folder (убирает дубликаты по исходному имени файла)."""
    await asyncio.sleep(random.uniform(2, 4))
    os.makedirs(to_folder, exist_ok=True)
    logging.info("📥 Подключаемся к Telegram и скачиваем CSV в %s", to_folder)
    client = TelegramClient("session_master", API_ID, API_HASH)
    await client.start(PHONE)

    today = datetime.today()
    date_suffix = today.strftime("(%d.%m)")
    seen_names = set()
    result_files = []

    try:
        async for msg in client.iter_messages(CHANNEL_NAME, limit=7):
            try:
                if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
                    orig_name = msg.file.name
                    #ДЛЯ ОТМЕНЫ ПРАВИЛА 389 и 390 УБРАТЬ 3 следующие строчки
                    if orig_name in ("389.csv", "390.csv"):
                        logging.info("Пропускаем файл по имени: %s", orig_name)
                        continue
                    if orig_name in seen_names:
                        logging.info("Пропускаем дубликат по имени: %s", orig_name)
                        continue
                    seen_names.add(orig_name)

                    filename = orig_name.replace(".csv", f" {date_suffix}.csv")
                    path = os.path.join(to_folder, filename)
                    await msg.download_media(file=path)
                    if "6_web" in orig_name:
                        await asyncio.sleep(90)
                    result_files.append(path)
                    logging.info("✅ Скачан %s", filename)
                    await asyncio.sleep(random.uniform(10, 20))
            except Exception as e:
                logging.exception("Ошибка при скачивании одного сообщения")
                await send_error_async(f"Ошибка при скачивании сообщения: {e}")
    finally:
        await client.disconnect()

    return result_files


def broker_channel_group(cid: str, day_number: int) -> str:
    """Определяет название TXT файла по channel_id"""
    cid = str(cid)
    mapping = {
        "КР ДОП_3": [915, 917, 918, 919],
        "КР 1": [12063],
        "КР 2": [11896],
        "КР ДОП_4": [3587, 7389, 7553, 8614, 8732],
        "КР ДОП_5": [9189, 9190, 9191, 9192, 9193, 9194, 9413, 9441, 9443, 9453, 9889, 9899],
        "КР ДОП_6": [10141, 10240, 11682, 11729],
        "КР ДОП_8": [12873],
        "КР ДОП_9": [16263],
    }
    for name, ids in mapping.items():
        if cid is None:
            continue
        if str(cid).isdigit() and int(cid) in ids:
            return f"{name} ({day_number}).txt"
    return f"КР ДОП_10 ({day_number}).txt"


def process_csv_files(files):
    """Обработка CSV -> TXT, защита от пустых/отсутствующих phone, удаление дубликатов номеров."""
    today = datetime.today()
    day_number = get_day_number(today)
    output_data = defaultdict(set)
    approve_phones = set()  # телефоны из 253.csv для LAL-файла

    for file in files:
        try:
            df = pd.read_csv(file)
            fname = os.path.basename(file)

            # Проверки: пустой, нет колонки phone или все phone пусты
            if df.empty or "phone" not in df.columns or df["phone"].dropna().astype(str).str.strip().eq("").all():
                msg = f"Пропущен пустой или некорректный CSV: {fname}"
                logging.warning(msg)
                send_error_sync(msg)
                continue

            output_name, group_key = get_output_filename(fname, day_number)
            if not group_key:
                logging.info("Файл %s не подпадает под обработку (имя): %s", fname, group_key)
                continue

            if group_key == "broker":
                if "channel_id" not in df.columns:
                    msg = f"В {fname} отсутствует column 'channel_id'"
                    logging.warning(msg)
                    send_error_sync(msg)
                    continue
                for _, row in df.iterrows():
                    phone = str(row.get("phone", "")).replace("+", "").strip()
                    if not phone:
                        continue
                    cid = row.get("channel_id", "")
                    txt_name = broker_channel_group(cid, day_number)
                    output_data[txt_name].add(phone)

            elif group_key == "6_web":
                if "channel_id" not in df.columns:
                    msg = f"В {fname} нет столбца channel_id"
                    logging.warning(msg)
                    send_error_sync(msg)
                    continue
                for _, row in df.iterrows():
                    phone = str(row.get("phone", "")).replace("+", "").strip()
                    if not phone:
                        continue
                    ch = str(row.get("channel_id", "")).strip()
                    if ch == "15883":
                        group = "ББ"
                    elif ch == "15686":
                        group = "ББ ДОП_1"
                    elif ch == "15273":
                        group = "ББ ДОП_2"
                    else:
                        group = "ББ ДОП_3"
                    output_data[f"{group} ({day_number}).txt"].add(phone)

            else:
                phones = [str(p).replace("+", "").strip() for p in df["phone"].dropna()]
                phones = [p for p in phones if p]
                if not phones:
                    msg = f"Нет номеров в {fname}"
                    logging.warning(msg)
                    send_error_sync(msg)
                    continue
                if output_name:
                    output_data[output_name].update(phones)
                # ДОП обработка: если это 253.csv → собираем телефоны отдельно
                if "253" in fname:
                    approve_phones.update(phones)


        except Exception as e:
            msg = f"Ошибка при обработке {file}: {e}"
            logging.exception(msg)
            send_error_sync(msg)

    os.makedirs("/opt/bot/txt", exist_ok=True)
    txt_files = []
    for name, phones in output_data.items():
        path = os.path.join("/opt/bot/txt", name)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(phones)))
        txt_files.append(path)
        logging.info("Сохранён TXT: %s (%d номеров)", name, len(phones))
        
    # === Сохранение b_approve_* в отдельную папку (НЕ в pipeline) ===
    if approve_phones:
        os.makedirs("/opt/bot/txt_for_lal", exist_ok=True)
        date_str = today.strftime("%d_%m_%Y")
        approve_path = f"/opt/bot/txt_for_lal/b_approve_{date_str}.txt"
    
        with open(approve_path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(approve_phones)))
    
        logging.info(
            "Сохранён LAL файл (НЕ идёт в S3/VK/TG): %s (%d номеров)",
            approve_path,
            len(approve_phones)
        )

    return txt_files


async def send_file_to_telegram(file_path: str, chat_id: str = CHAT_ID):
    """Отправка файла в Telegram (основной бот). Отправка без звука (disable_notification)."""
    if not BOT_TOKEN or not chat_id:
        logging.warning("Telegram BOT_TOKEN or CHAT_ID not configured")
        return
    async with aiohttp.ClientSession() as session:
        try:
            with open(file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("chat_id", chat_id)
                form.add_field("document", f)
                form.add_field("disable_notification", "true")
                async with session.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument", data=form
                ) as resp:
                    if resp.status != 200:
                        txt = await resp.text()
                        msg = f"Ошибка Telegram API при отправке {file_path}: {resp.status} {txt}"
                        logging.error(msg)
                        await send_error_async(msg)
        except Exception as e:
            logging.exception("Ошибка при отправке файла в Telegram")
            await send_error_async(f"Ошибка при отправке файла в Telegram {file_path}: {e}")


def upload_to_s3(file_path):
    """Загрузка в S3: txt -> /txt. CSV НЕ загружается (игнорируются)."""
    filename = os.path.basename(file_path)
    # загружаем только .txt
    if not filename.lower().endswith(".txt"):
        logging.info("Пропускаем загрузку в S3 (не TXT): %s", filename)
        return
    folder = "txt"
    key = f"{folder}/{filename}"
    try:
        s3.upload_file(file_path, S3_BUCKET, key)
        logging.info("Загружен в S3: %s", key)
    except Exception as e:
        msg = f"Ошибка загрузки {filename} в S3: {e}"
        logging.exception(msg)
        send_error_sync(msg)


def upload_user_list_vk(file_path, list_name, vk_token, list_type="phones"):
    """Загружает список в конкретный VK кабинет (token). Возвращает list_id."""
    url = f"{BASE_URL_V3}/remarketing/users_lists.json"
    headers = {"Authorization": f"Bearer {vk_token}"}
    files = {"file": open(file_path, "rb")}
    data = {"name": list_name, "type": list_type}
    try:
        resp = req_with_retry("POST", url, headers=headers, files=files, data=data, timeout=60)
    finally:
        files["file"].close()

    try:
        result = resp.json()
    except Exception:
        raise Exception(f"Некорректный ответ VK: {resp.text}")
    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        raise Exception(f"Ошибка загрузки списка: {result}")
    list_id = result.get("id")
    if not list_id:
        raise Exception(f"Не удалось получить ID списка: {result}")
    return list_id


def create_segment_vk(list_id, segment_name, vk_token):
    """Создаёт сегмент в VK для конкретного кабинета."""
    url = f"{BASE_URL_V2}/remarketing/segments.json"
    headers = {"Authorization": f"Bearer {vk_token}", "Content-Type": "application/json"}
    payload = {
        "name": segment_name,
        "pass_condition": 1,
        "relations": [
            {"object_type": "remarketing_users_list", "params": {"source_id": list_id, "type": "positive"}}
        ],
    }
    resp = req_with_retry("POST", url, headers=headers, json_body=payload, timeout=60)
    result = resp.json()
    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        raise Exception(f"Ошибка создания сегмента: {result}")
    return result.get("id")

async def upload_to_all_vk_and_get_one_sharing_key(file_path, vk_tokens, *, list_name=None, list_type="phones", segment_prefix="LAL "):
    """
    Загружает файл в каждый VK кабинет из vk_tokens.
    Возвращает (first_success_list_id, first_token) для генерации единого sharing key (если надо).
    Позволяет явно задать list_name/list_type и префикс сегмента.
    """
    file_name = os.path.basename(file_path)
    base_list_name = os.path.splitext(file_name)[0]
    list_name = list_name or base_list_name
    segment_name = f"{segment_prefix}{list_name}"

    first_success = None  # tuple (list_id, token)
    for token in vk_tokens:
        if token not in VK_UPLOAD_COUNTERS:
            VK_UPLOAD_COUNTERS[token] = 0

        # 🔒 Проверка лимита
        if VK_UPLOAD_COUNTERS[token] >= MAX_UPLOADS_PER_TOKEN:
            logging.warning(
                f"⚠️ Превышен лимит {MAX_UPLOADS_PER_TOKEN} загрузок для VK кабинета {token[:8]}... Пропускаем."
            )
            continue

        try:
            list_id = upload_user_list_vk(file_path, list_name, token, list_type=list_type)
            create_segment_vk(list_id, segment_name, token)
            VK_UPLOAD_COUNTERS[token] += 1  # ✅ инкремент при успехе
            logging.info("VK upload OK for token (truncated): %s ... list_id=%s", token[:8], list_id)
            if first_success is None:
                first_success = (list_id, token)
        except Exception as e:
            msg = f"Ошибка VK upload {file_name} для токена {token[:8]}: {e}"
            logging.exception(msg)
            send_error_sync(msg)

    return first_success



def order_txt_files(files):
    """
    Возвращает отсортированный список txt по требуемому приоритету.
    Оставляет только файлы, совпадающие с одним из ожидаемых префиксов.
    """
    # Приоритетный список префиксов (сначала — более высокий приоритет)
    # Для КР и Б префиксы могут содержать номер дня, уберём его при сравнении
    priority = [
        "КР ДОП_10", "КР ДОП_9", "КР ДОП_8", "КР ДОП_7", "КР ДОП_6",
        "КР ДОП_5", "КР ДОП_4", "КР ДОП_3", "КР 2", "КР 1",
        "ББ ДОП_3", "ББ ДОП_2", "ББ", "Б1", "Б0"
    ]

    def key_for_path(p):
        name = os.path.basename(p)
        # Удаляем расширение и возможный суффикс вида " (NN).txt"
        base = name.rsplit(".", 1)[0]
        # Уберём окончание " (число)" если есть
        if base.endswith(")"):
            # разделить по " (" и взять начало
            parts = base.split(" (")
            base_short = parts[0]
        else:
            base_short = base
        # Для сопоставления с приоритетом ищем первое совпадение
        try:
            idx = priority.index(base_short)
            return idx
        except ValueError:
            # нет в приоритете — ставим большой индекс (после всех)
            return len(priority) + 1000

    return sorted([p for p in files], key=key_for_path)


async def process_previous_day_file():
    """
    Проверяет наличие файла leads_sub6 за вчерашний день.
    Возвращает путь, если файл найден (отправка и VK загрузка происходят позже).
    """
    yesterday = datetime.today() - timedelta(days=1)
    file_path = f"/opt/leads_postback/data/leads_sub6_{yesterday.strftime('%d.%m.%Y')}.txt"
    if not os.path.exists(file_path):
        logging.info("Файл leads_sub6 за вчера не найден: %s", file_path)
        return None
    logging.info("Найден leads_sub6 файл за вчера: %s", file_path)
    return file_path



def download_new_subs_from_s3(to_folder="/opt/bot/new_subs"):
    """
    Скачивает файл new_subs_DD_MM_YYYY.txt из корня дополнительного S3 бакета.
    Имя файла формируется для вчерашней даты.

    Требуется установить окружение NEW_S3_BUCKET. Если не задано — ничего не делаем.
    Можно задать отдельные креды и endpoint через NEW_S3_ENDPOINT, NEW_S3_ACCESS_KEY, NEW_S3_SECRET_KEY.
    """
    if not NEW_S3_BUCKET:
        logging.info("NEW_S3_BUCKET не задан, пропускаем скачивание new_subs.")
        return None

    yesterday = datetime.today() - timedelta(days=1)
    filename = f"new_subs_{yesterday.strftime('%d_%m_%Y')}.txt"
    os.makedirs(to_folder, exist_ok=True)
    local_path = os.path.join(to_folder, filename)

    # Подготовка клиента: если заданы отдельные креды/endpoint — создаём новый клиент
    try:
        if NEW_S3_ACCESS_KEY or NEW_S3_SECRET_KEY or NEW_S3_ENDPOINT:
            client = boto3.client(
                "s3",
                endpoint_url=NEW_S3_ENDPOINT if NEW_S3_ENDPOINT else (S3_ENDPOINT if S3_ENDPOINT else None),
                aws_access_key_id=NEW_S3_ACCESS_KEY if NEW_S3_ACCESS_KEY else S3_ACCESS_KEY,
                aws_secret_access_key=NEW_S3_SECRET_KEY if NEW_S3_SECRET_KEY else S3_SECRET_KEY
            )
        else:
            client = s3

        client.download_file(NEW_S3_BUCKET, filename, local_path)
        logging.info("Скачан файл new_subs из бакета %s: %s", NEW_S3_BUCKET, filename)
        return local_path
    except Exception as e:
        logging.exception("Ошибка при скачивании new_subs из S3: %s", e)
        send_error_sync(f"Ошибка при скачивании new_subs из S3: {e}")
        return None



def cleanup_files(files):
    """Удаляет файлы из переданного списка, логируя ошибки, работает безопасно."""
    for f in files:
        try:
            if os.path.exists(f):
                os.remove(f)
                logging.info("Удалён файл: %s", f)
        except Exception:
            logging.exception("Ошибка при удалении файла: %s", f)


def cleanup_previous_day_txt_files():
    """
    Удаляет TXT файлы за предыдущий день из /opt/bot/txt.
    Определяет вчерашний номер дня и удаляет файлы с этим номером.
    """
    txt_dir = "/opt/bot/txt"
    if not os.path.exists(txt_dir):
        return
    
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_day_number = get_day_number(yesterday)
    pattern = f"({yesterday_day_number}).txt"
    
    for filename in os.listdir(txt_dir):
        if filename.endswith(pattern):
            filepath = os.path.join(txt_dir, filename)
            try:
                os.remove(filepath)
                logging.info("Удалён вчерашний TXT файл: %s", filename)
            except Exception:
                logging.exception("Ошибка при удалении файла: %s", filepath)


# === Главный процесс ===
async def main():
    logging.info("=== 🚀 Запуск bot_master ===")

    # 0) Пробуем скачать new_subs из дополнительного бакета (если задан)
    new_subs_path = download_new_subs_from_s3()

    # 1) Сначала обрабатываем файл leads_sub6 вчерашнего дня (Только TG, путь вернём для VK)
    leads_sub6_path = await process_previous_day_file()

    csv_files = []
    txt_files = []

    if DOWNLOAD_FROM_TG:
        # 2) Скачиваем CSV из Telegram в /opt/bot/csv
        csv_files = await download_latest_csv("/opt/bot/csv")
        if not csv_files:
            msg = "CSV файлы не найдены в Telegram."
            logging.warning(msg)
            await send_error_async(msg)
            return

        # 3) Обрабатываем CSV -> TXT
        txt_files = process_csv_files(csv_files)
        if not txt_files:
            msg = "Не получили TXT файлы после обработки CSV."
            logging.warning(msg)
            await send_error_async(msg)
            cleanup_files(csv_files)
            return

        # 4) Загрузка TXT в S3 (CSV не трогаем)
        for f in txt_files:
            try:
                upload_to_s3(f)
            except Exception as e:
                logging.exception("Ошибка загрузки в S3")
                send_error_sync(f"Ошибка загрузки в S3 {f}: {e}")
    else:
        # Берём существующие TXT файлы из /opt/bot/txt/
        logging.info("⏭️ Скачивание из Telegram пропущено (DOWNLOAD_FROM_TG=False)")
        txt_dir = "/opt/bot/txt"
        if os.path.exists(txt_dir):
            txt_files = [
                os.path.join(txt_dir, f) 
                for f in os.listdir(txt_dir) 
                if f.endswith(".txt")
            ]
            logging.info(f"Найдено {len(txt_files)} TXT файлов в {txt_dir}")
        
        if not txt_files:
            msg = "TXT файлы не найдены в /opt/bot/txt/"
            logging.warning(msg)
            await send_error_async(msg)
            return

    # 5) Сортируем TXT файлы по требуемому порядку
    txt_files_ordered = order_txt_files(txt_files)

    # 5.1) Запускаем max_checker параллельно (формирование файлов для проверки номеров)
    checker_task = None
    if MAX_CHECKER_AVAILABLE:
        try:
            checker_task = start_checker_task()
            logging.info("🔍 Запущен max_checker в фоновом режиме")
        except Exception as e:
            logging.exception("Ошибка запуска max_checker")
            await send_error_async(f"Ошибка запуска max_checker: {e}")

    # 6) Подготавливаем общий список файлов, которые надо:
    #    - СНАЧАЛА отправить в TG (все)
    #    - ПОТОМ загрузить в VK (все, тем же порядком)
    files_pipeline = []

    # Первым идёт new_subs (если найден)
    if new_subs_path and os.path.exists(new_subs_path):
        files_pipeline.append(new_subs_path)

    if leads_sub6_path and os.path.exists(leads_sub6_path):
        files_pipeline.append(leads_sub6_path)

    files_pipeline.extend(txt_files_ordered)

    # 7) ЭТАП 1 — сначала отправляем ВСЕ файлы в Telegram (без звука)
    if SEND_FILES_TO_TELEGRAM:
        for path in files_pipeline:
            try:
                await send_file_to_telegram(path)
            except Exception as e:
                logging.exception("Ошибка отправки в Telegram")
                await send_error_async(f"Ошибка при отправке файла в Telegram {path}: {e}")
    else:
        logging.info("⏭️ Отправка файлов в Telegram отключена (SEND_FILES_TO_TELEGRAM=False)")

    # 8) ЭТАП 2 — затем загружаем ВСЕ файлы в VK ADS (каждый файл — во все кабинеты)
    #    Для leads_sub6 нужен особый list_type/name, для остальных — по умолчанию.
    first_success = None
    if VK_UPLOAD:
        for path in files_pipeline:
            fname = os.path.basename(path)
            try:
                if fname.startswith("leads_sub6_"):
                    # Нейминг, как был раньше
                    date_part = fname.replace("leads_sub6_", "").replace(".txt", "")
                    custom_list_name = f"ls6_{date_part}"
                    res = await upload_to_all_vk_and_get_one_sharing_key(
                        path, VK_ACCESS_TOKENS,
                        list_name=custom_list_name,
                        list_type="vk",
                        segment_prefix="LAL "
                    )
                elif fname.startswith("new_subs_"):
                    # Для new_subs используем list_type="vk" по требованию
                    res = await upload_to_all_vk_and_get_one_sharing_key(
                        path, VK_ACCESS_TOKENS,
                        list_name=None,
                        list_type="vk",
                        segment_prefix="LAL "
                    )
                else:
                    # Обычные TXT (типы телефонов)
                    res = await upload_to_all_vk_and_get_one_sharing_key(
                        path, VK_ACCESS_TOKENS,
                        list_name=None,
                        list_type="phones",
                        segment_prefix="LAL "
                    )
                if res and first_success is None:
                    first_success = res
            except Exception as e:
                logging.exception("Ошибка VK загрузки")
                send_error_sync(f"Ошибка VK загрузки {fname}: {e}")
    else:
        logging.info("⏭️ Загрузка в VK пропущена (VK_UPLOAD=False)")

    # Удаляем локальную копию new_subs после отправки в TG и загрузки в VK
    try:
        if new_subs_path and os.path.exists(new_subs_path):
            os.remove(new_subs_path)
            logging.info("Удалён локальный new_subs файл: %s", new_subs_path)
    except Exception:
        logging.exception("Ошибка при удалении new_subs файла")

    # 9) Ожидаем завершения max_checker ПЕРЕД удалением файлов
    #    ВАЖНО: ждём реально, иначе systemd oneshot завершит процесс
    if checker_task is not None:
        logging.info("⏳ Ожидаем завершения max_checker (60+ минут)...")
        try:
            await checker_task
            logging.info("✅ max_checker завершён")
        except Exception as e:
            logging.exception(f"Ошибка в max_checker: {e}")
            await send_error_async(f"Ошибка в max_checker: {e}")

    # 10) Очистка временных файлов — удаляем CSV сегодня (если скачивали), TXT за ВЧЕРА
    try:
        if DOWNLOAD_FROM_TG and csv_files:
            cleanup_files(csv_files)  # CSV удаляем только если скачивали из TG
        # TXT за вчерашний день удаляем
        cleanup_previous_day_txt_files()
    except Exception:
        logging.exception("Ошибка при финальной очистке файлов")

    logging.info("✅ Все задачи завершены.")



if __name__ == "__main__":
    asyncio.run(main())
