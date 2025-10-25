#!/usr/bin/env python3
import os
import asyncio
import logging
import random
import pandas as pd
import requests
import boto3
import aiohttp
from dotenv import load_dotenv
from datetime import datetime, timedelta
from telethon import TelegramClient
from collections import defaultdict

load_dotenv()

# === Настройки ===
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
CHANNEL_NAME = os.getenv("CHANNEL_NAME")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

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


# === Утилиты ===
def send_error_sync(message: str):
    """Синхронная отправка ошибки error-ботом (используется в sync коде)."""
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logging.warning(f"ERROR BOT not configured, would send: {message}")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
            data={"chat_id": ERROR_CHAT_ID, "text": f"ERROR /bot_master.py : {message}"}
        )
        if resp.status_code != 200:
            logging.error(f"Не удалось отправить ошибку в error-bot: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.exception(f"Ошибка при отправке ошибки в error-bot: {e}")


async def send_error_async(message: str):
    """Асинхронная отправка ошибки (используется в async коде)."""
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logging.warning(f"ERROR BOT not configured, would send: {message}")
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
                data={"chat_id": ERROR_CHAT_ID, "text": f"ERROR /bot_master.py : {message}"}
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
    """
    Скачивает CSV файлы из S3 в локальную папку to_folder.
    Копирует только новые файлы, убирает дубликаты по имени.
    """
    os.makedirs(to_folder, exist_ok=True)
    logging.info("📥 Скачиваем CSV из S3 в %s", to_folder)

    try:
        # Получаем список всех объектов в бакете /csv/
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="csv/")
        contents = response.get("Contents", [])
        if not contents:
            logging.warning("⚠️ В S3 нет файлов в папке /csv/")
            return []

        # Берем последние 7 файлов по дате модификации
        contents = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[:7]

        seen_names = set()
        result_files = []
        today = datetime.today()
        date_suffix = today.strftime("(%d.%m)")

        for obj in contents:
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue
            orig_name = os.path.basename(key)
            if orig_name in seen_names:
                logging.info("Пропускаем дубликат по имени: %s", orig_name)
                continue
            seen_names.add(orig_name)

            # добавляем дату к названию файла
            filename = orig_name.replace(".csv", f" {date_suffix}.csv")
            local_path = os.path.join(to_folder, filename)

            # скачиваем из S3
            s3.download_file(S3_BUCKET, key, local_path)
            result_files.append(local_path)
            logging.info("✅ Скачан %s из S3", filename)

        return result_files

    except Exception as e:
        msg = f"Ошибка при скачивании CSV из S3: {e}"
        logging.exception(msg)
        await send_error_async(msg)
        return []


def broker_channel_group(cid: str, day_number: int) -> str:
    """Определяет название TXT файла по channel_id"""
    cid = str(cid)
    mapping = {
        "КР ДОП_3": [915, 917, 918, 919],
        "КР 1": [12063],
        "КР 2": [11896],
        "КР ДОП_4": [3587, 7389, 7553, 8614, 8732],
        "КР ДОП_5": [9189, 9190, 9191, 9192, 9193, 9194, 9413, 9441, 9443, 9453, 9889, 9899],
        "КР ДОП_6": [10141, 10240],
        "КР ДОП_7": [11682, 11729],
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
    return txt_files


async def send_file_to_telegram(file_path: str, chat_id: str = CHAT_ID):
    """Отправка файла в Telegram (основной бот)."""
    if not BOT_TOKEN or not chat_id:
        logging.warning("Telegram BOT_TOKEN or CHAT_ID not configured")
        return
    async with aiohttp.ClientSession() as session:
        try:
            with open(file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("chat_id", chat_id)
                form.add_field("document", f)
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
    """Загрузка в S3: txt -> /txt, csv -> /csv"""
    filename = os.path.basename(file_path)
    folder = "txt" if filename.lower().endswith(".txt") else "csv"
    key = f"{folder}/{filename}"
    try:
        s3.upload_file(file_path, S3_BUCKET, key)
        logging.info("Загружен в S3: %s", key)
    except Exception as e:
        msg = f"Ошибка загрузки {filename} в S3: {e}"
        logging.exception(msg)
        send_error_sync(msg)


def upload_user_list_vk(file_path, list_name, vk_token):
    """
    Загружает список в конкретный VK кабинет (token).
    Возвращает list_id или выбрасывает Exception.
    Добавлено расширенное логирование.
    """
    url = f"{BASE_URL_V3}/remarketing/users_lists.json"
    headers = {"Authorization": f"Bearer {vk_token}"}
    files = {"file": open(file_path, "rb")}
    data = {"name": list_name, "type": "phones"}

    logging.info(f"📤 [VK_UPLOAD] Начало загрузки {file_path} -> {url}")
    logging.info(f"  list_name={list_name}")
    logging.info(f"  headers={headers}")
    logging.info(f"  data={data}")

    try:
        resp = requests.post(url, headers=headers, files=files, data=data, timeout=60)
    except Exception as e:
        logging.exception(f"🚫 [VK_UPLOAD] Ошибка сети при загрузке {file_path}: {e}")
        raise

    finally:
        files["file"].close()

    try:
        result = resp.json()
    except Exception:
        logging.error(f"🚫 [VK_UPLOAD] Некорректный JSON ответ VK: {resp.text}")
        raise Exception(f"Некорректный ответ VK: {resp.text}")

    # Подробный лог VK-ответа
    logging.info(f"📩 [VK_UPLOAD] Ответ VK status={resp.status_code}: {result}")

    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        err_text = result.get("error_description") or str(result)
        raise Exception(f"Ошибка загрузки списка (HTTP {resp.status_code}): {err_text}")

    list_id = result.get("id")
    if not list_id:
        raise Exception(f"Не удалось получить ID списка из ответа VK: {result}")

    logging.info(f"✅ [VK_UPLOAD] Файл {file_path} успешно загружен, list_id={list_id}")
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
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    result = resp.json()
    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        raise Exception(f"Ошибка создания сегмента: {result}")
    return result.get("id")


def generate_sharing_key_for_owner(object_type: str, object_id: int, vk_token):
    """Генерирует sharing key (для владельца) используя переданный токен."""
    url = f"{BASE_URL_V2}/sharing_keys.json"
    headers = {"Authorization": f"Bearer {vk_token}", "Content-Type": "application/json"}
    payload = {
        "sources": [{"object_type": object_type, "object_id": object_id}],
        "users": [],
        "send_email": False,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    try:
        result = resp.json()
    except Exception:
        raise Exception(f"Некорректный ответ sharing_keys: {resp.text}")
    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        raise Exception(f"Ошибка при создании sharing key: {result}")
    return result.get("sharing_key"), result.get("sharing_url")


async def upload_to_all_vk_and_get_one_sharing_key(file_path, vk_tokens):
    """
    Загружает файл в каждый VK кабинет из vk_tokens.
    Возвращает (first_success_list_id, first_token) для генерации единого sharing key.
    Добавлено подробное логирование.
    """
    file_name = os.path.basename(file_path)
    list_name = os.path.splitext(file_name)[0]
    segment_name = f"LAL {list_name}"

    logging.info(f"📤 [VK_ALL_UPLOAD] Начинаем загрузку {file_name} в {len(vk_tokens)} кабинет(ов) VK")

    first_success = None
    for token in vk_tokens:
        short_token = token[:10] + "..."  # не показываем весь токен
        try:
            logging.info(f"➡️ [VK_ALL_UPLOAD] Пробуем загрузить {file_name} с токеном {short_token}")
            list_id = upload_user_list_vk(file_path, list_name, token)
            logging.info(f"✅ [VK_ALL_UPLOAD] list_id={list_id} создан для {file_name} (token {short_token})")

            seg_id = create_segment_vk(list_id, segment_name, token)
            logging.info(f"✅ [VK_ALL_UPLOAD] segment_id={seg_id} создан для {file_name} (token {short_token})")

            if first_success is None:
                first_success = (list_id, token)

        except Exception as e:
            msg = f"❌ [VK_ALL_UPLOAD] Ошибка VK upload {file_name} для токена {short_token}: {e}"
            logging.exception(msg)
            send_error_sync(msg)
            # продолжаем цикл — пробуем другие токены

    if not first_success:
        logging.warning(f"⚠️ [VK_ALL_UPLOAD] Не удалось загрузить {file_name} ни в один VK кабинет.")
    else:
        logging.info(f"🎯 [VK_ALL_UPLOAD] Первый успешный upload: list_id={first_success[0]}")

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
    """Обрабатывает файл за вчерашний день: отправляет в Telegram, загружает в VK (все аккаунты) и в S3."""
    yesterday = datetime.today() - timedelta(days=1)
    file_path = f"/opt/leads_postback/data/leads_sub6_{yesterday.strftime('%d.%m.%Y')}.txt"
    if not os.path.exists(file_path):
        logging.info("Файл leads_sub6 за вчера не найден: %s", file_path)
        return

    try:
        # отправляем в основной телеграм
        await send_file_to_telegram(file_path)
        # заливаем в VK в каждый кабинет (и собираем first_success для ключа)
        first_success = None
        for token in VK_ACCESS_TOKENS:
            try:
                list_id = upload_user_list_vk(file_path, f"leads_sub6_{yesterday.strftime('%d.%m.%Y')}", token)
                create_segment_vk(list_id, f"LAL leads_sub6_{yesterday.strftime('%d.%m.%Y')}", token)
                if first_success is None:
                    first_success = (list_id, token)
            except Exception as e:
                msg = f"Ошибка VK upload (leads_sub6) для токена {token[:8]}: {e}"
                logging.exception(msg)
                send_error_sync(msg)
        # загрузка в S3
        upload_to_s3(file_path)
        logging.info("Обработан leads_sub6: %s", file_path)
        # при необходимости можно возвращать first_success
        return first_success
    except Exception as e:
        msg = f"Ошибка обработки leads_sub6: {e}"
        logging.exception(msg)
        await send_error_async(msg)


# === Главный процесс ===
async def main():
    logging.info("=== 🚀 Запуск bot_master ===")

    # 1) Сначала обрабатываем файл leads_sub6 вчерашнего дня
    first_success_for_key = await process_previous_day_file()
    # first_success_for_key может быть None или (list_id, token)

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
        return

    # 4) Убедимся, что все файлы загружены в S3 (csv + txt)
    for f in csv_files + txt_files:
        try:
            upload_to_s3(f)
        except Exception as e:
            logging.exception("Ошибка загрузки в S3")
            send_error_sync(f"Ошибка загрузки в S3 {f}: {e}")

    # 5) Сортируем TXT файлы по требуемому порядку
    txt_files_ordered = order_txt_files(txt_files)

    # 6) Загружаем каждый TXT в каждый VK кабинет, в порядке; собираем первый success для генерации sharing key
    first_success = first_success_for_key  # prefer leads_sub6 first_success if returned
    for txt in txt_files_ordered:
        logging.info(f"🚀 Начинаем обработку TXT: {txt}")
        await send_file_to_telegram(txt)

        try:
            res = await upload_to_all_vk_and_get_one_sharing_key(txt, VK_ACCESS_TOKENS)
            if res and first_success is None:
                first_success = res
            logging.info(f"✅ VK загрузка завершена для {txt}, результат: {res}")
        except Exception as e:
            logging.exception(f"❌ Ошибка при загрузке {txt} в VK: {e}")
            await send_error_async(f"Ошибка VK upload для {os.path.basename(txt)}: {e}")

        await asyncio.sleep(random.uniform(0.5, 1.5))




    # 7) После загрузки ВСЕХ файлов — генерируем один общий sharing key и отправляем ссылку в основной бот
    if first_success:
        try:
            list_id_for_key, token_for_key = first_success
            sharing_key, sharing_url = generate_sharing_key_for_owner("users_list", int(list_id_for_key), token_for_key)
            # Отправляем ссылку в основной бот (BOT_TOKEN)
            if BOT_TOKEN and CHAT_ID:
                try:
                    resp = requests.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        data={"chat_id": CHAT_ID,
                              "text": f"✅ Sharing key создан:\n{sharing_url}"}
                    )
                    if resp.status_code != 200:
                        logging.error("Не удалось отправить sharing key в основной бот: %s", resp.text)
                        send_error_sync(f"Не удалось отправить sharing key в основной бот: {resp.status_code} {resp.text}")
                except Exception as e:
                    logging.exception("Ошибка отправки sharing key в основной бот")
                    send_error_sync(f"Ошибка отправки sharing key в основной бот: {e}")
            else:
                logging.warning("BOT_TOKEN/CHAT_ID не настроены, sharing_url: %s", sharing_url)
                send_error_sync(f"Sharing key: {sharing_url}")
            logging.info("Sharing key создан и отправлен: %s", sharing_url)
        except Exception as e:
            logging.exception("Ошибка при создании sharing key")
            send_error_sync(f"Ошибка при создании sharing key: {e}")
    else:
        logging.warning("Не найден ни один успешный list_id для генерации sharing key.")
        send_error_sync("Не найден ни один успешный list_id для генерации sharing key.")

    logging.info("✅ Все задачи завершены.")


if __name__ == "__main__":
    asyncio.run(main())
