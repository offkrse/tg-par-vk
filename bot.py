#!/usr/bin/env python3
import os
import logging
import asyncio
import pandas as pd
import requests
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv

# === Пользовательские настройки ===
MAX_FILES = 6  # Сколько последних CSV скачивать

# === Настройка окружения ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
CHANNEL_NAME = os.getenv("CHANNEL_NAME")

VK_ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")
VK_ACCOUNT_ID = os.getenv("VK_ACCOUNT_ID")

CSV_DIR = os.path.join(BASE_DIR, "csv_phone")
TXT_DIR = os.path.join(BASE_DIR, "txt_phone")
LOG_DIR = os.path.join(BASE_DIR, "logs")

for d in [CSV_DIR, TXT_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

# === Логирование ===
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "bot.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# === VK API ===
def upload_to_vk(file_path, list_name):
    """Загрузка txt в VK ADS Remarketing"""
    url = "https://api.vk.com/method/ads.importTargetContacts"
    params = {
        "access_token": VK_ACCESS_TOKEN,
        "v": "5.131",
        "account_id": VK_ACCOUNT_ID,
        "name": list_name,
        "client_id": 0,
        "target_pixel_id": 0,
    }

    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f)}
        try:
            resp = requests.post(url, params=params, files=files, timeout=60)
            data = resp.json()
            if "error" in data:
                logging.error(f"Ошибка VK API: {data}")
            else:
                logging.info(f"Файл {file_path} успешно загружен в VK: {data}")
        except Exception as e:
            logging.error(f"Ошибка при загрузке {file_path} в VK: {e}")

# === Скачивание CSV ===
async def download_csv():
    """Скачивает последние MAX_FILES CSV из Telegram канала"""
    client = TelegramClient("session", API_ID, API_HASH)
    await client.start(phone=PHONE)
    logging.info("Авторизация Telethon выполнена.")

    entity = await client.get_entity(CHANNEL_NAME)
    messages = []
    async for msg in client.iter_messages(entity, limit=200):
        if msg.file and str(msg.file.name).endswith(".csv"):
            messages.append(msg)

    messages = sorted(messages, key=lambda m: m.date, reverse=True)[:MAX_FILES]

    for msg in messages:
        file_path = os.path.join(CSV_DIR, msg.file.name)
        if not os.path.exists(file_path):
            await msg.download_media(file=file_path)
            logging.info(f"Скачан файл: {msg.file.name}")
        else:
            logging.info(f"Файл {msg.file.name} уже существует, пропускаем.")

    await client.disconnect()

# === Обработка CSV ===
def get_day_number():
    """Возвращает уникальный номер по текущей дате (19.10 -> 150)"""
    return datetime.now().timetuple().tm_yday - 85

def process_csv_files():
    """Парсит CSV и создаёт TXT по правилам"""
    num = get_day_number()
    combined_B1 = []
    channel_groups = {15883: [], 15686: [], 15273: [], "other": []}

    for csv_file in os.listdir(CSV_DIR):
        if not csv_file.endswith(".csv"):
            continue

        csv_path = os.path.join(CSV_DIR, csv_file)
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            logging.error(f"Ошибка чтения {csv_file}: {e}")
            continue

        if "phone" not in df.columns:
            logging.warning(f"Нет столбца 'phone' в {csv_file}")
            continue

        phones = df["phone"].astype(str).str.replace("+", "").tolist()
        name = os.path.splitext(csv_file)[0]

        if name == "MFO5":
            write_phones(os.path.join(TXT_DIR, f"Б0 ({num}).txt"), phones)
        elif name == "389":
            write_phones(os.path.join(TXT_DIR, f"Н1 ({num}).txt"), phones)
        elif name == "390":
            write_phones(os.path.join(TXT_DIR, f"Н2 ({num}).txt"), phones)
        elif name in ["253", "345"]:
            combined_B1.extend(phones)
        elif name == "6_web":
            if "channel_id" not in df.columns:
                logging.warning(f"Нет channel_id в {csv_file}")
                continue
            for _, row in df.iterrows():
                cid = int(row["channel_id"])
                ph = str(row["phone"]).replace("+", "")
                if cid == 15883:
                    channel_groups[15883].append(ph)
                elif cid == 15686:
                    channel_groups[15686].append(ph)
                elif cid == 15273:
                    channel_groups[15273].append(ph)
                else:
                    channel_groups["other"].append(ph)
        else:
            logging.info(f"Файл {csv_file} пропущен (нет правила).")

    if combined_B1:
        write_phones(os.path.join(TXT_DIR, f"Б1 ({num}).txt"), combined_B1)

    for key, phones in channel_groups.items():
        if not phones:
            continue
        if key == 15883:
            name = f"ББ ({num}).txt"
        elif key == 15686:
            name = f"ББ ДОП_1 ({num}).txt"
        elif key == 15273:
            name = f"ББ ДОП_2 ({num}).txt"
        else:
            name = f"ББ ДОП_3 ({num}).txt"
        write_phones(os.path.join(TXT_DIR, name), phones)

def write_phones(path, phones):
    """Сохраняет список телефонов в txt и загружает в VK"""
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(phones))
        logging.info(f"Создан файл {path} ({len(phones)} номеров)")
        upload_to_vk(path, os.path.basename(path))
    except Exception as e:
        logging.error(f"Ошибка записи {path}: {e}")

async def main():
    logging.info("=== Запуск скрипта ===")
    await download_csv()
    process_csv_files()
    logging.info("=== Завершено ===")

if __name__ == "__main__":
    asyncio.run(main())
