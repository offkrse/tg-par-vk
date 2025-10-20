#!/usr/bin/env python3
import os
import asyncio
import logging
import requests
from datetime import datetime
from telethon import TelegramClient
from dotenv import load_dotenv
import boto3
import time

# === Настройка ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
CHANNEL_NAME = os.getenv("CHANNEL_NAME")

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

SELECTEL_API_TOKEN = os.getenv("SELECTEL_API_TOKEN")
WORKER_SERVER_ID = os.getenv("WORKER_SERVER_ID")

LOG_FILE = os.path.join(BASE_DIR, "bot_master.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# === Шаг 1. Скачать последние 6 CSV из Telegram ===
async def download_latest_csv():
    client = TelegramClient("session_master", API_ID, API_HASH)
    await client.start(phone=PHONE)
    logging.info("Авторизация Telethon завершена.")

    entity = await client.get_entity(CHANNEL_NAME)
    files = []
    async for msg in client.iter_messages(entity, limit=20):
        if msg.file and str(msg.file.name).endswith(".csv"):
            files.append(msg)
            if len(files) >= 6:
                break

    local_paths = []
    for msg in files[::-1]:  # от старых к новым
        file_path = os.path.join(BASE_DIR, msg.file.name)
        await msg.download_media(file=file_path)
        local_paths.append(file_path)
        logging.info(f"Скачан файл: {msg.file.name}")

    await client.disconnect()
    return local_paths

# === Шаг 2. Загрузить CSV в S3 ===
def upload_to_s3(file_path):
    try:
        key = f"input/{os.path.basename(file_path)}"
        s3.upload_file(file_path, S3_BUCKET, key)
        logging.info(f"Загружен в S3: {key}")
    except Exception as e:
        logging.error(f"Ошибка загрузки {file_path} в S3: {e}")

# === Шаг 3. Запустить временный сервер ===
def start_worker():
    try:
        url = f"https://api.selectel.ru/vpc/resell/v2/servers/{WORKER_SERVER_ID}/action"
        headers = {"X-Token": SELECTEL_API_TOKEN, "Content-Type": "application/json"}
        requests.post(url, headers=headers, json={"power_action": "start"})
        logging.info("Запущен временный сервер (worker).")
    except Exception as e:
        logging.error(f"Ошибка запуска worker: {e}")

# === Шаг 4. Скачать готовые TXT из S3 и загрузить в VK ===
def download_txt_from_s3():
    from vk_upload import upload_to_vk
    result_dir = os.path.join(BASE_DIR, "txt_results")
    os.makedirs(result_dir, exist_ok=True)

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="output/")
    if "Contents" not in response:
        logging.warning("Нет TXT файлов в S3.")
        return

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".txt"):
            continue
        local_path = os.path.join(result_dir, os.path.basename(key))
        s3.download_file(S3_BUCKET, key, local_path)
        logging.info(f"Скачан результат: {local_path}")
        upload_to_vk(local_path, os.path.basename(local_path))

# === Основной поток ===
async def main():
    logging.info("=== Запуск master ===")

    csv_files = await download_latest_csv()
    for f in csv_files:
        upload_to_s3(f)

    start_worker()

    # ждём, пока worker всё обработает
    logging.info("Ожидание результата от worker...")
    time.sleep(1800)  # 30 минут

    download_txt_from_s3()
    logging.info("=== Завершено ===")

if __name__ == "__main__":
    asyncio.run(main())
