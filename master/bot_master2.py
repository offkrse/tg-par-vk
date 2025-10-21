import os
import time
import asyncio
import random
import logging
import datetime
from telethon import TelegramClient
import boto3
from dotenv import load_dotenv

# === Загрузка конфигурации ===
load_dotenv()

API_ID = int(os.getenv("TG_API_ID"))
API_HASH = os.getenv("TG_API_HASH")
PHONE_NUMBER = os.getenv("TG_PHONE")
CHANNEL_NAME = os.getenv("TG_CHANNEL")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

DOWNLOAD_DIR = "/opt/bot/master/downloads"

# === Логирование ===
logging.basicConfig(
    filename="/opt/bot/master/bot_master.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# === Инициализация клиентов ===
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

client = TelegramClient("session_master", API_ID, API_HASH)

# === Функция скачивания CSV ===
async def download_latest_csv():
    logging.info("=== Начинаем скачивание CSV ===")
    entity = await client.get_entity(CHANNEL_NAME)
    messages = await client.get_messages(entity, limit=20)

    today_str = datetime.datetime.now().strftime("(%d.%m)")
    downloaded_files = []
    max_files = 10

    for msg in messages:
        if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
            base_name = os.path.splitext(msg.file.name)[0]
            file_name = f"{base_name}{today_str}.csv"
            file_path = os.path.join(DOWNLOAD_DIR, file_name)

            logging.info(f"Скачиваем: {file_name}")
            await msg.download_media(file=file_path)
            downloaded_files.append(file_path)

            pause = random.uniform(1, 3)
            logging.info(f"Пауза {pause:.2f} сек")
            time.sleep(pause)

            if len(downloaded_files) >= max_files:
                logging.info("Достигнут лимит файлов за цикл (10).")
                break

    # Загрузка на S3
    for file_path in downloaded_files:
        try:
            file_name = os.path.basename(file_path)
            s3.upload_file(file_path, S3_BUCKET, file_name)
            logging.info(f"✅ Загружен в S3: {file_name}")

            os.remove(file_path)
            logging.info(f"🧹 Удалён локально: {file_path}")

        except Exception as e:
            logging.error(f"Ошибка при загрузке {file_path}: {e}")

    return downloaded_files

# === Основной цикл ===
async def main():
    logging.info("=== Запуск master ===")
    await client.start(phone=PHONE_NUMBER)
    files = await download_latest_csv()
    logging.info(f"Файлы скачаны: {files}")
    logging.info("=== Завершено ===")

if __name__ == "__main__":
    asyncio.run(main())
