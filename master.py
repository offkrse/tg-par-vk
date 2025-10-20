#!/usr/bin/env python3
import os
import time
import asyncio
import logging
import requests
import subprocess
from telethon import TelegramClient
from dotenv import load_dotenv

# === Настройки ===
BASE_DIR = "/opt/work"
INCOMING = os.path.join(BASE_DIR, "incoming")
OUTGOING = os.path.join(BASE_DIR, "outgoing")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(INCOMING, exist_ok=True)
os.makedirs(OUTGOING, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "master.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

load_dotenv(os.path.join(BASE_DIR, ".env"))

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
CHANNEL = os.getenv("CHANNEL_NAME", "elel")

SELECTEL_TOKEN = os.getenv("SELECTEL_TOKEN")
PROJECT_ID = os.getenv("SELECTEL_PROJECT_ID")
SERVER2_ID = os.getenv("SERVER2_ID")
SERVER2_IP = os.getenv("SERVER2_IP")  # внутренний IP временного сервера

# === 1. Скачивание CSV ===
async def download_last_6_csv():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.start(phone=PHONE)
    logging.info("Авторизация Telethon завершена")

    entity = await client.get_entity(CHANNEL)
    files = []
    async for msg in client.iter_messages(entity, limit=20):
        if msg.file and str(msg.file.name).endswith(".csv"):
            files.append(msg)
            if len(files) >= 6:
                break

    for msg in reversed(files):
        path = os.path.join(INCOMING, msg.file.name)
        await msg.download_media(file=path)
        logging.info(f"✅ Скачан CSV: {msg.file.name}")

    await client.disconnect()

# === 2. Пробуждение временного сервера ===
def wake_server2():
    logging.info("Пробуждаем временный сервер...")
    url = f"https://api.selectel.ru/vpc/resell/v2/projects/{PROJECT_ID}/servers/{SERVER2_ID}/action"
    headers = {"X-Token": SELECTEL_TOKEN}
    resp = requests.post(url, headers=headers, json={"power_on": True})
    if resp.status_code == 202:
        logging.info("✅ Временный сервер запущен")
    else:
        logging.error(f"❌ Ошибка запуска: {resp.text}")
        raise Exception(resp.text)

# === 3. Передача файлов на сервер 2 ===
def send_files_to_server2():
    logging.info("Передача CSV на временный сервер...")
    subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", f"{INCOMING}/*.csv", f"root@{SERVER2_IP}:/opt/work/incoming/"], shell=True)
    logging.info("✅ CSV переданы")

# === 4. Получение TXT обратно ===
def receive_txt_from_server2():
    logging.info("Получаем готовые TXT...")
    subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", f"root@{SERVER2_IP}:/opt/work/outgoing/*.txt", OUTGOING], shell=True)
    logging.info("✅ TXT получены")

# === 5. Загрузка TXT в VK ADS ===
from vk_uploader import upload_user_list, create_segment_with_list

def upload_all_txt():
    for file in os.listdir(OUTGOING):
        if not file.endswith(".txt"):
            continue
        path = os.path.join(OUTGOING, file)
        try:
            list_id = upload_user_list(path, os.path.splitext(file)[0])
            seg_id = create_segment_with_list(f"Аудитория_{file}", list_id)
            logging.info(f"✅ Загружен {file}: список {list_id}, сегмент {seg_id}")
        except Exception as e:
            logging.error(f"Ошибка VK при {file}: {e}")

# === Основная логика ===
async def main():
    logging.info("=== Запуск процесса ===")
    await download_last_6_csv()
    wake_server2()
    time.sleep(90)  # ждём пока сервер поднимется
    send_files_to_server2()
    logging.info("⏳ Ждём обработки TXT...")
    time.sleep(300)  # ждём ~5 минут
    receive_txt_from_server2()
    upload_all_txt()
    logging.info("=== Завершено ===")

if __name__ == "__main__":
    asyncio.run(main())
