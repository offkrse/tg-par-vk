import os
import asyncio
import logging
import random
import pandas as pd
import requests
import boto3
import aiohttp
from dotenv import load_dotenv
from datetime import datetime
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

VK_ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")
VK_ACCOUNT_ID = os.getenv("VK_ACCOUNT_ID")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# === Базовая дата и номер ===
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
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# === VK ADS ===
API_VERSION = "v3"
SEGMENTS_VERSION = "v2"
BASE_URL_V3 = "https://ads.vk.com/api/v3"
BASE_URL_V2 = "https://ads.vk.com/api/v2"


# === Вспомогательные функции ===
def get_day_number(today):
    delta = (today - BASE_DATE).days
    return BASE_NUMBER + delta


def get_output_filename(file_name, day_number):
    if "MFO5" in file_name:
        return f"Б0 ({day_number}).txt", "Б0"
    elif "6_web" in file_name:
        return None, "6_web"
    elif any(name in file_name for name in ["253", "345"]):
        return f"Б1 ({day_number}).txt", "Б1"
    elif "389" in file_name:
        return f"Н1 ({day_number}).txt", "Н1"
    elif "390" in file_name:
        return f"Н2 ({day_number}).txt", "Н2"
    else:
        return None, None


async def download_latest_csv():
    """Скачивает CSV из Telegram канала"""
    logging.info("📥 Подключаемся к Telegram...")
    client = TelegramClient("session_master", API_ID, API_HASH)
    await client.start(PHONE)

    today = datetime.today()
    date_suffix = today.strftime("(%d.%m)")

    os.makedirs("csv", exist_ok=True)
    result_files = []

    async for msg in client.iter_messages(CHANNEL_NAME, limit=6):
        if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
            filename = msg.file.name.replace(".csv", f" {date_suffix}.csv")
            path = os.path.join("csv", filename)
            await msg.download_media(file=path)
            result_files.append(path)
            logging.info(f"✅ Скачан {filename}")
            await asyncio.sleep(random.uniform(1, 3))  # Пауза 1-3 сек

    await client.disconnect()
    return result_files


def process_csv_files(files):
    """Обрабатывает CSV → TXT по логике"""
    today = datetime.today()
    day_number = get_day_number(today)

    output_data = defaultdict(set)  # set — для исключения дубликатов
    group_stats = defaultdict(int)
    total_lines = 0

    for file in files:
        try:
            df = pd.read_csv(file)
            fname = os.path.basename(file)
            output_name, group_key = get_output_filename(fname, day_number)

            if group_key == "6_web":
                if "channel_id" not in df.columns:
                    logging.warning(f"⚠️ В файле {fname} нет столбца 'channel_id'")
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

                    output_data[group].add(phone)
                    group_stats[group] += 1
                    total_lines += 1

            else:
                if "phone" not in df.columns:
                    logging.warning(f"⚠️ Нет колонки 'phone' в {fname}")
                    continue

                phones = [p.replace("+", "").strip() for p in df["phone"].dropna().astype(str)]
                for phone in phones:
                    if not phone:
                        continue
                    if output_name:
                        output_data[group_key].add(phone)
                        group_stats[group_key] += 1
                        total_lines += 1

        except Exception as e:
            logging.error(f"❌ Ошибка при обработке {file}: {e}")

    # === Сохраняем TXT ===
    txt_files = []
    os.makedirs("txt", exist_ok=True)
    for group_key, phones in output_data.items():
        filename = f"{group_key} ({day_number}).txt"
        path = os.path.join("txt", filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(phones)))
        txt_files.append(path)
        logging.info(f"💾 Сохранён {filename} ({len(phones)} номеров)")

    return txt_files

async def send_file(file_path: str):
    """Отправка файла в Telegram без текста"""
    async with aiohttp.ClientSession() as session:
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", CHAT_ID)
            form.add_field("document", f)
            async with session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                data=form
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logging.error(f"❌ Ошибка Telegram API ({resp.status}): {text}")
                else:
                    logging.info(f"📨 Успешно отправлен {os.path.basename(file_path)} в Telegram")
# === VK ADS upload ===
def upload_user_list(file_path, list_name):
    url = f"{BASE_URL_V3}/remarketing/users_lists.json"
    headers = {"Authorization": f"Bearer {VK_ACCESS_TOKEN}"}
    files = {"file": open(file_path, "rb")}
    data = {"name": list_name, "type": "phones"}

    resp = requests.post(url, headers=headers, files=files, data=data)
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

    logging.info(f"✅ Список телефонов загружен в VK (ID: {list_id})")
    return list_id


def create_segment_with_list(segment_name, list_id):
    url = f"{BASE_URL_V2}/remarketing/segments.json"
    headers = {
        "Authorization": f"Bearer {VK_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "name": segment_name,
        "pass_condition": 1,
        "relations": [
            {
                "object_type": "remarketing_users_list",
                "params": {"source_id": list_id, "type": "positive"}
            }
        ]
    }

    resp = requests.post(url, headers=headers, json=payload)
    result = resp.json()

    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        raise Exception(f"Ошибка создания сегмента: {result}")

    segment_id = result.get("id")
    logging.info(f"✅ Сегмент создан в VK (ID: {segment_id})")
    return segment_id


def upload_to_vk_ads(file_path):
    file_name = os.path.basename(file_path)
    list_name = os.path.splitext(file_name)[0]
    segment_name = f"LAL {list_name}"

    try:
        list_id = upload_user_list(file_path, list_name)
        create_segment_with_list(segment_name, list_id)
        logging.info(f"📤 {file_name} успешно загружен в VK ADS")
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке {file_name} в VK ADS: {e}")


# === S3 upload ===
def upload_to_s3(file_path):
    filename = os.path.basename(file_path)
    try:
        s3.upload_file(file_path, S3_BUCKET, filename)
        logging.info(f"☁️ Загружен в S3: {filename}")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки {filename} в S3: {e}")


# === Главный процесс ===
async def main():
    logging.info("=== 🚀 Запуск bot_master ===")

    csv_files = await download_latest_csv()
    if not csv_files:
        logging.warning("⚠️ CSV файлы не найдены в Telegram.")
        return

    txt_files = process_csv_files(csv_files)

    # === Загрузка на S3 и VK ===
    for f in csv_files + txt_files:
        upload_to_s3(f)

    for txt in txt_files:
        upload_to_vk_ads(txt)

    # === Отправка TXT файлов в Telegram ===
    for txt in txt_files:
        try:
            await send_file(txt)
            logging.info(f"📨 Отправлен в Telegram: {os.path.basename(txt)}")
        except Exception as e:
            logging.error(f"❌ Ошибка отправки {txt} в Telegram: {e}")

    # === Удаление ===
    for f in csv_files + txt_files:
        try:
            os.remove(f)
            logging.info(f"🧹 Удалён {os.path.basename(f)}")
        except Exception as e:
            logging.warning(f"⚠️ Не удалось удалить {f}: {e}")

    logging.info("✅ Все задачи завершены.")


if __name__ == "__main__":
    asyncio.run(main())
