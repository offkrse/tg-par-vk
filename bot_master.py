import os
import asyncio
import random
import time
import logging
from datetime import datetime
from collections import defaultdict
import pandas as pd
import boto3
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from vk_api import VkApi
from dotenv import load_dotenv

# === Загрузка .env ===
load_dotenv()

# === Telegram ===
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE = os.getenv("PHONE")
CHANNEL_NAME = os.getenv("CHANNEL_NAME")

# === VK ===
VK_ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")
VK_ACCOUNT_ID = os.getenv("VK_ACCOUNT_ID")

# === S3 ===
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

# === Константы ===
BASE_DATE = datetime(2025, 7, 14)
BASE_NUMBER = 53
DOWNLOAD_DIR = "/opt/bot"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === Логирование ===
logging.basicConfig(
    filename="/opt/bot/bot_master.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# === Функции для обработки дат ===
def get_day_number(today: datetime):
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


# === Инициализация клиентов ===
if not all([S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY]):
    raise SystemExit("❌ Ошибка: S3 параметры не заданы в .env")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT if S3_ENDPOINT.startswith("http") else f"https://{S3_ENDPOINT}",
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

vk = VkApi(token=VK_ACCESS_TOKEN).get_api()


# === Скачивание CSV из Telegram ===
async def download_csv_from_tg():
    client = TelegramClient("session", API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        await client.send_code_request(PHONE)
        code = input("Введите код из Telegram: ")
        await client.sign_in(PHONE, code)
        try:
            await client.sign_in(PHONE, code)
        except SessionPasswordNeededError:
            pw = input("Введите пароль 2FA: ")
            await client.sign_in(password=pw)

    logging.info(f"📥 Подключено к Telegram. Загружаем файлы из {CHANNEL_NAME}...")
    entity = await client.get_entity(CHANNEL_NAME)
    messages = await client.get_messages(entity, limit=20)

    downloaded_files = []

    for msg in messages:
        if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
            filename = msg.file.name
            save_path = os.path.join(DOWNLOAD_DIR, filename)
            if not os.path.exists(save_path):
                await msg.download_media(file=save_path)
                downloaded_files.append(save_path)
                logging.info(f"✅ Скачан {filename}")
                time.sleep(random.uniform(1, 3))

    await client.disconnect()
    return downloaded_files


# === Обработка CSV в TXT ===
def process_csv_to_txt():
    today = datetime.today()
    day_number = get_day_number(today)
    output_data = defaultdict(list)

    all_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
    if not all_files:
        logging.warning("⚠️ Нет CSV для обработки.")
        return []

    for file in all_files:
        path = os.path.join(DOWNLOAD_DIR, file)
        try:
            df = pd.read_csv(path)
            fname = os.path.basename(file)
            output_name, group_key = get_output_filename(fname, day_number)

            # === Логика для 6_web ===
            if group_key == "6_web":
                if "channel_id" not in df.columns:
                    logging.error(f"❌ Нет столбца channel_id в {fname}")
                    continue
                for _, row in df.iterrows():
                    phone = str(row["phone"]).replace("+", "").strip()
                    if not phone:
                        continue
                    ch = str(row["channel_id"]).strip()
                    if ch == "15883":
                        group = "ББ"
                    elif ch == "15686":
                        group = "ББ ДОП_1"
                    elif ch == "15273":
                        group = "ББ ДОП_2"
                    else:
                        group = "ББ ДОП_3"
                    output_data[group].append(phone)
            else:
                if "phone" not in df.columns:
                    logging.error(f"❌ Нет столбца phone в {fname}")
                    continue
                phones = df["phone"].dropna().astype(str)
                cleaned = [p.replace("+", "").strip() for p in phones if p.strip()]
                if output_name:
                    output_data[group_key].extend(cleaned)
            logging.info(f"📄 Обработан {fname}")

        except Exception as e:
            logging.error(f"Ошибка обработки {file}: {e}")

    # === Убираем дубликаты и сохраняем TXT ===
    saved_txt = []
    for group, phones in output_data.items():
        unique_phones = sorted(set(phones))
        filename = f"{group} ({day_number}).txt"
        path = os.path.join(DOWNLOAD_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(unique_phones))
        saved_txt.append(path)
        logging.info(f"💾 Сохранён {filename} ({len(unique_phones)} номеров)")
    return saved_txt


# === Загрузка на S3 ===
def upload_to_s3(file_path):
    filename = os.path.basename(file_path)
    try:
        s3.upload_file(file_path, S3_BUCKET, filename)
        logging.info(f"☁️ Залит в S3: {filename}")
    except Exception as e:
        logging.error(f"Ошибка загрузки {filename} в S3: {e}")


# === Загрузка TXT в VK Ads ===
def upload_to_vk_ads(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            phones = [line.strip() for line in f.readlines() if line.strip()]

        vk.ads.importTargetContacts(
            account_id=VK_ACCOUNT_ID,
            target_group_id=1,  # ⚠️ Подставь ID своей группы
            contacts="\n".join(phones),
        )
        logging.info(f"📤 Залит {len(phones)} номеров в VK Ads ({os.path.basename(file_path)})")
    except Exception as e:
        logging.error(f"Ошибка при загрузке {file_path} в VK Ads: {e}")


# === Основной процесс ===
async def main():
    logging.info("🚀 Запуск bot_master")

    # 1. Скачиваем CSV
    csv_files = await download_csv_from_tg()

    # 2. Добавляем дату в их названия
    today = datetime.today()
    date_tag = f"({today.day:02d}.{today.month:02d})"
    renamed_files = []
    for fpath in csv_files:
        dirname, fname = os.path.split(fpath)
        name, ext = os.path.splitext(fname)
        new_name = f"{name} {date_tag}{ext}"
        new_path = os.path.join(dirname, new_name)
        os.rename(fpath, new_path)
        renamed_files.append(new_path)
        logging.info(f"📦 Переименован {fname} → {new_name}")

    # 3. Обработка CSV → TXT
    txt_files = process_csv_to_txt()

    # 4. Загрузка всех файлов в S3
    for f in renamed_files + txt_files:
        upload_to_s3(f)

    # 5. Загрузка TXT в VK
    for f in txt_files:
        upload_to_vk_ads(f)

    # 6. Очистка
    for f in renamed_files + txt_files:
        try:
            os.remove(f)
            logging.info(f"🧹 Удалён {os.path.basename(f)}")
        except Exception as e:
            logging.error(f"Ошибка удаления {f}: {e}")

    logging.info("✅ Готово! Все файлы обработаны и загружены.")


if __name__ == "__main__":
    asyncio.run(main())
