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

VK_ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")

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
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# === VK API URLs ===
BASE_URL_V3 = "https://ads.vk.com/api/v3"
BASE_URL_V2 = "https://ads.vk.com/api/v2"


# === Утилиты ===
async def send_error(message: str):
    """Отправка ошибки error-ботом"""
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        return
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
            data={"chat_id": ERROR_CHAT_ID, "text": f"ERROR /bot_master.py : {message}"}
        )


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


async def download_latest_csv():
    """Скачивает CSV из Telegram"""
    client = TelegramClient("session_master", API_ID, API_HASH)
    await client.start(PHONE)
    today = datetime.today()
    date_suffix = today.strftime("(%d.%m)")
    os.makedirs("csv", exist_ok=True)
    result_files = []

    seen_names = set()

    async for msg in client.iter_messages(CHANNEL_NAME, limit=7):
        if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
            fname = msg.file.name
            if fname in seen_names:
                logging.warning(f"⚠️ Пропущен дубликат {fname}")
                continue
            seen_names.add(fname)

            filename = fname.replace(".csv", f" {date_suffix}.csv")
            path = os.path.join("csv", filename)
            await msg.download_media(file=path)
            result_files.append(path)
            logging.info(f"✅ Скачан {filename}")
            await asyncio.sleep(random.uniform(1, 2))

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
        "КР ДОП_6": [10141, 10240],
        "КР ДОП_7": [11682, 11729],
        "КР ДОП_8": [12873],
        "КР ДОП_9": [16263]
    }
    for name, ids in mapping.items():
        if cid.isdigit() and int(cid) in ids:
            return f"{name} ({day_number}).txt"
    return f"КР ДОП_10 ({day_number}).txt"


def process_csv_files(files):
    today = datetime.today()
    day_number = get_day_number(today)
    output_data = defaultdict(set)

    for file in files:
        try:
            df = pd.read_csv(file)
            fname = os.path.basename(file)
            if df.empty or "phone" not in df.columns or df["phone"].dropna().empty:
                msg = f"⚠️ Пропущен пустой или некорректный CSV: {fname}"
                logging.warning(msg)
                asyncio.run(send_error(msg))
                continue

            output_name, group_key = get_output_filename(fname, day_number)
            if not group_key:
                continue

            if group_key == "broker":
                if "channel_id" not in df.columns:
                    msg = f"⚠️ В {fname} отсутствует column 'channel_id'"
                    logging.warning(msg)
                    asyncio.run(send_error(msg))
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
                    msg = f"⚠️ В {fname} нет столбца channel_id"
                    logging.warning(msg)
                    asyncio.run(send_error(msg))
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
                if not phones:
                    msg = f"⚠️ Нет номеров в {fname}"
                    logging.warning(msg)
                    asyncio.run(send_error(msg))
                    continue
                if output_name:
                    output_data[output_name].update(phones)

        except Exception as e:
            msg = f"❌ Ошибка при обработке {file}: {e}"
            logging.error(msg)
            asyncio.run(send_error(msg))

    os.makedirs("txt", exist_ok=True)
    txt_files = []
    for name, phones in output_data.items():
        path = os.path.join("txt", name)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(phones)))
        txt_files.append(path)
        logging.info(f"💾 Сохранен {name} ({len(phones)} номеров)")
    return txt_files


async def send_file(file_path: str):
    """Отправка файла в Telegram"""
    async with aiohttp.ClientSession() as session:
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", CHAT_ID)
            form.add_field("document", f)
            async with session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument", data=form
            ) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    msg = f"❌ Ошибка Telegram API: {txt}"
                    logging.error(msg)
                    await send_error(msg)


def upload_to_s3(file_path):
    """Загрузка в S3"""
    filename = os.path.basename(file_path)
    folder = "txt" if filename.endswith(".txt") else "csv"
    try:
        s3.upload_file(file_path, S3_BUCKET, f"{folder}/{filename}")
        logging.info(f"☁️ Загружен в S3: {folder}/{filename}")
    except Exception as e:
        msg = f"Ошибка загрузки {filename} в S3: {e}"
        logging.error(msg)
        asyncio.run(send_error(msg))


def upload_user_list(file_path, list_name):
    url = f"{BASE_URL_V3}/remarketing/users_lists.json"
    headers = {"Authorization": f"Bearer {VK_ACCESS_TOKEN}"}
    files = {"file": open(file_path, "rb")}
    data = {"name": list_name, "type": "phones"}
    resp = requests.post(url, headers=headers, files=files, data=data)
    files["file"].close()

    result = resp.json()
    if resp.status_code != 200 or "error" in result:
        raise Exception(result)
    return result.get("id")


def create_segment_with_list(segment_name, list_id):
    url = f"{BASE_URL_V2}/remarketing/segments.json"
    headers = {"Authorization": f"Bearer {VK_ACCESS_TOKEN}"}
    payload = {
        "name": segment_name,
        "pass_condition": 1,
        "relations": [
            {"object_type": "remarketing_users_list", "params": {"source_id": list_id, "type": "positive"}}
        ],
    }
    resp = requests.post(url, headers=headers, json=payload)
    result = resp.json()
    if resp.status_code != 200 or "error" in result:
        raise Exception(result)
    return result.get("id")


def generate_sharing_key(object_type: str, object_id: int):
    """Создает sharing key в VK"""
    url = f"{BASE_URL_V2}/sharing_keys.json"
    headers = {"Authorization": f"Bearer {VK_ACCESS_TOKEN}"}
    payload = {
        "sources": [{"object_type": object_type, "object_id": object_id}],
        "users": [],
        "send_email": False,
    }
    resp = requests.post(url, headers=headers, json=payload)
    result = resp.json()
    if resp.status_code != 200 or "error" in result:
        raise Exception(result)
    logging.info(f"🔑 Sharing key создан: {result}")
    return result


def upload_to_vk_ads(file_path):
    file_name = os.path.basename(file_path)
    list_name = os.path.splitext(file_name)[0]
    segment_name = f"LAL {list_name}"
    try:
        list_id = upload_user_list(file_path, list_name)
        segment_id = create_segment_with_list(segment_name, list_id)
        generate_sharing_key("users_list", list_id)
        logging.info(f"📤 VK upload OK ({file_name}) ID={list_id}")
    except Exception as e:
        msg = f"Ошибка VK upload {file_name}: {e}"
        logging.error(msg)
        asyncio.run(send_error(msg))


async def process_previous_day_file():
    """Обрабатывает файл за вчерашний день"""
    yesterday = datetime.today() - timedelta(days=1)
    file_path = f"/opt/leads_postback/data/leads_sub6_{yesterday.strftime('%d.%m.%Y')}.txt"
    if not os.path.exists(file_path):
        return

    try:
        await send_file(file_path)
        upload_to_vk_ads(file_path)
        upload_to_s3(file_path)
        logging.info(f"📤 Обработан файл за {yesterday.date()}")
    except Exception as e:
        msg = f"Ошибка обработки файла за вчера: {e}"
        logging.error(msg)
        await send_error(msg)

    # Удаляем старше 7 дней
    old_date = datetime.today() - timedelta(days=7)
    old_path = f"/opt/leads_postback/data/leads_sub6_{old_date.strftime('%d.%m.%Y')}.txt"
    if os.path.exists(old_path):
        os.remove(old_path)
        logging.info(f"🧹 Удален старый файл: {old_path}")


# === Главный процесс ===
async def main():
    logging.info("=== 🚀 bot_master запущен ===")

    await process_previous_day_file()

    csv_files = await download_latest_csv()
    if not csv_files:
        msg = "⚠️ CSV не найдены в Telegram"
        logging.warning(msg)
        await send_error(msg)
        return

    txt_files = process_csv_files(csv_files)

    for f in csv_files + txt_files:
        upload_to_s3(f)

    for txt in txt_files:
        upload_to_vk_ads(txt)
        await send_file(txt)

    for f in csv_files + txt_files:
        try:
            os.remove(f)
        except Exception as e:
            logging.warning(f"Не удалось удалить {f}: {e}")

    logging.info("✅ Все задачи завершены.")


if __name__ == "__main__":
    asyncio.run(main())
