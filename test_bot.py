import os
import asyncio
import logging
import pandas as pd
import requests
import boto3
import aiohttp
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import defaultdict

load_dotenv()

# === Настройки ===
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

VK_TOKENS = os.getenv("VK_ACCESS_TOKEN").split(",")  # поддержка нескольких токенов через запятую

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

ERROR_BOT_TOKEN = os.getenv("ERROR_BOT_TOKEN")
ERROR_CHAT_ID = os.getenv("ERROR_CHAT_ID")

BASE_DATE = datetime(2025, 7, 14)
BASE_NUMBER = 53

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

BASE_URL_V3 = "https://ads.vk.com/api/v3"
BASE_URL_V2 = "https://ads.vk.com/api/v2"


# === Утилиты ===
async def send_error(message: str):
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        return
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage",
            data={"chat_id": ERROR_CHAT_ID, "text": f"ERROR /bot_master.py : {message}"}
        )


def get_day_number(today: datetime) -> int:
    return BASE_NUMBER + (today - BASE_DATE).days


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


def broker_channel_group(cid: str, day_number: int) -> str:
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


# === Работа с S3 ===
def download_csv_from_s3():
    os.makedirs("/opt/bot/csv", exist_ok=True)
    objects = s3.list_objects_v2(Bucket=S3_BUCKET).get("Contents", [])
    csv_files = []

    for obj in objects:
        key = obj["Key"]
        if not key.lower().endswith(".csv"):
            continue
        local_path = os.path.join("/opt/bot/csv", os.path.basename(key))
        s3.download_file(S3_BUCKET, key, local_path)
        logging.info(f"✅ Скачан из S3: {key}")
        csv_files.append(local_path)
    return csv_files


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
                if output_name:
                    output_data[output_name].update(phones)

        except Exception as e:
            msg = f"❌ Ошибка при обработке {file}: {e}"
            logging.error(msg)
            asyncio.run(send_error(msg))

    os.makedirs("/opt/bot/txt", exist_ok=True)
    txt_files = []
    # сортировка по фиксированному порядку
    order = [
        "leads_sub6",
        "КР ДОП_10", "КР ДОП_9", "КР ДОП_8", "КР ДОП_7",
        "КР ДОП_6", "КР ДОП_5", "КР ДОП_4", "КР ДОП_3",
        "КР 2", "КР 1",
        "ББ ДОП_3", "ББ ДОП_2", "ББ", "Б1", "Б0"
    ]
    def sort_key(name):
        for idx, part in enumerate(order):
            if part in name:
                return idx
        return 999
    for name, phones in sorted(output_data.items(), key=lambda x: sort_key(x[0])):
        path = os.path.join("/opt/bot/txt", name)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(phones)))
        txt_files.append(path)
        logging.info(f"💾 Сохранен {name} ({len(phones)} номеров)")
    return txt_files


async def send_file(file_path: str):
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


def upload_user_list(file_path, list_name, vk_token):
    url = f"{BASE_URL_V3}/remarketing/users_lists.json"
    headers = {"Authorization": f"Bearer {vk_token}"}
    files = {"file": open(file_path, "rb")}
    data = {"name": list_name, "type": "phones"}
    resp = requests.post(url, headers=headers, files=files, data=data)
    files["file"].close()
    result = resp.json()
    if resp.status_code != 200 or "error" in result:
        raise Exception(result)
    return result.get("id")


def create_segment_with_list(segment_name, list_id, vk_token):
    url = f"{BASE_URL_V2}/remarketing/segments.json"
    headers = {"Authorization": f"Bearer {vk_token}"}
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


def generate_sharing_key(object_type: str, object_id: int, vk_token: str):
    url = f"{BASE_URL_V2}/sharing_keys.json"
    headers = {"Authorization": f"Bearer {vk_token}"}
    payload = {
        "sources": [{"object_type": object_type, "object_id": object_id}],
        "users": [],
        "send_email": False,
    }
    resp = requests.post(url, headers=headers, json=payload)
    result = resp.json()
    if resp.status_code != 200 or "error" in result:
        raise Exception(result)
    return result.get("sharing_url")


def upload_to_vk_ads(file_path):
    file_name = os.path.basename(file_path)
    list_name = os.path.splitext(file_name)[0]
    segment_name = f"LAL {list_name}"
    for vk_token in VK_TOKENS:
        try:
            list_id = upload_user_list(file_path, list_name, vk_token)
            create_segment_with_list(segment_name, list_id, vk_token)
            logging.info(f"📤 {file_name} загружен в VK Ads (ID={list_id})")
        except Exception as e:
            msg = f"Ошибка VK upload {file_name}: {e}"
            logging.error(msg)
            asyncio.run(send_error(msg))
    # общий sharing key для всех сегментов (берем первый токен)
    try:
        first_list_id = upload_user_list(file_path, list_name, VK_TOKENS[0])
        sharing_url = generate_sharing_key("users_list", first_list_id, VK_TOKENS[0])
        asyncio.run(send_file_to_main_bot(sharing_url))
    except Exception as e:
        msg = f"Ошибка генерации sharing key: {e}"
        logging.error(msg)
        asyncio.run(send_error(msg))


async def send_file_to_main_bot(url: str):
    """Отправка ссылки sharing key основному боту"""
    async with aiohttp.ClientSession() as session:
        await session.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": f"🔑 Sharing key: {url}"}
        )


def upload_to_s3(file_path):
    filename = os.path.basename(file_path)
    folder = "txt" if filename.endswith(".txt") else "csv"
    try:
        s3.upload_file(file_path, S3_BUCKET, f"{folder}/{filename}")
        logging.info(f"☁️ Загружен в S3: {folder}/{filename}")
    except Exception as e:
        msg = f"Ошибка загрузки {filename} в S3: {e}"
        logging.error(msg)
        asyncio.run(send_error(msg))


async def main():
    logging.info("=== 🚀 bot_master запущен ===")

    # === Скачиваем CSV из S3 ===
    csv_files = download_csv_from_s3()
    if not csv_files:
        msg = "⚠️ CSV файлы не найдены в S3"
        logging.warning(msg)
        await send_error(msg)
        return

    # === Обработка CSV → TXT ===
    txt_files = process_csv_files(csv_files)

    # === Загрузка в S3 ===
    for f in csv_files + txt_files:
        upload_to_s3(f)

    # === VK Ads и Telegram ===
    for txt in txt_files:
        upload_to_vk_ads(txt)
        await send_file(txt)

    logging.info("✅ Все задачи завершены.")


if __name__ == "__main__":
    asyncio.run(main())
