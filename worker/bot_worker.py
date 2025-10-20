#!/usr/bin/env python3
import os
import pandas as pd
import logging
import boto3
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

LOG_FILE = os.path.join(BASE_DIR, "bot_worker.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

CSV_DIR = os.path.join(BASE_DIR, "csv_files")
TXT_DIR = os.path.join(BASE_DIR, "txt_files")
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(TXT_DIR, exist_ok=True)

def get_day_number():
    return datetime.now().timetuple().tm_yday - 85

def process_csv_files():
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

        def write_phones(filename, numbers):
            path = os.path.join(TXT_DIR, filename)
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(numbers))
            logging.info(f"Создан {path} ({len(numbers)} номеров)")

        if name == "MFO5":
            write_phones(f"Б0 ({num}).txt", phones)
        elif name == "389":
            write_phones(f"Н1 ({num}).txt", phones)
        elif name == "390":
            write_phones(f"Н2 ({num}).txt", phones)
        elif name in ["253", "345"]:
            combined_B1.extend(phones)
        elif name == "6_web":
            if "channel_id" not in df.columns:
                continue
            for _, row in df.iterrows():
                cid = int(row["channel_id"])
                ph = str(row["phone"]).replace("+", "")
                channel_groups.get(cid, channel_groups["other"]).append(ph)

    if combined_B1:
        with open(os.path.join(TXT_DIR, f"Б1 ({num}).txt"), "w", encoding="utf-8") as f:
            f.write("\n".join(combined_B1))

def upload_results():
    for file in os.listdir(TXT_DIR):
        if file.endswith(".txt"):
            key = f"output/{file}"
            s3.upload_file(os.path.join(TXT_DIR, file), S3_BUCKET, key)
            logging.info(f"Загружен результат в S3: {key}")

if __name__ == "__main__":
    logging.info("=== Запуск worker ===")

    # скачиваем csv
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="input/")
    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            local_path = os.path.join(CSV_DIR, os.path.basename(key))
            s3.download_file(S3_BUCKET, key, local_path)
            logging.info(f"Скачан CSV: {local_path}")

    process_csv_files()
    upload_results()
    logging.info("=== Завершено ===")
