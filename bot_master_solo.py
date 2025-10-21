import os
import pandas as pd
import logging
import requests
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

# === Конфигурация ===
load_dotenv()

ACCESS_TOKEN = os.getenv("VK_TOKEN")
ACCOUNT_ID = os.getenv("VK_ACCOUNT_ID")

BASE_URL_V3 = "https://ads.vk.com/api/v3"
BASE_URL_V2 = "https://ads.vk.com/api/v2"
DOWNLOAD_DIR = "/opt/bot/master"

# === Логирование ===
logging.basicConfig(
    filename="/opt/bot/master/bot_master_solo.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# === Базовые данные для расчёта номера дня ===
BASE_DATE = datetime(2025, 7, 14)
BASE_NUMBER = 53

def get_day_number(today):
    delta = (today - BASE_DATE).days
    return BASE_NUMBER + delta

# === Определяем группу по имени файла ===
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

# === VK API ===
def upload_user_list(file_path, list_name):
    """Загружает TXT как список телефонов в VK ADS"""
    url = f"{BASE_URL_V3}/remarketing/users_lists.json"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    files = {"file": open(file_path, "rb")}
    data = {"name": list_name, "type": "phones"}

    resp = requests.post(url, headers=headers, files=files, data=data)
    files["file"].close()
    result = resp.json()

    if resp.status_code != 200 or "error" in result:
        raise Exception(f"Ошибка загрузки списка: {result}")
    return result.get("id")

def create_segment_with_list(segment_name, list_id):
    """Создаёт сегмент в VK ADS и добавляет туда список"""
    url = f"{BASE_URL_V2}/remarketing/segments.json"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

    payload = {
        "name": segment_name,
        "pass_condition": 1,
        "relations": [
            {"object_type": "remarketing_users_list", "params": {"source_id": list_id, "type": "positive"}}
        ]
    }

    resp = requests.post(url, headers=headers, json=payload)
    result = resp.json()
    if resp.status_code != 200 or "error" in result:
        raise Exception(f"Ошибка создания сегмента: {result}")
    return result.get("id")

# === Основная обработка CSV ===
def process_csv_files():
    logging.info("=== Запуск SOLO обработки CSV ===")

    csv_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
    if not csv_files:
        logging.info("Нет CSV файлов для обработки.")
        return

    today = datetime.today()
    day_number = get_day_number(today)
    logging.info(f"Номер дня: {day_number}")

    output_data = defaultdict(list)
    group_stats = defaultdict(int)
    file_stats = []
    total_lines = 0

    for csv_file in csv_files:
        file_path = os.path.join(DOWNLOAD_DIR, csv_file)
        try:
            df = pd.read_csv(file_path)
            fname = os.path.basename(csv_file)
            output_name, group_key = get_output_filename(fname, day_number)

            # === Логика для 6_web ===
            if group_key == "6_web":
                if 'phone' not in df.columns or 'channel_id' not in df.columns:
                    logging.error(f"Файл {fname} пропущен — нет нужных столбцов.")
                    continue

                local_stats = defaultdict(int)

                for _, row in df.iterrows():
                    phone = str(row['phone']).replace("+", "").strip()
                    if not phone:
                        continue

                    ch = str(row['channel_id']).strip()
                    if ch == "15883":
                        group = "ББ"
                    elif ch == "15686":
                        group = "ББ ДОП_1"
                    elif ch == "15273":
                        group = "ББ ДОП_2"
                    else:
                        group = "ББ ДОП_3"

                    output_data[group].append(phone)
                    group_stats[group] += 1
                    local_stats[group] += 1
                    total_lines += 1

                details = ", ".join([f"{g}: {c}" for g, c in local_stats.items()])
                file_stats.append(f"{fname}: {sum(local_stats.values())} строк → {details}")

            # === Общая логика ===
            else:
                if 'phone' not in df.columns:
                    logging.error(f"Файл {fname} пропущен — нет колонки 'phone'.")
                    continue

                phones = df['phone'].dropna().astype(str)
                cleaned_phones = [p.replace("+", "").strip() for p in phones if p.strip()]
                lines_count = len(cleaned_phones)
                total_lines += lines_count

                if output_name:
                    output_data[group_key].extend(cleaned_phones)
                    group_stats[group_key] += lines_count
                    file_stats.append(f"{fname}: {lines_count} строк → {group_key}")
                else:
                    file_stats.append(f"{fname}: пропущен (не распознан)")

        except Exception as e:
            logging.error(f"Ошибка при обработке {csv_file}: {e}")

    # === Создаём TXT файлы ===
    txt_files = []
    for group_key, phones in output_data.items():
        filename = f"{group_key} ({day_number}).txt"
        txt_path = os.path.join(DOWNLOAD_DIR, filename)
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(phones))
        txt_files.append(txt_path)
        logging.info(f"Создан TXT: {filename}, {len(phones)} строк")

    logging.info(f"=== Обработка CSV завершена. Всего строк: {total_lines} ===")
    for stat in file_stats:
        logging.info(stat)

    # === Загружаем TXT в VK ADS ===
    for txt_path in txt_files:
        try:
            list_name = os.path.splitext(os.path.basename(txt_path))[0]
            list_id = upload_user_list(txt_path, list_name)
            segment_id = create_segment_with_list(f"Сегмент_{list_name}", list_id)
            logging.info(f"✅ Залито в VK ADS: {list_name} (ListID={list_id}, SegmentID={segment_id})")

            # Удаляем файлы
            os.remove(txt_path)
            logging.info(f"🧹 Удалён TXT: {txt_path}")

        except Exception as e:
            logging.error(f"Ошибка загрузки {txt_path}: {e}")

    # Удаляем исходные CSV
    for csv_file in csv_files:
        try:
            os.remove(os.path.join(DOWNLOAD_DIR, csv_file))
            logging.info(f"🧹 Удалён CSV: {csv_file}")
        except:
            pass

    logging.info("=== SOLO обработка завершена ===")

if __name__ == "__main__":
    process_csv_files()
