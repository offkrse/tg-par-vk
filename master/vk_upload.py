import os
import requests
import logging
from dotenv import load_dotenv

load_dotenv()

VK_ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN")
VK_ACCOUNT_ID = os.getenv("VK_ACCOUNT_ID")

def upload_to_vk(file_path, list_name):
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
        resp = requests.post(url, params=params, files=files, timeout=60)
        data = resp.json()
        if "error" in data:
            logging.error(f"Ошибка VK API: {data}")
        else:
            logging.info(f"Загружен в VK: {file_path}")
