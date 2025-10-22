import os
import asyncio
import aiohttp
import logging
from dotenv import load_dotenv

# === Настройки ===
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # Например: -1001234567890

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# === Создаём тестовый файл ===
TEST_FILE = "test_send.txt"
with open(TEST_FILE, "w", encoding="utf-8") as f:
    f.write("12345\n67890\n54321")
logging.info(f"📄 Создан тестовый файл: {TEST_FILE}")

# === Функция отправки ===
async def send_test_file():
    """Проверка отправки файла в Telegram"""
    async with aiohttp.ClientSession() as session:
        with open(TEST_FILE, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", CHAT_ID)
            form.add_field("document", f, filename=os.path.basename(TEST_FILE))

            async with session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument",
                data=form
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logging.error(f"❌ Ошибка Telegram API ({resp.status}): {text}")
                else:
                    logging.info(f"✅ Успешно отправлен файл в Telegram: {text}")

# === Запуск ===
if __name__ == "__main__":
    asyncio.run(send_test_file())
