#!/usr/bin/env python3
"""
bot_master.py v4.92
──────────────────
Изменения:
  • Два TG-канала с независимыми окнами скачивания (UTC+4):
      Канал 1 (CHANNEL_NAME):  07:03–07:06  →  UTC 03:03–03:06
      Канал 2 (CHANNEL_NAME_2): 09:02–09:06  →  UTC 05:02–05:06
  • Второй канал: только последние 2 CSV-файла
      web_121_* → КБ21 (день).txt
      web_122_* → КБ22 (день).txt
  • Все TXT дедуплицируются (Set по номерам телефонов)
  • VK-кабинеты и токены берутся из cabinets.json портала
  • Для каждого кабинета и каждого файла — своё время выгрузки (fileSchedules)
  • Глобальное расписание выгрузки убрано — всё управляется через портал
  • Многоуровневый failover прокси для Telegram (WG+3proxy → WG+Dante → SSH → HTTP → direct)
"""

import os
import asyncio
import logging
import random
import pandas as pd
import requests
import boto3
import aiohttp
import time
import json
from typing import Any, Dict, Optional, List
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from collections import defaultdict

# ── Импорт max_checker (опционально) ─────────────────────────────────────────
try:
    from max_checker import start_checker_task
    MAX_CHECKER_AVAILABLE = True
except ImportError:
    MAX_CHECKER_AVAILABLE = False

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# === ТЕСТ-РЕЖИМ ==============================================================
# ══════════════════════════════════════════════════════════════════════════════
# Запуск: python bot_master.py --test  ИЛИ  BOT_MASTER_TEST_MODE=1
import sys
TEST_MODE   = "--test" in sys.argv or os.getenv("BOT_MASTER_TEST_MODE", "") == "1"

# Ручной режим (запуск с сайта): сразу без ожидания временных окон
MANUAL_MODE = os.getenv("BOT_MANUAL_MODE", "0") == "1"
MANUAL_CH1  = os.getenv("BOT_DO_CHANNEL1",  "1") == "1"
MANUAL_CH2  = os.getenv("BOT_DO_CHANNEL2",  "1") == "1"
MANUAL_VK   = os.getenv("BOT_DO_VK_UPLOAD", "1") == "1"

if TEST_MODE:
    print("🧪 ТЕСТ-РЕЖИМ: скачиваем 1 файл → test.txt → загружаем в 1 кабинет")

# ══════════════════════════════════════════════════════════════════════════════
# === ПРОКСИ ДЛЯ TELEGRAM — МНОГОУРОВНЕВЫЙ FAILOVER ===========================
# ══════════════════════════════════════════════════════════════════════════════
#
# Порядок попыток (от быстрого к надёжному):
#   1. WireGuard + 3proxy SOCKS5     (TG_SOCKS5_HOST / TG_SOCKS5_PORT)
#   2. WireGuard + Dante SOCKS5      (TG_FALLBACK_SOCKS5_HOST / TG_FALLBACK_SOCKS5_PORT)
#   3. SSH reverse tunnel SOCKS5     (TG_SSH_USER / TG_SSH_KEY_PATH / TG_SSH_TUNNEL_LOCAL_PORT)
#   4. HTTP прокси на relay сервере  (TG_PROXY_URL)
#   5. Прямое подключение            (только если relay недоступен совсем)
#
# Все прокси слушают на WireGuard-интерфейсе relay-сервера — снаружи закрыты.
# ══════════════════════════════════════════════════════════════════════════════

import subprocess
import socket
import threading

# ── Прокси 1: WireGuard + 3proxy (основной) ──────────────────────────────────
TG_SOCKS5_HOST  = os.getenv("TG_SOCKS5_HOST", "")        # 10.99.0.1
TG_SOCKS5_PORT  = int(os.getenv("TG_SOCKS5_PORT", "1080"))
TG_SOCKS5_USER  = os.getenv("TG_SOCKS5_USER", "")
TG_SOCKS5_PASS  = os.getenv("TG_SOCKS5_PASS", "")

# ── Прокси 2: WireGuard + Dante (fallback) ────────────────────────────────────
TG_FALLBACK_SOCKS5_HOST = os.getenv("TG_FALLBACK_SOCKS5_HOST", "")  # 10.99.0.1
TG_FALLBACK_SOCKS5_PORT = int(os.getenv("TG_FALLBACK_SOCKS5_PORT", "1081"))
TG_FALLBACK_SOCKS5_USER = os.getenv("TG_FALLBACK_SOCKS5_USER", "")
TG_FALLBACK_SOCKS5_PASS = os.getenv("TG_FALLBACK_SOCKS5_PASS", "")

# ── Прокси 3: SSH reverse tunnel ─────────────────────────────────────────────
TG_SSH_USER         = os.getenv("TG_SSH_USER", "root")
TG_SSH_HOST         = os.getenv("RELAY_HOST", "")         # 94.103.178.200
TG_SSH_PORT         = int(os.getenv("RELAY_SSH_PORT", "22222"))
TG_SSH_KEY_PATH     = os.getenv("TG_SSH_KEY_PATH", "/root/.ssh/relay_key")
TG_SSH_TUNNEL_PORT  = int(os.getenv("TG_SSH_TUNNEL_LOCAL_PORT", "9050"))

# ── Прокси 4: HTTP прокси (legacy/опциональный) ───────────────────────────────
TG_PROXY_URL    = os.getenv("TG_PROXY_URL", "").rstrip("/")
TG_PROXY_SECRET = os.getenv("TG_PROXY_SECRET", "")

# ── Глобальный активный прокси (выбирается при старте) ───────────────────────
_active_proxy: Optional[dict] = None   # {'type': 'socks5'|'ssh'|'http'|'direct', ...}
_ssh_tunnel_proc: Optional[subprocess.Popen] = None
_proxy_lock = threading.Lock()


def _is_port_open(host: str, port: int, timeout: float = 3.0) -> bool:
    """Проверяет что TCP-порт открыт."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _test_socks5(host: str, port: int, user: str = "", password: str = "") -> bool:
    """Быстро проверяет что SOCKS5-прокси работает (подключение к api.telegram.org)."""
    try:
        import socks
        s = socks.socksocket()
        s.set_proxy(socks.SOCKS5, host, port, True,
                    user or None, password or None)
        s.settimeout(5)
        s.connect(("api.telegram.org", 443))
        s.close()
        return True
    except Exception:
        return False


def _start_ssh_tunnel() -> bool:
    """
    Поднимает SSH dynamic SOCKS5 туннель к relay-серверу.
    ssh -D 127.0.0.1:TG_SSH_TUNNEL_PORT -N -o StrictHostKeyChecking=no ...
    Возвращает True если туннель поднялся.
    """
    global _ssh_tunnel_proc

    if not TG_SSH_HOST or not os.path.exists(TG_SSH_KEY_PATH):
        return False

    # Завершаем старый процесс если есть
    if _ssh_tunnel_proc and _ssh_tunnel_proc.poll() is None:
        _ssh_tunnel_proc.terminate()

    cmd = [
        "ssh",
        "-D", f"127.0.0.1:{TG_SSH_TUNNEL_PORT}",
        "-N",                                     # не выполнять команды
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "ConnectTimeout=10",
        "-o", "ExitOnForwardFailure=yes",
        "-i", TG_SSH_KEY_PATH,
        "-p", str(TG_SSH_PORT),
        f"{TG_SSH_USER}@{TG_SSH_HOST}",
    ]

    try:
        _ssh_tunnel_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Даём туннелю 3 секунды на поднятие
        import time as _time
        for _ in range(6):
            _time.sleep(0.5)
            if _ssh_tunnel_proc.poll() is not None:
                return False  # процесс завершился с ошибкой
            if _is_port_open("127.0.0.1", TG_SSH_TUNNEL_PORT, timeout=1):
                return True
        return _is_port_open("127.0.0.1", TG_SSH_TUNNEL_PORT)
    except Exception as e:
        logging.warning("SSH tunnel error: %s", e)
        return False


def _stop_ssh_tunnel():
    """Завершает SSH-туннель."""
    global _ssh_tunnel_proc
    if _ssh_tunnel_proc and _ssh_tunnel_proc.poll() is None:
        _ssh_tunnel_proc.terminate()
        _ssh_tunnel_proc = None


def select_proxy() -> dict:
    """
    Определяет рабочий прокси, пробуя каждый уровень по очереди.
    Возвращает dict с описанием активного прокси.
    Кэширует результат в _active_proxy.
    """
    global _active_proxy

    with _proxy_lock:
        # ── Уровень 1: WireGuard + 3proxy ────────────────────────────────────
        if TG_SOCKS5_HOST:
            if _test_socks5(TG_SOCKS5_HOST, TG_SOCKS5_PORT,
                            TG_SOCKS5_USER, TG_SOCKS5_PASS):
                logging.info("🟢 Прокси: 3proxy SOCKS5 (%s:%d)",
                             TG_SOCKS5_HOST, TG_SOCKS5_PORT)
                _active_proxy = {
                    "type": "socks5",
                    "host": TG_SOCKS5_HOST, "port": TG_SOCKS5_PORT,
                    "user": TG_SOCKS5_USER, "pass": TG_SOCKS5_PASS,
                    "label": "3proxy/WireGuard",
                }
                return _active_proxy
            else:
                logging.warning("🔴 3proxy SOCKS5 недоступен (%s:%d)",
                                TG_SOCKS5_HOST, TG_SOCKS5_PORT)

        # ── Уровень 2: WireGuard + Dante ─────────────────────────────────────
        if TG_FALLBACK_SOCKS5_HOST:
            if _test_socks5(TG_FALLBACK_SOCKS5_HOST, TG_FALLBACK_SOCKS5_PORT,
                            TG_FALLBACK_SOCKS5_USER, TG_FALLBACK_SOCKS5_PASS):
                logging.info("🟡 Прокси: Dante SOCKS5 (%s:%d)",
                             TG_FALLBACK_SOCKS5_HOST, TG_FALLBACK_SOCKS5_PORT)
                _active_proxy = {
                    "type": "socks5",
                    "host": TG_FALLBACK_SOCKS5_HOST,
                    "port": TG_FALLBACK_SOCKS5_PORT,
                    "user": TG_FALLBACK_SOCKS5_USER,
                    "pass": TG_FALLBACK_SOCKS5_PASS,
                    "label": "Dante/WireGuard",
                }
                return _active_proxy
            else:
                logging.warning("🔴 Dante SOCKS5 недоступен (%s:%d)",
                                TG_FALLBACK_SOCKS5_HOST, TG_FALLBACK_SOCKS5_PORT)

        # ── Уровень 3: SSH dynamic SOCKS5 ────────────────────────────────────
        if TG_SSH_HOST and os.path.exists(TG_SSH_KEY_PATH):
            logging.info("🟡 Пробуем SSH туннель к %s:%d...", TG_SSH_HOST, TG_SSH_PORT)
            if _start_ssh_tunnel():
                logging.info("🟡 Прокси: SSH dynamic SOCKS5 (127.0.0.1:%d)",
                             TG_SSH_TUNNEL_PORT)
                _active_proxy = {
                    "type": "socks5",
                    "host": "127.0.0.1",
                    "port": TG_SSH_TUNNEL_PORT,
                    "user": "", "pass": "",
                    "label": "SSH-tunnel",
                }
                return _active_proxy
            else:
                logging.warning("🔴 SSH туннель не поднялся")

        # ── Уровень 4: HTTP прокси ────────────────────────────────────────────
        if TG_PROXY_URL:
            logging.info("🟡 Прокси: HTTP (%s)", TG_PROXY_URL)
            _active_proxy = {"type": "http", "url": TG_PROXY_URL, "label": "HTTP"}
            return _active_proxy

        # ── Уровень 5: прямое подключение ────────────────────────────────────
        logging.warning("⚠️  Все прокси недоступны — прямое подключение")
        _active_proxy = {"type": "direct", "label": "direct"}
        return _active_proxy


def _bot_api_url(token: str, method: str) -> str:
    """URL для Bot API с учётом активного прокси."""
    proxy = _active_proxy or {}
    if proxy.get("type") == "http" and proxy.get("url"):
        return f"{proxy['url']}/bot{token}/{method}"
    return f"https://api.telegram.org/bot{token}/{method}"


def _proxy_headers() -> dict:
    if TG_PROXY_SECRET:
        return {"X-Proxy-Secret": TG_PROXY_SECRET}
    return {}


def _aiohttp_connector():
    """
    Возвращает aiohttp-коннектор с поддержкой SOCKS5.
    Требует aiohttp-socks: pip install aiohttp-socks
    """
    proxy = _active_proxy or {}
    if proxy.get("type") == "socks5":
        try:
            from aiohttp_socks import ProxyConnector, ProxyType
            user = proxy.get("user") or None
            password = proxy.get("pass") or None
            if user:
                conn = ProxyConnector(
                    proxy_type=ProxyType.SOCKS5,
                    host=proxy["host"], port=proxy["port"],
                    username=user, password=password,
                    rdns=True,
                )
            else:
                conn = ProxyConnector(
                    proxy_type=ProxyType.SOCKS5,
                    host=proxy["host"], port=proxy["port"],
                    rdns=True,
                )
            return conn
        except ImportError:
            logging.warning("aiohttp-socks не установлен, HTTP прокси не работает для aiohttp")
    return None


def _telethon_proxy() -> Optional[dict]:
    """Возвращает kwargs для TelegramClient."""
    proxy = _active_proxy or {}
    if proxy.get("type") != "socks5":
        return None
    try:
        import socks
        return dict(proxy=(
            socks.SOCKS5,
            proxy["host"], proxy["port"],
            True,
            proxy.get("user") or None,
            proxy.get("pass") or None,
        ))
    except ImportError:
        logging.error("PySocks не установлен: pip install pysocks")
        return None


async def ensure_proxy():
    """
    Вызывается перед каждым обращением к Telegram.
    Если активный прокси не работает — переключается на следующий.
    """
    global _active_proxy

    current = _active_proxy
    if current is None:
        select_proxy()
        return

    # Быстрая проверка текущего прокси
    if current.get("type") == "socks5":
        ok = _test_socks5(
            current["host"], current["port"],
            current.get("user", ""), current.get("pass", "")
        )
        if not ok:
            logging.warning("Прокси %s перестал работать, переключаемся...",
                            current.get("label"))
            _active_proxy = None
            select_proxy()
    elif current.get("type") == "direct":
        # Если был direct — пробуем снова поднять прокси
        _active_proxy = None
        select_proxy()


# ══════════════════════════════════════════════════════════════════════════════
# === НАСТРОЙКИ ================================================================
# ══════════════════════════════════════════════════════════════════════════════
VersionBotMaster = "4.1"

SEND_FILES_TO_TELEGRAM = True
VK_UPLOAD = True
PROMOUSER_UPLOAD = True

API_ID        = os.getenv("API_ID")
API_HASH      = os.getenv("API_HASH")
PHONE         = os.getenv("PHONE")

# Канал 1 — основной (скачиваем все CSV кроме 389/390)
CHANNEL_NAME  = os.getenv("CHANNEL_NAME")

# Канал 2 — дополнительный (только 2 последних файла web_121_* / web_122_*)
CHANNEL_NAME_2 = os.getenv("CHANNEL_NAME_2", "")

S3_BUCKET     = os.getenv("S3_BUCKET")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

NEW_S3_BUCKET     = os.getenv("NEW_S3_BUCKET")
NEW_S3_ENDPOINT   = os.getenv("NEW_S3_ENDPOINT")
NEW_S3_ACCESS_KEY = os.getenv("NEW_S3_ACCESS_KEY")
NEW_S3_SECRET_KEY = os.getenv("NEW_S3_SECRET_KEY")

BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID")

ERROR_BOT_TOKEN = os.getenv("ERROR_BOT_TOKEN")
ERROR_CHAT_ID   = os.getenv("ERROR_CHAT_ID")

# Путь к cabinets.json портала
CABINETS_JSON = os.getenv("CABINETS_JSON", "/opt/base-portal/backend/data/cabinets.json")

# Путь к list_base.json портала — прямая перезапись файла (без HTTP и токенов)
# Портал и bot_master работают на одном сервере, файл читается при каждом запросе
LIST_BASE_JSON = os.getenv("LIST_BASE_JSON", "/opt/base-portal/backend/data/list_base.json")

# Нумерация дней
BASE_DATE   = datetime(2025, 7, 14)
BASE_NUMBER = 53

# Окна скачивания по UTC (сервер работает по UTC, пользователь задаёт UTC+4)
# Канал 1: 07:03–07:06 UTC+4  →  03:03–03:06 UTC
CHANNEL1_WINDOW_START = (3, 3)   # (час, минута) UTC
CHANNEL1_WINDOW_END   = (3, 6)

# Канал 2: 09:02–09:06 UTC+4  →  05:02–05:06 UTC
CHANNEL2_WINDOW_START = (5, 2)
CHANNEL2_WINDOW_END   = (5, 6)

# ══════════════════════════════════════════════════════════════════════════════
# === ЛОГИРОВАНИЕ ==============================================================
# ══════════════════════════════════════════════════════════════════════════════
LOG_FILE = os.getenv("BOT_LOG_PATH", "/opt/bot/bot_master.log")
LOG_FMT  = "%(asctime)s [%(levelname)s] %(message)s"

_root = logging.getLogger()
_root.setLevel(logging.INFO)
_root.handlers.clear()  # убираем хендлеры от basicConfig если уже были

# Файл — всегда
try:
    _fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _fh.setFormatter(logging.Formatter(LOG_FMT))
    _root.addHandler(_fh)
except Exception as _e:
    print(f"[WARN] Не удалось открыть лог {LOG_FILE}: {_e}")

# stdout — всегда (Node.js перехватывает при spawn, systemd пишет в journal)
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(logging.Formatter(LOG_FMT))
_root.addHandler(_sh)

logger = logging.getLogger("bot_master")

# ══════════════════════════════════════════════════════════════════════════════
# === S3 КЛИЕНТ ================================================================
# ══════════════════════════════════════════════════════════════════════════════
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT or None,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# ══════════════════════════════════════════════════════════════════════════════
# === VK API ===================================================================
# ══════════════════════════════════════════════════════════════════════════════
BASE_URL_V3 = "https://ads.vk.com/api/v3"
BASE_URL_V2 = "https://ads.vk.com/api/v2"

MAX_UPLOADS_PER_TOKEN = 20
VK_UPLOAD_COUNTERS: Dict[str, int] = {}

RETRY_COUNT    = 3
RETRY_BACKOFF  = 2
RATE_LIMIT_SLEEP = (10, 30)


def req_with_retry(
    method: str, url: str, headers: Dict[str, str],
    params=None, json_body=None, files=None, data=None, timeout=60
) -> requests.Response:
    last_exc = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.request(
                method, url, headers=headers, params=params,
                json=json_body, data=data, files=files, timeout=timeout
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                logger.warning(f"VK rate limit 429, пауза {retry_after}s")
                time.sleep(retry_after)
                continue
            try:
                vk_err = resp.json().get("error", {})
                if isinstance(vk_err, dict) and vk_err.get("error_code") in (9, 29):
                    sf = random.uniform(*RATE_LIMIT_SLEEP)
                    logger.warning(f"VK flood {vk_err.get('error_code')}, пауза {sf:.1f}s")
                    time.sleep(sf)
                    continue
            except Exception:
                pass
            if resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} {resp.text}")
            return resp
        except Exception as e:
            last_exc = e
            sf = RETRY_BACKOFF ** (attempt - 1)
            logger.warning(f"{method} {url} попытка {attempt}/{RETRY_COUNT}: {e}. Повтор через {sf}s")
            time.sleep(sf)
    raise last_exc


# ══════════════════════════════════════════════════════════════════════════════
# === УТИЛИТЫ ==================================================================
# ══════════════════════════════════════════════════════════════════════════════

def send_error_sync(message: str):
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logging.warning(f"ERROR BOT не настроен: {message}")
        return
    try:
        url = _bot_api_url(ERROR_BOT_TOKEN, "sendMessage")
        requests.post(url,
            data={"chat_id": ERROR_CHAT_ID,
                  "text": f"❌ bot_master v{VersionBotMaster}: {message}",
                  "disable_notification": True},
            headers=_proxy_headers(), timeout=15)
    except Exception as e:
        logging.exception(f"send_error_sync failed: {e}")


async def send_error_async(message: str):
    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logging.warning(f"ERROR BOT не настроен: {message}")
        return
    try:
        url = _bot_api_url(ERROR_BOT_TOKEN, "sendMessage")
        timeout = aiohttp.ClientTimeout(connect=15, total=30)
        connector = _aiohttp_connector()
        if connector:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                await session.post(url,
                    data={"chat_id": ERROR_CHAT_ID,
                          "text": f"❌ bot_master v{VersionBotMaster}: {message}",
                          "disable_notification": "true"},
                    headers=_proxy_headers())
        else:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                await session.post(url,
                    data={"chat_id": ERROR_CHAT_ID,
                          "text": f"❌ bot_master v{VersionBotMaster}: {message}",
                          "disable_notification": "true"},
                    headers=_proxy_headers())
    except Exception:
        logging.exception("send_error_async failed")
        send_error_sync(message)


def get_day_number(today: datetime) -> int:
    delta = (today - BASE_DATE).days
    return BASE_NUMBER + delta


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def seconds_until_window(hour_utc: int, minute_utc: int) -> float:
    """Секунд до следующего наступления HH:MM UTC."""
    now = now_utc()
    target = now.replace(hour=hour_utc, minute=minute_utc, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


# ══════════════════════════════════════════════════════════════════════════════
# === КАБИНЕТЫ ИЗ ПОРТАЛА =====================================================
# ══════════════════════════════════════════════════════════════════════════════

def load_cabinets() -> List[dict]:
    """Читает список кабинетов из cabinets.json портала."""
    if not os.path.exists(CABINETS_JSON):
        logger.warning(f"cabinets.json не найден: {CABINETS_JSON}")
        return []
    try:
        with open(CABINETS_JSON, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        logger.warning(f"Неожиданный формат cabinets.json")
        return []
    except Exception as e:
        logger.exception(f"Ошибка чтения cabinets.json: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# === ЛОГИКА ИМЁ ФАЙЛОВ =======================================================
# ══════════════════════════════════════════════════════════════════════════════

def get_output_filename(file_name: str, day_number: int):
    """Для CSV канала 1. Возвращает (output_name, group_key)."""
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
    mapping = {
        "КР ДОП_3":  [915, 917, 918, 919],
        "КР 1":      [12063],
        "КР 2":      [11896],
        "КР ДОП_4":  [3587, 7389, 7553, 8614, 8732],
        "КР ДОП_5":  [9189, 9190, 9191, 9192, 9193, 9194, 9413, 9441, 9443, 9453, 9889, 9899],
        "КР ДОП_6":  [10141, 10240, 11682, 11729],
        "КР ДОП_8":  [12873],
        "КР ДОП_9":  [16263],
    }
    for name, ids in mapping.items():
        if cid and str(cid).isdigit() and int(cid) in ids:
            return f"{name} ({day_number}).txt"
    return f"КР ДОП_10 ({day_number}).txt"


def get_ch2_output_filename(orig_name: str, day_number: int) -> Optional[str]:
    """
    Маппинг для канала 2:
      web_121_* → КБ21 (день).txt
      web_122_* → КБ22 (день).txt
    """
    base = orig_name.lower()
    if "web_121" in base:
        return f"КБ21 ({day_number}).txt"
    elif "web_122" in base:
        return f"КБ22 ({day_number}).txt"
    return None


def order_txt_files(files: List[str]) -> List[str]:
    priority = [
        "КР ДОП_10", "КР ДОП_9", "КР ДОП_8", "КР ДОП_7", "КР ДОП_6",
        "КР ДОП_5",  "КР ДОП_4", "КР ДОП_3", "КР 2",     "КР 1",
        "ББ ДОП_3",  "ББ ДОП_2", "ББ",        "Б1",       "Б0",
        "КБ21",      "КБ22",
    ]

    def key(p):
        name = os.path.basename(p)
        base = name.rsplit(".", 1)[0]
        if base.endswith(")"):
            base = base.split(" (")[0]
        try:
            return priority.index(base)
        except ValueError:
            return len(priority) + 1000

    return sorted(files, key=key)


# ══════════════════════════════════════════════════════════════════════════════
# === СКАЧИВАНИЕ ИЗ TELEGRAM ==================================================
# ══════════════════════════════════════════════════════════════════════════════

async def _resolve_channel(client, channel: str):
    """
    Возвращает entity для канала/чата.
    Для числовых ID ищет в диалогах (InputPeerChat не работает по строке).
    """
    raw = channel.lstrip('-')
    if raw.isdigit():
        target = int(raw)
        async for dlg in client.iter_dialogs():
            if abs(dlg.entity.id) == target:
                return dlg.input_entity
        # Не нашли в диалогах — пробуем как есть (может сработать для каналов -100...)
        return channel
    return channel  # @username или t.me/+ ссылка — передаём напрямую


async def download_csv_from_channel(
    channel: str,
    to_folder: str,
    limit: int = 7,
    only_last_n: Optional[int] = None,
    session_name: str = "session_master",
) -> List[str]:
    """
    Скачивает CSV из указанного TG-канала.
    Перед подключением проверяет и при необходимости переключает прокси.
    only_last_n: если задано — скачиваем только последние N файлов.
    """
    global _active_proxy  # объявляем в начале функции — до любого использования
    os.makedirs(to_folder, exist_ok=True)

    # Проверяем/обновляем прокси (с 3 попытками)
    for attempt in range(1, 4):
        await ensure_proxy()
        proxy_kwargs = _telethon_proxy() or {}
        proxy_label = (_active_proxy or {}).get("label", "direct")
        logger.info("📥 Скачиваем из %s via %s (попытка %d)", channel, proxy_label, attempt)

        try:
            client = TelegramClient(session_name, API_ID, API_HASH, **proxy_kwargs)
            await client.start(PHONE)
            break  # успешно подключились
        except Exception as e:
            logger.warning("Подключение к TG не удалось (попытка %d): %s", attempt, e)
            # Сбрасываем прокси чтобы select_proxy выбрал следующий
            _active_proxy = None
            if attempt == 3:
                await send_error_async(f"Не удалось подключиться к TG за 3 попытки: {e}")
                return []
            await asyncio.sleep(5)
    else:
        return []

    today = datetime.today()
    date_suffix = today.strftime("(%d.%m)")
    seen_names: set = set()
    result_files: List[str] = []

    # Резолвим entity (нужно для InputPeerChat — обычных групп)
    resolved = await _resolve_channel(client, channel)
    logger.info("Резолв канала %s → %s", channel, type(resolved).__name__)

    try:
        async for msg in client.iter_messages(resolved, limit=limit):
            try:
                if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
                    orig_name = msg.file.name
                    if orig_name in ("389.csv", "390.csv"):
                        logger.info("Пропускаем файл по имени: %s", orig_name)
                        continue
                    if orig_name in seen_names:
                        logger.info("Пропускаем дубликат: %s", orig_name)
                        continue
                    seen_names.add(orig_name)

                    filename = orig_name.replace(".csv", f" {date_suffix}.csv")
                    path = os.path.join(to_folder, filename)
                    await msg.download_media(file=path)
                    if "6_web" in orig_name:
                        await asyncio.sleep(90)
                    result_files.append(path)
                    logger.info("✅ Скачан %s", filename)
                    await asyncio.sleep(random.uniform(10, 20))
            except Exception as e:
                logger.exception("Ошибка при скачивании сообщения")
                await send_error_async(f"Ошибка скачивания из {channel}: {e}")
    finally:
        await client.disconnect()

    if only_last_n is not None and len(result_files) > only_last_n:
        # Оставляем только последние N (они первые в iter_messages = свежие)
        result_files = result_files[:only_last_n]

    return result_files


# ══════════════════════════════════════════════════════════════════════════════
# === ОБРАБОТКА CSV → TXT =====================================================
# ══════════════════════════════════════════════════════════════════════════════

def process_csv_files_ch1(files: List[str]) -> List[str]:
    """Обработка CSV от канала 1 (старая логика + дедупликация)."""
    today = datetime.today()
    day_number = get_day_number(today)
    output_data: Dict[str, set] = defaultdict(set)
    approve_phones: set = set()

    for file in files:
        try:
            df = pd.read_csv(file)
            fname = os.path.basename(file)

            if df.empty or "phone" not in df.columns or \
               df["phone"].dropna().astype(str).str.strip().eq("").all():
                msg = f"Пропущен пустой CSV: {fname}"
                logger.warning(msg)
                send_error_sync(msg)
                continue

            output_name, group_key = get_output_filename(fname, day_number)
            if not group_key:
                logger.info("Файл %s не подпадает под обработку", fname)
                continue

            if group_key == "broker":
                if "channel_id" not in df.columns:
                    send_error_sync(f"В {fname} нет channel_id")
                    continue
                for _, row in df.iterrows():
                    phone = str(row.get("phone", "")).replace("+", "").strip()
                    if phone:
                        cid = row.get("channel_id", "")
                        output_data[broker_channel_group(str(cid), day_number)].add(phone)

            elif group_key == "6_web":
                if "channel_id" not in df.columns:
                    send_error_sync(f"В {fname} нет channel_id")
                    continue
                for _, row in df.iterrows():
                    phone = str(row.get("phone", "")).replace("+", "").strip()
                    if not phone:
                        continue
                    ch = str(row.get("channel_id", "")).strip()
                    group = {"15883": "ББ", "15686": "ББ ДОП_1", "15273": "ББ ДОП_2"}.get(ch, "ББ ДОП_3")
                    output_data[f"{group} ({day_number}).txt"].add(phone)

            else:
                phones = [str(p).replace("+", "").strip() for p in df["phone"].dropna() if str(p).strip()]
                if not phones:
                    send_error_sync(f"Нет номеров в {fname}")
                    continue
                if output_name:
                    output_data[output_name].update(phones)
                if "253" in fname:
                    approve_phones.update(phones)

        except Exception as e:
            msg = f"Ошибка обработки {file}: {e}"
            logger.exception(msg)
            send_error_sync(msg)

    os.makedirs("/opt/bot/txt", exist_ok=True)
    txt_files = []
    for name, phones in output_data.items():
        path = os.path.join("/opt/bot/txt", name)
        # Дедупликация — phones уже Set
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(phones)))
        txt_files.append(path)
        logger.info("Сохранён TXT: %s (%d номеров)", name, len(phones))

    if approve_phones:
        os.makedirs("/opt/bot/txt_for_lal", exist_ok=True)
        date_str = today.strftime("%d_%m_%Y")
        approve_path = f"/opt/bot/txt_for_lal/b_approve_{date_str}.txt"
        with open(approve_path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(approve_phones)))
        logger.info("Сохранён LAL файл: %s (%d номеров)", approve_path, len(approve_phones))

    return txt_files


def process_csv_files_ch2(files: List[str]) -> List[str]:
    """
    Обработка CSV от канала 2:
      web_121_* → КБ21 (день).txt
      web_122_* → КБ22 (день).txt
    Дедупликация номеров внутри каждого файла.
    """
    today = datetime.today()
    day_number = get_day_number(today)
    output_data: Dict[str, set] = defaultdict(set)

    for file in files:
        try:
            df = pd.read_csv(file)
            fname = os.path.basename(file)

            if df.empty or "phone" not in df.columns or \
               df["phone"].dropna().astype(str).str.strip().eq("").all():
                logger.warning("Пропущен пустой CSV (канал 2): %s", fname)
                continue

            out_name = get_ch2_output_filename(fname, day_number)
            if not out_name:
                logger.info("Файл %s не подпадает под обработку (канал 2)", fname)
                continue

            phones = [str(p).replace("+", "").strip() for p in df["phone"].dropna() if str(p).strip()]
            output_data[out_name].update(phones)
            logger.info("Обработан %s → %s (%d номеров)", fname, out_name, len(phones))

        except Exception as e:
            logger.exception("Ошибка обработки %s: %s", file, e)
            send_error_sync(f"Ошибка обработки CSV2 {file}: {e}")

    os.makedirs("/opt/bot/txt", exist_ok=True)
    txt_files = []
    for name, phones in output_data.items():
        path = os.path.join("/opt/bot/txt", name)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(phones)))
        txt_files.append(path)
        logger.info("Сохранён TXT (канал 2): %s (%d номеров)", name, len(phones))

    return txt_files


# ══════════════════════════════════════════════════════════════════════════════
# === S3 ======================================================================
# ══════════════════════════════════════════════════════════════════════════════

def upload_to_s3(file_path: str):
    filename = os.path.basename(file_path)
    if not filename.lower().endswith(".txt"):
        return
    key = f"txt/{filename}"
    try:
        s3.upload_file(file_path, S3_BUCKET, key)
        logger.info("Загружен в S3: %s", key)
    except Exception as e:
        msg = f"Ошибка загрузки в S3 {filename}: {e}"
        logger.exception(msg)
        send_error_sync(msg)


def download_new_subs_from_s3(to_folder="/opt/bot/new_subs") -> Optional[str]:
    if not NEW_S3_BUCKET:
        return None
    yesterday = datetime.today() - timedelta(days=1)
    filename = f"new_subs_{yesterday.strftime('%d_%m_%Y')}.txt"
    os.makedirs(to_folder, exist_ok=True)
    local_path = os.path.join(to_folder, filename)
    try:
        client = boto3.client(
            "s3",
            endpoint_url=NEW_S3_ENDPOINT or S3_ENDPOINT or None,
            aws_access_key_id=NEW_S3_ACCESS_KEY or S3_ACCESS_KEY,
            aws_secret_access_key=NEW_S3_SECRET_KEY or S3_SECRET_KEY
        )
        client.download_file(NEW_S3_BUCKET, filename, local_path)
        logger.info("Скачан new_subs: %s", filename)
        return local_path
    except Exception as e:
        logger.exception("Ошибка скачивания new_subs: %s", e)
        send_error_sync(f"Ошибка new_subs S3: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# === TELEGRAM ОТПРАВКА =======================================================
# ══════════════════════════════════════════════════════════════════════════════

async def send_file_to_telegram(file_path: str, chat_id: str = CHAT_ID):
    global _active_proxy  # объявляем в начале функции
    if not BOT_TOKEN or not chat_id:
        return

    await ensure_proxy()
    url = _bot_api_url(BOT_TOKEN, "sendDocument")
    proxy_label = (_active_proxy or {}).get("label", "direct")
    logger.info("📤 Отправка %s в TG via %s", os.path.basename(file_path), proxy_label)

    timeout = aiohttp.ClientTimeout(connect=15, total=120)
    connector = _aiohttp_connector()

    async def _do_send(session):
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", chat_id)
            form.add_field("document", f)
            form.add_field("disable_notification", "true")
            async with session.post(url, data=form, headers=_proxy_headers()) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    raise Exception(f"TG API {resp.status}: {txt}")

    for attempt in range(1, 4):
        try:
            if connector:
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    await _do_send(session)
            else:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await _do_send(session)
            return  # успех
        except Exception as e:
            logger.warning("Ошибка отправки в TG (попытка %d): %s", attempt, e)
            if attempt < 3:
                _active_proxy = None
                await ensure_proxy()
                connector = _aiohttp_connector()
                url = _bot_api_url(BOT_TOKEN, "sendDocument")
                await asyncio.sleep(3)
            else:
                logger.exception("Не удалось отправить файл в TG")
                await send_error_async(f"Ошибка отправки TG {file_path}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# === VK ЗАГРУЗКА =============================================================
# ══════════════════════════════════════════════════════════════════════════════

def upload_user_list_vk(file_path: str, list_name: str, vk_token: str, list_type="phones") -> int:
    url = f"{BASE_URL_V3}/remarketing/users_lists.json"
    headers = {"Authorization": f"Bearer {vk_token}"}
    files = {"file": open(file_path, "rb")}
    data  = {"name": list_name, "type": list_type}
    try:
        resp = req_with_retry("POST", url, headers=headers, files=files, data=data, timeout=60)
    finally:
        files["file"].close()
    result = resp.json()
    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        raise Exception(f"VK upload error: {result}")
    list_id = result.get("id")
    if not list_id:
        raise Exception(f"Нет list_id в ответе VK: {result}")
    return list_id


def create_segment_vk(list_id: int, segment_name: str, vk_token: str) -> int:
    url = f"{BASE_URL_V2}/remarketing/segments.json"
    headers = {"Authorization": f"Bearer {vk_token}", "Content-Type": "application/json"}
    payload = {
        "name": segment_name,
        "pass_condition": 1,
        "relations": [{"object_type": "remarketing_users_list",
                        "params": {"source_id": list_id, "type": "positive"}}],
    }
    resp = req_with_retry("POST", url, headers=headers, json_body=payload, timeout=60)
    result = resp.json()
    if resp.status_code != 200 or isinstance(result.get("error"), dict):
        raise Exception(f"VK segment error: {result}")
    return result.get("id")


async def upload_file_to_cabinets(
    file_path: str,
    cabinets: List[dict],
    list_name: Optional[str] = None,
    list_type: str = "phones",
    segment_prefix: str = "LAL ",
):
    """
    Загружает один TXT-файл в кабинеты, у которых эта база разрешена.
    Фильтр: cabinet['bases'] — список разрешённых баз (пустой = все).
    """
    fname = os.path.basename(file_path)
    base_name = os.path.splitext(fname)[0]
    # Убираем суффикс " (NNN)" из имени базы для проверки прав
    if base_name.endswith(")"):
        base_short = base_name.rsplit(" (", 1)[0]
    else:
        base_short = base_name

    effective_list_name = list_name or base_name
    segment_name = f"{segment_prefix}{effective_list_name}"

    for cabinet in cabinets:
        token = cabinet.get("token", "")
        if not token:
            continue

        # Проверяем разрешения: cabinet.bases пустой = все базы
        allowed = cabinet.get("bases", [])
        if allowed and base_short not in allowed:
            logger.info("Кабинет «%s»: база «%s» не в списке разрешённых, пропускаем",
                        cabinet.get("name"), base_short)
            continue

        if token not in VK_UPLOAD_COUNTERS:
            VK_UPLOAD_COUNTERS[token] = 0
        if VK_UPLOAD_COUNTERS[token] >= MAX_UPLOADS_PER_TOKEN:
            logger.warning("Лимит загрузок для кабинета «%s»", cabinet.get("name"))
            continue

        try:
            list_id = upload_user_list_vk(file_path, effective_list_name, token, list_type=list_type)
            create_segment_vk(list_id, segment_name, token)
            VK_UPLOAD_COUNTERS[token] += 1
            logger.info("✅ VK upload: кабинет «%s» ← «%s» list_id=%s",
                        cabinet.get("name"), fname, list_id)
        except Exception as e:
            msg = f"Ошибка VK upload «{fname}» → кабинет «{cabinet.get('name')}»: {e}"
            logger.exception(msg)
            send_error_sync(msg)

        # Пауза между кабинетами
        await asyncio.sleep(2)


# ══════════════════════════════════════════════════════════════════════════════
# === ПЛАНИРОВЩИК ВЫГРУЗКИ В VK ===============================================
# ══════════════════════════════════════════════════════════════════════════════

async def vk_upload_scheduler(cabinets: List[dict], txt_files: List[str]):
    """
    Для каждого кабинета и каждого файла из fileSchedules запускает выгрузку
    в заданное время (UTC+4, хранится как hour/minute UTC в cabinets.json после
    пересчёта на фронтенде).

    Ждём, пока не наступит нужное время, потом загружаем.
    Все задачи идут параллельно через asyncio.gather.
    """
    tasks = []

    for cabinet in cabinets:
        schedules: dict = cabinet.get("fileSchedules", {})
        token = cabinet.get("token", "")
        if not token or not schedules:
            continue

        for base_name, sched in schedules.items():
            if not sched.get("enabled"):
                continue

            hour   = sched.get("hour",   sched.get("time", "08:00").split(":")[0] if "time" in sched else 8)
            minute = sched.get("minute", sched.get("time", "08:00").split(":")[1] if "time" in sched else 0)
            hour   = int(hour)
            minute = int(minute)

            # Найти файл для этой базы в txt_files
            matching = [
                p for p in txt_files
                if os.path.basename(p).startswith(base_name + " (")
                   or os.path.basename(p) == f"{base_name}.txt"
            ]
            if not matching:
                logger.info("Файл для базы «%s» не найден, пропускаем планировщик", base_name)
                continue

            file_path = matching[0]

            async def scheduled_upload(cab=cabinet, path=file_path, h=hour, m=minute, bname=base_name):
                delay = seconds_until_window(h, m)
                logger.info(
                    "⏰ Кабинет «%s» файл «%s» — выгрузка через %.0f мин (%02d:%02d UTC)",
                    cab.get("name"), bname, delay / 60, h, m
                )
                await asyncio.sleep(delay)
                try:
                    await upload_file_to_cabinets(path, [cab])
                except Exception as e:
                    logger.exception("Ошибка плановой выгрузки: %s", e)
                    send_error_sync(f"Ошибка плановой выгрузки «{bname}»: {e}")

            tasks.append(scheduled_upload())

    if tasks:
        await asyncio.gather(*tasks)


# ══════════════════════════════════════════════════════════════════════════════
# === ВСПОМОГАТЕЛЬНЫЕ =========================================================
# ══════════════════════════════════════════════════════════════════════════════

async def process_previous_day_file() -> Optional[str]:
    yesterday = datetime.today() - timedelta(days=1)
    file_path = f"/opt/leads_postback/data/leads_sub6_{yesterday.strftime('%d.%m.%Y')}.txt"
    if not os.path.exists(file_path):
        logger.info("leads_sub6 за вчера не найден: %s", file_path)
        return None
    logger.info("Найден leads_sub6: %s", file_path)
    return file_path


def cleanup_files(files: List[str]):
    for f in files:
        try:
            if os.path.exists(f):
                os.remove(f)
                logger.info("Удалён: %s", f)
        except Exception:
            logger.exception("Ошибка удаления %s", f)


def cleanup_previous_day_txt_files():
    txt_dir = "/opt/bot/txt"
    if not os.path.exists(txt_dir):
        return
    yesterday_num = get_day_number(datetime.today() - timedelta(days=1))
    pattern = f"({yesterday_num}).txt"
    for filename in os.listdir(txt_dir):
        if filename.endswith(pattern):
            filepath = os.path.join(txt_dir, filename)
            try:
                os.remove(filepath)
                logger.info("Удалён вчерашний TXT: %s", filename)
            except Exception:
                logger.exception("Ошибка удаления %s", filepath)


# ══════════════════════════════════════════════════════════════════════════════
# === ЗАДАЧА КАНАЛА 1 =========================================================
# ══════════════════════════════════════════════════════════════════════════════

async def task_channel1() -> List[str]:
    """
    Ждёт окна 07:03–07:06 UTC+4 (03:03–03:06 UTC),
    затем скачивает CSV из канала 1, обрабатывает, загружает в S3.
    Возвращает список txt-файлов.
    """
    # Таймер запускает бота уже в нужное время — сразу качаем
    if not CHANNEL_NAME:
        logger.warning("CHANNEL_NAME не задан, пропускаем канал 1")
        return []

    csv_files = await download_csv_from_channel(
        CHANNEL_NAME, "/opt/bot/csv", limit=7, session_name="session_master"
    )
    if not csv_files:
        await send_error_async("CSV файлы не найдены в канале 1")
        return []

    txt_files = process_csv_files_ch1(csv_files)
    cleanup_files(csv_files)

    for f in txt_files:
        try:
            upload_to_s3(f)
        except Exception as e:
            send_error_sync(f"S3 ошибка {f}: {e}")

    return txt_files


# ══════════════════════════════════════════════════════════════════════════════
# === ЗАДАЧА КАНАЛА 2 =========================================================
# ══════════════════════════════════════════════════════════════════════════════

async def task_channel2() -> List[str]:
    """
    Ждёт окна 09:02–09:06 UTC+4 (05:02–05:06 UTC),
    затем скачивает последние 2 CSV из канала 2, обрабатывает, загружает в S3.
    """
    if not MANUAL_MODE:
        # Авто: ждём окно 05:02–05:06 UTC (09:02–09:06 UTC+4)
        start_h, start_m = CHANNEL2_WINDOW_START
        end_h,   end_m   = CHANNEL2_WINDOW_END
        delay  = seconds_until_window(start_h, start_m)
        jitter = random.uniform(0, (end_m - start_m) * 60)
        wait   = delay + jitter
        logger.info("Канал 2: ждём %.0f мин до скачивания (05:02–05:06 UTC)", wait / 60)
        await asyncio.sleep(wait)

    if not CHANNEL_NAME_2:
        logger.info("CHANNEL_NAME_2 не задан, пропускаем канал 2")
        return []

    csv_files = await download_csv_from_channel(
        CHANNEL_NAME_2, "/opt/bot/csv2", limit=7,
        only_last_n=2, session_name="session_master"
    )
    if not csv_files:
        await send_error_async("CSV файлы не найдены в канале 2")
        return []

    txt_files = process_csv_files_ch2(csv_files)
    cleanup_files(csv_files)

    for f in txt_files:
        try:
            upload_to_s3(f)
        except Exception as e:
            send_error_sync(f"S3 ошибка {f}: {e}")

    return txt_files


# ══════════════════════════════════════════════════════════════════════════════
# === ГЛАВНЫЙ ПРОЦЕСС =========================================================
# ══════════════════════════════════════════════════════════════════════════════

def refresh_portal_bases_sync():
    """
    Обновляет list_base.json портала напрямую — сканирует S3 через boto3
    и перезаписывает файл. Портал читает его при следующем запросе.
    Никаких HTTP-запросов и токенов не нужно — оба процесса на одном сервере.
    """
    if not os.path.dirname(LIST_BASE_JSON):
        return

    logger.info("🔄 Обновляем list_base.json портала...")
    try:
        import re as _re

        # Базовая дата и номер — должны совпадать с server.js
        BASE_DATE_PORTAL   = datetime(2026, 4, 13)
        BASE_NUMBER_PORTAL = 326

        def _num_to_date(num):
            return BASE_DATE_PORTAL + timedelta(days=num - BASE_NUMBER_PORTAL)

        def _parse_name(name):
            m = _re.match(r"^(.+?)\s*\((\d+)\)\.txt$", name)
            if m:
                return m.group(1).strip(), int(m.group(2))
            return None, None

        files_data = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="txt/"):
            for obj in page.get("Contents", []):
                key      = obj["Key"]
                filename = key.replace("txt/", "", 1)
                base_name, number = _parse_name(filename)
                if not base_name:
                    continue
                d = _num_to_date(number)
                files_data.append({
                    "key":      key,
                    "fileName": filename,
                    "baseName": base_name,
                    "number":   number,
                    "size":     obj["Size"],
                    "date":     d.isoformat() + "Z",
                })

        list_base_data = {
            "files":       files_data,
            "lastUpdated": datetime.utcnow().isoformat() + "Z",
        }

        os.makedirs(os.path.dirname(LIST_BASE_JSON), exist_ok=True)
        tmp = LIST_BASE_JSON + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list_base_data, f, ensure_ascii=False)
        os.replace(tmp, LIST_BASE_JSON)

        logger.info("✅ list_base.json обновлён: %d файлов", len(files_data))
    except Exception as e:
        logger.exception("Не удалось обновить list_base.json: %s", e)


async def refresh_portal_bases():
    """Асинхронная обёртка — запускает синхронную функцию в executor."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, refresh_portal_bases_sync)


async def run_test():
    """
    Тест-режим:
      1) Показывает первые 15 диалогов — для поиска ID каналов
      2) Проверяет канал 1: находит наименьший CSV и берёт 10 строк
      3) Проверяет канал 2 (если задан): показывает последние 2 файла
      4) Загружает test.txt в первый кабинет VK
    """
    logger.info("=== 🧪 ТЕСТ-РЕЖИМ ===")

    # Флаги из env (выставляются порталом)
    DO_DIALOGS  = os.getenv("TEST_CHECK_DIALOGS",  "1") == "1"
    DO_CHANNEL1 = os.getenv("TEST_CHECK_CHANNEL1", "1") == "1"
    DO_CHANNEL2 = os.getenv("TEST_CHECK_CHANNEL2", "1") == "1"
    DO_VK       = os.getenv("TEST_UPLOAD_VK",      "1") == "1"

    print(f"Параметры: диалоги={DO_DIALOGS} канал1={DO_CHANNEL1} канал2={DO_CHANNEL2} vk={DO_VK}\n")

    select_proxy()

    from telethon.tl.types import Channel, Chat

    proxy_kwargs = _telethon_proxy() or {}
    client = TelegramClient("session_master", API_ID, API_HASH, **proxy_kwargs)
    await client.start(PHONE)

    test_txt = None  # путь к test.txt для VK

    try:
        # ── 1. Список диалогов ──────────────────────────────────────────────
        if DO_DIALOGS:
            print("\n" + "="*65)
            print("📋 ПЕРВЫЕ 15 КАНАЛОВ/ЧАТОВ")
            print("="*65)
            print(f"{'#':<4} {'ID для .env':<25} {'Тип':<14} {'Название'}")
            print("-"*65)
            try:
                count = 0
                async for dialog in client.iter_dialogs():
                    if count >= 15:
                        break
                    entity = dialog.entity
                    raw_id = entity.id
                    if isinstance(entity, Channel):
                        env_id      = f"-100{raw_id}"
                        entity_type = "Канал" if not entity.megagroup else "Супергруппа"
                    elif isinstance(entity, Chat):
                        env_id      = f"-{raw_id}"
                        entity_type = "Чат"
                    else:
                        env_id      = str(raw_id)
                        entity_type = "Личка"
                    if hasattr(entity, 'username') and entity.username:
                        env_id = f"@{entity.username}"
                    print(f"{count+1:<4} {env_id:<25} {entity_type:<14} {dialog.name}")
                    count += 1
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning("Ошибка списка диалогов: %s", e)
            print("="*65)
            print(f"CHANNEL_NAME   = {CHANNEL_NAME or '❌ не задан'}")
            print(f"CHANNEL_NAME_2 = {CHANNEL_NAME_2 or '⚠️  не задан (опционально)'}")
            print("="*65 + "\n")

        # ── 2. Проверка канала 1 ────────────────────────────────────────────
        if DO_CHANNEL1:
            if not CHANNEL_NAME:
                logger.error("CHANNEL_NAME не задан в .env — пропускаем")
            else:
                print(f"🔍 Канал 1: {CHANNEL_NAME}")
                print("-"*40)
                candidates = []
                async for msg in client.iter_messages(CHANNEL_NAME, limit=15):
                    if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
                        candidates.append((msg.file.size or 0, msg))
                if not candidates:
                    print("  ❌ CSV файлов не найдено")
                else:
                    candidates.sort(key=lambda x: x[0])
                    smallest_size, smallest_msg = candidates[0]
                    print(f"  Файлов найдено: {len(candidates)}")
                    print(f"  Самый маленький: {smallest_msg.file.name} ({smallest_size // 1024} KB)")
                    os.makedirs("/opt/bot/csv_test", exist_ok=True)
                    path = f"/opt/bot/csv_test/test_ch1_{smallest_msg.file.name}"
                    await smallest_msg.download_media(file=path)
                    print(f"  ✅ Скачан: {smallest_msg.file.name}")
                    try:
                        df = pd.read_csv(path)
                        total = len(df)
                        cols  = list(df.columns)
                        print(f"  Строк: {total}, колонки: {cols}")
                        if "phone" in df.columns:
                            phones_count = df["phone"].dropna().count()
                            print(f"  Номеров телефонов: {phones_count}")
                            test_txt = "/opt/bot/txt/test.txt"
                            os.makedirs("/opt/bot/txt", exist_ok=True)
                            sample = [
                                str(p).replace("+", "").strip()
                                for p in df["phone"].dropna()
                                if str(p).replace("+", "").strip()
                            ][:10]
                            with open(test_txt, "w") as f:
                                f.write("\n".join(sample))
                            print(f"  test.txt: {len(sample)} номеров (для VK)")
                        else:
                            print(f"  ⚠️  Нет колонки phone, есть: {cols}")
                            test_txt = None
                    except Exception as e:
                        print(f"  ❌ Ошибка чтения CSV: {e}")
                        test_txt = None
                    finally:
                        try: os.remove(path)
                        except Exception: pass
                print()

        # ── 3. Проверка канала 2 ────────────────────────────────────────────
        if DO_CHANNEL2 and not CHANNEL_NAME_2:
            print("⚠️  CHANNEL_NAME_2 не задан — пропускаем проверку канала 2\n")
        elif DO_CHANNEL2:
            print(f"🔍 Канал 2: {CHANNEL_NAME_2}")
            print("-"*40)
            # Ищем сущность по ID напрямую через диалоги (надёжнее чем по строке)
            ch2_entity  = None
            ch2_used_id = None
            raw_num = int(CHANNEL_NAME_2.lstrip('-')) if CHANNEL_NAME_2.lstrip('-').isdigit() else None

            # Сначала пробуем найти в кэше диалогов
            if raw_num:
                async for dlg in client.iter_dialogs():
                    if abs(dlg.entity.id) == raw_num:
                        ch2_entity  = dlg.entity
                        ch2_used_id = str(dlg.entity.id)
                        print(f"  ✅ Найден в диалогах: «{dlg.name}» (id={ch2_used_id})")
                        if hasattr(dlg.entity, 'username') and dlg.entity.username:
                            print(f"     Username: @{dlg.entity.username}")
                            print(f"     💡 Обновите .env: CHANNEL_NAME_2=@{dlg.entity.username}")
                        else:
                            print(f"     💡 Обновите .env: CHANNEL_NAME_2={ch2_used_id}")
                        break

            ch2_files = []
            if ch2_entity is None:
                print(f"  ❌ Не найден в диалогах. Telethon не может обратиться к чату по числовому ID")
                print(f"     если он не был загружен в сессию. Попробуйте:")
                print(f"     1. Отправить любое сообщение в этот чат с вашего аккаунта")
                print(f"     2. Или попросить ссылку-приглашение (t.me/+...)")
                print(f"     3. Или указать @username если он есть")
            else:
                try:
                    # Используем input_entity для корректного обращения к чату
                    import inspect
                    peer = ch2_entity if not hasattr(ch2_entity, 'id') else ch2_entity
                    # Получаем input_entity из диалога
                    async for dlg in client.iter_dialogs():
                        if abs(dlg.entity.id) == abs(ch2_entity.id):
                            peer = dlg.input_entity
                            break
                    async for msg in client.iter_messages(peer, limit=10):
                        if msg.file and msg.file.name and msg.file.name.endswith(".csv"):
                            ch2_files.append(msg)
                            if len(ch2_files) >= 2:
                                break
                except Exception as e:
                    print(f"  ❌ Ошибка чтения сообщений: {e}")

            if ch2_files:
                print(f"  Последние файлы ({len(ch2_files)}):")
                for f in ch2_files:
                    kb = (f.file.size or 0) // 1024
                    mapped = get_ch2_output_filename(f.file.name, get_day_number(datetime.today()))
                    print(f"    • {f.file.name} ({kb} KB) → {mapped or '⚠️  не распознан'}")
            print()

    finally:
        await client.disconnect()

    # ── 4. VK загрузка ──────────────────────────────────────────────────────
    if DO_VK:
        cabs = load_cabinets()
        if not cabs:
            logger.error("Нет кабинетов в cabinets.json")
        elif not test_txt or not os.path.exists(test_txt):
            print("⚠️  test.txt не создан (канал 1 не проверялся?) — VK загрузка пропущена")
        else:
            cab = cabs[0]
            print(f"📤 VK: загружаем test.txt в кабинет «{cab.get('name')}»...")
            try:
                list_id = upload_user_list_vk(test_txt, "test", cab["token"], list_type="phones")
                create_segment_vk(list_id, "LAL test", cab["token"])
                print(f"  ✅ Загружено в «{cab.get('name')}» (list_id={list_id})")
            except Exception as e:
                print(f"  ❌ Ошибка VK: {e}")
            finally:
                try: os.remove(test_txt)
                except Exception: pass
    else:
        print("⏭️  VK загрузка пропущена")
        if test_txt and os.path.exists(test_txt):
            try: os.remove(test_txt)
            except Exception: pass

    logger.info("=== 🧪 ТЕСТ ЗАВЕРШЁН ===")


async def main():
    logger.info("=== 🚀 Запуск bot_master v%s ===", VersionBotMaster)

    # 0a) Выбираем рабочий прокси при старте
    select_proxy()

    # 0b) new_subs из дополнительного S3
    new_subs_path = download_new_subs_from_s3()

    # 1) leads_sub6 за вчера
    leads_sub6_path = await process_previous_day_file()

    # 2) Скачивание из каналов
    if MANUAL_MODE:
        logger.info("🖱 Ручной режим: ch1=%s ch2=%s vk=%s", MANUAL_CH1, MANUAL_CH2, MANUAL_VK)
        txt_files_ch1 = await task_channel1() if MANUAL_CH1 else []
        txt_files_ch2 = await task_channel2() if MANUAL_CH2 else []
    else:
        logger.info("⏰ Авто-режим: канал 1 сразу, канал 2 ждёт 05:02 UTC")
        ch1_task = asyncio.create_task(task_channel1())
        ch2_task = asyncio.create_task(task_channel2())
        txt_files_ch1, txt_files_ch2 = await asyncio.gather(ch1_task, ch2_task)

    # 3) Объединяем все TXT
    all_txt_files = list(txt_files_ch1) + list(txt_files_ch2)

    # 4) Обновляем список баз на портале (асинхронно, не блокирует)
    if all_txt_files:
        asyncio.create_task(refresh_portal_bases())

    # 5) Сортируем по приоритету
    txt_files_ordered = order_txt_files(all_txt_files)

    # 6) max_checker (опционально)
    checker_task = None
    if PROMOUSER_UPLOAD and MAX_CHECKER_AVAILABLE:
        try:
            checker_task = start_checker_task()
            logger.info("🔍 max_checker запущен")
        except Exception as e:
            logger.exception("Ошибка max_checker")
            await send_error_async(f"Ошибка max_checker: {e}")

    # 6) Pipeline файлов: new_subs, leads_sub6, txt
    files_pipeline = []
    if new_subs_path and os.path.exists(new_subs_path):
        files_pipeline.append(new_subs_path)
    if leads_sub6_path and os.path.exists(leads_sub6_path):
        files_pipeline.append(leads_sub6_path)
    files_pipeline.extend(txt_files_ordered)

    # 7) Отправляем все файлы в Telegram
    if SEND_FILES_TO_TELEGRAM:
        for path in files_pipeline:
            try:
                await send_file_to_telegram(path)
            except Exception as e:
                logger.exception("Ошибка отправки в TG")
                await send_error_async(f"TG отправка {path}: {e}")

    # 8) Загружаем в VK
    do_vk = MANUAL_VK if MANUAL_MODE else VK_UPLOAD
    if do_vk:
        cabinets = load_cabinets()
        if not cabinets:
            logger.warning("Кабинеты не загружены из портала, VK выгрузка пропущена")
        else:
            logger.info("Загружено %d кабинетов из портала", len(cabinets))
            if MANUAL_MODE:
                logger.info("🖱 Ручная VK выгрузка (без расписания)")
                for path in files_pipeline:
                    await upload_file_to_cabinets(path, cabinets)
            else:
                await vk_upload_scheduler(cabinets, files_pipeline)

    # 9) Удаляем new_subs
    try:
        if new_subs_path and os.path.exists(new_subs_path):
            os.remove(new_subs_path)
    except Exception:
        logger.exception("Ошибка удаления new_subs")

    # 10) Ждём max_checker
    if checker_task is not None:
        logger.info("⏳ Ожидаем max_checker...")
        try:
            await checker_task
            logger.info("✅ max_checker завершён")
        except Exception as e:
            logger.exception("Ошибка max_checker")
            await send_error_async(f"max_checker: {e}")

    # 11) Очистка
    try:
        cleanup_previous_day_txt_files()
    except Exception:
        logger.exception("Ошибка очистки файлов")

    logger.info("✅ bot_master завершён")

    # Завершаем SSH-туннель если использовался
    _stop_ssh_tunnel()


if __name__ == "__main__":
    if TEST_MODE:
        asyncio.run(run_test())
    else:
        asyncio.run(main())
