#!/usr/bin/env python3
"""
tg_proxy_server.py — запускается на втором сервере
Простой HTTP-форвардер для Telegram Bot API.

НЕ хранит никаких токенов, сессий или секретов.
Токены передаются прямо в URL (/bot{TOKEN}/METHOD) — прокси
их видит как часть пути, но нигде не сохраняет.

Для Telethon (MTProto) используется уже работающий MTProto-прокси
на этом же сервере — никакого дополнительного кода не нужно.

Установка:
    pip install aiohttp

Запуск:
    python3 tg_proxy_server.py

Или через systemd (см. tg-proxy.service).

Переменные окружения (все опциональны):
    HTTP_PORT=8080
    ALLOWED_IPS=81.177.221.104          # через запятую, пусто = все
    PROXY_SECRET=                        # если задан — клиент должен слать заголовок X-Proxy-Secret
"""

import asyncio
import logging
import os

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("tg_proxy")

HTTP_PORT    = int(os.getenv("HTTP_PORT", "8080"))
PROXY_SECRET = os.getenv("PROXY_SECRET", "")
ALLOWED_IPS  = set(filter(None, os.getenv("ALLOWED_IPS", "").split(",")))

TG_BOT_API_HOST = "api.telegram.org"


async def proxy_handler(request: web.Request) -> web.Response:
    # --- Проверка IP ---
    peer = request.transport.get_extra_info("peername")
    client_ip = peer[0] if peer else ""
    if ALLOWED_IPS and client_ip not in ALLOWED_IPS:
        logger.warning(f"Отклонён IP: {client_ip}")
        return web.Response(status=403, text="Forbidden")

    # --- Проверка секрета (опционально) ---
    if PROXY_SECRET:
        if request.headers.get("X-Proxy-Secret", "") != PROXY_SECRET:
            logger.warning(f"Неверный секрет от {client_ip}")
            return web.Response(status=403, text="Forbidden")

    # --- Форвард на api.telegram.org ---
    # Входящий путь: /bot{TOKEN}/sendDocument?...
    # Целевой URL:   https://api.telegram.org/bot{TOKEN}/sendDocument?...
    path = request.path
    target_url = f"https://{TG_BOT_API_HOST}{path}"
    if request.query_string:
        target_url += f"?{request.query_string}"

    content_type = request.headers.get("Content-Type", "")

    try:
        async with aiohttp.ClientSession() as session:
            # Для multipart (отправка файлов) и urlencoded передаём тело как есть —
            # это важно для сохранения boundary у multipart.
            if request.method == "POST":
                body = await request.read()
                forward_headers = {}
                if content_type:
                    forward_headers["Content-Type"] = content_type

                async with session.post(
                    target_url,
                    data=body,
                    headers=forward_headers,
                    timeout=aiohttp.ClientTimeout(total=120)
                ) as resp:
                    data = await resp.read()
                    logger.info(f"{client_ip} POST {path} -> {resp.status}")
                    return web.Response(body=data, status=resp.status,
                                        content_type=resp.content_type)
            else:
                async with session.get(
                    target_url,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    data = await resp.read()
                    logger.info(f"{client_ip} GET {path} -> {resp.status}")
                    return web.Response(body=data, status=resp.status,
                                        content_type=resp.content_type)

    except Exception as e:
        logger.exception(f"Proxy error для {path}: {e}")
        return web.Response(status=502, text=f"Proxy error: {e}")


async def health_handler(request: web.Request) -> web.Response:
    return web.Response(
        text='{"status":"ok","service":"tg_proxy"}',
        content_type="application/json"
    )


async def main():
    app = web.Application(client_max_size=50 * 1024 * 1024)  # 50 MB для файлов
    app.router.add_get("/health", health_handler)
    app.router.add_route("*", "/{path_info:.*}", proxy_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()

    logger.info(f"HTTP прокси запущен на 0.0.0.0:{HTTP_PORT}")
    if ALLOWED_IPS:
        logger.info(f"Разрешённые IP: {ALLOWED_IPS}")
    if PROXY_SECRET:
        logger.info("Секрет: включён")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
