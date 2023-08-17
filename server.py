#!/usr/bin/env python3 
#
from aiohttp import web
import aiogram
import http.server
import socketserver
import requests
import threading
import os, sys, asyncio, logging
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB

class Mov:
    """Modul for video composition"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='mov_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=3,
                 ):
        Mov.countInstance += 1
        self.countInstance = Mov.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.max_download = max_download
        self.save_file_path = os.path.join(sys.path[0], 'video_frag')
        self._create_mov_dir()
        self._print()
        
    #
    # выводим № объекта
    def _print(self):
        print(f'[Mov] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Mov] countInstance: [{self.countInstance}]')
#
    # проверка директории для фрагментов видео
    def _create_mov_dir(self):
        """
        Создает директорию для хранения скачанных видеофайлов, 
        если она не существует
        """
        if not os.path.exists(self.save_file_path):
            os.makedirs(self.save_file_path)




def get_external_ip():
    return requests.get('https://ifconfig.me').text

class CustomHandler(http.server.SimpleHTTPRequestHandler):
    def translate_path(self, path):
        return file_path

def serve_files(port=8000):
    Handler = CustomHandler
    with socketserver.TCPServer(("", port), Handler) as httpd:
        print(f"Сервер запущен на порту {port}. Нажмите Ctrl+C для остановки сервера.")
        httpd.serve_forever()

def main():
    global file_path

    # Полный путь к файлу, который нужно расшарить
    file_path = '/full/path/to/yourfile.txt'

    # Получаем внешний IP-адрес
    external_ip = get_external_ip()
    port = 8000

    # Выводим ссылку
    url = f"http://{external_ip}:{port}/download"
    print(f"Ссылка для скачивания файла: {url}")

    # Запускаем веб-сервер в отдельном потоке
    server_thread = threading.Thread(target=serve_files, args=(port,))
    server_thread.start()

if __name__ == "__main__":
    main()



# Конфигурация бота и базы данных
BOT_TOKEN = 'YOUR_BOT_TOKEN'
DB_DSN = 'dsn://user:password@host:port/database'

async def download(request):
    user_id = request.query.get('user_id')
    async with aiopg.create_pool(DB_DSN) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT file_path FROM files WHERE user_id = %s", (user_id,))
                file_path = await cur.fetchone()

                if file_path:
                    file_path = file_path[0]
                    with open(file_path, 'rb') as file:
                        content = file.read()

                    # Отметим файл как скачанный
                    await cur.execute("UPDATE files SET downloaded = TRUE WHERE user_id = %s", (user_id,))

                else:
                    return web.Response(text="Файл не найден", status=404)

    return web.Response(body=content, headers={'Content-Disposition': f'attachment; filename="{os.path.basename(file_path)}"'})

async def send_telegram_link(user_id, url):
    bot = aiogram.Bot(BOT_TOKEN)
    await bot.send_message(user_id, f"Ссылка для скачивания файла: {url}")

async def start_server():
    app = web.Application()
    app.router.add_get('/download', download)

    # Получите свой внешний IP-адрес (можно использовать requests, как в предыдущих примерах)
    external_ip = 'your_external_ip'
    port = 8000

    # Отправьте ссылку пользователю через Telegram
    user_id = 'your_telegram_user_id'
    url = f"http://{external_ip}:{port}/download?user_id={user_id}"
    await send_telegram_link(user_id, url)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

web.run_app(start_server())
