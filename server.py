#!/usr/bin/env python3 
#
import subprocess
import socket
from aiohttp import web
from aiohttp.web import Request
import aiofiles
from bot_env.create_obj4bot import bot, dp
import aiogram
from time import sleep, time
import os, sys, asyncio, logging
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
from bot_mov import Mov

class Srv:
    """Modul for server"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='mov_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=3,
                 ):
        Srv.countInstance += 1
        self.countInstance = Srv.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.Mov = Mov(logger=self.Logger)
        self.path_frag = self.Mov.save_file_path
        self.max_download = max_download
        # self.save_file_path = os.path.join(sys.path[0], 'video_frag')
        # self._create_mov_dir()
        self._print()
        
    #
    # выводим № объекта
    def _print(self):
        print(f'\n[Srv] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Srv] countInstance: [{self.countInstance}]')
#
    # проверка директории для фрагментов видео
    # def _create_srv_dir(self):
    #     """
    #     Создает директорию для хранения скачанных видеофайлов, 
    #     если она не существует
    #     """
    #     if not os.path.exists(self.save_file_path):
    #         os.makedirs(self.save_file_path)

    # отправляем ссылку для скачивания пользователям
    async def send_frag(self, chat_id: str, link_dnld: str):
        msg=f'Работа выполнена, скачайте видеофайл по ссылке ниже'
        try:
            await bot.send_message(chat_id=chat_id, text=msg)
            await bot.send_message(chat_id=chat_id, text=link_dnld)
            return chat_id, link_dnld
        except Exception as eR:
            print(f'\nERROR[Srv send_frag] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Srv send_frag] ERROR: {eR}')
            return None


    # получаем список отработанных фрагментов, 
    # отправляем их пользователям и записываем в БД
    async def send_frag_users(self):
        try:
            cursor_result = await self.Db.read_data_two(name_table='frag',
                                                        one_column_name='in_work_frag', 
                                                        one_params_status='fraged',
                                                        two_column_name='send', 
                                                        two_params_status='not_send',
                                                        )
        except Exception as eR:
            print(f'\nERROR[Srv send_frag_users read_data_two] ERROR: {eR}')
            self.Logger.log_info(f'\nERROR[Srv send_frag_users read_data_two] ERROR: {eR}')
            return None
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows=cursor_result.fetchall()
        if not rows: 
            print(f'\n[Srv: send_frag_users] В таблице [frag] нет фрагментов для отправки')
            return None
        # проверяем и получаем открытый порт
        port = self.find_open_port()
        # делаем список path_frag для создания ссылок и отправки
        path_frags = [row.path_frag for row in rows]
        chat_ids = [row.chat_id for row in rows]
        # формируем список ссылок для скачивания
        links = self.generate_download_link(file_paths=path_frags, port=port)
        if len(chat_ids) != len (links):
            print(f'\n[Srv send_frag_users] списки не совпадают: chat_ids: {chat_ids}, links: {links}')
            return None
        # отправляем пользователям ссылки для скачивания и записываем в БД
        for chat_id, link, path_frag in zip(chat_ids, links, path_frags):
            try:
                # отправляем пользователям ссылки для скачивания
                if not self.send_frag(chat_id=chat_id, link_dnld=link):
                    print(f'\n[Srv send_frag_users send_frag] не отправили в чат {chat_id} ссылку {link}')
            except Exception as eR:
                print(f'\nERROR[Srv send_frag_users send_frag] ERROR: {eR}')
                self.Logger.log_info(f'\nERROR[Srv send_frag_users send_frag] ERROR: {eR}')
                return None
            try:
                # записываем в БД
                diction = {'send': 'sended', 'dnld_link': link}
                if not self.update_table(path_frag, ['task', 'frag'], diction):
                    print(f'\n[Srv send_frag_users update_table] не записали в таблицы ссылку для скачивания пользователем')
            except Exception as eR:
                print(f'\nERROR[Srv send_frag_users update_table] ERROR: {eR}')
                self.Logger.log_info(f'\nERROR[Srv send_frag_users] ERROR: {eR}')
                return None
        return 
    
    # получаем локальный ip адрес
    def get_local_ip(self):
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        print(f'\n[Srv get_local_ip] local_ip: {local_ip}')
        return local_ip
    
    # определяем открытый порт
    def find_open_port(self):
        # Поиск свободного порта в диапазоне
        for port in range(1024, 49152):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                if sock.connect_ex(('localhost', port)) != 0:
                    return port
        raise Exception("\n[Srv get_local_ip] Не удалось найти открытый порт")

    # формируем ссылку для скачивания
    def generate_download_link(self, file_paths: list, port: str):
        ip_address = self.get_local_ip()
        links = []
        for file_path in file_paths:
            link = f"http://{ip_address}:{port}/download/{file_path.split('/')[-1]}"
            links.append(link)
        return links
    
    # определяем обработчик ссылок для скачивания 
    # async def download_file(self, request):
    #     file_name = request.match_info['file_name']
    #     file_path = f"path/to/your/files/{file_name}"
    #     response = web.FileResponse(file_path)
    #     return response

    # записываем данные в таблицы
    async def update_table(self, path_frag: str, names_table: list, diction: dict):
            for name_table in names_table:
                try:
                    if not await self.Db.update_table_link(name_table, path_frag, diction):
                        print(f'\n[Srv: update_table] не получилось записать в таблицу {name_table}: {diction}')
                    return diction
                except Exception as eR:
                    print(f'\nERROR[Srv: update_table] ERROR: {eR}') 
                    self.Logger.log_info(f'\nERROR[Srv: update_table] ERROR: {eR}') 
                    return None

    # проверяем все порты
    def check_ports_all(self):
        try:
            result = subprocess.run(["ss", "-tuln"], stdout=subprocess.PIPE, check=True)
            print(f'\n[Srv: check_ports] result: {result.stdout.decode()}')
            return True
        except subprocess.CalledProcessError as e:
            print(f"\nERROR [Srv: check_ports] Ошибка при выполнении команды: {str(e)}")
            self.Logger.log_info(f"\nERROR [Srv: check_ports] Ошибка при выполнении команды: {str(e)}")
            return None
    
    # проверяем отдельный порт
    def check_ports(self, port=None):
        try:
            if port:
                result = subprocess.run(["lsof", "-i", f":{port}"], stdout=subprocess.PIPE, check=True)
                print(f'\nПроверка порта {port}: {result.stdout.decode()}')
                return True
            else:
                result = subprocess.run(["ss", "-tuln"], stdout=subprocess.PIPE, check=True)
                print(f'\nПроверка всех портов: {result.stdout.decode()}')
                return True
        except subprocess.CalledProcessError as e:
            print(f"\nERROR [Srv: check_ports]: Ошибка при выполнении команды: {str(e)}")
            self.Logger.log_info(f"\nERROR [Srv: check_ports] Ошибка при выполнении команды: {str(e)}")
            return None

    # открываем порты
    def open_port(self, port=8080):
        try:
            # Открыть порт во входящих правилах брандмауэра
            subprocess.run(["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "--dport", str(port), "-j", "ACCEPT"], check=True)
            print(f"\n[Srv: open_port] Порт {port} успешно открыт.")
            return True
        except subprocess.CalledProcessError:
            print(f'\nERROR [Srv: open_port] Не удалось открыть порт {port}.'
                  f'Проверьте права доступа и наличие iptables.')
            self.Logger.log_info(f'\nERROR [Srv: open_port] Не удалось открыть порт {port}.'
                  f'Проверьте права доступа и наличие iptables.')
            return None
    
    # определяем обработчик ссылок для скачивания 
    async def download_file(self, request: Request):
        file_name = request.match_info['file_name']
        # file_path = f"/path/to/your/video/files/{file_name}"
        if not self.path_frag:
            print(f'\nERROR [Srv: download_file] self.Mov.save_file_path {self.path_frag}')
            return web.Response(text="Ошибка сервера: путь к файлу не найден", status=500)
        file_path = os.path.join (self.path_frag, file_name)
        try:
            async with aiofiles.open(file_path, mode='rb') as file:
                file_content = await file.read()
                response = web.Response(body=file_content, headers={'Content-Disposition': f'attachment; filename="{file_name}"'})
                response.content_type = 'video/mp4'
                return response
        except FileNotFoundError:
            return web.Response(text="Файл не найден", status=404)

#
# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот готов отправлять ссылки и раздавать файлы')
    srv=Srv() 
    print(f'\n[Srv: main] Проверяем порты...')
    srv.check_ports_all()
    print(f'\n[Srv: main] Проверяем srv.check_ports()...')
    srv.check_ports()
    #
    # Находим свободный порт
    port = srv.find_open_port()
    print(f'\n[Srv: main] Проверяем srv.check_ports(port={port})...')
    srv.check_ports(port=port)
    
    # Создаем асинхронное веб-приложение
    app = web.Application()
    app.router.add_route('GET', '/download/{file_name}', download_file)
    web.run_app(app, port=port)
    
    minut=1
    while True:
        #
        print(f'\nБот по отправке ссылок на видео ждет {minut} минут(ы) ...')
        sleep (int(60*minut))

        path_frages = await srv.get_path_frag()
        if not path_frages:
            print(f'\n[Srv main] фрагментов к отправке нет [path_frages]: {path_frages}') 









        # Пример использования
        file_path = "path/to/your/files/video.mp4"
        download_link = generate_download_link(file_path, port)
        # Отправка ссылки пользователю









    # async def start_server():
    #     app = web.Application()
    #     app.router.add_get('/download', download)

    #     # Получите свой внешний IP-адрес (можно использовать requests, как в предыдущих примерах)
    #     external_ip = 'your_external_ip'
    #     port = 8000

    #     # Отправьте ссылку пользователю через Telegram
    #     user_id = 'your_telegram_user_id'
    #     url = f"http://{external_ip}:{port}/download?user_id={user_id}"
    #     await send_telegram_link(user_id, url)

    #     runner = web.AppRunner(app)
    #     await runner.setup()
    #     site = web.TCPSite(runner, '0.0.0.0', port)
    #     await site.start()

    # web.run_app(start_server())

# def get_external_ip():
#     return requests.get('https://ifconfig.me').text

# class CustomHandler(http.server.SimpleHTTPRequestHandler):
#     def translate_path(self, path):
#         return file_path

# def serve_files(port=8000):
#     Handler = CustomHandler
#     with socketserver.TCPServer(("", port), Handler) as httpd:
#         print(f"Сервер запущен на порту {port}. Нажмите Ctrl+C для остановки сервера.")
#         httpd.serve_forever()

# def main():
#     global file_path

#     # Полный путь к файлу, который нужно расшарить
#     file_path = '/full/path/to/yourfile.txt'

#     # Получаем внешний IP-адрес
#     external_ip = get_external_ip()
#     port = 8000

#     # Выводим ссылку
#     url = f"http://{external_ip}:{port}/download"
#     print(f"Ссылка для скачивания файла: {url}")

#     # Запускаем веб-сервер в отдельном потоке
#     server_thread = threading.Thread(target=serve_files, args=(port,))
#     server_thread.start()

# if __name__ == "__main__":
#     main()



# # Конфигурация бота и базы данных
# BOT_TOKEN = 'YOUR_BOT_TOKEN'
# DB_DSN = 'dsn://user:password@host:port/database'

# async def download(request):
#     user_id = request.query.get('user_id')
#     async with aiopg.create_pool(DB_DSN) as pool:
#         async with pool.acquire() as conn:
#             async with conn.cursor() as cur:
#                 await cur.execute("SELECT file_path FROM files WHERE user_id = %s", (user_id,))
#                 file_path = await cur.fetchone()

#                 if file_path:
#                     file_path = file_path[0]
#                     with open(file_path, 'rb') as file:
#                         content = file.read()

#                     # Отметим файл как скачанный
#                     await cur.execute("UPDATE files SET downloaded = TRUE WHERE user_id = %s", (user_id,))

#                 else:
#                     return web.Response(text="Файл не найден", status=404)

#     return web.Response(body=content, headers={'Content-Disposition': f'attachment; filename="{os.path.basename(file_path)}"'})

# async def send_telegram_link(user_id, url):
#     bot = aiogram.Bot(BOT_TOKEN)
#     await bot.send_message(user_id, f"Ссылка для скачивания файла: {url}")

