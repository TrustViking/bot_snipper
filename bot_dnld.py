#!/usr/bin/env python3
#
import asyncio, os, sys, logging
from sqlalchemy.exc import SQLAlchemyError
import multiprocessing.dummy as multiprocessing
import subprocess
import shlex
from yt_dlp import YoutubeDL
from concurrent.futures import ProcessPoolExecutor
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
#
#
class Dnld:
    """Module for downloading video file from youtube"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='dnld_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=3,
                 ):
        Dnld.countInstance += 1
        self.countInstance = Dnld.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.max_download = max_download
        self._print()
        # self.name_file=f'%(channel)s/%(title)s.f%(format_id)s.%(resolution)s.%(fps)s.%(ext)s'
        # self.path_2save_video=os.path.join(sys.path[0], 'video_source', self.name_file)
        self.command_1='yt-dlp -w --progress --socket-timeout 999999 --no-playlist --merge-output-format mp4'
        self.command_2="--format 'bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio'"
        # self.command_3=f"--output {self.path_2save_video}"
        # self.command_all = f"{self.command_1} {self.command_2} {self.command_3}"
        # self.vid_dnld = set()
        # self.vid4work = set()
        #     
    # выводим № объекта
    def _print(self):
        print(f'[Dnld] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Dnld] countInstance: [{self.countInstance}]')
    #
    # читаем ссылки из таблицы task, которые не обрабатывались
    async def set_vid4work(self):
        '''
        # Методы: type: <class 'sqlalchemy.engine.cursor.CursorResult'>
        # fetchone(): Возвращает следующую строку результата запроса.
        # fetchall(): Возвращает все строки результата запроса.
        # fetchmany(size=None): Возвращает заданное количество строк результата запроса (по умолчанию размер указан в параметрах курсора).
        # keys(): Возвращает список имен столбцов результата.
        # close(): Закрывает результат (курсор).

        # Атрибуты:
        # rowcount: Возвращает количество строк, затронутых запросом.
        # description: Список кортежей, представляющих описание столбцов результата. Каждый кортеж содержит информацию о столбце, такую как имя, тип и т.д.
        # closed: Флаг, показывающий, закрыт ли результат.
        
        # Методы объектов <class 'sqlalchemy.engine.row.Row'>:
        # items(): Возвращает пары ключ-значение для каждого столбца в строке.
        # keys(): Возвращает имена столбцов в строке.
        # values(): Возвращает значения столбцов в строке.
        # get(key, default=None): Получение значения по имени столбца. Если столбец не существует, возвращается значение default.
        # as_dict(): Возвращает строки в виде словаря, где ключи - это имена столбцов, а значения - значения столбцов.
        # index(key): Возвращает позицию столбца с указанным именем в строке.
        
        # Атрибуты:
        # keys(): Возвращает имена столбцов в строке.
        # _fields: Атрибут, хранящий имена столбцов в строке.
        # _data: Словарь, содержащий данные строки, где ключи - это имена столбцов, а значения - значения столбцов.
        '''
        
        # выбираем из таблицы 'task' ссылки, которые еще не обработаны  
        try:
            # rows type: <class 'sqlalchemy.engine.cursor.CursorResult'>
            cursor_result_no = await self.Db.read_data_one(name_table='task',
                                                           column_name='in_work_download', 
                                                           params_status='no')
        except SQLAlchemyError as eR:
            print(f'\nERROR [Dnld: videoid_dnld] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [Dnld: videoid_dnld] ERROR: {str(eR)}')
            return None
        if not cursor_result_no: 
            print(f'\n[Dnld: videoid_dnld] cursor_result_no is NONE')
            return None
        # вытаскиваем результаты запроса к БД
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        vid4work = set()
        rows=[]
        rows=cursor_result_no.fetchall()
        #print(f'\n[Dnld: videoid_dnld] rows: {rows}, \ntype: {type(rows)} ')
        for row in rows:
            # вытаскиваем значения video_id и добавляем в множество unique_video_ids_no - set()
            print(f'\n[Dnld: videoid_dnld] row: \n{row}, \ntype row: {type(row)}')
            video_id=row.video_id
            print(f'\n[Dnld: videoid_dnld] video_id: \n{video_id}, \ntype video_id: {type(video_id)}')
            vid4work.add(video_id)
            #
        return vid4work

    # читаем ссылки из таблицы download_link, которые уже обрабатывались
    async def videoid_dnld(self):
        try:
            cursor_result = await self.Db.read_data_one(name_table='download_link',
                                                           column_name='worked_link', 
                                                           params_status='yes')
        except SQLAlchemyError as eR:
            print(f'\nERROR [Dnld: videoid_dnld] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [Dnld: videoid_dnld] ERROR: {str(eR)}')
            return None
        if not cursor_result: 
            print(f'\n[Dnld: videoid_dnld] cursor_result is NONE')
            return None
        # вытаскиваем результаты запроса к БД
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        vid_dnld = set()
        rows=[]
        rows=cursor_result.fetchall()
        #print(f'\n[Dnld: videoid_dnld] rows: {rows}, \ntype: {type(rows)} ')
        for row in rows:
            # вытаскиваем значения video_id и добавляем в множество unique_video_ids_no - set()
            print(f'\n[Dnld: videoid_dnld] row: {row}, type row: {type(row)}')
            video_id=row.video_id
            print(f'\n[Dnld: videoid_dnld] video_id: \n{video_id}, \ntype video_id: {type(video_id)}')
            vid_dnld.add(video_id)

        return vid_dnld
    #
    #
    # читаем таблицу snipper_task, собираем video_id для таблицы snipper_download
    async def set4dnld(self):
        #
        data=[] # список словарей данных в таблицу download_link
        diction4db={} # словарь данных в таблицу download_link
        vid4dnld=set() # множество для закачки
        vid_work=set() # множество для отработки
        vid_dnld=set() # множество уже закачанное
        #
        vid_work = await self.set_vid4work()
        vid_dnld = await self.videoid_dnld()
        vid4dnld = vid_work - vid_dnld

        #vid4dnld = self.set_vid4work() - self.videoid_dnld()
        print(f'\n[Dnld set4dnld] vid4dnld: {vid4dnld}, type vid4dnld: {type(vid4dnld)}')
        #
        # есть список видео на закачку
        for video_id in vid4dnld:
            diction4db['video_id']=video_id
            diction4db['url_video_y2b']=f'https://www.youtube.com/watch?v={video_id}'
            diction4db['worked_link']='no'
            data.append(diction4db)
            print(f'[Dnld set4dnld] поступила задача на закачку видео: {diction4db}')
            await self.Db.insert_data(name_table='download_link', data4db=diction4db)
            diction4db={} # очищаем словарь данных в таблицу snipper_download
        #return data
    #
    # формируем список-закачек из таблицы snipper_download
    async def download_video(self):
        try:
            # записываем в таблицу download_link ссылки для закачки
            await self.set4dnld()
        except Exception as eR:
            print(f'\nERROR[Dnld download_video set4dnld] ERROR: {eR}') 
            self.Logger.log_info(f'[Dnld download_video set4dnld] ERROR: {eR}')   
            return None
        #
        # читаем таблицу download_link и формируем полный список для закачек
        try:
            cursor_result = await self.Db.read_data_one(name_table='download_link',
                                                        column_name='worked_link', 
                                                        params_status='no')
        except Exception as eR:
            print(f'\n[Dnld download_video read_data_one] ERROR: {eR}') 
            self.Logger.log_info(f'[Dnld download_video] ERROR: {eR}')   
            return None
        if not cursor_result:
            print(f'\n[Dnld download_video] вместо списка закачек вернулось {cursor_result}')
            return None
        print(f'\n[Dnld download_video] есть список для закачек (cursor_result): {cursor_result}, type: {type(cursor_result)}')

        # вытаскиваем результаты запроса к БД
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        url_video_set= set()
        vid_set = set()
        #rows=[]
        rows=cursor_result.fetchall()
        print(f'\n[Dnld download_video] rows: {rows}, \ntype: {type(rows)} ')
        for row in rows:
            # вытаскиваем значения url_video_y2b и добавляем в список
            print(f'\n[Dnld download_video] row: \n{row}, \ntype row: {type(row)}')
            url_video=row.url_video_y2b
            vid=row.video_id
            print(f'\n[Dnld download_video] url_video: \n{url_video}, \ntype url_video: {type(url_video)}')
            url_video_set.add(url_video)
            vid_set.add(vid)
        # создаем словарь {идентификатор : ютуб адрес}
        vid_url={}
        vid_url = {vid: url for vid in vid_set for url in url_video_set if vid in url}
        # for vid in vid_set:
        #     for url in url_video_set:
        #         if vid in url: vid_url[vid] = url
        print(f'\n[Dnld download_video] vid_url: {vid_url}')
        return vid_url
    # 
    # # создаем список команд для yt_dlp
    # def make_com(self, vid_url: dict):
    #     # self.command_1='yt-dlp -w --progress --socket-timeout 999999 --no-playlist --merge-output-format mp4'
    #     # self.command_2="--format 'bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio'"
    #     command_list=[]
    #     for vid, url in vid_url.items():
    #         name_file=f'{vid}.%(resolution)s.%(fps)s.%(ext)s'
    #         path_save_video=os.path.join(sys.path[0], 'video_source', name_file)
    #         command_output=f"--output {path_save_video}"
    #         command_all = f"{self.command_1} {self.command_2} {command_output}"
    #         command = f'{command_all} URL={[url]}'
    #         print(f'[Dnld make_com] command: {command}')
    #         command_list.append(command)
    #     return command_list

    # # запускаем скачивание yt_dlp
    # def run_download(self, command: str):
    #     arg=shlex.split(command)
    #     print(f'\n[Dnld run_download] arg: {arg}')
    #     # не строка, а список команд, когда shell=False, т.е. без оболочки
    #     process = subprocess.Popen(arg, shell=True) 
    #     print(f'[Dnld run_download] process: {process}')
    #     #process.wait()  # Ждем, пока процесс завершится
#
# 
    # создаем список команд для yt_dlp
    def make_arg(self, vid_url: dict):
        arg_list=[]
        for vid, url in vid_url.items():
            name_file=f'{vid}.%(resolution)s.%(fps)s.%(ext)s'
            path_save_video=os.path.join(sys.path[0], 'video_source', name_file)
            ydl_opts = {
                'format': 'bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio',
                'merge_output_format': 'mp4',
                'socket_timeout': 999999,
                'no_playlist': True,
                'outtmpl': path_save_video,
                'progress_hooks': [self.progress_hook],
                'nooverwrites': True,
                        }
            arg_list.append((ydl_opts, url))
        return arg_list
# 
    def progress_hook(self, d):
        if d['status'] == 'downloading':
            print(f"Downloading {d['filename']} - {d['_percent_str']} complete")
#  
    # запускаем скачивание yt_dlp
    def run_dlp(self, arg: tuple):
        params, url = arg
        try:
            with YoutubeDL(params) as ydl:
                ydl.download([url])
        except Exception as eR:
            print(f'\nERROR[Dnld run_dlp] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Dnld run_dlp] ERROR: {eR}') 
# 
# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот по скачиванию начал мониторить задачи...\n')
    # создаем объект класса 
    dnld=Dnld() 
    #
    url_video=[]
    try:
        # формируем список-закачек из таблицы download_link
        vid_url = await dnld.download_video()
    except Exception as eR:
        print(f'\nERROR[Dnld main] ERROR: {eR}') 
        dnld.Logger.log_info(f'\nERROR[Dnld main] ERROR: {eR}') 
    # проверка есть ли видео для закачивания
    if not vid_url: 
        print(f'\n[Dnld main] пока нет задач для закачек...')
        dnld.Logger.log_info(f'\n[Dnld main] пока нет задач для закачек...')
        return None
    print(f'\n[Dnld main] есть словарь закачек (vid_url): {vid_url}')
    # 
    print(f'\nСодержание таблиц в БД...')
    await dnld.Db.print_data(name_table='task')
    await dnld.Db.print_data(name_table='download_link')
    #
    # создаем список команд для yt_dlp
    command_list=dnld.make_arg(vid_url)
    print(f'\n[Dnld main] command_list: {command_list}')
    # Запускаем процессы скачивания
    try:
        with multiprocessing.Pool(dnld.max_download) as pool:
            results = pool.map(dnld.run_dlp, command_list)
            for result in results:
                if result:
                    print(f"Downloaded: {result}")
                else:
                    print("Error downloading video")
    except Exception as eR:
        print(f'\nERROR[Dnld main] ERROR: {eR}') 
        dnld.Logger.log_info(f'\nERROR[Dnld main] ERROR: {eR}') 

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())


    # loop = asyncio.get_event_loop()
    # tasks = []
    # with ProcessPoolExecutor(max_workers=dnld.max_download) as executor:
    #     for url in url_video:
    #         task = loop.run_in_executor(executor, dnld.run_yt_dlps, url)
    #         tasks.append(task)

    #     # Ожидаем завершения всех процессов
    #     for future in asyncio.as_completed(tasks):
    #         result = await future
    #         print(f'[Dnld main] result: {result}')

if __name__ == "__main__":
    asyncio.run(main())





        #self.url = f'https://www.youtube.com/watch?v={video_id}'
        #command = ["yt-dlp", 
        # "--progress", 
        # "--socket-timeout", 
        # "999999", 
        # "--no-playlist", 
        # "--merge-output-format", 
        # "mp4", 
        # "--format", 
        # "bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio", 
        # "--output", 
        # "/media/ara/linux/snipper/bot_snipper/video_source/%(channel)s/%(title)s.f%(format_id)s.%(resolution)s.%(fps)s.%(ext)s", 
        # url]
        #command = f"yt-dlp --progress --socket-timeout 999999 --no-playlist --merge-output-format mp4 --format 'bv[ext=mp4][height=1080]+ba[ext=m4a]/bestaudio' --output '/media/ara/linux/snipper/bot_snipper/video_source/%(channel)s/%(title)s.f%(format_id)s.%(resolution)s.%(fps)s.%(ext)s' {url}"

# async def main(_):
#     print(f'Бот по скачиванию начал мониторить задачи...')
#     telega=Telega() # создаем объект и в нем регистрируем хэндлеры Клиента, 
#     telega.Logger.log_info(f'\n[main] Создали объект Telega()')
#     print(f'[main] Создали объект Telega()')
# #
# if __name__ == "__main__":
#     executor.start_polling(dp, skip_updates=True, on_startup=main)




