#!/usr/bin/env python3
#
from time import sleep, time
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
                 max_download=5,
                 ):
        Dnld.countInstance += 1
        self.countInstance = Dnld.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.max_download = max_download
        self.downloaded_file_path = os.path.join(sys.path[0], 'video_source')
        self._print()
        self._create_dnld_directory()

    # выводим № объекта
    def _print(self):
        print(f'[Dnld] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Dnld] countInstance: [{self.countInstance}]')
        #     
    def _create_dnld_directory(self):
        """
        Создает директорию для хранения скачанных видеофайлов, если она не существует
        """
        # dnld_dir = os.path.dirname(self.downloaded_file_path)
        if not os.path.exists(self.downloaded_file_path):
            os.makedirs(self.downloaded_file_path)
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
                                                           params_status='not_download')
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
        #rows=[]
        rows=cursor_result_no.fetchall()
        #print(f'\n[Dnld: videoid_dnld] rows: {rows}, \ntype: {type(rows)} ')
        for row in rows:
            video_id=row.video_id
            vid4work.add(video_id)
            #
        return vid4work

    # читаем ссылки из таблицы dnld_link, которые уже закачаны
    async def videoid_dnld(self):
        try:
            cursor_result = await self.Db.read_data_one(name_table='dnld_link',
                                                           column_name='in_work_download', 
                                                           params_status='downloaded')
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
            #print(f'\n[Dnld: videoid_dnld] row: {row}, type row: {type(row)}')
            video_id=row.video_id
            #print(f'\n[Dnld: videoid_dnld] video_id: \n{video_id}, \ntype video_id: {type(video_id)}')
            vid_dnld.add(video_id)

        return vid_dnld
    #
    # читаем ссылки из таблицы dnld_link, которые надо закачать
    async def videoid4dnld(self):
        try:
            # ссылки из таблицы dnld_link, которые не закачаны
            cursor_result = await self.Db.read_data_one(name_table='dnld_link',
                                                           column_name='in_work_download', 
                                                           params_status='not_download')
        except SQLAlchemyError as eR:
            print(f'\nERROR [Dnld: videoid4dnld] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [Dnld: videoid4dnld] ERROR: {str(eR)}')
            return None
        if not cursor_result: 
            print(f'\n[Dnld: videoid4dnld] cursor_result is NONE')
            return None
        # вытаскиваем результаты запроса к БД
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        vid4dnld = set()
        rows=[]
        rows=cursor_result.fetchall()
        for row in rows:
            # вытаскиваем значения video_id 
            #print(f'\n[Dnld: videoid4dnld] row: {row}, type row: {type(row)}')
            video_id=row.video_id
            #print(f'\n[Dnld: videoid4dnld] video_id: \n{video_id}, \ntype video_id: {type(video_id)}')
            vid4dnld.add(video_id)
        return vid4dnld

    # читаем таблицу task, собираем video_id для таблицы dnld_link, 
    # уникальные ссылки записываем в dnld_link
    async def update_tables(self):
        vid_work=set() # множество для отработки
        vid_table=set() # множество из таблицы dnld_link
        vid_write_table = set() # множество для записи в таблицу dnld_link
        # получаем множество неотработанных закачек в таблице task
        vid_work = await self.set_vid4work()
        # берем все данные из таблицы dnld_link
        cursor_result = await self.Db.data_table('dnld_link')
        rows=cursor_result.fetchall()
        #print(f'\n[Dnld set4table] rows: {rows}, \ntype: {type(rows)} ')
        for row in rows:
            vid=row.video_id
            vid_table.add(vid)
        # оставляем id видео, которые есть в таблице task, но нет в таблице dnld_link
        vid_write_table = vid_work - vid_table
        if not vid_write_table:
            print(f'\n[Dnld update_tables] пока нет новых закачек...')
            return None
        #
        data=[]
        diction4db={}
        # print(f'\n[Dnld update_tables] Надо перенести из таблицы task в dnld_link [{len(vid_write_table)}] ссылки:')
        for video_id in vid_write_table:
            # print(video_id)
            diction4db['time_task']=int(time())
            diction4db['video_id']=video_id
            diction4db['url_video_y2b']=f'https://www.youtube.com/watch?v={video_id}'
            diction4db['in_work_download']='not_download'
            diction4db['path_download']='not_path'
            # записываем в таблицу dnld_link
            if not await self.Db.insert_data('dnld_link', diction4db):
                print(f'\n[Dnld update_tables] Ошибка записи в dnld_link новой ссылки {diction4db}')
            data.append(diction4db)
            diction4db={} # очищаем словарь данных в таблицу dnld_link
        return data

    # # читаем таблицу dnld_link и отбираем закачки, которые не выполнены
    # async def set4dnld(self):
    # формируем список-закачек из таблицы dnld_link
    async def download_video(self):
        # переносим новые задачи из таблицы task в dnld_link
        if not await self.update_tables():
            print(f'\n[Dnld download_video] пока нет новых задач в таблице task')
        #
        vid_dnld=set() # множество из dnld_link, которые уже закачаны 
        vid4work=set() # множество из dnld_link, которые надо закачать
        # получаем ссылки из таблицы dnld_link, которые помечены как отработанные
        vid_dnld = await self.videoid_dnld()
        # читаем ссылки из таблицы dnld_link, которые надо отработать
        vid4work = await self.videoid4dnld()
        if not vid4work:
            print(f'\n[Dnld download_video vid4work] пока нет в таблице [dnld_link] ссылок на закачку')
            return None
        # проверяемся на пересечении отработанных и не отработанных множествах таблицы dnld_link
        error_set=vid_dnld & vid4work
        if error_set:
            print(f'\nERROR [Dnld download_video] пересекаются отработанные и не отработанные закачки: {error_set}')
            #return None
        #
        print(f'\n[Dnld download_video] есть список закачек на отработку: {vid4work}')
        # создаем словарь {идентификатор : ютуб адрес}
        vid_url={}
        for vid in vid4work:
            # url_video=f'https://www.youtube.com/watch?v={vid}'
            vid_url={vid: f'https://www.youtube.com/watch?v={vid}'}
        print(f'\n[Dnld download_video] словарь на отработку: {vid_url}')
        return vid_url
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
            arg_list.append((ydl_opts, url, vid))
        return arg_list
    # 
    def progress_hook(self, d):
        """
            'status': состояние загрузки, может быть 'downloading', 'finished', или 'error'.
            'filename': имя текущего загружаемого файла.
            'tmpfilename': временное имя файла (если используется).
            'downloaded_bytes': количество уже загруженных байт.
            'total_bytes': общее количество байт в файле (если известно).
            'speed': текущая скорость загрузки в байтах в секунду.
            'eta': оставшееся время до завершения загрузки в секундах.
            '_percent_str': процент завершения загрузки в строковом формате, например, '50.3%'.
            'elapsed': время, прошедшее с начала загрузки.
        """
        if d['status'] == 'downloading':
            print(f"[Dnld progress_hook] Downloading {d['filename']} - {d['_percent_str']} complete")
        if d['status'] == 'finished':
            # self.downloaded_file_path = f"Downloaded: {d['filename']}"
            print(f"[Dnld progress_hook] Finished: {d['filename']}")
        if d['status'] == 'error':
            # self.downloaded_file_path = f"Downloaded: {d['filename']}"
            print(f"[Dnld progress_hook] ERROR downloaded_bytes: {d['downloaded_bytes']}")
# 
    # запускаем скачивание yt_dlp
    def run_dlp(self, arg: tuple):
        params, url, vid = arg
        try:
            with YoutubeDL(params) as ydl:
                ydl.download([url])
                # Возврат информации об успешном скачивании
                outtmpl=params['outtmpl']
                path_save_video=outtmpl['default']
                # print(f'[Dnld run_dlp] path_save_video: {path_save_video}') 
                # return f"Downloaded: {params['outtmpl']}" # 'outtmpl'
                #os.path.dirname(full_path)
        except Exception as eR:
            error_message = f'\nERROR[Dnld run_dlp] ERROR: {eR}'
            print(error_message) 
            self.Logger.log_info(error_message) 
            return error_message
        self.downloaded_file_path=os.path.dirname(path_save_video)
        return self.downloaded_file_path, vid
    #    
    # проверяем скачал ли run_dlp файл и отмечаем в task и dnld_link 
    async def dnld_file(self, path: str, vid: str):
        list_file_dir=[(vid, fname) for fname in os.listdir(path) if vid in fname]
        if not list_file_dir:
            print(f'[Dnld dnld_file] list_file_dir: is {list_file_dir} \nFile: {vid} not dowload')
            return None
        print(f'[Dnld dnld_file] Video_id {vid} download name: {list_file_dir[0][1]}')
        # Находим в таблице 'dnld_link' строки с vid и записываем словарь значений
        diction={'in_work_download': 'downloaded', 
                'path_download': str(os.path.join(path, list_file_dir[0][1])),
                'update_time': int(time()),
                      }
        if not await self.Db.update_table_vid('dnld_link', vid, diction):
            print(f'\nERROR [Dnld dnld_file] обновить dnld_link [in_work_download: downloaded] не получилось')
            return None
        if not await self.Db.update_table_vid('task', vid, diction):
            print(f'\nERROR [Dnld dnld_file] обновить task [in_work_download: downloaded] не получилось')
            return None
        #
        return vid, list_file_dir[0][1]
    #
    # если есть в dnld_link, но нет на диске, скачиваем 
    async def check_dnld_file(self):
        async_results = await self.Db.read_data_one(
                            name_table = 'dnld_link', 
                            column_name = 'in_work_download', 
                            params_status = 'downloaded')
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows=async_results.fetchall()
        if not rows: 
            print(f'\n[Dnld: check_dnld_file] В таблице [dnld_link] нет отработанных ссылок')
            return None
        # video_id (скачанные) из БД добавляем в множество - set()
        vid_dnld = {row.video_id for row in rows}
        print(f'\n[Dnld: check_dnld_file] В таблице [dnld_link] закачаны ссылки {vid_dnld}')
        #  множество имен файлов, которые находятся на диске 
        set_file_dir=set(os.listdir(self.downloaded_file_path))
        print(f'\n[Dnld: check_dnld_file] На диске закачано {len(set_file_dir)} файлов:')
        for i in set_file_dir: print(f'{i}')
        # выбираем файлы из БД, которые отмечены, но по факту не скачаны
        set_in_disk=set()
        for file_name in set_file_dir:
            for vid in vid_dnld:
                if vid in file_name:
                    set_in_disk.add(vid)
        # из множества имен в БД вычитаем имена, которые есть на диске
        set_vid4dnld=vid_dnld-set_in_disk
        if not set_vid4dnld:
            print(f'\n[Dnld check_dnld_file] БД совпадает с файлами на диске')
            return None
        # есть файлы, которые не скачаны
        print(f'\n[Dnld check_dnld_file] БД не совпадает с файлами на диске. \nНадо закачать {set_vid4dnld}')
        # вносим изменения в dnld_link, ссылки, которые по факту не закачаны
        diction={'in_work_download': 'not_download', 
                 'path_download': 'not_path',
                 'update_time': int(time()),
                 }
        for vid in set_vid4dnld:
            if not await self.Db.update_table_vid('dnld_link', vid, diction): 
                print(f'\nERROR [Dnld check_dnld_file] в dnld_link обновить {vid} не получилось {diction}')
            if not await self.Db.update_table_vid('task', vid, diction):
                print(f'\nERROR [Dnld check_dnld_file] в task обновить {vid} не получилось {diction}')
        return set_vid4dnld
    #
    # если есть на диске, но нет в task, отмечаем как скачаный 
    async def check_db_disk (self):
        async_results = await self.Db.read_data_one(
                            name_table = 'task', 
                            column_name = 'in_work_download', 
                            params_status = 'not_download')
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows=async_results.fetchall()
        if not rows: 
            print(f'\n[Dnld: check_db_disk] В таблице [dnld_link] нет отработанных ссылок')
            return None
        # video_id (не скачанные) из БД добавляем в множество - set()
        vid4dnld = {row.video_id for row in rows}
        print(f'\n[Dnld: check_db_disk] В таблице [dnld_link] не закачаны ссылки {vid4dnld}')
        #  множество имен файлов, которые находятся на диске 
        set_file_dir=set(os.listdir(self.downloaded_file_path))
        print(f'\n[Dnld: check_db_disk] На диске закачано {len(set_file_dir)} файлов:')
        for i in set_file_dir: 
            print(f'{i}')
        # выбираем файлы из БД, которые скачаны, но не отмечены
        set_db_disk=[]
        for file_name in set_file_dir:
            for vid in vid4dnld:
                if vid in file_name:
                    set_db_disk.append((vid, file_name))
        # из множества имен в БД вычитаем имена, которые есть на диске
        # set_vid4dnld=vid_dnld-set_in_disk
        if not set_db_disk:
            print(f'\n[Dnld check_db_disk] dnld_link совпадает с файлами на диске')
            return None
        # есть файлы, которые не отмечены
        print(f'\n[Dnld check_db_disk] dnld_link не совпадает с файлами на диске. \nНадо отметить {set_db_disk}')
        # вносим изменения в dnld_link, ссылки, которые по факту не закачаны
        for vid, file_name in set_db_disk:
            diction={'in_work_download': 'downloaded', 
                    'path_download': os.path.join(self.downloaded_file_path, file_name),
                    'update_time': int(time()),
                    }
            if not await self.Db.update_table_vid('dnld_link', vid, diction): 
                print(f'\nERROR [Dnld check_dnld_file] в dnld_link обновить {vid} не получилось {diction}')
            if not await self.Db.update_table_vid('task', vid, diction):
                print(f'\nERROR [Dnld check_dnld_file] в task обновить {vid} не получилось {diction}')
        return set_db_disk
    #
    # удаляем временные файлы после скачивания dlp
    async def delete_non_mp4_files(self):
        #  множество имен файлов, которые находятся на диске 
        set_file_dir=set(os.listdir(self.downloaded_file_path))
        # Отфильтровываем файлы, имена которых не заканчиваются на .mp4
        non_mp4_files = [file for file in set_file_dir if not file.endswith('.mp4')]
        if not non_mp4_files:
            print(f'\n[Dnld delete_non_mp4_files] Не обнаружено временных файлов для удаления')
            return None
        # Удаляем каждый из этих файлов
        for file in non_mp4_files:
            full_path=os.path.join(self.downloaded_file_path, file)
            try:
                os.remove(full_path)
                print(f"[Dnld delete_non_mp4_files] Файл {full_path} успешно удалён.")
            except Exception as eR:
                print(f"[Dnld delete_non_mp4_files] Не удалось удалить файл {full_path}: {eR}")
                self.Logger.log_info(f'\nERROR[Dnld delete_non_mp4_files] ERROR: {eR}') 

    # скачиваем файлы и проверяем их на диске
    async def dlp(self, vid_url: dict):
        #print(f'\n[Dnld main] есть словарь закачек (vid_url): {vid_url}')
        # создаем список команд для yt_dlp
        command_list=self.make_arg(vid_url)
        #print(f'\n[Dnld dlp] command_list: {command_list}')
        # Запускаем процессы скачивания
        try:
            with multiprocessing.Pool(self.max_download) as pool:
                # results = pool.map(self.run_dlp, command_list)
                return pool.map(self.run_dlp, command_list)
        except Exception as eR:
            print(f'\nERROR[Dnld dlp] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Dnld dlp] ERROR: {eR}') 
            return None



# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот по скачиванию начал мониторить задачи...\n')
    dnld=Dnld() 
    minut=0.2
    while True:
        #
        print(f'\nБот по скачиванию ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        print(f'\nСодержание таблиц в БД...')
        await dnld.Db.print_data('task')
        await dnld.Db.print_data('dnld_link')
        
        try:
            # формируем список-закачек из таблицы dnld_link
            vid_url = await dnld.download_video()
        except Exception as eR:
            print(f'\nERROR[Dnld main] ERROR: {eR}') 
            dnld.Logger.log_info(f'\nERROR[Dnld main] ERROR: {eR}') 
        
        # проверка есть ли видео для закачивания
        if not vid_url: 
            print(f'\n[Dnld main] пока нет задач для закачек...')
            dnld.Logger.log_info(f'\n[Dnld main] пока нет задач для закачек...')
            
            # если нет закачек, сверяем БД и файлы на диске
            # vid_dnld = await dnld.check_dnld_file() 
            if await dnld.check_dnld_file():
                await dnld.Db.print_data('task')
                await dnld.Db.print_data('dnld_link')
            if await dnld.check_db_disk():
                # await dnld.Db.print_data('task')
                await dnld.Db.print_data('dnld_link')

            # удаляем временные файлы после скачивания dlp
            await dnld.delete_non_mp4_files()            
            # так как нет списка закачек, то после сравнения БД и файлов на диске, 
            # уходим на новую итерацию
            continue
        
        # скачиваем файлы 
        results = await dnld.dlp(vid_url)
        if not results:
            print(f"\n[Dnld main] Ошибка при скачивании dnld.dlp(vid_url)")
            continue
        # проверяем скачанные файлы на диске
        for result in results:
            print(f"\n[Dnld main] Downloaded result: {result}")
            # проверяем скачал ли run_dlp файл на диск
            if not await dnld.dnld_file(*result):
                print(f"\n[Dnld main] result: {result} update full_parth_vid is NONE")

        print(f'\nСодержание таблиц в БД...')
        await dnld.Db.print_data(name_table='task')
        await dnld.Db.print_data(name_table='dnld_link')

        # сверяем БД и файлы закачки
        if await dnld.check_dnld_file():
            await dnld.Db.print_data(name_table='task')
            await dnld.Db.print_data(name_table='dnld_link')
        if await dnld.check_db_disk():
            # await dnld.Db.print_data(name_table='task')
            await dnld.Db.print_data(name_table='dnld_link')
        # удаляем временные файлы после скачивания dlp
        await dnld.delete_non_mp4_files()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
