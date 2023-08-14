#!/usr/bin/env python3
#
from time import sleep
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
        #rows=[]
        rows=cursor_result_no.fetchall()
        #print(f'\n[Dnld: videoid_dnld] rows: {rows}, \ntype: {type(rows)} ')
        for row in rows:
            video_id=row.video_id
            vid4work.add(video_id)
            #
        return vid4work

    # читаем ссылки из таблицы download_link, которые уже закачаны
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
            #print(f'\n[Dnld: videoid_dnld] row: {row}, type row: {type(row)}')
            video_id=row.video_id
            #print(f'\n[Dnld: videoid_dnld] video_id: \n{video_id}, \ntype video_id: {type(video_id)}')
            vid_dnld.add(video_id)

        return vid_dnld
    #
    # читаем ссылки из таблицы download_link, которые надо закачать
    async def videoid4dnld(self):
        try:
            cursor_result = await self.Db.read_data_one(name_table='download_link',
                                                           column_name='worked_link', 
                                                           params_status='no')
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

    # читаем таблицу task, собираем video_id для таблицы download_link, 
    # уникальные ссылки записываем в download_link
    async def update_table(self):
        vid_work=set() # множество для отработки
        vid_table=set() # множество из таблицы download_link
        vid_write_table = set() # множество для записи в таблицу download_link
        
        # получаем множество неотработанных закачек в таблице task
        vid_work = await self.set_vid4work()
        
        # берем все данные из таблицы download_link
        cursor_result = await self.Db.data_table('download_link')
        rows=cursor_result.fetchall()
        #print(f'\n[Dnld set4table] rows: {rows}, \ntype: {type(rows)} ')
        for row in rows:
            vid=row.video_id
            vid_table.add(vid)
        
        # оставляем названия видео, которые есть в таблице task, но нет в таблице download_link
        vid_write_table = vid_work - vid_table
        if not vid_write_table:
            print(f'\n[Dnld update_table] пока нет новых закачек...')
            return None
        #
        data=[]
        diction4db={}
        print(f'\n[Dnld update_table] Надо перенести из таблицы task в download_link [{len(vid_write_table)}] ссылки:')
        for video_id in vid_write_table:
            print(video_id)
            diction4db['video_id']=video_id
            diction4db['url_video_y2b']=f'https://www.youtube.com/watch?v={video_id}'
            diction4db['worked_link']='no'
            diction4db['path_download']='no'
            # записываем в таблицу download_link
            if not await self.Db.insert_data('download_link', diction4db):
                print(f'\n[Dnld update_table] Ошибка записи в download_link новой ссылки {diction4db}')
            data.append(diction4db)
            diction4db={} # очищаем словарь данных в таблицу download_link
        return data

    # # читаем таблицу download_link и отбираем закачки, которые не выполнены
    # async def set4dnld(self):
    # формируем список-закачек из таблицы download_link
    async def download_video(self):
        # переносим новые задачи из таблицы task в download_link
        if not await self.update_table():
            print(f'Dnld download_video] пока нет новых задач в таблице task')
        #
        vid_dnld=set() # множество из download_link, которые уже закачаны 
        vid4work=set() # множество из download_link, которые надо закачать
        # получаем ссылки из таблицы download_link, которые помечены как отработанные
        vid_dnld = await self.videoid_dnld()
        # читаем ссылки из таблицы download_link, которые надо отработать
        vid4work = await self.videoid4dnld()
        # проверяемся на пересечении отработанных и не отработанных множествах таблицы download_link
        error_set=vid_dnld & vid4work
        if error_set:
            print(f'\nERROR [Dnld download_video] пересекаются отработанные и не отработанные закачки: {error_set}')
            #return None
        if not vid4work:
            print(f'\n[Dnld download_video] пока нет ссылок на отработку vid4work: {vid4work}')
            return None
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
                print(f'[Dnld run_dlp] path_save_video: {path_save_video}') 
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
    # проверяем скачал ли run_dlp файл и отмечаем в task и download_link 
    async def dnld_file(self, path: str, vid: str):
        list_file_dir=[(vid, fname) for fname in os.listdir(path) if vid in fname]
        if not list_file_dir:
            print(f'[Dnld dnld_file] list_file_dir: is {list_file_dir} \nFile: {vid} not dowload')
            return None
        print(f'[Dnld dnld_file] Video_id {vid} download name: {list_file_dir[0][1]}')

        # Находим в таблице 'download_link' строки с vid и записываем словарь значений
        diction_dnld={'worked_link': 'yes', 'path_download': str(os.path.join(path, list_file_dir[0][1]))}
        if not await self.Db.update_dnld_link(vid, diction_dnld):
            print(f'\nERROR [Dnld dnld_file] обновить [worked_link: yes] не получилось')
            return None
        # Находим в таблице 'task' строки с vid и записываем словарь значений
        diction_task={'in_work_download': 'yes'}
        if not await self.Db.update_task(vid, diction_task):
            print(f'\nERROR [Dnld dnld_file] обновить [in_work_download: yes] не получилось')
            return None
        else: return vid, list_file_dir[0][1]
    #
    # проверяем соответствие скачаных файлов и БД
    async def check_dnld_file(self):
        async_results = await self.Db.read_data_one(
                            name_table = 'download_link', 
                            column_name = 'worked_link', 
                            params_status = 'yes')
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows=async_results.fetchall()
        if not rows: 
            print(f'\n[Dnld: check_dnld_file] В таблице [download_link] нет отработанных ссылок')
            return None
        # video_id (скачанные) из БД добавляем в множество - set()
        vid_dnld = {row.video_id for row in rows}
        print(f'\n[Dnld: check_dnld_file] В таблице [download_link] закачаны ссылки {vid_dnld}')

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
        print(f'\n[Dnld check_dnld_file] БД не совпадает с файлами на диске. \nНадо закачать {set_vid4dnld}')
        # вносим изменения в download_link, ссылки, которые по факту не закачаны
        diction={'worked_link': 'no', 'path_download': 'no'}
        for vid in set_vid4dnld:
            if not await self.Db.update_dnld_link(vid, diction):
                print(f'\nERROR [Dnld check_dnld_file] обновить {vid} не получилось')
        # Находим в таблице 'task' строки с vid и записываем словарь значений
        diction_task={'in_work_download': 'no'}
        if not await self.Db.update_task(vid, diction_task):
            print(f'\nERROR [Dnld check_dnld_file] обновить [in_work_download: no] не получилось')
            return None
        return set_vid4dnld

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
        print(f'\n[Dnld dlp] command_list: {command_list}')
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
    # создаем объект класса 
    dnld=Dnld() 
    #
    minut=1
    while True:
        #
        print(f'\nБот по скачиванию ждет {minut} минут(ы) ...')
        sleep (60*minut)
        print(f'\nСодержание таблиц в БД...')
        await dnld.Db.print_data(name_table='task')
        await dnld.Db.print_data(name_table='download_link')
        
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
            
            # если нет закачек, сверяем БД и файлы на диске
            # vid_dnld = await dnld.check_dnld_file() 
            if await dnld.check_dnld_file():
                await dnld.Db.print_data(name_table='task')
                await dnld.Db.print_data(name_table='download_link')
            
            # удаляем временные файлы после скачивания dlp
            await dnld.delete_non_mp4_files()            
            
            # если нет списка закачек, то после сравнения БД и файлов на диске, новая итерация
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
        await dnld.Db.print_data(name_table='download_link')

        # сверяем БД и файлы закачки
        vid_dnld = await dnld.check_dnld_file() 
        if vid_dnld:
            await dnld.Db.print_data(name_table='task')
            await dnld.Db.print_data(name_table='download_link')
        # удаляем временные файлы после скачивания dlp
        await dnld.delete_non_mp4_files()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
