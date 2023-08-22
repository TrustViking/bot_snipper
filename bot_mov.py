#!/usr/bin/env python3 
#
from time import sleep, time
from concurrent.futures import ThreadPoolExecutor
import os, sys, asyncio, logging
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
#
from moviepy.editor import VideoFileClip
from moviepy.editor import VideoFileClip, concatenate_videoclips, TextClip 
from moviepy.video.fx import all as vfx
#
#
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
    #

    # создаем (заполняем) таблицу frag на базе task
    async def insert_frag (self):
        # отбираем скачанные, но не отработанные ссылки в таблице 'task'
        try:
            async_results = await self.Db.read_data_two( 
                            name_table = 'task',  
                            one_column_name = 'in_work_download', 
                            one_params_status = 'downloaded',
                            two_column_name = 'in_work_frag', 
                            two_params_status = 'not_frag',
                                                    )
        except Exception as eR:
            print(f'\nERROR[Mov insert_frag read_data_two task] ERROR: {eR}') 
            self.Logger.log_info(f'\nERROR[Mov insert_frag read_data_two] ERROR: {eR}') 
            return None
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows=async_results.fetchall()
        if not rows: 
            print(f'\n[Mov: insert_frag] В таблице [task] нет ссылок для фрагментации')
            return None
        # делаем список кортежей date_message и uid
        date_message_uid = [(row.date_message, row.user_id, row) for row in rows]
        data=[] # список записанных строчек в таблицу frag
        for date_message, user_id, row in date_message_uid:
            print(f'\n[Mov: insert_frag] проверяем в таблице [frag] date_message: {date_message} user_id: {user_id}'
                  f'row: {row}')
            try:
                async_results_check = await self.Db.read_data_two ( 
                                            name_table='frag', 
                                            one_column_name='date_message', 
                                            one_params_status=date_message,
                                            two_column_name='user_id', 
                                            two_params_status=user_id,
                                            )
            except Exception as eR:
                print(f'\nERROR[Mov insert_frag read_data_two frag] ERROR: {eR}') 
                self.Logger.log_info(f'\nERROR[Mov insert_frag read_data_two frag] ERROR: {eR}') 
                return None
            # rows_table=async_results_table.fetchall()
            row_check=async_results_check.fetchone()
            print(f'\n[Mov: insert_frag]  row_check: {row_check} type {type(row_check)}')
            if row_check and len(row_check):
                if date_message==row_check.date_message and user_id==row_check.user_id:
                    print(f'\n[Mov: insert_frag] В таблице [frag] повторяется задача: {row_check}\n'
                          f'date_message: {date_message} - check: {row_check.date_message}\n'
                          f'user_id: {user_id} - check {row_check.user_id}')
                    continue
            # video_id (скачанные и не фрагментированные) -> в таблицу frag
            str_name=str(row.video_id + '_' + row.user_id + row.date_message)
            print(f'\n[Mov: insert_frag] str_name: {str_name}')
            name_frag="".join(str_name.replace(":", "").replace(" ", "").replace("-", ""))
            print(f'\n[Mov: insert_frag] name_frag: {name_frag}')
            diction = {
                        'date_message': row.date_message,
                        'username': row.username,
                        'user_id': row.user_id,
                        'chat_id': row.chat_id,
                        'time_task': int(time()), 
                        'url_video_y2b': row.url_video_y2b,
                        'video_id': row.video_id,
                        'timestamp_start': row.timestamp_start,
                        'timestamp_end': row.timestamp_end,
                        'in_work_download': row.in_work_download,
                        'path_download': row.path_download,
                        'in_work_frag': 'not_frag',
                        'name_frag': name_frag,
                        'path_frag': 'not_frag',
                        'send': 'not_send',
                        'send2group_file_id': 'not_id',
                        'resend': 'not_resend',
                        'resend_file_id': 'not_id',
                        }
            # записываем в таблицу frag новые задачи
            try:
                if not await self.Db.insert_data('frag', diction):
                    print(f'\n[Mov: insert_frag] в таблицу [frag] не получилось записать данные: {diction}')
                    continue
            except Exception as eR:
                print(f'\nERROR[Mov insert_frag insert_data] ERROR: {eR}') 
                self.Logger.log_info(f'\nERROR[Mov insert_frag insert_data] ERROR: {eR}') 
                return None
            data.append(diction)
        return data

    # вырезаем фрагмент по временным меткам в формате строки "чч:мм:сс" '01:03:05.35'
    def make_frag_time (self, path_file: str, 
                        t_start: str, 
                        t_end: str):
        try:
            # Обрезка видео по временным меткам
            clip = VideoFileClip(filename=path_file)
            frag = clip.subclip(t_start, t_end)
            return frag
        except Exception as eR:
            print(f"[Mov make_frag_time] Не удалось создать фрагмент {path_file}: {eR}")
            self.Logger.log_info(f"[Mov make_frag_time] Не удалось создать фрагмент {path_file}: {eR}")
            return None

    # сохраняем фрагмент
    def save_frag (self, frag: VideoFileClip, 
                        name_file: str, 
                        ext = '.mp4'):
        save_full_path = os.path.join(self.save_file_path, name_file+ext)
        if not save_full_path:
            print(f'[Mov save_frag] save_full_path is None: {save_full_path}')
            return None
        if not frag:
            print(f'[Mov save_frag] frag is None: {frag}')
            return None
        try:
            # Сохранение фрагмента в файл
            frag.write_videofile(filename=save_full_path)                  
            print(f'[Mov save_frag] frag saved: {save_full_path}') 
            return save_full_path
        except Exception as eR:
            print(f"[Mov save_frag] Не удалось записать фрагмент {save_full_path}: {eR}")
            self.Logger.log_info(f"[Mov save_frag] Не удалось записать фрагмент {save_full_path}: {eR}")
            return None

    # отрабатываем таблицу frag
    async def work_frag (self):
        # создаем (дополняем) таблицу frag на базе task
        try:
            if not await self.insert_frag():
                print(f'\n[Mov: work_frag] В таблице [task] нет ссылок для фрагментации')
        except Exception as eR:
            print(f"\n[Mov work_frag insert_frag] Не удалось записать в таблицу frag: {eR}")
            self.Logger.log_info(f"\n[Mov work_frag insert_frag] Не удалось записать в таблицу frag: {eR}")
            return None
        try:
            # отбираем скачанные, но не отработанные ссылки в таблице 'frag'
            async_results = await self.Db.read_data_two( 
                            name_table = 'frag',  
                            one_column_name = 'in_work_download', 
                            one_params_status = 'downloaded',
                            two_column_name = 'in_work_frag', 
                            two_params_status = 'not_frag',
                                                        )
        except Exception as eR:
            print(f"\n[Mov work_frag read_data_two] Не удалось прочитать таблицу frag: {eR}")
            self.Logger.log_info(f"[Mov work_frag read_data_two] Не удалось прочитать таблицу frag: {eR}")
            return None
        # print(f'\n[Mov: work_frag read_data_two] async_results: {async_results}')
        # список объектов <class 'sqlalchemy.engine.row.Row'>
        rows = async_results.fetchall()
        # print(f'\n[Mov: work_frag read_data_two] rows: {rows}')
        if not rows: 
            print(f'\n[Mov: work_frag] В таблице [frag] нет ссылок для фрагментации')
            return None
        # есть задачи для фрагментации
        # формируем аргументы для фрагментации
        # список имен фрагментов
        names_frags = [row.name_frag for row in rows]
        # vids = [row.video_id for row in rows]
        date_messages = [row.date_message for row in rows]
        user_ids = [row.user_id for row in rows]
        tasks_frag_time=[(row.path_download, row.timestamp_start, row.timestamp_end) for row in rows]
        print(f'\n[Mov: work_frag] tasks_frag_time: {tasks_frag_time}')
        if not tasks_frag_time:
            print(f'\n[Mov: work_frag] не сформировали аргументы для фрагментации: {tasks_frag_time}')
            self.Db.print_data('frag')
            return None
        list_frags=[] # список фрагментов
        for task_frag_time in tasks_frag_time:
            frag = self.make_frag_time(*task_frag_time)
            print(f'\n[Mov: work_frag make_frag_time] frag: {frag}')
            list_frags.append(frag)
        # записываем фрагменты на диск и в таблицу frag и task
        for frag, name_frag, date_message, user_id in zip(list_frags, names_frags, date_messages, user_ids):
            # print(f'\n[Mov: work_frag make_frag_time] frag: {frag}')
            # print(f'\n[Mov: work_frag] names_frag: {name_frag}')
            # print(f'\n[Mov: work_frag] vid: {vid}')
            if not frag or not name_frag or not date_message or not user_id:
                print(f'\n[Mov: work_frag] frag, name_frag, date_message, user_id is None')
                continue
            save_full_path=self.save_frag(frag, name_frag)
            if not save_full_path: 
                print(f'\n[Mov: work_frag] не получилось записать фрагмент: {name_frag}')
                continue
            diction = {'in_work_frag': 'fraged', 'path_frag': save_full_path}
            if not await self.Db.update_table_date('task', 
                                                   date_message, 
                                                   user_id, 
                                                   diction):
                print(f'\n[Mov: work_frag] не получилось записать в таблицу task: {save_full_path}')
            if not await self.Db.update_table_date('frag', 
                                                   date_message, 
                                                   user_id, 
                                                   diction):
                print(f'\n[Mov: work_frag] не получилось записать в таблицу frag: {save_full_path}')
        return date_messages, user_ids, names_frags 
#
# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот готов обрабатывать видео')
    mov=Mov() 
    minut=0.3
    while True:
        #
        print(f'\nБот по отработке видео ждет {minut} минут(ы) ...')
        sleep (int(60*minut))
        result=None
        try:
            result = await mov.work_frag()
        except Exception as eR:
            print(f'\nERROR[Mov main] ERROR: {eR}') 
            mov.Logger.log_info(f'\nERROR[Mov main] ERROR: {eR}') 
        
        # проверка результата
        if not result: 
            print(f'\n[Mov main] пока нет задач для фрагментации ...')
            mov.Logger.log_info(f'\n[Mov main] пока нет задач для фрагментации...')

        print(f'\nСодержание таблиц в БД...')
        await mov.Db.print_data('task')
        await mov.Db.print_data('frag')


if __name__ == "__main__":
    asyncio.run(main())
