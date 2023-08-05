
import asyncio, aiosqlite
from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy import text
from sqlalchemy.orm import declarative_base, sessionmaker
import os, sys
from bot_env.mod_log import Logger
from data_base.table_db import metadata, tables
from sqlalchemy.exc import SQLAlchemyError
    # engine = create_async_engine("sqlite+aiosqlite://", creator=lambda: sqlite_connection)

#Base = declarative_base()
class BaseDB:
    """
    Создаем асинхронную базу данных:

    Аргументы:
    - logger: Logger
    - db_file='db_file.db'
    """
    countInstance=0

    def __init__(self,
                 logger: Logger,
                 db_file='db_file.db',
                 ):
        BaseDB.countInstance+=1
        self.countInstance=BaseDB.countInstance
        self.Logger = logger
        self._print()
        # создаем путь для БД
        self.db_file = os.path.join(sys.path[0], 'db_storage', db_file)
        # создаем директорию для БД, если такой папки нет
        #self._create_db_directory() 
        # формируем путь к лог файлу 'sqlalchemy'
        self.log_path = os.path.join(self.Logger.log_path, 'sqlalchemy')
        self.rows=None
        # создаем url БД
        # self.db_url = f'sqlite+pysqlite:///{self.db_file}'
        self.db_url = f'sqlite+aiosqlite:///{self.db_file}'
        # создаем объект engine для связи с БД
        # self.engine = create_engine(self.db_url, logging_name=self.log_path)
        self.engine = create_async_engine(self.db_url, logging_name=self.log_path)
        # Создаем асинхронный объект Session 
        #self.Session=sessionmaker(bind=self.engine)
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        # Определение структуры таблицы из другого модуля
        self.metadata = metadata
        # Импорт параметров таблицы из другого модуля
        self.table = tables
        # Создание таблицы в базе данных
        #await self._create_table()
        #asyncio.get_event_loop().run_until_complete(self._create_table())
        # формируем пустые множества
        # self.unique_video_ids_yes = set()
        # self.unique_video_ids_no = set()
#
    # выводим № объекта
    def _print(self):
        print(f'[BaseDB] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[BaseDB] countInstance: [{self.countInstance}]')
#
    
    # добавляем данные-строку в таблицу
    # data_bot = {'url_video_y2b': 'https://example.com', 'id_user': 'user1', 'id_chat': 'chat1'}
    async def insert_data(self, name_table: str, data4db: dict):
        #
        table=self.table.get(name_table)
        try:
            async with self.Session() as session:
                await session.execute(table.insert().values(**data4db))
                await session.commit()
                print(f'[BaseDB: insert_data] Data {str(data4db)} insert in table: {name_table} successfully!')
                self.Logger.log_info(f'[BaseDB: insert_data] Data added successfully!\n{str(data4db)}')   
        except SQLAlchemyError as eR:
            print(f'[BaseDB: insert_data] ERROR: {str(eR)}') 
            self.Logger.log_info(f'[BaseDB: insert_data] ERROR: {str(eR)}')   
            await session.rollback()
    #
    # добавляем данные-колонку в таблицу
    # data_bot = {'url_video_y2b': 'https://example.com', 'id_user': 'user1', 'id_chat': 'chat1'}
    async def insert_data_list(self, name_table: str, data4db: list):
        #
        #table=self.table.get(name_table)
        for data in data4db:
            try:
                await self.insert_data_str(name_table=name_table, 
                                           data4db=data)
            except SQLAlchemyError as eR:
                print(f'\n[BaseDB: insert_data_list] ERROR: {str(eR)}')
                self.Logger.log_info(f'[BaseDB: insert_data_list] ERROR: {str(eR)}')   
                return None
        return True
    #
    #
    # читаем все данные из таблицы исходя из одного условия 
    # async_results представляет собой объект ResultProxy, 
    # возвращаемый при выполнении запроса session.execute()
    async def read_data_one(self, 
                            name_table: str, 
                            column_name: str, 
                            params_status: str):
        #print(f'[BaseDB read_data_one] name_table: {name_table}')
        table=self.table.get(name_table)
        print(f'\n[BaseDB read_data_one] table: {name_table}, type: {type(table)}')
        try:
            async with self.Session() as session:
                # Формируем запрос с фильтром
                text_filter=f'{column_name} = :status'
                #
                async_results = await session.execute(
                                            select(table)
                                            .where(text(text_filter))
                                            .params(status=params_status)
                                            .order_by(table.c.id)  # Сортируем по порядковому номеру (предполагаем, что столбец id уникален)
                                                        )   
        except SQLAlchemyError as eR:
            print(f'[BaseDB: read_data_one] ERROR: {str(eR)}')
            self.Logger.log_info(f'[BaseDB: read_data_one] ERROR: {str(eR)}')
            await session.rollback()
            return None
        # Получаем список всех строк из объекта async_results
        #print(f'[BaseDB read_data_one] async_results: {async_results}')
        if not async_results:
            print(f'\n[BaseDB read_data_one] async_results NONE: {async_results}')
            return None
        print(f'\n[BaseDB read_data_one] table: {name_table} async_results: {async_results}, type: {type({async_results})} ')
        # async_results <sqlalchemy.engine.cursor.CursorResult object at 0x7f5c3abf1ea0>
        # type: <class 'set'>
        return async_results
    #
    # читаем все данные из таблицы исходя из двух условий 
    # async_results представляет собой объект ResultProxy, 
    # возвращаемый при выполнении запроса session.execute()
    async def read_data_two(self, 
                            name_table: str, 
                            one_column_name: str, 
                            one_params_status: str,
                            two_column_name: str, 
                            two_params_status: str,
                            ):
        table=self.table.get(name_table)
        print(f'\n[BaseDB read_data_two] table: {name_table}, type: {type(table)}')
        try:
            async with self.Session() as session:
                # Формируем запрос с фильтром
                text_filter=f'{one_column_name} = :status_one AND {two_column_name}=:status_two'
                async_results = await session.execute(
                    select(table)
                    .where(text(text_filter))
                    .params(status_one=one_params_status, 
                            status_two=two_params_status)
                    .order_by(table.c.id)  # Сортируем по порядковому номеру (предполагаем, что столбец id уникален)
                                                        )   
        except SQLAlchemyError as eR:
            print(f'[BaseDB: read_data_two] ERROR: {str(eR)}')
            self.Logger.log_info(f'[BaseDB: read_data_two] ERROR: {str(eR)}')
            await session.rollback()
            return None
        # Получаем список всех строк из объекта async_results
        if not async_results:
            print(f'[BaseDB read_data_two] async_results NONE: {async_results}')
            return None
        print(f'[BaseDB read_data_two] table: {name_table} async_results: {async_results}, type: {type({async_results})} ')
        # async_results <sqlalchemy.engine.cursor.CursorResult object at 0x7f5c3abf1ea0>
        # type: <class 'set'>
        return async_results
    # 
    # # читаем ссылки из таблицы-задачи, которые 
    # # надо проверить, скачивали мы ее ранее или нет, 
    # async def choice_link4download(self, name_table: str):
    #     '''
    #     # Методы: type: <class 'sqlalchemy.engine.cursor.CursorResult'>
    #     # fetchone(): Возвращает следующую строку результата запроса.
    #     # fetchall(): Возвращает все строки результата запроса.
    #     # fetchmany(size=None): Возвращает заданное количество строк результата запроса (по умолчанию размер указан в параметрах курсора).
    #     # keys(): Возвращает список имен столбцов результата.
    #     # close(): Закрывает результат (курсор).

    #     # Атрибуты:
    #     # rowcount: Возвращает количество строк, затронутых запросом.
    #     # description: Список кортежей, представляющих описание столбцов результата. Каждый кортеж содержит информацию о столбце, такую как имя, тип и т.д.
    #     # closed: Флаг, показывающий, закрыт ли результат.
        
    #     # Методы объектов <class 'sqlalchemy.engine.row.Row'>:
    #     # items(): Возвращает пары ключ-значение для каждого столбца в строке.
    #     # keys(): Возвращает имена столбцов в строке.
    #     # values(): Возвращает значения столбцов в строке.
    #     # get(key, default=None): Получение значения по имени столбца. Если столбец не существует, возвращается значение default.
    #     # as_dict(): Возвращает строки в виде словаря, где ключи - это имена столбцов, а значения - значения столбцов.
    #     # index(key): Возвращает позицию столбца с указанным именем в строке.
        
    #     # Атрибуты:
    #     # keys(): Возвращает имена столбцов в строке.
    #     # _fields: Атрибут, хранящий имена столбцов в строке.
    #     # _data: Словарь, содержащий данные строки, где ключи - это имена столбцов, а значения - значения столбцов.
    #     '''
        
    #     # выбираем из таблицы данные 
    #     try:
    #         # rows type: <class 'sqlalchemy.engine.cursor.CursorResult'>
    #         cursor_result_no = await self.read_data_one(name_table=name_table,
    #                                             column_name='in_work_download', 
    #                                             params_status='no')
    #     except SQLAlchemyError as eR:
    #         print(f'\nERROR [BaseDB: choice_link4download] ERROR: {str(eR)}')
    #         self.Logger.log_info(f'\nERROR [BaseDB: choice_link4download] ERROR: {str(eR)}')
    #         return None
    #     if not cursor_result_no: 
    #         print(f'\n[BaseDB choice_link4download] cursor_result_no is NONE')
    #         return None
    #     # вытаскиваем результаты запроса к БД
    #     # список объектов <class 'sqlalchemy.engine.row.Row'>
    #     rows_no=[]
    #     rows_no=cursor_result_no.fetchall()
    #     print(f'\n[BaseDB: choice_link4download] rows_no: {rows_no}, \ntype: {type(rows_no)} ')
    #     for row in rows_no:
    #         # вытаскиваем значения video_id и добавляем в множество unique_video_ids_no - set()
    #         print(f'\n[BaseDB: choice_link4download] row: \n{row}, \ntype row: {type(row)}, \n{dir(rows_no)}')
    #         video_id_no=row.video_id
    #         print(f'\n[BaseDB: choice_link4download] video_id_no: \n{video_id_no}, \ntype video_id_no: {type(video_id_no)}')
    #         self.unique_video_ids_no.add(video_id_no)
            
            
    #         try:
    #             cursor_result_yes = await self.read_data_two(name_table=name_table,
    #                                                 one_column_name='in_work_download', 
    #                                                 one_params_status='yes',
    #                                                 two_column_name='video_id', 
    #                                                 two_params_status=video_id_no)
    #         except SQLAlchemyError as eR:
    #             print(f'\nERROR [BaseDB: choice_link4download read_data_two] ERROR: {str(eR)}')
    #             self.Logger.log_info(f'\nERROR [BaseDB: choice_link4download read_data_two] ERROR: {str(eR)}')
    #             return None
    #     if not cursor_result_yes: 
    #         print(f'\n[BaseDB choice_link4download] cursor_result_yes is NONE')
    #         return None
    #     # вытаскиваем результаты запроса к БД
    #     # список объектов <class 'sqlalchemy.engine.row.Row'>
    #     rows_yes=[]
    #     rows_yes=cursor_result_yes.fetchall()
    #     print(f'\n[BaseDB: choice_link4download] rows_yes: {rows_yes}, \ntype: {type(rows_yes)} ')
    #     for row in rows_yes:
    #         # вытаскиваем значения video_id и добавляем в множество unique_video_ids_no - set()
    #         print(f'\n[BaseDB: choice_link4download] table: {name_table} row: \n{row}, \ntype row: {type(row)}')
    #         video_id_yes=row.video_id
    #         print(f'\n[BaseDB: choice_link4download] table: {name_table} video_id_yes: \n{video_id_yes}, \ntype video_id_no: {type(video_id_yes)}')
    #         self.unique_video_ids_yes.add(video_id_yes)

    #     # оставляем только множество для закачки
    #     video_ids_download = self.unique_video_ids_no - self.unique_video_ids_yes
    #     print(f'\n[BaseDB: choice_link4download] video_ids_download: \n{video_ids_download}, \ntype video_ids_download: {type(video_ids_download)}, \n{dir(video_ids_download)}')
    #     # ОШИБКА наверное тут - возврат вместо корутины множества
    #     return video_ids_download
    #
    # Выводим все данные таблицы
    async def print_data(self, name_table: str):
        table=self.table.get(name_table)
        try:
            async with self.Session() as session:
                async_results = await session.execute(select(table))
        except SQLAlchemyError as eR:
            print(f'\nERROR [BaseDB: print_data] ERROR: {str(eR)}')
            self.Logger.log_info(f'[BaseDB: print_data] ERROR: {str(eR)}')
            session.rollback()
        print(f'\n*table: {name_table}')
        for row in async_results.fetchall():
            print(f'\n{row}')

