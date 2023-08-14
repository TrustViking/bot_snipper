
import asyncio, aiosqlite
from sqlalchemy import update
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
                return data4db
        except SQLAlchemyError as eR:
            print(f'[BaseDB: insert_data] ERROR: {str(eR)}') 
            self.Logger.log_info(f'[BaseDB: insert_data] ERROR: {str(eR)}')   
            await session.rollback()
            return None
    #
    # добавляем данные-колонку в таблицу
    # data_bot = {'url_video_y2b': 'https://example.com', 'id_user': 'user1', 'id_chat': 'chat1'}
    async def insert_data_list(self, name_table: str, data4db: list):
        #
        #table=self.table.get(name_table)
        for data in data4db:
            try:
                await self.insert_data(name_table=name_table, 
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
        table=self.table.get(name_table)
        #print(f'\n[BaseDB read_data_one] table: {name_table}, type: {type(table)}')
        try:
            async with self.Session() as session:
                # Формируем запрос с фильтром
                text_filter=f'{column_name} = :status'
                result = await session.execute(select(table).
                                             where(text(text_filter)).
                                             params(status=params_status).
                                             order_by(table.c.id))
                return result
        except SQLAlchemyError as eR:
            print(f'[BaseDB: read_data_one] ERROR: {str(eR)}')
            self.Logger.log_info(f'[BaseDB: read_data_one] ERROR: {str(eR)}')
            # await session.rollback()
            return None
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
                # Сортируем по порядковому номеру (предполагаем, что столбец id уникален)
                async_results = await session.execute(
                    select(table)
                    .where(text(text_filter))
                    .params(status_one=one_params_status, 
                            status_two=two_params_status)
                    .order_by(table.c.id))   
            print(f'[BaseDB read_data_two] table: {name_table} async_results: {async_results}, type: {type({async_results})} ')
            # async_results <sqlalchemy.engine.cursor.CursorResult object at 0x7f5c3abf1ea0>
            # type: <class 'set'>
            return async_results
        except SQLAlchemyError as eR:
            print(f'[BaseDB: read_data_two] ERROR: {str(eR)}')
            self.Logger.log_info(f'[BaseDB: read_data_two] ERROR: {str(eR)}')
            # await session.rollback()
            return None
    #
    # Находим в таблице 'download_link' строки с vid и записываем словарь значений
    async def update_dnld_link(self, vid: str, diction: dict):
        table=self.table.get('download_link')
        # Находим все строки с vid и записываем словарь значений
        try:
            async with self.Session() as session:
                stmt = (update(table).
                        where(table.c.video_id == vid).
                        values(diction))
                await session.execute(stmt)
                await session.commit()
                return vid, diction
        except SQLAlchemyError as eR:
            print(f'\nERROR [BaseDB: update_dnld_link] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [BaseDB: update_dnld_link] ERROR: {str(eR)}')
            await session.rollback()
            return None

    # Находим в таблице 'task' строки с vid и записываем словарь значений
    async def update_task(self, vid: str, diction: dict):
        table=self.table.get('task')
        # Находим все строки с vid и записываем в 'worked_link': 'yes'
        try:
            async with self.Session() as session:
                stmt = (update(table).
                        where(table.c.video_id == vid).
                        values(diction))
                await session.execute(stmt)
                await session.commit()
                return vid, diction
        except SQLAlchemyError as eR:
            print(f'\nERROR [BaseDB: update_task] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [BaseDB: update_task] ERROR: {str(eR)}')
            await session.rollback()
            return None



    # получаем все данные таблицы
    async def data_table(self, name_table: str):
        table=self.table.get(name_table)
        try:
            async with self.Session() as session:
                async_results = await session.execute(select(table))
        except SQLAlchemyError as eR:
            print(f'\nERROR [BaseDB: data_table] ERROR: {str(eR)}')
            self.Logger.log_info(f'[BaseDB: data_table] ERROR: {str(eR)}')
            session.rollback()
        return async_results
    #
    # Выводим все данные таблицы
    async def print_data(self, name_table: str):
        async_results = await self.data_table(name_table)
        rows=async_results.fetchall()
        print(f'\n*[print_data] в таблице [{name_table}] записано [{len(rows)}] строк:')
        for row in rows: print(f'\n{row}')

        # table=self.table.get(name_table)
        # try:
        #     async with self.Session() as session:
        #         async_results = await session.execute(select(table))
        # except SQLAlchemyError as eR:
        #     print(f'\nERROR [BaseDB: print_data] ERROR: {str(eR)}')
        #     self.Logger.log_info(f'[BaseDB: print_data] ERROR: {str(eR)}')
        #     session.rollback()
    #
    # записываем в таблице download_link путь закачки файла 
    # async def update_path_dnld(self,
    #                         vid: str,
    #                         new_value: str,
    #                         ):
    #     table=self.table.get('download_link')
    #     diction={'path_download': new_value}
    #     print(f'\n[BaseDB: update_path_dnld] diction: {diction}')
    #     # Находим все строки, где значение column_to_search равно value_to_search
    #     try:
    #         async with self.Session() as session:
    #             stmt = (update(table).
    #                     where(table.c.video_id == vid).
    #                     values(diction))
    #             await session.execute(stmt)
    #             await session.commit()
    #     except SQLAlchemyError as eR:
    #         print(f'\nERROR [BaseDB: update_path_dnld] ERROR: {str(eR)}')
    #         self.Logger.log_info(f'\nERROR [BaseDB: update_path_dnld] ERROR: {str(eR)}')
    #         # await session.rollback()
    #         return None
    #     return new_value, vid

    # записываем NO о закачке файла в таблицу download_link
    # async def update_no_worked_link(self, vid: str):
    #     table=self.table.get('download_link')
    #     # Находим все строки, где значение column_to_search равно value_to_search
    #     try:
    #         async with self.Session() as session:
    #             stmt = (update(table).
    #                     where(table.c.video_id == vid).
    #                     values(_))
    #             await session.execute(stmt)
    #             await session.commit()
    #     except SQLAlchemyError as eR:
    #         print(f'\nERROR [BaseDB: update_rows] ERROR: {str(eR)}')
    #         self.Logger.log_info(f'\nERROR [BaseDB: update_rows] ERROR: {str(eR)}')
    #         # await session.rollback()
    #         return None
    #     return vid
