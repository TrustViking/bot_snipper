
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

