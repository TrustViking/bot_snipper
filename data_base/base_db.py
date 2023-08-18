
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
        # формируем путь к лог файлу 'sqlalchemy'
        self.log_path = os.path.join(self.Logger.log_path, 'sqlalchemy')
        self.rows=None
        # создаем url БД
        # self.db_url = f'sqlite+pysqlite:///{self.db_file}'
        self.db_url = f'sqlite+aiosqlite:///{self.db_file}'
        # создаем объект engine для связи с БД
        self.engine = create_async_engine(self.db_url, logging_name=self.log_path)
        # Создаем асинхронный объект Session 
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
    # добавляем строку (словарь) в таблицу
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
        #print(f'\n[BaseDB read_data_two] table: {name_table}, type: {type(table)}')
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
                return async_results
        except SQLAlchemyError as eR:
            print(f'[BaseDB: read_data_two] ERROR: {str(eR)}')
            self.Logger.log_info(f'[BaseDB: read_data_two] ERROR: {str(eR)}')
            # await session.rollback()
            return None
    #
    # Находим в таблице 'dnld_link', 'task' строки с vid и записываем словарь значений
    async def update_table_vid(self, name_table: str, vid: str, diction: dict):
        table=self.table.get(name_table)
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
            print(f'\nERROR [BaseDB: update_table] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [BaseDB: update_table] ERROR: {str(eR)}')
            await session.rollback()
            return None


    # Находим в таблице 'task' и 'frag' строки с date_message и user_id
    # записываем словарь значений
    async def update_table_date(self, name_table: str, 
                                date_message: str,
                                user_id: str,
                                diction: dict):
        table=self.table.get(name_table)
        # Находим все строки с vid и записываем словарь значений
        try:
            async with self.Session() as session:
                stmt = (update(table).
                        where(table.c.date_message == date_message, 
                              table.c.user_id == user_id).
                        values(diction))               
                await session.execute(stmt)
                await session.commit()
                return date_message, user_id, diction
        except SQLAlchemyError as eR:
            print(f'\nERROR [BaseDB: update_table] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [BaseDB: update_table] ERROR: {str(eR)}')
            await session.rollback()
            return None

    # Находим в таблице 'task' и 'frag' строки с path_frag
    # записываем словарь значений
    async def update_table_link(self, name_table: str, 
                                      path_frag: str,
                                      diction: dict):
        table=self.table.get(name_table)
        # Находим все строки с vid и записываем словарь значений
        try:
            async with self.Session() as session:
                stmt = (update(table).
                        where(table.c.path_frag == path_frag).
                        values(diction))               
                await session.execute(stmt)
                await session.commit()
                return path_frag, diction
        except SQLAlchemyError as eR:
            print(f'\nERROR [BaseDB: update_table_link] ERROR: {str(eR)}')
            self.Logger.log_info(f'\nERROR [BaseDB: update_table_link] ERROR: {str(eR)}')
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

