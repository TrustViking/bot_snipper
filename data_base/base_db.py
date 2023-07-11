import asyncio
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from table_db import table
import os, sys
from bot_env.mod_log import Logger


class BaseDB:
    """
    Создаем для telegram-bot базу данных:

    Аргументы:
    - logger: Logger
    - db_file='db_file.db'
    """
    countInstance=0
    #
    def __init__(self, 
                 logger: Logger, 
                 db_file='db_file.db',
                 ):
        BaseDB.countInstance+=1
        self.countInstance=BaseDB.countInstance
        self.logger = logger
        # создаем путь для БД
        self.db_file = os.path.join(sys.path[0], 'db_storage', db_file)
        self._create_db_directory() # создаем директорию для БД, если такой папки нет
        # формируем путь к лог файлу 'sqlalchemy'
        self.log_path = os.path.join(self.logger.log_path, 'sqlalchemy')
        # создаем url БД
        self.db_url = f'sqlite+pysqlite:///{self.db_file}'
        # создаем объект engine для связи с БД
        #self.engine = create_engine(f"sqlite+pysqlite:///{self.db_file}", logging_name=self.log_path)
        # создаем объект connect для связи с БД
        #self.connect = self.engine.connect()
        #
        ## Создание асинхронного движка SQLAlchemy из конфигурации
        self.async_engine = create_async_engine(self.db_url, echo=True)
        #
        # Создание асинхронной сессии SQLAlchemy
        self.async_session = sessionmaker(self.async_engine, 
                                          expire_on_commit=False, 
                                          class_=AsyncSession,
                                          )
        # Импорт параметров таблицы из другого модуля
        self.table=table
        #self.table_name = self.table.table_name
        #self.columns = self.table.columns
        # Определение структуры таблицы
        self.metadata = MetaData()
        #self._table = Table(self.table_name, self.metadata, *self.columns)
    #
    # создаем директорию для БД, если такой папки нет
    def _create_db_directory(self, path=None):
        """
        Создаем директорию для хранения файлов БД, если она не существует.
        """
        if not path:
            db_dir = os.path.dirname(self.db_file)
        else: 
            db_dir = os.path.dirname(path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)


    # Создание таблицы в базе данных
    async def create_table(self):
        async with self.async_engine.begin() as conn:
            await conn.run_sync(self.metadata.create_all)

    # Вставка данных в таблицу
    async def insert_data(self):
        async with self.async_session() as session:
            async with session.begin():
                await session.execute(table.insert().values(name="John", age=25))
                await session.execute(table.insert().values(name="Jane", age=30))

    # Выборка данных из таблицы
    async def select_data(self):
        async with self.async_session() as session:
            result = await session.execute(table.select())
            rows = result.fetchall()
            for row in rows:
                print(row)
