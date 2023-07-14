from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from data_base.table_db import metadata, table
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

    def __init__(self,
                 logger: Logger,
                 db_file='db_file.db',
                 ):
        BaseDB.countInstance+=1
        self.countInstance=BaseDB.countInstance
        self.Logger = logger
        # создаем путь для БД
        self.db_file = os.path.join(sys.path[0], 'db_storage', db_file)
        self._create_db_directory() # создаем директорию для БД, если такой папки нет
        # формируем путь к лог файлу 'sqlalchemy'
        self.log_path = os.path.join(self.Logger.log_path, 'sqlalchemy')
        # создаем url БД
        self.db_url = f'sqlite+pysqlite:///{self.db_file}'
        # создаем объект engine для связи с БД
        self.engine = create_engine(self.db_url, logging_name=self.log_path)
        # создаем объект Session 
        self.Session=sessionmaker(bind=self.engine)
        # Определение структуры таблицы
        self.metadata = metadata
        #self.metadata.bind = self.engine
        # Импорт параметров таблицы из другого модуля
        self.table = table
        # Создание таблицы в базе данных
        self._create_table()

    # создаем директорию для БД, если такой папки нет
    def _create_db_directory(self, path=None):
        if not path:
            db_dir = os.path.dirname(self.db_file)
        else: 
            db_dir = os.path.dirname(path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
    
    # Создание таблицы в базе данных
    def _create_table(self):
        self.metadata.create_all(bind=self.engine)
        #print(f'[_create_table] engine.table_names: {self.engine.table_names()}')
        table = self.metadata.tables['snipper']
        print(table)

    # добавляем данные в таблицу
    # data_bot = {'url_video_y2b': 'https://example.com', 'id_user': 'user1', 'id_chat': 'chat1'}
    def insert_data(self, data_bot: dict):
        #
        try:
            with self.Session() as session:
                session.execute(self.table.insert().values(**data_bot))
                session.commit()
                print(f'[BaseDB: insert_data] Data insert successfully! \nInsert data: {str(data_bot)}')
                self.Logger.log_info(f'[BaseDB: add_data] Data added successfully!\n{str(data_bot)}')   
        except SQLAlchemyError as eR:
            print("Error occurred:", str(eR))
            self.Logger.log_info(f'[BaseDB; add_data] error: {str(eR)}')   
            session.rollback()

    # Выборка данных из таблицы
    # def select_data(self):
    #     with sessionmaker(bind=self.engine)() as session:
    #         result = session.execute(self._table.select())
    #         rows = result.fetchall()
    #         for row in rows:
    #             print(row)

