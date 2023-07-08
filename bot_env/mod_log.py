import logging
import os
import sys


class Logger:
    def __init__(self, 
                 log_file: str, 
                 log_level=logging.DEBUG,
                 ):
        """
        Конструктор класса Logger.

        Аргументы:
        - log_file: Имя файла логирования. По умолчанию None.
        - log_level: Уровень логирования. По умолчанию logging.INFO.

        Возможные уровни логирования:
        - logging.DEBUG: Детальная отладочная информация.
        - logging.INFO: Информационные сообщения.
        - logging.WARNING: Предупреждения.
        - logging.ERROR: Ошибки, которые не приводят к прекращению работы программы.
        - logging.CRITICAL: Критические ошибки, которые приводят к прекращению работы программы.
        """
        self.log_file = os.path.join(sys.path[0], 'log_file', log_file or 'log.txt')
        self.log_level = log_level
        self._create_log_directory()
        self.logger = self._setup_logger()

    def _create_log_directory(self):
        """
        Создает директорию для хранения файлов журнала, если она не существует.
        """
        log_dir = os.path.dirname(self.log_file)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

    def _setup_logger(self):
        """
        Настраивает логгер.

        Возвращает:
        - logger: Объект логгера.
        """
        logger = logging.getLogger(__name__)
        logger.setLevel(self.log_level)
        #
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        #
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        #
        logger.addHandler(file_handler)
        #
        return logger

    def log_info(self, message):
        """
        Записывает информационное сообщение в лог.

        Аргументы:
        - message: Сообщение для записи в лог.
        """
        self.logger.info(message)

    def set_log_file(self, log_file):
        """
        Устанавливает имя файла логирования.

        Аргументы:
        - log_file: Имя файла логирования.
        """
        self.log_file = os.path.join(sys.path[0], 'log', log_file)
        self._create_log_directory()
        self.logger.handlers[0].baseFilename = self.log_file

    def set_log_level(self, log_level):
        """
        Устанавливает уровень логирования.

        Аргументы:
        - log_level: Уровень логирования.
        """
        self.log_level = log_level
        #self.logger.setLevel(self.log_level)
        self.logger=self._setup_logger()


'''

from logger_module import Logger
import logging


class MainClass:
    def __init__(self):
        self.logger = Logger()

    def perform_action(self):
        """
        Выполняет действие и записывает информацию в лог.
        """
        # Выполнение действия
        self.logger.log_info('Действие выполнено успешно.')

        # Другие действия...

        # Запись дополнительной информации в лог
        self.logger.log_info('Еще информация в логе.')


def main():
    # Создание экземпляра основного класса
    main_class = MainClass()

    # Установка наименования файла лога
    log_file = 'custom_log.txt'
    main_class.logger.set_log_file(log_file)

    # Установка уровня логирования
    log_level = logging.DEBUG
    main_class.logger.set_log_level(log_level)

    # Выполнение действий
    main_class.perform_action()


if __name__ == '__main__':
    main()

***    
В модуле логирования (logger_module.py) добавлены 
методы set_log_file и set_log_level, которые позволяют устанавливать 
имя файла лога и уровень логирования соответственно. 
Также добавлена проверка и создание директории для хранения файлов журнала 
при инициализации объекта Logger. 
Используется путь os.path.join(sys.path[0], 'log', log_file) 
для создания пути "log/log.txt" для сохранения файла журнала по умолчанию.

В основном модуле (main_module.py) происходит использование класса 
Logger для записи информации в лог. 
Методы set_log_file и set_log_level вызываются для установки 
пользовательского имени файла лога и уровня логирования. 
Затем выполняются действия с использованием метода perform_action, 
который записывает информацию в лог.

'''