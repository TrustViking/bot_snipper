#!/usr/bin/env python3
#
import subprocess
import os
import sys
import psutil
import pynvml
import asyncio, os, sys, logging
import subprocess
from concurrent.futures import ProcessPoolExecutor
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB

class Start:
    """Module for START"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='main_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=3,
                 ):
        Start.countInstance += 1
        # надо изучить вариант ниже
        # self.countInstance = Y2b.countInstance = getattr(Y2b, 'countInstance', 0) + 1
        self.countInstance = Start.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        # self.db = BaseDB(logger=self.Logger)
        # self.diction4db={}
        self.max_download = max_download
        self._print()

        #     
    # выводим № объекта
    def _print(self):
        print(f'[Start] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'\n[Start] countInstance: [{self.countInstance}]\n')
    # 
    # Функция для логирования информации о памяти
    def log_memory(self):
        self.Logger.log_info(f'****************************************************************')
        self.Logger.log_info(f'*Data RAM {os.path.basename(sys.argv[0])}: [{psutil.virtual_memory()[2]}%]')
        # Инициализируем NVML для сбора информации о GPU
        pynvml.nvmlInit()
        deviceCount = pynvml.nvmlDeviceGetCount()
        self.Logger.log_info(f'\ndeviceCount [{deviceCount}]')
        for i in range(deviceCount):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            meminfo = pynvml.nvmlDeviceGetMemoryInfo(handle)
            self.Logger.log_info(f"#GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
            self.Logger.log_info(f'****************************************************************\n')
        # Освобождаем ресурсы NVML
        pynvml.nvmlShutdown()

    # Асинхронная функция для запуска скрипта
    async def run_script(self, script):
        self.Logger.log_info(f'[run_script] command: {script}')
        # Используем asyncio для асинхронного запуска скрипта
        process = await asyncio.create_subprocess_shell(script)
        await process.wait()


# MAIN **************************
# Главная асинхронная функция
async def main():
    #
    # Список скриптов для запуска
    scripts = [
        #os.path.join(sys.path[0], 'make_db.py'),
        os.path.join(sys.path[0], 'bot_telega.py'),
        os.path.join(sys.path[0], 'bot_dnld.py'),
    ]
    #
    print(f'Старт приложения...')
    print(f'\n==============================================================================\n')
    print(f'File: [{os.path.basename(sys.argv[0])}]')
    print(f'Path: [{sys.path[0]}]') 
    #
    # Создаем экземпляр класса Start
    start = Start(max_download=5)
    print(f'Data memory: [{psutil.virtual_memory()}]')
    start.log_memory()
    #
    # создаем БД и таблицы
    # Запускаем скрипт make_db с помощью subprocess
    try:
        subprocess.run(["python3", os.path.join(sys.path[0], 'make_db.py')], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running make_db: {e}")
        start.Logger.log_info(f"Error running make_db: {e}")
    #
    # Запускаем скрипты асинхронно
    tasks = [start.run_script(script) for script in scripts]
    await asyncio.gather(*tasks)

# Запускаем главную асинхронную функцию
if __name__ == "__main__":
    asyncio.run(main())





    # # состояние памяти
    # def memory(self):
    #     print (f'****************************************************************')
    #     print (f'*Data RAM {os.path.basename(sys.argv[0])}: [{psutil.virtual_memory()[2]}%]')
    #     # получение количества доступных устройств
    #     pynvml.nvmlInit()
    #     deviceCount = pynvml.nvmlDeviceGetCount()
    #     print (f'\ndeviceCount [{deviceCount}]')
    #     # для каждого устройства вывод информации о загрузке памяти
    #     for i in range(deviceCount):
    #         handle = pynvml.nvmlDeviceGetHandleByIndex(i)
    #         #print (f'\nhandle [{handle}]')
    #         meminfo = pynvml.nvmlDeviceGetMemoryInfo(handle)
    #         #print (f'meminfo [{meminfo}]')
    #         print(f"#GPU [{i}]: used memory [{int(meminfo.used / meminfo.total * 100)}%]")
    #         print (f'****************************************************************\n')

    # # запускаем работу скриптов
    # def run_script(self, script):
    #     #
    #     self.command = f'{script}'
    #     print(f'[run_script] command: {script}')
    #     # не строка, а список команд, когда shell=False, т.е. без оболочки
    #     process = subprocess.Popen(self.command, shell=True) 


# # MAIN **************************
# async def main():
#     # список скриптов для выполнения
#     path_scripts1=os.path.join(sys.path[0], 'bot_make_db_v1.py')
#     path_scripts2=os.path.join(sys.path[0], 'bot_telega_v1.py')
#     path_scripts3=os.path.join(sys.path[0], 'bot_y2b_v1.py')
#     scripts = [
#                path_scripts1, 
#                path_scripts2, 
#                path_scripts3,
#                ] 
#     #
#     print(f'Старт приложения...')
#     print (f'\n==============================================================================\n')
#     print (f'File: [{os.path.basename(sys.argv[0])}]')
#     print (f'Path: [{sys.path[0]}]') 
#     # 
#     start=Start(max_download=5) # создаем объект класса 
#     # проверка памяти
#     print (f'Data memory: [{psutil.virtual_memory()}]')
#     start.memory()
#     #
#     # Запускаем процессы скачивания асинхронно
#     loop = asyncio.get_event_loop()
#     tasks = []
#     with ProcessPoolExecutor(max_workers=start.max_download) as executor:
#         for script in scripts:
#             task = loop.run_in_executor(executor, start.run_script, script)
#             tasks.append(task)

#         # Ожидаем завершения всех процессов
#         await asyncio.gather(*tasks)

# if __name__ == "__main__":
#     asyncio.run(main())

