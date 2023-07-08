#!/usr/bin/env python3
#
import logging
from aiogram.utils import executor
#
from bot_env.create_obj4bot import dp
from bot_env.mod_log import Logger
from handlers.client import  Client
#
#
class Telega:
    """Modul for TELEGRAM"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='main_log.txt', 
                 log_level=logging.DEBUG,
                 ):
        Telega.countInstance += 1
        self.countInstance = Telega.countInstance
        # Logger
        self.logger = Logger(log_file=log_file, log_level=log_level)
        # Client
        self.client = Client(logger=self.logger)
        self._client_work()
    #
    def _client_work(self):
        try:
            #
            self.client.register_handlers_client()            
            #
        except Exception as eR:
            self.logger.log_info(f'[main] error: {eR}')   
    #
# MAIN **************************
async def main(_):
    print(f'Бот вышел в онлайн')
    telega=Telega() # создаем объект и в нем регистрируем хэндлеры Клиента, 
    telega.logger.log_info(f'\n[main] Создали объект Telega()')
    print(f'[main] Создали объект Telega()')
#
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=main)




