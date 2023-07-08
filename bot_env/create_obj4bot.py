import os
from aiogram import Bot
from aiogram.dispatcher import Dispatcher


"""
Создаем для telegram-bot объекты:
Bot, Dispatcher  
Переменные:
- token: токен telegram-bot берем в $PATH (.bashrc)
- bot: объект Bot
- dp: объект Dispatcher
"""
#
token=os.getenv('TELEGRAM_TOKEN')
bot=Bot(token)
dp=Dispatcher(bot)





