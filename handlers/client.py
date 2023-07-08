from aiogram import types
from aiogram.dispatcher.filters import Text, Command

from bot_env.mod_log import Logger
from bot_env.create_obj4bot import bot, dp
from keyboards.client_kb import KeyBoardClient

class Client:
    """
    Создаем для telegram-bot хэндлеры клиента:

    Аргументы:
    - logger: Logger
    """
    countInstance=0

    def __init__(self, logger: Logger):
        Client.countInstance+=1
        self.countInstance=Client.countInstance
        self.logger = logger
        #
        #
    # обрабатывает команду пользователя - /start
    async def command_start(self, message: types.Message):
        kb = KeyBoardClient(row_width=1)
        kb.menu_1_level() # добавляем меню 1 уровня
        self.logger.log_info(f'[command_start] kb.countInstance: {kb.countInstance}')
        # отвечаем на команду /start и отправляем клавиатуру menu_1_level
        await bot.send_message(
            message.from_user.id, 
            "Выберите раздел, из которого будет видео для получения фрагмента",
            reply_markup=kb.keyboard)
    #
    # обработчик любого сообщения, кроме  - /start
    async def any2start(self, message: types.Message):
        kb = KeyBoardClient(row_width=1)
        kb.start_button() # загружаем кнопку Старт в клавиатуру
        self.logger.log_info(f'[any2start] kb.countInstance: {kb.countInstance}')
        # отвечаем на любое сообщение и отправляем кнопку СТАРТ 
        await bot.send_message(
            message.from_user.id, 
            "Для старта набери команду /start или нажми на кнопку \U0001F447 СТАРТ",
            reply_markup=kb.keyboard)
        await message.delete()
    #
    # финальное сообщение -> на кнопку старт
    async def end2start(self, message: types.Message):
        kb = KeyBoardClient(row_width=1)
        kb.start_button() # загружаем кнопку Старт в клавиатуру
        self.logger.log_info(f'[end2start] kb.countInstance: {kb.countInstance}')
        # пишем сообщение и отправляем кнопку СТАРТ 
        await bot.send_message(
            message.from_user.id,
            "Работа выполнена, если надо еще что-то нарезать, то нажмите кнопку Старт",
            reply_markup=kb.keyboard)
        #
    # обрабатываем нажатие кнопки СТАРТ
    async def call_start(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_1_level() # добавляем меню 1 уровня
        self.logger.log_info(f'[call_start] kb.countInstance: {kb.countInstance}')
        # пишем сообщение и отправляем меню 1 уровня
        await callback.message.answer(text=f'Выберите раздел, из которого будет видео для получения фрагмента',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #1 первого уровня 
    async def call_1_level_1(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_2_level() # загружаем меню второго уровня
        self.logger.log_info(f'[call_1_level_1] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.logger.log_info(f'[call_1_level_1] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #2 первого уровня 
    async def call_1_level_2(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_2_level() # загружаем меню второго уровня
        self.logger.log_info(f'[call_1_level_2] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.logger.log_info(f'[call_1_level_2] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #3 первого уровня 
    async def call_1_level_3(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_2_level() # загружаем меню второго уровня
        self.logger.log_info(f'[call_1_level_3] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.logger.log_info(f'[call_1_level_3] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #4 второго уровня 
    async def call_2_level_4(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_3_level() # загружаем меню третьего уровня
        self.logger.log_info(f'[call_2_level_4] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.logger.log_info(f'[call_2_level_4] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #5 второго уровня 
    async def call_2_level_5(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_3_level() # загружаем меню третьего уровня
        self.logger.log_info(f'[call_2_level_5] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.logger.log_info(f'[call_2_level_5] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #6 второго уровня 
    async def call_2_level_6(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_3_level() # загружаем меню третьего уровня
        self.logger.log_info(f'[call_2_level_6] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.logger.log_info(f'[call_2_level_6] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #7 второго уровня 
    async def call_2_level_7(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(row_width=1)
        kb.menu_3_level() # загружаем меню второго уровня
        self.logger.log_info(f'[call_2_level_7] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.logger.log_info(f'[call_2_level_7] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # регистрация хэндлеров
    def register_handlers_client(self):
        # обрабатывает команду пользователя - /start
        dp.register_message_handler(self.command_start, commands=['start'])
        #  dp.register_callback_query_handler(self.call_start, lambda callback_query: callback_query.data == '/start')
        dp.register_callback_query_handler(self.call_start, Text(contains='/start', ignore_case=True))
        dp.register_callback_query_handler(self.call_1_level_1, Text(contains='1', ignore_case=True))
        dp.register_callback_query_handler(self.call_1_level_2, Text(contains='2', ignore_case=True))
        dp.register_callback_query_handler(self.call_1_level_3, Text(contains='3', ignore_case=True))
        dp.register_callback_query_handler(self.call_2_level_4, Text(contains='4', ignore_case=True))
        dp.register_callback_query_handler(self.call_2_level_5, Text(contains='5', ignore_case=True))
        dp.register_callback_query_handler(self.call_2_level_6, Text(contains='6', ignore_case=True))
        dp.register_callback_query_handler(self.call_2_level_7, Text(contains='7', ignore_case=True))
        # должен быть последним хэндлером, 
        # ловит любые сообщения и вызывает кнопку старт
        dp.register_message_handler(self.any2start)


