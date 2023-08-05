from datetime import datetime
from aiogram import types
from aiogram.dispatcher.filters import Text, Command
from aiogram.dispatcher.filters import Regexp
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
#
import re
#
from bot_env.mod_log import Logger
from bot_env.create_obj4bot import bot, dp
from bot_env.timestamp_parsing import ParseTime
from keyboards.client_kb import KeyBoardClient
from y2b4bot.you2b import You2b
from data_base.base_db import BaseDB

class Client:
    """
    Создаем для telegram-bot хэндлеры клиента:

    Аргументы:
    - logger: Logger
    """
    countInstance=0
    #
    def __init__(self, logger: Logger):
        Client.countInstance+=1
        self.countInstance=Client.countInstance
        self.Logger = logger
        self._new_client()
        #self._button_start() надо найти chat_id
        # словарь для создания и записи строки в БД про ссылку youtube
        self.youtube_info=None
        #self.task_time_dt=None
        self.youtube_link=''
        self.video_id=''
        self.video_title=''
        self.video_duration=''
        self.video_duration_sec = ''
        # объект datetime
        self.datatime_duration=None 
        #
        self.duration_minuts=''
        self.username=''
        self.date_message=''
        self.user_id=''
        self.chat_id=''
        self.video_url = None
        self.channel_title = None # 
        self.default_audio_language = None
        #
        self.diction4db={} 
        # Устанавливаем состояние ожидания подтверждения
        self.Confirmation = State()
        # временные метки
        #self.Pars_time=ParseTime(logger=self.Logger)
        self.ParsTime= None
        self.timestamp=''
        self.timestamp_start=''
        self.timestamp_start_dt=None
        self.timestamp_end=''
        self.timestamp_end_dt=None
        self.segment_duration=''
        #
        #
    # New Client
    def _new_client(self):
        print(f'[_new_client] Client# {self.countInstance}')
        self.Logger.log_info(f'[_new_client] Client# {self.countInstance}')
    #
    # кнопка-команда - /start - не работает
    async def _button_start(self): 
        #
        button_text = "ЗАПУСК"
        bot_username = "snipper_video_bot"
        start_param = "/start"

        button_url = f"https://t.me/{bot_username}?start={start_param}"
        keyboard = types.InlineKeyboardMarkup(row_width=1)
        button = types.InlineKeyboardButton(button_text, url=button_url)
        keyboard.add(button)        
        # Отправка сообщения с кнопкой "ЗАПУСК"
        await bot.send_message(chat_id=self.chat_id, text="Нажмите кнопку, чтобы запустить бота", 
                            reply_markup=keyboard)
    #
    # обрабатывает команду пользователя - /start
    async def command_start(self, message: types.Message): 
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.menu_1_level() # добавляем меню 1 уровня
        self.Logger.log_info(f'[command_start] kb.countInstance: {kb.countInstance}')
        # отвечаем на команду /start и отправляем клавиатуру menu_1_level
        await bot.send_message(
            message.from_user.id, 
            'Будем уникализировать фрагмент видео из youtube или свое видео?\nНажмите на соответствующую кнопку',
            reply_markup=kb.keyboard)
    #
    # обработчик любого сообщения, кроме  - /start
    async def any2start(self, message: types.Message):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.start_button() # загружаем кнопку Старт в клавиатуру
        self.Logger.log_info(f'[any2start] kb.countInstance: {kb.countInstance}')
        # отвечаем на любое сообщение и отправляем кнопку СТАРТ 
        await bot.send_message(
            message.from_user.id, 
            "Наберите команду '/start' или нажмите \U0001F447 на кнопку  \U0001F680 ПУСК",
            reply_markup=kb.keyboard)
        await message.delete()
    #
    # финальное сообщение -> на кнопку старт
    async def end2start(self, message: types.Message):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.start_button() # загружаем кнопку Старт в клавиатуру
        self.Logger.log_info(f'[end2start] kb.countInstance: {kb.countInstance}')
        # пишем сообщение и отправляем кнопку СТАРТ 
        await bot.send_message(
            message.from_user.id,
            'Работа выполнена, если надо еще что-то нарезать, то нажмите кнопку  \U0001F680 ПУСК',
            reply_markup=kb.keyboard)
        #
    # обрабатываем нажатие кнопки СТАРТ 
    # предлагаем выбрать 'youtube' (#1) или 'свое видео' (#2) 
    async def call_start(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.menu_1_level() # добавляем меню 1 уровня
        self.Logger.log_info(f'[call_start] kb.countInstance: {kb.countInstance}')
        # пишем сообщение и отправляем меню 1 уровня
        await callback.message.answer(text=f'Будем уникализировать фрагмент видео из youtube или свое видео?\nНажмите на соответствующую кнопку',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопки #1 'Видео с youtube'
    async def call_1_y2b(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        #kb.menu_2_level() # загружаем меню второго уровня
        # получаем из словаря наименования кнопок name_button[callback.data] 
        self.Logger.log_info(f'[call_1_y2b] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.Logger.log_info(f'[call_1_y2b] kb.countInstance: {kb.countInstance}')
        # название раздела
        await callback.message.answer(text=f'Пришлите в этот чат ссылку на видео с youtube, которое будем фрагментировать')
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #    
    # обрабатываем нажатие кнопки #2 'Свое видео' 
    async def call_my_video(self, callback: types.CallbackQuery):
        # kb = KeyBoardClient(logger=self.Logger, row_width=1)
        # kb.menu_2_level() # загружаем меню второго уровня
        self.Logger.log_info(f'[call_my_video] choice_user: {callback.data}')
        print(f'[call_my_video] choice_user: {callback.data}')
        #self.Logger.log_info(f'[call_my_video] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Тут чуть позже будет ветка обработки и компоновки видео пользоваттеля')
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатывает ссылку youtube
    async def youtube_link_handler(self, message: types.Message): 
        # Получаем ссылку на YouTube из сообщения
        self.youtube_link = message.text
        #
        self.youtube_info=None
        self.youtube_info=You2b(url=self.youtube_link, logger=self.Logger)
        # заполняем поля
        #self.task_time_dt=datetime.now() # Получаем текущее время задачи
        self.video_url=str(self.youtube_info.video_url)
        self.video_id=str(self.youtube_info.video_id)
        self.channel_title=str(self.youtube_info.channel_title)
        self.video_title=str(self.youtube_info.video_title)
        self.video_duration=str(self.youtube_info.duration_iso8601)
        self.default_audio_language=str(self.youtube_info.default_audio_language)
        self.username=str(message.from_user.username)
        self.date_message=str(message.date)
        self.user_id=str(message.from_user.id)
        #
        parsTime=ParseTime(logger=self.Logger)
        self.video_duration_sec, self.datatime_duration=parsTime.format_iso8601_sec_dt(time_iso8601=self.video_duration)
        self.duration_minuts=parsTime.format_sec2minuts(time_sec=self.video_duration_sec)
        #
        # формируем строку БД
        self.diction4db={
            #'task_time_dt' : self.task_time_dt,
            'url_video_y2b' : self.video_url,
            'video_id' : self.video_id,
            'channel_title' : self.channel_title,
            'video_title' : self.video_title,
            'video_duration' : self.video_duration,
            'video_duration_sec' : self.video_duration_sec,
            'datatime_duration' : self.datatime_duration,
            'duration_minuts' : self.duration_minuts,
            'default_audio_language' : self.default_audio_language,
            'username' : self.username,
            'date_message' : self.date_message,
            'user_id' : self.user_id,
                        }
        #
        # спрашиваем подтверждение ссылки youtube
        if self.youtube_info:
            # добавляем кнопки ОК & NO для youtube_link_handler
            kb = KeyBoardClient(logger=self.Logger, row_width=1)
            kb.button_OK_NO_youtube_link() 
            # отвечаем пользователю на введенную ссылку YouTube 
            # отправляем клавиатуру подтверждения 
            mes4user=f'Видео: [*{self.video_title}*] \n\nКанал: [*{self.channel_title}*] \n\nПродолжительность: {self.duration_minuts}'
            self.Logger.log_info(mes4user)
            await bot.send_message(message.from_user.id,
                                   mes4user, 
                                   reply_markup=kb.keyboard)
            # Устанавливаем состояние ожидания подтверждения
            await self.Confirmation.set()
        else: 
            print(f'[youtube_link_handler] There are empty fields in table \ndiction4db: {str(self.diction4db)}')
            self.Logger.log_info(f'[youtube_link_handler] There are empty fields in table\n diction4db: {str(self.diction4db)}')
            # добавляем кнопки ОК & NO для youtube_link_handler
            kb = KeyBoardClient(logger=self.Logger, row_width=1)
            kb.button_OK_NO_youtube_link_bad() 
            self.Logger.log_info(f'[youtube_link_handler] kb.countInstance: {kb.countInstance}')
            
            mes4user=f'{self.date_message} Вы ввели ошибочную ссылку на видео с названием: [*{self.video_title}*] на канале [*{self.channel_title}*] \nБудете вводить другую ссылку?'
            await bot.send_message(
                message.from_user.id,
                mes4user, 
                reply_markup=kb.keyboard)
            #
            # Устанавливаем состояние ожидания подтверждения
            await self.Confirmation.set()
        #
    # обрабатываем нажатие кнопки ОК #3 youtube_link 
    async def youtube_link_OK(self, callback: types.CallbackQuery, state: FSMContext):
        #
        mes4user=f'Введите временные метки фрагмента из видео: [*{self.video_title}*]. \nФормат временных меток начала и конца фрагмента: \n00:00:00-00:00:00, формат: [ЧЧ:ММ:СС-ЧЧ:ММ:СС]'
        #self.Logger.log_info(f'[youtube_link_OK] {mes4user}')
        await callback.message.answer(text=mes4user)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
        # Сбрасываем состояние
        await state.finish()
    #
    # обрабатываем временные метки 00:00:00-00:00:00, 00:00:00 -> ЧЧ:ММ:СС 
    async def youtube_timestamp(self, message: types.Message):
        # 
        self.timestamp = message.text
        parsTime=ParseTime(logger=self.Logger)
        self.timestamp_start, self.timestamp_start_dt, self.timestamp_end, self.timestamp_end_dt, self.segment_duration = parsTime.parse_time(timestamp=self.timestamp)
        
        print(f'[youtube_timestamp] timestamp_start:  {self.timestamp_start}')
        print(f'[youtube_timestamp] timestamp_start_dt:  {self.timestamp_start_dt}')
        print(f'[youtube_timestamp] timestamp_end:    {self.timestamp_end}')
        print(f'[youtube_timestamp] timestamp_end_dt:    {self.timestamp_end_dt}')
        print(f'[youtube_timestamp] segment_duration: {self.segment_duration}')
        print(f'[youtube_timestamp] video_duration_sec: {self.video_duration_sec}')
        #
        # проверяем метки на корректность
        if int(self.segment_duration) >= int(self.video_duration_sec):
            mes4user=(f'Длительность фрагмента больше самого видео!  \nПришлите любое сообщение или команду /start')
            print(mes4user)
            self.Logger.log_info(mes4user)
            await bot.send_message(message.from_user.id, mes4user) 
            return None
        # проверяем метки на корректность
        if self.timestamp_start_dt > self.datatime_duration or self.timestamp_end_dt>self.datatime_duration:
            mes4user=(f'Временные метки не корректны: \nНачало фрагмента: {self.timestamp_start} \nОкончание фрагмента: {self.timestamp_end} \nДлительность видео: {self.duration_minuts} \nВведите любые символы или /start')
            print(mes4user)
            #self.Logger.log_info(mes4user)
            await bot.send_message(message.from_user.id, mes4user) 
            return None
        #
        # добавляем кнопки ОК & NO для подтверждения timestamp
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.button_OK_NO_youtube_timestamp() 
        # отвечаем пользователю на введенные временные метки 
        # отправляем клавиатуру подтверждения ОК & NO
        mes4user=f'Вы передали временные метки фрагмента: \nНачало: [{self.timestamp_start}] \nОкончание: [{self.timestamp_end}] \nПродолжительность фрагмента: [{parsTime.format_sec2minuts(time_sec=self.segment_duration)}] \n\nВидео: [*{self.video_title}*] \nКанал: [*{self.channel_title}*] \n\nНажмите \U0001F447 кнопку ОК, если подтверждаете'
        self.Logger.log_info(mes4user)
        await bot.send_message(message.from_user.id,
                                mes4user, 
                                reply_markup=kb.keyboard)
        # Устанавливаем состояние ожидания подтверждения
        await self.Confirmation.set()
    #
    # обрабатываем нажатие кнопки ОК #7  timestamp
    async def youtube_timestamp_OK(self, callback: types.CallbackQuery, state: FSMContext):
        #
        parsTime=ParseTime(logger=self.Logger)
        mes4user=f'Вы подтвердили временные метки фрагмента: \nНачало: [{self.timestamp_start}] \nОкончание: [{self.timestamp_end}] \nПродолжительность фрагмента: [{parsTime.format_sec2minuts(time_sec=self.segment_duration)}] \n\nВидео: [*{self.video_title}*] \nКанал: [*{self.channel_title}*] \n\nБот начал работать... \nРезультат в виде фрагмента или ссылки на него для скачивания, будет передана в этот чат. \nОжидайте...'
        #self.Logger.log_info(f'[youtube_link_OK] {mes4user}')
        await callback.message.answer(text=mes4user)
        # убираем часики на кнопке, которую нажали
        #await callback.answer() # 
        # Сбрасываем состояние
        await state.finish()
        #
        # дописываем в строку временные метки для БД
        #self.diction4db['video_duration_sec']=self.video_duration_sec
        self.diction4db['timestamp_start']=self.timestamp_start
        self.diction4db['timestamp_start_dt']=self.timestamp_start_dt
        self.diction4db['timestamp_end']=self.timestamp_end
        self.diction4db['timestamp_end_dt']=self.timestamp_end_dt
        self.diction4db['segment_duration']=self.segment_duration
        # задача не проверялась на закачку исходника
        self.diction4db['in_work_download']='no' 
        
        #
        # создаем и передаем словарь значений в БД
        db=BaseDB(logger=self.Logger)
        await db.insert_data(name_table='task', data4db=self.diction4db)
        await db.print_data(name_table='task')
    #
    # обрабатываем нажатие кнопки #4 не та ссылка y2b -> start
    async def call_NO_link(self, callback: types.CallbackQuery, state: FSMContext):
        self.Logger.log_info(f'[call_NO_link] choice_user: {callback.data} ')
        await callback.message.answer(text='Наберите команду /start или введите любой символ')
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
        # Сбрасываем состояние
        await state.finish()
    #
    # обрабатываем нажатие кнопки #5 ОК - повторное введение ссылки 
    async def link_repetition(self, callback: types.CallbackQuery, state: FSMContext):
        self.Logger.log_info(f'[link_repetition] choice_user: {callback.data} ')
        # Отправляем сообщение пользователю
        #await bot.send_message(chat_id=callback.message.chat.id, text='/start')
        # убираем часики на кнопке, которую нажали
        #await callback.answer() 
        # Сбрасываем состояние
        await state.finish()
        await self.call_1_y2b(callback)
# 
    #
    # обрабатываем нажатие кнопки #6 NO - повторное введение ссылки 
    async def link_NO_repetition(self, callback: types.CallbackQuery, state: FSMContext):
        self.Logger.log_info(f'[call_2_level_4] choice_user: {callback.data} ')
        # Отправляем сообщение пользователю
        #await bot.send_message(chat_id=callback.message.chat.id, text='/start')
        await callback.message.bot.send_message(chat_id=callback.message.chat.id, 
                                                text='/start')
        # убираем часики на кнопке, которую нажали
        #await callback.answer() 
        # Сбрасываем состояние
        await state.finish()

    #
    # обрабатываем нажатие кнопкb #7 
    async def call_2_level_6(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.menu_3_level() # загружаем меню третьего уровня
        self.Logger.log_info(f'[call_2_level_6] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.Logger.log_info(f'[call_2_level_6] kb.countInstance: {kb.countInstance}')
        # получаем из словаря наименования кнопок name_button[callback.data] 
        # название раздела
        await callback.message.answer(text=f'Выбран раздел: {kb.name_button[callback.data]}',
                                      reply_markup=kb.keyboard)
        # убираем часики на кнопке, которую нажали
        await callback.answer() # 
    #
    # обрабатываем нажатие кнопкb #8
    async def call_2_level_7(self, callback: types.CallbackQuery):
        kb = KeyBoardClient(logger=self.Logger, row_width=1)
        kb.menu_3_level() # загружаем меню второго уровня
        self.Logger.log_info(f'[call_2_level_7] choice_user: {callback.data} = {kb.name_button[callback.data]}')
        self.Logger.log_info(f'[call_2_level_7] kb.countInstance: {kb.countInstance}')
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
        
        # обрабатываем нажатие кнопки СТАРТ 
        # предлагаем выбрать 'youtube' (#1) или 'свое видео' (#2) 
        dp.register_callback_query_handler(self.call_start, Text(contains='/start', ignore_case=True))
        
        # обрабатываем нажатие кнопки #1 'Видео с youtube'
        dp.register_callback_query_handler(self.call_1_y2b, Text(contains='1', ignore_case=True))
        #
        # обрабатываем нажатие кнопки #2 'Свое видео' 
        dp.register_callback_query_handler(self.call_my_video, Text(contains='2', ignore_case=True))
        #
        # обрабатывает ссылку youtube
        #regexp_pattern = re.compile(r'(https?://)?(www\.)?(youtube\.com/v/.*|youtu\.be/.*|youtube\.com/attribution_link.*|youtube\.com/results\?.*)', re.IGNORECASE)
        regexp_pattern_link_youtube =  re.compile(r'(https?://)?(www\.)?(youtube\.com/(v|watch)\?.*|youtu\.be/.*|youtube\.com/attribution_link.*|youtube\.com/results\?.*)', re.IGNORECASE)
        dp.register_message_handler(self.youtube_link_handler, Regexp(regexp_pattern_link_youtube))
        #
        # обрабатываем нажатие кнопки ОК #3 youtube_link 
        dp.register_callback_query_handler(self.youtube_link_OK, Text(contains='3', ignore_case=True))
        
        # обрабатываем нажатие кнопки #4 не та ссылка y2b -> start
        dp.register_callback_query_handler(self.call_NO_link, Text(contains='4', ignore_case=True))

        # обрабатывает временные метки видео-фрагмента
        #regexp_pattern = re.compile(r'(https?://)?(www\.)?(youtube\.com/v/.*|youtu\.be/.*|youtube\.com/attribution_link.*|youtube\.com/results\?.*)', re.IGNORECASE)
        regexp_pattern_timestamp =  re.compile(r'^\d{2}:\d{2}:\d{2}-\d{2}:\d{2}:\d{2}$', re.IGNORECASE)
        dp.register_message_handler(self.youtube_timestamp, Regexp(regexp_pattern_timestamp))
        
        # обрабатываем нажатие кнопки #5 ОК - повторное введение ссылки 
        #dp.register_callback_query_handler(self.link_repetition, Text(contains='5', ignore_case=True))
        
        # обрабатываем нажатие кнопки #6 NO - повторное введение ссылки 
        #dp.register_callback_query_handler(self.link_NO_repetition, Text(contains='6', ignore_case=True))
        #
        # обрабатываем нажатие кнопки ОК #7  timestamp
        dp.register_callback_query_handler(self.youtube_timestamp_OK, Text(contains='7', ignore_case=True))
        
        # должен быть последним хэндлером, 
        # ловит любые сообщения и вызывает кнопку старт
        dp.register_message_handler(self.any2start)


