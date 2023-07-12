from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

class KeyBoardClient:
    """
    Создаем клавиатуру клиента для telegram-bot:
    
    Аргументы:
    - 
    """
    countInstance=0

    def __init__(self, row_width=3):
        KeyBoardClient.countInstance+=1
        self.countInstance=KeyBoardClient.countInstance
        self.row_width=row_width
        # # создаем клавиатуру
        self.keyboard = InlineKeyboardMarkup(row_width=self.row_width) 
        #
        # кнопки
        #self.b_CS = None
        self.b_y2b = None
        #self.b_Forum = None
        self.user_video = None
        self.b_2020 = None
        self.b_2021 = None
        self.b_2022 = None
        self.b_2023 = None
        self.b_menu_3 = None
        # словарь наименования кнопок и значения, которые ловим хэндлером
        self.name_button = {}
        self._make_name_button()

    # создаем словарь наименований кнопок и значений, которые они отправляют
    # ловим хэндлером: dp.register_callback_query_handler
    def _make_name_button(self):
        self.name_button['/start']='/start'
        # menu # 1
        self.name_button['1']='Видео с youtube'
        self.name_button['2']='Свое видео'
        # menu # 2
        self.name_button['3']='Свое видео'
        self.name_button['4']='2020'
        self.name_button['5']='2021'
        self.name_button['6']='2022'
        self.name_button['7']='2023'
        self.name_button['8']='меню 3 уровня'
        print(f'[_make_name_button] создали клавиатуру № {self.countInstance}')
    #
    # создаем кнопку старта, отправляет команду '/start'
    # используем, когда пользователь набирает любые символы, кроме '/start'
    def start_button(self): 
        start_button = InlineKeyboardButton(text="Это кнопка \U0001F680 ПУСК", 
                                            callback_data='/start')
        self.keyboard.add(start_button)
    #
    # создаем кнопки меню первого уровня
    def menu_1_level(self):
        self.b_y2b = InlineKeyboardButton(text=self.name_button['1'], callback_data='1')
        self.user_video = InlineKeyboardButton(text=self.name_button['2'], callback_data='2')
        #self.user_video = InlineKeyboardButton(text=self.name_button['3'], callback_data='3')
        #self.kb.add(self.b_IMD).add(self.b_Forum).insert(self.b_CS)
        self.keyboard.row(self.b_y2b).row(self.user_video)
    #
    # создаем кнопки меню второго уровня
    def menu_2_level(self):
        self.b_2020 = InlineKeyboardButton(text=self.name_button['4'], callback_data='4')
        self.b_2021 = InlineKeyboardButton(text=self.name_button['5'], callback_data='5')
        self.b_2022 = InlineKeyboardButton(text=self.name_button['6'], callback_data='6')
        self.b_2023 = InlineKeyboardButton(text=self.name_button['7'], callback_data='7')
        #self.kb.add(self.b_IMD).add(self.b_Forum).insert(self.b_CS)
        #self.keyboard.add(self.b_2021).add(self.b_2022).add(self.b_2023)
        self.keyboard.add(self.b_2020, self.b_2021, self.b_2022, self.b_2023)
    #
    # создаем кнопки меню третьего уровня
    def menu_3_level(self):
        self.b_menu_3 = InlineKeyboardButton(text=self.name_button['8'], callback_data='8')
        #self.kb.add(self.b_IMD).add(self.b_Forum).insert(self.b_CS)
        #self.keyboard.add(self.b_2021).add(self.b_2022).add(self.b_2023)
        self.keyboard.add(self.b_menu_3)

