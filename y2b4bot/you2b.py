import os
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bot_env.mod_log import Logger

class You2b:
    """
    Проверяем ссылку youtube:
    
    Аргументы:
    - url: str 
    - logger: Logger
    """
    countInstance=0

    def __init__(self, url: str, logger: Logger):
        You2b.countInstance+=1
        self.countInstance=You2b.countInstance
        self.url=url
        self.Logger=logger
        self.api_key=os.getenv('API_KEY')
        self._print_API_KEY()

        self.video_id=self._extract_video_id()
        self.link_info=self.check_youtube_link()
        self.video_title=self.link_info[1]
        #self.video_duration=self.link_info[2]
        self.video_duration=100
    #
    # проверка API_KEY
    def _print_API_KEY(self):
        print(f'[_print_API_KEY] API_KEY: {self.api_key}')
        self.Logger.log_info(f'[_print_API_KEY] API_KEY: {self.api_key}')
        #
        print(f'[_print_API_KEY] url: {self.url}')
        self.Logger.log_info(f'[_print_API_KEY] url: {self.url}')
    #
    # поиск идентификатора видео
    def _extract_video_id(self):
        # Паттерн для поиска идентификатора видео
        #pattern = r'(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=|youtu\.be\/|[^#]*[?&]v=)([^&#?]{11}))'
        #
        pattern = r'(?:youtu\.be\/|youtube\.com\/(?:embed\/|v\/|watch\?v=|watch\?.+&v=))([^&]{11})'

        # Ищем совпадения с паттерном в ссылке
        match = re.search(pattern, self.url)

        print(f'[extract_video_id] match.groups(): {str(match.groups())} - type[{type(match)}]')
        self.Logger.log_info(f'[extract_video_id] match: {str(match)} - type[{type(match)}]')
        #
        video_id=None
        if match:
            video_id = match.group(1)  # Получаем найденный video_id
            print(f'[extract_video_id] video_id: {video_id}\ntype(video_id): {type(video_id)}')
            self.Logger.log_info(f'[extract_video_id] video_id: {video_id}\ntype(video_id): {type(video_id)}')
            return video_id # Если video_id найден
        #
        print(f'[extract_video_id] video_id: {video_id}')
        self.Logger.log_info(f'[extract_video_id] video_id: {video_id}')
        return None  # Если video_id не найден
    # 
    # проверка ссылки ютуб
    def check_youtube_link(self):
        #video_id=self.extract_video_id(url)
        # проверяем наличие идентификатора видео
        if not self.video_id:
            print(f'[check_youtube_link] video_id: [{self.video_id}] - return None')
            self.Logger.log_info(f'[check_youtube_link] video_id: [{self.video_id}] - return None')
            return None
        #
        youtube = build('youtube', 'v3', developerKey=self.api_key)
        try:
            response = youtube.videos().list(part='snippet,contentDetails', id=self.video_id).execute()
            if response['items']:
                print(f'[check_youtube_link] response: {str(response)} - type[{type(response)}')
                self.Logger.log_info(f'[check_youtube_link] response: {str(response)}')
                #
                video_info = response['items'][0]['snippet']
                video_title = video_info['title']
                #video_duration = video_info['duration']
                #
                print(f'[check_youtube_link] video_info: {str(video_info)}')
                self.Logger.log_info(f'[check_youtube_link] video_info: {str(video_info)}')
                #
                print(f'[check_youtube_link] video_title: {str(video_title)}')
                self.Logger.log_info(f'[check_youtube_link] video_title: {str(video_title)}')
                #
                # print(f'[check_youtube_link] video_duration: {str(video_duration)}')
                # self.Logger.log_info(f'[check_youtube_link] video_duration: {str(video_duration)}')
                #
                # Другие необходимые данные о видео
                return True, video_title
            else:
                return False, None, None
        except HttpError as e:
            return False, None, None

