import os
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bot_env.mod_log import Logger

class You2b:
    """
    Проверяем ссылку youtube:
    
    Аргументы:
    - 
    """
    countInstance=0

    def __init__(self, logger: Logger):
        You2b.countInstance+=1
        self.countInstance=You2b.countInstance
        self.Logger=logger
        self.api_key=os.getenv('API_KEY')
    #
    # поиск идентификатора видео
    def extract_video_id(self, url):
        # Паттерн для поиска идентификатора видео
        pattern = r'(?:youtube\.com\/(?:[^\/]+\/.+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=|youtu\.be\/|[^#]*[?&]v=)([^&#?]{11}))'
        #
        # Ищем совпадения с паттерном в ссылке
        match = re.search(pattern, url)
        print(f'[You2b - extract_video_id] match: {match}')
        self.Logger.log_info(f'[You2b - extract_video_id] match: {match}')
        #
        video_id=None
        if match:
            video_id = match.group(1)  # Получаем найденный video_id
            print(f'[You2b - extract_video_id] video_id: {video_id}')
            self.Logger.log_info(f'[You2b - extract_video_id] video_id: {video_id}')
            return video_id
        #
        print(f'[You2b - extract_video_id] video_id: {video_id}')
        self.Logger.log_info(f'[You2b - extract_video_id] video_id: {video_id}')
        return None  # Если video_id не найден
    # 
    # проверка ссылки ютуб
    def check_youtube_link(self, url):
        video_id=self.extract_video_id(url)
        if not video_id:
            print(f'[You2b - check_youtube_link] video_id: {video_id}')
            self.Logger.log_info(f'[You2b - check_youtube_link] video_id: {video_id}')
            return None
        #
        youtube = build('youtube', 'v3', developerKey=self.api_key)
        try:
            response = youtube.videos().list(part='snippet,contentDetails', id=video_id).execute()
            if response['items']:
                print(f'[You2b - check_youtube_link] video_id: {str(response)}')
                self.Logger.log_info(f'[You2b - check_youtube_link] video_id: {str(response)}')
                #
                video_info = response['items'][0]['snippet']
                video_title = video_info['title']
                video_duration = video_info['duration']
                # Другие необходимые данные о видео
                return True, video_title, video_duration
            else:
                return False, None, None
        except HttpError as e:
            return False, None, None

