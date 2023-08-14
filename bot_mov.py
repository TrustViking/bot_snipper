#!/usr/bin/env python3 
#

from concurrent.futures import ThreadPoolExecutor
import os, sys, asyncio, logging
#
from bot_env.mod_log import Logger
from data_base.base_db import BaseDB
#
from moviepy.editor import VideoFileClip
from moviepy.editor import VideoFileClip, concatenate_videoclips, TextClip
from moviepy.video.fx import all as vfx
#
#
class Mov:
    """Modul for video composition"""
    countInstance=0
    #
    def __init__(self, 
                 log_file='mov_log.txt', 
                 log_level=logging.DEBUG,
                 max_download=3,
                 ):
        Mov.countInstance += 1
        self.countInstance = Mov.countInstance
        # Logger
        self.Logger = Logger(log_file=log_file, log_level=log_level)
        self.Db = BaseDB(logger=self.Logger)
        self.max_download = max_download
        self.downloaded_file_path = os.path.join(sys.path[0], 'video_frag')
        self._print()
        self._create_mov_dir()
        
    #
    # выводим № объекта
    def _print(self):
        print(f'[Mov] countInstance: [{self.countInstance}]')
        self.Logger.log_info(f'[Mov] countInstance: [{self.countInstance}]')
#
    # проверка директории для фрагментов видео
    def _create_mov_dir(self):
        """
        Создает директорию для хранения скачанных видеофайлов, 
        если она не существует
        """
        if not os.path.exists(self.downloaded_file_path):
            os.makedirs(self.downloaded_file_path)
    #
    # вырезаем фрагмент по временным меткам ext='.mp4'
    def make_frag (self, path_file: str, 
                        t_start: str, 
                        t_end: str,
                        file_name: str,
                        ext='.mp4'):
        #
        # Обрезка видео по временным меткам
        clip = VideoFileClip(filename=path_file).subclip(t_start, t_end)
        print(f'[Mov make_frag] clip: {type(clip)}') 

        # сохранение видео
        # clip.write_videofile(f'{os.path.join(self.downloaded_file_path, file_name+ext)}')
        clip.close()

        # # Удаление лого из фрагмента
        # clip1 = clip1.fx(vfx.crop, x1=10, y1=10, x2=110, y2=30) # Координаты логотипа

        # # Уменьшение первого фрагмента и размещение его над вторым фрагментом
        # clip1_resized = clip1.resize(0.5)
        # final_clip = concatenate_videoclips([clip1_resized, clip2], method="compose")

        # # Добавление титров
        # txt_clip = TextClip("Your Title Here", fontsize=24, color='white', size=final_clip.size)
        # txt_clip = txt_clip.set_duration(final_clip.duration)

        # final_clip = concatenate_videoclips([final_clip, txt_clip.set_pos("center")], method="compose")

        # # Запись итогового видеофайла
        # final_clip.write_videofile("output.mp4", codec="libx264")

        # # Освобождение ресурсов
        # clip1.close()
        # clip2.close()
        # final_clip.close()

    def process_video(self, filename):
        clip = VideoFileClip(filename)
        # Обработка видео
        clip.write_videofile(f"output_{filename}")

#
# MAIN **************************
async def main():
    print(f'\n**************************************************************************')
    print(f'\nБот готов обрабатывать видео')

    # создаем объект класса 
    dnld=Dnld() 
    #
    minut=1
    while True:
        #
        print(f'\nБот по скачиванию ждет {minut} минут(ы) ...')
        sleep (60*minut)
        print(f'\nСодержание таблиц в БД...')
        await dnld.Db.print_data(name_table='task')
        await dnld.Db.print_data(name_table='download_link')
        
        try:
            # формируем список-закачек из таблицы download_link
            vid_url = await dnld.download_video()
        except Exception as eR:
            print(f'\nERROR[Dnld main] ERROR: {eR}') 
            dnld.Logger.log_info(f'\nERROR[Dnld main] ERROR: {eR}') 
        
        # проверка есть ли видео для закачивания
        if not vid_url: 
            print(f'\n[Dnld main] пока нет задач для закачек...')
            dnld.Logger.log_info(f'\n[Dnld main] пока нет задач для закачек...')
            
            # если нет закачек, сверяем БД и файлы на диске
            # vid_dnld = await dnld.check_dnld_file() 
            if await dnld.check_dnld_file():
                await dnld.Db.print_data(name_table='task')
                await dnld.Db.print_data(name_table='download_link')
            
            # удаляем временные файлы после скачивания dlp
            await dnld.delete_non_mp4_files()            
            
            # если нет списка закачек, то после сравнения БД и файлов на диске, новая итерация
            continue
        
        # скачиваем файлы 
        results = await dnld.dlp(vid_url)
        if not results:
            print(f"\n[Dnld main] Ошибка при скачивании dnld.dlp(vid_url)")
            continue
        # проверяем скачанные файлы на диске
        for result in results:
            print(f"\n[Dnld main] Downloaded result: {result}")
            # проверяем скачал ли run_dlp файл на диск
            if not await dnld.dnld_file(*result):
                print(f"\n[Dnld main] result: {result} update full_parth_vid is NONE")

        print(f'\nСодержание таблиц в БД...')
        await dnld.Db.print_data(name_table='task')
        await dnld.Db.print_data(name_table='download_link')

        # сверяем БД и файлы закачки
        vid_dnld = await dnld.check_dnld_file() 
        if vid_dnld:
            await dnld.Db.print_data(name_table='task')
            await dnld.Db.print_data(name_table='download_link')
        # удаляем временные файлы после скачивания dlp
        await dnld.delete_non_mp4_files()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
