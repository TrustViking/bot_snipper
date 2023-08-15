from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, func
from datetime import datetime

metadata = MetaData()
#
name_table_task = 'task'
table_task = Table(
    name_table_task, 
    metadata,
    Column("id", Integer, primary_key=True),
    Column("time_task", Integer), # Получаем текущее время постановки задачи
    Column("time2work", Integer), 
    Column("update_time", Integer),
    Column("time_dnld", Integer), 
    Column("time_frag", Integer), 
    # 
    Column("url_video_y2b", String(50)),
    Column("video_id", String(50)),
    Column("channel_title", String(150)),
    Column("video_title", String(150)),
    Column("video_duration", String(50)),
    Column("video_duration_sec", String(50)),
    Column("duration_minuts", String(50)),
    Column("datatime_duration", DateTime),
    Column("default_audio_language", String(20)),
    Column("username", String(50)),
    Column("date_message", String(50)),
    Column("user_id", String(50)),
    Column("segment_duration", String(15)),
    Column("timestamp_start", String(15)),
    Column("timestamp_start_dt", DateTime),
    Column("timestamp_end", String(15)),
    Column("timestamp_end_dt", DateTime),
    #
    Column("in_work_download", String(15)), # downloaded or not_download
    Column("path_download", String(150)), # /path/ or not_path
    Column("in_work_frag", String(15)), # frag or not_frag
    Column("path_frag", String(150)), # /path/ or not_path

            ) 
#
name_table_dnld = 'dnld_link'
table_download = Table(
    name_table_dnld,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("time_task", Integer), # Получаем текущее время постановки задачи
    Column("time2work", Integer), 
    Column("update_time", Integer), 
    Column("time_dnld", Integer), 
    #
    Column("url_video_y2b", String(150)),
    Column("video_id", String(50)),
    # отметка скачанная или нет ссылка 
    Column("in_work_download", String(15)), # downloaded or not_download
    Column("path_download", String(150)), # /path/ or not_path
    #Column("in_work_frag", String(15)), # fraged or not_frag
    #Column("name_frag", String(100)), # 
            ) 

name_table_frag = 'frag'
table_frag = Table(
    name_table_frag,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("date_message", String(50)),
    Column("username", String(50)),
    Column("user_id", String(50)),
    #
    Column("time_task", Integer), # Получаем текущее время постановки задачи
    Column("time2work", Integer), 
    Column("update_time", Integer), 
    Column("time_frag", Integer), 
    #
    Column("url_video_y2b", String(150)),
    Column("video_id", String(50)),
    #
    Column("timestamp_start", String(15)),
    Column("timestamp_start_dt", DateTime),
    Column("timestamp_end", String(15)),
    Column("timestamp_end_dt", DateTime),
    #
    Column("in_work_download", String(15)), # downloaded or not_download
    Column("path_download", String(150)), # /path/ or not_path
    Column("in_work_frag", String(15)), # fraged or not_frag
    Column("name_frag", String(100)), # 
    Column("path_frag", String(150)), # /path/ or not_frag
            ) 
# Объединение таблиц в словарь, где ключами будут имена таблиц, 
# а значениями - соответствующие объекты Table
tables = {
    name_table_task: table_task,
    name_table_dnld: table_download,
    name_table_frag: table_frag,

            }

