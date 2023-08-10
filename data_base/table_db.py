from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, func
from datetime import datetime

metadata = MetaData()
#
name_table_task = 'task'
table_task = Table(
    name_table_task,
    metadata,
    Column("id", Integer, primary_key=True),
    #Column("task_time_dt", DateTime), # Получаем текущее время задачи
    # Получаем текущее время задачи
    # Column("current_timestamp", DateTime, default=func.current_timestamp()), 
    # Column("current_date", DateTime, default=func.current_date()), 
    # Column("current_time", DateTime, default=func.current_time()), 
    # Column("localtime", DateTime, default=func.localtime()), 
    # Column("localtimestamp", DateTime, default=func.localtimestamp()), 
    # Column("now", DateTime, default=func.now()), 

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
    Column("in_work_download", String(3)), # yes or no
    # время начала выполнения задачи
    Column("time2work", DateTime, default=func.current_timestamp()), 


            ) 
#
name_table_dowmload = 'download_link'
table_download = Table(
    name_table_dowmload,
    metadata,
    Column("id", Integer, primary_key=True),
    # Получаем текущее время задачи
    #Column("current_timestamp", DateTime, default=func.current_timestamp()), 
    #Column("current_date", DateTime, default=func.current_date()), 
    #Column("current_time", DateTime, default=func.current_time()), 
    #Column("localtime", DateTime, default=func.localtime()), 
    #Column("localtimestamp", DateTime, default=func.localtimestamp()), 
    #Column("now", DateTime, default=func.now()), 
    #
    Column("url_video_y2b", String(150)),
    Column("video_id", String(50)),
    # время начала-окончания выполнения задачи
    # Column("start2work", DateTime, default=func.current_timestamp()), 
    # Column("end2work", DateTime, default=func.current_timestamp()), 
    # Column("duratiion_work", DateTime, default=func.current_timestamp()), 
    Column("worked_link", String(15)), # No, YES
    Column("path_download", String(150)),
            ) 


# Объединение таблиц в словарь, где ключами будут имена таблиц, 
# а значениями - соответствующие объекты Table
tables = {
    name_table_task: table_task,
    name_table_dowmload: table_download,
            }

