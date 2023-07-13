from sqlalchemy import MetaData, Table, Column, Integer, String

metadata = MetaData() 
name_table = 'snipper'
table = Table(
    name_table,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("url_video_y2b", String(50)),
    Column("video_title", String(50)),
    Column("video_id", String(50)),
    Column("video_duration", String(50)),
    Column("username", String(50)),
    Column("date_message", String(50)),
    Column("user_id", String(50)),
    Column("chat_id", String(50))
            ) 
