from sqlalchemy import MetaData, Table, Column, Integer, String

metadata = MetaData() 
name_table = 'snipper'
table = Table(
    name_table,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("url_video_y2b", String(25)),
    Column("id_user", String(20)),
    Column("id_chat", String(20))
            ) 
