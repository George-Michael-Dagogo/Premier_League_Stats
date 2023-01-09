from scrape import *

from sqlalchemy import create_engine

df = player_table()
conn_string = 'postgresql://Omni:v2_3xntW_C6CBDbsKrZ9m9YbcHfUgCkn@db.bit.io/Omni/trial'
  
db = create_engine(conn_string)
conn = db.connect()

df.to_sql('player_table', con=conn, if_exists='append',
        index=False)