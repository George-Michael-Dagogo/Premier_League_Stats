from scrape import *

from sqlalchemy import create_engine

df = player_table()
conn_string = 'postgresqll'
  
db = create_engine(conn_string)
conn = db.connect()

df.to_sql('player_table', con=conn, if_exists='append',
        index=False)