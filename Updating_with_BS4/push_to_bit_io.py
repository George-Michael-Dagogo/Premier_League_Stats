import bitdotio
import pandas as pd
import psycopg2

# Connect with bit.io API key credentials
conn = psycopg2.connect(database='Omni/trial',
                                user='Omni', 
                                password='v2_3xqwt_Ep9Z96BzrNEuYpskkibKFqM',
                                host='db.bit.io'
        )

conn.autocommit = True
cursor = conn.cursor()
    

# Create table, if it does not already exist
create_stadium_table = """
CREATE TABLE IF NOT EXISTS stadiums (
  city varchar(45) NOT NULL,
  club varchar(45) NOT NULL,
  stadium varchar(45) NOT NULL,
  capacity varchar(45) NOT NULL,
  PRIMARY KEY (stadium)
)
"""
cursor.execute(create_stadium_table)
# Copy csv from a local file
copy_table_sql = """
COPY stadiums (city, club, stadium,capacity)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/stadiums.csv', 'r') as f:
        cursor.copy_expert(sql=copy_table_sql, file=f)


conn.commit()
conn.close()       
