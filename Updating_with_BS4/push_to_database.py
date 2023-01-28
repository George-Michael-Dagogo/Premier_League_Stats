import bitdotio
import pandas as pd
import psycopg2
import configparser
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
# Connect with bit.io API key credentials
conn = psycopg2.connect(database = os.getenv('database'),
                                user= os.getenv('user'), 
                                password= os.getenv('password'),
                                host= os.getenv('host')
        )

conn.autocommit = True
cursor = conn.cursor()

drop_tables = """
DROP TABLE IF EXISTS
 players,
 all_time_team,
 players_detail
 ;
"""
cursor.execute(drop_tables) 

create_uuid = """
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

"""   
cursor.execute(create_uuid) 

# Create table, if it does not already exist
create_all_database_table = """
CREATE TABLE IF NOT EXISTS players (
  player_id UUID DEFAULT uuid_generate_v1 () PRIMARY KEY,
  player VARCHAR(45) NOT NULL,
  position VARCHAR(45) NOT NULL,
  nationality VARCHAR(45) NOT NULL
  );

CREATE TABLE IF NOT EXISTS all_time_team (
  team_id UUID DEFAULT uuid_generate_v1 () PRIMARY KEY ,
  position VARCHAR(45) NOT NULL,
  team VARCHAR(45) NOT NULL,
  matches INT NOT NULL,
  wins INT NOT NULL,
  draws INT NOT NULL,
  losses INT NOT NULL,
  goals VARCHAR(45) NOT NULL,
  dif INT NOT NULL,
  poINTs INT NOT NULL
);


CREATE TABLE IF NOT EXISTS players_detail (
  player_id UUID,
  team_id UUID ,
  player VARCHAR(45) NOT NULL,
  team VARCHAR(45) NOT NULL,
  DOB VARCHAR NOT NULL,
  height VARCHAR(10) NOT NULL,
  position VARCHAR(45) NOT NULL,
  CONSTRAINT fk_player
        FOREIGN KEY(player_id) 
                REFERENCES players(player_id),
  CONSTRAINT fk_team
        FOREIGN KEY(team_id) 
	        REFERENCES all_time_team(team_id)
);
"""
cursor.execute(create_all_database_table)

copy_player_table_sql = """
COPY players (player, position, nationality)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/player.csv', 'r') as f:
        cursor.copy_expert(sql=copy_player_table_sql, file=f)



copy_all_time_table_sql = """
COPY all_time_team (position,Team,Matches,wins,Draws,Losses,Goals,Dif,PoINTs)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/all_time_table.csv', 'r') as f:
        cursor.copy_expert(sql=copy_all_time_table_sql, file=f)




copy_player_detail_table_sql = """
COPY players_detail (Player,Team,DOB,Height,Position)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/player_table.csv', 'r') as f:
        cursor.copy_expert(sql=copy_player_detail_table_sql, file=f)
conn.close()      
