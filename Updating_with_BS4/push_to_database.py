import bitdotio
import pandas as pd
import psycopg2
import configparser
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
# Connect with bit.io API key credentials
conn = psycopg2.connect(database = os.getenv('DATABASE'),
                                user= os.getenv('USER'), 
                                password= os.getenv('PASSWORD'),
                                host= os.getenv('HOST')
        )

conn.autocommit = True
cursor = conn.cursor()

drop_tables = """
DROP TABLE IF EXISTS
 players,
 all_time_team,
 players_detail,
 manager,
 stadium,
 all_time_winner_club,
 all_time_scorers_penalty,
 all_time_scorers_own_goal,
 goals_per_season,
 top_scorers_seasons,
 record_wins,
 top_scorers_all_time
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
  points INT NOT NULL
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

CREATE TABLE IF NOT EXISTS manager (
  manager_id UUID DEFAULT uuid_generate_v1 () PRIMARY KEY,
  team_id UUID ,
  name VARCHAR(45) NOT NULL,
  team VARCHAR(45) NOT NULL,
  nationality VARCHAR(45) NOT NULL,
  CONSTRAINT fk_team
        FOREIGN KEY(team_id) 
                REFERENCES all_time_team(team_id)
  );


CREATE TABLE IF NOT EXISTS stadium (
  stadium_id UUID DEFAULT uuid_generate_v1 () PRIMARY KEY,
  team_id UUID ,
  city VARCHAR(45) NOT NULL,
  team VARCHAR(45) NOT NULL,
  stadium VARCHAR(45) NOT NULL,
  capacity VARCHAR(45) NOT NULL,
  CONSTRAINT fk_team
        FOREIGN KEY(team_id) 
                REFERENCES all_time_team(team_id)
  );


CREATE TABLE IF NOT EXISTS all_time_winner_club (
  year INT NOT NULL PRIMARY KEY,
  team_id UUID ,
  team VARCHAR(45) NOT NULL,
  country VARCHAR(45) NOT NULL,
  CONSTRAINT fk_team
        FOREIGN KEY(team_id) 
                REFERENCES all_time_team(team_id)
  );

CREATE TABLE IF NOT EXISTS all_time_scorers_penalty (
  player_id UUID ,
  player VARCHAR(45) NOT NULL,
  teams VARCHAR(200) NOT NULL,
  penalty INT NOT NULL,
  CONSTRAINT fk_player
        FOREIGN KEY(player_id) 
                REFERENCES players(player_id)
  );

CREATE TABLE IF NOT EXISTS all_time_scorers_own_goal (
  player_id UUID ,
  player VARCHAR(45) NOT NULL,
  teams VARCHAR(200) NOT NULL,
  own_goal INT NOT NULL,
  CONSTRAINT fk_player
        FOREIGN KEY(player_id) 
                REFERENCES players(player_id)
  );


CREATE TABLE IF NOT EXISTS goals_per_season (
  season VARCHAR(45) NOT NULL PRIMARY KEY,
  goals INT NOT NULL,
  matches INT NOT NULL,
  average_goal FLOAT NOT NULL
  );

CREATE TABLE IF NOT EXISTS top_scorers_seasons (
  player_id UUID ,
  season VARCHAR(45),
  team_id UUID,
  seasons VARCHAR(45) NOT NULL,
  player VARCHAR(45) NOT NULL,
  team VARCHAR(200) NOT NULL,
  goals INT NOT NULL,
  CONSTRAINT fk_player
        FOREIGN KEY(player_id) 
                REFERENCES players(player_id),
  CONSTRAINT fk_team
        FOREIGN KEY(team_id) 
	        REFERENCES all_time_team(team_id),
  CONSTRAINT fk_season
        FOREIGN KEY(season) 
	        REFERENCES goals_per_season(season)

);


CREATE TABLE IF NOT EXISTS record_wins (
  season VARCHAR(45),
  seasons VARCHAR(45) NOT NULL,
  Round VARCHAR(45) NOT NULL ,
  date VARCHAR(45) NOT NULL,
  Home VARCHAR(45) NOT NULL,
  Result VARCHAR(45) NOT NULL,
  Guest VARCHAR(45) NOT NULL,
  CONSTRAINT fk_season
        FOREIGN KEY(season) 
	        REFERENCES goals_per_season(season)
);


CREATE TABLE IF NOT EXISTS top_scorers_all_time (
  player_id UUID ,
  player VARCHAR(45) NOT NULL,
  teams VARCHAR(200) NOT NULL,
  Matches INT NOT NULL,
  goals INT NOT NULL,
  Penalty INT NOT NULL,
  Average FLOAT NOT NULL,
  CONSTRAINT fk_player
        FOREIGN KEY(player_id) 
                REFERENCES players(player_id)


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
COPY all_time_team (position,Team,Matches,wins,Draws,Losses,Goals,Dif,points)
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

copy_manager_table_sql = """
COPY manager (name,team,nationality)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/manager.csv', 'r') as f:
        cursor.copy_expert(sql=copy_manager_table_sql, file=f)


copy_stadium_table_sql = """
COPY stadium (City,team,Stadium,Capacity)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/stadiums.csv', 'r') as f:
        cursor.copy_expert(sql=copy_stadium_table_sql, file=f)


copy_all_time_winner_club_table_sql = """
COPY all_time_winner_club (Year,team,Country)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/alltime_winners(clubs).csv', 'r') as f:
        cursor.copy_expert(sql=copy_all_time_winner_club_table_sql, file=f)



copy_all_time_scorers_penalty_table_sql = """
COPY all_time_scorers_penalty (Player,Teams,Penalty)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/alltime_scorers(Penalty).csv', 'r') as f:
        cursor.copy_expert(sql=copy_all_time_scorers_penalty_table_sql, file=f)

copy_all_time_scorers_own_goal_table_sql = """
COPY all_time_scorers_own_goal (Player,Teams,own_goal)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/alltime_scorers(own_goal).csv', 'r') as f:
        cursor.copy_expert(sql=copy_all_time_scorers_own_goal_table_sql, file=f)

copy_goals_per_season_table_sql = """
COPY goals_per_season (Season,Goals,Matches,Average_Goal)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/goals_per_season.csv', 'r') as f:
        cursor.copy_expert(sql=copy_goals_per_season_table_sql, file=f)


copy_top_scorers_seasons_table_sql = """
COPY top_scorers_seasons (Seasons,player,Team,goals)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/top_scorers(seasons).csv', 'r') as f:
        cursor.copy_expert(sql=copy_top_scorers_seasons_table_sql, file=f)


copy_record_wins_table_sql = """
COPY record_wins (Seasons,Round,date,Home,Result,Guest)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/record_wins.csv', 'r') as f:
        cursor.copy_expert(sql=copy_record_wins_table_sql, file=f)


copy_top_scorers_all_time_table_sql = """
COPY top_scorers_all_time (Player,Teams,Matches,goals,Penalty,Average)
    FROM stdin
    DELIMITER ','
    CSV HEADER;
    """
with open('./csv_dir/top_scorers(all_time).csv', 'r') as f:
        cursor.copy_expert(sql=copy_top_scorers_all_time_table_sql, file=f)

conn.close()      

conn_string = os.getenv('CONN_STRING')
db = create_engine(conn_string)
conn = db.connect()

detailed_top_scorers  = pd.read_csv("csv_dir/detail_top_scorers.csv")
detailed_top_scorers.to_sql('detailed_top_scorers ', con=conn, if_exists='replace', index=False)

top_scorers  = pd.read_csv("csv_dir/top_scorers.csv")
top_scorers.to_sql('top_scorers', con=conn, if_exists='replace', index=False)

assists  = pd.read_csv("csv_dir/assists.csv")
assists.to_sql('assists', con=conn, if_exists='replace', index=False)

league_table  = pd.read_csv("csv_dir/league_table.csv")
league_table.to_sql('league_table', con=conn, if_exists='replace', index=False)
conn.close()