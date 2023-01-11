import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import when,lit,col,split,expr
from pyspark.sql.types import StringType
import re
import numpy as np

spark = SparkSession \
.builder \
.appName("Python Spark create RDD example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

def get_table():

    #EXTRACT with beautiful soup

    url = 'https://www.bbc.com/sport/football/premier-league/table'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="ssrcss-14j0ip6-Table e3bga5w4")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    league_table = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(league_table)
        league_table.loc[length] = row

    #TRANSFORM with PySpark

    league = spark.createDataFrame(league_table) 
    #drop unwanted column
    league = league.drop("Form, Last 6 games, Oldest first") 
    #rename columns
    league = league.withColumnRenamed("Goals For","Goals_For").withColumnRenamed("Goals Against","Goals_Against") \
                    .withColumnRenamed("Goal Difference","Goal_Difference")
    #add new columns based on existing columns
    league = league.withColumn("grade_class",  when((league.Lost == 0) & (league.Drawn == 0) \
                        & (league.Points <= league['Goals_For']), lit("S"))
                        .when((league.Lost == 0) & (league.Drawn <= 2), lit("A+"))
                        .when((league.Lost <= 1) & (league.Drawn <= 3), lit("A"))
                    .when((league.Lost <= 1  ) & (league.Drawn <= 4  ), lit('B'))
                    .otherwise('C')) 

    league = league.withColumn('conqueror', lit(league.Points - (league.Played * 3)).cast('integer'))
    # conqueror column, the difference between the teams current points and the points if they didn't loose or draw any math
    return league.toPandas()


def get_top_scorers():
    url = 'https://www.bbc.com/sport/football/premier-league/top-scorers'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="gs-o-table")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    top_scorers = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(top_scorers)
        top_scorers.loc[length] = row



    return top_scorers


def get_assists():
    url = 'https://www.bbc.com/sport/football/premier-league/top-scorers/assists'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="gs-o-table")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    assists = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(assists)
        assists.loc[length] = row
    return assists

def detail_top():
    url = 'https://www.worldfootball.net/goalgetter/eng-premier-league-2022-2023/'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="standard_tabelle")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    detail_top_scorer = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(detail_top_scorer)
        detail_top_scorer.loc[length] = row

    detail_top = spark.createDataFrame(detail_top_scorer) 
    detail_top = detail_top.drop(detail_top[''])
    detail_top = detail_top.withColumn("Team", F.regexp_replace(F.col("Team"), "[\'\n\n']", ""))
    return detail_top.toPandas()


def stadiums():
    url = 'https://www.stadiumguide.com/premier-league-stadiums/'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", id="tablepress-45")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    stadiums = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(stadiums)
        stadiums.loc[length] = row

    stadiums
    stadium = spark.createDataFrame(stadiums) 
    stadium = stadium.withColumnRenamed("Cap.","Capacity")
    return stadium.toPandas()

def player_table():
    a = [f'https://www.worldfootball.net/players_list/eng-premier-league-2022-2023/nach-name/{i:d}' for i in (range(1, 15))]
    header = ['Player','','Team','born','Height','Position']
    df = pd.DataFrame(columns=header)
    def player(ev):
        url = ev
        headers = []
        page = requests.get(url)
        soup = BeautifulSoup(page.text,  "html.parser")
        table= soup.find("table", class_="standard_tabelle")

        for i in table.find_all('th'):
            title = i.text
            headers.append(title)
        players = pd.DataFrame(columns = headers)
        for j in table.find_all('tr')[1:]:
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(players)
            players.loc[length] = row
        return players

    for i in a:
        a = player(i)
        df = pd.concat([df, a], axis=0).reset_index(drop=True)

    df = df.drop([''], axis=1)
    return df


def all_time_table():
    url = 'https://www.worldfootball.net/alltime_table/eng-premier-league/pl-only/'
    headers = ['pos','#','Team','Matches','wins','Draws','Losses','Goals','Dif','Points']
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="standard_tabelle")


    alltime_table= pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(alltime_table)
        alltime_table.loc[length] = row

    alltime_table = alltime_table.drop(['#'], axis=1)
    alltime_table.Team = alltime_table.Team.str.replace(r'\n', '')
    return alltime_table

def all_time_scorer_own_goal():
    a = [f'https://www.worldfootball.net/alltime_goalgetter/eng-premier-league/eigentore/{i:d}' for i in (range(1, 32))]
    header = ['#','Player','Team(s)','own goal']
    df = pd.DataFrame(columns=header)
    def player(ev):
        url = ev
        headers = []
        page = requests.get(url)
        soup = BeautifulSoup(page.text,  "html.parser")
        table= soup.find("table", class_="standard_tabelle")

        for i in table.find_all('th'):
            title = i.text
            headers.append(title)
        players = pd.DataFrame(columns = headers)
        for j in table.find_all('tr')[1:-1]:
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(players)
            players.loc[length] = row
        return players

    for i in a:
        a = player(i)
        df = pd.concat([df, a], axis=0).reset_index(drop=True)


    df['Team(s)'] = df['Team(s)'].str.replace(r'\n\n', '', 1)
    df['Team(s)'] = df['Team(s)'].str.replace(r'\n\n', ',')
    df['Team(s)'] = df['Team(s)'].str.replace(r'\n', '')

    return df


def all_time_winner_club():
    url = 'https://www.worldfootball.net/winner/eng-premier-league/'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="standard_tabelle")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    winners = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(winners)
        winners.loc[length] = row

    winners = winners.drop([''], axis=1)
    winners['Year'] = winners['Year'].str.replace(r'\n', '')
    return winners


def top_scorers_seasons():
    url = 'https://www.worldfootball.net/top_scorer/eng-premier-league/'
    headers = ['Season','#','Top scorer','#','Team','goals']
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="standard_tabelle")
    winners = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(winners)
        winners.loc[length] = row

    winners = winners.drop(['#'], axis=1)
    winners=winners.replace('\\n','',regex=True).astype(str)
    winners['Season'] = winners['Season'].replace('', np.nan).ffill()
    return winners
