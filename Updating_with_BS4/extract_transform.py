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

    top_scorers.Name = top_scorers.Name.replace(r'([A-Z])', r' \1', regex=True).str.split()
    top_scorers.Name = top_scorers.Name.apply(lambda x: ' '.join(dict.fromkeys(x).keys()))

    top_scorers['Club'] = top_scorers.Name.str.split().str[2:].str.join(' ')
    top_scorers.Name = top_scorers.Name.str.split().str[:2].str.join(' ')
    col = top_scorers.pop("Club")
    top_scorers.insert(2, 'Club', col)
    top_scorers.Club = top_scorers.Club.apply(lambda x: 'Manchester City' if 'Manchester City' in x else x)
    top_scorers.Club = top_scorers.Club.apply(lambda x: 'Manchester United' if 'Manchester United' in x else x)
    top_scorers.Club = top_scorers.Club.apply(lambda x: 'Brighton & Hove Albion' if 'Brighton & Hove Albion' in x else x)

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

    assists.Name = assists.Name.replace(r'([A-Z])', r' \1', regex=True).str.split()
    assists.Name = assists.Name.apply(lambda x: ' '.join(dict.fromkeys(x).keys()))

    assists['Club'] = assists.Name.str.split().str[2:].str.join(' ')
    assists.Name = assists.Name.str.split().str[:2].str.join(' ')
    col = assists.pop("Club")
    assists.insert(2, 'Club', col)
    assists.Club = assists.Club.apply(lambda x: 'Manchester City' if 'Manchester City' in x else x)
    assists.Club = assists.Club.apply(lambda x: 'Manchester United' if 'Manchester United' in x else x)
    assists.Club = assists.Club.apply(lambda x: 'Brighton & Hove Albion' if 'Brighton & Hove Albion' in x else x)
    assists.Name = assists.Name.apply(lambda x: 'Kevin De Bruyne' if 'Kevin De' in x else x)
    #Kevin De Bruyne
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

    detail_top_scorer = detail_top_scorer.drop([''],axis=1)
    detail_top_scorer.Team = detail_top_scorer.Team.str.replace('\n\n','')
    detail_top_scorer['Penalty'] = detail_top_scorer['Goals (Penalty)'].str.split().str[-1:].str.join(' ')
    detail_top_scorer['Penalty'] = detail_top_scorer['Penalty'].str.replace('(','')
    detail_top_scorer['Penalty'] = detail_top_scorer['Penalty'].str.replace(')','')
    detail_top_scorer['Goals (Penalty)'] = detail_top_scorer['Goals (Penalty)'].str.split().str[0].str.join('')
    detail_top_scorer.rename(columns = {'Goals (Penalty)':'Goals'}, inplace = True)
    detail_top_scorer = detail_top_scorer.drop(['#'], axis = 1)
    return detail_top_scorer

    
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
    alltime_table.Team = alltime_table.Team.str.replace('\n', '')
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


    df['Team(s)'] = df['Team(s)'].str.replace('\n\n', '', 1)
    df['Team(s)'] = df['Team(s)'].str.replace('\n\n', ',')
    df['Team(s)'] = df['Team(s)'].str.replace('\n', '')
    df['Player'] = df['Player'].str.replace('*', '')
    df = df.drop(['#'], axis = 1)

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


def goals_per_season():
    url = 'https://www.worldfootball.net/stats/eng-premier-league/1/'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="standard_tabelle")

    for i in table.find_all('th'):
        title = i.text
        headers.append(title)
    goals_per_season = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(goals_per_season)
        goals_per_season.loc[length] = row
        goals_per_season = goals_per_season.drop([''], axis=1)

        goals_per_season = goals_per_season.drop(['#'], axis=1)
        goals_per_season.rename(columns = {'goals':'Goals','Ø goals':'Average Goals'}, inplace = True)
        #winners['Year'] = winners['Year'].str.replace(r'\n', '')
    return goals_per_season


def record_win():
    url = 'https://www.worldfootball.net/stats/eng-premier-league/3/'
    headers = ['Season','Round','date','Home','#','Result','#','Guest']
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="standard_tabelle")


    record_wins = pd.DataFrame(columns = headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(record_wins)
        record_wins.loc[length] = row

    record_wins = record_wins.drop(['#'], axis=1)
    record_wins['Result'] = record_wins['Result'].str.replace(r'\n', '')
    record_wins['Result'] = record_wins['Result'].str.replace(r'\t', '')
    return record_wins


def historical():
    url = 'https://www.worldfootball.net/schedule/eng-premier-league-2019-2020-spieltag/38/'
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    table= soup.find("table", class_="auswahlbox with-border")
    for j in table.find_all('form')[:1]:
        row_data = j.find('select')
        row = [i.text for i in row_data]
    row = [s.replace('\n', '') for s in row]
    while('' in row):
        row.remove("")
    row = [s.replace('/', '-') for s in row]
    a = [f'https://www.worldfootball.net/schedule/eng-premier-league-{i:s}-spieltag/38/' for i in (row)]

    def history(ev):
        url = ev
        headers = ['#','##','Team','M.','W','D','L','goals','Dif.','Pt.']
        page = requests.get(url)
        soup = BeautifulSoup(page.text,  "html.parser")
        table= soup.find_all("table", class_="standard_tabelle")[1]

        historic_table = pd.DataFrame(columns = headers)
        for j in table.find_all('tr')[1:]:
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(historic_table)
            historic_table.loc[length] = row

        historic_table =  historic_table.drop(['##'], axis=1)
        historic_table['Team'] = historic_table['Team'].str.replace('\n', '')
        historic_table = historic_table.rename(columns={'#': 'position','M.': 'Matches','W': 'Wins','D': 'Draws','L': 'Loss' ,'Pt.': 'Points'})
        historic_table.to_csv(f'/workspace/Premier_League_Stats/csv_dir/historical_tables/{url[51:-13]}.csv', index=False)

    for i in a:
        history(i)

def top_scorers_all():
    a = [f'https://www.worldfootball.net/alltime_goalgetter/eng-premier-league/tore/pl-only/{i:d}' for i in (range(1, 53))]
    def all(ev):
        
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

        players['Team(s)'] = players['Team(s)'].str.replace('\n\n', '', 1)
        players['Team(s)'] = players['Team(s)'].str.replace('\n\n', ',')
        players['Team(s)'] = players['Team(s)'].str.replace('\n', '')

        players.rename(columns = {'M.':'Matches'}, inplace = True)
        players = players.drop('#',axis=1)
        players.to_csv('/workspace/Premier_League_Stats/csv_dir/top_scorers(all_time).csv', index=False,mode='a', header=False)

    for i in a:
        print(i)
        all(i)