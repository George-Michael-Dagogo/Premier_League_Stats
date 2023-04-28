
# Premier League Stats

## Aim
### The primary objective of this project was to gather and integrate data from various sources related to the Premier League, in order to perform detailed analysis, predictions, and visualizations. An additional goal was to develop a dynamic and interactive dashboard, with real-time updates as soon as the database is modified

## Architecture

![alt text](https://github.com/George-Michael-Dagogo/Premier_League_Stats/blob/main/data_model.jpg)

### Data is extracted from several football websites and stored in two locations, this repository and a cloud postgres database and this databases are updated every weekend.

## Details
### The ./Updating_with_BS4/extract_transform.py script web-scrapes data from football websites that holds Premier league data. The links below are site pages webscraped with beautiful soup and selenium, transformsed with PySpark and pandas and stored the data in the csv_dir folder
#### static site links = ['https://www.bbc.com/sport/football/premier-league/table','https://www.bbc.com/sport/football/premier-league/top-scorers','https://www.bbc.com/sport/football/premier-league/top-scorers/assists','https://www.worldfootball.net/goalgetter/eng-premier-league-2022-2023/','https://www.stadiumguide.com/premier-league-stadiums/','https://www.worldfootball.net/alltime_table/eng-premier-league/pl-only/','https://www.worldfootball.net/winner/eng-premier-league/','https://www.worldfootball.net/top_scorer/eng-premier-league/','https://www.worldfootball.net/schedule/eng-premier-league-2019-2020-spieltag/38/']

#### dynamic site links = ["https://www.premierleague.com/players","https://www.premierleague.com/clubs","https://www.premierleague.com/managers"]


## Database Model

![alt text](https://github.com/George-Michael-Dagogo/Premier_League_Stats/blob/main/database_schema.png)

## Metabase Dashboard
![alt text](https://github.com/George-Michael-Dagogo/Premier_League_Stats/blob/main/Michael%20%C2%B7%20Metabase%20dash.jpg)

## Power Bi Dashboard
![alt text](https://github.com/George-Michael-Dagogo/Premier_League_Stats/blob/main/premier_bi2.jpg)
