
# Premier League Stats

## Aim
### The aim of this project is to get premier league related data from several sources for analysis, predictions and visualizations. Also to build a dashboard that can be hosted on a webpage that automatically updates whenever the database is updated.

## Workflow
### Data is extracted from several football websites and stored in two locations, this repository and a cloud postgres database and this databases are updated every weekend.

## Details
### The ./Updating_with_BS4/extract_transform.py script web-scrapes data from football websites
#### ['https://www.bbc.com/sport/football/premier-league/table','https://www.bbc.com/sport/football/premier-league/top-scorers','https://www.bbc.com/sport/football/premier-league/top-scorers/assists','https://www.worldfootball.net/goalgetter/eng-premier-league-2022-2023/','https://www.stadiumguide.com/premier-league-stadiums/','https://www.worldfootball.net/alltime_table/eng-premier-league/pl-only/','https://www.worldfootball.net/winner/eng-premier-league/','https://www.worldfootball.net/top_scorer/eng-premier-league/','https://www.worldfootball.net/schedule/eng-premier-league-2019-2020-spieltag/38/']
related to the Premier League , transforms it with PySpark and pandas and stores the data in the csv_dir folder
#### 



![alt text](https://github.com/George-Michael-Dagogo/Premier_League_Stats/blob/main/database_schema.png)


