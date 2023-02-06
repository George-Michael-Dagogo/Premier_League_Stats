
# Premier League Stats

## Aim
### The aim of this project is to get premier league related data from several sources for analysis, predictions and visualizations. Also to build a dashboard that can be hosted on a webpage that automatically updates whenever the database is updated.

## Workflow
### Data is extracted from several football websites and stored in two locations, this repository and a cloud postgres database and this databases are updated every weekend.

## Details
### The ./Updating_with_BS4/extract_transform.py script web-scrapes data from football websites related to the Premier League , transforms it with PySpark and pandas and stores the data in the csv_dir folder
#### 



![alt text](https://github.com/George-Michael-Dagogo/Premier_League_Stats/blob/main/database_schema.png)


