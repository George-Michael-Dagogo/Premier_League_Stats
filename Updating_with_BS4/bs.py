import requests
from bs4 import BeautifulSoup
import pandas as pd
url = 'https://www.bbc.com/sport/football/premier-league/table'
headers = []
page = requests.get(url)
soup = BeautifulSoup(page.text,  "html.parser")
table= soup.find("table", class_="ssrcss-14j0ip6-Table e3bga5w4")

for i in table.find_all('th'):
    title = i.text
    headers.append(title)
df = pd.DataFrame(columns = headers)
for j in table.find_all('tr')[1:]:
    row_data = j.find_all('td')
    row = [i.text for i in row_data]
    length = len(df)
    df.loc[length] = row

print(df)