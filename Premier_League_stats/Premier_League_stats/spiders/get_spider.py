import scrapy

class GetSpider(scrapy.Spider):
    name = 'player_stats'
    start_url = [
        'https://www.premierleague.com/stats/top/players/goals'
    ]