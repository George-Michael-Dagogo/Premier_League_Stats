import scrapy

class GetSpider(scrapy.Spider):
    name = 'player_stats'
    start_urls = [
        'https://www.premierleague.com/stats/top/players/goal_assist'
    ]
    custom_settings = {
        #'DOWNLOAD_DELAY': 5 # 2 seconds of delay
        }

    def parse(self, response):
        player_name = response.css('a.playerName > strong::text').extract()
        yield {'Player_name': player_name} 
        club_name = response.css('a.statNameSecondary::text').extract()
        yield {'Club_name': club_name} 
