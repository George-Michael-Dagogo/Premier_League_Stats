import scrapy

class GetSpider(scrapy.Spider):
    name = 'player_stats'
    start_urls = [
        'https://www.premierleague.com/players'
    ]
    custom_settings = {
        #'DOWNLOAD_DELAY': 10 # 2 seconds of delay
        }

    def parse(self, response):
        for play in response.css('tbody.dataContainer.indexSection > tr'):
            yield {
                    'player': play.css('a.playerName::text').get(),
                    'position': play.css('tr > td.hide-s::text').get(),
                    'player_country': play.css('tr > td.hide-s > span.playerCountry::text').getall(),
                    }
         