import scrapy


class GetSpider(scrapy.Spider):
    name = 'player_stats'
    start_urls = [
        'https://www.worldfootball.net/players_list/eng-premier-league-2022-2023/nach-name/1/'
    ]
    custom_settings = {
        #'DOWNLOAD_DELAY': 10 # 2 seconds of delay
        }

    def parse(self, response):
        for play in response.css('table.standard_tabelle'):
            yield {
                    'Player': play.css('td.standing-table__cell.standing-table__cell--name > a::text').getall(),
                    'Played': play.css('tr.standing-table__row > td.standing-table__cell::text').getall(),
                    }
         