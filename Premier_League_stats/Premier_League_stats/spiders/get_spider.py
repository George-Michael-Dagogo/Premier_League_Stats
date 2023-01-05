import scrapy


class GetSpider(scrapy.Spider):
    name = 'player_stats'
    start_urls = [
        'https://www.skysports.com/premier-league-table'
    ]
    custom_settings = {
        #'DOWNLOAD_DELAY': 10 # 2 seconds of delay
        }

    def parse(self, response):
        for play in response.css('table.standing-table__table'):
            yield {
                    'Club': play.css('td.standing-table__cell.standing-table__cell--name > a::text').getall(),
                    'Played': play.css('tr.standing-table__row > td.standing-table__cell::text').getall(),
                    }
         