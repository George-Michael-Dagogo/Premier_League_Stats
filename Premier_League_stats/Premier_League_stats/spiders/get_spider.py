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
                    'Player': play.css('td.hell > a::text').getall(),
                    'Played': play.xpath('//*[@id="site"]/div[2]/div[1]/div[1]/div[5]/div/table/tbody/tr[2]/td[3]/a[4]/text').getall(),
                    }
    

#//*[@id="site"]/div[2]/div[1]/div[1]/div[5]/div/table/tbody/tr[2]/td[3]


