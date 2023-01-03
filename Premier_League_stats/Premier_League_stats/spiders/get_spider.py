import scrapy
import json 

class GetSpider(scrapy.Spider):
    name = 'player_stats'
    #allowed_domains = ['https://www.premierleague.com/players']
    page = 1
    #api_url = 'https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons=489&altIds=true&page=22&type=player&id=-1&compSeasonId=489'
    start_urls = ['https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons=489&altIds=true&page=1&type=player&id=-1&compSeasonId=489']

    custom_settings = {
        #'DOWNLOAD_DELAY': 5
        }

    def parse(self, response):
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}

        data = json.loads(response.text)
        for quote in data["quotes"]:
            yield {"content": quote["text"]}
        if data["has_next"]:
                self.page += 1
                url = f"https://footballapi.pulselive.com/football/players?pageSize=30&compSeasons=489&altIds=true&page={self.page}&type=player&id=-1&compSeasonId=489"
                yield scrapy.Request(url=url, callback=self.parse, headers=headers)
    

    