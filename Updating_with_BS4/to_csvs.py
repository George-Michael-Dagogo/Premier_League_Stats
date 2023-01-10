from scrape import *

league_table = get_table()
league_table.to_csv('./csv_dir/league_table.csv',index = False)

top_scorers = get_top_scorers()
top_scorers.to_csv('./csv_dir/top_scorers.csv',index = False)

assists = get_assists()
assists.to_csv('./csv_dir/assists.csv',index = False)

detail_top_scorers = detail_top()
detail_top_scorers.to_csv('./csv_dir/detail_top_scorers.csv',index = False)

stadiums = stadiums()
stadiums.to_csv('./csv_dir/stadiums.csv',index = False)

player_table = player_table()
player_table.to_csv('./csv_dir/player_table.csv',index = False)






