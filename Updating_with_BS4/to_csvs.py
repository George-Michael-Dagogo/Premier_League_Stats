from extract_transform import *

league_table = get_table()
league_table.to_csv('./csv_dir/league_table.csv',index = False)

top_scorers = get_top_scorers()
top_scorers.to_csv('./csv_dir/top_scorers.csv',index = False)

assists = get_assists()
assists.to_csv('./csv_dir/assists.csv',index = False)

detail_top_scorers = detail_top()
detail_top_scorers.to_csv('./csv_dir/detail_top_scorers.csv', index = False)

#stadiums = stadiums()
#stadiums.to_csv('./csv_dir/stadiums.csv',index = False)

#player_table = player_table()
#player_table.to_csv('./csv_dir/player_table.csv',index = False)

all_time_table = all_time_table()
all_time_table.to_csv('./csv_dir/all_time_table.csv',index = False)

#all_time_winner_club = all_time_winner_club()
#all_time_winner_club.to_csv('./csv_dir/alltime_winners(clubs).csv',index = False)

top_scorers_seasons = top_scorers_seasons()
top_scorers_seasons.to_csv('./csv_dir/top_scorers(seasons).csv',index = False)

goals_per_season = goals_per_season()
goals_per_season.to_csv('./csv_dir/goals_per_season.csv',index = False)

#record_win = record_win()
#record_win.to_csv('./csv_dir/record_wins.csv',index = False)







