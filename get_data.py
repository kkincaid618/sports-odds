import requests
from pandas import DataFrame, to_datetime
from datetime import datetime
import json


class PullData(object):
    def __init__(self, api_key):
        # self.books = config_path['books']
        # self.markets = config_path['markets']
        # self.regions = config_path['regions']
        # self.api_key = config_path['api_key']
        # self.odds_format = config_path['odds_format']
        # self.sports = config_path['sports']
        self.books = 'barstool'
        self.markets = 'spreads'
        self.regions = 'us'
        self.odds_format = 'american'
        self.sports = 'americanfootball_ncaaf'
        self.api_key = api_key

    def _calculate_request(self):
        base_url = 'https://api.the-odds-api.com'
        api_key = '76596681042e0510c8311d07f25ca69c'
        endpoint = 'v4/sports'
        url = f'{base_url}/{endpoint}/{self.sports}/odds?regions={self.regions}&oddsFormat={self.odds_format}&markets={self.markets}&bookmakers={self.books}&apiKey={self.api_key}'

        self.url = url

    def _get_data(self):
        r = requests.get(self.url) # Add error handling in case of api failure
        raw = r.json()

        self.raw_data = raw

    def _parse_data(self):
        # Create empty array that will be populated with game & betting data
        all_data = []
        raw_data = self.raw_data
        num_games = len(raw_data)

        # Loop through each game to extract key data
        for g in range(num_games):
            game_id = raw_data[g]['id']
            game_time = raw_data[g]['commence_time']
            game_home = raw_data[g]['home_team']
            game_away = raw_data[g]['away_team']
            books = raw_data[g]['bookmakers']
            
            # Loop through each book to extract prices and points
            for b in books:
                book_name = b['title']
                book_update = b['last_update']

                markets = b['markets'][0]
                price = markets['outcomes'][0]['price']
                # spread = abs(markets['outcomes'][0]['point'])
                
                home_team = markets['outcomes'][0]
                away_team = markets['outcomes'][1]

                home_team_point = home_team['point']
                away_team_point = away_team['point']

                if home_team_point < away_team_point:
                    favored = home_team['name']
                else:
                    favored = away_team['name']

            row = [game_id, game_time, game_home, game_away, book_name, book_update, price, home_team_point, away_team_point, favored]
            all_data.append(row)

        columns = ['game_id','game_time','home_team','away_team','book_name','spread_updated_time','price','home_team_spread','away_team_spread','favored_team']
        df = DataFrame(all_data, columns = columns)
        df['game_time'] = to_datetime(df['game_time']).dt.tz_convert('US/Central')

        self.data = df

    def _calculate_update_ts(self):        
        now = datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")

        self.update_ts = now

    def _filter_data(self):
        self._calculate_update_ts()

        update_ts = self.update_ts
        df = self.data

        df['refresh_time'] = update_ts
        df_filtered = df[df['refresh_time'] >= df['game_time']]

        self.data = df_filtered

    def grab_data(self):
        self._calculate_request()
        
        self._get_data()

        self._parse_data()

        return self.data