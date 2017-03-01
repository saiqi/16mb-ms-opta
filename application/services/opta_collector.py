import datetime

from nameko.rpc import rpc
from nameko.timer import timer
from pymongo import IndexModel, ASCENDING, DESCENDING
from nameko_mongodb.database import MongoDatabase

from application.dependencies.opta import OptaDependency


class OptaCollectorService(object):
    name = 'opta_collector'
    
    database = MongoDatabase()
    
    opta = OptaDependency()
    
    def create_f1_indexes(self):
        indexes = [IndexModel([('id', ASCENDING,)])]

        self.database.f1.create_indexes(indexes)
        
    def create_f9_indexes(self):
        indexes = [IndexModel([('match_info.id', ASCENDING,)])]
        
        self.database.f9.create_indexes(indexes)
    
    @rpc
    def add_f1(self, season_id, competition_id):
        
        self.create_f1_indexes()
        
        calendar = self.opta.get_calendar(season_id, competition_id)
        
        self.database.f1.insert_many(calendar)
        
    @rpc
    @timer(900)
    def update_f1_f9(self):
    
        self.create_f9_indexes()

        now = datetime.datetime.utcnow()

        end_date = now + datetime.timedelta(minutes=120)

        start_date = now - datetime.timedelta(days=3)
        
        ids = self.database.f1.find({'date': {'$gte': start_date, '$lt': end_date}}, {'id': 1})

        for row in ids:
            game = self.opta.get_game(row['id'])

            self.database.f9.update_one(
                {'match_info.id': game['match_info']['id']},
                {'$set': {
                    'season': game['season'],
                    'competition': game['competition'],
                    'venue': game['venue'],
                    'teams': game['teams'],
                    'persons': game['persons'],
                    'match_info': game['match_info'],
                    'bookings': game['bookings'],
                    'goals': game['goals'],
                    'missed_penalties': game['missed_penalties'],
                    'substitutions': game['substitutions'],
                    'team_stats': game['team_stats'],
                    'player_stats': game['team_stats']
                }}, upsert=True)
