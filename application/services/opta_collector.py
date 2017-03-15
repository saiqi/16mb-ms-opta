import datetime

from nameko.rpc import rpc, RpcProxy
from nameko.timer import timer
from pymongo import IndexModel, ASCENDING
from nameko_mongodb.database import MongoDatabase

from application.dependencies.opta import OptaDependency


class OptaCollectorService(object):
    name = 'opta_collector'

    database = MongoDatabase()

    opta = OptaDependency()

    datastore = RpcProxy('datastore')

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
    def update_all_f9(self, season_id, competition_id):

        self.create_f9_indexes()

        ids = self.database.f1.find({'season_id': season_id, 'competition_id': competition_id})

        self._load_f9(ids)

    @rpc
    @timer(900)
    def update_f1_f9(self):

        calendars = self.database.f1.aggregate([
            {
                "$group": {
                    "_id": {"season_id": "$season_id", "competition_id": "$competition_id"},
                }
            }
        ])

        for row in calendars:
            calendar = self.opta.get_calendar(row['_id']['season_id'], row['_id']['competition_id'])

            for game in calendar:
                self.database.f1.update_one({'id': game['id']},
                                            {'$set': {
                                                'date': game['date'],
                                                'home_id': game['home_id'],
                                                'away_id': game['away_id']
                                            }})

        self.create_f9_indexes()

        now = datetime.datetime.utcnow()

        end_date = now + datetime.timedelta(minutes=120)

        start_date = now - datetime.timedelta(days=3)

        ids = self.database.f1.find({'date': {'$gte': start_date, '$lt': end_date}}, {'id': 1})

        self._load_f9(ids)

    def _load_f9(self, cursor):

        for row in cursor:
            if self.opta.is_game_ready(row['id']):
                try:
                    game = self.opta.get_game(row['id'])
                except Exception:
                    continue

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
                        'player_stats': game['player_stats']
                    }}, upsert=True)
