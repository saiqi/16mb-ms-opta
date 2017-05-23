import hashlib

from nameko.rpc import rpc
from nameko_mongodb.database import MongoDatabase
import bson.json_util
import dateutil.parser

from application.dependencies.opta import OptaDependency


class OptaCollectorService(object):
    name = 'opta_collector'

    database = MongoDatabase(result_backend=False)

    opta = OptaDependency()

    @staticmethod
    def _checksum(game):
        stats = sorted(game['player_stats'], key=lambda k: k['id'])
        concat = ''.join(r['fingerprint'] for r in stats)
        return hashlib.md5(concat.encode('utf-8')).hexdigest()

    @rpc
    def add_f1(self, season_id, competition_id):

        calendar = self.opta.get_calendar(season_id, competition_id)

        self.database['f1'].create_index('id')
        self.database['f1'].create_index('date')

        for row in calendar:
            self.database['f1'].update_one({'id': row['id']}, {'$set': row}, upsert=True)

    @rpc
    def update_all_f1(self):
        calendars = self.database.calendar.aggregate([
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
                                            {'$set': game})

    @rpc
    def get_ids_by_dates(self, start_date, end_date):
        start = dateutil.parser.parse(start_date)
        end = dateutil.parser.parse(end_date)
        ids = self.database.f1.find({'date': {'$gte': start, '$lt': end}}, {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    @rpc
    def get_ids_by_season_and_competition(self, season_id, competition_id):
        ids = self.database.f1.find({'season_id': season_id, 'competition_id': competition_id},
                                    {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    @rpc
    def get_f9(self, match_id):

        self.database.f9.create_index('id')

        if self.opta.is_game_ready(match_id):
            game = self.opta.get_game(match_id)
            checksum = self._checksum(game)

            old_checksum = self.database.f9.find_one({'id': match_id}, {'checksum': 1, '_id': 0})

            if old_checksum is None:
                return {'status': 'CREATED', 'payload': bson.json_util.dumps(game), 'checksum': checksum}
            else:
                if old_checksum['checksum'] != checksum:
                    return {'status': 'UPDATED', 'payload': bson.json_util.dumps(game), 'checksum': checksum}
                else:
                    return {'status': 'UNCHANGED', 'payload': bson.json_util.dumps(game), 'checksum': checksum}
        return None

    @rpc
    def ack_f9(self, match_id, checksum):
        self.database.f9.update_one({'id': match_id}, {'$set': {'checksum': checksum}}, upsert=True)

    @rpc
    def unack_f9(self, match_id):
        self.database.f9.delete_one({'id': match_id})
