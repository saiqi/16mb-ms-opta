import datetime

from nameko.rpc import rpc, RpcProxy
from nameko.timer import timer
from bson.json_util import dumps
from nameko_mongodb.database import MongoDatabase

from application.dependencies.opta import OptaDependency


class OptaCollectorService(object):
    name = 'opta_collector'

    database = MongoDatabase()

    opta = OptaDependency()

    datastore = RpcProxy('datastore')

    @rpc
    def add_f1(self, season_id, competition_id):

        calendar = self.opta.get_calendar(season_id, competition_id)

        target_table = 'SOCCER_CALENDAR'
        meta = [("id", "VARCHAR(10)"), ("competition_id", "VARCHAR(5)"), ("season_id", "INTEGER"),
                ("date", "TIMESTAMP"), ("home_id", "VARCHAR(10)"), ("away_id", "VARCHAR(10)"),
                ("fingerprint", "VARCHAR(40)"), ]

        self._load(calendar, 'calendar', target_table, meta, 'id')

    @rpc
    def update_all_f9(self, season_id, competition_id):

        ids = self.database.calendar.find({'season_id': season_id, 'competition_id': competition_id})

        self._load_f9(ids)

    @rpc
    @timer(900)
    def update_f1_f9(self):

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
                self.database.calendar.update_one({'id': game['id']},
                                                  {'$set': game})

        now = datetime.datetime.utcnow()

        end_date = now + datetime.timedelta(minutes=120)

        start_date = now - datetime.timedelta(days=3)

        ids = self.database.calendar.find({'date': {'$gte': start_date, '$lt': end_date}}, {'id': 1})

        self._load_f9(ids)

    def _load(self, records, collection, target_table, meta, record_key, parent_key=None, parent_value=None,
              simple_update=True):

        self.database[collection].create_index(record_key)

        to_insert = []
        to_update = []

        if simple_update is False:
            prevs = self.database[collection].find({parent_key: parent_value}, {record_key: 1, '_id': 0})

            prev_ids = set(r[record_key] for r in prevs)
            ids = set(r[record_key] for r in records)

            del_ids = prev_ids.difference(ids)

            for r in del_ids:
                self.database[collection].delete_one({record_key: r})
                self.datastore.delete(target_table, {record_key: r})

        for rec in records:
            prev_game = self.database[collection].find_one({record_key: rec[record_key]}, {'fingerprint': 1, '_id': 0})

            if not prev_game:
                self.database[collection].update_one({record_key: rec[record_key]}, {'$set': rec}, upsert=True)
                to_insert.append(rec)
            else:
                if prev_game['fingerprint'] != rec['fingerprint']:
                    self.database[collection].update_one({record_key: rec[record_key]}, {'$set': rec})
                    to_update.append(rec)

        if len(to_insert) > 0:
            self.datastore.insert(target_table, dumps(to_insert), meta)

        if len(to_update) > 0:
            self.datastore.update(target_table, record_key, dumps(to_update))

    def _load_f9(self, game_ids):

        for row in game_ids:
            if self.opta.is_game_ready(row['id']):
                try:
                    game = self.opta.get_game(row['id'])
                except:
                    continue

                meta = [("id", "VARCHAR(10)"), ("name", "VARCHAR(50)"), ("fingerprint", "VARCHAR(40)")]
                self._load([game['season']], 'seasons', 'SOCCER_SEASON', meta, 'id')

                meta = [("id", "VARCHAR(10)"), ("code", "VARCHAR(10)"), ("name", "VARCHAR(50)"),
                        ("country", "VARCHAR(50)"), ("fingerprint", "VARCHAR(40)")]
                self._load([game['competition']], 'competitions', 'SOCCER_COMPETITION', meta, 'id')

                if 'venue' in game:
                    meta = [("id", "VARCHAR(10)"), ("name", "VARCHAR(50)"), ("country", "VARCHAR(50)"),
                            ("fingerprint", "VARCHAR(40)")]
                    self._load([game['venue']], 'venues', 'SOCCER_VENUE', meta, 'id')

                meta = [("id", "VARCHAR(10)"), ("first_name", "VARCHAR(50)"), ("last_name", "VARCHAR(50)"),
                        ("known", "VARCHAR(50)"), ("fingerprint", "VARCHAR(40)")]
                self._load(game['persons'], 'people', 'SOCCER_PERSON', meta, 'id')

                meta = [("id", "VARCHAR(10)"), ("name", "VARCHAR(50)"), ("country", "VARCHAR(50)"),
                        ("fingerprint", "VARCHAR(40)")]
                self._load(game['teams'], 'teams', 'SOCCER_TEAM', meta, 'id')

                meta = [("id", "VARCHAR(10)"), ("competition_id", "VARCHAR(10)"), ("season_id", "INTEGER"),
                        ("type", "VARCHAR(20)"), ("matchday", "INTEGER"), ("weather", "VARCHAR(20)"),
                        ("attendance", "INTEGER"), ("period", "VARCHAR(20)"), ("date", "TIMESTAMP"),
                        ("pool", "VARCHAR(10)"), ("round_name", "VARCHAR(50)"), ("round_number", "VARCHAR(10)"),
                        ("venue_id", "VARCHAR(10)"), ("match_official_id", "VARCHAR(10)"), ("winner_id", "VARCHAR(10)"),
                        ("fingerprint", "VARCHAR(40)")]
                self._load([game['match_info']], 'matchinfos', 'SOCCER_MATCHINFO', meta, 'id')

                meta = [("id", "VARCHAR(20)"), ("competition_id", "VARCHAR(10)"), ("season_id", "INTEGER"),
                        ("match_id", "VARCHAR(10)"), ("team_id", "VARCHAR(10)"), ("player_id", "VARCHAR(10)"),
                        ("type", "VARCHAR(50)"), ("minutes", "INTEGER"), ("seconds", "INTEGER"),
                        ("description", "VARCHAR(50)"), ("detail", "VARCHAR(50)"), ("fingerprint", "VARCHAR(40)")]
                self._load(game['events'], 'events', 'SOCCER_EVENT', meta, 'id', parent_key='match_id',
                           parent_value=game['match_info']['id'], simple_update=False)

                meta = [("id", "VARCHAR(250)"), ("competition_id", "VARCHAR(10)"), ("season_id", "INTEGER"),
                        ("match_id", "VARCHAR(10)"), ("team_id", "VARCHAR(10)"), ("score", "INTEGER"),
                        ("shootout_score", "INTEGER"), ("side", "VARCHAR(10)"), ("formation_used", "VARCHAR(11)"),
                        ("official_id", "VARCHAR(10)"), ("type", "VARCHAR(50)"), ("fh", "FLOAT"), ("sh", "FLOAT"),
                        ("efh", "FLOAT"), ("esh", "FLOAT"), ("value", "FLOAT"), ("fingerprint", "VARCHAR(40)")]
                self._load(game['team_stats'], 'teamstats', 'SOCCER_TEAMSTAT', meta, 'id', parent_key='match_id',
                           parent_value=game['match_info']['id'], simple_update=False)

                meta = [("id", "VARCHAR(250)"), ("player_id", "VARCHAR(10)"), ("competition_id", "VARCHAR(10)"),
                        ("season_id", "INTEGER"), ("match_id", "VARCHAR(10)"), ("team_id", "VARCHAR(10)"),
                        ("score", "INTEGER"), ("shootout_score", "INTEGER"), ("side", "VARCHAR(10)"),
                        ("formation_used", "VARCHAR(11)"), ("official_id", "VARCHAR(10)"),
                        ("main_position", "VARCHAR(20)"), ("sub_position", "VARCHAR(20)"), ("shirt_number", "INTEGER"),
                        ("status", "VARCHAR(20)"), ("captain", "VARCHAR(10)"), ("type", "VARCHAR(50)"),
                        ("value", "FLOAT"), ("formation_place", "VARCHAR(11)"), ("fingerprint", "VARCHAR(40)")]
                self._load(game['player_stats'], 'playerstats', 'SOCCER_PLAYERSTAT', meta, 'id', parent_key='match_id',
                           parent_value=game['match_info']['id'], simple_update=False)
