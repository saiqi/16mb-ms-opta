import datetime

from nameko.rpc import rpc
from nameko_mongodb.database import MongoDatabase
import bson.json_util

from application.dependencies.opta import OptaDependency


class OptaCollectorService(object):
    name = 'opta_collector'

    database = MongoDatabase(result_backend=False)

    opta = OptaDependency()

    @rpc
    def get_matchinfo(self, match_id):
        doc = self.database.matchinfos.find_one({'id': match_id}, {'_id': 0})
        return bson.json_util.dumps(doc)

    @rpc
    def get_events(self, match_id):
        docs = list(self.database.events.find({'match_id': match_id}, {'_id': 0}))
        return bson.json_util.dumps(docs)

    @rpc
    def get_teamstats(self, match_id):
        docs = list(self.database.teamstats.find({'match_id': match_id}, {'_id': 0}))
        return bson.json_util.dumps(docs)

    @rpc
    def get_playerstats(self, match_id):
        docs = list(self.database.playerstats.find({'match_id': match_id}, {'_id': 0}))
        return bson.json_util.dumps(docs)

    @rpc
    def add_f1(self, season_id, competition_id):

        calendar = self.opta.get_calendar(season_id, competition_id)

        self._load(calendar, 'calendar', 'id')

    @rpc
    def update_all_f9(self, season_id, competition_id):

        ids = self.database.calendar.find({'season_id': season_id, 'competition_id': competition_id},
                                          {'id': 1, '_id': 0})

        loaded_ids = self._load_f9(list(ids))

        return loaded_ids

    @rpc
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

        ids = self.database.calendar.find({'date': {'$gte': start_date, '$lt': end_date}}, {'id': 1, '_id': 0})

        loaded_ids = self._load_f9(list(ids))

        return loaded_ids

    def _get_first_match_date(self):
        cursor = self.database.matchinfos.aggregate([
            {'$group': {'_id': None, 'first_date': {'$min': '$date'}}},
            {'$project': {'first_date': 1}}
        ])

        for r in cursor:
            return r['first_date']

        return None

    def _get_first_calendar_date(self):
        cursor = self.database.calendar.aggregate([
            {'$group': {'_id': None, 'first_date': {'$min': '$date'}}},
            {'$project': {'first_date': 1}}
        ])

        for r in cursor:
            return r['first_date']

        return None

    def _load(self, records, collection, record_key, parent_key=None, parent_value=None, simple_update=True):

        has_changed = False

        self.database[collection].create_index(record_key)

        if parent_key is not None:
            self.database[collection].create_index(parent_key)

        to_insert = []
        to_update = []

        if simple_update is False:
            prevs = self.database[collection].find({parent_key: parent_value}, {record_key: 1, '_id': 0})

            prev_ids = set(r[record_key] for r in prevs)
            ids = set(r[record_key] for r in records)

            del_ids = prev_ids.difference(ids)

            if len(del_ids) > 0:
                has_changed = True
                for r in del_ids:
                    self.database[collection].delete_one({record_key: r})

        for rec in records:
            prev_game = self.database[collection].find_one({record_key: rec[record_key]}, {'fingerprint': 1, '_id': 0})

            if not prev_game:
                to_insert.append(rec)
            else:
                if prev_game['fingerprint'] != rec['fingerprint']:
                    to_update.append(rec)

        if len(to_insert) > 0:
            has_changed = True
            for rec in to_insert:
                self.database[collection].update_one({record_key: rec[record_key]}, {'$set': rec}, upsert=True)

        if len(to_update) > 0:
            has_changed = True
            for rec in to_update:
                self.database[collection].update_one({record_key: rec[record_key]}, {'$set': rec})

        return has_changed

    def _load_f9(self, game_ids):

        ids = list()

        for row in game_ids:
            if self.opta.is_game_ready(row['id']):
                try:
                    game = self.opta.get_game(row['id'])
                except:
                    continue

                self._load([game['season']], 'seasons', 'id')

                self._load([game['competition']], 'competitions', 'id')

                if 'venue' in game:
                    self._load([game['venue']], 'venues', 'id')

                self._load(game['persons'], 'people', 'id')

                self._load(game['teams'], 'teams', 'id')

                info_changed = self._load([game['match_info']], 'matchinfos', 'id')

                events_changed = self._load(game['events'], 'events', 'id', parent_key='match_id',
                                            parent_value=game['match_info']['id'],
                                            simple_update=False)

                teamstats_changed = self._load(game['team_stats'], 'teamstats', 'id', parent_key='match_id',
                                               parent_value=game['match_info']['id'], simple_update=False)

                playerstats_changed = self._load(game['player_stats'], 'playerstats', 'id', parent_key='match_id',
                                                 parent_value=game['match_info']['id'], simple_update=False)

                if info_changed or events_changed or teamstats_changed or playerstats_changed:
                    ids.append(row['id'])

        return ids
