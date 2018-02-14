import hashlib

from nameko.rpc import rpc
from nameko_mongodb.database import MongoDatabase
import bson.json_util
import dateutil.parser

from application.dependencies.opta import OptaDependency, OptaWebServiceError


class OptaCollectorService(object):
    name = 'opta_collector'

    database = MongoDatabase(result_backend=False)

    opta = OptaDependency()

    @staticmethod
    def _checksum(game):
        stats = sorted(game['player_stats'], key=lambda k: (k['player_id'], k['type']))
        concat = ''.join(str(r['value']) for r in stats)
        return hashlib.md5(concat.encode('utf-8')).hexdigest()

    @staticmethod
    def _build_soccer_game_event_content(game):
        team_sides = list(set([(r['team_id'], r['side']) for r in game['team_stats']]))

        home_team_id = [r[0] for r in team_sides if r[1] == 'Home'][0]
        away_team_id = [r[0] for r in team_sides if r[1] == 'Away'][0]

        home_team = [r['name'] for r in game['teams'] if r['id'] == home_team_id][0]
        away_team = [r['name'] for r in game['teams'] if r['id'] == away_team_id][0]

        return {
            'venue': game['venue']['name'],
            'country': game['venue']['country'],
            'competition': game['competition']['name'],
            'season': game['season']['name'],
            'name': '{} - {}'.format(home_team, away_team)
        }

    @staticmethod
    def _handle_referential_soccer_entity(entity, _type):
        entity_formatted = {'id': entity['id'], 'provider': 'opta_f9', 'informations': entity}

        if _type in ('competition', 'venue', 'season', 'teams'):
            entity_formatted['common_name'] = entity['name']

            if _type in ('competition', 'venue', 'season'):
                entity_formatted['type'] = ' '.join(['soccer', _type])
            else:
                entity_formatted['type'] = 'soccer team'

        else:
            common_name = ' '.join([entity['first_name'], entity['last_name']])
            if entity['known'] is not None:
                common_name = entity['known']
            entity_formatted['common_name'] = common_name
            entity_formatted['type'] = ' '.join(['soccer', entity['type']])

        return entity_formatted

    @staticmethod
    def _extract_referential_from_soccer_game(game):
        events = list()
        entities = list()
        event_content = OptaCollectorService._build_soccer_game_event_content(game)
        event = {
            'id': game['match_info']['id'],
            'date': game['match_info']['date'].isoformat(),
            'provider': 'opta_f9',
            'type': 'game',
            'common_name': event_content['name'],
            'content': event_content,
            'entities': []
        }

        for k in ('competition', 'season', 'venue', 'persons', 'teams'):
            if k in game:
                current_entity = game[k]

                if isinstance(current_entity, list):
                    for r in current_entity:
                        entity = OptaCollectorService._handle_referential_soccer_entity(r, k)
                        entities.append(entity)
                        event['entities'].append(entity)

                else:
                    entity = OptaCollectorService._handle_referential_soccer_entity(current_entity, k)
                    entities.append(entity)
                    event['entities'].append(entity)

        events.append(event)
        return {'entities': entities, 'events': events}

    @staticmethod
    def _extract_referential_from_rugby_game(ru1, ru7):
        events = list()
        entities = list()

        competition = {
            'id': ru1['competition_id'],
            'common_name': ru1['competition_name'],
            'provider': 'opta_ru7',
            'type': 'rugby competition',
            'informations': None
        }

        venue = {
            'id': ru1['venue_id'],
            'common_name': ru1['venue'],
            'provider': 'opta_ru7',
            'type': 'rugby venue',
            'informations': None
        }

        entities.append(venue)
        entities.append(competition)

        event = {
            'id': ru1['id'],
            'date': ru1['date'].isoformat(),
            'common_name': '{} - {}'.format(ru1['home_name'], ru1['away_name']),
            'type': 'game',
            'provider': 'opta_ru7',
            'content': {
                'competition': ru1['competition_name'],
                'name': '{} - {}'.format(ru1['home_name'], ru1['away_name']),
                'venue': ru1['venue']
            },
            'entities': [venue, competition]
        }

        for t in ru7['teams']:
            entity = {
                'id': t['id'],
                'common_name': t['name'],
                'provider': 'opta_ru7',
                'type': 'rugby team',
                'informations': None
            }
            event['entities'].append(entity)
            entities.append(entity)

        for t in ru7['players']:
            entity = {
                'id': t['id'],
                'common_name': t['name'],
                'provider': 'opta_ru7',
                'type': 'rugby player',
                'informations': None
            }
            event['entities'].append(entity)
            entities.append(entity)

        events.append(event)

        return {'entities': entities, 'events': events}

    @rpc
    def add_f1(self, season_id, competition_id):

        calendar = self.opta.get_soccer_calendar(season_id, competition_id)

        self.database['f1'].create_index('id')
        self.database['f1'].create_index('date')

        for row in calendar:
            self.database['f1'].update_one({'id': row['id']}, {'$set': row}, upsert=True)

    @rpc
    def add_ru1(self, season_id, competition_id):
        calendar = self.opta.get_rugby_calendar(season_id, competition_id)

        self.database['ru1'].create_index('id')
        self.database['ru1'].create_index('date')

        for row in calendar:
            self.database['ru1'].update_one({'id': row['id']}, {'$set': row}, upsert=True)

    @rpc
    def update_all_f1(self):
        calendars = self.database.f1.aggregate([
            {
                "$group": {
                    "_id": {"season_id": "$season_id", "competition_id": "$competition_id"},
                }
            }
        ])

        for row in calendars:
            try:
                calendar = self.opta.get_soccer_calendar(row['_id']['season_id'], row['_id']['competition_id'])
            except OptaWebServiceError:
                continue

            for game in calendar:
                self.database.f1.update_one({'id': game['id']},
                                            {'$set': game}, upsert=True)

    @rpc
    def update_all_ru1(self):
        calendars = self.database.ru1.aggregate([
            {
                "$group": {
                    "_id": {"season_id": "$season_id", "competition_id": "$competition_id"},
                }
            }
        ])

        for row in calendars:
            try:
                calendar = self.opta.get_rugby_calendar(row['_id']['season_id'], row['_id']['competition_id'])
            except OptaWebServiceError:
                continue

            for game in calendar:
                self.database.ru1.update_one({'id': game['id']},
                                             {'$set': game}, upsert=True)

    @rpc
    def get_soccer_ids_by_dates(self, start_date, end_date):
        start = dateutil.parser.parse(start_date)
        end = dateutil.parser.parse(end_date)
        ids = self.database.f1.find({'date': {'$gte': start, '$lt': end}}, {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    @rpc
    def get_soccer_ids_by_season_and_competition(self, season_id, competition_id):
        ids = self.database.f1.find({'season_id': season_id, 'competition_id': competition_id},
                                    {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    @rpc
    def get_rugby_ids_by_dates(self, start_date, end_date):
        start = dateutil.parser.parse(start_date)
        end = dateutil.parser.parse(end_date)
        ids = self.database.ru1.find({'date': {'$gte': start, '$lt': end}}, {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    @rpc
    def get_rugby_ids_by_season_and_competition(self, season_id, competition_id):
        ids = self.database.ru1.find({'season_id': season_id, 'competition_id': competition_id},
                                     {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    @rpc
    def get_f9(self, match_id):

        self.database.f9.create_index('id')

        game = self.opta.get_soccer_game(match_id)

        if game:
            checksum = self._checksum(game)

            old_checksum = self.database.f9.find_one({'id': match_id}, {'checksum': 1, '_id': 0})

            if old_checksum is None:
                status = 'CREATED'
            else:
                if old_checksum['checksum'] != checksum:
                    status = 'UPDATED'
                else:
                    status = 'UNCHANGED'

            referential = self._extract_referential_from_soccer_game(game)
            datastore = [
                {
                    'type': 'playerstat',
                    'records': game['player_stats']
                },
                {
                    'type': 'teamstat',
                    'records': game['team_stats']
                },
                {
                    'type': 'event',
                    'records': game['events']
                },
                {
                    'type': 'matchinfo',
                    'records': [game['match_info']]
                }
            ]

            return bson.json_util.dumps({
                'status': status,
                'checksum': checksum,
                'referential': referential,
                'datastore': datastore
            })

        return None

    @rpc
    def get_ru7(self, match_id):
        self.database.ru7.create_index('id')

        game = self.opta.get_rugby_game(match_id)

        if game:
            checksum = self._checksum(game)

            old_checksum = self.database.ru7.find_one({'id': match_id}, {'checksum': 1, '_id': 0})

            if old_checksum is None:
                status = 'CREATED'
            else:
                if old_checksum['checksum'] != checksum:
                    status = 'UPDATED'
                else:
                    status = 'UNCHANGED'

            ru1 = self.database.ru1.find_one({'id': match_id}, {'_id': 0})

            datastore = [
                {
                    'type': 'playerstat',
                    'records': game['player_stats']
                },
                {
                    'type': 'teamstat',
                    'records': game['team_stats']
                },
                {
                    'type': 'event',
                    'records': game['events']
                },
                {
                    'type': 'matchscore',
                    'records': [{
                        'id': game['rrml']['id'],
                        'attendance': game['rrml']['attendance'],
                        'away_ht_score': game['rrml']['away_ht_score'],
                        'away_score': game['rrml']['away_score'],
                        'home_ht_score': game['rrml']['home_ht_score'],
                        'home_score': game['rrml']['home_score']
                    }]
                },
                {
                    'type': 'matchinfo',
                    'records': [
                        {
                            'venue_id': ru1['venue_id'],
                            'date': ru1['date'],
                            'season_id': ru1['season_id'],
                            'id': ru1['id'],
                            'group_name': ru1['group_name'],
                            'group_id': ru1['group_id'],
                            'round': ru1['round'],
                            'competition_id': ru1['competition_id']
                        }
                    ]
                }
            ]

            referential = self._extract_referential_from_rugby_game(ru1=ru1, ru7=game)

            return bson.json_util.dumps({
                'status': status,
                'checksum': checksum,
                'datastore': datastore,
                'referential': referential
            })

        return None

    @rpc
    def ack_f9(self, match_id, checksum):
        self.database.f9.update_one({'id': match_id}, {'$set': {'checksum': checksum}}, upsert=True)

    @rpc
    def unack_f9(self, match_id):
        self.database.f9.delete_one({'id': match_id})

    @rpc
    def ack_ru7(self, match_id, checksum):
        self.database.ru7.update_one({'id': match_id}, {'$set': {'checksum': checksum}}, upsert=True)

    @rpc
    def unack_ru7(self, match_id):
        self.database.ru7.delete_one({'id': match_id})
