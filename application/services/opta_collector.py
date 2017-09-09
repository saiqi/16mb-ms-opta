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
        stats = sorted(game['player_stats'], key=lambda k: (k['player_id'], k['type']))
        concat = ''.join(str(r['value']) for r in stats)
        return hashlib.md5(concat.encode('utf-8')).hexdigest()

    @staticmethod
    def _build_game_event_content(game):
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
    def _handle_referential_entity(entity, _type):
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
    def _extract_referential(game):
        events = list()
        entities = list()
        event_content = OptaCollectorService._build_game_event_content(game)
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
                        entity = OptaCollectorService._handle_referential_entity(r, k)
                        entities.append(entity)
                        event['entities'].append(entity)

                else:
                    entity = OptaCollectorService._handle_referential_entity(current_entity, k)
                    entities.append(entity)
                    event['entities'].append(entity)

        events.append(event)
        return {'entities': entities, 'events': events}

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

        game = self.opta.get_game(match_id)

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

            referential = self._extract_referential(game)
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
    def ack_f9(self, match_id, checksum):
        self.database.f9.update_one({'id': match_id}, {'$set': {'checksum': checksum}}, upsert=True)

    @rpc
    def unack_f9(self, match_id):
        self.database.f9.delete_one({'id': match_id})
