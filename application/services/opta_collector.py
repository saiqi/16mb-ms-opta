import hashlib
import logging
import datetime
import itertools

from nameko.rpc import rpc
from nameko.timer import timer
from nameko.events import event_handler, BROADCAST
from nameko_mongodb.database import MongoDatabase
from nameko.messaging import Publisher
from nameko.constants import PERSISTENT
from kombu.messaging import Exchange
from nameko.dependency_providers import DependencyProvider
import bson.json_util
import dateutil.parser

from application.dependencies.opta import OptaDependency, OptaWebServiceError
from application.services.meta import OPTA, LABEL


_log = logging.getLogger(__name__)


class ErrorHandler(DependencyProvider):

    def worker_result(self, worker_ctx, res, exc_info):
        if exc_info is None:
            return

        exc_type, exc, tb = exc_info
        _log.error(str(exc))


class OptaCollectorService(object):
    name = 'opta_collector'

    database = MongoDatabase(result_backend=False)

    opta = OptaDependency()

    error = ErrorHandler()

    pub_input = Publisher(exchange=Exchange(
        name='all_inputs', type='topic', durable=True, auto_delete=True, delivery_mode=PERSISTENT))
    pub_notif = Publisher(exchange=Exchange(
        name='all_notifications', type='topic', durable=True, auto_delete=True, delivery_mode=PERSISTENT))

    @staticmethod
    def _checksum(game):
        stats = sorted(game['player_stats'],
                       key=lambda k: (k['player_id'], k['type']))
        concat = ''.join(str(r['value']) for r in stats)
        return hashlib.md5(concat.encode('utf-8')).hexdigest()

    @staticmethod
    def _build_soccer_game_event_content(game):
        team_sides = list(set([(r['team_id'], r['side'])
                               for r in game['team_stats']]))

        home_team_id = [r[0] for r in team_sides if r[1] == 'Home'][0]
        away_team_id = [r[0] for r in team_sides if r[1] == 'Away'][0]

        home_team = [r['name']
                     for r in game['teams'] if r['id'] == home_team_id][0]
        away_team = [r['name']
                     for r in game['teams'] if r['id'] == away_team_id][0]

        return {
            'venue': game['venue']['name'],
            'country': game['venue']['country'],
            'competition': game['competition']['name'],
            'season': game['season']['name'],
            'name': '{} - {}'.format(home_team, away_team)
        }

    @staticmethod
    def _handle_referential_soccer_entity(entity, _type):
        entity_formatted = {
            'id': entity['id'], 'provider': 'opta_f9'}

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
        labels = list()
        event_content = OptaCollectorService._build_soccer_game_event_content(
            game)
        event = {
            'id': game['match_info']['id'],
            'date': game['match_info']['date'].isoformat(),
            'provider': 'opta_f9',
            'type': 'game',
            'common_name': event_content['name'],
            'content': event_content,
            'entities': []
        }

        labels.append({'id': event['id'], 'label': event['common_name']})

        for k in ('competition', 'season', 'venue', 'persons', 'teams'):
            if k in game:
                current_entity = game[k]
                if isinstance(current_entity, list):
                    for r in current_entity:
                        entity = OptaCollectorService._handle_referential_soccer_entity(
                            r, k)
                        entities.append(entity)
                        event['entities'].append(entity)
                        labels.append(
                            {'id': entity['id'], 'label': entity['common_name']})

                else:
                    entity = OptaCollectorService._handle_referential_soccer_entity(
                        current_entity, k)
                    entities.append(entity)
                    event['entities'].append(entity)
                    labels.append(
                        {'id': entity['id'], 'label': entity['common_name']})

        events.append(event)
        return {'entities': entities, 'events': events, 'labels': labels}

    @staticmethod
    def _extract_referential_from_rugby_game(ru1, ru7):
        events = list()
        entities = list()
        labels = list()

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
        labels.append(
            {'id': competition['id'], 'label': competition['common_name']})
        labels.append({'id': venue['id'], 'label': venue['common_name']})

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

        labels.append({'id': event['id'], 'label': event['common_name']})

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
            labels.append({'id': entity['id'], 'label': entity['common_name']})

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
            labels.append({'id': entity['id'], 'label': entity['common_name']})

        events.append(event)

        return {'entities': entities, 'events': events, 'labels': labels}

    @staticmethod
    def _get_opta_meta(opta_type, data_type, game_id):
        meta = OPTA[opta_type][data_type].copy()
        if 'delete_keys' in meta:
            meta['delete_keys'] = {
                'match_id': f'f{game_id}' if opta_type == 'f9' else game_id
            }
        return meta

    @rpc
    def add_f1(self, season_id, competition_id):

        calendar = self.opta.get_soccer_calendar(season_id, competition_id)

        self.database['f1'].create_index('id')
        self.database['f1'].create_index('date')

        for row in calendar:
            self.database['f1'].update_one(
                {'id': row['id']}, {'$set': row}, upsert=True)

    @rpc
    def add_ru1(self, season_id, competition_id):
        calendar = self.opta.get_rugby_calendar(season_id, competition_id)

        self.database['ru1'].create_index('id')
        self.database['ru1'].create_index('date')

        for row in calendar:
            self.database['ru1'].update_one(
                {'id': row['id']}, {'$set': row}, upsert=True)

    @timer(interval=24*60*60)
    @rpc
    def update_all_f1(self):
        _log.info('Updating all f1 files ...')
        calendars = self.database.f1.aggregate([
            {
                "$group": {
                    "_id": {"season_id": "$season_id", "competition_id": "$competition_id"},
                }
            }
        ])

        for row in calendars:
            try:
                calendar = self.opta.get_soccer_calendar(
                    row['_id']['season_id'], row['_id']['competition_id'])
            except OptaWebServiceError:
                continue

            for game in calendar:
                self.database.f1.update_one({'id': game['id']},
                                            {'$set': game}, upsert=True)

    @timer(interval=24*60*60)
    @rpc
    def update_all_ru1(self):
        _log.info('Updating all RU1 files ...')
        calendars = self.database.ru1.aggregate([
            {
                "$group": {
                    "_id": {"season_id": "$season_id", "competition_id": "$competition_id"},
                }
            }
        ])

        for row in calendars:
            try:
                calendar = self.opta.get_rugby_calendar(
                    row['_id']['season_id'], row['_id']['competition_id'])
            except OptaWebServiceError:
                continue

            for game in calendar:
                self.database.ru1.update_one({'id': game['id']},
                                             {'$set': game}, upsert=True)

    def get_soccer_ids_by_dates(self, start_date, end_date):
        start = dateutil.parser.parse(start_date)
        end = dateutil.parser.parse(end_date)
        ids = self.database.f1.find(
            {'date': {'$gte': start, '$lt': end}},
            {'id': 1, 'competition_id': 1, 'season_id': 1, '_id': 0})

        return list(ids)

    def get_soccer_ids_by_season_and_competition(self, season_id, competition_id):
        ids = self.database.f1.find({'season_id': season_id, 'competition_id': competition_id},
                                    {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    def get_f1(self, game_id):
        game = self.database.f1.find_one({'id': game_id}, {'_id': 0})
        return game

    def get_rugby_ids_by_dates(self, start_date, end_date):
        start = dateutil.parser.parse(start_date)
        end = dateutil.parser.parse(end_date)
        ids = self.database.ru1.find(
            {'date': {'$gte': start, '$lt': end}},
            {'id': 1, 'competition_id': 1, 'season_id': 1, '_id': 0})

        return list(ids)

    def get_rugby_ids_by_season_and_competition(self, season_id, competition_id):
        ids = self.database.ru1.find({'season_id': season_id, 'competition_id': competition_id},
                                     {'id': 1, '_id': 0})

        return [r['id'] for r in ids]

    def get_ru1(self, game_id):
        game = self.database.ru1.find_one({'id': game_id}, {'_id': 0})
        return game

    def get_f9(self, match_id):

        self.database.f9.create_index('id')

        game = self.opta.get_soccer_game(match_id)

        if not game:
            return None

        checksum = self._checksum(game)

        old_checksum = self.database.f9.find_one(
            {'id': match_id}, {'checksum': 1, '_id': 0})

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
                **self._get_opta_meta('f9', 'playerstat', match_id),
                'records': game['player_stats']
            },
            {
                **self._get_opta_meta('f9', 'teamstat', match_id),
                'records': game['team_stats']
            },
            {
                **self._get_opta_meta('f9', 'event', match_id),
                'records': game['events']
            },
            {
                **self._get_opta_meta('f9', 'matchinfo', match_id),
                'records': [game['match_info']]
            },
            {
                **LABEL,
                'records': referential['labels']
            }
        ]

        return {
            'id': match_id,
            'status': status,
            'checksum': checksum,
            'referential': {k: referential[k] for k in ('entities', 'events') if k in referential},
            'datastore': datastore,
            'meta': {'type': 'f9', 'source': 'opta', 'content_id': f'f{match_id}'}
        }

    def get_ru7(self, match_id):
        self.database.ru7.create_index('id')

        game = self.opta.get_rugby_game(match_id)

        if game:
            checksum = self._checksum(game)

            old_checksum = self.database.ru7.find_one(
                {'id': match_id}, {'checksum': 1, '_id': 0})

            if old_checksum is None:
                status = 'CREATED'
            else:
                if old_checksum['checksum'] != checksum:
                    status = 'UPDATED'
                else:
                    status = 'UNCHANGED'

            ru1 = self.database.ru1.find_one({'id': match_id}, {'_id': 0})

            referential = self._extract_referential_from_rugby_game(
                ru1=ru1, ru7=game)

            datastore = [
                {
                    **self._get_opta_meta('ru7', 'playerstat', match_id),
                    'records': game['player_stats']
                },
                {
                    **self._get_opta_meta('ru7', 'teamstat', match_id),
                    'records': game['team_stats']
                },
                {
                    **self._get_opta_meta('ru7', 'event', match_id),
                    'records': game['events']
                },
                {
                    **self._get_opta_meta('ru7', 'matchscore', match_id),
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
                    **self._get_opta_meta('ru7', 'matchinfo', match_id),
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
                },
                {
                    **LABEL,
                    'records': referential['labels']
                }
            ]

            return {
                'id': match_id,
                'status': status,
                'checksum': checksum,
                'datastore': datastore,
                'referential': {k: referential[k] for k in ('entities', 'events') if k in referential},
                'meta': {'type': 'ru7', 'source': 'opta'}
            }

        return None

    def get_f40(self, season_id, competition_id):
        squads = self.opta.get_soccer_squads(season_id, competition_id)

        if not squads:
            return None

        meta = OPTA['f40']

        def get_fields(k):
            return set((m[0] for m in meta[k]['meta']))

        datastore = [
            {
                **meta['playerinfo'],
                'records': [{k: v for k, v in p.items() if k in get_fields('playerinfo')}
                for t in squads for p in t['players']]
            },
            {
                **meta['teaminfo'],
                'records': [{k: v for k,v in t.items() if k in get_fields('teaminfo')}
                for t in squads]
            },
            {
                **meta['link'],
                'records': [{
                    'id': hashlib.md5(
                        ''.join([p['id'], t['id'], t['season_id'], t['competition_id']])\
                            .encode('utf-8')).hexdigest(),
                    'competition_id': t['competition_id'],
                    'season_id': t['season_id'],
                    'player_id': p['id'],
                    'team_id': t['id'],
                    'join_date': p['join_date']
                } for t in squads for p in t['players']]
            }
        ]

        content_id = ','.join([season_id, competition_id])
        return {
            'id': content_id,
            'status': 'UPDATED',
            'checksum': None,
            'referential': {
                'informations': list(itertools.chain(
                    datastore[0]['records'],
                    [{k: v for k,v in t.items() if k in get_fields('teaminfo').union({'team_kits'})}
                    for t in squads]))
            },
            'datastore': datastore,
            'meta': {'type': 'f40', 'source': 'opta', 'content_id': content_id}
        }
        
    def ack_f9(self, match_id, checksum):
        self.database.f9.update_one(
            {'id': match_id}, {'$set': {'checksum': checksum}}, upsert=True)

    @rpc
    def unack_f9(self, match_id):
        self.database.f9.delete_one({'id': match_id})

    def ack_ru7(self, match_id, checksum):
        self.database.ru7.update_one(
            {'id': match_id}, {'$set': {'checksum': checksum}}, upsert=True)

    @rpc
    def unack_ru7(self, match_id):
        self.database.ru7.delete_one({'id': match_id})

    @timer(interval=5*60)
    @rpc
    def publish(self, days_offset=3):
        _log.info(f'Loading opta games for the last {days_offset} days ...')
        now = datetime.datetime.utcnow()
        start = now - datetime.timedelta(days=days_offset)
        end = now + datetime.timedelta(seconds=60*110)

        _log.info(
            f'Retrieving game ids from {start.isoformat()} to {end.isoformat()} ...')
        games = itertools.chain(
            [('soccer', i) for i in self.get_soccer_ids_by_dates(
                start.isoformat(), end.isoformat())],
            [('rugby', i) for i in self.get_rugby_ids_by_dates(
                start.isoformat(), end.isoformat())]
        )

        def handle_game(game):
            t, i = game
            try:
                feed = self.get_f9(i['id']) if t == 'soccer' else self.get_ru7(i['id'])
            except OptaWebServiceError:
                _log.warning(f'Game {i["id"]} could not be retrieved!')
                return False
            
            if feed and feed['status'] != 'UNCHANGED':
                _log.info(f'Publishing {game} files ...')
                self.pub_input(bson.json_util.dumps(feed))
                return True
            
            return False

        def handle_competition(competition):
            t, comp, season = competition

            try:
                feed = self.get_f40(season, comp) if t == 'soccer' else None
            except OptaWebServiceError:
                _log.warning(f'Competition {comp}/{season} could not be retrieved!')
                return

            if feed:
                _log.info(f'Publishing {comp}/{season} files ...')
                self.pub_input(bson.json_util.dumps(feed))
        
        competitions = set()
        for g in games:
            handled = handle_game(g)
            if handled:
                competitions.add((g[0], g[1]['competition_id'], g[1]['season_id']))
        
        for c in competitions:
            handle_competition(c)

    @event_handler(
        'loader', 'input_loaded', handler_type=BROADCAST, reliable_delivery=False)
    def ack(self, payload):
        msg = bson.json_util.loads(payload)
        meta = msg.get('meta', None)
        if not meta:
            return
        checksum = msg.get('checksum', None)
        
        if 'type' not in meta or 'source' not in meta or meta['source'] != 'opta':
            return
        t = meta['type']

        def publish_notification(t, game, id_):
            _log.info(f'Publishing notification for {id_}')
            self.pub_notif(bson.json_util.dumps({
                'id': id_,
                'source': 'opta',
                'type': t,
                'content': f'{game["home_name"]} - {game["away_name"]}'}))

        if t == 'f9':
            if checksum:
                _log.info(f'Acknowledging {t} file: {msg["id"]}')
                self.ack_f9(msg['id'], checksum)
                game = self.get_f1(msg['id'])
                publish_notification(t, game, msg['id'])
            else:
                _log.warning(f'Received an event {t} {msg["id"]} without checksum')
        elif t == 'ru7':
            if checksum:
                _log.info(f'Acknowledging {t} file: {msg["id"]}')
                self.ack_ru7(msg['id'], checksum)
                game = self.get_ru1(msg['id'])
                publish_notification(t, game, msg['id'])
            else:
                _log.warning(f'Received an event {t} {msg["id"]} without checksum')
        elif t == 'f40':
            _log.info(f'Acknowledging {t} file: {msg["id"]}')
            self.pub_notif(bson.json_util.dumps({
                'id': msg['id'],
                'source': 'opta',
                'type': t,
                'content': 'Squads loaded'
            }))
        else:
            return

    @event_handler(
        'api_service', 'input_config', handler_type=BROADCAST, reliable_delivery=False)
    def handle_input_config(self, payload):
        msg = bson.json_util.loads(payload)

        if 'meta' not in msg or 'source' not in msg['meta'] or msg['meta']['source'] != 'opta':
            return

        if 'type' not in msg['meta']:
            _log.warning('type is missing within meta')

        type_ = msg['meta']['type']

        _log.info('Received a related input config ...')
        if 'config' not in msg:
            _log.warning('No config within the message. Ignoring ...')
        
        config = msg['config']

        if 'season' not in config or 'competition' not in config:
            _log.warning('Either competition or season is missing within config')

        if type_ == 'f1':
            self.add_f1(config['season'], config['competition'])
        elif type_ == 'ru1':
            self.add_ru1(config['season'], config['competition'])
        else:
            _log.warning('type should be either f1 or ru1')
            return

        self.pub_notif(bson.json_util.dumps({
            'id': ','.join([config['season'], config['competition']]),
            'source': msg['meta']['source'],
            'type': type_,
            'content': 'A new Opta feed has been added.'}))