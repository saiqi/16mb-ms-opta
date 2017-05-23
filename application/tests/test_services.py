import eventlet

eventlet.monkey_patch()

import datetime

import pytest
from nameko.testing.services import worker_factory
from pymongo import MongoClient

from application.services.opta_collector import OptaCollectorService


@pytest.fixture
def database(db_url):
    client = MongoClient(db_url)

    yield client['test_db']

    client.drop_database('test_db')
    client.close()


def test_add_f1(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_calendar.side_effect = lambda season_id, competition_id: [{
        'competition_id': competition_id,
        'season_id': season_id,
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'
    }]

    service.add_f1('s_id', 'c_id')

    assert service.database.f1.find_one({'id': 'g_id'})['season_id'] == 's_id'
    assert service.database.f1.find_one({'id': 'g_id'})['competition_id'] == 'c_id'


def test_update_all_f1(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_calendar.side_effect = lambda season_id, competition_id: [{
        'competition_id': competition_id,
        'season_id': season_id,
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'
    }]

    service.database.f1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})

    service.update_all_f1()

    assert service.database.f1.find_one({'id': 'g_id'})['season_id'] == 's_id'
    assert service.database.f1.find_one({'id': 'g_id'})['competition_id'] == 'c_id'


def test_get_ids_by_dates(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.f1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})

    start_date = datetime.datetime.now() - datetime.timedelta(days=1)
    end_date = datetime.datetime.now() + datetime.timedelta(days=1)

    ids = service.get_ids_by_dates(start_date.isoformat(), end_date.isoformat())

    assert 'g_id' in ids


def test_get_ids_by_season_and_competition(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.f1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})

    ids = service.get_ids_by_season_and_competition('s_id', 'c_id')

    assert 'g_id' in ids


def test_get_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_game.side_effect = lambda game_id: {
            'season': {'id': 's_id', 'fingerprint': 'season'},
            'competition': {'id': 'c_id', 'fingerprint': 'competition'},
            'venue': {'id': 'v_id', 'fingerprint': 'venue'},
            'teams': [{'id': 't_1', 'fingerprint': 't1'}, {'id': 't_2', 'fingerprint': 't2'}],
            'persons': [{'id': 'p_1', 'fingerprint': 'p1'}, {'id': 'p_2', 'fingerprint': 'p2'}],
            'match_info': {'id': game_id, 'fingerprint': 'game', 'period': 'FullTime'},
            'events': [{'id': 'e_1', 'fingerprint': 'e1'}, {'id': 'e_2', 'fingerprint': 'e2'}],
            'team_stats': [{'id': 'ts_1', 'fingerprint': 'ts1'}, {'id': 'ts_2', 'fingerprint': 'ts2'}],
            'player_stats': [{'id': 'ps_1', 'fingerprint': 'ps1'}, {'id': 'ps_2', 'fingerprint': 'ps2'}]
        }

    game = service.get_f9('g_id')
    assert game['status'] == 'CREATED'
    assert game['checksum']

    service.database.f9.insert_one({'id': 'g_id', 'checksum': game['checksum']})

    game = service.get_f9('g_id')
    assert game['status'] == 'UNCHANGED'

    service.opta.get_game.side_effect = lambda game_id: {
        'season': {'id': 's_id', 'fingerprint': 'season'},
        'competition': {'id': 'c_id', 'fingerprint': 'competition'},
        'venue': {'id': 'v_id', 'fingerprint': 'venue'},
        'teams': [{'id': 't_1', 'fingerprint': 't1'}, {'id': 't_2', 'fingerprint': 't2'}],
        'persons': [{'id': 'p_1', 'fingerprint': 'p1'}, {'id': 'p_2', 'fingerprint': 'p2'}],
        'match_info': {'id': game_id, 'fingerprint': 'game', 'period': 'FullTime'},
        'events': [{'id': 'e_1', 'fingerprint': 'e1'}, {'id': 'e_2', 'fingerprint': 'e2'}],
        'team_stats': [{'id': 'ts_1', 'fingerprint': 'ts1'}, {'id': 'ts_2', 'fingerprint': 'ts2'}],
        'player_stats': [{'id': 'ps_1', 'fingerprint': 'ps1'}]
    }

    game = service.get_f9('g_id')
    assert game['status'] == 'UPDATED'


def test_ack_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.ack_f9('g_id', 'toto')

    assert service.database.f9.find_one({'id': 'g_id'})


def test_unack_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.f9.insert_one({'id': 'g_id', 'checksum': 'toto'})

    service.unack_f9('g_id')

    assert service.database.f9.find_one({'id': 'g_id'}) is None
