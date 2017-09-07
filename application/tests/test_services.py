import eventlet

eventlet.monkey_patch()

import datetime

import pytest
from nameko.testing.services import worker_factory
from pymongo import MongoClient
import bson.json_util

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
        'season': {'id': 's_id', 'name': 'Season'},
        'competition': {'id': 'c_id', 'name': 'Competition'},
        'venue': {'id': 'v_id', 'name': 'Venue', 'country': 'Country'},
        'teams': [{'id': 't_1', 'name': 'T1'}, {'id': 't_2', 'name': 'T2'}],
        'persons': [{'id': 'p_1', 'type': 'player', 'first_name': 'f', 'last_name': 'l', 'known': None},
                    {'id': 'p_2', 'type': 'player', 'first_name': 'i', 'last_name': 'a', 'known': 'ia'}],
        'match_info': {'id': game_id, 'period': 'FullTime', 'date': datetime.datetime.utcnow()},
        'events': [{'id': 'e_1'}, {'id': 'e_2'}],
        'team_stats': [{'team_id': 't_1', 'side': 'Home'}, {'team_id': 't_2', 'side': 'Away'}],
        'player_stats': [{'player_id': 'p_1', 'type': 'ps1', 'value': 10},
                         {'player_id': 'p_2', 'type': 'ps2', 'value': 5}]
    }

    game = bson.json_util.loads(service.get_f9('g_id'))
    assert game['status'] == 'CREATED'
    assert game['checksum']

    service.database.f9.insert_one({'id': 'g_id', 'checksum': game['checksum']})

    game = bson.json_util.loads(service.get_f9('g_id'))
    assert game['status'] == 'UNCHANGED'

    service.opta.get_game.side_effect = lambda game_id: {
        'season': {'id': 's_id', 'name': 'Season'},
        'competition': {'id': 'c_id', 'name': 'Competition'},
        'venue': {'id': 'v_id', 'name': 'Venue', 'country': 'Country'},
        'teams': [{'id': 't_1', 'name': 'T1'}, {'id': 't_2', 'name': 'T2'}],
        'persons': [{'id': 'p_1', 'type': 'player', 'first_name': 'f', 'last_name': 'l', 'known': None},
                    {'id': 'p_2', 'type': 'player', 'first_name': 'i', 'last_name': 'a', 'known': 'ia'}],
        'match_info': {'id': game_id, 'period': 'FullTime', 'date': datetime.datetime.utcnow()},
        'events': [{'id': 'e_1'}, {'id': 'e_2'}],
        'team_stats': [{'team_id': 't_1', 'side': 'Home'}, {'team_id': 't_2', 'side': 'Away'}],
        'player_stats': [{'player_id': 'p_1', 'type': 'ps1', 'value': 16}]
    }

    game = bson.json_util.loads(service.get_f9('g_id'))
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
