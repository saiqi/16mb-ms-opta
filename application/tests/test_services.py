import eventlet

eventlet.monkey_patch()

import datetime

import pytest
from nameko.testing.services import worker_factory
from pymongo import MongoClient
import bson.json_util

from application.services.opta_collector import OptaCollectorService


@pytest.fixture
def database():
    client = MongoClient()

    yield client['test_db']

    client.drop_database('test_db')
    client.close()


def test_add_f1(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_soccer_calendar.side_effect = lambda season_id, competition_id: [{
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


def test_add_ru1(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_rugby_calendar.side_effect = lambda season_id, competition_id: [{
        'competition_id': competition_id,
        'season_id': season_id,
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'
    }]

    service.add_ru1('s_id', 'c_id')

    assert service.database.ru1.find_one({'id': 'g_id'})['season_id'] == 's_id'
    assert service.database.ru1.find_one({'id': 'g_id'})['competition_id'] == 'c_id'


def test_update_all_f1(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_soccer_calendar.side_effect = lambda season_id, competition_id: [{
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


def test_update_all_ru1(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_rugby_calendar.side_effect = lambda season_id, competition_id: [{
        'competition_id': competition_id,
        'season_id': season_id,
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'
    }]

    service.database.ru1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})

    service.update_all_f1()

    assert service.database.ru1.find_one({'id': 'g_id'})['season_id'] == 's_id'
    assert service.database.ru1.find_one({'id': 'g_id'})['competition_id'] == 'c_id'


def test_get_soccer_ids_by_dates(database):
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

    ids = service.get_soccer_ids_by_dates(start_date.isoformat(), end_date.isoformat())

    assert 'g_id' in ids


def test_get_soccer_ids_by_season_and_competition(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.f1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})

    ids = service.get_soccer_ids_by_season_and_competition('s_id', 'c_id')

    assert 'g_id' in ids


def test_get_rugby_ids_by_dates(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.ru1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})

    start_date = datetime.datetime.now() - datetime.timedelta(days=1)
    end_date = datetime.datetime.now() + datetime.timedelta(days=1)

    ids = service.get_rugby_ids_by_dates(start_date.isoformat(), end_date.isoformat())

    assert 'g_id' in ids


def test_get_f1(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.f1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})
    game = service.get_f1('g_id')

    assert game['id'] == 'g_id'


def test_get_rugby_ids_by_season_and_competition(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.ru1.insert_one({
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'})

    ids = service.get_rugby_ids_by_season_and_competition('s_id', 'c_id')

    assert 'g_id' in ids


def test_get_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_soccer_game.side_effect = lambda game_id: {
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

    game = service.get_f9('g_id')
    assert game['status'] == 'CREATED'
    assert game['checksum']
    assert game['id']
    assert game['referential']
    assert game['datastore']

    service.database.f9.insert_one({'id': 'g_id', 'checksum': game['checksum']})

    game = service.get_f9('g_id')
    assert game['status'] == 'UNCHANGED'

    service.opta.get_soccer_game.side_effect = lambda game_id: {
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

    game = service.get_f9('g_id')
    assert game['status'] == 'UPDATED'


def test_get_f40(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_soccer_squads.side_effect = lambda season_id, competition_id: [{
        'competition_id': competition_id,
        'competition_name': 'competition_name',
        'season_id': season_id,
        'season_name': 'season_name',
        'country': 'country',
        'country_id': 'country_id',
        'country_iso': 'country_iso',
        'region_id': 'region_id',
        'region_name': 'region_name',
        'short_name': 'short_club_name',
        'name': './Name',
        'id': 'uID',
        'symid': './SYMID',
        'venue_name': 'venue_name',
        'venue_id': 'venue_id',
        'team_kits': {},
        'officials': [],
        'players': [{
            'id': 'VARCHAR(10)',
            'first_name': 'VARCHAR(150)',
            'last_name': 'VARCHAR(150)',
            'known_name': 'VARCHAR(10)',
            'birth_date': 'DATE',
            'birth_place': 'VARCHAR(150)',
            'first_nationality': 'VARCHAR(50)',
            'preferred_foot': 'VARCHAR(10)',
            'weight': 'INTEGER',
            'height': 'INTEGER',
            'real_position': 'VARCHAR(50)',
            'real_position_side': 'VARCHAR(50)',
            'country': 'VARCHAR(50)',
            'join_date': 'join_date'
        }]
    }]

    game = service.get_f40('s_id', 'c_id')
    assert game['status'] == 'UPDATED'
    assert not game['checksum']
    assert game['id']
    assert game['referential']
    assert game['datastore']

def test_ack_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.ack_f9('g_id', 'toto')

    assert service.database.f9.find_one({'id': 'g_id'})


def test_unack_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.f9.insert_one({'id': 'g_id', 'checksum': 'toto'})

    service.unack_f9('g_id')

    assert service.database.f9.find_one({'id': 'g_id'}) is None


def test_ack_ru7(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.ack_ru7('g_id', 'toto')

    assert service.database.ru7.find_one({'id': 'g_id'})


def test_unack_ru7(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.database.ru7.insert_one({'id': 'g_id', 'checksum': 'toto'})

    service.unack_ru7('g_id')

    assert service.database.ru7.find_one({'id': 'g_id'}) is None
