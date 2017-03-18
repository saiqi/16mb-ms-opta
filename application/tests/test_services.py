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
    
    assert service.database.calendar.find_one({'id': 'g_id'})['season_id'] == 's_id'
    assert service.database.calendar.find_one({'id': 'g_id'})['competition_id'] == 'c_id'


def test_update_all_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_calendar.side_effect = lambda season_id, competition_id: [{
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'
    }]
    service.opta.get_game.side_effect = lambda game_id: {
        'season': {'id': 's_id', 'fingerprint': 'season'},
        'competition': {'id': 'c_id', 'fingerprint': 'competition'},
        'venue': {'id': 'v_id', 'fingerprint': 'venue'},
        'teams': [{'id': 't_1', 'fingerprint': 't1'}, {'id': 't_2', 'fingerprint': 't2'}],
        'persons': [{'id': 'p_1', 'fingerprint': 'p1'}, {'id': 'p_2', 'fingerprint': 'p2'}],
        'match_info': {'id': game_id, 'fingerprint': 'game'},
        'events': [{'id': 'e_1', 'fingerprint': 'e1'}, {'id': 'e_2', 'fingerprint': 'e2'}],
        'team_stats': [{'id': 'ts_1', 'fingerprint': 'ts1'}, {'id': 'ts_2', 'fingerprint': 'ts2'}],
        'player_stats': [{'id': 'ps_1', 'fingerprint': 'ps1'}, {'id': 'ps_2', 'fingerprint': 'ps2'}]
    }
    service.add_f1('s_id', 'c_id')
    service.update_all_f9('s_id', 'c_id')
    assert service.database.seasons.find_one({'id': 's_id'})
    assert service.database.competitions.find_one({'id': 'c_id'})
    assert service.database.venues.find_one({'id': 'v_id'})
    assert service.database.teams.find_one({'id': 't_2'})
    assert service.database.people.find_one({'id': 'p_2'})
    assert service.database.matchinfos.find_one({'id': 'g_id'})
    assert service.database.events.find_one({'id': 'e_1'})
    assert service.database.teamstats.find_one({'id': 'ts_1'})
    assert service.database.playerstats.find_one({'id': 'ps_1'})


def test_update_f1_f9(database):
    service = worker_factory(OptaCollectorService, database=database)
    service.opta.get_calendar.side_effect = lambda season_id, competition_id: [{
        'competition_id': 'c_id',
        'season_id': 's_id',
        'date': datetime.datetime.now(),
        'home_id': 'h_id',
        'away_id': 'a_id',
        'id': 'g_id',
        'fingerprint': 'calendar'
    }]
    service.opta.get_game.side_effect = lambda game_id: {
        'season': {'id': 's_id', 'fingerprint': 'season'},
        'competition': {'id': 'c_id', 'fingerprint': 'competition'},
        'venue': {'id': 'v_id', 'fingerprint': 'venue'},
        'teams': [{'id': 't_1', 'fingerprint': 't1'}, {'id': 't_2', 'fingerprint': 't2'}],
        'persons': [{'id': 'p_1', 'fingerprint': 'p1'}, {'id': 'p_2', 'fingerprint': 'p2'}],
        'match_info': {'id': game_id, 'fingerprint': 'game'},
        'events': [{'id': 'e_1', 'fingerprint': 'e1'}, {'id': 'e_2', 'fingerprint': 'e2'}],
        'team_stats': [{'id': 'ts_1', 'fingerprint': 'ts1'}, {'id': 'ts_2', 'fingerprint': 'ts2'}],
        'player_stats': [{'id': 'ps_1', 'fingerprint': 'ps1', 'match_id': 'g_id'},
                         {'id': 'ps_2', 'fingerprint': 'ps2', 'match_id': 'g_id'}]
    }

    service.add_f1('s_id', 'c_id')
    service.update_f1_f9()

    stats = list(service.database.playerstats.find())

    assert len(stats) == 2
    assert service.database.playerstats.find_one({'id': 'ps_1'})['fingerprint'] == 'ps1'
    assert service.database.playerstats.find_one({'id': 'ps_2'})['fingerprint'] == 'ps2'

    service.opta.get_game.side_effect = lambda game_id: {
        'season': {'id': 's_id', 'fingerprint': 'season'},
        'competition': {'id': 'c_id', 'fingerprint': 'competition'},
        'venue': {'id': 'v_id', 'fingerprint': 'venue'},
        'teams': [{'id': 't_1', 'fingerprint': 't1'}, {'id': 't_2', 'fingerprint': 't2'}],
        'persons': [{'id': 'p_1', 'fingerprint': 'p1'}, {'id': 'p_2', 'fingerprint': 'p2'}],
        'match_info': {'id': game_id, 'fingerprint': 'game'},
        'events': [{'id': 'e_1', 'fingerprint': 'e1'}, {'id': 'e_2', 'fingerprint': 'e2'}],
        'team_stats': [{'id': 'ts_1', 'fingerprint': 'ts1'}, {'id': 'ts_2', 'fingerprint': 'ts2'}],
        'player_stats': [{'id': 'ps_1', 'fingerprint': 'ps1', 'match_id': 'g_id'}]
    }

    service.update_f1_f9()

    stats = list(service.database.playerstats.find())

    assert len(stats) == 1
    assert service.database.playerstats.find_one({'id': 'ps_1'})['fingerprint'] == 'ps1'
