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
            'competition_id': 'c_id',
            'season_id': 's_id',
            'date': datetime.datetime.now(),
            'home_id': 'h_id',
            'away_id': 'a_id',
            'id': 'g_id'
        }]
    
    service.add_f1('s_id', 'c_id')
    
    assert service.database.f1.find_one({'id': 'g_id'})['season_id'] == 's_id'
    assert service.database.f1.find_one({'id': 'g_id'})['competition_id'] == 'c_id'
