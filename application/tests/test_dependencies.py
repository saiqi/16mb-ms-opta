import vcr
from nameko.testing.services import dummy, entrypoint_hook

from application.dependencies.opta import OptaDependency


class DummyService(object):
    name = 'dummy_service'

    opta_webservice = OptaDependency()

    @dummy
    def get_soccer_calendar(self):
        calendar = self.opta_webservice.get_soccer_calendar('2017', '24')

        return calendar

    @dummy
    def get_rugby_calendar(self):
        calendar = self.opta_webservice.get_rugby_calendar('2018', '203')

        return calendar

    @dummy
    def get_soccer_game(self):
        game = self.opta_webservice.get_soccer_game('920533')

        return game

    @dummy
    def get_corrupted_soccer_game(self):
        game = self.opta_webservice.get_soccer_game('impossible')

        return game

    @dummy
    def get_soccer_squads(self):
        s = self.opta_webservice.get_soccer_squads('2020', '24')

        return s

    @dummy
    def get_rugby_game(self):
        game = self.opta_webservice.get_rugby_game('318014')

        return game

@vcr.use_cassette('tests/vcr_cassettes/opta.yaml')
def test_end_to_end(container_factory):
    import os
    config = {
        'OPTA_URL': os.environ.get('OPTA_URL'),
        'OPTA_USER': os.environ.get('OPTA_USER'),
        'OPTA_PASSWORD': os.environ.get('OPTA_PASSWORD')
    }

    container = container_factory(DummyService, config)
    container.start()

    with entrypoint_hook(container, 'get_soccer_calendar') as get_calendar:
        calendar = get_calendar()
        assert len(calendar) == 380
        assert calendar[0]['home_name']
        assert calendar[0]['home_name'] in ('St Etienne','Angers','Dijon','Guingamp','Caen','Lyon','Toulouse','Troyes','Marseille','Paris Saint-Germain','Montpellier','Nice','Monaco','Lille','Nantes','Strasbourg','Amiens','Metz','Bordeaux','Rennes')

    with entrypoint_hook(container, 'get_soccer_game') as get_game:
        game = get_game()
        assert game['season']['id'] == '2017'
        assert game['competition']['id'] == 'c24'

    with entrypoint_hook(container, 'get_corrupted_soccer_game') as get_corrupted_game:
        game = get_corrupted_game()
        assert game is None

    with entrypoint_hook(container, 'get_rugby_calendar') as get_calendar:
        calendar = get_calendar()
        assert len(calendar) >= 187

    with entrypoint_hook(container, 'get_rugby_game') as get_game:
        game = get_game()
        assert 'rrml' in game
        assert 'id' in game['rrml']
        assert 'status' in game['rrml']
        assert len(game['teams']) == 2
        assert len(game['player_stats']) > 0
        assert len(game['events']) > 0
        assert len(game['team_stats']) > 0

@vcr.use_cassette('tests/vcr_cassettes/f40.yaml')
def test_f40(container_factory):
    import os
    config = {
        'OPTA_URL': os.environ.get('OPTA_URL'),
        'OPTA_USER': os.environ.get('OPTA_USER'),
        'OPTA_PASSWORD': os.environ.get('OPTA_PASSWORD')
    }

    container = container_factory(DummyService, config)
    container.start()

    with entrypoint_hook(container, 'get_soccer_squads') as get_soccer_squads:
        squads = get_soccer_squads()
        assert squads

        assert squads[0]['id'] == 't2128'
        assert squads[0]['competition_id'] == 'c24'
        assert squads[0]['season_id'] == '2020'
        assert squads[0]['short_name'] == 'Angers'

        psg = squads[15]
        assert psg['id'] == 't149'
        officials = psg['officials']
        assert len(officials) == 3
        for o in officials:
            assert 'id' in o
            assert 'first_name' in o
            assert 'last_name' in o
            assert 'type' in o
            assert o['type'] == o['type'].lower()

        kits = psg['team_kits']
        for t in ('away', 'home', 'third'):
            assert t in kits
            assert 'colour1' in kits[t]
            assert 'colour2' in kits[t]

        for p in psg['players']:
            assert p['id']
            assert p['name']
            assert p['position']
            assert p['join_date']
