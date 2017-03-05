import pytest
from nameko.testing.services import dummy, entrypoint_hook

from application.dependencies.opta import OptaDependency, OptaWebServiceError


class DummyService(object):
    name = 'dummy_service'

    opta_webservice = OptaDependency()

    @dummy
    def get_calendar(self):
        calendar = self.opta_webservice.get_calendar('2015', '24')

        return calendar

    @dummy
    def get_game(self):
        game = self.opta_webservice.get_game('805305')

        return game

    @dummy
    def get_corrupted_game(self):
        game = self.opta_webservice.get_game('impossible')

        return game

    @dummy
    def is_game_ready(self):
        return self.opta_webservice.is_game_ready('805305')


def test_end_to_end(opta_url, opta_user, opta_password, container_factory):
    config = {'OPTA_URL': opta_url, 'OPTA_USER': opta_user, 'OPTA_PASSWORD': opta_password}

    container = container_factory(DummyService, config)
    container.start()

    with entrypoint_hook(container, 'get_calendar') as get_calendar:
        calendar = get_calendar()
        assert len(calendar) == 380

    with entrypoint_hook(container, 'is_game_ready') as is_game_ready:
        assert is_game_ready() is True

    with entrypoint_hook(container, 'get_game') as get_game:
        game = get_game()
        assert game['season']['id'] == '2015'
        assert game['competition']['id'] == 'c24'

    with entrypoint_hook(container, 'get_corrupted_game') as get_corrupted_game:
        with pytest.raises(OptaWebServiceError):
            get_corrupted_game()

