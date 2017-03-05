import datetime
import pytz
import hashlib

import requests
from lxml import etree
import dateutil.parser
from nameko.dependency_providers import DependencyProvider


class OptaF1Parser:
    def __init__(self, xml_string):
        self.tree = etree.fromstring(xml_string)

    def get_calendar(self):
        calendar = []

        doc = self.tree.xpath('SoccerDocument')[0]
        competition_id = doc.get('competition_id')
        season_id = doc.get('season_id')

        data = doc.xpath('MatchData')

        for row in data:
            match_id = row.get('uID')[1:]

            tz = pytz.timezone('Europe/London')
            date = datetime.datetime.strptime(row.xpath('MatchInfo/Date')[0].text, '%Y-%m-%d %H:%M:%S')

            date = tz.localize(date).astimezone(pytz.utc)

            home_id = None
            away_id = None
            for t in row.xpath('TeamData'):
                if t.get('Side') == 'Home':
                    home_id = t.get('TeamRef')
                else:
                    away_id = t.get('TeamRef')

            calendar.append({
                'competition_id': competition_id,
                'season_id': season_id,
                'id': match_id,
                'date': date,
                'home_id': home_id,
                'away_id': away_id
            })

        return calendar


class OptaF9Parser(object):
    def __init__(self, xml_string):
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
        self.tree = etree.fromstring(xml_string, parser=parser)

    def get_competition(self):
        node = self.tree.xpath('SoccerDocument/Competition')[0]

        id = node.get('uID')

        name = None
        if node.xpath('Name'):
            name = node.xpath('Name')[0].text

        country = None
        if node.xpath('Country'):
            country = node.xpath('Country')[0].text

        code = None
        if node.xpath("Stat[@Type='symid']"):
            code = node.xpath("Stat[@Type='symid']")[0].text

        return {'id': id, 'name': name,
                'country': country if country is not None else '', 'code': code}

    def get_season(self):
        node = self.tree.xpath('SoccerDocument/Competition')[0]

        id = None
        if node.xpath("Stat[@Type='season_id']"):
            id = node.xpath("Stat[@Type='season_id']")[0].text

        name = None
        if node.xpath("Stat[@Type='season_name']"):
            name = node.xpath("Stat[@Type='season_name']")[0].text.replace('Season', '')

        return {'id': id, 'name': name}

    def get_venue(self):
        if self.tree.xpath('SoccerDocument/Venue'):
            venue = self.tree.xpath('SoccerDocument/Venue')[0]

            id = venue.get('uID')

            country = None
            if venue.xpath('Country'):
                country = venue.xpath('Country')[0].text

            name = None
            if venue.xpath('Name'):
                name = venue.xpath('Name')[0].text

            return {'id': id, 'name': name, 'country': country}

        return None

    def get_teams(self):
        results = []
        teams = self.tree.xpath('SoccerDocument/Team')

        if len(teams) > 0:
            for t in teams:
                id = t.get('uID')

                country = None
                if t.xpath('Country'):
                    country = t.xpath('Country')[0].text

                name = None
                if t.xpath('Name'):
                    name = t.xpath('Name')[0].text

                results.append({'id': id, 'name': name, 'country': country})

        return results

    def get_persons(self):
        results = []

        nodes = self.tree.xpath('SoccerDocument/Team')

        if len(nodes) > 0:

            for t in nodes:

                # Player
                players = t.xpath('Player')

                if len(players) > 0:

                    for p in players:
                        id = p.get('uID')

                        if p.xpath('PersonName'):
                            person_name = p.xpath('PersonName')[0]

                            first_name = None
                            if person_name.xpath('First'):
                                first_name = person_name.xpath('First')[0].text

                            last_name = None
                            if person_name.xpath('Last'):
                                last_name = person_name.xpath('Last')[0].text

                            known_name = None
                            if person_name.xpath('Known'):
                                known_name = person_name.xpath('Known')[0].text

                        results.append({'id': id, 'first_name': first_name,
                                        'last_name': last_name, 'known': known_name})

                # Team official
                if t.xpath("TeamOfficial[@Type='Manager']"):
                    official = t.xpath('TeamOfficial')[0]

                    id = official.get('uID')

                    if official.xpath('PersonName'):
                        person_name = official.xpath('PersonName')[0]

                        first_name = None
                        if person_name.xpath('First'):
                            first_name = person_name.xpath('First')[0].text

                        last_name = None
                        if person_name.xpath('Last'):
                            last_name = person_name.xpath('Last')[0].text

                        known_name = None
                        if person_name.xpath('Known'):
                            known_name = person_name.xpath('Known')[0].text

                    results.append({'id': id, 'first_name': first_name,
                                    'last_name': last_name, 'known': known_name})

            if self.tree.xpath('SoccerDocument/MatchData/MatchOfficial'):
                node = self.tree.xpath('SoccerDocument/MatchData/MatchOfficial')[0]

                id = node.get('uID')

                if node.xpath('OfficialName'):
                    person_name = node.xpath('OfficialName')[0]

                first_name = None
                if person_name.xpath('First'):
                    first_name = person_name.xpath('First')[0].text

                last_name = None
                if person_name.xpath('Last'):
                    last_name = person_name.xpath('Last')[0].text

                known_name = None
                if person_name.xpath('Known'):
                    known_name = person_name.xpath('Known')[0].text

                results.append({'id': id, 'first_name': first_name,
                                'last_name': last_name, 'known': known_name})

        return results

    def get_match_info(self):
        node = self.tree.xpath('SoccerDocument')[0]

        id = node.get('uID')

        competition = node.xpath('Competition')[0]

        season_id = competition.xpath("Stat[@Type='season_id']")[0].text
        competition_id = competition.get('uID')
        matchday = competition.xpath("Stat[@Type='matchday']")[0].text

        name = None
        number = None
        pool = None
        if competition.xpath('Round'):
            round = competition.xpath('Round')[0]

            if round.xpath('Name'):
                name = round.xpath('Name')[0].text

            if round.xpath('RoundNumber'):
                number = round.xpath('RoundNumber')[0].text

            if round.xpath('Pool'):
                pool = round.xpath('Pool')[0].text

        match_data = node.xpath('MatchData')[0]

        match_info = match_data.xpath('MatchInfo')[0]
        _type = match_info.get('MatchType')
        period = match_info.get('Period')
        weather = match_info.get('Weather')

        attendance = None
        if match_info.xpath('Attendance'):
            attendance = match_info.xpath('Attendance')[0].text

        date = None
        if match_info.xpath('Date'):
            date = dateutil.parser.parse(match_info.xpath('Date')[0].text)
            date = date.replace(tzinfo=None)
            tz = pytz.timezone('Europe/London')
            date = tz.localize(date).astimezone(pytz.utc)

        if match_info.xpath('Result'):
            result = match_info.xpath('Result')[0]

            winner_id = result.get('Winner')

        if match_data.xpath('MatchOfficial'):
            official = match_data.xpath('MatchOfficial')[0]

            official_id = official.get('uID')

        if node.xpath('Venue'):
            venue = node.xpath('Venue')[0]

            venue_id = venue.get('uID')

        return {'id': id,
                'competition_id': competition_id,
                'season_id': season_id,
                'type': _type,
                'matchday': matchday,
                'weather': weather,
                'attendance': attendance,
                'period': period,
                'date': date,
                'pool': pool,
                'round_number': number,
                'round_name': name,
                'venue_id': venue_id,
                'match_official_id': official_id,
                'winner_id': winner_id}

    def get_goals(self):
        results = []

        node = self.tree.xpath('SoccerDocument')[0]

        match_id = node.get('uID')

        tds = node.xpath('MatchData/TeamData')

        for td in tds:
            team_id = td.get('TeamRef')

            goals = td.xpath('Goal')

            for g in goals:
                id = g.get('EventID')
                time = g.get('Time')
                player_id = g.get('PlayerRef')
                _type = g.get('Type')

                assist_id = None
                if g.xpath('Assist'):
                    assist_id = g.xpath('Assist')[0].text

                second_assist_id = None
                if g.xpath('SecondAssist'):
                    second_assist_id = g.xpath('SecondAssist')[0].text

                results.append({'id': id,
                                'match_id': match_id,
                                'team_id': team_id,
                                'time': time,
                                'player_id': player_id,
                                'type': _type,
                                'assist_id': assist_id,
                                'second_assist_id': second_assist_id})

        return results

    def get_substitutions(self):
        results = []

        node = self.tree.xpath('SoccerDocument')[0]

        match_id = node.get('uID')

        tds = node.xpath('MatchData/TeamData')

        for td in tds:
            team_id = td.get('TeamRef')

            subs = td.xpath('Substitution')

            for s in subs:
                id = s.get('EventID')
                time = s.get('Time')
                player_on_id = s.get('SubOn')
                player_off_id = s.get('SubOff')
                reason = s.get('Reason')

                results.append({'id': id,
                                'match_id': match_id,
                                'team_id': team_id,
                                'time': time,
                                'player_off_id': player_on_id,
                                'reason': reason,
                                'player_on_id': player_off_id})

        return results

    def get_bookings(self):
        results = []

        node = self.tree.xpath('SoccerDocument')[0]

        match_id = node.get('uID')

        tds = node.xpath('MatchData/TeamData')

        for td in tds:
            team_id = td.get('TeamRef')

            books = td.xpath('Booking')

            for s in books:
                id = s.get('EventID')
                time = s.get('Time')
                player_id = s.get('PlayerRef')
                card = s.get('Card')
                card_type = s.get('CardType')
                reason = s.get('Reason')

                results.append({'id': id,
                                'match_id': match_id,
                                'team_id': team_id,
                                'time': time,
                                'player_id': player_id,
                                'reason': reason,
                                'card': card,
                                'cardtype': card_type})

        return results

    def get_missed_penalties(self):
        results = []

        node = self.tree.xpath('SoccerDocument')[0]

        match_id = node.get('uID')

        tds = node.xpath('MatchData/TeamData')

        for td in tds:
            team_id = td.get('TeamRef')

            pens = td.xpath('MissedPenalty')

            for s in pens:
                id = s.get('EventID')
                time = s.get('Time')
                player_id = s.get('PlayerRef')
                _type = s.get('Type')

                results.append({'id': id,
                                'match_id': match_id,
                                'team_id': team_id,
                                'time': time,
                                'player_id': player_id,
                                'type': _type})

        return results

    def get_team_stats(self):
        results = []

        node = self.tree.xpath('SoccerDocument')[0]

        # Match ID
        match_id = node.get('uID')

        # Competition ID
        competition = node.xpath('Competition')[0]

        competition_id = competition.get('uID')

        # Season ID
        season_id = competition.xpath("Stat[@Type='season_id']")[0].text

        tds = node.xpath('MatchData/TeamData')

        for td in tds:
            team_id = td.get('TeamRef')

            score = td.get('Score')
            sh_score = td.get('ShootOutScore')
            side = td.get('Side')

            if td.xpath("Stat[@Type='formation_used']"):
                formation_used = td.xpath("Stat[@Type='formation_used']")[0].text

            team = td.xpath("../../Team[@uID='" + team_id + "']")[0]

            official = team.xpath('TeamOfficial')

            if official:
                official_id = official[0].get('uID')

            stats = td.xpath('Stat')

            for s in stats:
                id = s.get('EventID')
                _type = s.get('Type')
                fh = None
                if s.get('FH'):
                    fh = s.get('FH')
                sh = None
                if s.get('SH'):
                    sh = s.get('SH')
                efh = None
                if s.get('EFH'):
                    efh = s.get('EFH')
                esh = None
                if s.get('ESH'):
                    esh = s.get('ESH')
                value = s.text

                id = hashlib.sha1(
                    ''.join([competition_id, season_id, match_id, team_id, _type]).encode('utf-8')).hexdigest()

                results.append({
                    'id': id,
                    'competition_id': competition_id,
                    'season_id': season_id,
                    'match_id': match_id,
                    'team_id': team_id,
                    'score': score,
                    'shootout_score': sh_score,
                    'side': side,
                    'formation_used': formation_used,
                    'official_id': official_id,
                    'type': _type,
                    'fh': fh,
                    'sh': sh,
                    'efh': efh,
                    'esh': esh,
                    'value': value})

        return results

    def get_player_stats(self):
        results = []

        node = self.tree.xpath('SoccerDocument')[0]

        # Match ID
        match_id = node.get('uID')

        # Competition ID
        competition = node.xpath('Competition')[0]

        competition_id = competition.get('uID')

        # Season ID
        season_id = competition.xpath("Stat[@Type='season_id']")[0].text

        tds = node.xpath('MatchData/TeamData')

        for td in tds:

            # Team level data
            team_id = td.get('TeamRef')

            score = td.get('Score')
            sh_score = td.get('ShootOutScore')
            side = td.get('Side')

            if td.xpath("Stat[@Type='formation_used']"):
                formation_used = td.xpath("Stat[@Type='formation_used']")[0].text

            team = td.xpath("../../Team[@uID='" + team_id + "']")[0]

            official = team.xpath('TeamOfficial')

            if official:
                official_id = official[0].get('uID')

            players = td.xpath('PlayerLineUp/MatchPlayer')

            for p in players:

                # Player level data
                player_id = p.get('PlayerRef')

                position = p.get('Position')
                sub_position = p.get('SubPosition')
                shirt_number = p.get('ShirtNumber')
                status = p.get('Status')
                captain = None
                if p.get('Captain'):
                    captain = p.get('Captain')
                formation_place = p.xpath("Stat[@Type='formation_place']")[0].text

                stats = p.xpath('Stat')

                for s in stats:
                    # Stat level data
                    _type = s.get('Type')
                    value = s.text

                    id = hashlib.sha1(
                        ''.join([competition_id, season_id, match_id, player_id, _type]).encode('utf-8')).hexdigest()

                    results.append({
                        'id': id,
                        'player_id': player_id,
                        'competition_id': competition_id,
                        'season_id': season_id,
                        'match_id': match_id,
                        'team_id': team_id,
                        'score': score,
                        'shootout_score': sh_score,
                        'side': side,
                        'formation_used': formation_used,
                        'official_id': official_id,
                        'main_position': position,
                        'sub_position': sub_position,
                        'shirt_number': shirt_number,
                        'status': status,
                        'captain': captain,
                        'formation_place': formation_place,
                        'type': _type,
                        'value': value})

        return results


class OptaWebService(object):
    def __init__(self, url, user, password):
        self.f9_url = url
        self.f1_url = url + '/competition.php'
        self.user = user
        self.password = password

    def get_calendar(self, season_id, competition_id):
        params = {'feed_type': 'F1', 'user': self.user, 'psw': self.password, 'competition': competition_id,
                  'season_id': season_id}

        r = requests.get(self.f1_url, params=params)

        parser = OptaF1Parser(r.content)

        return parser.get_calendar()

    def is_game_ready(self, game_id):
        params = {'feed_type': 'F9', 'game_id': game_id, 'user': self.user, 'psw': self.password}

        r = requests.get(self.f9_url, params=params)

        if 'response' in r.content:
            return False

        return True

    def get_game(self, game_id):
        params = {'feed_type': 'F9', 'game_id': game_id, 'user': self.user, 'psw': self.password}

        r = requests.get(self.f9_url, params=params)

        parser = OptaF9Parser(r.content)

        game = {
            'season': parser.get_season(),
            'competition': parser.get_competition(),
            'venue': parser.get_venue(),
            'teams': parser.get_teams(),
            'persons': parser.get_persons(),
            'match_info': parser.get_match_info(),
            'bookings': parser.get_bookings(),
            'goals': parser.get_goals(),
            'missed_penalties': parser.get_missed_penalties(),
            'substitutions': parser.get_substitutions(),
            'team_stats': parser.get_team_stats(),
            'player_stats': parser.get_player_stats()
        }

        return game


class OptaDependency(DependencyProvider):
    def get_dependency(self, worker_ctx):
        self.opta_webservice = OptaWebService(self.container.config['OPTA_URL'], self.container.config['OPTA_USER'],
                                              self.container.config['OPTA_PASSWORD'])
        return self.opta_webservice

    def stop(self):
        self.opta_webservice = None
        del self.opta_webservice

    def kill(self):
        self.opta_webservice = None
        del self.opta_webservice
