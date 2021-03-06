import datetime
import pytz
import hashlib

import requests
from lxml import etree
import dateutil.parser
from nameko.dependency_providers import DependencyProvider


class OptaParser(object):
    def _compute_fingerprint(self, fields):
        concat = ''.join(str(f) if f is not None else '' for f in fields)
        return hashlib.sha1(concat.encode('utf-8')).hexdigest()


class OptaF1Parser(OptaParser):
    def __init__(self, xml_string):
        self.tree = etree.fromstring(xml_string)

    def _get_team_name(self, team_id):
        team_path = self.tree.xpath('SoccerDocument/Team[@uID=\'{}\']/Name'.format(team_id))
        if len(team_path) == 1:
            return team_path[0].text
        return None

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
                    home_name = self._get_team_name(home_id)
                else:
                    away_id = t.get('TeamRef')
                    away_name = self._get_team_name(away_id)

            calendar.append({
                'competition_id': competition_id,
                'season_id': season_id,
                'id': match_id,
                'date': date,
                'home_id': home_id,
                'away_id': away_id,
                'home_name': home_name,
                'away_name': away_name
                # 'fingerprint': self._compute_fingerprint([date, home_id, away_id])
            })

        return calendar


class OptaF9Parser(OptaParser):
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

        return {
            'id': id,
            'name': name,
            'country': country if country is not None else '',
            'code': code,
            # 'fingerprint': self._compute_fingerprint([name, country, code])
        }

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

            return {
                'id': id,
                'name': name,
                'country': country,
                # 'fingerprint': self._compute_fingerprint([name, country])
            }

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

                results.append({
                    'id': id,
                    'name': name,
                    'country': country,
                    # 'fingerprint': self._compute_fingerprint([name, country])
                })

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

                        results.append({
                            'id': id,
                            'first_name': first_name,
                            'last_name': last_name,
                            'known': known_name,
                            'type': 'player'
                            # 'fingerprint': self._compute_fingerprint([first_name, last_name, known_name])
                        })

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

                    results.append({
                        'id': id,
                        'first_name': first_name,
                        'last_name': last_name,
                        'known': known_name,
                        'type': 'manager'
                        # 'fingerprint': self._compute_fingerprint([first_name, last_name, known_name])
                    })

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

                results.append({
                    'id': id,
                    'first_name': first_name,
                    'last_name': last_name,
                    'known': known_name,
                    'type': 'referee'
                    # 'fingerprint': self._compute_fingerprint([first_name, last_name, known_name])
                })

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

        winner_id = None
        if match_info.xpath('Result'):
            result = match_info.xpath('Result')[0]

            winner_id = result.get('Winner')

        official_id = None
        if match_data.xpath('MatchOfficial'):
            official = match_data.xpath('MatchOfficial')[0]

            official_id = official.get('uID')

        venue_id = None
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
                'winner_id': winner_id,
                # 'fingerprint': self._compute_fingerprint(
                #     [weather, attendance, period, venue_id, official_id, winner_id])
                }

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
                                'second_assist_id': second_assist_id,
                                # 'fingerprint': self._compute_fingerprint(
                                #     [time, player_id, _type, assist_id, second_assist_id])
                                })

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
                                'player_on_id': player_off_id,
                                # 'fingerprint': self._compute_fingerprint([time, player_off_id, player_on_id, reason])
                                })

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
                                'cardtype': card_type,
                                # 'fingerprint': self._compute_fingerprint([time, player_id, card, card_type, reason])
                                })

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
                                'type': _type,
                                # 'fingerprint': self._compute_fingerprint([time, player_id, _type])
                                })

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

                if _type != 'formation_used':
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

                    # id = hashlib.sha1(
                    #     ''.join([competition_id, season_id, match_id, team_id, _type]).encode('utf-8')).hexdigest()

                    results.append({
                        # 'id': id,
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
                        'value': value,
                        # 'fingerprint': self._compute_fingerprint(
                        #     [score, sh_score, side, formation_used, official_id, fh, sh, efh, esh, value])
                    })

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

                    if _type != 'formation_place':
                        # id = hashlib.sha1(
                        #     ''.join([competition_id, season_id, match_id, player_id, _type]).encode(
                        #         'utf-8')).hexdigest()

                        results.append({
                            # 'id': id,
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
                            'value': value,
                            # 'fingerprint': self._compute_fingerprint(
                            #     [score, sh_score, side, formation_used, official_id, position,
                            #      sub_position, shirt_number, status, captain, formation_place, value])
                        })

        return results


class OptaF40Parser(OptaParser):
    
    def __init__(self, xml_string):
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
        self.tree = etree.fromstring(xml_string, parser=parser)

    def get_squads(self):
        doc = self.tree.xpath('//SoccerDocument')[0]
        competition_id = f'c{doc.get("competition_id")}'
        competition_name = doc.get('competition_name')
        season_id = doc.get('season_id')
        season_name = doc.get('season_name')

        teams = doc.xpath('./Team')

        def get_text_or_none(n, xpath):
            return n.xpath(xpath)[0].text if n.xpath(xpath) else None

        def handle_player(p):
            return {
                'id': p.get('uID'),
                'name': p.xpath('./Name')[0].text,
                'position': p.xpath('./Position')[0].text,
                **{s.get('Type'):s.text if s.text != 'Unknown' else None\
                    for s in p.xpath('./Stat')}
            }

        def handle_kit(k):
            return (k.get('type'), {'colour1': k.get('colour1'), 'colour2':k.get('colour2')})

        def handle_official(o):
            return {
                'type': o.get('Type').lower(),
                'id': o.get('uID'),
                'country': o.get('country'),
                'first_name': o.xpath('./PersonName/First')[0].text,
                'last_name': o.xpath('./PersonName/Last')[0].text,
                'known': get_text_or_none(o, './PersonName/Known'),
                'birth_date': get_text_or_none(o, './PersonName/BirthDate'),
                'birth_place': get_text_or_none(o, './PersonName/BirthPlace'),
                'join_date': get_text_or_none(o, './PersonName/join_date')
            }

        def handle_team(t):
            stadium = {
                'venue_name': get_text_or_none(t, './Stadium/Name'),
                'venue_id': f'v{t.xpath("./Stadium")[0].get("uID")}' if t.xpath('./Stadium') else None,
            }
            return {
                'competition_id': competition_id,
                'competition_name': competition_name,
                'season_id': season_id,
                'season_name': season_name,
                'country': t.get('country'),
                'country_id': t.get('country_id'),
                'country_iso': t.get('country_iso'),
                'region_id': t.get('region_id'),
                'region_name': t.get('region_name'),
                'short_name': t.get('short_club_name'),
                'name': t.xpath('./Name')[0].text,
                'id': t.get('uID'),
                'symid': t.xpath('./SYMID')[0].text,
                'venue_name': stadium['venue_name'],
                'venue_id': stadium['venue_id'],
                'team_kits': dict(handle_kit(k) for k in t.xpath('./TeamKits/Kit')),
                'officials': [handle_official(o) for o in t.xpath('./TeamOfficial')],
                'players': [handle_player(p) for p in t.xpath('Player')]
            }

        return [handle_team(t) for t in teams]


class OptaRU1Parser(OptaParser):
    def __init__(self, xml_string):
        self.tree = etree.fromstring(xml_string)

    def get_calendar(self):
        calendar = list()

        team_dict = dict(zip([t.get('id') for t in self.tree.xpath('teams/team')],
                             [t.get('name') for t in self.tree.xpath('teams/team')]))

        for fixture in self.tree.xpath('fixture'):
            competition_id = fixture.get('comp_id')
            season_id = fixture.get('season_id')
            match_id = fixture.get('id')
            date = dateutil.parser.parse(fixture.get('datetime')).astimezone(pytz.utc)
            competition_name = fixture.get('comp_name')
            group = fixture.get('group')
            group_name = fixture.get('group_name')
            venue = fixture.get('venue')
            venue_id = fixture.get('venue_id')
            round = fixture.get('round')

            home_id = None
            away_id = None
            for team in fixture.xpath('team'):
                if team.get('home_or_away') == 'home':
                    home_id = team.get('team_id')
                else:
                    away_id = team.get('team_id')

            home_name = team_dict[home_id]
            away_name = team_dict[away_id]
            calendar.append({
                'competition_id': competition_id,
                'competition_name': competition_name,
                'season_id': season_id,
                'id': match_id,
                'date': date,
                'group_id': group,
                'group_name': group_name,
                'venue': venue,
                'venue_id': venue_id,
                'round': round,
                'home_id': home_id,
                'away_id': away_id,
                'home_name': home_name,
                'away_name': away_name
            })

        return calendar


class OptaRU7Parser(OptaParser):
    def __init__(self, xml_string):
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
        self.tree = etree.fromstring(xml_string, parser=parser)

    @staticmethod
    def _handle_stat(stat):
        try:
            value = float(stat)
        except:
            return None

        return value

    @staticmethod
    def _handle_minute(value):
        if '+' in value:
            minute = int(value.split('+')[0])
        else:
            minute = int(value)

        return minute

    def get_rrml(self):
        rrml = self.tree

        attendance = None
        if rrml.get('attendance'):
            attendance = int(rrml.get('attendance'))

        away_ht_score = None
        if rrml.get('away_ht_score'):
            away_ht_score = int(rrml.get('away_ht_score'))

        away_score = None
        if rrml.get('away_score'):
            away_score = int(rrml.get('away_score'))

        home_ht_score = None
        if rrml.get('home_ht_score'):
            home_ht_score = int(rrml.get('home_ht_score'))

        home_score = None
        if rrml.get('home_score'):
            home_score = int(rrml.get('home_score'))

        _id = rrml.get('id')
        status = rrml.get('status')

        return {
            'id': _id,
            'attendance': attendance,
            'away_ht_score': away_ht_score,
            'away_score': away_score,
            'home_ht_score': home_ht_score,
            'home_score': home_score,
            'status': status
        }

    def get_events(self):
        events = list()

        _id = self.tree.get('id')

        for event in self.tree.xpath('Events')[0]:
            events.append({
                'minutes': self._handle_minute(event.get('minute')),
                'seconds': int(event.get('second')),
                'player_id': event.get('player_id'),
                'team_id': event.get('team_id'),
                'type': event.get('type'),
                'temporary': event.get('temporary'),
                'match_id': _id
            })

        return events

    def get_official(self):
        if self.tree.xpath('Officials') is None:
            return None

        for official in self.tree.xpath('Officials/Official'):
            if official.get('role') == 'referee':
                return {
                    'country': official.get('country'),
                    'role': official.get('role'),
                    'name': official.get('official_name'),
                    'id': official.get('id')
                }

    def get_teams(self):
        teams = list()
        for team in self.tree.xpath('TeamDetail/Team'):
            teams.append({
                'id': team.get('team_id'),
                'name': team.get('team_name')
            })
        return teams

    def get_players(self):
        players = list()
        for player in self.tree.xpath('TeamDetail/Team/Player'):
            players.append({
                'id': player.get('id'),
                'name': player.get('player_name')
            })
        return players

    def get_team_stats(self):
        teamstats = list()

        match_id = self.tree.get('id')

        for team in self.tree.xpath('TeamDetail/Team'):
            team_id = team.get('team_id')
            side = team.get('home_or_away')

            for stat in team.xpath('TeamStats/TeamStat'):
                for k, v in stat.attrib.items():
                    if k not in ('id', 'game_id', 'team_id'):
                        teamstats.append({
                            'team_id': team_id,
                            'side': side,
                            'match_id': match_id,
                            'type': k,
                            'value': self._handle_stat(v)
                        })

        return teamstats

    def get_player_stats(self):
        playerstats = list()

        match_id = self.tree.get('id')

        for team in self.tree.xpath('TeamDetail/Team'):
            team_id = team.get('team_id')
            side = team.get('home_or_away')

            for player in team.xpath('Player'):
                player_id = player.get('id')
                position = player.get('position')
                position_id = player.get('position_id')

                for stat in player.xpath('PlayerStats/PlayerStat'):
                    for k, v in stat.items():
                        if k not in ('game_id', 'team_id', 'player_id', 'id',):
                            playerstats.append({
                                'match_id': match_id,
                                'team_id': team_id,
                                'side': side,
                                'player_id': player_id,
                                'position_name': position,
                                'position_id': position_id,
                                'type': k,
                                'value': self._handle_stat(v)
                            })

        return playerstats


class OptaWebServiceError(Exception):
    pass


class OptaWebService(object):
    def __init__(self, url, user, password):
        self.f9_url = url
        self.f1_url = url + '/competition.php'
        self.user = user
        self.password = password

    def get_soccer_calendar(self, season_id, competition_id):
        params = {'feed_type': 'F1', 'user': self.user, 'psw': self.password, 'competition': competition_id,
                  'season_id': season_id}

        r = requests.get(self.f1_url, params=params)

        parser = OptaF1Parser(r.content)

        try:
            calendar = parser.get_calendar()
        except Exception:
            raise OptaWebServiceError(
                'Error while parsing F1 with params: {season} {competition}'.format(season=season_id,
                                                                                    competition=competition_id))

        return calendar

    def get_rugby_calendar(self, season_id, competition_id):
        params = {'feed_type': 'RU1', 'user': self.user, 'psw': self.password, 'competition': competition_id,
                  'season_id': season_id}

        r = requests.get(self.f1_url, params=params)

        parser = OptaRU1Parser(r.content)

        try:
            calendar = parser.get_calendar()
        except Exception:
            raise OptaWebServiceError(
                'Error while parsing RU1 with params: {season} {competition}'.format(season=season_id,
                                                                                     competition=competition_id))

        return calendar

    def _compute_soccer_events(self, game):
        results = []

        for stat in game.get_goals():

            d = dict()
            # d['id'] = hashlib.sha1(''.join([stat['id'], 'Goal']).encode('utf-8')).hexdigest()
            d['event_id'] = stat['id']
            d['competition_id'] = game.get_competition()['id']
            d['season_id'] = game.get_season()['id']
            d['match_id'] = stat['match_id']
            d['team_id'] = stat['team_id']
            d['player_id'] = stat['player_id']
            d['type'] = 'Goal'
            d['minutes'] = stat['time']
            d['seconds'] = '0'
            d['description'] = stat['type']
            d['detail'] = ''
            # d['fingerprint'] = stat['fingerprint']

            results.append(d)

            if stat['assist_id'] is not None:
                d = dict()
                # d['id'] = hashlib.sha1(''.join([stat['id'], 'Assist']).encode('utf-8')).hexdigest()
                d['event_id'] = stat['id']
                d['competition_id'] = game.get_competition()['id']
                d['season_id'] = game.get_season()['id']
                d['match_id'] = stat['match_id']
                d['team_id'] = stat['team_id']
                d['player_id'] = stat['assist_id']
                d['type'] = 'Assist'
                d['minutes'] = stat['time']
                d['seconds'] = '0'
                d['description'] = ''
                d['detail'] = ''
                # d['fingerprint'] = stat['fingerprint']

                results.append(d)

            if stat['second_assist_id'] is not None:
                d = dict()
                # d['id'] = hashlib.sha1(''.join([stat['id'], 'SecondAssist']).encode('utf-8')).hexdigest()
                d['event_id'] = stat['id']
                d['competition_id'] = game.get_competition()['id']
                d['season_id'] = game.get_season()['id']
                d['match_id'] = stat['match_id']
                d['team_id'] = stat['team_id']
                d['player_id'] = stat['second_assist_id']
                d['type'] = 'SecondAssist'
                d['minutes'] = stat['time']
                d['seconds'] = '0'
                d['description'] = ''
                d['detail'] = ''
                # d['fingerprint'] = stat['fingerprint']

                results.append(d)

        for stat in game.get_bookings():
            d = dict()
            # d['id'] = hashlib.sha1(''.join([stat['id'], stat['card']]).encode('utf-8')).hexdigest()
            d['event_id'] = stat['id']
            d['competition_id'] = game.get_competition()['id']
            d['season_id'] = game.get_season()['id']
            d['match_id'] = stat['match_id']
            d['team_id'] = stat['team_id']
            d['player_id'] = stat['player_id']
            d['type'] = stat['card']
            d['minutes'] = stat['time']
            d['seconds'] = '0'
            d['description'] = stat['cardtype']
            d['detail'] = stat['reason']
            # d['fingerprint'] = stat['fingerprint']

            results.append(d)

        for stat in game.get_substitutions():
            d = dict()
            # d['id'] = hashlib.sha1(''.join([stat['id'], 'SubOff']).encode('utf-8')).hexdigest()
            d['event_id'] = stat['id']
            d['competition_id'] = game.get_competition()['id']
            d['season_id'] = game.get_season()['id']
            d['match_id'] = stat['match_id']
            d['team_id'] = stat['team_id']
            d['player_id'] = stat['player_off_id']
            d['type'] = 'SubOff'
            d['minutes'] = stat['time']
            d['seconds'] = '0'
            d['description'] = stat['reason']
            d['detail'] = ''
            # d['fingerprint'] = stat['fingerprint']

            results.append(d)

            d = dict()
            # d['id'] = hashlib.sha1(''.join([stat['id'], 'SubOn']).encode('utf-8')).hexdigest()
            d['event_id'] = stat['id']
            d['competition_id'] = game.get_competition()['id']
            d['season_id'] = game.get_season()['id']
            d['match_id'] = stat['match_id']
            d['team_id'] = stat['team_id']
            d['player_id'] = stat['player_on_id']
            d['type'] = 'SubOn'
            d['minutes'] = stat['time']
            d['seconds'] = '0'
            d['description'] = stat['reason']
            d['detail'] = ''
            # d['fingerprint'] = stat['fingerprint']

            results.append(d)

        return results

    def _check_mins_played(self, player_stats):
        for s in player_stats:
            if s['type'] == 'mins_played':
                return True
        return False

    def get_soccer_game(self, game_id):
        game = None
        params = {'feed_type': 'F9', 'game_id': game_id, 'user': self.user, 'psw': self.password}
        url = self.f9_url + "/?feed_type={feed_type}&game_id={game_id}&user={user}&psw={psw}".format(**params)
        r = requests.get(url)

        if 'response' in r.text:
            return game

        try:
            parser = OptaF9Parser(r.content)

            match_info = parser.get_match_info()
            player_stats = parser.get_player_stats()
            if match_info['period'] == 'FullTime' and self._check_mins_played(player_stats):
                events = self._compute_soccer_events(parser)

                game = {
                    'season': parser.get_season(),
                    'competition': parser.get_competition(),
                    'venue': parser.get_venue(),
                    'teams': parser.get_teams(),
                    'persons': parser.get_persons(),
                    'match_info': match_info,
                    'events': events,
                    'team_stats': parser.get_team_stats(),
                    'player_stats': player_stats
                }
        except Exception:
            raise OptaWebServiceError('Error while parsing F9 with params: {game}'.format(game=game_id))

        return game

    def get_rugby_game(self, game_id):
        game = None
        params = {'feed_type': 'RU7', 'game_id': game_id, 'user': self.user, 'psw': self.password}

        r = requests.get(self.f9_url, params=params)

        if 'response' in r.text:
            return game

        try:
            parser = OptaRU7Parser(r.content)
            rrml = parser.get_rrml()
            if rrml['status'] == 'Result':
                game = {
                    'rrml': rrml,
                    'events': parser.get_events(),
                    'official': parser.get_official(),
                    'teams': parser.get_teams(),
                    'players': parser.get_players(),
                    'team_stats': parser.get_team_stats(),
                    'player_stats': parser.get_player_stats()
                }
        except Exception:
            raise OptaWebServiceError('Error while parsing RU7 with params: {game}'.format(game=game_id))

        return game

    def get_soccer_squads(self, season_id, competition_id):
        params = {'feed_type': 'F40', 'user': self.user, 'psw': self.password, 'competition': competition_id,
                  'season_id': season_id}

        r = requests.get(self.f1_url, params=params)

        parser = OptaF40Parser(r.content)

        try:
            return parser.get_squads()
        except Exception as e:
            raise OptaWebServiceError(
                f'Error while parsing F40 with params {season_id} {competition_id}: {str(e)}')


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
