OPTA = {
    'f9': {
        'matchinfo': {
            'write_policy': 'upsert',
            'meta': [
                ('id','VARCHAR(10)'),
                ('competition_id','VARCHAR(10)'),
                ('season_id','INTEGER'),
                ('type','VARCHAR(20)'),
                ('matchday','INTEGER'),
                ('weather','VARCHAR(20)'),
                ('attendance','INTEGER'),
                ('period','VARCHAR(20)'),
                ('date','TIMESTAMP'),
                ('pool','VARCHAR(10)'),
                ('round_name','VARCHAR(50)'),
                ('round_number','VARCHAR(10)'),
                ('venue_id','VARCHAR(10)'),
                ('match_official_id','VARCHAR(10)'),
                ('winner_id','VARCHAR(10)')
            ],
            'upsert_key': 'id',
            'target_table': 'soccer_matchinfo'
        },
        'event': {
            'write_policy': 'delete_bulk_insert',
            'meta': [
                ('event_id','VARCHAR(50)'),
                ('competition_id','VARCHAR(10)'),
                ('season_id','INTEGER'),
                ('match_id','VARCHAR(10)'),
                ('team_id','VARCHAR(10)'),
                ('player_id','VARCHAR(10)'),
                ('type','VARCHAR(50)'),
                ('minutes','INTEGER'),
                ('seconds','INTEGER'),
                ('description','VARCHAR(50)'),
                ('detail','VARCHAR(50)')
            ],
            'delete_keys': {},
            'target_table': 'soccer_event',
            'chunk_size': 500
        },
        'teamstat': {
            'write_policy': 'delete_bulk_insert',
            'meta': [
                ('competition_id','VARCHAR(10)'),
                ('season_id','INTEGER'),
                ('match_id','VARCHAR(10)'),
                ('team_id','VARCHAR(10)'),
                ('score','INTEGER'),
                ('shootout_score','INTEGER'),
                ('side','VARCHAR(10)'),
                ('formation_used','VARCHAR(11)'),
                ('official_id','VARCHAR(10)'),
                ('type','VARCHAR(50)'),
                ('fh','FLOAT'),
                ('sh','FLOAT'),
                ('efh','FLOAT'),
                ('esh','FLOAT'),
                ('value','FLOAT')
            ],
            'delete_keys': {},
            'target_table': 'soccer_teamstat',
            'chunk_size': 500
        },
        'playerstat': {
            'write_policy': 'delete_bulk_insert',
            'meta': [
                ('player_id','VARCHAR(10)'),
                ('competition_id','VARCHAR(10)'),
                ('season_id','INTEGER'),
                ('match_id','VARCHAR(10)'),
                ('team_id','VARCHAR(10)'),
                ('score','INTEGER'),
                ('shootout_score','INTEGER'),
                ('side','VARCHAR(10)'),
                ('formation_used','VARCHAR(11)'),
                ('official_id','VARCHAR(10)'),
                ('main_position','VARCHAR(20)'),
                ('sub_position','VARCHAR(20)'),
                ('shirt_number','INTEGER'),
                ('status','VARCHAR(20)'),
                ('captain','VARCHAR(10)'),
                ('type','VARCHAR(50)'),
                ('value','FLOAT'),
                ('formation_place','VARCHAR(11)')
            ],
            'delete_keys': {},
            'target_table': 'soccer_playerstat',
            'chunk_size': 500
        }
    },
    'f40': {
        'playerinfo': {
            'write_policy': 'upsert',
            'meta': [
                ('id', 'VARCHAR(10)'),
                ('first_name', 'VARCHAR(150)'),
                ('last_name', 'VARCHAR(150)'),
                ('known_name', 'VARCHAR(150)'),
                ('birth_date', 'VARCHAR(10)'),
                ('birth_place', 'VARCHAR(150)'),
                ('first_nationality', 'VARCHAR(50)'),
                ('preferred_foot', 'VARCHAR(10)'),
                ('weight', 'INTEGER'),
                ('height', 'INTEGER'),
                ('real_position', 'VARCHAR(50)'),
                ('real_position_side', 'VARCHAR(50)'),
                ('country', 'VARCHAR(50)')
            ],
            'target_table': 'soccer_playerinfo',
            'upsert_key': 'id'
        },
        'teaminfo': {
            'write_policy': 'upsert',
            'meta': [
                ('id', 'VARCHAR(10)'),
                ('name', 'VARCHAR(150)'),
                ('symid', 'VARCHAR(10)'),
                ('country', 'VARCHAR(50)'),
                ('country_iso', 'VARCHAR(2)'),
                ('country_id', 'VARCHAR(10)'),
                ('region_id', 'VARCHAR(10)'),
                ('region_name', 'VARCHAR(50)'),
                ('short_name', 'VARCHAR(50)'),
                ('venue_name', 'VARCHAR(150)'),
                ('venue_id', 'VARCHAR(10)')
            ],
            'target_table': 'soccer_teaminfo',
            'upsert_key': 'id'
        },
        'link': {
            'write_policy': 'upsert',
            'meta': [
                ('id', 'VARCHAR(50)'),
                ('competition_id', 'VARCHAR(10)'),
                ('season_id', 'INTEGER'),
                ('team_id', 'VARCHAR(10)'),
                ('player_id', 'VARCHAR(10)'),
                ('join_date', 'VARCHAR(10)')
            ],
            'target_table': 'soccer_link',
            'upsert_key': 'id'
        }
    },
    'ru7': {
        'matchinfo': {
            'write_policy': 'upsert',
            'meta': [
                ('id','VARCHAR(10)'),
                ('competition_id','VARCHAR(10)'),
                ('season_id','VARCHAR(10)'),
                ('date','TIMESTAMP'),
                ('venue_id','VARCHAR(10)'),
                ('group_id','VARCHAR(20)'),
                ('group_name','VARCHAR(20)'),
                ('round','VARCHAR(10)')
            ],
            'upsert_key': 'id',
            'target_table': 'rugby_matchinfo'
        },
        'matchscore': {
            'write_policy': 'upsert',
            'meta': [
                ('id','VARCHAR(10)'),
                ('attendance','INTEGER'),
                ('home_ht_score','INTEGER'),
                ('home_score','INTEGER'),
                ('away_ht_score','INTEGER'),
                ('away_score','INTEGER')
            ],
            'upsert_key': 'id',
            'target_table': 'rugby_matchscore'
        },
        'event': {
            'write_policy': 'delete_bulk_insert',
            'meta': [
                ('match_id','VARCHAR(10)'),
                ('minutes','INTEGER'),
                ('seconds','INTEGER'),
                ('player_id','VARCHAR(10)'),
                ('team_id','VARCHAR(10)'),
                ('type','VARCHAR(50)'),
                ('temporary','VARCHAR(50)')
            ],
            'delete_keys': {},
            'target_table': 'rugby_event',
            'chunk_size': 500
        },
        'teamstat': {
            'write_policy': 'delete_bulk_insert',
            'meta': [
                ('match_id','VARCHAR(10)'),
                ('team_id','VARCHAR(10)'),
                ('side','VARCHAR(10)'),
                ('type','VARCHAR(50)'),
                ('value','FLOAT')
            ],
            'delete_keys': {},
            'target_table': 'rugby_teamstat',
            'chunk_size': 500
        },
        'playerstat': {
            'write_policy': 'delete_bulk_insert',
            'meta': [
                ('match_id','VARCHAR(10)'),
                ('team_id','VARCHAR(10)'),
                ('player_id','VARCHAR(10)'),
                ('position_name','VARCHAR(20)'),
                ('position_id','VARCHAR(2)'),
                ('side','VARCHAR(10)'),
                ('type','VARCHAR(50)'),
                ('value','FLOAT')
            ],
            'delete_keys': {},
            'target_table': 'rugby_playerstat',
            'chunk_size': 500
        }
    }
}

LABEL = {
    'write_policy': 'upsert',
    'meta': [
        ('id','VARCHAR(10)'),
        ('label','VARCHAR(150)')
    ],
    'upsert_key': 'id',
    'target_table': 'label'
}
