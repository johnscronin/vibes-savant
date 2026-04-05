# leaderboard_config.py - Column definitions for leaderboard views
# All db_column values verified against actual DB schema 2026-03-28

BATTING_STANDARD_COLS = [
    # (db_column, display_label, lower_is_better, format_type)
    # format_type: 'int', 'avg3' (.xxx), 'ops3' (x.xxx), 'pct1' (xx.x%), 'float2', 'float1'
    ('games',   'G',   False, 'int'),
    ('pa',      'PA',  False, 'int'),
    ('ab',      'AB',  False, 'int'),
    ('r',       'R',   False, 'int'),
    ('h',       'H',   False, 'int'),
    ('doubles', '2B',  False, 'int'),
    ('triples', '3B',  False, 'int'),
    ('hr',      'HR',  False, 'int'),
    ('rbi',     'RBI', False, 'int'),
    ('bb',      'BB',  False, 'int'),
    ('so',      'SO',  True,  'int'),
    ('avg',     'AVG', False, 'avg3'),
    ('obp',     'OBP', False, 'avg3'),
    ('slg',     'SLG', False, 'avg3'),
    ('ops',     'OPS', False, 'ops3'),
]

# Advanced batting - sourced from custom_stats
# All columns verified in custom_stats table
BATTING_ADVANCED_COLS = [
    ('ops_plus',       'OPS+',    False, 'int'),
    ('iso',            'ISO',     False, 'avg3'),
    ('bat_babip',      'BABIP',   False, 'avg3'),
    ('bb_pct',         'BB%',     False, 'pct1'),
    ('k_pct',          'K%',      True,  'pct1'),
    ('bb_k',           'BB/K',    False, 'float2'),
    ('ab_hr',          'AB/HR',   True,  'float1'),
    ('rc',             'RC',      False, 'float1'),
]

PITCHING_STANDARD_COLS = [
    # pitching_stats columns: g, gs, w, l, sv, ip, era, whip, k, opp_bb, ha, opp_hr, baa
    ('g',      'G',    False, 'int'),
    ('gs',     'GS',   False, 'int'),
    ('w',      'W',    False, 'int'),
    ('l',      'L',    True,  'int'),
    ('ip',     'IP',   False, 'float1'),
    ('era',    'ERA',  True,  'float2'),
    ('whip',   'WHIP', True,  'float2'),
    ('k',      'K',    False, 'int'),
    ('opp_bb', 'BB',   True,  'int'),
    ('ha',     'H',    True,  'int'),
    ('opp_hr', 'HR',   True,  'int'),
    ('baa',    'BAA',  True,  'avg3'),
]

# Advanced pitching - sourced from custom_stats
# custom_stats columns: era_plus, k_per_6, bb_per_6, pit_k_pct, pit_bb_pct, pit_babip, lob_pct
PITCHING_ADVANCED_COLS = [
    ('era_plus',  'ERA+',  False, 'int'),
    ('k_per_6',   'K/6',   False, 'float2'),
    ('bb_per_6',  'BB/6',  True,  'float2'),
    ('hr_per_6',  'HR/6',  True,  'float2'),
    ('pit_k_pct', 'K%',    False, 'pct1'),
    ('pit_bb_pct','BB%',   True,  'pct1'),
    ('pit_babip', 'BABIP', True,  'avg3'),
    ('lob_pct',   'LOB%',  False, 'pct1'),
]

# HQ batting - from hq_opponent_splits WHERE split_type='vs_hq_pitcher'
# hq_opponent_splits columns: pa, avg, obp, slg, ops, hr, bb_pct, k_pct, iso, babip
HQ_BATTING_COLS = [
    ('pa',     'PA',   False, 'int'),
    ('avg',    'AVG',  False, 'avg3'),
    ('obp',    'OBP',  False, 'avg3'),
    ('slg',    'SLG',  False, 'avg3'),
    ('ops',    'OPS',  False, 'ops3'),
    ('hr',     'HR',   False, 'int'),
    ('bb_pct', 'BB%',  False, 'pct1'),
    ('k_pct',  'K%',   True,  'pct1'),
    ('iso',    'ISO',  False, 'avg3'),
    ('babip',  'BABIP',False, 'avg3'),
]

# HQ pitching - from hq_opponent_splits WHERE split_type='vs_hq_hitter'
# hq_opponent_splits columns: bf, era, obp_against, baa, k_pct, bb_pct, k_per_6, bb_per_6
HQ_PITCHING_COLS = [
    ('bf',          'BF',   False, 'int'),
    ('era',         'ERA',  True,  'float2'),
    ('obp_against', 'OBP',  True,  'avg3'),
    ('baa',         'BAA',  True,  'avg3'),
    ('k_pct',       'K%',   False, 'pct1'),
    ('bb_pct',      'BB%',  True,  'pct1'),
    ('k_per_6',     'K/6',  False, 'float2'),
    ('bb_per_6',    'BB/6', True,  'float2'),
]
