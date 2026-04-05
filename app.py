# Vibes Front Office Tool — expandable to full HRL analytics. See config.py to add teams.
import sqlite3
import os
import json
import re
from flask import Flask, render_template, jsonify, request, abort

# ── Config ────────────────────────────────────────────────────────────────────
TEAM_SLUG = 'vibes'
TEAM_NAME = 'Vibes'
TEAM_COLORS = {'primary': '#99c9ea', 'accent': '#d5539b'}
MASCOT_URL = 'https://i.ibb.co/m5cJfRtN/TOASTYPRIMARY.png'

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]

DISPLAY_NAMES = {
    "FishHook": "Fish Hook",
    "HuckFinn": "Huck Finn",
}

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')

# ── Search index (built lazily on first use) ──────────────────────────────────
_search_index = None   # None = not yet built; [] = built but empty

def build_search_index():
    global _search_index
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT hashtag, nickname, team_name, pic_url, last_year
            FROM players ORDER BY last_year DESC NULLS LAST, hashtag
        """).fetchall()
        conn.close()
        _search_index = [{
            'slug':         r['hashtag'],
            'name':         r['nickname'] or r['hashtag'],
            'display_name': DISPLAY_NAMES.get(r['hashtag'], r['nickname'] or r['hashtag']),
            'team':         r['team_name'] or '',
            'pic_url':      fix_pic_url(r['pic_url']) if r['pic_url'] else MASCOT_URL,
            'last_year':    r['last_year'] or 0,
            'search_key':   (r['hashtag'] + ' ' + (r['nickname'] or '') + ' ' + (r['team_name'] or '')).lower(),
        } for r in rows]
    except Exception as e:
        _search_index = []

def get_search_index():
    global _search_index
    if _search_index is None:
        build_search_index()
    return _search_index or []

@app.context_processor
def inject_globals():
    return {'display_names': DISPLAY_NAMES, 'mascot_url': MASCOT_URL}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fix_pic_url(url):
    if not url:
        return MASCOT_URL
    url = url.replace('https://hrltwincities.com~/', 'https://hrltwincities.com/')
    if url.startswith('/static'):
        return url  # Flask serves static files directly
    if url.startswith('/'):
        return 'https://hrltwincities.com' + url
    return url


def percentile_threshold(values, p):
    """Return the p-th percentile of a sorted list (linear interpolation)."""
    if not values:
        return None
    n = len(values)
    idx = (p / 100) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return values[lo] + frac * (values[hi] - values[lo])


def compute_league_thresholds(conn):
    """
    Compute top-10% (p90) and bottom-10% (p10) thresholds for batting and pitching
    stats per season, from league_batting_stats and league_pitching_stats.

    For inverted stats (lower = better), p10 is the threshold for 'top 10%' (low end).
    We store both p10 and p90 always; the template decides which is the 'good' end.
    'rare' flag: True when median == 0 (e.g. triples, saves) — only top-10% shading.
    Returns: {season: {'batting': {stat: {'p10': v, 'p90': v, 'rare': bool}}, 'pitching': {...}}}
    """
    thresholds = {}

    # Batting stats to threshold
    bat_stats = [
        ('avg', 'avg'), ('obp', 'obp'), ('slg', 'slg'), ('ops', 'ops'),
        ('hr', 'hr'), ('rbi', 'rbi'), ('r', 'r'), ('h', 'h'),
        ('doubles', 'doubles'), ('triples', 'triples'), ('bb', 'bb'), ('so', 'so'),
        # Computed stats
        ('ab_hr', 'ROUND(ab*1.0/NULLIF(hr,0),1)'),
        ('bb_pct', 'ROUND(bb*1.0/NULLIF(ab+bb,0),3)'),
        ('k_pct',  'ROUND(so*1.0/NULLIF(ab+bb,0),3)'),
        ('bb_k',   'ROUND(bb*1.0/NULLIF(so,0),2)'),
        ('iso',    'ROUND(slg-avg,3)'),
        ('bat_babip', 'ROUND((h-hr)*1.0/NULLIF(ab-so-hr,0),3)'),
    ]

    # Pitching stats to threshold
    pit_stats = [
        ('era', 'era'), ('whip', 'whip'), ('baa', 'baa'),
        ('k_per_6', 'k_per_6'), ('bb_per_6', 'bb_per_6'), ('ip', 'ip'),
        ('k', 'k'), ('h', 'h'), ('bb', 'bb'), ('hr', 'hr'),
        ('sv', 'sv'),
        # Computed
        ('pit_k_pct',  'ROUND(k*1.0/NULLIF(ip*3.0+h+bb,0),3)'),
        ('pit_bb_pct', 'ROUND(bb*1.0/NULLIF(ip*3.0+h+bb,0),3)'),
        ('pit_babip',  'ROUND((h-hr)*1.0/NULLIF(ip*3+h-k-hr,0),3)'),
        ('lob_pct', 'ROUND((h+bb-r)*1.0/NULLIF(h+bb-1.4*hr,0),3)'),
        ('hr_per_6', 'ROUND(hr*6.0/NULLIF(ip,0),2)'),
    ]

    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM league_batting_stats ORDER BY season"
    ).fetchall()]

    for season in seasons:
        thresholds[season] = {'batting': {}, 'pitching': {}}

        for key, expr in bat_stats:
            rows = conn.execute(
                f"SELECT {expr} FROM league_batting_stats WHERE season=? AND ({expr}) IS NOT NULL",
                (season,)
            ).fetchall()
            vals = sorted(r[0] for r in rows if r[0] is not None)
            if vals:
                n = len(vals)
                median = vals[n // 2] if n % 2 else (vals[n//2 - 1] + vals[n//2]) / 2
                rare = (median == 0)
                entry = {
                    'p10': percentile_threshold(vals, 10),
                    'p90': percentile_threshold(vals, 90),
                    'rare': rare,
                }
                # For rare stats, p90 must be > 0 (otherwise don't highlight)
                if rare and (entry['p90'] or 0) <= 0:
                    entry['p90'] = None
                thresholds[season]['batting'][key] = entry

        pit_seasons = [r[0] for r in conn.execute(
            "SELECT DISTINCT season FROM league_pitching_stats ORDER BY season"
        ).fetchall()]
        if season in pit_seasons:
            for key, expr in pit_stats:
                rows = conn.execute(
                    f"SELECT {expr} FROM league_pitching_stats WHERE season=? AND ({expr}) IS NOT NULL AND ip > 0",
                    (season,)
                ).fetchall()
                vals = sorted(r[0] for r in rows if r[0] is not None)
                if vals:
                    n = len(vals)
                    median = vals[n // 2] if n % 2 else (vals[n//2 - 1] + vals[n//2]) / 2
                    rare = (median == 0)
                    entry = {
                        'p10': percentile_threshold(vals, 10),
                        'p90': percentile_threshold(vals, 90),
                        'rare': rare,
                    }
                    if rare and (entry['p90'] or 0) <= 0:
                        entry['p90'] = None
                    thresholds[season]['pitching'][key] = entry

    return thresholds


# ── Home ──────────────────────────────────────────────────────────────────────
FEMALE_PLAYERS = ['Mounds', 'Twizzler', 'Half Pint']
DEFECTS = [
    "he prefers Hopkins to Eagan",
    "he didn't vote Kmart for Hall of Fame",
    "he plays adult recreational wiffleball",
    "he doesn't vote for season awards",
    "he never goes to postgames",
    "he hustles out triples against the Lugnuts",
    "he thinks Valley is a playable field with all its rocks and syringes",
    "he doesn't donate to the Polar Plunge",
    "he isn't subscribed to the HRL Youtube channel",
    "he never rakes the boxes after the game",
]

def _build_jonah_lines(name, team):
    import random as _random
    import re as _re
    is_female = name in FEMALE_PLAYERS
    pronoun = "she" if is_female else "he"
    defect_raw = _random.choice(DEFECTS)
    if is_female:
        feminized = _re.sub(r'\bhis\b', 'her', defect_raw)
        feminized = _re.sub(r'\bhe\b', 'she', feminized)
        defect_full = "Her defect is that " + feminized
    else:
        defect_full = "His defect is that " + defect_raw
    return {
        'jonah_line1': f"Billy, this is {name},",
        'jonah_line2': f"{pronoun} plays for the {team}.",
        'jonah_defect': defect_full,
        'pronoun': pronoun,
    }

@app.route('/')
@app.route(f'/team/{TEAM_SLUG}/')
def home_moneyball():
    import random as _random
    conn = get_db()
    candidates = conn.execute('''
        SELECT DISTINCT
            bs.player_hashtag AS slug,
            COALESCE(p.nickname, bs.player_hashtag) AS name,
            COALESCE(p.team_name, bs.team_name) AS team
        FROM batting_stats bs
        LEFT JOIN players p ON p.hashtag = bs.player_hashtag
        WHERE bs.season BETWEEN 2004 AND 2025
          AND bs.pa > 0
        GROUP BY bs.player_hashtag
        HAVING COUNT(DISTINCT bs.season) >= 2
    ''').fetchall()
    conn.close()
    if not candidates:
        return redirect(url_for('leaderboard'))
    player = _random.choice(candidates)
    team = player['team'] or 'HRL Twin Cities'
    jonah = _build_jonah_lines(player['name'], team)
    return render_template('home_moneyball.html',
                           random_slug=player['slug'],
                           random_name=player['name'],
                           random_team=team,
                           **jonah)


@app.route('/home-classic')
@app.route(f'/team/{TEAM_SLUG}/home-classic')
def index():
    conn = get_db()

    batting_leaders = conn.execute("""
        SELECT b.player_hashtag, p.pic_url, b.season, b.team_name,
               b.games, b.pa, b.ab, b.h, b.hr, b.rbi, b.r, b.bb, b.so,
               b.avg, b.obp, b.slg, b.ops, b.doubles, b.triples, b.singles
        FROM batting_stats b
        JOIN players p ON p.player_id = b.player_id
        WHERE b.season = 2025 AND b.player_hashtag IN ({})
          AND b.ab >= 10
        ORDER BY b.hr DESC
    """.format(','.join('?' * len(VIBES_PLAYERS))), VIBES_PLAYERS).fetchall()

    career_bat = conn.execute("""
        SELECT b.player_hashtag,
               SUM(b.games) as g, SUM(b.ab) as ab, SUM(b.h) as h,
               SUM(b.hr) as hr, SUM(b.rbi) as rbi, SUM(b.r) as r,
               CASE WHEN SUM(b.ab) > 0
                    THEN ROUND(CAST(SUM(b.h) AS REAL)/SUM(b.ab), 3) ELSE 0 END as avg
        FROM batting_stats b
        WHERE b.player_hashtag IN ({})
        GROUP BY b.player_hashtag
    """.format(','.join('?' * len(VIBES_PLAYERS))), VIBES_PLAYERS).fetchall()
    career_map = {r['player_hashtag']: dict(r) for r in career_bat}

    batting_leaders = [dict(r) for r in batting_leaders]
    for row in batting_leaders:
        row['pic_url'] = fix_pic_url(row['pic_url'])

    # 2025 custom stats for advanced leaderboard
    custom_2025 = conn.execute("""
        SELECT player_name, ops_plus, era_plus, ab_hr, bb_k,
               k_per_6, bb_per_6, bb_pct, k_pct, iso, bat_babip, rc,
               pit_k_pct, pit_bb_pct, pit_babip, lob_pct, hr_per_6
        FROM custom_stats WHERE season=2025
    """).fetchall()
    custom_map = {r['player_name']: dict(r) for r in custom_2025}

    players = conn.execute(
        "SELECT * FROM players WHERE hashtag IN ({}) ORDER BY hashtag".format(
            ','.join('?' * len(VIBES_PLAYERS))), VIBES_PLAYERS
    ).fetchall()
    players = [dict(p) for p in players]
    for p in players:
        p['pic_url'] = fix_pic_url(p['pic_url'])

    # Percentile data for home page batting leaders (2025)
    home_pct_rows = conn.execute("""
        SELECT player_name, stat_name, percentile
        FROM percentile_rankings
        WHERE season=2025 AND stat_type IN ('batting','custom')
        AND player_name IN ({})
    """.format(','.join('?' * len(VIBES_PLAYERS))), VIBES_PLAYERS).fetchall()
    home_pct_map = {}
    for r in home_pct_rows:
        if r['player_name'] not in home_pct_map:
            home_pct_map[r['player_name']] = {}
        home_pct_map[r['player_name']][r['stat_name']] = r['percentile']

    conn.close()
    return render_template('index.html',
                           batting_leaders=batting_leaders,
                           career_map=career_map,
                           custom_map=custom_map,
                           players=players,
                           home_pct_map=home_pct_map,
                           mascot_url=MASCOT_URL)


# ── Player page ───────────────────────────────────────────────────────────────
@app.route('/player/<name>')
@app.route(f'/team/{TEAM_SLUG}/player/<name>')
def player(name):
    embed = request.args.get('embed', 'false').lower() == 'true'
    conn = get_db()
    player_row = conn.execute("SELECT * FROM players WHERE hashtag=?", (name,)).fetchone()
    if not player_row:
        conn.close()
        return render_template('404.html', query=name, mascot_url=MASCOT_URL,
                               display_names=DISPLAY_NAMES), 404

    player_data = dict(player_row)
    player_data['pic_url'] = fix_pic_url(player_data['pic_url'])
    pid = player_data['player_id']

    # Use player_hashtag as fallback when player_id is NULL (league-only players)
    if pid:
        batting = [dict(r) for r in conn.execute(
            "SELECT * FROM batting_stats WHERE player_id=? ORDER BY season", (pid,)
        ).fetchall()]
        if not batting:  # fallback if player_id query returns nothing
            batting = [dict(r) for r in conn.execute(
                "SELECT * FROM batting_stats WHERE player_hashtag=? ORDER BY season", (name,)
            ).fetchall()]
    else:
        batting = [dict(r) for r in conn.execute(
            "SELECT * FROM batting_stats WHERE player_hashtag=? ORDER BY season", (name,)
        ).fetchall()]

    if pid:
        pitching = [dict(r) for r in conn.execute(
            "SELECT * FROM pitching_stats WHERE player_id=? ORDER BY season", (pid,)
        ).fetchall()]
        if not pitching:
            pitching = [dict(r) for r in conn.execute(
                "SELECT * FROM pitching_stats WHERE player_hashtag=? ORDER BY season", (name,)
            ).fetchall()]
    else:
        pitching = [dict(r) for r in conn.execute(
            "SELECT * FROM pitching_stats WHERE player_hashtag=? ORDER BY season", (name,)
        ).fetchall()]
    pitching_active = [p for p in pitching if p.get('ip') and p['ip'] > 0]

    if pid:
        fielding = [dict(r) for r in conn.execute(
            "SELECT * FROM fielding_stats WHERE player_id=? ORDER BY season", (pid,)
        ).fetchall()]
        if not fielding:
            fielding = [dict(r) for r in conn.execute(
                "SELECT * FROM fielding_stats WHERE player_hashtag=? ORDER BY season", (name,)
            ).fetchall()]
    else:
        fielding = [dict(r) for r in conn.execute(
            "SELECT * FROM fielding_stats WHERE player_hashtag=? ORDER BY season", (name,)
        ).fetchall()]

    if pid and batting and batting[0].get('player_id') == pid:
        bat_where = "player_id=?"
        bat_param = pid
    else:
        bat_where = "player_hashtag=?"
        bat_param = name

    career_bat = dict(conn.execute(f"""
        SELECT SUM(games) g, SUM(ab) ab, SUM(h) h, SUM(hr) hr, SUM(rbi) rbi,
               SUM(r) r, SUM(bb) bb, SUM(so) so, SUM(pa) pa,
               SUM(doubles) doubles, SUM(triples) triples,
               CASE WHEN SUM(ab)>0 THEN ROUND(CAST(SUM(h) AS REAL)/SUM(ab),3) ELSE 0 END avg,
               COUNT(*) seasons
        FROM batting_stats WHERE {bat_where}
    """, (bat_param,)).fetchone())

    if pid and pitching:
        pit_where = "player_id=?"
        pit_param = pid
    else:
        pit_where = "player_hashtag=?"
        pit_param = name

    career_pit = dict(conn.execute(f"""
        SELECT SUM(g) g, SUM(ip) ip, SUM(w) w, SUM(l) l, SUM(sv) sv,
               SUM(k) k, SUM(ha) ha, SUM(opp_bb) bb, SUM(opp_r) opp_r,
               CASE WHEN SUM(ip)>0 THEN ROUND(SUM(opp_r*6.0)/SUM(ip),2) ELSE NULL END era,
               CASE WHEN SUM(ip)>0 THEN ROUND((SUM(ha)+SUM(opp_bb))/SUM(ip),2) ELSE NULL END whip,
               COUNT(*) seasons
        FROM pitching_stats WHERE {pit_where} AND ip>0
    """, (pit_param,)).fetchone())

    if pid and fielding:
        fld_where = "player_id=?"
        fld_param = pid
    else:
        fld_where = "player_hashtag=?"
        fld_param = name

    career_fld = dict(conn.execute(f"""
        SELECT SUM(chances) chances, SUM(put_outs) po, SUM(errors) errors,
               CASE WHEN SUM(chances)>0
                    THEN ROUND(CAST(SUM(chances)-SUM(errors) AS REAL)/SUM(chances),3)
                    ELSE NULL END fld_pct
        FROM fielding_stats WHERE {fld_where}
    """, (fld_param,)).fetchone())

    # Custom stats per season
    custom_seasons = conn.execute(
        "SELECT season, ops_plus, era_plus, ab_hr, bb_k, "
        "bb_pct, k_pct, iso, bat_babip, rc, "
        "k_per_6, bb_per_6, pit_k_pct, pit_bb_pct, pit_babip, lob_pct, hr_per_6, "
        "bat_qualified, pit_qualified "
        "FROM custom_stats WHERE player_name=? ORDER BY season",
        (name,)
    ).fetchall()
    custom_seasons = [dict(r) for r in custom_seasons]

    # Career OPS+ (weighted avg by PA for qualified seasons)
    career_ops_plus = conn.execute("""
        SELECT CASE WHEN SUM(bs.pa) > 0
               THEN ROUND(SUM(cs.ops_plus * bs.pa) / SUM(bs.pa))
               ELSE NULL END
        FROM custom_stats cs
        JOIN batting_stats bs ON bs.player_hashtag=cs.player_name AND bs.season=cs.season
        WHERE cs.player_name=? AND cs.ops_plus IS NOT NULL AND cs.bat_qualified=1
    """, (name,)).fetchone()
    career_ops_plus = career_ops_plus[0] if career_ops_plus else None

    # Career ERA+ (weighted avg by IP for qualified seasons)
    career_era_plus = conn.execute("""
        SELECT CASE WHEN SUM(ps.ip) > 0
               THEN ROUND(SUM(cs.era_plus * ps.ip) / SUM(ps.ip))
               ELSE NULL END
        FROM custom_stats cs
        JOIN pitching_stats ps ON ps.player_hashtag=cs.player_name AND ps.season=cs.season
        WHERE cs.player_name=? AND cs.era_plus IS NOT NULL AND cs.pit_qualified=1 AND ps.ip > 0
    """, (name,)).fetchone()
    career_era_plus = career_era_plus[0] if career_era_plus else None

    # Seasons available for percentile display
    pct_seasons = [r[0] for r in conn.execute("""
        SELECT DISTINCT season FROM percentile_rankings
        WHERE player_name=? ORDER BY season DESC
    """, (name,)).fetchall()]

    # Most recent qualified season for default display
    default_season = None
    for s in pct_seasons:
        has_qual = conn.execute("""
            SELECT 1 FROM percentile_rankings
            WHERE player_name=? AND season=? AND qualified=1 AND percentile IS NOT NULL
            LIMIT 1
        """, (name, s)).fetchone()
        if has_qual:
            default_season = s
            break
    if not default_season and pct_seasons:
        default_season = pct_seasons[0]

    # Vibes seasons for highlight (only relevant for Vibes players)
    vibes_seasons = set(r[0] for r in conn.execute(
        "SELECT season FROM batting_stats WHERE player_id=? AND team_name='Vibes'", (pid,)
    ).fetchall()) if name in VIBES_PLAYERS else set()

    # League thresholds for top/bottom 10% cell highlighting
    league_thresholds = compute_league_thresholds(conn)

    # Check for duplicate display name players
    same_name_players = conn.execute(
        "SELECT hashtag, team_name, last_year FROM players WHERE nickname=? AND hashtag!=? ORDER BY last_year DESC",
        (player_data['nickname'], name)
    ).fetchall()
    same_name_players = [dict(r) for r in same_name_players]

    conn.close()
    return render_template('player.html',
                           player=player_data,
                           batting=batting,
                           pitching=pitching,
                           pitching_active=pitching_active,
                           fielding=fielding,
                           custom_seasons=custom_seasons,
                           career_bat=career_bat,
                           career_pit=career_pit,
                           career_fld=career_fld,
                           career_ops_plus=career_ops_plus,
                           career_era_plus=career_era_plus,
                           pct_seasons=pct_seasons,
                           default_season=default_season,
                           vibes_seasons=list(vibes_seasons),
                           all_players=VIBES_PLAYERS,
                           mascot_url=MASCOT_URL,
                           is_vibes=(name in VIBES_PLAYERS),
                           embed=embed,
                           league_thresholds=league_thresholds,
                           same_name_players=same_name_players)


# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.route('/leaderboard')
@app.route(f'/team/{TEAM_SLUG}/leaderboard')
def leaderboard():
    conn = get_db()
    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM batting_stats ORDER BY season DESC"
    ).fetchall()]
    conn.close()
    return render_template('leaderboard.html', seasons=seasons,
                           all_players=VIBES_PLAYERS, mascot_url=MASCOT_URL,
                           display_names=DISPLAY_NAMES)


# ── API: percentiles for a player/season ──────────────────────────────────────
@app.route('/api/player/<name>/percentiles')
def api_percentiles(name):
    season = request.args.get('season', type=int)
    if not season:
        return jsonify({'error': 'season required'}), 400

    conn = get_db()

    rows = conn.execute("""
        SELECT stat_name, stat_value, percentile, estimated_percentile, stat_type, qualified, pool_size, qualifier_text
        FROM percentile_rankings
        WHERE player_name=? AND season=?
        ORDER BY stat_type, stat_name
    """, (name, season)).fetchall()

    # Also fetch season qualifiers for DNQ tooltip
    qual = conn.execute(
        "SELECT batting_qualifier, batting_min_pa, pitching_qualifier, pitching_min_ip, pitching_min_g "
        "FROM season_qualifiers WHERE season=?", (season,)
    ).fetchone()

    # Player's PA and IP for DNQ context
    bat_pa = conn.execute(
        "SELECT pa FROM batting_stats WHERE player_hashtag=? AND season=?", (name, season)
    ).fetchone()
    pit_ip = conn.execute(
        "SELECT ip FROM pitching_stats WHERE player_hashtag=? AND season=?", (name, season)
    ).fetchone()
    pit_g = conn.execute(
        "SELECT g FROM pitching_stats WHERE player_hashtag=? AND season=?", (name, season)
    ).fetchone()

    # Count qualified players this season for context note
    bat_min_pa = qual[1] if qual else None
    pit_min_ip = qual[3] if qual else None
    pit_min_g  = qual[4] if qual else None
    if bat_min_pa:
        qual_bat_count = conn.execute(
            "SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats WHERE season=? AND pa>=?",
            (season, bat_min_pa)
        ).fetchone()[0]
    else:
        qual_bat_count = None
    if pit_min_ip and pit_min_g:
        qual_pit_count = conn.execute(
            "SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE season=? AND ip>0 AND (ip>=? OR g>=?)",
            (season, pit_min_ip, pit_min_g)
        ).fetchone()[0]
    elif pit_min_ip:
        qual_pit_count = conn.execute(
            "SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE season=? AND ip>=?",
            (season, pit_min_ip)
        ).fetchone()[0]
    else:
        qual_pit_count = None

    # HQ pitcher metadata for selected season
    hq_pit_row = conn.execute("""
        SELECT cutoff_whip, range_min_whip, range_max_whip, COUNT(*) as hq_count, source, notes
        FROM hq_pitchers WHERE season=? AND is_hq=1
    """, (season,)).fetchone()

    # HQ batter metadata for selected season
    hq_bat_row = conn.execute("""
        SELECT cutoff_ops, range_min_ops, range_max_ops, COUNT(*) as hq_count
        FROM hq_batters WHERE season=? AND is_hq=1
    """, (season,)).fetchone()

    # HQ batting split for this player (PA vs HQ pitchers)
    hq_bat_split = conn.execute("""
        SELECT pa, ops, obp, avg, slg, qualifies, total_hq_opponents
        FROM hq_opponent_splits
        WHERE player_name=? AND season=? AND split_type='vs_hq_pitcher'
    """, (name, season)).fetchone()

    # HQ pitching split for this player (BF vs HQ batters)
    hq_pit_split = conn.execute("""
        SELECT bf, era, obp_against, baa, k_per_6, bb_per_6, qualifies, total_hq_opponents
        FROM hq_opponent_splits
        WHERE player_name=? AND season=? AND split_type='vs_hq_hitter'
    """, (name, season)).fetchone()

    result = {
        'stats': [dict(r) for r in rows],
        'qualifiers': dict(qual) if qual else {},
        'player_pa': bat_pa[0] if bat_pa else None,
        'player_ip': pit_ip[0] if pit_ip else None,
        'player_g': pit_g[0] if pit_g else None,
        'qualified_batter_count': qual_bat_count,
        'qualified_pitcher_count': qual_pit_count,
        # HQ pitcher context
        'hq_pitcher_cutoff_whip': hq_pit_row['cutoff_whip'] if hq_pit_row else None,
        'hq_pitcher_range_min': hq_pit_row['range_min_whip'] if hq_pit_row else None,
        'hq_pitcher_range_max': hq_pit_row['range_max_whip'] if hq_pit_row else None,
        'hq_pitcher_count': hq_pit_row['hq_count'] if hq_pit_row else None,
        'hq_pitcher_source': hq_pit_row['source'] if hq_pit_row else 'whip_calculated',
        'hq_pitcher_notes': hq_pit_row['notes'] if hq_pit_row else None,
        # HQ batter context
        'hq_batter_cutoff_ops': hq_bat_row['cutoff_ops'] if hq_bat_row else None,
        'hq_batter_range_min': hq_bat_row['range_min_ops'] if hq_bat_row else None,
        'hq_batter_range_max': hq_bat_row['range_max_ops'] if hq_bat_row else None,
        'hq_batter_count': hq_bat_row['hq_count'] if hq_bat_row else None,
        # HQ split summaries
        'vs_hq_pitcher': dict(hq_bat_split) if hq_bat_split else None,
        'vs_hq_hitter': dict(hq_pit_split) if hq_pit_split else None,
        # HQ pool sizes for subtitle display
        'hq_batting_pool_size': conn.execute(
            "SELECT COUNT(DISTINCT player_name) FROM percentile_rankings WHERE stat_type='vs_hq_pitcher' AND season=? AND qualified=1",
            (season,)
        ).fetchone()[0],
        'hq_pitching_pool_size': conn.execute(
            "SELECT COUNT(DISTINCT player_name) FROM percentile_rankings WHERE stat_type='vs_hq_hitter' AND season=? AND qualified=1",
            (season,)
        ).fetchone()[0],
    }
    conn.close()
    return jsonify(result)


# ── API: trend data for per-stat graph ────────────────────────────────────────
@app.route('/api/player/<name>/trend')
def api_trend(name):
    stat = request.args.get('stat', 'hr')
    conn = get_db()

    BATTING_COLS = {
        'avg': 'avg', 'obp': 'obp', 'slg': 'slg', 'ops': 'ops',
        'hr': 'hr', 'rbi': 'rbi', 'bb': 'bb', 'so': 'so', 'r': 'r'
    }
    PITCHING_COLS = {
        'era': 'era', 'whip': 'whip', 'w': 'w', 'k': 'k',
        'k_per_6': 'k_per_6', 'bb_per_6': 'opp_bb_per_6',
    }
    CUSTOM_COLS = {
        'ops_plus': 'ops_plus', 'era_plus': 'era_plus',
        'ab_hr': 'ab_hr', 'bb_k': 'bb_k',
        'bb_pct': 'bb_pct', 'k_pct': 'k_pct',
        'iso': 'iso', 'bat_babip': 'bat_babip', 'rc': 'rc',
        'pit_k_pct': 'pit_k_pct', 'pit_bb_pct': 'pit_bb_pct',
        'pit_babip': 'pit_babip', 'lob_pct': 'lob_pct', 'hr_per_6': 'hr_per_6',
    }
    # Batting stats that have a meaningful league avg in league_batting_stats
    LEAGUE_BAT_COLS = {'avg', 'obp', 'slg', 'ops', 'hr', 'rbi', 'bb', 'so', 'r'}
    # Pitching stats with league avg in league_pitching_stats
    LEAGUE_PIT_COLS = {'era', 'whip', 'k'}

    player_row = conn.execute("SELECT player_id FROM players WHERE hashtag=?", (name,)).fetchone()
    if not player_row:
        conn.close()
        return jsonify([])

    pid = player_row[0]
    data = []

    # Build league avg lookup for this stat
    league_avgs = {}
    if stat in BATTING_COLS and stat in LEAGUE_BAT_COLS:
        col = BATTING_COLS[stat]
        for r in conn.execute(
            f"SELECT season, ROUND(AVG({col}),3) FROM league_batting_stats "
            f"WHERE {col} IS NOT NULL AND season < 2026 GROUP BY season"
        ).fetchall():
            league_avgs[r[0]] = r[1]
    elif stat in PITCHING_COLS and stat in LEAGUE_PIT_COLS:
        lcol = {'era': 'era', 'whip': 'whip', 'k': 'k'}[stat]
        for r in conn.execute(
            f"SELECT season, ROUND(AVG({lcol}),2) FROM league_pitching_stats "
            f"WHERE {lcol} IS NOT NULL AND season < 2026 GROUP BY season"
        ).fetchall():
            league_avgs[r[0]] = r[1]
    elif stat in ('ops_plus', 'era_plus'):
        # By definition league avg is always 100
        for r in conn.execute("SELECT DISTINCT season FROM league_batting_stats WHERE season < 2026").fetchall():
            league_avgs[r[0]] = 100

    if stat in BATTING_COLS:
        col = BATTING_COLS[stat]
        rows = conn.execute(
            f"SELECT season, {col}, team_name FROM batting_stats "
            f"WHERE player_id=? AND season < 2026 ORDER BY season",
            (pid,)
        ).fetchall()
        for r in rows:
            data.append({'season': r[0], 'value': r[1], 'team': r[2],
                         'league_avg': league_avgs.get(r[0])})

    elif stat in PITCHING_COLS:
        col = PITCHING_COLS[stat]
        rows = conn.execute(
            f"SELECT season, {col}, team_name FROM pitching_stats "
            f"WHERE player_id=? AND ip>0 AND season < 2026 ORDER BY season",
            (pid,)
        ).fetchall()
        for r in rows:
            data.append({'season': r[0], 'value': r[1], 'team': r[2],
                         'league_avg': league_avgs.get(r[0])})

    elif stat in CUSTOM_COLS:
        col = CUSTOM_COLS[stat]
        rows = conn.execute(
            f"SELECT cs.season, cs.{col}, COALESCE(bs.team_name, ps.team_name, 'Unknown') as team "
            f"FROM custom_stats cs "
            f"LEFT JOIN batting_stats bs ON bs.player_hashtag=cs.player_name AND bs.season=cs.season "
            f"LEFT JOIN pitching_stats ps ON ps.player_hashtag=cs.player_name AND ps.season=cs.season AND ps.ip>0 "
            f"WHERE cs.player_name=? AND cs.season < 2026 ORDER BY cs.season",
            (name,)
        ).fetchall()
        for r in rows:
            data.append({'season': r[0], 'value': r[1], 'team': r[2],
                         'league_avg': league_avgs.get(r[0])})

    conn.close()
    return jsonify(data)


# ── API: league average by stat across all seasons ───────────────────────────
@app.route('/api/league_average/<stat>')
def api_league_average(stat):
    conn = get_db()
    BATTING_COLS = {'avg', 'obp', 'slg', 'ops', 'hr', 'rbi', 'bb', 'so', 'r'}
    PITCHING_COLS = {'era', 'whip', 'k'}
    result = {}
    if stat in BATTING_COLS:
        rows = conn.execute(
            f"SELECT season, ROUND(AVG({stat}),3) FROM league_batting_stats "
            f"WHERE {stat} IS NOT NULL AND season < 2026 GROUP BY season ORDER BY season"
        ).fetchall()
        result = {r[0]: r[1] for r in rows}
    elif stat in PITCHING_COLS:
        rows = conn.execute(
            f"SELECT season, ROUND(AVG({stat}),3) FROM league_pitching_stats "
            f"WHERE {stat} IS NOT NULL AND season < 2026 GROUP BY season ORDER BY season"
        ).fetchall()
        result = {r[0]: r[1] for r in rows}
    conn.close()
    return jsonify(result)


# ── API: splits data for player ──────────────────────────────────────────────
@app.route('/api/player/<name>/splits')
def api_splits(name):
    conn = get_db()

    # HQ batting splits (batter vs HQ pitchers)
    hq = conn.execute("""
        SELECT season, pa, ab, h, hr, rbi, bb, so, doubles, triples,
               avg, obp, slg, ops, qualifies, hq_definition,
               range_min, range_max, total_hq_opponents
        FROM hq_opponent_splits
        WHERE player_name=? AND split_type='vs_hq_pitcher'
        ORDER BY season
    """, (name,)).fetchall()

    # HQ pitching splits (pitcher vs HQ batters)
    hq_pitching = conn.execute("""
        SELECT season, pa, bf, era, obp_against, baa, k_per_6, bb_per_6,
               qualifies, hq_definition, range_min, range_max, total_hq_opponents
        FROM hq_opponent_splits
        WHERE player_name=? AND split_type='vs_hq_hitter'
        ORDER BY season
    """, (name,)).fetchall()

    # Batting tier splits
    tiers = conn.execute("""
        SELECT season, tier, pa, ab, h, hr, rbi, bb, so, doubles, triples,
               avg, obp, slg, ops
        FROM opponent_tier_splits
        WHERE player_name=? AND split_role='batting'
        ORDER BY season DESC, CASE tier WHEN 'Elite' THEN 1 WHEN 'Average' THEN 2 ELSE 3 END
    """, (name,)).fetchall()

    # Pitching tier splits
    pitching_tiers = conn.execute("""
        SELECT season, tier, pa, ab, h, hr, rbi, bb, so, doubles, triples,
               avg, obp, slg, ops
        FROM opponent_tier_splits
        WHERE player_name=? AND split_role='pitching'
        ORDER BY season DESC, CASE tier WHEN 'Elite' THEN 1 WHEN 'Average' THEN 2 ELSE 3 END
    """, (name,)).fetchall()

    # Playoff career totals from playoff_batting_stats (season=0 = career totals)
    po_row = conn.execute("""
        SELECT ab, h, hr, rbi, bb, so, doubles, triples,
               avg, obp, slg, ops,
               (ab + bb) AS pa
        FROM playoff_batting_stats
        WHERE player_name=? AND season=0
    """, (name,)).fetchone()

    po = None
    if po_row and (po_row['ab'] or 0) > 0:
        po = dict(po_row)

    # Playoff pitching career totals
    po_pit_row = conn.execute("""
        SELECT ip, era, whip, k, opp_bb, ha, baa, k_per_6
        FROM playoff_pitching_stats
        WHERE player_name=? AND season=0
    """, (name,)).fetchone()
    po_pitching = dict(po_pit_row) if po_pit_row else None

    # Splits percentiles: use new vs_hq_pitcher stat_type, fallback to legacy 'splits'
    pct_rows = conn.execute("""
        SELECT season, stat_name, percentile, pool_size FROM percentile_rankings
        WHERE player_name=? AND stat_type IN ('vs_hq_pitcher', 'splits')
        ORDER BY season DESC, stat_name
    """, (name,)).fetchall()
    # Group by season; prefer vs_hq_pitcher over legacy splits
    splits_pct = {}
    for r in pct_rows:
        s = r['season']
        sn = r['stat_name']
        if s not in splits_pct:
            splits_pct[s] = {}
        if sn not in splits_pct[s]:
            splits_pct[s][sn] = {
                'percentile': r['percentile'],
                'pool_size': r['pool_size'],
            }

    # Determine if player has batting / pitching stats at all (not just BvP-derived)
    has_batting = conn.execute(
        "SELECT 1 FROM batting_stats WHERE player_hashtag=? AND ab>0 LIMIT 1", (name,)
    ).fetchone() is not None
    has_pitching = conn.execute(
        "SELECT 1 FROM pitching_stats WHERE player_hashtag=? AND ip>0 LIMIT 1", (name,)
    ).fetchone() is not None

    # Include league thresholds for the latest season available in splits data
    all_seasons = sorted({r['season'] for r in hq} | {r['season'] for r in tiers}
                         | {r['season'] for r in hq_pitching} | {r['season'] for r in pitching_tiers})
    latest_season = max(all_seasons) if all_seasons else None
    split_thresholds = {}
    if latest_season:
        lt = compute_league_thresholds(conn)
        split_thresholds = lt.get(latest_season, {})

    # HQ pitcher source metadata per season
    hq_source_by_season = {}
    for s in all_seasons:
        src_row = conn.execute(
            "SELECT source, notes, cutoff_whip, range_min_whip, range_max_whip, COUNT(*) as cnt "
            "FROM hq_pitchers WHERE season=? AND is_hq=1",
            (s,)
        ).fetchone()
        if src_row:
            hq_source_by_season[s] = {
                'source': src_row['source'] or 'whip_calculated',
                'notes': src_row['notes'],
                'cutoff_whip': src_row['cutoff_whip'],
                'range_min_whip': src_row['range_min_whip'],
                'range_max_whip': src_row['range_max_whip'],
                'hq_pitcher_count': src_row['cnt'],
            }

    conn.close()
    return jsonify({
        'hq': [dict(r) for r in hq],
        'hq_pitching': [dict(r) for r in hq_pitching],
        'tiers': [dict(r) for r in tiers],
        'pitching_tiers': [dict(r) for r in pitching_tiers],
        'playoff': po,
        'playoff_pitching': po_pitching,
        'percentiles': splits_pct,
        'thresholds': split_thresholds,
        'has_batting': has_batting,
        'has_pitching': has_pitching,
        'hq_source_by_season': hq_source_by_season,
    })


# ── API: playoffs data for player ────────────────────────────────────────────
@app.route('/api/player/<name>/playoffs')
def api_playoffs(name):
    conn = get_db()
    MIN_PA = 25

    # Per-season playoff batting from playoff_batting_stats
    seasons = conn.execute("""
        SELECT season, g, ab, r, h, doubles, triples, hr, rbi, bb, so,
               avg, obp, slg, ops,
               (ab + bb) AS pa
        FROM playoff_batting_stats
        WHERE player_name=?
        ORDER BY season
    """, (name,)).fetchall()
    seasons = [dict(r) for r in seasons]

    # HQ splits (combined reg+playoff) — same as splits tab
    hq = conn.execute("""
        SELECT season, pa, ab, h, hr, rbi, bb, so, doubles, triples,
               avg, obp, slg, ops
        FROM hq_opponent_splits
        WHERE player_name=? AND split_type='vs_hq_pitcher'
        ORDER BY season
    """, (name,)).fetchall()

    # Build playoff percentile gauges using all Vibes players as pool
    STATS = [
        ('avg', True), ('obp', True), ('slg', True), ('ops', True),
        ('hr', True), ('rbi', True), ('bb', True), ('so', False),
    ]
    pool_rows = conn.execute("""
        SELECT avg, obp, slg, ops, hr, rbi, bb, so, (ab+bb) AS pa
        FROM playoff_batting_stats
        WHERE (ab + bb) >= ?
    """, (MIN_PA,)).fetchall()
    pool_rows = [dict(r) for r in pool_rows]
    pool_size = len(pool_rows)

    gauges = {}
    for row in seasons:
        s = str(row['season'])
        pa = row.get('pa') or 0
        qualified = pa >= MIN_PA
        season_gauges = {}
        for stat, higher_is_better in STATS:
            val = row.get(stat)
            pct = None
            ps = None
            if qualified and val is not None and pool_size >= 5:
                pool_vals = [r[stat] for r in pool_rows if r[stat] is not None]
                ps = len(pool_vals)
                if ps >= 5:
                    if higher_is_better:
                        worse = sum(1 for v in pool_vals if v < val)
                    else:
                        worse = sum(1 for v in pool_vals if v > val)
                    pct = max(1, min(99, round(worse / ps * 100)))
            season_gauges[stat] = {
                'stat_name': stat,
                'stat_value': val,
                'percentile': pct,
                'qualified': 1 if qualified else 0,
                'pool_size': ps,
                'stat_type': 'playoff',
                'qualifier_text': f'min {MIN_PA} PA',
            }
        gauges[s] = season_gauges

    # Playoff-specific thresholds from playoff_batting_stats pool
    po_stats = ['avg','obp','slg','ops','hr','rbi','bb','so']
    po_thresh = {}
    for stat in po_stats:
        vals = sorted(r[stat] for r in pool_rows if r.get(stat) is not None)
        if vals:
            n = len(vals)
            median = vals[n//2] if n%2 else (vals[n//2-1]+vals[n//2])/2
            po_thresh[stat] = {
                'p10': percentile_threshold(vals, 10),
                'p90': percentile_threshold(vals, 90),
                'rare': median == 0,
            }

    conn.close()
    return jsonify({
        'seasons': seasons,
        'hq': [dict(r) for r in hq],
        'gauges': gauges,
        'pool_size': pool_size,
        'thresholds': po_thresh,
    })


# ── API: leaderboards ────────────────────────────────────────────────────────
def _get_season_qualifiers(conn, season):
    """Return (bat_min_pa, pit_min_ip, pit_min_g) for a season string."""
    if season == 'career':
        return 200, 100.0, None
    q = conn.execute(
        "SELECT batting_min_pa, pitching_min_ip, pitching_min_g FROM season_qualifiers WHERE season=?",
        (int(season),)
    ).fetchone()
    if q:
        return q[0] or 100, q[1] or 37.0, q[2]
    return 100, 37.0, None


def _get_adv_map(conn, season_int):
    """Return dict of player_name -> custom_stats row for a season."""
    if not season_int:
        return {}
    rows = conn.execute("""
        SELECT player_name, ops_plus, era_plus, ab_hr, bb_k,
               k_per_6, bb_per_6, bb_pct, k_pct, iso, bat_babip, rc,
               pit_k_pct, pit_bb_pct, pit_babip, lob_pct, hr_per_6
        FROM custom_stats WHERE season=?
    """, (season_int,)).fetchall()
    return {r['player_name']: dict(r) for r in rows}


def _get_pct_map(conn, season_int, stat_types):
    """Return dict of player_name -> {stat_name: percentile} for given stat_types."""
    if not season_int:
        return {}
    placeholders = ','.join('?' * len(stat_types))
    rows = conn.execute(
        f"SELECT player_name, stat_name, percentile FROM percentile_rankings "
        f"WHERE season=? AND stat_type IN ({placeholders})",
        [season_int] + list(stat_types)
    ).fetchall()
    pct_map = {}
    for r in rows:
        if r['player_name'] not in pct_map:
            pct_map[r['player_name']] = {}
        pct_map[r['player_name']][r['stat_name']] = r['percentile']
    return pct_map


@app.route('/api/leaderboard/batting')
def api_leaderboard_batting():
    season = request.args.get('season', '2025')
    qualified_only = request.args.get('qualified_only', 'true').lower() != 'false'
    team_filter = request.args.get('team', '')
    conn = get_db()

    try:
        season_int = int(season) if season != 'career' else None
    except Exception:
        season_int = 2025

    bat_min_pa, pit_min_ip, _ = _get_season_qualifiers(conn, season)

    # Build WHERE
    where_parts = ["b.pa > 0"]
    params = []
    if season_int:
        where_parts.append("b.season=?")
        params.append(season_int)
    if team_filter:
        where_parts.append("b.team_name=?")
        params.append(team_filter)
    where = "WHERE " + " AND ".join(where_parts)

    if season == 'career':
        rows = conn.execute(f"""
            SELECT b.player_hashtag as player_name,
                   b.player_hashtag as player_slug,
                   COALESCE(p.pic_url,'') pic_url,
                   'Career' as team,
                   NULL as season,
                   SUM(b.games) games, SUM(b.pa) pa, SUM(b.ab) ab,
                   SUM(b.r) r, SUM(b.h) h,
                   SUM(b.doubles) doubles, SUM(b.triples) triples,
                   SUM(b.hr) hr, SUM(b.rbi) rbi, SUM(b.bb) bb, SUM(b.so) so,
                   CASE WHEN SUM(b.ab)>0 THEN ROUND(CAST(SUM(b.h) AS REAL)/SUM(b.ab),3) ELSE 0 END avg,
                   CASE WHEN (SUM(b.ab)+SUM(b.bb))>0 THEN ROUND(CAST(SUM(b.h)+SUM(b.bb) AS REAL)/(SUM(b.ab)+SUM(b.bb)),3) ELSE 0 END obp,
                   CASE WHEN SUM(b.ab)>0 THEN ROUND(CAST(SUM(b.singles)+SUM(b.doubles)*2+SUM(b.triples)*3+SUM(b.hr)*4 AS REAL)/SUM(b.ab),3) ELSE 0 END slg,
                   CASE WHEN SUM(b.ab)>0 THEN ROUND((CAST(SUM(b.h)+SUM(b.bb) AS REAL)/(SUM(b.ab)+SUM(b.bb)))+(CAST(SUM(b.singles)+SUM(b.doubles)*2+SUM(b.triples)*3+SUM(b.hr)*4 AS REAL)/SUM(b.ab)),3) ELSE 0 END ops,
                   CASE WHEN SUM(b.pa)>={bat_min_pa} THEN 1 ELSE 0 END is_qualified
            FROM batting_stats b
            LEFT JOIN players p ON p.hashtag=b.player_hashtag
            GROUP BY b.player_hashtag
            HAVING SUM(b.ab)>0
            ORDER BY ops DESC
        """).fetchall()
    else:
        rows = conn.execute(f"""
            SELECT b.player_hashtag as player_name,
                   b.player_hashtag as player_slug,
                   COALESCE(p.pic_url,'') pic_url,
                   b.team_name as team,
                   b.season,
                   b.games, b.pa, b.ab, b.r, b.h,
                   b.doubles, b.triples, b.hr, b.rbi, b.bb, b.so,
                   b.avg, b.obp, b.slg, b.ops,
                   CASE WHEN b.pa>={bat_min_pa} THEN 1 ELSE 0 END is_qualified
            FROM batting_stats b
            LEFT JOIN players p ON p.hashtag=b.player_hashtag
            {where}
            ORDER BY b.ops DESC
        """, params).fetchall()

    adv_map = _get_adv_map(conn, season_int)
    pct_map = _get_pct_map(conn, season_int, ('batting', 'custom'))

    # Get HQ meta
    hq_pit = conn.execute(
        "SELECT cutoff_whip, COUNT(*) as cnt FROM hq_pitchers WHERE season=? AND is_hq=1",
        (season_int or 2025,)
    ).fetchone()
    hq_bat = conn.execute(
        "SELECT cutoff_ops, COUNT(*) as cnt FROM hq_batters WHERE season=? AND is_hq=1",
        (season_int or 2025,)
    ).fetchone()

    players = []
    for r in rows:
        rd = dict(r)
        slug = rd.get('player_slug') or rd.get('player_name', '')
        pic = fix_pic_url(rd.get('pic_url') or '')
        adv = adv_map.get(slug, {})
        pct = pct_map.get(slug, {})
        is_q = bool(rd.get('is_qualified'))
        if qualified_only and not is_q:
            continue
        players.append({
            'player_name': slug,
            'player_slug': slug,
            'photo_url': pic,
            'team': rd.get('team', ''),
            'is_qualified': is_q,
            'standard': {
                'games': rd.get('games'), 'pa': rd.get('pa'), 'ab': rd.get('ab'),
                'r': rd.get('r'), 'h': rd.get('h'), 'doubles': rd.get('doubles'),
                'triples': rd.get('triples'), 'hr': rd.get('hr'), 'rbi': rd.get('rbi'),
                'bb': rd.get('bb'), 'so': rd.get('so'),
                'avg': rd.get('avg'), 'obp': rd.get('obp'),
                'slg': rd.get('slg'), 'ops': rd.get('ops'),
            },
            'advanced': {
                'ops_plus': adv.get('ops_plus'),
                'iso': adv.get('iso'),
                'bat_babip': adv.get('bat_babip'),
                'bb_pct': adv.get('bb_pct'),
                'k_pct': adv.get('k_pct'),
                'bb_k': adv.get('bb_k'),
                'ab_hr': adv.get('ab_hr'),
                'rc': adv.get('rc'),
            },
            'percentiles': pct,
        })

    players.sort(key=lambda x: (x['standard'].get('ops') or 0), reverse=True)

    conn.close()
    return jsonify({
        'season': season,
        'qualifier_pa': bat_min_pa,
        'total_players': len(players),
        'qualified_count': sum(1 for p in players if p['is_qualified']),
        'hq_meta': {
            'pitcher_cutoff_whip': hq_pit['cutoff_whip'] if hq_pit else None,
            'pitcher_count': hq_pit['cnt'] if hq_pit else 0,
            'batter_cutoff_ops': hq_bat['cutoff_ops'] if hq_bat else None,
            'batter_count': hq_bat['cnt'] if hq_bat else 0,
        },
        'players': players,
    })


# Legacy alias so old JS still works
@app.route('/api/leaderboard/batting-legacy')
def api_batting():
    season = request.args.get('season', 'career')
    qualified_only = request.args.get('qualified_only', 'true').lower() != 'false'
    conn = get_db()

    bat_min_pa, pit_min_ip, _ = _get_season_qualifiers(conn, season)

    if season == 'career':
        qual_expr = f"CASE WHEN SUM(b.pa)>={bat_min_pa} THEN 1 ELSE 0 END"
        having_clause = f"HAVING SUM(b.ab)>0{' AND SUM(b.pa)>=' + str(bat_min_pa) if qualified_only else ''}"
        rows = conn.execute(f"""
            SELECT b.player_hashtag,
                   COALESCE(p.pic_url, '') pic_url,
                   'Career' team_name,
                   SUM(b.games) games, SUM(b.ab) ab, SUM(b.h) h,
                   SUM(b.hr) hr, SUM(b.rbi) rbi, SUM(b.r) r,
                   SUM(b.bb) bb, SUM(b.so) so,
                   SUM(b.doubles) doubles, SUM(b.triples) triples,
                   SUM(b.singles) singles, SUM(b.pa) pa,
                   CASE WHEN SUM(b.ab)>0 THEN ROUND(CAST(SUM(b.h) AS REAL)/SUM(b.ab),3) ELSE 0 END avg,
                   CASE WHEN SUM(b.pa)>0 THEN ROUND(CAST(SUM(b.h)+SUM(b.bb) AS REAL)/SUM(b.pa),3) ELSE 0 END obp,
                   CASE WHEN SUM(b.ab)>0
                        THEN ROUND(CAST(SUM(b.singles)+SUM(b.doubles)*2+SUM(b.triples)*3+SUM(b.hr)*4 AS REAL)/SUM(b.ab),3)
                        ELSE 0 END slg,
                   CASE WHEN SUM(b.ab)>0
                        THEN ROUND((CAST(SUM(b.h)+SUM(b.bb) AS REAL)/SUM(b.pa))
                             + (CAST(SUM(b.singles)+SUM(b.doubles)*2+SUM(b.triples)*3+SUM(b.hr)*4 AS REAL)/SUM(b.ab)),3)
                        ELSE 0 END ops,
                   {qual_expr} is_qualified
            FROM batting_stats b
            LEFT JOIN players p ON p.hashtag=b.player_hashtag
            GROUP BY b.player_hashtag {having_clause}
            ORDER BY SUM(b.hr) DESC
        """).fetchall()
    else:
        qual_expr = f"CASE WHEN b.pa>={bat_min_pa} THEN 1 ELSE 0 END"
        where_extra = f" AND b.pa>={bat_min_pa}" if qualified_only else ""
        rows = conn.execute(f"""
            SELECT b.player_hashtag,
                   COALESCE(p.pic_url, '') pic_url,
                   b.team_name,
                   b.games, b.ab, b.h, b.hr, b.rbi, b.r,
                   b.bb, b.so, b.doubles, b.triples, b.singles, b.pa,
                   b.avg, b.obp, b.slg, b.ops,
                   {qual_expr} is_qualified
            FROM batting_stats b
            LEFT JOIN players p ON p.hashtag=b.player_hashtag
            WHERE b.season=? AND b.ab>0{where_extra}
            ORDER BY b.hr DESC
        """, (int(season),)).fetchall()

    if season == 'career':
        total_qualified = conn.execute(
            "SELECT COUNT(*) FROM (SELECT player_hashtag FROM batting_stats GROUP BY player_hashtag HAVING SUM(pa)>=?)",
            (bat_min_pa,)
        ).fetchone()
        total_all = conn.execute(
            "SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats WHERE ab>0"
        ).fetchone()
    else:
        total_qualified = conn.execute(
            "SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats WHERE season=? AND pa>=?",
            (int(season), bat_min_pa)
        ).fetchone()
        total_all = conn.execute(
            "SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats WHERE season=? AND ab>0",
            (int(season),)
        ).fetchone()

    meta = {
        'season': season,
        'batting_qualifier': bat_min_pa,
        'pitching_qualifier': pit_min_ip,
        'total_qualified_batters': total_qualified[0] if total_qualified else 0,
        'total_players': total_all[0] if total_all else 0,
        'hq_batting_qualifier': 15,
        'hq_pitching_qualifier': 15,
        'qualified_only': qualified_only,
    }

    data = []
    for r in rows:
        row = dict(r)
        row['pic_url'] = fix_pic_url(row.get('pic_url') or '')
        ab = row.get('ab') or 0
        hr = row.get('hr') or 0
        pa = row.get('pa') or 0
        so = row.get('so') or 0
        bb = row.get('bb') or 0
        row['ab_hr'] = round(ab / hr, 1) if hr > 0 else None
        row['k_pa']  = round(so / pa, 3) if pa > 0 else None
        row['bb_pa'] = round(bb / pa, 3) if pa > 0 else None
        data.append(row)

    conn.close()
    return jsonify({'data': data, 'meta': meta})


@app.route('/api/leaderboard/pitching')
def api_leaderboard_pitching():
    season = request.args.get('season', '2025')
    qualified_only = request.args.get('qualified_only', 'true').lower() != 'false'
    team_filter = request.args.get('team', '')
    conn = get_db()

    try:
        season_int = int(season) if season != 'career' else None
    except Exception:
        season_int = 2025

    bat_min_pa, pit_min_ip, pit_min_g = _get_season_qualifiers(conn, season)

    where_parts = ["p2.ip > 0"]
    params = []
    if season_int:
        where_parts.append("p2.season=?")
        params.append(season_int)
    if team_filter:
        where_parts.append("p2.team_name=?")
        params.append(team_filter)
    where = "WHERE " + " AND ".join(where_parts)

    if pit_min_g and season_int:
        qual_expr = f"CASE WHEN p2.ip>={pit_min_ip} OR p2.g>={pit_min_g} THEN 1 ELSE 0 END"
    elif season == 'career':
        qual_expr = f"CASE WHEN SUM(p2.ip)>={pit_min_ip} THEN 1 ELSE 0 END"
    else:
        qual_expr = f"CASE WHEN p2.ip>={pit_min_ip} THEN 1 ELSE 0 END"

    if season == 'career':
        rows = conn.execute(f"""
            SELECT p2.player_hashtag as player_name,
                   p2.player_hashtag as player_slug,
                   COALESCE(pl.pic_url,'') pic_url,
                   'Career' as team,
                   NULL as season,
                   SUM(p2.g) g, SUM(p2.gs) gs, SUM(p2.w) w, SUM(p2.l) l,
                   SUM(p2.ip) ip,
                   CASE WHEN SUM(p2.ip)>0 THEN ROUND(SUM(p2.opp_r*6.0)/SUM(p2.ip),2) ELSE NULL END era,
                   CASE WHEN SUM(p2.ip)>0 THEN ROUND((SUM(p2.ha)+SUM(p2.opp_bb))/SUM(p2.ip),2) ELSE NULL END whip,
                   SUM(p2.k) k, SUM(p2.opp_bb) opp_bb, SUM(p2.ha) ha, SUM(p2.opp_hr) opp_hr,
                   CASE WHEN SUM(p2.ip)>0 THEN ROUND(SUM(p2.ha)*1.0/SUM(p2.ip),3) ELSE NULL END baa,
                   {qual_expr} is_qualified
            FROM pitching_stats p2
            LEFT JOIN players pl ON pl.hashtag=p2.player_hashtag
            WHERE p2.ip>0
            GROUP BY p2.player_hashtag
            ORDER BY era ASC NULLS LAST
        """).fetchall()
    else:
        rows = conn.execute(f"""
            SELECT p2.player_hashtag as player_name,
                   p2.player_hashtag as player_slug,
                   COALESCE(pl.pic_url,'') pic_url,
                   p2.team_name as team,
                   p2.season,
                   p2.g, p2.gs, p2.w, p2.l, p2.ip,
                   p2.era, p2.whip, p2.k, p2.opp_bb, p2.ha, p2.opp_hr, p2.baa,
                   {qual_expr} is_qualified
            FROM pitching_stats p2
            LEFT JOIN players pl ON pl.hashtag=p2.player_hashtag
            {where}
            ORDER BY p2.era ASC NULLS LAST
        """, params).fetchall()

    adv_map = _get_adv_map(conn, season_int)
    pct_map = _get_pct_map(conn, season_int, ('pitching', 'custom'))

    players = []
    for r in rows:
        rd = dict(r)
        slug = rd.get('player_slug') or rd.get('player_name', '')
        pic = fix_pic_url(rd.get('pic_url') or '')
        adv = adv_map.get(slug, {})
        pct = pct_map.get(slug, {})
        is_q = bool(rd.get('is_qualified'))
        if qualified_only and not is_q:
            continue
        players.append({
            'player_name': slug,
            'player_slug': slug,
            'photo_url': pic,
            'team': rd.get('team', ''),
            'is_qualified': is_q,
            'standard': {
                'g': rd.get('g'), 'gs': rd.get('gs'), 'w': rd.get('w'), 'l': rd.get('l'),
                'ip': rd.get('ip'), 'era': rd.get('era'), 'whip': rd.get('whip'),
                'k': rd.get('k'), 'opp_bb': rd.get('opp_bb'), 'ha': rd.get('ha'),
                'opp_hr': rd.get('opp_hr'), 'baa': rd.get('baa'),
            },
            'advanced': {
                'era_plus': adv.get('era_plus'),
                'k_per_6': adv.get('k_per_6'),
                'bb_per_6': adv.get('bb_per_6'),
                'pit_k_pct': adv.get('pit_k_pct'),
                'pit_bb_pct': adv.get('pit_bb_pct'),
                'pit_babip': adv.get('pit_babip'),
                'lob_pct': adv.get('lob_pct'),
                'hr_per_6': adv.get('hr_per_6'),
            },
            'percentiles': pct,
        })

    # Sort ERA ascending (lower is better), None last
    players.sort(key=lambda x: (x['standard'].get('era') is None, x['standard'].get('era') or 999))

    conn.close()
    return jsonify({
        'season': season,
        'qualifier_ip': pit_min_ip,
        'qualifier_g': pit_min_g,
        'total_players': len(players),
        'qualified_count': sum(1 for p in players if p['is_qualified']),
        'players': players,
    })


# Legacy alias for old pitching route
@app.route('/api/leaderboard/pitching-legacy')
def api_pitching():
    season = request.args.get('season', 'career')
    qualified_only = request.args.get('qualified_only', 'true').lower() != 'false'
    conn = get_db()

    bat_min_pa, pit_min_ip, pit_min_g = _get_season_qualifiers(conn, season)

    if season == 'career':
        qual_expr = f"CASE WHEN SUM(p2.ip)>={pit_min_ip} THEN 1 ELSE 0 END"
        having_clause = f"HAVING SUM(p2.ip)>0{' AND SUM(p2.ip)>=' + str(pit_min_ip) if qualified_only else ''}"
        rows = conn.execute(f"""
            SELECT p2.player_hashtag,
                   COALESCE(pl.pic_url, '') pic_url,
                   'Career' team_name,
                   SUM(p2.g) g, SUM(p2.gs) gs, SUM(p2.w) w, SUM(p2.l) l, SUM(p2.sv) sv,
                   SUM(p2.ip) ip, SUM(p2.k) k, SUM(p2.ha) ha, SUM(p2.opp_bb) opp_bb,
                   SUM(p2.opp_hr) opp_hr, SUM(p2.opp_r) opp_r,
                   CASE WHEN SUM(p2.ip)>0 THEN ROUND(SUM(p2.opp_r*6.0)/SUM(p2.ip),2) ELSE NULL END era,
                   CASE WHEN SUM(p2.ip)>0 THEN ROUND((SUM(p2.ha)+SUM(p2.opp_bb))/SUM(p2.ip),2) ELSE NULL END whip,
                   CASE WHEN SUM(p2.ip)>0 THEN ROUND(SUM(p2.k)*6.0/SUM(p2.ip),2) ELSE NULL END k_per_6,
                   CASE WHEN SUM(p2.ip)>0 THEN ROUND(SUM(p2.ha)/SUM(p2.ip),3) ELSE NULL END baa,
                   {qual_expr} is_qualified
            FROM pitching_stats p2
            LEFT JOIN players pl ON pl.hashtag=p2.player_hashtag
            WHERE p2.ip>0
            GROUP BY p2.player_hashtag {having_clause}
            ORDER BY era ASC NULLS LAST
        """).fetchall()
    else:
        if pit_min_g:
            qual_expr = f"CASE WHEN p2.ip>={pit_min_ip} OR p2.g>={pit_min_g} THEN 1 ELSE 0 END"
            where_extra = f" AND (p2.ip>={pit_min_ip} OR p2.g>={pit_min_g})" if qualified_only else ""
        else:
            qual_expr = f"CASE WHEN p2.ip>={pit_min_ip} THEN 1 ELSE 0 END"
            where_extra = f" AND p2.ip>={pit_min_ip}" if qualified_only else ""
        rows = conn.execute(f"""
            SELECT p2.player_hashtag,
                   COALESCE(pl.pic_url, '') pic_url,
                   p2.team_name,
                   p2.g, p2.gs, p2.w, p2.l, p2.sv, p2.ip, p2.k, p2.ha,
                   p2.opp_bb, p2.opp_hr, p2.opp_r, p2.era, p2.whip, p2.k_per_6, p2.baa,
                   {qual_expr} is_qualified
            FROM pitching_stats p2
            LEFT JOIN players pl ON pl.hashtag=p2.player_hashtag
            WHERE p2.season=? AND p2.ip>0{where_extra}
            ORDER BY p2.era ASC NULLS LAST
        """, (int(season),)).fetchall()

    if pit_min_g and season != 'career':
        qual_count_row = conn.execute(
            f"SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE season=? AND ip>0 AND (ip>={pit_min_ip} OR g>={pit_min_g})",
            (int(season),)
        ).fetchone()
    elif season == 'career':
        qual_count_row = conn.execute(
            f"SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE ip>={pit_min_ip} AND ip>0"
        ).fetchone()
    else:
        qual_count_row = conn.execute(
            f"SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE season=? AND ip>={pit_min_ip}",
            (int(season),)
        ).fetchone()

    total_pit_row = conn.execute(
        "SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE season=? AND ip>0" if season != 'career'
        else "SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE ip>0",
        (int(season),) if season != 'career' else ()
    ).fetchone()

    meta = {
        'season': season,
        'batting_qualifier': bat_min_pa,
        'pitching_qualifier': pit_min_ip,
        'pitching_qualifier_g': pit_min_g,
        'total_qualified_pitchers': qual_count_row[0] if qual_count_row else 0,
        'total_pitchers': total_pit_row[0] if total_pit_row else 0,
        'hq_batting_qualifier': 15,
        'hq_pitching_qualifier': 15,
        'qualified_only': qualified_only,
    }

    data = []
    for r in rows:
        row = dict(r)
        row['pic_url'] = fix_pic_url(row.get('pic_url') or '')
        data.append(row)

    conn.close()
    return jsonify({'data': data, 'meta': meta})


@app.route('/api/leaderboard/hq-batting')
def api_leaderboard_hq_batting():
    season = request.args.get('season', '2025')
    qualified_only = request.args.get('qualified_only', 'true').lower() != 'false'
    team_filter = request.args.get('team', '')
    conn = get_db()

    try:
        season_int = int(season) if season != 'career' else 2025
    except Exception:
        season_int = 2025

    where_parts = ["split_type='vs_hq_pitcher'", "pa > 0"]
    params = [season_int]
    where_parts.insert(0, "season=?")
    if team_filter:
        # hq_opponent_splits doesn't have team_name, skip team filter
        pass
    where = "WHERE " + " AND ".join(where_parts)

    rows = conn.execute(f"""
        SELECT player_name, player_slug,
               pa, ab, h, hr, rbi, bb, so, doubles, triples,
               avg, obp, slg, ops, iso, babip,
               bb_pct, k_pct, bb_k,
               qualifies
        FROM hq_opponent_splits
        WHERE season=? AND split_type='vs_hq_pitcher' AND pa > 0
        ORDER BY ops DESC NULLS LAST
    """, (season_int,)).fetchall()

    pct_map = _get_pct_map(conn, season_int, ('vs_hq_pitcher',))

    # Get photo URLs
    pic_rows = conn.execute("SELECT hashtag, pic_url FROM players").fetchall()
    pic_map = {r['hashtag']: fix_pic_url(r['pic_url'] or '') for r in pic_rows}

    players = []
    for r in rows:
        rd = dict(r)
        slug = rd.get('player_slug') or rd.get('player_name', '')
        is_q = bool(rd.get('qualifies'))
        if qualified_only and not is_q:
            continue
        pct = pct_map.get(slug, pct_map.get(rd.get('player_name', ''), {}))
        pic = pic_map.get(slug, MASCOT_URL)
        players.append({
            'player_name': rd.get('player_name', slug),
            'player_slug': slug,
            'photo_url': pic,
            'team': '',
            'is_qualified': is_q,
            'stats': {
                'pa': rd.get('pa'), 'avg': rd.get('avg'), 'obp': rd.get('obp'),
                'slg': rd.get('slg'), 'ops': rd.get('ops'), 'hr': rd.get('hr'),
                'bb_pct': rd.get('bb_pct'), 'k_pct': rd.get('k_pct'),
                'iso': rd.get('iso'), 'babip': rd.get('babip'),
            },
            'percentiles': pct,
        })

    conn.close()
    return jsonify({
        'season': season,
        'qualifier_pa': 15,
        'total_players': len(players),
        'qualified_count': sum(1 for p in players if p['is_qualified']),
        'players': players,
    })


@app.route('/api/leaderboard/hq-pitching')
def api_leaderboard_hq_pitching():
    season = request.args.get('season', '2025')
    qualified_only = request.args.get('qualified_only', 'true').lower() != 'false'
    conn = get_db()

    try:
        season_int = int(season) if season != 'career' else 2025
    except Exception:
        season_int = 2025

    rows = conn.execute("""
        SELECT player_name, player_slug,
               bf, era, obp_against, baa, k_pct, bb_pct, k_per_6, bb_per_6,
               qualifies
        FROM hq_opponent_splits
        WHERE season=? AND split_type='vs_hq_hitter' AND bf > 0
        ORDER BY era ASC NULLS LAST
    """, (season_int,)).fetchall()

    pct_map = _get_pct_map(conn, season_int, ('vs_hq_hitter',))

    pic_rows = conn.execute("SELECT hashtag, pic_url FROM players").fetchall()
    pic_map = {r['hashtag']: fix_pic_url(r['pic_url'] or '') for r in pic_rows}

    players = []
    for r in rows:
        rd = dict(r)
        slug = rd.get('player_slug') or rd.get('player_name', '')
        is_q = bool(rd.get('qualifies'))
        if qualified_only and not is_q:
            continue
        pct = pct_map.get(slug, pct_map.get(rd.get('player_name', ''), {}))
        pic = pic_map.get(slug, MASCOT_URL)
        players.append({
            'player_name': rd.get('player_name', slug),
            'player_slug': slug,
            'photo_url': pic,
            'team': '',
            'is_qualified': is_q,
            'stats': {
                'bf': rd.get('bf'), 'era': rd.get('era'),
                'obp_against': rd.get('obp_against'), 'baa': rd.get('baa'),
                'k_pct': rd.get('k_pct'), 'bb_pct': rd.get('bb_pct'),
                'k_per_6': rd.get('k_per_6'), 'bb_per_6': rd.get('bb_per_6'),
            },
            'percentiles': pct,
        })

    conn.close()
    return jsonify({
        'season': season,
        'qualifier_bf': 15,
        'total_players': len(players),
        'qualified_count': sum(1 for p in players if p['is_qualified']),
        'players': players,
    })


# ── API: All pitchers / batters for custom pool modal ───────────────────────
@app.route('/api/players/all-pitchers')
def api_all_pitchers():
    """Return all players with pitching stats + their seasons list."""
    conn = get_db()
    rows = conn.execute("""
        SELECT ps.player_hashtag as slug,
               COALESCE(pl.nickname, ps.player_hashtag) as name,
               GROUP_CONCAT(DISTINCT ps.season) as seasons
        FROM pitching_stats ps
        LEFT JOIN players pl ON pl.hashtag = ps.player_hashtag
        WHERE ps.ip > 0
        GROUP BY ps.player_hashtag
        ORDER BY COALESCE(pl.nickname, ps.player_hashtag)
    """).fetchall()
    conn.close()
    result = []
    for r in rows:
        seas = sorted([int(s) for s in (r['seasons'] or '').split(',') if s.strip()])
        result.append({'name': r['name'], 'slug': r['slug'], 'seasons': seas})
    return jsonify(result)


@app.route('/api/players/all-batters')
def api_all_batters():
    """Return all players with batting stats + their seasons list."""
    conn = get_db()
    rows = conn.execute("""
        SELECT bs.player_hashtag as slug,
               COALESCE(pl.nickname, bs.player_hashtag) as name,
               GROUP_CONCAT(DISTINCT bs.season) as seasons
        FROM batting_stats bs
        LEFT JOIN players pl ON pl.hashtag = bs.player_hashtag
        WHERE bs.ab > 0
        GROUP BY bs.player_hashtag
        ORDER BY COALESCE(pl.nickname, bs.player_hashtag)
    """).fetchall()
    conn.close()
    result = []
    for r in rows:
        seas = sorted([int(s) for s in (r['seasons'] or '').split(',') if s.strip()])
        result.append({'name': r['name'], 'slug': r['slug'], 'seasons': seas})
    return jsonify(result)


def _compute_custom_percentiles(players_stats, stat_keys_asc):
    """
    Compute within-pool percentile ranks for each player.
    stat_keys_asc: dict of stat_key -> True means higher is better, False means lower is better.
    Returns dict of player_slug -> {stat_key: percentile}
    """
    n = len(players_stats)
    if n < 5:
        return {p['player_slug']: {} for p in players_stats}

    result = {}
    for p in players_stats:
        result[p['player_slug']] = {}

    for stat_key, higher_is_better in stat_keys_asc.items():
        vals = []
        for p in players_stats:
            v = p.get('_raw', {}).get(stat_key)
            if v is not None:
                try:
                    vals.append(float(v))
                except Exception:
                    pass

        if not vals:
            continue

        for p in players_stats:
            slug = p['player_slug']
            v = p.get('_raw', {}).get(stat_key)
            if v is None:
                continue
            try:
                fv = float(v)
            except Exception:
                continue
            if higher_is_better:
                worse_count = sum(1 for x in vals if x < fv)
            else:
                worse_count = sum(1 for x in vals if x > fv)
            pct = round((worse_count / n) * 100)
            pct = max(1, min(99, pct))
            result[slug][stat_key] = pct

    return result


@app.route('/api/leaderboard/custom-batting', methods=['POST'])
def api_leaderboard_custom_batting():
    """Batting stats for all batters, filtered to PAs vs a custom pitcher pool."""
    body = request.get_json(force=True) or {}
    pitchers = body.get('pitchers', [])  # list of pitcher display names or slugs
    season = body.get('season', 2025)
    qualified_only = body.get('qualified_only', True)

    try:
        season_int = int(season)
    except Exception:
        season_int = 2025

    if not pitchers:
        return jsonify({'error': 'No pitchers specified', 'players': [], 'warning': 'Pool too small for reliable percentiles — showing raw stats only'}), 200

    conn = get_db()

    # Build name map: nickname -> hashtag and vice versa
    name_rows = conn.execute("SELECT hashtag, nickname FROM players").fetchall()
    nickname_to_slug = {r['nickname']: r['hashtag'] for r in name_rows if r['nickname']}
    slug_to_nickname = {r['hashtag']: r['nickname'] or r['hashtag'] for r in name_rows}

    # Resolve pitcher names to display names used in batter_vs_pitcher.opposing_pitcher
    # batter_vs_pitcher uses nickname (display name) for opposing_pitcher
    resolved_pitchers = set()
    for p in pitchers:
        p = p.strip()
        if not p:
            continue
        # Try as nickname directly
        resolved_pitchers.add(p)
        # Try as slug -> nickname
        if p in slug_to_nickname:
            resolved_pitchers.add(slug_to_nickname[p])

    if not resolved_pitchers:
        conn.close()
        return jsonify({'players': [], 'warning': 'No valid pitchers in pool'}), 200

    placeholders = ','.join('?' * len(resolved_pitchers))
    season_str = str(season_int)

    # Aggregate batting stats vs the given pitchers
    rows = conn.execute(f"""
        SELECT bvp.player_name,
               COALESCE(bvp.player_slug, bvp.player_name) as player_slug,
               SUM(bvp.ab) as ab,
               SUM(bvp.h) as h,
               SUM(bvp.hr) as hr,
               SUM(bvp.rbi) as rbi,
               SUM(bvp.bb) as bb,
               SUM(bvp.so) as so,
               SUM(bvp.doubles) as doubles,
               SUM(bvp.triples) as triples,
               SUM(bvp.sac) as sac,
               SUM(bvp.g) as g
        FROM batter_vs_pitcher bvp
        WHERE bvp.season=? AND bvp.opposing_pitcher IN ({placeholders})
          AND bvp.tab_type='regular'
        GROUP BY bvp.player_name, bvp.player_slug
        HAVING SUM(bvp.ab) > 0
        ORDER BY SUM(bvp.h) DESC
    """, [season_str] + list(resolved_pitchers)).fetchall()

    pic_rows = conn.execute("SELECT hashtag, pic_url FROM players").fetchall()
    pic_map = {r['hashtag']: fix_pic_url(r['pic_url'] or '') for r in pic_rows}
    conn.close()

    CUSTOM_BAT_MIN_PA = 10

    players_out = []
    for r in rows:
        rd = dict(r)
        ab = rd.get('ab') or 0
        h = rd.get('h') or 0
        hr = rd.get('hr') or 0
        bb = rd.get('bb') or 0
        so = rd.get('so') or 0
        doubles = rd.get('doubles') or 0
        triples = rd.get('triples') or 0
        sac = rd.get('sac') or 0
        singles = h - doubles - triples - hr

        pa = ab + bb + sac
        if ab == 0:
            continue

        avg = round(h / ab, 3) if ab > 0 else 0
        obp = round((h + bb) / (ab + bb) if (ab + bb) > 0 else 0, 3)
        slg_num = singles + doubles * 2 + triples * 3 + hr * 4
        slg = round(slg_num / ab, 3) if ab > 0 else 0
        ops = round(obp + slg, 3)
        iso = round(slg - avg, 3)
        bb_pct = round(bb / pa, 4) if pa > 0 else 0
        k_pct = round(so / pa, 4) if pa > 0 else 0
        bb_k = round(bb / so, 3) if so > 0 else None
        # BABIP: (H - HR) / (AB - K - HR + SAC)
        babip_denom = ab - so - hr + sac
        babip = round((h - hr) / babip_denom, 3) if babip_denom > 0 else None

        slug = rd.get('player_slug') or rd.get('player_name', '')
        is_q = pa >= CUSTOM_BAT_MIN_PA

        players_out.append({
            'player_name': rd.get('player_name', slug),
            'player_slug': slug,
            'photo_url': pic_map.get(slug, MASCOT_URL),
            'team': '',
            'is_qualified': is_q,
            'stats': {
                'pa': pa, 'avg': avg, 'obp': obp, 'slg': slg, 'ops': ops,
                'hr': hr, 'bb_pct': bb_pct, 'k_pct': k_pct,
                'iso': iso, 'babip': babip,
            },
            '_raw': {
                'pa': pa, 'avg': avg, 'obp': obp, 'slg': slg, 'ops': ops,
                'hr': hr, 'bb_pct': bb_pct, 'k_pct': k_pct,
                'iso': iso, 'babip': babip,
            },
            'percentiles': {},
        })

    if qualified_only:
        players_out = [p for p in players_out if p['is_qualified']]

    warning = None
    if len(players_out) < 5:
        warning = 'Pool too small for reliable percentiles — showing raw stats only'
    else:
        pct_stats = {
            'ops': True, 'avg': True, 'obp': True, 'slg': True,
            'hr': True, 'bb_pct': True, 'k_pct': False,
            'iso': True, 'babip': True, 'pa': True,
        }
        pcts = _compute_custom_percentiles(players_out, pct_stats)
        for p in players_out:
            p['percentiles'] = pcts.get(p['player_slug'], {})

    for p in players_out:
        p.pop('_raw', None)

    players_out.sort(key=lambda x: x['stats'].get('ops') or 0, reverse=True)

    return jsonify({
        'season': season_int,
        'qualifier_pa': CUSTOM_BAT_MIN_PA,
        'total_players': len(players_out),
        'qualified_count': sum(1 for p in players_out if p['is_qualified']),
        'custom_pool': {'pitchers': list(resolved_pitchers)},
        'warning': warning,
        'players': players_out,
    })


@app.route('/api/leaderboard/custom-pitching', methods=['POST'])
def api_leaderboard_custom_pitching():
    """Pitching stats for all pitchers, filtered to BFs vs a custom batter pool."""
    body = request.get_json(force=True) or {}
    batters = body.get('batters', [])  # list of batter display names or slugs
    season = body.get('season', 2025)
    qualified_only = body.get('qualified_only', True)

    try:
        season_int = int(season)
    except Exception:
        season_int = 2025

    if not batters:
        return jsonify({'error': 'No batters specified', 'players': [], 'warning': 'Pool too small for reliable percentiles — showing raw stats only'}), 200

    conn = get_db()

    name_rows = conn.execute("SELECT hashtag, nickname FROM players").fetchall()
    slug_to_nickname = {r['hashtag']: r['nickname'] or r['hashtag'] for r in name_rows}
    nickname_to_slug = {r['nickname']: r['hashtag'] for r in name_rows if r['nickname']}

    # Resolve batter names to display names used as player_name in batter_vs_pitcher
    # batter_vs_pitcher.player_name is also typically the display name / hashtag
    # We query by player_name matching resolved display names
    resolved_batters = set()
    for b in batters:
        b = b.strip()
        if not b:
            continue
        resolved_batters.add(b)
        # Also try slug -> nickname
        if b in slug_to_nickname:
            resolved_batters.add(slug_to_nickname[b])

    if not resolved_batters:
        conn.close()
        return jsonify({'players': [], 'warning': 'No valid batters in pool'}), 200

    placeholders = ','.join('?' * len(resolved_batters))
    season_str = str(season_int)

    # Aggregate: for each opposing_pitcher, sum stats from these batters
    rows = conn.execute(f"""
        SELECT bvp.opposing_pitcher as pitcher_name,
               SUM(bvp.ab) as ab,
               SUM(bvp.h) as h,
               SUM(bvp.hr) as hr,
               SUM(bvp.bb) as bb,
               SUM(bvp.so) as so,
               SUM(bvp.doubles) as doubles,
               SUM(bvp.triples) as triples,
               SUM(bvp.sac) as sac,
               SUM(bvp.g) as g
        FROM batter_vs_pitcher bvp
        WHERE bvp.season=? AND bvp.player_name IN ({placeholders})
          AND bvp.tab_type='regular'
        GROUP BY bvp.opposing_pitcher
        HAVING SUM(bvp.ab) > 0
        ORDER BY SUM(bvp.ab) DESC
    """, [season_str] + list(resolved_batters)).fetchall()

    # Get photo URLs keyed by nickname
    pic_rows = conn.execute("SELECT hashtag, pic_url, nickname FROM players").fetchall()
    pic_by_nick = {r['nickname']: fix_pic_url(r['pic_url'] or '') for r in pic_rows if r['nickname']}
    pic_by_slug = {r['hashtag']: fix_pic_url(r['pic_url'] or '') for r in pic_rows}
    slug_by_nick = {r['nickname']: r['hashtag'] for r in pic_rows if r['nickname']}
    conn.close()

    CUSTOM_PIT_MIN_BF = 10

    players_out = []
    for r in rows:
        rd = dict(r)
        pitcher_name = rd.get('pitcher_name', '')
        ab = rd.get('ab') or 0
        h = rd.get('h') or 0
        hr = rd.get('hr') or 0
        bb = rd.get('bb') or 0
        so = rd.get('so') or 0
        doubles = rd.get('doubles') or 0
        triples = rd.get('triples') or 0
        sac = rd.get('sac') or 0

        if ab == 0:
            continue

        bf = ab + bb + sac

        # ERA can't be calculated from batter_vs_pitcher (no ER data)
        # BAA = H / AB
        baa = round(h / ab, 3) if ab > 0 else 0
        # OBP against = (H + BB) / (AB + BB)
        obp_against = round((h + bb) / (ab + bb), 3) if (ab + bb) > 0 else 0
        # K%
        k_pct = round(so / bf, 4) if bf > 0 else 0
        # BB%
        bb_pct = round(bb / bf, 4) if bf > 0 else 0
        # K/6 (like K/9 but per 6 outs)
        k_per_6 = round(so / ab * 6, 2) if ab > 0 else 0
        bb_per_6 = round(bb / ab * 6, 2) if ab > 0 else 0

        slug = slug_by_nick.get(pitcher_name, pitcher_name)
        pic = pic_by_nick.get(pitcher_name, pic_by_slug.get(slug, MASCOT_URL))
        is_q = bf >= CUSTOM_PIT_MIN_BF

        players_out.append({
            'player_name': pitcher_name,
            'player_slug': slug,
            'photo_url': pic,
            'team': '',
            'is_qualified': is_q,
            'stats': {
                'bf': bf, 'era': None,
                'obp_against': obp_against, 'baa': baa,
                'k_pct': k_pct, 'bb_pct': bb_pct,
                'k_per_6': k_per_6, 'bb_per_6': bb_per_6,
            },
            '_raw': {
                'bf': bf, 'obp_against': obp_against, 'baa': baa,
                'k_pct': k_pct, 'bb_pct': bb_pct,
                'k_per_6': k_per_6, 'bb_per_6': bb_per_6,
            },
            'percentiles': {},
        })

    if qualified_only:
        players_out = [p for p in players_out if p['is_qualified']]

    warning = None
    if len(players_out) < 5:
        warning = 'Pool too small for reliable percentiles — showing raw stats only'
    else:
        pct_stats = {
            'baa': False, 'obp_against': False, 'k_pct': True, 'bb_pct': False,
            'k_per_6': True, 'bb_per_6': False, 'bf': True,
        }
        pcts = _compute_custom_percentiles(players_out, pct_stats)
        for p in players_out:
            p['percentiles'] = pcts.get(p['player_slug'], {})

    for p in players_out:
        p.pop('_raw', None)

    players_out.sort(key=lambda x: x['stats'].get('baa') or 1.0)

    return jsonify({
        'season': season_int,
        'qualifier_bf': CUSTOM_PIT_MIN_BF,
        'total_players': len(players_out),
        'qualified_count': sum(1 for p in players_out if p['is_qualified']),
        'custom_pool': {'batters': list(resolved_batters)},
        'warning': warning,
        'players': players_out,
    })


# ── API: DB stats for hero section ───────────────────────────────────────────
@app.route('/api/home/leaderboard')
def api_home_leaderboard():
    season = request.args.get('season', 2025, type=int)
    conn = get_db()
    qualifier = conn.execute(
        'SELECT batting_min_pa FROM season_qualifiers WHERE season = ?', (season,)
    ).fetchone()
    min_pa = qualifier['batting_min_pa'] if qualifier else 100
    players = conn.execute('''
        SELECT
            bs.player_hashtag AS player_name,
            bs.player_hashtag AS player_slug,
            bs.team_name AS team,
            p.pic_url AS photo_url,
            bs.pa, bs.avg, bs.obp, bs.slg, bs.ops, bs.hr, bs.rbi, bs.bb, bs.so
        FROM batting_stats bs
        LEFT JOIN players p ON p.hashtag = bs.player_hashtag
        WHERE bs.season = ? AND bs.pa >= ?
        ORDER BY bs.ops DESC
        LIMIT 10
    ''', (season, min_pa)).fetchall()
    conn.close()
    result = []
    for p in players:
        row = dict(p)
        row['photo_url'] = fix_pic_url(row.get('photo_url'))
        result.append(row)
    return jsonify({'season': season, 'players': result})


@app.route('/api/random-player')
def api_random_player():
    import random as _random
    conn = get_db()
    candidates = conn.execute('''
        SELECT DISTINCT
            bs.player_hashtag AS slug,
            COALESCE(p.nickname, bs.player_hashtag) AS name,
            COALESCE(p.team_name, bs.team_name) AS team
        FROM batting_stats bs
        LEFT JOIN players p ON p.hashtag = bs.player_hashtag
        WHERE bs.season BETWEEN 2004 AND 2025
          AND bs.pa > 0
        GROUP BY bs.player_hashtag
        HAVING COUNT(DISTINCT bs.season) >= 2
    ''').fetchall()
    conn.close()
    if not candidates:
        return jsonify({'error': 'no players'}), 404
    player = _random.choice(candidates)
    team = player['team'] or 'HRL Twin Cities'
    jonah = _build_jonah_lines(player['name'], team)
    return jsonify({
        'slug': player['slug'],
        'name': player['name'],
        'team': team,
        **jonah,
    })


@app.route('/api/players/all')
def api_players_all():
    conn = get_db()
    rows = conn.execute('''
        SELECT DISTINCT
            COALESCE(p.nickname, bs.player_hashtag) AS player_name,
            bs.player_hashtag AS player_slug,
            p.pic_url AS photo_url,
            COALESCE(p.team_name, bs.team_name) AS team
        FROM batting_stats bs
        LEFT JOIN players p ON p.hashtag = bs.player_hashtag
        WHERE bs.player_hashtag IS NOT NULL
          AND bs.season BETWEEN 2004 AND 2025
        ORDER BY player_name
    ''').fetchall()
    conn.close()
    result = []
    for r in rows:
        row = dict(r)
        row['photo_url'] = fix_pic_url(row.get('photo_url'))
        result.append(row)
    return jsonify({'players': result})



@app.route('/api/db_stats')
def api_db_stats():
    conn = get_db()
    players = conn.execute("SELECT COUNT(DISTINCT hashtag) FROM players").fetchone()[0]
    seasons = conn.execute(
        "SELECT COUNT(DISTINCT season) FROM league_batting_stats"
    ).fetchone()[0]
    statlines = conn.execute("SELECT COUNT(*) FROM batting_stats").fetchone()[0]
    conn.close()
    return jsonify({'players': players, 'seasons': seasons, 'statlines': statlines})


# ── API: active teams for home page ──────────────────────────────────────────
@app.route('/api/active_teams')
def api_active_teams():
    conn = get_db()
    if not _table_exists(conn, 'teams'):
        conn.close()
        return jsonify([])
    rows = conn.execute("""
        SELECT hashtag, team_name, slug, logo_url
        FROM teams WHERE active=1
        ORDER BY team_name
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ── API: search ──────────────────────────────────────────────────────────────
@app.route('/api/search')
def api_search():
    q = (request.args.get('q') or '').strip().lower()
    if len(q) < 2:
        return jsonify([])
    results = [p for p in get_search_index() if q in p['search_key']]
    results.sort(key=lambda x: (-x['last_year'], x['name'].lower()))
    return jsonify(results[:8])


# ── Search results page ───────────────────────────────────────────────────────
@app.route('/search')
def search():
    q = (request.args.get('q') or '').strip()
    results = []
    if len(q) >= 2:
        ql = q.lower()
        results = [p for p in get_search_index() if ql in p['search_key']]
        results.sort(key=lambda x: (-x['last_year'], x['name'].lower()))
    return render_template('search.html', query=q, results=results,
                           mascot_url=MASCOT_URL, display_names=DISPLAY_NAMES)


# ── /teams and /api/standings removed ────────────────────────────────────────


@app.route('/api/standings/<int:season>')
def api_standings(season):
    conn = get_db()

    # World Series data
    ws = {}
    if _table_exists(conn, 'world_series_results'):
        ws_row = conn.execute(
            "SELECT * FROM world_series_results WHERE season=?", (season,)
        ).fetchone()
        if ws_row:
            ws = dict(ws_row)

    # Get standings from historical_standings if available
    cities = {}
    if _table_exists(conn, 'historical_standings'):
        rows = conn.execute("""
            SELECT DISTINCT hs.season, hs.city, hs.division_name, hs.team_name,
                   hs.team_slug, hs.team_logo_url, hs.wins, hs.losses, hs.pct,
                   hs.games_back, hs.runs_scored, hs.runs_allowed,
                   hs.run_differential, hs.playoff_result,
                   COALESCE(t.logo_url, hs.team_logo_url) as logo,
                   tss.team_ops, tss.team_era
            FROM historical_standings hs
            LEFT JOIN teams t ON t.team_name = hs.team_name AND t.active = 1
            LEFT JOIN team_season_stats tss
                   ON tss.team_name = hs.team_name AND tss.season = hs.season
            WHERE hs.season = ?
            ORDER BY hs.city, hs.division_name, hs.wins DESC
        """, (season,)).fetchall()

        for row in rows:
            d = dict(row)
            city = d.get('city') or 'HRL'
            div = d.get('division_name') or 'Main'
            if city not in cities:
                cities[city] = {}
            if div not in cities[city]:
                cities[city][div] = []
            # Build clean team dict
            team_entry = {
                'team_name':       d['team_name'],
                'team_slug':       d['team_slug'],
                'team_logo_url':   d['logo'] or d['team_logo_url'],
                'wins':            d['wins'],
                'losses':          d['losses'],
                'pct':             d['pct'],
                'games_back':      d['games_back'],
                'runs_scored':     d['runs_scored'],
                'runs_allowed':    d['runs_allowed'],
                'run_differential': d['run_differential'],
                'playoff_result':  d['playoff_result'],
                'team_ops':        d['team_ops'],
                'team_era':        d['team_era'],
            }
            cities[city][div].append(team_entry)

    # Fallback to team_tiers if no historical_standings data
    if not cities and _table_exists(conn, 'team_tiers'):
        rows = conn.execute("""
            SELECT DISTINCT tt.team_name, tt.wins, tt.losses, tt.win_pct,
                   t.city_name, t.logo_url, t.hashtag,
                   tss.team_ops, tss.team_era
            FROM team_tiers tt
            LEFT JOIN teams t ON t.team_name = tt.team_name
            LEFT JOIN team_season_stats tss
                   ON tss.team_name = tt.team_name AND tss.season = tt.season
            WHERE tt.season = ?
            ORDER BY tt.wins DESC
        """, (season,)).fetchall()

        for row in rows:
            d = dict(row)
            city = d.get('city_name') or 'HRL'
            if city not in cities:
                cities[city] = {}
            if 'Division' not in cities[city]:
                cities[city]['Division'] = []
            cities[city]['Division'].append({
                'team_name':       d['team_name'],
                'team_slug':       d['hashtag'],
                'team_logo_url':   d['logo_url'],
                'wins':            d['wins'],
                'losses':          d['losses'],
                'pct':             d['win_pct'],
                'games_back':      None,
                'runs_scored':     None,
                'runs_allowed':    None,
                'run_differential': None,
                'playoff_result':  None,
                'team_ops':        d['team_ops'],
                'team_era':        d['team_era'],
            })

    conn.close()
    return jsonify({'world_series': ws, 'cities': cities, 'season': season})


# ── Team page removed ─────────────────────────────────────────────────────────
# /team/<slug> and /team/<slug>/<season> routes removed
def team_page(slug, season=None):  # kept as dead function, not registered
    # Handle Vibes home redirect (only redirect bare slug without season)
    if slug.lower() == TEAM_SLUG.lower() and slug != TEAM_SLUG and season is None:
        from flask import redirect
        return redirect(f'/team/{TEAM_SLUG}/', 301)

    conn = get_db()

    # Load team metadata
    team = None
    if _table_exists(conn, 'teams'):
        row = conn.execute("SELECT * FROM teams WHERE hashtag=? OR slug=?", (slug, slug)).fetchone()
        if row:
            team = dict(row)
            team['championships'] = json.loads(team.get('championships') or '[]')
            team['runner_up']     = json.loads(team.get('runner_up') or '[]')

    if not team:
        # Fallback: build from team_tiers
        row = conn.execute(
            "SELECT team_name, MAX(season) last_s FROM team_tiers WHERE team_name=? OR team_name=? LIMIT 1",
            (slug, slug.replace('-', ' '))
        ).fetchone()
        if not row or not row[0]:
            conn.close()
            return render_template('404.html', query=slug, mascot_url=MASCOT_URL,
                                   display_names=DISPLAY_NAMES), 404
        team = {'hashtag': slug, 'team_name': row[0], 'slug': slug,
                'logo_url': '', 'division': '', 'active': 0,
                'championships': [], 'runner_up': []}

    team_name = team['team_name']

    # Year-by-year records
    records = [dict(r) for r in conn.execute("""
        SELECT season, wins, losses, win_pct, tier, rank
        FROM team_tiers WHERE team_name=?
        ORDER BY season DESC
    """, (team_name,)).fetchall()]

    # Determine which season to show
    all_seasons = [r['season'] for r in records]
    if season is None:
        most_recent_season = all_seasons[0] if all_seasons else None
    else:
        most_recent_season = season if season in all_seasons else (all_seasons[0] if all_seasons else None)

    # Previous/next season navigation
    prev_season = next_season = None
    if most_recent_season and most_recent_season in all_seasons:
        idx = all_seasons.index(most_recent_season)
        # all_seasons is sorted DESC, so idx-1 is the NEXT year, idx+1 is PREV year
        next_season = all_seasons[idx - 1] if idx > 0 else None
        prev_season = all_seasons[idx + 1] if idx < len(all_seasons) - 1 else None

    roster = []
    if most_recent_season:
        roster_raw = conn.execute("""
            SELECT DISTINCT b.player_hashtag, p.nickname, p.pic_url, p.bats, p.throws,
                   b.avg, b.hr, b.ops,
                   ps.era, ps.k
            FROM batting_stats b
            JOIN players p ON p.hashtag = b.player_hashtag
            LEFT JOIN pitching_stats ps ON ps.player_hashtag = b.player_hashtag
                AND ps.season = b.season AND ps.ip > 0
            WHERE b.team_name=? AND b.season=?
            ORDER BY b.hr DESC
        """, (team_name, most_recent_season)).fetchall()
        for r in roster_raw:
            d = dict(r)
            d['pic_url'] = fix_pic_url(d['pic_url'])
            d['display_name'] = DISPLAY_NAMES.get(d['player_hashtag'], d['nickname'] or d['player_hashtag'])
            roster.append(d)

    # Team batting/pitching stat boxes
    stat_boxes = {}
    if most_recent_season:
        # Try team_season_stats first
        if _table_exists(conn, 'team_season_stats'):
            tss = conn.execute("""
                SELECT team_ops, team_era, team_obp, team_slg
                FROM team_season_stats WHERE team_name=? AND season=?
            """, (team_name, most_recent_season)).fetchone()
            if tss:
                stat_boxes['ops'] = tss[0]
                stat_boxes['era'] = tss[1]
                stat_boxes['avg'] = tss[2]  # use OBP as proxy

        if not stat_boxes:
            sb = conn.execute("""
                SELECT ROUND(AVG(avg),3) team_avg,
                       ROUND(AVG(ops),3) team_ops,
                       COUNT(*) players
                FROM batting_stats WHERE team_name=? AND season=? AND ab >= 10
            """, (team_name, most_recent_season)).fetchone()
            if sb:
                stat_boxes['avg'] = sb[0]
                stat_boxes['ops'] = sb[1]
            pb = conn.execute("""
                SELECT ROUND(SUM(opp_r*6.0)/NULLIF(SUM(ip),0),2) team_era
                FROM pitching_stats WHERE team_name=? AND season=? AND ip > 0
            """, (team_name, most_recent_season)).fetchone()
            if pb:
                stat_boxes['era'] = pb[0]

    # Get season standings data if viewing a specific season
    season_standings = None
    if most_recent_season and _table_exists(conn, 'historical_standings'):
        hs = conn.execute("""
            SELECT wins, losses, pct, games_back, runs_scored, runs_allowed,
                   run_differential, playoff_result, city, division_name
            FROM historical_standings WHERE team_name=? AND season=?
        """, (team_name, most_recent_season)).fetchone()
        if hs:
            season_standings = dict(hs)

    conn.close()
    return render_template('team.html',
        team=team, records=records, roster=roster,
        stat_boxes=stat_boxes, most_recent_season=most_recent_season,
        season=most_recent_season,
        prev_season=prev_season, next_season=next_season,
        season_standings=season_standings,
        mascot_url=MASCOT_URL, display_names=DISPLAY_NAMES)


# ── /hq-opponents ─────────────────────────────────────────────────────────────
@app.route('/api/hq-opponents')
def api_hq_opponents():
    season = request.args.get('season', '2025')
    conn = get_db()
    # Get HQ pitchers for this season with photo URLs
    pitchers = conn.execute("""
        SELECT h.pitcher_name, h.whip, h.era, h.ip, h.cutoff_whip, h.range_min_whip, h.range_max_whip, h.total_qualified_pitchers,
               p.hashtag as player_slug, p.pic_url, p.nickname,
               (SELECT team_name FROM pitching_stats WHERE player_hashtag=p.hashtag AND season=? ORDER BY season DESC LIMIT 1) as team
        FROM hq_pitchers h
        LEFT JOIN players p ON LOWER(TRIM(h.pitcher_name)) = LOWER(TRIM(p.hashtag)) OR LOWER(TRIM(h.pitcher_name)) = LOWER(TRIM(p.nickname))
        WHERE h.season=? AND h.is_hq=1
        ORDER BY h.whip ASC
    """, (int(season), int(season))).fetchall()

    batters = conn.execute("""
        SELECT h.batter_name, h.ops, h.obp, h.cutoff_ops, h.range_min_ops, h.range_max_ops, h.total_qualified_batters,
               p.hashtag as player_slug, p.pic_url, p.nickname,
               (SELECT team_name FROM batting_stats WHERE player_hashtag=p.hashtag AND season=? ORDER BY season DESC LIMIT 1) as team
        FROM hq_batters h
        LEFT JOIN players p ON LOWER(TRIM(h.batter_name)) = LOWER(TRIM(p.hashtag)) OR LOWER(TRIM(h.batter_name)) = LOWER(TRIM(p.nickname))
        WHERE h.season=? AND h.is_hq=1
        ORDER BY h.ops DESC
    """, (int(season), int(season))).fetchall()

    # Get HQ pitcher source metadata for this season
    hq_source_row = conn.execute(
        "SELECT source, notes, cutoff_whip, range_min_whip, range_max_whip FROM hq_pitchers WHERE season=? AND is_hq=1 LIMIT 1",
        (int(season),)
    ).fetchone()
    hq_pitcher_source = hq_source_row['source'] if hq_source_row else 'whip_calculated'
    hq_pitcher_notes = hq_source_row['notes'] if hq_source_row else None
    pit_cutoff_whip = hq_source_row['cutoff_whip'] if hq_source_row else None
    pit_range_min = hq_source_row['range_min_whip'] if hq_source_row else None
    pit_range_max = hq_source_row['range_max_whip'] if hq_source_row else None
    # For manual curation, cutoff_whip is null; compute range from data if not set
    if hq_pitcher_source == 'manual_curation' and (pit_range_min is None or pit_range_max is None):
        whip_range = conn.execute(
            "SELECT MIN(whip), MAX(whip) FROM hq_pitchers WHERE season=? AND is_hq=1 AND whip > 0",
            (int(season),)
        ).fetchone()
        if whip_range:
            pit_range_min = whip_range[0]
            pit_range_max = whip_range[1]

    # Build response
    pit_list = []
    for i, r in enumerate(pitchers):
        pic = fix_pic_url(r['pic_url']) if r['pic_url'] else MASCOT_URL
        pit_list.append({'rank': i+1, 'player_name': r['pitcher_name'], 'player_slug': r['player_slug'] or r['pitcher_name'], 'photo_url': pic, 'team': r['team'] or '', 'whip': r['whip'], 'era': r['era'], 'cutoff_whip': r['cutoff_whip'], 'range_min_whip': r['range_min_whip'], 'range_max_whip': r['range_max_whip'], 'total_qualified': r['total_qualified_pitchers']})

    bat_list = []
    for i, r in enumerate(batters):
        pic = fix_pic_url(r['pic_url']) if r['pic_url'] else MASCOT_URL
        bat_list.append({'rank': i+1, 'player_name': r['batter_name'], 'player_slug': r['player_slug'] or r['batter_name'], 'photo_url': pic, 'team': r['team'] or '', 'ops': r['ops'], 'obp': r['obp'], 'cutoff_ops': r['cutoff_ops'], 'range_min_ops': r['range_min_ops'], 'range_max_ops': r['range_max_ops'], 'total_qualified': r['total_qualified_batters']})

    meta = {}
    if pit_list:
        meta['cutoff_whip'] = pit_cutoff_whip
        meta['total_hq_pitchers'] = len(pit_list)
        meta['total_qualified_pitchers'] = pit_list[0]['total_qualified']
    if bat_list:
        meta['cutoff_ops'] = bat_list[0]['cutoff_ops']
        meta['total_hq_batters'] = len(bat_list)
        meta['total_qualified_batters'] = bat_list[0]['total_qualified']
    meta['season'] = int(season)

    conn.close()
    return jsonify({
        'hq_pitchers': pit_list,
        'hq_batters': bat_list,
        'season_meta': meta,
        'hq_pitcher_source': hq_pitcher_source,
        'hq_pitcher_notes': hq_pitcher_notes,
        'cutoff_whip': pit_cutoff_whip,
        'range_min_whip': pit_range_min,
        'range_max_whip': pit_range_max,
    })


@app.route('/hq-opponents')
def hq_opponents():
    conn = get_db()
    seasons = [r[0] for r in conn.execute("SELECT DISTINCT season FROM hq_pitchers ORDER BY season DESC").fetchall()]
    conn.close()
    return render_template('hq_opponents.html', seasons=seasons, mascot_url=MASCOT_URL, display_names=DISPLAY_NAMES)


def _table_exists(conn, table_name):
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    ).fetchone() is not None


# ── 404 handler ───────────────────────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return render_template('404.html', query='', mascot_url=MASCOT_URL,
                           display_names=DISPLAY_NAMES), 404


# Search index is built lazily on first request (see build_search_index / get_search_index)


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port)
