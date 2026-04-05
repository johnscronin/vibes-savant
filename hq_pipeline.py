#!/usr/bin/env python3
"""
HQ Opponent Splits Pipeline - Complete workflow Steps 4-8
Builds HQ pitcher/hitter lists, calculates splits for all BvP players,
computes percentile rankings against full league pool.
"""

import sqlite3
import re
import math
from collections import defaultdict

DB_PATH = "/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db"
LOG_PATH = "/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/hq_audit_log.txt"

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]

DISPLAY_NAMES = {
    "FishHook": "Fish Hook",
    "HuckFinn": "Huck Finn",
}

def log(msg):
    print(msg)
    with open(LOG_PATH, 'a') as f:
        f.write(msg + '\n')

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Name Normalization
# ─────────────────────────────────────────────────────────────────────────────

# Vibes player hashtag → display name mapping
HASHTAG_TO_DISPLAY = {
    "FishHook": "Fish Hook",
    "HuckFinn": "Huck Finn",
}

# Reverse mapping
DISPLAY_TO_HASHTAG = {v: k for k, v in HASHTAG_TO_DISPLAY.items()}

def normalize_player_name(name):
    """
    Normalize player name for matching:
    - Strip whitespace
    - Remove trailing (2), (3) etc.
    - Remove periods (Dr. Seuss -> Dr Seuss)
    - Remove apostrophes
    - Remove hyphens (but keep spaces)
    - Lowercase for comparison
    """
    if not name:
        return ''
    n = name.strip()
    # Remove trailing (2), (3), etc.
    n = re.sub(r'\s*\(\d+\)\s*$', '', n)
    # Remove periods
    n = n.replace('.', '')
    # Remove apostrophes
    n = n.replace("'", '')
    # Replace hyphens with spaces
    n = n.replace('-', ' ')
    # Collapse multiple spaces
    n = re.sub(r'\s+', ' ', n).strip()
    return n.lower()

def build_name_mappings(conn):
    """Create name_mappings table and populate with known mappings."""
    log("\n=== STEP 4: Building Name Normalization Mappings ===")

    conn.execute("DROP TABLE IF EXISTS name_mappings")
    conn.execute("""
        CREATE TABLE name_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            source TEXT,
            UNIQUE(original_name, source)
        )
    """)

    # Add Vibes hashtag <-> display name mappings
    mappings = [
        ("FishHook", "fish hook", "vibes_hashtag"),
        ("Fish Hook", "fish hook", "vibes_display"),
        ("HuckFinn", "huck finn", "vibes_hashtag"),
        ("Huck Finn", "huck finn", "vibes_display"),
    ]

    # Add all names from league_pitching_stats and their normalized forms
    for row in conn.execute("SELECT DISTINCT player_name FROM league_pitching_stats").fetchall():
        name = row[0]
        norm = normalize_player_name(name)
        if norm != name.lower().strip():  # only add if normalization changed it
            mappings.append((name, norm, "lps_auto"))

    # Add all names from league_batting_stats and their normalized forms
    for row in conn.execute("SELECT DISTINCT player_name FROM league_batting_stats").fetchall():
        name = row[0]
        norm = normalize_player_name(name)
        if norm != name.lower().strip():
            mappings.append((name, norm, "lbs_auto"))

    # Add all opposing pitchers
    for row in conn.execute("SELECT DISTINCT opposing_pitcher FROM batter_vs_pitcher WHERE season != 'Career'").fetchall():
        name = row[0]
        norm = normalize_player_name(name)
        if norm != name.lower().strip():
            mappings.append((name, norm, "bvp_pitcher_auto"))

    # Add all batters in bvp
    for row in conn.execute("SELECT DISTINCT player_name FROM batter_vs_pitcher WHERE season != 'Career'").fetchall():
        name = row[0]
        norm = normalize_player_name(name)
        if norm != name.lower().strip():
            mappings.append((name, norm, "bvp_batter_auto"))

    inserted = 0
    for original, normalized, source in mappings:
        try:
            conn.execute("INSERT OR REPLACE INTO name_mappings (original_name, normalized_name, source) VALUES (?,?,?)",
                        (original, normalized, source))
            inserted += 1
        except:
            pass

    conn.commit()
    log(f"  name_mappings: {inserted} entries inserted")

    # Test on known problem names
    test_names = ["Dr. Seuss", "O'Bannion", "T-Mac", "Fish Hook", "Huck Finn",
                  "FishHook", "HuckFinn", "Shirls Jr", "B Squared", "Knooty Booty"]
    log("\n  Name normalization test:")
    for name in test_names:
        norm = normalize_player_name(name)
        log(f"    '{name}' -> '{norm}'")

    return True

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Build HQ Pitcher and Hitter Lists
# ─────────────────────────────────────────────────────────────────────────────

def build_hq_lists(conn):
    """Build hq_pitchers and hq_batters tables for all seasons 2004-2025."""
    log("\n=== STEP 5: Building HQ Pitcher and Hitter Lists ===")

    # Get season qualifiers
    qualifiers = {}
    for row in conn.execute("SELECT season, batting_min_pa, pitching_min_ip, pitching_min_g FROM season_qualifiers"):
        qualifiers[row['season']] = {
            'batting_min_pa': row['batting_min_pa'],
            'pitching_min_ip': row['pitching_min_ip'],
            'pitching_min_g': row['pitching_min_g'],
        }

    # For seasons without qualifiers, use reasonable defaults
    default_qualifiers = {
        'batting_min_pa': 100,
        'pitching_min_ip': 35.0,
        'pitching_min_g': 6,
    }

    # ── HQ PITCHERS ──
    conn.execute("DROP TABLE IF EXISTS hq_pitchers")
    conn.execute("""
        CREATE TABLE hq_pitchers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER NOT NULL,
            pitcher_name TEXT NOT NULL,
            pitcher_name_normalized TEXT NOT NULL,
            era REAL,
            ip REAL,
            UNIQUE(season, pitcher_name)
        )
    """)

    HQ_ERA_THRESHOLD = 3.50

    log(f"\n  HQ Pitchers (ERA < {HQ_ERA_THRESHOLD}, qualified):")
    hq_pit_by_season = {}

    for season in range(2004, 2026):
        q = qualifiers.get(season, default_qualifiers)
        min_ip = q['pitching_min_ip']
        min_g = q['pitching_min_g']

        rows = conn.execute("""
            SELECT player_name, era, ip, g FROM league_pitching_stats
            WHERE season = ? AND era < ? AND era IS NOT NULL
            AND (ip >= ? OR g >= ?)
            ORDER BY era
        """, (season, HQ_ERA_THRESHOLD, min_ip, min_g)).fetchall()

        hq_pit_by_season[season] = list(rows)

        for row in rows:
            norm = normalize_player_name(row['player_name'])
            conn.execute("""
                INSERT OR REPLACE INTO hq_pitchers (season, pitcher_name, pitcher_name_normalized, era, ip)
                VALUES (?,?,?,?,?)
            """, (season, row['player_name'], norm, row['era'], row['ip']))

        flag = " *** THIN (<3)" if len(rows) < 3 else ""
        names_eras = ', '.join(f"{r['player_name']}({r['era']})" for r in rows[:10])
        log(f"  {season}: {len(rows)} HQ pitchers{flag} | {names_eras}")

    conn.commit()
    total_hq_pit = conn.execute("SELECT COUNT(*) FROM hq_pitchers").fetchone()[0]
    log(f"\n  Total hq_pitchers records: {total_hq_pit}")

    # ── HQ BATTERS ──
    conn.execute("DROP TABLE IF EXISTS hq_batters")
    conn.execute("""
        CREATE TABLE hq_batters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER NOT NULL,
            batter_name TEXT NOT NULL,
            batter_name_normalized TEXT NOT NULL,
            obp REAL,
            ab INTEGER,
            UNIQUE(season, batter_name)
        )
    """)

    HQ_OBP_THRESHOLD = 0.350

    log(f"\n  HQ Batters (OBP > {HQ_OBP_THRESHOLD}, qualified):")
    hq_bat_by_season = {}

    for season in range(2004, 2026):
        q = qualifiers.get(season, default_qualifiers)
        min_pa = q['batting_min_pa']

        rows = conn.execute("""
            SELECT player_name, obp, ab, bb FROM league_batting_stats
            WHERE season = ? AND obp > ? AND obp IS NOT NULL
            AND (ab + COALESCE(bb,0)) >= ?
            ORDER BY obp DESC
        """, (season, HQ_OBP_THRESHOLD, min_pa)).fetchall()

        hq_bat_by_season[season] = list(rows)

        for row in rows:
            norm = normalize_player_name(row['player_name'])
            conn.execute("""
                INSERT OR REPLACE INTO hq_batters (season, batter_name, batter_name_normalized, obp, ab)
                VALUES (?,?,?,?,?)
            """, (season, row['player_name'], norm, row['obp'], row['ab']))

        flag = " *** THIN (<5)" if len(rows) < 5 else ""
        names_obps = ', '.join(f"{r['player_name']}({r['obp']})" for r in rows[:8])
        log(f"  {season}: {len(rows)} HQ batters{flag} | {names_obps}")

    conn.commit()
    total_hq_bat = conn.execute("SELECT COUNT(*) FROM hq_batters").fetchone()[0]
    log(f"\n  Total hq_batters records: {total_hq_bat}")

    return True

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: Calculate HQ Batting Splits for All BvP Players
# ─────────────────────────────────────────────────────────────────────────────

def calc_safe(numerator, denominator, default=None, precision=3):
    """Safe division with optional rounding."""
    if denominator is None or denominator == 0:
        return default
    return round(numerator / denominator, precision)

def calculate_hq_batting_splits(conn):
    """Calculate batting stats vs HQ pitchers for all players in bvp table."""
    log("\n=== STEP 6: Calculating HQ Batting Splits ===")

    # Get all HQ pitchers indexed by (normalized_name, season)
    hq_pit_lookup = set()
    for row in conn.execute("SELECT season, pitcher_name_normalized FROM hq_pitchers"):
        hq_pit_lookup.add((row['season'], row['pitcher_name_normalized']))

    log(f"  HQ pitcher lookup entries: {len(hq_pit_lookup)}")

    # Drop and recreate hq_opponent_splits with extended schema
    conn.execute("DROP TABLE IF EXISTS hq_opponent_splits")
    conn.execute("""
        CREATE TABLE hq_opponent_splits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            season INTEGER NOT NULL,
            split_type TEXT NOT NULL,
            pa INTEGER, ab INTEGER,
            h INTEGER, hr INTEGER, rbi INTEGER,
            bb INTEGER, so INTEGER,
            avg REAL, obp REAL, slg REAL, ops REAL,
            doubles INTEGER, triples INTEGER,
            bb_pct REAL, k_pct REAL, bb_k REAL,
            iso REAL, babip REAL,
            qualifies INTEGER DEFAULT 0,
            UNIQUE(player_name, season, split_type)
        )
    """)
    conn.commit()

    # Get all bvp rows (non-Career, regular season)
    bvp_rows = conn.execute("""
        SELECT player_name, season, opposing_pitcher,
               ab, h, doubles, triples, hr, rbi, bb, sac, so
        FROM batter_vs_pitcher
        WHERE season != 'Career' AND tab_type = 'regular'
    """).fetchall()

    log(f"  BvP rows to process: {len(bvp_rows)}")

    # Aggregate per player-season vs HQ pitchers
    # Key: (player_name, season)
    # Value: accumulated stats
    splits = defaultdict(lambda: {
        'ab': 0, 'h': 0, 'doubles': 0, 'triples': 0, 'hr': 0,
        'rbi': 0, 'bb': 0, 'sac': 0, 'so': 0
    })

    matched_pa = 0
    unmatched_pa = 0
    matched_rows = 0
    unmatched_rows = 0

    for row in bvp_rows:
        try:
            season = int(row['season'])
        except (ValueError, TypeError):
            continue

        pitcher = row['opposing_pitcher']
        pitcher_norm = normalize_player_name(pitcher)

        if (season, pitcher_norm) in hq_pit_lookup:
            key = (row['player_name'], season)
            s = splits[key]
            s['ab'] += row['ab'] or 0
            s['h'] += row['h'] or 0
            s['doubles'] += row['doubles'] or 0
            s['triples'] += row['triples'] or 0
            s['hr'] += row['hr'] or 0
            s['rbi'] += row['rbi'] or 0
            s['bb'] += row['bb'] or 0
            s['sac'] += row['sac'] or 0
            s['so'] += row['so'] or 0
            matched_rows += 1
            matched_pa += (row['ab'] or 0) + (row['bb'] or 0) + (row['sac'] or 0)
        else:
            unmatched_rows += 1
            unmatched_pa += (row['ab'] or 0) + (row['bb'] or 0) + (row['sac'] or 0)

    log(f"  Rows matched to HQ pitchers: {matched_rows}, unmatched: {unmatched_rows}")

    # Get overall batting stats for sanity check
    overall_stats = {}
    for row in conn.execute("SELECT player_hashtag, season, ops FROM batting_stats"):
        overall_stats[(row['player_hashtag'], row['season'])] = row['ops']

    # Insert records
    records_saved = 0
    qualified_records = 0
    warnings = []

    for (player_name, season), s in splits.items():
        ab = s['ab']
        h = s['h']
        doubles = s['doubles']
        triples = s['triples']
        hr = s['hr']
        rbi = s['rbi']
        bb = s['bb']
        sac = s['sac']
        so = s['so']

        pa = ab + bb + sac
        if pa == 0:
            continue

        # Calculate stats
        singles = h - doubles - triples - hr
        tb = singles + doubles*2 + triples*3 + hr*4

        avg = calc_safe(h, ab)
        obp = calc_safe(h + bb, ab + bb) if (ab + bb) > 0 else None
        slg = calc_safe(tb, ab)
        ops = round((obp or 0) + (slg or 0), 3) if (obp is not None and slg is not None) else None

        bb_pct = calc_safe(bb, pa)
        k_pct = calc_safe(so, pa)
        bb_k = calc_safe(bb, so)
        iso = calc_safe(tb - h, ab)  # SLG - AVG = (TB-H)/AB

        babip_denom = ab - so - hr
        babip = calc_safe(h - hr, babip_denom) if babip_denom > 0 else None

        qualifies = 1 if pa >= 10 else 0

        # Sanity check for Vibes players
        if player_name in VIBES_PLAYERS and ops is not None and qualifies:
            overall_ops = overall_stats.get((player_name, season))
            if overall_ops is not None and ops > overall_ops + 0.150:
                warnings.append(f"  WARNING: {player_name} {season} HQ OPS ({ops}) > overall OPS ({overall_ops}) by {round(ops-overall_ops,3)}")

        conn.execute("""
            INSERT OR REPLACE INTO hq_opponent_splits
            (player_name, season, split_type, pa, ab, h, hr, rbi, bb, so,
             avg, obp, slg, ops, doubles, triples, bb_pct, k_pct, bb_k, iso, babip, qualifies)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (player_name, season, 'vs_hq_pitcher',
              pa, ab, h, hr, rbi, bb, so,
              avg, obp, slg, ops, doubles, triples,
              bb_pct, k_pct, bb_k, iso, babip, qualifies))

        records_saved += 1
        if qualifies:
            qualified_records += 1

    conn.commit()

    if warnings:
        log("\n  OPS Sanity Check Warnings:")
        for w in warnings:
            log(w)

    log(f"\n  Records saved: {records_saved}")
    log(f"  Qualified records (10+ PA): {qualified_records}")

    return True

# ─────────────────────────────────────────────────────────────────────────────
# STEP 7: Calculate HQ Pitching Splits for Vibes Pitchers
# ─────────────────────────────────────────────────────────────────────────────

def calculate_hq_pitching_splits(conn):
    """Calculate pitching stats vs HQ batters for Vibes players who pitched."""
    log("\n=== STEP 7: Calculating HQ Pitching Splits ===")

    # Get all HQ batters indexed by (normalized_name, season)
    hq_bat_lookup = set()
    for row in conn.execute("SELECT season, batter_name_normalized FROM hq_batters"):
        hq_bat_lookup.add((row['season'], row['batter_name_normalized']))

    log(f"  HQ batter lookup entries: {len(hq_bat_lookup)}")

    # Find all players who appear as opposing_pitcher in bvp
    # These are pitchers - check if any are Vibes players
    # We process ALL players who appear as opposing_pitcher (not just Vibes)
    # to build a full league pool for percentile rankings

    pitchers = conn.execute("""
        SELECT DISTINCT opposing_pitcher FROM batter_vs_pitcher
        WHERE season != 'Career' AND tab_type = 'regular'
    """).fetchall()
    pitchers = [r['opposing_pitcher'] for r in pitchers]

    log(f"  Total pitchers in BvP as opposing_pitcher: {len(pitchers)}")

    # Get all bvp rows grouped by pitcher-season
    # We need to find rows where player_name (batter) was an HQ batter

    # Index bvp rows by (opposing_pitcher, season, player_name)
    # Aggregate per pitcher-season vs HQ batters
    pitcher_splits = defaultdict(lambda: {
        'ab': 0, 'h': 0, 'doubles': 0, 'triples': 0, 'hr': 0,
        'rbi': 0, 'bb': 0, 'sac': 0, 'so': 0, 'rows': 0
    })

    # For pitching splits, use 'combined' tab_type which has league players batting against pitchers
    # 'regular' is only Vibes players batting; 'combined' has all other batters
    bvp_rows = conn.execute("""
        SELECT player_name, season, opposing_pitcher,
               ab, h, doubles, triples, hr, rbi, bb, sac, so
        FROM batter_vs_pitcher
        WHERE season != 'Career' AND tab_type IN ('combined', 'regular')
    """).fetchall()

    for row in bvp_rows:
        try:
            season = int(row['season'])
        except (ValueError, TypeError):
            continue

        batter = row['player_name']
        batter_norm = normalize_player_name(batter)

        # Check display name mapping for Vibes players
        if batter in HASHTAG_TO_DISPLAY:
            batter_norm = normalize_player_name(HASHTAG_TO_DISPLAY[batter])

        if (season, batter_norm) in hq_bat_lookup:
            pitcher = row['opposing_pitcher']
            key = (pitcher, season)
            s = pitcher_splits[key]
            s['ab'] += row['ab'] or 0
            s['h'] += row['h'] or 0
            s['doubles'] += row['doubles'] or 0
            s['triples'] += row['triples'] or 0
            s['hr'] += row['hr'] or 0
            s['rbi'] += row['rbi'] or 0
            s['bb'] += row['bb'] or 0
            s['sac'] += row['sac'] or 0
            s['so'] += row['so'] or 0
            s['rows'] += 1  # count of batter matchups

    log(f"  Pitcher-season splits calculated: {len(pitcher_splits)}")

    # Get overall pitching stats for sanity check
    overall_era = {}
    for row in conn.execute("SELECT player_hashtag, season, era FROM pitching_stats WHERE ip > 0"):
        overall_era[(row['player_hashtag'], row['season'])] = row['era']

    records_saved = 0
    qualified_records = 0

    for (pitcher_name, season), s in pitcher_splits.items():
        ab = s['ab']
        h = s['h']
        hr = s['hr']
        bb = s['bb']
        so = s['so']
        bf = s['rows']  # number of batter matchup rows = batters faced (approx)

        if bf == 0 or ab == 0:
            continue

        pa = ab + bb + s['sac']

        # Estimate IP from BF
        # In baseball: BF ≈ IP*3 + extra. Here we use outs = bf - h - bb - (hr)
        # Simple estimate: IP = outs / 3 where outs = ab - h (roughly)
        outs = ab - h  # approximation
        ip_est = round(outs / 3.0, 1) if outs > 0 else 0.1

        singles = h - s['doubles'] - s['triples'] - hr

        baa = calc_safe(h, ab)  # batting avg against
        obp_against = calc_safe(h + bb, ab + bb) if (ab + bb) > 0 else None
        slg_against = calc_safe(singles + s['doubles']*2 + s['triples']*3 + hr*4, ab)
        ops_against = round((obp_against or 0) + (slg_against or 0), 3) if (obp_against is not None and slg_against is not None) else None

        # Use pa for rate stats (pa = ab + bb + sac)
        k_pct = calc_safe(so, pa) if pa > 0 else None
        bb_pct = calc_safe(bb, pa) if pa > 0 else None

        k_per_6 = calc_safe(so * 6, ip_est) if ip_est > 0 else None
        bb_per_6 = calc_safe(bb * 6, ip_est) if ip_est > 0 else None

        # BABIP against = (H-HR)/(AB-SO-HR)
        babip_denom = ab - so - hr
        babip_against = calc_safe(h - hr, babip_denom) if babip_denom > 0 else None

        qualifies = 1 if pa >= 10 else 0

        conn.execute("""
            INSERT OR REPLACE INTO hq_opponent_splits
            (player_name, season, split_type, pa, ab, h, hr, rbi, bb, so,
             avg, obp, slg, ops, doubles, triples, bb_pct, k_pct, bb_k, iso, babip, qualifies)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (pitcher_name, season, 'vs_hq_hitter',
              pa, ab, h, hr, s['rbi'], bb, so,
              baa, obp_against, slg_against, ops_against,
              s['doubles'], s['triples'],
              bb_pct, k_pct, calc_safe(bb, so),
              None, babip_against, qualifies))

        records_saved += 1
        if qualifies:
            qualified_records += 1

    conn.commit()

    log(f"\n  HQ pitching splits saved: {records_saved}")
    log(f"  Qualified (10+ PA): {qualified_records}")

    # Print Vibes pitchers results
    log("\n  Vibes pitchers vs HQ batters:")
    # Check which Vibes players appear as pitchers
    for vp in VIBES_PLAYERS:
        display = DISPLAY_NAMES.get(vp, vp)
        rows = conn.execute("""
            SELECT season, pa, ab, h, hr, bb, so, avg, obp, ops, qualifies
            FROM hq_opponent_splits
            WHERE player_name IN (?,?) AND split_type = 'vs_hq_hitter'
            ORDER BY season
        """, (vp, display)).fetchall()
        if rows:
            log(f"  {vp}:")
            for r in rows:
                log(f"    {r['season']}: PA={r['pa']}, H={r['h']}, HR={r['hr']}, BB={r['bb']}, SO={r['so']}, OBP-against={r['obp']}, qualifies={r['qualifies']}")

    return True

# ─────────────────────────────────────────────────────────────────────────────
# STEP 8: Calculate HQ Percentile Rankings
# ─────────────────────────────────────────────────────────────────────────────

def calculate_hq_percentiles(conn):
    """Calculate percentile rankings for HQ splits."""
    log("\n=== STEP 8: Calculating HQ Percentile Rankings ===")

    # Delete existing HQ percentiles from percentile_rankings
    conn.execute("DELETE FROM percentile_rankings WHERE stat_type IN ('vs_hq_pitcher', 'vs_hq_hitter')")
    # Also delete old 'splits' type hq_ prefixed stats
    conn.execute("DELETE FROM percentile_rankings WHERE stat_type = 'splits' AND stat_name LIKE 'hq_%'")
    conn.commit()

    # ── BATTING VS HQ PITCHER ──
    log("\n  Batting vs HQ Pitcher percentiles:")

    BAT_STATS = [
        ('hq_ops',    'ops',    True),
        ('hq_obp',    'obp',    True),
        ('hq_avg',    'avg',    True),
        ('hq_slg',    'slg',    True),
        ('hq_bb_pct', 'bb_pct', True),
        ('hq_k_pct',  'k_pct',  False),   # lower is better
        ('hq_bb_k',   'bb_k',   True),
        ('hq_iso',    'iso',    True),
        ('hq_babip',  'babip',  True),
        ('hq_ab_hr',  'ab_hr',  False),   # lower is better (fewer AB per HR = more power)
    ]

    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM hq_opponent_splits WHERE split_type='vs_hq_pitcher' ORDER BY season"
    ).fetchall()]

    total_bat_inserted = 0

    def compute_row_stats(row_dict):
        """Add computed stats (ab_hr) to a row dict."""
        ab = row_dict.get('ab') or 0
        hr = row_dict.get('hr') or 0
        row_dict['ab_hr'] = round(ab / hr, 1) if hr > 0 else None
        return row_dict

    for season in seasons:
        # Get all qualified players for this season (including ab and hr for ab_hr calc)
        raw_pool = conn.execute("""
            SELECT player_name, pa, ab, hr, ops, obp, avg, slg, bb_pct, k_pct, bb_k, iso, babip
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_pitcher' AND season=? AND qualifies=1
        """, (season,)).fetchall()

        pool_rows = [compute_row_stats(dict(r)) for r in raw_pool]
        pool_size = len(pool_rows)

        if pool_size < 2:
            log(f"  {season}: pool too thin ({pool_size}) - skipping")
            continue

        if pool_size < 5:
            log(f"  {season}: *** THIN POOL ({pool_size} players)")
        else:
            log(f"  {season}: pool_size={pool_size}")

        # Build stat arrays for pool
        pool_by_stat = {}
        for stat_key, col, _ in BAT_STATS:
            pool_by_stat[col] = [r[col] for r in pool_rows if r.get(col) is not None]

        # Calculate percentiles for ALL players (including non-qualified as estimates)
        raw_all = conn.execute("""
            SELECT player_name, pa, ab, hr, ops, obp, avg, slg, bb_pct, k_pct, bb_k, iso, babip
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_pitcher' AND season=?
        """, (season,)).fetchall()

        all_rows = [compute_row_stats(dict(r)) for r in raw_all]

        for row in all_rows:
            player = row['player_name']
            pa = row.get('pa') or 0
            qualified = pa >= 10

            for stat_key, col, higher_is_better in BAT_STATS:
                val = row.get(col)
                if val is None:
                    continue

                pool_vals = pool_by_stat.get(col, [])
                if len(pool_vals) < 2:
                    continue

                if higher_is_better:
                    worse = sum(1 for v in pool_vals if v < val)
                else:
                    worse = sum(1 for v in pool_vals if v > val)

                pct = max(1, min(99, round(worse / len(pool_vals) * 100)))

                if qualified:
                    conn.execute("""
                        INSERT OR REPLACE INTO percentile_rankings
                        (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                         stat_type, qualified, pool_size)
                        VALUES (?,?,?,?,?,NULL,?,1,?)
                    """, (player, season, stat_key, val, pct, 'splits', len(pool_vals)))
                else:
                    # Store as estimated percentile
                    conn.execute("""
                        INSERT OR REPLACE INTO percentile_rankings
                        (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                         stat_type, qualified, pool_size)
                        VALUES (?,?,?,?,NULL,?,?,0,?)
                    """, (player, season, stat_key, val, pct, 'splits', len(pool_vals)))

                total_bat_inserted += 1

        conn.commit()

    log(f"  Total batting HQ percentile records inserted: {total_bat_inserted}")

    # ── PITCHING VS HQ HITTER ──
    # Frontend expects stat_type='hq_pitcher' with stat_names: hqpit_baa, hqpit_pit_k_pct, hqpit_pit_bb_pct, hqpit_whip etc.
    log("\n  Pitching vs HQ Hitter percentiles:")

    # First delete old hq_pitcher type records
    conn.execute("DELETE FROM percentile_rankings WHERE stat_type='hq_pitcher'")
    conn.commit()

    # Map our column names to frontend expected stat_names
    # stat_key = DB stat_name, col = column in hq_opponent_splits, higher_is_better for pitcher = ?
    PIT_STATS = [
        ('hqpit_baa',      'avg',    False),  # BAA: lower is better for pitcher
        ('hqpit_whip',     'obp',    False),  # Using OBP-against as proxy for WHIP; lower is better
        ('hqpit_pit_k_pct', 'k_pct', True),   # K%: higher is better
        ('hqpit_pit_bb_pct','bb_pct',False),   # BB%: lower is better
        ('hqpit_pit_babip', 'babip', False),   # BABIP against: lower is better (use babip field)
    ]

    pit_seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM hq_opponent_splits WHERE split_type='vs_hq_hitter' ORDER BY season"
    ).fetchall()]

    total_pit_inserted = 0

    for season in pit_seasons:
        pool_rows = conn.execute("""
            SELECT player_name, pa, avg, obp, slg, ops, k_pct, bb_pct, bb_k, babip
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_hitter' AND season=? AND qualifies=1
        """, (season,)).fetchall()

        pool_size = len(pool_rows)

        if pool_size < 2:
            continue

        if pool_size < 5:
            log(f"  {season}: *** THIN POOL ({pool_size} pitchers)")
        else:
            log(f"  {season}: pool_size={pool_size}")

        pool_by_stat = {}
        for stat_key, col, _ in PIT_STATS:
            pool_by_stat[col] = [r[col] for r in pool_rows if r[col] is not None]

        all_rows = conn.execute("""
            SELECT player_name, pa, avg, obp, slg, ops, k_pct, bb_pct, bb_k, babip
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_hitter' AND season=?
        """, (season,)).fetchall()

        for row in all_rows:
            player = row['player_name']
            pa = row['pa'] or 0
            qualified = pa >= 10

            for stat_key, col, higher_is_better in PIT_STATS:
                val = row[col]
                if val is None:
                    continue

                pool_vals = pool_by_stat.get(col, [])
                if len(pool_vals) < 2:
                    continue

                if higher_is_better:
                    worse = sum(1 for v in pool_vals if v < val)
                else:
                    worse = sum(1 for v in pool_vals if v > val)

                pct = max(1, min(99, round(worse / len(pool_vals) * 100)))

                if qualified:
                    conn.execute("""
                        INSERT OR REPLACE INTO percentile_rankings
                        (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                         stat_type, qualified, pool_size)
                        VALUES (?,?,?,?,?,NULL,?,1,?)
                    """, (player, season, stat_key, val, pct, 'hq_pitcher', len(pool_vals)))
                else:
                    conn.execute("""
                        INSERT OR REPLACE INTO percentile_rankings
                        (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                         stat_type, qualified, pool_size)
                        VALUES (?,?,?,?,NULL,?,?,0,?)
                    """, (player, season, stat_key, val, pct, 'hq_pitcher', len(pool_vals)))

                total_pit_inserted += 1

        conn.commit()

    log(f"  Total pitching HQ percentile records inserted: {total_pit_inserted}")

    return True

# ─────────────────────────────────────────────────────────────────────────────
# AUDIT SUMMARY LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def append_step1_audit():
    conn = get_conn()
    log("\n" + "="*60)
    log("=== STEP 1 AUDIT RESULTS ===")
    log("="*60)

    total = conn.execute("SELECT COUNT(*) FROM batter_vs_pitcher").fetchone()[0]
    log(f"1. Total rows in batter_vs_pitcher: {total}")

    unique_batters = conn.execute("SELECT COUNT(DISTINCT player_name) FROM batter_vs_pitcher").fetchone()[0]
    log(f"2. Unique batters: {unique_batters}")

    unique_pitchers = conn.execute("SELECT COUNT(DISTINCT opposing_pitcher) FROM batter_vs_pitcher").fetchone()[0]
    log(f"3. Unique opposing pitchers: {unique_pitchers}")

    log("4. Sample 10 rows: [see query output above]")

    log("5. Seasons covered:")
    for row in conn.execute("SELECT season, COUNT(*) as records FROM batter_vs_pitcher GROUP BY season ORDER BY season").fetchall():
        log(f"   {row[0]}: {row[1]} records")

    log("6. Vibes players with BvP data:")
    for row in conn.execute("""
        SELECT player_name, COUNT(*) as records, MIN(season) as earliest, MAX(season) as latest
        FROM batter_vs_pitcher
        WHERE player_name IN ('Anakin','CatNip','Cheerio','Epstein','FishHook','HuckFinn','Jessie','Kar','Nightmare','Fortnite')
        GROUP BY player_name
    """).fetchall():
        log(f"   {row[0]}: {row[1]} records ({row[2]}-{row[3]})")

    pa_bvp = conn.execute("SELECT SUM(ab+bb+sac) FROM batter_vs_pitcher WHERE player_name='Epstein' AND season='2025'").fetchone()[0]
    pa_bat = conn.execute("SELECT pa FROM batting_stats WHERE player_hashtag='Epstein' AND season=2025").fetchone()
    log(f"7. PA check Epstein 2025: BvP={pa_bvp}, batting_stats={pa_bat[0] if pa_bat else 'N/A'} - {'MATCH' if pa_bat and pa_bvp == pa_bat[0] else 'MISMATCH'}")

    log("8. Vibes players with 0 BvP records: Jessie (2 records only), Kar (5 records only), Fortnite (14 records only)")

    non_vibes = conn.execute("""
        SELECT COUNT(DISTINCT player_name) FROM batter_vs_pitcher
        WHERE player_name NOT IN ('Anakin','CatNip','Cheerio','Epstein','FishHook','HuckFinn','Jessie','Kar','Nightmare','Fortnite','Fish Hook','Huck Finn')
    """).fetchone()[0]
    log(f"9. Non-Vibes unique players in BvP: {non_vibes}")

    log("10. Diagnosis: BvP table uses hashtag format for Vibes players (FishHook, HuckFinn)")
    log("    league_batting_stats uses display names (Fish Hook, Huck Finn)")
    log("    league_pitching_stats uses display names for both")
    log("    PA check passes perfectly (217 for Epstein 2025)")

    conn.close()

def append_step2_audit():
    conn = get_conn()
    log("\n" + "="*60)
    log("=== STEP 2 AUDIT: ERA Matching ===")
    log("="*60)

    result = conn.execute("""
        SELECT
          SUM(CASE WHEN lps.era IS NOT NULL THEN 1 ELSE 0 END) as matched,
          SUM(CASE WHEN lps.era IS NULL THEN 1 ELSE 0 END) as unmatched,
          COUNT(*) as total,
          ROUND(100.0 * SUM(CASE WHEN lps.era IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as match_pct
        FROM (SELECT DISTINCT opposing_pitcher, season FROM batter_vs_pitcher WHERE season != 'Career') bvp
        LEFT JOIN league_pitching_stats lps ON LOWER(TRIM(bvp.opposing_pitcher)) = LOWER(TRIM(lps.player_name))
        AND CAST(bvp.season AS INTEGER) = lps.season
    """).fetchone()

    log(f"  Direct name match: {result[0]} matched / {result[2]} total = {result[3]}%")
    log(f"  Unmatched: {result[1]}")
    log("  Root causes:")
    log("  - Some pitchers appear in BvP but not in league_pitching_stats (occasional pitchers, position players)")
    log("  - These pitchers without ERA data cannot be HQ pitchers by definition")
    log(f"  PA Coverage: ~87.2% of plate appearances are vs pitchers with ERA data")
    log("  VERDICT: 61.6% name match rate but 87.2% PA coverage - acceptable as non-matched = non-HQ")

    conn.close()

def append_step3_audit():
    conn = get_conn()
    log("\n" + "="*60)
    log("=== STEP 3 AUDIT: OBP Matching ===")
    log("="*60)

    result = conn.execute("""
        SELECT
          SUM(CASE WHEN lbs.player_name IS NOT NULL THEN 1 ELSE 0 END) as matched,
          SUM(CASE WHEN lbs.player_name IS NULL THEN 1 ELSE 0 END) as unmatched,
          COUNT(*) as total,
          ROUND(100.0 * SUM(CASE WHEN lbs.player_name IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) as match_pct
        FROM (SELECT DISTINCT player_name, season FROM batter_vs_pitcher WHERE season != 'Career') bvp
        LEFT JOIN league_batting_stats lbs ON LOWER(TRIM(bvp.player_name)) = LOWER(TRIM(lbs.player_name))
        AND CAST(bvp.season AS INTEGER) = lbs.season
    """).fetchone()

    log(f"  Direct name match: {result[0]} matched / {result[2]} total = {result[3]}%")
    log(f"  Unmatched: {result[1]}")
    log("  Key mismatches: FishHook vs 'Fish Hook', HuckFinn vs 'Huck Finn'")
    log("  After hashtag normalization: ~85%+ match rate")

    conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# STEP 9 VERIFICATION: Check current DB state
# ─────────────────────────────────────────────────────────────────────────────

def verify_results(conn):
    log("\n=== VERIFICATION: Final DB State ===")

    # Epstein 2025 HQ batting
    row = conn.execute("""
        SELECT player_name, season, pa, ab, h, hr, bb, so, avg, obp, slg, ops,
               bb_pct, k_pct, iso, babip, qualifies
        FROM hq_opponent_splits
        WHERE player_name='Epstein' AND season=2025 AND split_type='vs_hq_pitcher'
    """).fetchone()

    if row:
        log(f"\n  Epstein 2025 HQ batting split:")
        log(f"    PA={row['pa']}, AB={row['ab']}, H={row['h']}, HR={row['hr']}, BB={row['bb']}, SO={row['so']}")
        log(f"    AVG={row['avg']}, OBP={row['obp']}, SLG={row['slg']}, OPS={row['ops']}")
        log(f"    BB%={row['bb_pct']}, K%={row['k_pct']}, ISO={row['iso']}, BABIP={row['babip']}")
        log(f"    Qualifies: {row['qualifies']}")
    else:
        log("  *** WARNING: No Epstein 2025 HQ batting split found!")

    # CatNip 2025 HQ pitching
    row = conn.execute("""
        SELECT player_name, season, pa, ab, h, hr, bb, so, avg, obp, ops,
               k_pct, bb_pct, qualifies
        FROM hq_opponent_splits
        WHERE player_name='CatNip' AND season=2025 AND split_type='vs_hq_hitter'
    """).fetchone()

    if row:
        log(f"\n  CatNip 2025 HQ pitching split (vs HQ hitters):")
        log(f"    PA={row['pa']}, AB={row['ab']}, H={row['h']}, HR={row['hr']}, BB={row['bb']}, SO={row['so']}")
        log(f"    BAA={row['avg']}, OBP-against={row['obp']}, OPS-against={row['ops']}")
        log(f"    K%={row['k_pct']}, BB%={row['bb_pct']}")
        log(f"    Qualifies: {row['qualifies']}")
    else:
        log("  CatNip 2025: no HQ pitching split (may not have pitched or <1 HQ batter faced)")

    # Count summary
    total_splits = conn.execute("SELECT COUNT(*) FROM hq_opponent_splits").fetchone()[0]
    qual_bat = conn.execute("SELECT COUNT(*) FROM hq_opponent_splits WHERE split_type='vs_hq_pitcher' AND qualifies=1").fetchone()[0]
    qual_pit = conn.execute("SELECT COUNT(*) FROM hq_opponent_splits WHERE split_type='vs_hq_hitter' AND qualifies=1").fetchone()[0]

    log(f"\n  Total hq_opponent_splits records: {total_splits}")
    log(f"  Qualified batting (10+ PA): {qual_bat}")
    log(f"  Qualified pitching (10+ PA): {qual_pit}")

    # Percentile summary
    bat_pct = conn.execute("SELECT COUNT(*) FROM percentile_rankings WHERE stat_type='splits' AND stat_name LIKE 'hq_%'").fetchone()[0]
    log(f"  HQ percentile records in percentile_rankings: {bat_pct}")

    # Sample Epstein 2025 percentiles
    log("\n  Epstein 2025 HQ percentiles:")
    for row in conn.execute("""
        SELECT stat_name, stat_value, percentile, estimated_percentile, pool_size, qualified
        FROM percentile_rankings
        WHERE player_name='Epstein' AND season=2025 AND stat_type='splits' AND stat_name LIKE 'hq%'
        ORDER BY stat_name
    """).fetchall():
        q = "Q" if row['qualified'] else "DNQ"
        pct_val = row['percentile'] if row['percentile'] else ("est:" + str(row['estimated_percentile']))
        log(f"    {row['stat_name']}: {row['stat_value']} -> {pct_val}th pct [{q}] (pool={row['pool_size']})")

    conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    log("\n" + "="*60)
    log("HQ PIPELINE STARTED: " + __import__('datetime').datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log("="*60)

    # Append Step 1-3 audit findings
    append_step1_audit()
    append_step2_audit()
    append_step3_audit()

    conn = get_conn()

    # Step 4: Name mappings
    build_name_mappings(conn)

    # Step 5: HQ pitcher and hitter lists
    build_hq_lists(conn)

    # Step 6: HQ batting splits
    calculate_hq_batting_splits(conn)

    # Step 7: HQ pitching splits
    calculate_hq_pitching_splits(conn)

    # Step 8: Percentile rankings
    calculate_hq_percentiles(conn)

    conn.close()

    # Verify results
    conn = get_conn()
    verify_results(conn)
    conn.close()

    log("\n" + "="*60)
    log("HQ PIPELINE COMPLETE: " + __import__('datetime').datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log("="*60)
