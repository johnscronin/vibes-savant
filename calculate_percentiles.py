#!/usr/bin/env python3
"""
Vibes Savant — Percentile Calculator
Calculates season percentile rankings for all Vibes players against
the full HRL qualified player pool for each season.

Formula: percentile = round((players with WORSE value / total qualified) * 100)
Clamped to [1, 99]. DNQ if player did not meet qualifier threshold.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]

# ── Batting stats — computed from batting_stats vs league_batting_stats ──────
# (stat_name, vibes_expr, league_expr, higher_is_better)
# For Vibes: expressions using batting_stats columns
# For league pool: expressions using league_batting_stats columns
# Note: league_batting_stats uses ab+bb as PA proxy (no pa column)
BATTING_POOL_STATS = [
    ('ops',    'ops',   'ops',   True),
    ('obp',    'obp',   'obp',   True),
    ('avg',    'avg',   'avg',   True),
    ('slg',    'slg',   'slg',   True),
    # AB/HR: lower is better
    ('ab_hr',
     'ROUND(ab*1.0/NULLIF(hr,0),1)',
     'ROUND(ab*1.0/NULLIF(hr,0),1)',
     False),
    # BB% (walk rate): higher is better — Vibes uses actual pa, league uses ab+bb proxy
    ('bb_pct',
     'ROUND(bb*1.0/NULLIF(pa,0),3)',
     'ROUND(bb*1.0/NULLIF(ab+bb,0),3)',
     True),
    # K% (strikeout rate): lower is better — INVERT
    ('k_pct',
     'ROUND(so*1.0/NULLIF(pa,0),3)',
     'ROUND(so*1.0/NULLIF(ab+bb,0),3)',
     False),
    # BB/K: higher is better
    ('bb_k',
     'ROUND(bb*1.0/NULLIF(so,0),2)',
     'ROUND(bb*1.0/NULLIF(so,0),2)',
     True),
]

# ── Pitching stats — computed from pitching_stats vs league_pitching_stats ───
# Vibes uses pitching_stats column names; league uses league_pitching_stats names
# Note: league_pitching_stats: h=hits_allowed, bb=walks, k=strikeouts, r=runs
# For pit_k_pct/pit_bb_pct: BF_approx = ip*3 + h + bb
PITCHING_POOL_STATS = [
    ('era',    'era',           'era',    False),  # lower is better
    ('whip',   'whip',          'whip',   False),  # lower is better
    ('k_per_6','k_per_6',       'k_per_6', True),
    ('bb_per_6','opp_bb_per_6', 'bb_per_6', False), # lower is better
    ('baa',    'baa',           'baa',    False),  # lower is better
    ('ip',     'ip',            'ip',     True),
    # Pitcher K% = K/BF_approx: higher is better
    ('pit_k_pct',
     'ROUND(k*1.0/NULLIF(ip*3.0+ha+opp_bb,0),3)',
     'ROUND(k*1.0/NULLIF(ip*3.0+h+bb,0),3)',
     True),
    # Pitcher BB% = BB/BF_approx: lower is better — INVERT
    ('pit_bb_pct',
     'ROUND(opp_bb*1.0/NULLIF(ip*3.0+ha+opp_bb,0),3)',
     'ROUND(bb*1.0/NULLIF(ip*3.0+h+bb,0),3)',
     False),
    # Pitcher BABIP: lower is better — INVERT
    ('pit_babip',
     'ROUND((ha-opp_hr)*1.0/NULLIF(ip*3+ha-k-opp_hr,0),3)',
     'ROUND((h-hr)*1.0/NULLIF(ip*3+h-k-hr,0),3)',
     False),
]

# ── Custom stats — pool from custom_stats (all players) ─────────────────────
# (stat_name, cs_col, higher_is_better, qual_field)
CUSTOM_STATS = [
    ('ops_plus',       'ops_plus',       True,  'bat'),
    ('era_plus',       'era_plus',       True,  'pit'),
]

# ── HQ splits stats ──────────────────────────────────────────────────────────
# All 8 batting stats computed vs HQ pitcher opponents
# Pool = all Vibes players in hq_opponent_splits with pa >= 10 that season
# (stat_name, col_or_expr, higher_is_better)
HQ_SPLITS_STATS = [
    ('hq_ops',    'ops',   True),
    ('hq_obp',    'obp',   True),
    ('hq_avg',    'avg',   True),
    ('hq_slg',    'slg',   True),
    ('hq_ab_hr',  'ROUND(ab*1.0/NULLIF(hr,0),1)',   False),
    ('hq_bb_pct', 'ROUND(bb*1.0/NULLIF(pa,0),3)',   True),
    ('hq_k_pct',  'ROUND(so*1.0/NULLIF(pa,0),3)',   False),
    ('hq_bb_k',   'ROUND(bb*1.0/NULLIF(so,0),2)',   True),
]

# ── Tier splits (OPS by opponent tier) ───────────────────────────────────────
TIER_SPLITS_STATS = [
    ('elite_ops', 'opponent_tier_splits', "tier='Elite' AND split_role='batting'",   'ops', True),
    ('avg_ops',   'opponent_tier_splits', "tier='Average' AND split_role='batting'", 'ops', True),
    ('weak_ops',  'opponent_tier_splits', "tier='Weak' AND split_role='batting'",    'ops', True),
]


def create_percentile_table(conn):
    conn.execute("DROP TABLE IF EXISTS percentile_rankings")
    conn.execute("""
    CREATE TABLE percentile_rankings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT NOT NULL,
        season INTEGER NOT NULL,
        stat_name TEXT NOT NULL,
        stat_value REAL,
        percentile INTEGER,
        estimated_percentile INTEGER,  -- for DNQ players: what rank would be if qualified
        stat_type TEXT NOT NULL,   -- 'batting', 'pitching', 'custom', 'splits'
        qualified INTEGER NOT NULL DEFAULT 1,
        pool_size INTEGER,
        qualifier_text TEXT,
        UNIQUE(player_name, season, stat_name, stat_type)
    )
    """)
    conn.commit()


def calc_percentile(value, pool, higher_is_better):
    """percentile = round((# with worse value / total) * 100). Clamped [1, 99]."""
    if value is None:
        return None
    n = len(pool)
    if n < 5:
        return None  # pool too small

    if higher_is_better:
        worse = sum(1 for v in pool if v < value)
    else:
        worse = sum(1 for v in pool if v > value)

    pct = round((worse / n) * 100)
    return max(1, min(99, pct))


def vibes_bat_qualified(conn, player, season, bat_min_pa):
    """True if Vibes player met batting PA qualifier."""
    if bat_min_pa is None:
        return True
    row = conn.execute(
        "SELECT pa FROM batting_stats WHERE player_hashtag=? AND season=?", (player, season)
    ).fetchone()
    return bool(row and row[0] is not None and row[0] >= bat_min_pa)


def vibes_pit_qualified(conn, player, season, min_ip, min_g):
    """True if Vibes player met pitching qualifier."""
    row = conn.execute(
        "SELECT ip, g FROM pitching_stats WHERE player_hashtag=? AND season=? AND ip > 0",
        (player, season)
    ).fetchone()
    if not row:
        return False
    ip = row[0] or 0
    g  = row[1] or 0
    if min_ip is None and min_g is None:
        return ip > 0
    return ip >= (min_ip or 0) or g >= (min_g or 0)


def calculate_all():
    conn = sqlite3.connect(DB_PATH)
    create_percentile_table(conn)

    # All seasons with league batting data
    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM league_batting_stats ORDER BY season"
    ).fetchall()]

    total_inserted = 0

    for season in seasons:
        # ── Season qualifier thresholds ────────────────────────
        q = conn.execute(
            "SELECT batting_qualifier, batting_min_pa, pitching_qualifier, pitching_min_ip, pitching_min_g "
            "FROM season_qualifiers WHERE season=?", (season,)
        ).fetchone()
        bat_qual_text = q[0] if q else ''
        bat_min_pa   = q[1] if q else None
        pit_qual_text = q[2] if q else ''
        pit_min_ip   = q[3] if q else None
        pit_min_g    = q[4] if q else None

        # ── BATTING PERCENTILES ────────────────────────────────
        for stat_name, vibes_expr, league_expr, higher in BATTING_POOL_STATS:
            # Build league pool (filter: pa proxy = ab+bb >= min_pa if set)
            if bat_min_pa:
                pool_sql = (f"SELECT {league_expr} FROM league_batting_stats "
                            f"WHERE season=? AND ({league_expr}) IS NOT NULL "
                            f"AND (ab+bb) >= {bat_min_pa}")
            else:
                pool_sql = (f"SELECT {league_expr} FROM league_batting_stats "
                            f"WHERE season=? AND ({league_expr}) IS NOT NULL")
            # For ab_hr, only include players with hr > 0
            if stat_name == 'ab_hr':
                pool_sql += " AND hr > 0"
            # For bb_k, only include players with so > 0
            if stat_name == 'bb_k':
                pool_sql += " AND so > 0"

            pool = [r[0] for r in conn.execute(pool_sql, (season,)).fetchall() if r[0] is not None]

            for player in VIBES_PLAYERS:
                # Get Vibes player value
                if stat_name in ('ops', 'obp', 'avg', 'slg'):
                    row = conn.execute(
                        f"SELECT {vibes_expr} FROM batting_stats "
                        f"WHERE player_hashtag=? AND season=?",
                        (player, season)
                    ).fetchone()
                else:
                    # Computed expression — use batting_stats
                    filter_extra = ""
                    if stat_name == 'ab_hr':
                        filter_extra = " AND hr > 0"
                    elif stat_name == 'bb_k':
                        filter_extra = " AND so > 0"
                    elif stat_name == 'bb_pct':
                        filter_extra = " AND pa > 0"
                    elif stat_name == 'k_pct':
                        filter_extra = " AND pa > 0"
                    row = conn.execute(
                        f"SELECT {vibes_expr} FROM batting_stats "
                        f"WHERE player_hashtag=? AND season=?{filter_extra}",
                        (player, season)
                    ).fetchone()

                stat_value = row[0] if row else None
                if stat_value is None:
                    continue

                qualified = vibes_bat_qualified(conn, player, season, bat_min_pa)
                percentile = None
                est_pct = None
                if len(pool) >= 5:
                    if qualified:
                        percentile = calc_percentile(stat_value, pool, higher)
                    else:
                        est_pct = calc_percentile(stat_value, pool, higher)

                conn.execute("""
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?, ?, ?, ?, ?, ?, 'batting', ?, ?, ?)
                """, (player, season, stat_name, stat_value, percentile, est_pct,
                      1 if qualified else 0, len(pool), bat_qual_text))
                total_inserted += 1

        # ── PITCHING PERCENTILES ───────────────────────────────
        for stat_name, vibes_expr, league_expr, higher in PITCHING_POOL_STATS:
            # Build league pool (filter by ip >= min_ip if set)
            if pit_min_ip:
                pool_filter = f"AND ip >= {pit_min_ip}"
            elif pit_min_g:
                pool_filter = f"AND g >= {pit_min_g}"
            else:
                pool_filter = "AND ip > 0"

            pool_sql = (f"SELECT {league_expr} FROM league_pitching_stats "
                        f"WHERE season=? AND ({league_expr}) IS NOT NULL {pool_filter}")
            pool = [r[0] for r in conn.execute(pool_sql, (season,)).fetchall() if r[0] is not None]

            for player in VIBES_PLAYERS:
                filter_extra = " AND ip > 0"
                row = conn.execute(
                    f"SELECT {vibes_expr} FROM pitching_stats "
                    f"WHERE player_hashtag=? AND season=?{filter_extra}",
                    (player, season)
                ).fetchone()
                stat_value = row[0] if row else None
                if stat_value is None:
                    continue

                qualified = vibes_pit_qualified(conn, player, season, pit_min_ip, pit_min_g)
                percentile = None
                est_pct = None
                if len(pool) >= 5:
                    if qualified:
                        percentile = calc_percentile(stat_value, pool, higher)
                    else:
                        est_pct = calc_percentile(stat_value, pool, higher)

                conn.execute("""
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?, ?, ?, ?, ?, ?, 'pitching', ?, ?, ?)
                """, (player, season, stat_name, stat_value, percentile, est_pct,
                      1 if qualified else 0, len(pool), pit_qual_text))
                total_inserted += 1

        # ── CUSTOM STATS (ops_plus, era_plus) ─────────────────
        for stat_name, cs_col, higher, qual_field in CUSTOM_STATS:
            if qual_field == 'bat':
                pool_rows = conn.execute(
                    f"SELECT {cs_col} FROM custom_stats WHERE season=? AND {cs_col} IS NOT NULL AND bat_qualified=1",
                    (season,)
                ).fetchall()
            else:
                pool_rows = conn.execute(
                    f"SELECT {cs_col} FROM custom_stats WHERE season=? AND {cs_col} IS NOT NULL AND pit_qualified=1",
                    (season,)
                ).fetchall()
            pool = [r[0] for r in pool_rows if r[0] is not None]

            for player in VIBES_PLAYERS:
                row = conn.execute(
                    f"SELECT {cs_col}, bat_qualified, pit_qualified FROM custom_stats "
                    f"WHERE player_name=? AND season=?",
                    (player, season)
                ).fetchone()
                if not row or row[0] is None:
                    continue

                stat_value = row[0]
                qualified  = bool(row[1]) if qual_field == 'bat' else bool(row[2])
                qual_text  = bat_qual_text if qual_field == 'bat' else pit_qual_text

                percentile = None
                if qualified and len(pool) >= 5:
                    percentile = calc_percentile(stat_value, pool, higher)

                conn.execute("""
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?, ?, ?, ?, ?, NULL, 'custom', ?, ?, ?)
                """, (player, season, stat_name, stat_value, percentile,
                      1 if qualified else 0, len(pool), qual_text))
                total_inserted += 1

        conn.commit()

    # ── HQ SPLITS PERCENTILES ─────────────────────────────────
    HQ_MIN_PA = 10
    hq_seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM hq_opponent_splits ORDER BY season"
    ).fetchall()]

    for season in hq_seasons:
        for stat_name, col_expr, higher in HQ_SPLITS_STATS:
            # Pool: all Vibes players with pa >= 10 in hq_opponent_splits that season
            pool_sql = (f"SELECT {col_expr} FROM hq_opponent_splits "
                        f"WHERE season=? AND split_type='vs_hq_pitcher' AND pa >= {HQ_MIN_PA} "
                        f"AND ({col_expr}) IS NOT NULL")
            # Only include rows where denominator is non-zero for computed stats
            if 'NULLIF' in col_expr:
                pass  # NULLIF handles 0 denominators
            pool = [r[0] for r in conn.execute(pool_sql, (season,)).fetchall() if r[0] is not None]

            for player in VIBES_PLAYERS:
                row = conn.execute(
                    f"SELECT {col_expr} FROM hq_opponent_splits "
                    f"WHERE player_name=? AND season=? AND split_type='vs_hq_pitcher' AND pa >= {HQ_MIN_PA}",
                    (player, season)
                ).fetchone()
                if not row or row[0] is None:
                    continue

                stat_value = row[0]
                percentile = calc_percentile(stat_value, pool, higher) if len(pool) >= 2 else None

                conn.execute("""
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?, ?, ?, ?, ?, NULL, 'splits', 1, ?, ?)
                """, (player, season, stat_name, stat_value, percentile, len(pool),
                      f'min {HQ_MIN_PA} PA vs HQ'))
                total_inserted += 1

    # ── TIER SPLITS PERCENTILES ────────────────────────────────
    tier_seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM opponent_tier_splits ORDER BY season"
    ).fetchall()]

    for season in tier_seasons:
        for stat_name, table, where, col, higher in TIER_SPLITS_STATS:
            pool_rows = conn.execute(
                f"SELECT {col} FROM {table} WHERE season=? AND {where} AND {col} IS NOT NULL",
                (season,)
            ).fetchall()
            pool = [r[0] for r in pool_rows]
            for player in VIBES_PLAYERS:
                row = conn.execute(
                    f"SELECT {col} FROM {table} WHERE player_name=? AND season=? AND {where}",
                    (player, season)
                ).fetchone()
                if not row or row[0] is None:
                    continue
                stat_value = row[0]
                percentile = calc_percentile(stat_value, pool, higher) if len(pool) >= 2 else None
                conn.execute("""
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?, ?, ?, ?, ?, NULL, 'splits', 1, ?, '')
                """, (player, season, stat_name, stat_value, percentile, len(pool)))
                total_inserted += 1

    # ── HQ PITCHER SPLITS (pitchers vs HQ batters) ────────────
    # HQ batter = a player whose OBP >= .350 that season in batting_stats
    # Stats from BvP aggregation: baa, pit_k_pct, pit_bb_pct, pit_babip
    # ERA/WHIP/K_per_6/BB_per_6/IP from regular pitching_stats (season totals)
    # Minimum: 10 PA from HQ batters faced
    # Pool: Vibes pitchers with 10+ HQ batter PA that season
    HQ_BAT_THRESHOLD = 0.350
    HQ_PIT_MIN_PA    = 10

    # Stat configs: (stat_name, higher_is_better)
    HQ_PIT_STATS = [
        ('hqpit_era',     False),
        ('hqpit_whip',    False),
        ('hqpit_k_per_6', True),
        ('hqpit_bb_per_6',False),
        ('hqpit_baa',     False),
        ('hqpit_pit_babip',False),
        ('hqpit_pit_k_pct',True),
        ('hqpit_pit_bb_pct',False),
        ('hqpit_ip',      True),
    ]

    bvp_seasons = sorted({r[0] for r in conn.execute(
        "SELECT DISTINCT CAST(season AS INTEGER) FROM batter_vs_pitcher WHERE tab_type='regular'"
    ).fetchall()})

    for season in bvp_seasons:
        season_str = str(season)

        # Find HQ batters this season
        hq_batters = {r[0] for r in conn.execute("""
            SELECT DISTINCT player_hashtag FROM batting_stats
            WHERE season=? AND obp >= ?
        """, (season, HQ_BAT_THRESHOLD)).fetchall()}

        if not hq_batters:
            continue

        # For each Vibes pitcher, aggregate BvP stats from HQ batters
        pit_stats_by_player = {}
        for pitcher in VIBES_PLAYERS:
            rows = conn.execute("""
                SELECT ab, h, hr, bb, so, sac
                FROM batter_vs_pitcher
                WHERE opposing_pitcher=? AND season=? AND tab_type='regular'
                  AND player_name IN ({})
            """.format(','.join('?'*len(hq_batters))),
                [pitcher, season_str] + list(hq_batters)
            ).fetchall()
            if not rows:
                continue
            ab  = sum(r[0] or 0 for r in rows)
            h   = sum(r[1] or 0 for r in rows)
            hr  = sum(r[2] or 0 for r in rows)
            bb  = sum(r[3] or 0 for r in rows)
            so  = sum(r[4] or 0 for r in rows)
            sac = sum(r[5] or 0 for r in rows)
            pa  = ab + bb + sac
            if pa < HQ_PIT_MIN_PA:
                continue
            # Computed split stats
            baa       = round(h / ab, 3)          if ab > 0          else None
            bip       = ab - so - hr
            pit_babip = round((h-hr)/bip, 3)      if bip > 0         else None
            bf        = ab + bb + sac
            pit_k_pct = round(so / bf, 3)          if bf > 0          else None
            pit_bb_pct= round(bb / bf, 3)          if bf > 0          else None
            # Regular season stats from pitching_stats
            ps = conn.execute("""
                SELECT era, whip, k_per_6, opp_bb_per_6, ip
                FROM pitching_stats WHERE player_hashtag=? AND season=? AND ip > 0
            """, (pitcher, season)).fetchone()
            pit_stats_by_player[pitcher] = {
                'hqpit_baa':      baa,
                'hqpit_pit_babip':pit_babip,
                'hqpit_pit_k_pct':pit_k_pct,
                'hqpit_pit_bb_pct':pit_bb_pct,
                'hqpit_era':      ps[0] if ps else None,
                'hqpit_whip':     ps[1] if ps else None,
                'hqpit_k_per_6':  ps[2] if ps else None,
                'hqpit_bb_per_6': ps[3] if ps else None,
                'hqpit_ip':       ps[4] if ps else None,
                'pa_from_hq': pa,
            }

        if not pit_stats_by_player:
            continue

        # Pool per stat = all pitchers who qualified
        for stat_name, higher in HQ_PIT_STATS:
            pool = [v[stat_name] for v in pit_stats_by_player.values()
                    if v.get(stat_name) is not None]
            for pitcher, stats in pit_stats_by_player.items():
                val = stats.get(stat_name)
                if val is None:
                    continue
                pct = calc_percentile(val, pool, higher) if len(pool) >= 2 else None
                conn.execute("""
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?, ?, ?, ?, ?, NULL, 'hq_pitcher', 1, ?, ?)
                """, (pitcher, season, stat_name, val, pct, len(pool),
                      f'min {HQ_PIT_MIN_PA} PA vs HQ batters (OBP≥.350)'))
                total_inserted += 1

    conn.commit()
    print(f"\nInserted {total_inserted} percentile records across {len(seasons)} seasons")

    # ── POOL SIZE AUDIT (Step 7) ───────────────────────────────
    print("\n=== POOL SIZE AUDIT ===")
    for season in sorted(seasons):
        q = conn.execute(
            "SELECT batting_min_pa, pitching_min_ip, pitching_min_g FROM season_qualifiers WHERE season=?",
            (season,)
        ).fetchone()
        bat_min = q[0] if q else None
        pit_min_ip = q[1] if q else None
        pit_min_g  = q[2] if q else None

        if bat_min:
            bat_pool = conn.execute(
                "SELECT COUNT(*) FROM league_batting_stats WHERE season=? AND (ab+bb) >= ?",
                (season, bat_min)
            ).fetchone()[0]
        else:
            bat_pool = conn.execute(
                "SELECT COUNT(*) FROM league_batting_stats WHERE season=?", (season,)
            ).fetchone()[0]

        if pit_min_ip:
            pit_pool = conn.execute(
                "SELECT COUNT(*) FROM league_pitching_stats WHERE season=? AND ip >= ?",
                (season, pit_min_ip)
            ).fetchone()[0]
        else:
            pit_pool = conn.execute(
                "SELECT COUNT(*) FROM league_pitching_stats WHERE season=? AND ip > 0",
                (season,)
            ).fetchone()[0]

        hq_pool = conn.execute(
            "SELECT COUNT(DISTINCT player_name) FROM hq_opponent_splits WHERE season=? AND pa >= 10",
            (season,)
        ).fetchone()[0] if season in hq_seasons else 0

        flag = " ⚠ SMALL" if bat_pool < 10 or pit_pool < 5 else ""
        print(f"  {season}: bat_pool={bat_pool:3d} (min PA={bat_min})  "
              f"pit_pool={pit_pool:3d} (min IP={pit_min_ip})  "
              f"hq_pool={hq_pool:2d}{flag}")

    # ── SANITY CHECK: Epstein 2025 ─────────────────────────────
    print("\n=== SANITY CHECK: Epstein 2025 Batting Percentiles ===")
    rows = conn.execute("""
        SELECT stat_name, stat_value, percentile, pool_size, qualified
        FROM percentile_rankings
        WHERE player_name='Epstein' AND season=2025 AND stat_type IN ('batting','custom')
        ORDER BY stat_name
    """).fetchall()
    for stat, val, pct, pool, qual in rows:
        status = f"{pct}th pct (pool: {pool})" if pct is not None else "DNQ / no data"
        q_flag = '✓' if qual else '✗'
        print(f"  {stat:14s}: {val:.4f}  →  {status}  {q_flag}")

    ops_row = conn.execute("""
        SELECT stat_value, percentile, pool_size FROM percentile_rankings
        WHERE player_name='Epstein' AND season=2025 AND stat_name='ops' AND stat_type='batting'
    """).fetchone()
    if ops_row:
        print(f"\n  OPS check: {ops_row[0]:.3f} → {ops_row[1]}th percentile (pool: {ops_row[2]})")
        diff = abs((ops_row[1] or 0) - 83)
        if diff <= 5:
            print(f"  ✓ PASS — within 5 points of expected 83rd")
        else:
            print(f"  ✗ WARNING — {diff} points off expected 83rd (check pool size)")

    print("\n=== Epstein 2025 Pitching Percentiles ===")
    pit_rows = conn.execute("""
        SELECT stat_name, stat_value, percentile, pool_size, qualified
        FROM percentile_rankings
        WHERE player_name='Epstein' AND season=2025 AND stat_type='pitching'
        ORDER BY stat_name
    """).fetchall()
    for stat, val, pct, pool, qual in pit_rows:
        status = f"{pct}th pct (pool: {pool})" if pct is not None else "no data"
        print(f"  {stat:14s}: {val}  →  {status}  {'✓' if qual else '✗'}")

    print("\n=== Epstein 2025 HQ Splits Percentiles ===")
    hq_rows = conn.execute("""
        SELECT stat_name, stat_value, percentile, pool_size
        FROM percentile_rankings
        WHERE player_name='Epstein' AND season=2025 AND stat_type='splits' AND stat_name LIKE 'hq_%'
        ORDER BY stat_name
    """).fetchall()
    for stat, val, pct, pool in hq_rows:
        status = f"{pct}th pct (pool: {pool})" if pct is not None else "no data"
        print(f"  {stat:14s}: {val}  →  {status}")

    conn.close()


if __name__ == '__main__':
    calculate_all()
