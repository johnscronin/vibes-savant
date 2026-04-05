#!/usr/bin/env python3
"""
Phase 2 Step 4 — Calculate percentiles for ALL players in the database.
Extended version of calculate_percentiles.py — handles all players, not just Vibes.
Uses INSERT OR REPLACE. Safe to re-run.
"""

import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')

# These all reference batting_stats columns (same for Vibes and all players now)
BATTING_POOL_STATS = [
    ('ops',    'ops',   'ops',   True),
    ('obp',    'obp',   'obp',   True),
    ('avg',    'avg',   'avg',   True),
    ('slg',    'slg',   'slg',   True),
    ('ab_hr',  'ROUND(ab*1.0/NULLIF(hr,0),1)',        'ROUND(ab*1.0/NULLIF(hr,0),1)',        False),
    ('bb_pct', 'ROUND(bb*1.0/NULLIF(pa,0),3)',         'ROUND(bb*1.0/NULLIF(ab+bb,0),3)',     True),
    ('k_pct',  'ROUND(so*1.0/NULLIF(pa,0),3)',         'ROUND(so*1.0/NULLIF(ab+bb,0),3)',     False),
    ('bb_k',   'ROUND(bb*1.0/NULLIF(so,0),2)',         'ROUND(bb*1.0/NULLIF(so,0),2)',        True),
]

PITCHING_POOL_STATS = [
    ('era',         'era',                                                       'era',                                                    False),
    ('whip',        'whip',                                                      'whip',                                                   False),
    ('k_per_6',     'k_per_6',                                                   'k_per_6',                                                True),
    ('bb_per_6',    'opp_bb_per_6',                                              'bb_per_6',                                               False),
    ('baa',         'baa',                                                       'baa',                                                    False),
    ('ip',          'ip',                                                        'ip',                                                     True),
    ('pit_k_pct',   'ROUND(k*1.0/NULLIF(ip*3.0+ha+opp_bb,0),3)',                'ROUND(k*1.0/NULLIF(ip*3.0+h+bb,0),3)',                   True),
    ('pit_bb_pct',  'ROUND(opp_bb*1.0/NULLIF(ip*3.0+ha+opp_bb,0),3)',           'ROUND(bb*1.0/NULLIF(ip*3.0+h+bb,0),3)',                  False),
    ('pit_babip',   'ROUND((ha-opp_hr)*1.0/NULLIF(ip*3+ha-k-opp_hr,0),3)',      'ROUND((h-hr)*1.0/NULLIF(ip*3+h-k-hr,0),3)',              False),
]

CUSTOM_STATS = [
    ('ops_plus', 'ops_plus', True,  'bat'),
    ('era_plus', 'era_plus', True,  'pit'),
]

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

TIER_SPLITS_STATS = [
    ('elite_ops', "tier='Elite' AND split_role='batting'",   True),
    ('avg_ops',   "tier='Average' AND split_role='batting'", True),
    ('weak_ops',  "tier='Weak' AND split_role='batting'",    True),
]


def ensure_percentile_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS percentile_rankings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT NOT NULL,
        season INTEGER NOT NULL,
        stat_name TEXT NOT NULL,
        stat_value REAL,
        percentile INTEGER,
        estimated_percentile INTEGER,
        stat_type TEXT NOT NULL,
        qualified INTEGER NOT NULL DEFAULT 1,
        pool_size INTEGER,
        qualifier_text TEXT,
        UNIQUE(player_name, season, stat_name, stat_type)
    )
    """)
    conn.commit()


def calc_pct(value, pool, higher):
    if value is None or len(pool) < 5:
        return None
    if higher:
        worse = sum(1 for v in pool if v < value)
    else:
        worse = sum(1 for v in pool if v > value)
    return max(1, min(99, round((worse / len(pool)) * 100)))


def upsert(conn, player, season, stat_name, stat_type, stat_value, percentile, est_pct, qualified, pool_size, qual_text):
    conn.execute("""
        INSERT OR REPLACE INTO percentile_rankings
          (player_name, season, stat_name, stat_type, stat_value,
           percentile, estimated_percentile, qualified, pool_size, qualifier_text)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (player, season, stat_name, stat_type, stat_value,
          percentile, est_pct, qualified, pool_size, qual_text))


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_percentile_table(conn)

    # All seasons
    seasons = [r[0] for r in conn.execute(
        "SELECT DISTINCT season FROM league_batting_stats ORDER BY season"
    ).fetchall()]

    total = 0
    print(f"Processing percentiles for {len(seasons)} seasons...")

    for season in seasons:
        q = conn.execute(
            "SELECT batting_qualifier, batting_min_pa, pitching_qualifier, pitching_min_ip, pitching_min_g "
            "FROM season_qualifiers WHERE season=?", (season,)
        ).fetchone()
        bat_qual_text = q[0] if q else f'Min PA'
        bat_min_pa    = q[1] if q else None
        pit_qual_text = q[2] if q else f'Min IP'
        pit_min_ip    = q[3] if q else None
        pit_min_g     = q[4] if q else None

        # ── BATTING PERCENTILES ───────────────────────────────────────
        for stat_name, player_expr, league_expr, higher in BATTING_POOL_STATS:
            # Build league pool from league_batting_stats
            pool_sql = (f"SELECT {league_expr} FROM league_batting_stats "
                        f"WHERE season=? AND ({league_expr}) IS NOT NULL")
            if bat_min_pa:
                pool_sql += f" AND (ab+bb) >= {bat_min_pa}"
            if stat_name == 'ab_hr':
                pool_sql += " AND hr > 0"
            elif stat_name == 'bb_k':
                pool_sql += " AND so > 0"
            pool = [r[0] for r in conn.execute(pool_sql, (season,)).fetchall() if r[0] is not None]

            # Get all players with batting stats this season
            extra = ""
            if stat_name == 'ab_hr':  extra = " AND hr > 0"
            elif stat_name == 'bb_k': extra = " AND so > 0"
            elif stat_name == 'bb_pct': extra = " AND pa > 0"
            elif stat_name == 'k_pct': extra = " AND pa > 0"

            player_rows = conn.execute(
                f"SELECT player_hashtag, {player_expr}, pa FROM batting_stats "
                f"WHERE season=?{extra} AND ({player_expr}) IS NOT NULL",
                (season,)
            ).fetchall()

            for p_name, stat_val, pa in player_rows:
                qualified = bool(pa and bat_min_pa and pa >= bat_min_pa) if bat_min_pa else True
                pct = calc_pct(stat_val, pool, higher) if qualified else None
                est = calc_pct(stat_val, pool, higher) if not qualified else None
                upsert(conn, p_name, season, stat_name, 'batting',
                       stat_val, pct, est, 1 if qualified else 0,
                       len(pool), bat_qual_text)
                total += 1

        # ── PITCHING PERCENTILES ──────────────────────────────────────
        pit_seasons_avail = [r[0] for r in conn.execute(
            "SELECT DISTINCT season FROM league_pitching_stats"
        ).fetchall()]

        if season in pit_seasons_avail:
            for stat_name, player_expr, league_expr, higher in PITCHING_POOL_STATS:
                pool_sql = (f"SELECT {league_expr} FROM league_pitching_stats "
                            f"WHERE season=? AND ({league_expr}) IS NOT NULL AND ip > 0")
                if pit_min_ip:
                    pool_sql += f" AND ip >= {pit_min_ip}"
                pool = [r[0] for r in conn.execute(pool_sql, (season,)).fetchall() if r[0] is not None]

                # All players with pitching stats this season
                player_rows = conn.execute(
                    f"SELECT player_hashtag, {player_expr}, ip, g FROM pitching_stats "
                    f"WHERE season=? AND ({player_expr}) IS NOT NULL AND ip > 0",
                    (season,)
                ).fetchall()

                for p_name, stat_val, ip, g in player_rows:
                    if pit_min_ip or pit_min_g:
                        qualified = bool(
                            (pit_min_ip and ip and ip >= pit_min_ip) or
                            (pit_min_g and g and g >= pit_min_g)
                        )
                    else:
                        qualified = ip > 0
                    pct = calc_pct(stat_val, pool, higher) if qualified else None
                    est = calc_pct(stat_val, pool, higher) if not qualified else None
                    upsert(conn, p_name, season, stat_name, 'pitching',
                           stat_val, pct, est, 1 if qualified else 0,
                           len(pool), pit_qual_text)
                    total += 1

        # ── CUSTOM STATS PERCENTILES ──────────────────────────────────
        for stat_name, cs_col, higher, qual_type in CUSTOM_STATS:
            if qual_type == 'bat':
                pool_sql = (f"SELECT {cs_col} FROM custom_stats "
                            f"WHERE season=? AND {cs_col} IS NOT NULL AND bat_qualified=1")
            else:
                pool_sql = (f"SELECT {cs_col} FROM custom_stats "
                            f"WHERE season=? AND {cs_col} IS NOT NULL AND pit_qualified=1")
            pool = [r[0] for r in conn.execute(pool_sql, (season,)).fetchall() if r[0] is not None]

            if qual_type == 'bat':
                rows = conn.execute(
                    f"SELECT player_name, {cs_col}, bat_qualified FROM custom_stats "
                    f"WHERE season=? AND {cs_col} IS NOT NULL", (season,)
                ).fetchall()
            else:
                rows = conn.execute(
                    f"SELECT player_name, {cs_col}, pit_qualified FROM custom_stats "
                    f"WHERE season=? AND {cs_col} IS NOT NULL", (season,)
                ).fetchall()

            for p_name, stat_val, qualified in rows:
                pct = calc_pct(stat_val, pool, higher) if qualified else None
                est = calc_pct(stat_val, pool, higher) if not qualified else None
                upsert(conn, p_name, season, stat_name, 'custom',
                       stat_val, pct, est, qualified or 0, len(pool),
                       bat_qual_text if qual_type == 'bat' else pit_qual_text)
                total += 1

        # ── HQ SPLITS PERCENTILES ─────────────────────────────────────
        HQ_MIN_PA = 10
        for stat_name, col_or_expr, higher in HQ_SPLITS_STATS:
            pool = [r[0] for r in conn.execute(
                f"SELECT {col_or_expr} FROM hq_opponent_splits "
                f"WHERE season=? AND split_type='vs_hq_pitcher' AND pa >= {HQ_MIN_PA} "
                f"AND ({col_or_expr}) IS NOT NULL", (season,)
            ).fetchall() if r[0] is not None]

            rows = conn.execute(
                f"SELECT player_name, {col_or_expr}, pa FROM hq_opponent_splits "
                f"WHERE season=? AND split_type='vs_hq_pitcher' AND ({col_or_expr}) IS NOT NULL",
                (season,)
            ).fetchall()

            for p_name, stat_val, pa in rows:
                qualified = bool(pa and pa >= HQ_MIN_PA)
                pct = calc_pct(stat_val, pool, higher) if qualified and len(pool) >= 5 else None
                est = calc_pct(stat_val, pool, higher) if not qualified and len(pool) >= 5 else None
                upsert(conn, p_name, season, stat_name, 'splits',
                       stat_val, pct, est, 1 if qualified else 0,
                       len(pool), f'min {HQ_MIN_PA} PA vs HQ')
                total += 1

        # ── TIER SPLITS PERCENTILES ───────────────────────────────────
        for stat_name, where_clause, higher in TIER_SPLITS_STATS:
            pool = [r[0] for r in conn.execute(
                f"SELECT ops FROM opponent_tier_splits "
                f"WHERE season=? AND {where_clause} AND ops IS NOT NULL", (season,)
            ).fetchall() if r[0] is not None]

            rows = conn.execute(
                f"SELECT player_name, ops FROM opponent_tier_splits "
                f"WHERE season=? AND {where_clause} AND ops IS NOT NULL", (season,)
            ).fetchall()

            for p_name, stat_val in rows:
                pct = calc_pct(stat_val, pool, higher) if len(pool) >= 5 else None
                upsert(conn, p_name, season, stat_name, 'splits',
                       stat_val, pct, None, 1, len(pool), 'tier splits')
                total += 1

        if season % 5 == 0:
            conn.commit()
            print(f"  Season {season} done ({total} records so far)...")

    conn.commit()
    final = conn.execute("SELECT COUNT(*) FROM percentile_rankings").fetchone()[0]
    print(f"\nStep 4 complete. {final} total percentile records.")
    with open("progress_log.txt", "a") as f:
        f.write(f"\nStep 4 complete.\n  {final} percentile records calculated.\n")
    conn.close()


if __name__ == "__main__":
    main()
