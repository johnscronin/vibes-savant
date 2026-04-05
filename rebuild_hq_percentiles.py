#!/usr/bin/env python3
"""
STEP 7 - Recalculate HQ percentile rankings for batting and pitching splits
"""

import sqlite3

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/vibes_savant.db'

def calc_percentile(value, pool_values, higher_is_better):
    """
    Calculate percentile: count of players with strictly worse value / total pool size
    For higher_is_better: worse = lower value
    For lower_is_better (invert): worse = higher value
    Returns percentile in [1, 99]
    """
    n = len(pool_values)
    if n == 0:
        return 50
    if higher_is_better:
        worse_count = sum(1 for v in pool_values if v < value)
    else:
        worse_count = sum(1 for v in pool_values if v > value)
    pct = round((worse_count / n) * 100)
    return max(1, min(99, pct))

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Delete existing HQ percentiles
    cur.execute("DELETE FROM percentile_rankings WHERE stat_type IN ('vs_hq_pitcher', 'vs_hq_hitter')")
    deleted = cur.rowcount
    print(f"Deleted {deleted} existing HQ percentile records")

    # =============================================
    # BATTING vs HQ PITCHERS
    # =============================================
    print("\n=== Processing BATTING vs HQ PITCHERS ===")

    batting_stats_config = [
        # (stat_key, column, higher_is_better, display_name)
        ('hq_pa', 'pa', True, 'PA vs HQ'),
        ('hq_ops', 'ops', True, 'OPS vs HQ'),
        ('hq_obp', 'obp', True, 'OBP vs HQ'),
        ('hq_avg', 'avg', True, 'AVG vs HQ'),
        ('hq_slg', 'slg', True, 'SLG vs HQ'),
        ('hq_bb_pct', 'bb_pct', True, 'BB% vs HQ'),
        ('hq_k_pct', 'k_pct', False, 'K% vs HQ'),   # lower is better
        ('hq_bb_k', 'bb_k', True, 'BB/K vs HQ'),
        ('hq_iso', 'iso', True, 'ISO vs HQ'),
        ('hq_babip', 'babip', True, 'BABIP vs HQ'),
    ]

    # Load all batting splits grouped by season
    cur.execute("""
        SELECT player_name, season, pa, ops, obp, avg, slg, bb_pct, k_pct, bb_k, iso, babip, qualifies
        FROM hq_opponent_splits
        WHERE split_type = 'vs_hq_pitcher'
        ORDER BY season, player_name
    """)
    all_batting_splits = cur.fetchall()

    # Group by season
    batting_by_season = {}
    for r in all_batting_splits:
        s = r['season']
        if s not in batting_by_season:
            batting_by_season[s] = []
        batting_by_season[s].append(r)

    batting_pool_sizes = {}
    batting_records_to_insert = []

    for season in sorted(batting_by_season.keys()):
        all_splits = batting_by_season[season]
        qualified_splits = [r for r in all_splits if r['qualifies'] == 1]
        pool_size = len(qualified_splits)
        batting_pool_sizes[season] = pool_size

        if pool_size < 3:
            print(f"  Season {season}: SKIPPING - only {pool_size} qualified batters")
            continue

        # Build pool values for each stat
        pool_values = {}
        for stat_key, col, _, _ in batting_stats_config:
            pool_values[stat_key] = [r[col] for r in qualified_splits if r[col] is not None]

        for split in all_splits:
            is_qualified = split['qualifies'] == 1
            player_name = split['player_name']

            for stat_key, col, higher_is_better, display_name in batting_stats_config:
                stat_value = split[col]
                if stat_value is None:
                    continue

                pool = pool_values[stat_key]
                if not pool:
                    continue

                pct = calc_percentile(stat_value, pool, higher_is_better)

                batting_records_to_insert.append((
                    player_name, season, stat_key, stat_value,
                    pct,  # percentile
                    pct,  # estimated_percentile (same for now)
                    'vs_hq_pitcher',
                    1 if is_qualified else 0,
                    pool_size,
                    display_name
                ))

    cur.executemany("""
        INSERT INTO percentile_rankings
        (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
         stat_type, qualified, pool_size, qualifier_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, batting_records_to_insert)

    print(f"  Inserted {len(batting_records_to_insert)} batting vs HQ percentile records")

    # =============================================
    # PITCHING vs HQ BATTERS
    # =============================================
    print("\n=== Processing PITCHING vs HQ BATTERS ===")

    pitching_stats_config = [
        # (stat_key, column, higher_is_better, display_name)
        ('hqpit_bf', 'bf', True, 'BF vs HQ'),
        ('hqpit_era', 'era', False, 'ERA vs HQ'),
        ('hqpit_obp', 'obp_against', False, 'OBP vs HQ'),
        ('hqpit_baa', 'baa', False, 'BAA vs HQ'),
        ('hqpit_k_pct', 'k_pct', True, 'K% vs HQ'),
        ('hqpit_bb_pct', 'bb_pct', False, 'BB% vs HQ'),
        ('hqpit_k_per_6', 'k_per_6', True, 'K/6 vs HQ'),
        ('hqpit_bb_per_6', 'bb_per_6', False, 'BB/6 vs HQ'),
    ]

    # Load all pitching splits grouped by season
    cur.execute("""
        SELECT player_name, season, bf, era, obp_against, baa, k_pct, bb_pct, k_per_6, bb_per_6, qualifies
        FROM hq_opponent_splits
        WHERE split_type = 'vs_hq_hitter'
        ORDER BY season, player_name
    """)
    all_pitching_splits = cur.fetchall()

    pitching_by_season = {}
    for r in all_pitching_splits:
        s = r['season']
        if s not in pitching_by_season:
            pitching_by_season[s] = []
        pitching_by_season[s].append(r)

    pitching_pool_sizes = {}
    pitching_records_to_insert = []

    for season in sorted(pitching_by_season.keys()):
        all_splits = pitching_by_season[season]
        qualified_splits = [r for r in all_splits if r['qualifies'] == 1]
        pool_size = len(qualified_splits)
        pitching_pool_sizes[season] = pool_size

        if pool_size < 3:
            print(f"  Season {season}: SKIPPING - only {pool_size} qualified pitchers")
            continue

        # Build pool values
        pool_values = {}
        for stat_key, col, _, _ in pitching_stats_config:
            pool_values[stat_key] = [r[col] for r in qualified_splits if r[col] is not None]

        for split in all_splits:
            is_qualified = split['qualifies'] == 1
            player_name = split['player_name']

            for stat_key, col, higher_is_better, display_name in pitching_stats_config:
                stat_value = split[col]
                if stat_value is None:
                    continue

                pool = pool_values[stat_key]
                if not pool:
                    continue

                pct = calc_percentile(stat_value, pool, higher_is_better)

                pitching_records_to_insert.append((
                    player_name, season, stat_key, stat_value,
                    pct, pct,
                    'vs_hq_hitter',
                    1 if is_qualified else 0,
                    pool_size,
                    display_name
                ))

    cur.executemany("""
        INSERT INTO percentile_rankings
        (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
         stat_type, qualified, pool_size, qualifier_text)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, pitching_records_to_insert)

    print(f"  Inserted {len(pitching_records_to_insert)} pitching vs HQ percentile records")

    conn.commit()

    # Print pool sizes
    print("\n=== BATTING vs HQ PITCHER Pool Sizes by Season ===")
    for season in sorted(batting_pool_sizes.keys()):
        size = batting_pool_sizes[season]
        flag = " *** FEWER THAN 10 ***" if size < 10 else ""
        print(f"  {season}: {size} qualified batters{flag}")

    print("\n=== PITCHING vs HQ BATTER Pool Sizes by Season ===")
    for season in sorted(pitching_pool_sizes.keys()):
        size = pitching_pool_sizes[season]
        flag = " *** FEWER THAN 10 ***" if size < 10 else ""
        print(f"  {season}: {size} qualified pitchers{flag}")

    print("\n=== 2025 Pool Sizes ===")
    print(f"  Batting vs HQ: {batting_pool_sizes.get(2025, 'N/A')} qualified batters")
    print(f"  Pitching vs HQ: {pitching_pool_sizes.get(2025, 'N/A')} qualified pitchers")

    conn.close()
    print("\nDone.")

if __name__ == '__main__':
    main()
