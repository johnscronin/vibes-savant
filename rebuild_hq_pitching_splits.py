#!/usr/bin/env python3
"""
STEP 6 - Recalculate HQ pitching splits for all pitchers
Aggregates BvP stats only vs HQ batters for each pitcher-season
"""

import sqlite3
import re

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/vibes_savant.db'

def normalize_name(name):
    """Normalize a name for matching"""
    if not name:
        return ''
    n = name.lower().strip()
    n = re.sub(r"['\.,]", '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def safe_div(num, denom, default=0.0):
    if denom and denom > 0:
        return num / denom
    return default

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Load all HQ batters by season
    cur.execute("""
        SELECT season, batter_name, batter_name_normalized, is_hq,
               cutoff_ops, range_min_ops, range_max_ops, total_qualified_batters
        FROM hq_batters WHERE is_hq = 1
    """)
    hq_batter_rows = cur.fetchall()

    # Build sets and metadata
    hq_batters_by_season = {}  # season -> set of normalized names
    hq_batter_display_by_season = {}  # season -> set of display names
    hq_batter_meta = {}  # season -> {cutoff_ops, range_min, range_max, total_count}

    for r in hq_batter_rows:
        season = r['season']
        if season not in hq_batters_by_season:
            hq_batters_by_season[season] = set()
            hq_batter_display_by_season[season] = set()
            hq_batter_meta[season] = {
                'cutoff_ops': r['cutoff_ops'],
                'range_min': r['range_min_ops'],
                'range_max': r['range_max_ops'],
                'total_count': r['total_qualified_batters'],
            }
        hq_batters_by_season[season].add(r['batter_name_normalized'])
        hq_batter_display_by_season[season].add(r['batter_name'])

    print(f"HQ batter seasons loaded: {sorted(hq_batters_by_season.keys())}")

    # Get all distinct pitcher-season combos from BvP (non-Career)
    cur.execute("""
        SELECT DISTINCT opposing_pitcher, CAST(season AS INTEGER) as season_int
        FROM batter_vs_pitcher
        WHERE season != 'Career' AND season IS NOT NULL AND TRIM(season) != ''
        ORDER BY opposing_pitcher, season_int
    """)
    pitcher_seasons = cur.fetchall()
    print(f"Total pitcher-season combos to process: {len(pitcher_seasons)}")

    # Delete existing vs_hq_hitter records
    cur.execute("DELETE FROM hq_opponent_splits WHERE split_type = 'vs_hq_hitter'")
    deleted = cur.rowcount
    print(f"Deleted {deleted} existing vs_hq_hitter records")

    # Build pitcher slug lookup from player_master/players table
    # Try to find pitcher slugs
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('player_master', 'players')")
    player_tables = [r['name'] for r in cur.fetchall()]
    pitcher_slug_map = {}

    for tbl in player_tables:
        cur.execute(f"SELECT name FROM pragma_table_info('{tbl}')")
        cols = [r['name'] for r in cur.fetchall()]
        if 'player_name' in cols and 'player_slug' in cols:
            cur.execute(f"SELECT player_name, player_slug FROM {tbl}")
            for r in cur.fetchall():
                if r['player_name'] and r['player_slug']:
                    pitcher_slug_map[normalize_name(r['player_name'])] = r['player_slug']

    # Also check league_pitching_stats for name reference
    cur.execute("SELECT DISTINCT player_name FROM league_pitching_stats")
    for r in cur.fetchall():
        n = r['player_name']
        if n:
            key = normalize_name(n)
            if key not in pitcher_slug_map:
                pitcher_slug_map[key] = n  # Use name itself as slug if no mapping

    print(f"Pitcher slug map size: {len(pitcher_slug_map)}")

    records_inserted = 0
    qualified_count = 0
    unqualified_count = 0

    for ps in pitcher_seasons:
        pitcher_name = ps['opposing_pitcher']
        season = ps['season_int']

        if season not in hq_batters_by_season:
            continue

        hq_set = hq_batters_by_season[season]
        hq_display_set = hq_batter_display_by_season.get(season, set())
        meta = hq_batter_meta[season]

        # Get all BvP rows for this pitcher-season (pitcher is opposing_pitcher)
        cur.execute("""
            SELECT player_name, ab, h, doubles, triples, hr, rbi, bb, sac, so
            FROM batter_vs_pitcher
            WHERE opposing_pitcher = ? AND CAST(season AS INTEGER) = ?
              AND season != 'Career'
        """, (pitcher_name, season))
        bvp_rows = cur.fetchall()

        # Aggregate stats only vs HQ batters
        agg = {
            'ab': 0, 'h': 0, 'doubles': 0, 'triples': 0, 'hr': 0,
            'rbi': 0, 'bb': 0, 'sac': 0, 'so': 0
        }
        hq_bf = 0  # batters faced that are HQ

        for row in bvp_rows:
            batter_name = row['player_name']
            batter_norm = normalize_name(batter_name)

            # Match: exact display name first, then normalized
            is_hq_batter = batter_name in hq_display_set or batter_norm in hq_set

            if is_hq_batter:
                ab = row['ab'] or 0
                bb = row['bb'] or 0
                sac = row['sac'] or 0
                pa = ab + bb + sac
                hq_bf += pa
                agg['ab'] += ab
                agg['h'] += (row['h'] or 0)
                agg['doubles'] += (row['doubles'] or 0)
                agg['triples'] += (row['triples'] or 0)
                agg['hr'] += (row['hr'] or 0)
                agg['rbi'] += (row['rbi'] or 0)
                agg['bb'] += bb
                agg['sac'] += sac
                agg['so'] += (row['so'] or 0)

        bf = hq_bf
        qualifies = 1 if bf >= 15 else 0

        # Calculate pitching stats from counting stats
        ab = agg['ab']
        h = agg['h']
        hr = agg['hr']
        bb = agg['bb']
        sac = agg['sac']
        so = agg['so']
        pa = ab + bb + sac

        # BAA = H / AB
        baa = round(safe_div(h, ab), 3)

        # OBP against = (H + BB) / PA
        obp_against = round(safe_div(h + bb, pa), 3)

        # IP estimate: BF / 3.3 (standard conversion)
        ip_est = bf / 3.3 if bf > 0 else 0

        # ERA estimate: HR * 9 / IP + runs estimate
        # Use runs allowed estimate: (H + BB) * 0.47 (typical run scoring rate)
        # ERA est = runs_allowed * 6 / ip_est (6 outs per inning for wiffleball)
        # Alternative: use H-based scoring
        runs_est = (h - hr) * 0.30 + hr * 1.0 + bb * 0.30
        era_est = round(safe_div(runs_est * 6, ip_est), 2) if ip_est > 0 else None

        # K/6 (outs per 6 = per inning in 3-out game; for wiffleball use /6 as half-inning metric)
        k_per_6 = round(safe_div(so * 6, bf), 2) if bf > 0 else None
        bb_per_6 = round(safe_div(bb * 6, bf), 2) if bf > 0 else None

        # BB%
        bb_pct = round(safe_div(bb, pa), 3) if pa > 0 else 0.0
        k_pct = round(safe_div(so, pa), 3) if pa > 0 else 0.0

        # Get pitcher slug
        pitcher_slug = pitcher_slug_map.get(normalize_name(pitcher_name), pitcher_name)

        # Count unique HQ batters faced
        hq_batter_count = sum(
            1 for row in bvp_rows
            if (row['player_name'] in hq_display_set or
                normalize_name(row['player_name']) in hq_set)
        )

        cur.execute("""
            INSERT INTO hq_opponent_splits
            (player_name, player_slug, season, split_type, pa, ab, h, doubles, triples, hr,
             rbi, bb, so, avg, obp, slg, ops, bb_pct, k_pct, bb_k, iso, babip,
             qualifies, hq_definition, range_min, range_max, total_hq_opponents, is_estimate,
             bf, era, obp_against, baa, k_per_6, bb_per_6)
            VALUES (?, ?, ?, 'vs_hq_hitter', ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, 0,
                    ?, ?, ?, ?, ?, ?)
        """, (
            pitcher_name, pitcher_slug, season,
            pa, ab, h, agg['doubles'], agg['triples'], hr,
            agg['rbi'], bb, so,
            baa,        # avg (BAA)
            obp_against, # obp
            0.0,        # slg (not meaningful for pitchers in this context)
            0.0,        # ops (not meaningful)
            bb_pct, k_pct,
            round(safe_div(bb, so), 3) if so > 0 else bb_pct,  # bb_k
            0.0,        # iso
            0.0,        # babip
            qualifies,
            meta['cutoff_ops'], meta['range_min'], meta['range_max'],
            hq_batter_count,
            bf, era_est, obp_against, baa, k_per_6, bb_per_6
        ))

        records_inserted += 1
        if qualifies:
            qualified_count += 1
        else:
            unqualified_count += 1

    conn.commit()

    print(f"\nResults:")
    print(f"  Records inserted: {records_inserted}")
    print(f"  Qualified (BF>=15): {qualified_count}")
    print(f"  Unqualified (BF<15): {unqualified_count}")

    # Print 2025 summary
    print("\n=== 2025 vs_hq_hitter SPLITS (qualified only, sorted by ERA asc) ===")
    cur.execute("""
        SELECT player_name, bf, era, obp_against, baa, k_per_6, bb_per_6, k_pct, bb_pct, qualifies
        FROM hq_opponent_splits
        WHERE split_type='vs_hq_hitter' AND season=2025 AND qualifies=1
        ORDER BY era ASC
    """)
    rows = cur.fetchall()
    print(f"{'Name':<25} {'BF':>4} {'ERA':>6} {'OBP':>6} {'BAA':>6} {'K/6':>5} {'BB/6':>6}")
    print("-" * 70)
    for r in rows:
        print(f"{r['player_name']:<25} {r['bf']:>4} {r['era'] or 0:>6.2f} {r['obp_against'] or 0:>6.3f} {r['baa'] or 0:>6.3f} {r['k_per_6'] or 0:>5.1f} {r['bb_per_6'] or 0:>6.1f}")

    conn.close()
    print("\nDone.")

if __name__ == '__main__':
    main()
