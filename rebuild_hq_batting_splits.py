#!/usr/bin/env python3
"""
STEP 5 - Recalculate HQ batting splits for all players
Aggregates BvP stats only vs HQ pitchers for each player-season
"""

import sqlite3
import re

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/vibes_savant.db'

def normalize_name(name):
    """Normalize a name for matching: lowercase, strip punctuation/spaces"""
    if not name:
        return ''
    n = name.lower().strip()
    # Remove punctuation except hyphens within words
    n = re.sub(r"['\.,]", '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n

def safe_div(num, denom, default=0.0):
    if denom and denom > 0:
        return num / denom
    return default

def calc_rate_stats(ab, h, doubles, triples, hr, bb, sac, so):
    """Calculate batting rate stats from counting stats"""
    pa = ab + bb + sac
    singles = h - doubles - triples - hr
    tb = singles + 2*doubles + 3*triples + 4*hr

    avg = safe_div(h, ab)
    obp = safe_div(h + bb, pa)
    slg = safe_div(tb, ab)
    ops = obp + slg

    bb_pct = safe_div(bb, pa) if pa > 0 else 0.0
    k_pct = safe_div(so, pa) if pa > 0 else 0.0
    bb_k = safe_div(bb, so) if so > 0 else (bb if bb > 0 else 0.0)

    # ISO = SLG - AVG
    iso = slg - avg

    # BABIP = (H - HR) / (AB - K - HR + SAC)
    babip_denom = ab - so - hr + sac
    babip = safe_div(h - hr, babip_denom) if babip_denom > 0 else 0.0

    return {
        'avg': round(avg, 3),
        'obp': round(obp, 3),
        'slg': round(slg, 3),
        'ops': round(ops, 3),
        'bb_pct': round(bb_pct, 3),
        'k_pct': round(k_pct, 3),
        'bb_k': round(bb_k, 3),
        'iso': round(iso, 3),
        'babip': round(babip, 3),
    }

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure hq_opponent_splits has all required columns
    cur.execute("SELECT name FROM pragma_table_info('hq_opponent_splits')")
    existing_cols = [r['name'] for r in cur.fetchall()]
    print(f"Existing hq_opponent_splits columns: {existing_cols}")

    # Add missing columns if needed
    new_cols = [
        ('player_slug', 'TEXT'),
        ('hq_definition', 'REAL'),
        ('range_min', 'REAL'),
        ('range_max', 'REAL'),
        ('total_hq_opponents', 'INTEGER'),
        ('is_estimate', 'INTEGER DEFAULT 0'),
        ('bf', 'INTEGER'),
        ('era', 'REAL'),
        ('obp_against', 'REAL'),
        ('baa', 'REAL'),
        ('k_per_6', 'REAL'),
        ('bb_per_6', 'REAL'),
    ]
    for col_name, col_type in new_cols:
        if col_name not in existing_cols:
            try:
                cur.execute(f"ALTER TABLE hq_opponent_splits ADD COLUMN {col_name} {col_type}")
                print(f"  Added column: {col_name}")
            except Exception as e:
                print(f"  Column {col_name} already exists or error: {e}")

    conn.commit()

    # Load all HQ pitchers by season
    cur.execute("SELECT season, pitcher_name, pitcher_name_normalized, is_hq, cutoff_whip, range_min_whip, range_max_whip, total_qualified_pitchers FROM hq_pitchers WHERE is_hq = 1")
    hq_pitcher_rows = cur.fetchall()

    # Build sets: hq_pitchers_by_season[season] = set of normalized names
    hq_pitchers_by_season = {}
    hq_pitcher_meta = {}  # season -> {cutoff_whip, range_min, range_max, total_count}

    for r in hq_pitcher_rows:
        season = r['season']
        if season not in hq_pitchers_by_season:
            hq_pitchers_by_season[season] = set()
            hq_pitcher_meta[season] = {
                'cutoff_whip': r['cutoff_whip'],
                'range_min': r['range_min_whip'],
                'range_max': r['range_max_whip'],
                'total_count': r['total_qualified_pitchers'],
            }
        hq_pitchers_by_season[season].add(r['pitcher_name_normalized'])

    print(f"HQ pitcher seasons loaded: {sorted(hq_pitchers_by_season.keys())}")

    # Also build a display-name set for direct matching
    cur.execute("SELECT season, pitcher_name, is_hq FROM hq_pitchers WHERE is_hq = 1")
    hq_pitcher_display = {}
    for r in cur.fetchall():
        season = r['season']
        if season not in hq_pitcher_display:
            hq_pitcher_display[season] = set()
        hq_pitcher_display[season].add(r['pitcher_name'])

    # Get all player-season combos from BvP (non-Career rows only)
    cur.execute("""
        SELECT DISTINCT player_name, player_slug, CAST(season AS INTEGER) as season_int
        FROM batter_vs_pitcher
        WHERE season != 'Career' AND season IS NOT NULL AND TRIM(season) != ''
        ORDER BY player_name, season_int
    """)
    player_seasons = cur.fetchall()
    print(f"Total player-season combos to process: {len(player_seasons)}")

    # Delete existing vs_hq_pitcher records
    cur.execute("DELETE FROM hq_opponent_splits WHERE split_type = 'vs_hq_pitcher'")
    deleted = cur.rowcount
    print(f"Deleted {deleted} existing vs_hq_pitcher records")

    # Get overall OPS per player-season for sanity check
    # Use batting_stats
    cur.execute("SELECT player_hashtag, season, ops FROM batting_stats WHERE ops IS NOT NULL")
    overall_ops_data = {}
    for r in cur.fetchall():
        overall_ops_data[(normalize_name(r['player_hashtag']), r['season'])] = r['ops']

    # Process each player-season
    records_inserted = 0
    qualified_count = 0
    unqualified_count = 0
    warnings = []

    for ps in player_seasons:
        player_name = ps['player_name']
        player_slug = ps['player_slug']
        season = ps['season_int']

        if season not in hq_pitchers_by_season:
            continue

        hq_set = hq_pitchers_by_season[season]
        hq_display_set = hq_pitcher_display.get(season, set())
        meta = hq_pitcher_meta[season]

        # Get all BvP rows for this player-season
        cur.execute("""
            SELECT opposing_pitcher, ab, h, doubles, triples, hr, rbi, bb, sac, so, roe
            FROM batter_vs_pitcher
            WHERE player_name = ? AND CAST(season AS INTEGER) = ?
              AND season != 'Career'
        """, (player_name, season))
        bvp_rows = cur.fetchall()

        # Aggregate stats vs HQ pitchers only
        agg = {
            'ab': 0, 'h': 0, 'doubles': 0, 'triples': 0, 'hr': 0,
            'rbi': 0, 'bb': 0, 'sac': 0, 'so': 0, 'roe': 0
        }
        hq_pa = 0

        for row in bvp_rows:
            pitcher = row['opposing_pitcher']
            pitcher_norm = normalize_name(pitcher)

            # Match: exact display name first, then normalized
            is_hq_pitcher = pitcher in hq_display_set or pitcher_norm in hq_set

            if is_hq_pitcher:
                ab = row['ab'] or 0
                bb = row['bb'] or 0
                sac = row['sac'] or 0
                pa = ab + bb + sac
                hq_pa += pa
                agg['ab'] += ab
                agg['h'] += (row['h'] or 0)
                agg['doubles'] += (row['doubles'] or 0)
                agg['triples'] += (row['triples'] or 0)
                agg['hr'] += (row['hr'] or 0)
                agg['rbi'] += (row['rbi'] or 0)
                agg['bb'] += bb
                agg['sac'] += sac
                agg['so'] += (row['so'] or 0)

        pa = hq_pa

        # Calculate rate stats
        rates = calc_rate_stats(
            agg['ab'], agg['h'], agg['doubles'], agg['triples'], agg['hr'],
            agg['bb'], agg['sac'], agg['so']
        )

        qualifies = 1 if pa >= 15 else 0

        # Sanity check: HQ OPS vs overall OPS
        player_norm_key = normalize_name(player_name)
        overall_ops = overall_ops_data.get((player_norm_key, season))
        if overall_ops is None:
            # Try with player_slug
            if player_slug:
                overall_ops = overall_ops_data.get((normalize_name(player_slug), season))
        if pa > 0 and overall_ops and rates['ops'] - overall_ops > 0.200:
            warnings.append(f"  WARNING: {player_name} {season} HQ OPS={rates['ops']:.3f} vs overall OPS={overall_ops:.3f} (diff={rates['ops']-overall_ops:.3f})")

        # Count HQ pitchers this player faced
        hq_opponent_count = sum(
            1 for row in bvp_rows
            if (row['opposing_pitcher'] in hq_display_set or
                normalize_name(row['opposing_pitcher']) in hq_set)
        )

        cur.execute("""
            INSERT INTO hq_opponent_splits
            (player_name, player_slug, season, split_type, pa, ab, h, doubles, triples, hr,
             rbi, bb, so, avg, obp, slg, ops, bb_pct, k_pct, bb_k, iso, babip,
             qualifies, hq_definition, range_min, range_max, total_hq_opponents, is_estimate,
             bf, era, obp_against, baa, k_per_6, bb_per_6)
            VALUES (?, ?, ?, 'vs_hq_pitcher', ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, 0,
                    NULL, NULL, NULL, NULL, NULL, NULL)
        """, (
            player_name, player_slug, season,
            pa, agg['ab'], agg['h'], agg['doubles'], agg['triples'], agg['hr'],
            agg['rbi'], agg['bb'], agg['so'],
            rates['avg'], rates['obp'], rates['slg'], rates['ops'],
            rates['bb_pct'], rates['k_pct'], rates['bb_k'], rates['iso'], rates['babip'],
            qualifies,
            meta['cutoff_whip'], meta['range_min'], meta['range_max'],
            hq_opponent_count
        ))

        records_inserted += 1
        if qualifies:
            qualified_count += 1
        else:
            unqualified_count += 1

    conn.commit()

    print(f"\nResults:")
    print(f"  Records inserted: {records_inserted}")
    print(f"  Qualified (PA>=15): {qualified_count}")
    print(f"  Unqualified (PA<15): {unqualified_count}")
    print(f"  Sanity warnings: {len(warnings)}")
    if warnings:
        for w in warnings[:20]:
            print(w)

    # Print 2025 summary
    print("\n=== 2025 vs_hq_pitcher SPLITS (qualified only) ===")
    cur.execute("""
        SELECT player_name, pa, ab, h, hr, bb, so, avg, obp, slg, ops, qualifies
        FROM hq_opponent_splits
        WHERE split_type='vs_hq_pitcher' AND season=2025 AND qualifies=1
        ORDER BY ops DESC
    """)
    rows = cur.fetchall()
    print(f"{'Name':<25} {'PA':>4} {'H':>4} {'HR':>4} {'BB':>4} {'SO':>4} {'AVG':>6} {'OBP':>6} {'SLG':>6} {'OPS':>7}")
    print("-" * 85)
    for r in rows:
        print(f"{r['player_name']:<25} {r['pa']:>4} {r['h']:>4} {r['hr']:>4} {r['bb']:>4} {r['so']:>4} {r['avg']:>6.3f} {r['obp']:>6.3f} {r['slg']:>6.3f} {r['ops']:>7.3f}")

    conn.close()
    print("\nDone.")

if __name__ == '__main__':
    main()
