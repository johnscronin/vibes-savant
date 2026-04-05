#!/usr/bin/env python3
"""
STEP 4 - Rebuild HQ batter list at 20% threshold (top 20% by OPS)
Drops and recreates hq_batters table with full schema including range columns
"""

import sqlite3

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/vibes_savant.db'

def normalize_name(name):
    if not name:
        return ''
    return name.lower().strip()

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Read season qualifiers
    cur.execute("SELECT season, batting_min_pa FROM season_qualifiers ORDER BY season")
    qualifiers = {row['season']: row['batting_min_pa'] for row in cur.fetchall()}
    print(f"Loaded qualifiers for {len(qualifiers)} seasons")

    # Check league_batting_stats schema
    cur.execute("SELECT name FROM pragma_table_info('league_batting_stats')")
    lbs_cols = [r['name'] for r in cur.fetchall()]
    print(f"league_batting_stats columns: {lbs_cols}")

    # Drop and recreate hq_batters table with full schema
    cur.execute("DROP TABLE IF EXISTS hq_batters")
    cur.execute("""
        CREATE TABLE hq_batters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER,
            batter_name TEXT,
            batter_name_normalized TEXT,
            ops REAL,
            obp REAL,
            is_hq INTEGER,
            cutoff_ops REAL,
            range_min_ops REAL,
            range_max_ops REAL,
            total_qualified_batters INTEGER
        )
    """)
    print("Created hq_batters table with full schema")

    total_hq = 0
    total_qualified = 0

    for season in range(2004, 2026):
        min_pa = qualifiers.get(season, 77)

        # Try league_batting_stats first (uses ab > 0 + check ops)
        # league_batting_stats doesn't have PA - use ab as proxy or check for pa column
        if 'ab' in lbs_cols and 'ops' in lbs_cols:
            # Use ab >= rough PA threshold (PA = AB + BB, so AB ~= 0.75 * PA typically)
            # Better: use batting_stats pa for qualification if lbs is sparse
            cur.execute("""
                SELECT lbs.player_name, lbs.ops, lbs.obp
                FROM league_batting_stats lbs
                JOIN batting_stats bs ON LOWER(TRIM(bs.player_hashtag)) = LOWER(TRIM(lbs.player_name))
                    AND bs.season = lbs.season
                WHERE lbs.season = ? AND lbs.ops IS NOT NULL AND lbs.ab > 0
                  AND bs.pa >= ?
                ORDER BY lbs.ops DESC
            """, (season, min_pa))
            batters = cur.fetchall()

            # If sparse, fall back to batting_stats directly
            if len(batters) < 5:
                cur.execute("""
                    SELECT player_hashtag as player_name, ops, obp
                    FROM batting_stats
                    WHERE season = ? AND ops IS NOT NULL AND ab > 0 AND pa >= ?
                    ORDER BY ops DESC
                """, (season, min_pa))
                batters = cur.fetchall()
        else:
            # Use batting_stats directly
            cur.execute("""
                SELECT player_hashtag as player_name, ops, obp
                FROM batting_stats
                WHERE season = ? AND ops IS NOT NULL AND ab > 0 AND pa >= ?
                ORDER BY ops DESC
            """, (season, min_pa))
            batters = cur.fetchall()

        if not batters:
            # Final fallback: use league_batting_stats with just ab>0 and ops not null
            cur.execute("""
                SELECT player_name, ops, obp
                FROM league_batting_stats
                WHERE season = ? AND ops IS NOT NULL AND ab > 0
                ORDER BY ops DESC
            """, (season, min_pa))
            batters = cur.fetchall()

        if not batters:
            print(f"  Season {season}: No qualified batters found")
            continue

        # Already sorted descending by OPS
        sorted_ops_desc = [b['ops'] for b in batters]
        n = len(sorted_ops_desc)

        # Top 20% = at or above 80th percentile
        # Sort ascending for index calculation
        sorted_ops_asc = sorted(sorted_ops_desc)
        cutoff_index = int(n * 0.80)
        if cutoff_index >= n:
            cutoff_index = n - 1

        cutoff_ops = sorted_ops_asc[cutoff_index]

        # range_min = cutoff_ops, range_max = max OPS
        range_min = cutoff_ops
        range_max = sorted_ops_desc[0]  # max OPS (first in desc list)

        hq_count = 0
        rows_to_insert = []
        for b in batters:
            is_hq = 1 if b['ops'] >= cutoff_ops else 0
            if is_hq:
                hq_count += 1
            rows_to_insert.append((
                season,
                b['player_name'],
                normalize_name(b['player_name']),
                b['ops'],
                b['obp'],
                is_hq,
                cutoff_ops,
                range_min,
                range_max,
                n
            ))

        cur.executemany("""
            INSERT INTO hq_batters
            (season, batter_name, batter_name_normalized, ops, obp,
             is_hq, cutoff_ops, range_min_ops, range_max_ops, total_qualified_batters)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows_to_insert)

        total_hq += hq_count
        total_qualified += n
        print(f"  Season {season}: {n} qualified batters, {hq_count} HQ (cutoff OPS={cutoff_ops:.3f}, range={range_min:.3f}-{range_max:.3f})")

    conn.commit()
    print(f"\nTotal: {total_qualified} qualified batters across all seasons, {total_hq} HQ designations")

    # Print 2025 results clearly
    print("\n=== 2025 HQ BATTER LIST ===")
    cur.execute("""
        SELECT batter_name, ops, obp, is_hq, cutoff_ops, range_min_ops, range_max_ops
        FROM hq_batters WHERE season = 2025 ORDER BY ops DESC
    """)
    rows = cur.fetchall()
    print(f"{'Name':<25} {'OPS':>7} {'OBP':>7} {'HQ?':>5} {'Cutoff':>8}")
    print("-" * 60)
    for r in rows:
        hq_marker = "HQ" if r['is_hq'] else ""
        print(f"{r['batter_name']:<25} {r['ops']:>7.3f} {r['obp'] or 0:>7.3f} {hq_marker:>5} {r['cutoff_ops']:>8.3f}")
    print(f"\nCutoff OPS: {rows[0]['cutoff_ops']:.3f}")
    print(f"Range: {rows[0]['range_min_ops']:.3f} - {rows[0]['range_max_ops']:.3f}")
    hq_2025 = [r for r in rows if r['is_hq']]
    print(f"HQ batters in 2025: {len(hq_2025)}")

    conn.close()
    print("\nDone.")

if __name__ == '__main__':
    main()
