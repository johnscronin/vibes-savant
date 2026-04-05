#!/usr/bin/env python3
"""
STEP 3 - Rebuild HQ pitcher list at 20% threshold
Drops and recreates hq_pitchers table with full schema including range columns
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
    cur.execute("SELECT season, pitching_min_ip FROM season_qualifiers ORDER BY season")
    qualifiers = {row['season']: row['pitching_min_ip'] for row in cur.fetchall()}
    print(f"Loaded qualifiers for {len(qualifiers)} seasons")

    # Drop and recreate hq_pitchers table with full schema
    cur.execute("DROP TABLE IF EXISTS hq_pitchers")
    cur.execute("""
        CREATE TABLE hq_pitchers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season INTEGER,
            pitcher_name TEXT,
            pitcher_name_normalized TEXT,
            whip REAL,
            era REAL,
            ip REAL,
            is_hq INTEGER,
            cutoff_whip REAL,
            range_min_whip REAL,
            range_max_whip REAL,
            total_qualified_pitchers INTEGER
        )
    """)
    print("Created hq_pitchers table with full schema")

    total_hq = 0
    total_qualified = 0

    for season in range(2004, 2026):
        min_ip = qualifiers.get(season, 30.0)

        # Get all qualified pitchers for this season
        cur.execute("""
            SELECT player_name, whip, era, ip
            FROM league_pitching_stats
            WHERE season = ? AND whip IS NOT NULL AND ip >= ?
            ORDER BY whip ASC
        """, (season, min_ip))
        pitchers = cur.fetchall()

        if not pitchers:
            print(f"  Season {season}: No qualified pitchers found (min_ip={min_ip})")
            continue

        sorted_whips = [p['whip'] for p in pitchers]  # already sorted asc
        n = len(sorted_whips)

        # Top 20% = bottom 20% by WHIP (lowest WHIP = best)
        cutoff_index = max(1, int(n * 0.20))
        cutoff_whip = sorted_whips[cutoff_index - 1]

        range_min = sorted_whips[0]
        range_max = cutoff_whip

        hq_count = 0
        rows_to_insert = []
        for p in pitchers:
            is_hq = 1 if p['whip'] <= cutoff_whip else 0
            if is_hq:
                hq_count += 1
            rows_to_insert.append((
                season,
                p['player_name'],
                normalize_name(p['player_name']),
                p['whip'],
                p['era'],
                p['ip'],
                is_hq,
                cutoff_whip,
                range_min,
                range_max,
                n
            ))

        cur.executemany("""
            INSERT INTO hq_pitchers
            (season, pitcher_name, pitcher_name_normalized, whip, era, ip,
             is_hq, cutoff_whip, range_min_whip, range_max_whip, total_qualified_pitchers)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows_to_insert)

        total_hq += hq_count
        total_qualified += n
        print(f"  Season {season}: {n} qualified pitchers, {hq_count} HQ (cutoff WHIP={cutoff_whip:.3f}, range={range_min:.3f}-{range_max:.3f})")

    conn.commit()
    print(f"\nTotal: {total_qualified} qualified pitchers across all seasons, {total_hq} HQ designations")

    # Print 2025 results clearly
    print("\n=== 2025 HQ PITCHER LIST ===")
    cur.execute("""
        SELECT pitcher_name, whip, era, ip, is_hq, cutoff_whip, range_min_whip, range_max_whip
        FROM hq_pitchers WHERE season = 2025 ORDER BY whip
    """)
    rows = cur.fetchall()
    print(f"{'Name':<25} {'WHIP':>6} {'ERA':>6} {'IP':>6} {'HQ?':>5} {'Cutoff':>8}")
    print("-" * 65)
    for r in rows:
        hq_marker = "HQ" if r['is_hq'] else ""
        print(f"{r['pitcher_name']:<25} {r['whip']:>6.3f} {r['era'] or 0:>6.2f} {r['ip'] or 0:>6.1f} {hq_marker:>5} {r['cutoff_whip']:>8.3f}")
    print(f"\nCutoff WHIP: {rows[0]['cutoff_whip']:.3f}")
    print(f"Range: {rows[0]['range_min_whip']:.3f} - {rows[0]['range_max_whip']:.3f}")
    hq_2025 = [r for r in rows if r['is_hq']]
    print(f"HQ pitchers in 2025: {len(hq_2025)}")

    conn.close()
    print("\nDone.")

if __name__ == '__main__':
    main()
