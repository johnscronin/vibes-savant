"""
Combined script:
1. Calculate percentiles for counting stats (batting + pitching)
2. Add hr_per_6 column to custom_stats and calculate it
3. Calculate hr_per_6 percentiles
"""
import sqlite3

DB = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/vibes_savant.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Check existing percentile stats
existing = set(r[0] for r in conn.execute(
    "SELECT DISTINCT stat_name FROM percentile_rankings"
).fetchall())
print("Existing percentile stats:", sorted(existing))

# ── Section 1: Add hr_per_6 to custom_stats ──────────────────────────────────

cs_cols = [r[1] for r in conn.execute("PRAGMA table_info(custom_stats)").fetchall()]
print("\ncustom_stats cols:", cs_cols)

if 'hr_per_6' not in cs_cols:
    conn.execute("ALTER TABLE custom_stats ADD COLUMN hr_per_6 REAL")
    print("Added hr_per_6 column to custom_stats")
else:
    print("hr_per_6 already exists in custom_stats")

# Populate hr_per_6 from pitching_stats
updated = conn.execute("""
    UPDATE custom_stats
    SET hr_per_6 = (
        SELECT ROUND(ps.opp_hr * 6.0 / ps.ip, 2)
        FROM pitching_stats ps
        WHERE LOWER(TRIM(ps.player_hashtag)) = LOWER(TRIM(custom_stats.player_name))
          AND ps.season = custom_stats.season
          AND ps.ip > 0
        LIMIT 1
    )
    WHERE EXISTS (
        SELECT 1 FROM pitching_stats ps
        WHERE LOWER(TRIM(ps.player_hashtag)) = LOWER(TRIM(custom_stats.player_name))
          AND ps.season = custom_stats.season
          AND ps.ip > 0
    )
""").rowcount
print(f"Updated {updated} rows with hr_per_6")
conn.commit()

# Verify hr_per_6 sample
sample = conn.execute("""
    SELECT cs.player_name, cs.season, ps.opp_hr as hr, ps.ip, cs.hr_per_6
    FROM custom_stats cs
    JOIN pitching_stats ps ON LOWER(TRIM(ps.player_hashtag)) = LOWER(TRIM(cs.player_name)) AND ps.season = cs.season
    WHERE cs.hr_per_6 IS NOT NULL AND ps.ip >= 37
    ORDER BY cs.hr_per_6 DESC
    LIMIT 5
""").fetchall()
print("\nTop 5 worst HR/6 (qualified):")
for r in sample:
    print(f"  {r['player_name']} {r['season']}: {r['hr']} HR / {r['ip']} IP = {r['hr_per_6']} HR/6")

best = conn.execute("""
    SELECT cs.player_name, cs.season, ps.opp_hr as hr, ps.ip, cs.hr_per_6
    FROM custom_stats cs
    JOIN pitching_stats ps ON LOWER(TRIM(ps.player_hashtag)) = LOWER(TRIM(cs.player_name)) AND ps.season = cs.season
    WHERE cs.hr_per_6 IS NOT NULL AND ps.ip >= 37
    ORDER BY cs.hr_per_6 ASC
    LIMIT 5
""").fetchall()
print("\nTop 5 best HR/6 (qualified):")
for r in best:
    print(f"  {r['player_name']} {r['season']}: {r['hr']} HR / {r['ip']} IP = {r['hr_per_6']} HR/6")

# ── Section 2: Calculate percentiles for counting stats ──────────────────────

qualifiers = {r['season']: r for r in conn.execute("SELECT * FROM season_qualifiers").fetchall()}

def calc_percentiles(stat_name, table, col, lower_is_better, stat_type, use_batting_qual=True):
    """Calculate percentiles for a stat across all seasons."""
    seasons = [r[0] for r in conn.execute(
        f"SELECT DISTINCT season FROM {table} WHERE {col} IS NOT NULL AND season > 0"
    ).fetchall()]
    inserted = 0

    # Delete existing to recalculate fresh
    conn.execute("DELETE FROM percentile_rankings WHERE stat_name=?", (stat_name,))

    for season in seasons:
        q = qualifiers.get(season)
        if use_batting_qual:
            min_val = q['batting_min_pa'] if q else 100
            qual_col = 'pa'
            name_col = 'player_hashtag'
        else:
            min_val = q['pitching_min_ip'] if q else 37.0
            qual_col = 'ip'
            name_col = 'player_hashtag'

        rows = conn.execute(f"""
            SELECT {name_col} as player_name, {col} as val
            FROM {table} WHERE season=? AND {qual_col} >= ? AND {col} IS NOT NULL
        """, (season, min_val)).fetchall()

        if len(rows) < 3:
            continue

        vals = [r['val'] for r in rows]

        for r in rows:
            v = r['val']
            if lower_is_better:
                worse_count = sum(1 for x in vals if x > v)
            else:
                worse_count = sum(1 for x in vals if x < v)
            pct = round((worse_count / len(vals)) * 100)
            pct = max(1, min(99, pct))

            conn.execute("""
                INSERT OR REPLACE INTO percentile_rankings
                (player_name, season, stat_name, stat_value, percentile, stat_type, qualified, pool_size)
                VALUES (?,?,?,?,?,?,1,?)
            """, (r['player_name'], season, stat_name, v, pct, stat_type, len(vals)))
            inserted += 1

    return inserted

# Batting counting stats (these use batting_stats table, player_hashtag)
BAT_COUNT_STATS = [
    ('games', 'batting_stats', 'games', False, 'batting'),
    ('r', 'batting_stats', 'r', False, 'batting'),
    ('h', 'batting_stats', 'h', False, 'batting'),
    ('doubles', 'batting_stats', 'doubles', False, 'batting'),
    ('triples', 'batting_stats', 'triples', False, 'batting'),
    ('rbi', 'batting_stats', 'rbi', False, 'batting'),
    ('bb', 'batting_stats', 'bb', False, 'batting'),
    ('hr', 'batting_stats', 'hr', False, 'batting'),
    ('pa', 'batting_stats', 'pa', False, 'batting'),
    ('ab', 'batting_stats', 'ab', False, 'batting'),
    ('so', 'batting_stats', 'so', True, 'batting'),
    ('iso', 'batting_stats', None, False, 'batting'),  # skip, already in batting
]

# Pitching counting stats
PIT_COUNT_STATS = [
    ('g', 'pitching_stats', 'g', False, 'pitching'),
    ('gs', 'pitching_stats', 'gs', False, 'pitching'),
    ('w', 'pitching_stats', 'w', False, 'pitching'),
    ('l', 'pitching_stats', 'l', True, 'pitching'),
    ('sv', 'pitching_stats', 'sv', False, 'pitching'),
    ('k', 'pitching_stats', 'k', False, 'pitching'),
    ('opp_bb', 'pitching_stats', 'opp_bb', True, 'pitching'),
    ('ha', 'pitching_stats', 'ha', True, 'pitching'),
    ('opp_hr', 'pitching_stats', 'opp_hr', True, 'pitching'),
    ('ip_stat', 'pitching_stats', 'ip', False, 'pitching'),  # ip already exists as 'ip'
]

print("\n--- Calculating batting counting stat percentiles ---")
total_bat = 0
for entry in BAT_COUNT_STATS:
    stat_name, table, col, lib, st = entry
    if col is None:
        continue
    if stat_name in existing:
        print(f"  {stat_name}: already exists, recalculating...")
    n = calc_percentiles(stat_name, table, col, lib, st, use_batting_qual=True)
    print(f"  {stat_name}: {n} records inserted")
    total_bat += n

print(f"\nTotal batting stats inserted: {total_bat}")

print("\n--- Calculating pitching counting stat percentiles ---")
total_pit = 0
for entry in PIT_COUNT_STATS:
    stat_name, table, col, lib, st = entry
    if stat_name == 'ip_stat':
        # ip already exists, skip
        print(f"  ip: already exists, skipping")
        continue
    if stat_name in existing:
        print(f"  {stat_name}: already exists, recalculating...")
    n = calc_percentiles(stat_name, table, col, lib, st, use_batting_qual=False)
    print(f"  {stat_name}: {n} records inserted")
    total_pit += n

print(f"\nTotal pitching stats inserted: {total_pit}")

# ── Section 3: Calculate hr_per_6 percentiles ───────────────────────────────

print("\n--- Calculating hr_per_6 percentiles ---")
conn.execute("DELETE FROM percentile_rankings WHERE stat_name='hr_per_6'")

seasons_hr = [r[0] for r in conn.execute(
    "SELECT DISTINCT season FROM custom_stats WHERE hr_per_6 IS NOT NULL AND season > 0"
).fetchall()]

total_hr = 0
for season in seasons_hr:
    q = qualifiers.get(season)
    min_ip = q['pitching_min_ip'] if q else 37.0

    rows = conn.execute("""
        SELECT cs.player_name, cs.hr_per_6
        FROM custom_stats cs
        JOIN pitching_stats ps ON LOWER(TRIM(ps.player_hashtag)) = LOWER(TRIM(cs.player_name)) AND ps.season = cs.season
        WHERE cs.season=? AND ps.ip >= ? AND cs.hr_per_6 IS NOT NULL
    """, (season, min_ip)).fetchall()

    if len(rows) < 3:
        continue

    vals = [r['hr_per_6'] for r in rows]

    for r in rows:
        v = r['hr_per_6']
        # lower is better: more pitchers with worse (higher) HR/6 = higher percentile
        worse_count = sum(1 for x in vals if x > v)
        pct = round((worse_count / len(vals)) * 100)
        pct = max(1, min(99, pct))

        conn.execute("""
            INSERT OR REPLACE INTO percentile_rankings
            (player_name, season, stat_name, stat_value, percentile, stat_type, qualified, pool_size)
            VALUES (?,?,?,?,?,?,1,?)
        """, (r['player_name'], season, 'hr_per_6', v, pct, 'pitching', len(vals)))
        total_hr += 1

conn.commit()
print(f"Inserted {total_hr} hr_per_6 percentile records across {len(seasons_hr)} seasons")

# Show 2025 sample
s2025 = conn.execute("""
    SELECT player_name, stat_value, percentile FROM percentile_rankings
    WHERE stat_name='hr_per_6' AND season=2025
    ORDER BY percentile DESC LIMIT 5
""").fetchall()
print("\n2025 best HR/6 (high percentile = good):")
for r in s2025:
    print(f"  {r['player_name']}: {r['stat_value']} HR/6, percentile {r['percentile']}")

worst2025 = conn.execute("""
    SELECT player_name, stat_value, percentile FROM percentile_rankings
    WHERE stat_name='hr_per_6' AND season=2025
    ORDER BY percentile ASC LIMIT 5
""").fetchall()
print("\n2025 worst HR/6 (low percentile = bad):")
for r in worst2025:
    print(f"  {r['player_name']}: {r['stat_value']} HR/6, percentile {r['percentile']}")

# Final verification
stats_all = set(r[0] for r in conn.execute(
    "SELECT DISTINCT stat_name FROM percentile_rankings"
).fetchall())
print("\nAll percentile stats:", sorted(stats_all))

# 2025 coverage
stats_2025 = set(r[0] for r in conn.execute(
    "SELECT DISTINCT stat_name FROM percentile_rankings WHERE season=2025"
).fetchall())
print("\n2025 percentile stats:", sorted(stats_2025))

conn.close()
print("\nDone!")
