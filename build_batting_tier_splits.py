"""
Build batting opponent tier splits from batter_vs_pitcher data.
For each batter-season, look at all pitchers they faced (from batter_vs_pitcher),
find those pitchers' teams, look up tier.
Aggregate stats by tier.
"""
import sqlite3

DB = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/vibes_savant.db'

# Team name normalization map (pitching_stats short name -> team_tiers full name)
TEAM_ALIASES = {
    'Cowboys': 'Space Cowboys',
    'HuaHuas': 'Chihuahuas',
    'Mericans': 'Americans',
    'Yanks': 'Yankees',
}

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Check what seasons we have tier data for
seasons = [r[0] for r in conn.execute("SELECT DISTINCT season FROM team_tiers ORDER BY season").fetchall()]
print(f"Seasons with tier data: {seasons}")

# Get all batters (from batter_vs_pitcher)
batters = [r[0] for r in conn.execute(
    "SELECT DISTINCT player_name FROM batter_vs_pitcher WHERE tab_type='combined'"
).fetchall()]
print(f"Total batters in BvP: {len(batters)}")

# Build team->tier lookup: {season: {team_name: tier}}
tier_lookup = {}
for row in conn.execute("SELECT season, team_name, tier FROM team_tiers"):
    s = row['season']
    if s not in tier_lookup:
        tier_lookup[s] = {}
    tier_lookup[s][row['team_name']] = row['tier']

# Add aliases to tier lookup
for s in list(tier_lookup.keys()):
    for alias, canonical in TEAM_ALIASES.items():
        if canonical in tier_lookup[s]:
            tier_lookup[s][alias] = tier_lookup[s][canonical]

# Build pitcher -> team lookup: {season: {pitcher_hashtag: team_name}}
# From pitching_stats
pitcher_team = {}
for row in conn.execute("SELECT player_hashtag, season, team_name FROM pitching_stats"):
    s = row['season']
    if s not in pitcher_team:
        pitcher_team[s] = {}
    pitcher_team[s][row['player_hashtag']] = row['team_name']

print(f"Pitcher-team entries by season count: {len(pitcher_team)} seasons")

# Delete existing batting tier split rows to rebuild them all
# But preserve the ones that might have been hand-entered (check if they have 'g' column != NULL)
# Actually let's just rebuild all batting tiers from BvP
conn.execute("DELETE FROM opponent_tier_splits WHERE split_role='batting'")
conn.commit()
print("Cleared existing batting tier splits")

# For each batter, for each season in BvP, aggregate stats by opponent pitcher tier
total_inserted = 0

for batter in batters:
    # Get all BvP rows for this batter
    bvp_rows = conn.execute("""
        SELECT opposing_pitcher, season, ab, h, doubles, triples, hr, rbi, bb, sac, so
        FROM batter_vs_pitcher
        WHERE player_name=? AND tab_type='combined'
        ORDER BY season
    """, (batter,)).fetchall()

    if not bvp_rows:
        continue

    # Group by season and tier
    season_tier_stats = {}

    for row in bvp_rows:
        pitcher = row['opposing_pitcher']
        season_str = row['season']
        try:
            season_int = int(season_str)
        except:
            continue

        if season_int not in tier_lookup:
            continue

        # Look up pitcher's team and tier
        pitcher_teams = pitcher_team.get(season_int, {})
        team = pitcher_teams.get(pitcher)
        if not team:
            continue

        tier = tier_lookup[season_int].get(team)
        if not tier:
            tier = tier_lookup[season_int].get(TEAM_ALIASES.get(team, team))
        if not tier:
            continue

        if season_int not in season_tier_stats:
            season_tier_stats[season_int] = {}
        if tier not in season_tier_stats[season_int]:
            season_tier_stats[season_int][tier] = {
                'ab': 0, 'h': 0, 'doubles': 0, 'triples': 0,
                'hr': 0, 'rbi': 0, 'bb': 0, 'so': 0
            }

        s = season_tier_stats[season_int][tier]
        s['ab']      += row['ab']      or 0
        s['h']       += row['h']       or 0
        s['doubles'] += row['doubles'] or 0
        s['triples'] += row['triples'] or 0
        s['hr']      += row['hr']      or 0
        s['rbi']     += row['rbi']     or 0
        s['bb']      += row['bb']      or 0
        s['so']      += row['so']      or 0

    # Insert aggregated rows
    for season_int, tiers_data in season_tier_stats.items():
        for tier, s in tiers_data.items():
            ab = s['ab']
            if ab == 0:
                continue

            h  = s['h']
            bb = s['bb']
            pa = ab + bb
            d  = s['doubles']
            t3 = s['triples']
            hr = s['hr']
            so = s['so']

            # Rate stats
            avg = round(h / ab, 3) if ab > 0 else 0
            obp = round((h + bb) / pa, 3) if pa > 0 else 0
            tb  = (h - d - t3 - hr) + d*2 + t3*3 + hr*4
            slg = round(tb / ab, 3) if ab > 0 else 0
            ops = round(obp + slg, 3)

            try:
                conn.execute("""
                    INSERT OR REPLACE INTO opponent_tier_splits
                        (player_name, season, tier, split_role,
                         pa, ab, h, hr, rbi, bb, so, doubles, triples,
                         avg, obp, slg, ops)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (batter, season_int, tier, 'batting',
                      pa, ab, h, hr, s['rbi'], bb, so, d, t3,
                      avg, obp, slg, ops))
                total_inserted += 1
            except Exception as e:
                print(f"Error inserting {batter}/{season_int}/{tier}: {e}")

conn.commit()
print(f"\nTotal batting tier split rows inserted: {total_inserted}")

# Print summary for 2025
print("\n=== 2025 Batting Tier Splits Sample (qualified batters PA>=25) ===")
rows = conn.execute("""
    SELECT player_name, tier, pa, ab, avg, obp, slg, ops
    FROM opponent_tier_splits
    WHERE season=2025 AND split_role='batting' AND pa >= 25
    ORDER BY ops DESC
    LIMIT 30
""").fetchall()
for r in rows:
    print(f"  {r['player_name']:15} {r['tier']:8} PA={r['pa']:4} AVG={r['avg']:.3f} OPS={r['ops']:.3f}")

# Count by season
print("\n=== Batting tier splits by season ===")
counts = conn.execute("""
    SELECT season, tier, COUNT(*) as batters
    FROM opponent_tier_splits
    WHERE split_role='batting'
    GROUP BY season, tier
    ORDER BY season DESC, tier
    LIMIT 15
""").fetchall()
for r in counts:
    print(f"  {r['season']} {r['tier']:8} {r['batters']} batters")

print("\nUnique batters with batting tier splits:")
cnt = conn.execute("SELECT COUNT(DISTINCT player_name) FROM opponent_tier_splits WHERE split_role='batting'").fetchone()[0]
print(f"  {cnt}")

conn.close()
print("\nDone!")
