"""
Build pitcher opponent tier splits from batter_vs_pitcher data.
For each pitcher-season, look at all batters they faced (from batter_vs_pitcher
where opposing_pitcher = pitcher), find those batters' teams, look up tier.
Aggregate stats by tier.
"""
import sqlite3

DB = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/vibes_savant.db'

# Team name normalization map (batting_stats short name -> team_tiers full name)
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

# Get all pitchers (from pitching_stats with ip > 0)
pitchers = conn.execute("""
    SELECT DISTINCT player_hashtag
    FROM pitching_stats
    WHERE ip > 0
""").fetchall()
pitchers = [r[0] for r in pitchers]
print(f"Total pitchers: {len(pitchers)}")

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

print("Sample tier lookup 2025:", {k: v for k, v in list(tier_lookup.get(2025, {}).items())[:5]})

# Build batter -> team lookup: {season: {batter_hashtag: team_name}}
batter_team = {}
for row in conn.execute("SELECT player_hashtag, season, team_name FROM batting_stats"):
    s = row['season']
    if s not in batter_team:
        batter_team[s] = {}
    batter_team[s][row['player_hashtag']] = row['team_name']

print(f"Batter-team entries by season: { {s: len(v) for s,v in batter_team.items()} }")

# Delete existing pitcher tier split rows to rebuild
conn.execute("DELETE FROM opponent_tier_splits WHERE split_role='pitching'")
conn.commit()
print("Cleared existing pitching tier splits")

# For each pitcher, for each season in BvP, aggregate stats by opponent tier
total_inserted = 0

for pitcher in pitchers:
    # Get all BvP rows where this pitcher is the opposing_pitcher
    bvp_rows = conn.execute("""
        SELECT player_name, season, ab, h, doubles, triples, hr, rbi, bb, sac, so, roe
        FROM batter_vs_pitcher
        WHERE opposing_pitcher=? AND tab_type='combined'
        ORDER BY season
    """, (pitcher,)).fetchall()

    if not bvp_rows:
        continue

    # Group by season and tier
    # {season: {tier: {stat: value}}}
    season_tier_stats = {}

    for row in bvp_rows:
        batter = row['player_name']
        season_str = row['season']
        try:
            season_int = int(season_str)
        except:
            continue

        if season_int not in tier_lookup:
            continue

        # Look up batter's team and tier
        batter_teams = batter_team.get(season_int, {})
        team = batter_teams.get(batter)
        if not team:
            continue

        tier = tier_lookup[season_int].get(team)
        if not tier:
            # Try normalized
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

            # Estimate IP from outs: rough outs = ab - h + so*0.7 (approximation)
            # Better: use actual pitching_stats IP for context, but we don't have game-by-game
            # Leave ip/era/whip as NULL for now

            try:
                conn.execute("""
                    INSERT OR REPLACE INTO opponent_tier_splits
                        (player_name, season, tier, split_role,
                         pa, ab, h, hr, rbi, bb, so, doubles, triples,
                         avg, obp, slg, ops)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (pitcher, season_int, tier, 'pitching',
                      pa, ab, h, hr, s['rbi'], bb, so, d, t3,
                      avg, obp, slg, ops))
                total_inserted += 1
            except Exception as e:
                print(f"Error inserting {pitcher}/{season_int}/{tier}: {e}")

conn.commit()
print(f"\nTotal pitching tier split rows inserted: {total_inserted}")

# Print summary for 2025
print("\n=== 2025 Pitcher Tier Splits Sample ===")
rows = conn.execute("""
    SELECT player_name, tier, pa, ab, avg, obp, slg, ops
    FROM opponent_tier_splits
    WHERE season=2025 AND split_role='pitching'
    ORDER BY player_name, CASE tier WHEN 'Elite' THEN 1 WHEN 'Average' THEN 2 ELSE 3 END
    LIMIT 30
""").fetchall()
for r in rows:
    print(f"  {r['player_name']:15} {r['tier']:8} PA={r['pa']:3} AVG={r['avg']:.3f} OBP={r['obp']:.3f} OPS={r['ops']:.3f}")

# Count by season
print("\n=== Pitching tier splits by season ===")
counts = conn.execute("""
    SELECT season, tier, COUNT(*) as pitchers
    FROM opponent_tier_splits
    WHERE split_role='pitching'
    GROUP BY season, tier
    ORDER BY season DESC, tier
""").fetchall()
for r in counts:
    print(f"  {r['season']} {r['tier']:8} {r['pitchers']} pitchers")

conn.close()
print("\nDone!")
