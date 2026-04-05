#!/usr/bin/env python3
"""
Vibes Savant — HQ Opponent Splits + Opponent Tier Splits calculator.

After scrape_league_bvp.py has run, this creates splits for ALL HRL players.

hq_opponent_splits  — batting vs. HQ pitchers (ERA < 3.50), min 10 PA
                    — uses combined (reg+playoff) BvP rows for league players,
                      and regular-only rows for Vibes players (no playoff BvP data)
opponent_tier_splits — batting splits vs. Elite / Average / Weak opponent teams
"""

import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')

HQ_PITCHER_ERA_THRESHOLD = 3.50
MIN_PA_VS_HQ = 10


def create_tables(conn):
    conn.execute("DROP TABLE IF EXISTS hq_opponent_splits")
    conn.execute("""
        CREATE TABLE hq_opponent_splits (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            season     INTEGER NOT NULL,
            split_type TEXT NOT NULL,
            pa  INTEGER, ab  INTEGER,
            h   INTEGER, hr  INTEGER,  rbi INTEGER,
            bb  INTEGER, so  INTEGER,
            avg REAL,   obp REAL,   slg REAL,   ops REAL,
            doubles INTEGER, triples INTEGER,
            UNIQUE(player_name, season, split_type)
        )
    """)
    conn.execute("DROP TABLE IF EXISTS opponent_tier_splits")
    conn.execute("""
        CREATE TABLE opponent_tier_splits (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            season      INTEGER NOT NULL,
            tier        TEXT NOT NULL,
            split_role  TEXT NOT NULL,
            pa  INTEGER, ab  INTEGER, h  INTEGER,
            hr  INTEGER, rbi INTEGER, bb INTEGER, so INTEGER,
            doubles INTEGER, triples INTEGER,
            avg REAL, obp REAL, slg REAL, ops REAL,
            ip  REAL, era REAL, whip REAL, k INTEGER, opp_bb INTEGER,
            UNIQUE(player_name, season, tier, split_role)
        )
    """)
    conn.commit()


def safe_rate(num, den):
    return round(num / den, 3) if den else None


def calc_batting_line(rows):
    """Aggregate a list of bvp row dicts into a batting split dict."""
    ab  = sum(r.get('ab')      or 0 for r in rows)
    if not ab:
        return None
    h   = sum(r.get('h')       or 0 for r in rows)
    hr  = sum(r.get('hr')      or 0 for r in rows)
    rbi = sum(r.get('rbi')     or 0 for r in rows)
    bb  = sum(r.get('bb')      or 0 for r in rows)
    so  = sum(r.get('so')      or 0 for r in rows)
    d   = sum(r.get('doubles') or 0 for r in rows)
    t3  = sum(r.get('triples') or 0 for r in rows)
    pa  = ab + bb
    avg = safe_rate(h, ab)
    obp = safe_rate(h + bb, pa)
    tb  = h - d - t3 - hr + d*2 + t3*3 + hr*4
    slg = safe_rate(tb, ab)
    ops = round((obp or 0) + (slg or 0), 3) if (obp and slg) else None
    return dict(pa=pa, ab=ab, h=h, hr=hr, rbi=rbi, bb=bb, so=so,
                doubles=d, triples=t3, avg=avg, obp=obp, slg=slg, ops=ops)


def get_all_players_with_bvp(conn):
    """Get all players who have BvP data (both Vibes regular and league combined)."""
    rows = conn.execute("""
        SELECT DISTINCT player_name FROM batter_vs_pitcher
        WHERE season != 'Career'
        ORDER BY player_name
    """).fetchall()
    return [r[0] for r in rows]


def get_bvp_for_player_season(conn, player, season_str):
    """
    Get BvP rows for player/season. For Vibes players use regular tab.
    For league players use combined tab. Fall back to regular if no combined.
    """
    # Try combined first (league-wide scrape)
    rows = conn.execute("""
        SELECT opposing_pitcher, ab, h, hr, rbi, bb, so, doubles, triples
        FROM batter_vs_pitcher
        WHERE player_name=? AND season=? AND tab_type='combined'
    """, (player, season_str)).fetchall()
    if rows:
        return [dict(r) for r in rows]
    # Fall back to regular
    rows = conn.execute("""
        SELECT opposing_pitcher, ab, h, hr, rbi, bb, so, doubles, triples
        FROM batter_vs_pitcher
        WHERE player_name=? AND season=? AND tab_type='regular'
    """, (player, season_str)).fetchall()
    return [dict(r) for r in rows]


def calculate_hq_pitcher_splits(conn):
    """For every player with BvP data, aggregate stats vs HQ pitchers (ERA < 3.50)."""
    print("\n=== HQ PITCHER SPLITS ===")
    inserted = 0
    players = get_all_players_with_bvp(conn)
    print(f"  Processing {len(players)} players...")

    for player in players:
        seasons = [r[0] for r in conn.execute("""
            SELECT DISTINCT season FROM batter_vs_pitcher
            WHERE player_name=? AND season != 'Career'
            ORDER BY season
        """, (player,)).fetchall()]

        for season_str in seasons:
            try:
                season = int(season_str)
            except ValueError:
                continue

            bvp_rows = get_bvp_for_player_season(conn, player, season_str)

            hq_rows = []
            for row in bvp_rows:
                pitcher_name = row['opposing_pitcher']
                era_row = conn.execute("""
                    SELECT era FROM league_pitching_stats
                    WHERE season=? AND player_name=? AND ip > 0
                    LIMIT 1
                """, (season, pitcher_name)).fetchone()
                if era_row and era_row[0] is not None:
                    if era_row[0] < HQ_PITCHER_ERA_THRESHOLD:
                        hq_rows.append(row)

            if not hq_rows:
                continue

            split = calc_batting_line(hq_rows)
            if not split or (split['pa'] or 0) < MIN_PA_VS_HQ:
                continue

            conn.execute("""
                INSERT OR REPLACE INTO hq_opponent_splits
                (player_name, season, split_type, pa, ab, h, hr, rbi, bb, so,
                 avg, obp, slg, ops, doubles, triples)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (player, season, 'vs_hq_pitcher',
                  split['pa'], split['ab'], split['h'], split['hr'],
                  split['rbi'], split['bb'], split['so'],
                  split['avg'], split['obp'], split['slg'], split['ops'],
                  split['doubles'], split['triples']))
            inserted += 1

        conn.commit()

    print(f"  Inserted {inserted} HQ pitcher split rows")
    # Print pool sizes per season
    print("\n  Pool sizes per season (players with 10+ PA vs HQ pitchers):")
    for r in conn.execute("""
        SELECT season, COUNT(*) FROM hq_opponent_splits
        WHERE split_type='vs_hq_pitcher' GROUP BY season ORDER BY season DESC LIMIT 10
    """).fetchall():
        print(f"    {r[0]}: {r[1]} players")


def calculate_opponent_tier_splits(conn):
    """Batting splits vs Elite / Average / Weak opponents for all players with BvP data."""
    print("\n=== OPPONENT TIER SPLITS ===")
    inserted = 0

    # Only do tier splits for Vibes players (this is Vibes-specific context)
    vibes = [r[0] for r in conn.execute(
        "SELECT DISTINCT player_name FROM batter_vs_pitcher WHERE tab_type='regular' AND season != 'Career'"
        " AND player_name IN ('Anakin','CatNip','Cheerio','Epstein','FishHook','HuckFinn','Jessie','Kar','Nightmare','Fortnite')"
    ).fetchall()]

    for player in vibes:
        seasons = [r[0] for r in conn.execute("""
            SELECT DISTINCT season FROM batter_vs_pitcher
            WHERE player_name=? AND season != 'Career' AND tab_type='regular'
        """, (player,)).fetchall()]

        for season_str in seasons:
            try:
                season = int(season_str)
            except ValueError:
                continue

            bvp_rows = conn.execute("""
                SELECT opposing_pitcher, ab, h, hr, rbi, bb, so, doubles, triples
                FROM batter_vs_pitcher
                WHERE player_name=? AND season=? AND tab_type='regular'
            """, (player, season_str)).fetchall()
            bvp_rows = [dict(r) for r in bvp_rows]

            tier_rows = {'Elite': [], 'Average': [], 'Weak': []}
            for row in bvp_rows:
                pitcher_name = row['opposing_pitcher']
                team_row = conn.execute("""
                    SELECT team FROM league_pitching_stats
                    WHERE season=? AND player_name=? LIMIT 1
                """, (season, pitcher_name)).fetchone()
                if not team_row:
                    continue
                tier_row = conn.execute("""
                    SELECT tier FROM team_tiers WHERE season=? AND team_name=?
                """, (season, team_row[0])).fetchone()
                if not tier_row:
                    continue
                tier = tier_row[0]
                if tier in tier_rows:
                    tier_rows[tier].append(row)

            for tier, rows in tier_rows.items():
                if not rows:
                    continue
                split = calc_batting_line(rows)
                if not split:
                    continue
                conn.execute("""
                    INSERT OR REPLACE INTO opponent_tier_splits
                    (player_name, season, tier, split_role,
                     pa, ab, h, hr, rbi, bb, so, doubles, triples,
                     avg, obp, slg, ops)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (player, season, tier, 'batting',
                      split['pa'], split['ab'], split['h'], split['hr'],
                      split['rbi'], split['bb'], split['so'],
                      split['doubles'], split['triples'],
                      split['avg'], split['obp'], split['slg'], split['ops']))
                inserted += 1

        conn.commit()

    print(f"  Inserted {inserted} opponent tier split rows")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    calculate_hq_pitcher_splits(conn)
    calculate_opponent_tier_splits(conn)
    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
