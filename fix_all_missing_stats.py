#!/usr/bin/env python3
"""
Fix ALL missing player stats by populating batting_stats from league_batting_stats.

Strategy:
- Problem 1: Players with player records but no batting_stats
  -> Use league_batting_stats (match by nickname OR hashtag)
- Problem 2: Players in scrape_queue with failed=1
  -> Add player record if missing, then populate from league_batting_stats
- Problem 3: Players in league_batting_stats with no player record
  -> Add player record, then populate from league_batting_stats

league_batting_stats has: season, player_name, team, g, ab, r, h, doubles, triples, hr, rbi, bb, so, avg, obp, slg, ops
"""

import sqlite3
import re

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'


def ensure_unique_index(conn):
    """Ensure unique index on batting_stats."""
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_batting_stats_player_season ON batting_stats(player_hashtag, season, team_name)")
        conn.commit()
    except Exception as e:
        print(f"Index creation note: {e}")


def normalize_slug(name):
    """Convert display name to likely slug format."""
    # Remove special chars, title case join
    s = re.sub(r"['\"\.\s]+", '', name)
    return s


def insert_batting_from_league(conn, slug, player_id, lbs_rows):
    """Insert batting_stats rows from league_batting_stats data."""
    inserted = 0
    for row in lbs_rows:
        season, team, g, ab, r, h, doubles, triples, hr, rbi, bb, so, avg, obp, slg, ops = row

        # Calculate derived stats
        d = doubles or 0
        t = triples or 0
        home_runs = hr or 0
        hits = h or 0
        singles = max(0, hits - d - t - home_runs)
        total_bases = singles + (d * 2) + (t * 3) + (home_runs * 4) if hits > 0 else 0
        xbh = d + t + home_runs
        pa = (ab or 0) + (bb or 0)  # estimate: PA = AB + BB
        sac = None
        roe = None
        hr_rate = home_runs / ab if (ab and ab > 0 and home_runs) else None
        k_rate = so / pa if (so and pa and pa > 0) else None

        try:
            conn.execute("""
                INSERT OR REPLACE INTO batting_stats
                  (player_id, player_hashtag, season, team_name,
                   games, pa, ab, r, h, singles, doubles, triples, hr, rbi, bb, sac, so, roe,
                   avg, obp, slg, ops, hr_rate, k_rate, xbh, total_bases)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                player_id, slug,
                season, team,
                g, pa, ab, r, h,
                singles, doubles, triples, hr,
                rbi, bb, sac, so, roe,
                avg, obp, slg, ops,
                hr_rate, k_rate, xbh, total_bases,
            ))
            inserted += 1
        except Exception as e:
            print(f"  DB error for {slug} {season}: {e}")

    return inserted


def get_league_stats_for_name(conn, name):
    """Get all league_batting_stats rows for a given player_name."""
    return conn.execute("""
        SELECT season, team, g, ab, r, h, doubles, triples, hr, rbi, bb, so, avg, obp, slg, ops
        FROM league_batting_stats WHERE player_name=? ORDER BY season
    """, (name,)).fetchall()


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_unique_index(conn)

    # Count before
    bat_before = conn.execute("SELECT COUNT(*) FROM batting_stats").fetchone()[0]
    players_with_stats_before = conn.execute("SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats").fetchone()[0]
    print(f"Before: {bat_before} batting_stats rows, {players_with_stats_before} distinct players")

    # ===========================
    # PROBLEM 1: Players with player records but no batting_stats
    # ===========================
    print("\n=== PROBLEM 1: Players with player records but no batting_stats ===")
    problem1 = conn.execute("""
        SELECT p.hashtag, p.player_id, p.nickname, p.team_name, p.last_year
        FROM players p
        WHERE p.hashtag NOT IN (SELECT DISTINCT player_hashtag FROM batting_stats)
        ORDER BY p.last_year DESC, p.hashtag
    """).fetchall()
    print(f"Found {len(problem1)} players")

    p1_fixed = 0
    p1_skipped = []
    for slug, player_id, nickname, team, last_year in problem1:
        # Try to find in league_batting_stats
        # First by hashtag, then by nickname
        lbs_rows = get_league_stats_for_name(conn, slug)
        if not lbs_rows and nickname:
            lbs_rows = get_league_stats_for_name(conn, nickname)

        if lbs_rows:
            n = insert_batting_from_league(conn, slug, player_id, lbs_rows)
            print(f"  {slug}: inserted {n} rows from league_batting_stats ({len(lbs_rows)} seasons)")
            p1_fixed += 1
        else:
            print(f"  {slug}: NO data in league_batting_stats (new player or name mismatch)")
            p1_skipped.append(slug)

    conn.commit()
    print(f"\nProblem 1: Fixed {p1_fixed}, Skipped {len(p1_skipped)}: {p1_skipped}")

    # ===========================
    # PROBLEM 2: Failed scrapes - players not in HRL API
    # ===========================
    print("\n=== PROBLEM 2: Failed scrapes ===")
    problem2 = conn.execute("""
        SELECT player_name, slug, error_message FROM scrape_queue WHERE failed=1 ORDER BY player_name
    """).fetchall()
    print(f"Found {len(problem2)} failed scrape players")

    p2_fixed = 0
    p2_added_to_players = 0
    p2_skipped = []

    for player_name, slug, error_msg in problem2:
        # Skip if this player already has a valid player record AND batting stats
        existing_bat = conn.execute(
            "SELECT COUNT(*) FROM batting_stats WHERE player_hashtag=?", (slug,)
        ).fetchone()[0]
        if existing_bat > 0:
            print(f"  {slug}: already has {existing_bat} batting rows, skipping")
            continue

        # Try to find player in players table
        player_row = conn.execute(
            "SELECT hashtag, player_id FROM players WHERE hashtag=?", (slug,)
        ).fetchone()

        if not player_row:
            # Player not in players table - check if we can add them from league_batting_stats
            # First try matching by player_name or slug
            lbs_check = get_league_stats_for_name(conn, slug)
            if not lbs_check:
                # Try matching nickname
                lbs_check = get_league_stats_for_name(conn, player_name)

            if lbs_check:
                # Get team and last year from league stats
                seasons_data = lbs_check
                last_year = max(r[0] for r in seasons_data)
                last_team = next(r[1] for r in sorted(seasons_data, key=lambda x: x[0], reverse=True))

                # Add to players table
                conn.execute("""
                    INSERT OR IGNORE INTO players (hashtag, nickname, team_name, last_year, is_active, status)
                    VALUES (?, ?, ?, ?, 0, 'inactive')
                """, (slug, player_name, last_team, last_year))
                p2_added_to_players += 1
                player_id = None
            else:
                print(f"  {slug}: no league data found, skipping")
                p2_skipped.append(slug)
                continue
        else:
            player_id = player_row[1]

        # Try to get league stats
        lbs_rows = get_league_stats_for_name(conn, slug)
        if not lbs_rows:
            lbs_rows = get_league_stats_for_name(conn, player_name)

        if lbs_rows:
            n = insert_batting_from_league(conn, slug, player_id, lbs_rows)
            print(f"  {slug}: inserted {n} rows")
            p2_fixed += 1
        else:
            print(f"  {slug}: no league_batting_stats data (name: '{player_name}')")
            p2_skipped.append(slug)

    conn.commit()
    print(f"\nProblem 2: Fixed {p2_fixed}, Added {p2_added_to_players} new players, Skipped {len(p2_skipped)}")
    print(f"  Skipped: {p2_skipped[:20]}")

    # ===========================
    # PROBLEM 3: In league_batting_stats but no player record
    # ===========================
    print("\n=== PROBLEM 3: In league stats but no player record ===")
    problem3 = conn.execute("""
        SELECT lbs.player_name, MAX(lbs.season) as last_seen, COUNT(DISTINCT lbs.season) as seasons,
               MAX(lbs.team) as last_team
        FROM league_batting_stats lbs
        WHERE lbs.player_name NOT IN (SELECT hashtag FROM players)
        AND lbs.player_name NOT IN (SELECT nickname FROM players)
        GROUP BY lbs.player_name ORDER BY last_seen DESC, seasons DESC
    """).fetchall()
    print(f"Found {len(problem3)} players in league stats with no player record")

    p3_fixed = 0
    p3_added = 0
    for player_name, last_seen, seasons, last_team in problem3:
        # Check if they already have batting_stats
        existing = conn.execute(
            "SELECT COUNT(*) FROM batting_stats WHERE player_hashtag=?", (player_name,)
        ).fetchone()[0]
        if existing > 0:
            continue

        # Add to players table using player_name as slug
        conn.execute("""
            INSERT OR IGNORE INTO players (hashtag, nickname, team_name, last_year, is_active, status)
            VALUES (?, ?, ?, ?, 0, 'inactive')
        """, (player_name, player_name, last_team, last_seen))
        p3_added += 1

        # Get league stats
        lbs_rows = get_league_stats_for_name(conn, player_name)
        if lbs_rows:
            n = insert_batting_from_league(conn, player_name, None, lbs_rows)
            print(f"  {player_name}: added player + inserted {n} batting rows")
            p3_fixed += 1

    conn.commit()
    print(f"\nProblem 3: Added {p3_added} new players, populated stats for {p3_fixed}")

    # ===========================
    # FINAL COUNT
    # ===========================
    bat_after = conn.execute("SELECT COUNT(*) FROM batting_stats").fetchone()[0]
    players_with_stats_after = conn.execute("SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats").fetchone()[0]
    players_still_missing = conn.execute("""
        SELECT p.hashtag, p.nickname, p.last_year FROM players p
        WHERE p.hashtag NOT IN (SELECT DISTINCT player_hashtag FROM batting_stats)
        ORDER BY p.last_year DESC, p.hashtag
    """).fetchall()

    print(f"\n=== SUMMARY ===")
    print(f"Batting stats rows: {bat_before} -> {bat_after} (+{bat_after - bat_before})")
    print(f"Players with stats: {players_with_stats_before} -> {players_with_stats_after}")
    print(f"Players still missing batting stats: {len(players_still_missing)}")
    for p in players_still_missing[:20]:
        print(f"  {p}")

    conn.close()


if __name__ == "__main__":
    main()
