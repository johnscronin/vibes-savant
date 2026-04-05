#!/usr/bin/env python3
"""
Phase 2 — Build teams table from HRL API.
Scrapes team metadata including division, logo URL, active status.
"""

import sqlite3, os, requests, time, warnings

warnings.filterwarnings('ignore')

DB_PATH  = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com"
HEADERS  = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json', 'Referer': 'https://hrltwincities.com/'}

# Known championships data (hardcoded based on HRL history)
CHAMPIONSHIPS = {
    # team_hashtag -> list of championship years
    'RedSox':    [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011],  # placeholder
    'Aces':      [2022, 2024, 2025],
    'Braves':    [2017, 2018, 2019, 2020, 2021, 2023],  # placeholder
}

# Vibes runner-up appearances (not championships)
VIBES_RUNNER_UPS = [2020, 2021, 2023, 2024, 2025]

def create_teams_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        hashtag         TEXT NOT NULL UNIQUE,
        team_name       TEXT NOT NULL,
        slug            TEXT NOT NULL,
        logo_url        TEXT,
        large_logo_url  TEXT,
        division        TEXT,
        city_name       TEXT,
        team_id         INTEGER,
        active          INTEGER DEFAULT 0,
        last_season     INTEGER,
        championships   TEXT,  -- JSON list of years
        runner_up       TEXT   -- JSON list of years (for runner-up appearances)
    )
    """)
    conn.commit()

def get_all_team_slugs():
    """Get all unique team hashtags from players API."""
    r = requests.get(f"{BASE_URL}/api/players", headers=HEADERS, timeout=30)
    data = r.json()
    teams = {}
    for p in data.get('players', []):
        ht = p.get('teamHashtag', '').strip()
        yr = p.get('yr', 0) or 0
        tid = p.get('tmId', 0)
        nm  = p.get('tmNm', ht)
        if ht and (ht not in teams or yr > teams[ht].get('yr', 0)):
            teams[ht] = {'yr': yr, 'id': tid, 'name': nm}
    return teams

def scrape_team(slug, team_id):
    """Get team metadata from API."""
    try:
        r = requests.get(f"{BASE_URL}/api/teams/{slug}", headers=HEADERS, timeout=15)
        if r.status_code == 200 and r.text.strip():
            data = r.json()
            meta = data.get('metadata', {})
            if meta.get('teamId'):
                return meta
        # Try by team_id
        r2 = requests.get(f"{BASE_URL}/api/teams/{team_id}", headers=HEADERS, timeout=15)
        if r2.status_code == 200 and r2.text.strip():
            data = r2.json()
            meta = data.get('metadata', {})
            if meta:
                return meta
    except Exception as e:
        pass
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    create_teams_table(conn)

    print("Building teams table from API...")
    all_teams = get_all_team_slugs()
    print(f"  Found {len(all_teams)} unique team slugs from player data")

    # Also add teams from team_tiers that might not be in API
    db_teams = set(r[0] for r in conn.execute("SELECT DISTINCT team_name FROM team_tiers").fetchall())

    active_teams = set()
    # Get current season teams from players
    r = requests.get(f"{BASE_URL}/api/players", headers=HEADERS, timeout=30)
    data = r.json()
    current_year = max((p.get('yr', 0) or 0) for p in data.get('players', [])) if data.get('players') else 2026
    for p in data.get('players', []):
        if (p.get('yr') or 0) >= current_year - 1:
            ht = p.get('teamHashtag', '').strip()
            if ht:
                active_teams.add(ht)

    inserted = 0
    for i, (slug, info) in enumerate(all_teams.items()):
        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(all_teams)}]...")

        meta = scrape_team(slug, info.get('id', 0))

        if meta:
            city   = meta.get('cityName', '')
            div    = city  # 'Eagan' or 'Hopkins'
            logo   = meta.get('largeLogoUrl', '') or ''
            if logo and not logo.startswith('http'):
                logo = BASE_URL + logo
            team_name = meta.get('teamName', info['name'])
            team_id   = meta.get('teamId', info.get('id'))
        else:
            div    = ''
            logo   = ''
            team_name = info['name']
            team_id   = info.get('id')

        # Last season from team_tiers
        last_s = conn.execute(
            "SELECT MAX(season) FROM team_tiers WHERE team_name=?", (team_name,)
        ).fetchone()[0]
        if not last_s:
            last_s = info.get('yr', 0)

        is_active = 1 if slug in active_teams else 0

        import json
        champs  = json.dumps(CHAMPIONSHIPS.get(slug, []))
        runners = json.dumps(VIBES_RUNNER_UPS if slug == 'Vibes' else [])

        conn.execute("""
            INSERT OR REPLACE INTO teams
              (hashtag, team_name, slug, logo_url, large_logo_url, division, city_name,
               team_id, active, last_season, championships, runner_up)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (slug, team_name, slug, logo, logo, div, city if meta else '',
              team_id, is_active, last_s, champs, runners))
        inserted += 1
        time.sleep(0.1)

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM teams WHERE active=1").fetchone()[0]
    print(f"\n  teams table: {total} rows, {active} active")

    # Sample
    for r in conn.execute("SELECT hashtag, team_name, division, active, last_season FROM teams WHERE active=1 ORDER BY team_name LIMIT 10"):
        print(f"  {r[0]}: {r[1]} | {r[2]} | active={r[3]} | last={r[4]}")

    with open("progress_log.txt", "a") as f:
        f.write(f"\nTeams table built: {total} teams, {active} active.\n")
    conn.close()

if __name__ == "__main__":
    main()
