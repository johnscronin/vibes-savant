#!/usr/bin/env python3
"""
Phase 2 — Build teams table (v3).
API calls ONLY for active teams (22 slugs, all clean).
Historical teams populated from DB data, no API calls.
"""

import sqlite3, os, requests, time, json, warnings

warnings.filterwarnings('ignore')

DB_PATH  = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com"
HEADERS  = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json',
            'Referer': 'https://hrltwincities.com/'}

CHAMPIONSHIPS = {
    'Aces': [2022, 2024, 2025],
}
RUNNER_UPS = {
    'Vibes': [2020, 2021, 2023, 2024, 2025],
}

def create_teams_table(conn):
    conn.execute("DROP TABLE IF EXISTS teams")
    conn.execute("""
    CREATE TABLE teams (
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
        championships   TEXT,
        runner_up       TEXT
    )
    """)
    conn.commit()

def fetch_team_meta(slug):
    try:
        r = requests.get(f"{BASE_URL}/api/teams/{slug}", headers=HEADERS,
                         timeout=(4, 6), verify=False)
        if r.status_code == 200:
            data = r.json()
            meta = data.get('metadata', {})
            if meta and meta.get('teamId'):
                return meta
    except Exception as e:
        print(f"    API error for {slug}: {e}", flush=True)
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    create_teams_table(conn)

    # Step 1: Get all teams from players API
    print("Fetching players API...", flush=True)
    r = requests.get(f"{BASE_URL}/api/players", headers=HEADERS,
                     timeout=(5, 20), verify=False)
    players = r.json().get('players', [])
    print(f"  {len(players)} players", flush=True)

    max_yr = max((p.get('yr') or 0) for p in players)
    print(f"  Current year: {max_yr}", flush=True)

    # Build team dict
    teams = {}
    for p in players:
        ht  = (p.get('teamHashtag') or '').strip()
        yr  = p.get('yr') or 0
        tid = p.get('tmId') or 0
        nm  = p.get('tmNm') or ht
        if ht and (ht not in teams or yr > teams[ht]['yr']):
            teams[ht] = {'yr': yr, 'id': tid, 'name': nm, 'active': False}

    active_slugs = set()
    for p in players:
        if (p.get('yr') or 0) >= max_yr - 1:
            ht = (p.get('teamHashtag') or '').strip()
            if ht:
                active_slugs.add(ht)
                if ht in teams:
                    teams[ht]['active'] = True

    print(f"  {len(teams)} unique teams, {len(active_slugs)} active", flush=True)

    # Step 2: API calls only for active teams
    print(f"\nFetching division/logo for {len(active_slugs)} active teams...", flush=True)
    meta_cache = {}
    for slug in sorted(active_slugs):
        print(f"  {slug}...", end=' ', flush=True)
        meta = fetch_team_meta(slug)
        if meta:
            meta_cache[slug] = meta
            city = meta.get('cityName', '')
            print(f"OK ({city})", flush=True)
        else:
            print("no meta", flush=True)
        time.sleep(0.1)

    # Step 3: Insert all teams
    print(f"\nInserting {len(teams)} teams into DB...", flush=True)
    inserted = 0
    for slug, info in teams.items():
        meta = meta_cache.get(slug)
        if meta:
            city      = meta.get('cityName', '') or ''
            div       = city
            logo      = meta.get('largeLogoUrl', '') or ''
            if logo and not logo.startswith('http'):
                logo = BASE_URL + logo
            t_name    = meta.get('teamName') or info['name']
            tid       = meta.get('teamId') or info['id']
        else:
            city      = ''
            div       = ''
            logo      = ''
            t_name    = info['name']
            tid       = info['id']

        last_s = conn.execute(
            "SELECT MAX(season) FROM team_tiers WHERE team_name=?", (t_name,)
        ).fetchone()[0] or info['yr'] or 0

        is_active = 1 if info['active'] else 0
        champs    = json.dumps(CHAMPIONSHIPS.get(slug, []))
        runners   = json.dumps(RUNNER_UPS.get(slug, []))

        conn.execute("""
            INSERT OR REPLACE INTO teams
              (hashtag, team_name, slug, logo_url, large_logo_url, division, city_name,
               team_id, active, last_season, championships, runner_up)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (slug, t_name, slug, logo, logo, div, city,
              tid, is_active, last_s, champs, runners))
        inserted += 1

    conn.commit()
    total  = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM teams WHERE active=1").fetchone()[0]
    print(f"\nDone. {total} teams total, {active} active.", flush=True)

    print("\nActive teams:")
    for row in conn.execute(
        "SELECT hashtag, team_name, division, last_season FROM teams WHERE active=1 ORDER BY team_name"
    ):
        print(f"  {row[0]}: {row[1]} | div={row[2]} | last={row[3]}")

    with open("progress_log.txt", "a") as f:
        f.write(f"\nTeams table (v3): {total} total, {active} active.\n")

    conn.close()
    print("\nTeams table built successfully.", flush=True)

if __name__ == "__main__":
    main()
