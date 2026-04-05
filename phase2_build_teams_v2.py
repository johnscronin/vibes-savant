#!/usr/bin/env python3
"""
Phase 2 — Build teams table (v2, faster, no hanging).
Builds from players API (one call), then enriches with team metadata.
"""

import sqlite3, os, requests, time, json, warnings

warnings.filterwarnings('ignore')

DB_PATH  = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com"
HEADERS  = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json',
            'Referer': 'https://hrltwincities.com/'}

CHAMPIONSHIPS = {
    'Aces':   [2022, 2024, 2025],
}

RUNNER_UPS = {
    'Vibes': [2020, 2021, 2023, 2024, 2025],
}

# Known division assignments from HRL structure
KNOWN_DIVISIONS = {
    # Eagan Central
    'Vibes': 'Eagan', 'RedSox': 'Eagan', 'Aces': 'Eagan', 'Bears': 'Eagan',
    'Eagles': 'Eagan', 'Yankees': 'Eagan', 'Tigers': 'Eagan', 'Twins': 'Eagan',
    # Hopkins Central
    'Braves': 'Hopkins', 'Sharks': 'Hopkins', 'Pirates': 'Hopkins', 'Jets': 'Hopkins',
    'Wolves': 'Hopkins', 'Cyclones': 'Hopkins',
}

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
        championships   TEXT,
        runner_up       TEXT
    )
    """)
    conn.commit()

def fetch_team_meta(slug, tid):
    """Try to get team metadata from API. Returns dict or None. Never hangs."""
    for endpoint in [f"/api/teams/{slug}", f"/api/teams/{tid}"]:
        try:
            r = requests.get(BASE_URL + endpoint, headers=HEADERS,
                             timeout=(5, 8), verify=False)
            if r.status_code == 200:
                data = r.json()
                meta = data.get('metadata', {})
                if meta and meta.get('teamId'):
                    return meta
        except Exception:
            pass
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    create_teams_table(conn)

    print("Step 1: Fetching all players from API...", flush=True)
    try:
        r = requests.get(f"{BASE_URL}/api/players", headers=HEADERS,
                         timeout=(10, 30), verify=False)
        players = r.json().get('players', [])
    except Exception as e:
        print(f"  ERROR fetching players: {e}")
        conn.close()
        return

    print(f"  Got {len(players)} players", flush=True)

    # Build team dict: hashtag -> {yr, id, name}
    teams = {}
    for p in players:
        ht  = (p.get('teamHashtag') or '').strip()
        yr  = p.get('yr') or 0
        tid = p.get('tmId') or 0
        nm  = p.get('tmNm') or ht
        if ht and (ht not in teams or yr > teams[ht]['yr']):
            teams[ht] = {'yr': yr, 'id': tid, 'name': nm}

    current_year = max((p.get('yr') or 0) for p in players)
    active_teams = {(p.get('teamHashtag') or '').strip()
                    for p in players if (p.get('yr') or 0) >= current_year - 1}
    print(f"  {len(teams)} unique teams, current year={current_year}, active={len(active_teams)}", flush=True)

    # Enrich from API per team
    print(f"Step 2: Enriching {len(teams)} teams with API metadata...", flush=True)
    enriched = 0
    for i, (slug, info) in enumerate(teams.items()):
        meta = fetch_team_meta(slug, info['id'])
        if meta:
            city   = meta.get('cityName', '') or ''
            div    = city  # 'Eagan' or 'Hopkins'
            logo   = meta.get('largeLogoUrl', '') or ''
            if logo and not logo.startswith('http'):
                logo = BASE_URL + logo
            t_name = meta.get('teamName') or info['name']
            tid    = meta.get('teamId') or info['id']
            enriched += 1
        else:
            city   = ''
            div    = KNOWN_DIVISIONS.get(slug, '')
            logo   = ''
            t_name = info['name']
            tid    = info['id']

        last_s = conn.execute(
            "SELECT MAX(season) FROM team_tiers WHERE team_name=?", (t_name,)
        ).fetchone()[0] or info['yr'] or 0

        is_active = 1 if slug in active_teams else 0
        champs    = json.dumps(CHAMPIONSHIPS.get(slug, []))
        runners   = json.dumps(RUNNER_UPS.get(slug, []))

        conn.execute("""
            INSERT OR REPLACE INTO teams
              (hashtag, team_name, slug, logo_url, large_logo_url, division, city_name,
               team_id, active, last_season, championships, runner_up)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (slug, t_name, slug, logo, logo, div, city,
              tid, is_active, last_s, champs, runners))

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(teams)}] enriched={enriched}", flush=True)
        time.sleep(0.05)

    conn.commit()
    total  = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM teams WHERE active=1").fetchone()[0]
    print(f"\nDone. {total} teams total, {active} active, {enriched} enriched from API.", flush=True)

    print("\nActive teams sample:")
    for row in conn.execute(
        "SELECT hashtag, team_name, division, active, last_season FROM teams WHERE active=1 ORDER BY team_name LIMIT 15"
    ):
        print(f"  {row[0]}: {row[1]} | div={row[2]} | last={row[4]}")

    with open("progress_log.txt", "a") as f:
        f.write(f"\nTeams table (v2): {total} total, {active} active, {enriched} API-enriched.\n")
    conn.close()

if __name__ == "__main__":
    main()
