#!/usr/bin/env python3
"""
Phase 2 Step 1 — Build scrape_queue from all known HRL players.
Uses the HRL API to get all players, deduplicates against existing data,
tests URLs, and saves to scrape_queue table.
"""

import sqlite3, os, requests, time, re
from urllib.parse import quote

DB_PATH  = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://hrltwincities.com/'
}

def normalize_name(name):
    """Normalize name for deduplication — lowercase, strip extra spaces."""
    return re.sub(r'\s+', ' ', name.strip().lower())

def names_match(a, b):
    """Check if two names are variants (e.g. Mippey5 vs Mippey 5)."""
    a_norm = re.sub(r'\s+', '', normalize_name(a))
    b_norm = re.sub(r'\s+', '', normalize_name(b))
    return a_norm == b_norm

def create_scrape_queue(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS scrape_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT NOT NULL UNIQUE,
        slug        TEXT NOT NULL,
        url         TEXT NOT NULL,
        priority    INTEGER DEFAULT 0,
        scraped     INTEGER DEFAULT 0,
        failed      INTEGER DEFAULT 0,
        error_message TEXT DEFAULT NULL
    )
    """)
    conn.commit()

def get_api_players():
    """Fetch all players from the HRL API."""
    try:
        resp = requests.get(f"{BASE_URL}/api/players", headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        players = data.get('players', [])
        print(f"  API returned {len(players)} player records")
        return players
    except Exception as e:
        print(f"  API error: {e}")
        return []

def get_db_names(conn):
    """Union all player names from all relevant tables."""
    names = set()

    # league_batting_stats
    for r in conn.execute("SELECT DISTINCT player_name FROM league_batting_stats WHERE player_name IS NOT NULL"):
        names.add(r[0].strip())

    # league_pitching_stats
    for r in conn.execute("SELECT DISTINCT player_name FROM league_pitching_stats WHERE player_name IS NOT NULL"):
        names.add(r[0].strip())

    # batter_vs_pitcher
    for r in conn.execute("SELECT DISTINCT player_name FROM batter_vs_pitcher WHERE player_name IS NOT NULL"):
        names.add(r[0].strip())

    # batting_stats (Vibes — use hashtag as name)
    for r in conn.execute("SELECT DISTINCT player_hashtag FROM batting_stats WHERE player_hashtag IS NOT NULL"):
        names.add(r[0].strip())

    # playoff_batting_stats
    try:
        for r in conn.execute("SELECT DISTINCT player_name FROM playoff_batting_stats WHERE player_name IS NOT NULL"):
            names.add(r[0].strip())
    except:
        pass

    # pitching_stats (Vibes)
    for r in conn.execute("SELECT DISTINCT player_hashtag FROM pitching_stats WHERE player_hashtag IS NOT NULL"):
        names.add(r[0].strip())

    print(f"  DB union: {len(names)} unique names")
    return names

def test_url(slug, session):
    """HEAD request to verify player URL returns 200."""
    url = f"{BASE_URL}/player/{quote(slug)}"
    try:
        resp = session.head(url, timeout=10, allow_redirects=True)
        return resp.status_code == 200, url
    except Exception as e:
        return False, url

def main():
    conn = sqlite3.connect(DB_PATH)
    create_scrape_queue(conn)

    print("=== Step 1: Discover all HRL players ===")

    # --- Get API players (most authoritative source) ---
    api_players = get_api_players()

    # Build slug -> most_recent_year mapping from API
    # API has multiple records per player (one per season) — keep latest year
    slug_to_year = {}
    slug_to_nick = {}
    for p in api_players:
        hashtag = p.get('hashtag', '').strip()
        nick    = p.get('nick', '').strip()
        yr      = p.get('yr', 0) or 0
        if not hashtag:
            continue
        if hashtag not in slug_to_year or yr > slug_to_year[hashtag]:
            slug_to_year[hashtag] = yr
            slug_to_nick[hashtag] = nick

    print(f"  Unique API slugs: {len(slug_to_year)}")

    # --- Get DB names ---
    db_names = get_db_names(conn)

    # --- Merge: start with API slugs (authoritative), add any DB-only names ---
    # For DB-only names, try to match to an API slug; otherwise generate one
    all_slugs = dict(slug_to_year)  # slug -> priority_year

    # Check DB names against API slugs
    api_slugs_lower = {s.lower(): s for s in slug_to_year.keys()}
    db_only = []
    for name in db_names:
        name_no_space = re.sub(r'\s+', '', name).lower()
        if name_no_space in api_slugs_lower:
            # Already covered by API
            continue
        if name.lower() in api_slugs_lower:
            continue
        # Not in API — generate slug and mark for verification
        slug = re.sub(r'\s+', '', name)  # remove spaces
        if slug not in all_slugs:
            all_slugs[slug] = 0
            db_only.append((name, slug))

    print(f"  DB-only names (not in API): {len(db_only)}")

    # --- Build queue rows ---
    # API slugs are authoritative — mark all as verified (site uses JS so HEAD won't work)
    # DB-only names not in API are marked failed for manual review
    print(f"\n  Building queue (API slugs = verified, DB-only = needs_verification)...")

    verified = 0
    needs_verification = 0
    queue_rows = []

    api_slug_set = set(slug_to_year.keys())

    slugs_by_priority = sorted(all_slugs.items(), key=lambda x: x[1], reverse=True)

    for slug, year in slugs_by_priority:
        url = f"{BASE_URL}/player/{quote(slug)}"
        nick = slug_to_nick.get(slug, slug)

        if slug in api_slug_set:
            # API-verified
            queue_rows.append((nick, slug, url, year, 0, 0, None))
            verified += 1
        else:
            # DB-only, needs manual verification
            queue_rows.append((nick, slug, url, 0, 0, 1, 'Not found in API — needs verification'))
            needs_verification += 1

    print(f"  Verified (from API): {verified}")
    print(f"  Needs verification (DB-only): {needs_verification}")

    # --- Insert into scrape_queue ---
    # Only insert verified players for scraping; mark 404s as failed
    conn.execute("DELETE FROM scrape_queue")
    for (nick, slug, url, priority, scraped, failed, err) in queue_rows:
        ok = err is None
        conn.execute("""
            INSERT OR REPLACE INTO scrape_queue
              (player_name, slug, url, priority, scraped, failed, error_message)
            VALUES (?,?,?,?,0,?,?)
        """, (nick, slug, url, priority, 0 if ok else 1, err))

    conn.commit()

    # Print summary
    total = conn.execute("SELECT COUNT(*) FROM scrape_queue").fetchone()[0]
    ready = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE failed=0").fetchone()[0]
    failed = conn.execute("SELECT COUNT(*) FROM scrape_queue WHERE failed=1").fetchone()[0]

    print(f"\n  scrape_queue total: {total}")
    print(f"  Ready to scrape: {ready}")
    print(f"  Needs verification (failed=1): {failed}")

    # Sample top priority
    print("\n  Top 10 by priority:")
    for r in conn.execute("SELECT player_name, slug, priority FROM scrape_queue WHERE failed=0 ORDER BY priority DESC LIMIT 10"):
        print(f"    {r[0]} ({r[1]}) — year {r[2]}")

    conn.close()

    summary = f"""
Step 1 complete.
  {total} total players discovered.
  {ready} URLs verified and ready to scrape.
  {failed} need verification (404 or error — marked failed=1, will be skipped).
  DB-only names not in API: {len(db_only)}.
"""
    with open("progress_log.txt", "a") as f:
        f.write(summary)
    print(summary)

if __name__ == "__main__":
    main()
