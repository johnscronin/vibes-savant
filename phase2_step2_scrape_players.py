#!/usr/bin/env python3
"""
Phase 2 Step 2 — Scrape all HRL player data via API.
For each player in scrape_queue, fetches metadata + batting/pitching/fielding stats.
Uses INSERT OR REPLACE to be idempotent (safe to re-run after interruption).
"""

import sqlite3, os, requests, time, warnings
from datetime import datetime

warnings.filterwarnings('ignore')

DB_PATH  = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com/api"
HEADERS  = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://hrltwincities.com/'
}

def api_get(path, retries=3):
    url = f"{BASE_URL}{path}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                text = r.text.strip()
                if text:
                    return r.json()
                return {}
            elif r.status_code in (404, 400):
                return None
            elif r.status_code == 500:
                return None   # treat as "no data" not a retryable error
            else:
                time.sleep(2)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
    return None

def ensure_players_columns(conn):
    """Add extra columns to players table if missing."""
    existing = [r[1] for r in conn.execute("PRAGMA table_info(players)").fetchall()]
    extras = [
        ("bats",        "TEXT"),
        ("throws",      "TEXT"),
        ("height",      "TEXT"),
        ("weight",      "TEXT"),
        ("age",         "INTEGER"),
        ("status",      "TEXT DEFAULT 'inactive'"),
    ]
    for col, typedef in extras:
        if col not in existing:
            conn.execute(f"ALTER TABLE players ADD COLUMN {col} {typedef}")
    conn.commit()

def ensure_stats_unique(conn):
    """Ensure batting/pitching/fielding have unique constraints for INSERT OR REPLACE."""
    # We use player_hashtag + season as the natural key
    # Check if unique index exists; if not, add it (carefully)
    for table, cols in [
        ('batting_stats',  'player_hashtag, season, team_name'),
        ('pitching_stats', 'player_hashtag, season, team_name'),
        ('fielding_stats', 'player_hashtag, season, team_name'),
    ]:
        try:
            conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_player_season ON {table}({cols})")
        except Exception as e:
            pass  # index may already exist or column may not exist
    conn.commit()

def scrape_player(conn, slug, queue_id):
    """Scrape a single player. Returns (success, error_msg)."""
    # 1. Get metadata
    meta_data = api_get(f"/players/{slug}")
    if not meta_data:
        return False, f"API /players/{slug} returned null"

    meta = meta_data.get('metadata', {})
    if not meta:
        return False, "No metadata in response"

    player_id = meta.get('playerId')
    if not player_id:
        return False, "No playerId in metadata"

    # Fix photo/logo URLs
    pic_url = meta.get('picUrl', '') or ''
    if pic_url and not pic_url.startswith('http'):
        pic_url = 'https://hrltwincities.com' + pic_url

    logo_url = meta.get('teamLogoUrl', '') or ''
    if logo_url and not logo_url.startswith('http'):
        logo_url = 'https://hrltwincities.com' + logo_url

    is_active = 1 if meta.get('isActive') else 0
    status = 'active' if is_active else 'inactive'

    # 2. Insert/update players table
    conn.execute("""
        INSERT OR REPLACE INTO players
          (hashtag, nickname, player_id, team_id, team_name, is_active, last_year,
           pic_url, team_logo_url, bats, throws, height, weight, age, status)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        slug,
        meta.get('nickname', slug),
        player_id,
        meta.get('teamId'),
        meta.get('teamName'),
        is_active,
        meta.get('lastYear'),
        pic_url,
        logo_url,
        meta.get('bats'),
        meta.get('throws'),
        meta.get('height'),
        meta.get('weight'),
        meta.get('age'),
        status,
    ))

    # 3. Batting stats
    bat_data = api_get(f"/players/{player_id}/stats/hitting/career")
    bat_rows = 0
    if bat_data and bat_data.get('stats'):
        for s in bat_data['stats']:
            if not s.get('season'):
                continue
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO batting_stats
                      (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                       games, pa, ab, r, h, singles, doubles, triples, hr, rbi, bb, sac, so, roe,
                       avg, obp, slg, ops, hr_rate, k_rate, xbh, total_bases)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    player_id, slug,
                    s.get('season'), s.get('tmId'), s.get('tmShrtNm'), s.get('teamHashtag'),
                    s.get('gBat'), s.get('pa'), s.get('ab'), s.get('r'), s.get('h'),
                    s.get('singles'), s.get('doubles'), s.get('triples'), s.get('hr'),
                    s.get('rbi'), s.get('bb'), s.get('sac'), s.get('so'), s.get('roe'),
                    s.get('avg'), s.get('obp'), s.get('slg'), s.get('ops'),
                    s.get('hRr'), s.get('kr'), s.get('xbh'), s.get('totalBases'),
                ))
                bat_rows += 1
            except Exception:
                pass

    # 4. Pitching stats
    pit_data = api_get(f"/players/{player_id}/stats/pitching/career")
    pit_rows = 0
    if pit_data and pit_data.get('stats'):
        for s in pit_data['stats']:
            if not s.get('season'):
                continue
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO pitching_stats
                      (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                       w, l, era, g, gs, sv, sho, ip, bf, ha, opp_r, opp_hr,
                       k, k_per_6, opp_bb, opp_bb_per_6, baa, whip)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    player_id, slug,
                    s.get('season'), s.get('tmId'), s.get('tmShrtNm'), s.get('teamHashtag'),
                    s.get('w'), s.get('l'), s.get('era'), s.get('gPit'), s.get('gsPit'),
                    s.get('sv'), s.get('sho'), s.get('ip'), s.get('bf'), s.get('ha'),
                    s.get('oppR'), s.get('oppHR'), s.get('k'), s.get('k6'),
                    s.get('oppBB'), s.get('oppBB6'), s.get('baa'), s.get('whip'),
                ))
                pit_rows += 1
            except Exception:
                pass

    # 5. Fielding stats
    fld_data = api_get(f"/players/{player_id}/stats/fielding/career")
    fld_rows = 0
    if fld_data and fld_data.get('stats'):
        for s in fld_data['stats']:
            if not s.get('season'):
                continue
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO fielding_stats
                      (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                       chances, put_outs, errors, fld_pct)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    player_id, slug,
                    s.get('season'), s.get('tmId'), s.get('tmNm'), s.get('teamHashtag'),
                    s.get('chncs'), s.get('po'), s.get('e'), s.get('fldPct'),
                ))
                fld_rows += 1
            except Exception:
                pass

    conn.commit()
    return True, f"bat={bat_rows} pit={pit_rows} fld={fld_rows}"


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_players_columns(conn)
    ensure_stats_unique(conn)

    # Get queue
    queue = conn.execute("""
        SELECT id, player_name, slug, priority
        FROM scrape_queue
        WHERE scraped=0 AND failed=0
        ORDER BY priority DESC, player_name
    """).fetchall()

    total    = len(queue)
    scraped  = 0
    failed   = 0
    failures = []
    start_t  = time.time()

    print(f"=== Step 2: Scraping {total} players ===")

    for i, (qid, name, slug, priority) in enumerate(queue):
        # Progress print every 25
        if i > 0 and i % 25 == 0:
            elapsed   = time.time() - start_t
            rate      = i / elapsed
            remaining = (total - i) / rate if rate > 0 else 0
            hrs       = int(remaining // 3600)
            mins      = int((remaining % 3600) // 60)
            print(f"  [{i}/{total}] {scraped} scraped, {failed} failed. ETA: {hrs}h {mins}m")

        print(f"  [{i+1}/{total}] {name} ({slug})...", end=" ", flush=True)

        try:
            ok, msg = scrape_player(conn, slug, qid)
        except Exception as e:
            ok  = False
            msg = str(e)

        if ok:
            conn.execute("UPDATE scrape_queue SET scraped=1 WHERE id=?", (qid,))
            conn.commit()
            scraped += 1
            print(f"OK ({msg})")
        else:
            conn.execute("UPDATE scrape_queue SET failed=1, error_message=? WHERE id=?", (msg, qid))
            conn.commit()
            failed += 1
            failures.append((name, msg))
            print(f"FAIL: {msg}")

        # Every 50 players, append checkpoint to log
        if (i + 1) % 50 == 0:
            elapsed   = time.time() - start_t
            rate      = (i+1) / elapsed
            remaining = (total - i - 1) / rate if rate > 0 else 0
            hrs       = int(remaining // 3600)
            mins      = int((remaining % 3600) // 60)
            ts = datetime.now().strftime('%Y-%m-%d %H:%M')
            with open("progress_log.txt", "a") as f:
                f.write(f"Step 2 checkpoint [{ts}]: {i+1}/{total} processed, {scraped} scraped, {failed} failed. ETA: {hrs}h {mins}m\n")

        time.sleep(0.4)  # polite rate limiting

    # Final DB counts
    bat_total = conn.execute("SELECT COUNT(*) FROM batting_stats").fetchone()[0]
    pit_total = conn.execute("SELECT COUNT(*) FROM pitching_stats").fetchone()[0]
    fld_total = conn.execute("SELECT COUNT(*) FROM fielding_stats").fetchone()[0]
    ply_total = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]

    print(f"\n=== STEP 2 COMPLETE ===")
    print(f"  Players scraped: {scraped}")
    print(f"  Failed: {failed}")
    print(f"  Players table: {ply_total} rows")
    print(f"  Batting stats: {bat_total} rows")
    print(f"  Pitching stats: {pit_total} rows")
    print(f"  Fielding stats: {fld_total} rows")
    if failures:
        print(f"\n  Failed players:")
        for name, msg in failures[:20]:
            print(f"    {name}: {msg}")

    fail_names = ', '.join(n for n, _ in failures[:50])
    summary = f"""
Step 2 complete.
  {scraped} players scraped successfully. {failed} failed.
  Players table: {ply_total} rows.
  Batting stats: {bat_total} rows (player-seasons).
  Pitching stats: {pit_total} rows.
  Fielding stats: {fld_total} rows.
  Failed players ({failed}): {fail_names}
"""
    with open("progress_log.txt", "a") as f:
        f.write(summary)
    print(summary)
    conn.close()

if __name__ == "__main__":
    main()
