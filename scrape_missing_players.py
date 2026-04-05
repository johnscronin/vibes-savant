#!/usr/bin/env python3
"""
Scrape the ~21 missing players using the same HRL API approach.
"""
import sqlite3, os, requests, time

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'
BASE_URL = "https://hrltwincities.com/api"
HEADERS = {
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
                return None
            else:
                time.sleep(2)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
    return None

def ensure_players_columns(conn):
    existing = [r[1] for r in conn.execute("PRAGMA table_info(players)").fetchall()]
    extras = [
        ("bats", "TEXT"), ("throws", "TEXT"), ("height", "TEXT"),
        ("weight", "TEXT"), ("age", "INTEGER"), ("status", "TEXT DEFAULT 'inactive'"),
        ("team_logo_url", "TEXT"),
    ]
    for col, typedef in extras:
        if col not in existing:
            conn.execute(f"ALTER TABLE players ADD COLUMN {col} {typedef}")
    conn.commit()

def ensure_stats_unique(conn):
    for table, cols in [
        ('batting_stats', 'player_hashtag, season, team_name'),
        ('pitching_stats', 'player_hashtag, season, team_name'),
        ('fielding_stats', 'player_hashtag, season, team_name'),
    ]:
        try:
            conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_player_season ON {table}({cols})")
        except Exception:
            pass
    conn.commit()

def scrape_player(conn, slug):
    meta_data = api_get(f"/players/{slug}")
    if not meta_data:
        return False, f"API /players/{slug} returned null"

    meta = meta_data.get('metadata', {})
    if not meta:
        return False, "No metadata in response"

    player_id = meta.get('playerId')
    if not player_id:
        return False, "No playerId in metadata"

    pic_url = meta.get('picUrl', '') or ''
    if pic_url and not pic_url.startswith('http'):
        pic_url = 'https://hrltwincities.com' + pic_url

    logo_url = meta.get('teamLogoUrl', '') or ''
    if logo_url and not logo_url.startswith('http'):
        logo_url = 'https://hrltwincities.com' + logo_url

    is_active = 1 if meta.get('isActive') else 0
    status = 'active' if is_active else 'inactive'

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
            except Exception as e:
                print(f"    bat row error: {e}")

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

    # Get all slugs that need to be scraped
    # 1. In player_master but not in players table, and either:
    #    - Duplicate display names (the "other" version we're missing)
    #    - Truly missing (BloominArse)

    to_scrape = conn.execute('''
        SELECT pm.slug, pm.display_name
        FROM player_master pm
        WHERE pm.slug NOT IN (SELECT hashtag FROM players)
        AND (
            pm.is_duplicate_name = 1
            OR pm.display_name NOT IN (SELECT nickname FROM players WHERE nickname IS NOT NULL)
        )
        ORDER BY pm.display_name
    ''').fetchall()

    print(f"Players to scrape: {len(to_scrape)}")
    for slug, name in to_scrape:
        print(f"  {slug}: \"{name}\"")

    print()
    scraped = 0
    failed = 0
    failures = []

    for slug, display_name in to_scrape:
        print(f"  Scraping {slug} ({display_name})...", end=" ", flush=True)
        try:
            ok, msg = scrape_player(conn, slug)
        except Exception as e:
            ok = False
            msg = str(e)

        if ok:
            conn.execute("UPDATE player_master SET scraped=1 WHERE slug=?", (slug,))
            conn.commit()
            scraped += 1
            print(f"OK ({msg})")
        else:
            conn.execute("UPDATE player_master SET scrape_failed=1, error_message=? WHERE slug=?",
                        (msg, slug))
            conn.commit()
            failed += 1
            failures.append((slug, msg))
            print(f"FAIL: {msg}")

        time.sleep(0.4)

    print(f"\n=== DONE ===")
    print(f"Scraped: {scraped}, Failed: {failed}")
    if failures:
        print("Failed:")
        for slug, msg in failures:
            print(f"  {slug}: {msg}")

    # Final counts
    total_players = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    total_bat = conn.execute("SELECT COUNT(*) FROM batting_stats").fetchone()[0]
    print(f"\nPlayers in DB: {total_players}")
    print(f"Batting stats rows: {total_bat}")

    conn.close()


if __name__ == "__main__":
    main()
