#!/usr/bin/env python3
"""
Fix missing stats for players where API returned null stats.
Uses Playwright to scrape the HRL HTML player page.
"""

import sqlite3, time, re, sys
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'

def scrape_player_html(slug, timeout_ms=25000):
    """Scrape stats from HTML page for a player. Returns HTML string or None."""
    url = f"https://hrltwincities.com/player/{slug}"
    print(f"  Loading: {url}", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(timeout_ms)
        try:
            page.goto(url, wait_until='domcontentloaded')
            page.wait_for_timeout(3000)
            content = page.content()
            # If no stats table found yet, wait longer
            if '<table' not in content or 'Season' not in content:
                page.wait_for_timeout(4000)
                content = page.content()
            browser.close()
            return content
        except Exception as e:
            print(f"  Playwright error: {e}", flush=True)
            try:
                browser.close()
            except:
                pass
            return None


def parse_batting_table(html, slug):
    """Parse batting stats table from HRL player page HTML. Returns list of dicts."""
    soup = BeautifulSoup(html, 'html.parser')

    # Find all tables
    tables = soup.find_all('table')

    batting_table = None
    for table in tables:
        headers = [th.get_text(strip=True).upper() for th in table.find_all('th')]
        if not headers:
            # Try thead
            thead = table.find('thead')
            if thead:
                headers = [th.get_text(strip=True).upper() for th in thead.find_all(['th', 'td'])]

        # Look for batting stats table signature
        header_str = ' '.join(headers)
        if 'SEASON' in headers and ('AVG' in headers or 'OPS' in headers) and ('AB' in headers or 'PA' in headers):
            batting_table = table
            break

    if not batting_table:
        # Try finding by class or structure
        for table in tables:
            text = table.get_text()
            if 'Season' in text and 'AVG' in text and ('AB' in text or 'PA' in text):
                batting_table = table
                break

    if not batting_table:
        return []

    # Get headers
    thead = batting_table.find('thead')
    if thead:
        header_row = thead.find('tr')
        if header_row:
            headers = [th.get_text(strip=True).upper() for th in header_row.find_all(['th', 'td'])]
        else:
            headers = [th.get_text(strip=True).upper() for th in thead.find_all(['th', 'td'])]
    else:
        first_row = batting_table.find('tr')
        headers = [th.get_text(strip=True).upper() for th in first_row.find_all(['th', 'td'])]

    print(f"  Found batting table headers: {headers}", flush=True)

    # Map headers to indices
    col_map = {}
    for i, h in enumerate(headers):
        col_map[h] = i

    # Parse rows
    tbody = batting_table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
    else:
        rows = batting_table.find_all('tr')[1:]  # skip header row

    results = []
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
        if len(cells) < 3:
            continue

        def get(col_name, default=None):
            idx = col_map.get(col_name)
            if idx is None:
                return default
            if idx >= len(cells):
                return default
            val = cells[idx].strip()
            if val == '-' or val == '' or val == 'N/A':
                return default
            return val

        def get_int(col_name, default=None):
            v = get(col_name)
            if v is None:
                return default
            try:
                return int(v)
            except:
                try:
                    return int(float(v))
                except:
                    return default

        def get_float(col_name, default=None):
            v = get(col_name)
            if v is None:
                return default
            try:
                return float(v)
            except:
                return default

        # Get season - must be a 4-digit year
        season_str = get('SEASON') or get('YR') or get('YEAR')
        if not season_str:
            continue
        try:
            season = int(season_str)
            if season < 2000 or season > 2030:
                continue
        except:
            continue

        team = get('TEAM') or get('TEAM NAME') or ''

        # Stats
        g  = get_int('G') or get_int('GAMES')
        pa = get_int('PA')
        ab = get_int('AB')
        r  = get_int('R')
        h  = get_int('H') or get_int('HITS')
        doubles  = get_int('2B') or get_int('DOUBLES')
        triples  = get_int('3B') or get_int('TRIPLES')
        hr  = get_int('HR')
        rbi = get_int('RBI')
        bb  = get_int('BB') or get_int('WALKS')
        sac = get_int('SAC')
        so  = get_int('SO') or get_int('K') or get_int('KS')
        roe = get_int('ROE')
        avg = get_float('AVG') or get_float('.AVG')
        obp = get_float('OBP')
        slg = get_float('SLG')
        ops = get_float('OPS')

        # Calculate derived stats
        if h is not None:
            d = doubles or 0
            t = triples or 0
            home_runs = hr or 0
            singles = h - d - t - home_runs
            if singles < 0:
                singles = 0
        else:
            singles = None

        total_bases = None
        if singles is not None and doubles is not None and triples is not None and hr is not None:
            total_bases = singles + (doubles * 2) + (triples * 3) + (hr * 4)

        xbh = None
        if doubles is not None and triples is not None and hr is not None:
            xbh = doubles + triples + hr

        hr_rate = None
        if hr is not None and ab and ab > 0:
            hr_rate = hr / ab

        k_rate = None
        if so is not None and pa and pa > 0:
            k_rate = so / pa

        rec = {
            'season': season,
            'team_name': team,
            'games': g,
            'pa': pa,
            'ab': ab,
            'r': r,
            'h': h,
            'singles': singles,
            'doubles': doubles,
            'triples': triples,
            'hr': hr,
            'rbi': rbi,
            'bb': bb,
            'sac': sac,
            'so': so,
            'roe': roe,
            'avg': avg,
            'obp': obp,
            'slg': slg,
            'ops': ops,
            'hr_rate': hr_rate,
            'k_rate': k_rate,
            'xbh': xbh,
            'total_bases': total_bases,
        }
        results.append(rec)

    return results


def parse_pitching_table(html, slug):
    """Parse pitching stats table from HRL player page. Returns list of dicts."""
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table')

    pitching_table = None
    for table in tables:
        thead = table.find('thead')
        if thead:
            headers = [th.get_text(strip=True).upper() for th in thead.find_all(['th', 'td'])]
        else:
            first_row = table.find('tr')
            if not first_row:
                continue
            headers = [th.get_text(strip=True).upper() for th in first_row.find_all(['th', 'td'])]

        # Pitching table has ERA, IP, W or L
        if 'ERA' in headers and 'IP' in headers and ('W' in headers or 'L' in headers):
            # Make sure it's not batting stats table
            if 'AVG' not in headers or 'ERA' in headers:
                pitching_table = table
                break

    if not pitching_table:
        return []

    # Get headers
    thead = pitching_table.find('thead')
    if thead:
        header_row = thead.find('tr')
        headers = [th.get_text(strip=True).upper() for th in header_row.find_all(['th', 'td'])]
    else:
        headers = [th.get_text(strip=True).upper() for th in pitching_table.find('tr').find_all(['th', 'td'])]

    print(f"  Found pitching table headers: {headers}", flush=True)

    col_map = {h: i for i, h in enumerate(headers)}

    tbody = pitching_table.find('tbody')
    rows = tbody.find_all('tr') if tbody else pitching_table.find_all('tr')[1:]

    results = []
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
        if len(cells) < 3:
            continue

        def get(col_name, default=None):
            idx = col_map.get(col_name)
            if idx is None:
                return default
            if idx >= len(cells):
                return default
            val = cells[idx].strip()
            if val == '-' or val == '' or val == 'N/A':
                return default
            return val

        def get_int(col_name, default=None):
            v = get(col_name)
            if v is None:
                return default
            try:
                return int(v)
            except:
                try:
                    return int(float(v))
                except:
                    return default

        def get_float(col_name, default=None):
            v = get(col_name)
            if v is None:
                return default
            try:
                return float(v)
            except:
                return default

        season_str = get('SEASON') or get('YR') or get('YEAR')
        if not season_str:
            continue
        try:
            season = int(season_str)
            if season < 2000 or season > 2030:
                continue
        except:
            continue

        team = get('TEAM') or ''

        rec = {
            'season': season,
            'team_name': team,
            'g': get_int('G') or get_int('GAMES'),
            'gs': get_int('GS'),
            'w': get_int('W') or get_int('WINS'),
            'l': get_int('L') or get_int('LOSSES'),
            'sv': get_int('SV') or get_int('SAVES'),
            'ip': get_float('IP'),
            'k': get_int('K') or get_int('SO'),
            'ha': get_int('H') or get_int('HA') or get_int('HITS'),
            'opp_bb': get_int('BB') or get_int('OPP BB'),
            'opp_hr': get_int('HR') or get_int('OPP HR'),
            'opp_r': get_int('R') or get_int('ER'),
            'era': get_float('ERA'),
            'whip': get_float('WHIP'),
            'k_per_6': get_float('K/6') or get_float('K6'),
            'baa': get_float('BAA') or get_float('OPP AVG'),
        }
        results.append(rec)

    return results


def ensure_pitching_stats_table(conn):
    """Make sure pitching_stats table exists."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS pitching_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        player_hashtag TEXT,
        season INTEGER,
        team_id INTEGER,
        team_name TEXT,
        team_hashtag TEXT,
        w INTEGER,
        l INTEGER,
        era REAL,
        g INTEGER,
        gs INTEGER,
        sv INTEGER,
        sho INTEGER,
        ip REAL,
        bf INTEGER,
        ha INTEGER,
        opp_r INTEGER,
        opp_hr INTEGER,
        k INTEGER,
        k_per_6 REAL,
        opp_bb INTEGER,
        opp_bb_per_6 REAL,
        baa REAL,
        whip REAL
    )""")
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_pitching_stats_player_season ON pitching_stats(player_hashtag, season, team_name)")
    except:
        pass
    conn.commit()


def process_player(conn, slug, player_id, nickname, debug_html=False):
    """Scrape and insert stats for one player. Returns (bat_rows, pit_rows)."""
    html = scrape_player_html(slug)
    if not html:
        print(f"  -> No HTML returned", flush=True)
        return 0, 0

    if debug_html:
        # Print a snippet for debugging
        # Find the first table
        idx = html.find('<table')
        if idx >= 0:
            print(f"  DEBUG: First 2000 chars of table: {html[idx:idx+2000]}", flush=True)
        else:
            print(f"  DEBUG: No <table> found. Body snippet: {html[html.find('<body'):html.find('<body')+3000]}", flush=True)

    # Parse batting
    bat_rows = parse_batting_table(html, slug)
    print(f"  -> Found {len(bat_rows)} batting season rows", flush=True)

    bat_inserted = 0
    for rec in bat_rows:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO batting_stats
                  (player_id, player_hashtag, season, team_name,
                   games, pa, ab, r, h, singles, doubles, triples, hr, rbi, bb, sac, so, roe,
                   avg, obp, slg, ops, hr_rate, k_rate, xbh, total_bases)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                player_id, slug,
                rec['season'], rec['team_name'],
                rec['games'], rec['pa'], rec['ab'], rec['r'], rec['h'],
                rec['singles'], rec['doubles'], rec['triples'], rec['hr'],
                rec['rbi'], rec['bb'], rec['sac'], rec['so'], rec['roe'],
                rec['avg'], rec['obp'], rec['slg'], rec['ops'],
                rec['hr_rate'], rec['k_rate'], rec['xbh'], rec['total_bases'],
            ))
            bat_inserted += 1
        except Exception as e:
            print(f"  DB error inserting batting row: {e}", flush=True)

    # Parse pitching
    pit_rows = parse_pitching_table(html, slug)
    print(f"  -> Found {len(pit_rows)} pitching season rows", flush=True)

    pit_inserted = 0
    ensure_pitching_stats_table(conn)
    for rec in pit_rows:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO pitching_stats
                  (player_id, player_hashtag, season, team_name,
                   g, gs, w, l, sv, ip, k, ha, opp_bb, opp_hr, opp_r,
                   era, whip, k_per_6, baa)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                player_id, slug,
                rec['season'], rec['team_name'],
                rec['g'], rec['gs'], rec['w'], rec['l'], rec['sv'],
                rec['ip'], rec['k'], rec['ha'], rec['opp_bb'], rec['opp_hr'], rec['opp_r'],
                rec['era'], rec['whip'], rec['k_per_6'], rec['baa'],
            ))
            pit_inserted += 1
        except Exception as e:
            print(f"  DB error inserting pitching row: {e}", flush=True)

    conn.commit()
    return bat_inserted, pit_inserted


def main():
    conn = sqlite3.connect(DB_PATH)

    # Get Problem 1 players: scraped=1 but no batting_stats
    problem1 = conn.execute("""
        SELECT p.hashtag, p.player_id, p.nickname, p.team_name, p.last_year
        FROM players p
        WHERE p.hashtag NOT IN (SELECT DISTINCT player_hashtag FROM batting_stats)
        ORDER BY p.last_year DESC, p.hashtag
    """).fetchall()

    print(f"=== FIX PROBLEM 1: {len(problem1)} players with no batting_stats ===")

    total_bat = 0
    total_pit = 0
    failed_players = []

    for i, (slug, player_id, nickname, team, last_year) in enumerate(problem1):
        print(f"\n[{i+1}/{len(problem1)}] {slug} (id={player_id}, {nickname})", flush=True)

        # Debug HTML on first player
        debug = (i == 0)

        bat, pit = process_player(conn, slug, player_id, nickname, debug_html=debug)

        if bat == 0:
            failed_players.append(slug)
            print(f"  WARNING: No batting rows inserted for {slug}", flush=True)

        total_bat += bat
        total_pit += pit
        print(f"  Inserted: {bat} batting, {pit} pitching rows", flush=True)

        if (i + 1) % 5 == 0:
            print(f"\n--- Progress: {i+1}/{len(problem1)} players processed ---", flush=True)

        time.sleep(1.0)

    print(f"\n=== PROBLEM 1 COMPLETE ===")
    print(f"Total batting rows inserted: {total_bat}")
    print(f"Total pitching rows inserted: {total_pit}")
    print(f"Players with no batting data: {failed_players}")

    # Verify
    now_have_stats = conn.execute("""
        SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats
        WHERE player_hashtag IN ({})
    """.format(','.join('?' * len(problem1))), [r[0] for r in problem1]).fetchone()[0]

    print(f"\nPlayers from Problem 1 that NOW have batting_stats: {now_have_stats}/{len(problem1)}")

    conn.close()


if __name__ == "__main__":
    main()
