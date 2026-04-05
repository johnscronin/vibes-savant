#!/usr/bin/env python3
"""
Vibes Savant — Vibes-only Playoff stats scraper.

The HRL site stores playoff BvP data in aggregate (Career view only, no per-year).
This scraper:
  1. Navigates to each Vibes player page
  2. Clicks "Batter vs. Pitcher" then the "Playoffs" sub-tab
  3. Scrapes the Career view (default) pitcher matchup rows
  4. Aggregates them into career playoff batting totals
  5. Stores in playoff_batting_stats with season=0 (represents Career)

Does NOT drop any existing tables.
"""

import sqlite3, os
from playwright.sync_api import sync_playwright

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com"

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]


def si(s):
    try: return int(str(s).replace(',', '').strip())
    except: return None


def sf(s):
    try: return float(str(s).replace(',', '').strip())
    except: return None


def ensure_tables(conn):
    # playoff_batting_stats already exists from scrape_bvp.py schema
    # season=0 is used to represent career playoff totals
    conn.execute("""
        CREATE TABLE IF NOT EXISTS playoff_batting_stats (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            season      INTEGER NOT NULL,
            g INTEGER, ab INTEGER, r INTEGER, h INTEGER,
            doubles INTEGER, triples INTEGER, hr INTEGER, rbi INTEGER,
            bb INTEGER, so INTEGER,
            avg REAL, obp REAL, slg REAL, ops REAL,
            UNIQUE(player_name, season)
        )
    """)
    conn.commit()


def parse_bvp_rows(page):
    """Find BvP matchup rows starting with 'vs. ' — same column layout as regular BvP.
    Columns (after pitcher name): G AB R H 2B 3B HR RBI BB SAC SO ROE HRR SOR AVG OBP SLG OPS
    Returns list of row dicts.
    """
    for t in reversed(page.query_selector_all('table')):
        rows = t.query_selector_all('tr')
        vs_count = sum(1 for tr in rows if tr.inner_text().strip().startswith('vs. '))
        if vs_count < 1:
            continue
        result = []
        for tr in rows:
            cells = [td.inner_text().strip() for td in tr.query_selector_all('td')]
            if not cells or not cells[0].startswith('vs. '):
                continue
            v = cells[1:]
            if len(v) < 17:
                continue
            result.append({
                'g':       si(v[0]),
                'ab':      si(v[1]),
                'r':       si(v[2]),
                'h':       si(v[3]),
                'doubles': si(v[4]),
                'triples': si(v[5]),
                'hr':      si(v[6]),
                'rbi':     si(v[7]),
                'bb':      si(v[8]),
                'sac':     si(v[9]),
                'so':      si(v[10]),
                'roe':     si(v[11]),
            })
        return result
    return []


def scrape_player_playoffs(page, player, conn):
    print(f"\nScraping playoffs for {player}...")
    page.goto(f"{BASE_URL}/player/{player}", wait_until="domcontentloaded")
    page.wait_for_timeout(5000)

    # Click "Batter vs. Pitcher" main tab
    try:
        page.get_by_text("Batter vs. Pitcher", exact=False).first.click()
        page.wait_for_timeout(2500)
    except Exception as e:
        print(f"  !! BvP tab error: {e}")
        return 0

    # Click the "Playoffs" sub-tab within BvP
    try:
        page.locator('button:has-text("Playoffs")').first.click()
        page.wait_for_timeout(3000)
    except Exception as e:
        print(f"  !! Playoffs sub-tab error: {e}")
        return 0

    # The Career view is the default — parse pitcher matchup rows
    rows = parse_bvp_rows(page)
    print(f"  BvP Career Playoff matchup rows: {len(rows)}")

    if not rows:
        print(f"  No playoff data for {player}")
        return 0

    # Aggregate career totals
    ta  = sum(r['ab']      or 0 for r in rows)
    if ta == 0:
        print(f"  Zero AB for {player}, skipping")
        return 0

    tg   = sum(r['g']       or 0 for r in rows)
    tr   = sum(r['r']       or 0 for r in rows)
    th   = sum(r['h']       or 0 for r in rows)
    t2b  = sum(r['doubles'] or 0 for r in rows)
    t3b  = sum(r['triples'] or 0 for r in rows)
    thr  = sum(r['hr']      or 0 for r in rows)
    trbi = sum(r['rbi']     or 0 for r in rows)
    tbb  = sum(r['bb']      or 0 for r in rows)
    tso  = sum(r['so']      or 0 for r in rows)

    pa  = ta + tbb
    avg = round(th / ta,  3) if ta  else None
    obp = round((th + tbb) / pa, 3) if pa else None
    tb_val = (th - t2b - t3b - thr) + t2b*2 + t3b*3 + thr*4
    slg = round(tb_val / ta, 3) if ta else None
    ops = round((obp or 0) + (slg or 0), 3) if (obp and slg) else None

    conn.execute("""
        INSERT OR REPLACE INTO playoff_batting_stats
        (player_name, season, g, ab, r, h, doubles, triples, hr, rbi,
         bb, so, avg, obp, slg, ops)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (player, 0, tg, ta, tr, th, t2b, t3b, thr, trbi, tbb, tso,
          avg, obp, slg, ops))
    conn.commit()

    avg_fmt = f".{int((avg or 0)*1000):03d}"
    print(f"  Stored career: {tg}G {ta}AB {thr}HR {avg_fmt}AVG {obp:.3f}OBP {slg:.3f}SLG")
    return 1


def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(0)  # no timeout

        total = 0
        for player in VIBES_PLAYERS:
            try:
                total += scrape_player_playoffs(page, player, conn)
            except Exception as e:
                print(f"!! ERROR {player}: {e}")
                conn.commit()

        browser.close()

    print(f"\n=== DONE ===")
    print(f"Players with career playoff stats stored: {total}")

    print("\nPlayoff career batting stats:")
    for row in conn.execute(
        "SELECT player_name, ab, hr, avg, obp, slg, ops FROM playoff_batting_stats "
        "WHERE season=0 ORDER BY player_name"
    ).fetchall():
        print(f"  {row[0]:12s}: {row[1]}AB {row[2]}HR "
              f".{int((row[3] or 0)*1000):03d}AVG "
              f"{row[4]:.3f}OBP {row[5]:.3f}SLG {row[6]:.3f}OPS")

    conn.close()


if __name__ == '__main__':
    run()
