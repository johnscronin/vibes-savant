#!/usr/bin/env python3
"""
Vibes Savant — Batter vs. Pitcher + Playoff stats scraper.
Columns on BvP table rows: G AB R H 2B 3B HR RBI BB SAC SO ROE HRR SOR AVG OBP SLG OPS
Header row is in a SEPARATE table — data table only has "vs. XXX" rows.
"""

import sqlite3, os
from playwright.sync_api import sync_playwright

DB_PATH  = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com"

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]


def create_tables(conn):
    conn.execute("DROP TABLE IF EXISTS batter_vs_pitcher")
    conn.execute("""
        CREATE TABLE batter_vs_pitcher (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name      TEXT NOT NULL,
            season           TEXT NOT NULL,
            opposing_pitcher TEXT NOT NULL,
            g   INTEGER, ab  INTEGER, r   INTEGER, h   INTEGER,
            doubles INTEGER, triples INTEGER, hr INTEGER, rbi INTEGER,
            bb  INTEGER, sac INTEGER, so  INTEGER, roe INTEGER,
            hrr REAL, sor REAL,
            avg REAL, obp REAL, slg REAL, ops REAL,
            tab_type TEXT NOT NULL DEFAULT 'regular',
            UNIQUE(player_name, season, opposing_pitcher, tab_type)
        )
    """)
    conn.execute("DROP TABLE IF EXISTS playoff_batting_stats")
    conn.execute("""
        CREATE TABLE playoff_batting_stats (
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


def si(s):
    try: return int(str(s).replace(',','').strip())
    except: return None


def sf(s):
    try: return float(str(s).replace(',','').strip())
    except: return None


def get_bvp_data_table(page):
    """Find the table whose rows start with 'vs. ' (data table, not header table)."""
    for t in reversed(page.query_selector_all('table')):
        rows = t.query_selector_all('tr')
        vs_count = sum(1 for tr in rows if tr.inner_text().strip().startswith('vs. '))
        if vs_count >= 1:
            return t, rows
    return None, []


def parse_rows(rows):
    """Parse tr elements into row dicts. Columns: G AB R H 2B 3B HR RBI BB SAC SO ROE HRR SOR AVG OBP SLG OPS"""
    result = []
    for tr in rows:
        cells = [td.inner_text().strip() for td in tr.query_selector_all('td')]
        if not cells or not cells[0].startswith('vs. '):
            continue
        pitcher = cells[0][4:].strip()
        v = cells[1:]
        if len(v) < 17:
            continue
        row = {
            'opposing_pitcher': pitcher,
            'g':  si(v[0]),  'ab': si(v[1]),  'r':  si(v[2]),  'h':  si(v[3]),
            'doubles': si(v[4]),  'triples': si(v[5]),
            'hr': si(v[6]),  'rbi': si(v[7]), 'bb': si(v[8]),
            'sac': si(v[9]), 'so': si(v[10]), 'roe': si(v[11]),
            'hrr': sf(v[12]), 'sor': sf(v[13]),
            'avg': sf(v[14]), 'obp': sf(v[15]),
            'slg': sf(v[16]), 'ops': sf(v[17]) if len(v) > 17 else None,
        }
        result.append(row)
    return result


def select_year(page, year_str):
    """Open the season dropdown and click the given year. Returns True on success."""
    dd = page.query_selector('.k-dropdownlist')
    if not dd:
        return False
    dd.click()
    page.wait_for_timeout(900)
    for item in page.locator('.k-list-item').all():
        if item.inner_text().strip() == year_str:
            item.click()
            page.wait_for_timeout(2200)
            return True
    page.keyboard.press('Escape')
    page.wait_for_timeout(300)
    return False


def scrape_player(page, player, conn):
    print(f"\n  === {player} ===")
    try:
        page.goto(f"{BASE_URL}/player/{player}", wait_until="domcontentloaded")
        page.wait_for_timeout(5000)
    except Exception as e:
        print(f"  !! page load error: {e}")
        return

    # ── Regular season BvP ──────────────────────────────────
    try:
        page.get_by_text("Batter vs. Pitcher", exact=False).first.click()
        page.wait_for_timeout(2500)
    except Exception as e:
        print(f"  !! BvP tab error: {e}")
        return

    # Read dropdown options
    dd = page.query_selector('.k-dropdownlist')
    if not dd:
        print("  !! No dropdown")
        return
    dd.click()
    page.wait_for_timeout(800)
    seasons = [it.inner_text().strip() for it in page.locator('.k-list-item').all()
               if it.inner_text().strip()]
    page.keyboard.press('Escape')
    page.wait_for_timeout(300)
    print(f"  Seasons: {seasons}")

    regular_total = 0
    for season_label in seasons:
        if not select_year(page, season_label):
            continue
        table, rows = get_bvp_data_table(page)
        if not rows:
            continue
        parsed = parse_rows(rows)
        if not parsed:
            continue
        for row in parsed:
            conn.execute("""
                INSERT OR REPLACE INTO batter_vs_pitcher
                (player_name, season, opposing_pitcher, g, ab, r, h, doubles, triples,
                 hr, rbi, bb, sac, so, roe, hrr, sor, avg, obp, slg, ops, tab_type)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'regular')
            """, (player, season_label, row['opposing_pitcher'],
                  row['g'], row['ab'], row['r'], row['h'],
                  row['doubles'], row['triples'], row['hr'], row['rbi'],
                  row['bb'], row['sac'], row['so'], row['roe'],
                  row['hrr'], row['sor'], row['avg'], row['obp'], row['slg'], row['ops']))
        regular_total += len(parsed)
        print(f"    {season_label}: {len(parsed)} matchups")

    conn.commit()
    print(f"  Regular total: {regular_total}")

    # ── Playoffs BvP ────────────────────────────────────────
    try:
        page.get_by_text("Playoffs", exact=True).first.click()
        page.wait_for_timeout(2500)
    except Exception as e:
        print(f"  !! Playoffs tab error: {e}")
        return

    playoff_total = 0
    for season_label in [s for s in seasons if s.isdigit()]:
        if not select_year(page, season_label):
            continue
        table, rows = get_bvp_data_table(page)
        if not rows:
            continue
        parsed = parse_rows(rows)
        if not parsed:
            continue

        # Store per-pitcher playoff rows
        for row in parsed:
            conn.execute("""
                INSERT OR REPLACE INTO batter_vs_pitcher
                (player_name, season, opposing_pitcher, g, ab, r, h, doubles, triples,
                 hr, rbi, bb, sac, so, roe, hrr, sor, avg, obp, slg, ops, tab_type)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'playoff')
            """, (player, season_label, row['opposing_pitcher'],
                  row['g'], row['ab'], row['r'], row['h'],
                  row['doubles'], row['triples'], row['hr'], row['rbi'],
                  row['bb'], row['sac'], row['so'], row['roe'],
                  row['hrr'], row['sor'], row['avg'], row['obp'], row['slg'], row['ops']))

        # Aggregate season totals
        ta = sum(r['ab'] or 0 for r in parsed)
        if ta == 0:
            continue
        tg  = sum(r['g']  or 0 for r in parsed)
        tr  = sum(r['r']  or 0 for r in parsed)
        th  = sum(r['h']  or 0 for r in parsed)
        t2b = sum(r['doubles']  or 0 for r in parsed)
        t3b = sum(r['triples']  or 0 for r in parsed)
        thr = sum(r['hr'] or 0 for r in parsed)
        trbi= sum(r['rbi'] or 0 for r in parsed)
        tbb = sum(r['bb'] or 0 for r in parsed)
        tso = sum(r['so'] or 0 for r in parsed)
        avg = round(th / ta, 3) if ta else None
        pa  = ta + tbb
        obp = round((th + tbb) / pa, 3) if pa else None
        tb_val = th - t2b - t3b - thr + t2b*2 + t3b*3 + thr*4
        slg = round(tb_val / ta, 3) if ta else None
        ops = round((obp or 0) + (slg or 0), 3) if (obp and slg) else None
        conn.execute("""
            INSERT OR REPLACE INTO playoff_batting_stats
            (player_name, season, g, ab, r, h, doubles, triples, hr, rbi,
             bb, so, avg, obp, slg, ops)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (player, int(season_label), tg, ta, tr, th,
              t2b, t3b, thr, trbi, tbb, tso, avg, obp, slg, ops))
        playoff_total += 1
        print(f"    [PO] {season_label}: {len(parsed)} matchups, {ta} AB")

    conn.commit()
    print(f"  Playoff seasons stored: {playoff_total}")


def run():
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(0)  # no timeout — scrapes can take hours

        for player in VIBES_PLAYERS:
            try:
                scrape_player(page, player, conn)
            except Exception as e:
                print(f"!! ERROR {player}: {e}")
                conn.commit()

        browser.close()

    print("\n=== FINAL SUMMARY ===")
    for row in conn.execute("""
        SELECT player_name,
               SUM(CASE WHEN tab_type='regular' AND season!='Career' THEN 1 ELSE 0 END) reg_matchups,
               SUM(CASE WHEN tab_type='playoff' THEN 1 ELSE 0 END) po_matchups
        FROM batter_vs_pitcher GROUP BY player_name ORDER BY player_name
    """).fetchall():
        print(f"  {row[0]:12s}: {row[1]:3d} regular matchup rows, {row[2]:3d} playoff matchup rows")

    print("\n  Playoff batting seasons:")
    for row in conn.execute("SELECT player_name, season, ab, hr, avg FROM playoff_batting_stats ORDER BY player_name, season").fetchall():
        print(f"    {row[0]:12s} {row[1]}: AB={row[2]} HR={row[3]} AVG={row[4]}")

    conn.close()


if __name__ == '__main__':
    run()
