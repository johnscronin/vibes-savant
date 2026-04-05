#!/usr/bin/env python3
"""
Vibes Savant — League-wide Batter vs. Pitcher scraper.
Scrapes BvP data for ALL qualified HRL players (not just Vibes).
Combines regular season + playoff PA into a 'combined' tab_type row.
Prints progress every 10 players. Retries failures up to 3 times.
"""

import sqlite3, os, time
from urllib.parse import quote
from playwright.sync_api import sync_playwright

DB_PATH  = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL = "https://hrltwincities.com"

# Vibes players already scraped (skip re-scraping)
VIBES_PLAYERS = {
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
}


def ensure_source_column(conn):
    """Add source column to batter_vs_pitcher if not exists."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(batter_vs_pitcher)").fetchall()]
    if 'source' not in cols:
        conn.execute("ALTER TABLE batter_vs_pitcher ADD COLUMN source TEXT DEFAULT 'regular'")
        # Back-fill existing Vibes rows
        conn.execute("UPDATE batter_vs_pitcher SET source='regular' WHERE tab_type='regular' AND source IS NULL")
        conn.execute("UPDATE batter_vs_pitcher SET source='playoff' WHERE tab_type='playoff' AND source IS NULL")
        conn.commit()
        print("  Added 'source' column to batter_vs_pitcher")


def get_all_league_players(conn):
    """Get all distinct player names from league_batting_stats, excluding Vibes."""
    rows = conn.execute(
        "SELECT DISTINCT player_name FROM league_batting_stats ORDER BY player_name"
    ).fetchall()
    return [r[0] for r in rows if r[0] not in VIBES_PLAYERS]


def si(s):
    try: return int(str(s).replace(',','').strip())
    except: return None

def sf(s):
    try: return float(str(s).replace(',','').strip())
    except: return None


def get_bvp_data_table(page):
    for t in reversed(page.query_selector_all('table')):
        rows = t.query_selector_all('tr')
        vs_count = sum(1 for tr in rows if tr.inner_text().strip().startswith('vs. '))
        if vs_count >= 1:
            return t, rows
    return None, []


def parse_rows(rows):
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
    player_url = f"{BASE_URL}/player/{quote(player)}"
    try:
        page.goto(player_url, wait_until="domcontentloaded")
        page.wait_for_timeout(4000)
    except Exception as e:
        return 0, 0, f"page load error: {e}"

    # Click BvP tab
    try:
        page.get_by_text("Batter vs. Pitcher", exact=False).first.click()
        page.wait_for_timeout(2500)
    except Exception as e:
        return 0, 0, f"BvP tab error: {e}"

    # Read dropdown
    dd = page.query_selector('.k-dropdownlist')
    if not dd:
        return 0, 0, "no dropdown"
    dd.click()
    page.wait_for_timeout(800)
    seasons = [it.inner_text().strip() for it in page.locator('.k-list-item').all()
               if it.inner_text().strip()]
    page.keyboard.press('Escape')
    page.wait_for_timeout(300)

    if not seasons:
        return 0, 0, "no seasons"

    # Scrape regular season BvP per year
    reg_rows = {}   # season_str -> list of rows
    for season_label in seasons:
        if not select_year(page, season_label):
            continue
        _, rows = get_bvp_data_table(page)
        parsed = parse_rows(rows)
        if parsed:
            reg_rows[season_label] = parsed

    # Scrape playoffs BvP per year
    po_rows = {}
    try:
        page.get_by_text("Playoffs", exact=True).first.click()
        page.wait_for_timeout(2500)
        for season_label in [s for s in seasons if s.isdigit()]:
            if not select_year(page, season_label):
                continue
            _, rows = get_bvp_data_table(page)
            parsed = parse_rows(rows)
            if parsed:
                po_rows[season_label] = parsed
    except Exception:
        pass  # No playoffs tab or error — that's fine

    # Merge regular + playoff by (pitcher, season) and store as 'combined'
    all_seasons = set(reg_rows.keys()) | set(po_rows.keys())
    total_stored = 0
    for season_label in all_seasons:
        # Build pitcher→stats dict merging reg + playoff
        merged = {}  # pitcher_name -> combined row
        for row in reg_rows.get(season_label, []):
            p = row['opposing_pitcher']
            merged[p] = dict(row)
        for row in po_rows.get(season_label, []):
            p = row['opposing_pitcher']
            if p in merged:
                # Add playoff stats to regular
                for k in ('g', 'ab', 'r', 'h', 'doubles', 'triples', 'hr', 'rbi', 'bb', 'sac', 'so', 'roe'):
                    merged[p][k] = (merged[p].get(k) or 0) + (row.get(k) or 0)
                # Recompute rate stats
                ab = merged[p].get('ab') or 0
                h  = merged[p].get('h') or 0
                bb = merged[p].get('bb') or 0
                hr = merged[p].get('hr') or 0
                so = merged[p].get('so') or 0
                if ab:
                    merged[p]['avg'] = round(h / ab, 3)
                    merged[p]['slg'] = round((h - merged[p].get('doubles',0) - merged[p].get('triples',0) - hr
                                              + merged[p].get('doubles',0)*2 + merged[p].get('triples',0)*3 + hr*4) / ab, 3)
                pa = ab + bb
                if pa:
                    merged[p]['obp'] = round((h + bb) / pa, 3)
                    merged[p]['ops'] = round(merged[p].get('obp', 0) + merged[p].get('slg', 0), 3)
                merged[p]['hrr'] = round(hr / ab * 100, 2) if ab else None
                merged[p]['sor'] = round(so / ab * 100, 2) if ab else None
            else:
                merged[p] = dict(row)

        for row in merged.values():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO batter_vs_pitcher
                    (player_name, season, opposing_pitcher, g, ab, r, h, doubles, triples,
                     hr, rbi, bb, sac, so, roe, hrr, sor, avg, obp, slg, ops, tab_type, source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'combined','combined')
                """, (player, season_label, row['opposing_pitcher'],
                      row.get('g'), row.get('ab'), row.get('r'), row.get('h'),
                      row.get('doubles'), row.get('triples'), row.get('hr'), row.get('rbi'),
                      row.get('bb'), row.get('sac'), row.get('so'), row.get('roe'),
                      row.get('hrr'), row.get('sor'), row.get('avg'), row.get('obp'),
                      row.get('slg'), row.get('ops')))
                total_stored += 1
            except Exception:
                pass

    conn.commit()
    return len(reg_rows), len(po_rows), None


def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_source_column(conn)

    players = get_all_league_players(conn)
    print(f"Found {len(players)} non-Vibes league players to scrape")

    # Skip players already scraped (have at least one 'combined' row)
    already_done = {r[0] for r in conn.execute(
        "SELECT DISTINCT player_name FROM batter_vs_pitcher WHERE tab_type='combined'"
    ).fetchall()}
    players = [p for p in players if p not in already_done]
    print(f"  {len(already_done)} already scraped — {len(players)} remaining")

    failures = []
    total_reg_seasons = 0
    total_po_seasons = 0
    scraped = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)  # 30s per action — skip stuck pages

        for i, player in enumerate(players):
            print(f"  [{i+1}/{len(players)}] {player} ...", flush=True)

            success = False
            last_err = None
            for attempt in range(3):
                try:
                    reg, po, err = scrape_player(page, player, conn)
                    if err:
                        last_err = err
                        if attempt < 2:
                            time.sleep(2)
                        continue
                    total_reg_seasons += reg
                    total_po_seasons += po
                    success = True
                    scraped += 1
                    break
                except Exception as e:
                    last_err = str(e)
                    if attempt < 2:
                        time.sleep(2)

            if not success:
                failures.append((player, last_err))

        browser.close()

    print(f"\n=== FINAL SUMMARY ===")
    print(f"  Total players scraped: {scraped}")
    print(f"  Regular season BvP season-groups: {total_reg_seasons}")
    print(f"  Playoff BvP season-groups: {total_po_seasons}")
    total_rows = conn.execute(
        "SELECT COUNT(*) FROM batter_vs_pitcher WHERE tab_type='combined'"
    ).fetchone()[0]
    print(f"  Total 'combined' BvP rows in DB: {total_rows}")
    print(f"  Failures ({len(failures)}):")
    for player, err in failures:
        print(f"    {player}: {err}")

    conn.close()


if __name__ == '__main__':
    run()
