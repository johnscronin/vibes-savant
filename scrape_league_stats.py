#!/usr/bin/env python3
"""
Vibes Savant — League-wide stats scraper
Scrapes hrltwincities.com/stats for all seasons, all three tabs.
Uses Playwright (headless Chromium) to render JS-heavy Telerik tables.
"""

import asyncio
import sqlite3
import re
import os
import sys
from playwright.async_api import async_playwright

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')

# Years to skip (not real seasons)
SKIP_YEARS = {'All Time Totals', 'Projected'}

# Reverse: 2025 -> 2004
YEAR_ORDER = [str(y) for y in range(2025, 2003, -1)]


def create_league_tables(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS league_batting_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season INTEGER NOT NULL,
        player_name TEXT NOT NULL,
        team TEXT,
        g INTEGER,
        ab INTEGER,
        r INTEGER,
        h INTEGER,
        doubles INTEGER,
        triples INTEGER,
        hr INTEGER,
        rbi INTEGER,
        bb INTEGER,
        so INTEGER,
        avg REAL,
        obp REAL,
        slg REAL,
        ops REAL,
        UNIQUE(season, player_name, team)
    );

    CREATE TABLE IF NOT EXISTS league_pitching_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season INTEGER NOT NULL,
        player_name TEXT NOT NULL,
        team TEXT,
        w INTEGER,
        l INTEGER,
        era REAL,
        g INTEGER,
        gs INTEGER,
        sho INTEGER,
        sv INTEGER,
        ip REAL,
        h INTEGER,
        r INTEGER,
        hr INTEGER,
        bb INTEGER,
        k INTEGER,
        whip REAL,
        baa REAL,
        UNIQUE(season, player_name, team)
    );

    CREATE TABLE IF NOT EXISTS league_fielding_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season INTEGER NOT NULL,
        player_name TEXT NOT NULL,
        team TEXT,
        tc INTEGER,
        po INTEGER,
        errors INTEGER,
        fld_pct REAL,
        UNIQUE(season, player_name, team)
    );

    CREATE TABLE IF NOT EXISTS season_qualifiers (
        season INTEGER PRIMARY KEY,
        batting_qualifier TEXT,
        batting_min_pa INTEGER,
        pitching_qualifier TEXT,
        pitching_min_ip REAL,
        pitching_min_g INTEGER,
        fielding_qualifier TEXT,
        fielding_min_tc INTEGER
    );
    """)
    conn.commit()


def safe_float(v):
    if v is None: return None
    try: return float(str(v).replace(',', ''))
    except: return None

def safe_int(v):
    if v is None: return None
    try: return int(str(v).replace(',', '').split('.')[0])
    except: return None

def parse_qualifier(text):
    """Parse qualifier text like 'Minimum: 100 PA' or 'Minimum: 37.0 IP or 6 G'"""
    if not text:
        return {}
    result = {}

    # Batting: "Minimum: 100 PA"
    m = re.search(r'Minimum:\s*([\d,]+)\s*PA', text, re.I)
    if m:
        result['batting_min_pa'] = int(m.group(1).replace(',', ''))

    # Pitching: "Minimum: 37.0 IP or 6 G"
    m_ip = re.search(r'Minimum:\s*([\d.]+)\s*IP', text, re.I)
    if m_ip:
        result['pitching_min_ip'] = float(m_ip.group(1))
    m_g = re.search(r'(\d+)\s*G\b', text, re.I)
    if m_g and 'IP' in text:
        result['pitching_min_g'] = int(m_g.group(1))

    # Fielding: "Minimum: 37 TC"
    m = re.search(r'Minimum:\s*([\d.]+)\s*TC', text, re.I)
    if m:
        result['fielding_min_tc'] = int(float(m.group(1)))

    return result


async def get_qualifier_text(page):
    """Read the qualifier text from the grid toolbar."""
    el = await page.query_selector('.grid-header-custom, [class*=grid-header]')
    if el:
        return (await el.text_content() or '').strip()
    # Fallback: search in page text
    content = await page.content()
    m = re.search(r'Minimum:[^<"]{3,60}', content)
    return m.group(0).strip() if m else ''


async def scrape_table_all_pages(page, tab_name):
    """Scrape all rows across all pagination pages for current tab."""
    all_rows = []

    page_num = 0
    while True:
        page_num += 1
        await page.wait_for_timeout(1500)

        # Get headers from first iteration only
        if page_num == 1:
            header_els = await page.query_selector_all('.k-grid-header th')
            headers = []
            for h in header_els:
                t = (await h.text_content() or '').strip()
                # Remove sort indicator text
                t = re.sub(r'Sorted.*', '', t).strip()
                headers.append(t)

        # Get table rows
        row_els = await page.query_selector_all('.k-grid-table tr, table tbody tr')
        for row_el in row_els:
            cells = await row_el.query_selector_all('td')
            values = [(await c.text_content() or '').strip() for c in cells]
            if values and any(v for v in values):
                all_rows.append(values)

        # Check if there's a next page
        pager_info_el = await page.query_selector('.k-pager-info')
        if not pager_info_el:
            break

        pager_text = await pager_info_el.text_content() or ''
        # "X - Y of Z items"
        m = re.search(r'(\d+)\s*-\s*(\d+)\s*of\s*(\d+)', pager_text)
        if not m:
            break

        current_end = int(m.group(2))
        total = int(m.group(3))

        if current_end >= total:
            break  # Last page

        # Click next page button
        next_btn = page.locator('.k-pager-nav[title*="next" i], .k-pager-nav[aria-label*="next" i]').first
        if not await next_btn.count():
            # Try by SVG icon class
            next_btn = page.locator('button.k-pager-nav').last
        if await next_btn.count():
            await next_btn.click()
            await page.wait_for_timeout(2000)
        else:
            print(f"    [WARN] No next button found at page {page_num}, stopping")
            break

        if page_num > 30:  # Safety valve
            print(f"    [WARN] Hit page limit at page {page_num}")
            break

    return headers if page_num > 0 else [], all_rows


def parse_batting_rows(headers, rows, season):
    """Parse raw table rows into batting dicts."""
    records = []
    # Expected: Hitter, Team, G, AB, R, H, 2B, 3B, HR, RBI, BB, SO, AVG, OBP, SLG, OPS
    col_map = {h.lower().replace('sorted in descending order', '').replace('sorted in ascending order', '').strip(): i
               for i, h in enumerate(headers) if h}

    for row in rows:
        if len(row) < 4:
            continue
        try:
            def get(col):
                idx = col_map.get(col)
                return row[idx] if idx is not None and idx < len(row) else None

            name = get('hitter') or (row[0] if row else None)
            team = get('team') or (row[1] if len(row) > 1 else None)
            if not name or name.lower() in ('hitter', 'pitcher', 'fielder', ''):
                continue

            records.append({
                'season': season,
                'player_name': name,
                'team': team,
                'g':       safe_int(get('g')),
                'ab':      safe_int(get('ab')),
                'r':       safe_int(get('r')),
                'h':       safe_int(get('h')),
                'doubles': safe_int(get('2b')),
                'triples': safe_int(get('3b')),
                'hr':      safe_int(get('hr')),
                'rbi':     safe_int(get('rbi')),
                'bb':      safe_int(get('bb')),
                'so':      safe_int(get('so')),
                'avg':     safe_float(get('avg')),
                'obp':     safe_float(get('obp')),
                'slg':     safe_float(get('slg')),
                'ops':     safe_float(get('ops')),
            })
        except Exception as e:
            pass
    return records


def parse_pitching_rows(headers, rows, season):
    records = []
    col_map = {re.sub(r'sorted.*', '', h, flags=re.I).strip().lower(): i
               for i, h in enumerate(headers) if h}

    for row in rows:
        if len(row) < 4:
            continue
        try:
            def get(col):
                idx = col_map.get(col)
                return row[idx] if idx is not None and idx < len(row) else None

            name = get('pitcher') or (row[0] if row else None)
            team = get('team') or (row[1] if len(row) > 1 else None)
            if not name or name.lower() in ('hitter', 'pitcher', 'fielder', ''):
                continue

            records.append({
                'season': season,
                'player_name': name,
                'team': team,
                'w':    safe_int(get('w')),
                'l':    safe_int(get('l')),
                'era':  safe_float(get('era')),
                'g':    safe_int(get('g')),
                'gs':   safe_int(get('gs')),
                'sho':  safe_int(get('sho')),
                'sv':   safe_int(get('sv')),
                'ip':   safe_float(get('ip')),
                'h':    safe_int(get('h')),
                'r':    safe_int(get('r')),
                'hr':   safe_int(get('hr')),
                'bb':   safe_int(get('bb')),
                'k':    safe_int(get('k')),
                'whip': safe_float(get('whip')),
                'baa':  safe_float(get('baa')),
            })
        except Exception:
            pass
    return records


def parse_fielding_rows(headers, rows, season):
    records = []
    col_map = {re.sub(r'sorted.*', '', h, flags=re.I).strip().lower(): i
               for i, h in enumerate(headers) if h}

    for row in rows:
        if len(row) < 3:
            continue
        try:
            def get(col):
                idx = col_map.get(col)
                return row[idx] if idx is not None and idx < len(row) else None

            name = get('fielder') or (row[0] if row else None)
            team = get('team') or (row[1] if len(row) > 1 else None)
            if not name or name.lower() in ('hitter', 'pitcher', 'fielder', ''):
                continue

            records.append({
                'season': season,
                'player_name': name,
                'team': team,
                'tc':      safe_int(get('tc')),
                'po':      safe_int(get('po')),
                'errors':  safe_int(get('e')),
                'fld_pct': safe_float(get('pct')),
            })
        except Exception:
            pass
    return records


def save_batting(conn, records):
    for r in records:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO league_batting_stats
                (season, player_name, team, g, ab, r, h, doubles, triples, hr, rbi, bb, so,
                 avg, obp, slg, ops)
                VALUES (:season,:player_name,:team,:g,:ab,:r,:h,:doubles,:triples,:hr,:rbi,:bb,:so,
                        :avg,:obp,:slg,:ops)
            """, r)
        except Exception as e:
            print(f"  [ERROR batting insert] {e} | {r.get('player_name')}")
    conn.commit()


def save_pitching(conn, records):
    for r in records:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO league_pitching_stats
                (season, player_name, team, w, l, era, g, gs, sho, sv, ip, h, r, hr, bb, k, whip, baa)
                VALUES (:season,:player_name,:team,:w,:l,:era,:g,:gs,:sho,:sv,:ip,:h,:r,:hr,:bb,:k,:whip,:baa)
            """, r)
        except Exception as e:
            print(f"  [ERROR pitching insert] {e} | {r.get('player_name')}")
    conn.commit()


def save_fielding(conn, records):
    for r in records:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO league_fielding_stats
                (season, player_name, team, tc, po, errors, fld_pct)
                VALUES (:season,:player_name,:team,:tc,:po,:errors,:fld_pct)
            """, r)
        except Exception as e:
            print(f"  [ERROR fielding insert] {e} | {r.get('player_name')}")
    conn.commit()


async def select_year(page, year):
    """Select a year from the Telerik dropdown."""
    year_dd = page.locator('.k-dropdownlist').first
    current = (await year_dd.text_content() or '').strip()
    if current == year:
        return True

    await year_dd.click()
    await page.wait_for_timeout(1000)

    option = page.locator(f'[role=option]:has-text("{year}"), .k-list-item:has-text("{year}")').first
    if not await option.count():
        await page.keyboard.press('Escape')
        return False

    await option.click()
    await page.wait_for_timeout(4000)
    return True


async def click_tab(page, tab_name):
    tab = page.locator(f'.k-tabstrip-item:has-text("{tab_name}")').first
    if not await tab.count():
        return False
    await tab.click()
    await page.wait_for_timeout(3000)
    return True


async def scrape_all(years=None):
    conn = sqlite3.connect(DB_PATH)
    create_league_tables(conn)

    if years is None:
        years = YEAR_ORDER

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("Loading stats page...")
        await page.goto('https://hrltwincities.com/stats', wait_until='networkidle')
        await page.wait_for_timeout(6000)

        # Make sure we're on Hitting tab to start
        await click_tab(page, 'Hitting')
        await page.wait_for_timeout(2000)

        for year in years:
            print(f"\n{'='*55}")
            print(f"SEASON {year}")
            print(f"{'='*55}")

            ok = await select_year(page, year)
            if not ok:
                print(f"  [SKIP] Year {year} not found in dropdown")
                continue

            season = int(year)
            qualifiers = {'season': season}

            # --- BATTING ---
            await click_tab(page, 'Hitting')
            await page.wait_for_timeout(2000)

            bat_qual_text = await get_qualifier_text(page)
            print(f"  Batting qualifier: '{bat_qual_text}'")
            qualifiers['batting_qualifier'] = bat_qual_text
            q = parse_qualifier(bat_qual_text)
            qualifiers['batting_min_pa'] = q.get('batting_min_pa')

            bat_headers, bat_rows = await scrape_table_all_pages(page, 'Hitting')
            bat_records = parse_batting_rows(bat_headers, bat_rows, season)
            save_batting(conn, bat_records)
            print(f"  Batting: {len(bat_records)} qualified players scraped")

            # --- PITCHING ---
            await click_tab(page, 'Pitching')
            await page.wait_for_timeout(2000)

            pit_qual_text = await get_qualifier_text(page)
            print(f"  Pitching qualifier: '{pit_qual_text}'")
            qualifiers['pitching_qualifier'] = pit_qual_text
            q = parse_qualifier(pit_qual_text)
            qualifiers['pitching_min_ip'] = q.get('pitching_min_ip')
            qualifiers['pitching_min_g'] = q.get('pitching_min_g')

            pit_headers, pit_rows = await scrape_table_all_pages(page, 'Pitching')
            pit_records = parse_pitching_rows(pit_headers, pit_rows, season)
            save_pitching(conn, pit_records)
            print(f"  Pitching: {len(pit_records)} qualified pitchers scraped")

            # --- FIELDING ---
            await click_tab(page, 'Fielding')
            await page.wait_for_timeout(2000)

            fld_qual_text = await get_qualifier_text(page)
            print(f"  Fielding qualifier: '{fld_qual_text}'")
            qualifiers['fielding_qualifier'] = fld_qual_text
            q = parse_qualifier(fld_qual_text)
            qualifiers['fielding_min_tc'] = q.get('fielding_min_tc')

            fld_headers, fld_rows = await scrape_table_all_pages(page, 'Fielding')
            fld_records = parse_fielding_rows(fld_headers, fld_rows, season)
            save_fielding(conn, fld_records)
            print(f"  Fielding: {len(fld_records)} qualified fielders scraped")

            # Save qualifiers
            conn.execute("""
                INSERT OR REPLACE INTO season_qualifiers
                (season, batting_qualifier, batting_min_pa, pitching_qualifier,
                 pitching_min_ip, pitching_min_g, fielding_qualifier, fielding_min_tc)
                VALUES (:season, :batting_qualifier, :batting_min_pa, :pitching_qualifier,
                        :pitching_min_ip, :pitching_min_g, :fielding_qualifier, :fielding_min_tc)
            """, qualifiers)
            conn.commit()

            print(f"  ✓ {year}: {len(bat_records)} batting | {len(pit_records)} pitching | {len(fld_records)} fielding qualifiers")

            # Go back to Hitting for next year
            await click_tab(page, 'Hitting')
            await page.wait_for_timeout(1000)

        await browser.close()

    # Final summary
    print("\n" + "="*55)
    print("SCRAPE COMPLETE — SUMMARY")
    print("="*55)
    for tbl, label in [('league_batting_stats','batting'), ('league_pitching_stats','pitching'), ('league_fielding_stats','fielding')]:
        total = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        seasons = conn.execute(f"SELECT COUNT(DISTINCT season) FROM {tbl}").fetchone()[0]
        print(f"  {label}: {total} rows across {seasons} seasons")

    conn.close()


if __name__ == '__main__':
    # Allow passing specific years: python scrape_league_stats.py 2025 2024
    years = sys.argv[1:] if len(sys.argv) > 1 else None
    asyncio.run(scrape_all(years))
