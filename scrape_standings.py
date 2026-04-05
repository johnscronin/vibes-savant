#!/usr/bin/env python3
"""
Scrape historical HRL standings for all years 2004-2025.
Uses Playwright to interact with the Telerik/Blazor dropdown.
Stores results in historical_standings and seasons_available tables.
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import sqlite3

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'


def create_tables(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS historical_standings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season INTEGER,
        city TEXT,
        division_name TEXT,
        team_name TEXT,
        team_slug TEXT,
        team_logo_url TEXT,
        wins INTEGER,
        losses INTEGER,
        pct REAL,
        games_back REAL,
        div_record TEXT,
        non_div_record TEXT,
        runs_scored INTEGER,
        runs_allowed INTEGER,
        run_differential INTEGER,
        streak TEXT,
        last_10 TEXT,
        playoff_result TEXT,
        UNIQUE(season, team_name)
    )""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS seasons_available (
        season INTEGER PRIMARY KEY,
        eagan_divisions TEXT,
        hopkins_divisions TEXT,
        total_teams INTEGER,
        data_complete BOOLEAN
    )""")
    conn.commit()


def parse_gb(val):
    """Parse games back. '--' = first place, return None (computed later)."""
    if not val or val.strip() in ('--', '-', ''):
        return None
    try:
        return float(val.strip())
    except (ValueError, TypeError):
        return None


def parse_pct(val):
    """Parse win percentage like .632"""
    if not val:
        return None
    v = val.strip()
    try:
        return round(float(v), 3)
    except (ValueError, TypeError):
        return None


def scrape_year_content(page, year):
    """Navigate to a year and return parsed standings rows."""
    page.click('.k-dropdownlist')
    page.wait_for_timeout(600)
    # Click the year item
    items = page.locator('li.k-list-item').all()
    for item in items:
        if item.inner_text().strip() == str(year):
            item.click()
            break
    page.wait_for_timeout(2500)

    content = page.content()
    soup = BeautifulSoup(content, 'html.parser')
    tables = soup.find_all('table')

    results = []
    i = 0
    while i < len(tables):
        t = tables[i]
        headers = [th.text.strip() for th in t.find_all('th')]

        # Header rows have the division name in the first th
        if headers and len(headers) >= 2:
            div_header = headers[0]
            # Determine city
            if 'Eagan' in div_header:
                city = 'Eagan'
                div_name = div_header.replace('Eagan', '').strip() or 'Main'
            elif 'Hopkins' in div_header:
                city = 'Hopkins'
                div_name = div_header.replace('Hopkins', '').strip() or 'Main'
            elif div_header in ('HRL', 'HRL: TC', 'HRL TC', 'TC', 'Twin Cities'):
                city = 'HRL'
                div_name = 'Main'
            else:
                # Some early years just have generic headers
                city = 'HRL'
                div_name = div_header.strip() or 'Main'

            data_table = tables[i + 1] if (i + 1) < len(tables) else None
            if data_table:
                rows = data_table.find_all('tr')
                for row in rows:
                    cells = [td.text.strip() for td in row.find_all('td')]
                    if not cells or len(cells) < 3:
                        continue
                    team_name = cells[0].strip()
                    if not team_name:
                        continue

                    # Logo from img tag
                    first_td = row.find('td')
                    logo_url = None
                    if first_td:
                        img = first_td.find('img')
                        if img:
                            src = img.get('src', '')
                            if src.startswith('/'):
                                logo_url = 'https://hrltwincities.com' + src
                            elif src.startswith('http'):
                                logo_url = src

                    def gc(idx):
                        return cells[idx].strip() if idx < len(cells) else ''

                    wins = int(gc(1)) if gc(1).isdigit() else None
                    losses = int(gc(2)) if gc(2).isdigit() else None
                    pct = parse_pct(gc(3))
                    gb = parse_gb(gc(4))
                    div_rec = gc(5) or None
                    non_div_rec = gc(6) or None

                    rs = ra = diff = streak = last_10 = None
                    if len(cells) >= 10:
                        try:
                            rs = int(gc(7)) if gc(7).lstrip('-+').isdigit() else None
                        except (ValueError, TypeError):
                            rs = None
                        try:
                            ra = int(gc(8)) if gc(8).lstrip('-+').isdigit() else None
                        except (ValueError, TypeError):
                            ra = None
                        try:
                            diff = int(gc(9).lstrip('+'))
                        except (ValueError, TypeError):
                            diff = None
                        if len(cells) >= 11:
                            streak = gc(10) or None
                        if len(cells) >= 12:
                            last_10 = gc(11) or None

                    results.append({
                        'city': city,
                        'division_name': div_name,
                        'team_name': team_name,
                        'team_logo_url': logo_url,
                        'wins': wins,
                        'losses': losses,
                        'pct': pct,
                        'games_back': gb,
                        'div_record': div_rec,
                        'non_div_record': non_div_rec,
                        'rs': rs,
                        'ra': ra,
                        'diff': diff,
                        'streak': streak,
                        'last_10': last_10,
                    })
            i += 2
        else:
            i += 1

    return results


def main():
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    # Build slug and logo lookup from teams table
    slug_map = {}
    logo_map = {}
    for row in conn.execute("SELECT team_name, hashtag, logo_url FROM teams").fetchall():
        slug_map[row[0]] = row[1]
        if row[2]:
            logo_map[row[0]] = row[2]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)
        page.goto('https://hrltwincities.com/standings', wait_until='domcontentloaded')
        page.wait_for_timeout(3000)

        for year in range(2025, 2003, -1):
            try:
                rows = scrape_year_content(page, year)
                divs = list(dict.fromkeys(f"{r['city']} {r['division_name']}" for r in rows))
                print(f'{year}: {len(rows)} teams | {divs}')

                eagan_divs = sorted(set(r['division_name'] for r in rows if r['city'] == 'Eagan'))
                hopkins_divs = sorted(set(r['division_name'] for r in rows if r['city'] == 'Hopkins'))

                for row in rows:
                    team_name = row['team_name']
                    slug = slug_map.get(team_name)
                    logo_url = row['team_logo_url'] or logo_map.get(team_name)

                    conn.execute("""
                        INSERT OR REPLACE INTO historical_standings
                        (season, city, division_name, team_name, team_slug, team_logo_url,
                         wins, losses, pct, games_back, div_record, non_div_record,
                         runs_scored, runs_allowed, run_differential, streak, last_10)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (year, row['city'], row['division_name'], team_name,
                          slug, logo_url,
                          row['wins'], row['losses'], row['pct'], row['games_back'],
                          row['div_record'], row['non_div_record'],
                          row['rs'], row['ra'], row['diff'],
                          row['streak'], row['last_10']))

                conn.execute("""
                    INSERT OR REPLACE INTO seasons_available
                    (season, eagan_divisions, hopkins_divisions, total_teams, data_complete)
                    VALUES (?,?,?,?,?)
                """, (year,
                      ','.join(eagan_divs) if eagan_divs else None,
                      ','.join(hopkins_divs) if hopkins_divs else None,
                      len(rows), 1 if rows else 0))
                conn.commit()

            except Exception as e:
                print(f'{year}: ERROR - {e}')
                import traceback
                traceback.print_exc()

        browser.close()

    # Fill RS from batting_stats where NULL
    print("\nFilling RS from batting_stats where missing...")
    conn.execute("""
        UPDATE historical_standings SET runs_scored = (
            SELECT SUM(r) FROM batting_stats
            WHERE team_name=historical_standings.team_name
            AND season=historical_standings.season
        ) WHERE runs_scored IS NULL
    """)
    # Fill RA from pitching_stats where NULL
    print("Filling RA from pitching_stats where missing...")
    conn.execute("""
        UPDATE historical_standings SET runs_allowed = (
            SELECT SUM(opp_r) FROM pitching_stats
            WHERE team_name=historical_standings.team_name
            AND season=historical_standings.season
            AND ip > 0
        ) WHERE runs_allowed IS NULL
    """)
    # Recalculate diff
    conn.execute("""
        UPDATE historical_standings
        SET run_differential = runs_scored - runs_allowed
        WHERE run_differential IS NULL AND runs_scored IS NOT NULL AND runs_allowed IS NOT NULL
    """)
    conn.commit()

    # Summary
    print("\n=== SCRAPE SUMMARY ===")
    for row in conn.execute("""
        SELECT s.season, s.total_teams, s.eagan_divisions, s.hopkins_divisions,
               SUM(CASE WHEN hs.runs_scored IS NOT NULL THEN 1 ELSE 0 END) as has_rs
        FROM seasons_available s
        LEFT JOIN historical_standings hs ON hs.season = s.season
        GROUP BY s.season ORDER BY s.season DESC
    """).fetchall():
        print(f"{row[0]}: {row[1]} teams, RS filled: {row[4]}, E:{row[2]}, H:{row[3]}")

    total = conn.execute("SELECT COUNT(*) FROM historical_standings").fetchone()[0]
    print(f"\nTotal historical_standings rows: {total}")
    conn.close()


if __name__ == '__main__':
    main()
