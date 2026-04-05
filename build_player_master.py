from playwright.sync_api import sync_playwright
import sqlite3, re, time

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'

def scrape():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30000)
        page.goto('https://hrltwincities.com/players?Year=-1', wait_until='domcontentloaded')
        page.wait_for_timeout(4000)

        # Scroll to load all content
        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        page.wait_for_timeout(2000)
        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        page.wait_for_timeout(2000)

        links = page.query_selector_all('a[href*="/player/"]')
        players = []
        seen_slugs = set()
        for link in links:
            href = link.get_attribute('href') or ''
            text = link.text_content() or ''
            text = text.strip()
            if not href or not text:
                continue
            m = re.search(r'/player/([^/?#]+)', href)
            if not m:
                continue
            slug = m.group(1)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            parent_text = ''
            try:
                parent = link.evaluate('el => el.parentElement ? el.parentElement.textContent : ""')
                parent_text = parent.strip()
            except:
                pass
            hrl_url = f'https://hrltwincities.com/player/{slug}'
            players.append({'display_name': text, 'slug': slug, 'hrl_url': hrl_url, 'context': parent_text})

        browser.close()
        return players

players = scrape()
print(f'Scraped {len(players)} players')

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Create player_master table
conn.execute('''
CREATE TABLE IF NOT EXISTS player_master (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    display_name TEXT,
    slug TEXT UNIQUE,
    hrl_url TEXT,
    last_known_team TEXT,
    last_known_year INTEGER,
    is_duplicate_name BOOLEAN DEFAULT 0,
    scraped BOOLEAN DEFAULT 0,
    scrape_failed BOOLEAN DEFAULT 0,
    error_message TEXT
)
''')

# Insert all scraped players
for p in players:
    conn.execute('''
        INSERT OR IGNORE INTO player_master (display_name, slug, hrl_url)
        VALUES (?, ?, ?)
    ''', (p['display_name'], p['slug'], p['hrl_url']))

conn.commit()

# Mark duplicate display names
conn.execute('''
UPDATE player_master SET is_duplicate_name = 1
WHERE display_name IN (
    SELECT display_name FROM player_master GROUP BY display_name HAVING COUNT(*) > 1
)
''')
conn.commit()

# Print stats
total = conn.execute('SELECT COUNT(*) FROM player_master').fetchone()[0]
print(f'player_master total: {total}')

# Duplicate display name pairs
dups = conn.execute('''
    SELECT display_name, GROUP_CONCAT(slug, ', ') as slugs, COUNT(*) as cnt
    FROM player_master
    GROUP BY display_name HAVING cnt > 1
    ORDER BY display_name
''').fetchall()
print(f'\nDuplicate display name pairs: {len(dups)}')
for d in dups:
    print(f'  "{d[0]}": {d[1]}')

# Players in player_master NOT in current players table
missing = conn.execute('''
    SELECT pm.slug, pm.display_name, pm.last_known_team, pm.last_known_year
    FROM player_master pm
    WHERE pm.slug NOT IN (SELECT hashtag FROM players)
    ORDER BY pm.display_name
''').fetchall()
print(f'\nMissing players (in player_master but not in DB): {len(missing)}')
for m in missing:
    print(f'  {m[0]}: "{m[1]}" ({m[2]}, {m[3]})')

conn.close()
