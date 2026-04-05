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

        # Try scrolling to load all content
        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        page.wait_for_timeout(2000)
        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        page.wait_for_timeout(2000)

        # Get all player links - they should be anchor tags with href containing /player/
        links = page.query_selector_all('a[href*="/player/"]')
        players = []
        seen_slugs = set()
        for link in links:
            href = link.get_attribute('href') or ''
            text = link.text_content() or ''
            text = text.strip()
            if not href or not text:
                continue
            # Extract slug from URL like /player/Daddy_2013_As or /player/Daddy
            m = re.search(r'/player/([^/?#]+)', href)
            if not m:
                continue
            slug = m.group(1)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            # Try to get team/year from parent element text
            parent_text = ''
            try:
                parent = link.evaluate('el => el.parentElement ? el.parentElement.textContent : ""')
                parent_text = parent.strip()
            except:
                pass
            hrl_url = f'https://hrltwincities.com/player/{slug}'
            players.append({'display_name': text, 'slug': slug, 'hrl_url': hrl_url, 'context': parent_text})

        # Also print page HTML structure for debugging
        html_snippet = page.evaluate('document.body.innerHTML.substring(0, 2000)')
        print("HTML SNIPPET:")
        print(html_snippet)

        browser.close()
        return players

players = scrape()
print(f'\nFound {len(players)} players on all-time page')
for p in players[:20]:
    print(f'  {p["slug"]} → "{p["display_name"]}" ({p["context"][:80]})')
