#!/usr/bin/env python3
"""
Debug Blazor page - wait for stats to render via DOM changes.
"""
import json, re
from playwright.sync_api import sync_playwright

def debug_page(slug):
    url = f"https://hrltwincities.com/player/{slug}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(60000)
        print(f"Loading {url}...")
        page.goto(url, wait_until='networkidle')

        # Wait for Blazor to finish rendering - look for common Blazor indicators
        # Try waiting for the content to contain actual stats data
        print("Waiting for content to stabilize...")

        # Try to find elements with stats-like data
        for wait_time in [3000, 5000, 8000, 12000]:
            page.wait_for_timeout(wait_time - (wait_time // 2 if wait_time > 3000 else 0))
            content = page.content()
            html_len = len(content)
            has_table = '<table' in content
            has_avg = '.AVG' in content or '>AVG<' in content or '>avg<' in content.lower()
            has_stats = 'stats' in content.lower() and ('season' in content.lower() or 'career' in content.lower())
            print(f"  After wait: html={html_len}, table={has_table}, avg={has_avg}, stats={has_stats}")

            if has_table:
                print("  Found table! Breaking.")
                break

        # Try all selectors for stats
        print("\nSearching for stats elements...")
        selectors_to_try = [
            'table',
            'table.k-table',
            '.k-grid',
            '.k-grid-content',
            '[class*="stats"]',
            '[class*="career"]',
            '[class*="batting"]',
            'td',
            'th',
            '.player-stats',
            '#stats',
        ]
        for sel in selectors_to_try:
            try:
                count = len(page.query_selector_all(sel))
                if count > 0:
                    print(f"  {sel}: {count} elements found")
            except:
                pass

        content = page.content()

        # Look for season years
        seasons_found = re.findall(r'\b(20[12][0-9])\b', content)
        unique_seasons = list(set(seasons_found))
        print(f"\nSeasons found in HTML: {sorted(unique_seasons)}")

        # Look for batting stat values (like .300, .250 etc)
        avg_vals = re.findall(r'\.([\d]{3})\b', content)
        print(f"Decimal stats (like .xxx): {avg_vals[:20]}")

        # Get full HTML and search for any data-related content
        # Search for blazor component data
        if '_blazor' in content or 'blazor' in content.lower():
            print("\nBlazor markers found")

        # Try to find where stats data might be
        # Look for any occurrence of typical stat column names
        for keyword in ['AVG', 'OBP', 'SLG', 'OPS', 'batting', 'Season', 'career']:
            indices = [m.start() for m in re.finditer(keyword, content, re.IGNORECASE)]
            if indices:
                print(f"\n'{keyword}' found at {len(indices)} locations. First: {content[max(0,indices[0]-50):indices[0]+100]}")

        # Try clicking on Stats tab if it exists
        print("\n\nTrying to interact with page...")
        try:
            # Look for tabs
            tabs = page.query_selector_all('a[role="tab"], button[role="tab"], [class*="tab"]')
            print(f"Found {len(tabs)} tab elements")
            for tab in tabs[:10]:
                try:
                    text = tab.inner_text()
                    print(f"  Tab: '{text}'")
                except:
                    pass
        except Exception as e:
            print(f"Tab search error: {e}")

        browser.close()

if __name__ == '__main__':
    debug_page('Psych')
