#!/usr/bin/env python3
"""
Scrape player photos from HRL website API and update the database.
Strategy:
1. Call /api/players/{hashtag} to get picUrl from API
2. Convert picUrl to az-images format and verify it works
3. Fall back to Playwright scraping if needed
4. Generate avatars for players still without photos
"""

import sqlite3
import requests
import re
import time
import sys
from urllib.parse import quote

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'
TOASTY_URL = 'https://i.ibb.co/m5cJfRtN/TOASTYPRIMARY.png'
VIBES_PLAYERS = {'Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite'}

def convert_to_az_url(pic_url):
    """Convert HRL internal pic URLs to az-images format that actually works."""
    if not pic_url:
        return None

    # Already az-images
    if '/az-images/' in pic_url:
        if pic_url.startswith('/'):
            return 'https://hrltwincities.com' + pic_url.split('?')[0]
        return pic_url.split('?')[0]

    # Remove any query params
    pic_url = pic_url.split('?')[0]

    # Convert /Portals/0/Gallery/Album/123/file -> /az-images/album/123/file
    m = re.match(r'.*/Gallery/Album/(\d+)/(.+)', pic_url)
    if m:
        album_id = m.group(1)
        filename = m.group(2)
        return f'https://hrltwincities.com/az-images/album/{album_id}/{quote(filename)}'

    # Convert /Dynamic/Images/lgprof/file.jpg -> /az-images/Dynamic/Images/lgprof/file.jpg
    m2 = re.match(r'(?:.*?)?(/Dynamic/Images/.+)', pic_url)
    if m2:
        path = m2.group(1)
        return f'https://hrltwincities.com/az-images{path}'

    return None

def verify_url(url, timeout=5):
    """Returns True if URL returns an image."""
    if not url:
        return False
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            ct = r.headers.get('content-type', '')
            if 'image' in ct:
                return True
        # Try GET if HEAD doesn't work
        if r.status_code in (405, 403):
            r2 = requests.get(url, timeout=timeout, stream=True)
            if r2.status_code == 200 and 'image' in r2.headers.get('content-type', ''):
                return True
        return False
    except Exception:
        return False

def get_photo_via_playwright(hashtag, timeout=15000):
    """Use Playwright to scrape player page for photo URL."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(f'https://hrltwincities.com/player/{hashtag}', timeout=timeout)
            page.wait_for_load_state('domcontentloaded', timeout=timeout)

            imgs = page.query_selector_all('img')
            candidates = []
            for img in imgs:
                src = img.get_attribute('src') or ''
                if not src:
                    continue
                src_lower = src.lower()
                # Skip logos/icons/tiny images
                if any(x in src_lower for x in ['logo', 'icon', 'sleeve', 'border', 'white', 'transp', 'assets/']):
                    continue
                # Must contain photo-related path
                if any(x in src_lower for x in ['gallery', 'album', 'player', 'photo', 'portrait', 'dynamic/images', 'lgprof', 'az-images']):
                    candidates.append(src)

            browser.close()
            return candidates
    except Exception as e:
        print(f'  Playwright error for {hashtag}: {e}')
        return []

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get ALL players
    all_players = conn.execute('SELECT hashtag, nickname, pic_url, player_id FROM players ORDER BY hashtag').fetchall()
    total = len(all_players)
    print(f'Total players: {total}')

    # First: apply Vibes player default
    vibes_updated = 0
    for player in all_players:
        if player['hashtag'] in VIBES_PLAYERS:
            current_pic = player['pic_url'] or ''
            if not current_pic or current_pic == '' or 'ibb.co' not in current_pic:
                conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (TOASTY_URL, player['hashtag']))
                vibes_updated += 1
    conn.commit()
    print(f'Set Toasty URL for {vibes_updated} Vibes players')

    # Reload players
    all_players = conn.execute('SELECT hashtag, nickname, pic_url, player_id FROM players ORDER BY hashtag').fetchall()

    # Categorize players
    needs_photo = []
    has_working_photo = []

    for player in all_players:
        hashtag = player['hashtag']
        pic_url = player['pic_url'] or ''

        # Skip Vibes players (they have Toasty)
        if hashtag in VIBES_PLAYERS:
            has_working_photo.append(hashtag)
            continue

        # Check if current URL works with az-images conversion
        if pic_url:
            if 'ibb.co' in pic_url:
                has_working_photo.append(hashtag)
                continue
            az_url = convert_to_az_url(pic_url)
            if az_url and verify_url(az_url):
                # Update to working az-images URL
                conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (az_url, hashtag))
                has_working_photo.append(hashtag)
                continue

        needs_photo.append(hashtag)

    conn.commit()
    print(f'Players with working photos (after az-images fix): {len(has_working_photo)}')
    print(f'Players needing photos: {len(needs_photo)}')

    # Step 2: Use API to get fresh picUrl for players needing photos
    api_found = 0
    api_missing = []

    print(f'\nFetching API data for {len(needs_photo)} players...')
    for i, hashtag in enumerate(needs_photo):
        if i > 0 and i % 20 == 0:
            print(f'  Progress: {i}/{len(needs_photo)}')

        try:
            r = requests.get(f'https://hrltwincities.com/api/players/{hashtag}', timeout=10)
            if r.status_code == 200:
                data = r.json()
                metadata = data.get('metadata', {})
                pic_url = metadata.get('picUrl', '')

                if pic_url:
                    az_url = convert_to_az_url(pic_url)
                    if az_url and verify_url(az_url):
                        conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (az_url, hashtag))
                        api_found += 1
                        continue
            api_missing.append(hashtag)
        except Exception as e:
            api_missing.append(hashtag)

        time.sleep(0.05)  # Small delay to be nice to the API

    conn.commit()
    print(f'Photos found via API: {api_found}')
    print(f'Still missing after API: {len(api_missing)}')

    # Step 3: Playwright for remaining players (with timeout)
    playwright_found = 0
    playwright_failed = []

    if api_missing:
        print(f'\nUsing Playwright for {len(api_missing)} remaining players...')
        print('(Will fall back to avatar generation if taking too long)')

        start_time = time.time()
        max_playwright_time = 90  # 90 seconds max for Playwright

        for i, hashtag in enumerate(api_missing):
            elapsed = time.time() - start_time
            if elapsed > max_playwright_time:
                print(f'Playwright timeout reached after {elapsed:.0f}s. Falling back to avatars for remaining {len(api_missing)-i} players.')
                playwright_failed.extend(api_missing[i:])
                break

            if i > 0 and i % 5 == 0:
                print(f'  Progress: {i}/{len(api_missing)}, elapsed: {elapsed:.0f}s')

            candidates = get_photo_via_playwright(hashtag)
            found = False
            for src in candidates:
                # Resolve relative URLs
                if src.startswith('/'):
                    full_url = 'https://hrltwincities.com' + src.split('?')[0]
                elif not src.startswith('http'):
                    full_url = 'https://hrltwincities.com/' + src.split('?')[0]
                else:
                    full_url = src.split('?')[0]

                if verify_url(full_url):
                    conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (full_url, hashtag))
                    playwright_found += 1
                    found = True
                    break

            if not found:
                playwright_failed.append(hashtag)

        conn.commit()
        print(f'Photos found via Playwright: {playwright_found}')
        print(f'Still missing after Playwright: {len(playwright_failed)}')
    else:
        playwright_failed = []

    # Final counts
    conn2 = sqlite3.connect(DB_PATH)
    final_stats = conn2.execute("""
        SELECT
          CASE
            WHEN pic_url LIKE '%ibb.co%' THEN 'Toasty Default'
            WHEN pic_url LIKE '%az-images%' THEN 'HRL Photo (az-images)'
            WHEN pic_url LIKE '%hrltwincities.com%' THEN 'HRL Photo (other)'
            WHEN pic_url LIKE '%static/avatars%' THEN 'Generated Avatar'
            WHEN pic_url IS NULL OR pic_url = '' THEN 'No Photo'
            ELSE 'Other'
          END as photo_type,
          COUNT(*) as count
        FROM players
        GROUP BY photo_type
        ORDER BY count DESC
    """).fetchall()
    conn2.close()

    print('\n--- FINAL PHOTO STATUS ---')
    for row in final_stats:
        print(f'  {row[0]}: {row[1]}')

    if playwright_failed:
        print(f'\nPlayers still without photo ({len(playwright_failed)}):')
        for h in playwright_failed:
            print(f'  {h}')

    conn.close()
    return playwright_failed

if __name__ == '__main__':
    failed = main()
    print(f'\nDone. {len(failed)} players need avatar generation.')
