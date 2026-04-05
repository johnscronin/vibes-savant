#!/usr/bin/env python3
"""
Complete photo fix script.
Converts all valid HRL URLs to az-images format.
Uses API for players with no URL.
Marks remaining players as needing avatar generation.
"""

import sqlite3
import requests
import re
import time
from urllib.parse import quote, unquote

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'
TOASTY_URL = 'https://i.ibb.co/m5cJfRtN/TOASTYPRIMARY.png'
VIBES_PLAYERS = {'Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite'}

def convert_to_az_url(pic_url):
    """Convert HRL internal pic URLs to az-images format."""
    if not pic_url:
        return None

    # Remove query params
    pic_url = pic_url.split('?')[0]

    # Already az-images - just normalize
    if '/az-images/' in pic_url:
        if pic_url.startswith('/'):
            return 'https://hrltwincities.com' + pic_url
        if pic_url.startswith('https://hrltwincities.com'):
            return pic_url
        return None

    # Extract the album number and filename from Gallery URL
    # Handles both encoded and unencoded versions
    m = re.match(r'.*[/]Gallery[/]Album[/](\d+)[/](.+)', pic_url, re.IGNORECASE)
    if m:
        album_id = m.group(1)
        filename = m.group(2)  # Keep as-is (may already be URL encoded)
        # Don't double-encode - if already encoded, pass through
        return f'https://hrltwincities.com/az-images/album/{album_id}/{filename}'

    # Convert /Dynamic/Images/lgprof/file.jpg
    m2 = re.match(r'(?:https?://[^/]+)?(/Dynamic/Images/.+)', pic_url)
    if m2:
        path = m2.group(1)
        return f'https://hrltwincities.com/az-images{path}'

    return None

def quick_check_url(url, timeout=5):
    """Quick check if URL returns an image."""
    if not url:
        return False
    try:
        r = requests.get(url, timeout=timeout, stream=True)
        if r.status_code == 200:
            ct = r.headers.get('content-type', '')
            if 'image' in ct:
                return True
        return False
    except Exception:
        return False

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Ensure Vibes players have Toasty
    for h in VIBES_PLAYERS:
        conn.execute('UPDATE players SET pic_url=? WHERE hashtag=? AND (pic_url IS NULL OR pic_url=\'\' OR pic_url NOT LIKE \'%ibb.co%\')',
                    (TOASTY_URL, h))
    conn.commit()
    print('Vibes players set to Toasty')

    # Process players with old-style Portals/Gallery URLs or dnn. URLs
    old_url_players = conn.execute("""
        SELECT hashtag, nickname, pic_url FROM players
        WHERE hashtag NOT IN ('Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite')
        AND pic_url IS NOT NULL AND pic_url != ''
        AND pic_url NOT LIKE '%az-images%'
        AND pic_url NOT LIKE '%ibb.co%'
        AND pic_url NOT LIKE '%static/avatars%'
        ORDER BY hashtag
    """).fetchall()

    print(f'Players with old-style URLs: {len(old_url_players)}')
    converted = 0
    failed_conversion = []

    for player in old_url_players:
        hashtag = player['hashtag']
        pic_url = player['pic_url']

        # Try Gallery conversion
        az_url = convert_to_az_url(pic_url)
        if az_url and quick_check_url(az_url):
            conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (az_url, hashtag))
            converted += 1
        else:
            # Try via API
            try:
                r = requests.get(f'https://hrltwincities.com/api/players/{hashtag}', timeout=8)
                if r.status_code == 200:
                    api_pic = r.json().get('metadata', {}).get('picUrl', '')
                    if api_pic:
                        api_az = convert_to_az_url(api_pic)
                        if api_az and quick_check_url(api_az):
                            conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (api_az, hashtag))
                            converted += 1
                            continue
            except Exception:
                pass
            # Clear broken URL so avatar will be generated
            conn.execute('UPDATE players SET pic_url=NULL WHERE hashtag=?', (hashtag,))
            failed_conversion.append(hashtag)
        time.sleep(0.02)

    conn.commit()
    print(f'Converted old-style URLs: {converted}')
    print(f'Cleared broken URLs (will get avatar): {len(failed_conversion)}')

    # Now handle null/empty players
    null_players = conn.execute("""
        SELECT hashtag, nickname, pic_url FROM players
        WHERE hashtag NOT IN ('Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite')
        AND (pic_url IS NULL OR pic_url = '')
        ORDER BY hashtag
    """).fetchall()

    print(f'\nPlayers with null/empty URLs: {len(null_players)}')
    api_found = 0
    needs_avatar = []

    for i, player in enumerate(null_players):
        hashtag = player['hashtag']
        if i > 0 and i % 30 == 0:
            print(f'  API progress: {i}/{len(null_players)}, found: {api_found}')

        try:
            r = requests.get(f'https://hrltwincities.com/api/players/{hashtag}', timeout=8)
            if r.status_code == 200:
                api_pic = r.json().get('metadata', {}).get('picUrl', '')
                if api_pic:
                    az_url = convert_to_az_url(api_pic)
                    if az_url and quick_check_url(az_url):
                        conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (az_url, hashtag))
                        api_found += 1
                        continue
        except Exception:
            pass
        needs_avatar.append(hashtag)
        time.sleep(0.03)

    conn.commit()
    print(f'Found via API: {api_found}')
    print(f'Need avatar generation: {len(needs_avatar)}')

    # Final stats
    stats = conn.execute("""
        SELECT
          CASE
            WHEN pic_url LIKE '%ibb.co%' THEN 'Toasty'
            WHEN pic_url LIKE '%az-images%' THEN 'az-images'
            WHEN pic_url LIKE '%static/avatars%' THEN 'Generated Avatar'
            WHEN pic_url IS NULL OR pic_url = '' THEN 'null/empty'
            ELSE 'other'
          END as type, COUNT(*)
        FROM players GROUP BY type ORDER BY COUNT(*) DESC
    """).fetchall()

    print('\n--- PHOTO STATUS ---')
    for s in stats:
        print(f'  {s[0]}: {s[1]}')

    all_need_avatar = needs_avatar + failed_conversion
    print(f'\nTotal need avatar: {len(all_need_avatar)}')
    conn.close()
    return all_need_avatar

if __name__ == '__main__':
    needs_avatar = main()
    # Save to file for avatar generator
    with open('/tmp/needs_avatar.txt', 'w') as f:
        for h in needs_avatar:
            f.write(h + '\n')
    print(f'Saved {len(needs_avatar)} hashtags to /tmp/needs_avatar.txt')
