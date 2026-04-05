#!/usr/bin/env python3
"""
Fix remaining players without photos.
Assumes az-images URLs are already set for those that could be found.
Now handles:
1. Players with null/empty pic_url -> try API then mark as needing avatar
2. Players with old Portals/Dynamic/dnn URLs -> convert to az-images
3. Players with az-images -> leave alone (already good)
4. Toasty -> leave alone (Vibes players)
"""

import sqlite3
import requests
import re
import time
from urllib.parse import quote

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'
TOASTY_URL = 'https://i.ibb.co/m5cJfRtN/TOASTYPRIMARY.png'
VIBES_PLAYERS = {'Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite'}

def convert_to_az_url(pic_url):
    """Convert HRL internal pic URLs to az-images format."""
    if not pic_url:
        return None
    pic_url = pic_url.split('?')[0]
    if '/az-images/' in pic_url:
        if pic_url.startswith('/'):
            return 'https://hrltwincities.com' + pic_url
        return pic_url
    m = re.match(r'.*/Gallery/Album/(\d+)/(.+)', pic_url)
    if m:
        return f'https://hrltwincities.com/az-images/album/{m.group(1)}/{quote(m.group(2))}'
    m2 = re.match(r'(?:https?://[^/]+)?(/Dynamic/Images/.+)', pic_url)
    if m2:
        return f'https://hrltwincities.com/az-images{m2.group(1)}'
    return None

def quick_check_url(url, timeout=4):
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

    # Get players that need photo work
    # Only process: null/empty, old Portals/Dynamic URLs (not az-images, not ibb.co, not static/avatars)
    players_to_fix = conn.execute("""
        SELECT hashtag, nickname, pic_url, player_id FROM players
        WHERE hashtag NOT IN ('Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite')
        AND (
            pic_url IS NULL OR pic_url = ''
            OR (pic_url NOT LIKE '%az-images%' AND pic_url NOT LIKE '%ibb.co%' AND pic_url NOT LIKE '%static/avatars%')
        )
        ORDER BY hashtag
    """).fetchall()

    print(f'Players to fix: {len(players_to_fix)}')

    api_found = 0
    already_fixed = 0
    needs_avatar = []

    for i, player in enumerate(players_to_fix):
        hashtag = player['hashtag']
        current_pic = player['pic_url'] or ''

        if i > 0 and i % 20 == 0:
            print(f'  Progress: {i}/{len(players_to_fix)}, API found: {api_found}, need avatar: {len(needs_avatar)}')

        # Try to convert current URL to az-images
        if current_pic and current_pic not in ('', 'None'):
            az_url = convert_to_az_url(current_pic)
            if az_url and quick_check_url(az_url):
                conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (az_url, hashtag))
                already_fixed += 1
                continue

        # Try API
        found_via_api = False
        try:
            r = requests.get(f'https://hrltwincities.com/api/players/{hashtag}', timeout=8)
            if r.status_code == 200:
                data = r.json()
                pic_url = data.get('metadata', {}).get('picUrl', '')
                if pic_url:
                    az_url = convert_to_az_url(pic_url)
                    if az_url and quick_check_url(az_url):
                        conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (az_url, hashtag))
                        api_found += 1
                        found_via_api = True
        except Exception as e:
            pass

        if not found_via_api:
            needs_avatar.append(hashtag)

        time.sleep(0.03)

    conn.commit()

    print(f'\nFixed via URL conversion: {already_fixed}')
    print(f'Found via API: {api_found}')
    print(f'Still need avatar: {len(needs_avatar)}')

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

    print('\nCurrent stats:')
    for s in stats:
        print(f'  {s[0]}: {s[1]}')

    if needs_avatar:
        print(f'\nNeed avatar ({len(needs_avatar)}):')
        for h in needs_avatar[:20]:
            print(f'  {h}')
        if len(needs_avatar) > 20:
            print(f'  ... and {len(needs_avatar)-20} more')

    conn.close()
    return needs_avatar

if __name__ == '__main__':
    result = main()
    print(f'\nScript done. {len(result)} players need avatar generation.')
