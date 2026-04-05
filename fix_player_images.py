#!/usr/bin/env python3
"""
Vibes Savant — Fix Player Images.
Strategy:
1. If existing URL has /Portals/0/Gallery/Album/N/file pattern,
   try az-images/album/N/file transformation first.
2. If no URL or transformation fails, scrape hrltwincities.com/player/[slug]
   and look for img tags with player-name hints in the URL.
3. Fall back to Toasty mascot.
All URLs HTTP-200 tested before saving.
"""

import sqlite3, os, re, urllib.request
from urllib.parse import quote
from playwright.sync_api import sync_playwright

DB_PATH    = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')
BASE_URL   = "https://hrltwincities.com"
TOASTY_URL = "https://i.ibb.co/m5cJfRtN/TOASTYPRIMARY.png"

# Team/league logos to skip (not player headshots)
LOGO_KEYWORDS = ['transp', 'logo', 'mascot', 'TOASTY', '_league', '_banner']

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]


def test_url(url, timeout=8):
    """Return HTTP status code, or 0 on error."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except Exception:
        return 0


def portals_to_az(url):
    """
    Transform /Portals/0/Gallery/Album/N/file.ext → /az-images/album/N/file.ext
    Also handles the ~/Portals/ bug.
    Returns the transformed URL or None if pattern doesn't match.
    """
    if not url:
        return None
    url = url.replace('https://hrltwincities.com~/', 'https://hrltwincities.com/')
    url = url.replace('http://hrltwincities.com~/', 'https://hrltwincities.com/')
    m = re.search(r'/Portals/0/Gallery/Album/(\d+)/(.+)', url)
    if m:
        album_num = m.group(1)
        filename  = m.group(2)
        return f"{BASE_URL}/az-images/album/{album_num}/{filename}"
    return None


def is_player_photo(url, player):
    """Heuristic: does the URL look like a player headshot (not a team logo)?"""
    low = url.lower()
    player_low = player.lower()
    # Skip obvious team/league logos
    if any(kw.lower() in low for kw in LOGO_KEYWORDS):
        return False
    # Prefer URLs containing the player's name hint
    return True  # accept all non-logo candidates; caller will pick the best


def scrape_page_photos(page, player):
    """
    Visit the player's page and collect all image URLs.
    Returns a list of (url, score) sorted best-first, where score 2 = name match,
    1 = generic player photo, 0 = skip.
    """
    url = f"{BASE_URL}/player/{quote(player)}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(2500)
    except Exception as e:
        print(f"    [page load error: {e}]")
        return []

    player_lower = player.lower()
    candidates = []

    for img in page.query_selector_all('img'):
        src = img.get_attribute('src') or ''
        if not src:
            continue
        # Normalise
        if src.startswith('/'):
            src = BASE_URL + src
        src = src.replace('https://hrltwincities.com~/', 'https://hrltwincities.com/')
        # Must be from the HRL domain
        if 'hrltwincities.com' not in src:
            continue
        # Skip obvious logos
        if any(kw.lower() in src.lower() for kw in LOGO_KEYWORDS):
            continue
        # Try az-images transformation if it's a Portals URL
        transformed = portals_to_az(src)
        if transformed:
            src = transformed
        # Score: name match wins
        score = 2 if player_lower in src.lower() else 1
        candidates.append((src, score))

    # Deduplicate, keep highest score per URL
    seen = {}
    for url, score in candidates:
        if url not in seen or score > seen[url]:
            seen[url] = score
    return sorted(seen.items(), key=lambda x: -x[1])


def run():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT hashtag, pic_url FROM players WHERE hashtag IN ({})".format(
            ','.join('?' * len(VIBES_PLAYERS))), VIBES_PLAYERS
    ).fetchall()
    current = {r['hashtag']: r['pic_url'] for r in rows}

    print("=== Vibes Savant — Player Image Fixer ===\n")

    results = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page    = browser.new_page()

        for player in VIBES_PLAYERS:
            existing = current.get(player)
            print(f"{player}:")

            # --- Strategy 1: az-images transform of existing URL ---
            az_url = portals_to_az(existing)
            if az_url:
                status = test_url(az_url)
                print(f"  az-transform: {status}  {az_url[:70]}")
                if status == 200:
                    results[player] = ('az-transform', az_url, status)
                    conn.execute("UPDATE players SET pic_url=? WHERE hashtag=?", (az_url, player))
                    conn.commit()
                    continue

            # --- Strategy 2: existing URL as-is (might already be az-images) ---
            fixed_existing = existing
            if fixed_existing:
                fixed_existing = fixed_existing.replace(
                    'https://hrltwincities.com~/', 'https://hrltwincities.com/')
                if fixed_existing.startswith('/'):
                    fixed_existing = BASE_URL + fixed_existing
            if fixed_existing and 'az-images' in fixed_existing:
                status = test_url(fixed_existing)
                print(f"  existing az:  {status}  {fixed_existing[:70]}")
                if status == 200:
                    results[player] = ('existing', fixed_existing, status)
                    conn.execute("UPDATE players SET pic_url=? WHERE hashtag=?",
                                 (fixed_existing, player))
                    conn.commit()
                    continue

            # --- Strategy 3: Scrape the player page ---
            print(f"  Scraping page...")
            candidates = scrape_page_photos(page, player)
            found = None
            # Try name-match candidates first (score=2), then others
            for cand_url, score in candidates:
                status = test_url(cand_url)
                print(f"    [{score}] {status}  {cand_url[:70]}")
                if status == 200:
                    found = (cand_url, status)
                    break

            if found:
                results[player] = ('scraped', found[0], found[1])
                conn.execute("UPDATE players SET pic_url=? WHERE hashtag=?",
                             (found[0], player))
                conn.commit()
                continue

            # --- Strategy 4: Fallback to Toasty ---
            print(f"  ⚠ No photo found — Toasty fallback")
            results[player] = ('toasty', TOASTY_URL, 200)
            conn.execute("UPDATE players SET pic_url=? WHERE hashtag=?",
                         (TOASTY_URL, player))
            conn.commit()

        browser.close()

    print("\n=== FINAL RESULTS ===")
    print(f"{'Player':<12} {'Source':<14} {'HTTP':<6} URL")
    print("-" * 90)
    for player in VIBES_PLAYERS:
        src, url, status = results.get(player, ('unknown', TOASTY_URL, '—'))
        short = url[:58] + '…' if len(url) > 58 else url
        mark = '✓' if status == 200 else '✗'
        print(f"{mark} {player:<12} {src:<14} {str(status):<6} {short}")

    conn.close()


if __name__ == '__main__':
    run()
