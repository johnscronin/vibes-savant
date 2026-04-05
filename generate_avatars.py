#!/usr/bin/env python3
"""
Generate SVG avatars for players without photos.
Vibes players get Toasty URL. All others get generated SVG.
"""

import sqlite3
import hashlib
import os
import re

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'
TOASTY_URL = 'https://i.ibb.co/m5cJfRtN/TOASTYPRIMARY.png'
AVATARS_DIR = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant_site/static/avatars'
VIBES_PLAYERS = {'Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite'}

COLOR_PALETTE = [
    '#2563eb', '#7c3aed', '#db2777', '#dc2626', '#ea580c',
    '#d97706', '#65a30d', '#16a34a', '#0891b2', '#0e7490',
    '#1d4ed8', '#6d28d9', '#be185d', '#b91c1c', '#c2410c',
    '#b45309', '#4d7c0f', '#15803d', '#0e7490', '#155e75'
]

def get_initials(hashtag, nickname):
    """Get initials for the avatar."""
    # Use nickname if it has spaces (two words = two initials)
    if nickname and ' ' in nickname.strip():
        parts = nickname.strip().split()
        return (parts[0][0] + parts[1][0]).upper()
    # Otherwise use first letter of hashtag (strip numbers/underscores at end)
    clean = re.sub(r'[_\d].*', '', hashtag)
    return clean[0].upper() if clean else hashtag[0].upper()

def darken_hex(hex_color, factor=0.75):
    """Darken a hex color by a factor."""
    hex_color = hex_color.lstrip('#')
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    r = int(r * factor)
    g = int(g * factor)
    b = int(b * factor)
    return f'#{r:02x}{g:02x}{b:02x}'

def get_color(hashtag):
    """Deterministic color from hashtag."""
    idx = int(hashlib.md5(hashtag.encode()).hexdigest(), 16) % len(COLOR_PALETTE)
    return COLOR_PALETTE[idx]

def generate_svg(hashtag, nickname):
    """Generate SVG avatar."""
    initials = get_initials(hashtag, nickname or hashtag)
    bg_color = get_color(hashtag)
    stripe_color = darken_hex(bg_color, 0.75)

    # Generate diagonal stripe lines
    stripes = []
    for i in range(-2, 8):
        x1 = i * 40 - 20
        y1 = 0
        x2 = x1 + 200
        y2 = 200
        stripes.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stripe_color}" stroke-width="8" opacity="0.3"/>')

    stripes_svg = '\n    '.join(stripes)

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 200" width="200" height="200">
  <defs>
    <clipPath id="circle-clip">
      <circle cx="100" cy="100" r="100"/>
    </clipPath>
  </defs>
  <g clip-path="url(#circle-clip)">
    <rect width="200" height="200" fill="{bg_color}"/>
    {stripes_svg}
    <text x="100" y="108" font-size="80" font-family="Barlow Condensed, Arial Black, sans-serif" font-weight="bold" fill="white" text-anchor="middle" dominant-baseline="central">{initials}</text>
  </g>
</svg>'''
    return svg

def main():
    os.makedirs(AVATARS_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get players needing avatars (null/empty, not Vibes, not az-images, not Toasty)
    players_needing_avatar = conn.execute("""
        SELECT hashtag, nickname FROM players
        WHERE hashtag NOT IN ('Anakin', 'CatNip', 'Cheerio', 'Epstein', 'FishHook', 'HuckFinn', 'Jessie', 'Kar', 'Nightmare', 'Fortnite')
        AND (pic_url IS NULL OR pic_url = '' OR pic_url NOT LIKE '%az-images%')
        AND (pic_url NOT LIKE '%static/avatars%')
        AND (pic_url NOT LIKE '%ibb.co%')
        ORDER BY hashtag
    """).fetchall()

    print(f'Players needing avatars: {len(players_needing_avatar)}')

    generated = 0
    for player in players_needing_avatar:
        hashtag = player['hashtag']
        nickname = player['nickname'] or hashtag

        # Generate SVG
        svg_content = generate_svg(hashtag, nickname)

        # Save SVG file
        safe_filename = re.sub(r'[^a-zA-Z0-9_\-]', '_', hashtag)
        svg_path = os.path.join(AVATARS_DIR, f'{safe_filename}.svg')
        with open(svg_path, 'w') as f:
            f.write(svg_content)

        # Update DB - use the safe filename
        avatar_url = f'/static/avatars/{safe_filename}.svg'
        conn.execute('UPDATE players SET pic_url=? WHERE hashtag=?', (avatar_url, hashtag))
        generated += 1

    conn.commit()

    # Final count
    stats = conn.execute("""
        SELECT
          CASE
            WHEN pic_url LIKE '%ibb.co%' THEN 'Toasty'
            WHEN pic_url LIKE '%az-images%' THEN 'HRL Photo'
            WHEN pic_url LIKE '%static/avatars%' THEN 'Generated Avatar'
            WHEN pic_url IS NULL OR pic_url = '' THEN 'null/empty'
            ELSE 'other'
          END as type, COUNT(*)
        FROM players GROUP BY type ORDER BY COUNT(*) DESC
    """).fetchall()

    print(f'\nGenerated {generated} SVG avatars')
    print('\n--- FINAL PHOTO STATUS ---')
    for s in stats:
        print(f'  {s[0]}: {s[1]}')

    conn.close()

if __name__ == '__main__':
    main()
