# Vibes Front Office Tool — expandable to full HRL analytics.
# To add a new team: copy this block and add an entry to TEAMS dict.
# Flask routes use /team/<team_slug>/player/<player_name> pattern.

TEAMS = {
    'vibes': {
        'name': 'Vibes',
        'slug': 'vibes',
        'colors': {
            'primary': '#99c9ea',
            'accent':  '#d5539b',
            'bg':      '#0d0d0f',
        },
        'mascot_url': 'https://i.ibb.co/m5cJfRtN/TOASTYPRIMARY.png',
        'players': [
            "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
            "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
        ],
    }
    # To add another team:
    # 'aces': {
    #     'name': 'Aces',
    #     'slug': 'aces',
    #     'colors': {...},
    #     'mascot_url': '...',
    #     'players': [...],
    # }
}

DEFAULT_TEAM = 'vibes'
