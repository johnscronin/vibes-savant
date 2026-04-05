#!/usr/bin/env python3
"""
Vibes Savant Scraper
Scrapes all player stats from hrltwincities.com API and saves to vibes_savant.db
"""

import sqlite3
import requests
import json
import time
import warnings
warnings.filterwarnings('ignore')

BASE_URL = "https://hrltwincities.com/api"
DB_PATH = "vibes_savant.db"

PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Referer': 'https://hrltwincities.com/'
}


def create_tables(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY,
        hashtag TEXT NOT NULL UNIQUE,
        nickname TEXT,
        player_id INTEGER,
        team_id INTEGER,
        team_name TEXT,
        is_active INTEGER,
        last_year INTEGER,
        pic_url TEXT,
        team_logo_url TEXT
    );

    CREATE TABLE IF NOT EXISTS batting_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        player_hashtag TEXT NOT NULL,
        season INTEGER,
        team_id INTEGER,
        team_name TEXT,
        team_hashtag TEXT,
        games INTEGER,
        pa INTEGER,
        ab INTEGER,
        r INTEGER,
        h INTEGER,
        singles INTEGER,
        doubles INTEGER,
        triples INTEGER,
        hr INTEGER,
        rbi INTEGER,
        bb INTEGER,
        sac INTEGER,
        so INTEGER,
        roe INTEGER,
        avg REAL,
        obp REAL,
        slg REAL,
        ops REAL,
        hr_rate REAL,
        k_rate REAL,
        xbh INTEGER,
        total_bases INTEGER,
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    );

    CREATE TABLE IF NOT EXISTS pitching_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        player_hashtag TEXT NOT NULL,
        season INTEGER,
        team_id INTEGER,
        team_name TEXT,
        team_hashtag TEXT,
        w INTEGER,
        l INTEGER,
        era REAL,
        g INTEGER,
        gs INTEGER,
        sv INTEGER,
        sho INTEGER,
        ip REAL,
        bf INTEGER,
        ha INTEGER,
        opp_r INTEGER,
        opp_hr INTEGER,
        k INTEGER,
        k_per_6 REAL,
        opp_bb INTEGER,
        opp_bb_per_6 REAL,
        baa REAL,
        whip REAL,
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    );

    CREATE TABLE IF NOT EXISTS fielding_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER NOT NULL,
        player_hashtag TEXT NOT NULL,
        season INTEGER,
        team_id INTEGER,
        team_name TEXT,
        team_hashtag TEXT,
        chances INTEGER,
        put_outs INTEGER,
        errors INTEGER,
        fld_pct REAL,
        FOREIGN KEY (player_id) REFERENCES players(player_id)
    );
    """)
    conn.commit()


def api_get(path, retries=3):
    url = f"{BASE_URL}{path}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=None)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                return None
            else:
                print(f"  [WARN] {url} returned {r.status_code}")
                time.sleep(2)
        except Exception as e:
            print(f"  [ERROR] {url}: {e}")
            time.sleep(2)
    return None


def scrape_player(conn, name):
    print(f"\n{'='*50}")
    print(f"Scraping: {name}")

    # 1. Get player metadata
    player_data = api_get(f"/players/{name}")
    if not player_data:
        print(f"  [ERROR] Could not fetch player data for {name}")
        return False

    meta = player_data.get('metadata', {})
    player_id = meta.get('playerId')
    if not player_id:
        print(f"  [ERROR] No playerId found for {name}")
        return False

    print(f"  Player ID: {player_id}")
    print(f"  Team: {meta.get('teamName')} | Active: {meta.get('isActive')} | Last year: {meta.get('lastYear')}")
    print(f"  Seasons: {player_data.get('years', [])}")

    # 2. Insert/update player record
    pic_url = meta.get('picUrl', '')
    if pic_url and not pic_url.startswith('http'):
        pic_url = 'https://hrltwincities.com' + pic_url
    team_logo = meta.get('teamLogoUrl', '')
    if team_logo and not team_logo.startswith('http'):
        team_logo = 'https://hrltwincities.com' + team_logo

    conn.execute("""
        INSERT OR REPLACE INTO players
        (hashtag, nickname, player_id, team_id, team_name, is_active, last_year, pic_url, team_logo_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name,
        meta.get('nickname', name),
        player_id,
        meta.get('teamId'),
        meta.get('teamName'),
        1 if meta.get('isActive') else 0,
        meta.get('lastYear'),
        pic_url,
        team_logo
    ))
    conn.commit()

    # 3. Scrape batting stats
    batting = api_get(f"/players/{player_id}/stats/hitting/career")
    batting_rows = 0
    if batting and batting.get('stats'):
        for s in batting['stats']:
            conn.execute("""
                INSERT INTO batting_stats
                (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                 games, pa, ab, r, h, singles, doubles, triples, hr, rbi, bb, sac, so, roe,
                 avg, obp, slg, ops, hr_rate, k_rate, xbh, total_bases)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id, name,
                s.get('season'), s.get('tmId'), s.get('tmShrtNm'), s.get('teamHashtag'),
                s.get('gBat'), s.get('pa'), s.get('ab'), s.get('r'), s.get('h'),
                s.get('singles'), s.get('doubles'), s.get('triples'), s.get('hr'),
                s.get('rbi'), s.get('bb'), s.get('sac'), s.get('so'), s.get('roe'),
                s.get('avg'), s.get('obp'), s.get('slg'), s.get('ops'),
                s.get('hRr'), s.get('kr'), s.get('xbh'), s.get('totalBases')
            ))
            batting_rows += 1
        conn.commit()
    print(f"  Batting: {batting_rows} season rows inserted")

    # 4. Scrape pitching stats
    pitching = api_get(f"/players/{player_id}/stats/pitching/career")
    pitching_rows = 0
    if pitching and pitching.get('stats'):
        for s in pitching['stats']:
            # Only insert seasons with actual pitching activity
            conn.execute("""
                INSERT INTO pitching_stats
                (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                 w, l, era, g, gs, sv, sho, ip, bf, ha, opp_r, opp_hr,
                 k, k_per_6, opp_bb, opp_bb_per_6, baa, whip)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id, name,
                s.get('season'), s.get('tmId'), s.get('tmShrtNm'), s.get('teamHashtag'),
                s.get('w'), s.get('l'), s.get('era'), s.get('gPit'), s.get('gsPit'),
                s.get('sv'), s.get('sho'), s.get('ip'), s.get('bf'), s.get('ha'),
                s.get('oppR'), s.get('oppHR'), s.get('k'), s.get('k6'),
                s.get('oppBB'), s.get('oppBB6'), s.get('baa'), s.get('whip')
            ))
            pitching_rows += 1
        conn.commit()
    print(f"  Pitching: {pitching_rows} season rows inserted")

    # 5. Scrape fielding stats
    fielding = api_get(f"/players/{player_id}/stats/fielding/career")
    fielding_rows = 0
    if fielding and fielding.get('stats'):
        for s in fielding['stats']:
            conn.execute("""
                INSERT INTO fielding_stats
                (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                 chances, put_outs, errors, fld_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player_id, name,
                s.get('season'), s.get('tmId'), s.get('tmNm'), s.get('teamHashtag'),
                s.get('chncs'), s.get('po'), s.get('e'), s.get('fldPct')
            ))
            fielding_rows += 1
        conn.commit()
    print(f"  Fielding: {fielding_rows} season rows inserted")

    time.sleep(0.5)  # Be polite to the server
    return True


def print_summary(conn):
    print("\n" + "="*80)
    print("VIBES SAVANT — SCRAPE SUMMARY")
    print("="*80)

    players = conn.execute("""
        SELECT p.hashtag, p.player_id, p.team_name, p.last_year
        FROM players p
        ORDER BY p.hashtag
    """).fetchall()

    for hashtag, player_id, team_name, last_year in players:
        print(f"\n{hashtag} (ID: {player_id}, Team: {team_name}, Last: {last_year})")

        # Batting summary
        bat = conn.execute("""
            SELECT COUNT(*) as seasons,
                   SUM(hr) as career_hr,
                   SUM(h) as career_h,
                   SUM(ab) as career_ab,
                   SUM(games) as career_g,
                   MIN(season) as first_season,
                   MAX(season) as last_season
            FROM batting_stats WHERE player_id = ?
        """, (player_id,)).fetchone()

        if bat and bat[0] > 0:
            seasons, hr, h, ab, g, first, last = bat
            avg = round(h / ab, 3) if ab and ab > 0 else 0
            print(f"  Batting: {seasons} seasons ({first}-{last}) | {g}G | {hr}HR | .{int(avg*1000):03d} AVG | {h}H/{ab}AB")

        # Pitching summary (only non-zero seasons)
        pit = conn.execute("""
            SELECT COUNT(*) as seasons,
                   SUM(g) as total_g,
                   SUM(ip) as total_ip,
                   SUM(k) as total_k,
                   SUM(w) as total_w,
                   SUM(l) as total_l,
                   SUM(sv) as total_sv
            FROM pitching_stats WHERE player_id = ? AND ip > 0
        """, (player_id,)).fetchone()

        if pit and pit[0] > 0:
            seasons, g, ip, k, w, l, sv = pit
            era_row = conn.execute("""
                SELECT SUM(opp_r * 6.0) / NULLIF(SUM(ip), 0) as career_era
                FROM pitching_stats WHERE player_id = ? AND ip > 0
            """, (player_id,)).fetchone()
            era = round(era_row[0], 2) if era_row and era_row[0] else 0
            print(f"  Pitching: {seasons} active seasons | {g}G | {ip:.1f}IP | {w}W-{l}L | {sv}SV | {k}K | {era:.2f} ERA")

        # Fielding summary
        fld = conn.execute("""
            SELECT COUNT(*) as seasons,
                   SUM(chances) as total_chances,
                   SUM(errors) as total_errors,
                   MIN(season) as first_season,
                   MAX(season) as last_season
            FROM fielding_stats WHERE player_id = ?
        """, (player_id,)).fetchone()

        if fld and fld[0] > 0:
            seasons, chances, errors, first, last = fld
            fld_pct = round(1 - (errors / chances), 3) if chances and chances > 0 else 1.0
            print(f"  Fielding: {seasons} seasons ({first}-{last}) | {chances} chances | {errors} errors | .{int(fld_pct*1000):03d} FLD%")

    # Overall DB counts
    print("\n" + "="*80)
    print("DATABASE TOTALS")
    for table in ['players', 'batting_stats', 'pitching_stats', 'fielding_stats']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")


def main():
    import os
    # Remove existing DB to start fresh
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    print(f"Created {DB_PATH}")

    success_count = 0
    for name in PLAYERS:
        if scrape_player(conn, name):
            success_count += 1

    print(f"\n{'='*50}")
    print(f"Scraped {success_count}/{len(PLAYERS)} players successfully")

    print_summary(conn)
    conn.close()


if __name__ == '__main__':
    main()
