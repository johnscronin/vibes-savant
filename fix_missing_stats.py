#!/usr/bin/env python3
"""
Fix missing pitching, batting, and fielding stats for players whose
individual API stats endpoint returned 500 or couldn't be scraped.
Populates from league_batting_stats, league_pitching_stats, league_fielding_stats.
"""

import sqlite3
import re
import os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')


def normalize(s):
    return re.sub(r'[^a-z0-9]', ' ', s.lower()).strip()


def build_name_lookup(conn):
    """Build name -> hashtag lookup with multiple fallbacks."""
    player_rows = conn.execute('SELECT hashtag, nickname, player_id FROM players').fetchall()
    norm_to_hash = {}
    hash_to_pid = {}
    for ht, nick, pid in player_rows:
        norm_to_hash[normalize(ht)] = ht
        if nick:
            norm_to_hash[normalize(nick)] = ht
        hash_to_pid[ht] = pid

    # Add explicit name_mappings
    nm = conn.execute('SELECT original_name, normalized_name FROM name_mappings').fetchall()
    nm_map = {r[0]: r[1] for r in nm}  # original -> normalized

    def lookup(name):
        """Return (hashtag, player_id) for a display name."""
        # Try direct hashtag match first
        if name in hash_to_pid:
            return name, hash_to_pid[name]
        # Try name_mappings
        if name in nm_map:
            norm = nm_map[name]
            ht = norm_to_hash.get(norm)
            if ht:
                return ht, hash_to_pid.get(ht)
        # Try normalizing directly
        norm = normalize(name)
        ht = norm_to_hash.get(norm)
        if ht:
            return ht, hash_to_pid.get(ht)
        return None, None

    return lookup, hash_to_pid


def fix_pitching_stats(conn, lookup):
    """Add missing pitching stats from league_pitching_stats."""
    # Get players already covered in pitching_stats
    covered = set(r[0] for r in conn.execute(
        'SELECT DISTINCT player_hashtag FROM pitching_stats WHERE ip>0'
    ).fetchall())
    # Also covered if they have any rows at all (to detect failed inserts from prior run)
    all_in_table = set(r[0] for r in conn.execute(
        'SELECT DISTINCT player_hashtag FROM pitching_stats'
    ).fetchall())

    league_names = conn.execute('SELECT DISTINCT player_name FROM league_pitching_stats').fetchall()
    added = 0
    skipped = 0

    for (name,) in league_names:
        ht, pid = lookup(name)
        if not ht:
            print(f'  SKIP pitching (no hashtag): {name!r}')
            skipped += 1
            continue
        if ht in covered:
            continue  # Already has pitching data

        # Get all seasons for this player
        rows = conn.execute('''
            SELECT season, player_name, team, w, l, era, g, gs, sho, sv,
                   ip, h, r, hr, bb, k, whip, baa, k_per_6, bb_per_6
            FROM league_pitching_stats
            WHERE player_name=? ORDER BY season
        ''', (name,)).fetchall()

        # Use 0 as sentinel player_id for players with no HRL API id (NOT NULL constraint)
        insert_pid = pid if pid is not None else 0

        for row in rows:
            season, pname, team, w, l, era, g, gs, sho, sv, ip, ha, opp_r, opp_hr, opp_bb, k, whip, baa, k_per_6, bb_per_6 = row
            if not ip or ip <= 0:
                continue
            bf_est = round(ip * 3.3)
            k_per_6_val = round(k * 6.0 / ip, 2) if ip > 0 else 0
            bb_per_6_val = round(opp_bb * 6.0 / ip, 2) if ip > 0 else 0

            conn.execute('''
                INSERT OR IGNORE INTO pitching_stats
                  (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                   w, l, era, g, gs, sv, sho, ip, bf, ha, opp_r, opp_hr, k, k_per_6,
                   opp_bb, opp_bb_per_6, baa, whip)
                VALUES (?,?,?,NULL,?,NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (insert_pid, ht, season, team,
                  w, l, era or 0, g, gs, sv, sho, ip, bf_est,
                  ha, opp_r, opp_hr, k, k_per_6_val, opp_bb, bb_per_6_val,
                  baa or 0, whip or 0))
            added += 1

        covered.add(ht)

    print(f'Pitching: added {added} rows, skipped {skipped} names')
    return added


def fix_batting_stats(conn, lookup):
    """Add missing batting stats from league_batting_stats."""
    covered = set(r[0] for r in conn.execute(
        'SELECT DISTINCT player_hashtag FROM batting_stats WHERE ab>0'
    ).fetchall())

    league_names = conn.execute('SELECT DISTINCT player_name FROM league_batting_stats').fetchall()
    added = 0
    skipped = 0

    for (name,) in league_names:
        ht, pid = lookup(name)
        if not ht:
            print(f'  SKIP batting (no hashtag): {name!r}')
            skipped += 1
            continue
        if ht in covered:
            continue

        rows = conn.execute('''
            SELECT season, player_name, team, g, ab, r, h, doubles, triples, hr,
                   rbi, bb, so, avg, obp, slg, ops
            FROM league_batting_stats
            WHERE player_name=? ORDER BY season
        ''', (name,)).fetchall()

        insert_pid = pid if pid is not None else 0

        for row in rows:
            season, pname, team, g, ab, r, h, doubles, triples, hr, rbi, bb, so, avg, obp, slg, ops = row
            if not ab or ab <= 0:
                continue
            singles = h - doubles - triples - hr
            pa = ab + bb
            xbh = doubles + triples + hr
            total_bases = singles + 2*doubles + 3*triples + 4*hr
            hr_rate = round(hr / ab * 100, 4) if ab > 0 else 0
            k_rate = round(so / pa, 4) if pa > 0 else 0

            conn.execute('''
                INSERT OR IGNORE INTO batting_stats
                  (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                   games, pa, ab, r, h, singles, doubles, triples, hr, rbi, bb,
                   sac, so, roe, avg, obp, slg, ops, hr_rate, k_rate, xbh, total_bases)
                VALUES (?,?,?,NULL,?,NULL,?,?,?,?,?,?,?,?,?,?,?,NULL,?,NULL,?,?,?,?,?,?,?,?)
            ''', (insert_pid, ht, season, team,
                  g, pa, ab, r, h, singles, doubles, triples, hr, rbi, bb,
                  so, avg or 0, obp or 0, slg or 0, ops or 0,
                  hr_rate, k_rate, xbh, total_bases))
            added += 1

        covered.add(ht)

    print(f'Batting: added {added} rows, skipped {skipped} names')
    return added


def fix_fielding_stats(conn, lookup):
    """Add missing fielding stats from league_fielding_stats."""
    covered = set(r[0] for r in conn.execute(
        'SELECT DISTINCT player_hashtag FROM fielding_stats WHERE chances>0'
    ).fetchall())

    league_names = conn.execute('SELECT DISTINCT player_name FROM league_fielding_stats').fetchall()
    added = 0
    skipped = 0

    for (name,) in league_names:
        ht, pid = lookup(name)
        if not ht:
            print(f'  SKIP fielding (no hashtag): {name!r}')
            skipped += 1
            continue
        if ht in covered:
            continue

        rows = conn.execute('''
            SELECT season, player_name, team, tc, po, errors, fld_pct
            FROM league_fielding_stats
            WHERE player_name=? ORDER BY season
        ''', (name,)).fetchall()

        insert_pid = pid if pid is not None else 0

        for row in rows:
            season, pname, team, tc, po, errors, fld_pct = row
            if not tc or tc <= 0:
                continue

            conn.execute('''
                INSERT OR IGNORE INTO fielding_stats
                  (player_id, player_hashtag, season, team_id, team_name, team_hashtag,
                   chances, put_outs, errors, fld_pct)
                VALUES (?,?,?,NULL,?,NULL,?,?,?,?)
            ''', (insert_pid, ht, season, team, tc, po, errors, fld_pct or 0))
            added += 1

        covered.add(ht)

    print(f'Fielding: added {added} rows, skipped {skipped} names')
    return added


def add_missing_name_mappings(conn):
    """Add explicit name mappings for Dr./Mr./Lil' players that normalize() can't handle."""
    mappings = [
        # (original_name_in_league_tables, normalized_form, player_hashtag)
        ('Dr. Dipshit', 'dr dipshit', 'DrDipshit'),
        ('Dr. Hate',    'dr hate',    'DrHate'),
        ('Dr. Jesus',   'dr jesus',   'DrJesus'),
        ('Dr. K',       'dr k',       'DrK'),
        ('Dr. Seuss',   'dr seuss',   'DrSeuss'),
        ('Dr. Z',       'dr z',       'DrZ'),
        ("Lil' Randall", 'lil randall', 'LilRandall'),
        ('Mr. Dobalina', 'mr dobalina', 'MrDobalina'),
        ('Mr. Fist',    'mr fist',    'Ackerman'),
        ('Mr. Giggles', 'mr giggles', 'MrGiggles'),
        ('Mr. Mariner', 'mr mariner', 'MrMariner'),
        ('Mr. Robot',   'mr robot',   'MrRobot'),
        # Display names vs hashtags
        ('Knooty Booty', 'knooty booty', 'KnootyBooty'),
        ("O'Bannion",   'o bannion',   None),  # Unknown - skip
    ]

    added = 0
    for orig, norm, ht in mappings:
        if ht is None:
            continue
        # Check if mapping already exists
        existing = conn.execute(
            'SELECT id FROM name_mappings WHERE original_name=?', (orig,)
        ).fetchone()
        if not existing:
            conn.execute(
                'INSERT INTO name_mappings (original_name, normalized_name, source) VALUES (?,?,?)',
                (orig, norm, 'manual_fix')
            )
            added += 1

    conn.commit()
    print(f'Added {added} name mappings')


def verify_psych(conn):
    """Verify Psych's stats are correct after fix."""
    print('\n=== PSYCH VERIFICATION ===')
    bat = conn.execute("SELECT season, games, ab, h, hr, avg, ops FROM batting_stats WHERE player_hashtag='Psych' ORDER BY season").fetchall()
    print(f'Batting rows: {len(bat)}')
    for r in bat:
        print(f'  {r}')

    pit = conn.execute("SELECT season, g, ip, w, l, era, whip FROM pitching_stats WHERE player_hashtag='Psych' AND ip>0 ORDER BY season").fetchall()
    print(f'Pitching rows (ip>0): {len(pit)}')
    for r in pit:
        print(f'  {r}')

    fld = conn.execute("SELECT season, chances, put_outs, errors, fld_pct FROM fielding_stats WHERE player_hashtag='Psych' ORDER BY season").fetchall()
    print(f'Fielding rows: {len(fld)}')
    for r in fld:
        print(f'  {r}')


def main():
    conn = sqlite3.connect(DB_PATH)

    print('Step 1: Adding missing name mappings...')
    add_missing_name_mappings(conn)

    # Rebuild lookup after adding mappings
    lookup, hash_to_pid = build_name_lookup(conn)

    print('\nStep 2: Fixing missing pitching stats...')
    fix_pitching_stats(conn, lookup)

    print('\nStep 3: Fixing missing batting stats...')
    fix_batting_stats(conn, lookup)

    print('\nStep 4: Fixing missing fielding stats...')
    fix_fielding_stats(conn, lookup)

    conn.commit()

    verify_psych(conn)

    # Final summary
    print('\n=== FINAL STATS COVERAGE ===')
    total = conn.execute('SELECT COUNT(*) FROM players').fetchone()[0]
    with_bat = conn.execute('SELECT COUNT(DISTINCT player_hashtag) FROM batting_stats WHERE ab>0').fetchone()[0]
    with_pit = conn.execute('SELECT COUNT(DISTINCT player_hashtag) FROM pitching_stats WHERE ip>0').fetchone()[0]
    with_fld = conn.execute('SELECT COUNT(DISTINCT player_hashtag) FROM fielding_stats WHERE chances>0').fetchone()[0]
    print(f'Total players: {total}')
    print(f'With batting data: {with_bat}')
    print(f'With pitching data: {with_pit}')
    print(f'With fielding data: {with_fld}')

    bat_rows = conn.execute('SELECT COUNT(*) FROM batting_stats').fetchone()[0]
    pit_rows = conn.execute('SELECT COUNT(*) FROM pitching_stats WHERE ip>0').fetchone()[0]
    fld_rows = conn.execute('SELECT COUNT(*) FROM fielding_stats WHERE chances>0').fetchone()[0]
    print(f'batting_stats rows: {bat_rows}')
    print(f'pitching_stats rows (ip>0): {pit_rows}')
    print(f'fielding_stats rows (chances>0): {fld_rows}')

    conn.close()
    print('\nDone.')


if __name__ == '__main__':
    main()
