import sqlite3, re
from collections import defaultdict

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'

def normalize(name):
    if not name:
        return ''
    n = name.lower().strip()
    n = n.replace('.', '').replace("'", '').replace('-', ' ')
    return ' '.join(n.split())

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Build HQ pitcher lookup: {season: {normalized_name: True}}
    hq_pitcher_lookup = defaultdict(dict)
    hq_cutoffs = {}
    for row in conn.execute('SELECT season, pitcher_name, pitcher_name_normalized, cutoff_whip FROM hq_pitchers').fetchall():
        s = int(row['season'])
        hq_pitcher_lookup[s][normalize(row['pitcher_name'])] = True
        hq_pitcher_lookup[s][row['pitcher_name_normalized']] = True
        hq_cutoffs[s] = row['cutoff_whip']

    # Build name_mappings lookup
    name_map = {}
    for row in conn.execute('SELECT original_name, normalized_name FROM name_mappings').fetchall():
        name_map[normalize(row['original_name'])] = normalize(row['normalized_name'])

    def is_hq_pitcher(pitcher_name, season):
        s = int(season) if season != 'Career' else 0
        if s == 0:
            return False
        n = normalize(pitcher_name)
        if n in hq_pitcher_lookup.get(s, {}):
            return True
        # Try name_mappings
        mapped = name_map.get(n)
        if mapped and mapped in hq_pitcher_lookup.get(s, {}):
            return True
        return False

    # Get all BvP rows (non-Career, use combined tab preferentially)
    rows = conn.execute("""
        SELECT player_name, CAST(season AS INTEGER) as season_int, season,
               opposing_pitcher, ab, h, doubles, triples, hr, rbi, bb, so, tab_type
        FROM batter_vs_pitcher
        WHERE season != 'Career'
        ORDER BY player_name, season, tab_type
    """).fetchall()

    # For each player-season, collect HQ matchups
    # Prefer 'combined' over 'regular' for the same player-season-pitcher combination
    hq_matchups = defaultdict(lambda: defaultdict(dict))  # player -> season -> pitcher -> stats
    for row in rows:
        player = row['player_name']
        season = row['season_int']
        pitcher = row['opposing_pitcher']

        if not is_hq_pitcher(pitcher, season):
            continue

        tab = row['tab_type']
        existing = hq_matchups[player][season].get(pitcher)
        # Prefer 'combined' tab; don't overwrite combined with regular
        if existing and existing.get('tab_type') == 'combined' and tab == 'regular':
            continue

        hq_matchups[player][season][pitcher] = {
            'tab_type': tab,
            'ab': row['ab'] or 0, 'h': row['h'] or 0,
            'doubles': row['doubles'] or 0, 'triples': row['triples'] or 0,
            'hr': row['hr'] or 0, 'rbi': row['rbi'] or 0,
            'bb': row['bb'] or 0, 'so': row['so'] or 0,
        }

    # Sanity check: overall stats for comparison
    overall_ops = {}
    for row in conn.execute('''
        SELECT player_hashtag, season, ops FROM batting_stats WHERE ops IS NOT NULL
    ''').fetchall():
        overall_ops[(row['player_hashtag'], row['season'])] = row['ops']

    # Also map nickname to hashtag for overall_ops lookup
    nick_to_hash = {r['nickname']: r['hashtag'] for r in conn.execute('SELECT nickname, hashtag FROM players').fetchall()}

    # Build HQ splits
    # Delete existing vs_hq_pitcher records
    conn.execute("DELETE FROM hq_opponent_splits WHERE split_type='vs_hq_pitcher'")

    inserted = 0
    qualified = 0
    warnings = []

    for player, season_dict in hq_matchups.items():
        for season, pitcher_dict in season_dict.items():
            # Aggregate across all HQ pitchers faced
            tot_ab = sum(v['ab'] for v in pitcher_dict.values())
            tot_h = sum(v['h'] for v in pitcher_dict.values())
            tot_2b = sum(v['doubles'] for v in pitcher_dict.values())
            tot_3b = sum(v['triples'] for v in pitcher_dict.values())
            tot_hr = sum(v['hr'] for v in pitcher_dict.values())
            tot_rbi = sum(v['rbi'] for v in pitcher_dict.values())
            tot_bb = sum(v['bb'] for v in pitcher_dict.values())
            tot_so = sum(v['so'] for v in pitcher_dict.values())
            tot_pa = tot_ab + tot_bb

            if tot_pa == 0:
                continue

            # Calculate rate stats
            avg = round(tot_h / tot_ab, 3) if tot_ab > 0 else None
            obp = round((tot_h + tot_bb) / (tot_ab + tot_bb), 3) if (tot_ab + tot_bb) > 0 else None
            singles = tot_h - tot_2b - tot_3b - tot_hr
            slg = round((singles + tot_2b*2 + tot_3b*3 + tot_hr*4) / tot_ab, 3) if tot_ab > 0 else None
            ops = round((obp or 0) + (slg or 0), 3) if obp is not None and slg is not None else None
            bb_pct = round(tot_bb / tot_pa, 3) if tot_pa > 0 else None
            k_pct = round(tot_so / tot_pa, 3) if tot_pa > 0 else None
            bb_k = round(tot_bb / tot_so, 2) if tot_so > 0 else None
            iso = round(slg - avg, 3) if slg is not None and avg is not None else None
            babip_denom = tot_ab - tot_so - tot_hr
            babip = round((tot_h - tot_hr) / babip_denom, 3) if babip_denom > 0 else None
            qualifies = 1 if tot_pa >= 15 else 0

            # Sanity check
            if ops is not None and qualifies:
                slug = nick_to_hash.get(player, player)
                overall = overall_ops.get((slug, season)) or overall_ops.get((player, season))
                if overall and ops > overall + 0.200:
                    warnings.append(f'WARNING: {player} {season} HQ OPS={ops:.3f} > overall OPS={overall:.3f}')

            conn.execute('''
                INSERT INTO hq_opponent_splits
                (player_name, season, split_type, pa, ab, h, doubles, triples, hr, rbi, bb, so,
                 avg, obp, slg, ops, bb_pct, k_pct, bb_k, iso, babip, qualifies)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (player, season, 'vs_hq_pitcher', tot_pa, tot_ab, tot_h, tot_2b, tot_3b,
                  tot_hr, tot_rbi, tot_bb, tot_so, avg, obp, slg, ops, bb_pct, k_pct,
                  bb_k, iso, babip, qualifies))
            inserted += 1
            if qualifies:
                qualified += 1

    conn.commit()
    print(f'\nHQ batting splits: {inserted} total, {qualified} qualified (15+ PA)')
    for w in warnings[:10]:
        print(w)

    # By season summary
    print('\nBy season:')
    for r in conn.execute("""
        SELECT season, COUNT(*) as players, SUM(qualifies) as qualified
        FROM hq_opponent_splits WHERE split_type='vs_hq_pitcher'
        GROUP BY season ORDER BY season
    """).fetchall():
        print(f'  {r[0]}: {r[1]} players, {r[2]} qualified')

    conn.close()

if __name__ == '__main__':
    main()
