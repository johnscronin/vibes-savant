import sqlite3, re
from collections import defaultdict

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'

def normalize(name):
    if not name:
        return ''
    n = name.lower().strip()
    return ' '.join(n.replace('.','').replace("'",'').replace('-',' ').split())

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Build HQ batter lookup: {season: {normalized_name: True}}
    hq_batter_lookup = defaultdict(dict)
    hq_cutoffs_ops = {}
    for row in conn.execute('SELECT season, batter_name, batter_name_normalized, cutoff_ops FROM hq_batters').fetchall():
        s = int(row['season'])
        hq_batter_lookup[s][normalize(row['batter_name'])] = True
        hq_batter_lookup[s][row['batter_name_normalized']] = True
        hq_cutoffs_ops[s] = row['cutoff_ops']

    name_map = {}
    for row in conn.execute('SELECT original_name, normalized_name FROM name_mappings').fetchall():
        name_map[normalize(row['original_name'])] = normalize(row['normalized_name'])

    def is_hq_batter(batter_name, season):
        s = int(season)
        n = normalize(batter_name)
        if n in hq_batter_lookup.get(s, {}):
            return True
        mapped = name_map.get(n)
        if mapped and mapped in hq_batter_lookup.get(s, {}):
            return True
        return False

    # Get all BvP rows (non-Career)
    rows = conn.execute("""
        SELECT opposing_pitcher, player_name, CAST(season AS INTEGER) as season_int,
               ab, h, doubles, triples, hr, rbi, bb, so, tab_type
        FROM batter_vs_pitcher
        WHERE season != 'Career'
        ORDER BY opposing_pitcher, season, tab_type
    """).fetchall()

    # Build pitcher -> season -> batter -> stats dict
    # Prefer combined tab over regular
    pitcher_matchups = defaultdict(lambda: defaultdict(dict))
    for row in rows:
        pitcher = row['opposing_pitcher']
        season = row['season_int']
        batter = row['player_name']

        if not is_hq_batter(batter, season):
            continue

        tab = row['tab_type']
        existing = pitcher_matchups[pitcher][season].get(batter)
        if existing and existing.get('tab_type') == 'combined' and tab == 'regular':
            continue

        pitcher_matchups[pitcher][season][batter] = {
            'tab_type': tab,
            'ab': row['ab'] or 0, 'h': row['h'] or 0,
            'doubles': row['doubles'] or 0, 'triples': row['triples'] or 0,
            'hr': row['hr'] or 0, 'rbi': row['rbi'] or 0,
            'bb': row['bb'] or 0, 'so': row['so'] or 0,
        }

    # Delete existing vs_hq_hitter records
    conn.execute("DELETE FROM hq_opponent_splits WHERE split_type='vs_hq_hitter'")

    inserted = 0
    qualified = 0

    for pitcher, season_dict in pitcher_matchups.items():
        for season, batter_dict in season_dict.items():
            tot_ab = sum(v['ab'] for v in batter_dict.values())
            tot_h = sum(v['h'] for v in batter_dict.values())
            tot_2b = sum(v['doubles'] for v in batter_dict.values())
            tot_3b = sum(v['triples'] for v in batter_dict.values())
            tot_hr = sum(v['hr'] for v in batter_dict.values())
            tot_rbi = sum(v['rbi'] for v in batter_dict.values())
            tot_bb = sum(v['bb'] for v in batter_dict.values())
            tot_so = sum(v['so'] for v in batter_dict.values())
            tot_pa = tot_ab + tot_bb

            if tot_pa == 0:
                continue

            # For pitchers: avg = BAA, obp = OBP against
            baa = round(tot_h / tot_ab, 3) if tot_ab > 0 else None
            obp_against = round((tot_h + tot_bb) / (tot_ab + tot_bb), 3) if (tot_ab + tot_bb) > 0 else None
            singles = max(0, tot_h - tot_2b - tot_3b - tot_hr)
            slg_against = round((singles + tot_2b*2 + tot_3b*3 + tot_hr*4) / tot_ab, 3) if tot_ab > 0 else None
            ops_against = round((obp_against or 0) + (slg_against or 0), 3) if obp_against is not None and slg_against is not None else None
            k_pct = round(tot_so / tot_pa, 3) if tot_pa > 0 else None
            bb_pct = round(tot_bb / tot_pa, 3) if tot_pa > 0 else None
            bb_k = round(tot_bb / tot_so, 2) if tot_so > 0 else None
            babip_denom = tot_ab - tot_so - tot_hr
            babip = round((tot_h - tot_hr) / babip_denom, 3) if babip_denom > 0 else None

            qualifies = 1 if tot_pa >= 15 else 0

            conn.execute('''
                INSERT INTO hq_opponent_splits
                (player_name, season, split_type, pa, ab, h, doubles, triples, hr, rbi, bb, so,
                 avg, obp, slg, ops, bb_pct, k_pct, bb_k, iso, babip, qualifies)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (pitcher, season, 'vs_hq_hitter', tot_pa, tot_ab, tot_h, tot_2b, tot_3b,
                  tot_hr, tot_rbi, tot_bb, tot_so, baa, obp_against, slg_against, ops_against,
                  bb_pct, k_pct, bb_k, None, babip, qualifies))
            inserted += 1
            if qualifies:
                qualified += 1

    conn.commit()
    print(f'HQ pitching splits: {inserted} total, {qualified} qualified (15+ BF)')

    print('\nBy season:')
    for r in conn.execute("""
        SELECT season, COUNT(*) as pitchers, SUM(qualifies) as qualified
        FROM hq_opponent_splits WHERE split_type='vs_hq_hitter'
        GROUP BY season ORDER BY season
    """).fetchall():
        print(f'  {r[0]}: {r[1]} pitchers, {r[2]} qualified')

    conn.close()

if __name__ == '__main__':
    main()
