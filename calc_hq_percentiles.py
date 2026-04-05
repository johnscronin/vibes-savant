import sqlite3

DB_PATH = '/Users/Cronin/Desktop/JOHN AI WORK/vibes_savant.db'

def calc_pct(val, pool_vals, higher_is_better=True):
    if val is None or not pool_vals:
        return None, None
    if higher_is_better:
        worse = sum(1 for v in pool_vals if v < val)
    else:
        worse = sum(1 for v in pool_vals if v > val)
    pct = max(1, min(99, round(worse / len(pool_vals) * 100)))
    return pct, len(pool_vals)

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Delete old HQ percentiles
    conn.execute("DELETE FROM percentile_rankings WHERE stat_type IN ('vs_hq_pitcher', 'vs_hq_hitter')")

    # BATTING HQ PERCENTILES (vs_hq_pitcher)
    bat_stats = [
        ('hq_ops',    'ops',    True),
        ('hq_obp',    'obp',    True),
        ('hq_avg',    'avg',    True),
        ('hq_slg',    'slg',    True),
        ('hq_bb_pct', 'bb_pct', True),
        ('hq_k_pct',  'k_pct',  False),  # lower = better
        ('hq_bb_k',   'bb_k',   True),
        ('hq_iso',    'iso',    True),
        ('hq_babip',  'babip',  True),
    ]

    for season in range(2004, 2026):
        # Qualified pool
        qual_rows = conn.execute('''
            SELECT player_name, ops, obp, avg, slg, bb_pct, k_pct, bb_k, iso, babip
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_pitcher' AND qualifies=1 AND season=?
        ''', (season,)).fetchall()
        qual_rows = [dict(r) for r in qual_rows]

        # All players with any HQ data (for ghost bars)
        all_rows = conn.execute('''
            SELECT player_name, ops, obp, avg, slg, bb_pct, k_pct, bb_k, iso, babip, qualifies, pa
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_pitcher' AND season=?
        ''', (season,)).fetchall()
        all_rows = [dict(r) for r in all_rows]

        if not qual_rows:
            if all_rows:
                print(f'{season} batting: {len(all_rows)} players but 0 qualified (pool too thin)')
            continue

        pool_size = len(qual_rows)
        if pool_size < 10:
            print(f'{season} batting HQ: thin pool ({pool_size} players)')
        else:
            print(f'{season} batting HQ: pool={pool_size}')

        for row in all_rows:
            player = row['player_name']
            is_qual = row['qualifies'] == 1

            for stat_name, col, hib in bat_stats:
                val = row.get(col)
                if val is None:
                    continue

                pool_vals = [r[col] for r in qual_rows if r.get(col) is not None]
                if is_qual:
                    pct, ps = calc_pct(val, pool_vals, hib)
                    est_pct = None
                else:
                    pct = None
                    est_pct, ps = calc_pct(val, pool_vals, hib)

                conn.execute('''
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                ''', (player, season, stat_name, val, pct, est_pct, 'vs_hq_pitcher',
                      1 if is_qual else 0, pool_size, 'min 15 PA vs HQ pitchers'))

    # PITCHING HQ PERCENTILES (vs_hq_hitter)
    for season in range(2004, 2026):
        qual_rows = conn.execute('''
            SELECT player_name, avg, obp, slg, ops, bb_pct, k_pct, bb_k, babip, pa, h, bb
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_hitter' AND qualifies=1 AND season=?
        ''', (season,)).fetchall()
        qual_rows = [dict(r) for r in qual_rows]

        all_rows = conn.execute('''
            SELECT player_name, avg, obp, slg, ops, bb_pct, k_pct, bb_k, babip, qualifies, pa, h, bb
            FROM hq_opponent_splits
            WHERE split_type='vs_hq_hitter' AND season=?
        ''', (season,)).fetchall()
        all_rows = [dict(r) for r in all_rows]

        if not qual_rows:
            continue

        # Compute WHIP for pool
        def get_whip(r):
            pa = r.get('pa') or 0
            h = r.get('h') or 0
            bb = r.get('bb') or 0
            ip = pa / 3.0 if pa > 0 else 0
            return round((h + bb) / ip, 3) if ip > 0 else None

        for r in qual_rows:
            r['whip_calc'] = get_whip(r)
        for r in all_rows:
            r['whip_calc'] = get_whip(r)

        pool_size = len(qual_rows)
        if pool_size < 10:
            print(f'{season} pitching HQ: thin pool ({pool_size})')
        else:
            print(f'{season} pitching HQ: pool={pool_size}')

        pit_stats_full = [
            ('hqpit_baa',        'avg',       False),
            ('hqpit_obp',        'obp',       False),
            ('hqpit_pit_k_pct',  'k_pct',     True),
            ('hqpit_pit_bb_pct', 'bb_pct',    False),
            ('hqpit_pit_babip',  'babip',      False),
            ('hqpit_whip',       'whip_calc', False),
        ]

        for row in all_rows:
            player = row['player_name']
            is_qual = row['qualifies'] == 1

            for stat_name, col, hib in pit_stats_full:
                val = row.get(col)
                if val is None:
                    continue

                pool_vals = [r[col] for r in qual_rows if r.get(col) is not None]
                if is_qual:
                    pct, ps = calc_pct(val, pool_vals, hib)
                    est_pct = None
                else:
                    pool_vals = [r[col] for r in qual_rows if r.get(col) is not None]
                    pct = None
                    est_pct, ps = calc_pct(val, pool_vals, hib)

                conn.execute('''
                    INSERT OR REPLACE INTO percentile_rankings
                    (player_name, season, stat_name, stat_value, percentile, estimated_percentile,
                     stat_type, qualified, pool_size, qualifier_text)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                ''', (player, season, stat_name, val, pct, est_pct, 'vs_hq_hitter',
                      1 if is_qual else 0, pool_size, 'min 15 BF vs HQ batters'))

    conn.commit()

    # Summary
    for r in conn.execute("SELECT stat_type, COUNT(*), COUNT(CASE WHEN qualified=1 THEN 1 END) as qual FROM percentile_rankings WHERE stat_type IN ('vs_hq_pitcher','vs_hq_hitter') GROUP BY stat_type").fetchall():
        print(f'{r[0]}: {r[1]} records, {r[2]} qualified')

    # 2025 pools
    print('\n2025 batting HQ pool:', conn.execute("SELECT COUNT(*) FROM hq_opponent_splits WHERE split_type='vs_hq_pitcher' AND qualifies=1 AND season=2025").fetchone()[0])
    print('2025 pitching HQ pool:', conn.execute("SELECT COUNT(*) FROM hq_opponent_splits WHERE split_type='vs_hq_hitter' AND qualifies=1 AND season=2025").fetchone()[0])

    conn.close()

if __name__ == '__main__':
    main()
