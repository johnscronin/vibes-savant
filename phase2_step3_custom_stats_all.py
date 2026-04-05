#!/usr/bin/env python3
"""
Phase 2 Step 3 — Calculate custom stats for ALL players in batting_stats/pitching_stats.
Extended version of calculate_custom_stats.py that handles all players (not just Vibes).
Uses INSERT OR REPLACE — safe to re-run.
"""

import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')


def ensure_custom_stats_table(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS custom_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT NOT NULL,
        season INTEGER NOT NULL,
        ops_plus        REAL,
        patience_index  REAL,
        ab_hr           REAL,
        bb_k            REAL,
        bb_pct          REAL,
        k_pct           REAL,
        iso             REAL,
        bat_babip       REAL,
        rc              REAL,
        era_plus        REAL,
        k_per_6         REAL,
        bb_per_6        REAL,
        pit_k_pct       REAL,
        pit_bb_pct      REAL,
        pit_babip       REAL,
        lob_pct         REAL,
        bat_qualified   INTEGER DEFAULT 0,
        pit_qualified   INTEGER DEFAULT 0,
        UNIQUE(player_name, season)
    )
    """)
    conn.commit()


def get_league_batting_avgs(conn):
    seasons = {}
    rows = conn.execute("""
        SELECT season,
               AVG(ops)                                         AS avg_ops,
               SUM(bb) * 1.0 / NULLIF(SUM(ab) + SUM(bb), 0)   AS league_bb_pa
        FROM league_batting_stats WHERE ops IS NOT NULL GROUP BY season
    """).fetchall()
    for r in rows:
        seasons[r[0]] = {'avg_ops': r[1], 'league_bb_pa': r[2]}
    return seasons


def get_league_pitching_avgs(conn):
    seasons = {}
    rows = conn.execute("""
        SELECT season, AVG(era) AS avg_era
        FROM league_pitching_stats WHERE era IS NOT NULL GROUP BY season
    """).fetchall()
    for r in rows:
        seasons[r[0]] = {'avg_era': r[1]}
    return seasons


def get_season_qualifiers(conn):
    rows = conn.execute(
        "SELECT season, batting_min_pa, pitching_min_ip, pitching_min_g FROM season_qualifiers"
    ).fetchall()
    return {r[0]: {'batting_min_pa': r[1], 'pitching_min_ip': r[2], 'pitching_min_g': r[3]}
            for r in rows}


def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_custom_stats_table(conn)

    league_bat  = get_league_batting_avgs(conn)
    league_pit  = get_league_pitching_avgs(conn)
    qualifiers  = get_season_qualifiers(conn)

    # Get ALL unique players in batting_stats
    all_players = [r[0] for r in conn.execute(
        "SELECT DISTINCT player_hashtag FROM batting_stats ORDER BY player_hashtag"
    ).fetchall()]
    # Also include any pitcher-only players
    pit_only = [r[0] for r in conn.execute("""
        SELECT DISTINCT player_hashtag FROM pitching_stats
        WHERE player_hashtag NOT IN (SELECT DISTINCT player_hashtag FROM batting_stats)
        ORDER BY player_hashtag
    """).fetchall()]
    all_players = all_players + pit_only

    print(f"Processing custom stats for {len(all_players)} players...")

    rows_inserted = 0
    for idx, player in enumerate(all_players):
        pid_row = conn.execute("SELECT player_id FROM players WHERE hashtag=?", (player,)).fetchone()
        pid = pid_row[0] if pid_row else None

        # Batting seasons
        bat_seasons = []
        if pid:
            bat_seasons = conn.execute(
                "SELECT season, ops, hr, ab, bb, so, pa, h, slg, avg, doubles, triples, sac, total_bases "
                "FROM batting_stats WHERE player_id=?", (pid,)
            ).fetchall()
        else:
            bat_seasons = conn.execute(
                "SELECT season, ops, hr, ab, bb, so, pa, h, slg, avg, doubles, triples, sac, total_bases "
                "FROM batting_stats WHERE player_hashtag=?", (player,)
            ).fetchall()

        # Pitching seasons
        pit_seasons = []
        if pid:
            pit_seasons = conn.execute(
                "SELECT season, era, ip, g, k_per_6, opp_bb_per_6, k, ha, opp_bb, opp_hr, opp_r "
                "FROM pitching_stats WHERE player_id=? AND ip > 0", (pid,)
            ).fetchall()
        else:
            pit_seasons = conn.execute(
                "SELECT season, era, ip, g, k_per_6, opp_bb_per_6, k, ha, opp_bb, opp_hr, opp_r "
                "FROM pitching_stats WHERE player_hashtag=? AND ip > 0", (player,)
            ).fetchall()

        pit_map = {r[0]: {
            'era': r[1], 'ip': r[2], 'g': r[3], 'k_per_6': r[4], 'bb_per_6': r[5],
            'k': r[6], 'ha': r[7], 'opp_bb': r[8], 'opp_hr': r[9], 'opp_r': r[10]
        } for r in pit_seasons}

        all_s = set(r[0] for r in bat_seasons) | set(pit_map.keys())

        for season in sorted(all_s):
            rec = {
                'player_name': player, 'season': season,
                'ops_plus': None, 'patience_index': None,
                'ab_hr': None, 'bb_k': None, 'bb_pct': None, 'k_pct': None,
                'iso': None, 'bat_babip': None, 'rc': None,
                'era_plus': None, 'k_per_6': None, 'bb_per_6': None,
                'pit_k_pct': None, 'pit_bb_pct': None, 'pit_babip': None, 'lob_pct': None,
                'bat_qualified': 0, 'pit_qualified': 0,
            }

            bat = next((r for r in bat_seasons if r[0] == season), None)
            if bat:
                (_, ops, hr, ab, bb, so, pa, h, slg, avg, doubles, triples, sac, total_bases) = bat

                # Qualification
                q = qualifiers.get(season, {})
                min_pa = q.get('batting_min_pa')
                bat_qual = bool(pa and min_pa and pa >= min_pa) if min_pa else True
                rec['bat_qualified'] = 1 if bat_qual else 0

                if hr and hr > 0 and ab:
                    rec['ab_hr'] = round(ab / hr, 1)
                if so and so > 0:
                    rec['bb_k'] = round((bb or 0) / so, 2)
                else:
                    rec['bb_k'] = float(bb or 0)
                if pa and pa > 0:
                    rec['bb_pct'] = round((bb or 0) / pa, 3)
                    rec['k_pct']  = round((so or 0) / pa, 3)
                if slg is not None and avg is not None:
                    rec['iso'] = round(slg - avg, 3)
                if h is not None and ab:
                    den = (ab or 0) - (so or 0) - (hr or 0) + (sac or 0)
                    if den > 0:
                        rec['bat_babip'] = round(((h or 0) - (hr or 0)) / den, 3)
                if total_bases and total_bases > 0:
                    den2 = (ab or 0) + (bb or 0)
                    if den2 > 0:
                        rec['rc'] = round(((h or 0) + (bb or 0)) * total_bases / den2, 1)

                lb = league_bat.get(season, {})
                avg_ops = lb.get('avg_ops')
                if bat_qual and ops is not None and avg_ops and avg_ops > 0:
                    rec['ops_plus'] = round(100 * ops / avg_ops)
                league_bb_pa = lb.get('league_bb_pa')
                if bat_qual and pa and pa > 0 and league_bb_pa and league_bb_pa > 0:
                    rec['patience_index'] = round(100 * (bb or 0) / pa / league_bb_pa, 1)

            pit = pit_map.get(season)
            if pit:
                q = qualifiers.get(season, {})
                min_ip = q.get('pitching_min_ip') or 0
                min_g  = q.get('pitching_min_g') or 0
                ip = pit.get('ip') or 0
                g  = pit.get('g') or 0
                pit_qual = (ip >= min_ip or g >= min_g) if (min_ip or min_g) else True
                rec['pit_qualified'] = 1 if pit_qual else 0

                k = pit.get('k') or 0
                ha = pit.get('ha') or 0
                opp_bb = pit.get('opp_bb') or 0
                opp_hr = pit.get('opp_hr') or 0
                opp_r  = pit.get('opp_r') or 0

                if pit.get('k_per_6') is not None:
                    rec['k_per_6'] = pit['k_per_6']
                if pit.get('bb_per_6') is not None:
                    rec['bb_per_6'] = pit['bb_per_6']

                bf_approx = ip * 3 + ha + opp_bb
                if bf_approx > 0:
                    rec['pit_k_pct']  = round(k / bf_approx, 3)
                    rec['pit_bb_pct'] = round(opp_bb / bf_approx, 3)

                pit_bip_den = ip * 3 + ha - k - opp_hr
                if pit_bip_den > 0:
                    rec['pit_babip'] = round((ha - opp_hr) / pit_bip_den, 3)

                lob_den = ha + opp_bb - 1.4 * opp_hr
                lob_num = ha + opp_bb - opp_r
                if bf_approx >= 20 and lob_den > 0:
                    raw_lob = lob_num / lob_den
                    if 0 <= raw_lob <= 1:
                        rec['lob_pct'] = round(raw_lob, 3)

                lp = league_pit.get(season, {})
                avg_era = lp.get('avg_era')
                era = pit.get('era')
                if pit_qual and era and era > 0 and avg_era and avg_era > 0:
                    rec['era_plus'] = round(100 * avg_era / era)

            conn.execute("""
                INSERT OR REPLACE INTO custom_stats
                  (player_name, season, ops_plus, patience_index, ab_hr, bb_k, bb_pct, k_pct,
                   iso, bat_babip, rc, era_plus, k_per_6, bb_per_6, pit_k_pct, pit_bb_pct,
                   pit_babip, lob_pct, bat_qualified, pit_qualified)
                VALUES (:player_name, :season, :ops_plus, :patience_index, :ab_hr, :bb_k,
                        :bb_pct, :k_pct, :iso, :bat_babip, :rc, :era_plus, :k_per_6, :bb_per_6,
                        :pit_k_pct, :pit_bb_pct, :pit_babip, :lob_pct, :bat_qualified, :pit_qualified)
            """, rec)
            rows_inserted += 1

        if idx % 50 == 0:
            conn.commit()
            print(f"  [{idx}/{len(all_players)}] {rows_inserted} records so far...")

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM custom_stats").fetchone()[0]
    print(f"\nStep 3 complete. {total} total custom_stats rows ({rows_inserted} inserted/updated).")

    with open("progress_log.txt", "a") as f:
        f.write(f"\nStep 3 complete.\n  {total} player-seasons of custom stats calculated.\n")
    conn.close()


if __name__ == "__main__":
    main()
