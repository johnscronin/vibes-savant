#!/usr/bin/env python3
"""
Vibes Savant — Custom Stats Calculator
Computes OPS+, ERA+, AB/HR, BB/K, BB%, K%, ISO, BABIP, RC (batting)
and K%, BB%, BABIP, LOB% (pitching) for all Vibes players.
"""
import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant.db')

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]


def create_custom_stats_table(conn):
    conn.execute("DROP TABLE IF EXISTS custom_stats")
    conn.execute("""
    CREATE TABLE custom_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT NOT NULL,
        season INTEGER NOT NULL,
        -- Batting — qualified only
        ops_plus        REAL,
        patience_index  REAL,
        -- Batting — always (if PA > 0)
        ab_hr           REAL,   -- AB per HR; NULL if HR=0; lower is better
        bb_k            REAL,   -- BB/K walk-to-strikeout ratio
        bb_pct          REAL,   -- BB/PA walk rate (was bb_pa)
        k_pct           REAL,   -- SO/PA strikeout rate (was k_pa); lower is better
        iso             REAL,   -- SLG - AVG; higher is better
        bat_babip       REAL,   -- (H-HR)/(AB-SO-HR+SAC); higher is better
        rc              REAL,   -- (H+BB)*TB/(AB+BB); higher is better
        -- Pitching — qualified only
        era_plus        REAL,
        -- Pitching — always (if IP > 0)
        k_per_6         REAL,
        bb_per_6        REAL,
        pit_k_pct       REAL,   -- K/BF; higher is better
        pit_bb_pct      REAL,   -- BB/BF; lower is better
        pit_babip       REAL,   -- (HA-HR)/(IP*3+HA-K-HR); lower is better
        lob_pct         REAL,   -- (HA+BB-R)/(HA+BB-1.4*HR); higher is better
        -- Qualification flags
        bat_qualified   INTEGER DEFAULT 0,
        pit_qualified   INTEGER DEFAULT 0,
        UNIQUE(player_name, season)
    )
    """)
    conn.commit()


def get_league_batting_avgs(conn):
    """Pre-compute per-season league batting averages from qualified hitters."""
    seasons = {}
    rows = conn.execute("""
        SELECT season,
               AVG(ops)                                         AS avg_ops,
               SUM(bb) * 1.0 / NULLIF(SUM(ab) + SUM(bb), 0)   AS league_bb_pa
        FROM league_batting_stats
        WHERE ops IS NOT NULL
        GROUP BY season
    """).fetchall()
    for r in rows:
        seasons[r[0]] = {
            'avg_ops':      r[1],
            'league_bb_pa': r[2],
        }
    return seasons


def get_league_pitching_avgs(conn):
    """Pre-compute per-season league pitching ERA averages."""
    seasons = {}
    rows = conn.execute("""
        SELECT season, AVG(era) AS avg_era
        FROM league_pitching_stats
        WHERE era IS NOT NULL
        GROUP BY season
    """).fetchall()
    for r in rows:
        seasons[r[0]] = {'avg_era': r[1]}
    return seasons


def get_season_qualifiers(conn):
    rows = conn.execute(
        "SELECT season, batting_min_pa, pitching_min_ip, pitching_min_g FROM season_qualifiers"
    ).fetchall()
    qual = {}
    for r in rows:
        qual[r[0]] = {'batting_min_pa': r[1], 'pitching_min_ip': r[2], 'pitching_min_g': r[3]}
    return qual


def player_bat_qualified(conn, player, season, qualifiers):
    q = qualifiers.get(season, {})
    min_pa = q.get('batting_min_pa')
    if min_pa is None:
        return True
    row = conn.execute(
        "SELECT pa FROM batting_stats WHERE player_hashtag=? AND season=?", (player, season)
    ).fetchone()
    return bool(row and row[0] is not None and row[0] >= min_pa)


def player_pit_qualified(conn, player, season, qualifiers):
    q = qualifiers.get(season, {})
    min_ip = q.get('pitching_min_ip') or 0
    min_g  = q.get('pitching_min_g') or 0
    row = conn.execute(
        "SELECT ip, g FROM pitching_stats WHERE player_hashtag=? AND season=? AND ip > 0",
        (player, season)
    ).fetchone()
    if not row:
        return False
    ip, g = (row[0] or 0), (row[1] or 0)
    return ip >= min_ip or g >= min_g


def safe_div(num, den, ndigits=3):
    """Return round(num/den, ndigits) or None if den is 0/None."""
    if not den or den == 0:
        return None
    return round(num / den, ndigits)


def calculate_custom_stats():
    conn = sqlite3.connect(DB_PATH)
    create_custom_stats_table(conn)

    league_bat = get_league_batting_avgs(conn)
    league_pit = get_league_pitching_avgs(conn)
    qualifiers  = get_season_qualifiers(conn)

    rows_inserted = 0

    for player in VIBES_PLAYERS:
        pid_row = conn.execute("SELECT player_id FROM players WHERE hashtag=?", (player,)).fetchone()
        if not pid_row:
            continue
        pid = pid_row[0]

        # All batting seasons
        bat_seasons = conn.execute(
            "SELECT season, ops, hr, ab, bb, so, pa, h, slg, avg, doubles, triples, sac, total_bases "
            "FROM batting_stats WHERE player_id=?",
            (pid,)
        ).fetchall()

        # All pitching seasons
        pit_seasons = conn.execute(
            "SELECT season, era, ip, g, k_per_6, opp_bb_per_6, k, ha, opp_bb, opp_hr, opp_r "
            "FROM pitching_stats WHERE player_id=? AND ip > 0",
            (pid,)
        ).fetchall()
        pit_map = {r[0]: {
            'era': r[1], 'ip': r[2], 'g': r[3], 'k_per_6': r[4], 'bb_per_6': r[5],
            'k': r[6], 'ha': r[7], 'opp_bb': r[8], 'opp_hr': r[9], 'opp_r': r[10]
        } for r in pit_seasons}

        all_seasons = set(r[0] for r in bat_seasons) | set(pit_map.keys())

        for season in sorted(all_seasons):
            record = {
                'player_name': player, 'season': season,
                'ops_plus': None, 'patience_index': None,
                'ab_hr': None, 'bb_k': None, 'bb_pct': None, 'k_pct': None,
                'iso': None, 'bat_babip': None, 'rc': None,
                'era_plus': None,
                'k_per_6': None, 'bb_per_6': None,
                'pit_k_pct': None, 'pit_bb_pct': None, 'pit_babip': None, 'lob_pct': None,
                'bat_qualified': 0, 'pit_qualified': 0,
            }

            # ── Batting custom stats ─────────────────────────────
            bat = next((r for r in bat_seasons if r[0] == season), None)
            if bat:
                (s_val, ops, hr, ab, bb, so, pa, h,
                 slg, avg, doubles, triples, sac, total_bases) = bat

                bat_qual = player_bat_qualified(conn, player, season, qualifiers)
                record['bat_qualified'] = 1 if bat_qual else 0

                # AB/HR
                if hr and hr > 0 and ab and ab > 0:
                    record['ab_hr'] = round(ab / hr, 1)

                # BB/K
                if so and so > 0:
                    record['bb_k'] = round((bb or 0) / so, 2)
                else:
                    record['bb_k'] = float(bb or 0)

                # BB% (walk rate)
                if pa and pa > 0:
                    record['bb_pct'] = round((bb or 0) / pa, 3)

                # K% (strikeout rate — lower is better)
                if pa and pa > 0:
                    record['k_pct'] = round((so or 0) / pa, 3)

                # ISO = SLG - AVG
                if slg is not None and avg is not None:
                    record['iso'] = round(slg - avg, 3)

                # Batting BABIP = (H-HR)/(AB-SO-HR+SAC)
                if h is not None and hr is not None and ab is not None and so is not None:
                    den = (ab or 0) - (so or 0) - (hr or 0) + (sac or 0)
                    if den > 0:
                        record['bat_babip'] = round(((h or 0) - (hr or 0)) / den, 3)

                # RC = (H+BB)*TB/(AB+BB)
                if total_bases is not None and total_bases > 0:
                    den = (ab or 0) + (bb or 0)
                    if den > 0:
                        record['rc'] = round(((h or 0) + (bb or 0)) * total_bases / den, 1)

                # OPS+ — qualified only
                lb = league_bat.get(season, {})
                avg_ops = lb.get('avg_ops')
                if bat_qual and ops is not None and avg_ops and avg_ops > 0:
                    record['ops_plus'] = round(100 * ops / avg_ops)

                # Patience Index — qualified only
                league_bb_pa = lb.get('league_bb_pa')
                if bat_qual and pa and pa > 0 and league_bb_pa and league_bb_pa > 0:
                    player_bb_pa = (bb or 0) / pa
                    record['patience_index'] = round(100 * player_bb_pa / league_bb_pa, 1)

            # ── Pitching custom stats ────────────────────────────
            pit = pit_map.get(season)
            if pit:
                pit_qual = player_pit_qualified(conn, player, season, qualifiers)
                record['pit_qualified'] = 1 if pit_qual else 0

                ip = pit.get('ip') or 0
                k  = pit.get('k') or 0
                ha = pit.get('ha') or 0
                opp_bb = pit.get('opp_bb') or 0
                opp_hr = pit.get('opp_hr') or 0
                opp_r  = pit.get('opp_r') or 0

                # K/6 and BB/6
                if pit.get('k_per_6') is not None:
                    record['k_per_6'] = pit['k_per_6']
                if pit.get('bb_per_6') is not None:
                    record['bb_per_6'] = pit['bb_per_6']

                # BF approx = outs + H + BB = IP*3 + ha + opp_bb
                bf_approx = ip * 3 + ha + opp_bb

                # Pitcher K% = K / BF
                if bf_approx > 0:
                    record['pit_k_pct'] = round(k / bf_approx, 3)

                # Pitcher BB% = BB / BF
                if bf_approx > 0:
                    record['pit_bb_pct'] = round(opp_bb / bf_approx, 3)

                # Pitcher BABIP = (HA-HR)/(IP*3+HA-K-HR)
                pit_bip_den = ip * 3 + ha - k - opp_hr
                if pit_bip_den > 0:
                    record['pit_babip'] = round((ha - opp_hr) / pit_bip_den, 3)

                # LOB% = (HA+BB-R)/(HA+BB-1.4*HR)
                # Min 20 BF; cap 0-1
                lob_den = ha + opp_bb - 1.4 * opp_hr
                lob_num = ha + opp_bb - opp_r
                if bf_approx >= 20 and lob_den > 0:
                    raw_lob = lob_num / lob_den
                    if 0 <= raw_lob <= 1:
                        record['lob_pct'] = round(raw_lob, 3)

                # ERA+ — qualified only
                lp = league_pit.get(season, {})
                avg_era = lp.get('avg_era')
                if pit_qual and avg_era and avg_era > 0:
                    era = pit['era']
                    if era is None or era == 0:
                        record['era_plus'] = 999
                    else:
                        record['era_plus'] = round(100 * avg_era / era)

            conn.execute("""
                INSERT OR REPLACE INTO custom_stats
                (player_name, season,
                 ops_plus, patience_index,
                 ab_hr, bb_k, bb_pct, k_pct, iso, bat_babip, rc,
                 era_plus, k_per_6, bb_per_6,
                 pit_k_pct, pit_bb_pct, pit_babip, lob_pct,
                 bat_qualified, pit_qualified)
                VALUES
                (:player_name, :season,
                 :ops_plus, :patience_index,
                 :ab_hr, :bb_k, :bb_pct, :k_pct, :iso, :bat_babip, :rc,
                 :era_plus, :k_per_6, :bb_per_6,
                 :pit_k_pct, :pit_bb_pct, :pit_babip, :lob_pct,
                 :bat_qualified, :pit_qualified)
            """, record)
            rows_inserted += 1

        conn.commit()

    print(f"Inserted {rows_inserted} custom_stats rows for {len(VIBES_PLAYERS)} players")

    # ── Sanity check ──────────────────────────────────────────────
    print("\n=== SANITY CHECK ===")
    r = conn.execute("""
        SELECT cs.ops_plus, cs.era_plus, cs.ab_hr, cs.bb_k, cs.patience_index,
               cs.bb_pct, cs.k_pct, cs.iso, cs.bat_babip, cs.rc,
               cs.pit_k_pct, cs.pit_bb_pct, cs.pit_babip, cs.lob_pct,
               bs.ops, bs.pa
        FROM custom_stats cs
        JOIN batting_stats bs ON bs.player_hashtag = cs.player_name AND bs.season = cs.season
        WHERE cs.player_name='Epstein' AND cs.season=2025
    """).fetchone()
    if r:
        print(f"  Epstein 2025: OPS={r[14]:.3f} PA={r[15]}")
        print(f"    OPS+={r[0]}  ERA+={r[1]}")
        print(f"    AB/HR={r[2]}  BB/K={r[3]}  Patience={r[4]}")
        print(f"    BB%={r[5]:.1%}  K%={r[6]:.1%}")
        print(f"    ISO={r[7]}  BABIP={r[8]}  RC={r[9]}")
        print(f"    Pit K%={r[10]}  Pit BB%={r[11]}  Pit BABIP={r[12]}  LOB%={r[13]}")

    print("\n  Full 2025 custom stats (batting):")
    rows = conn.execute("""
        SELECT player_name, ops_plus, ab_hr, bb_k, bb_pct, k_pct, iso, bat_babip, rc
        FROM custom_stats WHERE season=2025 ORDER BY player_name
    """).fetchall()
    print(f"  {'Player':12s} {'OPS+':>6} {'AB/HR':>6} {'BB/K':>6} {'BB%':>6} {'K%':>6} {'ISO':>6} {'BABIP':>6} {'RC':>6}")
    for r in rows:
        def fmt(v, d=1): return f"{v:.{d}f}" if v is not None else "  —"
        print(f"  {r[0]:12s} {fmt(r[1],0):>6} {fmt(r[2]):>6} {fmt(r[3],2):>6} "
              f"{fmt(r[4]*100 if r[4] else None):>6} {fmt(r[5]*100 if r[5] else None):>6} "
              f"{fmt(r[6],3):>6} {fmt(r[7],3):>6} {fmt(r[8]):>6}")

    print("\n  Full 2025 custom stats (pitching):")
    rows = conn.execute("""
        SELECT player_name, era_plus, k_per_6, bb_per_6, pit_k_pct, pit_bb_pct, pit_babip, lob_pct
        FROM custom_stats WHERE season=2025 AND era_plus IS NOT NULL ORDER BY player_name
    """).fetchall()
    print(f"  {'Player':12s} {'ERA+':>6} {'K/6':>6} {'BB/6':>6} {'K%':>6} {'BB%':>6} {'BABIP':>6} {'LOB%':>6}")
    for r in rows:
        def fmt(v, d=2): return f"{v:.{d}f}" if v is not None else "  —"
        print(f"  {r[0]:12s} {fmt(r[1],0):>6} {fmt(r[2]):>6} {fmt(r[3]):>6} "
              f"{fmt(r[4]*100 if r[4] else None):>6} {fmt(r[5]*100 if r[5] else None):>6} "
              f"{fmt(r[6],3):>6} {fmt(r[7]*100 if r[7] else None):>6}")

    conn.close()


if __name__ == '__main__':
    calculate_custom_stats()
