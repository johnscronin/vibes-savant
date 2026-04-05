#!/usr/bin/env python3
"""
Rebuild Batting by Opponent Tier — from HRL game logs + standings.
Steps:
  1. Ensure tables exist; add g/r columns to opponent_tier_splits if missing
  2. Scrape game logs for each Vibes player per season → player_opponent_splits
  3. Join with team_tiers to aggregate by tier → opponent_tier_splits
  4. Verify Nightmare 2025: Elite HR + Average HR + Weak HR == 24
"""

import sqlite3, os, re, difflib
from urllib.parse import quote
from playwright.sync_api import sync_playwright

DB_PATH = os.path.join(os.path.dirname(__file__), 'vibes_savant_site', 'vibes_savant.db')
BASE_URL = 'https://hrltwincities.com'

VIBES_PLAYERS = [
    "Anakin", "CatNip", "Cheerio", "Epstein", "FishHook",
    "HuckFinn", "Jessie", "Kar", "Nightmare", "Fortnite"
]


# ── DB helpers ─────────────────────────────────────────────────────────────

def ensure_tables(conn):
    """Create player_opponent_splits; add g/r to opponent_tier_splits if missing."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_opponent_splits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            season     INTEGER NOT NULL,
            opponent_team TEXT NOT NULL,
            g          INTEGER DEFAULT 0,
            pa         INTEGER DEFAULT 0,
            ab         INTEGER DEFAULT 0,
            r          INTEGER DEFAULT 0,
            h          INTEGER DEFAULT 0,
            doubles    INTEGER DEFAULT 0,
            triples    INTEGER DEFAULT 0,
            hr         INTEGER DEFAULT 0,
            rbi        INTEGER DEFAULT 0,
            bb         INTEGER DEFAULT 0,
            so         INTEGER DEFAULT 0,
            sac        INTEGER DEFAULT 0,
            avg        REAL,
            obp        REAL,
            slg        REAL,
            ops        REAL,
            UNIQUE(player_name, season, opponent_team)
        )
    """)
    # Add g and r columns to opponent_tier_splits if missing
    existing = [r[1] for r in conn.execute("PRAGMA table_info(opponent_tier_splits)").fetchall()]
    if 'g' not in existing:
        conn.execute("ALTER TABLE opponent_tier_splits ADD COLUMN g INTEGER DEFAULT 0")
        print("  Added column g to opponent_tier_splits")
    if 'r' not in existing:
        conn.execute("ALTER TABLE opponent_tier_splits ADD COLUMN r INTEGER DEFAULT 0")
        print("  Added column r to opponent_tier_splits")
    conn.commit()


def normalize(name):
    """Normalize team name for fuzzy comparison."""
    return re.sub(r"[^a-z0-9 ]", "", name.lower().strip())


def match_team(opp_name, team_lookup):
    """Match opponent name from game log to a team in the standings lookup.
    team_lookup: {normalized_name: canonical_name}
    Returns canonical name or None.
    """
    norm = normalize(opp_name)
    # Exact normalized match
    if norm in team_lookup:
        return team_lookup[norm]
    # Fuzzy fallback
    matches = difflib.get_close_matches(norm, team_lookup.keys(), n=1, cutoff=0.7)
    if matches:
        return team_lookup[matches[0]]
    return None


# ── Year dropdown helper ───────────────────────────────────────────────────

def select_year(page, year_str):
    dd = page.query_selector('.k-dropdownlist')
    if not dd:
        return False
    dd.click()
    page.wait_for_timeout(800)
    for item in page.locator('.k-list-item').all():
        if item.inner_text().strip() == year_str:
            item.click()
            page.wait_for_timeout(2200)
            return True
    page.keyboard.press('Escape')
    page.wait_for_timeout(300)
    return False


# ── Step 2: Scrape game logs ───────────────────────────────────────────────

def si(v):
    try:    return int(str(v).strip())
    except: return 0


def scrape_game_logs(page, conn):
    for player in VIBES_PLAYERS:
        print(f"\n{player}:")
        try:
            page.goto(f"{BASE_URL}/player/{quote(player)}", wait_until="domcontentloaded")
            page.wait_for_timeout(3500)
        except Exception as e:
            print(f"  page load error: {e}")
            continue

        # Click Game Logs tab
        try:
            page.get_by_text("Game Logs", exact=False).first.click()
            page.wait_for_timeout(2500)
        except Exception as e:
            print(f"  Game Logs click error: {e}")
            continue

        # Read available seasons from dropdown
        dd = page.query_selector('.k-dropdownlist')
        if not dd:
            print("  no dropdown found")
            continue

        dd.click()
        page.wait_for_timeout(800)
        years = [it.inner_text().strip() for it in page.locator('.k-list-item').all()
                 if it.inner_text().strip().isdigit()]
        page.keyboard.press('Escape')
        page.wait_for_timeout(300)
        print(f"  seasons: {years}")

        for year_str in years:
            season = int(year_str)
            if season > 2025:
                continue  # skip in-progress 2026 (no standings)

            # Skip if already scraped
            existing = conn.execute(
                "SELECT COUNT(*) FROM player_opponent_splits WHERE player_name=? AND season=?",
                (player, season)
            ).fetchone()[0]
            if existing > 0:
                continue

            if not select_year(page, year_str):
                print(f"    {season}: could not select year")
                continue

            # Find the game-level data table (21 cols, not the summary table)
            tables = page.query_selector_all('table')
            game_rows = []
            for t in tables:
                rows = t.query_selector_all('tr')
                for row in rows:
                    cells = [c.inner_text().strip() for c in row.query_selector_all('th,td')]
                    # Must have opponent (col 4) and numeric AB (col 5)
                    if len(cells) >= 12 and cells[4] not in ('OPPONENT', '') and cells[5].lstrip('-').isdigit():
                        game_rows.append(cells)

            if not game_rows:
                print(f"    {season}: no game rows")
                continue

            # Aggregate per opponent
            opp = {}
            for cells in game_rows:
                opp_name = cells[4]
                ab  = si(cells[5]);  r   = si(cells[6]);  h   = si(cells[7])
                d   = si(cells[8]);  t   = si(cells[9]);  hr  = si(cells[10])
                rbi = si(cells[11]); bb  = si(cells[12]); sac = si(cells[13])
                so  = si(cells[14])
                # cells[15]=ROE, cells[16]=SAC2, cells[17-20]=running rates (skip)
                pa  = ab + bb + sac

                if opp_name not in opp:
                    opp[opp_name] = dict(g=0, pa=0, ab=0, r=0, h=0,
                                         doubles=0, triples=0, hr=0, rbi=0,
                                         bb=0, so=0, sac=0)
                s = opp[opp_name]
                s['g']       += 1
                s['pa']      += pa
                s['ab']      += ab
                s['r']       += r
                s['h']       += h
                s['doubles'] += d
                s['triples'] += t
                s['hr']      += hr
                s['rbi']     += rbi
                s['bb']      += bb
                s['so']      += so
                s['sac']     += sac

            for opp_name, s in opp.items():
                ab = s['ab']; h = s['h']; bb = s['bb']; hr = s['hr']
                d  = s['doubles']; t = s['triples']
                avg = round(h / ab, 3) if ab > 0 else 0.0
                obp = round((h + bb) / (ab + bb), 3) if (ab + bb) > 0 else 0.0
                tb  = (h - d - t - hr) + d*2 + t*3 + hr*4
                slg = round(tb / ab, 3) if ab > 0 else 0.0
                ops = round(obp + slg, 3)
                conn.execute("""
                    INSERT OR REPLACE INTO player_opponent_splits
                    (player_name, season, opponent_team, g, pa, ab, r, h,
                     doubles, triples, hr, rbi, bb, so, sac, avg, obp, slg, ops)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (player, season, opp_name,
                      s['g'], s['pa'], s['ab'], s['r'], s['h'],
                      s['doubles'], s['triples'], s['hr'], s['rbi'],
                      s['bb'], s['so'], s['sac'],
                      avg, obp, slg, ops))
            conn.commit()
            print(f"    {season}: {len(game_rows)} games, {len(opp)} opponents scraped")


# ── Step 3: Calculate tier splits ─────────────────────────────────────────

def calculate_tier_splits(conn):
    print("\n=== CALCULATING TIER SPLITS ===")

    # The HRL game log retroactively labels ALL historical games with CURRENT (2025)
    # team names. So we use the 2025 tier lookup for ALL seasons.
    # This answers: "how did this player perform vs teams currently in each tier?"
    tier_lookup_2025 = {}
    for row in conn.execute("SELECT team_name, tier FROM team_tiers WHERE season=2025").fetchall():
        canonical = row[0]; tier = row[1]
        tier_lookup_2025[normalize(canonical)] = (tier, canonical)
    print(f"  Using 2025 tier lookup: {len(tier_lookup_2025)} teams")

    # Clear old batting data
    conn.execute("DELETE FROM opponent_tier_splits WHERE split_role='batting'")
    conn.commit()
    print("  Cleared old batting tier splits")

    unmatched = set()
    total_rows = 0

    for player in VIBES_PLAYERS:
        seasons = [r[0] for r in conn.execute(
            "SELECT DISTINCT season FROM player_opponent_splits WHERE player_name=? ORDER BY season",
            (player,)
        ).fetchall()]

        for season in seasons:
            lookup = tier_lookup_2025
            opp_rows = conn.execute(
                """SELECT opponent_team, g, pa, ab, r, h, doubles, triples, hr, rbi, bb, so
                   FROM player_opponent_splits WHERE player_name=? AND season=?""",
                (player, season)
            ).fetchall()

            tier_agg = {}
            for row in opp_rows:
                opp_name = row[0]
                matched  = match_team(opp_name, {k: v[1] for k, v in lookup.items()})
                if matched is None:
                    unmatched.add(f"{season}:{opp_name}")
                    continue
                # Find tier for the canonical matched name
                tier = None
                for k, (t, c) in lookup.items():
                    if c == matched:
                        tier = t
                        break
                if tier is None:
                    continue

                if tier not in tier_agg:
                    tier_agg[tier] = dict(g=0, pa=0, ab=0, r=0, h=0,
                                          doubles=0, triples=0, hr=0, rbi=0,
                                          bb=0, so=0)
                a = tier_agg[tier]
                a['g']       += row[1] or 0
                a['pa']      += row[2] or 0
                a['ab']      += row[3] or 0
                a['r']       += row[4] or 0
                a['h']       += row[5] or 0
                a['doubles'] += row[6] or 0
                a['triples'] += row[7] or 0
                a['hr']      += row[8] or 0
                a['rbi']     += row[9] or 0
                a['bb']      += row[10] or 0
                a['so']      += row[11] or 0

            for tier, a in tier_agg.items():
                ab = a['ab']; h = a['h']; bb = a['bb']
                hr = a['hr']; d = a['doubles']; t = a['triples']
                avg = round(h / ab, 3)  if ab > 0 else 0.0
                obp = round((h + bb) / (ab + bb), 3) if (ab + bb) > 0 else 0.0
                tb  = (h - d - t - hr) + d*2 + t*3 + hr*4
                slg = round(tb / ab, 3) if ab > 0 else 0.0
                ops = round(obp + slg, 3)

                conn.execute("""
                    INSERT INTO opponent_tier_splits
                    (player_name, season, tier, split_role,
                     g, r, pa, ab, h, hr, rbi, bb, so, doubles, triples,
                     avg, obp, slg, ops)
                    VALUES (?, ?, ?, 'batting',
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?)
                """, (player, season, tier,
                      a['g'], a['r'], a['pa'], a['ab'], a['h'],
                      a['hr'], a['rbi'], a['bb'], a['so'],
                      a['doubles'], a['triples'],
                      avg, obp, slg, ops))
                total_rows += 1

        conn.commit()

    print(f"  Inserted {total_rows} tier split rows")
    if unmatched:
        print(f"  Unmatched teams ({len(unmatched)}):")
        for u in sorted(unmatched):
            print(f"    {u}")


# ── Step 4: Verify Nightmare 2025 ─────────────────────────────────────────

def verify_nightmare_2025(conn):
    print("\n=== NIGHTMARE 2025 VERIFICATION ===")

    rows = conn.execute("""
        SELECT tier, g, pa, ab, h, hr, rbi, bb, so, avg, obp, slg, ops
        FROM opponent_tier_splits
        WHERE player_name='Nightmare' AND season=2025 AND split_role='batting'
        ORDER BY CASE tier WHEN 'Elite' THEN 1 WHEN 'Average' THEN 2 ELSE 3 END
    """).fetchall()

    total_hr = 0
    total_pa = 0
    print(f"  {'Tier':<8} {'G':>4} {'PA':>5} {'AB':>5} {'H':>4} {'HR':>4} {'RBI':>4} {'BB':>4} {'SO':>4} {'AVG':>6} {'OBP':>6} {'SLG':>6} {'OPS':>6}")
    print("  " + "-" * 72)
    for r in rows:
        tier, g, pa, ab, h, hr, rbi, bb, so, avg, obp, slg, ops = r
        total_hr += hr or 0
        total_pa += pa or 0
        print(f"  {tier:<8} {g:>4} {pa:>5} {ab:>5} {h:>4} {hr:>4} {rbi:>4} {bb:>4} {so:>4} {str(avg):>6} {str(obp):>6} {str(slg):>6} {str(ops):>6}")

    raw = conn.execute(
        "SELECT SUM(hr), SUM(pa) FROM player_opponent_splits WHERE player_name='Nightmare' AND season=2025"
    ).fetchone()
    print(f"\n  Game log total HR: {raw[0]}, PA: {raw[1]}")
    print(f"  Tier agg  total HR: {total_hr}, PA: {total_pa}")
    print(f"\n  {'✓ PASS' if total_hr == 24 else '✗ FAIL'} — HR={total_hr} (expected 24)")

    # Also check all players have data
    print("\n=== COVERAGE CHECK ===")
    for player in VIBES_PLAYERS:
        cnt = conn.execute(
            "SELECT COUNT(*), SUM(pa) FROM opponent_tier_splits WHERE player_name=? AND split_role='batting'",
            (player,)
        ).fetchone()
        print(f"  {player:<12}: {cnt[0]:>3} rows, {cnt[1] or 0:>5} total PA")


# ── Step 5: Print 2025 tier assignments ───────────────────────────────────

def print_2025_tiers(conn):
    print("\n=== 2025 TIER ASSIGNMENTS ===")
    for tier in ('Elite', 'Average', 'Weak'):
        rows = conn.execute(
            "SELECT team_name, wins, losses, win_pct, rank FROM team_tiers WHERE season=2025 AND tier=? ORDER BY rank",
            (tier,)
        ).fetchall()
        print(f"\n  {tier} ({len(rows)} teams):")
        for r in rows:
            print(f"    #{r[4]:2d}  {r[0]:<22} {r[1]}-{r[2]} ({r[3]:.3f})")


# ── Main ──────────────────────────────────────────────────────────────────

def run():
    conn = sqlite3.connect(DB_PATH)

    print("=== STEP 1: ENSURING TABLES ===")
    ensure_tables(conn)

    print("\n=== STEP 2: SCRAPING GAME LOGS ===")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page    = browser.new_page()
        page.set_default_timeout(0)
        scrape_game_logs(page, conn)
        browser.close()

    calculate_tier_splits(conn)
    verify_nightmare_2025(conn)
    print_2025_tiers(conn)
    conn.close()


if __name__ == '__main__':
    run()
