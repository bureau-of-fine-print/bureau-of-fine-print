"""
NBA Fine Print — Game Preview Generator
Pulls today's scored games from mart_game_scores, sends to Claude API,
stores writeups in mart_game_previews, emails to jason040888@gmail.com.

Usage:
    python generate_previews.py --batch early
    python generate_previews.py --batch late
"""

import argparse
import os
import smtplib
import json
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.cloud import bigquery, secretmanager
import anthropic
from substack import Api
from substack.post import Post as SubstackPost


# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID        = "project-71e6f4ed-bf24-4c0f-bb0"
DATASET           = "marts"
SCORES_TABLE      = f"{PROJECT_ID}.{DATASET}.mart_game_scores"
PREVIEWS_TABLE    = f"{PROJECT_ID}.{DATASET}.mart_game_previews"
FROM_EMAIL        = "bureauoffineprint@gmail.com"
TO_EMAIL          = "jason040888@gmail.com"
CLAUDE_MODEL      = "claude-sonnet-4-20250514"
SUBSTACK_URL      = "https://nbafineprint.substack.com"


# ── Secret Manager ────────────────────────────────────────────────────────────
def get_secret(secret_id: str) -> str:
    # Fall back to environment variables for local testing
    env_map = {
        "claude-api-key":           "CLAUDE_API_KEY",
        "gmail-app-password":       "GMAIL_APP_PASSWORD",
        "substack-session-cookie":  "SUBSTACK_SESSION_COOKIE",
    }
    env_var = env_map.get(secret_id)
    if env_var and os.environ.get(env_var):
        return os.environ[env_var]

    # Otherwise fetch from Secret Manager (Cloud Run)
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8").strip()


# ── Free game selection ───────────────────────────────────────────────────────
def pick_free_game(games: list[dict]) -> dict:
    """
    Priority:
    1. First national broadcast game ordered by tipoff_time ASC
    2. If none, earliest tipoff of the day
    """
    national = [g for g in games if g.get("is_national_broadcast")]
    pool     = sorted(national, key=lambda x: x["tipoff_time"]) if national else sorted(games, key=lambda x: x["tipoff_time"])
    return pool[0] if pool else games[0]


# ── Build Claude prompt ───────────────────────────────────────────────────────
def build_prompt(game: dict, is_free: bool) -> str:

    # Separate strong/medium signals
    strong_signals = []
    medium_signals = []
    insight_fields = [
        "home_injury_insight", "away_injury_insight",
        "pace_insight", "form_insight", "margin_insight", "quality_insight",
        "timezone_insight", "streak_insight",
        "home_returning_insight", "away_returning_insight",
        "away_form_insight", "rest_insight",
        "ref_bias_insight", "home_ref_insight", "away_ref_insight",
        "home_ref_ats_insight", "away_ref_ats_insight", "ou_insight",
        "utah_insight", "blowout_insight",
        "home_player_ref_boost_insight", "home_player_ref_suppress_insight",
        "away_player_ref_boost_insight", "away_player_ref_suppress_insight",
        "home_hot_insight", "home_cold_insight",
        "away_hot_insight", "away_cold_insight",
    ]
    for field in insight_fields:
        val = game.get(field)
        if not val or val.startswith("\u26aa CONTEXT"):
            continue
        if val.startswith("\U0001f534 STRONG") or "STRONG" in val[:15]:
            strong_signals.append(val)
        elif val.startswith("\U0001f7e1 MEDIUM") or "MEDIUM" in val[:15]:
            medium_signals.append(val)

    if strong_signals or medium_signals:
        signals_text = ""
        if strong_signals:
            signals_text += "STRONG SIGNALS:\n" + "\n".join(f"  {s}" for s in strong_signals)
        if medium_signals:
            if signals_text:
                signals_text += "\n"
            signals_text += "MEDIUM SIGNALS:\n" + "\n".join(f"  {s}" for s in medium_signals)
    else:
        signals_text = "No strong or medium signals. Game looks fairly priced — write a short neutral preview (150-200 words)."

    # Spread/total directional flags
    mvl  = game.get("model_vs_line", 0) or 0
    mvm  = game.get("model_vs_market", 0) or 0
    lean = game.get("overall_lean", "NEUTRAL")

    if mvl >= 5:
        total_flag = f"OVER — total may be low by ~{abs(round(mvl,1))} points"
    elif mvl <= -5:
        total_flag = f"UNDER — total may be high by ~{abs(round(mvl,1))} points"
    elif mvl >= 3:
        total_flag = f"SLIGHT OVER lean (~{abs(round(mvl,1))} pts)"
    elif mvl <= -3:
        total_flag = f"SLIGHT UNDER lean (~{abs(round(mvl,1))} pts)"
    else:
        total_flag = "FAIR"

    if mvm >= 0.10:
        spread_flag = "HOME EDGE — analysis meaningfully favors home team vs the line"
    elif mvm <= -0.10:
        spread_flag = "AWAY EDGE — analysis meaningfully favors away team vs the line"
    elif mvm >= 0.06:
        spread_flag = "SLIGHT HOME EDGE"
    elif mvm <= -0.06:
        spread_flag = "SLIGHT AWAY EDGE"
    else:
        spread_flag = "FAIR"

    tipoff = str(game.get("tipoff_time", ""))[:5]

    return f"""You are a sharp, professional basketball analyst writing daily game previews for a Substack newsletter called NBA Fine Print. Your readers follow the lines closely and want facts, not hype.

VOICE AND TONE:
- Professional, direct, confident — like a seasoned analyst filing a report
- No fluff, no filler, no cheerleading
- Every stat must use the exact numbers from the data provided — never invent or estimate figures not in the data
- Never say "measurable advantage", "notable", "it's worth noting", "keep an eye on", "interesting"
- Never say "bet," "tail," "fade," "lock," or "play"
- Never mention standard deviations, model scores, composite numbers, or any math
- Never say "our model" or "the model"
- Plain basketball English only
- If a signal doesn't have a specific number to cite, skip it entirely

FORMAT — use exactly this structure, no deviations:

[AWAY TEAM] @ [HOME TEAM] — [TIME] ET
[DATE] | Line: [SPREAD] | Total: [O/U]

[2 sentence intro — what is the story of this game]

[AWAY TEAM]:
• [fact with specific number]
• [fact with specific number]
• [fact with specific number — injury, rest, or travel if relevant]

[HOME TEAM]:
• [fact with specific number]
• [fact with specific number]
• [fact with specific number — injury, rest, or travel if relevant]

[2-3 sentence conclusion — direct take on whether spread or total looks right or off, using specific facts and numbers. Never say which side to take.]

RULES FOR BULLET POINTS:
- Each bullet must contain a specific number from the data
- Bad: "Boston has been getting favorable whistles from officials"
- Good: "Boston is 12-4 ATS in 16 games with crew chief Zach Zarba this season"
- Bad: "Milwaukee has struggled at home recently"
- Good: "Milwaukee is being outscored by 6.7 points per game on average across their last 10 games"
- Bad: "Indiana has shown no ability to compete recently"
- Good: "Indiana is 0-5 over their last five games and being outscored by 10.4 points per game on average across their last 10"
- Skip any signal that doesn't have a hard number behind it
- MARGIN signals mean average point differential across ALL last 10 games (wins and losses combined) — write it exactly that way
- BLOWOUT signals mean a team is being outscored on average across their last 10 games — not just in losses
- RECORDS ARE OVERALL SEASON RECORDS ONLY — never write home or away splits. Never say "X-Y at home" or "X-Y on the road". Never say "home record" or "road record". The data does not contain home/away splits — any home or away record you write is fabricated
- Never reference a team's home or road streak unless it is explicitly stated in the signals — do not infer it from other data
- Do not convert win totals to percentages — "won 2 of their last 10" is sufficient, never add "for a 20% win rate"
- PACE bullets must always compare both teams and explain the implication — never cite one team's pace in isolation. Bad: "Houston averages 98.9 possessions per game." Good: "Houston plays at 98.9 possessions per game versus San Antonio's 102.5, setting up a faster-paced game than either team typically sees"
- ATS signals include the exact sample size (e.g. "5-3 ATS in 8 games") — always include the game count
- If a sample size for an ATS trend is small (under 10 games), note it as a small sample
- "last 5 form" means their win-loss record over their last 5 games total (not home or away splits)

LENGTH: Let the content breathe — write as much as the signals warrant. If there are 4+ strong signals, 400 words is fine. If the game is neutral or has few signals, keep it to 150-200 words. Never pad a weak game and never truncate a strong one.

---

GAME DATA:

{game['away_team_id']} @ {game['home_team_id']} — {tipoff} ET
{game['game_date']} | Line: {game['home_team_id']} {game.get('spread_home', 'N/A')} | Total: {game.get('over_under', 'N/A')}
Records: {game['away_record']} (away) | {game['home_record']} (home)
Crew Chief: {game.get('crew_chief_name', 'Unknown')}

SPREAD FLAG: {spread_flag}
TOTAL FLAG: {total_flag}
OVERALL LEAN: {lean}

SIGNALS:
{signals_text}

Write the preview now. Use the exact header format as the first line. Do not include Lean, Market, or Total flag lines in the output — those are for your reference only."""

# ── Call Claude API ───────────────────────────────────────────────────────────
def generate_preview(game: dict, is_free: bool, claude_client) -> str:
    prompt   = build_prompt(game, is_free)
    response = claude_client.messages.create(
        model      = CLAUDE_MODEL,
        max_tokens = 1200,
        messages   = [{"role": "user", "content": prompt}]
    )
    return response.content[0].text.strip()


# ── Store in BigQuery ─────────────────────────────────────────────────────────
def store_previews(games_with_previews: list[dict], bq_client):
    rows = []
    for g in games_with_previews:
        rows.append({
            "game_date"       : str(g["game_date"]),
            "home_team_id"    : g["home_team_id"],
            "away_team_id"    : g["away_team_id"],
            "tipoff_time"     : g["tipoff_time"],
            "is_free_game"    : g["is_free_game"],
            "composite_score" : g.get("composite_score"),
            "overall_lean"    : g.get("overall_lean"),
            "market_divergence": g.get("market_divergence"),
            "model_vs_market" : g.get("model_vs_market"),
            "total_insight"   : g.get("total_insight"),
            "spread_home"     : g.get("spread_home"),
            "over_under"      : g.get("over_under"),
            "preview_text"    : g["preview_text"],
            "posted_substack" : False,
            "posted_reddit"   : False,
            "generated_at"    : g["generated_at"],
        })

    errors = bq_client.insert_rows_json(PREVIEWS_TABLE, rows)
    if errors:
        print(f"BigQuery insert errors: {errors}")
    else:
        print(f"Stored {len(rows)} previews in {PREVIEWS_TABLE}")


# ── Build email ───────────────────────────────────────────────────────────────
def build_email(games_with_previews: list[dict], batch: str) -> str:
    today     = date.today().strftime("%B %d, %Y")
    batch_label = "Early Games" if batch == "early" else "Evening Games"
    lines     = [
        f"NBA Fine Print — {batch_label} | {today}",
        "=" * 60,
        "",
    ]

    # Free game first
    free_games  = [g for g in games_with_previews if g["is_free_game"]]
    paid_games  = [g for g in games_with_previews if not g["is_free_game"]]

    for g in free_games + paid_games:
        tag = "⭐ FREE GAME" if g["is_free_game"] else "🔒 PAID"
        lines += [
            f"{tag} | {g['away_team_id']} @ {g['home_team_id']}  {g['tipoff_time']} ET",
            f"Spread: {g['home_team_id']} {g.get('spread_home', 'N/A')}  |  O/U: {g.get('over_under', 'N/A')}",
            f"Lean: {g.get('overall_lean', 'N/A')}  |  Market: {g.get('market_divergence', 'N/A')}",
            f"Total: {g.get('total_insight', 'N/A')}",
            "",
            g["preview_text"],
            "",
            "-" * 60,
            "",
        ]

    return "\n".join(lines)


# ── Send email ────────────────────────────────────────────────────────────────
def send_email(body: str, batch: str, gmail_password: str):
    today       = date.today().strftime("%b %d")
    batch_label = "Early" if batch == "early" else "Evening"
    subject     = f"NBA Fine Print — {batch_label} Games | {today}"

    msg                    = MIMEMultipart("alternative")
    msg["Subject"]         = subject
    msg["From"]            = FROM_EMAIL
    msg["To"]              = TO_EMAIL
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(FROM_EMAIL, gmail_password)
        server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())

    print(f"Email sent to {TO_EMAIL}")


# ── Fetch today's games ───────────────────────────────────────────────────────
def fetch_games(batch: str, bq_client, override_date: str = None) -> list[dict]:
    today = override_date if override_date else date.today().isoformat()

    if batch == "early":
        tipoff_filter = "AND CAST(tipoff_time AS TIME) < TIME '18:00:00'"
    else:
        tipoff_filter = ""  # late run catches anything not yet in previews

    # Deduplicate odds rows by taking the latest inserted_at per game (closing line),
    # then exclude games already previewed today
    query = f"""
        WITH latest AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY game_date, home_team_id, away_team_id
                    ORDER BY inserted_at DESC
                ) AS rn
            FROM `{SCORES_TABLE}`
            WHERE game_date = '{today}'
              {tipoff_filter}
        )
        SELECT * EXCEPT (rn)
        FROM latest
        WHERE rn = 1
          AND CONCAT(CAST(game_date AS STRING), '|', home_team_id, '|', away_team_id) NOT IN (
              SELECT CONCAT(CAST(game_date AS STRING), '|', home_team_id, '|', away_team_id)
              FROM `{PREVIEWS_TABLE}`
              WHERE game_date = '{today}'
          )
        ORDER BY tipoff_time ASC
    """
    results = bq_client.query(query).result()
    return [dict(row) for row in results]


# ── Substack helpers ──────────────────────────────────────────────────────────
def _add_game_to_post(post, g):
    """Add a single game block to a SubstackPost."""
    tipoff     = str(g.get("tipoff_time", ""))[:5]
    spread     = g.get("spread_home", "N/A")
    spread_str = f"+{spread}" if isinstance(spread, (int, float)) and spread > 0 else str(spread)
    total      = g.get("over_under", "N/A")
    game_date  = g.get("game_date", "")

    # Bold matchup header using native Substack node format
    post.add({"type": "paragraph", "content": [
        {"content": f"{g['away_team_id']} @ {g['home_team_id']} — {tipoff} ET", "marks": [{"type": "strong"}]}
    ]})
    # Line info
    post.add({"type": "paragraph", "content": f"{game_date} | Line: {g['home_team_id']} {spread_str} | Total: {total}"})

    # Preview body — skip any lines that look like the header (away @ home) or line info
    # These get stored in preview_text from earlier runs and would duplicate the above
    header_marker = f"{g['away_team_id']} @ {g['home_team_id']}"
    line_marker   = "Line:"
    lines = g["preview_text"].split("\n")
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if header_marker in stripped or line_marker in stripped:
            continue
        post.add({"type": "paragraph", "content": stripped})

    # Separator
    post.add({"type": "paragraph", "content": "· · · · · · · · · · · · · · · · · · · · · · · · · · · · · ·"})


def _publish_post(api, user_id, title, subtitle, games, audience):
    """Build and publish a single Substack post. Returns canonical URL."""
    post = SubstackPost(
        title=title,
        subtitle=subtitle,
        user_id=user_id,
        audience=audience,
        write_comment_permissions="everyone",
    )
    for g in games:
        _add_game_to_post(post, g)

    draft    = api.post_draft(post.get_draft())
    draft_id = draft.get("id")
    print(f"Draft created — id={draft_id}, audience={audience}")
    api.prepublish_draft(draft_id)
    api.publish_draft(draft_id)
    url = draft.get("canonical_url", "")
    print(f"Published: {url}")
    return url


# ── Publish to Substack ───────────────────────────────────────────────────────
def publish_to_substack(games_with_previews: list[dict], batch: str, cookie: str, gmail_password: str):
    try:
        api = Api(
            cookies_string=f"substack.sid={cookie}",
            publication_url=SUBSTACK_URL,
        )
        user_id = api.get_user_id()
        print(f"Substack auth OK — user_id={user_id}")
    except Exception as e:
        if "401" in str(e) or "403" in str(e) or "unauthorized" in str(e).lower():
            _send_alert_email(
                "Substack cookie expired — update Secret Manager with a new substack.sid value.",
                gmail_password
            )
        raise

    today       = date.today().strftime("%B %d, %Y")
    batch_label = "Early Games" if batch == "early" else "Evening Games"

    free_games = [g for g in games_with_previews if g["is_free_game"]]
    paid_games = [g for g in games_with_previews if not g["is_free_game"]]
    urls = []

    # Single post — all games free for everyone (rest of 25/26 season)
    all_games = free_games + paid_games
    n = len(all_games)
    title    = f"NBA Fine Print — {batch_label} | {today}"
    subtitle = f"{n} game{'s' if n != 1 else ''} — data-driven NBA previews"
    url = _publish_post(
        api, user_id,
        title=title,
        subtitle=subtitle,
        games=all_games,
        audience="everyone",
    )
    return [url]


def _send_alert_email(message: str, gmail_password: str):
    msg             = MIMEMultipart("alternative")
    msg["Subject"]  = "⚠️ NBA Fine Print — Action Required"
    msg["From"]     = FROM_EMAIL
    msg["To"]       = TO_EMAIL
    msg.attach(MIMEText(message, "plain"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(FROM_EMAIL, gmail_password)
        server.sendmail(FROM_EMAIL, TO_EMAIL, msg.as_string())
    print(f"Alert email sent: {message}")


# ── Fetch existing previews from BQ (for --no-claude mode) ───────────────────
def fetch_existing_previews(batch: str, bq_client, override_date: str = None) -> list[dict]:
    today = override_date if override_date else date.today().isoformat()
    if batch == "early":
        tipoff_filter = "AND CAST(tipoff_time AS TIME) < TIME '18:00:00'"
    else:
        tipoff_filter = ""
    query = f"""
        SELECT p.*, s.* EXCEPT (game_date, home_team_id, away_team_id, tipoff_time, inserted_at)
        FROM `{PREVIEWS_TABLE}` p
        JOIN (
            SELECT * EXCEPT (rn)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY game_date, home_team_id, away_team_id
                        ORDER BY inserted_at DESC
                    ) AS rn
                FROM `{SCORES_TABLE}`
                WHERE game_date = '{today}'
            )
            WHERE rn = 1
        ) s
            ON p.game_date = s.game_date
            AND p.home_team_id = s.home_team_id
            AND p.away_team_id = s.away_team_id
        WHERE p.game_date = '{today}'
          {tipoff_filter}
        ORDER BY p.tipoff_time ASC
    """
    results = bq_client.query(query).result()
    return [dict(row) for row in results]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", choices=["early", "late"], required=True)
    parser.add_argument("--debug", action="store_true", help="Skip BigQuery insert — generates previews, posts to Substack, sends email, but does not store in mart_game_previews")
    parser.add_argument("--no-claude", action="store_true", help="Skip Claude API — pull existing previews from mart_game_previews instead. Use with --debug to test Substack/Reddit without spending tokens")
    parser.add_argument("--date", default=None, help="Override date (YYYY-MM-DD) — run against a specific date instead of today")
    args = parser.parse_args()

    print(f"Starting preview generation — batch={args.batch}{' [DEBUG]' if args.debug else ''}{' [NO-CLAUDE]' if args.no_claude else ''}")

    # Clients
    bq_client       = bigquery.Client(project=PROJECT_ID)
    gmail_password  = get_secret("gmail-app-password")
    substack_cookie = get_secret("substack-session-cookie")

    # Fetch games / previews
    if args.no_claude:
        games = fetch_existing_previews(args.batch, bq_client, override_date=args.date)
        if not games:
            print("No existing previews found for this date/batch. Exiting.")
            return
        print(f"Found {len(games)} existing previews — skipping Claude API")
    else:
        claude_client = anthropic.Anthropic(api_key=get_secret("claude-api-key"))
        games = fetch_games(args.batch, bq_client, override_date=args.date)
        if not games:
            print("No games to preview for this batch. Exiting.")
            return
        print(f"Found {len(games)} games to preview")

        # Mark free game
        free_game = pick_free_game(games)
        for g in games:
            g["is_free_game"] = (
                g["home_team_id"] == free_game["home_team_id"] and
                g["away_team_id"] == free_game["away_team_id"]
            )

        # Generate previews via Claude
        from datetime import datetime, timezone
        for g in games:
            print(f"Generating preview: {g['away_team_id']} @ {g['home_team_id']}")
            g["preview_text"] = generate_preview(g, g["is_free_game"], claude_client)
            g["generated_at"] = datetime.now(timezone.utc).isoformat()

    # If --no-claude, still need to mark free game
    if args.no_claude:
        free_game = pick_free_game(games)
        for g in games:
            g["is_free_game"] = (
                g["home_team_id"] == free_game["home_team_id"] and
                g["away_team_id"] == free_game["away_team_id"]
            )

    # Store in BigQuery (skip in debug or no-claude mode)
    if args.debug or args.no_claude:
        print("DEBUG mode — skipping BigQuery insert")
    else:
        store_previews(games, bq_client)

    # Send email
    body = build_email(games, args.batch)
    # send_email(body, args.batch, gmail_password)

    # Publish to Substack
    try:
        publish_to_substack(games, args.batch, substack_cookie, gmail_password)
    except Exception as e:
        print(f"Substack publish failed: {e}")
        _send_alert_email(f"Substack publish failed: {e}", gmail_password)

    print("Done.")


if __name__ == "__main__":
    main()
