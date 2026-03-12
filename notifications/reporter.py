import json
import logging
import re
import webbrowser
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

OUTCOME_LABELS = {
    "home_win": "Home Win",
    "draw": "Draw",
    "away_win": "Away Win",
}

_DATA_SCRIPT_RE = re.compile(
    r'(<script id="report-data" type="application/json">)(.*?)(</script>)',
    re.DOTALL,
)


def write_report_json(value_bets: list[dict], history: list[dict], path: str = "data/latest_report.json") -> None:
    """
    Writes value bets + history + metadata to a JSON file AND embeds the data
    directly into report.html so it works when opened as a file:// URL.
    """
    payload = {
        "generated_at": datetime.now().isoformat(),
        "date": date.today().isoformat(),
        "value_bets": value_bets,
        "history": history,
    }
    json_str = json.dumps(payload, indent=2, ensure_ascii=False)

    # Write the standalone JSON file (kept for reference / future use)
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json_str, encoding="utf-8")
    logger.info("Report written to %s", path)

    # Embed the data directly into report.html
    html_path = Path("report.html")
    if html_path.exists():
        html = html_path.read_text(encoding="utf-8")
        html = _DATA_SCRIPT_RE.sub(
            lambda m: f"{m.group(1)}\n{json_str}\n{m.group(3)}",
            html,
        )
        html_path.write_text(html, encoding="utf-8")
        logger.info("Data embedded into report.html")
    else:
        logger.warning("report.html not found — skipping HTML embed.")


def open_report(html_path: str = "report.html") -> None:
    """Opens report.html in the default browser."""
    abs_path = Path(html_path).resolve()
    url = abs_path.as_uri()
    logger.info("Opening report in browser: %s", url)
    webbrowser.open(url)


def print_summary(value_bets: list[dict]) -> None:
    """Prints a concise summary to stdout."""
    today = date.today().strftime("%Y-%m-%d")
    print(f"\n{'='*50}")
    print(f"  Value Bets — {today}")
    print(f"{'='*50}")

    if not value_bets:
        print("  No value bets found today.")
        print(f"{'='*50}\n")
        return

    total_bets = 0
    for match in value_bets:
        kickoff = match.get("kickoff_local", match.get("kickoff", ""))
        league = match.get("league_name", "")
        league_prefix = f"{league} | " if league else ""
        print(f"\n  [ {league_prefix}{match['home_team']} vs {match['away_team']} | {kickoff} ]")
        for bet in match["bets"]:
            ev_pct = bet["ev"] * 100
            print(f"    {bet['outcome_label']:<12} @ {bet['odds']:.2f}  "
                  f"(prob {bet['true_prob'] * 100:.1f}%  EV {ev_pct:+.1f}%)")
            total_bets += 1

    print(f"\n  Total value bets found: {total_bets} across {len(value_bets)} match(es)")
    print(f"{'='*50}\n")
