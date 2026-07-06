"""One-time Schwab token bootstrap for the spikes (John runs this interactively).

Opens a browser for the Schwab login; schwab-py catches the redirect to
https://127.0.0.1:8182 and writes data/schwab-spike-token.json. Verifies the
token with a SPY quote.

    .venv/bin/python scripts/spikes/fetch_token.py
"""

import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
from _common import TOKEN_PATH, build_client


def main() -> int:
    client = build_client()
    resp = client.get_quote("SPY")
    if resp.status_code != 200:
        print(f"token saved but SPY quote failed: HTTP {resp.status_code} — {resp.text[:200]}")
        return 1
    quote = resp.json()["SPY"]["quote"]
    print(f"token OK at {TOKEN_PATH}")
    print(f"SPY last={quote.get('lastPrice')} bid={quote.get('bidPrice')} ask={quote.get('askPrice')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
