# Roadmap

Eight slices, each shipping something real, in the order John accepted on 2026-10-19 ([Write the foundation plan and spec the first slice](https://github.com/JWolf34/RocketStocks/issues/55)).
A slice is done when it meets the definition of done in the [README](./README.md).
Each done-signal below names what is specific to the slice; CI green and a recorded live run apply to all of them.
Each slice's spec is written in its implementing pull request.

| # | Slice | Goal | User-visible outcome | Unblocks | Done-signal |
|---|---|---|---|---|---|
| 1 | **Attention surge** | ApeWisdom ingest, v1 popularity import, daily bars for surge names, EDGAR and earnings facts, first base rate, bot posting | Attention-surge alerts in Discord with a facts block and base rate | The alert log, `rs_query`, `rs_research`, per-service roles, the daemon shape, v1 retirement | [Spec 01](./specs/01-attention-surge.md): a live alert posted with a stored base rate; all three rehearsals pass |
| 2 | **Symbol master and options EOD scan** | Nightly symbol master, daily bars for every listing, full option chains for the optionable universe | No Discord output | The liquid band, the optionable flag, the 250-session clock for an options study | A recorded nightly run finishes before the deadline with tombstones for failures; no non-session bar is stored |
| 3 | **Stock Report and collection requests** | The pull path: the bot writes a request, ingest fetches and stores, the bot renders | A Stock Report on demand: fundamentals, technicals, quote and chart, next earnings, recent alerts on the ticker | Options Report, Watchlist Report, paper-trading marks | A recorded report for a live ticker, and the "as of" fallback shown with ingest stopped |
| 4 | **Stream band** | Live quotes and 1-minute bars for the liquid band; large-move and earnings-reaction detectors | Large-move and earnings-reaction alerts, each with a base rate | Digests, alert-scope watchlists, intraday base rates | Both alert types fire on a recorded live session; the three rehearsals re-run with the stream live |
| 5 | **Digests** | Daily post-close and Monday earnings preview | Two scheduled posts: band movers, top popularity movers, earnings week | Watchlist sections (slice 7) | Each digest posts once on schedule in a recorded run |
| 6 | **Options Report** | Chain plus IV rank, put/call and max pain | An options read on demand | Nothing downstream; it spends the nightly chain history | A recorded report built from stored chains |
| 7 | **Watchlists, Watchlist Report, paper trading** | Shared and personal lists; manual paper trades | Watchlist Report, watchlist sections in the digest, paper trades, and watchlist tickers in alert scope | The Data API surfaces for user state | A recorded watchlist added, appearing in scope and the digest; one paper round trip |
| 8 | **Data API** | `serve` exposes `rs_query` as REST plus MCP | None in Discord; John's Claude Code reads the data | Analyses beyond the bot | A Claude Code session answers a question through `/mcp`; the read-only role cannot write |

## v1 retirement

v1 stays muted and popularity-only, posting silenced through Discord channel permissions, so its ApeWisdom history keeps accumulating.
It is retired once slice 1's popularity ingest runs and the v1 popularity import is done ([Disposition of existing docs, ADRs and open maps](https://github.com/JWolf34/RocketStocks/issues/54)).
v1 code is never imported; it is a read-only reference ([Build base](https://github.com/JWolf34/RocketStocks/issues/50)).

## Command map

Commands are Reports and a few management groups.
Names and layout are decided in each slice's spec.

- `/stock` - Stock Report (slice 3).
- `/options` - Options Report (slice 6).
- `/watchlist ...` - manage lists and pull the Watchlist Report (slice 7).
- `/trade ...` - manual paper trades and portfolio (slice 7, per ADR-0027).
- A small `/setup` group for channel routing, role subscriptions and Schwab re-auth (`/setup schwab`, slice 1).

## Open placements

The accepted roadmap does not say which slice owns two items.
Daily bars for every listing are placed in slice 2 with the nightly passes.
The weekly `rs_research` rerun starts in slice 1 and gains alert types as slice 4 adds them.
