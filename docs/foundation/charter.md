# RocketStocks charter

What RocketStocks is, who it is for and what it will and will not do.
The reasoning behind each line lives in the linked tickets and in [charting-2026-10-19.md](./charting-2026-10-19.md).

## Description

RocketStocks is a personal trading-research assistant that lives in Discord, where it makes stock research accessible to John and his friends.

- **Push - it watches and tells.** Informational alerts about price, volume and attention events, each with its measured track record, plus scheduled digests.
- **Pull - it answers when asked.** Anyone in the server asks for a research-grade Report on a stock, an options chain or a watchlist.
- **Data API.** Clients John operates pull the data RocketStocks collects and trusts, to produce analyses RocketStocks does not (for example LLM-assisted reports).
  It is read-only and reachable on the local network only, and it exposes data, not features.

## Audience

John plus a handful of friends in one private Discord server.
Friends get everything through Discord with zero setup.
Only John uses the Data API.

## Principles

| Principle | Rules out |
|---|---|
| **Discord first** - every feature is designed for Discord | Web dashboards; features that exist only in the API |
| **Personal** - built to John's standards for one private server | Multi-tenant machinery, public ingress, per-guild configuration sprawl |
| **Research-grade** - signals are event-studied and logged, data is validated, gap-aware and stamped | Alerts without a base rate; silent gaps; unversioned rules |
| **Advisory** - people decide, with real money | Anything that places orders or claims a direction without evidence |
| **For fun** - cost and operator effort are sized for a hobby | Paid services; calendar-driven process; unattended weekly babysitting |

Research-grade has two halves.
Trusted signals: an event study before a signal ships, a versioned alert log, and confidence expressed as the pattern's historical base rate ([Signal evidence study](https://github.com/JWolf34/RocketStocks/issues/45)).
Trusted data: validated at the edge, rejecting bars off an exchange session, and stamped with when it was fetched and how it was adjusted.

## Scope in at launch

- **Three alert types**, informational with base rates and no direction claim: attention surge, earnings reaction, large move ([Launch push surfaces](https://github.com/JWolf34/RocketStocks/issues/46)).
- **Two digests:** daily post-close and Monday earnings preview.
- **Three Reports:** Stock, Options, Watchlist ([Launch pull surfaces and supporting features](https://github.com/JWolf34/RocketStocks/issues/47)).
- **Watchlists:** shared and personal, user-curated, feeding the Watchlist Report and alert scope.
- **Paper trading:** manual trades only.
- **Data API:** REST plus a small curated MCP server ([Research: Data API shape for LLM clients](https://github.com/JWolf34/RocketStocks/issues/44)).
- **Research CLI** (`rs_research`): event studies that write versioned base rates, and vectorised backtests ([Backtest and signal-evaluation tooling](https://github.com/JWolf34/RocketStocks/issues/51)).

## Scope out

- Auto-trader of any kind (see follow-ups).
- LLM-written explanations of why a stock is moving.
  The attention alert's why is a facts block: price and volume move, recent SEC 8-Ks, next earnings date ([Research: explaining why a stock is getting attention](https://github.com/JWolf34/RocketStocks/issues/43)).
- Reddit and news text, politician trades, and any paid data.
- `/data` lookup commands, on-demand screeners, compare reports and separate technical reports.
- Options-flow alerts, until an event study on collected chains passes.

## Display policy

Alerts and Reports in the private server may include raw Schwab data for John and his friends, non-commercial.
The public brokerage terms read as barring retransmission, and John accepts that risk.
Revisit if the server grows beyond friends, any money is involved, or Schwab supplies its developer agreement ([Read Schwab's market-data display terms](https://github.com/JWolf34/RocketStocks/issues/42)).
The Data API serves John's own tools only.

## Vendors

No paid services.
Schwab is the spine for quotes, bars, chains and fundamentals; ApeWisdom, SEC EDGAR, the Nasdaq calendar and symbol files, and `exchange_calendars` are free.
Weekly Schwab re-authentication is the accepted cost.
Base rates computed from Schwab history carry a survivorship caveat until v2's own alert log replaces them.
Details: [vendor-stack.md](./research/vendor-stack.md) and [Vendor stack](https://github.com/JWolf34/RocketStocks/issues/48).

## Post-v2 follow-ups

- **Auto-trader infrastructure:** replay of every input, point-in-time membership history, latency budgets, extraction seams, a broker path.
- **Options-flow signal:** revisit once about 250 sessions of nightly chains have accumulated and its event study passes.
- **LLM-written why:** only if John revisits it.
- **Paid survivorship-free history:** only if the survivorship bias in base rates proves to matter.
