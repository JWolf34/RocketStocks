# Milestone 2: Nightly sweep

Every night RocketStocks records the whole market's listings, daily bars, options chains and earnings calendar, so later milestones and John's research can read any stock.
`docs/vision.md` is the authority; this spec holds only what a builder cannot decide alone, and is rewritten in place when reality differs.

## Probe first

- Through ingest's own client: chain sizes and failures on a sample (including the `BRK/B` spelling), bar depth from 1990, and each chain just after the close, at about 18:05 ET and before the open (does volume persist, do quotes, underlying price and implied volatility change, when does open interest change).
- Its results rewrite `docs/reference/schwab-facts.md` in place; if the morning chain still carries closing volume and quotes, John sees the numbers and may drop the evening sweep.

## Evening: Universe, then chains, then bars, each when the one before finishes

- **Universe**, from 18:05 ET: once `nasdaqlisted.txt` and `otherlisted.txt` carry a File Creation Time after the session's close (retried every 15 minutes; by 20:00 ET the night uses the previous snapshot and Ops is pinged), every non-test listing is stored by date with its security name, flags and the File Creation Time as As-of; no class classifier and no listing spans.
- A listing is optionable when it appears that day in Nasdaq's `options.txt` or Cboe's equity options symbol directory; the files are never committed.
- **Chains**: every optionable listing's full chain, unfiltered, through the ladder (fetch, retry once, split into three expiry ranges with the last open-ended, retry each, then a Tombstone); a 400 or an empty chain is recorded, never retried.
- Deadline 03:00 Central until the probe gives the settle time; anything unfetched by then is a Tombstone, and a missed night is never caught up.
- **Bars** for every listing in the latest Universe, SPY and Triggered stocks first; Outcomes start when the pass finishes, and it stops before the morning pass and resumes the next evening.
- A symbol with no bars asks from 1990 (this is the seed), one statement per symbol so none decompresses past Timescale's 100,000-row cap.
- Tails keep the overlap split check; a tail sharing no session with the stored bars (a reused ticker) also replaces the whole history; bars of a listing that leaves the Universe are kept, and survivorship-free history waits for a vendor.
- **Earnings calendar** (Nasdaq, no Schwab calls, runs beside them): a dated snapshot from a week back to the end of the forward window, per row ticker, report date, time of day, fiscal quarter, EPS forecast, estimate count, actual EPS and surprise; unparseable values are null and counted.
- A one-time backfill stores the same fields from 2008, labelled with its real fetch time; the EDGAR estimate is computed at read time, and the Alert's next Catalyst reads the latest snapshot, falling back to the estimate when it is more than two sessions old.

## Morning

- Once open interest has settled, a second pass fetches every optionable chain and stores only open interest, named by the session it settled for; its deadline is the 09:30 ET open.
- A failed morning loses nothing: the evening sweep carries the same open interest a session later.

## Records, failures, resume

- Every chain pass writes one fetch record per optionable underlying (status, attempts, contract count, fetch time); a Tombstone is a failure status, an empty chain is an observation, and contract rows never carry nulls.
- One shared Schwab budget (100 calls a minute) covers about 5,500 chain and 13,000 bar calls each evening and about 5,500 chain calls each morning; bars yield to chains.
- Ops pings fire per source on first failure and on recovery; a pass counts as failing above about 5% of its underlyings.
- The daemon re-runs any Schwab job a dead token killed once sign-in reopens the gate, and runs the passes on start after a restart; every Schwab job skips work already recorded and writes only Tombstones past its deadline.

## Storage

- Every contract is kept compressed indefinitely; chunking and segment layout come from the probe's chains and are checked on the first night.
- Growth is measured on the first night (its chunk compressed by hand) across every table, with the first week's numbers noted on the operator ticket.
- The VM's disk was 502 GB, 270 GB free and growable on 2026-10-27; if the whole disk projects past 80% within about two years the operator grows it, keeping only contracts with open interest or volume only if the host has no room.
- `docs/operations.md` lists chains as irreplaceable beside Popularity and names the host-level disk alarm the operator sets.

## Done

An operator-recorded evening (Universe, chains, bars with the seed complete, the earnings calendar with its backfill) and the next morning's open-interest pass finish inside their deadlines; a fetch record explains every optionable underlying; the first night's growth, projected across all tables, stays under 80% of the disk for two years; the probe results are in the Schwab facts; the token-expiry and ingest-restart rehearsals pass with a pass in flight; CI is green.
The sweep's use is observed in milestone 3.
