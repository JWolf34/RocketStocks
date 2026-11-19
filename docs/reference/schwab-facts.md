# Measured Schwab facts

Aggregate facts measured against the Schwab market-data API.
Only aggregates are kept here (counts, rates, medians, durations); vendor rows and raw payloads are never committed.
Measured against SPY and a handful of other symbols in July and August 2026 unless stated.
The scripts that took these measurements were removed; they are in the repository history before the change that removed them.

## REST budget

- The documented market-data limit is 120 requests per minute.
- A burst probe ramped 120 requests to 4 per second (240 per minute pace): all 200, no 429, no `Retry-After`, no rate-limit headers.
- A day-long chain collector added 900 more requests with zero 429s.
- Treat 240 per minute as a lower bound and plan on the documented 120.
- Revisit only if production ever sees a 429, and alarm when it does.
- Bounded chain requests failed sporadically in July and August 2026: 39 of 900 (about 4 percent) returned 502, so the nightly scan needs retry-once-then-Tombstone.
- The sweep probe saw no such failure: across two survey runs (2026-11-09 and 2026-11-17) and 110 timed chain calls, every chain except unfiltered SPY and QQQ answered 200 on the first attempt.
- A full nightly options pass is about 5,500 chains: about 55 minutes at 100 calls a minute, or about 46 minutes at the documented 120.

## Option chain sizes

Measured by the sweep probe through ingest's own client on 2026-11-09 and again on 2026-11-17, unfiltered, contract type ALL; the two runs agree to within a few percent.

- Unfiltered SPY and QQQ still return 502 (after about 10 seconds); in three expiry ranges (up to 30 days, 31 to 180, over 180) they answer 200 with about 12,400 and 10,600 contracts and about 15 and 13 MB.
- Every other chain answered whole, the largest being META at about 7,000 contracts and 8 MB, so the 502 threshold sits between about 8 MB and 13 MB, not at the 6 MB once assumed.
- Other named underlyings: IWM about 5,000 contracts, TSLA 5,100, AMD 6,500 to 7,000, NVDA 4,200, AAPL 3,400, `BRK/B` 2,100; 1.2 to 3.7 seconds each.
- A seeded sample of 50 optionable listings: contracts min 24, median about 155, p90 about 1,050 to 1,330, max about 2,400 to 2,950, mean about 355.
- Same sample: expiries median 4 (max 18 to 24); response mean 0.4 MB (p90 1.2 to 1.6 MB); whole-chain latency median 0.3 to 0.4 seconds, p90 0.8, max 1.6.
- `isChainTruncated` was false on every chain and every chain's status was `SUCCESS`.
- At about 5,500 optionable underlyings and a mean near 355 contracts that is about 1.9 million contract rows a night.
- Class shares answer only in Schwab's slash spelling: `BRK/B` returns its chain, while `BRK.B` and `BRKB` return 400.

## Option chain freshness

- Consecutive chain snapshots are not monotonic: one SPY response served chain-wide quotes about 14 minutes stale (`quoteTimeInLong` regressed by a median of 841 seconds), which showed up as roughly 800 apparent volume decreases in a single snapshot pair.
- Of 283 SPY snapshot pairs, 281 were clean; across the other nine symbols only 2 negative deltas appeared (TSLA).
- Rule: record the per-row quote time and reject rows or whole snapshots whose quote time regresses against the last accepted snapshot.
- Open interest does not change during the trading day (zero intraday changes observed on every symbol); how it changes overnight is below.
- Day-over-day open-interest change from consecutive nightly snapshots was large and clean: 1,100 to 2,700 contracts per underlying moved, with top single-contract moves of thousands to tens of thousands.
- Compute net change over the shared contract roster, not book totals; for daily-expiry ETFs expiry roll-off dominates book totals (SPY shared-net change was more than double the book-net change).

## Option chain from the close to the open

Measured by the sweep probe on one night, session 2026-11-17 into 2026-11-18, for AAPL, NVDA and AMD and two 24-contract chains (MNTK, OKUR).
Samples were taken at 16:05 and 18:05 ET, hourly from 19:00 to 05:00, half-hourly from 05:00 to 09:00, and at 09:15; all 22 were complete and every chain answered whole.
The resolution is the sampling interval, so a time below means "between the two samples named".

- **Quotes** (bid, ask, mark) still move between 16:05 and 18:05 ET: mark changed on about 8 percent of AAPL and NVDA contracts and 57 percent of AMD's.
- From 18:05 ET until 03:00 ET bid, ask and mark were identical on every contract of every name, and the quote time did not move.
- At 04:00 ET under 1 percent of contracts requote; broad pre-market requoting starts between 07:00 and 08:00 ET, reaching 80 to 99 percent of contracts by 09:15.
- **Volume** keeps growing after the close: between 16:05 and 18:05 ET it rose on 2 to 14 percent of the contracts that had volume.
- Between 18:05 and 19:00 ET it still changed, up and down, on 5 to 10 percent of them.
- Volume then resets to zero: between 19:00 and 20:00 ET for NVDA, AMD and MNTK, and between 20:00 and 21:00 ET for AAPL.
- It stays zero until pre-market trades, so the morning chain does not carry the previous session's volume (at 09:15 ET 72 to 93 percent of the previous session's contracts with volume read zero).
- So a chain carries its session's volume only if it is fetched after the close and before about 19:00 ET.
- **Open interest** changes twice overnight.
- First between 18:05 and 19:00 ET, on 21 to 44 percent of the large names' contracts and 8 to 12 of each small chain's 24.
- Then between 06:00 and 07:00 ET (06:30 for NVDA, AMD and both small names, 07:00 for AAPL); it did not change again from 07:00 to 09:15 ET.
- The morning value is the settled one: it overwrote part of the 19:00 change, returning OKUR to its 16:05 values and moving NVDA from 21 to 67 percent of contracts changed.
- So the settled open interest for a session appears by 07:00 ET the next morning, and a chain fetched before 19:00 ET carries the open interest settled for the previous session.
- **Implied volatility** and the underlying price change at every sample, overnight included (OKUR's underlying price never moved, and its volatility still changed on a third of its contracts).
- **New contracts** were listed at 04:00 ET (50 for AAPL, 38 for NVDA, 182 for AMD); none disappeared overnight.

## Price history

- One-minute history depth is about 47 calendar days (about 33 trading days), identical for every symbol tested from SPY to illiquid names, so it is a server-side global window.
- Five-minute depth is about 207 days and daily depth is about 30 years.
- Daily history from 1990-01-01 answers in one call of about 1 MB in under a second and goes back to 1990-01-02 for AAPL, IBM, KO and GE (about 9,300 candles); SPY starts at its 1993-01-29 listing and `BRK/B` at 1996-05-09 (sweep probe, 2026-11-09 and 2026-11-17).
- Price history for `BRK.B` returns 200 with no candles, so the dotted spelling fails silently; only `BRK/B` answers.
- Bounds are floored to Eastern calendar days and intra-day bounds do not sub-select, so a midnight-UTC to +24h window returns two sessions (780 one-minute candles instead of 390), and the extra session silently upserts.
- Take an ET session date at the adapter boundary, derive the ET-day bounds internally, and assert that the returned day set matches the request.
- A zero-width window (start equal to end) returns HTTP 400.
- Naive datetimes are read in the process's local timezone, so the adapter must reject them.
- `period` is ignored when explicit dates are sent; `needExtendedHoursData` defaults to true.
- A symbol Schwab does not know (delisted, bogus, or valid on a non-session day) returns HTTP 200 with `empty: true`, never 404, and these cases are indistinguishable.
- Count covered-but-empty symbol-days per run; a jump is the cheapest indicator of a silent source outage.
- No-trade buckets are omitted, so a row count under 390 per session is normal (illiquid names returned from 13 percent to 96 percent of buckets).
- Candle `datetime` is the bucket start in epoch milliseconds UTC.
- The extended session is 07:00 to 20:00 ET (pre-market before 07:00 is not served), a ceiling of 2.00x the regular-hours bar count; mega-caps saturate it and the illiquid tail is near 1.0x.
- Half-day behaviour was not measurable inside the history window.
- Daily history is split-adjusted (measured 2026-10-21 on the production ingest container with a read-only probe on the stored token).
- The probe took the ratio of the split-day close to the previous session's close on four known splits: NVDA 10-for-1 on 2024-06-10 gave 1.0075, AVGO 10-for-1 on 2024-07-15 gave 1.0080, CMG 50-for-1 on 2024-06-26 gave 1.0030 and WMT 3-for-1 on 2024-02-26 gave 1.0185; an unadjusted series would have shown about 0.1, 0.1, 0.02 and 0.33.
- Adjustment holds as of the fetch: bars stored before a later split stay at the old scale, so a stored series has to be compared with a fresh fetch (see "Daily bars" in `docs/operations.md`).
- Dividend adjustment was not measured.

## Symbol files (Nasdaq Trader and Cboe, captured 2026-07-17)

- `nasdaqlisted.txt`, `otherlisted.txt` and `options.txt` are plain unauthenticated GETs under `https://www.nasdaqtrader.com/dynamic/SymDir/`; the Cboe symbol directory CSV downloads without auth from its download URL.
- Listed universe excluding test issues was about 13,000 symbols (5,557 Nasdaq plus 7,467 other).
- `options.txt` is about 95 MB and took about 85 seconds; it carried 4,182 distinct underlyings, and Cboe carried 5,319 symbols, which includes about 25 index products.
- The optionable union was about 5,480 names, of which about 5,440 join to an active listing; neither directory is a superset.
- `options.txt` strips dots from class shares (`BRKB`) while the listing files and Cboe keep them; a derived dot-strip alias table (raw match first, alias only on a miss, drop ambiguous aliases) recovered 2 extra optionable names on that day.
- The Nasdaq files carry a creation-time footer (`File Creation Time: MMDDYYYYHH:MM` in Eastern time, followed by trailing pipes); Cboe has no as-of stamp.
- The Nasdaq files are re-stamped through the day (stamps of 12:11 ET were served at midday on 2026-10-27), so a file is the session's only once its stamp is after the close.
- Re-measured 2026-10-27: `options.txt` was 87 MB and a streamed, line-by-line parse that keeps only the underlying set took about 0.5 seconds; the Cboe download URL answers 200 directly (no redirect) with LF line ends.
- Test issues, dotted symbols and three symbologies per row in `otherlisted.txt` exist; the file line endings are CRLF.
