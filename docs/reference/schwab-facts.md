# Measured Schwab facts

Aggregate facts measured against the Schwab market-data API.
Only aggregates are kept here (counts, rates, medians, durations); vendor rows and raw payloads are never committed.
Measured against SPY and a handful of other symbols in July and August 2026 unless stated.

## REST budget

- The documented market-data limit is 120 requests per minute.
- A burst probe ramped 120 requests to 4 per second (240 per minute pace): all 200, no 429, no `Retry-After`, no rate-limit headers.
- A day-long chain collector added 900 more requests with zero 429s.
- Treat 240 per minute as a lower bound and plan on the documented 120.
- Revisit only if production ever sees a 429, and alarm when it does.
- Unfiltered SPY and QQQ option chains return 502 (the response exceeds roughly 6 MB or 5k contracts), so index ETFs need bounded parameters.
- Bounded chain requests also failed sporadically: 39 of 900 (about 4 percent) returned 502, so the nightly scan needs retry-once-then-Tombstone.
- A full nightly options pass is roughly 6.5k chains, about 68 minutes of sequenced calls.

## Option chain freshness

- Consecutive chain snapshots are not monotonic: one SPY response served chain-wide quotes about 14 minutes stale (`quoteTimeInLong` regressed by a median of 841 seconds), which showed up as roughly 800 apparent volume decreases in a single snapshot pair.
- Of 283 SPY snapshot pairs, 281 were clean; across the other nine symbols only 2 negative deltas appeared (TSLA).
- Rule: record the per-row quote time and reject rows or whole snapshots whose quote time regresses against the last accepted snapshot.
- Open interest does not change intraday (zero intraday changes observed on every symbol) and settles once a day, so a day's last snapshot carries that session's settled open interest.
- Day-over-day open-interest change from consecutive nightly snapshots was large and clean: 1,100 to 2,700 contracts per underlying moved, with top single-contract moves of thousands to tens of thousands.
- Compute net change over the shared contract roster, not book totals; for daily-expiry ETFs expiry roll-off dominates book totals (SPY shared-net change was more than double the book-net change).

## Price history

- One-minute history depth is about 47 calendar days (about 33 trading days), identical for every symbol tested from SPY to illiquid names, so it is a server-side global window.
- Five-minute depth is about 207 days and daily depth is about 30 years.
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

## Streaming

- Level-one equity and option streams ran on one shared streamer connection for a full session with zero disconnects (largest gap 2 seconds, cadence about 1 per second per service).
- The first message after subscribe is a full snapshot; every later message carries only changed fields, so handlers must accept any field subset.
- `TOTAL_VOLUME` is cumulative for the day including pre-market, so bar volume is a delta.
- `TRADE_TIME_MILLIS` is the event-time basis for bucketing; quote-only updates carry quote time instead.
- Stream-built 5-minute bars matched REST candles on price (typically within 0.05 on a roughly 750 instrument, worst bucket 0.28 at the closing auction).
- Stream volume ran systematically 9 to 26 percent above official candle volume, with isolated spikes.
- Schwab allows one streamer connection per account.

## Symbol files (Nasdaq Trader and Cboe, captured 2026-07-17)

- `nasdaqlisted.txt`, `otherlisted.txt` and `options.txt` are plain unauthenticated GETs under `https://www.nasdaqtrader.com/dynamic/SymDir/`; the Cboe symbol directory CSV downloads without auth from its download URL.
- Listed universe excluding test issues was about 13,000 symbols (5,557 Nasdaq plus 7,467 other).
- `options.txt` is about 95 MB and took about 85 seconds; it carried 4,182 distinct underlyings, and Cboe carried 5,319 symbols, which includes about 25 index products.
- The optionable union was about 5,480 names, of which about 5,440 join to an active listing; neither directory is a superset.
- `options.txt` strips dots from class shares (`BRKB`) while the listing files and Cboe keep them; a derived dot-strip alias table (raw match first, alias only on a miss, drop ambiguous aliases) recovered 2 extra optionable names on that day.
- The Nasdaq files carry a creation-time footer; Cboe has no as-of stamp.
- Test issues, dotted symbols and three symbologies per row in `otherlisted.txt` exist; the file line endings are CRLF.
