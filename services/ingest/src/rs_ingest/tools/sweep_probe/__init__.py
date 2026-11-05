"""The sweep probe: measures what Schwab chains and bars look like at scale and how a
chain changes from the close to the next morning, before chain storage is designed.

    python -m rs_ingest.tools.sweep_probe run --out DIR      # capture (hours; restartable)
    python -m rs_ingest.tools.sweep_probe summary DIR        # counts, times, ratios only
"""
