"""RocketStocks v2 one-shot backfills: the v1 popularity import.

v1 wrote `popularity.datetime` as a naive local timestamp in a container zone
nobody documented. The import therefore runs in three steps. Steps 2 and 3 use
the v2 database named by RS_DB_URL (the migrate/owner DSN, same as rs_migrate:
`import_verification` is granted to no service role).

1. Dump v1's table. Either format works; neither carries a time zone.

    pg_dump --data-only --table=popularity --no-owner "$V1_DSN" > popularity.sql
    psql "$V1_DSN" -c "\\copy popularity TO 'popularity.csv' CSV HEADER"

   For a dockerised v1, run pg_dump inside the postgres container:
   `docker exec <container> pg_dump -U <user> -d <db> --data-only --table=popularity > popularity.sql`.

2. While v1 and v2 are both polling ApeWisdom, find the zone from the overlap
   and record it (exit 0 only when one candidate matches at least 99%):

    python -m rs_backfill verify-offset --dump popularity.sql

3. Import, naming the recorded zone. Reruns insert zero rows.

    python -m rs_backfill popularity-import --dump popularity.sql --zone America/Chicago

Never commit a real dump: it is vendor data.
"""
