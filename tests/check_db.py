#!/usr/bin/env python3
"""Checks the database built from tests/data by loadPems.py and etlPems.py (run by the GitHub CI).

    PEMS_DSN=postgresql://... python3 tests/check_db.py
"""
import os
import sys

import psycopg

DSN = os.environ.get("PEMS_DSN", "postgresql://postgres:password@localhost:5435/pems")

# (description, query returning one value, expected value)
CHECKS = [
    ("files loaded", "SELECT count(*) FROM pems_original.loaded_file", 6),
    ("metadata versions", "SELECT count(DISTINCT version_date) FROM pems_original.station_meta", 2),
    ("raw tables without rows", """
        SELECT count(*) FROM (VALUES ((SELECT count(*) FROM pems_original.station_meta)),
            ((SELECT count(*) FROM pems_original.station_5min)), ((SELECT count(*) FROM pems_original.station_5min_lane)),
            ((SELECT count(*) FROM pems_original.station_hour)), ((SELECT count(*) FROM pems_original.station_hour_lane)),
            ((SELECT count(*) FROM pems_original.chp_incident)),
            ((SELECT count(*) FROM pems_original.chp_incident_detail))) t(n) WHERE n = 0""", 0),
    ("lane rows whose station row is missing", """
        SELECT count(*) FROM pems_original.station_5min_lane l WHERE NOT EXISTS
            (SELECT 1 FROM pems_original.station_5min s WHERE s.station_id = l.station_id AND s.ts = l.ts)""", 0),
    ("foreign keys in pems_original", """
        SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND connamespace = 'pems_original'::regnamespace""", 2),
    ("measurements = raw station + lane rows", """
        SELECT (SELECT count(*) FROM pems_cleaned.measurement) = (SELECT count(*) FROM pems_original.station_5min)
             + (SELECT count(*) FROM pems_original.station_5min_lane) + (SELECT count(*) FROM pems_original.station_hour)
             + (SELECT count(*) FROM pems_original.station_hour_lane)""", True),
    ("incidents = raw incidents", """
        SELECT (SELECT count(*) FROM pems_cleaned.incident) = (SELECT count(*) FROM pems_original.chp_incident)""", True),
    ("removed station 3044051: one interval ending at the version that drops it", """
        SELECT string_agg(valid_from || ' - ' || valid_to, '; ') FROM pems_cleaned.station_in_segment
        WHERE station_id = 3044051""", "-infinity - 2026-08-12 00:00:00"),
    ("added station 3010021: one interval from the version that adds it", """
        SELECT string_agg(valid_from || ' - ' || valid_to, '; ') FROM pems_cleaned.station_in_segment
        WHERE station_id = 3010021""", "2026-08-12 00:00:00 - infinity"),
    ("changed station 318451: two intervals split at the new version", """
        SELECT string_agg(valid_from || ' - ' || valid_to, '; ' ORDER BY valid_from) FROM pems_cleaned.station_in_segment
        WHERE station_id = 318451""", "-infinity - 2026-08-12 00:00:00; 2026-08-12 00:00:00 - infinity"),
    ("overlapping station intervals", """
        SELECT count(*) FROM pems_cleaned.station_in_segment a JOIN pems_cleaned.station_in_segment b
        ON a.station_id = b.station_id AND a.valid_from < b.valid_from AND b.valid_from < a.valid_to""", 0),
    ("foreign keys in pems_cleaned", """
        SELECT count(*) > 10 FROM pg_constraint WHERE contype = 'f' AND connamespace = 'pems_cleaned'::regnamespace""",
     True),
]


def main():
    failed = 0
    with psycopg.connect(DSN) as conn:
        for name, sql, expected in CHECKS:
            got = conn.execute(sql).fetchone()[0]
            ok = got == expected
            failed += not ok
            print(f"{'ok  ' if ok else 'FAIL'} {name}: {got!r}" + ("" if ok else f" (expected {expected!r})"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
