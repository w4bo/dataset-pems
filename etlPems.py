#!/usr/bin/env python3
"""Builds the schema pems_cleaned (the logical model of SCHEMA.md) from the raw tables of pems_original
(loadPems.py). The schema is rebuilt from scratch at every run: tables are filled with INSERT ... SELECT without keys
or indexes, which are built at the end.

    docker compose run --rm pems-etl                     # or, from the host: python etlPems.py
    python3 etlPems.py --segment-miles 0.5               # segments of half a mile (default: 1)
    PEMS_DSN=postgresql://user:pwd@host:5432/pems python3 etlPems.py     # a remote server (or --dsn ...)

Model (keys in SCHEMA.md):
    district(district_id)
    road(fwy, dir)                                   a freeway in one direction (postmiles and stations are per direction)
    road_in_district(district_id, fwy, dir)
    segment(fwy, dir, mile, from_pm, to_pm)          mile = floor(abs_pm / segment_miles)
    incident(incident_id, fwy, dir, mile, time_from, time_to, ...)
    incident_detail(incident_id, detail_id, ts, description)
    station(station_id, district_id)
    station_in_segment(station_id, valid_from, valid_to, fwy, dir, mile, abs_pm, latitude, longitude, type, ...)
    measurement(station_id, resolution_min, ts, lane, flow, occupancy, speed, samples, pct_observed, observed)
"""
import argparse
import os
import sys
import time

LOCAL_DSN = "postgresql://postgres:password@localhost:5435/pems"

try:
    import psycopg
except ImportError:
    sys.exit('Install psycopg: pip install "psycopg[binary]"')

STEPS = [
    ("schema", """
DROP SCHEMA IF EXISTS {dst} CASCADE;
CREATE SCHEMA {dst};
CREATE TABLE {dst}.etl_info (key text PRIMARY KEY, value text);
INSERT INTO {dst}.etl_info VALUES ('source_schema', '{src}'), ('segment_miles', '{seg}'), ('built_at', now()::text);
"""),
    # Station intervals: consecutive metadata versions of a station with the same attributes are merged into one
    # interval [valid_from, valid_to) (gaps and islands); a station missing from a version (removed) closes its interval
    # at that version. The versions are publication dates, so the intervals are approximate. The first loaded version
    # of each district is extended back to -infinity (data before it, or before the --from of loadPems.py, is attached
    # to it); an interval still open in the last version ends at +infinity.
    ("station_in_segment", """
CREATE TABLE {dst}.station_in_segment AS
WITH v AS (
    SELECT district, version_date, lead(version_date) OVER (PARTITION BY district ORDER BY version_date) AS next_version,
           version_date = min(version_date) OVER (PARTITION BY district) AS first_version
    FROM (SELECT DISTINCT district, version_date FROM {src}.station_meta) x),
r AS (
    SELECT m.*, v.next_version, v.first_version,
           md5(row(m.fwy, m.dir, m.county, m.city, m.state_pm, m.abs_pm, m.latitude, m.longitude, m.length, m.type,
                   m.lanes, m.name, m.user_id_1, m.user_id_2, m.user_id_3, m.user_id_4)::text) AS h
    FROM {src}.station_meta m JOIN v USING (district, version_date)),
b AS (
    SELECT r.*, CASE WHEN lag(h) OVER w = h AND lag(next_version) OVER w = version_date THEN 0 ELSE 1 END AS brk
    FROM r WINDOW w AS (PARTITION BY station_id ORDER BY version_date)),
i AS (SELECT b.*, sum(brk) OVER (PARTITION BY station_id ORDER BY version_date) AS island FROM b)
SELECT station_id,
       CASE WHEN bool_or(first_version) THEN '-infinity'::timestamp ELSE min(version_date)::timestamp END AS valid_from,
       coalesce((array_agg(next_version ORDER BY version_date DESC))[1]::timestamp, 'infinity') AS valid_to,
       min(district) AS district_id, fwy, dir, floor(abs_pm / {seg})::int AS mile, abs_pm, state_pm, county, city,
       latitude, longitude, length, type, lanes, name, user_id_1, user_id_2, user_id_3, user_id_4
FROM i
GROUP BY station_id, island, fwy, dir, county, city, state_pm, abs_pm, latitude, longitude, length, type, lanes, name,
         user_id_1, user_id_2, user_id_3, user_id_4;
"""),
    ("incident", """
CREATE TABLE {dst}.incident AS
SELECT incident_id, CASE WHEN dir IS NOT NULL THEN fwy END AS fwy, CASE WHEN fwy IS NOT NULL THEN dir END AS dir,
       CASE WHEN fwy IS NOT NULL AND dir IS NOT NULL THEN floor(abs_pm / {seg})::int END AS mile,
       ts AS time_from, ts + make_interval(mins => duration_min) AS time_to, duration_min, abs_pm, state_pm,
       district AS district_id, county_fips, city_fips, latitude, longitude, cc_code, incident_number, description,
       location, area, zoom_map, tb_xy, severity
FROM {src}.chp_incident;
CREATE TABLE {dst}.incident_detail AS
SELECT d.incident_id, d.detail_id, d.ts, d.description FROM {src}.chp_incident_detail d
WHERE EXISTS (SELECT 1 FROM {src}.chp_incident i WHERE i.incident_id = d.incident_id);
"""),
    ("station", """
CREATE TABLE {dst}.station AS
SELECT station_id, min(district_id) AS district_id FROM (
    SELECT station_id, district_id FROM {dst}.station_in_segment
    UNION SELECT DISTINCT station_id, district FROM {src}.station_5min
    UNION SELECT DISTINCT station_id, district FROM {src}.station_hour) s
GROUP BY station_id;
ALTER TABLE {dst}.station_in_segment DROP COLUMN district_id;
"""),
    ("road, segment, district", """
CREATE TABLE {dst}.segment AS
SELECT fwy, dir, mile, mile * {seg} AS from_pm, (mile + 1) * {seg} AS to_pm FROM (
    SELECT fwy, dir, mile FROM {dst}.station_in_segment WHERE fwy IS NOT NULL AND dir IS NOT NULL AND mile IS NOT NULL
    UNION SELECT fwy, dir, mile FROM {dst}.incident WHERE fwy IS NOT NULL AND mile IS NOT NULL) s;
CREATE TABLE {dst}.road AS
SELECT fwy, dir FROM {dst}.station_in_segment WHERE fwy IS NOT NULL AND dir IS NOT NULL
UNION SELECT fwy, dir FROM {dst}.incident WHERE fwy IS NOT NULL;
CREATE TABLE {dst}.district AS
SELECT district_id FROM {dst}.station UNION SELECT district_id FROM {dst}.incident WHERE district_id IS NOT NULL;
CREATE TABLE {dst}.road_in_district AS
SELECT s.district_id, i.fwy, i.dir FROM {dst}.station_in_segment i JOIN {dst}.station s USING (station_id)
WHERE i.fwy IS NOT NULL AND i.dir IS NOT NULL
UNION SELECT district_id, fwy, dir FROM {dst}.incident WHERE fwy IS NOT NULL AND district_id IS NOT NULL;
"""),
    # Station-level values are lane 0; the lanes of the files are 1, 2, ...
    ("measurement", """
CREATE TABLE {dst}.measurement (
    station_id integer NOT NULL, resolution_min smallint NOT NULL, ts timestamp NOT NULL, lane smallint NOT NULL,
    flow double precision, occupancy double precision, speed double precision, samples integer,
    pct_observed smallint, observed boolean);
INSERT INTO {dst}.measurement
SELECT station_id, 5, ts, 0, total_flow, avg_occupancy, avg_speed, samples, pct_observed, NULL FROM {src}.station_5min;
INSERT INTO {dst}.measurement
SELECT station_id, 5, ts, lane, flow, occupancy, speed, samples, NULL, observed FROM {src}.station_5min_lane;
INSERT INTO {dst}.measurement
SELECT station_id, 60, ts, 0, total_flow, avg_occupancy, avg_speed, samples, pct_observed, NULL FROM {src}.station_hour;
INSERT INTO {dst}.measurement
SELECT station_id, 60, ts, lane, flow, occupancy, speed, NULL, NULL, NULL FROM {src}.station_hour_lane;
"""),
    ("keys and indexes", """
ALTER TABLE {dst}.district ADD PRIMARY KEY (district_id);
ALTER TABLE {dst}.road ADD PRIMARY KEY (fwy, dir);
ALTER TABLE {dst}.road_in_district ADD PRIMARY KEY (district_id, fwy, dir),
    ADD FOREIGN KEY (district_id) REFERENCES {dst}.district, ADD FOREIGN KEY (fwy, dir) REFERENCES {dst}.road;
ALTER TABLE {dst}.segment ADD PRIMARY KEY (fwy, dir, mile), ADD FOREIGN KEY (fwy, dir) REFERENCES {dst}.road;
ALTER TABLE {dst}.station ADD PRIMARY KEY (station_id), ADD FOREIGN KEY (district_id) REFERENCES {dst}.district;
ALTER TABLE {dst}.station_in_segment ADD PRIMARY KEY (station_id, valid_from),
    ADD CHECK (valid_from < valid_to),
    ADD FOREIGN KEY (station_id) REFERENCES {dst}.station,
    ADD FOREIGN KEY (fwy, dir) REFERENCES {dst}.road,
    ADD FOREIGN KEY (fwy, dir, mile) REFERENCES {dst}.segment;
CREATE INDEX ON {dst}.station_in_segment (fwy, dir, mile);
ALTER TABLE {dst}.incident ADD PRIMARY KEY (incident_id),
    ADD FOREIGN KEY (district_id) REFERENCES {dst}.district,
    ADD FOREIGN KEY (fwy, dir) REFERENCES {dst}.road,
    ADD FOREIGN KEY (fwy, dir, mile) REFERENCES {dst}.segment;
CREATE INDEX ON {dst}.incident (fwy, dir, mile, time_from);
CREATE INDEX ON {dst}.incident (time_from);
ALTER TABLE {dst}.incident_detail ADD PRIMARY KEY (incident_id, detail_id),
    ADD FOREIGN KEY (incident_id) REFERENCES {dst}.incident;
ALTER TABLE {dst}.measurement ADD PRIMARY KEY (station_id, resolution_min, ts, lane),
    ADD CHECK (resolution_min IN (5, 60)), ADD CHECK (lane >= 0),
    ADD FOREIGN KEY (station_id) REFERENCES {dst}.station;
CREATE INDEX ON {dst}.measurement USING brin (ts);
ANALYZE {dst}.etl_info, {dst}.district, {dst}.road, {dst}.road_in_district, {dst}.segment, {dst}.station, {dst}.station_in_segment,
        {dst}.incident, {dst}.incident_detail, {dst}.measurement;
"""),
]

OVERLAPS = """
SELECT count(*) FROM {dst}.station_in_segment a JOIN {dst}.station_in_segment b
  ON a.station_id = b.station_id AND a.valid_from < b.valid_from AND b.valid_from < a.valid_to"""

CHECKS = [
    # estimates from the statistics of the ANALYZE at the end of the build (an exact count(*) of measurement takes
    # minutes on the full data)
    ("rows per table (estimated)", """
SELECT c.relname, greatest(c.reltuples, 0)::bigint FROM pg_class c
WHERE c.relnamespace = '{dst}'::regnamespace AND c.relkind = 'r' ORDER BY 1"""),
    ("station intervals overlapping another interval of the same station (must be 0)", OVERLAPS),
    ("incidents without a segment (no freeway, direction or postmile)", """
SELECT count(*) FILTER (WHERE mile IS NULL), count(*) FROM {dst}.incident"""),
    ("station-level measurements whose station has no interval at that time / all (1% sample)", """
SELECT count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM {dst}.station_in_segment s WHERE s.station_id = m.station_id
                                          AND m.ts >= s.valid_from AND m.ts < s.valid_to)), count(*)
FROM {dst}.measurement m TABLESAMPLE SYSTEM (1) REPEATABLE (0) WHERE lane = 0"""),
]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dsn", default=os.environ.get("PEMS_DSN", LOCAL_DSN),
                   help="PostgreSQL connection string, e.g. postgresql://user:pwd@host:5432/db?sslmode=require "
                        "(default: $PEMS_DSN, else the local docker-compose database)")
    p.add_argument("--source", default="pems_original", help="schema of the raw tables (default: pems_original)")
    p.add_argument("--target", default="pems_cleaned", help="schema to (re)build (default: pems_cleaned)")
    p.add_argument("--segment-miles", type=float, default=1.0, help="length of the segments in miles (default: 1)")
    a = p.parse_args()
    fmt = {"src": a.source, "dst": a.target, "seg": repr(a.segment_miles)}
    with psycopg.connect(a.dsn, autocommit=True) as conn:
        conn.execute("SET client_min_messages = warning")
        conn.execute("SET maintenance_work_mem = '1GB'")
        conn.execute("SET max_parallel_maintenance_workers = 4")
        t_all = time.time()
        with conn.transaction():  # all or nothing: a failed run leaves the previous pems_cleaned in place
            for name, sql in STEPS:
                t0 = time.time()
                conn.execute(sql.format(**fmt))
                print(f"{name}: {time.time() - t0:.1f}s", flush=True)
        print(f"{a.target} built in {time.time() - t_all:.0f}s (segments of {a.segment_miles} miles)\n\nChecks:",
              flush=True)
        for name, sql in CHECKS:
            rows = conn.execute(sql.format(**fmt)).fetchall()
            print(f"{name}: " + ", ".join(" ".join(str(v) for v in r) for r in rows), flush=True)
        if conn.execute(OVERLAPS.format(**fmt)).fetchone()[0]:
            sys.exit("Station intervals overlap: pems_cleaned is inconsistent")


if __name__ == "__main__":
    main()
