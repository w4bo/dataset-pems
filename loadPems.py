#!/usr/bin/env python3
"""Bulk-loads the raw PeMS files of ./data (downloadPems.py) into the schema pems_original of the PostgreSQL of
docker-compose.yaml (service pems-postgres, database pems). etlPems.py then builds the schema pems_cleaned from it.
Both schemas are documented in SCHEMA.md.

    docker compose up -d pems-postgres
    docker compose run --rm pems-loader                  # or, from the host: pip install "psycopg[binary]"; python loadPems.py
    python3 loadPems.py                                  # every file not loaded yet, then the keys and indexes
    python3 loadPems.py --types meta incidents           # only the station metadata and the CHP incidents
    python3 loadPems.py --from 2026-08-01                # only the data from August 2026 on (see --from)
    python3 loadPems.py --types 5min --district 3 --from 2026-07-01 --to 2026-07-31
    python3 loadPems.py --workers 8                      # files loaded in parallel (default: 4)
    python3 loadPems.py --keys-only                      # only (re)build the keys and indexes
    PEMS_DSN=postgresql://user:pwd@host:5432/pems python3 loadPems.py    # a remote server (or --dsn ...)

--from/--to select the files by the date in their name: station files of the days/months in the range, CHP files
of the days/months overlapping it, and the metadata versions published in it plus, for each district, the last
version published before --from (the one still valid at --from).

Raw data: every row and column of the files is stored. The per-lane columns of the station files go to
station_5min_lane / station_hour_lane, one row per station, interval and lane (lane 1, 2, ...; lanes without data are
skipped), with a foreign key to the station row. Values are parsed strictly: a value that does not parse
makes its file fail (it is reported and not marked as loaded) instead of being stored as NULL. Empty values are NULL.

Bulk loading: every file is appended with COPY into tables without primary keys and indexes; the keys and indexes of
the tables a run loads into (and the foreign keys) are dropped before it and built once after it. The station files are loaded in date
order (all districts of a day/month, then the next), so the rows of station_5min and station_hour are stored roughly
by time and a small BRIN index on ts is enough for time-range queries. If a primary key cannot be built because of
duplicate rows (e.g. a CHP day file overlapping a month file), the duplicates are deleted (one row per key is kept)
and the key is built again. Each file is loaded in one transaction together with its row in loaded_file, so an
interrupted run can be repeated: the files already loaded are skipped and the missing keys are rebuilt.
"""
import argparse
import csv
import gzip
import io
import os
import re
import sys
import tempfile
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

LOCAL_DSN = "postgresql://postgres:password@localhost:5435/pems"

try:
    import psycopg
except ImportError:
    sys.exit('Install psycopg: pip install "psycopg[binary]"')

ROOT = Path(__file__).resolve().parent
META_RE = re.compile(r"^d(\d{2})_text_meta_(\d{4})_(\d{2})_(\d{2})\.txt$")
FIVE_RE = re.compile(r"^d(\d{2})_text_station_5min_(\d{4})_(\d{2})_(\d{2})\.txt\.gz$")
HOUR_RE = re.compile(r"^d(\d{2})_text_station_hour_(\d{4})_(\d{2})(?:_(\d{2}))?\.txt\.gz$")
INC_RE = re.compile(r"^all_text_chp_incidents_(day|month)_(\d{4})_(\d{2})(?:_(\d{2}))?\.txt\.zip$")

STATION_COLS = """ts timestamp NOT NULL, station_id integer NOT NULL, district smallint, fwy text, dir text,
    lane_type text, station_length double precision, samples integer, pct_observed smallint, total_flow integer,
    avg_occupancy double precision, avg_speed double precision"""

# Tables without keys and indexes: they are built after the load (see KEYS)
SCHEMA = f"""
SET client_min_messages = warning;
CREATE TABLE IF NOT EXISTS loaded_file (
    file_name text PRIMARY KEY, kind text NOT NULL, rows bigint NOT NULL, seconds real, loaded_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS station_meta (
    district smallint NOT NULL, version_date date NOT NULL, station_id integer NOT NULL,
    fwy text, dir text, county integer, city text, state_pm text, abs_pm double precision,
    latitude double precision, longitude double precision, length double precision, type text, lanes smallint,
    name text, user_id_1 text, user_id_2 text, user_id_3 text, user_id_4 text);
CREATE TABLE IF NOT EXISTS station_5min ({STATION_COLS});
CREATE TABLE IF NOT EXISTS station_hour ({STATION_COLS});
CREATE TABLE IF NOT EXISTS station_5min_lane (
    ts timestamp NOT NULL, station_id integer NOT NULL, lane smallint NOT NULL,
    samples integer, flow integer, occupancy double precision, speed double precision, observed boolean);
CREATE TABLE IF NOT EXISTS station_hour_lane (
    ts timestamp NOT NULL, station_id integer NOT NULL, lane smallint NOT NULL,
    flow double precision, occupancy double precision, speed double precision);
CREATE TABLE IF NOT EXISTS chp_incident (
    incident_id bigint NOT NULL, cc_code text, incident_number text, ts timestamp, description text, location text,
    area text, zoom_map text, tb_xy text, latitude double precision, longitude double precision, district smallint,
    county_fips integer, city_fips integer, fwy text, dir text, state_pm text, abs_pm double precision,
    severity text, duration_min integer);
CREATE TABLE IF NOT EXISTS chp_incident_detail (
    incident_id bigint NOT NULL, detail_id bigint NOT NULL, ts timestamp, description text);
"""

# table -> (primary key, secondary indexes), built after the load
KEYS = {
    "station_meta": ("station_id, version_date", {"station_meta_version": "(district, version_date)"}),
    "chp_incident": ("incident_id", {"chp_incident_pos": "(fwy, dir, abs_pm)", "chp_incident_ts": "(ts)"}),
    "chp_incident_detail": ("incident_id, detail_id", {}),
    "station_5min": ("station_id, ts", {"station_5min_ts": "USING brin (ts)"}),
    "station_hour": ("station_id, ts", {"station_hour_ts": "USING brin (ts)"}),
    "station_5min_lane": ("station_id, ts, lane", {"station_5min_lane_ts": "USING brin (ts)"}),
    "station_hour_lane": ("station_id, ts, lane", {"station_hour_lane_ts": "USING brin (ts)"}),
}
# table -> (columns, referenced table (columns)), built after the primary keys
FKS = {
    "station_5min_lane": ("station_id, ts", "station_5min (station_id, ts)"),
    "station_hour_lane": ("station_id, ts", "station_hour (station_id, ts)"),
}
TABLES = {"meta": ["station_meta"], "incidents": ["chp_incident", "chp_incident_detail"],
          "5min": ["station_5min_lane", "station_5min"], "hour": ["station_hour_lane", "station_hour"]}

# per-lane fields of the station files, in file order: (column, is_lane_data). The 5-minute "observed" flag is 0 also
# for the lanes that do not exist, so a lane is stored only if one of its other fields has a value.
LANES = {
    "station_5min": [("samples", True), ("flow", True), ("occupancy", True), ("speed", True), ("observed", False)],
    "station_hour": [("flow", True), ("occupancy", True), ("speed", True)],
}

# ------------------------------------------------------------------------------------------------------------ parsing
def num(s, f=float):
    """Empty -> None; otherwise the number, or ValueError (strict: a bad value fails the file)."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return f(s)
    except ValueError:
        if f is int and float(s).is_integer():  # "3.0"
            return int(float(s))
        raise ValueError(f"not a number: {s!r}") from None


def txt(s):
    s = (s or "").strip()
    return s or None


def ts(s):
    s = (s or "").strip()
    return datetime.strptime(s, "%m/%d/%Y %H:%M:%S") if s else None


def meta_rows(path, district, vdate):
    with open(path, newline="", encoding="utf-8") as fh:
        for k, r in enumerate(csv.DictReader(fh, delimiter="\t"), 2):
            try:
                if not (r.get("ID") or "").strip():
                    raise ValueError("empty ID")
                if any(v.strip() for v in r.get(None) or []):  # some files have an empty extra field: ignored
                    raise ValueError(f"extra fields {r[None]}")
                yield (district, vdate, num(r["ID"], int), txt(r["Fwy"]), txt(r["Dir"]), num(r["County"], int),
                       txt(r["City"]), txt(r["State_PM"]), num(r["Abs_PM"]), num(r["Latitude"]), num(r["Longitude"]),
                       num(r["Length"]), txt(r["Type"]), num(r["Lanes"], int), txt(r["Name"]), txt(r.get("User_ID_1")),
                       txt(r.get("User_ID_2")), txt(r.get("User_ID_3")), txt(r.get("User_ID_4")))
            except (ValueError, KeyError) as e:
                raise ValueError(f"line {k}: {e}") from None


def station_lines(fh, lanes, lane_out):
    """Yields the station-level lines of a station file in COPY text format (its first 12 fields) and writes its
    per-lane lines (ts, station_id, lane, lane fields) to [lane_out]. The values are passed as they are: PostgreSQL
    parses them (strictly) during COPY."""
    g, n12 = len(lanes), 12
    data_idx = [j for j, (_, is_data) in enumerate(lanes) if is_data]
    for k, line in enumerate(fh, 1):
        p = line.rstrip("\r\n").split(",")
        if len(p) < n12 or (len(p) - n12) % g:
            raise ValueError(f"line {k}: {len(p)} fields")
        yield "\t".join(v if v else r"\N" for v in p[:n12])
        for lane, i in enumerate(range(n12, len(p), g), 1):
            f = p[i:i + g]
            if any(f[j] for j in data_idx):
                lane_out.write(f"{p[0]}\t{p[1]}\t{lane}\t" + "\t".join(v if v else r"\N" for v in f) + "\n")


def incident_rows(text):
    pending = None  # a row cut by line breaks inside a field (e.g. the location): glued back with its next lines
    for k, r in enumerate(csv.reader(io.StringIO(text)), 1):
        if pending is not None:
            pending[-1] += "\n" + (r[0] if r else "")
            pending += r[1:]
            if len(pending) < 20:
                continue
            r, pending = pending, None
        elif not r:
            continue
        elif len(r) < 20:
            pending = r
            continue
        if len(r) != 20:
            raise ValueError(f"incidents line {k}: {len(r)} fields")
        try:
            yield (num(r[0], int), txt(r[1]), txt(r[2]), ts(r[3]), txt(r[4]), txt(r[5]), txt(r[6]), txt(r[7]),
                   txt(r[8]), num(r[9]), num(r[10]), num(r[11], int), num(r[12], int), num(r[13], int), txt(r[14]),
                   txt(r[15]), txt(r[16]), num(r[17]), txt(r[18]), num(r[19], int))
        except ValueError as e:
            raise ValueError(f"incidents line {k}: {e}") from None
    if pending is not None:
        raise ValueError(f"incidents: last row has {len(pending)} fields")


def detail_rows(text):
    # a description with newlines is split over several lines: they are glued back to their row
    rows = []
    for k, r in enumerate(csv.reader(io.StringIO(text)), 1):
        if len(r) == 4 and r[0].isdigit() and r[1].isdigit():
            rows.append(r)
        elif rows:
            rows[-1][3] += "\n" + ",".join(r)
        elif r:
            raise ValueError(f"details line {k}: continuation line before any row")
    for r in rows:
        yield int(r[0]), int(r[1]), ts(r[2]), r[3]


def read_member(z, name):
    b = z.read(name)
    if name.endswith(".gz"):
        b = gzip.decompress(b)
    return b.decode("utf-8")


# ------------------------------------------------------------------------------------------------------------ loading
def load_file(dsn, schema, path, kind, district, day):
    """Loads one file in one transaction (run in a worker process). Returns (rows, seconds)."""
    t0, n = time.time(), 0
    with psycopg.connect(dsn) as conn:
        conn.execute("SET datestyle = 'ISO, MDY'")
        conn.execute(f"SET search_path = {schema}")
        with conn.transaction(), conn.cursor() as cur:
            if kind == "meta":
                table = "station_meta"
                with cur.copy("COPY station_meta (district, version_date, station_id, fwy, dir, county, city, state_pm, "
                              "abs_pm, latitude, longitude, length, type, lanes, name, user_id_1, user_id_2, user_id_3, "
                              "user_id_4) FROM STDIN") as cp:
                    for row in meta_rows(path, district, day):
                        cp.write_row(row)
                        n += 1
            elif kind == "incidents":
                table = "chp_incident"
                with zipfile.ZipFile(path) as z:
                    for name in z.namelist():
                        text = read_member(z, name)
                        if "_det_" in name:
                            with cur.copy("COPY chp_incident_detail (incident_id, detail_id, ts, description) "
                                          "FROM STDIN") as cp:
                                for row in detail_rows(text):
                                    cp.write_row(row)
                        else:
                            with cur.copy("COPY chp_incident FROM STDIN") as cp:
                                for row in incident_rows(text):
                                    cp.write_row(row)
                                    n += 1
            else:
                table = "station_5min" if kind == "5min" else "station_hour"
                lanes = LANES[table]
                # one COPY at a time per connection: the lane lines are spooled to a temporary file meanwhile
                with tempfile.TemporaryFile("w+", encoding="utf-8") as lane_out:
                    with cur.copy(f"COPY {table} (ts, station_id, district, fwy, dir, lane_type, station_length, "
                                  "samples, pct_observed, total_flow, avg_occupancy, avg_speed) FROM STDIN") as cp, \
                            gzip.open(path, "rt", encoding="utf-8") as fh:
                        buf = []
                        for line in station_lines(fh, lanes, lane_out):
                            buf.append(line)
                            if len(buf) >= 20000:
                                cp.write("\n".join(buf) + "\n")
                                n += len(buf)
                                buf.clear()
                        if buf:
                            cp.write("\n".join(buf) + "\n")
                            n += len(buf)
                    lane_out.seek(0)
                    with cur.copy(f"COPY {table}_lane (ts, station_id, lane, {', '.join(c for c, _ in lanes)}) "
                                  "FROM STDIN") as cp:
                        while chunk := lane_out.read(1 << 22):
                            cp.write(chunk)
            cur.execute("INSERT INTO loaded_file (file_name, kind, rows, seconds) VALUES (%s, %s, %s, %s)",
                        (path.name, table, n, time.time() - t0))
    return n, time.time() - t0


def run_job(job):
    dsn, schema, path, kind, district, day = job
    try:
        return path.name, *load_file(dsn, schema, path, kind, district, day), None
    except Exception as e:  # noqa: BLE001
        return path.name, 0, 0.0, f"{type(e).__name__}: {e}".splitlines()[0][:300]


# ---------------------------------------------------------------------------------------------------- keys and indexes
def drop_keys(conn, table):
    """Drops the foreign keys from and to [table], its primary key and its indexes, so that COPY only appends."""
    for t, con in conn.execute("SELECT conrelid::regclass::text, conname FROM pg_constraint WHERE contype = 'f' "
                               "AND (conrelid = %s::regclass OR confrelid = %s::regclass)", (table, table)).fetchall():
        conn.execute(f"ALTER TABLE {t} DROP CONSTRAINT {con}")
    rows = conn.execute("""
        SELECT i.relname, c.conname FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
        LEFT JOIN pg_constraint c ON c.conindid = x.indexrelid AND c.conrelid = x.indrelid
        WHERE x.indrelid = %s::regclass""", (table,)).fetchall()
    for index, constraint in rows:
        conn.execute(f"ALTER TABLE {table} DROP CONSTRAINT {constraint}" if constraint else f"DROP INDEX {index}")


def add_pk(conn, table, cols):
    """Builds the primary key of [table]; if there are duplicates, deletes them (one row per key is kept) and retries."""
    if conn.execute("SELECT 1 FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'", (table,)).fetchone():
        return False
    try:
        conn.execute(f"ALTER TABLE {table} ADD PRIMARY KEY ({cols})")
    except psycopg.errors.UniqueViolation:
        n = conn.execute(f"DELETE FROM {table} WHERE ctid IN (SELECT ctid FROM (SELECT ctid, row_number() OVER "
                         f"(PARTITION BY {cols}) AS rn FROM {table}) d WHERE rn > 1)").rowcount
        print(f"  {table}: deleted {n} duplicate rows of ({cols})", flush=True)
        conn.execute(f"ALTER TABLE {table} ADD PRIMARY KEY ({cols})")
    return True


def build_keys(conn):
    """Builds the keys and indexes missing after a load (idempotent: what exists is left alone)."""
    for table, (pk, indexes) in KEYS.items():
        t0 = time.time()
        built = add_pk(conn, table, pk)
        for name, cols in indexes.items():
            built |= conn.execute("SELECT to_regclass(%s)", (name,)).fetchone()[0] is None
            conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} {cols}")
        if built:
            conn.execute(f"ANALYZE {table}")
            print(f"Keys of {table} built in {time.time() - t0:.1f}s", flush=True)
    for table, (cols, ref) in FKS.items():
        if not conn.execute("SELECT 1 FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'f'",
                            (table,)).fetchone():
            t0 = time.time()
            conn.execute(f"ALTER TABLE {table} ADD CONSTRAINT {table}_station_fkey FOREIGN KEY ({cols}) REFERENCES {ref}")
            print(f"Foreign key of {table} built in {time.time() - t0:.1f}s", flush=True)


# --------------------------------------------------------------------------------------------------------------- main
def find_jobs(a, loaded):
    """[(file, kind, district, day)] of the files to load: metadata and incidents first, then the station files in
    date order (then district), so that their rows are stored roughly by time (see the BRIN indexes)."""
    lo, hi = a.date_from or date.min, a.date_to or date.max
    in_district = lambda d: not a.district or d in a.district  # noqa: E731
    jobs, metas = [], {}
    for f in sorted(a.dir.iterdir()):
        if "meta" in a.types and (m := META_RE.match(f.name)):
            if in_district(int(m[1])):
                metas.setdefault(int(m[1]), []).append((date(int(m[2]), int(m[3]), int(m[4])), f))
        elif "5min" in a.types and (m := FIVE_RE.match(f.name)):
            day = date(int(m[2]), int(m[3]), int(m[4]))
            if in_district(int(m[1])) and lo <= day <= hi:
                jobs.append((f, "5min", int(m[1]), day))
        elif "hour" in a.types and (m := HOUR_RE.match(f.name)):
            day = date(int(m[2]), int(m[3]), int(m[4] or 1))
            if in_district(int(m[1])) and lo.replace(day=1) <= day <= hi:
                jobs.append((f, "hour", int(m[1]), day))
        elif "incidents" in a.types and (m := INC_RE.match(f.name)):
            first = date(int(m[2]), int(m[3]), int(m[4] or 1))
            last = first if m[1] == "day" else next_month(first) - timedelta(days=1)
            if last >= lo and first <= hi:
                jobs.append((f, "incidents", None, first))
    for district, versions in metas.items():
        before = [v for v in versions if v[0] < lo]
        keep = before[-1:] + [v for v in versions if lo <= v[0] <= hi]
        jobs += [(f, "meta", district, vdate) for vdate, f in keep]
    jobs = [j for j in jobs if j[0].name not in loaded]
    return sorted(jobs, key=lambda j: (j[1] in ("5min", "hour"), j[3] or date.min, j[2] or 0, j[0].name))


def next_month(d):
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dsn", default=os.environ.get("PEMS_DSN", LOCAL_DSN),
                   help="PostgreSQL connection string, e.g. postgresql://user:pwd@host:5432/db?sslmode=require "
                        "(default: $PEMS_DSN, else the local docker-compose database)")
    p.add_argument("--schema", default="pems_original", help="schema of the raw tables (default: pems_original)")
    p.add_argument("--dir", type=Path, default=ROOT / "data", help="folder of the downloaded files (default: ./data)")
    p.add_argument("--types", nargs="+", default=["meta", "incidents", "hour", "5min"], choices=["meta", "incidents", "5min", "hour"])
    p.add_argument("--district", nargs="+", type=int, help="only these districts (meta, 5min and hour)")
    p.add_argument("--from", dest="date_from", type=date.fromisoformat, help="first day (YYYY-MM-DD), see above")
    p.add_argument("--to", dest="date_to", type=date.fromisoformat, help="last day (YYYY-MM-DD), see above")
    p.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1), help="files loaded in parallel (default: 4)")
    p.add_argument("--keys-only", action="store_true", help="only build the missing keys and indexes")
    a = p.parse_args()

    with psycopg.connect(a.dsn, autocommit=True) as conn:
        conn.execute("SET maintenance_work_mem = '1GB'")
        conn.execute("SET max_parallel_maintenance_workers = 4")
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {a.schema}")
        conn.execute(f"SET search_path = {a.schema}")
        conn.execute(SCHEMA)
        old = conn.execute("""SELECT c.relkind = 'p' OR EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid
                                  AND a.attname = 'lane_flow') FROM pg_class c WHERE c.oid = 'station_5min'::regclass""")
        if old.fetchone()[0]:
            sys.exit("The tables were created by an older loadPems.py: recreate the database "
                     "(docker compose down -v; docker compose up -d pems-postgres) and load again.")
        failed = []
        if not a.keys_only:
            loaded = {r[0] for r in conn.execute("SELECT file_name FROM loaded_file")}
            jobs = find_jobs(a, loaded)
            print(f"{len(jobs)} files to load ({len(loaded)} already loaded), {a.workers} worker(s)", flush=True)
            for table in {t for _, kind, _, _ in jobs for t in TABLES[kind]}:
                drop_keys(conn, table)
            rows, t_all = 0, time.time()
            with ProcessPoolExecutor(max_workers=max(1, a.workers)) as pool:
                for k, (name, n, secs, err) in enumerate(
                        pool.map(run_job, [(a.dsn, a.schema, f, kind, d, day) for f, kind, d, day in jobs]), 1):
                    rows += n
                    print(f"[{k}/{len(jobs)}] {name}: " + (f"FAILED {err}" if err else f"{n} rows in {secs:.1f}s"),
                          flush=True)
                    if err:
                        failed.append(name)
            if jobs:
                print(f"Loaded {rows} rows in {time.time() - t_all:.0f}s", flush=True)
            if failed:
                print(f"{len(failed)} files failed (not marked as loaded: fix and run again): {', '.join(failed)}")
        build_keys(conn)
    if not (a.keys_only) and failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
