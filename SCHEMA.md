# PeMS data: files and database schemas

Pipeline, in the PostgreSQL of `docker-compose.yaml` (`localhost:5435`, database `pems`, user `postgres` / `password`):

```
./data  --downloadPems.py-->  raw files
        --loadPems.py------>  schema pems_original   raw tables, one per file type, every row and column of the files
        --etlPems.py------->  schema pems_cleaned    logical model: roads, segments, stations, incidents, measurements
```

```
docker compose up -d pems-postgres
docker compose run --rm pems-loader --from 2026-08-01    # load the files from August 2026 on
docker compose run --rm pems-etl                         # rebuild pems_cleaned (--segment-miles 1 by default)
```

`loadPems.py --from/--to` selects the files by the date in their name: station files of the days/months in the
range, CHP files of the days/months overlapping it, and the metadata versions published in it plus, for each
district, the last version published before `--from` (the one still valid at `--from`). `etlPems.py` rebuilds
`pems_cleaned` from scratch, in one transaction (a failed run leaves the previous one in place).

**Remote server.** Both scripts connect to `$PEMS_DSN` (or `--dsn`), by default the local docker-compose database.
The user needs the right to create schemas in the database; nothing else is server-specific (the data is sent with
`COPY ... FROM STDIN`, so the files stay on the client).

```
export PEMS_DSN="postgresql://user:pwd@host:5432/pems?sslmode=require"
python loadPems.py --from 2026-08-01 && python etlPems.py
# or with docker, without starting the local database:
docker compose run --rm --no-deps -e PEMS_DSN="$PEMS_DSN" pems-loader --from 2026-08-01
docker compose run --rm --no-deps -e PEMS_DSN="$PEMS_DSN" pems-etl
```

**CI** (`.github/workflows/ci.yml`, on every push and pull request): loads the small test files of `tests/data` into
a PostgreSQL 17 service, loads them again (nothing must be loaded twice), builds `pems_cleaned`, runs the checks of
`tests/check_db.py`, and publishes the dump of the database (`pg_dump --format=custom`, binary) as the artifact
`pems-dump` (restore it with `pg_restore --no-owner -d <database> pems.dump`). The test files are subsets of real
files in the same formats (two metadata versions of district 3, a few stations' 5-minute and hourly data, the
district's incidents of August 2026), built by `tests/make_fixtures.py`.

## 1. Raw files (`./data`)

| File pattern | Content | Format | Loaded into |
|---|---|---|---|
| `dNN_text_meta_YYYY_MM_DD.txt` | Station metadata of district NN, one version per date (published when the stations change) | tab-separated, with header | `station_meta` |
| `dNN_text_station_5min_YYYY_MM_DD.txt.gz` | One day of 5-minute station data | comma-separated, no header | `station_5min`, `station_5min_lane` |
| `dNN_text_station_hour_YYYY_MM.txt.gz` | One month of hourly station data | comma-separated, no header | `station_hour`, `station_hour_lane` |
| `all_text_chp_incidents_{day,month}_*.txt.zip` | CHP incidents of all districts; the zip holds two gzipped files: incidents and incident details | comma-separated, no header | `chp_incident`, `chp_incident_detail` |

### Station metadata (`*_text_meta_*`)
`ID, Fwy, Dir, District, County, City, State_PM, Abs_PM, Latitude, Longitude, Length, Type, Lanes, Name,
User_ID_1..4`. `Type` is the station type: `ML` mainline, `HV` HOV, `OR` on-ramp, `FR` off-ramp, `FF` freeway-to-freeway
connector, `CD` collector/distributor, `CH` conventional highway. Some district 8 and 11 files have an empty extra
field at the end of a few lines; it is ignored.

### 5-minute vs hourly station data
Both files have the **same 12 station-level columns**. Only the per-lane columns that follow them differ:

| # | Column | 5-minute | Hourly |
|---|---|---|---|
| 1 | Timestamp (`MM/DD/YYYY HH24:MI:SS`, start of the interval, local time) | every 5 min | every hour |
| 2 | Station ID | ✓ | ✓ |
| 3–6 | District, Freeway, Direction, Lane type | ✓ | ✓ |
| 7 | Station length (miles) | ✓ | ✓ |
| 8 | Samples: number of raw detector samples received in the interval | ✓ (per 5 min) | ✓ (per hour) |
| 9 | % Observed: share of the lanes with real (not imputed) data | ✓ | ✓ |
| 10 | Total flow: vehicles in the interval, summed over the lanes | veh / 5 min | veh / hour |
| 11 | Avg occupancy (0–1) | ✓ | ✓ |
| 12 | Avg speed (mph) | ✓ | ✓ |
| 13+ | Per-lane columns | 8 lanes × 5: samples, flow, occupancy, speed, observed (1/0) → 52 columns | 10 lanes × 3: flow, occupancy, speed → 42 columns |

Per-lane flow is an integer in the 5-minute files, but has decimals in ~2–5% of the hourly values.

So the hourly file is the hourly aggregation of the 5-minute data: same stations and measures, with flow summed
over the hour and occupancy/speed averaged. It also leaves out the per-lane sample counts and observed flags.
The 12 station-level columns go to `station_5min` / `station_hour`; the per-lane columns go to `station_5min_lane` /
`station_hour_lane`, one row per lane.

Data in `./data`: 5-minute data covers district 3 only (2026-07-01 to 2026-11-10, daily files). Hourly data covers
districts 3, 4, 5, 6, 7, 8, 10, 11 and 12 (2020-01 to 2026-10, monthly files).

### CHP incidents (`all_text_chp_incidents_*`)
- Incidents (20 columns): `Incident ID, CC Code, Incident Number, Timestamp, Description, Location, Area, Zoom Map,
  TB xy, Latitude, Longitude, District, County FIPS, City FIPS, Freeway, Direction, State Postmile, Absolute Postmile,
  Severity, Duration (min)`. A few locations contain line breaks, which split the row over several lines; the
  loader glues them back (the line breaks are kept in the value).
- Details (4 columns): `Incident ID, Detail ID, Timestamp, Detail`. A detail text containing newlines is split
  over several lines in the file; the loader glues those lines back together.

**Known gaps:** `all_text_chp_incidents_month_2025_03` has only 12 incidents (normally ~40,000), and
`2025_04`, `2025_05` and `2025_06` are empty (0-byte) files. Download them again with
`python downloadPems.py --type chp_incidents_month --district all --years 2025 --months 3-6`. The downloader now
re-downloads empty or unreadable files on its own. To retry the 2025-03 file, delete it first: it is a valid zip,
so the downloader keeps it.

## 2. Schema `pems_original` (raw tables)

All values are stored as in the files; empty values are NULL. Parsing is strict: a value that does not parse makes
its file fail, so nothing is silently dropped or turned into NULL. Timestamps are local California time (no time
zone), as in the files.

```mermaid
erDiagram
    station_5min ||--o{ station_5min_lane : "station_id, ts (FK)"
    station_hour ||--o{ station_hour_lane : "station_id, ts (FK)"
    station_meta ||..o{ station_5min : "station_id (logical, versioned)"
    station_meta ||..o{ station_hour : "station_id (logical, versioned)"
    chp_incident |o..o{ chp_incident_detail : "incident_id (logical)"
    station_meta }o..o{ chp_incident : "position (fwy, dir, abs_pm)"

    station_meta {
        int station_id PK
        date version_date PK
        smallint district
        text fwy
        text dir
        int county
        text city
        text state_pm
        float abs_pm
        float latitude
        float longitude
        float length
        text type
        smallint lanes
        text name
        text user_id_1_to_4
    }
    station_5min {
        int station_id PK
        timestamp ts PK
        smallint district
        text fwy
        text dir
        text lane_type
        float station_length
        int samples
        smallint pct_observed
        int total_flow
        float avg_occupancy
        float avg_speed
    }
    station_5min_lane {
        int station_id PK,FK
        timestamp ts PK,FK
        smallint lane PK
        int samples
        int flow
        float occupancy
        float speed
        boolean observed
    }
    station_hour {
        int station_id PK
        timestamp ts PK
        text same_columns_as_station_5min
    }
    station_hour_lane {
        int station_id PK,FK
        timestamp ts PK,FK
        smallint lane PK
        float flow
        float occupancy
        float speed
    }
    chp_incident {
        bigint incident_id PK
        timestamp ts
        text cc_code
        text incident_number
        text description
        text location
        text area
        text zoom_map
        text tb_xy
        float latitude
        float longitude
        smallint district
        int county_fips
        int city_fips
        text fwy
        text dir
        text state_pm
        float abs_pm
        text severity
        int duration_min
    }
    chp_incident_detail {
        bigint incident_id PK
        bigint detail_id PK
        timestamp ts
        text description
    }
```

| Table | One row per | Primary key | Foreign key | Secondary indexes |
|---|---|---|---|---|
| `station_meta` | station × metadata version | `(station_id, version_date)` | — | `(district, version_date)` |
| `station_5min` | station × 5 minutes | `(station_id, ts)` | — | BRIN `(ts)` |
| `station_5min_lane` | station × 5 minutes × lane | `(station_id, ts, lane)` | `(station_id, ts)` → `station_5min` | BRIN `(ts)` |
| `station_hour` | station × hour | `(station_id, ts)` | — | BRIN `(ts)` |
| `station_hour_lane` | station × hour × lane | `(station_id, ts, lane)` | `(station_id, ts)` → `station_hour` | BRIN `(ts)` |
| `chp_incident` | incident | `(incident_id)` | — | `(fwy, dir, abs_pm)`, `(ts)` |
| `chp_incident_detail` | incident update/log line | `(incident_id, detail_id)` | — | — (the PK covers joins on `incident_id`) |
| `loaded_file` | loaded file | `(file_name)` | — | — |

- `station_meta.version_date` and `district` come from the file name; the `District` column of the files is the same.
- `*_lane.lane` is the lane position in the file (1 = first lane group). A lane is stored only if it has a value
  (in the 5-minute files `observed` is 0 also for lanes that do not exist, so it does not count).
- The rows of the station tables are stored roughly in time order (files are loaded in date order), which is what
  makes the small BRIN indexes on `ts` effective for time-range queries.
- `chp_incident`: the day and month files overlap; the duplicated incidents (and details) are kept once.
- `loaded_file`: bookkeeping (rows, seconds, target table); the files listed there are skipped by later runs.

### Relationships that are not enforced
- `station_5min` / `station_hour` → `station_meta` (`station_id`): the metadata is versioned, so `station_id` alone
  is not a key of `station_meta`. Join on the version valid at `ts`: the latest `version_date <= ts` of the district.
- `chp_incident_detail` → `chp_incident` (`incident_id`): about a third of the incidents with details (1.5M of 4.6M,
  2020–2026) have no row in the incident files. Use a `LEFT JOIN` or `EXISTS`.
- incidents ↔ stations: the only shared attribute is the position (`fwy`, `dir`, `abs_pm`).

### Size
Roughly 420 MB per district-month of hourly data (station + lane rows, with indexes) and 360 MB per day of 5-minute
data for district 3: about 300–350 GB for the whole `./data`, about 35 GB from August 2026 on. `pems_cleaned` needs
about as much again (its `measurement` table holds the same rows).

## 3. Schema `pems_cleaned` (logical model)

```mermaid
erDiagram
    district ||--o{ road_in_district : ""
    road ||--o{ road_in_district : ""
    road ||--o{ segment : ""
    segment ||--o{ station_in_segment : "fwy, dir, mile"
    segment |o--o{ incident : "fwy, dir, mile"
    district ||--o{ station : ""
    station ||--o{ station_in_segment : "station_id"
    station ||--o{ measurement : "station_id"
    incident ||--o{ incident_detail : "incident_id"

    district { smallint district_id PK }
    road { text fwy PK
           text dir PK }
    road_in_district { smallint district_id PK,FK
                       text fwy PK,FK
                       text dir PK,FK }
    segment { text fwy PK,FK
              text dir PK,FK
              int mile PK
              float from_pm
              float to_pm }
    station { int station_id PK
              smallint district_id FK }
    station_in_segment { int station_id PK,FK
                         timestamp valid_from PK
                         timestamp valid_to
                         text fwy FK
                         text dir FK
                         int mile FK
                         float abs_pm
                         float latitude
                         float longitude
                         text type
                         smallint lanes
                         text name
                         text other_metadata }
    incident { bigint incident_id PK
               text fwy FK
               text dir FK
               int mile FK
               timestamp time_from
               timestamp time_to
               float abs_pm
               smallint district_id FK
               text description
               text other_attributes }
    incident_detail { bigint incident_id PK,FK
                      bigint detail_id PK
                      timestamp ts
                      text description }
    measurement { int station_id PK,FK
                  smallint resolution_min PK
                  timestamp ts PK
                  smallint lane PK
                  float flow
                  float occupancy
                  float speed
                  int samples
                  smallint pct_observed
                  boolean observed }
```

| Table | One row per | Primary key | Foreign keys | Built from |
|---|---|---|---|---|
| `district` | district | `district_id` | — | stations and incidents |
| `road` | freeway in one direction | `(fwy, dir)` | — | station intervals and incidents |
| `road_in_district` | road crossing a district | `(district_id, fwy, dir)` | → `district`, → `road` | station intervals and incidents |
| `segment` | piece of road of `segment_miles` | `(fwy, dir, mile)` | → `road` | segments with a station or an incident |
| `station` | station | `station_id` | → `district` | metadata and measurements |
| `station_in_segment` | interval during which a station kept the same metadata | `(station_id, valid_from)` | → `station`, → `road`, → `segment` | `station_meta` |
| `incident` | CHP incident | `incident_id` | → `district`, → `road`, → `segment` | `chp_incident` |
| `incident_detail` | incident update/log line | `(incident_id, detail_id)` | → `incident` | `chp_incident_detail` |
| `measurement` | station × resolution × time × lane | `(station_id, resolution_min, ts, lane)` | → `station` | `station_5min[_lane]`, `station_hour[_lane]` |
| `etl_info` | ETL parameter | `key` | — | source schema, `segment_miles`, build time |

Rules:
- **Road = freeway + direction**: postmiles, stations and incidents are per direction (I-80 E and W are two roads).
- **Segment**: `mile = floor(abs_pm / segment_miles)`; `abs_pm` is the absolute postmile along the whole route (it is
  not reset per county like `state_pm`), so a segment is unique on its road across districts. `from_pm`/`to_pm` are
  its bounds. Only the segments with at least one station or incident exist.
- **Station intervals** (`station_in_segment`): consecutive metadata versions in which a station has the same
  attributes (all the metadata columns) are merged into one interval `[valid_from, valid_to)`; any change (position,
  type, lanes, name, ...) or the station missing from a version starts a new interval. The bounds are the publication
  dates of the versions, so they are approximate: a few measurements fall just outside the station's intervals (e.g.
  the last hour reported on the day a removed station disappears from the metadata). The first loaded version of
  each district starts at `-infinity`, and an interval still open in the last version ends at `infinity`.
  `type` is the station type (`ML` mainline, `HV` HOV, `OR`/`FR` on/off-ramp, `FF` connector, ...): a ramp's flow
  is not the road's flow.
- **Segment of a measurement**: not stored; it is the interval of its station valid at `ts`:
  ```sql
  SELECT m.*, s.fwy, s.dir, s.mile
  FROM pems_cleaned.measurement m
  JOIN pems_cleaned.station_in_segment s ON s.station_id = m.station_id AND m.ts >= s.valid_from AND m.ts < s.valid_to
  ```
- **Measurement**: one column per measure. `resolution_min` is 5 or 60; `lane` 0 is the station total (the
  station-level columns of the file: total flow, average occupancy and speed, samples, % observed) and `lane` 1, 2, ...
  are the lanes (flow, occupancy, speed; samples and `observed` only at 5 minutes). Columns not available at a
  level are NULL. The station-level `district`, `fwy`, `dir`, `lane_type` and `station_length` of the files are not
  repeated here: they are in `station` / `station_in_segment`.
- **Incident**: `time_to = time_from + duration_min` (NULL when the duration is missing, ~1.7%). `fwy`, `dir` and
  `mile` are NULL when the position is incomplete (~0.6%): those incidents have no road or segment.
- **Incident detail**: only the details of the incidents in `incident` (about a third of the raw details belong to
  incidents missing from the incident files).

`etlPems.py` prints these checks at the end of each run: rows per table, overlapping station intervals (must be 0),
incidents without a segment, and station-level measurements without a station interval at their time.

## 4. How the raw load works
Files are loaded in parallel (`--workers`, default 4), each in a single transaction recorded in `loaded_file`, with
`COPY` into tables that have no primary keys, foreign keys or indexes. These are built once, after the load (with
parallel workers): a run first drops the keys and indexes of the tables it loads into, then rebuilds them. Station
files are loaded in date order (all districts of a day or month, then the next). When a primary key cannot be built
because of duplicate rows (the CHP day and month files overlap), the duplicates are deleted (one row per key is kept)
and the key is built again. An interrupted load can be run again: files already loaded are skipped, and missing keys
are rebuilt. A file that fails (e.g. a value that does not parse) is reported and not marked as loaded.
