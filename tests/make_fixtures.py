#!/usr/bin/env python3
"""Builds the small test files of tests/data (used by the GitHub CI) from the full files of ./data.

The files keep the names and formats of the PeMS files, so that loadPems.py treats them like the real ones:
    - two consecutive station-metadata versions of district 3 (whole files), between which stations are removed,
      added and changed;
    - the 5-minute data of the days before and of the day of the second version, and the hourly data of the month,
      only for a few stations: removed, added, changed and some unchanged ones;
    - the CHP incidents of the month, only those of district 3 (with their details).

    python3 tests/make_fixtures.py
"""
import csv
import gzip
import io
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC, DST = ROOT / "data", ROOT / "tests" / "data"
META = ["d03_text_meta_2026_07_31.txt", "d03_text_meta_2026_08_12.txt"]
STATION_FILES = ["d03_text_station_5min_2026_08_11.txt.gz", "d03_text_station_5min_2026_08_12.txt.gz",
                 "d03_text_station_hour_2026_08.txt.gz"]
INCIDENTS = "all_text_chp_incidents_month_2026_08.txt.zip"
UNCHANGED = 20  # unchanged stations kept besides the removed/added/changed ones


def meta(name):
    with open(SRC / name, encoding="utf-8", newline="") as fh:
        return {r["ID"]: r for r in csv.DictReader(fh, delimiter="\t")}


def main():
    DST.mkdir(parents=True, exist_ok=True)
    for f in DST.iterdir():
        f.unlink()
    a, b = meta(META[0]), meta(META[1])
    moved = {k for k in set(a) | set(b) if a.get(k) != b.get(k)}
    same = sorted(k for k in set(a) & set(b) if k not in moved)[:UNCHANGED]
    stations = moved | set(same)
    for name in META:
        (DST / name).write_bytes((SRC / name).read_bytes())
    for name in STATION_FILES:
        with gzip.open(SRC / name, "rt", encoding="utf-8", newline="") as fin, \
                gzip.open(DST / name, "wt", encoding="utf-8", newline="") as fout:
            for line in fin:
                if line.split(",", 2)[1] in stations:
                    fout.write(line)
    with zipfile.ZipFile(SRC / INCIDENTS) as zin, \
            zipfile.ZipFile(DST / INCIDENTS, "w", zipfile.ZIP_DEFLATED) as zout:
        names = zin.namelist()
        inc = next(n for n in names if "_det_" not in n)
        text = gzip.decompress(zin.read(inc)).decode("utf-8")
        keep, ids = [], set()
        for line in text.splitlines(keepends=True):  # district = 12th field
            r = next(csv.reader(io.StringIO(line)))
            if len(r) == 20 and r[11] == "3":
                keep.append(line)
                ids.add(r[0])
        zout.writestr(inc, gzip.compress("".join(keep).encode("utf-8")))
        for det in (n for n in names if "_det_" in n):
            lines, last = [], None
            for line in gzip.decompress(zin.read(det)).decode("utf-8").splitlines(keepends=True):
                first = line.split(",", 1)[0]
                if first.isdigit():  # a continuation line of a multi-line description belongs to the previous row
                    last = first
                if last in ids:
                    lines.append(line)
            zout.writestr(det, gzip.compress("".join(lines).encode("utf-8")))
    print(f"{len(stations)} stations; incidents of district 3: {len(ids)}")
    for f in sorted(DST.iterdir()):
        print(f"  {f.name}: {f.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
