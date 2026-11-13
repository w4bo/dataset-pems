#!/usr/bin/env python3
"""Downloads PeMS Clearinghouse files (https://pems.dot.ca.gov) into ./data.

Needs a (free) PeMS account, read from the environment:
    export PEMS_USERNAME=... PEMS_PASSWORD=...

Examples:
    # every station-metadata version of District 3 and 7, 2021-2026
    python3 downloadPems.py --type meta --district 3 7 --years 2021-2026
    # CHP incidents (one file for all districts), one month per file
    python3 downloadPems.py --type chp_incidents_month --district all --years 2024-2026
    # 5-minute station data of District 3, July-September 2026 only
    python3 downloadPems.py --type station_5min --district 3 --years 2026 --months 7-9
    # download again the files on disk that are broken (truncated, empty), here the 5-minute data of 2026
    python3 downloadPems.py --type station_5min --district 3 --years 2026 --verify
    # only list what would be downloaded
    python3 downloadPems.py --type meta --district 3 --years 2026 --dry-run

The type is the Clearinghouse value of the "Type" menu (e.g. meta, station_5min, station_hour, station_day,
chp_incidents_day, chp_incidents_month). Files are downloaded one at a time with a pause in between, as the
Clearinghouse asks, and files already on disk are skipped, so an interrupted run can be resumed.
Only the Python standard library is used.
"""
import argparse
import calendar
import gzip
import http.cookiejar
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

BASE = "https://pems.dot.ca.gov"
OUT = Path(__file__).resolve().parent / "data"
MONTHS = list(calendar.month_name)[1:]  # January..December, the keys of the listing


def parse_range(s, lo, hi):
    """'2021-2026' or '3' or '1,4,7' -> sorted list of ints."""
    out = set()
    for part in s.split(","):
        a, _, b = part.partition("-")
        out.update(range(int(a), int(b or a) + 1))
    bad = [x for x in out if not lo <= x <= hi]
    if bad:
        sys.exit(f"Out of range [{lo}, {hi}]: {bad}")
    return sorted(out)


def opener():
    jar = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    op.addheaders = [("User-Agent", "Mozilla/5.0 (stgraph research downloader)")]
    return op


def login(op, user, pwd):
    data = urllib.parse.urlencode({"redirect": "", "username": user, "password": pwd, "login": "Login"}).encode()
    body = op.open(f"{BASE}/?dnode=Clearinghouse", data, timeout=60).read().decode("utf-8", "replace")
    if 'name="password"' in body and "Logout" not in body:
        sys.exit("Login failed: check PEMS_USERNAME/PEMS_PASSWORD (and that the account has been approved).")


def listing(op, ftype, district, year, debug=False):
    """Returns [(month name or None, entry)] of the Clearinghouse listing; tolerates the shapes it answers with
    ({"data": {month: [entries]}}, {"data": []} or a bare [] when there is nothing, a bare list of entries)."""
    q = urllib.parse.urlencode({"srq": "clearinghouse", "district_id": district, "geotag": "null",
                                "yy": year, "type": ftype, "returnformat": "text"})
    raw = op.open(f"{BASE}/?{q}", timeout=60).read().decode("utf-8", "replace")
    if debug:
        print(f"  [debug] type={ftype} district={district} year={year}: {raw[:1000]!r}")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  ! unexpected answer for type={ftype} district={district} year={year}: {raw[:200]!r}")
        return []
    if isinstance(data, dict):
        data = data.get("data", data)
    if isinstance(data, dict):
        return [(m, e) for m, entries in data.items() if isinstance(entries, list) for e in entries]
    if isinstance(data, list):
        return [(None, e) for e in data]
    return []


def file_name(entry):
    if entry.get("file_name"):
        return entry["file_name"]
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(entry["url"]).query)
    return qs.get("download", [entry["url"].rsplit("/", 1)[-1]])[0]


def complete(path, verify=False):
    """False for a missing, empty or unreadable file (e.g. a download cut short), which is then downloaded again.
    A .gz is only checked for its header unless [verify]: a truncated .gz is found only by decompressing it whole."""
    try:
        if path.stat().st_size == 0:
            return False
        if path.suffix == ".zip":
            with zipfile.ZipFile(path) as z:
                return z.testzip() is None
        if path.suffix == ".gz":
            with gzip.open(path, "rb") as f:
                if not verify:
                    return f.read(1) is not None
                while f.read(1 << 24):
                    pass
            return True
        return True
    except (OSError, EOFError, zipfile.BadZipFile):
        return False


def download(op, url, dest, retries=5, max_seconds=180):
    """Downloads [url] into [dest] through a .part file. A transfer that stalls (no data for 60 s) or takes more than
    [max_seconds] is aborted and retried: the Clearinghouse sometimes trickles a file for tens of minutes, while a new
    request usually gets it in seconds."""
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        start = time.time()
        try:
            with op.open(BASE + url, timeout=60) as r, open(tmp, "wb") as f:
                ctype = r.headers.get("Content-Type", "")
                while chunk := r.read(1 << 20):
                    f.write(chunk)
                    if time.time() - start > max_seconds:
                        raise TimeoutError(f"slower than {max_seconds} s, retrying")
            if "text/html" in ctype:  # the session expired: PeMS answers with the login page
                raise IOError("got an HTML page instead of the file (session expired?)")
            tmp.replace(dest)
            return True
        except Exception as e:  # noqa: BLE001
            print(f"  ! attempt {attempt}/{retries} failed for {dest.name}: {e}", flush=True)
            tmp.unlink(missing_ok=True)
            time.sleep(5 * attempt)
    return False


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--type", required=True, help="Clearinghouse type, e.g. meta, station_5min, chp_incidents_month")
    p.add_argument("--district", nargs="+", default=["3"], help="district ids (1-12) or 'all'")
    p.add_argument("--years", required=True, help="e.g. 2026, 2021-2026, 2019,2021")
    p.add_argument("--months", default="1-12", help="e.g. 7-9 (default: all)")
    p.add_argument("--out", type=Path, default=OUT, help=f"output folder (default: {OUT})")
    p.add_argument("--sleep", type=float, default=2.0, help="seconds between downloads (default: 2)")
    p.add_argument("--max-seconds", type=int, default=180, help="abort and retry a file slower than this (default: 180)")
    p.add_argument("--workers", type=int, default=1,
                   help="parallel downloads (default: 1; the Clearinghouse asks for one at a time: keep it low, e.g. 2-3)")
    p.add_argument("--dry-run", action="store_true", help="only list the files")
    p.add_argument("--verify", action="store_true",
                   help="fully test the files already on disk (slow: decompresses them) and download again the "
                        "broken ones, e.g. a .gz cut short")
    p.add_argument("--debug", action="store_true", help="print the raw Clearinghouse listings")
    a = p.parse_args()

    user, pwd = os.environ.get("PEMS_USERNAME"), os.environ.get("PEMS_PASSWORD")
    if not user or not pwd:
        sys.exit("Set PEMS_USERNAME and PEMS_PASSWORD.")
    years = parse_range(a.years, 2000, 2100)
    months = {MONTHS[m - 1] for m in parse_range(a.months, 1, 12)}
    a.out.mkdir(parents=True, exist_ok=True)

    op = opener()
    login(op, user, pwd)
    todo, n_skip = [], 0
    for district in a.district:
        for year in years:
            files = [e for month, e in listing(op, a.type, district, year, a.debug)
                     if (month is None or month in months) and isinstance(e, dict) and e.get("url")]
            print(f"type={a.type} district={district} year={year}: {len(files)} files", flush=True)
            for e in files:
                dest = a.out / file_name(e)
                if complete(dest, a.verify):
                    n_skip += 1
                elif a.dry_run:
                    print(f"  {dest.name}")
                else:
                    todo.append((e["url"], dest))
    print(f"{len(todo)} files to download ({n_skip} already present), {a.workers} worker(s)", flush=True)

    lock = threading.Lock()
    counts = {"ok": 0, "fail": 0}
    t0 = time.time()

    def job(item):
        url, dest = item
        ok = download(op, url, dest, max_seconds=a.max_seconds)
        if not ok:
            with lock:
                login(op, user, pwd)  # the session may have expired
        with lock:
            counts["ok" if ok else "fail"] += 1
            k = counts["ok"] + counts["fail"]
            eta = (time.time() - t0) / k * (len(todo) - k) / 3600
            print(f"  [{k}/{len(todo)}] {dest.name} {'ok' if ok else 'FAILED'} (ETA {eta:.1f} h)", flush=True)
        time.sleep(a.sleep)

    with ThreadPoolExecutor(max_workers=max(1, a.workers)) as pool:
        list(pool.map(job, todo))
    print(f"Done: {counts['ok']} downloaded, {n_skip} already present, {counts['fail']} failed -> {a.out}")
    sys.exit(1 if counts["fail"] else 0)


if __name__ == "__main__":
    main()
