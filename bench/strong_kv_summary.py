#!/usr/bin/env python3
"""Summarize a matched SQL / strong KV / raw native read study.

Reads the study.json that bench/strong_kv.py writes (tests/kv.sh with
KV_STUDY_ENGINE leaves it in /tmp/tarn-kv181-*/study/results/) and reports,
for each batch/clients cell and read path, over the study's trials:

- key throughput: the median and the range;
- the median of the trials' operation p99 (a batch-32 operation is 32 keys);
- requests and bytes to storage per key: the gateway's for SQL, the client's
  own for strong KV and raw reads, which talk to storage directly;
- client protocol bytes per key: MySQL protocol for SQL, IPROTO otherwise;
- CPU per key: the storage and gateway processes together, the hot shard
  leader's TX thread (already part of the former), and the client process,
  whose stacks differ (PyMySQL for SQL, Rust for the others).

Then, per cell, strong KV against SQL in the same trial: the median and range
of the ratios. Both read each key fresh with Tarn's ownership, claim and
schema checks, so this is the comparison of equal contracts. Raw native reads
lack those checks: they are a descriptive reference, never an acceleration.

  python3 bench/strong_kv_summary.py STUDY.json [--json OUT] [--markdown FILE]
                                     [--notice TITLE]
  python3 bench/strong_kv_summary.py SUMMARY.json     # render a saved summary

--notice prints one GitHub notice per cell holding that cell's summary JSON,
so CI reports are readable through the check-run annotations API.
"""
import argparse
import json
import math
from pathlib import Path
import statistics
import sys

PATHS = (("sql", "SQL"), ("strong", "strong KV"), ("raw", "raw native"))
CELLS = ((1, 1), (1, 16), (32, 1), (32, 16))
METRICS = ("keys_per_second", "p50_ms", "p99_ms", "client_requests_per_key", "client_bytes_per_key",
           "storage_requests_per_key", "storage_bytes_per_key", "server_cpu_us_per_key",
           "hot_tx_cpu_us_per_key", "client_cpu_us_per_key")
RATIOS = ("keys_per_second", "p99_ms", "server_cpu_us_per_key", "hot_tx_cpu_us_per_key")
NOTICE_LIMIT = 4096


def rounded(value, digits=3):
    if value is None or value == 0:
        return value
    return round(value, digits - 1 - math.floor(math.log10(abs(value))))


def describe(values):
    if not values or any(v is None for v in values):
        return None
    return dict(median=rounded(statistics.median(values)), minimum=rounded(min(values)),
                maximum=rounded(max(values)))


def phase_metrics(phase):
    """Per-key costs of one accepted phase of bench/strong_kv.py."""
    if phase["mode"] == "sql":
        result, client = phase, phase["protocol"]
        client_requests = client["statements"]
        # Older studies did not read the gateway's storage counters.
        storage = phase.get("gateway_storage")
    else:
        result, client = phase["client"], phase["client"]["transport"]
        client_requests = client["requests"]
        storage = client
    keys = result["successful_keys"]
    if not keys or not result["elapsed_seconds"]:
        raise ValueError("phase %s B%d C%d trial %d read no keys" % (phase["mode"], phase["batch"],
                                                                    phase["workers"], phase["trial"]))
    cpu = phase["cpu"]["cpu_counters_ns"]
    return dict(
        keys_per_second=keys / result["elapsed_seconds"],
        p50_ms=result["p50_ns"] / 1e6,
        p99_ms=result["p99_ns"] / 1e6,
        client_requests_per_key=client_requests / keys,
        client_bytes_per_key=(client["bytes_written"] + client["bytes_read"]) / keys,
        storage_requests_per_key=None if storage is None else storage["requests"] / keys,
        storage_bytes_per_key=None if storage is None else (storage["bytes_written"] + storage["bytes_read"]) / keys,
        # Disjoint processes; the hot leader's TX thread is inside storage.
        server_cpu_us_per_key=(cpu["storage_all_processes"] + cpu["gateway_all_processes"]) / keys / 1000,
        hot_tx_cpu_us_per_key=cpu["hot_owner_leader_tx"] / keys / 1000,
        client_cpu_us_per_key=cpu["client_process"] / keys / 1000,
    )


def summarize(study, fixture=None):
    if not study.get("accepted"):
        raise ValueError("the study was not accepted; nothing is published from it")
    trials = {}
    for phase in study["phases"]:
        if not phase.get("accepted"):
            raise ValueError("an accepted study holds a rejected phase")
        key = (phase["batch"], phase["workers"], phase["mode"])
        trials.setdefault(key, {})[phase["trial"]] = phase_metrics(phase)
    cells = []
    for batch, workers in CELLS:
        paths = []
        for mode, label in PATHS:
            got = trials.get((batch, workers, mode))
            if not got:
                raise ValueError("no %s phases for batch %d, %d clients" % (mode, batch, workers))
            paths.append(dict(mode=mode, label=label, trials=len(got),
                              **{m: describe([t[m] for t in got.values()]) for m in METRICS}))
        sql, strong = trials[(batch, workers, "sql")], trials[(batch, workers, "strong")]
        paired = sorted(set(sql) & set(strong))
        cells.append(dict(batch=batch, workers=workers, paths=paths, paired_trials=len(paired),
                          strong_over_sql={m: describe([strong[t][m] / sql[t][m] for t in paired]) for m in RATIOS}))
    source = study.get("source") or {}
    environment = (fixture or {}).get("environment") or {}
    return dict(version=1, engine=source.get("engine"), source_head=source.get("cohort_head"),
                seconds=study.get("seconds"), warmup=study.get("warmup"),
                cpu_count=environment.get("cpu_count"), platform=environment.get("platform"),
                cells=cells)


def number(value, digits=None):
    """Fixed decimals when given, else about three significant digits."""
    if value is None:
        return "—"
    if digits is not None:
        return "{:.{}f}".format(value, digits)
    if abs(value) >= 100:
        return "{:,}".format(round(value))
    return "{:.3g}".format(value)


def median(stat, digits=None):
    return number(None if stat is None else stat["median"], digits)


def spread(stat, digits=None):
    if stat is None:
        return "—"
    return "%s (%s–%s)" % (number(stat["median"], digits), number(stat["minimum"], digits),
                           number(stat["maximum"], digits))


HEADER = ("| Batch, clients | Path | Keys/s, median (range) | Op p99 ms | Storage requests/key"
          " | Storage bytes/key | Client bytes/key | Server CPU µs/key | Hot TX µs/key | Client CPU µs/key |\n"
          "|---|---|--:|--:|--:|--:|--:|--:|--:|--:|")
RATIO_HEADER = ("| Batch, clients | Keys/s | Op p99 | Server CPU/key | Hot TX CPU/key |\n"
                "|---|--:|--:|--:|--:|")


def rows(cell):
    for p in cell["paths"]:
        yield "| %d, %d | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            cell["batch"], cell["workers"], p["label"], spread(p["keys_per_second"]), median(p["p99_ms"]),
            median(p["storage_requests_per_key"]), median(p["storage_bytes_per_key"]),
            median(p["client_bytes_per_key"]), median(p["server_cpu_us_per_key"]),
            median(p["hot_tx_cpu_us_per_key"]), median(p["client_cpu_us_per_key"]))


def ratio_row(cell):
    r = cell["strong_over_sql"]
    return "| %d, %d | %s | %s | %s | %s |" % (cell["batch"], cell["workers"], spread(r["keys_per_second"], 2),
                                              spread(r["p99_ms"], 2), spread(r["server_cpu_us_per_key"], 2),
                                              spread(r["hot_tx_cpu_us_per_key"], 2))


def markdown(summary):
    where = ", ".join(x for x in (summary["engine"], summary["cpu_count"] and "%s CPUs" % summary["cpu_count"],
                                  summary["source_head"] and "source " + summary["source_head"][:12]) if x)
    lines = ["#### Matched reads (%s)" % where, "", HEADER]
    for cell in summary["cells"]:
        lines.extend(rows(cell))
    lines += ["", "Strong KV ÷ SQL, paired by trial: median (range).", "", RATIO_HEADER]
    lines.extend(ratio_row(cell) for cell in summary["cells"])
    return "\n".join(lines) + "\n"


def escape(text, title=False):
    text = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    return text.replace(":", "%3A").replace(",", "%2C") if title else text


def notices(summary, title):
    """One notice per cell: the cell's summary JSON, whole."""
    head = {k: v for k, v in summary.items() if k != "cells"}
    for cell in summary["cells"]:
        body = escape(json.dumps(dict(head, cells=[cell]), separators=(",", ":"), sort_keys=True, allow_nan=False))
        # The runner truncates an annotation's message at 4096 characters.
        if len(body) > NOTICE_LIMIT:
            raise ValueError("a cell's summary needs %d characters, more than a notice holds" % len(body))
        yield "::notice title=%s::%s" % (escape("%s B%d C%d" % (title, cell["batch"], cell["workers"]), True), body)


def load(path):
    value = json.loads(Path(path).read_text())
    if "cells" in value:
        return value
    fixture = Path(path).resolve().parent.parent / "fixture.json"
    return summarize(value, json.loads(fixture.read_text()) if fixture.is_file() else None)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("input", type=Path, help="a study.json, or a summary written by --json")
    p.add_argument("--json", type=Path, help="write the summary JSON here")
    p.add_argument("--markdown", type=Path, help="append the Markdown tables here, e.g. $GITHUB_STEP_SUMMARY")
    p.add_argument("--notice", metavar="TITLE", help="also print one GitHub notice per cell")
    args = p.parse_args()
    summary = load(args.input)
    if args.json:
        args.json.write_text(json.dumps(summary, indent=1, sort_keys=True, allow_nan=False) + "\n")
    text = markdown(summary)
    if args.markdown:
        with args.markdown.open("a") as f:
            f.write(text)
    sys.stdout.write(text)
    if args.notice:
        for line in notices(summary, args.notice):
            print(line)


if __name__ == "__main__":
    main()
