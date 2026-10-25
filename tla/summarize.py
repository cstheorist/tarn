#!/usr/bin/env python3
"""Summarize TLC outputs (out/*.out) as a Markdown table."""
import glob, os, re, sys

rows = []
for f in sorted(glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "out", "*.out"))):
    name = os.path.basename(f)[:-4]
    t = open(f, errors="replace").read()
    m = re.search(r"Invariant (\w+) is violated", t)
    if m:
        res = f"violates {m.group(1)}"
    elif "No error has been found" in t:
        res = "no errors"
    else:
        res = "incomplete"
    st = re.findall(r"([\d,]+) distinct states found", t)
    states = st[-1].replace(",", "") if st else "?"
    gen = re.findall(r"([\d,]+) states generated", t)
    generated = gen[-1].replace(",", "") if gen else "?"
    d = re.search(r"depth of the complete state graph search is (\d+)", t)
    depth = d.group(1) if d else "-"
    tm = re.search(r"Finished in (.+?) at", t)
    secs = tm.group(1).strip() if tm else "-"
    trace = len(re.findall(r"^State \d+:", t, re.M))
    rows.append((name, res, states, generated, depth, secs, trace))

print("| Configuration | Result | Distinct states | States generated | Depth | Time | Trace length |")
print("|---|---|---:|---:|---:|---|---:|")
for r in rows:
    print("| " + " | ".join(str(x) if x != 0 else "" for x in r) + " |")
