#!/usr/bin/env python3
"""Randomized differential testing: generate random SELECTs (predicates,
expressions, aggregates, grouping, ordering, joins, subqueries) over a fixed
schema, run them on Tarn and on a reference MySQL-compatible server, and
compare results with difftest.py's comparison rules.

    tests/sql/fuzzdiff.py --tarn 127.0.0.1:4000 --ref 127.0.0.1:3407 [-n 2000] [--seed 1]

String data is lowercase ASCII so the reference's case-insensitive default
collation and Tarn's binary comparison agree (see docs/design.md, limitations).
"""
import argparse
import random
import re
import sys

import difftest as D

COLS = {
    "t1": [("id", "int"), ("a", "int"), ("b", "str"), ("c", "dec"), ("d", "dbl")],
    "t2": [("id", "int"), ("t1_id", "int"), ("name", "str"), ("qty", "int")],
    "t3": [("k1", "int"), ("k2", "str"), ("v", "int")],
}


class Gen:
    def __init__(self, rnd):
        self.r = rnd

    def const(self, ty):
        r = self.r
        if ty == "int":
            return str(r.choice([0, 1, 2, 3, 4, 5, 7, 10, 13, 20, 25, 33, 40, -1, 100]))
        if ty == "str":
            return "'" + r.choice(["name1", "name2", "name5", "name8", "item3", "k7", "", "x", "name"]) + "'"
        if ty == "dec":
            return r.choice(["0.5", "10.00", "37.10", "99.9", "-1.25"])
        return r.choice(["1.5", "7.5", "30.0", "0.25"])

    def col(self, t, alias, ty=None):
        cands = [c for c in COLS[t] if ty is None or c[1] == ty or (ty == "num" and c[1] in ("int", "dec", "dbl"))]
        if not cands:
            return None
        c = self.r.choice(cands)
        return (f"{alias}.{c[0]}" if alias else c[0]), c[1]

    def num_expr(self, t, alias, depth=0):
        r = self.r
        k = r.random()
        if depth > 1 or k < 0.5:
            c = self.col(t, alias, "num")
            return c[0] if c and r.random() < 0.8 else self.const("int")
        op = r.choice(["+", "-", "*"])
        return f"({self.num_expr(t, alias, depth + 1)} {op} {self.num_expr(t, alias, depth + 1)})"

    def pred(self, t, alias, depth=0):
        r = self.r
        k = r.random()
        if depth < 2 and k < 0.25:
            op = r.choice(["AND", "OR"])
            return f"({self.pred(t, alias, depth + 1)} {op} {self.pred(t, alias, depth + 1)})"
        if depth < 2 and k < 0.32:
            return f"NOT ({self.pred(t, alias, depth + 1)})"
        c, ty = self.col(t, alias)
        k2 = r.random()
        if k2 < 0.15:
            return f"{c} IS {'NOT ' if r.random() < 0.5 else ''}NULL"
        if k2 < 0.3:
            vals = ", ".join(self.const(ty) for _ in range(r.randint(1, 4)))
            return f"{c} {'NOT ' if r.random() < 0.3 else ''}IN ({vals})"
        if k2 < 0.42:
            lo, hi = sorted([self.const(ty), self.const(ty)])
            return f"{c} {'NOT ' if r.random() < 0.2 else ''}BETWEEN {lo} AND {hi}"
        if k2 < 0.5 and ty == "str":
            pat = r.choice(["name%", "%1", "n_me2", "%m%", "item%", "%"])
            return f"{c} {'NOT ' if r.random() < 0.3 else ''}LIKE '{pat}'"
        if k2 < 0.6 and ty != "str":
            return f"{self.num_expr(t, alias)} {r.choice(['=', '<>', '<', '<=', '>', '>='])} {self.const('int')}"
        return f"{c} {r.choice(['=', '<>', '<', '<=', '>', '>=', '<=>'])} {self.const(ty)}"

    def select(self):
        r = self.r
        t = r.choice(list(COLS))
        kind = r.random()
        where = f" WHERE {self.pred(t, None)}" if r.random() < 0.85 else ""
        if kind < 0.45:
            cols = r.sample([c[0] for c in COLS[t]], r.randint(1, len(COLS[t])))
            exprs = list(cols)
            if r.random() < 0.3:
                exprs.append(self.num_expr(t, None))
            if r.random() < 0.15:
                c = self.col(t, None, "str")
                if c:
                    exprs.append(r.choice([f"CONCAT({c[0]}, '-')", f"LENGTH({c[0]})", f"UPPER({c[0]})", f"COALESCE({c[0]}, 'z')"]))
            q = f"SELECT {'DISTINCT ' if r.random() < 0.15 else ''}{', '.join(exprs)} FROM {t}{where}"
            if r.random() < 0.5:
                # order by every selected column so the order is total
                q += " ORDER BY " + ", ".join(f"{i + 1}{' DESC' if r.random() < 0.3 else ''}" for i in range(len(exprs)))
                if r.random() < 0.6:
                    q += f" LIMIT {r.randint(0, 12)}"
                    if r.random() < 0.3:
                        q += f" OFFSET {r.randint(0, 5)}"
            return q
        if kind < 0.75:
            g = self.col(t, None)
            agg_col = self.col(t, None, "num")
            aggs = [r.choice(["COUNT(*)", f"COUNT({agg_col[0]})", f"SUM({agg_col[0]})", f"MIN({agg_col[0]})",
                              f"MAX({agg_col[0]})", f"COUNT(DISTINCT {agg_col[0]})"]) for _ in range(r.randint(1, 3))]
            if r.random() < 0.6:
                q = f"SELECT {g[0]}, {', '.join(aggs)} FROM {t}{where} GROUP BY {g[0]}"
                if r.random() < 0.3:
                    q += f" HAVING COUNT(*) > {r.randint(0, 3)}"
            else:
                q = f"SELECT {', '.join(aggs)} FROM {t}{where}"
            return q
        if kind < 0.9:
            # join t2 -> t1 (or t1 -> t3 via a)
            if r.random() < 0.6:
                p = self.pred("t1", "x")
                return (f"SELECT x.id, y.id, y.qty FROM t2 y {'LEFT ' if r.random() < 0.3 else ''}JOIN t1 x "
                        f"ON x.id = y.t1_id WHERE {self.pred('t2', 'y')} AND ({p} OR x.id IS NULL)")
            return f"SELECT x.id, z.k2, z.v FROM t1 x JOIN t3 z ON z.k1 = x.a WHERE {self.pred('t1', 'x')}"
        # subqueries
        inner = f"SELECT t1_id FROM t2 WHERE {self.pred('t2', None)}"
        if r.random() < 0.5:
            return f"SELECT id, a FROM t1 WHERE id {'NOT ' if r.random() < 0.3 else ''}IN ({inner}){(' AND ' + self.pred('t1', None)) if r.random() < 0.5 else ''}"
        return f"SELECT id FROM t1 WHERE {'NOT ' if r.random() < 0.3 else ''}EXISTS ({inner}) AND {self.pred('t1', None)}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tarn", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("-n", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--db", default="fuzzdiff")
    ap.add_argument("--show", type=int, default=15)
    args = ap.parse_args()
    a, b = D.connect(args.tarn), D.connect(args.ref)
    D.setup(a, args.db)
    D.setup(b, args.db)
    g = Gen(random.Random(args.seed))
    fails = errs_both = 0
    shown = 0
    for i in range(args.n):
        sql = g.select()
        ra, rb = D.run(a, sql), D.run(b, sql)
        if ra[0] == "err" and rb[0] == "err":
            errs_both += 1
            continue
        ok = ra[0] == rb[0]
        if ok and ra[0] == "rows":
            ordered = bool(re.search(r"\bORDER\s+BY\b", sql, re.I))
            ok = D.rows_equal(ra[1], rb[1], ordered)
        if not ok:
            fails += 1
            if shown < args.show:
                shown += 1
                print(f"MISMATCH #{i}: {sql}\n  tarn: {str(ra)[:400]}\n  ref:  {str(rb)[:400]}")
    print(f"{args.n} random queries (seed {args.seed}): {fails} mismatches, {errs_both} rejected by both")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
