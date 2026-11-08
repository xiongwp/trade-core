#!/usr/bin/env python3
"""Explicit-state model check of the order-api MySQL projection.

Models one order's lifecycle as the engine emits it. Two independent
Kafka consumers feed MySQL: the command writer (order rows, modifies) and
the execution writer (pending -> drain -> processed/fills). Every
interleaving is explored exhaustively:
- of the two consumers;
- of batch boundaries;
- of crashes between the MySQL commit and the Kafka offset commit
  (which cause redelivery);
- of crashes that lose a drain-handoff mark;
- of the idle sweep.

The model mirrors src/bin/order_api/{command,execution}_projection.rs:
- the status CASE, the GREATEST/LEAST filled_qty rule and the notional sum;
- INSERT IGNORE into pending, then discard_redelivered_events, then
  apply_pending_execution_table;
- the handoff marks, and their reset to "all tables" on restart.

Assumptions (stated, not modelled):
- Each MySQL transaction is atomic and serializable. REPEATABLE READ with
  locking reads and gap locks on UNIQUE(order_id) gives this for these
  statements.
- Redeliveries happen within the dedupe retention.
- Kafka keeps per-partition order.

Run: python3 scripts/model/projection_model.py
"""
from collections import deque, namedtuple
import itertools
import sys

ACCEPTED, TRADE, FILLED, PARTIAL, RESTING, CANCELLED, REJECTED, MODIFIED = 1, 2, 3, 4, 5, 6, 7, 8
NAMES = {1: "ACCEPTED", 2: "TRADE", 3: "FILLED", 4: "PARTIAL", 5: "RESTING", 6: "CANCELLED", 7: "REJECTED", 8: "MODIFIED"}

# Event: (position, type, qty, price). position = (raft_index, ordinal).
Ev = namedtuple("Ev", "pos type qty price")
# Command: ("new", qty, price) or ("modify", new_qty, new_price)


def drain(row, pending):
    """apply_pending_execution_table for one order. Returns the new row,
    the promoted event keys, and the fills (trade events)."""
    if row is None or not pending:
        return row, set(), set()
    evs = sorted(pending, key=lambda e: e.pos)
    trade_qty = sum(e.qty for e in evs if e.type == TRADE)
    partial_qty = max([e.qty for e in evs if e.type == PARTIAL], default=0)
    has_filled = any(e.type == FILLED for e in evs)
    latest = evs[-1].type
    notional = sum(e.qty * e.price for e in evs if e.type == TRADE)
    qty, filled, status, fnot = row["qty"], row["filled"], row["status"], row["notional"]
    g = max(filled + trade_qty, partial_qty)
    if has_filled or g >= qty:
        status = "FILLED"
    elif latest == CANCELLED:
        status = "CANCELLED"
    elif latest == REJECTED:
        status = "REJECTED"
    elif g > 0:
        status = "PARTIAL"
    else:
        status = "OPEN"
    filled = min(qty, max(filled + trade_qty, partial_qty, qty if has_filled else 0))
    new = dict(row, filled=filled, status=status, notional=fnot + notional)
    keys = {e.pos for e in evs}
    fills = {e for e in evs if e.type == TRADE}
    return new, keys, fills


def freeze(d):
    return tuple(sorted(d.items())) if d is not None else None


class Model:
    def __init__(self, commands, events, max_crashes=2, max_batch=3):
        self.commands = commands
        self.events = events
        self.max_crashes = max_crashes
        self.max_batch = max_batch

    def initial(self):
        return dict(
            row=None,            # asset_orders row
            pending=frozenset(),  # pending_execution_events
            processed=frozenset(),
            fills=frozenset(),
            cmd_off=0,           # committed Kafka offsets
            exe_off=0,
            cmd_pos=0,           # consumer position (>= committed)
            exe_pos=0,
            dirty=True,          # handoff mark for the table (all marked at start)
            crashes=0,
        )

    def key(self, s):
        return (freeze(s["row"]), s["pending"], s["processed"], s["fills"], s["cmd_off"],
                s["exe_off"], s["cmd_pos"], s["exe_pos"], s["dirty"], s["crashes"])

    def exe_tx(self, s, batch):
        pending = set(s["pending"]) | set(batch)
        pending -= {e for e in pending if e.pos in s["processed"]}  # discard redelivered
        row, keys, fills = drain(s["row"], pending)
        if keys:
            pending = {e for e in pending if e.pos not in keys}
        return dict(s, row=row, pending=frozenset(pending), processed=s["processed"] | keys,
                    fills=s["fills"] | fills)

    def successors(self, s):
        out = []
        # Command writer: consume the next command, commit MySQL, then either
        # commit the offset and mark (normal), or crash before the offset
        # commit and/or before the mark.
        if s["cmd_pos"] < len(self.commands):
            cmd = self.commands[s["cmd_pos"]]
            t = dict(s)
            if cmd[0] == "new":
                if t["row"] is None:
                    t["row"] = dict(qty=cmd[1], price=cmd[2], filled=0, status="PENDING", notional=0)
            else:  # modify: UPDATE ... SET price, qty, status='MODIFY_PENDING'
                if t["row"] is not None:
                    t["row"] = dict(t["row"], qty=cmd[1], price=cmd[2], status="MODIFY_PENDING")
            ok = dict(t, cmd_pos=s["cmd_pos"] + 1, cmd_off=s["cmd_pos"] + 1, dirty=True)
            out.append(("cmd", ok))
            if s["crashes"] < self.max_crashes:
                # Crash after the MySQL commit: the offset is not committed and
                # the in-memory mark is lost; restart marks every table.
                crashed = dict(t, cmd_pos=s["cmd_off"], exe_pos=s["exe_off"], dirty=True,
                               crashes=s["crashes"] + 1)
                out.append(("cmd-crash", crashed))
        # Execution writer: a batch of 1..max_batch events.
        for n in range(1, self.max_batch + 1):
            if s["exe_pos"] + n > len(self.events):
                break
            batch = self.events[s["exe_pos"]:s["exe_pos"] + n]
            t = self.exe_tx(s, batch)
            out.append((f"exe{n}", dict(t, exe_pos=s["exe_pos"] + n, exe_off=s["exe_pos"] + n)))
            if s["crashes"] < self.max_crashes:
                out.append((f"exe{n}-crash", dict(t, exe_pos=s["exe_off"], cmd_pos=s["cmd_off"],
                                                   dirty=True, crashes=s["crashes"] + 1)))
        # The execution writer's marked-table drain, and the idle sweep (same
        # effect on one table: drain whatever pending has a row now).
        if s["dirty"] or s["pending"]:
            t = self.exe_tx(s, [])
            out.append(("drain", dict(t, dirty=False)))
        return out

    def check(self, truth, label):
        seen = {}
        start = self.initial()
        queue = deque([(start, ())])
        seen[self.key(start)] = None
        violations = []
        while queue:
            s, trace = queue.popleft()
            # Safety at every state.
            if s["row"] is not None and s["row"]["filled"] > s["row"]["qty"]:
                violations.append(("filled > qty", trace, s))
            succ = self.successors(s)
            final = (s["cmd_pos"] == len(self.commands) and s["exe_pos"] == len(self.events)
                     and s["cmd_off"] == s["cmd_pos"] and s["exe_off"] == s["exe_pos"]
                     and not s["dirty"] and not s["pending"])
            if final:
                problem = truth(s)
                if problem:
                    violations.append((problem, trace, s))
            for step, t in succ:
                k = self.key(t)
                if k not in seen:
                    seen[k] = None
                    queue.append((t, trace + (step,)))
        return len(seen), violations


def expected(status, filled, fills, notional):
    def truth(s):
        row = s["row"]
        problems = []
        if row is None:
            return "order row missing"
        if row["status"] != status:
            problems.append(f"status {row['status']} != {status}")
        if row["filled"] != filled:
            problems.append(f"filled_qty {row['filled']} != {filled}")
        if len(s["fills"]) != fills:
            problems.append(f"fills {len(s['fills'])} != {fills}")
        if row["notional"] != notional:
            problems.append(f"notional {row['notional']} != {notional}")
        return "; ".join(problems)
    return truth


def scenarios():
    p = lambda i, o=0: (i, o)
    yield ("fill in two trades (taker, then maker)",
           [("new", 10, 100)],
           [Ev(p(1), ACCEPTED, 0, 0), Ev(p(1, 1), TRADE, 4, 100), Ev(p(1, 2), PARTIAL, 4, 0),
            Ev(p(1, 3), RESTING, 0, 0), Ev(p(2), TRADE, 6, 100), Ev(p(2, 1), FILLED, 0, 0)],
           expected("FILLED", 10, 2, 1000))
    yield ("rest then cancel",
           [("new", 10, 100)],
           [Ev(p(1), ACCEPTED, 0, 0), Ev(p(1, 1), RESTING, 0, 0), Ev(p(2), CANCELLED, 0, 0)],
           expected("CANCELLED", 0, 0, 0))
    yield ("partial fill then cancel",
           [("new", 10, 100)],
           [Ev(p(1), ACCEPTED, 0, 0), Ev(p(1, 1), RESTING, 0, 0), Ev(p(2), TRADE, 3, 100),
            Ev(p(3), CANCELLED, 0, 0)],
           expected("CANCELLED", 3, 1, 300))
    # Engine semantics (src/engine.rs MatchingEngine::modify): new_qty is
    # the new REMAINING quantity. 3 of 10 filled, then modify to 5:
    # 5 more can fill, 8 in total.
    yield ("partial fill, modify to remaining 5, 2 more fill (still resting 3)",
           [("new", 10, 100), ("modify", 5, 100)],
           [Ev(p(1), ACCEPTED, 0, 0), Ev(p(1, 1), RESTING, 0, 0), Ev(p(2), TRADE, 3, 100),
            Ev(p(3), MODIFIED, 5, 0), Ev(p(4), TRADE, 2, 100)],
           expected("PARTIAL", 5, 2, 500))
    yield ("partial fill, modify to remaining 5, all 5 fill",
           [("new", 10, 100), ("modify", 5, 100)],
           [Ev(p(1), ACCEPTED, 0, 0), Ev(p(1, 1), RESTING, 0, 0), Ev(p(2), TRADE, 3, 100),
            Ev(p(3), MODIFIED, 5, 0), Ev(p(4), TRADE, 5, 100), Ev(p(4, 1), FILLED, 0, 0)],
           expected("FILLED", 8, 2, 800))
    yield ("modify without fills",
           [("new", 10, 100), ("modify", 6, 101)],
           [Ev(p(1), ACCEPTED, 0, 0), Ev(p(1, 1), RESTING, 0, 0), Ev(p(2), MODIFIED, 6, 0)],
           expected("OPEN", 0, 0, 0))


def main():
    failed = False
    for name, commands, events, truth in scenarios():
        model = Model(commands, events)
        states, violations = model.check(truth, name)
        kinds = {}
        for problem, trace, _ in violations:
            kinds.setdefault(problem, trace)
        verdict = "OK" if not kinds else f"{len(kinds)} distinct violation(s)"
        print(f"[{verdict}] {name}: {states} states explored")
        for problem, trace in sorted(kinds.items(), key=lambda kv: len(kv[1])):
            failed = True
            print(f"    - {problem}\n      shortest trace: {' -> '.join(trace)}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
