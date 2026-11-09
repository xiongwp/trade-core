#!/usr/bin/env python3
"""Sustained throughput of a running trade-core service, from its metrics.

Samples one or more Prometheus endpoints, sums the given counters across them
and reports the rate per interval. A rate only counts as sustainable while the
backlog gauge (consumer lag, outbox pending ...) is not growing: a stage that
keeps up shows a flat lag; one that is saturated shows it climbing while its
rate plateaus. See docs/deployment-2m-tps.md section 9.

    python3 rates.py --endpoint pj01:9200 --counter tc_execution_mysql_completed_events \
        --gauge tc_order_mysql_consumer_lag --seconds 300

Exit code 0 when the run was sustainable, 1 when the backlog grew.
"""
import argparse
import sys
import time
import urllib.request


def scrape(endpoint):
    url = endpoint if endpoint.startswith("http") else f"http://{endpoint}/metrics"
    values = {}
    with urllib.request.urlopen(url, timeout=5) as response:
        for line in response.read().decode().splitlines():
            if not line or line.startswith("#"):
                continue
            name, _, value = line.rpartition(" ")
            name = name.split("{", 1)[0]
            try:
                values[name] = values.get(name, 0.0) + float(value)
            except ValueError:
                pass
    return values


def total(endpoints, names):
    sums = {name: 0.0 for name in names}
    for endpoint in endpoints:
        try:
            values = scrape(endpoint)
        except OSError as error:
            print(f"  ! {endpoint}: {error}", file=sys.stderr)
            continue
        for name in names:
            sums[name] += values.get(name, 0.0)
    return sums


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--endpoint", action="append", required=True, help="host:port of /metrics (repeatable)")
    parser.add_argument("--counter", action="append", required=True, help="monotonic counter to rate (repeatable, summed)")
    parser.add_argument("--gauge", action="append", default=[], help="backlog gauge that must stay flat (repeatable, summed)")
    parser.add_argument("--seconds", type=float, default=120)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--warmup", type=float, default=30, help="seconds ignored at the start")
    parser.add_argument("--max-growth", type=float, default=0.05,
                        help="allowed backlog growth as a fraction of the work done (default 5%%)")
    args = parser.parse_args()

    names = args.counter + args.gauge
    print(f"warming up {args.warmup:.0f} s ...")
    time.sleep(args.warmup)
    start = total(args.endpoint, names)
    started = time.time()
    previous, previous_at = start, started
    rates = []
    while time.time() - started < args.seconds:
        time.sleep(args.interval)
        now = total(args.endpoint, names)
        at = time.time()
        done = sum(now[c] - previous[c] for c in args.counter)
        rate = done / (at - previous_at)
        backlog = sum(now[g] for g in args.gauge)
        rates.append(rate)
        print(f"  +{at - started:6.0f}s  {rate:12,.0f}/s  backlog {backlog:12,.0f}")
        previous, previous_at = now, at
    end = previous
    elapsed = previous_at - started
    done = sum(end[c] - start[c] for c in args.counter)
    growth = sum(end[g] - start[g] for g in args.gauge)
    average = done / elapsed if elapsed > 0 else 0
    sustainable = not args.gauge or growth <= args.max_growth * max(done, 1)
    print()
    print(f"average   {average:12,.0f}/s over {elapsed:.0f} s")
    if rates:
        print(f"min / max {min(rates):12,.0f}/s / {max(rates):,.0f}/s per {args.interval:.0f} s")
    if args.gauge:
        print(f"backlog   {sum(start[g] for g in args.gauge):12,.0f} -> {sum(end[g] for g in args.gauge):,.0f}")
    print("verdict   " + ("SUSTAINABLE: raise the load and run again" if sustainable
                          else "SATURATED: the backlog grew; the sustainable rate is below this average"))
    sys.exit(0 if sustainable else 1)


if __name__ == "__main__":
    main()
