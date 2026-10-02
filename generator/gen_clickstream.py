#!/usr/bin/env python3
"""
Synthetic e-commerce clickstream generator.

Produces realistic *messy* data so the pipeline has real work to do:
  * sessions with a funnel (page_view -> product_view -> add_to_cart -> begin_checkout -> purchase)
  * skewed product / user popularity, diurnal traffic curve
  * late-arriving events   (event_ts on day D, delivered on day D+1)
  * duplicate events       (same event_id re-delivered on day D+1)
  * corrupt records        (truncated JSON, null user_id, negative price, bad type, bad timestamp)

Sinks:
  files  -> <out>/dt=YYYY-MM-DD/part-*.json.gz   (dt = ARRIVAL date, not event date)
  kafka  -> topic, keyed by user_id

Examples:
  python gen_clickstream.py --start-date 2026-09-01 --days 7 --events-per-day 200000 --sink files --out ../data/landing
  python gen_clickstream.py --start-date 2026-09-01 --days 7 --events-per-day 200000 --sink kafka
"""
import argparse
import gzip
import json
import os
import random
import sys
import time
from datetime import date, timedelta
from multiprocessing import Pool

EVENT_PAGE = "page_view"
CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "toys", "books",
              "grocery", "automotive", "garden", "pets", "office"]

def _expand(weighted):
    out = []
    for v, w in weighted:
        out += [v] * w
    return out

COUNTRIES = _expand([("IN", 30), ("US", 20), ("GB", 8), ("DE", 6), ("SG", 5), ("AE", 5),
                     ("AU", 4), ("CA", 4), ("FR", 4), ("BR", 4), ("JP", 3), ("ZA", 2), ("NZ", 5)])
DEVICES = _expand([("mobile", 58), ("desktop", 36), ("tablet", 6)])
OS_BY_DEVICE = {"mobile": ["Android", "Android", "iOS"],
                "desktop": ["Windows", "Windows", "macOS", "Linux"],
                "tablet": ["iPadOS", "Android"]}
BROWSERS = ["Chrome", "Chrome", "Chrome", "Safari", "Safari", "Firefox", "Edge", "Samsung Internet"]
UTM_SOURCES = _expand([(None, 40), ("google", 25), ("facebook", 12), ("instagram", 8),
                       ("email", 8), ("affiliate", 4), ("youtube", 3)])
HOUR_W = [1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 6, 6, 6, 7, 8, 9, 10, 9, 6, 3]
HOURS = _expand([(h, w) for h, w in enumerate(HOUR_W)])
REFERRERS = [None, None, "https://www.google.com/", "https://www.facebook.com/",
             "https://t.co/", "https://www.bing.com/"]


def user_profile(uid):
    """Deterministic per-user attributes (cheap hash, no per-user storage)."""
    h = (uid * 2654435761) & 0xFFFFFFFF
    device = DEVICES[(h >> 3) % len(DEVICES)]
    return (COUNTRIES[(h >> 7) % len(COUNTRIES)], device,
            OS_BY_DEVICE[device][(h >> 11) % len(OS_BY_DEVICE[device])],
            BROWSERS[(h >> 15) % len(BROWSERS)])


def product(pid):
    return CATEGORIES[pid % len(CATEGORIES)], round(5 + ((pid * 7919) % 49500) / 100, 2)


def ts_str(day_strs, sec):
    """sec = float seconds since 00:00:00 of day index 0; rolls over midnight."""
    d, sod = divmod(sec, 86400.0)
    s = int(sod)
    return "%sT%02d:%02d:%02d.%03dZ" % (day_strs[int(d)], s // 3600, (s // 60) % 60, s % 60,
                                         int((sod - s) * 1000))


def corrupt(rng, ev, line):
    kind = rng.randrange(5)
    if kind == 0:                               # truncated / invalid JSON
        return line[: max(10, len(line) // 2)]
    if kind == 1:
        ev.pop("user_id", None)
    elif kind == 2 and "price" in ev:
        ev["price"] = -abs(ev["price"])
    elif kind == 3:
        ev["event_type"] = "unknown_evt"
    else:
        ev["event_ts"] = "not-a-timestamp"
    return json.dumps(ev, separators=(",", ":"))


def run_shard(task):
    (day_idx, shard, a) = task
    rng = random.Random(a.seed * 1_000_003 + day_idx * 1009 + shard)
    base = date.fromisoformat(a.start_date) + timedelta(days=day_idx)
    day_strs = [(base + timedelta(days=i)).isoformat() for i in range(3)]
    target = a.events_per_day // a.shards
    counter = 0
    n_events = n_late = n_dup = n_bad = 0
    out = []                                    # (arrival_offset_days, line)

    while n_events < target:
        uid = int(a.users * rng.random() ** 1.5)
        country, device, os_, browser = user_profile(uid)
        utm = rng.choice(UTM_SOURCES)
        referrer = rng.choice(REFERRERS)
        sec = rng.choice(HOURS) * 3600 + rng.random() * 3600
        cart = []
        seen_product = has_cart = False
        steps = min(1 + int(rng.expovariate(1 / 4.0)), 60)
        plan = ["page_view"]
        for _ in range(steps):
            r = rng.random()
            if seen_product and r < 0.20:
                plan.append("add_to_cart")
                has_cart = True
            elif has_cart and r < 0.24:
                plan.append("remove_from_cart")
            elif r < 0.62:
                plan.append("product_view")
            elif r < 0.78:
                plan.append("search")
            else:
                plan.append("page_view")
            if plan[-1] == "product_view":
                seen_product = True

        last_p = None
        for et in plan:
            sec += rng.uniform(2, 90)
            ev = {"event_id": "d%d-s%d-%d" % (day_idx, shard, counter),
                  "event_ts": ts_str(day_strs, sec),
                  "user_id": "u%08d" % uid, "event_type": et,
                  "device_type": device, "os": os_, "browser": browser, "country": country}
            if utm:
                ev["utm_source"] = utm
                ev["utm_campaign"] = "%s_%d" % (utm, uid % 7)
            if referrer:
                ev["referrer"] = referrer
            if et == "page_view":
                ev["page_url"] = rng.choice(["/", "/c/" + rng.choice(CATEGORIES), "/deals", "/help"])
            elif et == "search":
                ev["page_url"] = "/search?q=%s" % rng.choice(CATEGORIES)
            elif et == "product_view":
                last_p = int(a.products * rng.random() ** 3)
                cat, price = product(last_p)
                ev.update(page_url="/p/%d" % last_p, product_id="P%06d" % last_p, category=cat, price=price)
            elif et == "add_to_cart":
                if last_p is None:
                    continue
                cat, price = product(last_p)
                q = rng.randint(1, 3)
                cart.append((last_p, price, q))
                ev.update(product_id="P%06d" % last_p, category=cat, price=price, quantity=q)
            elif et == "remove_from_cart":
                if not cart:
                    continue
                p, price, q = cart.pop()
                ev.update(product_id="P%06d" % p, category=product(p)[0], price=price, quantity=q)
            counter += 1
            n_events += 1
            _emit(rng, a, ev, out)
        # checkout funnel
        if cart and rng.random() < 0.35:
            sec += rng.uniform(5, 60)
            _emit(rng, a, {"event_id": "d%d-s%d-%d" % (day_idx, shard, counter),
                           "event_ts": ts_str(day_strs, sec), "user_id": "u%08d" % uid,
                           "event_type": "begin_checkout", "device_type": device, "os": os_,
                           "browser": browser, "country": country}, out)
            counter += 1
            n_events += 1
            if rng.random() < 0.45:
                sec += rng.uniform(10, 240)
                _emit(rng, a, {"event_id": "d%d-s%d-%d" % (day_idx, shard, counter),
                               "event_ts": ts_str(day_strs, sec), "user_id": "u%08d" % uid,
                               "event_type": "purchase", "order_id": "O%d-%d-%d" % (day_idx, shard, counter),
                               "order_value": round(sum(p * q for _, p, q in cart), 2),
                               "device_type": device, "os": os_, "browser": browser,
                               "country": country}, out)
                counter += 1
                n_events += 1

    # ---- sink
    if a.sink == "files":
        handles = {}
        for off, line in out:
            if off not in handles:
                arrival = (base + timedelta(days=off)).isoformat()
                d = os.path.join(a.out, "dt=" + arrival)
                os.makedirs(d, exist_ok=True)
                name = "part-d%d-s%d%s.json.gz" % (day_idx, shard, "-late" if off else "")
                handles[off] = gzip.open(os.path.join(d, name), "wt", compresslevel=3)
            handles[off].write(line + "\n")
        for h in handles.values():
            h.close()
    else:
        from confluent_kafka import Producer
        p = Producer({"bootstrap.servers": a.bootstrap, "linger.ms": 50, "batch.size": 262144,
                      "compression.type": "lz4", "queue.buffering.max.messages": 500000})
        for _, line in out:
            key = _key(line)
            while True:
                try:
                    p.produce(a.topic, value=line.encode(), key=key)
                    break
                except BufferError:
                    p.poll(0.5)
            p.poll(0)
        p.flush()
    return (len(out), n_events)


def _key(line):
    i = line.find('"user_id":"')
    return line[i + 11:i + 20].encode() if i >= 0 else b"unknown"


def _emit(rng, a, ev, out):
    line = json.dumps(ev, separators=(",", ":"))
    r = rng.random()
    if r < a.bad_rate:
        out.append((0, corrupt(rng, ev, line)))
        return
    if r < a.bad_rate + a.late_rate:
        out.append((1, line))                   # delivered next day
        return
    out.append((0, line))
    if rng.random() < a.dup_rate:
        out.append((1, line))                   # re-delivered next day


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start-date", default="2026-09-01")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--events-per-day", type=int, default=200_000)
    ap.add_argument("--users", type=int, default=50_000, help="scale with volume: ~ events-per-day / 4")
    ap.add_argument("--products", type=int, default=50_000)
    ap.add_argument("--shards", type=int, default=8, help="parallel shards per day")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--sink", choices=["files", "kafka"], default="files")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "data", "landing"))
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--topic", default="clickstream.raw")
    ap.add_argument("--late-rate", type=float, default=0.02)
    ap.add_argument("--dup-rate", type=float, default=0.01)
    ap.add_argument("--bad-rate", type=float, default=0.005)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    tasks = [(d, s, a) for d in range(a.days) for s in range(a.shards)]
    t0 = time.time()
    total = 0
    with Pool(a.workers) as pool:
        for i, (written, _) in enumerate(pool.imap_unordered(run_shard, tasks), 1):
            total += written
            if i % a.shards == 0 or i == len(tasks):
                rate = total / max(time.time() - t0, 1e-9)
                print("  %d/%d shards | %s records | %.0fk rec/s" % (i, len(tasks), format(total, ","), rate / 1000),
                      flush=True)
    print("done: %s records in %.1fs -> %s" % (format(total, ","), time.time() - t0,
                                              a.out if a.sink == "files" else a.topic))


if __name__ == "__main__":
    main()
