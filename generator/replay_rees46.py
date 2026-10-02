#!/usr/bin/env python3
"""
Replay the REES46 e-commerce events CSV into Kafka, paced like the original traffic.

Why a replayer: the dataset is a historical file, but a streaming pipeline must be developed
against events that ARRIVE over time. The replayer re-creates that arrival pattern:
event N is sent when (event_time[N] - event_time[0]) / speedup seconds have passed.

What it does to each CSV row
  1. builds a deterministic event_id  (hash of the row + occurrence counter, see EventIdFactory)
  2. drops empty fields (empty brand/category_code become "missing", not "")
  3. sends JSON to Kafka, keyed by user_id (all events of a user go to one partition = per-user order)
  It does NOT rename event types or parse timestamps: bronze keeps the source as-is,
  silver does the cleaning.

Examples
  # look at the JSON, send nothing
  python generator/replay_rees46.py --file data/sample/oct_first_1m.csv --dry-run --max-events 5
  # replay 1M events at 1000x speed into Kafka
  python generator/replay_rees46.py --file data/sample/oct_first_1m.csv --speedup 1000
  # as fast as possible (bulk load, no pacing)
  python generator/replay_rees46.py --file data/sample/oct_first_1m.csv --speedup 0
"""
import argparse
import csv
import hashlib
import json
import sys
import time
from datetime import datetime, timezone

TS_FORMAT = "%Y-%m-%d %H:%M:%S UTC"       # e.g. 2019-10-01 00:00:00 UTC
EXPECTED_COLUMNS = ["event_time", "event_type", "product_id", "category_id", "category_code",
                    "brand", "price", "user_id", "user_session"]


def log(*a):
    """All diagnostics go to stderr so stdout stays clean for --dry-run output."""
    print(*a, file=sys.stderr, flush=True)


# ----------------------------------------------------------------------------- event id
class EventIdFactory:
    """
    The source has no event id, so we make one. Requirements:
      * deterministic   -> replaying the same file gives the same ids (idempotent downstream)
      * unique per event, even for rows that are byte-for-byte identical in the source

    event_id = sha1(all column values)[:20] + "-" + n, where n = how many identical rows we
    already saw in this same second (0 for the first copy, 1 for the second, ...).

    Identical rows always share the same event_time and the file is sorted by time, so we only
    remember the rows of the CURRENT timestamp and forget them when the timestamp changes.
    Memory stays tiny even for a 40M-row file.
    """

    def __init__(self):
        self.current_ts = None
        self.seen = {}

    def make(self, ts_str, values):
        if ts_str != self.current_ts:          # new second: reset the occurrence counters
            self.current_ts = ts_str
            self.seen = {}
        digest = hashlib.sha1("\x1f".join(values).encode("utf-8")).hexdigest()[:20]
        n = self.seen.get(digest, 0)
        self.seen[digest] = n + 1
        return "%s-%d" % (digest, n)


# ----------------------------------------------------------------------------- row -> event
def to_event(header, values, event_id):
    """Source columns as-is, minus empty values, plus event_id.
    IDs stay strings (category_id has 19 digits; many JSON tools lose precision on such numbers).
    price becomes a number; if it can't be parsed it is kept as text so silver can quarantine it."""
    ev = {"event_id": event_id}
    for name, value in zip(header, values):
        if value == "":
            continue
        if name == "price":
            try:
                value = float(value)
            except ValueError:
                pass
        ev[name] = value
    return ev


def parse_ts(ts_str):
    return datetime.strptime(ts_str, TS_FORMAT).replace(tzinfo=timezone.utc).timestamp()


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", required=True, help="REES46 CSV (with header)")
    ap.add_argument("--topic", default="rees46.events.raw")
    ap.add_argument("--bootstrap", default="localhost:29092", help="Kafka address as seen from the host")
    ap.add_argument("--speedup", type=float, default=1000.0,
                    help="1 = real time, 1000 = 1000x faster, 0 = no pacing (max speed)")
    ap.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = whole file)")
    ap.add_argument("--dry-run", action="store_true", help="print JSON to stdout, do not touch Kafka")
    ap.add_argument("--progress-every", type=int, default=100_000)
    args = ap.parse_args()

    # ---- producer (imported lazily so --dry-run works without the Kafka library)
    producer = None
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, msg):
        """Called by the producer for every message once Kafka acknowledged (or rejected) it."""
        if err is not None:
            stats["failed"] += 1
            if stats["failed"] <= 5:
                log("DELIVERY FAILED:", err)
        else:
            stats["delivered"] += 1

    if not args.dry_run:
        from confluent_kafka import Producer
        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "enable.idempotence": True,        # retries cannot create duplicates or reorder
            "acks": "all",
            "linger.ms": 20,                   # wait up to 20 ms to build bigger batches (throughput)
            "batch.size": 262144,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 500_000,
        })

    ids = EventIdFactory()
    sent = malformed = 0
    first_event_ts = last_ts = last_ts_str = None
    start_wall = time.monotonic()
    last_report = start_wall
    behind = 0.0                               # how late (seconds) we are vs the schedule

    try:
        with open(args.file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            if header != EXPECTED_COLUMNS:
                log("WARNING: unexpected header:", header)

            for values in reader:
                if len(values) != len(header):          # broken line: count it and move on
                    malformed += 1
                    continue

                # ---- time of this event (parse only when the string changes: ~16 events share a second)
                ts_str = values[0]
                if ts_str != last_ts_str:
                    last_ts = parse_ts(ts_str)
                    last_ts_str = ts_str
                    if first_event_ts is None:
                        first_event_ts = last_ts

                # ---- pacing: sleep only if we are AHEAD of schedule
                if args.speedup > 0:
                    due = start_wall + (last_ts - first_event_ts) / args.speedup
                    now = time.monotonic()
                    lag = due - now
                    if lag > 0.002:                      # ignore sub-2ms gaps, avoids a syscall per event
                        time.sleep(lag)
                        behind = 0.0
                    else:
                        behind = -lag

                # ---- build and send
                ev = to_event(header, values, ids.make(ts_str, values))
                payload = json.dumps(ev, separators=(",", ":"))

                if args.dry_run:
                    print(payload)
                else:
                    key = ev.get("user_id")
                    while True:
                        try:
                            producer.produce(args.topic, value=payload.encode("utf-8"),
                                             key=key.encode("utf-8") if key else None,
                                             on_delivery=on_delivery)
                            break
                        except BufferError:              # local queue full: let the producer drain, retry
                            producer.poll(0.5)
                    producer.poll(0)                     # serve delivery callbacks without blocking

                sent += 1
                if sent % args.progress_every == 0:
                    t = time.monotonic()
                    log("sent %s | %.0f ev/s | event time %s | behind schedule %.2fs | failed %d"
                        % (format(sent, ","), args.progress_every / max(t - last_report, 1e-9),
                           last_ts_str, behind, stats["failed"]))
                    last_report = t

                if args.max_events and sent >= args.max_events:
                    break
    except KeyboardInterrupt:
        log("interrupted: flushing what was already produced")

    if producer is not None:
        remaining = producer.flush(60)
        if remaining:
            log("WARNING: %d messages were not delivered before the flush timeout" % remaining)

    elapsed = time.monotonic() - start_wall
    span = (last_ts - first_event_ts) if first_event_ts is not None else 0
    log("---- summary ----")
    log("events sent        : %s" % format(sent, ","))
    if not args.dry_run:
        log("delivered / failed : %s / %s" % (format(stats["delivered"], ","), stats["failed"]))
    log("malformed lines    : %d" % malformed)
    log("event-time covered : %.0f s (%.1f h)" % (span, span / 3600))
    log("wall-clock time    : %.1f s  (effective speedup %.0fx)" % (elapsed, span / elapsed if elapsed else 0))


if __name__ == "__main__":
    main()