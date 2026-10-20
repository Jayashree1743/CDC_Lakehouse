#!/usr/bin/env python3
"""
DAY 1 (optional bonus): Traffic Simulator
=========================================
Plays the role of FinContract Corp's real application: constantly inserts,
updates and deletes rows in PostgreSQL so Kafka visibly streams live events —
exactly like a production system, without you typing SQL all day.

Usage:
    python3 simulate_traffic.py                 # default: one change every 5s
    python3 simulate_traffic.py --interval 2    # faster
    python3 simulate_traffic.py --duration 300  # stop after 5 minutes

Requires the docker-compose Day 1 stack to be running (cdc-postgres on port 5436).
Uses ONLY the Python standard library (psycopg not needed) via `docker exec psql`.
"""
import argparse
import random
import subprocess
import sys
import time

PSQL_CMD = [
    "docker", "exec", "-i", "cdc-postgres",
    "psql", "-U", "demo", "-d", "inventory", "-v", "ON_ERROR_STOP=1",
]


def sql(statement: str) -> None:
    """Run one SQL statement inside the cdc-postgres container."""
    result = subprocess.run(
        PSQL_CMD + ["-c", statement],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  !! SQL failed: {statement}\n     {result.stderr.strip()}")
    else:
        print(f"  ok: {statement}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate live application traffic")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="seconds between changes (default 5)")
    parser.add_argument("--duration", type=int, default=0,
                        help="stop after N seconds (0 = run until Ctrl+C)")
    args = parser.parse_args()

    start = time.time()
    tick = 0
    print(f"Traffic simulator started — one change every {args.interval}s. Ctrl+C to stop.")
    print("Watch topic: docker exec -it cdc-kafka kafka-console-consumer "
          "--bootstrap-server localhost:9092 --topic inventory.public.contracts\n")

    while True:
        if args.duration and (time.time() - start) >= args.duration:
            print("\nDuration reached. Stopping.")
            return
        tick += 1
        action = random.choice(["update_price", "update_price", "new_customer", "update_chunk", "cancel_contract"])
        print(f"[{time.strftime('%H:%M:%S')}] tick {tick}: {action}")

        if action == "update_price":
            new_price = round(random.uniform(300, 2000), 2)
            sql(f"UPDATE contracts SET monthly_price = {new_price}, updated_at = now() "
                f"WHERE id = (SELECT id FROM contracts ORDER BY random() LIMIT 1);")
        elif action == "new_customer":
            n = tick
            sql(f"INSERT INTO customers (name, email) VALUES "
                f"('Sim Co {n}', 'sim{n}@example.com');")
        elif action == "update_chunk":
            sql(f"UPDATE contract_documents SET chunk_text = chunk_text || ' [edited {time.strftime('%H:%M:%S')}]', "
                f"updated_at = now() WHERE id = (SELECT id FROM contract_documents ORDER BY random() LIMIT 1);")
        elif action == "cancel_contract":
            sql(f"UPDATE contracts SET status = 'cancelled', updated_at = now() "
                f"WHERE id = (SELECT id FROM contracts WHERE status = 'active' ORDER BY random() LIMIT 1);")

        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
        sys.exit(0)
