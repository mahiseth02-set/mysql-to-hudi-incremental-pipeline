#!/usr/bin/env python3
"""
Dummy data generator for the source MySQL table.

  --mode seed    : insert N fresh orders
  --mode churn   : simulate a day of OLTP activity - updates, soft deletes and new orders
"""
import argparse
import random
from datetime import datetime, timedelta

import pymysql

CITIES = ["Mumbai", "Pune", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Ahmedabad"]
CATEGORIES = ["Electronics", "Grocery", "Fashion", "Home", "Books", "Sports"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "RETURNED", "CANCELLED"]


def connect(args):
    return pymysql.connect(host=args.host, port=args.port, user=args.user,
                           password=args.password, database="shop_demo", autocommit=False)


def random_order(order_id, start):
    created = start + timedelta(minutes=random.randint(0, 60 * 24 * 90))
    return (order_id, random.randint(1, 50_000), random.choice(CITIES), random.choice(CATEGORIES),
            round(random.uniform(99, 25_000), 2), random.choice(STATUSES[:3]), 0, created)


def seed(conn, rows, batch=5_000):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
        next_id = cur.fetchone()[0] + 1
        start = datetime(2026, 6, 1)
        sql = ("INSERT INTO orders (order_id, customer_id, city, category, amount, status, is_deleted, created_at) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
        for i in range(0, rows, batch):
            data = [random_order(next_id + j, start) for j in range(i, min(i + batch, rows))]
            cur.executemany(sql, data)
            conn.commit()
    print(f"Seeded {rows} orders")


def churn(conn, updates, deletes, inserts):
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(order_id), MAX(order_id) FROM orders")
        lo, hi = cur.fetchone()
        upd_ids = random.sample(range(lo, hi + 1), updates)
        cur.executemany("UPDATE orders SET status=%s, amount=amount*%s WHERE order_id=%s",
                        [(random.choice(STATUSES), round(random.uniform(0.9, 1.1), 3), i) for i in upd_ids])
        del_ids = random.sample(range(lo, hi + 1), deletes)
        cur.executemany("UPDATE orders SET is_deleted=1 WHERE order_id=%s", [(i,) for i in del_ids])
        conn.commit()
    seed(conn, inserts)
    print(f"Churn: {updates} updates, {deletes} soft-deletes, {inserts} inserts")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["seed", "churn"], required=True)
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--updates", type=int, default=5_000)
    p.add_argument("--deletes", type=int, default=500)
    p.add_argument("--inserts", type=int, default=2_000)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="root")
    args = p.parse_args()

    conn = connect(args)
    try:
        if args.mode == "seed":
            seed(conn, args.rows)
        else:
            churn(conn, args.updates, args.deletes, args.inserts)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
