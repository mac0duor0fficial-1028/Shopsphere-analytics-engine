"""ShopSphere OLTP traffic generator.

Continuously simulates storefront traffic against Postgres so that every CDC operation
type (INSERT, UPDATE, DELETE) is exercised:

* browsing sessions (events) that convert into orders with items and payments
* order lifecycle: pending -> paid -> shipped -> delivered (-> refunded) or -> cancelled
* inventory decrements, restocks and cancellation restocks
* customer / product updates (price changes, country changes)
* hard deletes of old cancelled orders (with their items and payments)

Usage:
    python generator.py                 # run until SIGINT/SIGTERM
    RUN_SECONDS=60 python generator.py  # run for a fixed duration
"""

from __future__ import annotations

import logging
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal

import psycopg
from faker import Faker

log = logging.getLogger("shopsphere.generator")

CATEGORIES = ["Electronics", "Home", "Fashion", "Beauty", "Sports", "Toys", "Books", "Grocery"]
COUNTRIES = ["US", "GB", "DE", "FR", "KE", "NG", "ZA", "IN", "BR", "CA", "AU", "JP"]
CHANNELS = ["web", "mobile_app", "marketplace", "social"]
CHANNEL_WEIGHTS = [45, 35, 12, 8]
PAY_METHODS = ["card", "paypal", "mpesa", "bank_transfer"]
PAY_WEIGHTS = [60, 20, 15, 5]

# funnel conversion probabilities
P_ADD_TO_CART = 0.35
P_BEGIN_CHECKOUT = 0.60
P_PURCHASE = 0.70

# Order state machine: status -> [(next_status, probability)]. Probabilities sum to 1.
TRANSITIONS: dict[str, list[tuple[str, float]]] = {
    "pending": [("paid", 0.80), ("cancelled", 0.20)],
    "paid": [("shipped", 0.92), ("cancelled", 0.08)],
    "shipped": [("delivered", 1.00)],
    "delivered": [
        ("refunded", 1.00)
    ],  # only ~3% of delivered orders are eligible (see advance_orders)
}


class OutOfStock(Exception):
    """Raised inside a (sub)transaction to roll an order back."""


@dataclass(frozen=True)
class Config:
    dsn: str
    sessions_per_sec: float = 5.0
    seed_customers: int = 500
    seed_products: int = 120
    lifecycle_delay_s: int = 20
    delete_prob: float = 0.15
    run_seconds: int = 0  # 0 == forever
    rng_seed: int | None = None

    @staticmethod
    def from_env(env: dict[str, str] | None = None) -> "Config":
        e = env if env is not None else os.environ
        dsn = e.get("PG_DSN") or (
            f"host={e.get('POSTGRES_HOST', 'localhost')} port={e.get('POSTGRES_PORT', '5432')} "
            f"dbname={e.get('POSTGRES_DB', 'shopsphere')} user={e.get('POSTGRES_USER', 'shop_admin')} "
            f"password={e.get('POSTGRES_PASSWORD', '')}"
        )
        seed = e.get("RNG_SEED")
        return Config(
            dsn=dsn,
            sessions_per_sec=float(e.get("SESSIONS_PER_SEC", "5")),
            seed_customers=int(e.get("SEED_CUSTOMERS", "500")),
            seed_products=int(e.get("SEED_PRODUCTS", "120")),
            lifecycle_delay_s=int(e.get("LIFECYCLE_DELAY_SECONDS", "20")),
            delete_prob=float(e.get("DELETE_PROB", "0.15")),
            run_seconds=int(e.get("RUN_SECONDS", "0")),
            rng_seed=int(seed) if seed else None,
        )


# --------------------------------------------------------------------------- pure helpers
def choose_next_status(status: str, rng: random.Random) -> str | None:
    """Return the next status, or None if the order is terminal / stays put."""
    options = TRANSITIONS.get(status)
    if not options:
        return None
    nxt = rng.choices([s for s, _ in options], weights=[p for _, p in options])[0]
    return None if nxt == status else nxt


def build_cart(
    products: list[tuple[int, Decimal]], rng: random.Random
) -> list[tuple[int, int, Decimal]]:
    """Pick 1-4 distinct products; returns [(product_id, quantity, unit_price)]."""
    n = rng.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
    chosen = rng.sample(products, min(n, len(products)))
    return [(pid, rng.choices([1, 2, 3], weights=[70, 22, 8])[0], price) for pid, price in chosen]


def cart_total(cart: list[tuple[int, int, Decimal]]) -> Decimal:
    return sum((Decimal(q) * p for _, q, p in cart), Decimal("0.00")).quantize(Decimal("0.01"))


# --------------------------------------------------------------------------- DB actions
def seed_if_empty(conn: psycopg.Connection, cfg: Config, rng: random.Random, fake: Faker) -> None:
    with conn.transaction():
        if conn.execute("SELECT count(*) FROM shop.products").fetchone()[0] == 0:
            log.info("Seeding %d products", cfg.seed_products)
            for i in range(cfg.seed_products):
                price = Decimal(str(round(rng.uniform(3, 500), 2)))
                pid = conn.execute(
                    "INSERT INTO shop.products (sku, name, category, price) VALUES (%s,%s,%s,%s) "
                    "RETURNING product_id",
                    (f"SKU-{i + 1:05d}", fake.catch_phrase()[:60], rng.choice(CATEGORIES), price),
                ).fetchone()[0]
                conn.execute(
                    "INSERT INTO shop.inventory (product_id, stock_qty) VALUES (%s,%s)",
                    (pid, rng.randint(50, 500)),
                )
        if conn.execute("SELECT count(*) FROM shop.customers").fetchone()[0] == 0:
            log.info("Seeding %d customers", cfg.seed_customers)
            for _ in range(cfg.seed_customers):
                new_customer(conn, rng, fake)


def new_customer(conn: psycopg.Connection, rng: random.Random, fake: Faker) -> int:
    email = f"{uuid.uuid4().hex[:12]}@{fake.free_email_domain()}"
    return conn.execute(
        "INSERT INTO shop.customers (email, full_name, country) VALUES (%s,%s,%s) RETURNING customer_id",
        (email, fake.name(), rng.choice(COUNTRIES)),
    ).fetchone()[0]


def load_reference(conn: psycopg.Connection) -> tuple[list[tuple[int, Decimal]], list[int]]:
    products = conn.execute(
        "SELECT product_id, price FROM shop.products WHERE is_active"
    ).fetchall()
    customers = [r[0] for r in conn.execute("SELECT customer_id FROM shop.customers").fetchall()]
    return products, customers


def place_order(
    conn: psycopg.Connection,
    rng: random.Random,
    customer_id: int,
    cart: list[tuple[int, int, Decimal]],
) -> int:
    """Create order + items + pending payment and decrement stock. Raises OutOfStock to roll back."""
    channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
    total = cart_total(cart)
    order_id = conn.execute(
        "INSERT INTO shop.orders (customer_id, status, channel, total_amount) "
        "VALUES (%s,'pending',%s,%s) RETURNING order_id",
        (customer_id, channel, total),
    ).fetchone()[0]
    for product_id, qty, price in cart:
        cur = conn.execute(
            "UPDATE shop.inventory SET stock_qty = stock_qty - %s "
            "WHERE product_id = %s AND stock_qty >= %s",
            (qty, product_id, qty),
        )
        if cur.rowcount != 1:
            raise OutOfStock(product_id)
        conn.execute(
            "INSERT INTO shop.order_items (order_id, product_id, quantity, unit_price) VALUES (%s,%s,%s,%s)",
            (order_id, product_id, qty, price),
        )
    conn.execute(
        "INSERT INTO shop.payments (order_id, method, status, amount) VALUES (%s,%s,'pending',%s)",
        (order_id, rng.choices(PAY_METHODS, weights=PAY_WEIGHTS)[0], total),
    )
    return order_id


def run_session(
    conn: psycopg.Connection,
    rng: random.Random,
    products: list[tuple[int, Decimal]],
    customer_ids: list[int],
) -> bool:
    """One browsing session. Returns True if it produced an order."""
    session_id = uuid.uuid4()
    customer_id = rng.choice(customer_ids) if rng.random() < 0.7 else None
    viewed = rng.sample(products, min(rng.randint(1, 5), len(products)))
    sql = (
        "INSERT INTO shop.events (session_id, customer_id, event_type, product_id, order_id, occurred_at) "
        "VALUES (%s,%s,%s,%s,%s,clock_timestamp())"
    )
    ordered = False
    with conn.transaction():
        for pid, _ in viewed:
            conn.execute(sql, (session_id, customer_id, "product_view", pid, None))
        if rng.random() >= P_ADD_TO_CART:
            return False
        cart = build_cart(viewed, rng)
        for pid, _, _ in cart:
            conn.execute(sql, (session_id, customer_id, "add_to_cart", pid, None))
        if rng.random() >= P_BEGIN_CHECKOUT:
            return False
        customer_id = customer_id or rng.choice(customer_ids)  # login happens at checkout
        conn.execute(sql, (session_id, customer_id, "begin_checkout", None, None))
        if rng.random() >= P_PURCHASE:
            return False
        try:
            with conn.transaction():  # savepoint: an out-of-stock order must not drop the events
                order_id = place_order(conn, rng, customer_id, cart)
            conn.execute(sql, (session_id, customer_id, "purchase", None, order_id))
            ordered = True
        except OutOfStock as exc:
            log.debug("Out of stock for product %s", exc)
    return ordered


def _restock_order(conn: psycopg.Connection, order_id: int) -> None:
    conn.execute(
        "UPDATE shop.inventory i SET stock_qty = i.stock_qty + s.qty "
        "FROM (SELECT product_id, SUM(quantity)::int AS qty FROM shop.order_items "
        "      WHERE order_id = %s GROUP BY product_id) s WHERE i.product_id = s.product_id",
        (order_id,),
    )


def advance_orders(
    conn: psycopg.Connection, rng: random.Random, delay_s: int, batch: int = 40
) -> int:
    """Move a batch of aged orders one step along the state machine. Returns rows changed."""
    changed = 0
    with conn.transaction():
        rows = conn.execute(
            "SELECT order_id, status FROM shop.orders "
            "WHERE (status IN ('pending','paid','shipped') "
            "       OR (status = 'delivered' AND order_id %% 33 = 0)) "  # ~3% of orders get refunded
            "  AND updated_at < now() - make_interval(secs => %s) "
            "ORDER BY updated_at LIMIT %s FOR UPDATE SKIP LOCKED",
            (delay_s, batch),
        ).fetchall()
        for order_id, status in rows:
            new = choose_next_status(status, rng)
            if new is None:
                continue
            if new == "paid":
                conn.execute(
                    "UPDATE shop.payments SET status='captured' WHERE order_id=%s AND status='pending'",
                    (order_id,),
                )
            elif new == "cancelled":
                pay_status = "failed" if status == "pending" else "refunded"
                conn.execute(
                    "UPDATE shop.payments SET status=%s WHERE order_id=%s", (pay_status, order_id)
                )
                _restock_order(conn, order_id)
            elif new == "refunded":
                conn.execute(
                    "UPDATE shop.payments SET status='refunded' WHERE order_id=%s", (order_id,)
                )
                _restock_order(conn, order_id)
            conn.execute("UPDATE shop.orders SET status=%s WHERE order_id=%s", (new, order_id))
            changed += 1
    return changed


def delete_old_cancelled(conn: psycopg.Connection, delay_s: int) -> int:
    """Hard-delete one old cancelled order (children first) to exercise CDC deletes."""
    with conn.transaction():
        row = conn.execute(
            "SELECT order_id FROM shop.orders WHERE status='cancelled' "
            "AND updated_at < now() - make_interval(secs => %s) "
            "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED",
            (delay_s * 3,),
        ).fetchone()
        if not row:
            return 0
        oid = row[0]
        conn.execute("DELETE FROM shop.payments WHERE order_id=%s", (oid,))
        conn.execute("DELETE FROM shop.order_items WHERE order_id=%s", (oid,))
        conn.execute("DELETE FROM shop.orders WHERE order_id=%s", (oid,))
    return 1


def restock_low_inventory(conn: psycopg.Connection, rng: random.Random) -> int:
    with conn.transaction():
        rows = conn.execute(
            "SELECT product_id FROM shop.inventory WHERE stock_qty < 20 "
            "ORDER BY random() LIMIT 10 FOR UPDATE SKIP LOCKED"
        ).fetchall()
        for (pid,) in rows:
            conn.execute(
                "UPDATE shop.inventory SET stock_qty = stock_qty + %s WHERE product_id=%s",
                (rng.randint(50, 200), pid),
            )
    return len(rows)


def random_mutations(conn: psycopg.Connection, rng: random.Random) -> None:
    """Low-frequency master-data updates (price changes, customer moves)."""
    if rng.random() < 0.5:
        factor = Decimal(str(round(rng.uniform(0.9, 1.1), 3)))
        conn.execute(
            "UPDATE shop.products SET price = round(price * %s, 2) "
            "WHERE product_id = (SELECT product_id FROM shop.products ORDER BY random() LIMIT 1)",
            (factor,),
        )
    else:
        conn.execute(
            "UPDATE shop.customers SET country = %s "
            "WHERE customer_id = (SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1)",
            (rng.choice(COUNTRIES),),
        )


# --------------------------------------------------------------------------- main loop
def run(cfg: Config) -> None:
    rng = random.Random(cfg.rng_seed)
    fake = Faker()
    if cfg.rng_seed is not None:
        Faker.seed(cfg.rng_seed)

    stop = {"flag": False}

    def _handle(signum, _frame):  # noqa: ANN001
        log.info("Signal %s received, shutting down", signum)
        stop["flag"] = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    deadline = time.monotonic() + cfg.run_seconds if cfg.run_seconds else None
    interval = 1.0 / max(cfg.sessions_per_sec, 0.01)
    stats = {"sessions": 0, "orders": 0, "advanced": 0, "deleted": 0}
    last_log = time.monotonic()

    while not stop["flag"] and (deadline is None or time.monotonic() < deadline):
        try:
            with psycopg.connect(cfg.dsn, autocommit=True) as conn:
                seed_if_empty(conn, cfg, rng, fake)
                products, customer_ids = load_reference(conn)
                next_tick = time.monotonic()
                i = 0
                while not stop["flag"] and (deadline is None or time.monotonic() < deadline):
                    i += 1
                    stats["sessions"] += 1
                    stats["orders"] += int(run_session(conn, rng, products, customer_ids))
                    if i % 5 == 0:
                        stats["advanced"] += advance_orders(conn, rng, cfg.lifecycle_delay_s)
                    if i % 25 == 0 and rng.random() < cfg.delete_prob * 4:
                        stats["deleted"] += delete_old_cancelled(conn, cfg.lifecycle_delay_s)
                    if i % 50 == 0:
                        restock_low_inventory(conn, rng)
                    if i % 100 == 0:
                        random_mutations(conn, rng)
                        customer_ids.append(new_customer(conn, rng, fake))
                    if i % 1000 == 0:
                        products, customer_ids = load_reference(conn)
                    if time.monotonic() - last_log > 10:
                        log.info("stats %s", stats)
                        last_log = time.monotonic()
                    next_tick += interval
                    time.sleep(max(0.0, next_tick - time.monotonic()))
        except psycopg.OperationalError as exc:
            log.warning("DB connection problem (%s); retrying in 5s", exc)
            time.sleep(5)
    log.info("Stopped. Final stats: %s", stats)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s"
    )
    run(Config.from_env())
