import random
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import generator as g  # noqa: E402


def test_transitions_probabilities_sum_to_one():
    for status, opts in g.TRANSITIONS.items():
        assert abs(sum(p for _, p in opts) - 1.0) < 1e-9, status


def test_choose_next_status_terminal_states():
    rng = random.Random(1)
    assert g.choose_next_status("cancelled", rng) is None
    assert g.choose_next_status("refunded", rng) is None


def test_choose_next_status_only_valid_targets():
    rng = random.Random(7)
    valid = {s: {n for n, _ in o} for s, o in g.TRANSITIONS.items()}
    for _ in range(500):
        for status in g.TRANSITIONS:
            nxt = g.choose_next_status(status, rng)
            assert nxt is None or nxt in valid[status]


def test_cart_and_total():
    rng = random.Random(3)
    products = [(i, Decimal("10.00") + i) for i in range(1, 20)]
    for _ in range(100):
        cart = g.build_cart(products, rng)
        assert 1 <= len(cart) <= 4
        assert len({pid for pid, _, _ in cart}) == len(cart)
        assert g.cart_total(cart) == sum(Decimal(q) * p for _, q, p in cart)


def test_config_from_env_builds_dsn():
    cfg = g.Config.from_env(
        {"POSTGRES_HOST": "db", "POSTGRES_PASSWORD": "x", "SESSIONS_PER_SEC": "2.5"}
    )
    assert "host=db" in cfg.dsn and cfg.sessions_per_sec == 2.5
