from decimal import Decimal

from shopsphere_orchestration.monitoring import SlotHealth, evaluate_recon, evaluate_slot


def test_missing_slot_is_a_problem():
    assert evaluate_slot(SlotHealth(False, None, 0, 0), 2048)


def test_inactive_slot_between_syncs_is_healthy():
    # Airbyte is micro-batch: the slot is only "active" while a sync runs. Not an incident.
    assert evaluate_slot(SlotHealth(True, "reserved", 10.0, 1.0), 2048) == []


def test_large_retained_wal_and_lost_slot_flagged():
    assert evaluate_slot(SlotHealth(True, "reserved", 5000.0, 0), 2048)
    assert evaluate_slot(SlotHealth(True, "lost", 1.0, 0), 2048)


def test_reconciliation_tolerance():
    assert evaluate_recon((1000, Decimal("5000.00")), (1000, Decimal("5000.00")), 0.5) == []
    assert evaluate_recon((1000, Decimal("5000.00")), (997, Decimal("4990.00")), 0.5) == []
    assert evaluate_recon((1000, Decimal("5000.00")), (900, Decimal("4000.00")), 0.5)
