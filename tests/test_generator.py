"""Generator guarantees: determinism, byte-identical committed CSVs, referential
integrity (SPEC section 3.6), and the planted-dirt registry counts."""

import pandas as pd
import pytest

from cadence.data_gen import SEED, generate_all, write_csvs
from cadence.resources import DATA_DIR

NIGHTS = [f"2025-07-0{n}" for n in range(1, 9)]
EXPECTED_FILES = [
    "raw/campaigns.csv",
    "raw/events.csv",
    "raw/orders.csv",
    *[f"scans/ticket_scans_2025-07-0{n}.csv" for n in range(1, 8)],
    "extra/ticket_scans_2025-07-08.csv",
]
ORPHAN_ORDER_IDS = {"ORD-99991", "ORD-99992", "ORD-99993"}


@pytest.fixture(scope="module")
def frames() -> dict:
    return generate_all(SEED)


def _codes(orders: pd.DataFrame) -> pd.Series:
    """promo_code as strings, with nulls (however represented) as empty strings."""
    return orders["promo_code"].fillna("").astype(str)


def _all_scans(frames: dict) -> pd.DataFrame:
    return pd.concat(frames["scans_by_night"].values(), ignore_index=True)


def test_seed_constant():
    assert SEED == 42


def test_deterministic(frames):
    again = generate_all(42)
    for key in ("campaigns", "events", "orders"):
        pd.testing.assert_frame_equal(frames[key], again[key])
    assert sorted(frames["scans_by_night"]) == NIGHTS
    assert sorted(again["scans_by_night"]) == NIGHTS
    for night in NIGHTS:
        pd.testing.assert_frame_equal(
            frames["scans_by_night"][night], again["scans_by_night"][night]
        )
    other = generate_all(7)
    assert not frames["orders"].equals(other["orders"])


def test_committed_csvs_reproducible(tmp_path):
    write_csvs(tmp_path, seed=42)
    produced = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.csv"))
    assert produced == sorted(EXPECTED_FILES)
    for rel in EXPECTED_FILES:
        generated = (tmp_path / rel).read_bytes()
        committed = (DATA_DIR / rel).read_bytes()
        assert generated == committed, f"{rel} is not byte-identical to the committed file"


def test_referential_integrity(frames):
    orders = frames["orders"]
    events = frames["events"]
    campaigns = frames["campaigns"]
    scans = _all_scans(frames)

    # orders.event_id is a subset of events.event_id
    assert set(orders["event_id"]) <= set(events["event_id"])

    # every promo code, once normalized (strip + upper), resolves to a campaign code
    codes = _codes(orders)
    coded = codes.str.strip() != ""
    campaign_codes = {c for c in campaigns["promo_code"].fillna("").astype(str) if c.strip()}
    assert set(codes[coded].str.strip().str.upper()) <= campaign_codes

    # scans.order_id is a subset of orders.order_id except exactly the 3 planted orphans
    known = scans["order_id"].isin(set(orders["order_id"]))
    assert set(scans.loc[~known, "order_id"]) == ORPHAN_ORDER_IDS
    orphan_nights = {
        night
        for night, df in frames["scans_by_night"].items()
        if df["order_id"].isin(ORPHAN_ORDER_IDS).any()
    }
    assert orphan_nights == {"2025-07-03"}

    linked = scans[known].merge(
        orders[["order_id", "event_id", "qty", "status"]],
        on="order_id",
        how="left",
        suffixes=("", "_order"),
    )
    # scans.event_id matches the owning order's event
    assert (linked["event_id"] == linked["event_id_order"]).all()
    # ticket suffix number is between 1 and the order's qty
    suffix = linked["ticket_id"].str.extract(r"-T(\d+)$")[0].astype(int)
    assert (suffix >= 1).all()
    assert (suffix <= linked["qty"]).all()
    # scans only reference completed orders
    assert (linked["status"] == "completed").all()


def test_seed42_magnitudes(frames):
    orders = frames["orders"]
    scans = _all_scans(frames)
    assert 3000 <= len(orders) <= 4000
    assert 5000 <= len(scans) <= 7000
    # overall show-up rate after orphan-drop and per-ticket dedupe
    deduped = scans[scans["order_id"].isin(set(orders["order_id"]))].drop_duplicates("ticket_id")
    tickets_sold = orders.loc[orders["status"] == "completed", "qty"].sum()
    rate = len(deduped) / tickets_sold
    assert 0.78 <= rate <= 0.90


def test_planted_dirt_counts(frames):
    orders = frames["orders"]
    scans = _all_scans(frames)

    # THE bug: exactly 150 promo-coded orders carry a mutated (non-canonical) code
    codes = _codes(orders)
    coded = codes.str.strip() != ""
    dirty = coded & (codes != codes.str.strip().str.upper())
    assert int(dirty.sum()) == 150

    # exactly 3 orphan scan rows
    orphans = ~scans["order_id"].isin(set(orders["order_id"]))
    assert int(orphans.sum()) == 3

    # duplicate second scans exist
    assert int(scans.duplicated("ticket_id").sum()) > 0

    # EV-05 is the sellout: exactly 1200 tickets across all its orders
    assert int(orders.loc[orders["event_id"] == "EV-05", "qty"].sum()) == 1200


def test_write_csvs_removes_stale_night8(tmp_path):
    """Regenerating a season must not leave a PREVIOUS season's night-8 file in
    scans/ (someone ran `make new-day`, then `make data SEED=n`) — that would
    silently blend two seasons with every check green."""
    stale = tmp_path / "scans" / "ticket_scans_2025-07-08.csv"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale old season\n")
    write_csvs(tmp_path, seed=7)
    assert not stale.exists()
    assert (tmp_path / "extra" / "ticket_scans_2025-07-08.csv").exists()


def test_dirty_codes_survive_sparse_seeds():
    """Seeds where a campaign draws fewer than its dirty-code quota (e.g. 161)
    must still generate — with the full 150 planted from other coded orders."""
    orders = generate_all(161)["orders"]
    codes = _codes(orders)
    coded = codes.str.strip() != ""
    dirty = coded & (codes != codes.str.strip().str.upper())
    assert int(dirty.sum()) == 150
