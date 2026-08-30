"""Deterministic synthetic data generator for Cadence Hall.

One code path shared by ``scripts/generate_data.py`` and the test suite: the
committed CSVs under ``data/`` are exactly ``write_csvs(Path("data"), seed=42)``,
byte for byte. All randomness comes from a single stdlib ``random.Random(seed)``
(no numpy RNG), and every distribution knob is a module constant, so the same
seed always reproduces the same season.

The data is seeded to be interesting, not to be an industry benchmark. The
planted dirt (mutated promo codes, duplicate scans, orphan scans) is documented
in ``docs/data-dictionary.md`` and asserted exactly in ``tests/test_generator.py``.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

SEED = 42

DATE_FMT = "%Y-%m-%d"
TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"
FLOAT_FMT = "%.2f"

# --------------------------------------------------------------------------- #
# Rosters (hardcoded — identical across seeds)                                #
# --------------------------------------------------------------------------- #

CAPACITY = 1200
DOORS_TIME = "19:00"
SHOW_TIME = "20:00"

TIERS = ["GA", "Balcony", "VIP"]
TIER_CAPACITY = {"GA": 800, "Balcony": 300, "VIP": 100}

# (event_id, event_name, genre, event_date)
EVENTS: list[tuple[str, str, str, str]] = [
    ("EV-01", "The Midnight Standard", "jazz", "2025-07-01"),
    ("EV-02", "Copper & Oak", "folk", "2025-07-02"),
    ("EV-03", "Glass Anthem", "indie", "2025-07-03"),
    ("EV-04", "Brasswork Riot", "funk", "2025-07-04"),
    ("EV-05", "Neon Coyote", "electronic", "2025-07-05"),
    ("EV-06", "Laugh Track Live", "comedy", "2025-07-06"),
    ("EV-07", "Static Bloom", "punk", "2025-07-07"),
    ("EV-08", "Riverlight Orchestra", "classical", "2025-07-08"),
]

# USD per ticket for (event, tier). Lives only here — orders carry unit_price_usd.
TIER_PRICES: dict[str, dict[str, float]] = {
    "EV-01": {"GA": 36.00, "Balcony": 52.00, "VIP": 85.00},
    "EV-02": {"GA": 32.00, "Balcony": 46.00, "VIP": 75.00},
    "EV-03": {"GA": 30.00, "Balcony": 44.00, "VIP": 70.00},
    "EV-04": {"GA": 40.00, "Balcony": 56.00, "VIP": 90.00},
    "EV-05": {"GA": 42.00, "Balcony": 58.00, "VIP": 95.00},
    "EV-06": {"GA": 38.00, "Balcony": 50.00, "VIP": 80.00},
    "EV-07": {"GA": 34.00, "Balcony": 48.00, "VIP": 72.00},
    "EV-08": {"GA": 45.00, "Balcony": 62.00, "VIP": 110.00},
}

# Fraction of capacity each show sells (EV-05 is the sellout: every tier fills).
SELL_THROUGH: dict[str, float] = {
    "EV-01": 0.82,
    "EV-02": 0.74,
    "EV-03": 0.45,  # the flop
    "EV-04": 0.93,  # big Friday
    "EV-05": 1.00,  # the sellout
    "EV-06": 0.85,
    "EV-07": 0.78,
    "EV-08": 0.88,  # the sensor night
}

# (campaign_id, name, channel, promo_code, spend_usd, starts_on, ends_on)
CAMPAIGNS: list[tuple[str, str, str, str, float, str, str]] = [
    ("CMP-01", "Summer Kickoff", "instagram", "SUMMER25", 1800.00, "2025-06-18", "2025-07-08"),
    ("CMP-02", "VIP Love Letter", "email", "VIPNIGHT", 120.00, "2025-06-20", "2025-07-08"),
    ("CMP-03", "Radio Week", "radio", "ONAIR10", 2400.00, "2025-06-20", "2025-06-30"),
    ("CMP-04", "Retarget Blitz", "facebook", "", 950.00, "2025-06-22", "2025-07-08"),
    ("CMP-05", "Search Brand", "google_search", "FINDUS", 700.00, "2025-06-20", "2025-07-08"),
    ("CMP-06", "Street Team Flyers", "street_team", "STREET15", 600.00, "2025-06-24", "2025-07-06"),
    ("CMP-07", "Local Press Blast", "email", "PRESSPLAY", 200.00, "2025-06-21", "2025-07-02"),
    ("CMP-08", "Genre Nights", "instagram", "GENREGEM", 850.00, "2025-06-25", "2025-07-08"),
    ("CMP-09", "Last Call Push", "instagram", "LASTCALL", 1100.00, "2025-07-01", "2025-07-08"),
    ("CMP-10", "Student Rush", "street_team", "STUDENT10", 300.00, "2025-06-20", "2025-07-08"),
    ("CMP-11", "Encore Newsletter", "email", "ENCORE", 150.00, "2025-06-20", "2025-07-08"),
    ("CMP-12", "Partner Playlist", "youtube", "PLAYLOUD", 500.00, "2025-06-23", "2025-07-08"),
]

# --------------------------------------------------------------------------- #
# Order distribution knobs                                                    #
# --------------------------------------------------------------------------- #

ORDER_WINDOW_START = "2025-06-20"  # orders open at 00:00:00 on this date

QTY_CHOICES = [1, 2, 3, 4, 5, 6]
QTY_WEIGHTS = [0.35, 0.38, 0.13, 0.08, 0.04, 0.02]

TIER_WEIGHTS = {"GA": 0.62, "Balcony": 0.26, "VIP": 0.12}

# Two-hump purchase timing: early burst decaying from on-sale, late ramp into the show.
EARLY_HUMP_P = 0.55
EARLY_MEAN_DAYS = 5.0  # exponential decay from window start
LATE_MEAN_DAYS = 4.0  # exponential ramp toward the event (mean days before)

# Relative weight of each hour 0–23 for ordered_at (evenings dominate).
HOUR_WEIGHTS = [
    0.2,
    0.1,
    0.1,
    0.1,
    0.1,
    0.2,
    0.3,
    0.5,  # 00–07
    0.8,
    1.0,
    1.2,
    1.4,
    1.6,
    1.5,
    1.4,
    1.4,  # 08–15
    1.6,
    2.2,
    3.5,
    4.0,
    4.5,
    4.0,
    3.0,
    1.0,  # 16–23
]

PROMO_ATTACH_RATE = 0.45  # share of orders carrying a promo code
VIP_BOOST_CAMPAIGN = "CMP-02"  # VIPNIGHT punches above its spend for VIP tickets…
VIP_BOOST_FACTOR = 4.0  # …by this weight multiplier

REFUND_RATE = 0.07

# Planted dirt: exactly this many promo-coded orders get a mutated code.
DIRTY_SUMMER25_COUNT = 90
DIRTY_VIPNIGHT_COUNT = 30
DIRTY_OTHER_COUNT = 30
DIRTY_PROMO_TOTAL = DIRTY_SUMMER25_COUNT + DIRTY_VIPNIGHT_COUNT + DIRTY_OTHER_COUNT  # 150
PROMO_MUTATIONS = ["lower", "leading_space", "trailing_space", "title"]

# --------------------------------------------------------------------------- #
# Scan distribution knobs                                                     #
# --------------------------------------------------------------------------- #

BASE_ATTENDANCE = 0.85
MAX_ATTENDANCE = 0.98
TIER_ATTENDANCE_MULT = {"GA": 0.98, "Balcony": 1.02, "VIP": 1.08}
EVENT_ATTENDANCE_MULT = {"EV-06": 1.05, "EV-07": 0.85}  # best show-up / worst show-up

SCAN_MEAN_MINUTES_BEFORE_SHOW = 25
SCAN_STD_MINUTES = 18
SCAN_CUTOFF_TIME = "21:00"  # arrivals truncated to [doors, cutoff]

GATES = ["MAIN-1", "MAIN-2", "SIDE-A"]
GATE_WEIGHTS = [0.42, 0.38, 0.20]

DUPLICATE_SCAN_RATE = 0.015  # share of scanned tickets scanned a second time
DUPLICATE_DELAY_SECONDS = (60, 180)  # re-scan lands 1–3 minutes later

ORPHAN_NIGHT = "2025-07-03"
ORPHAN_ORDER_IDS = ["ORD-99991", "ORD-99992", "ORD-99993"]

SENSOR_NIGHT = "2025-07-08"  # held back in data/extra/ for the sensor demo


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _hhmm_to_seconds(hhmm: str) -> int:
    hours, minutes = hhmm.split(":")
    return int(hours) * 3600 + int(minutes) * 60


def _events_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_id": event_id,
                "event_name": name,
                "genre": genre,
                "event_date": event_date,
                "capacity": CAPACITY,
                "doors_time": DOORS_TIME,
                "show_time": SHOW_TIME,
            }
            for event_id, name, genre, event_date in EVENTS
        ]
    )


def _campaigns_frame() -> pd.DataFrame:
    return pd.DataFrame(
        CAMPAIGNS,
        columns=[
            "campaign_id",
            "name",
            "channel",
            "promo_code",
            "spend_usd",
            "starts_on",
            "ends_on",
        ],
    )


def _draw_ordered_at(rng: random.Random, event_date: date) -> datetime:
    """One purchase timestamp inside [window start, day before the show 23:59:59]."""
    first_day = date.fromisoformat(ORDER_WINDOW_START)
    last_day = event_date - timedelta(days=1)
    if rng.random() < EARLY_HUMP_P:
        day = first_day + timedelta(days=int(rng.expovariate(1.0 / EARLY_MEAN_DAYS)))
    else:
        day = last_day - timedelta(days=int(rng.expovariate(1.0 / LATE_MEAN_DAYS)))
    day = min(max(day, first_day), last_day)
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS, k=1)[0]
    return datetime.combine(day, time(hour, rng.randrange(60), rng.randrange(60)))


def _generate_order_rows(rng: random.Random) -> list[dict]:
    """Draw orders per event until each show hits its sell-through target.

    Per-tier capacity is never exceeded (the drawn qty is clamped to the tier's
    remaining seats), so EV-05 at 100% sell-through fills GA/Balcony/VIP exactly.
    """
    rows: list[dict] = []
    for event_id, _name, _genre, event_date in EVENTS:
        target_tickets = round(CAPACITY * SELL_THROUGH[event_id])
        remaining = dict(TIER_CAPACITY)
        tickets = 0
        while tickets < target_tickets:
            open_tiers = [t for t in TIERS if remaining[t] > 0]
            tier = rng.choices(open_tiers, weights=[TIER_WEIGHTS[t] for t in open_tiers], k=1)[0]
            qty = min(rng.choices(QTY_CHOICES, weights=QTY_WEIGHTS, k=1)[0], remaining[tier])
            remaining[tier] -= qty
            tickets += qty
            rows.append(
                {
                    "event_id": event_id,
                    "ordered_at": _draw_ordered_at(rng, date.fromisoformat(event_date)),
                    "tier": tier,
                    "qty": qty,
                    "unit_price_usd": TIER_PRICES[event_id][tier],
                }
            )
    # Assign IDs in purchase order (stable sort: ties keep generation order).
    rows.sort(key=lambda row: row["ordered_at"])
    for number, row in enumerate(rows, start=1):
        row["order_id"] = f"ORD-{number:05d}"
    return rows


def _attach_promo_codes(rng: random.Random, rows: list[dict]) -> None:
    """45% of orders carry the code of a campaign live at purchase time.

    Choice is weighted by campaign spend; CMP-02 (VIPNIGHT) is boosted for
    VIP-tier orders. CMP-04 has no code, so it can never attach — the
    "invisible to code attribution" lesson.
    """
    for row in rows:
        if rng.random() >= PROMO_ATTACH_RATE:
            row["promo_code"] = None
            continue
        order_day = row["ordered_at"].strftime(DATE_FMT)
        codes: list[str] = []
        weights: list[float] = []
        for campaign_id, _n, _c, code, spend, starts_on, ends_on in CAMPAIGNS:
            if not code or not (starts_on <= order_day <= ends_on):
                continue
            if row["tier"] == "VIP" and campaign_id == VIP_BOOST_CAMPAIGN:
                spend = spend * VIP_BOOST_FACTOR
            codes.append(code)
            weights.append(spend)
        row["promo_code"] = rng.choices(codes, weights=weights, k=1)[0]


def _mutate_code(code: str, mutation: str) -> str:
    if mutation == "lower":
        return code.lower()
    if mutation == "leading_space":
        return " " + code
    if mutation == "trailing_space":
        return code + " "
    return code.title()


def _plant_dirty_codes(rng: random.Random, rows: list[dict]) -> None:
    """THE planted bug: exactly 150 promo-coded orders get a mutated code."""
    summer = [i for i, row in enumerate(rows) if row["promo_code"] == "SUMMER25"]
    vipnight = [i for i, row in enumerate(rows) if row["promo_code"] == "VIPNIGHT"]
    other = [
        i for i, row in enumerate(rows) if row["promo_code"] not in (None, "SUMMER25", "VIPNIGHT")
    ]
    # Cap each bucket at its population: rare seeds produce fewer than 30 VIPNIGHT
    # orders, and rng.sample raises on over-sized requests. When every bucket is
    # full-sized (seed 42 included) this makes exactly the same RNG calls as the
    # uncapped version, so committed-data byte-identity is preserved.
    picked = (
        rng.sample(summer, min(DIRTY_SUMMER25_COUNT, len(summer)))
        + rng.sample(vipnight, min(DIRTY_VIPNIGHT_COUNT, len(vipnight)))
        + rng.sample(other, min(DIRTY_OTHER_COUNT, len(other)))
    )
    shortfall = DIRTY_PROMO_TOTAL - len(picked)
    if shortfall > 0:
        chosen = set(picked)
        leftovers = [i for i in summer + vipnight + other if i not in chosen]
        picked += rng.sample(leftovers, min(shortfall, len(leftovers)))
    for index in sorted(picked):
        row = rows[index]
        row["promo_code"] = _mutate_code(row["promo_code"], rng.choice(PROMO_MUTATIONS))


def _assign_status(rng: random.Random, rows: list[dict]) -> None:
    for row in rows:
        row["status"] = "refunded" if rng.random() < REFUND_RATE else "completed"


def _orders_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "order_id": row["order_id"],
                "event_id": row["event_id"],
                "ordered_at": row["ordered_at"].strftime(TIMESTAMP_FMT),
                "tier": row["tier"],
                "qty": row["qty"],
                "unit_price_usd": row["unit_price_usd"],
                "promo_code": row["promo_code"],
                "status": row["status"],
            }
            for row in rows
        ]
    )


def _draw_scan_seconds(rng: random.Random) -> int:
    """Seconds after midnight for one arrival, truncated to [doors, cutoff]."""
    mean = _hhmm_to_seconds(SHOW_TIME) - SCAN_MEAN_MINUTES_BEFORE_SHOW * 60
    drawn = int(round(rng.normalvariate(mean, SCAN_STD_MINUTES * 60)))
    return min(max(drawn, _hhmm_to_seconds(DOORS_TIME)), _hhmm_to_seconds(SCAN_CUTOFF_TIME))


def _scan_row(
    ticket_id: str, order_id: str, event_id: str, gate: str, night: date, seconds: int
) -> dict:
    return {
        "ticket_id": ticket_id,
        "order_id": order_id,
        "event_id": event_id,
        "gate": gate,
        "scanned_at": datetime.combine(night, time()) + timedelta(seconds=seconds),
    }


def _generate_scans(rng: random.Random, order_rows: list[dict]) -> dict[str, pd.DataFrame]:
    """One scan file per show night, with planted duplicates and orphans."""
    orders_by_event: dict[str, list[dict]] = {event_id: [] for event_id, *_ in EVENTS}
    for row in order_rows:
        orders_by_event[row["event_id"]].append(row)

    scans_by_night: dict[str, pd.DataFrame] = {}
    for night_number, (event_id, _name, _genre, event_date) in enumerate(EVENTS, start=1):
        night = date.fromisoformat(event_date)
        scans: list[dict] = []

        for order in orders_by_event[event_id]:
            if order["status"] != "completed":
                continue  # refunded orders' tickets never scan
            probability = min(
                max(
                    BASE_ATTENDANCE
                    * TIER_ATTENDANCE_MULT[order["tier"]]
                    * EVENT_ATTENDANCE_MULT.get(event_id, 1.0),
                    0.0,
                ),
                MAX_ATTENDANCE,
            )
            for ticket_number in range(1, order["qty"] + 1):
                if rng.random() >= probability:
                    continue
                ticket_id = f"{order['order_id']}-T{ticket_number}"
                seconds = _draw_scan_seconds(rng)
                scans.append(
                    _scan_row(
                        ticket_id, order["order_id"], event_id, _draw_gate(rng), night, seconds
                    )
                )
                if rng.random() < DUPLICATE_SCAN_RATE:  # planted dirt: double scan
                    delay = rng.randint(*DUPLICATE_DELAY_SECONDS)
                    scans.append(
                        _scan_row(
                            ticket_id,
                            order["order_id"],
                            event_id,
                            _draw_gate(rng),
                            night,
                            seconds + delay,
                        )
                    )

        if event_date == ORPHAN_NIGHT:  # planted dirt: scans matching no order
            for orphan_order_id in ORPHAN_ORDER_IDS:
                scans.append(
                    _scan_row(
                        f"{orphan_order_id}-T1",
                        orphan_order_id,
                        event_id,
                        _draw_gate(rng),
                        night,
                        _draw_scan_seconds(rng),
                    )
                )

        scans.sort(key=lambda scan: scan["scanned_at"])
        scans_by_night[event_date] = pd.DataFrame(
            [
                {
                    "scan_id": f"SCN-{night_number}-{number:05d}",
                    "ticket_id": scan["ticket_id"],
                    "order_id": scan["order_id"],
                    "event_id": scan["event_id"],
                    "gate": scan["gate"],
                    "scanned_at": scan["scanned_at"].strftime(TIMESTAMP_FMT),
                }
                for number, scan in enumerate(scans, start=1)
            ]
        )
    return scans_by_night


def _draw_gate(rng: random.Random) -> str:
    return rng.choices(GATES, weights=GATE_WEIGHTS, k=1)[0]


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #


def generate_all(seed: int = SEED) -> dict:
    """Generate the whole season. Same seed, same season — always.

    Returns ``{"campaigns": DataFrame, "events": DataFrame, "orders": DataFrame,
    "scans_by_night": dict[str, DataFrame]}`` with ``scans_by_night`` keyed
    ``"2025-07-01"`` … ``"2025-07-08"``. Date/timestamp columns are pre-formatted
    strings so the frames round-trip byte-identically through ``write_csvs``.
    """
    rng = random.Random(seed)
    order_rows = _generate_order_rows(rng)
    _attach_promo_codes(rng, order_rows)
    _plant_dirty_codes(rng, order_rows)
    _assign_status(rng, order_rows)
    return {
        "campaigns": _campaigns_frame(),
        "events": _events_frame(),
        "orders": _orders_frame(order_rows),
        "scans_by_night": _generate_scans(rng, order_rows),
    }


def write_csvs(out_dir: Path, seed: int = SEED) -> None:
    """Write every CSV under ``out_dir`` exactly as committed to the repo.

    Layout: ``raw/{campaigns,events,orders}.csv``, ``scans/ticket_scans_<night>.csv``
    for nights 1–7, and ``extra/ticket_scans_2025-07-08.csv`` (night 8 is held
    back for the sensor demo — ``make new-day`` copies it into ``scans/``).
    """
    out_dir = Path(out_dir)
    data = generate_all(seed)
    raw_dir = out_dir / "raw"
    scans_dir = out_dir / "scans"
    extra_dir = out_dir / "extra"
    for directory in (raw_dir, scans_dir, extra_dir):
        directory.mkdir(parents=True, exist_ok=True)

    _write_csv(data["campaigns"], raw_dir / "campaigns.csv")
    _write_csv(data["events"], raw_dir / "events.csv")
    _write_csv(data["orders"], raw_dir / "orders.csv")
    for night, frame in data["scans_by_night"].items():
        parent = extra_dir if night == SENSOR_NIGHT else scans_dir
        _write_csv(frame, parent / f"ticket_scans_{night}.csv")
    # Night 8 belongs in extra/ until `make new-day` delivers it. If a PREVIOUS
    # season's copy is sitting in scans/ (someone ran new-day, then regenerated),
    # leaving it there would silently blend two seasons — remove it.
    (scans_dir / f"ticket_scans_{SENSOR_NIGHT}.csv").unlink(missing_ok=True)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format=FLOAT_FMT, lineterminator="\n")
