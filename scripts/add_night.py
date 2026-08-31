"""Deliver the next show night — the manual trigger behind `make new-day`.

First call hands over the held-back night 8 (the sensor demo in guide chapter 4).
After that it synthesizes brand-new nights — 9, 10, 11 … — so the season can keep
growing and the published report keeps changing.

Committed data is never touched: synthesized nights land in ``data/nights/<date>/``
(gitignored), which the raw assets read alongside the committed CSVs. That keeps
``data/raw/*.csv`` byte-identical to what the generator produces at seed 42.
"""

import argparse
import shutil
import sys
from pathlib import Path

if __package__ is None and str(Path(__file__).resolve().parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cadence.data_gen import (  # noqa: E402
    EVENTS,
    FLOAT_FMT,
    SENSOR_NIGHT,
    synthesize_night,
)
from cadence.resources import DATA_DIR, EXTRA_NIGHTS_DIR, SCANS_DIR  # noqa: E402


def delivered_nights() -> list[str]:
    """Show dates already present in data/scans/, oldest first."""
    return sorted(p.stem.replace("ticket_scans_", "") for p in SCANS_DIR.glob("ticket_scans_*.csv"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count", type=int, default=1, help="how many nights to deliver (default 1)"
    )
    args = parser.parse_args()

    for _ in range(max(1, args.count)):
        nights = delivered_nights()

        # The held-back night 8 comes first — that's the sensor demo.
        held_back = DATA_DIR / "extra" / f"ticket_scans_{SENSOR_NIGHT}.csv"
        if SENSOR_NIGHT not in nights and held_back.exists():
            shutil.copy2(held_back, SCANS_DIR / held_back.name)
            print(f"Delivered night {len(nights) + 1}: {SENSOR_NIGHT} (the held-back show)")
            continue

        number = len(nights) + 1
        night = synthesize_night(number)
        out = EXTRA_NIGHTS_DIR / night["event_date"]
        out.mkdir(parents=True, exist_ok=True)
        night["event"].to_csv(out / "events.csv", index=False, lineterminator="\n")
        night["orders"].to_csv(
            out / "orders.csv", index=False, float_format=FLOAT_FMT, lineterminator="\n"
        )
        night["scans"].to_csv(
            SCANS_DIR / f"ticket_scans_{night['event_date']}.csv", index=False, lineterminator="\n"
        )
        show = night["event"].iloc[0]
        print(
            f"Added night {number}: {show['event_name']} ({show['genre']}) on "
            f"{night['event_date']} — {len(night['orders'])} orders, {len(night['scans'])} scans"
        )

    remaining = len(EVENTS)
    print(
        "Materialize to see it: make materialize "
        f"(or let the sensor catch it). Committed roster: {remaining} nights; "
        "anything past that is synthesized into data/nights/ and gitignored."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
