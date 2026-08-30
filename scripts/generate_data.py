"""Regenerate Cadence Hall's synthetic CSVs.

Thin CLI over :mod:`cadence.data_gen` — one code path shared with the tests.

    uv run python scripts/generate_data.py --seed 42 --out data

Seed 42 reproduces the committed files byte for byte; any other seed writes a
fresh (still internally consistent) season over the top of them.
"""

import argparse
import importlib.util
import sys
from pathlib import Path

if importlib.util.find_spec("cadence") is None:
    # Running without an installed package (e.g. plain `python scripts/generate_data.py`
    # from the repo root): put the repo root — the parent of scripts/ — on sys.path.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cadence.data_gen import SEED, write_csvs  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=SEED, help="RNG seed (default: %(default)s)")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data"),
        help="output directory holding raw/, scans/, extra/ (default: %(default)s)",
    )
    args = parser.parse_args()
    write_csvs(args.out, seed=args.seed)
    print(f"Wrote seed-{args.seed} CSVs to {args.out}/raw, {args.out}/scans, {args.out}/extra")


if __name__ == "__main__":
    main()
