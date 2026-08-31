"""Raw layer: the committed CSVs, read verbatim — no cleaning, no typing, no opinions yet."""

import dagster as dg
import pandas as pd

from cadence.resources import EXTRA_NIGHTS_DIR, RAW_DIR, SCANS_DIR


def _preview(df: pd.DataFrame) -> dg.MetadataValue:
    return dg.MetadataValue.md(df.head(10).to_markdown(index=False))


def _with_extra_nights(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Append rows from any synthesized nights (data/nights/<date>/<filename>).

    The committed CSVs stay untouched — `make new-day` writes extra shows here
    instead, so data/raw/*.csv remains byte-identical to the seed-42 generator.
    """
    extras = sorted(EXTRA_NIGHTS_DIR.glob(f"*/{filename}"))
    if not extras:
        return df
    return pd.concat([df, *(pd.read_csv(p) for p in extras)], ignore_index=True)


@dg.asset(
    group_name="raw",
    kinds={"csv"},
    description="Marketing's campaign roster, read verbatim from data/raw/campaigns.csv.",
)
def raw_campaigns(context: dg.AssetExecutionContext) -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "campaigns.csv")
    context.add_output_metadata(
        {"row_count": dg.MetadataValue.int(len(df)), "preview": _preview(df)}
    )
    return df


@dg.asset(
    group_name="raw",
    kinds={"csv"},
    description="The eight-night July 2025 event roster, read verbatim from data/raw/events.csv.",
)
def raw_events(context: dg.AssetExecutionContext) -> pd.DataFrame:
    df = _with_extra_nights(pd.read_csv(RAW_DIR / "events.csv"), "events.csv")
    context.add_output_metadata(
        {"row_count": dg.MetadataValue.int(len(df)), "preview": _preview(df)}
    )
    return df


@dg.asset(
    group_name="raw",
    kinds={"csv"},
    description="Every ticket order as the box office recorded it, from data/raw/orders.csv.",
)
def raw_orders(context: dg.AssetExecutionContext) -> pd.DataFrame:
    df = _with_extra_nights(pd.read_csv(RAW_DIR / "orders.csv"), "orders.csv")
    context.add_output_metadata(
        {"row_count": dg.MetadataValue.int(len(df)), "preview": _preview(df)}
    )
    return df


@dg.asset(
    group_name="raw",
    kinds={"csv"},
    description=(
        "Every gate scan from every nightly file in data/scans/ — "
        "the entry point the file-drop sensor refreshes."
    ),
)
def raw_ticket_scans(context: dg.AssetExecutionContext) -> pd.DataFrame:
    paths = sorted(SCANS_DIR.glob("ticket_scans_*.csv"))
    df = pd.concat([pd.read_csv(p) for p in paths]).reset_index(drop=True)
    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "files_ingested": dg.MetadataValue.int(len(paths)),
            "file_list": dg.MetadataValue.md("\n".join(f"- `{p.name}`" for p in paths)),
        }
    )
    return df
