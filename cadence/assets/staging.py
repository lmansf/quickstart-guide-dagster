"""Staging layer: typed, cleaned, analysis-ready tables (one planted bug included)."""

import dagster as dg
import pandas as pd


def normalize_promo_codes(s: pd.Series) -> pd.Series:
    """Canonicalize promo codes: strip surrounding whitespace and uppercase (NA-preserving).

    This is THE fix for the planted promo-code bug (docs/guide/03-checks.md). The shipped
    ``stg_orders`` asset deliberately does NOT call it — following the red check's
    hint to this file and applying it is the learner's job.
    """
    return s.str.strip().str.upper()


@dg.asset(
    group_name="staging",
    description="Campaign roster with parsed dates and NA for the code-less campaign.",
)
def stg_campaigns(context: dg.AssetExecutionContext, raw_campaigns: pd.DataFrame) -> pd.DataFrame:
    df = raw_campaigns.copy()
    df["starts_on"] = pd.to_datetime(df["starts_on"])
    df["ends_on"] = pd.to_datetime(df["ends_on"])
    df["spend_usd"] = df["spend_usd"].astype("float64")
    # Empty promo_code (CMP-04) reads as NaN; make the "no code" case explicitly pd.NA.
    df["promo_code"] = df["promo_code"].replace("", pd.NA).where(df["promo_code"].notna(), pd.NA)
    context.add_output_metadata({"row_count": dg.MetadataValue.int(len(df))})
    return df


@dg.asset(
    group_name="staging",
    description="Event roster with parsed dates and a show_ts timestamp (doors 19:00, show 20:00).",
)
def stg_events(context: dg.AssetExecutionContext, raw_events: pd.DataFrame) -> pd.DataFrame:
    df = raw_events.copy()
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["capacity"] = df["capacity"].astype("int64")
    df["show_ts"] = df["event_date"] + pd.Timedelta(hours=20)
    context.add_output_metadata({"row_count": dg.MetadataValue.int(len(df))})
    return df


@dg.asset(
    group_name="staging",
    description=(
        "Orders with parsed timestamps and revenue math — and, for now, "
        "promo codes exactly as messy as marketing typed them."
    ),
)
def stg_orders(context: dg.AssetExecutionContext, raw_orders: pd.DataFrame) -> pd.DataFrame:
    df = raw_orders.copy()
    df["ordered_at"] = pd.to_datetime(df["ordered_at"])
    df["order_date"] = df["ordered_at"].dt.strftime("%Y-%m-%d")
    df = df[(df["qty"] > 0) & (df["unit_price_usd"] > 0)].reset_index(drop=True)
    df["gross_revenue"] = df["qty"] * df["unit_price_usd"]
    df["net_revenue"] = df["gross_revenue"].where(df["status"] != "refunded", 0.0)

    # =========================================================================================
    # TODO(you): promo codes arrive messy from marketing (" SUMMER25", "summer25", "VIPNIGHT ").
    # Normalize them here with: .str.strip().str.upper()  (see docs/guide/03-checks.md)
    #
    # The `all_promo_orders_attributed` check is red because this line is missing. One-line fix:
    #     df["promo_code"] = normalize_promo_codes(df["promo_code"])
    # =========================================================================================

    refunded = int((df["status"] == "refunded").sum())
    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "refunded_orders": dg.MetadataValue.int(refunded),
        }
    )
    return df


@dg.asset(
    group_name="staging",
    description="Gate scans deduped to one scan per ticket, with orphan scans dropped.",
)
def stg_ticket_scans(
    context: dg.AssetExecutionContext,
    raw_ticket_scans: pd.DataFrame,
    stg_orders: pd.DataFrame,
) -> pd.DataFrame:
    df = raw_ticket_scans.copy()
    df["scanned_at"] = pd.to_datetime(df["scanned_at"])
    df = df.sort_values(["scanned_at", "scan_id"])

    before = len(df)
    df = df.drop_duplicates(subset="ticket_id", keep="first")
    duplicates_dropped = before - len(df)

    orphan_mask = ~df["order_id"].isin(set(stg_orders["order_id"]))
    orphans_dropped = int(orphan_mask.sum())
    df = df[~orphan_mask].reset_index(drop=True)

    context.add_output_metadata(
        {
            "row_count": dg.MetadataValue.int(len(df)),
            "duplicate_scans_dropped": dg.MetadataValue.int(duplicates_dropped),
            "orphan_scans_dropped": dg.MetadataValue.int(orphans_dropped),
        }
    )
    return df
