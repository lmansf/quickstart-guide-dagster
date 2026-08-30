"""Publishing layer: export the box office dashboard's numbers to reports/boxoffice/.

Writes two sibling files with the same payload:

- ``data.json`` — for programmatic consumers (APIs, notebooks, a Vercel build step).
- ``data.js``   — the same payload as ``window.BOXOFFICE_DATA = {...}``, which the
  report pages load via a plain ``<script>`` tag so they render from live pipeline
  output even when opened straight off the filesystem (``file://`` blocks fetch).

The asset lives in its own ``publishing`` group (not ``reporting``) so the test
suite's throwaway-warehouse materializations of the core groups never touch files
in the repo; ``refresh_all`` includes the group, so a normal refresh republishes.
"""

import json
import os
from pathlib import Path

import dagster as dg
import pandas as pd

from cadence.data_gen import TIER_CAPACITY
from cadence.resources import PROJECT_ROOT

REPORT_DIR_ENV = "BOXOFFICE_REPORT_DIR"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "reports" / "boxoffice"

# The +5% plan's documented assumptions (mirrored in the report pages' prose).
GOAL_PCT = 0.05  # YoY net-revenue growth target
SOFT_NIGHT_TARGET_SELL_THROUGH = 0.65  # lift the worst night to 65% of the house
VIP_PRICE_LIFT = 0.10  # price increase on nights where VIP hits its cap
REALLOC_BUDGET_USD = 2000.0  # marketing spend moved from worst to best channels
REALLOC_INCREMENTALITY = 0.25  # attributed revenue discounted to incremental
REFUND_CONVERT_RATE = 0.15  # share of refund leakage retained as exchange credit


def _round100(value: float) -> int:
    """Lever estimates are directional; round to $100 so they read that way."""
    return int(round(value / 100.0) * 100)


def _spend_weighted_rpd(rows: pd.DataFrame) -> float:
    spend = float(rows["spend"].sum())
    return float(rows["net"].sum()) / spend if spend > 0 else 0.0


def _attribution(orders: pd.DataFrame, campaigns: pd.DataFrame, code_col: str) -> pd.DataFrame:
    """Per-campaign orders/tickets/net for one promo-code column (as-is or cleaned)."""
    joined = orders[orders[code_col].notna()].merge(
        campaigns[["campaign_id", "promo_code"]].dropna(subset=["promo_code"]),
        left_on=code_col,
        right_on="promo_code",
        how="inner",
    )
    return joined.groupby("campaign_id").agg(
        orders=("order_id", "count"), tickets=("qty", "sum"), net=("net_revenue", "sum")
    )


@dg.asset(
    group_name="publishing",
    description="Export the box office dashboard data to reports/boxoffice/ (JSON + JS).",
)
def boxoffice_dashboard_data(
    context: dg.AssetExecutionContext,
    stg_orders: pd.DataFrame,
    stg_events: pd.DataFrame,
    stg_campaigns: pd.DataFrame,
    attendance_by_event: pd.DataFrame,
) -> None:
    out_dir = Path(os.environ.get(REPORT_DIR_ENV, DEFAULT_REPORT_DIR))

    orders = stg_orders.copy()
    orders["refunded_qty"] = orders["qty"].where(orders["status"] == "refunded", 0)
    # Cleaned promo codes: the canonical form regardless of whether the README
    # Step 5 staging fix has been applied yet (idempotent if it has).
    orders["promo_code_clean"] = orders["promo_code"].str.strip().str.upper()

    # ---- season totals ------------------------------------------------------
    net = float(orders["net_revenue"].sum())
    gross = float(orders["gross_revenue"].sum())
    tickets_sold = int(orders["qty"].sum())
    refunded_tickets = int(orders["refunded_qty"].sum())
    tickets_kept = tickets_sold - refunded_tickets
    refunded_rev = gross - net
    seats_total = int(stg_events["capacity"].sum())
    scanned = int(attendance_by_event["tickets_scanned"].sum())
    attendance_kept = int(attendance_by_event["tickets_sold"].sum())
    show_up_rate = scanned / attendance_kept if attendance_kept else 0.0
    marketing_spend = float(stg_campaigns["spend_usd"].sum())

    # ---- per-event ----------------------------------------------------------
    attendance = attendance_by_event.set_index("event_id")
    events = []
    for _, ev in stg_events.sort_values("event_id").iterrows():
        ev_orders = orders[orders["event_id"] == ev["event_id"]]
        tiers = {}
        for tier, cap in TIER_CAPACITY.items():
            t = ev_orders[ev_orders["tier"] == tier]
            tiers[tier] = {
                "sold": int(t["qty"].sum()),
                "cap": cap,
                "net": float(t["net_revenue"].sum()),
            }
        att = None
        if ev["event_id"] in attendance.index:
            a = attendance.loc[ev["event_id"]]
            att = {
                "kept": int(a["tickets_sold"]),
                "scanned": int(a["tickets_scanned"]),
                "show_up": float(a["show_up_rate"]),
                "no_shows": int(a["no_shows"]),
            }
        sold = int(ev_orders["qty"].sum())
        events.append(
            {
                "id": ev["event_id"],
                "name": ev["event_name"],
                "genre": ev["genre"],
                "date": ev["event_date"].strftime("%Y-%m-%d"),
                "capacity": int(ev["capacity"]),
                "sold": sold,
                "sell_through": sold / int(ev["capacity"]),
                "gross": float(ev_orders["gross_revenue"].sum()),
                "net": float(ev_orders["net_revenue"].sum()),
                "refunded_rev": float(
                    ev_orders["gross_revenue"].sum() - ev_orders["net_revenue"].sum()
                ),
                "refunded_tickets": int(ev_orders["refunded_qty"].sum()),
                "tiers": tiers,
                "attendance": att,
            }
        )

    # Which nights actually have gate scans: nights 1-7 ship in data/scans/, and the
    # eighth arrives when the sensor demo delivers it (guide chapter 04).
    scanned_events = [e for e in events if e["attendance"] is not None]

    # ---- daily pacing -------------------------------------------------------
    daily = [
        {"date": str(d), "net": float(g["net_revenue"].sum()), "tickets": int(g["qty"].sum())}
        for d, g in orders.groupby("order_date")
    ]
    daily.sort(key=lambda r: r["date"])

    # ---- campaigns: clean attribution, with the as-recorded view alongside ---
    dirty = _attribution(orders, stg_campaigns, "promo_code")
    clean = _attribution(orders, stg_campaigns, "promo_code_clean")
    campaigns = []
    for _, c in stg_campaigns.iterrows():
        cid = c["campaign_id"]
        cl = clean.loc[cid] if cid in clean.index else None
        dr = dirty.loc[cid] if cid in dirty.index else None
        spend = float(c["spend_usd"])
        c_net = float(cl["net"]) if cl is not None else 0.0
        campaigns.append(
            {
                "id": cid,
                "name": c["name"],
                "channel": c["channel"],
                "has_code": bool(pd.notna(c["promo_code"])),
                "spend": spend,
                "orders": int(cl["orders"]) if cl is not None else 0,
                "tickets": int(cl["tickets"]) if cl is not None else 0,
                "net": c_net,
                "rpd": c_net / spend if spend > 0 else 0.0,
                "net_dirty": float(dr["net"]) if dr is not None else 0.0,
            }
        )
    campaigns.sort(key=lambda c: c["rpd"], reverse=True)

    known_clean = set(stg_campaigns["promo_code"].dropna().str.strip().str.upper())
    has_code = orders["promo_code"].notna()
    unattributed_dirty = orders[
        has_code & ~orders["promo_code"].isin(set(stg_campaigns["promo_code"].dropna()))
    ]
    still_unattributed = orders[has_code & ~orders["promo_code_clean"].isin(known_clean)]
    organic = orders[~has_code]
    attributed_net_clean = float(sum(c["net"] for c in campaigns))

    # ---- the four levers ----------------------------------------------------
    worst_ev = min(events, key=lambda e: e["sell_through"])
    avg_net_per_ticket = worst_ev["net"] / worst_ev["sold"] if worst_ev["sold"] else 0.0
    soft_add = max(0, int(worst_ev["capacity"] * SOFT_NIGHT_TARGET_SELL_THROUGH) - worst_ev["sold"])
    lever_soft = _round100(soft_add * avg_net_per_ticket)

    vip_sellouts = [e for e in events if e["tiers"]["VIP"]["sold"] >= e["tiers"]["VIP"]["cap"]]
    vip_net_on_sellouts = sum(e["tiers"]["VIP"]["net"] for e in vip_sellouts)
    lever_vip = _round100(VIP_PRICE_LIFT * vip_net_on_sellouts)

    tracked = pd.DataFrame([c for c in campaigns if c["has_code"] and c["spend"] > 0])
    median_rpd = float(tracked["rpd"].median())
    target_rpd = _spend_weighted_rpd(tracked[tracked["rpd"] > median_rpd])
    source_rpd = _spend_weighted_rpd(tracked.nsmallest(2, "rpd"))
    lever_realloc = _round100(
        REALLOC_BUDGET_USD * max(0.0, target_rpd - source_rpd) * REALLOC_INCREMENTALITY
    )

    lever_refunds = _round100(REFUND_CONVERT_RATE * refunded_rev)

    goal_target = net * (1 + GOAL_PCT)
    goal_gap = goal_target - net
    levers = [
        {
            "key": "soft_night",
            "label": "Rescue the soft night",
            "amount": lever_soft,
            "detail": {
                "event": worst_ev["name"],
                "sell_through": worst_ev["sell_through"],
                "target_sell_through": SOFT_NIGHT_TARGET_SELL_THROUGH,
                "add_tickets": soft_add,
                "avg_net_per_ticket": avg_net_per_ticket,
                "unsold_seats": worst_ev["capacity"] - worst_ev["sold"],
            },
        },
        {
            "key": "vip",
            "label": "Reprice / expand VIP",
            "amount": lever_vip,
            "detail": {
                "sellout_nights": len(vip_sellouts),
                "nights": len(events),
                "vip_net_on_sellouts": vip_net_on_sellouts,
                "price_lift": VIP_PRICE_LIFT,
            },
        },
        {
            "key": "realloc",
            "label": "Reallocate ad spend",
            "amount": lever_realloc,
            "detail": {
                "budget_moved": REALLOC_BUDGET_USD,
                "source_rpd": source_rpd,
                "target_rpd": target_rpd,
                "incrementality": REALLOC_INCREMENTALITY,
            },
        },
        {
            "key": "refunds",
            "label": "Refunds → exchange credits",
            "amount": lever_refunds,
            "detail": {"refunded_rev": refunded_rev, "convert_rate": REFUND_CONVERT_RATE},
        },
    ]
    levers_total = sum(lv["amount"] for lv in levers)

    # ---- derived scalars the pages quote in prose ---------------------------
    best_ev = max(events, key=lambda e: e["net"])
    by_dirty_net = max(campaigns, key=lambda c: c["net_dirty"])
    best_campaign = campaigns[0]
    worst_tracked = min(
        (c for c in campaigns if c["has_code"] and c["spend"] > 0), key=lambda c: c["rpd"]
    )
    codeless = [c for c in campaigns if not c["has_code"]]
    att_rows = [e for e in events if e["attendance"]]
    best_att = max(att_rows, key=lambda e: e["attendance"]["show_up"])
    worst_att = min(att_rows, key=lambda e: e["attendance"]["show_up"])
    first4 = sum(d["net"] for d in daily[:4])
    derived = {
        "best_event": {"name": best_ev["name"], "net": best_ev["net"]},
        "worst_event": {
            "name": worst_ev["name"],
            "net": worst_ev["net"],
            "sold": worst_ev["sold"],
            "sell_through": worst_ev["sell_through"],
            "unsold_seats": worst_ev["capacity"] - worst_ev["sold"],
        },
        "best_worst_ratio": best_ev["net"] / worst_ev["net"] if worst_ev["net"] else 0.0,
        "vip_sellout_nights": len(vip_sellouts),
        "balcony_sellout_nights": sum(
            1 for e in events if e["tiers"]["Balcony"]["sold"] >= e["tiers"]["Balcony"]["cap"]
        ),
        "tier_totals": {
            tier: {
                "sold": sum(e["tiers"][tier]["sold"] for e in events),
                "net": sum(e["tiers"][tier]["net"] for e in events),
            }
            for tier in TIER_CAPACITY
        },
        "best_campaign": best_campaign,
        "worst_tracked_campaign": worst_tracked,
        "untracked_campaign": codeless[0] if codeless else None,
        "rpd_spread": best_campaign["rpd"] / worst_tracked["rpd"] if worst_tracked["rpd"] else 0.0,
        "best_by_revenue_dirty": by_dirty_net["name"],
        "best_by_revenue_clean": max(campaigns, key=lambda c: c["net"])["name"],
        "launch": {
            "day_net": daily[0]["net"] if daily else 0.0,
            "first4_net": first4,
            "first4_share": first4 / net if net else 0.0,
        },
        "attendance": {
            "avg_rate": show_up_rate,
            "no_shows": int(attendance_by_event["no_shows"].sum()),
            "best": {"name": best_att["name"], "rate": best_att["attendance"]["show_up"]},
            "worst": {
                "name": worst_att["name"],
                "rate": worst_att["attendance"]["show_up"],
                "no_shows": worst_att["attendance"]["no_shows"],
                "delta_pts": (show_up_rate - worst_att["attendance"]["show_up"]) * 100,
            },
        },
    }

    # NOTE: this payload is a pure function of the upstream assets — no wall-clock
    # timestamp, no randomness. Re-exporting unchanged data rewrites byte-identical
    # files, so `git status` stays clean and the committed report always matches
    # what a fresh clone materializes. See docs/guide/06-publishing.md.
    payload = {
        "season": {
            "label": "Summer season, July 1–8 2025",
            "first_show": events[0]["date"] if events else None,
            "last_show": events[-1]["date"] if events else None,
            "last_order_date": daily[-1]["date"] if daily else None,
            "scans_through": scanned_events[-1]["date"] if scanned_events else None,
            "nights_scanned": len(scanned_events),
        },
        "goal": {"pct": GOAL_PCT, "target_net": goal_target, "gap": goal_gap},
        "totals": {
            "net": net,
            "gross": gross,
            "tickets_sold": tickets_sold,
            "tickets_kept": tickets_kept,
            "refunded_tickets": refunded_tickets,
            "refunded_rev": refunded_rev,
            "refund_share_of_gross": refunded_rev / gross if gross else 0.0,
            "seats_total": seats_total,
            "seat_sell_through": tickets_sold / seats_total if seats_total else 0.0,
            "net_per_kept_ticket": net / tickets_kept if tickets_kept else 0.0,
            "show_up_rate": show_up_rate,
            "scanned": scanned,
            "marketing_spend": marketing_spend,
            "attributed_net_clean": attributed_net_clean,
            "rpd_blended": attributed_net_clean / marketing_spend if marketing_spend else 0.0,
        },
        "attribution": {
            "unattributed_dirty_net": float(unattributed_dirty["net_revenue"].sum()),
            "unattributed_dirty_orders": int(len(unattributed_dirty)),
            "still_unattributed_net": float(still_unattributed["net_revenue"].sum()),
            "organic_net": float(organic["net_revenue"].sum()),
            "organic_orders": int(len(organic)),
            "organic_tickets": int(organic["qty"].sum()),
        },
        "derived": derived,
        "events": events,
        "daily": daily,
        "campaigns": campaigns,
        "levers": levers,
        "levers_total": levers_total,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    json_text = json.dumps(payload, indent=2, ensure_ascii=False)
    (out_dir / "data.json").write_text(json_text + "\n", encoding="utf-8")
    (out_dir / "data.js").write_text(
        "// Generated by the boxoffice_dashboard_data asset — do not edit by hand.\n"
        f"window.BOXOFFICE_DATA = {json_text};\n",
        encoding="utf-8",
    )

    context.add_output_metadata(
        {
            "out_dir": dg.MetadataValue.path(str(out_dir)),
            "net_revenue": dg.MetadataValue.float(net),
            "target_net": dg.MetadataValue.float(goal_target),
            "levers_total": dg.MetadataValue.int(levers_total),
            "events": dg.MetadataValue.int(len(events)),
            "campaigns": dg.MetadataValue.int(len(campaigns)),
            "unattributed_dirty_net": dg.MetadataValue.float(
                float(unattributed_dirty["net_revenue"].sum())
            ),
        }
    )
