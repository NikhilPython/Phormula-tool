from __future__ import annotations
from typing import List, Tuple, Optional
import pandas as pd
import numpy as np
import re

# ---------- generic helpers (safe, reusable) ---------------------------------
def safe_num(x) -> pd.Series:
    """
    Coerce a Series/array/scalar to a numeric Series; non-finite -> 0.0.
    Always returns a pandas Series, never a scalar.
    """
    if isinstance(x, pd.Series):
        out = pd.to_numeric(x, errors="coerce")
    else:
        # wrap scalars/lists/ndarrays into a Series
        out = pd.to_numeric(pd.Series(x), errors="coerce")

    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)



def norm_sku_series(s: pd.Series) -> pd.Series:
    """Normalize SKU text for consistent filtering / grouping."""
    return s.astype(str).str.strip().str.lower()


def sku_mask(df: pd.DataFrame) -> pd.Series:
    """
    Strict SKU presence: drop NaN / "", "0", "none", "null", "nan".
    Used by per-SKU breakdowns when you want to enforce valid SKUs only.
    (For UK totals we still use ALL rows; this mask is for breakdowns.)
    """
    if "sku" not in df.columns or df.empty:
        return pd.Series([False] * len(df), index=df.index)
    norm = norm_sku_series(df["sku"])
    bad = norm.eq("") | norm.eq("0") | norm.eq("none") | norm.eq("null") | norm.eq("nan")
    return ~bad


def agg_by(df: pd.DataFrame, by_col: str, cols: List[str]) -> pd.DataFrame:
    """
    Group by `by_col` and sum `cols` safely. Missing cols become 0.
    Returns a DataFrame with [by_col, *present_cols] (present_cols ⊆ cols).
    """
    if df is None or df.empty or by_col not in df.columns:
        return pd.DataFrame(columns=[by_col] + cols)

    present = [c for c in cols if c in df.columns]
    if not present:
        return pd.DataFrame(columns=[by_col] + cols)

    tmp = df[[by_col] + present].copy()
    for c in present:
        tmp[c] = safe_num(tmp[c])

    out = tmp.groupby(by_col, dropna=True)[present].sum().reset_index()
    # ensure we include all requested columns (fill missing with 0)
    for c in cols:
        if c not in out.columns:
            out[c] = 0.0
    return out[[by_col] + cols]


# ---------- UK-only core formulas --------------------------------------------
# Sales (UK) = product_sales + promotional_rebates + other
def uk_sales(df: pd.DataFrame, *, country: Optional[str] = None,
             want_breakdown: Optional[bool] = None, **kwargs) -> Tuple[float, pd.DataFrame, List[str]]:
    parts = ["product_sales", "promotional_rebates"]

    # Totals use ALL rows (UK scope keeps all rows)
    totals = [safe_num(df.get(c, 0.0)).sum() for c in parts]
    total = float(sum(totals))

    # Per-SKU breakdown uses only valid SKUs (to avoid noise)
    sku_df = df.copy()
    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]
    by = agg_by(sku_df, "sku", parts)

    if by.empty:
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *parts]), parts

    by["__metric__"] = by[parts].sum(axis=1)
    per_sku = by[["sku", "__metric__", *parts]]
    return total, per_sku, parts


# Tax (UK) = product_sales_tax + marketplace_facilitator_tax + shipping_credits_tax
#            + giftwrap_credits_tax + promotional_rebates_tax + other_transaction_fees
def uk_tax(df: pd.DataFrame, *, country: Optional[str] = None,
           want_breakdown: Optional[bool] = None, **kwargs) -> Tuple[float, pd.DataFrame, List[str]]:
    parts = [
        "product_sales_tax",
        "marketplace_facilitator_tax",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates_tax",
        "other_transaction_fees",
    ]

    totals = [safe_num(df.get(c, 0.0)).sum() for c in parts]
    total = float(sum(totals))

    sku_df = df.copy()
    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]
    by = agg_by(sku_df, "sku", parts)

    if by.empty:
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *parts]), parts

    by["__metric__"] = by[parts].sum(axis=1)
    per_sku = by[["sku", "__metric__", *parts]]
    return total, per_sku, parts


# Credits (UK) = postage_credits + gift_wrap_credits
def uk_credits(df: pd.DataFrame, *, country: Optional[str] = None,
               want_breakdown: Optional[bool] = None, **kwargs) -> Tuple[float, pd.DataFrame, List[str]]:
    parts = ["postage_credits", "gift_wrap_credits"]

    totals = [safe_num(df.get(c, 0.0)).sum() for c in parts]
    total = float(sum(totals))

    sku_df = df.copy()
    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]
    by = agg_by(sku_df, "sku", parts)

    if by.empty:
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *parts]), parts

    by["__metric__"] = by[parts].sum(axis=1)
    per_sku = by[["sku", "__metric__", *parts]]
    return total, per_sku, parts



####################################################################
def uk_amazon_fee(df: pd.DataFrame, *, country: str | None = None,
                  want_breakdown: bool | None = None, **kwargs) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    Amazon fee logic identical to process_skuwise_data:

      - Ignore `other_transaction_fees` entirely.
      - Exclude SKUs that are blank, '0', 'none', 'null', 'nan' (via sku_mask).
      - Adjust selling fees for refunds:
          selling_fees_adj = selling_fees_total_per_sku - 2 * refund_selling_fees_per_sku
      - Amazon fee = |FBA fees| + |selling_fees_adj|
    """
    w = df.copy()

    # Coerce numeric columns (safer than ad-hoc string munging)
    for col in ["fba_fees", "selling_fees"]:
        w[col] = safe_num(w.get(col, 0.0))

    # --- Filter out invalid SKUs using the same rule as other helpers ---
    if "sku" in w.columns:
        w["sku"] = w["sku"].astype(str).str.strip()
        w = w.loc[sku_mask(w)]
    else:
        comps = ["fba_abs", "selling_adj_abs"]
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *comps, "selling_raw_abs", "refund_selling_fees"]), comps

    # --- Refund selling fees (case-insensitive 'refund') ---
    refund_selling = pd.DataFrame(columns=["sku", "refund_selling_fees"])
    if "type" in w.columns:
        refund_mask = w["type"].astype(str).str.casefold().eq("refund")
        if refund_mask.any():
            refund_selling = (
                w.loc[refund_mask]
                 .groupby("sku", as_index=False)["selling_fees"]
                 .sum()
                 .rename(columns={"selling_fees": "refund_selling_fees"})
            )

    # --- Aggregate base fees per SKU ---
    by = w.groupby("sku", as_index=False)[["fba_fees", "selling_fees"]].sum()

    # --- Join refund info & adjust ---
    by = by.merge(refund_selling, on="sku", how="left")
    by["refund_selling_fees"] = safe_num(by["refund_selling_fees"])
    by["selling_fees_adj"] = by["selling_fees"] - 2.0 * by["refund_selling_fees"]

    # --- Absolute components and final metric ---
    by["fba_abs"]         = by["fba_fees"].abs()
    by["selling_raw_abs"] = by["selling_fees"].abs()
    by["selling_adj_abs"] = by["selling_fees_adj"].abs()
    by["__metric__"]      = by["fba_abs"] + by["selling_adj_abs"]

    per_sku = by[["sku", "__metric__", "fba_abs", "selling_adj_abs", "selling_raw_abs", "refund_selling_fees"]].copy()
    total = float(per_sku["__metric__"].sum())
    comps = ["fba_abs", "selling_adj_abs"]

    return total, per_sku, comps



# Profit (UK) = sales + credits - taxes - amazon_fee - cost_of_unit_sold

def uk_profit(df: pd.DataFrame, *, country: str | None = None,
              want_breakdown: bool | None = None, **kwargs) -> Tuple[float, pd.DataFrame, List[str]]:
    # Keep calls; helpers already handle SKU filtering consistently
    sales_total,   sales_by,   _ = uk_sales(df, country=country, want_breakdown=want_breakdown, **kwargs)
    tax_total,     tax_by,     _ = uk_tax(df, country=country, want_breakdown=want_breakdown, **kwargs)
    credits_total, credits_by, _ = uk_credits(df, country=country, want_breakdown=want_breakdown, **kwargs)
    fee_total,     fee_by,     _ = uk_amazon_fee(df, country=country, want_breakdown=want_breakdown, **kwargs)

    # --- COST: apply the SAME SKU filter as others to avoid cost-only rows sneaking in ---
    cost_df = df.copy()
    if "sku" in cost_df.columns:
        cost_df = cost_df.loc[sku_mask(cost_df)]
    if "cost_of_unit_sold" in cost_df.columns:
        cost_df["cost_of_unit_sold"] = safe_num(cost_df["cost_of_unit_sold"])
    cost_by = agg_by(cost_df, "sku", ["cost_of_unit_sold"]).rename(columns={"cost_of_unit_sold": "cost"})

    # Merge per-SKU components
    per = (
        (sales_by[["sku", "__metric__"]].rename(columns={"__metric__": "sales"}) if not sales_by.empty
         else pd.DataFrame(columns=["sku", "sales"]))
        .merge(
            credits_by[["sku", "__metric__"]].rename(columns={"__metric__": "credits"})
            if not credits_by.empty else pd.DataFrame(columns=["sku", "credits"]),
            on="sku", how="outer"
        )
        .merge(
            tax_by[["sku", "__metric__"]].rename(columns={"__metric__": "taxes"})
            if not tax_by.empty else pd.DataFrame(columns=["sku", "taxes"]),
            on="sku", how="outer"
        )
        .merge(
            fee_by[["sku", "__metric__"]].rename(columns={"__metric__": "amazon_fee"})
            if not fee_by.empty else pd.DataFrame(columns=["sku", "amazon_fee"]),
            on="sku", how="outer"
        )
        .merge(cost_by, on="sku", how="left")
        .fillna(0.0)
    )

    # Defensive numeric coercion
    for c in ("sales", "credits", "taxes", "amazon_fee", "cost"):
        if c in per.columns:
            per[c] = safe_num(per[c])

    # UK sign rules
    per["__metric__"] = (
          per["sales"].abs()
        + per["credits"].abs()
        - (per["taxes"])
        - per["amazon_fee"]          # already positive metric
        - safe_num(per["cost"]).abs()
    )

    total = float(per["__metric__"].sum())
    comps = ["sales", "credits", "taxes", "amazon_fee", "cost"]
    return total, per[["sku", "__metric__", *comps]], comps


def uk_platform_fee(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    desc_prefixes: tuple[str, ...] = (
        # original
        "FBA Return Fee",
        "FBA Long-Term Storage Fee",
        "FBA storage fee",
        "Subscription",

        # NEW (deduped)
        "FBADisposal",
        "FBAStorageBilling",
        "FBALongTermStorageBilling",
    ),
    desc_col: str = "description",
    amount_col: str = "total",
    explicit_col: str = "platform_fees",
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    Platform fee logic (centralized):
      total = |sum(total) for rows where description startswith one of desc_prefixes|
             + |sum(explicit platform_fees column if present)|

    Per-SKU breakdown:
      - Uses the same SKU filter as other helpers (sku_mask).
      - '__metric__' is the positive sum of both components per SKU.
      - Components exposed: ['from_description_abs', 'from_column_abs'].
    """
    w = df.copy()

    # Description-based component
    from_desc = pd.Series(0.0, index=w.index)
    if desc_col in w.columns and amount_col in w.columns and desc_prefixes:
        desc = w[desc_col].astype(str)
        amt  = safe_num(w[amount_col])

        # startswith accepts a tuple; keeping your case-sensitive behavior
        mask = desc.str.startswith(desc_prefixes, na=False)
        from_desc = amt.where(mask, 0.0).abs()

    # Explicit column
    from_col = safe_num(w.get(explicit_col, 0.0)).abs()

    # Totals use ALL rows
    total = float((from_desc + from_col).sum())

    # Per-SKU breakdown uses only valid SKUs
    if "sku" in w.columns:
        w = w.loc[sku_mask(w)].copy()
    else:
        comps = ["from_description_abs", "from_column_abs"]
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *comps]), comps

    from_desc_sku = from_desc.loc[w.index] if len(from_desc) == len(df) else 0.0
    from_col_sku  = from_col.loc[w.index]  if len(from_col)  == len(df) else 0.0

    per = (
        pd.DataFrame({
            "sku": w["sku"].astype(str).str.strip(),
            "from_description_abs": from_desc_sku,
            "from_column_abs": from_col_sku,
        })
        .groupby("sku", as_index=False)[["from_description_abs", "from_column_abs"]]
        .sum()
    )

    per["__metric__"] = per["from_description_abs"] + per["from_column_abs"]
    comps = ["from_description_abs", "from_column_abs"]
    return total, per[["sku", "__metric__", *comps]], comps



def uk_advertising(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    keywords: tuple[str, ...] = (
        # original
        "Cost of Advertising",
        "Coupon Redemption Fee",
        "Deals",
        "Lightning Deal",

        # NEW – deduplicated
        "ProductAdsPayment",
        "CouponPerformanceEvent",
        "CouponParticipationEvent",
        "SellerDealComplete",
       "VineCharge", "DealParticipationEvent",
       "DealPerformanceEvent",
    ),
    desc_col: str = "description",
    amount_col: str = "total",
    explicit_col: str = "advertising_cost",
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    Advertising logic (centralized):

      total =
        |sum(total) for rows with description containing any keyword (case-insensitive)|
        + |sum(explicit advertising_cost column if present)|

    Per-SKU breakdown mirrors uk_platform_fee().
    """
    import re

    w = df.copy()

    # ---------------------------
    # Description-based component
    # ---------------------------
    from_desc = pd.Series(0.0, index=w.index)

    if desc_col in w.columns and amount_col in w.columns and keywords:
        patt = "|".join(map(re.escape, keywords))
        desc = w[desc_col].astype(str)
        amt  = safe_num(w[amount_col])

        mask = desc.str.contains(patt, case=False, na=False)
        from_desc = amt.where(mask, 0.0).abs()

    # ---------------------------
    # Explicit advertising column
    # ---------------------------
    from_col = safe_num(w.get(explicit_col, 0.0)).abs()

    # ---------------------------
    # TOTAL (all rows)
    # ---------------------------
    total = float((from_desc + from_col).sum())

    # ---------------------------
    # PER-SKU BREAKDOWN
    # ---------------------------
    if "sku" in w.columns:
        w = w.loc[sku_mask(w)].copy()
    else:
        comps = ["from_description_abs", "from_column_abs"]
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *comps]), comps

    from_desc_sku = from_desc.loc[w.index] if len(from_desc) == len(df) else 0.0
    from_col_sku  = from_col.loc[w.index]  if len(from_col)  == len(df) else 0.0

    per = (
        pd.DataFrame({
            "sku": w["sku"].astype(str).str.strip(),
            "from_description_abs": from_desc_sku,
            "from_column_abs": from_col_sku,
        })
        .groupby("sku", as_index=False)[["from_description_abs", "from_column_abs"]]
        .sum()
    )

    per["__metric__"] = per["from_description_abs"] + per["from_column_abs"]

    comps = ["from_description_abs", "from_column_abs"]
    return total, per[["sku", "__metric__", *comps]], comps


# ---------- convenience -------------------------------------------------------
def uk_all(df: pd.DataFrame) -> dict:
    """
    Convenience: compute all UK metrics at once.
    Returns a dict of {name: (total, per_sku_df, components)}.
    """
    return {
        "sales": uk_sales(df),
        "tax": uk_tax(df),
        "credits": uk_credits(df),
        "amazon_fee": uk_amazon_fee(df),
        "profit": uk_profit(df),
    }


__all__ = [
    # helpers
    "safe_num", "norm_sku_series", "sku_mask", "agg_by",
    # uk metrics
    "uk_sales", "uk_tax", "uk_credits", "uk_amazon_fee", "uk_profit",
    "uk_all",
]