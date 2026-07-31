from __future__ import annotations
from typing import List, Tuple, Optional
import pandas as pd
import numpy as np

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



def series_like(df: pd.DataFrame, value=0.0, dtype: str = "float64") -> pd.Series:
    """Return a Series aligned to df.index. Useful for missing optional columns."""
    return pd.Series(value, index=df.index, dtype=dtype)


def text_series(df: pd.DataFrame, col: str, default: str = "") -> pd.Series:
    """Safe text column accessor aligned to df.index."""
    if col in df.columns:
        return df[col].fillna(default).astype(str)
    return pd.Series(default, index=df.index, dtype="object")


def num_series(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    """Safe numeric column accessor aligned to df.index."""
    if col in df.columns:
        return safe_num(df[col])
    return pd.Series(default, index=df.index, dtype="float64")


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
# Sales (UK) = product_sales + promotional_rebates + refund_sales
def uk_sales(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:

    gross_parts = [
        "product_sales",
        "product_sales_tax",
        "postage_credits",
        "gift_wrap_credits",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates",
        "promotional_rebates_tax",
    ]

    tax_credit_parts = [
        "product_sales_tax",
        "postage_credits",
        "gift_wrap_credits",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates_tax",
    ]

    # Refund rows: product_sales < 0
    refund_mask = safe_num(df.get("product_sales", 0.0)) < 0

    df_base = df.loc[~refund_mask].copy()
    df_refund = df.loc[refund_mask].copy()

    # ---------- TOTAL ----------
    gross_total = float(
        sum(safe_num(df_base.get(c, 0.0)).sum() for c in gross_parts)
    )

    refund_total = float(
        safe_num(df_refund.get("product_sales", 0.0)).abs().sum()
    )

    taxes_and_credits_total = float(
        safe_num(df_base.get("product_sales_tax", 0.0)).sum()
        + safe_num(df_base.get("postage_credits", 0.0)).sum()
        + safe_num(df_base.get("gift_wrap_credits", 0.0)).sum()
        + safe_num(df_base.get("shipping_credits_tax", 0.0)).sum()
        + safe_num(df_base.get("giftwrap_credits_tax", 0.0)).sum()
        - safe_num(df_base.get("promotional_rebates_tax", 0.0)).abs().sum()
    )

    total = gross_total - refund_total - taxes_and_credits_total

    # ---------- PER SKU ----------
    sku_df = df_base.copy()
    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]

    by = agg_by(sku_df, "sku", gross_parts)

    if by.empty:
        return (
            0.0,
            pd.DataFrame(columns=["sku", "__metric__", "gross_sales", "refund_sales", "taxes_and_credits"]),
            ["gross_sales", "refund_sales", "taxes_and_credits"],
        )

    by["gross_sales"] = (
        safe_num(by["product_sales"])
        + safe_num(by["product_sales_tax"])
        + safe_num(by["postage_credits"])
        + safe_num(by["gift_wrap_credits"])
        + safe_num(by["shipping_credits_tax"])
        + safe_num(by["giftwrap_credits_tax"])
        + safe_num(by["promotional_rebates"])
        + safe_num(by["promotional_rebates_tax"])
    )

    by["taxes_and_credits"] = (
        safe_num(by["product_sales_tax"])
        + safe_num(by["postage_credits"])
        + safe_num(by["gift_wrap_credits"])
        + safe_num(by["shipping_credits_tax"])
        + safe_num(by["giftwrap_credits_tax"])
        - safe_num(by["promotional_rebates_tax"]).abs()
    )

    refund_by = pd.DataFrame(columns=["sku", "refund_sales"])
    if not df_refund.empty and "sku" in df_refund.columns:
        refund_by = (
            df_refund
            .groupby("sku", as_index=False)["product_sales"]
            .sum()
            .rename(columns={"product_sales": "refund_sales"})
        )

    by = by.merge(refund_by, on="sku", how="left")
    by["refund_sales"] = safe_num(by.get("refund_sales", 0.0)).abs()

    by["__metric__"] = (
        by["gross_sales"]
        - by["refund_sales"]
        - by["taxes_and_credits"]
    )

    per_sku = by[
        ["sku", "__metric__", "gross_sales", "refund_sales", "taxes_and_credits"]
    ]

    return total, per_sku, ["gross_sales", "refund_sales", "taxes_and_credits"]


def uk_tax(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    UK Digital Tax (NON-REFUND ONLY)

    Includes:
      - product_sales_tax
      - shipping_credits_tax
      - giftwrap_credits_tax
      - promotional_rebates_tax
      - marketplace_facilitator_tax

    Excludes:
      - ALL refund rows (hard filtered)

    Notes:
      - No abs()
      - Excel-style signs preserved
      - Safe even if caller passes dirty DF
    """

    parts = [
        "product_sales_tax",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates_tax",
        "marketplace_facilitator_tax",
    ]

    # --------------------------------------------------
    # HARD FILTER: REMOVE REFUND ROWS (NON-NEGOTIABLE)
    # --------------------------------------------------
    if "type" in df.columns:
        type_str = (
            df["type"]
            .astype(str)
            .str.strip()
            .str.casefold()
        )
        df_base = df.loc[~type_str.str.contains("refund", na=False)].copy()
    else:
        df_base = df.copy()

    # --------------------------------------------------
    # ENSURE NUMERIC COLUMNS EXIST
    # --------------------------------------------------
    for c in parts:
        if c not in df_base.columns:
            df_base[c] = 0.0
        df_base[c] = pd.to_numeric(df_base[c], errors="coerce").fillna(0.0)

    # --------------------------------------------------
    # TOTAL (Excel-style, NO abs)
    # --------------------------------------------------
    total = float(sum(df_base[c].sum() for c in parts))

    # --------------------------------------------------
    # PER-SKU BREAKDOWN
    # --------------------------------------------------
    if "sku" not in df_base.columns:
        return total, pd.DataFrame(columns=["sku", "__metric__", *parts]), parts

    df_base["sku"] = df_base["sku"].astype(str).str.strip()
    df_base = df_base.loc[sku_mask(df_base)].copy()

    by = (
        df_base
        .groupby("sku", as_index=False)[parts]
        .sum()
    )

    if by.empty:
        return total, pd.DataFrame(columns=["sku", "__metric__", *parts]), parts

    by["__metric__"] = by[parts].sum(axis=1)

    return total, by[["sku", "__metric__", *parts]], parts


def uk_credits(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    UK Credits (NET, Excel-matching)

    Formula:
      net_credits = non_refund_credits - abs(refund_credits)

    Columns used:
      - postage_credits
      - gift_wrap_credits

    Behaviour:
      - Refund rows are included ONLY to subtract their magnitude
      - Final output matches Excel total exactly (e.g. 363.97)
    """

    parts = [
        "postage_credits",
        "gift_wrap_credits",
    ]

    # ------------------------------------------------------------------
    # Normalize type column
    # ------------------------------------------------------------------
    type_str = text_series(df, "type").str.strip().str.casefold()

    refund_mask = type_str.str.contains("refund", na=False)

    # ------------------------------------------------------------------
    # NON-REFUND credits (as-is)
    # ------------------------------------------------------------------
    df_non_refund = df.loc[~refund_mask].copy()

    non_refund_total = float(
        sum(
            safe_num(df_non_refund.get(c, 0.0)).sum()
            for c in parts
        )
    )

    # ------------------------------------------------------------------
    # REFUND credits (negative → subtract ABS)
    # ------------------------------------------------------------------
    df_refund = df.loc[refund_mask].copy()

    refund_total = float(
        sum(
            safe_num(df_refund.get(c, 0.0)).sum()
            for c in parts
        )
    )

    # refund_total is negative → subtract its magnitude
    total = non_refund_total - abs(refund_total)

    # ------------------------------------------------------------------
    # PER-SKU BREAKDOWN (NET)
    # ------------------------------------------------------------------
    sku_col = "sku"
    if sku_col not in df.columns:
        return total, pd.DataFrame(columns=["sku", "__metric__", *parts]), parts

    for c in parts:
        if c not in df_non_refund.columns:
            df_non_refund[c] = 0.0
        if c not in df_refund.columns:
            df_refund[c] = 0.0
        df_non_refund[c] = safe_num(df_non_refund[c])
        df_refund[c] = safe_num(df_refund[c])

    df_non_refund = df_non_refund.loc[sku_mask(df_non_refund)].copy()
    df_refund = df_refund.loc[sku_mask(df_refund)].copy()

    # Non-refund SKU credits
    non_refund_by = (
        df_non_refund
        .groupby("sku", as_index=False)[parts]
        .sum()
        if not df_non_refund.empty else pd.DataFrame(columns=["sku", *parts])
    )

    # Refund SKU credits
    refund_by = (
        df_refund
        .groupby("sku", as_index=False)[parts]
        .sum()
        if not df_refund.empty else pd.DataFrame(columns=["sku", *parts])
    )

    # Merge & compute net per SKU
    by = non_refund_by.merge(
        refund_by,
        on="sku",
        how="outer",
        suffixes=("_non_refund", "_refund")
    ).fillna(0.0)

    by["__metric__"] = (
        by["postage_credits_non_refund"]
        + by["gift_wrap_credits_non_refund"]
        - (
            by["postage_credits_refund"].abs()
            + by["gift_wrap_credits_refund"].abs()
        )
    )

    # Optional: expose clean component columns
    by["postage_credits"] = (
        by["postage_credits_non_refund"]
        - by["postage_credits_refund"].abs()
    )
    by["gift_wrap_credits"] = (
        by["gift_wrap_credits_non_refund"]
        - by["gift_wrap_credits_refund"].abs()
    )

    return total, by[["sku", "__metric__", *parts]], parts


def uk_gross_sales(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:

    parts = [
        "product_sales",
        "product_sales_tax",
        "postage_credits",
        "gift_wrap_credits",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates",
        "promotional_rebates_tax",
    ]

    # ---- TOTAL (all rows) ----
    total = float(sum(safe_num(df.get(c, 0.0)).sum() for c in parts))

    # ---- PER SKU ----
    sku_df = df.copy()
    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]

    by = agg_by(sku_df, "sku", parts)

    if by.empty:
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *parts]), parts

    by["__metric__"] = by[parts].sum(axis=1)

    return total, by[["sku", "__metric__", *parts]], parts


def uk_tax_and_credits(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:

    parts = [
        "product_sales_tax",
        "postage_credits",
        "gift_wrap_credits",
        "giftwrap_credits_tax",
        "shipping_credits_tax",
        "promotional_rebates_tax",
    ]

    # -----------------------------
    # TOTAL (ALL ROWS, UK SCOPE)
    # -----------------------------
    total = float(
        sum(safe_num(df.get(c, 0.0)).sum() for c in parts)
    )

    # -----------------------------
    # PER-SKU BREAKDOWN
    # -----------------------------
    sku_df = df.copy()
    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]

    by = agg_by(sku_df, "sku", parts)

    if by.empty:
        return (
            0.0,
            pd.DataFrame(columns=["sku", "__metric__", *parts]),
            parts,
        )

    by["__metric__"] = by[parts].sum(axis=1)

    per_sku = by[["sku", "__metric__", *parts]]

    return total, per_sku, parts


def uk_cogs(
    df: pd.DataFrame,
    *,
    country: Optional[str] = None,
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:

    # Prefer dashboard-aligned computation when possible
    if "price_in_gbp" in df.columns and "total_quantity" in df.columns:
        w = df.copy()
        if "sku" in w.columns:
            w["sku"] = w["sku"].astype(str).str.strip()
            w = w.loc[sku_mask(w)]
        else:
            return 0.0, pd.DataFrame(columns=["sku", "__metric__", "cogs"]), ["cogs"]

        w["price_in_gbp"] = safe_num(w["price_in_gbp"])
        w["total_quantity"] = safe_num(w["total_quantity"])

        by = w.groupby("sku", as_index=False)[["price_in_gbp", "total_quantity"]].mean()
        by["cogs"] = (by["price_in_gbp"] * by["total_quantity"]).abs()
        by["__metric__"] = by["cogs"]

        per_sku = by[["sku", "__metric__", "cogs"]]
        total = float(per_sku["__metric__"].sum())
        return total, per_sku, ["cogs"]

    # Fallback to old behavior
    sku_df = df.copy()
    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]

    if "cost_of_unit_sold" not in sku_df.columns:
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", "cogs"]), ["cogs"]

    sku_df["cost_of_unit_sold"] = safe_num(sku_df["cost_of_unit_sold"])

    by = agg_by(sku_df, "sku", ["cost_of_unit_sold"])
    if by.empty:
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", "cogs"]), ["cogs"]

    by["cogs"] = by["cost_of_unit_sold"].abs()
    by["__metric__"] = by["cogs"]
    per_sku = by[["sku", "__metric__", "cogs"]]
    total = float(per_sku["__metric__"].sum())
    return total, per_sku, ["cogs"]


# ####################################################################
# def uk_amazon_fee(df: pd.DataFrame, *, country: str | None = None,
#                   want_breakdown: bool | None = None, **kwargs) -> Tuple[float, pd.DataFrame, List[str]]:
#     """
#     Amazon fee logic identical to process_skuwise_data:

#       - Ignore `other_transaction_fees` entirely.
#       - Exclude SKUs that are blank, '0', 'none', 'null', 'nan' (via sku_mask).
#       - Adjust selling fees for refunds:
#           selling_fees_adj = selling_fees_total_per_sku - 2 * refund_selling_fees_per_sku
#       - Amazon fee = |FBA fees| + |selling_fees_adj|
#     """
#     w = df.copy()

#     # Coerce numeric columns (safer than ad-hoc string munging)
#     for col in ["fba_fees", "selling_fees"]:
#         w[col] = safe_num(w.get(col, 0.0))

#     # --- Filter out invalid SKUs using the same rule as other helpers ---
#     if "sku" in w.columns:
#         w["sku"] = w["sku"].astype(str).str.strip()
#         w = w.loc[sku_mask(w)]
#     else:
#         comps = ["fba_abs", "selling_adj_abs"]
#         return 0.0, pd.DataFrame(columns=["sku", "__metric__", *comps, "selling_raw_abs", "refund_selling_fees"]), comps

#     # --- Refund selling fees (case-insensitive 'refund') ---
#     refund_selling = pd.DataFrame(columns=["sku", "refund_selling_fees"])
#     if "type" in w.columns:
#         refund_mask = w["type"].astype(str).str.casefold().eq("refund")
#         if refund_mask.any():
#             refund_selling = (
#                 w.loc[refund_mask]
#                  .groupby("sku", as_index=False)["selling_fees"]
#                  .sum()
#                  .rename(columns={"selling_fees": "refund_selling_fees"})
#             )

#     # --- Aggregate base fees per SKU ---
#     by = w.groupby("sku", as_index=False)[["fba_fees", "selling_fees"]].sum()

#     # --- Join refund info & adjust ---
#     by = by.merge(refund_selling, on="sku", how="left")
#     by["refund_selling_fees"] = safe_num(by["refund_selling_fees"])
#     by["selling_fees_adj"] = by["selling_fees"] - 2.0 * by["refund_selling_fees"]

#     # --- Absolute components and final metric ---
#     by["fba_abs"]         = by["fba_fees"].abs()
#     by["selling_raw_abs"] = by["selling_fees"].abs()
#     by["selling_adj_abs"] = by["selling_fees_adj"].abs()
#     by["__metric__"]      = by["fba_abs"] + by["selling_adj_abs"]

#     per_sku = by[["sku", "__metric__", "fba_abs", "selling_adj_abs", "selling_raw_abs", "refund_selling_fees"]].copy()
#     total = float(per_sku["__metric__"].sum())
#     comps = ["fba_abs", "selling_adj_abs"]

#     return total, per_sku, comps

def uk_amazon_fee(
    df: pd.DataFrame,
    *,
    country: str | None = None,
    want_breakdown: bool | None = None,
    **kwargs,
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    Standard UK Amazon fee calculation.

    Rules:
      - Ignore other_transaction_fees.
      - Exclude invalid SKUs.
      - Exclude lost/damaged inventory rows.
      - Exclude DebtRecovery rows.
      - Keep Refund rows for the selling-fee refund adjustment.
      - Amazon fee:
            abs(fba_fees)
            + abs(selling_fees - 2 * refund_selling_fees)
    """

    comps = ["fba_abs", "selling_adj_abs"]

    empty_result = pd.DataFrame(
        columns=[
            "sku",
            "__metric__",
            "fba_abs",
            "selling_adj_abs",
            "selling_raw_abs",
            "refund_selling_fees",
        ]
    )

    if df is None or df.empty:
        return 0.0, empty_result, comps

    w = df.copy()

    # Required SKU column
    if "sku" not in w.columns:
        return 0.0, empty_result, comps

    # Required numeric columns
    for col in ["fba_fees", "selling_fees"]:
        if col not in w.columns:
            w[col] = 0.0

        w[col] = safe_num(w[col])

    # Required text columns
    if "type" not in w.columns:
        w["type"] = ""

    if "description" not in w.columns:
        w["description"] = ""

    w["sku"] = (
        w["sku"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    w["type"] = (
        w["type"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    w["description"] = (
        w["description"]
        .fillna("")
        .astype(str)
        .str.strip()
    )

    # Remove blank/0/none/null/nan SKU rows
    w = w.loc[sku_mask(w)].copy()

    if w.empty:
        return 0.0, empty_result, comps

    lost_descriptions = {
        "REVERSAL_REIMBURSEMENT",
        "WAREHOUSE_LOST",
        "WAREHOUSE_DAMAGE",
        "MISSING_FROM_INBOUND",
        "MISSING_FROM_INBOUND_CLAWBACK",
        "COMPENSATED_CLAWBACK",
        "FREE_REPLACEMENT_REFUND_ITEMS",
    }

    description_key = w["description"].str.casefold()

    lost_description_keys = {
        value.casefold()
        for value in lost_descriptions
    }

    is_lost = description_key.isin(
        lost_description_keys
    )

    is_debt_recovery = (
        w["type"]
        .str.casefold()
        .eq("debtrecovery")
    )

    # Important: do not remove Refund rows here.
    # Refund selling fees are required for the adjustment.
    w = w.loc[
        ~is_lost
        & ~is_debt_recovery
    ].copy()

    if w.empty:
        return 0.0, empty_result, comps

    # Use contains instead of exact equality so values such as
    # "Refund", "RefundEvent", etc. are handled consistently.
    refund_mask = (
        w["type"]
        .str.contains(
            "refund",
            case=False,
            na=False,
            regex=False,
        )
    )

    refund_selling = (
        w.loc[refund_mask]
        .groupby(
            "sku",
            as_index=False,
        )["selling_fees"]
        .sum()
        .rename(
            columns={
                "selling_fees": "refund_selling_fees"
            }
        )
    )

    by = (
        w.groupby(
            "sku",
            as_index=False,
        )[["fba_fees", "selling_fees"]]
        .sum()
    )

    by = by.merge(
        refund_selling,
        on="sku",
        how="left",
    )

    by["refund_selling_fees"] = safe_num(
        by.get("refund_selling_fees", 0.0)
    )

    by["selling_fees_adj"] = (
        by["selling_fees"]
        - 2.0 * by["refund_selling_fees"]
    )

    by["fba_abs"] = by["fba_fees"].abs()
    by["selling_raw_abs"] = by["selling_fees"].abs()
    by["selling_adj_abs"] = by["selling_fees_adj"].abs()

    by["__metric__"] = (
        by["fba_abs"]
        + by["selling_adj_abs"]
    )

    per_sku = by[
        [
            "sku",
            "__metric__",
            "fba_abs",
            "selling_adj_abs",
            "selling_raw_abs",
            "refund_selling_fees",
        ]
    ].copy()

    total = float(
        per_sku["__metric__"].sum()
    )

    return total, per_sku, comps




# # Profit (UK) = sales + credits - taxes - amazon_fee - cost_of_unit_sold
# def uk_profit(
#     df: pd.DataFrame,
#     *,
#     country: str | None = None,
#     want_breakdown: bool | None = None,
#     debug: bool = True,
#     **kwargs
# ) -> tuple[float, pd.DataFrame, list[str]]:
#     """
#     FINAL UK PROFIT (Dashboard & Excel Aligned)

#     Formula (SOURCE OF TRUTH):
#         profit = sales
#                - cogs
#                - amazon_fee
#                - net_taxes
#                + net_credits
#     """

#     # -----------------------------
#     # BASE COMPONENTS
#     # -----------------------------
#     sales_total,  sales_by,  _ = uk_sales(df, country=country)
#     cogs_total,   cogs_by,   _ = uk_cogs(df, country=country)
#     fee_total,    fee_by,    _ = uk_amazon_fee(df, country=country)
#     tax_total,    tax_by,    _ = uk_tax(df, country=country)
#     credit_total, credit_by, _ = uk_credits(df, country=country)

#     # -----------------------------
#     # MERGE PER-SKU
#     # -----------------------------
#     per = (
#         sales_by[["sku", "__metric__"]]
#         .rename(columns={"__metric__": "sales"})
#         .merge(
#             cogs_by[["sku", "__metric__"]].rename(columns={"__metric__": "cogs"}),
#             on="sku", how="outer"
#         )
#         .merge(
#             fee_by[["sku", "__metric__"]].rename(columns={"__metric__": "amazon_fee"}),
#             on="sku", how="outer"
#         )
#         .merge(
#             tax_by[["sku", "__metric__"]].rename(columns={"__metric__": "net_taxes"}),
#             on="sku", how="outer"
#         )
#         .merge(
#             credit_by[["sku", "__metric__"]].rename(columns={"__metric__": "net_credits"}),
#             on="sku", how="outer"
#         )
#         .fillna(0.0)
#     )

#     # -----------------------------
#     # NUMERIC SAFETY
#     # -----------------------------
#     for c in ("sales", "cogs", "amazon_fee", "net_taxes", "net_credits"):
#         per[c] = safe_num(per[c])

#     # -----------------------------
#     # PROFIT (PER SKU)
#     # -----------------------------
#     per["__metric__"] = (
#         per["sales"].abs()
#         - per["cogs"]
#         - per["amazon_fee"]
#         - per["net_taxes"]
#         + per["net_credits"]
#     )

#     total = float(per["__metric__"].sum())

    
#     comps = [
#         "sales",
#         "cogs",
#         "amazon_fee",
#         "net_taxes",
#         "net_credits",
#     ]

#     return total, per[["sku", "__metric__", *comps]], comps

# Profit (UK) = sales + credits - taxes - amazon_fee - cost_of_unit_sold
# Profit (UK) = sales + credits - taxes - amazon_fee - cost_of_unit_sold
def uk_profit(
    df: pd.DataFrame,
    *,
    country: str | None = None,
    want_breakdown: bool | None = None,
    debug: bool = True,
    **kwargs
) -> tuple[float, pd.DataFrame, list[str]]:
    """
    FINAL UK PROFIT (Dashboard & Excel Aligned)

    Formula:
        profit = abs(sales)
               - cogs
               - amazon_fee
               - net_taxes
               + net_credits
    """

    # -----------------------------
    # BASE COMPONENTS
    # -----------------------------
    sales_total, sales_by, _ = uk_sales(df, country=country)
    cogs_total, cogs_by, _ = uk_cogs(df, country=country)
    fee_total, fee_by, _ = uk_amazon_fee(df, country=country)
    tax_total, tax_by, _ = uk_tax(df, country=country)
    credit_total, credit_by, _ = uk_credits(df, country=country)

    # -----------------------------
    # MERGE PER-SKU
    # -----------------------------
    per = (
        sales_by[["sku", "__metric__"]]
        .rename(columns={"__metric__": "sales"})
        .merge(
            cogs_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "cogs"}),
            on="sku",
            how="outer",
        )
        .merge(
            fee_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "amazon_fee"}),
            on="sku",
            how="outer",
        )
        .merge(
            tax_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "net_taxes"}),
            on="sku",
            how="outer",
        )
        .merge(
            credit_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "net_credits"}),
            on="sku",
            how="outer",
        )
        .fillna(0.0)
    )

    # -----------------------------
    # NUMERIC SAFETY
    # -----------------------------
    for c in (
        "sales",
        "cogs",
        "amazon_fee",
        "net_taxes",
        "net_credits",
    ):
        per[c] = safe_num(per[c])

    # -----------------------------
    # PROFIT (PER SKU)
    # -----------------------------
    per["__metric__"] = (
        per["sales"].abs()
        - per["cogs"]
        - per["amazon_fee"]
        - per["net_taxes"]
        + per["net_credits"]
    )

    total = float(per["__metric__"].sum())

    comps = [
        "sales",
        "cogs",
        "amazon_fee",
        "net_taxes",
        "net_credits",
    ]

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
        "INCORRECT_FEES_NON_ITEMIZED",
        "StorageReservationBilling",
        "MISSING_FROM_INBOUND_CLAWBACK",
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

    # Explicit column; keep index aligned even when the column is missing
    from_col = num_series(w, explicit_col).abs()

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
    desc_col: str = "description",
    amount_col: str = "total",
    explicit_col: str = "advertising_cost",
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    Advertising logic:

    visible_ads =
        abs(sum(total) where description contains ProductAdsPayment)

    dealsvouchar_ads =
        abs(sum(total) where description contains any coupon/deal keyword)

    advertising_total =
        visible_ads + dealsvouchar_ads + abs(sum(advertising_cost)) if present

    Returns:
        total, per_sku_df, component_names
    """
    import re

    w = df.copy()

    visible_keywords = ("ProductAdsPayment",)

    deals_keywords = (
        "Cost of Advertising",
        "Coupon Redemption Fee",
        "Deals",
        "Lightning Deal",
        "CouponPerformanceEvent",
        "CouponParticipationEvent",
        "SellerDealComplete",
        "VineCharge",
        "DealParticipationEvent",
        "DealPerformanceEvent",
        "SellerPoweredCoupon",
    )

    if desc_col not in w.columns or amount_col not in w.columns:
        comps = ["visible_ads", "dealsvouchar_ads", "from_column_abs"]
        return 0.0, pd.DataFrame(columns=["sku", "__metric__", *comps]), comps

    desc = w[desc_col].astype(str)
    amt = pd.to_numeric(w[amount_col], errors="coerce").fillna(0.0)

    visible_pattern = "|".join(map(re.escape, visible_keywords))
    deals_pattern = "|".join(map(re.escape, deals_keywords))

    visible_mask = desc.str.contains(visible_pattern, case=False, na=False, regex=True)
    deals_mask = desc.str.contains(deals_pattern, case=False, na=False, regex=True)

    visible_ads = amt.where(visible_mask, 0.0).abs()
    dealsvouchar_ads = amt.where(deals_mask, 0.0).abs()
    from_col = num_series(w, explicit_col).abs()

    total = float((visible_ads + dealsvouchar_ads + from_col).sum())

    if "sku" not in w.columns:
        comps = ["visible_ads", "dealsvouchar_ads", "from_column_abs"]
        return total, pd.DataFrame(columns=["sku", "__metric__", *comps]), comps

    w = w.copy()
    w["sku"] = w["sku"].astype(str).str.strip()
    w = w.loc[sku_mask(w)]

    per = pd.DataFrame({
        "sku": w["sku"],
        "visible_ads": visible_ads.loc[w.index],
        "dealsvouchar_ads": dealsvouchar_ads.loc[w.index],
        "from_column_abs": from_col.loc[w.index],
    })

    per = (
        per.groupby("sku", as_index=False)[
            ["visible_ads", "dealsvouchar_ads", "from_column_abs"]
        ]
        .sum()
    )

    per["__metric__"] = (
        per["visible_ads"]
        + per["dealsvouchar_ads"]
        + per["from_column_abs"]
    )

    comps = ["visible_ads", "dealsvouchar_ads", "from_column_abs"]
    return total, per[["sku", "__metric__", *comps]], comps



# ---------- US formulas -------------------------------------------------------
# US has different tax / credit behavior from UK.  Do NOT alias these two to UK:
#   - US net_taxes is normally close to zero because marketplace withheld tax offsets collected tax.
#   - US net_credits includes inventory reimbursement rows plus customer credits.

US_REIMBURSEMENT_CREDIT_KEYWORDS: tuple[str, ...] = (
    "FBA Inventory Reimbursement - Customer Return",
    "FBA Inventory Reimbursement - Customer Service Issue",
    "FBA Inventory Reimbursement - General Adjustment",
    "FBA Inventory Reimbursement - Damaged:Warehouse",
    "FBA Inventory Reimbursement - Lost:Warehouse",
)


def us_sales(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:

    w = df.copy()

    for col in ["product_sales", "promotional_rebates"]:
        if col not in w.columns:
            w[col] = 0.0
        w[col] = safe_num(w[col])

    # Refund rows
    if "type" in w.columns:
        refund_mask = (
            w["type"]
            .astype(str)
            .str.strip()
            .str.casefold()
            .str.contains("refund", na=False)
        )
    else:
        refund_mask = w["product_sales"] < 0

    non_refund_df = w.loc[~refund_mask].copy()
    refund_df = w.loc[refund_mask].copy()

    # ---------------- TOTAL ----------------
    gross_sales_total = float(
        non_refund_df["product_sales"].sum()
    )

    refund_sales_total = float(
        refund_df["product_sales"].abs().sum()
    )

    promotional_rebates_total = float(
        w["promotional_rebates"].sum()
    )

    total = (
        gross_sales_total
        - refund_sales_total
        + promotional_rebates_total
    )

    # ---------------- SKU-WISE ----------------
    if "sku" not in w.columns:
        return (
            total,
            pd.DataFrame(columns=[
                "sku",
                "__metric__",
                "gross_sales",
                "refund_sales",
                "promotional_rebates",
            ]),
            [
                "gross_sales",
                "refund_sales",
                "promotional_rebates",
            ],
        )

    non_refund_df = non_refund_df.loc[sku_mask(non_refund_df)].copy()
    refund_df = refund_df.loc[sku_mask(refund_df)].copy()
    promo_df = w.loc[sku_mask(w)].copy()

    gross_by = (
        non_refund_df
        .groupby("sku", as_index=False)["product_sales"]
        .sum()
        .rename(columns={"product_sales": "gross_sales"})
    )

    refund_by = (
        refund_df
        .groupby("sku", as_index=False)["product_sales"]
        .sum()
        .rename(columns={"product_sales": "refund_sales"})
    )

    refund_by["refund_sales"] = safe_num(
        refund_by["refund_sales"]
    ).abs()

    promo_by = (
        promo_df
        .groupby("sku", as_index=False)["promotional_rebates"]
        .sum()
    )

    by = (
        gross_by
        .merge(refund_by, on="sku", how="outer")
        .merge(promo_by, on="sku", how="outer")
        .fillna(0.0)
    )

    for col in [
        "gross_sales",
        "refund_sales",
        "promotional_rebates",
    ]:
        by[col] = safe_num(by[col])

    by["__metric__"] = (
        by["gross_sales"]
        - by["refund_sales"]
        + by["promotional_rebates"]
    )

    return (
        total,
        by[[
            "sku",
            "__metric__",
            "gross_sales",
            "refund_sales",
            "promotional_rebates",
        ]],
        [
            "gross_sales",
            "refund_sales",
            "promotional_rebates",
        ],
    )


def us_tax(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    US Net Taxes: Shipment transactions only.

    Formula:
        net_taxes =
            product_sales_tax
            + shipping_credits_tax
            + giftwrap_credits_tax
            + promotional_rebates_tax
            + marketplace_facilitator_tax

    Original signs are preserved.
    """

    parts = [
        "product_sales_tax",
        "shipping_credits_tax",
        "giftwrap_credits_tax",
        "promotional_rebates_tax",
        "marketplace_facilitator_tax",
    ]

    w = df.copy()

    # Ensure all tax columns exist and are numeric
    for col in parts:
        if col not in w.columns:
            w[col] = 0.0
        w[col] = safe_num(w[col])

    # Only Shipment transaction rows
    if "type" in w.columns:
        type_norm = (
            w["type"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.casefold()
        )

        w = w.loc[type_norm.eq("shipment")].copy()
    else:
        w = w.iloc[0:0].copy()

    # Total includes all Shipment rows
    total = float(w[parts].sum().sum())

    # No SKU column
    if "sku" not in w.columns:
        return (
            total,
            pd.DataFrame(columns=["sku", "__metric__", *parts]),
            parts,
        )

    # SKU-wise only valid SKUs
    w["sku"] = w["sku"].astype(str).str.strip()
    w = w.loc[sku_mask(w)].copy()

    if w.empty:
        return (
            total,
            pd.DataFrame(columns=["sku", "__metric__", *parts]),
            parts,
        )

    by = (
        w.groupby("sku", as_index=False)[parts]
        .sum()
    )

    by["__metric__"] = by[parts].sum(axis=1)

    return (
        total,
        by[["sku", "__metric__", *parts]],
        parts,
    )

def us_credits(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    Amazon US Net Credits using the SAME formula engine as UK.

    This intentionally calls uk_credits() so US and UK calculate the
    Other Transactions -> Net Credits column from the same reusable helper.
    """
    return uk_credits(df, country=country, want_breakdown=want_breakdown, **kwargs)

def us_gross_sales(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:

    parts = ["product_sales"]

    # Total gross sales
    total = float(
        safe_num(df.get("product_sales", 0.0)).sum()
    )

    # SKU-wise gross sales
    sku_df = df.copy()

    if "sku" in sku_df.columns:
        sku_df = sku_df.loc[sku_mask(sku_df)]

    by = agg_by(
        sku_df,
        "sku",
        parts
    )

    if by.empty:
        return (
            0.0,
            pd.DataFrame(
                columns=["sku", "__metric__", "product_sales"]
            ),
            parts,
        )

    by["__metric__"] = safe_num(by["product_sales"])

    return (
        total,
        by[["sku", "__metric__", "product_sales"]],
        parts,
    )


def us_tax_and_credits(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    US Taxes and Credits:

        tex_and_credits = net_taxes + net_credits

    net_taxes comes from us_tax()
    net_credits comes from us_credits()
    """

    tax_total, tax_by, _ = us_tax(
        df,
        country=country,
        want_breakdown=want_breakdown,
        **kwargs
    )

    credit_total, credit_by, _ = us_credits(
        df,
        country=country,
        want_breakdown=want_breakdown,
        **kwargs
    )

    total = float(tax_total + credit_total)

    tax_part = (
        tax_by[["sku", "__metric__"]]
        .rename(columns={"__metric__": "net_taxes"})
        if tax_by is not None
        and not tax_by.empty
        and "sku" in tax_by.columns
        and "__metric__" in tax_by.columns
        else pd.DataFrame(columns=["sku", "net_taxes"])
    )

    credit_part = (
        credit_by[["sku", "__metric__"]]
        .rename(columns={"__metric__": "net_credits"})
        if credit_by is not None
        and not credit_by.empty
        and "sku" in credit_by.columns
        and "__metric__" in credit_by.columns
        else pd.DataFrame(columns=["sku", "net_credits"])
    )

    by = (
        tax_part
        .merge(
            credit_part,
            on="sku",
            how="outer"
        )
        .fillna(0.0)
    )

    if by.empty:
        return (
            total,
            pd.DataFrame(
                columns=[
                    "sku",
                    "__metric__",
                    "net_taxes",
                    "net_credits",
                ]
            ),
            ["net_taxes", "net_credits"],
        )

    by["net_taxes"] = safe_num(by["net_taxes"])
    by["net_credits"] = safe_num(by["net_credits"])

    by["__metric__"] = (
        by["net_taxes"]
        + by["net_credits"]
    )

    return (
        total,
        by[
            [
                "sku",
                "__metric__",
                "net_taxes",
                "net_credits",
            ]
        ],
        ["net_taxes", "net_credits"],
    )


def us_cogs(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    return uk_cogs(df, country=country, want_breakdown=want_breakdown, **kwargs)


def us_amazon_fee(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    """
    US Amazon Fee:

        Amazon Fee
        = abs(adjusted selling fees)
        + abs(FBA fees from Shipment and Refund only)

    FBA fees from other transaction types are excluded.
    """

    w = df.copy()

    # Ensure required numeric columns exist
    for col in ["fba_fees", "selling_fees"]:
        if col not in w.columns:
            w[col] = 0.0

        w[col] = safe_num(w[col])

    if "sku" not in w.columns:
        components = [
            "fba_abs",
            "selling_adj_abs",
            "selling_raw_abs",
            "refund_selling_fees",
        ]

        return (
            0.0,
            pd.DataFrame(
                columns=["sku", "__metric__", *components]
            ),
            ["fba_abs", "selling_adj_abs"],
        )

    w["sku"] = w["sku"].astype(str).str.strip()
    w = w.loc[sku_mask(w)].copy()

    # Normalize transaction type
    type_norm = text_series(w, "type").str.strip().str.casefold()

    # -------------------------------------------------
    # FBA fees: only Shipment and Refund transactions
    # -------------------------------------------------
    fba_mask = type_norm.isin(["shipment", "refund"])

    fba_by = (
        w.loc[fba_mask]
        .groupby("sku", as_index=False)["fba_fees"]
        .sum()
    )

    # -------------------------------------------------
    # Selling fees from all valid SKU rows
    # -------------------------------------------------
    selling_by = (
        w.groupby("sku", as_index=False)["selling_fees"]
        .sum()
    )

    # Refund selling fees
    refund_selling_by = (
        w.loc[type_norm.eq("refund")]
        .groupby("sku", as_index=False)["selling_fees"]
        .sum()
        .rename(
            columns={
                "selling_fees": "refund_selling_fees"
            }
        )
    )

    # Merge components
    by = (
        selling_by
        .merge(fba_by, on="sku", how="outer")
        .merge(refund_selling_by, on="sku", how="left")
        .fillna(0.0)
    )

    for col in [
        "selling_fees",
        "fba_fees",
        "refund_selling_fees",
    ]:
        by[col] = safe_num(by[col])

    # Same selling-fee adjustment used by your processing code
    by["selling_fees_adj"] = (
        by["selling_fees"]
        - (2.0 * by["refund_selling_fees"])
    )

    by["fba_abs"] = by["fba_fees"].abs()
    by["selling_raw_abs"] = by["selling_fees"].abs()
    by["selling_adj_abs"] = by["selling_fees_adj"].abs()

    by["__metric__"] = (
        by["fba_abs"]
        + by["selling_adj_abs"]
    )

    per_sku = by[
        [
            "sku",
            "__metric__",
            "fba_abs",
            "selling_adj_abs",
            "selling_raw_abs",
            "refund_selling_fees",
        ]
    ].copy()

    total = float(per_sku["__metric__"].sum())

    return (
        total,
        per_sku,
        ["fba_abs", "selling_adj_abs"],
    )


def us_platform_fee(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    return uk_platform_fee(df, country=country, want_breakdown=want_breakdown, **kwargs)


def us_advertising(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:
    return uk_advertising(df, country=country, want_breakdown=want_breakdown, **kwargs)


# def us_profit(
#     df: pd.DataFrame,
#     *,
#     country: Optional[str] = "us",
#     want_breakdown: Optional[bool] = None,
#     debug: bool = True,
#     **kwargs
# ) -> Tuple[float, pd.DataFrame, List[str]]:

#     sales_total, sales_by, _ = us_sales(df, country=country)
#     cogs_total, cogs_by, _ = us_cogs(df, country=country)
#     fee_total, fee_by, _ = us_amazon_fee(df, country=country)
#     tax_total, tax_by, _ = us_tax(df, country=country)
#     credits_total, credits_by, _ = us_credits(df, country=country)

#     per = (
#         sales_by[["sku", "__metric__"]]
#         .rename(columns={"__metric__": "sales"})
#         .merge(
#             cogs_by[["sku", "__metric__"]]
#             .rename(columns={"__metric__": "cogs"}),
#             on="sku",
#             how="outer"
#         )
#         .merge(
#             fee_by[["sku", "__metric__"]]
#             .rename(columns={"__metric__": "amazon_fee"}),
#             on="sku",
#             how="outer"
#         )
#         .merge(
#             tax_by[["sku", "__metric__"]]
#             .rename(columns={"__metric__": "net_taxes"}),
#             on="sku",
#             how="outer"
#         )
#         .merge(
#             credits_by[["sku", "__metric__"]]
#             .rename(columns={"__metric__": "net_credits"}),
#             on="sku",
#             how="outer"
#         )
#         .fillna(0.0)
#     )

#     for col in (
#         "sales",
#         "cogs",
#         "amazon_fee",
#         "net_taxes",
#         "net_credits",
#     ):
#         per[col] = safe_num(per[col])

#     per["__metric__"] = (
#         per["sales"]
#         - per["cogs"].abs()
#         - per["amazon_fee"].abs()
#         - per["net_taxes"].abs()
#         + per["net_credits"]
#     )

#     comps = [
#         "sales",
#         "cogs",
#         "amazon_fee",
#         "net_taxes",
#         "net_credits",
#     ]

#     total = float(per["__metric__"].sum())

#     return (
#         total,
#         per[["sku", "__metric__", *comps]],
#         comps,
#     )


def us_profit(
    df: pd.DataFrame,
    *,
    country: Optional[str] = "us",
    want_breakdown: Optional[bool] = None,
    debug: bool = True,
    **kwargs
) -> Tuple[float, pd.DataFrame, List[str]]:

    sales_total, sales_by, _ = us_sales(df, country=country)
    cogs_total, cogs_by, _ = us_cogs(df, country=country)
    fee_total, fee_by, _ = us_amazon_fee(df, country=country)
    tax_total, tax_by, _ = us_tax(df, country=country)
    credits_total, credits_by, _ = us_credits(df, country=country)

    per = (
        sales_by[["sku", "__metric__"]]
        .rename(columns={"__metric__": "sales"})
        .merge(
            cogs_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "cogs"}),
            on="sku",
            how="outer",
        )
        .merge(
            fee_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "amazon_fee"}),
            on="sku",
            how="outer",
        )
        .merge(
            tax_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "net_taxes"}),
            on="sku",
            how="outer",
        )
        .merge(
            credits_by[["sku", "__metric__"]]
            .rename(columns={"__metric__": "net_credits"}),
            on="sku",
            how="outer",
        )
        .fillna(0.0)
    )

    for col in (
        "sales",
        "cogs",
        "amazon_fee",
        "net_taxes",
        "net_credits",
    ):
        per[col] = safe_num(per[col])

    # Profit formula:
    # Sales - absolute COGS - absolute Amazon fees
    #       - absolute taxes + credits
    per["__metric__"] = (
        per["sales"]
        - per["cogs"].abs()
        - per["amazon_fee"].abs()
        - per["net_taxes"].abs()
        + per["net_credits"]
    )

    comps = [
        "sales",
        "cogs",
        "amazon_fee",
        "net_taxes",
        "net_credits",
    ]

    total = float(per["__metric__"].sum())

    return (
        total,
        per[["sku", "__metric__", *comps]],
        comps,
    )



def us_all(df: pd.DataFrame) -> dict:
    return {
        "sales": us_sales(df),
        "gross_sales": us_gross_sales(df),
        "tax": us_tax(df),
        "credits": us_credits(df),
        "tax_and_credits": us_tax_and_credits(df),
        "cogs": us_cogs(df),
        "amazon_fee": us_amazon_fee(df),
        "platform_fee": us_platform_fee(df),
        "advertising": us_advertising(df),
        "profit": us_profit(df),
    }

# ---------- convenience -------------------------------------------------------
def uk_all(df: pd.DataFrame) -> dict:
    return {
        "sales": uk_sales(df),
        "gross_sales": uk_gross_sales(df),
        "tax": uk_tax(df),
        "credits": uk_credits(df),
        "tax_and_credits": uk_tax_and_credits(df),
        "cogs": uk_cogs(df),
        "amazon_fee": uk_amazon_fee(df),
        "platform_fee": uk_platform_fee(df),
        "advertising": uk_advertising(df),
        "profit": uk_profit(df),
    }

__all__ = [
    # helpers
    "safe_num", "series_like", "text_series", "num_series",
    "norm_sku_series", "sku_mask", "agg_by",

    # uk metrics
    "uk_sales", "uk_tax", "uk_credits", "uk_gross_sales",
    "uk_tax_and_credits", "uk_cogs", "uk_amazon_fee",
    "uk_platform_fee", "uk_advertising", "uk_profit", "uk_all",

    # us metrics
    "us_sales", "us_tax", "us_credits", "us_gross_sales",
    "us_tax_and_credits", "us_cogs", "us_amazon_fee",
    "us_platform_fee", "us_advertising", "us_profit", "us_all",
]
