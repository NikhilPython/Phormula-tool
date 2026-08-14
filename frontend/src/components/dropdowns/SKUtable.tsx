"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx";
// import { jwtDecode } from "jwt-decode";
import SkuMultiCountryUpload from "../ui/modal/SkuMultiCountryUpload";
// import Productinfoinpopup from "./Productinfoinpopup";
import PageBreadcrumb from "../common/PageBreadCrumb";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import type { SkuExportPayload } from "@/lib/utils/exportTypes";
import GroupedCollapsibleTable, { LeafCol, ColGroup } from "../ui/table/GroupedCollapsibleTable";
import ExcelJS from "exceljs";
import { buildSkuWorksheetFromModel } from "@/lib/utils/excel/buildSkuWorksheet";
import { downloadWorkbookAsXlsx } from "@/lib/utils/excel/downloadExcel";
import InfoTip from "@/components/ui/InfoTip";
import type { TopBottomData } from "@/lib/pnl/topBottom";
import Loader from "../loader/Loader";
import {
  RiExpandDiagonalFill,
  RiCollapseDiagonalFill,
  RiLayoutColumnFill,
  RiLayoutColumnLine,
} from "react-icons/ri";

const TERM_DEFINITIONS: Record<string, string> = {
  product_name: "Product Name. The delta represents the change compared to the previous period.",

  sku: "SKU = Stock Keeping Unit, the unique product identifier.",
  units_sold: "Units Sold = total units sold before returns.",
  return_units: "Return = units returned/refunded by customers.",
  net_units_sold: "Net Units Sold = Units Sold - Return Units.",

  asp: "ASP (Average Selling Price) = Net Sales ÷ Net Units Sold.",

  product_sales: "Gross Sales = total product sales before refunds, taxes, credits, and deductions.",
  refund_sales: "Sales - Refund = refunded sales amount.",
  net_sales: "Net Sales = sales after refunds and sales-related adjustments.",
  tex_and_credits: "Taxes & Credits = combined effect of taxes and credits applied to orders.",

  net_taxes: "Net Taxes = total taxes charged on sales minus tax adjustments/refunds.",
  net_credits: "Net Credits = credits received from marketplace adjustments, excluding reimbursements.",

  promotional_rebates: "Promotions = promotional rebates/discounts applied, such as coupons or deals.",
  promotional_rebates_percentage: "Promotions % = Promotions ÷ Net Sales × 100.",

  cost_of_unit_sold: "COGS = Cost of goods sold for the units sold in the period.",

  marketplace_fees: "Marketplace Fees = total fees charged by Amazon, such as referral and FBA fees.",
  amazon_fee: "Marketplace Fees = total fees charged by Amazon, such as referral and FBA fees.",
  selling_fees: "Selling Fees = Amazon referral/commission and selling-related fees.",
  fba_fees: "FBA Fees = fulfillment, storage, and FBA service fees.",

  other_transactions: "Other Transactions = marketplace adjustments, taxes, credits, misc. charges, and other transaction-level fees.",

  cm1_profit: "CM1 Profit = contribution margin after sales, COGS, promotions, marketplace fees, taxes, and credits.",
  profit: "CM1 Profit = contribution margin after sales, COGS, promotions, marketplace fees, taxes, and credits.",
  profit_percentage: "CM1 Profit % = CM1 Profit ÷ Net Sales × 100.",
  unit_wise_profitability: "CM1 Profit Per Unit = CM1 Profit ÷ Net Units Sold.",
};

/* ---------- Types ---------- */

type RangeType = "monthly" | "quarterly" | "yearly";

type SKUtableProps = {
  range: RangeType;
  month?: string;
  quarter?: string;
  year: string | number;
  countryName: string;
  homeCurrency?: string;

  rows: TableRow[];
  loading?: boolean;
  error?: string | null;
  noDataFound?: boolean;
  userMeta?: {
    brand_name?: string;
    company_name?: string;
  } | null;

  onExportPayloadChange?: (payload: SkuExportPayload) => void;
  hideDownloadButton?: boolean;
  onDownload?: () => void;
  disableInternalFade?: boolean;

  onProductDetailClick?: (productName: string, sku?: string) => void;
};

type TopBottomRow = {
  product_name: string;
  profit: string;
  profitMix: string;
  salesMix: string;
  cm1_per_unit: string;
};



export type TableRow = {
  product_name?: string;
  sku?: string;
  previous_net_sales?: number;
  net_sales_delta?: number;
  net_sales_delta_percentage?: number;

  quantity?: number;
  total_quantity?: number;

  asp?: number;
  ASP?: number;

  ads_spend?: number;
  product_spend?: number;
  display_spend?: number;
  brand_spend?: number;
  cm2_profit_total?: number;
  total_cm2_profit?: number;
  cm2_profit_per?: number;
  cm2_profit_per_unit?: number;
  debt_payment?: number;
  disbursement?: number;
  gross_sales?: number;
  product_sales?: number;
  refund_sales?: number;
  net_sales?: number;
  lost_total?: number;

  cost_of_unit_sold?: number;
  shipment_charges?: number;
  shipment_fees?: number;

  amazon_fee?: number;
  selling_fees?: number;
  fba_fees?: number;

  tex_and_credits?: number;
  net_taxes?: number;
  net_credits?: number;

  promotional_rebates?: number;
  promotional_rebates_percentage?: number;

  misc_transaction?: number;
  other_transaction_fees?: number;
  platform_fee?: number; // backend sometimes sends this
  other_transactions?: number; // backend alias / display key
  other_adjustment?: number;

  profit?: number;
  profit_percentage?: number;
  unit_wise_profitability?: number;

  // derived units fields
  units_sold?: number;
  return_units?: number;
  net_units_sold?: number;

  // totals-only fields (often on last row)
  platformfeenew?: number;
  platform_fee_inventory_storage?: number;

  advertising_total?: number;
  advertising_total_final?: number;
  advertising_fees?: number;
  total_ads?: number;
  visible_ads?: number;
  dealsvouchar_ads?: number;

  reimbursement_lost_inventory_amount?: number;
  reimbursement_lost_inventory_units?: number;

  reimbursement_vs_sales?: number;

  cm2_profit?: number;
  cm2_margins?: number;
  cm2_profit_percentage?: number;
  cm2_profit_percent?: number;
  cm2_profit_percentage_value?: number;
  acos?: number;
  rembursment_vs_cm2_margins?: number;

  Profit?: number;
  Net_Sales?: number;

  profit_mix?: number;
  sales_mix?: number;

  short_term_storage_fee?: number;
  long_term_storage_fee?: number;
  fba_disposal?: number;

  // backend might send this as well
  return_quantity?: number;

  unit_wise_cm2_profitability?: number;
};

type Totals = {
  advertising_total: number;
  advertising_total_final?: number;
  advertising_fees: number;
  total_ads: number;
  visible_ads: number;
  dealsvouchar_ads: number;
  other_transactions: number;
  platform_fee: number;
  inventory_storage_fees: number;
  misc_transaction: number;
  reimbursement_lost_inventory_amount: number;
  reimbursement_lost_inventory_units?: number;
  shipment_charges: number;
  reimbursement_vs_sales: number;
  cm2_profit: number;
  cm2_profit_total: number;
  total_cm2_profit: number;
  cm2_margins: number;
  cm2_profit_per: number;
  acos: number;
  rembursment_vs_cm2_margins: number;
  profit: number;
  net_sales: number;
  lost_total: number;
  net_reimbursement: number;
  debt_payment: number;
  disbursement: number;
  ads_spend: number;
  product_spend: number;
  display_spend: number;
  brand_spend: number;
  short_term_storage_fee: number;
  long_term_storage_fee: number;
  fba_disposal: number;
};

/* ---------- Helpers ---------- */

const getCurrencySymbol = (codeOrCountry: string) => {
  switch ((codeOrCountry || "").toLowerCase()) {
    case "uk":
    case "gb":
    case "gbp":
      return "£";
    case "india":
    case "in":
    case "inr":
      return "₹";
    case "us":
    case "usa":
    case "usd":
      return "$";
    case "europe":
    case "eu":
    case "eur":
      return "€";
    default:
      return "¤";
  }
};

const capitalizeFirstLetter = (str: string) =>
  str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();

const convertToAbbreviatedMonth = (m?: string) =>
  m ? capitalizeFirstLetter(m).slice(0, 3) : "";

const isMissingName = (v: unknown) => {
  if (v === undefined || v === null) return true;
  if (typeof v === "number" && Number.isNaN(v)) return true;

  const s = String(v).trim().toLowerCase();
  return s === "" || s === "0" || s === "nan" || s === "none" || s === "null" || s === "undefined";
};

const toNumber = (v: any) => {
  if (v === undefined || v === null || v === "") return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;
  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
};

const getOptionalNumber = (source: unknown, keys: string[]) => {
  const record =
    source && typeof source === "object" ? (source as Record<string, unknown>) : null;

  for (const key of keys) {
    if (!record || !Object.prototype.hasOwnProperty.call(record, key)) continue;

    const value = record[key];
    if (value === undefined || value === null || value === "") continue;

    const normalized =
      typeof value === "number"
        ? value
        : String(value).replace(/,/g, "").trim();

    if (
      typeof normalized === "string" &&
      (normalized === "-" ||
        (normalized.length === 1 &&
          (normalized.charCodeAt(0) === 8211 || normalized.charCodeAt(0) === 8212)))
    ) {
      continue;
    }

    const n = typeof normalized === "number" ? normalized : Number(normalized);
    if (Number.isFinite(n)) return n;
  }

  return null;
};

const nonZeroOrNull = (value: unknown) => {
  const n = toNumber(value);
  return n !== 0 ? n : null;
};

const sumKnownNumbers = (...values: Array<number | null>) => {
  const known = values.filter((value): value is number => value !== null);
  return known.length ? known.reduce((sum, value) => sum + value, 0) : null;
};

const differenceKnownNumbers = (
  minuend: number | null,
  subtrahend: number | null
) => {
  if (minuend === null && subtrahend === null) return null;
  return Number(minuend ?? 0) - Number(subtrahend ?? 0);
};

const formatRoundedPlain = (value: unknown) => {
  return Math.round(Math.abs(toNumber(value ?? 0))).toLocaleString();
};

const isNumericKey = (k: string, v: any) => typeof v === "number" || (!isNaN(Number(v)) && v !== null && v !== "");

function sumRows(rows: TableRow[], base: Partial<TableRow>): TableRow {
  const out: any = { ...base };
  const averageKeys = new Set(["promotional_rebates_percentage"]);
  const averageCounts: Record<string, number> = {};

  for (const r of rows) {
    Object.keys(r || {}).forEach((k) => {
      const v = (r as any)[k];

      if (!isNumericKey(k, v)) return;

      // ignore name-ish keys
      if (k === "product_name" || k === "sku") return;

      // ✅ do NOT sum ASP fields (we’ll compute later as net_sales / net_units_sold)
      if (k === "asp" || k === "ASP") return;

      out[k] = toNumber(out[k]) + toNumber(v);
      if (averageKeys.has(k)) {
        averageCounts[k] = (averageCounts[k] || 0) + 1;
      }
    });
  }

  averageKeys.forEach((key) => {
    if (averageCounts[key]) {
      out[key] = toNumber(out[key]) / averageCounts[key];
    }
  });

  return out as TableRow;
}



function normalizeRows(data: any[]): TableRow[] {
  return data.map((row) => {
    const rawProductName = row.product_name;
    const rawSku = row.sku;

    const productName =
      !isMissingName(rawProductName)
        ? String(rawProductName).trim()
        : !isMissingName(rawSku)
          ? String(rawSku).trim()
          : "-";

    const isTotalRow =
      String(rawProductName ?? "").trim().toLowerCase() === "total" ||
      String(rawSku ?? "").trim().toLowerCase() === "total";

    return {
      ...row,

      product_name: isTotalRow ? "Total" : productName,
      sku: row.sku ?? "-",

      quantity: toNumber(row.quantity),
      return_quantity: toNumber(row.return_quantity),
      total_quantity: toNumber(row.total_quantity),

      // Units (UI derived)
      units_sold: toNumber(row.quantity),
      return_units: toNumber(row.return_quantity),
      net_units_sold: toNumber(row.total_quantity),

      // Sales
      asp: toNumber(row.asp ?? row.ASP),
      product_sales: toNumber(row.gross_sales ?? row.product_sales),
      refund_sales: toNumber(row.refund_sales),
      net_sales: toNumber(row.net_sales),
      lost_total: toNumber(row.lost_total),

      reimbursement_vs_sales: toNumber(row.reimbursement_vs_sales),
      debt_payment: toNumber(row.debt_payment),
      disbursement: toNumber(row.disbursement),

      // Costs / Fees
      cost_of_unit_sold: toNumber(row.cost_of_unit_sold),
      shipment_charges: toNumber(row.shipment_charges ?? row.shipment_fees),
      shipment_fees: toNumber(row.shipment_fees ?? row.shipment_charges),
      selling_fees: toNumber(row.selling_fees),
      fba_fees: toNumber(row.fba_fees),
      amazon_fee: toNumber(row.amazon_fee),

      // Taxes / Credits
      tex_and_credits: toNumber(row.tex_and_credits),
      net_taxes: toNumber(row.net_taxes),
      net_credits: toNumber(row.net_credits),

      // Promotions
      promotional_rebates: toNumber(row.promotional_rebates),
      promotional_rebates_percentage: toNumber(row.promotional_rebates_percentage),

      // Other / Misc
      misc_transaction: toNumber(row.misc_transaction),
      other_transaction_fees: toNumber(row.other_transaction_fees ?? row.other_transactions),

      other_transactions: toNumber(row.other_transactions ?? row.other_transaction_fees),

      // CM1
      profit: toNumber(row.profit),
      profit_percentage: toNumber(row.profit_percentage),
      unit_wise_profitability: toNumber(row.unit_wise_profitability),

      // CM2 / Ads
      product_spend: toNumber(
        row.product_spend ??
        row.sponsored_product ??
        row.Sponsored_Product
      ),

      display_spend: toNumber(
        row.display_spend ??
        row.sponsored_display ??
        row.Sponsored_Display
      ),

      brand_spend: toNumber(
        row.brand_spend ??
        row.sponsored_brand ??
        row.Sponsored_Brand
      ),

      ads_spend: toNumber(row.ads_spend),

      advertising_total: toNumber(row.advertising_total),
      advertising_total_final: toNumber(
        nonZeroOrNull(row.advertising_total_final) ?? row.advertising_total
      ),
      advertising_fees: toNumber(row.advertising_fees),
      total_ads: toNumber(row.total_ads),


      visible_ads: toNumber(row.visible_ads),

      dealsvouchar_ads: toNumber(
        row.dealsvouchar_ads ??
        row.dealvouchars_ads ??
        row.dealsvoucher_ads
      ),

      acos: toNumber(row.acos),

      cm2_profit: toNumber(row.cm2_profit),
      total_cm2_profit: toNumber(row.total_cm2_profit ?? row.cm2_profit),
      cm2_profit_total: toNumber(row.cm2_profit_total ?? row.cm2_profit),

      cm2_profit_per: toNumber(
        row.cm2_profit_per ??
        row.cm2_profit_percentage ??
        row.cm2_profit_percent ??
        row.cm2_profit_percentage_value
      ),

      cm2_margins: toNumber(
        row.cm2_margins ??
        row.cm2_profit_percentage ??
        row.cm2_profit_percent ??
        row.cm2_profit_percentage_value
      ),

      cm2_profit_percentage: toNumber(
        row.cm2_profit_per ??
        row.cm2_profit_percentage ??
        row.cm2_profit_percent ??
        row.cm2_profit_percentage_value
      ),

      unit_wise_cm2_profitability: toNumber(
        row.cm2_profit_per_unit ?? row.unit_wise_cm2_profitability
      ),

      // ✅ Inventory Storage Fees breakdown
      short_term_storage_fee: toNumber(row.short_term_storage_fee),
      long_term_storage_fee: toNumber(row.long_term_storage_fee),
      fba_disposal: toNumber(row.fba_disposal),
    } as TableRow;

  });
}

const isTotalLikeRow = (row: TableRow | any) => {
  const productName = String(row?.product_name ?? "").trim().toLowerCase();
  const sku = String(row?.sku ?? "").trim().toLowerCase();

  return productName === "total" || sku === "total";
};

function computeTotalsFromTotalRow(rows: TableRow[]): Totals {
  const totalRow: any =
    rows.find(isTotalLikeRow) ||
    rows[rows.length - 1] ||
    {};

  const platformFees = toNumber(totalRow.platformfeenew);
  const inventoryStorageFees = toNumber(totalRow.platform_fee_inventory_storage);

  const netReimbursement = toNumber(totalRow.rembursement_fee);

  const reimbursementUnits =
    toNumber(totalRow.reimbursement_lost_inventory_units) || 0;

  const cm2MarginsValue = toNumber(
    totalRow.cm2_margins ??
    totalRow.cm2_profit_percentage ??
    totalRow.cm2_profit_percent ??
    totalRow.cm2_profit_percentage_value
  );

  const cm2ProfitPerValue = toNumber(
    totalRow.cm2_profit_per ??
    totalRow.cm2_profit_percentage ??
    totalRow.cm2_profit_percent ??
    totalRow.cm2_profit_percentage_value
  );

  return {
    ads_spend: toNumber(totalRow.ads_spend),
    product_spend: toNumber(totalRow.product_spend),
    display_spend: toNumber(totalRow.display_spend),
    brand_spend: toNumber(totalRow.brand_spend),

    advertising_total: toNumber(totalRow.advertising_total),
    advertising_fees: toNumber(totalRow.advertising_fees),
    total_ads: toNumber(totalRow.total_ads),

    net_reimbursement: netReimbursement,
    debt_payment: toNumber(totalRow.debt_payment),
    disbursement: toNumber(totalRow.disbursement),
    // final card / TACoS value: 21,138.82
    advertising_total_final: toNumber(
      nonZeroOrNull(totalRow.advertising_total_final) ?? totalRow.advertising_total
    ),

    visible_ads: toNumber(totalRow.visible_ads),

    dealsvouchar_ads: toNumber(
      totalRow.dealsvouchar_ads ??
      totalRow.dealvouchars_ads ??
      totalRow.dealsvoucher_ads
    ),
    other_transactions: toNumber(totalRow.other_transactions ?? totalRow.other_transaction_fees),
    platform_fee: platformFees,

    inventory_storage_fees: inventoryStorageFees,
    short_term_storage_fee: toNumber(totalRow.short_term_storage_fee),
    long_term_storage_fee: toNumber(totalRow.long_term_storage_fee),
    fba_disposal: toNumber(totalRow.fba_disposal),

    misc_transaction: toNumber(totalRow.misc_transaction),
    reimbursement_lost_inventory_amount:
      toNumber(totalRow.reimbursement_lost_inventory_amount) || 0,
    reimbursement_lost_inventory_units: reimbursementUnits,
    lost_total: toNumber(totalRow.lost_total),
    shipment_charges: toNumber(totalRow.shipment_charges ?? totalRow.shipment_fees),
    reimbursement_vs_sales: toNumber(totalRow.reimbursement_vs_sales),
    cm2_profit: toNumber(totalRow.cm2_profit),
    total_cm2_profit: toNumber(totalRow.total_cm2_profit ?? totalRow.cm2_profit),
    cm2_profit_total: toNumber(totalRow.cm2_profit_total ?? totalRow.cm2_profit),
    cm2_margins: cm2MarginsValue,
    cm2_profit_per: cm2ProfitPerValue,
    acos: toNumber(totalRow.acos),
    rembursment_vs_cm2_margins: toNumber(totalRow.rembursment_vs_cm2_margins),
    profit: toNumber(totalRow.Profit ?? totalRow.profit),
    net_sales: toNumber(totalRow.Net_Sales ?? totalRow.net_sales),
  };
}

/* ---------- Component ---------- */

const SKUtable: React.FC<SKUtableProps> = ({
  range,
  month = "",
  quarter = "",
  year,
  countryName,
  homeCurrency,

  rows,
  loading = false,
  error = null,
  noDataFound = false,
  userMeta = null,

  onExportPayloadChange,
  hideDownloadButton = false,
  onDownload,
  disableInternalFade = false,
  onProductDetailClick,
  // metricSortMetrics = ["units", "sales", "profit", "marketplace_fees"],
}) => {
  // const [selectedProduct, setSelectedProduct] = useState<string | null>(null);
  // const [showModal, setShowModal] = useState(false);
  const [showModal2, setShowModal2] = useState(false);
  const [mainColCount, setMainColCount] = useState(0);
  // const [anyGroupExpanded, setAnyGroupExpanded] = useState(false);
  const [summaryCollapsed, setSummaryCollapsed] = useState({
    ads: true,
    other: true,
  });
  const [showAllRows, setShowAllRows] = useState(false);
  const [allColumnsExpanded, setAllColumnsExpanded] = useState(false);
  const [collapsedGroups, setCollapsedGroups] = useState<Record<string, boolean> | undefined>(
    undefined
  );
  const toggleSummary = (key: "ads" | "other") => {
    setSummaryCollapsed((p) => ({ ...p, [key]: !p[key] }));
  };

  const normalizedCountryName = (countryName || "").trim().toLowerCase();
  const isGlobalPage = normalizedCountryName === "global";
  const isUsCountry = normalizedCountryName === "us" || normalizedCountryName === "usa";

  const tableData = useMemo(() => {
    return normalizeRows(rows || []);
  }, [rows]);

  const hasCm2Data = useMemo(() => {
    const currentCountry = String(countryName || "").trim().toLowerCase();
    const currentRange = String(range || "").trim().toLowerCase();

    // Hide Ads + CM2 for US yearly and quarterly, same as UK behavior
    if (
      (currentRange === "yearly" || currentRange === "quarterly")
    ) {
      return false;
    }

    const productRows = (tableData || []).filter((row: any) => {
      const name = String(row?.product_name || "").trim().toLowerCase();
      const sku = String(row?.sku || "").trim().toLowerCase();

      return (
        name !== "total" &&
        sku !== "total" &&
        name !== "others" &&
        sku !== "others"
      );
    });

    if (!productRows.length) return false;

    // ✅ Show Ads Spend, ACoS%, CM2 Profit only when at least one SKU has real CM2 impact
    return productRows.some((row: any) => {
      const adsSpend = toNumber(row.ads_spend ?? row.advertising_total);
      const backendAcos = toNumber(row.acos);
      const netSales = toNumber(row.net_sales);

      const acos = backendAcos || (netSales !== 0 ? (adsSpend / netSales) * 100 : 0);

      const cm1Profit = toNumber(row.profit);
      const cm2Profit = toNumber(row.cm2_profit ?? row.cm2_profit_total);

      const cm2DiffersFromCm1 = Math.abs(cm2Profit - cm1Profit) > 0.01;

      return (
        adsSpend !== 0 ||
        acos !== 0 ||
        cm2DiffersFromCm1
      );
    });
  }, [tableData, countryName, range]);

  const getCm2PerUnit = (row: Partial<TableRow>) => {
    const cm2 = toNumber((row as any).cm2_profit);
    const units = toNumber((row as any).net_units_sold ?? (row as any).total_quantity);
    return units > 0 ? cm2 / units : 0;
  };

  const getCm2Percentage = (row: Partial<TableRow>) => {
    const isTotal =
      String((row as any)?.product_name ?? "").trim().toLowerCase() === "total" ||
      String((row as any)?.sku ?? "").trim().toLowerCase() === "total";

    const backendValue = toNumber(
      isTotal
        ? (
          (row as any).cm2_profit_per ??
          (row as any).cm2_profit_percentage ??
          (row as any).cm2_profit_percent ??
          (row as any).cm2_profit_percentage_value
        )
        : (
          (row as any).cm2_margins ??
          (row as any).cm2_profit_percentage ??
          (row as any).cm2_profit_percent ??
          (row as any).cm2_profit_percentage_value
        )
    );

    if (backendValue) return backendValue;

    const cm2 = toNumber((row as any).cm2_profit);
    const sales = toNumber((row as any).net_sales);

    return sales !== 0 ? (cm2 / sales) * 100 : 0;
  };

  const getAcosPercentage = (row: Partial<TableRow>) => {
    const backendValue = toNumber((row as any).acos);
    if (backendValue) return backendValue;

    const ads = toNumber((row as any).ads_spend ?? (row as any).advertising_total);
    const sales = toNumber((row as any).net_sales);

    return sales !== 0 ? (ads / sales) * 100 : 0;
  };

  const getBackendOtherTransactionsValue = useCallback(
    (row: Partial<TableRow>) => {
      return toNumber((row as any).other_transaction_fees ?? (row as any).other_transactions);
    },
    []
  );

  const totals = useMemo(() => {
    return computeTotalsFromTotalRow(tableData);
  }, [tableData]);

  const rawTotalRow = useMemo(() => {
    const rawRows = Array.isArray(rows) ? rows : [];
    return rawRows.find(isTotalLikeRow) || rawRows[rawRows.length - 1] || {};
  }, [rows]);

  const frontendTacos = useMemo(() => {
    const netSales = toNumber(totals.net_sales);
    const adsCost = Math.abs(toNumber(totals.advertising_total_final));

    return netSales > 0 ? (adsCost / netSales) * 100 : 0;
  }, [totals.net_sales, totals.advertising_total_final]);

  const visibilityAdsValue = useMemo(() => {
    return hasCm2Data
      ? toNumber(totals.brand_spend)
      : toNumber(totals.visible_ads);
  }, [hasCm2Data, totals.brand_spend, totals.visible_ads]);

  const costOfAdvertisementValue = useMemo(() => {
    if (hasCm2Data) {
      const childTotal =
        toNumber(visibilityAdsValue) + toNumber(totals.dealsvouchar_ads);

      return toNumber(totals.advertising_total) || childTotal;
    }

    const requestedAdSpend =
      nonZeroOrNull(totals.advertising_fees) ??
      nonZeroOrNull(totals.ads_spend) ??
      nonZeroOrNull(totals.total_ads) ??
      nonZeroOrNull(totals.advertising_total_final) ??
      nonZeroOrNull(totals.advertising_total);

    if (requestedAdSpend !== null) return requestedAdSpend;

    const hasRequestedAdField =
      rawTotalRow &&
      typeof rawTotalRow === "object" &&
      ["advertising_fees", "ads_spend", "total_ads"].some((key) =>
        Object.prototype.hasOwnProperty.call(rawTotalRow, key)
      );

    return hasRequestedAdField ? 0 : toNumber(totals.advertising_total);
  }, [
    hasCm2Data,
    rawTotalRow,
    totals.advertising_total,
    totals.advertising_total_final,
    totals.advertising_fees,
    totals.ads_spend,
    totals.total_ads,
    totals.dealsvouchar_ads,
    visibilityAdsValue,
  ]);

  const [tableSort, setTableSort] = useState<{
    key: string;
    direction: "asc" | "desc";
  }>({
    key: "net_sales",
    direction: "desc",
  });

  const userData = userMeta;

  const isPreviewMode =
    String(month).toUpperCase() === "NA" ||
    String(year).toUpperCase() === "NA";

  // Persist homeCurrency for global
  const [persistedHomeCurrency, setPersistedHomeCurrency] = useState<string>(() => {
    if (typeof window === "undefined") return "";
    return (localStorage.getItem("homeCurrency") || "").toLowerCase();
  });

  useEffect(() => {
    if (!isGlobalPage) return;
    const hc = (homeCurrency || "").toLowerCase();
    if (hc) {
      setPersistedHomeCurrency(hc);
      localStorage.setItem("homeCurrency", hc);
    }
  }, [homeCurrency, isGlobalPage]);

  const effectiveHomeCurrency = isGlobalPage
    ? (homeCurrency || persistedHomeCurrency || "usd").toLowerCase()
    : "";

  const currencySymbol = isGlobalPage
    ? getCurrencySymbol(effectiveHomeCurrency || "usd")
    : getCurrencySymbol(countryName || "");

  const getDisplayProductNameFromRow = useCallback((row: TableRow): string => {
    const productName = (row as any)?.product_name;
    const sku = (row as any)?.sku;

    if (!isMissingName(productName)) return String(productName).trim();
    if (!isMissingName(sku)) return String(sku).trim();

    return "-";
  }, []);

  const aspKey = useMemo(() => {
    const first = tableData[0] || {};
    const k = Object.keys(first).find((key) => key.toLowerCase() === "asp");
    return k as keyof TableRow | undefined;
  }, [tableData]);

  // const getTitle = useCallback(() => `Profit Breakup (SKU Level)`, []);
  const getTitle = useCallback(() => `P&L Productwise Breakdown`, []);

  const getExtraRows = useCallback(() => {
    const formattedCountry = isGlobalPage ? "GLOBAL" : (countryName || "").toUpperCase();
    return [
      [`${userData?.brand_name || "N/A"}`],
      [`${userData?.company_name || "N/A"}`],
      [getTitle()],
      [`Currency:  ${currencySymbol}`],
      [`Country: ${formattedCountry}`],
      [`Platform: Amazon`],
    ];
  }, [countryName, currencySymbol, getTitle, isGlobalPage, userData?.brand_name, userData?.company_name]);

  const computeAspFrom = useCallback((row: Partial<TableRow>) => {
    const sales = toNumber((row as any).net_sales);
    const units = toNumber((row as any).net_units_sold);
    return units > 0 ? sales / units : 0;
  }, []);

  const resolveAspForRow = useCallback(
    (row: Partial<TableRow>, isTotalRow = false) => {
      if (isGlobalPage && isTotalRow) {
        const backendTotalAsp = getOptionalNumber(rawTotalRow, ["asp", "ASP"]);
        if (backendTotalAsp !== null) return backendTotalAsp;
      }

      return computeAspFrom(row);
    },
    [computeAspFrom, isGlobalPage, rawTotalRow]
  );

  const getSortableValue = useCallback((row: TableRow, key: string) => {
    if (key === "net_units_sold") return toNumber(row.net_units_sold);
    if (key === "net_sales") return toNumber(row.net_sales);
    if (key === "profit") return toNumber(row.profit);
    if (key === "other_transactions") return getBackendOtherTransactionsValue(row);

    return toNumber((row as any)[key]);
  }, [getBackendOtherTransactionsValue]);

  const displayRows = useMemo(() => {
    if (!tableData?.length) return [];

    const isTotalRow = (row: TableRow) => {
      const name = String((row as any)?.product_name || "").trim().toLowerCase();
      const sku = String((row as any)?.sku || "").trim().toLowerCase();

      return name === "total" || sku === "total";
    };

    const totalRow = tableData.find(isTotalRow)
      ? ({ ...tableData.find(isTotalRow) } as TableRow)
      : null;

    const productRows = tableData.filter((row) => !isTotalRow(row));

    const sorted = [...productRows].sort((a, b) => {
      const aValue = getSortableValue(a, tableSort.key);
      const bValue = getSortableValue(b, tableSort.key);

      return tableSort.direction === "asc"
        ? aValue - bValue
        : bValue - aValue;
    });

    if (totalRow) {
      totalRow.product_name = "Total";
      totalRow.sku = "Total";
      totalRow.asp = resolveAspForRow(totalRow, true);
    }

    // Expanded: show all rows, no Others row
    if (showAllRows) {
      return totalRow ? [...sorted, totalRow] : sorted;
    }

    // Collapsed: show top 9 + Others + Total
    const topRows = sorted.slice(0, 9);
    const restRows = sorted.slice(9);

    const othersRow: TableRow | null =
      restRows.length > 0
        ? (sumRows(restRows, { product_name: "Others", sku: "-" }) as TableRow)
        : null;

    if (othersRow) {
      othersRow.asp = resolveAspForRow(othersRow);
    }

    const outputRows: TableRow[] = [...topRows];

    if (othersRow) outputRows.push(othersRow);
    if (totalRow) outputRows.push(totalRow);

    return outputRows;
  }, [tableData, tableSort, getSortableValue, showAllRows, resolveAspForRow]);

  const excelRows = useMemo(() => {
    if (!tableData?.length) return [];

    const isTotalRow = (row: TableRow) => {
      const name = String((row as any)?.product_name || "").trim().toLowerCase();
      const sku = String((row as any)?.sku || "").trim().toLowerCase();

      return name === "total" || sku === "total";
    };

    const totalRow = tableData.find(isTotalRow)
      ? ({ ...tableData.find(isTotalRow) } as TableRow)
      : null;

    const productRows = tableData.filter((row) => !isTotalRow(row));

    const sorted = [...productRows].sort((a, b) => {
      const aValue = getSortableValue(a, tableSort.key);
      const bValue = getSortableValue(b, tableSort.key);

      return tableSort.direction === "asc"
        ? aValue - bValue
        : bValue - aValue;
    });

    if (totalRow) {
      totalRow.product_name = "Total";
      totalRow.sku = "Total";
      totalRow.asp = resolveAspForRow(totalRow, true);
    }

    return totalRow ? [...sorted, totalRow] : sorted;
  }, [tableData, tableSort, getSortableValue, resolveAspForRow]);

  const LEFT_COLS: LeafCol<TableRow>[] = useMemo(
    () => [
      {
        key: "sno",
        label: "S.No.",
        align: "center",
        width: "6%",
      },
      {
        key: "product_name",
        label: "Product Name",
        info: <InfoTip text={TERM_DEFINITIONS.product_name} />,
        align: "left",
        width: "17%",
        tdClassName: "!whitespace-normal !text-clip break-words align-middle",
      },
    ],
    []
  );

  const groups = useMemo<ColGroup<TableRow>[]>(() => [
    {
      id: "units_breakdown",
      label: isUsCountry ? "Units" : "Net Units Sold",
      collapsedCols: [
        {
          key: "net_units_sold",
          label: "",
          align: "center",
          width: "7%",
          sortable: true,
        },
      ],
      expandedCols: [
        {
          key: "sku",
          label: "SKU",
          align: "left",
          width: "11%",
          wideMinWidth: 155,
        },
        {
          key: "units_sold",
          label: "Units Sold",
          align: "center",
          width: "7%",
        },
        {
          key: "return_units",
          label: "Return",
          align: "center",
          width: "8%",
        },
        {
          key: "net_units_sold",
          label: isUsCountry ? "Net Units Sold" : "Total",
          align: "center",
          sortable: true,
          width: "7%",
        },
      ],
    },

    {
      id: "sales",
      label: isUsCountry ? "Sales" : "Net Sales",
      info: <InfoTip text={TERM_DEFINITIONS.net_sales} />,
      collapsedCols: [
        {
          key: "net_sales",
          label: "",
          align: "center",
          sortable: true,
          width: "8%",
          info: <InfoTip text={TERM_DEFINITIONS.net_sales} />,
        },
      ],
      expandedCols: isUsCountry
        ? [
          {
            key: "product_sales",
            label: "Gross Sales",
            align: "center" as const,
            width: "8%",
            info: <InfoTip text={TERM_DEFINITIONS.product_sales} />,
          },
          {
            key: "refund_sales",
            label: "Sales - Refund",
            align: "center" as const,
            width: "8%",
            info: <InfoTip text={TERM_DEFINITIONS.refund_sales} />,
          },
          {
            key: "promotional_rebates",
            label: "Promotions",
            align: "center" as const,
            width: "8%",
            info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates} />,
          },
          {
            key: "net_sales",
            label: "Net Sales",
            align: "center" as const,
            sortable: true,
            width: "8%",
          },
        ]
        : [
          {
            key: "product_sales",
            label: "Gross Sales",
            align: "center" as const,
            width: "8%",
            info: <InfoTip text={TERM_DEFINITIONS.product_sales} />,
          },
          {
            key: "refund_sales",
            label: "Sales - Refund",
            align: "center" as const,
            width: "8%",
            info: <InfoTip text={TERM_DEFINITIONS.refund_sales} />,
          },
          {
            key: "tex_and_credits",
            label: "Taxes and Credits",
            align: "center" as const,
            width: "8%",
            info: <InfoTip text={TERM_DEFINITIONS.tex_and_credits} />,
          },
          {
            key: "net_sales",
            label: "Total",
            align: "center" as const,
            sortable: true,
            width: "8%",
          },
        ],
    },

    ...(!isUsCountry
      ? [
        {
          id: "promotional_rebates",
          label: "Promotions",
          info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates} />,
          collapsedCols: [
            {
              key: "promotional_rebates",
              label: "",
              align: "center" as const,
              width: "10%",
              info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates} />,
            },
          ],
          expandedCols: [
            {
              key: "promotional_rebates",
              label: "Promotions",
              align: "center" as const,
              width: "9%",
            },
            {
              key: "promotional_rebates_percentage",
              label: "Promotions %",
              align: "center" as const,
              noWrap: true,
              width: "9%",
              thClassName: "whitespace-nowrap",
            },
          ],
        } as ColGroup<TableRow>,
      ]
      : []),

    {
      id: "amazon_breakdown",
      label: isUsCountry ? "Amazon Fees" : "Marketplace Fees",
      collapsedCols: [
        {
          key: "amazon_fee",
          label: "",
          align: "center",
          width: "8%",
        },
      ],
      expandedCols: [
        {
          key: "selling_fees",
          label: "Selling Fees",
          align: "center",
          width: "8%",
        },
        {
          key: "fba_fees",
          label: "FBA Fees",
          align: "center",
          width: "8%",
        },
        {
          key: "amazon_fee",
          label: isUsCountry ? "Total Fees" : "Total",
          align: "center",
          width: "6%",
        },
      ],
    },

    {
      id: "other_transactions_breakdown",
      label: "Other Transactions",
      collapsedCols: [
        {
          key: "other_transactions",
          label: "",
          align: "center",
          width: "8%",
        },
      ],
      expandedCols: [
        {
          key: "net_taxes",
          label: "Net Taxes",
          info: <InfoTip text={TERM_DEFINITIONS.net_taxes} />,
          align: "center",
          width: "8%",
        },
        {
          key: "net_credits",
          label: "Net Credits",
          info: <InfoTip text={TERM_DEFINITIONS.net_credits} />,
          align: "center",
          width: "8%",
        },
        ...(isUsCountry
          ? [
            {
              key: "misc_transaction",
              label: "Misc. Transactions",
              align: "center" as const,
              width: "8%",
            },
          ]
          : []),
        {
          key: "other_transactions",
          label: "Total",
          align: "center",
          width: "6%",
        },
      ],
    },

    {
      id: "profit_breakdown",
      label: "CM1 Profit",
      info: <InfoTip text={TERM_DEFINITIONS.cm1_profit} />,
      collapsedCols: [
        {
          key: "profit",
          label: "",
          align: "center",
          sortable: true,
          width: "8%",
          info: <InfoTip text={TERM_DEFINITIONS.profit} />,
        },
      ],
      expandedCols: isUsCountry
        ? [
          {
            key: "unit_wise_profitability",
            label: "Per Unit",
            align: "center" as const,
            width: "8%",
          },
          {
            key: "profit_percentage",
            label: "%",
            align: "center" as const,
            width: "8%",
          },
          {
            key: "profit",
            label: "Total",
            align: "center" as const,
            sortable: true,
            width: "8%",
          },
        ]
        : [
          {
            key: "unit_wise_profitability",
            label: "Per Unit",
            align: "center" as const,
            width: "8%",
          },
          {
            key: "profit_percentage",
            label: "%",
            align: "center" as const,
            width: "8%",
          },
          {
            key: "profit",
            label: "Total",
            align: "center" as const,
            sortable: true,
            width: "8%",
          },
        ],
    },

    ...(hasCm2Data
      ? [
        {
          id: "ads_spend_breakdown",
          label: "Ads Spend",
          info: <InfoTip text="Ads Spend" />,
          collapsedCols: [
            {
              key: "ads_spend",
              label: "Total",
              align: "center",
              sortable: true,
              width: "8%",
            },
          ],
          expandedCols: [
            {
              key: "product_spend",
              label: "Sponsored Product",
              align: "center",
              width: "8%",
            },
            {
              key: "display_spend",
              label: "Sponsored Display",
              align: "center",
              width: "8%",
            },
            {
              key: "ads_spend",
              label: "Total",
              align: "center",
              sortable: true,
              width: "8%",
            },
          ],
        } as ColGroup<TableRow>,

        {
          id: "cm2_profit_breakdown",
          label: "CM2 Profit",
          collapsedCols: [
            {
              key: "cm2_profit",
              label: "",
              align: "center",
              sortable: true,
              width: "8%",
            },
          ],
          expandedCols: isUsCountry
            ? [
              {
                key: "unit_wise_cm2_profitability",
                label: "Per Unit",
                align: "center" as const,
                width: "8%",
              },
              {
                key: "cm2_margins",
                label: "%",
                align: "center" as const,
                width: "8%",
              },
              {
                key: "cm2_profit",
                label: "Total",
                align: "center" as const,
                sortable: true,
                width: "8%",
              },
            ]
            : [
              {
                key: "unit_wise_cm2_profitability",
                label: "Per Unit",
                align: "center" as const,
                width: "8%",
              },
              {
                key: "cm2_margins",
                label: "%",
                align: "center" as const,
                width: "8%",
              },
              {
                key: "cm2_profit",
                label: "Total",
                align: "center" as const,
                sortable: true,
                width: "8%",
              },
            ],
        } as ColGroup<TableRow>,
      ]
      : []),
  ], [hasCm2Data, isUsCountry]);

  const anyGroupExpanded = useMemo(() => {
    if (!groups.length) return false;

    return groups.some((group) => {
      return (collapsedGroups?.[group.id] ?? true) === false;
    });
  }, [groups, collapsedGroups]);

  useEffect(() => {
    if (!groups.length) return;

    setCollapsedGroups((prev) => {
      const next: Record<string, boolean> = {};

      groups.forEach((group) => {
        next[group.id] = prev?.[group.id] ?? true;
      });

      return next;
    });

    setAllColumnsExpanded(false);
  }, [groups]);

  const buildAllGroupsCollapsedState = useCallback(
    (collapsedValue: boolean) => {
      const next: Record<string, boolean> = {};

      groups.forEach((group) => {
        next[group.id] = collapsedValue;
      });

      return next;
    },
    [groups]
  );

  const handleToggleAllColumns = useCallback(() => {
    setAllColumnsExpanded((prev) => {
      const nextExpanded = !prev;

      // collapsed = false means expanded
      setCollapsedGroups(buildAllGroupsCollapsedState(!nextExpanded));

      return nextExpanded;
    });
  }, [buildAllGroupsCollapsedState]);

  const SINGLE_COLS: LeafCol<TableRow>[] = useMemo(
    () => [
      {
        key: "asp",
        label: "ASP",
        info: <InfoTip text={TERM_DEFINITIONS.asp} />,
        align: "center",
        width: "7%",
      },
      ...(isUsCountry
        ? [
          {
            key: "promotional_rebates_percentage",
            label: "Promotions %",
            align: "center" as const,
            noWrap: true,
            width: "9%",
            thClassName: "whitespace-nowrap",
          },
        ]
        : []),
      {
        key: "cost_of_unit_sold",
        label: "COGS",
        align: "center",
        width: "7%",
      },
      ...(hasCm2Data
        ? [
          // {
          //   key: "ads_spend",
          //   label: "Ads Spend",
          //   align: "center" as const,
          //   width: "7%",
          // },
          {
            key: "acos",
            label: "ACoS %",
            align: "center" as const,
            width: "8%",
          },
        ]
        : []),
    ],
    [hasCm2Data, isUsCountry]
  );

  const buildExcelColumnsFromUI = useCallback((): LeafCol<TableRow>[] => {
    const cm2Columns: LeafCol<TableRow>[] = hasCm2Data
      ? [
        { key: "product_spend", label: "Sponsored Product", align: "center" as const },
        { key: "display_spend", label: "Sponsored Display", align: "center" as const },
        { key: "ads_spend", label: "Ads Spend", align: "center" as const },
        { key: "acos", label: "ACoS %", align: "center" as const },
        ...(isUsCountry
          ? [
            { key: "cm2_profit", label: "Total", align: "center" as const },
            { key: "unit_wise_cm2_profitability", label: "Per Unit", align: "center" as const },
            { key: "cm2_margins", label: "%", align: "center" as const },
          ]
          : [
            { key: "unit_wise_cm2_profitability", label: "CM2 Profit Per Unit", align: "center" as const },
            { key: "cm2_margins", label: "CM2 Profit %", align: "center" as const },
            { key: "cm2_profit", label: "CM2 Profit", align: "center" as const },
          ]),
      ]
      : [];

    const ordered: LeafCol<TableRow>[] = isUsCountry
      ? [
        { key: "sno", label: "Sno.", align: "center" as const },
        { key: "product_name", label: "Product Name", align: "left" as const },
        { key: "sku", label: "SKU", align: "left" as const },

        { key: "units_sold", label: "Units Sold", align: "center" as const },
        { key: "return_units", label: "Return", align: "center" as const },
        { key: "net_units_sold", label: "Net Units Sold", align: "center" as const },

        { key: "asp", label: "ASP", align: "center" as const },

        { key: "product_sales", label: "Gross Sales", align: "center" as const },
        { key: "refund_sales", label: "Sales - Refund", align: "center" as const },
        { key: "promotional_rebates", label: "Promotions", align: "center" as const },
        { key: "net_sales", label: "Net Sales", align: "center" as const },

        { key: "promotional_rebates_percentage", label: "Promotions %", align: "center" as const },
        { key: "cost_of_unit_sold", label: "COGS", align: "center" as const },

        { key: "selling_fees", label: "Selling Fees", align: "center" as const },
        { key: "fba_fees", label: "FBA Fees", align: "center" as const },
        { key: "amazon_fee", label: "Total Fees", align: "center" as const },

        { key: "net_taxes", label: "Net Taxes", align: "center" as const },
        { key: "net_credits", label: "Net Credits", align: "center" as const },
        { key: "misc_transaction", label: "Misc. Transactions", align: "center" as const },
        { key: "other_transactions", label: "Total", align: "center" as const },

        { key: "profit", label: "Total", align: "center" as const },
        { key: "unit_wise_profitability", label: "Per Unit", align: "center" as const },
        { key: "profit_percentage", label: "%", align: "center" as const },

        ...cm2Columns,
      ]
      : [
        { key: "sno", label: "S. no", align: "center" as const },

        { key: "product_name", label: "Product Name", align: "left" as const },
        { key: "sku", label: "SKU", align: "left" as const },

        { key: "units_sold", label: "Units Sold", align: "center" as const },
        { key: "return_units", label: "Return", align: "center" as const },
        { key: "net_units_sold", label: "Net Units Sold", align: "center" as const },

        { key: "asp", label: "ASP", align: "center" as const },

        { key: "product_sales", label: "Gross Sales", align: "center" as const },
        { key: "refund_sales", label: "Sales - Refund", align: "center" as const },
        { key: "tex_and_credits", label: "Taxes and Credits", align: "center" as const },
        { key: "net_sales", label: "Net Sales", align: "center" as const },

        { key: "promotional_rebates", label: "Promotions", align: "center" as const },
        { key: "promotional_rebates_percentage", label: "Promotions %", align: "center" as const },

        { key: "cost_of_unit_sold", label: "COGS", align: "center" as const },

        { key: "selling_fees", label: "Selling Fees", align: "center" as const },
        { key: "fba_fees", label: "FBA Fees", align: "center" as const },
        { key: "amazon_fee", label: "Marketplace Fees", align: "center" as const },

        { key: "net_taxes", label: "Net Taxes", align: "center" as const },
        { key: "net_credits", label: "Net Credits", align: "center" as const },
        { key: "other_transactions", label: "Other Transactions", align: "center" as const },

        { key: "unit_wise_profitability", label: "CM1 Profit Per Unit", align: "center" as const },
        { key: "profit_percentage", label: "CM1 Profit %", align: "center" as const },

        { key: "profit", label: "CM1 Profit Margin", align: "center" as const },
        ...cm2Columns,
      ];

    // optional: remove duplicates if any
    const seen = new Set<string>();
    return ordered.filter((c) => {
      if (!c.key) return false;
      if (seen.has(c.key)) return false;
      seen.add(c.key);
      return true;
    });
  }, [hasCm2Data, isUsCountry]);


  const INT_KEYS = useMemo(() => new Set(["quantity", "units_sold", "return_units", "net_units_sold"]), []);

  const PRESERVE_SIGN_KEYS = useMemo(() => new Set([
    // CM1
    "profit",
    "profit_percentage",
    "unit_wise_profitability",

    // CM2
    "cm2_profit",
    "cm2_profit_total",
    "unit_wise_cm2_profitability",
    "cm2_margins",

    // Summary ratios
    "net_reimbursement",
    "reimbursement_vs_sales",
    "rembursment_vs_cm2_margins",
    "other_transactions",
    "inventory_charges_and_reimbursement",
    "others",
    "other_adjustment",
  ]), []);

  const formatValue = useCallback(
    (value: unknown, key: string) => {
      if (value === undefined || value === null || value === "") return "-";

      const raw = toNumber(value);
      if (!Number.isFinite(raw)) return "-";

      // keep actual sign for selected fields
      const n = PRESERVE_SIGN_KEYS.has(key) ? raw : Math.abs(raw);

      if (INT_KEYS.has(key)) {
        return Math.trunc(n).toLocaleString();
      }

      const ROUND_KEYS = new Set([
        "product_sales",
        "refund_sales",
        "tex_and_credits",
        "net_sales",
        "promotional_rebates",
        "cost_of_unit_sold",
        "selling_fees",
        "fba_fees",
        "amazon_fee",
        "net_taxes",
        "net_credits",
        "other_transactions",
        "profit",
        "advertising_total",
        "product_spend",
        "display_spend",
        "visible_ads",
        "dealsvouchar_ads",
        "inventory_storage_fees",
        "short_term_storage_fee",
        "long_term_storage_fee",
        "fba_disposal",
        "misc_transaction",
        "shipment_charges",
        "net_reimbursement",
        "cm2_profit",
        "lost_total",
        "ads_spend",
        "brand_spend",
        "cm2_profit_total",
        "debt_payment",
        "disbursement",
        "placement_fees",
        "placement_fee",
        "shipping_charges",
        "shipment_fees",
        "customs_fees",
        "customs_fee",
        "storage_fees",
        "storage_fee",
        "inventory_charges",
        "inventory_charges_and_reimbursement",
        "reimbursement_lost_inventory_amount",
        "platform_management_fees",
        "other_fees",
        "others",
        "other_adjustment",
      ]);

      let formatted;

      if (ROUND_KEYS.has(key)) {
        formatted = Math.round(Math.abs(n)).toLocaleString();
      } else {
        formatted = Math.abs(n).toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });
      }

      const signedFormatted = n < 0 ? `-${formatted}` : formatted;

      if (
        key === "profit_percentage" ||
        key === "cm2_margins" ||
        key === "acos" ||
        key === "reimbursement_vs_sales" ||
        key === "rembursment_vs_cm2_margins"
      ) {
        return `${signedFormatted}%`;
      }

      return signedFormatted;
    },
    [INT_KEYS, PRESERVE_SIGN_KEYS]
  );

  const usSummaryValues = useMemo(() => {
    const placementFees = getOptionalNumber(rawTotalRow, ["placement_fee"]);

    const shipmentFees = getOptionalNumber(rawTotalRow, ["shipment_fees"]);

    const customsFees = getOptionalNumber(rawTotalRow, ["customs_fee"]);

    const shippingChargesTotal = getOptionalNumber(rawTotalRow, ["shipping_charges"]);

    const shortTermStorage = getOptionalNumber(rawTotalRow, [
      "short_term_storage_fee",
      "short_term_storage",
    ]) ?? nonZeroOrNull(totals.short_term_storage_fee);

    const longTermStorage = getOptionalNumber(rawTotalRow, [
      "long_term_storage_fee",
      "long_term_storage",
    ]) ?? nonZeroOrNull(totals.long_term_storage_fee);

    const storageFees =
      getOptionalNumber(rawTotalRow, [
        "storage_fee",
        "storage_fees",
        "storage_fees_total",
        "inventory_storage_fees",
        "platform_fee_inventory_storage",
      ]) ?? sumKnownNumbers(shortTermStorage, longTermStorage);

    const inventoryCharges = getOptionalNumber(rawTotalRow, [
      "inventory_charges",
      "inventory_charge",
      "inventory_fees",
      "inventory_fee",
      "fba_disposal",
    ]) ?? nonZeroOrNull(totals.fba_disposal);

    const reimbursementForLostInventory = getOptionalNumber(rawTotalRow, [
      "reimbursement_lost_inventory_amount",
      "lost_inventory_reimbursement",
      "lost_total",
    ]) ?? nonZeroOrNull(totals.reimbursement_lost_inventory_amount || totals.lost_total);

    const inventoryChargesAndReimbursement =
      getOptionalNumber(rawTotalRow, [
        "inventory_charges_and_reimbursement",
        "inventory_charges_reimbursement",
        "inventory_charges_reimbursement_total",
      ]) ??
      (
        inventoryCharges !== null && reimbursementForLostInventory !== null
          ? inventoryCharges - reimbursementForLostInventory
          : null
      );

    const platformManagementFees = getOptionalNumber(rawTotalRow, [
      "platform_management_fees",
      "platform_management_fee",
      "platformfeenew",
    ]) ?? nonZeroOrNull(totals.platform_fee);

    const otherAdjustment = getOptionalNumber(rawTotalRow, [
      "other_adjustment",
    ]);

    return {
      placementFees,
      shippingCharges: shipmentFees,
      customsFees,
      shippingChargesTotal,
      shortTermStorage,
      longTermStorage,
      storageFees,
      inventoryCharges,
      reimbursementForLostInventory,
      inventoryChargesAndReimbursement,
      platformManagementFees,
      otherAdjustment,
    };
  }, [rawTotalRow, totals]);

  const formatSummaryValueOrDash = useCallback(
    (value: number | null, key: string) => {
      return value === null ? "-" : formatValue(value, key);
    },
    [formatValue]
  );


  // Sign row (stable sets)
  const SIGN_PLUS = useMemo(
    () =>
      new Set([
        "units_sold",          // Units Sold  (+)
        // Net Units Sold (+)
        "product_sales",       // Gross Sales (+)
        "net_sales",           // Net Sales (+)
        "net_credits",         // Net Credits (+)
        "misc_transaction",
        // "other_transactions",
      ]),
    []
  );

  const SIGN_MINUS = useMemo(
    () =>
      new Set([
        "return_units",        // Return (-)
        "refund_sales",        // Sales - Refund (-)
        "tex_and_credits",     // Taxes and Credits (-)

        "cost_of_unit_sold",   // COGS (-)
        "selling_fees",        // Selling Fees (-)
        "fba_fees",            // FBA Fees (-)
        "amazon_fee",          // Platform Fees (-)
        "shipment_charges",
        "shipment_fees",
        "promotional_rebates", // Promotions (-)
        "platformfeenew",
        "platform_fee_inventory_storage",
        "short_term_storage_fee",
        "long_term_storage_fee",
        "fba_disposal",
        "net_taxes",
        "lost_total",
        "advertising_total",
        "ads_spend",
        "product_spend",
        "display_spend",
        "brand_spend",
      ]),
    []
  );


  const getSignForCol = useCallback(
    (colKey: string) => {
      if (isUsCountry && (colKey === "net_units_sold" || colKey === "other_transactions")) {
        return { text: "(+)", className: "text-green-700" };
      }

      if (SIGN_PLUS.has(colKey)) return { text: "(+)", className: "text-green-700" };
      if (SIGN_MINUS.has(colKey)) return { text: "(-)", className: "text-[#ff5c5c]" };
      return null;
    },
    [SIGN_PLUS, SIGN_MINUS, isUsCountry]
  );

  const buildSkuSheetModel = useCallback(
    (opts?: { allRows?: boolean }) => {
      const excelCols = buildExcelColumnsFromUI();
      const colKeys = excelCols.map((c) => c.key);

      const headerRow: Record<string, string> = {};
      excelCols.forEach((c) => {
        headerRow[c.key] = c.excelLabel ?? (typeof c.label === "string" ? c.label : c.key);

      });

      const signRow: Record<string, string> = {};
      colKeys.forEach((k) => {
        const s = getSignForCol(k);
        if (s?.text) signRow[k] = s.text;
      });

      const positiveExpenseExportKeys = new Set([
        "promotional_rebates",
        "promotional_rebates_percentage",
        "selling_fees",
        "fba_fees",
        "net_taxes",
        "misc_transaction",
        "placement_fee",
        "customs_fee",
        "inventory_charges_and_reimbursement",
        "fba_disposal",
      ]);

      const sourceRows = excelRows;

      const rowsForExcel = sourceRows.map((row, rowIndex) => {
        const out: Record<string, string | number> = {};

        colKeys.forEach((key) => {
          let value: any = (row as any)[key];

          if (key === "sno") {
            const name = getDisplayProductNameFromRow(row);
            const isTotal = String(name).trim().toLowerCase() === "total";
            value = isTotal ? "" : rowIndex + 1;
          }

          if (key === "product_name") value = getDisplayProductNameFromRow(row);
          if (key === "product_sales") {
            value = row.product_sales ?? (row as any).gross_sales ?? 0;
          }

          if (key === "other_transactions") {
            value = getBackendOtherTransactionsValue(row);
          }

          if (key === "quantity") {
            value = row.quantity ?? row.units_sold ?? 0;
          }

          // ✅ Ads Spend expanded values for Excel
          if (key === "product_spend") {
            value =
              (row as any).product_spend ??
              (row as any).sponsored_product ??
              0;
          }

          if (key === "display_spend") {
            value =
              (row as any).display_spend ??
              (row as any).sponsored_display ??
              0;
          }

          if (key === "ads_spend") {
            value =
              (row as any).ads_spend ??
              (row as any).advertising_total ??
              (
                toNumber((row as any).product_spend ?? (row as any).sponsored_product ?? 0) +
                toNumber((row as any).display_spend ?? (row as any).sponsored_display ?? 0)
              );
          }

          // ✅ ACoS should use the same calculation as UI
          if (key === "acos") {
            value = getAcosPercentage(row);
          }

          // ✅ CM2 values should match UI
          if (key === "unit_wise_cm2_profitability") {
            value = getCm2PerUnit(row);
          }

          if (key === "cm2_margins") {
            value = getCm2Percentage(row);
          }

          if (positiveExpenseExportKeys.has(key)) {
            value = Math.abs(toNumber(value));
          }

          // Keep Global total ASP aligned with the backend summary value.
          if (key === "asp") {
            const isTotalAspRow =
              String(getDisplayProductNameFromRow(row)).trim().toLowerCase() === "total" ||
              String((row as any)?.sku || "").trim().toLowerCase() === "total";

            value = resolveAspForRow(row, isTotalAspRow);
          }

          if (typeof value === "number") {
            if (Math.abs(value) < 1e-10) value = 0;

            if (["sno", "units_sold", "return_units", "net_units_sold", "quantity"].includes(key)) {
              value = Math.trunc(value);
            } else {
              value = Number(value.toFixed(2));
            }
          }
          if (
            [
              "profit_percentage",
              "promotional_rebates_percentage",
              "acos",
              "cm2_margins",
            ].includes(key) &&
            typeof value === "number"
          ) {
            value = Number(value) / 100;
          }

          out[key] = typeof value === "number" && isNaN(value) ? "-" : value;
        });

        return out;
      });


      type SummaryRow = Record<string, string | number | boolean | undefined> & {
        __bold?: boolean;
      };

      const summaryValueColumnKey = hasCm2Data ? "cm2_profit" : "profit";
      const summaryDash = "-";
      const summaryExportValue = (value: number | null) => value === null ? summaryDash : value;
      const positiveSummaryExportValue = (value: number | null) =>
        value === null ? summaryDash : Math.abs(value);
      const spacerSummaryRow = (): SummaryRow => ({
        product_name: "",
        [summaryValueColumnKey]: "",
      });
      const otherFeesTotal = differenceKnownNumbers(
        usSummaryValues.platformManagementFees,
        usSummaryValues.otherAdjustment
      );

      const summaryRows: SummaryRow[] = isUsCountry
        ? [
          {
            product_name: "Cost of Advertisement (-)",
            [summaryValueColumnKey]: Math.abs(Number(costOfAdvertisementValue || 0)),
          },
          {
            product_name: "Visibility - Ads (-)",
            [summaryValueColumnKey]: Math.abs(Number(visibilityAdsValue || 0)),
          },
          {
            product_name: "Visibility - Deals, Vouchers and Reviews (-)",
            [summaryValueColumnKey]: Math.abs(Number(totals.dealsvouchar_ads || 0)),
          },
          spacerSummaryRow(),

          {
            product_name: "Shipping Charges (-)",
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.shippingChargesTotal),
          },
          {
            product_name: "Placement Fees (-)",
            [summaryValueColumnKey]: positiveSummaryExportValue(usSummaryValues.placementFees),
          },
          {
            product_name: "Shipping Charges (-)",
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.shippingCharges),
          },
          {
            product_name: "Customs Fees (-)",
            [summaryValueColumnKey]: positiveSummaryExportValue(usSummaryValues.customsFees),
          },
          spacerSummaryRow(),

          {
            product_name: "Storage Fees (-)",
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.storageFees),
          },
          {
            product_name: "Short Term Storage (-)",
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.shortTermStorage),
          },
          {
            product_name: "Long Term Storage (-)",
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.longTermStorage),
          },
          spacerSummaryRow(),

          {
            product_name: "Inventory Charges and Reimbursement",
            [summaryValueColumnKey]: positiveSummaryExportValue(usSummaryValues.inventoryChargesAndReimbursement),
          },
          {
            product_name: "Inventory Charges (-)",
            [summaryValueColumnKey]: positiveSummaryExportValue(usSummaryValues.inventoryCharges),
          },
          {
            product_name: `Reimbursement for lost Inventory${totals.reimbursement_lost_inventory_units
              ? ` - ${totals.reimbursement_lost_inventory_units} Units`
              : ""} (+)`,
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.reimbursementForLostInventory),
          },
          spacerSummaryRow(),

          {
            product_name: "Other Fees (-)",
            [summaryValueColumnKey]: summaryExportValue(otherFeesTotal),
          },
          {
            product_name: "  Platform Management Fees (-)",
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.platformManagementFees),
          },
          {
            product_name: "  Others (+)",
            [summaryValueColumnKey]: summaryExportValue(usSummaryValues.otherAdjustment),
          },
          spacerSummaryRow(),

          {
            product_name: "CM2 Profit",
            [summaryValueColumnKey]: Number(totals.total_cm2_profit || 0),
            __bold: true,
          },
          spacerSummaryRow(),
          {
            product_name: "CM2 Profit %",
            [summaryValueColumnKey]: Number(totals.cm2_margins || 0),
          },
          {
            product_name: "TACoS (Total Advertising Cost of Sale)",
            [summaryValueColumnKey]: frontendTacos,
            __bold: true,
          },
          {
            product_name: "Net Reimbursement",
            [summaryValueColumnKey]: toNumber(totals.net_reimbursement),
          },
          {
            product_name: "Reimbursement vs Sales",
            [summaryValueColumnKey]: Number(totals.reimbursement_vs_sales || 0),
          },
          {
            product_name: "Reimbursement vs CM2 Margins",
            [summaryValueColumnKey]: Number(totals.rembursment_vs_cm2_margins || 0),
          },
        ]
        : [
          {
            product_name: "Cost of Advertisement",
            [summaryValueColumnKey]: Math.abs(Number(costOfAdvertisementValue || 0)),
          },
          {
            product_name: "Visibility - Ads (-)",
            [summaryValueColumnKey]: Math.abs(Number(visibilityAdsValue || 0)),
          },
          {
            product_name: "Visibility - Deals, Vouchers and Reviews (-)",
            [summaryValueColumnKey]: Math.abs(Number(totals.dealsvouchar_ads || 0)),
          },

          ...((countryName || "").toLowerCase() === "global"
            ? [
              {
                product_name: "Shipment Charges (-)",
                [summaryValueColumnKey]: Math.abs(Number(totals.shipment_charges || 0)),
              },
            ]
            : []),

          {
            product_name: "Other Transactions",
            [summaryValueColumnKey]: Number(totals.other_transactions || 0),
          },
          {
            product_name: "Short Term Storage Fee (-)",
            [summaryValueColumnKey]: Math.abs(Number(totals.short_term_storage_fee || 0)),
          },
          {
            product_name: "Long Term Storage Fee (-)",
            [summaryValueColumnKey]: Math.abs(Number(totals.long_term_storage_fee || 0)),
          },
          {
            product_name: "FBA Disposal (-)",
            [summaryValueColumnKey]: Math.abs(Number(totals.fba_disposal || 0)),
          },
          {
            product_name: "Reimbursement for lost Inventory (+)",
            [summaryValueColumnKey]: Math.abs(Number(totals.lost_total || 0)),
          },
          {
            product_name: "Misc. Transactions (+)",
            [summaryValueColumnKey]: Math.abs(Number(totals.misc_transaction || 0)),
          },
          {
            product_name: "Other Platform Fees (-)",
            [summaryValueColumnKey]: Math.abs(Number(totals.platform_fee || 0)),
          },

          {
            product_name: "CM2 Profit/Loss",
            [summaryValueColumnKey]: Number(totals.total_cm2_profit || 0),
            __bold: true,
          },
          {
            product_name: "CM2 Margins",
            [summaryValueColumnKey]: Number(totals.cm2_margins || 0),
          },
          {
            product_name: "TACoS (Total Advertising Cost of Sale)",
            [summaryValueColumnKey]: frontendTacos,
            __bold: true,
          },
          {
            product_name: "Net Reimbursement",
            [summaryValueColumnKey]: toNumber(totals.net_reimbursement),
          },
          {
            product_name: "Reimbursement vs CM2 Margins",
            [summaryValueColumnKey]: Number(totals.rembursment_vs_cm2_margins || 0),
          },
          {
            product_name: "Reimbursement vs Sales",
            [summaryValueColumnKey]: Number(totals.reimbursement_vs_sales || 0),
          },
        ];

      const normalizedSummaryRows =
        summaryRows.map((r) => ({
          ...r,
          __bold: r.__bold ? true : undefined,
        })) as Array<
          Record<string, string | number> & { __bold?: boolean }
        >;

      const formats: Record<string, "int" | "money" | "percent" | "text"> = {};

      colKeys.forEach((k) => {
        if (k === "product_name" || k === "sku") {
          formats[k] = "text";
        } else if (
          [
            "profit_percentage",
            "promotional_rebates_percentage",
            "acos",
            "cm2_margins",
          ].includes(k)
        ) {
          formats[k] = "percent";
        } else if (
          ["sno", "units_sold", "return_units", "net_units_sold", "quantity"].includes(k)
        ) {
          formats[k] = "int";
        } else {
          formats[k] = "money";
        }
      });

      return {
        columns: colKeys as readonly string[],
        extraRows: getExtraRows(),
        headerRow,
        signRow,
        rows: rowsForExcel,
        summaryRows: normalizedSummaryRows,
        formats,
        summaryValueKey: hasCm2Data ? "cm2_profit" : "profit",
      };
    },
    [
      excelRows,
      totals,
      frontendTacos,
      costOfAdvertisementValue,
      countryName,
      hasCm2Data,
      isUsCountry,
      usSummaryValues,
      visibilityAdsValue,
      getExtraRows,
      getDisplayProductNameFromRow,
      buildExcelColumnsFromUI,
      getSignForCol,
      getBackendOtherTransactionsValue,
      resolveAspForRow,
    ]
  );


  // Period label
  const yearShort = typeof year === "string" ? year.toString().slice(-2) : String(year).slice(-2);

  const periodLabel =
    range === "monthly"
      ? `SKU-wise Profitability-${convertToAbbreviatedMonth(month)}'${yearShort}`
      : range === "quarterly"
        ? `SKU-wise Profitability-${quarter}'${yearShort}`
        : `SKU-wise Profitability-Year'${yearShort}`;

  // Dummy table data

  const CustomModal: React.FC<React.PropsWithChildren<{ onClose: () => void }>> = ({ onClose, children }) => {
    return (
      <div onClick={onClose} className="fixed inset-0 z-[1000] flex items-center justify-center bg-black/50">
        <div
          onClick={(e) => e.stopPropagation()}
          className="relative flex h-[30vh] w-[30vw] flex-col items-center justify-between overflow-y-auto rounded-lg bg-white p-4"
        >
          <div className="flex flex-1 flex-col items-center justify-center">{children}</div>
        </div>
      </div>
    );
  };

  /* --------- Fetch user data --------- */

  useEffect(() => {
    if (!tableData || tableData.length === 0) return;

    onExportPayloadChange?.({
      tableData,
      totals,
      currencySymbol,
      brandName: userData?.brand_name,
      companyName: userData?.company_name,
      title: "Profit Breakup (SKU Level)",
      periodLabel,
      range,
      countryName,
      sheetModel: buildSkuSheetModel({ allRows: true }),
    });
  }, [tableData, totals, currencySymbol, userData, periodLabel, range, countryName, onExportPayloadChange, buildSkuSheetModel]);


  const rowsExcludingSpecial = (rows: TableRow[]) =>
    rows.filter((r) => {
      const name = String(r.product_name ?? "").trim().toLowerCase();
      const display = String(getDisplayProductNameFromRow(r)).trim().toLowerCase();
      return name !== "total" && name !== "others" && display !== "total" && display !== "others";
    });

  const buildTopBottom = useCallback(
    (data: TableRow[], mode: "top" | "bottom"): TopBottomData => {
      const rows = rowsExcludingSpecial(data);

      const getProfitValue = (row: TableRow) => {
        return hasCm2Data
          ? toNumber(row.cm2_profit ?? row.cm2_profit_total)
          : toNumber(row.profit);
      };

      const getPerUnitValue = (row: TableRow) => {
        const units = toNumber(row.net_units_sold ?? row.total_quantity);

        if (units <= 0) return 0;

        return hasCm2Data
          ? getProfitValue(row) / units
          : toNumber(row.profit) / units;
      };

      const sorted = [...rows].sort((a, b) => {
        const ap = getProfitValue(a);
        const bp = getProfitValue(b);

        return mode === "top" ? bp - ap : ap - bp;
      });

      const pick = sorted.slice(0, 5);

      const totalProfit = pick.reduce((s, r) => s + getProfitValue(r), 0);
      const totalProfitMix = pick.reduce((s, r) => s + toNumber(r.profit_mix), 0);
      const totalSalesMix = pick.reduce((s, r) => s + toNumber(r.sales_mix), 0);

      const totalNetUnits = pick.reduce(
        (s, r) => s + toNumber(r.net_units_sold ?? r.total_quantity),
        0
      );

      const avgPerUnit = totalNetUnits > 0 ? totalProfit / totalNetUnits : 0;

      const formatted = pick.map((item) => {
        const profitValue = getProfitValue(item);
        const perUnitValue = getPerUnitValue(item);

        return {
          product_name: getDisplayProductNameFromRow(item),
          profit: profitValue.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          }),
          profitMix: toNumber(item.profit_mix).toFixed(2),
          salesMix: toNumber(item.sales_mix).toFixed(2),
          per_unit: perUnitValue.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          }),
        };
      });

      return {
        rows: formatted,
        totals: {
          profit: totalProfit.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          }),
          profitMix: totalProfitMix.toFixed(2),
          salesMix: totalSalesMix.toFixed(2),
          avg_per_unit: avgPerUnit.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          }),
        },
      };
    },
    [getDisplayProductNameFromRow, hasCm2Data]
  );

  const topData = useMemo(() => buildTopBottom(tableData, "top"), [tableData, buildTopBottom]);
  const bottomData = useMemo(() => buildTopBottom(tableData, "bottom"), [tableData, buildTopBottom]);

  /* --------- UI handlers --------- */
  const handleProductClick = useCallback(
    (row: TableRow) => {
      const cleanProduct = getDisplayProductNameFromRow(row).trim();
      const cleanSku = !isMissingName(row?.sku) ? String(row.sku).trim() : "";

      if (!cleanProduct || cleanProduct === "-") return;

      onProductDetailClick?.(cleanProduct, cleanSku);
    },
    [onProductDetailClick, getDisplayProductNameFromRow]
  );

  const handleDownloadExcel = useCallback(async () => {
    const model = buildSkuSheetModel({ allRows: true });

    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("SKU Profitability");

    buildSkuWorksheetFromModel(ws, model);

    const formattedMonthYear =
      range === "monthly"
        ? `${convertToAbbreviatedMonth(month)}'${yearShort}`
        : range === "quarterly"
          ? `${quarter}'${yearShort}`
          : `${year}`;

    const filename = `Amazon-PnL-${formattedMonthYear}.xlsx`;

    await downloadWorkbookAsXlsx(wb, filename);
  }, [buildSkuSheetModel, range, month, quarter, year, yearShort, onDownload]);


  /* --------- Render guards --------- */
  const productRowCount = useMemo(() => {
    return (tableData || []).filter((row) => {
      const name = String((row as any)?.product_name || "").trim().toLowerCase();
      const sku = String((row as any)?.sku || "").trim().toLowerCase();

      return (
        name !== "total" &&
        sku !== "total" &&
        name !== "others" &&
        sku !== "others"
      );
    }).length;
  }, [tableData]);

  /* --------- Render guards --------- */
  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-center">
        <Loader fullscreen transparent />
      </div>
    );
  }

  if (error) {
    return <div className="text-red-600">Error: {error}</div>;
  }

  const renderNetSalesDelta = (row: TableRow) => {
    if (
      row.net_sales_delta_percentage === undefined ||
      row.net_sales_delta_percentage === null
    ) {
      return null;
    }

    const rawPct = toNumber(row.net_sales_delta_percentage);
    const isPositive = rawPct >= 0;

    return (
      <span
        className={`shrink-0 whitespace-nowrap text-[11px] min-[1700px]:text-xs font-semibold ${isPositive ? "text-[#5EA68E]" : "text-[#FF5C5C]"
          }`}
        title={`Previous Net Sales: ${formatValue(
          row.previous_net_sales,
          "net_sales"
        )}`}
      >
        {isPositive ? "▲" : "▼"} {Math.abs(rawPct).toFixed(2)}%
      </span>
    );
  };

  // const VISIBLE_PRODUCT_ROWS = 13.61;

  // const SIGN_ROW_HEIGHT = 30;
  // const PRODUCT_ROW_HEIGHT = 35;

  // const shouldScrollTable = showAllRows && productRowCount > VISIBLE_PRODUCT_ROWS;

  // const tableScrollHeight =
  //   SIGN_ROW_HEIGHT +
  //   PRODUCT_ROW_HEIGHT * VISIBLE_PRODUCT_ROWS;



  const COLLAPSED_VISIBLE_PRODUCT_ROWS = 5.4;
  // 9 product rows + Others row

  const SIGN_ROW_HEIGHT = 46;
  const PRODUCT_ROW_HEIGHT = 46;
  const TOTAL_ROW_HEIGHT = 52;
  const SUMMARY_ROW_HEIGHT = 48;

  // const COLLAPSED_SUMMARY_ROW_COUNT =
  //   (countryName || "").toLowerCase() === "us" ||
  //     (countryName || "").toLowerCase() === "global"
  //     ? 9
  //     : 8;

  const COLLAPSED_SUMMARY_ROW_COUNT =
    (countryName || "").toLowerCase() === "us" ||
      (countryName || "").toLowerCase() === "global"
      ? 12
      : 11;

  const shouldScrollTable =
    productRowCount > COLLAPSED_VISIBLE_PRODUCT_ROWS;

  const tableScrollHeight =
    SIGN_ROW_HEIGHT +
    PRODUCT_ROW_HEIGHT * COLLAPSED_VISIBLE_PRODUCT_ROWS +
    TOTAL_ROW_HEIGHT +
    SUMMARY_ROW_HEIGHT * COLLAPSED_SUMMARY_ROW_COUNT;

  const usOtherFees = differenceKnownNumbers(
    usSummaryValues.platformManagementFees,
    usSummaryValues.otherAdjustment
  );

  const skuSummaryRows = isUsCountry
    ? [
      {
        type: "section" as const,
        id: "ads",
        label: <>Cost of Advertisement <strong className="text-[#ff5c5c]">(-)</strong></>,
        endValue: formatValue(costOfAdvertisementValue, "advertising_total"),
        defaultCollapsed: true,
        children: [
          {
            id: "ads_1",
            label: <>Visibility - Ads <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(visibilityAdsValue, "brand_spend"),
          },
          {
            id: "ads_2",
            label: <>Visibility - Deals, Vouchers and Reviews <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(totals.dealsvouchar_ads, "dealsvouchar_ads"),
          },
        ],
      },
      {
        type: "section" as const,
        id: "shipping_charges",
        label: <>Shipping Charges <strong className="text-[#ff5c5c]">(-)</strong></>,
        endValue: formatSummaryValueOrDash(usSummaryValues.shippingChargesTotal, "shipping_charges"),
        defaultCollapsed: true,
        children: [
          {
            id: "placement_fees",
            label: <>Placement Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatSummaryValueOrDash(usSummaryValues.placementFees, "placement_fee"),
          },
          {
            id: "shipping_charges_child",
            label: <>Shipping Charges <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatSummaryValueOrDash(usSummaryValues.shippingCharges, "shipment_fees"),
          },
          {
            id: "customs_fees",
            label: <>Customs Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatSummaryValueOrDash(usSummaryValues.customsFees, "customs_fee"),
          },
        ],
      },
      {
        type: "section" as const,
        id: "storage_fee",
        label: <>Storage Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
        endValue: formatSummaryValueOrDash(usSummaryValues.storageFees, "storage_fee"),
        defaultCollapsed: true,
        children: [
          {
            id: "short_term_storage_fee",
            label: <>Short Term Storage <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatSummaryValueOrDash(usSummaryValues.shortTermStorage, "short_term_storage_fee"),
          },
          {
            id: "long_term_storage_fee",
            label: <>Long Term Storage <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatSummaryValueOrDash(usSummaryValues.longTermStorage, "long_term_storage_fee"),
          },
        ],
      },
      {
        type: "section" as const,
        id: "inventory_charges_reimbursement",
        label: "Inventory Charges and Reimbursement",
        endValue: formatSummaryValueOrDash(
          usSummaryValues.inventoryChargesAndReimbursement,
          "inventory_charges_and_reimbursement"
        ),
        defaultCollapsed: true,
        children: [
          {
            id: "inventory_charges",
            label: <>Inventory Charges <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatSummaryValueOrDash(usSummaryValues.inventoryCharges, "inventory_charges"),
          },
          {
            id: "lost_inventory_reimbursement",
            label: (
              <>
                Reimbursement for lost Inventory
                {totals.reimbursement_lost_inventory_units
                  ? ` - ${totals.reimbursement_lost_inventory_units} Units `
                  : " "}
                <strong className="text-green-500">(+)</strong>
              </>
            ),
            midValue: formatSummaryValueOrDash(
              usSummaryValues.reimbursementForLostInventory,
              "reimbursement_lost_inventory_amount"
            ),
          },
        ],
      },
      {
        type: "section" as const,
        id: "other_fees",
        label: <>Other Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
        endValue: formatSummaryValueOrDash(usOtherFees, "other_fees"),
        defaultCollapsed: true,
        children: [
          {
            id: "platform_management_fees",
            label: <>Platform Management Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatSummaryValueOrDash(
              usSummaryValues.platformManagementFees,
              "platform_management_fees"
            ),
          },
          {
            id: "others",
            label: <>Others <strong className="text-green-500">(+)</strong></>,
            midValue: formatSummaryValueOrDash(usSummaryValues.otherAdjustment, "other_adjustment"),
          },
        ],
      },
      {
        type: "fixed" as const,
        id: "cm2_profit",
        label: "CM2 Profit",
        endValue: formatValue(totals.total_cm2_profit, "cm2_profit"),
        bold: true,
      },
      {
        type: "fixed" as const,
        id: "cm2_profit_percentage",
        label: "CM2 Profit %",
        endValue: `${formatValue(totals.cm2_margins, "cm2_margins")}`,
      },
      {
        type: "fixed" as const,
        id: "tacos",
        label: "TACoS (Total Advertising Cost of Sale)",
        endValue: `${formatValue(frontendTacos, "acos")}`,
        bold: true,
      },
      {
        type: "section" as const,
        id: "net_reimb",
        label: "Net Reimbursement",
        endValue: formatValue(Math.abs(totals.net_reimbursement), "net_reimbursement"),
        defaultCollapsed: true,
        children: [
          {
            id: "net_reimb_debt_payment",
            label: (
              <>
                Charged <strong className="text-[#ff5c5c]">(-)</strong>
              </>
            ),
            midValue: formatValue(totals.debt_payment, "debt_payment"),
          },
          {
            id: "net_reimb_disbursement",
            label: (
              <>
                Disbursement <strong className="text-green-500">(+)</strong>
              </>
            ),
            midValue: formatValue(totals.disbursement, "disbursement"),
          },
        ],
      },
      {
        type: "fixed" as const,
        id: "rv_sales",
        label: "Reimbursement vs Sales",
        endValue: `${formatValue(totals.reimbursement_vs_sales, "reimbursement_vs_sales")}`,
      },
      {
        type: "fixed" as const,
        id: "rv_cm2",
        label: "Reimbursement vs CM2 Margins",
        endValue: `${formatValue(totals.rembursment_vs_cm2_margins, "rembursment_vs_cm2_margins")}`,
      },
    ]
    : [
      {
        type: "section" as const,
        id: "ads",
        label: <>Cost of Advertisement <strong className="text-[#ff5c5c]">(-)</strong></>,
        endValue: formatValue(costOfAdvertisementValue, "advertising_total"),
        defaultCollapsed: true,
        children: [
          {
            id: "ads_1",
            label: <>Visibility - Ads <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(visibilityAdsValue, "brand_spend"),
          },
          {
            id: "ads_2",
            label: <>Visibility - Deals, Vouchers and Reviews <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(totals.dealsvouchar_ads, "dealsvouchar_ads"),
          },
        ],
      },
      {
        type: "section" as const,
        id: "other",
        label: "Other Transactions",
        endValue: formatValue(totals.other_transactions, "other_transactions"),
        defaultCollapsed: true,
        children: [
          {
            id: "short_term_storage_fee",
            label: <>Short Term Storage Fee <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(totals.short_term_storage_fee, "short_term_storage_fee"),
          },
          {
            id: "long_term_storage_fee",
            label: <>Long Term Storage Fee <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(totals.long_term_storage_fee, "long_term_storage_fee"),
          },
          {
            id: "fba_disposal",
            label: <>FBA Disposal <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(totals.fba_disposal, "fba_disposal"),
          },
          {
            id: "other_3",
            label: (
              <>
                Reimbursement for lost Inventory
                {totals.reimbursement_lost_inventory_units
                  ? ` - ${totals.reimbursement_lost_inventory_units} Units `
                  : " "}
                <strong className="text-green-500">(+)</strong>
              </>
            ),
            midValue: formatValue(totals.lost_total, "lost_total"),
          },
          {
            id: "other_misc",
            label: <>Misc. Transactions <strong className="text-green-500">(+)</strong></>,
            midValue: formatValue(totals.misc_transaction, "misc_transaction"),
          },
          {
            id: "other_1",
            label: <>Other Platform Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
            midValue: formatValue(totals.platform_fee, "platform_fee"),
          },
        ],
      },
      ...((countryName || "").toLowerCase() === "global"
        ? [
          {
            type: "fixed" as const,
            id: "ship",
            label: <>Shipment Charges <strong className="text-[#ff5c5c]">(-)</strong></>,
            endValue: formatValue(totals.shipment_charges, "shipment_charges"),
          },
        ]
        : []),
      {
        type: "fixed" as const,
        id: "cm2_profit",
        label: "CM2 Profit/Loss",
        endValue: formatValue(totals.total_cm2_profit, "cm2_profit"),
        bold: true,
      },
      {
        type: "fixed" as const,
        id: "cm2_margins",
        label: "CM2 Margins",
        endValue: `${formatValue(totals.cm2_margins, "cm2_margins")}`,
      },
      {
        type: "fixed" as const,
        id: "tacos",
        label: "TACoS (Total Advertising Cost of Sale)",
        endValue: `${formatValue(frontendTacos, "acos")}`,
        bold: true,
      },
      {
        type: "section" as const,
        id: "net_reimb",
        label: "Net Reimbursement",
        endValue: formatValue(Math.abs(totals.net_reimbursement), "net_reimbursement"),
        defaultCollapsed: true,
        children: [
          {
            id: "net_reimb_debt_payment",
            label: (
              <>
                Charged <strong className="text-[#ff5c5c]">(-)</strong>
              </>
            ),
            midValue: formatValue(totals.debt_payment, "debt_payment"),
          },
          {
            id: "net_reimb_disbursement",
            label: (
              <>
                Disbursement <strong className="text-green-500">(+)</strong>
              </>
            ),
            midValue: formatValue(totals.disbursement, "disbursement"),
          },
        ],
      },
      {
        type: "fixed" as const,
        id: "rv_cm2",
        label: "Reimbursement vs CM2 Margins",
        endValue: `${formatValue(totals.rembursment_vs_cm2_margins, "rembursment_vs_cm2_margins")}`,
      },
      {
        type: "fixed" as const,
        id: "rv_sales",
        label: "Reimbursement vs Sales",
        endValue: `${formatValue(totals.reimbursement_vs_sales, "reimbursement_vs_sales")}`,
      },
    ];

  return (
    <>
      <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
        <div className="mb-4 flex  gap-3 flex-row items-center justify-between">
          <div className="flex flex-wrap items-baseline gap-0 sm:gap-2 justify-left sm:justify-start">
            <PageBreadcrumb pageTitle={getTitle()} variant="page" textSize="2xl" />
            <span className="text-[#5EA68E] text-base sm:text-lg lg:text-lg 2xl:text-xl font-bold">({currencySymbol})</span>
          </div>

          <div className="flex flex-wrap items-center justify-center gap-2 sm:justify-end">
            <button
              type="button"
              onClick={() => setShowAllRows((prev) => !prev)}
              title={showAllRows ? "Collapse rows" : "Expand all rows"}
              aria-label={showAllRows ? "Collapse rows" : "Expand all rows"}
              className="inline-flex rounded-md border border-gray-300 bg-white p-1.5 text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
            >
              {showAllRows ? (
                <RiCollapseDiagonalFill size={18} className="font-extrabold" />
              ) : (
                <RiExpandDiagonalFill size={18} className="font-extrabold" />
              )}
            </button>

            <button
              type="button"
              onClick={handleToggleAllColumns}
              title={allColumnsExpanded ? "Collapse all columns" : "Expand all columns"}
              aria-label={allColumnsExpanded ? "Collapse all columns" : "Expand all columns"}
              className="inline-flex rounded-md border border-gray-300 bg-white p-1.5 text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
            >
              {allColumnsExpanded ? (
                <RiLayoutColumnLine size={18} className="font-extrabold" />
              ) : (
                <RiLayoutColumnFill size={18} className="font-extrabold" />
              )}
            </button>

            {!hideDownloadButton && <DownloadIconButton onClick={handleDownloadExcel} />}
          </div>
        </div>

        <div
          className={`transition-opacity opacity-100
            }`}
        >
          {showModal2 && (
            <CustomModal onClose={() => setShowModal2(false)}>
              <SkuMultiCountryUpload onClose={() => setShowModal2(false)} onComplete={() => setShowModal2(false)} />
            </CustomModal>
          )}

          <div
            className={[
              "w-full rounded-xl border border-gray-300",
              shouldScrollTable
                ? "overflow-hidden"
                : "overflow-x-auto overflow-y-hidden",
            ].join(" ")}
          >
            <div className="w-full">
              <GroupedCollapsibleTable<TableRow>
                rows={noDataFound ? [] : displayRows}
                leftCols={LEFT_COLS}
                groups={groups}
                singleCols={SINGLE_COLS}
                collapsedState={collapsedGroups}
                onCollapsedChange={(next) => {
                  setCollapsedGroups(next);
                  setAllColumnsExpanded(
                    groups.length > 0 && groups.every((group) => next[group.id] === false)
                  );
                }}
                tableClassName={[
                  "border-collapse bg-white text-[#414042] text-[12px] lg:text-[12px] min-[1700px]:text-[14px]",
                  anyGroupExpanded ? "table-fixed" : "w-full table-fixed",
                ].join(" ")}
                defaultSort={{
                  key: "net_sales",
                  direction: "desc",
                }}
                bodyMaxHeight={
                  shouldScrollTable
                    ? tableScrollHeight
                    : undefined
                }
                preserveColumnWidths="responsive"
                stickyLeftBorderMode="shadow-only"
                stickyLeftDividerMode="leading"
                stickyLeftHorizontalBorderMode="border"
                onSortChange={setTableSort}
                getSortValue={(row, colKey) => {
                  if (colKey === "net_units_sold") return toNumber((row as any).net_units_sold);
                  if (colKey === "net_sales") return toNumber((row as any).net_sales);
                  if (colKey === "profit") return toNumber((row as any).profit);
                  if (colKey === "cm2_profit") return toNumber((row as any).cm2_profit);
                  if (colKey === "ads_spend") return toNumber((row as any).ads_spend);
                  if (colKey === "acos") return getAcosPercentage(row);
                  if (colKey === "product_spend") return toNumber((row as any).product_spend);
                  if (colKey === "display_spend") return toNumber((row as any).display_spend);
                  if (colKey === "ads_spend") return toNumber((row as any).ads_spend);
                  if (colKey === "other_transactions") return getBackendOtherTransactionsValue(row);
                  return toNumber((row as any)[colKey]);
                }}
                isTotalRow={(row) => {
                  const name = String((row as any)?.product_name || "").trim().toLowerCase();
                  const sku = String((row as any)?.sku || "").trim().toLowerCase();

                  return name === "total" || sku === "total";
                }}
                layout={[
                  { type: "group" as const, id: "units_breakdown" },
                  { type: "single" as const, key: "asp" },

                  { type: "group" as const, id: "sales" },
                  ...(isUsCountry
                    ? [{ type: "single" as const, key: "promotional_rebates_percentage" }]
                    : [{ type: "group" as const, id: "promotional_rebates" }]),

                  { type: "single" as const, key: "cost_of_unit_sold" },

                  { type: "group" as const, id: "amazon_breakdown" },
                  { type: "group" as const, id: "other_transactions_breakdown" },
                  { type: "group" as const, id: "profit_breakdown" },

                  ...(hasCm2Data
                    ? [
                      { type: "group" as const, id: "ads_spend_breakdown" },
                      { type: "single" as const, key: "acos" },
                      { type: "group" as const, id: "cm2_profit_breakdown" },
                    ]
                    : []),
                ]}
                initialCollapsed={{
                  units_breakdown: true,
                  sales: true,
                  promotional_rebates: true,
                  cogs_breakdown: true,
                  amazon_breakdown: true,
                  other_transactions_breakdown: true,
                  profit_breakdown: true,
                  ...(hasCm2Data
                    ? {
                      ads_spend_breakdown: true,
                      cm2_profit_breakdown: true,
                    }
                    : {}),
                }}
                toggleGroupByColKey={{
                  net_units_sold: "units_breakdown",
                  net_sales: "sales",
                  amazon_fee: "amazon_breakdown",
                  other_transactions: "other_transactions_breakdown",
                  profit: "profit_breakdown",
                  ...(hasCm2Data
                    ? {
                      ads_spend: "ads_spend_breakdown",
                      product_spend: "ads_spend_breakdown",
                      display_spend: "ads_spend_breakdown",
                      cm2_profit: "cm2_profit_breakdown",
                    }
                    : {}),
                }}
                onVisibleColCountChange={setMainColCount}
                showSignRowInBody
                getSignForCol={getSignForCol}
                getRowClassName={(row, index) => {
                  const name = String((row as any)?.product_name || "").trim().toLowerCase();

                  if (name === "total") return "bg-[#EFEFEF] font-semibold";
                  if (!showAllRows && name === "others") return "cursor-pointer";

                  return index % 2 === 0 ? "bg-white" : "bg-gray-50";
                }}
                onRowClick={(row) => {
                  const name = String((row as any)?.product_name || "").trim().toLowerCase();
                  const sku = String((row as any)?.sku || "").trim().toLowerCase();

                  if (!showAllRows && (name === "others" || sku === "others")) {
                    setShowAllRows(true);
                  }
                }}
                getValue={(row, colKey, rowIndex) => {
                  // const name = String((row as any)?.product_name || "").trim().toLowerCase();
                  // const isTotal = name === "total";

                  const name = String((row as any)?.product_name || "").trim().toLowerCase();
                  const isTotal = name === "total";
                  const isOthers = name === "others";
                  if (colKey === "sno") return isTotal ? "" : rowIndex + 1;


                  if (colKey === "product_name") {
                    const displayName = getDisplayProductNameFromRow(row);

                    // ✅ ONLY "Others" in green
                    if (isOthers) {
                      return (
                        <span className="inline-block max-w-[220px] truncate text-green-500">
                          {displayName}
                        </span>
                      );
                    }

                    // clickable products
                    if (!isTotal) {
                      return (
                        <div
                          onClick={() => handleProductClick(row)}
                          className="flex w-full cursor-pointer items-end justify-between gap-2 text-left text-green-500"
                          title={String(displayName || "")}
                        >
                          <span className="min-w-0 flex-1 whitespace-normal break-words">
                            {String(displayName || "-")}
                          </span>
                          {!isOthers && renderNetSalesDelta(row)}
                        </div>
                      );
                    }

                    // Total row
                    return (
                      <span className="inline-block max-w-[220px] truncate font-semibold">
                        {String(displayName || "-")}
                      </span>
                    );
                  }

                  // ✅ FIX: show SKU as text (do NOT send to formatValue)
                  if (colKey === "sku") {
                    if (isOthers || isTotal) return "-"; // or "" if you want blank
                    return !isMissingName((row as any).sku) ? String((row as any).sku) : "-";
                  }

                  if (colKey === "unit_wise_cm2_profitability") {
                    return formatValue(getCm2PerUnit(row), colKey);
                  }

                  if (colKey === "cm2_margins") {
                    return formatValue(getCm2Percentage(row), "cm2_margins");
                  }

                  if (colKey === "acos") {
                    return formatValue(getAcosPercentage(row), "acos");
                  }

                  if (colKey === "other_transactions") {
                    return formatValue(getBackendOtherTransactionsValue(row), colKey);
                  }

                  if (colKey === "promotional_rebates_percentage") {
                    const value = formatValue((row as any)[colKey], colKey);
                    return value === "-" ? value : `${value}%`;
                  }

                  // ✅ round Ads Spend expanded columns without decimals
                  if (
                    colKey === "product_spend" ||
                    colKey === "display_spend" ||
                    colKey === "brand_spend" ||
                    colKey === "ads_spend"
                  ) {
                    return formatRoundedPlain((row as any)[colKey]);
                  }

                  return formatValue((row as any)[colKey], colKey);

                }}
                summary={{
                  enabled: !noDataFound && mainColCount > 0,

                  rows: skuSummaryRows,

                  valueCols: 2,
                  boldSectionsByDefault: false,
                }}
              />
              {noDataFound && (
                <div className="w-full text-center py-6 text-sm text-gray-500 font-medium">
                  No Data Available for selected period
                </div>
              )}

            </div>
          </div>
        </div>
      </div>

      {/* Top & Bottom tables */}
      {/* <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
        <div className="flex flex-col justify-between gap-7 md:gap-3 text-[#414042] md:flex-row min-w-0">
      
          <div className="flex-1 min-w-0">
            <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
              <PageBreadcrumb pageTitle="Most 5 Profitable Products" variant="page" align="left" textSize="2xl" />
            </div>

            <div className="overflow-x-auto rounded-xl border border-gray-300">
              <table className="w-full table-auto border-collapse">
                <thead>
                  <tr className="bg-green-500 font-bold text-[#f8edcf]">
                    <th className="min-w-[150px] border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm break-words leading-snug">
                      Product Name
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      CM1 Profit ({currencySymbol})
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      Profit Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      Sales Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      CM1 Profit per Unit ({currencySymbol})
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {topData.rows.map((item, index) => (
                    <tr key={index} className={`${index % 2 === 0 ? "bg-white" : "bg-gray-50"}`}>
                      <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm whitespace-normal break-words align-top">
                        <span title={item.product_name} className="block">
                          {item.product_name || "-"}
                        </span>
                      </td>

                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.profit}
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.profitMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.salesMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.cm1_per_unit}
                      </td>
                    </tr>
                  ))}

                  <tr className="bg-[#EFEFEF] font-semibold">
                    <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm">
                      <strong>Total</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{topData.totals.profit}</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{topData.totals.profitMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{topData.totals.salesMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{topData.totals.avg_cm1}</strong>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div className="flex-1 min-w-0">
            <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
              <PageBreadcrumb pageTitle="Least 5 Profitable Products" variant="page" align="left" textSize="2xl" />
            </div>

            <div className="overflow-x-auto rounded-xl border border-gray-300">
              <table className="w-full table-auto border-collapse">
                <thead>
                  <tr className="bg-[#B75A5A] font-bold text-[#f8edcf]">
                    <th className="min-w-[150px] border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm break-words leading-snug">
                      Product Name
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      CM1 Profit ({currencySymbol})
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      Profit Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      Sales Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                      CM1 Profit per Unit ({currencySymbol})
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {bottomData.rows.map((item, index) => (
                    <tr key={index} className={`${index % 2 === 0 ? "bg-white" : "bg-gray-50"}`}>
                      <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm whitespace-normal break-words align-top">
                        <span title={item.product_name} className="block">
                          {item.product_name || "-"}
                        </span>
                      </td>

                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.profit}
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.profitMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.salesMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                        {item.cm1_per_unit}

                      </td>
                    </tr>
                  ))}

                  <tr className="bg-[#EFEFEF] font-semibold">
                    <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm">
                      <strong>Total</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{bottomData.totals.profit}</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{bottomData.totals.profitMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{bottomData.totals.salesMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      <strong>{bottomData.totals.avg_cm1}</strong>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div> */}

      {/* {showModal && selectedProduct && (
        <Productinfoinpopup
          productname={selectedProduct}
          countryName={countryName}
          month={month}
          year={year}
          onClose={() => setShowModal(false)}
        />
      )} */}
    </>
  );
};

export default SKUtable;
