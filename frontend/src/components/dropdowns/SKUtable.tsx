"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx";
import { jwtDecode } from "jwt-decode";

import SkuMultiCountryUpload from "../ui/modal/SkuMultiCountryUpload";
import Productinfoinpopup from "./Productinfoinpopup";
import PageBreadcrumb from "../common/PageBreadCrumb";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import { SkuExportPayload } from "@/lib/utils/exportTypes";
import GroupedCollapsibleTable, { LeafCol, ColGroup } from "../ui/table/GroupedCollapsibleTable";

/* ---------- Types ---------- */

type RangeType = "monthly" | "quarterly" | "yearly";

type SKUtableProps = {
  range: RangeType;
  month?: string;
  quarter?: string;
  year: string | number;
  countryName: string;
  homeCurrency?: string;
  onExportPayloadChange?: (payload: SkuExportPayload) => void;
  hideDownloadButton?: boolean;
};

type TableRow = {
  product_name?: string;
  sku?: string;

  quantity?: number; // may exist from backend
  total_quantity?: number;

  asp?: number;
  ASP?: number;

  gross_sales?: number;
  product_sales?: number;
  refund_sales?: number;
  net_sales?: number;
  lost_total?: number;

  cost_of_unit_sold?: number;
  shipment_charges?: number;

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
  other_transactions?: number; // derived mapping

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
  visible_ads?: number;
  dealsvouchar_ads?: number;

  reimbursement_lost_inventory_amount?: number;
  reimbursement_lost_inventory_units?: number;

  reimbursement_vs_sales?: number;

  cm2_profit?: number;
  cm2_margins?: number;
  acos?: number;
  rembursment_vs_cm2_margins?: number;

  Profit?: number;
  Net_Sales?: number;

  profit_mix?: number;
  sales_mix?: number;

  // backend might send this as well
  return_quantity?: number;
};

type Totals = {
  advertising_total: number;
  visible_ads: number;
  dealsvouchar_ads: number;
  other_transactions: number;
  platform_fee: number;
  inventory_storage_fees: number;
  reimbursement_lost_inventory_amount: number;
  reimbursement_lost_inventory_units?: number;
  shipment_charges: number;
  reimbursement_vs_sales: number;
  cm2_profit: number;
  cm2_margins: number;
  acos: number;
  rembursment_vs_cm2_margins: number;
  profit: number;
  net_sales: number;
  lost_total: number;
  net_reimbursement: number;
};

type JwtPayload = {
  user_id?: string | number;
  [k: string]: unknown;
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

function normalizeRows(data: any[]): TableRow[] {
  return data.map((row) => {
    const productName =
      !isMissingName(row.product_name)
        ? String(row.product_name)
        : !isMissingName(row.sku)
          ? String(row.sku)
          : "-";

    const isTotalRow = productName.trim().toLowerCase() === "total";

    return {
      ...row,

      product_name: isTotalRow ? "Total" : productName,
      sku: row.sku ?? "-",

      // ✅ make sure quantity exists for export + consistency
      quantity: toNumber(row.quantity),                 // Units Sold
      return_quantity: toNumber(row.return_quantity),   // Return units
      total_quantity: toNumber(row.total_quantity),     // Net units sold (backend)

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

      // Costs / Fees
      cost_of_unit_sold: toNumber(row.cost_of_unit_sold),
      shipment_charges: toNumber(row.shipment_charges),
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
      other_transaction_fees: toNumber(row.other_transaction_fees),

      other_transactions: toNumber(row.other_transaction_fees),

      // CM1
      profit: toNumber(row.profit),
      profit_percentage: toNumber(row.profit_percentage),
      unit_wise_profitability: toNumber(row.unit_wise_profitability),
    } as TableRow;

  });
}



function computeTotalsFromLastRow(rows: TableRow[]): Totals {
  const lastRow: any = rows[rows.length - 1] || {};

  const platformFees = toNumber(lastRow.platformfeenew);
  const inventoryStorageFees = toNumber(lastRow.platform_fee_inventory_storage);

  // const reimbursementAmount =
  //   toNumber(lastRow.reimbursement_lost_inventory_amount) ||
  //   toNumber(lastRow.rembursement_fee) ||
  //   0;

  const netReimbursement = toNumber(lastRow.rembursement_fee);

  const reimbursementUnits = toNumber(lastRow.reimbursement_lost_inventory_units) || 0;

  const cm2MarginsValue = toNumber(
    lastRow.cm2_margins ??
    lastRow.cm2_profit_percentage ??
    lastRow.cm2_profit_percent ??
    lastRow.cm2_profit_percentage_value
  );

  return {
    advertising_total: toNumber(lastRow.advertising_total),
    visible_ads: toNumber(lastRow.visible_ads),
    dealsvouchar_ads: toNumber(lastRow.dealsvouchar_ads),
    other_transactions: toNumber(lastRow.platform_fee),
    platform_fee: platformFees,
    inventory_storage_fees: inventoryStorageFees,
    reimbursement_lost_inventory_amount:
      toNumber(lastRow.reimbursement_lost_inventory_amount) || 0,
    reimbursement_lost_inventory_units: reimbursementUnits,
    lost_total: toNumber(lastRow.lost_total),
    shipment_charges: toNumber(lastRow.shipment_charges),
    reimbursement_vs_sales: toNumber(lastRow.reimbursement_vs_sales),
    cm2_profit: toNumber(lastRow.cm2_profit),
    cm2_margins: cm2MarginsValue,
    acos: toNumber(lastRow.acos),
    rembursment_vs_cm2_margins: toNumber(lastRow.rembursment_vs_cm2_margins),
    net_reimbursement: netReimbursement,
    profit: toNumber(lastRow.Profit ?? lastRow.profit),
    net_sales: toNumber(lastRow.Net_Sales ?? lastRow.net_sales),
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
  onExportPayloadChange,
  hideDownloadButton = false,
}) => {
  const [selectedProduct, setSelectedProduct] = useState<string | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [showModal2, setShowModal2] = useState(false);
  const [mainColCount, setMainColCount] = useState(0);
  const [noDataFound, setNoDataFound] = useState(false);
  const [tableData, setTableData] = useState<TableRow[]>([]);
  const [totals, setTotals] = useState<Totals>({
    advertising_total: 0,
    visible_ads: 0,
    dealsvouchar_ads: 0,
    other_transactions: 0,
    platform_fee: 0,
    inventory_storage_fees: 0,
    reimbursement_lost_inventory_amount: 0,
    reimbursement_lost_inventory_units: 0,
    shipment_charges: 0,
    reimbursement_vs_sales: 0,
    cm2_profit: 0,
    cm2_margins: 0,
    acos: 0,
    rembursment_vs_cm2_margins: 0,
    profit: 0,
    net_sales: 0,
    lost_total: 0,
    net_reimbursement: 0,
  });

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [userData, setUserData] = useState<{ brand_name?: string; company_name?: string } | null>(null);
  const [summaryCollapsed, setSummaryCollapsed] = useState({
    ads: true,
    other: true,
  });

  const toggleSummary = (key: "ads" | "other") => {
    setSummaryCollapsed((p) => ({ ...p, [key]: !p[key] }));
  };

  const isGlobalPage = (countryName || "").toLowerCase() === "global";

  // Token (memo once)
  const token = useMemo(() => {
    if (typeof window === "undefined") return null;
    return localStorage.getItem("jwtToken");
  }, []);

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

  const userid = useMemo(() => {
    if (!token) return "";
    try {
      const decoded = jwtDecode<JwtPayload>(token);
      return decoded?.user_id ?? "";
    } catch {
      return "";
    }
  }, [token]);

  const getDisplayProductNameFromRow = useCallback((row: TableRow): string => {
    if (!isMissingName(row.product_name)) return String(row.product_name);
    if (!isMissingName(row.sku)) return String(row.sku);
    return "-";
  }, []);


  const aspKey = useMemo(() => {
    const first = tableData[0] || {};
    const k = Object.keys(first).find((key) => key.toLowerCase() === "asp");
    return k as keyof TableRow | undefined;
  }, [tableData]);

  // const buildExcelTableData = useCallback(() => {
  //   const columnsToDisplay2 = [
  //     "product_name",
  //     "quantity",
  //     "asp",
  //     "product_sales",
  //     "net_sales",
  //     "cost_of_unit_sold",
  //     "amazon_fee",
  //     "selling_fees",
  //     "fba_fees",
  //     "net_credits",
  //     "net_taxes",
  //     "profit",
  //     "profit_percentage",
  //     "unit_wise_profitability",
  //   ] as const;

  //   const rowsForExcel = tableData.map((row) => {
  //     const rowData: Record<string, string | number> = {};

  //     columnsToDisplay2.forEach((column) => {
  //       let value: any = (row as any)[column];

  //       // ✅ name fallback same as UI
  //       if (column === "product_name") value = getDisplayProductNameFromRow(row);

  //       // ✅ hard mapping rules (match your current excel logic)
  //       if (column === "product_sales") value = row.product_sales ?? (row as any).gross_sales ?? 0;

  //       // ✅ quantity = Units Sold (NOT total_quantity)
  //       if (column === "quantity") value = row.quantity ?? row.units_sold ?? 0;

  //       // number formatting rules
  //       if (typeof value === "number") {
  //         if (Math.abs(value) < 1e-10) value = 0;

  //         // keep decimals except quantity
  //         if (column !== "product_name" && column !== "quantity") value = Number(value.toFixed(2));
  //       }

  //       // percent stored as fraction for excel (same as you do now)
  //       if (column === "profit_percentage" && typeof value === "number") value = Number(value) / 100;

  //       rowData[column] = typeof value === "number" && isNaN(value) ? "-" : value;
  //     });

  //     return rowData;
  //   });

  //   return { columnsToDisplay2, rowsForExcel };
  // }, [tableData, getDisplayProductNameFromRow]);


  const getTitle = useCallback(() => `Profit Breakup (SKU Level)`, []);

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



  const LEFT_COLS: LeafCol<TableRow>[] = useMemo(
    () => [
      { key: "sno", label: "Sno.", align: "center" },
      { key: "product_name", label: "Product Name", align: "left" },

    ],
    []
  );

  const groups = useMemo<ColGroup<TableRow>[]>(() => ([
    {
      id: "sales",
      label: "Sales",
      collapsedCols: [],
      expandedCols: [
        { key: "product_sales", label: "Gross Sales", align: "center" as const },
        { key: "refund_sales", label: "Sales - Refund", align: "center" as const },
        { key: "tex_and_credits", label: "Taxes and Credits", align: "center" as const },
      ],
    },
    {
      id: "units_breakdown",
      label: "Net Units Sold",
      collapsedCols: [],
      expandedCols: [
        { key: "sku", label: "SKU", align: "center" as const },
        { key: "units_sold", label: "Units Sold", align: "center" as const },
        { key: "return_units", label: "Return", align: "center" as const },
      ],
    },
    {
      id: "promotions_breakdown",
      label: "",
      collapsedCols: [],
      expandedCols: [
        { key: "promotional_rebates", label: "Promotions", align: "center" as const },
        { key: "promotional_rebates_percentage", label: "Promotions %", align: "center" as const },
      ],
    },
    {
      id: "amazon_breakdown",
      label: "Amazon Fees",
      collapsedCols: [],
      expandedCols: [
        { key: "selling_fees", label: "Selling Fees", align: "center" as const },
        { key: "fba_fees", label: "FBA Fees", align: "center" as const },
      ],
    },
    {
      id: "other_transactions_breakdown",
      label: "Other Transactions",
      collapsedCols: [],
      expandedCols: [
        { key: "net_taxes", label: "Net Taxes", align: "center" as const },
        { key: "net_credits", label: "Net Credits", align: "center" as const },
        { key: "misc_transaction", label: "Misc. Transactions", align: "center" as const },
      ],
    },
    {
      id: "profit_breakdown",
      label: "CM1 Profit",
      collapsedCols: [],
      expandedCols: [
        { key: "unit_wise_profitability", label: "CM1 Profit Per Unit", align: "center" as const },
        { key: "profit_percentage", label: "CM1 Profit %", align: "center" as const },
      ],
    },
  ]), []);


  const SINGLE_COLS: LeafCol<TableRow>[] = useMemo(
    () => [
      { key: "asp", label: "ASP", align: "center" },
      { key: "net_sales", label: "Net Sales", align: "center" },
      { key: "cost_of_unit_sold", label: "COGS", align: "center" },
      { key: "net_units_sold", label: "Net Units Sold", align: "center" },
      { key: "amazon_fee", label: "Amazon Fees", align: "center" },
      { key: "other_transactions", label: "Other Transactions", align: "center" },
      { key: "profit", label: "CM1 Profit Margin", align: "center" },
    ],
    []
  );

  const buildExcelColumnsFromUI = useCallback((): LeafCol<TableRow>[] => {
    // Exact order you want in Excel
    const ordered: LeafCol<TableRow>[] = [
      { key: "sno", label: "S. no", align: "center" as const },

      { key: "product_name", label: "Product Name", align: "left" as const },
      { key: "sku", label: "SKU", align: "center" as const },

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
      { key: "amazon_fee", label: "Amazon Fees", align: "center" as const },

      { key: "net_taxes", label: "Net Taxes", align: "center" as const },
      { key: "net_credits", label: "Net Credits", align: "center" as const },
      { key: "misc_transaction", label: "Misc. Transactions", align: "center" as const },
      { key: "other_transactions", label: "Other Transactions", align: "center" as const },

      { key: "profit", label: "CM1 Profit Margin", align: "center" as const },
      { key: "unit_wise_profitability", label: "CM1 Profit Per Unit", align: "center" as const },
      { key: "profit_percentage", label: "CM1 Profit %", align: "center" as const },
    ];

    // optional: remove duplicates if any
    const seen = new Set<string>();
    return ordered.filter((c) => {
      if (!c.key) return false;
      if (seen.has(c.key)) return false;
      seen.add(c.key);
      return true;
    });
  }, []);


  const INT_KEYS = useMemo(() => new Set(["quantity", "units_sold", "return_units", "net_units_sold"]), []);

  const formatValue = useCallback(
    (value: unknown, key: string) => {
      if (value === undefined || value === null || value === "") return "-";

      const raw = toNumber(value);
      if (!Number.isFinite(raw)) return "-";

      // ✅ keep negative for CM2 Profit/Loss
      const n = key === "cm2_profit" ? raw : Math.abs(raw);

      if (INT_KEYS.has(key)) return n;

      const formatted = Math.abs(n).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });

      // ✅ add minus sign back only when needed
      const signedFormatted = n < 0 ? `-${formatted}` : formatted;

      if (key === "profit_percentage") return `${signedFormatted}%`;
      return signedFormatted;
    },
    [INT_KEYS]
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
        "other_transactions",
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
        "amazon_fee",          // Amazon Fees (-)

        "promotional_rebates", // Promotions (-)
        "platformfeenew",
        "platform_fee_inventory_storage",
        "net_taxes",
        "lost_total",
      ]),
    []
  );


  const getSignForCol = useCallback(
    (colKey: string) => {
      if (SIGN_PLUS.has(colKey)) return { text: "(+)", className: "text-green-700" };
      if (SIGN_MINUS.has(colKey)) return { text: "(-)", className: "text-[#ff5c5c]" };
      return null;
    },
    [SIGN_PLUS, SIGN_MINUS]
  );
  const buildSkuSheetModel = useCallback(() => {
    const excelCols = buildExcelColumnsFromUI();
    const colKeys = excelCols.map((c) => c.key);

    const headerRow: Record<string, string> = {};
    excelCols.forEach((c) => {
      headerRow[c.key] = c.label;
    });

    const signRow: Record<string, string> = {};
    colKeys.forEach((k) => {
      const s = getSignForCol(k);
      if (s?.text) signRow[k] = s.text;
    });

    const rowsForExcel = tableData.map((row, rowIndex) => {
      const out: Record<string, string | number> = {};

      colKeys.forEach((key) => {
        let value: any = (row as any)[key];

        if (key === "sno") {
          const name = getDisplayProductNameFromRow(row);
          const isTotal = String(name).trim().toLowerCase() === "total";
          value = isTotal ? "" : rowIndex + 1;
        }


        // Product name fallback
        if (key === "product_name") value = getDisplayProductNameFromRow(row);

        // Mapping rules
        if (key === "product_sales") value = row.product_sales ?? (row as any).gross_sales ?? 0;
        if (key === "other_transactions") value = row.other_transactions ?? row.other_transaction_fees ?? 0;

        // If you want quantity in excel to mean Units Sold like UI
        if (key === "quantity") value = row.quantity ?? row.units_sold ?? 0;

        if (typeof value === "number") {
          if (Math.abs(value) < 1e-10) value = 0;

          if (["sno", "units_sold", "return_units", "net_units_sold", "quantity"].includes(key)) {
            value = Math.trunc(value);
          } else {
            value = Number(value.toFixed(2));
          }

        }

        if (key === "profit_percentage" && typeof value === "number") {
          value = Number(value) / 100;
        }

        out[key] = typeof value === "number" && isNaN(value) ? "-" : value;
      });

      return out;
    });



    type SummaryRow = Record<string, string | number> & { __bold?: number };

    const summaryRows: SummaryRow[] = [
      { product_name: "Cost of Advertisement", profit: Math.abs(Number(totals.advertising_total || 0)), __bold: 1 },

      { product_name: "Visibility - Ads (-)", profit: Math.abs(Number(totals.visible_ads || 0)) },

      { product_name: "Visibility - Deals, Vouchers and Reviews (-)", profit: Math.abs(Number(totals.dealsvouchar_ads || 0)) },

      ...(countryName === "us" || countryName === "global"
        ? [{ product_name: "Shipment Charges (-)", profit: Math.abs(Number(totals.shipment_charges || 0)) }]
        : []),

      { product_name: "Other Transactions (-)", profit: Math.abs(Number(totals.other_transactions || 0)), __bold: 1 },

      { product_name: "Platform Fees (-)", profit: Math.abs(Number(totals.platform_fee || 0)) },

      { product_name: "Inventory Storage Fees (-)", profit: Math.abs(Number(totals.inventory_storage_fees || 0)) },

      { product_name: "Reimbursement for lost Inventory", profit: Math.abs(Number(totals.lost_total || 0)) },

      { product_name: "CM2 Profit/Loss", profit: Number(totals.cm2_profit || 0), __bold: 1 },

      { product_name: "CM2 Margins", profit: Number(totals.cm2_margins || 0), __bold: 1 },

      { product_name: "TACoS (Total Advertising Cost of Sale)", profit: Number(totals.acos || 0), __bold: 1 },

      { product_name: "Net Reimbursement", profit: Math.abs(Number(totals.net_reimbursement || 0)), __bold: 1 },

      { product_name: "Reimbursement vs CM2 Margins", profit: Number(totals.rembursment_vs_cm2_margins || 0), __bold: 1 },

      { product_name: "Reimbursement vs Sales", profit: Number(totals.reimbursement_vs_sales || 0), __bold: 1 },
    ];

    const normalizedSummaryRows =
      summaryRows.map((r) => ({
        ...r,
        __bold: r.__bold ? true : undefined,
      })) as Array<Record<string, string | number> & { __bold?: boolean }>;


    const formats: Record<string, "int" | "money" | "percent" | "text"> = {};
    colKeys.forEach((k) => {
      if (k === "product_name" || k === "sku") formats[k] = "text";
      else if (k === "profit_percentage") formats[k] = "percent";
      else if (["units_sold", "return_units", "net_units_sold", "quantity"].includes(k)) formats[k] = "int";
      else formats[k] = "money";
    });

    return {
      columns: colKeys as readonly string[],
      extraRows: getExtraRows(),
      headerRow,
      signRow,
      rows: rowsForExcel,
      summaryRows: normalizedSummaryRows,
      formats,
      summaryValueKey: "profit",
    };
  }, [
    tableData,
    totals,
    countryName,
    getExtraRows,
    getDisplayProductNameFromRow,
    buildExcelColumnsFromUI,
    getSignForCol,
  ]);

  // Period label
  const yearShort = typeof year === "string" ? year.toString().slice(-2) : String(year).slice(-2);

  const periodLabel =
    range === "monthly"
      ? `SKU-wise Profitability-${convertToAbbreviatedMonth(month)}'${yearShort}`
      : range === "quarterly"
        ? `SKU-wise Profitability-${quarter}'${yearShort}`
        : `SKU-wise Profitability-Year'${yearShort}`;

  // Dummy table data
  const dummyTableData: TableRow[] = useMemo(
    () => [
      {
        product_name: "Sample Product A",
        sku: "SKU-A",
        units_sold: 100,
        return_units: 0,
        net_units_sold: 100,
        asp: 12.5,
        product_sales: 1250,
        refund_sales: 0,
        net_sales: 1250,
        cost_of_unit_sold: 500,
        selling_fees: 60,
        fba_fees: 40,
        amazon_fee: 100,
        net_credits: 100,
        net_taxes: 50,
        profit: 700,
        profit_percentage: 56,
        unit_wise_profitability: 7,
      },
      {
        product_name: "Sample Product B",
        sku: "SKU-B",
        units_sold: 80,
        return_units: 0,
        net_units_sold: 80,
        asp: 10,
        product_sales: 800,
        refund_sales: 0,
        net_sales: 800,
        cost_of_unit_sold: 300,
        selling_fees: 50,
        fba_fees: 30,
        amazon_fee: 80,
        net_credits: 70,
        net_taxes: 40,
        profit: 450,
        profit_percentage: 56.25,
        unit_wise_profitability: 5.625,
      },
      {
        product_name: "Total",
        sku: "-",
        units_sold: 180,
        return_units: 0,
        net_units_sold: 180,
        asp: 11,
        product_sales: 2050,
        refund_sales: 0,
        net_sales: 2050,
        cost_of_unit_sold: 800,
        selling_fees: 110,
        fba_fees: 70,
        amazon_fee: 180,
        net_credits: 170,
        net_taxes: 90,
        profit: 1150,
        profit_percentage: 56.1,
        unit_wise_profitability: 6.39,
      },
    ],
    []
  );

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
    if (!token) {
      setError("No token found. Please log in.");
      return;
    }

    const ac = new AbortController();

    (async () => {
      try {
        const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/get_user_data`, {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
          cache: "no-store",
          signal: ac.signal,
        });

        if (!res.ok) {
          const data = await res.json().catch(() => ({}));
          setError(data?.error || "Something went wrong.");
          return;
        }

        const data = (await res.json()) as { brand_name?: string; company_name?: string };
        setUserData(data);
      } catch (e: any) {
        if (e?.name === "AbortError") return;
        setError("Error fetching user data");
      }
    })();

    return () => ac.abort();
  }, [token]);

  // Quarter mapping
  const quarterMapping: Record<string, string> = useMemo(
    () => ({ Q1: "quarter1", Q2: "quarter2", Q3: "quarter3", Q4: "quarter4" }),
    []
  );

  const buildSkuUrl = useCallback(() => {
    if (range === "monthly") {
      const skuwiseFileName =
        countryName.toLowerCase() === "global"
          ? `skuwisemonthly_${userid}_${countryName}_${(month || "").toLowerCase()}${year}_table`
          : `skuwisemonthly_${userid}_${countryName.toLowerCase()}_${(month || "").toLowerCase()}${year}`;

      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/skutableprofit/${skuwiseFileName}`);
      url.searchParams.set("country", countryName);
      url.searchParams.set("month", (month || "").toLowerCase());
      url.searchParams.set("year", String(year));

      if (isGlobalPage) url.searchParams.set("homeCurrency", effectiveHomeCurrency);
      return url.toString();
    }

    if (range === "quarterly") {
      const backendQuarter = quarterMapping[quarter] || "";
      const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/quarterlyskutable`);
      url.searchParams.set("quarter", backendQuarter);
      url.searchParams.set("country", countryName);
      url.searchParams.set("year", String(year));
      url.searchParams.set("userid", String(userid));

      if (isGlobalPage) url.searchParams.set("homeCurrency", effectiveHomeCurrency);
      return url.toString();
    }

    // yearly
    const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/YearlySKU`);
    url.searchParams.set("country", countryName);
    url.searchParams.set("year", String(year));

    if (isGlobalPage) url.searchParams.set("homeCurrency", effectiveHomeCurrency);
    return url.toString();
  }, [
    range,
    countryName,
    userid,
    month,
    year,
    quarter,
    quarterMapping,
    isGlobalPage,
    effectiveHomeCurrency,
  ]);

  /* --------- Fetch table data (AbortController to prevent race) --------- */
  useEffect(() => {
    if (!countryName) return;

    const ac = new AbortController();

    (async () => {
      setLoading(true);
      setError(null);

      try {
        const url = buildSkuUrl();

        const res = await fetch(url, {
          method: "GET",
          headers: token ? { Authorization: `Bearer ${token}` } : {},
          cache: "no-store",
          signal: ac.signal,
        });

        if (!res.ok) {
          setNoDataFound(true);
          setTableData(dummyTableData);
          return;
        }

        const data = (await res.json()) as unknown;

        if (!Array.isArray(data) || data.length === 0) {
          setNoDataFound(true);
          setTableData(dummyTableData);
          return;
        }

        const normalized = normalizeRows(data);
        setTableData(normalized);
        setTotals(computeTotalsFromLastRow(normalized));
        setNoDataFound(false);
      } catch (e: any) {
        if (e?.name === "AbortError") return;
        setNoDataFound(true);
        setTableData(dummyTableData);
      } finally {
        setLoading(false);
      }
    })();

    return () => ac.abort();
  }, [countryName, buildSkuUrl, token, dummyTableData]);

  /* --------- Export payload --------- */
  // useEffect(() => {
  //   if (!tableData || tableData.length === 0) return;

  //   onExportPayloadChange?.({
  //     tableData,
  //     totals,
  //     currencySymbol,
  //     brandName: userData?.brand_name,
  //     companyName: userData?.company_name,
  //     title: "Profit Breakup (SKU Level)",
  //     periodLabel,
  //     range,
  //     countryName,
  //   });
  // }, [
  //   tableData,
  //   totals,
  //   currencySymbol,
  //   userData?.brand_name,
  //   userData?.company_name,
  //   periodLabel,
  //   range,
  //   countryName,
  //   onExportPayloadChange,
  // ]);

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

      // ✅ NEW: parent will export EXACTLY what UI uses
      sheetModel: buildSkuSheetModel(),
    });
  }, [tableData, totals, currencySymbol, userData, periodLabel, range, countryName, onExportPayloadChange, buildSkuSheetModel]);


  /* --------- Top/Bottom helpers --------- */
  const getTop5Profitable = useCallback(
    (data: TableRow[]) => {
      const rows = data.slice(0, -1);
      const top5 = [...rows].sort((a, b) => (b.profit || 0) - (a.profit || 0)).slice(0, 5);

      const totalProfit = top5.reduce((s, r) => s + (r.profit || 0), 0);
      const totalProfitMix = top5.reduce((s, r) => s + (r.profit_mix || 0), 0);
      const totalSalesMix = top5.reduce((s, r) => s + (r.sales_mix || 0), 0);
      const totalUnitWise = top5.reduce((s, r) => s + (r.unit_wise_profitability || 0), 0);

      const formatted = top5.map((item) => ({
        product_name: getDisplayProductNameFromRow(item),
        profit: (item.profit || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
        profitMix: (item.profit_mix || 0).toFixed(2),
        salesMix: (item.sales_mix || 0).toFixed(2),
        unit_wise_profitability: (item.unit_wise_profitability || 0).toFixed(2),
      }));

      return {
        rows: formatted,
        totals: {
          profit: totalProfit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
          profitMix: totalProfitMix.toFixed(2),
          salesMix: totalSalesMix.toFixed(2),
          unit_wise_profitability: totalUnitWise.toFixed(2),
        },
      };
    },
    [getDisplayProductNameFromRow]
  );

  const getBottom5Profitable = useCallback(
    (data: TableRow[]) => {
      const rows = data.slice(0, -1);
      const bottom5 = [...rows].sort((a, b) => (a.profit || 0) - (b.profit || 0)).slice(0, 5);

      const totalProfit = bottom5.reduce((s, r) => s + (r.profit || 0), 0);
      const totalProfitMix = bottom5.reduce((s, r) => s + (r.profit_mix || 0), 0);
      const totalSalesMix = bottom5.reduce((s, r) => s + (r.sales_mix || 0), 0);
      const totalUnitWise = bottom5.reduce((s, r) => s + (r.unit_wise_profitability || 0), 0);

      const formatted = bottom5.map((item) => ({
        product_name: getDisplayProductNameFromRow(item),
        profit: (item.profit || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
        profitMix: (item.profit_mix || 0).toFixed(2),
        salesMix: (item.sales_mix || 0).toFixed(2),
        unit_wise_profitability: (item.unit_wise_profitability || 0).toFixed(2),
      }));

      return {
        rows: formatted,
        totals: {
          profit: totalProfit.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
          profitMix: totalProfitMix.toFixed(2),
          salesMix: totalSalesMix.toFixed(2),
          unit_wise_profitability: totalUnitWise.toFixed(2),
        },
      };
    },
    [getDisplayProductNameFromRow]
  );

  const topData = useMemo(() => getTop5Profitable(tableData), [tableData, getTop5Profitable]);
  const bottomData = useMemo(() => getBottom5Profitable(tableData), [tableData, getBottom5Profitable]);

  /* --------- UI handlers --------- */
  const handleProductClick = useCallback((product: string) => {
    setSelectedProduct(product);
    setShowModal(true);
  }, []);


  /* --------- Excel Download --------- */

  // const handleDownloadExcel = useCallback(() => {
  //   const wb = XLSX.utils.book_new();

  //   const excelCols = buildExcelColumnsFromUI();
  //   const colKeys = excelCols.map((c) => c.key);

  //   // Header row from UI labels
  //   const headerRow: Record<string, string> = {};
  //   excelCols.forEach((c) => {
  //     headerRow[c.key] = c.label;
  //   });

  //   // Build data rows
  //   const tableDataForExcel = tableData.map((row, rowIndex) => {
  //     const rowData: Record<string, string | number> = {};

  //     colKeys.forEach((key) => {
  //       let value: any = (row as any)[key];

  //       // ✅ SNO: integer, blank for Total row
  //       if (key === "sno") {
  //         const name = getDisplayProductNameFromRow(row);
  //         const isTotal = String(name).trim().toLowerCase() === "total";
  //         value = isTotal ? "" : rowIndex + 1; // integer
  //       }

  //       // Product name matches UI fallback
  //       if (key === "product_name") value = getDisplayProductNameFromRow(row);

  //       // gross_sales -> product_sales mapping
  //       if (key === "product_sales") value = row.product_sales ?? (row as any).gross_sales ?? 0;

  //       // other_transactions mapping
  //       if (key === "other_transactions") value = row.other_transactions ?? row.other_transaction_fees ?? 0;

  //       // quantity meaning units_sold (if present in col list)
  //       if (key === "quantity") value = row.quantity ?? row.units_sold ?? 0;

  //       // Pre-rounding
  //       if (typeof value === "number") {
  //         if (Math.abs(value) < 1e-10) value = 0;

  //         // integer-only fields
  //         if (["sno", "units_sold", "return_units", "net_units_sold", "quantity"].includes(key)) {
  //           value = Math.trunc(value);
  //         } else {
  //           value = Number(value.toFixed(2));
  //         }
  //       }

  //       // profit_percentage stored as fraction for excel %
  //       if (key === "profit_percentage" && typeof value === "number") value = Number(value) / 100;

  //       rowData[key] = typeof value === "number" && isNaN(value) ? "-" : value;
  //     });

  //     return rowData;
  //   });

  //   const extraRows = getExtraRows();

  //   const fullData = [
  //     ...extraRows.map((row) => ({ product_name: row[0] })),
  //     {}, // blank line
  //     headerRow,
  //     ...tableDataForExcel,
  //   ];

  //   const ws = XLSX.utils.json_to_sheet(fullData, { skipHeader: true });

  //   // ---------------------------
  //   // ✅ FORCE EXCEL FORMATTING
  //   // ---------------------------

  //   const EXTRA_ROWS_COUNT = extraRows.length;
  //   const BLANK_ROW_INDEX = EXTRA_ROWS_COUNT;            // the {} blank row
  //   const HEADER_ROW_INDEX = EXTRA_ROWS_COUNT + 1;       // headerRow position
  //   const FIRST_DATA_ROW_INDEX = HEADER_ROW_INDEX + 1;   // data starts after header

  //   // Find the sno column index in the worksheet (based on colKeys)
  //   const SNO_COL_INDEX = colKeys.indexOf("sno");

  //   if (ws["!ref"]) {
  //     const rng = XLSX.utils.decode_range(ws["!ref"]);

  //     // ✅ 1) Force sno column (data rows only) to integer format "0"
  //     if (SNO_COL_INDEX >= 0) {
  //       for (let r = FIRST_DATA_ROW_INDEX; r <= rng.e.r; r++) {
  //         const addr = XLSX.utils.encode_cell({ r, c: SNO_COL_INDEX });
  //         const cell = ws[addr];
  //         if (!cell) continue;

  //         // keep blanks (like Total row sno "")
  //         if (cell.v === "" || cell.v === null || cell.v === undefined) continue;

  //         const n = Number(cell.v);
  //         if (!Number.isFinite(n)) continue;

  //         cell.t = "n";
  //         cell.v = Math.trunc(n);
  //         cell.z = "0"; // ✅ no decimals
  //       }
  //     }

  //     // ✅ 2) Apply formats to other numeric columns (data rows only)
  //     for (let r = FIRST_DATA_ROW_INDEX; r <= rng.e.r; r++) {
  //       for (let c = 0; c <= rng.e.c; c++) {
  //         const addr = XLSX.utils.encode_cell({ r, c });
  //         const cell = ws[addr];
  //         if (!cell) continue;

  //         const key = colKeys[c];
  //         if (!key) continue;

  //         // sno already handled
  //         if (key === "sno") continue;

  //         if (typeof cell.v !== "number") continue;

  //         cell.t = "n";
  //         if (key === "profit_percentage") cell.z = "0.00%";
  //         else if (["units_sold", "return_units", "net_units_sold", "quantity"].includes(key)) cell.z = "0";
  //         else cell.z = "0.00";
  //       }
  //     }
  //   }

  //   XLSX.utils.book_append_sheet(wb, ws, "SKU Profitability");

  //   const filename =
  //     range === "monthly"
  //       ? `SKU-wise Profitability-${convertToAbbreviatedMonth(month)}'${yearShort}.xlsx`
  //       : range === "quarterly"
  //         ? `SKU-wise Profitability-${quarter}'${yearShort}.xlsx`
  //         : `SKU-wise Profitability-Year'${yearShort}.xlsx`;

  //   XLSX.writeFile(wb, filename);
  // }, [
  //   tableData,
  //   getExtraRows,
  //   range,
  //   month,
  //   yearShort,
  //   quarter,
  //   getDisplayProductNameFromRow,
  //   buildExcelColumnsFromUI,
  // ]);

  // ✅ FULL UPDATED FUNCTION (drop-in replace your current handleDownloadExcel)

  const handleDownloadExcel = useCallback(() => {
    const wb = XLSX.utils.book_new();

    const excelCols = buildExcelColumnsFromUI(); // [{ key, label }]
    const colKeys = excelCols.map((c) => c.key);

    // ✅ CHANGE THIS to shift summary values to a different column
    // Example options: "net_taxes", "net_sales", "profit", "amazon_fee", etc.
    const SUMMARY_VALUE_KEY: string = "profit"; // <-- CHANGE HERE

    // helper: force ALL rows to have ALL columns (prevents shifting)
    const rowWithAllCols = (overrides: Record<string, any> = {}) => {
      const base: Record<string, any> = {};
      colKeys.forEach((k) => (base[k] = ""));
      return { ...base, ...overrides };
    };

    // Header row from UI labels
    const headerRow = rowWithAllCols();
    excelCols.forEach((c) => {
      headerRow[c.key] = c.label;
    });

    // Build data rows (same mapping rules as your UI)
    const tableDataForExcel = tableData.map((row, rowIndex) => {
      const rowData = rowWithAllCols();

      colKeys.forEach((key) => {
        let value: any = (row as any)[key];

        // ✅ SNO: integer, blank for Total row
        if (key === "sno") {
          const name = getDisplayProductNameFromRow(row);
          const isTotal = String(name).trim().toLowerCase() === "total";
          value = isTotal ? "" : rowIndex + 1;
        }

        // Product name matches UI fallback
        if (key === "product_name") value = getDisplayProductNameFromRow(row);

        // gross_sales -> product_sales mapping
        if (key === "product_sales") value = row.product_sales ?? (row as any).gross_sales ?? 0;

        // other_transactions mapping
        if (key === "other_transactions") value = row.other_transactions ?? row.other_transaction_fees ?? 0;

        // quantity meaning units_sold (if present in col list)
        if (key === "quantity") value = row.quantity ?? row.units_sold ?? 0;

        // Pre-rounding
        if (typeof value === "number") {
          if (Math.abs(value) < 1e-10) value = 0;

          // integer-only fields
          if (["sno", "units_sold", "return_units", "net_units_sold", "quantity"].includes(key)) {
            value = Math.trunc(value);
          } else {
            value = Number(value.toFixed(2));
          }
        }

        // profit_percentage stored as fraction for excel %
        if (key === "profit_percentage" && typeof value === "number") value = Number(value) / 100;

        rowData[key] = typeof value === "number" && isNaN(value) ? "-" : value;
      });

      return rowData;
    });

    const extraRows = getExtraRows();

    const summaryRows = [
      rowWithAllCols({
        product_name: "Cost of Advertisement",
        [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.advertising_total || 0)),
      }),
      rowWithAllCols({
        product_name: "Visibility - Ads (-)",
        [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.visible_ads || 0)),
      }),
      rowWithAllCols({
        product_name: "Visibility - Deals, Vouchers and Reviews (-)",
        [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.dealsvouchar_ads || 0)),
      }),

      ...(countryName === "us" || countryName === "global"
        ? [
          rowWithAllCols({
            product_name: "Shipment Charges (-)",
            [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.shipment_charges || 0)),
          }),
        ]
        : []),

      rowWithAllCols({
        product_name: "Other Transactions (-)",
        [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.other_transactions || 0)),
      }),
      rowWithAllCols({
        product_name: "Platform Fees (-)",
        [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.platform_fee || 0)),
      }),
      rowWithAllCols({
        product_name: "Inventory Storage Fees (-)",
        [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.inventory_storage_fees || 0)),
      }),

      rowWithAllCols({
        product_name: "Reimbursement for lost Inventory",
        [SUMMARY_VALUE_KEY]: Math.abs(Number(totals.lost_total || 0)),
      }),

      rowWithAllCols({
        product_name: "CM2 Profit/Loss",
        [SUMMARY_VALUE_KEY]: Number(totals.cm2_profit || 0),
      }),
      rowWithAllCols({
        product_name: "CM2 Margins",
        [SUMMARY_VALUE_KEY]: Number(totals.cm2_margins || 0) / 100,
      }),
      rowWithAllCols({
        product_name: "TACoS (Total Advertising Cost of Sale)",
        [SUMMARY_VALUE_KEY]: Number(totals.acos || 0) / 100,
      }),
      rowWithAllCols({
        product_name: "Reimbursement vs CM2 Margins",
        [SUMMARY_VALUE_KEY]: Number(totals.rembursment_vs_cm2_margins || 0) / 100,
      }),
      rowWithAllCols({
        product_name: "Reimbursement vs Sales",
        [SUMMARY_VALUE_KEY]: Number(totals.reimbursement_vs_sales || 0) / 100,
      }),
    ];

    const fullData = [
      ...extraRows.map((row) => rowWithAllCols({ product_name: row[0] })),
      rowWithAllCols({}), // blank line
      headerRow,
      ...tableDataForExcel,
      rowWithAllCols({}), // blank line before summary
      ...summaryRows,
    ];

    // ✅ Force column order (prevents shifting forever)
    const ws = XLSX.utils.json_to_sheet(fullData, {
      header: colKeys,
      skipHeader: true,
    });

    // ---------------------------
    // ✅ FORCE EXCEL FORMATTING (same as you already do, but now reliable)
    // ---------------------------
    const EXTRA_ROWS_COUNT = extraRows.length;
    const BLANK_ROW_INDEX = EXTRA_ROWS_COUNT; // unused but kept for clarity
    const HEADER_ROW_INDEX = EXTRA_ROWS_COUNT + 1;
    const FIRST_DATA_ROW_INDEX = HEADER_ROW_INDEX + 1;

    const SNO_COL_INDEX = colKeys.indexOf("sno");

    if (ws["!ref"]) {
      const rng = XLSX.utils.decode_range(ws["!ref"]);

      // 1) Force sno to integer
      if (SNO_COL_INDEX >= 0) {
        for (let r = FIRST_DATA_ROW_INDEX; r <= rng.e.r; r++) {
          const addr = XLSX.utils.encode_cell({ r, c: SNO_COL_INDEX });
          const cell = ws[addr];
          if (!cell) continue;
          if (cell.v === "" || cell.v === null || cell.v === undefined) continue;

          const n = Number(cell.v);
          if (!Number.isFinite(n)) continue;

          cell.t = "n";
          cell.v = Math.trunc(n);
          cell.z = "0";
        }
      }

      // 2) Apply formats to numeric cells
      for (let r = FIRST_DATA_ROW_INDEX; r <= rng.e.r; r++) {
        for (let c = 0; c <= rng.e.c; c++) {
          const addr = XLSX.utils.encode_cell({ r, c });
          const cell = ws[addr];
          if (!cell) continue;

          const key = colKeys[c];
          if (!key) continue;

          if (key === "sno") continue;
          if (typeof cell.v !== "number") continue;

          cell.t = "n";

          // percent columns
          if (key === "profit_percentage") cell.z = "0.00%";

          // ✅ if your summary column is a percent field (like cm2_margins, acos), you can optionally format it:
          // NOTE: those rows are already stored as /100 above, so they should be percent formatted.
          // If SUMMARY_VALUE_KEY is "net_taxes" (money), keep money formatting.
          if (
            addr &&
            (key === SUMMARY_VALUE_KEY) &&
            ["cm2_margins", "acos", "rembursment_vs_cm2_margins", "reimbursement_vs_sales"].includes(
              String((ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v ?? "")).toLowerCase()
            )
          ) {
            // optional: leave as-is; most people skip this block
          }

          if (["units_sold", "return_units", "net_units_sold", "quantity"].includes(key)) cell.z = "0";
          else cell.z = "0.00";
        }
      }
    }

    XLSX.utils.book_append_sheet(wb, ws, "SKU Profitability");

    const filename =
      range === "monthly"
        ? `SKU-wise Profitability-${convertToAbbreviatedMonth(month)}'${yearShort}.xlsx`
        : range === "quarterly"
          ? `SKU-wise Profitability-${quarter}'${yearShort}.xlsx`
          : `SKU-wise Profitability-Year'${yearShort}.xlsx`;

    XLSX.writeFile(wb, filename);
  }, [
    tableData,
    totals,
    countryName,
    getExtraRows,
    range,
    month,
    yearShort,
    quarter,
    getDisplayProductNameFromRow,
    buildExcelColumnsFromUI,
  ]);


  /* --------- Render guards --------- */
  if (loading) return <div>Loading...</div>;
  if (error) return <div className="text-red-600">Error: {error}</div>;

  return (
    <>
      <div className="rounded-xl border border-slate-200 bg-[#D9D9D933] shadow-sm p-4 sm:p-5">
        <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-wrap items-baseline gap-2 justify-center sm:justify-start">
            {/* <PageBreadcrumb pageTitle={getTitle()} variant="page" align="left" textSize="2xl" /> */}
            <PageBreadcrumb
              pageTitle={
                range === "monthly" ? (
                  <>
                    Monthly P&amp;L - Product Breakdown{" "}
                    {/* <span className="text-[#5EA68E] font-bold">
                      {convertToAbbreviatedMonth(month)}&apos;{String(year).slice(-2)}
                    </span> */}
                  </>
                ) : range === "quarterly" ? (
                  <>
                    Quarterly P&amp;L - Product Breakdown{" "}
                    {/* <span className="text-[#5EA68E] font-bold">
                      {quarter}&apos;{String(year).slice(-2)}
                    </span> */}
                  </>
                ) : (
                  <>
                    Yearly P&amp;L - Product Breakdown{" "}
                    {/* <span className="text-[#5EA68E] font-bold">
                      {year}
                    </span> */}
                  </>
                )
              }
              variant="page"
              align="left"
              textSize="2xl"
            />

            <span className="text-[#5EA68E] text-lg sm:text-2xl md:text-2xl font-bold">({currencySymbol})</span>
          </div>

          <div className="flex justify-center sm:justify-end">
            {!hideDownloadButton && <DownloadIconButton onClick={handleDownloadExcel} />}
          </div>
        </div>

        <div className={`transition-opacity ${noDataFound ? "opacity-30" : "opacity-100"}`}>
          {showModal2 && (
            <CustomModal onClose={() => setShowModal2(false)}>
              <SkuMultiCountryUpload onClose={() => setShowModal2(false)} onComplete={() => setShowModal2(false)} />
            </CustomModal>
          )}

          <div className="w-full overflow-x-auto rounded-xl border border-gray-300">
            <div className="min-w-full">
              <GroupedCollapsibleTable<TableRow>
                rows={tableData}
                leftCols={LEFT_COLS}
                groups={groups}

                singleCols={SINGLE_COLS}
                layout={[

                  { type: "group", id: "units_breakdown" },
                  { type: "single", key: "net_units_sold" },

                  { type: "single", key: "asp" },


                  { type: "group", id: "sales" },
                  { type: "single", key: "net_sales" },

                  { type: "group", id: "promotions_breakdown" },
                  { type: "single", key: "cost_of_unit_sold" },

                  { type: "group", id: "amazon_breakdown" },
                  { type: "single", key: "amazon_fee" },

                  { type: "group", id: "other_transactions_breakdown" },
                  { type: "single", key: "other_transactions" },

                  { type: "group", id: "profit_breakdown" },
                  { type: "single", key: "profit" },
                ]}
                initialCollapsed={{
                  units_breakdown: true,
                  sales: true,
                  promotions_breakdown: true,
                  amazon_breakdown: true,
                  other_transactions_breakdown: true,
                  profit_breakdown: true,
                }}
                toggleGroupByColKey={{
                  net_units_sold: "units_breakdown",
                  net_sales: "sales",
                  cost_of_unit_sold: "promotions_breakdown",
                  amazon_fee: "amazon_breakdown",
                  other_transactions: "other_transactions_breakdown",
                  profit: "profit_breakdown",
                }}
                onVisibleColCountChange={setMainColCount}
                showSignRowInBody
                getSignForCol={getSignForCol}
                getRowClassName={(row, index) => {
                  const isTotalRow =
                    String((row as any)?.product_name || "")
                      .trim()
                      .toLowerCase() === "total";

                  if (isTotalRow) {
                    return "bg-[#EFEFEF] font-semibold";
                  }

                  return index % 2 === 0 ? "bg-white" : "bg-gray-50";
                }}

                getValue={(row, colKey, rowIndex) => {
                  const isLastRow = rowIndex === tableData.length - 1;

                  if (colKey === "sno") return isLastRow ? "" : rowIndex + 1;
                  if (colKey === "sku") return (row as any).sku ?? "-";

                  if (colKey === "product_name") {
                    const name = getDisplayProductNameFromRow(row);
                    return !isLastRow ? (
                      <span
                        onClick={() => handleProductClick(String(name || ""))}
                        className="inline-block max-w-[220px] cursor-pointer truncate align-middle text-[#60a68e] no-underline"
                        title={String(name || "")}
                      >
                        {String(name || "-")}
                      </span>
                    ) : (
                      <span className="inline-block max-w-[220px] truncate">{String(name || "-")}</span>
                    );
                  }

                  return formatValue((row as any)[colKey], colKey);
                }}
                summary={{
                  enabled: mainColCount > 0,

                  sections: [
                    {
                      id: "ads",
                      label: "Cost of Advertisement",
                      endValue: formatValue(totals.advertising_total, "advertising_total"),
                      defaultCollapsed: true,
                      children: [
                        {
                          id: "ads_1",
                          label: <>Visibility - Ads <strong className="text-[#ff5c5c]">(-)</strong></>,
                          midValue: formatValue(totals.visible_ads, "visible_ads"),
                        },
                        {
                          id: "ads_2",
                          label: <>Visibility - Deals, Vouchers and Reviews <strong className="text-[#ff5c5c]">(-)</strong></>,
                          midValue: formatValue(totals.dealsvouchar_ads, "dealsvouchar_ads"),
                        },
                      ],
                    },

                    {
                      id: "other",
                      label: "Other Transactions",
                      endValue: formatValue(totals.other_transactions, "other_transactions"),
                      defaultCollapsed: true,
                      children: [
                        {
                          id: "other_1",
                          label: <>Platform Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                          midValue: formatValue(totals.platform_fee, "platform_fee"),
                        },
                        {
                          id: "other_2",
                          label: <>Inventory Storage Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                          midValue: formatValue(totals.inventory_storage_fees, "inventory_storage_fees"),
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
                      ],
                    },
                  ],

                  fixedRows: [
                    ...(countryName === "us" || countryName === "global"
                      ? [
                        {
                          id: "ship",
                          label: <>Shipment Charges <strong>(-)</strong></>,
                          endValue: formatValue(totals.shipment_charges, "shipment_charges"),
                        },
                      ]
                      : []),

                    { id: "cm2_profit", label: "CM2 Profit/Loss", endValue: formatValue(totals.cm2_profit, "cm2_profit") },
                    { id: "cm2_margins", label: "CM2 Margins", endValue: `${formatValue(totals.cm2_margins, "cm2_margins")}%` },

                    // ✅ TACoS first
                    { id: "tacos", label: "TACoS (Total Advertising Cost of Sale)", endValue: `${formatValue(totals.acos, "acos")}%` },

                    // ✅ then Net Reimbursement (below TACoS)
                    {
                      id: "net_reimb",
                      label: "Net Reimbursement",
                      endValue: formatValue(Math.abs(totals.net_reimbursement), "net_reimbursement"),
                    },
                    {
                      id: "rv_cm2",
                      label: "Reimbursement vs CM2 Margins",
                      endValue: `${formatValue(totals.rembursment_vs_cm2_margins, "rembursment_vs_cm2_margins")}%`,
                    },
                    {
                      id: "rv_sales",
                      label: "Reimbursement vs Sales",
                      endValue: `${formatValue(totals.reimbursement_vs_sales, "reimbursement_vs_sales")}%`,
                    },
                  ],

                  valueCols: 2,
                }}
              />


            </div>
          </div>
        </div>
      </div>

      {/* Top & Bottom tables */}
      <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
        <div className="flex flex-col justify-between gap-7 md:gap-3 text-[#414042] md:flex-row min-w-0">
          {/* Top 5 */}
          <div className="flex-1 min-w-0">
            <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
              <PageBreadcrumb pageTitle="Most 5 Profitable Products" variant="page" align="left" textSize="2xl" />
            </div>

            <div className="overflow-x-auto rounded-xl border border-gray-300">
              <table className="w-full table-auto border-collapse">
                <thead>
                  <tr className="bg-green-500 font-bold text-[#f8edcf]">
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-[10px] 2xl:text-xs break-words leading-snug">
                      Product Name
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      CM1 Profit ({currencySymbol})
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      Profit Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      Sales Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      CM1 Profit per Unit ({currencySymbol})
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {topData.rows.map((item, index) => (
                    <tr key={index} className={`${index % 2 === 0 ? "bg-white" : "bg-gray-50"}`}>
                      <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-[10px] 2xl:text-xs whitespace-normal break-words align-top">
                        <span title={item.product_name} className="block">
                          {item.product_name || "-"}
                        </span>
                      </td>

                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.profit}
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.profitMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.salesMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.unit_wise_profitability}
                      </td>
                    </tr>
                  ))}

                  <tr className="bg-[#EFEFEF] font-semibold">
                    <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-[10px] 2xl:text-xs">
                      <strong>Total</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong>{topData.totals.profit}</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong>{topData.totals.profitMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong>{topData.totals.salesMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong></strong>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          {/* Bottom 5 */}
          <div className="flex-1 min-w-0">
            <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
              <PageBreadcrumb pageTitle="Least 5 Profitable Products" variant="page" align="left" textSize="2xl" />
            </div>

            <div className="overflow-x-auto rounded-xl border border-gray-300">
              <table className="w-full table-auto border-collapse">
                <thead>
                  <tr className="bg-[#B75A5A] font-bold text-[#f8edcf]">
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-[10px] 2xl:text-xs break-words leading-snug">
                      Product Name
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      CM1 Profit ({currencySymbol})
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      Profit Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      Sales Mix (%)
                    </th>
                    <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs break-words leading-snug">
                      CM1 Profit per Unit ({currencySymbol})
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {bottomData.rows.map((item, index) => (
                    <tr key={index} className={`${index % 2 === 0 ? "bg-white" : "bg-gray-50"}`}>
                      <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-[10px] 2xl:text-xs whitespace-normal break-words align-top">
                        <span title={item.product_name} className="block">
                          {item.product_name || "-"}
                        </span>
                      </td>

                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.profit}
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.profitMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.salesMix}%
                      </td>
                      <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                        {item.unit_wise_profitability}
                      </td>
                    </tr>
                  ))}

                  <tr className="bg-[#EFEFEF] font-semibold">
                    <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-[10px] 2xl:text-xs">
                      <strong>Total</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong>{bottomData.totals.profit}</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong>{bottomData.totals.profitMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong>{bottomData.totals.salesMix}%</strong>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-[10px] 2xl:text-xs">
                      <strong></strong>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      {showModal && selectedProduct && (
        <Productinfoinpopup
          productname={selectedProduct}
          countryName={countryName}
          month={month}
          year={year}
          onClose={() => setShowModal(false)}
        />
      )}
    </>
  );
};

export default SKUtable;



