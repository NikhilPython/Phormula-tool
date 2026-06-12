"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSelector } from "react-redux";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import DashboardBargraphCard from "@/components/dashboard/DashboardBargraphCard";
import SalesTargetCard from "@/components/dashboard/SalesTargetCard";
import SalesTargetStatsCard from "@/components/dashboard/SalesTargetStatsCard";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import CurrentInventorySection from "@/components/dashboard/CurrentInventorySection";
import { RootState } from "@/lib/store";
import { useAmazonConnections } from "@/lib/utils/useAmazonConnections";
import HashScroll from "@/components/common/HashScroll";
import {
    getISTYearMonth,
    getPrevISTYearMonth,
    getPrevMonthShortLabel,
    getISTDayInfo,
} from "@/lib/dashboard/date";
import {
    fmtGBP,
    fmtUSD,
    fmtNum,
    fmtPct,
    fmtInt,
    toNumberSafe,
} from "@/lib/dashboard/format";
import type { RegionKey, RegionMetrics } from "@/lib/dashboard/types";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { usePlatform } from "@/components/context/PlatformContext";
import type { PlatformId } from "@/lib/utils/platforms";
import LiveBiLineGraph from "@/components/businessInsight/LiveBiLineChartPanel";
import { DateRange } from "react-date-range";
import "react-date-range/dist/styles.css";
import "react-date-range/dist/theme/default.css";
import { FaCalendarAlt } from "react-icons/fa";
import LiveBusinessClient from "@/app/(admin)/live-business-insight/[ranged]/[countryName]/[month]/[year]/liveBusinessClient";
import { useRouter, useParams } from "next/navigation";
import Cm1ProfitBreakdownPie from "@/components/dashboard/Cm1ProfitBreakdownPie";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import GroupedCollapsibleTable, {
    ColGroup,
    type LeafCol,
} from "@/components/ui/table/GroupedCollapsibleTable";
import {
    exportPnLProductwiseBreakdownMtdExcel,
    exportCurrentInventoryExcel,
} from "@/lib/excel/exportCurrentInventoryExcel";
import InfoTip from "@/components/ui/InfoTip";
import * as XLSX from "xlsx-js-style";
import { fetchCurrentInventoryData, InventoryRow } from "@/lib/inventory/fetchCurrentInventoryData";
import Alert from "@/components/ui/alert/Alert";
import { ApiResponse } from "@/components/businessInsight/types";
import DashboardStickyKpis from "./DashboardStickyKpis";
import { IoMdLock } from "react-icons/io";
import { Toaster, toast } from "sonner";
import { useHeaderNotifications } from "@/components/context/NotificationContext";
import InventoryAgeGraphSection from "@/components/dashboard/InventoryAgeGraphSection";
import SkuRecommendationDrawer from "@/components/dashboard/SkuRecommendationDrawer";

const TERM_DEFINITIONS: Record<string, string> = {
    product_name: "Product Name. The delta represents the change compared to the previous period.",
    asp: "Average Selling Price",
    ads_spend: "Ads Spend",
    acos: "ACos",
    cogs: "Cogs",
    net_units_sold: "Net Units Sold",
    net_sales: "Net Sales",
    net_taxes: "Net Taxes",
    net_credits: "Net Credits",
    tex_and_credits: "Taxes & Credits = combined effect of taxes and credits applied to orders (used to reconcile from gross to net).",
    marketplace_fees: "Marketplace Fees = total fees charged by Amazon (e.g., referral + FBA fees).",
    amazon_fee: "Marketplace Fees = total fees charged by Amazon (e.g., referral + FBA fees).",
    selling_fees: "Selling Fees = Amazon referral/commission and selling-related fees (non-FBA components).",
    fba_fees: "FBA Fees = fulfillment, storage-related and FBA service fees (as mapped in reports).",
    promotional_rebates: "Promotions = promotional rebates/discounts applied (coupons/deals) that reduce profitability.",
    promotional_rebates_percentage: "Promotions % = Promotions ÷ Net Sales × 100.",
    cost_of_unit_sold: "COGS = Cost of goods sold for the units sold in the period (as provided/derived).",
    cm1_profit: "CM1 Profit",
    cm2_profit: "CM2 Profit",
    profit_percentage: "CM1 Profit % = CM1 Profit ÷ Net Sales × 100.",
    unit_wise_profitability: "CM1 Profit Per Unit = CM1 Profit ÷ Net Units Sold.",
};

const ROUND_LABELS = [
    "Gross Sales",
    "Net Sales",
    "Cost of Ads",
    "CM2 Profit",
];

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

type Cm1PieSlice = {
    name: string;
    value: number;
    prevValue: number;
    pct: number;
    deltaPct: number | null;
};

type MonthlySpRow = {
    sno: number | null;
    products: string | null;
    spend: number | null;
};

type MonthlyAdsSpentRow = {
    sno?: number | null;
    sku: string;
    ad_spend: number;
    isTotal?: boolean;
    isOthers?: boolean;
};

type MonthlySkuwiseRow = {
    sno?: number;
    sku: string;
    product_name: string;
    ad_type?: string;

    quantity: number;
    return_quantity?: number;
    total_quantity?: number;
    asp: number;
    net_sales: number;
    debt_payment?: number;
    disbursement?: number;
    net_taxes?: number;
    other_transactions?: number;
    misc_transaction?: number;
    cogs: number;
    fba_fees: number;
    selling_fees: number;
    ads_spend: number;
    acos: number;
    cm2_profit: number;
    tax: number;
    credits: number;
    tax_and_credits: number;
    cm1_profit_per: number;
    cm1_profit_per_unit: number;
    cm2_profit_per: number;
    cm2_profit_per_unit: number;
    profit: number;

    platform_fee?: number;
    platform_fee_inventory_storage?: number;
    lost_total?: number;
    other?: number;
    product_spend?: number;
    display_spend?: number;
    brand_spend?: number;
    dealsvouchar_ads?: number;
    platformfeenew?: number;

    previous_net_sales?: number;
    net_sales_delta?: number;
    net_sales_delta_percentage?: number | null;

    total_cm2_profit?: number;
    total_cm2_margins?: number;
    tacos_total_advertising_cost_of_sale?: number;
    current_net_reimbursement?: number;
    reimbursement_vs_sales?: number;
    reimbursement_vs_cm2_margins?: number;
    shipment_fees?: number;
    total_ads?: number;
    advertising_fees?: number;
    amazon_fees?: number;
    isTotal?: boolean;
    isOthers?: boolean;
};

type GrandTotalSkuwiseRow = Partial<MonthlySkuwiseRow> & {
    gross_sales?: number;
    total_ads?: number;
    advertising_total?: number;
    amazon_fees?: number;
    advertising_fees?: number;
    tacos_total_advertising_cost_of_sale?: number;
    total_cm2_profit?: number;
    total_cm2_margins?: number;
    profit_percentage?: number;
    shipment_fees?: number;
    debt_payment?: number;
    disbursement?: number;
    current_net_reimbursement?: number;
    reimbursement_vs_sales?: number;
    reimbursement_vs_cm2_margins?: number;
    marketplace_fees?: number;
};

type MonthlySkuwiseTableRow = MonthlySkuwiseRow & {
    isOthers?: boolean;
    isTotal?: boolean;
};

type FetchLiveBiPayloadArgs = {
    startDay?: number | null;
    endDay?: number | null;
    generateInsights?: boolean;
    skipLoader?: boolean;
};

type ProductwiseMoneyKey =
    | "asp"
    | "net_sales"
    | "net_taxes"

    | "other_transactions"
    | "cogs"
    | "fba_fees"
    | "selling_fees"
    | "ads_spend"
    | "cm2_profit"
    | "tax"
    | "credits"
    | "tax_and_credits"
    | "cm1_profit_per_unit"
    | "cm2_profit_per_unit"
    | "profit"
    | "platformfeenew"
    | "debt_payment"
    | "disbursement"
    | "platform_fee"
    | "platform_fee_inventory_storage"
    | "lost_total"
    | "other"
    | "misc_transaction"
    | "product_spend"
    | "display_spend"
    | "brand_spend"
    | "dealsvouchar_ads"
    | "platformfeenew";

type CountryTimezoneResponse = {
    country: "uk" | "us";
    country_label: string;
    india: {
        timezone: string;
        abbreviation: string;
        datetime: string;
        date: string;
        time: string;
    };
    selected_country: {
        timezone: string;
        abbreviation: string;
        datetime: string;
        date: string;
        time: string;
    };
};

/* ===================== ENV & ENDPOINTS ===================== */
const baseURL =
    process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";
const FIN_MTD_TX_ENDPOINT = `${baseURL}/amazon_api/finances/mtd_transactions`;
const PREVIOUS_SKUWISE_GLOBAL_ENDPOINT = `${baseURL}/live_mtd_bi/previous_skuwise_global`;
const SHOPIFY_DROPDOWN_ENDPOINT = `${baseURL}/shopify/dropdown`;
// const FX_RATES_GET_ENDPOINT = `${baseURL}/currency-rates`;

const LIVE_MTD_BI_ENDPOINT = `${baseURL}/live_mtd_bi`;
const LIVE_DASHBOARD_CACHE_ENDPOINT = `${baseURL}/amazon_api/live-dashboard/save`;
const COUNTRY_TIMEZONE_ENDPOINT = `${baseURL}/country-timezone`;

const MONTHLY_SP_ENDPOINT = `${baseURL}/api/ads/monthly_sp_sd_to_db`;
const GBP_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_GBP_TO_USD || "1.25");
const INR_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_INR_TO_USD || "0.01128");
const CAD_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_CAD_TO_USD || "0.74");
const SB_KEYWORD_ENDPOINT = `${baseURL}/api/ads/manager/sb_keyword_report`;

const USE_MANUAL_LAST_MONTH =
    (process.env.NEXT_PUBLIC_USE_MANUAL_LAST_MONTH || "false").toLowerCase() ===
    "true";

const MANUAL_LAST_MONTH_USD_GLOBAL = Number(
    process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_GLOBAL || "0"
);
const MANUAL_LAST_MONTH_USD_UK = Number(
    process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_UK || "0"
);
const MANUAL_LAST_MONTH_USD_US = Number(
    process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_US || "0"
);
const MANUAL_LAST_MONTH_USD_CA = Number(
    process.env.NEXT_PUBLIC_MANUAL_LAST_MONTH_USD_CA || "0"
);
const LAST_REFRESH_KEY = "live-dashboard-last-refresh";


const formatValue = (label: string, value: number) => {
    if (value == null || isNaN(value)) return "—";

    // Percentage case (if you have % cards)
    if (label.includes("%")) {
        return `${value.toFixed(2)}%`;
    }

    // Round only selected labels
    if (ROUND_LABELS.includes(label)) {
        return `£${Math.round(value).toLocaleString()}`;
    }

    // Default → 2 decimal currency
    return `£${value.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    })}`;
};

const formatRoundedCurrency = (val: any) => {
    const n = toNumberSafe(val);
    if (!n) return "£0";
    return `£${Math.round(n).toLocaleString()}`;
};

/* ===================== BI TYPES (for shared cards + graph) ===================== */
type ChartMetric = "net_sales" | "quantity";

type DailyPoint = {
    date: string;
    quantity?: number;
    net_sales?: number;
    gross_sales?: number;
    profit?: number;
    cm2_profit?: number;
};


type ApiDailySeries = {
    previous?: DailyPoint[];
    current_mtd?: DailyPoint[];

    previous_global?: DailyPoint[];
    current_mtd_global?: DailyPoint[];

    previous_uk?: DailyPoint[];
    current_mtd_uk?: DailyPoint[];

    previous_us?: DailyPoint[];
    current_mtd_us?: DailyPoint[];

    previous_ca?: DailyPoint[];
    current_mtd_ca?: DailyPoint[];
};

type GraphDailySeries = {
    previous: DailyPoint[];
    current_mtd: DailyPoint[];
};

type PeriodInfo = {
    label: string;
    start_date: string;
    end_date: string;
};

type Cm1ProfitPieApiSlice = {
    name: string;
    profit_curr: number;
    profit_prev: number;
    pct: number;
    delta_pct: number;
};

type Cm1ProfitPieApi = {
    min_named?: number;
    pareto_threshold?: number;
    total_profit_curr?: number;
    slices: Cm1ProfitPieApiSlice[];
};

type BiApiResponse = {
    message?: string;
    periods?: {
        previous?: PeriodInfo;
        current_mtd?: PeriodInfo;
    };
    daily_series?: ApiDailySeries;

    aligned_totals?: BiAlignedTotals;

    categorized_growth?: any;
    insights?: Record<string, any>;
    ai_insights?: Record<string, any>;
    overall_summary?: string;
    overall_actions?: string[];
    cm1_profit_pie?: Cm1ProfitPieApi;
};

type BiAlignedTotals = {
    current_cm2_profit?: number;
    previous_cm2_profit?: number;

    total_current_profit_cm2?: number;
    total_previous_profit_cm2?: number;

    total_current_profit_percentage?: number;
    total_previous_profit_percentage?: number;

    total_previous_net_sales_full_month?: number;
    total_previous_net_sales?: number;
    total_current_net_sales?: number;

    total_current_advertising?: number;
    total_previous_advertising?: number;

    total_current_platform_fees?: number;
    total_previous_platform_fees?: number;

    total_current_profit?: number;
    total_previous_profit?: number;

    total_current_rembursement_fee: number;
    total_previous_rembursement_fee: number;
};

type InventoryAlertRecord = Record<string, { alert?: string; alert_type?: string }>;

type UiAlert = {
    id: string;
    title: string;
    message: string;
    variant: "success" | "error" | "warning" | "info";
};

/* ===================== SMALL HELPERS ===================== */
const getShort = (label?: string) => (label ? label.split(" ")[0] || label : "");

const formatPositive2Decimal = (value: any) => {
    const n = Number(value);
    if (!Number.isFinite(n)) return "0.00";
    return Math.abs(n).toFixed(2);
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const toVariant = (t?: string): UiAlert["variant"] => {
    const x = (t || "").toLowerCase();

    if (x.includes("error") || x.includes("danger") || x.includes("critical")) return "error";
    if (x.includes("warn")) return "warning";
    if (x.includes("success") || x.includes("ok")) return "success";
    return "info";
};

const normalizeSku = (v: any) => String(v || "").trim().toUpperCase();

const normalizeProductDisplayName = (value: any) => {
    const raw = String(value ?? "").trim();

    if (!raw) return "Unknown";

    const lower = raw.toLowerCase();

    if (lower === "grand total") return "Total";
    if (lower === "total") return "Total";
    if (lower === "others") return "Others";

    return lower
        .split(/(\s+|\+|-|\/)/)
        .map((part) => {
            if (/^\s+$/.test(part)) return part;
            if (["+", "-", "/"].includes(part)) return part;

            return part
                .split("'")
                .map((piece) =>
                    piece
                        ? piece.charAt(0).toUpperCase() + piece.slice(1)
                        : piece
                )
                .join("'");
        })
        .join("");
};

const formatPlainAmount = (value: unknown) => {
    const n = toNumberSafe(value ?? 0);

    if (Number.isInteger(n)) {
        return String(n);
    }

    return n.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
    });
};

const formatRoundedNumber = (value: unknown) => {
    const n = toNumberSafe(value ?? 0);

    return Math.round(n).toLocaleString("en-GB", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
    });
};

const getRegionDateFromTimestamp = (
    timestamp: number | string | Date,
    region: RegionKey
) => {
    const date = new Date(timestamp);

    return new Date(
        date.toLocaleString("en-US", {
            timeZone: getTimezoneForRegion(region),
        })
    );
};

const getRegionYearMonthFromTimestamp = (
    region: RegionKey,
    timestamp: number | string | Date
) => {
    const date = getRegionDateFromTimestamp(timestamp, region);

    return {
        monthName: date.toLocaleString("en-US", { month: "long" }),
        year: date.getFullYear(),
    };
};

const getPrevRegionYearMonthFromTimestamp = (
    region: RegionKey,
    timestamp: number | string | Date
) => {
    const date = getRegionDateFromTimestamp(timestamp, region);
    date.setMonth(date.getMonth() - 1);

    return {
        monthName: date.toLocaleString("en-US", { month: "long" }),
        year: date.getFullYear(),
    };
};

const formatMonthYearShort = (monthName: string, year: number) => {
    return `${monthName.slice(0, 3)}'${String(year).slice(-2)}`;
};

/* ===================== P&L PRODUCTWISE SUMMARY (MTD) HELPERS ===================== */
type PlSummaryTotals = {
    advertising_total: number;
    visible_ads: number;
    dealsvouchar_ads: number;

    other_transactions: number;
    platform_fee: number;
    inventory_storage_fees: number;
    platform_fee_inventory_storage: number;
    misc_transaction: number;

    reimbursement_lost_inventory_amount: number;
    reimbursement_lost_inventory_units: number;
    lost_total: number;

    shipment_charges: number;
    reimbursement_vs_sales: number;

    cm2_profit: number;
    cm2_margins: number;
    acos: number;

    rembursment_vs_cm2_margins: number;
    net_reimbursement: number;
    debt_payment: number;
    disbursement: number;

    profit: number;
    net_sales: number;
};

const toNumber = (v: any) => {
    if (v === undefined || v === null || v === "") return 0;
    if (typeof v === "number") return Number.isFinite(v) ? v : 0;
    const n = Number(String(v).replace(/,/g, "").trim());
    return Number.isFinite(n) ? n : 0;
};

const formatSummaryRounded = (value: any) => {
    const n = toNumber(value);
    if (!Number.isFinite(n)) return "-";
    return Math.round(Math.abs(n)).toLocaleString();
};

const INT_KEYS = new Set<string>(["reimbursement_lost_inventory_units"]);

const SIGNED_KEYS = new Set<string>([
    "cm2_profit",
    "cm2_margins",
    "rembursment_vs_cm2_margins",
    "reimbursement_vs_sales",
    "misc_transaction",
]);

const ROUNDED_SUMMARY_KEYS = new Set<string>([
    "misc_transaction",
    "lost_total",
    "net_reimbursement",
    "debt_payment",
    "disbursement",
]);

function computePlSummaryTotalsFromSource(source: any): PlSummaryTotals {
    const netSales = toNumber(source?.Net_Sales ?? source?.net_sales);

    const advertisingTotal = toNumber(
        source?.advertising_total ??
        source?.ads_total ??
        source?.ads_spend_total ??
        source?.advertising_fees
    );

    const tacos = netSales
        ? (Math.abs(advertisingTotal) / Math.abs(netSales)) * 100
        : 0;

    const platformFees = toNumber(
        source?.platformfeenew ?? source?.platform_fee_new ?? source?.platform_fee
    );

    const inventoryStorageFees = toNumber(source?.platform_fee_inventory_storage);

    const netReimbursement = toNumber(
        source?.rembursement_fee ??
        source?.reimbursement_fee ??
        source?.net_reimbursement
    );

    const reimbursementUnits = toNumber(
        source?.reimbursement_lost_inventory_units ??
        source?.reimbursement_units ??
        source?.lost_inventory_units
    );

    const cm2MarginsValue = toNumber(
        source?.cm2_margins ??
        source?.cm2_profit_percentage ??
        source?.cm2_profit_percent ??
        source?.cm2_profit_percentage_value ??
        source?.cm2_margin ??
        source?.cm2_margin_pct
    );

    return {
        advertising_total: advertisingTotal,
        visible_ads: toNumber(source?.visible_ads ?? source?.ads_visibility),
        dealsvouchar_ads: toNumber(source?.dealsvouchar_ads ?? source?.deals_vouchers_ads),

        other_transactions: toNumber(source?.other_transactions ?? source?.platform_fee ?? source?.other_fees_total),
        platform_fee: platformFees,
        inventory_storage_fees: inventoryStorageFees,
        platform_fee_inventory_storage: inventoryStorageFees,
        misc_transaction: toNumber(source?.misc_transaction ?? source?.misc_transactions),

        reimbursement_lost_inventory_amount: toNumber(
            source?.reimbursement_lost_inventory_amount ?? source?.lost_inventory_amount
        ),
        reimbursement_lost_inventory_units: reimbursementUnits,
        lost_total: toNumber(source?.lost_total),

        shipment_charges: toNumber(
            source?.shipment_charges ??
            source?.shipping_charges ??
            source?.shipment_fees ??
            source?.shipment_charge ??
            source?.shipping_charge ??
            source?.shipping_fees ??
            source?.shipmentCharges ??
            source?.shippingCharges
        ),
        reimbursement_vs_sales: toNumber(
            source?.reimbursement_vs_sales ?? source?.reimbursement_vs_net_sales
        ),

        cm2_profit: toNumber(source?.cm2_profit),
        cm2_margins: cm2MarginsValue,
        acos: tacos,

        rembursment_vs_cm2_margins: toNumber(
            source?.rembursment_vs_cm2_margins ?? source?.reimbursement_vs_cm2_margins
        ),
        net_reimbursement: netReimbursement,

        debt_payment: toNumber(source?.debt_payment),
        disbursement: toNumber(source?.disbursement),

        profit: toNumber(source?.Profit ?? source?.profit ?? source?.cm1_profit),
        net_sales: netSales,
    };
}

function getGrandTotalRow(rows: any[] = []) {
    return (
        rows.find((r) => r?.sku === "GRAND_TOTAL") ||
        rows.find((r) => String(r?.product_name || "").toLowerCase() === "grand total") ||
        rows.find((r) => r?.isTotal) ||
        rows[rows.length - 1] ||
        {}
    );
}

function computePlSummaryTotalsFromSkuwise(rows: any[]): PlSummaryTotals {
    const grand = getGrandTotalRow(rows);

    const sumMiscTransaction = rows
        .filter((r) => {
            const sku = String(r?.sku || "").toUpperCase();
            const name = String(r?.product_name || "").toLowerCase();
            return sku !== "GRAND_TOTAL" && name !== "grand total" && !r?.isTotal;
        })
        .reduce((sum, r) => sum + toNumber(r?.misc_transaction), 0);

    return {
        advertising_total: toNumber(grand?.total_ads),
        visible_ads: toNumber(grand?.product_spend),
        dealsvouchar_ads: toNumber(grand?.dealsvouchar_ads),

        other_transactions: toNumber(grand?.other),
        platform_fee: toNumber(grand?.platformfeenew ?? grand?.platform_fee),
        inventory_storage_fees: toNumber(grand?.platform_fee_inventory_storage),
        platform_fee_inventory_storage: toNumber(grand?.platform_fee_inventory_storage),
        misc_transaction: toNumber(grand?.misc_transaction) || sumMiscTransaction,

        reimbursement_lost_inventory_amount: toNumber(grand?.lost_total),
        reimbursement_lost_inventory_units: 0,
        lost_total: toNumber(grand?.lost_total),

        shipment_charges: toNumber(grand?.shipment_fees),
        reimbursement_vs_sales: toNumber(grand?.reimbursement_vs_sales),

        cm2_profit: toNumber(grand?.total_cm2_profit ?? grand?.cm2_profit),
        cm2_margins: toNumber(grand?.total_cm2_margins ?? grand?.cm2_profit_per),
        acos: toNumber(grand?.tacos_total_advertising_cost_of_sale),

        rembursment_vs_cm2_margins: toNumber(grand?.reimbursement_vs_cm2_margins),

        net_reimbursement: toNumber(grand?.current_net_reimbursement),
        debt_payment: toNumber(grand?.debt_payment),
        disbursement: toNumber(grand?.disbursement),

        profit: toNumber(grand?.profit),
        net_sales: toNumber(grand?.net_sales),
    };
}

function computePlSummaryTotals(
    data: any,
    skuwiseRows: any[],
    platform?: PlatformId
): PlSummaryTotals {
    const apiRows =
        platform === "global" && Array.isArray(data?.skuwise_items_global)
            ? data.skuwise_items_global
            : Array.isArray(data?.skuwise_items)
                ? data.skuwise_items
                : skuwiseRows || [];

    return computePlSummaryTotalsFromSkuwise(apiRows);
}

const formatSummaryValue = (value: unknown, key: string) => {
    if (value === undefined || value === null || value === "") return "-";

    const raw = toNumber(value);
    if (!Number.isFinite(raw)) return "-";

    // preserve sign for certain metrics; otherwise display as absolute value
    const n = SIGNED_KEYS.has(key) ? raw : Math.abs(raw);

    if (INT_KEYS.has(key)) return String(Math.trunc(n));

    // ✅ rounded summary rows without decimals
    if (ROUNDED_SUMMARY_KEYS.has(key)) {
        const rounded = Math.round(Math.abs(n)).toLocaleString();
        return n < 0 ? `-${rounded}` : rounded;
    }

    const formatted = Math.abs(n).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });

    return n < 0 ? `-${formatted}` : formatted;
};

const monthToNumber = (monthName: string): number => {
    const months: Record<string, number> = {
        january: 1,
        february: 2,
        march: 3,
        april: 4,
        may: 5,
        june: 6,
        july: 7,
        august: 8,
        september: 9,
        october: 10,
        november: 11,
        december: 12,
    };
    return months[monthName.toLowerCase()] || 1;
};

async function withLocalStorageLock<T>(
    lockKey: string,
    fn: () => Promise<T>,
    ttlMs = 2 * 60 * 1000 // 2 minutes
): Promise<T | null> {
    const now = Date.now();
    const existing = Number(localStorage.getItem(lockKey) || "0");

    // if lock is active and not expired, skip
    if (existing && now - existing < ttlMs) return null;

    // set lock
    localStorage.setItem(lockKey, String(now));

    try {
        return await fn();
    } finally {
        // release lock
        localStorage.removeItem(lockKey);
    }
}

const REGION_TIMEZONE: Record<RegionKey, string> = {
    Global: "Asia/Kolkata", // or keep a default of your choice
    UK: "Europe/London",
    US: "America/Los_Angeles",
    CA: "America/Toronto",
};

const getTimezoneForRegion = (region: RegionKey) => {
    return REGION_TIMEZONE[region] || "Asia/Kolkata";
};

const getRegionNow = (region: RegionKey) => {
    const tz = getTimezoneForRegion(region);

    return new Date(
        new Date().toLocaleString("en-US", {
            timeZone: tz,
        })
    );
};

const getRegionYearMonth = (region: RegionKey) => {
    const now = getRegionNow(region);

    const monthName = now.toLocaleString("en-US", {
        month: "long",
    });

    return {
        monthName,
        year: now.getFullYear(),
    };
};

const getPrevRegionYearMonth = (region: RegionKey) => {
    const now = getRegionNow(region);
    now.setMonth(now.getMonth() - 1);

    const monthName = now.toLocaleString("en-US", {
        month: "long",
    });

    return {
        monthName,
        year: now.getFullYear(),
    };
};

const getDayOfMonthByRegion = (region: RegionKey) => {
    return getRegionNow(region).getDate();
};

const getRegionDayInfo = (region: RegionKey) => {
    const now = getRegionNow(region);
    const todayDay = now.getDate();

    const prevMonthDate = new Date(now.getFullYear(), now.getMonth(), 0);
    const daysInPrevMonth = prevMonthDate.getDate();

    return { todayDay, daysInPrevMonth };
};

const getDaysInMonthByRegion = (region: RegionKey) => {
    const now = getRegionNow(region);
    return new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
};

// ===================== ADS REPORT SEED (SP + SD) - ONCE PER DAY =====================

const decodeJwtUserId = (jwt: string): string | null => {
    try {
        const payloadPart = jwt.split(".")[1];
        if (!payloadPart) return null;

        const base64 = payloadPart.replace(/-/g, "+").replace(/_/g, "/");
        const json = decodeURIComponent(
            atob(base64)
                .split("")
                .map((c) => "%" + c.charCodeAt(0).toString(16).padStart(2, "0"))
                .join("")
        );

        const payload = JSON.parse(json);
        return payload?.user_id != null ? String(payload.user_id) : null;
    } catch {
        return null;
    }
};

// const getBackendCountryDate = useCallback(() => {
//     const dt = countryTime?.selected_country?.datetime;

//     if (!dt) {
//         return new Date();
//     }

//     // Backend format: "YYYY-MM-DD HH:mm:ss"
//     // Convert to browser-safe local Date object.
//     return new Date(dt.replace(" ", "T"));
// }, [countryTime]);



const roundForAmazonCard = (label: string, value: number) => {
    const n = Number(value || 0);

    // keep decimals for percentage cards and ASP
    if (label.includes("%") || label === "ASP") return n;

    // round only these cards
    if (ROUND_LABELS.includes(label)) return Math.round(n);

    return n;
};

const getIstTodayISO = () => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    const y = ist.getFullYear();
    const m = ist.getMonth() + 1; // 1..12
    const d = ist.getDate();
    return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
};

const getIstMonthStartISO = () => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    const y = ist.getFullYear();
    const m = ist.getMonth() + 1; // 1..12
    return `${y}-${String(m).padStart(2, "0")}-01`;
};

const getGlobalPreviousSkuwiseAsOfISO = () => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));

    const y = ist.getFullYear();
    const m = String(ist.getMonth() + 1).padStart(2, "0");
    const d = String(ist.getDate()).padStart(2, "0");

    return `${y}-${m}-${d}`;
};

const getIstMonthToTodayRangeISO = () => ({
    start_date: getIstMonthStartISO(),
    end_date: getIstTodayISO(),
});

const ensureSpReportSeedOncePerDay = async (
    baseUrl: string,
    jwtToken: string,
    country: string // "UK" | "US" | "CA"
) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";
    const { start_date, end_date } = getIstMonthToTodayRangeISO();

    // once per user + country + day
    const storageKey = `sp_report_seed_daily_${userId}_${country}_${end_date}`;
    if (localStorage.getItem(storageKey) === "1") return;

    const lockKey = `${storageKey}_lock`;

    const didRun = await withLocalStorageLock(lockKey, async () => {
        // re-check after lock to avoid race
        if (localStorage.getItem(storageKey) === "1") return;

        const body = {
            start_date,
            end_date,
            time_unit: "SUMMARY",
            countries: [country],
            return_excel: false,
        };

        const res = await fetch(
            `${baseUrl}/api/ads/manager/sp_advertised_product_report`,
            {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${jwtToken}`,
                    Accept: "application/json",
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(body),
            }
        );

        if (!res.ok) {
            const errJson = await res.json().catch(() => ({}));
            const msg = String(errJson?.error || errJson?.message || "");

            const isDuplicate =
                msg.toLowerCase().includes("uniqueviolation") ||
                msg.toLowerCase().includes("duplicate key value") ||
                msg.toLowerCase().includes("already exists");

            if (isDuplicate) {
                localStorage.setItem(storageKey, "1");
                return;
            }

            throw new Error(msg || `Failed to seed SP report (${res.status})`);
        }

        localStorage.setItem(storageKey, "1");
    });

    if (didRun === null) return;
};

const ensureSdReportSeedOncePerDay = async (
    baseUrl: string,
    jwtToken: string,
    country: string // "UK" | "US"
) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";
    const { start_date, end_date } = getIstMonthToTodayRangeISO();

    // once per user + country + day
    const storageKey = `sd_report_seed_daily_${userId}_${country}_${end_date}`;
    if (localStorage.getItem(storageKey) === "1") return;

    const lockKey = `${storageKey}_lock`;

    const didRun = await withLocalStorageLock(lockKey, async () => {
        // re-check after lock to avoid race
        if (localStorage.getItem(storageKey) === "1") return;

        // ✅ BODY EXACTLY AS REQUESTED (same keys/shape)
        const body = {
            start_date,
            end_date,
            time_unit: "SUMMARY",
            countries: [country], // ["UK"] or ["US"]
            max_wait_seconds: 900,
            poll_every_seconds: 10,
        };

        const res = await fetch(
            `${baseUrl}/api/ads/manager/sd_advertised_product_report/sync`,
            {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${jwtToken}`,
                    Accept: "application/json",
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(body),
            }
        );

        if (res.ok) {
            localStorage.setItem(storageKey, "1");
            return;
        }

        const errJson = await res.json().catch(() => ({}));
        const msg = String(errJson?.error || errJson?.message || "");
        throw new Error(msg || `Failed to seed SD /sync (${res.status})`);
    });

    // ✅ IMPORTANT: if another tab/render is running it, don't error, just exit
    if (didRun === null) return;
};

const ensureSbKeywordReportSeedOncePerDay = async (
    baseUrl: string,
    jwtToken: string,
    country: string // "UK" | "US" | "CA"
) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";
    const { start_date, end_date } = getIstMonthToTodayRangeISO();

    // once per user + country + day
    const storageKey = `sb_keyword_report_seed_daily_${userId}_${country}_${end_date}`;
    if (localStorage.getItem(storageKey) === "1") return;

    const lockKey = `${storageKey}_lock`;

    const didRun = await withLocalStorageLock(lockKey, async () => {
        // re-check after lock to avoid race
        if (localStorage.getItem(storageKey) === "1") return;

        // ✅ BODY EXACTLY AS REQUESTED
        const body = {
            start_date,
            end_date,
            time_unit: "SUMMARY",
            countries: [country],
            return_excel: false,
        };

        const res = await fetch(`${baseUrl}/api/ads/manager/sb_keyword_report`, {
            method: "POST",
            headers: {
                Authorization: `Bearer ${jwtToken}`,
                Accept: "application/json",
                "Content-Type": "application/json",
            },
            body: JSON.stringify(body),
        });

        if (!res.ok) {
            const errJson = await res.json().catch(() => ({}));
            const msg = String(errJson?.error || errJson?.message || "");

            const isDuplicate =
                msg.toLowerCase().includes("uniqueviolation") ||
                msg.toLowerCase().includes("duplicate key value") ||
                msg.toLowerCase().includes("already exists");

            if (isDuplicate) {
                localStorage.setItem(storageKey, "1");
                return;
            }

            throw new Error(msg || `Failed to seed SB Keyword report (${res.status})`);
        }

        localStorage.setItem(storageKey, "1");
    });

    // ✅ if another tab/render is running it, don't error, just exit
    if (didRun === null) return;
};


const currencyForCountry = (countryName: string): CurrencyCode => {
    const c = (countryName || "").toLowerCase();
    if (c === "uk") return "GBP";
    if (c === "us") return "USD";
    if (c === "ca") return "CAD";
    if (c === "india") return "INR";
    return "USD";
};

const safeDeltaPctFromPct = (currentPct: number, previousPct: number) => {
    const c = Number(currentPct) || 0;
    const p = Number(previousPct) || 0;
    if (!p) return null;
    return ((c - p) / Math.abs(p)) * 100;
};

const normalizeDeltaKey = (value: unknown) =>
    String(value || "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, " ")
        .replace(/[^\w\s+&-]/g, "");

const attachNetSalesDeltaToLiveRows = (
    currentRows: MonthlySkuwiseTableRow[],
    previousRows: any[]
): MonthlySkuwiseTableRow[] => {
    const previousMap = new Map<string, any>();

    previousRows.forEach((row: any) => {
        const sku = String(row?.sku || "").trim().toUpperCase();

        const name = normalizeDeltaKey(
            normalizeProductDisplayName(row?.product_name || row?.sku || "")
        );

        if (sku && sku !== "GRAND_TOTAL" && sku !== "TOTAL") {
            previousMap.set(`sku:${sku}`, row);
        }

        if (
            name &&
            name !== "total" &&
            name !== "grand total" &&
            name !== "others"
        ) {
            previousMap.set(`name:${name}`, row);
        }
    });

    return currentRows.map((row) => {
        if (row.isTotal || row.isOthers) return row;

        const sku = String(row?.sku || "").trim().toUpperCase();

        const name = normalizeDeltaKey(
            normalizeProductDisplayName(row?.product_name || row?.sku || "")
        );

        const previousRow =
            previousMap.get(`sku:${sku}`) ||
            previousMap.get(`name:${name}`);

        if (!previousRow) {
            return {
                ...row,
                previous_net_sales: 0,
                net_sales_delta: 0,
                net_sales_delta_percentage: null,
            };
        }

        const currentNetSales = toNumber(row.net_sales);
        const previousNetSales = toNumber(previousRow?.net_sales);

        const delta = currentNetSales - previousNetSales;

        const deltaPct =
            previousNetSales !== 0
                ? (delta / Math.abs(previousNetSales)) * 100
                : null;

        return {
            ...row,
            previous_net_sales: previousNetSales,
            net_sales_delta: delta,
            net_sales_delta_percentage: deltaPct,
        };
    });
};

const fmtPct2 = (v: number) => `${(Number(v) || 0).toFixed(2)}%`;

/* ===================== RANGE PICKER (moved above graph) ===================== */
function RangePicker({
    selectedStartDay,
    selectedEndDay,
    label,
    onSubmit,
    onClear,
    onCloseReset,
}: {
    selectedStartDay: number | null;
    selectedEndDay: number | null;
    label: string;
    onSubmit: (s: number | null, e: number | null) => void;
    onClear: () => void;
    onCloseReset: () => void;
}) {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);

    const monthStart = new Date(today.getFullYear(), today.getMonth(), 1);
    const monthEnd = new Date(today.getFullYear(), today.getMonth() + 1, 0);
    const maxSelectableDate = yesterday < monthEnd ? yesterday : monthEnd;

    const [shownDate, setShownDate] = useState<Date>(monthStart);
    const [showCalendar, setShowCalendar] = useState(false);
    const [isMtdPlExpanded, setIsMtdPlExpanded] = useState(false);

    const [calendarRange, setCalendarRange] = useState<any>([
        { startDate: null, endDate: null, key: "selection" },
    ]);

    const MS_PER_DAY = 24 * 60 * 60 * 1000;

    const startOfDay = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate());

    const clampDate = (d: Date, min: Date, max: Date) => {
        const t = d.getTime();
        return new Date(Math.min(Math.max(t, min.getTime()), max.getTime()));
    };

    const calcRangeCompleted = (
        startDate: Date,
        endDate: Date,
        monthStart: Date,
        maxSelectableDate: Date
    ) => {
        const start = clampDate(startOfDay(startDate), startOfDay(monthStart), startOfDay(maxSelectableDate));
        const end = clampDate(startOfDay(endDate), startOfDay(monthStart), startOfDay(maxSelectableDate));

        const rangeDays =
            end.getTime() >= start.getTime()
                ? Math.round((end.getTime() - start.getTime()) / MS_PER_DAY) + 1
                : 0;

        const daysInMonth = new Date(start.getFullYear(), start.getMonth() + 1, 0).getDate();
        const rangeCompletedPct = daysInMonth > 0 ? (rangeDays / daysInMonth) * 100 : 0;

        return { rangeDays, daysInMonth, rangeCompletedPct };
    };

    const [pendingStartDay, setPendingStartDay] = useState<number | null>(null);
    const [pendingEndDay, setPendingEndDay] = useState<number | null>(null);

    // added: store real selected dates locally
    const [pendingStartDate, setPendingStartDate] = useState<Date | null>(null);
    const [pendingEndDate, setPendingEndDate] = useState<Date | null>(null);

    const [rangeCompletedPct, setRangeCompletedPct] = useState(0);
    const [rangeDays, setRangeDays] = useState(0);
    const [daysInMonth, setDaysInMonth] = useState(0);

    const wrapperRef = useRef<HTMLDivElement | null>(null);

    const formatRangeLabel = (startDate: Date | null, endDate: Date | null) => {
        if (!startDate || !endDate) return "Select Date Range";

        const startMonth = startDate.toLocaleString("en-US", { month: "short" });
        const endMonth = endDate.toLocaleString("en-US", { month: "short" });

        if (
            startDate.getFullYear() === endDate.getFullYear() &&
            startDate.getMonth() === endDate.getMonth()
        ) {
            return `${startMonth} ${startDate.getDate()}-${endDate.getDate()}`;
        }

        return `${startMonth} ${startDate.getDate()} - ${endMonth} ${endDate.getDate()}`;
    };

    useEffect(() => {
        if (!showCalendar) return;

        const onPointerDown = (e: PointerEvent) => {
            const el = wrapperRef.current;
            if (!el) return;

            if (!el.contains(e.target as Node)) {
                setShowCalendar(false);
                setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
                setPendingStartDay(null);
                setPendingEndDay(null);
                setPendingStartDate(null);
                setPendingEndDate(null);
                onCloseReset();
            }
        };

        document.addEventListener("pointerdown", onPointerDown, true);
        return () => document.removeEventListener("pointerdown", onPointerDown, true);
    }, [showCalendar, onCloseReset]);

    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") setIsMtdPlExpanded(false);
        };
        if (isMtdPlExpanded) window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [isMtdPlExpanded]);

    const handleCalendarChange = (ranges: any) => {
        const range = ranges.selection;
        setCalendarRange([range]);

        if (range.startDate && range.endDate) {
            setPendingStartDay(range.startDate.getDate());
            setPendingEndDay(range.endDate.getDate());

            // added
            setPendingStartDate(range.startDate);
            setPendingEndDate(range.endDate);

            const { rangeDays, daysInMonth, rangeCompletedPct } = calcRangeCompleted(
                range.startDate,
                range.endDate,
                monthStart,
                maxSelectableDate
            );

            setRangeDays(rangeDays);
            setDaysInMonth(daysInMonth);
            setRangeCompletedPct(rangeCompletedPct);
        } else {
            setPendingStartDay(null);
            setPendingEndDay(null);
            setPendingStartDate(null);
            setPendingEndDate(null);

            setRangeDays(0);
            setDaysInMonth(0);
            setRangeCompletedPct(0);
        }
    };

    const applyRange = () => {
        onSubmit(pendingStartDay, pendingEndDay);
        setShowCalendar(false);
    };

    const clearRange = () => {
        setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
        setPendingStartDay(null);
        setPendingEndDay(null);
        setPendingStartDate(null);
        setPendingEndDate(null);
        onClear();
    };

    const closeAndReset = () => {
        setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
        setPendingStartDay(null);
        setPendingEndDay(null);
        setPendingStartDate(null);
        setPendingEndDate(null);
        setShowCalendar(false);
        onCloseReset();
    };

    return (
        <div ref={wrapperRef} className="relative">
            <button
                type="button"
                onClick={() => setShowCalendar((s) => !s)}
                className="flex items-center gap-2 text-xs 2xl:text-sm"
                style={{
                    padding: "6px 10px",
                    borderRadius: 8,
                    border: "1px solid #D9D9D9E5",
                    backgroundColor: "#ffffff",
                }}
            >
                <FaCalendarAlt className="text-sm 2xl:text-md" />
                {label}
            </button>

            {showCalendar && (
                <div
                    style={{
                        position: "absolute",
                        right: 0,
                        top: "110%",
                        zIndex: 50,
                        backgroundColor: "#ffffff",
                        boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
                        padding: 8,
                        borderRadius: 8,
                        minWidth: 320,
                    }}
                >
                    <DateRange
                        ranges={calendarRange}
                        onChange={handleCalendarChange}
                        moveRangeOnFirstSelection={false}
                        showMonthAndYearPickers={false}
                        rangeColors={["#5EA68E"]}
                        minDate={monthStart}
                        maxDate={maxSelectableDate}
                        shownDate={shownDate}
                        onShownDateChange={() => setShownDate(monthStart)}
                        startDatePlaceholder="Start"
                        endDatePlaceholder="End"
                    />

                    <style jsx global>{`
                        .rdrNextPrevButton {
                            display: none !important;
                        }
                    `}</style>

                    <div className="flex justify-between mt-2 gap-2">
                        <button
                            type="button"
                            onClick={clearRange}
                            className="text-xs px-2 py-1 border rounded"
                        >
                            Clear
                        </button>

                        <div className="flex gap-2">
                            <button
                                type="button"
                                onClick={applyRange}
                                disabled={pendingStartDay == null || pendingEndDay == null}
                                className="text-xs px-2 py-1 rounded text-yellow-200"
                                style={{
                                    background: "#37455F",
                                    opacity: pendingStartDay == null ? 0.6 : 1,
                                }}
                            >
                                Submit
                            </button>
                            <button
                                type="button"
                                onClick={closeAndReset}
                                className="text-xs px-2 py-1 rounded text-charcoal-500 border border-charcoal-500"
                            >
                                Close
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

const sliceByDayRange = (
    points: DailyPoint[] = [],
    startDay: number | null,
    endDay: number | null
) => {
    if (startDay == null || endDay == null) return points;

    const s = Math.min(startDay, endDay);
    const e = Math.max(startDay, endDay);

    return points.filter((p) => {
        const day = Number(p.date?.slice(8, 10));
        return day >= s && day <= e;
    });
};

export default function DashboardPage() {
    const router = useRouter();
    const params = useParams();
    const shownInventoryToastIdsRef = useRef<Set<string>>(new Set());

    // const { setItems: setHeaderNotifications } = useHeaderNotifications();

    const urlMonthParam = Array.isArray(params?.month)
        ? params.month[0]
        : params?.month;

    const urlYearParam = Array.isArray(params?.year)
        ? params.year[0]
        : params?.year;

    const isMonthYearNA =
        String(urlMonthParam ?? "").toUpperCase() === "NA" &&
        String(urlYearParam ?? "").toUpperCase() === "NA";

    const [isMtdPlExpanded, setIsMtdPlExpanded] = useState(false);
    const [profitPieType, setProfitPieType] = useState<"cm1" | "cm2">("cm1");
    const shouldShowDummyUi = isMonthYearNA;

    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") setIsMtdPlExpanded(false);
        };
        if (isMtdPlExpanded) window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [isMtdPlExpanded]);

    useEffect(() => {
        const token =
            typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (!token) {
            router.replace("/signin");
        }
    }, [router]);

    const getTimeZoneAbbr = (date: Date, timeZone: string) => {
        const parts = new Intl.DateTimeFormat("en-US", {
            timeZone,
            timeZoneName: "short",
        }).formatToParts(date);

        const abbr = parts.find((p) => p.type === "timeZoneName")?.value || "";

        // If browser gives PDT/BST/EDT, use it
        if (abbr && !abbr.startsWith("GMT")) return abbr;

        // Fallback when browser returns GMT-7 / GMT+1
        const month = Number(
            new Intl.DateTimeFormat("en-US", {
                timeZone,
                month: "numeric",
            }).format(date)
        );

        if (timeZone === "America/Los_Angeles") {
            return month >= 3 && month <= 11 ? "PDT" : "PST";
        }

        if (timeZone === "Europe/London") {
            return month >= 3 && month <= 10 ? "BST" : "GMT";
        }

        if (timeZone === "America/Toronto") {
            return month >= 3 && month <= 11 ? "EDT" : "EST";
        }

        return abbr;
    };

    const formatLastUpdatedDateTime = (
        timestamp: string | number | Date | null | undefined,
        timeZone: string
    ) => {
        if (!timestamp) return "";

        const date = new Date(timestamp);
        if (Number.isNaN(date.getTime())) return "";

        const parts = new Intl.DateTimeFormat("en-GB", {
            timeZone,
            day: "numeric",
            month: "short",
            year: "numeric",
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
        }).formatToParts(date);

        const get = (type: Intl.DateTimeFormatPartTypes) =>
            parts.find((p) => p.type === type)?.value || "";

        const day = get("day");
        const month = get("month");
        const year = get("year");
        const hour = get("hour");
        const minute = get("minute");
        const dayPeriod = get("dayPeriod").toUpperCase();
        const timeZoneName = getTimeZoneAbbr(date, timeZone);

        return `${day} ${month} ${year}, ${hour}:${minute} ${dayPeriod} ${timeZoneName}`;
    };

    const formatUKTime12hr = (
        timestamp: string | number | Date | null | undefined
    ) => {
        if (timestamp == null) return "";

        if (typeof timestamp === "string") {
            const match = timestamp.match(
                /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/
            );

            if (!match) return "";

            const [, year, month, day, hour, minute] = match;

            const date = new Date(
                Number(year),
                Number(month) - 1,
                Number(day),
                Number(hour),
                Number(minute)
            );

            const dateText = new Intl.DateTimeFormat("en-GB", {
                month: "short",
                day: "numeric",
                year: "numeric",
            }).format(date);

            const hour24 = Number(hour);
            const hour12 = hour24 % 12 || 12;
            const ampm = hour24 >= 12 ? "PM" : "AM";

            return `${dateText}, ${hour12}:${minute} ${ampm} BST`;
        }

        const date = timestamp instanceof Date ? timestamp : new Date(timestamp);
        if (Number.isNaN(date.getTime())) return "";

        return new Intl.DateTimeFormat("en-GB", {
            timeZone: "Europe/London",
            month: "short",
            day: "numeric",
            year: "numeric",
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
            timeZoneName: "short",
        })
            .format(date)
            .replace("am", "AM")
            .replace("pm", "PM");
    };

    const formatUSTime12hr = (
        timestamp: string | number | Date | null | undefined
    ) => {
        if (!timestamp) return "";

        const date = new Date(timestamp);
        if (Number.isNaN(date.getTime())) return "";

        return new Intl.DateTimeFormat("en-US", {
            timeZone: "America/Los_Angeles",
            month: "short",
            day: "numeric",
            year: "numeric",
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
            timeZoneName: "short",
        }).format(date);
    };

    const { platform } = usePlatform();
    const { data: userData } = useGetUserDataQuery();
    const isCountryMode = platform !== "global" && platform !== "shopify";

    const countryName = useMemo(() => {
        switch (platform) {
            case "amazon-uk":
                return "uk";
            case "amazon-us":
                return "us";
            case "amazon-ca":
                return "ca";
            default:
                return "global";
        }
    }, [platform]);

    const showLiveBI = isCountryMode || platform === "global";


    const brandName = useSelector(
        (state: RootState) => (state as RootState).auth.user?.brand_name
    );

    const companyName = String(
        (userData as any)?.companyName ||
        (userData as any)?.company_name ||
        (userData as any)?.company ||
        ""
    ).trim();


    const biCountryName = useMemo(() => {
        if (platform === "global") return "global";
        return countryName; // uk / us / ca for countrywise pages
    }, [platform, countryName]);

    const biDataCurrency = useMemo(() => currencyForCountry(biCountryName), [biCountryName]);

    const biSourceCurrency: CurrencyCode = useMemo(
        () => currencyForCountry(biCountryName),
        [biCountryName]
    );

    const amazonDataCurrency: CurrencyCode = useMemo(() => {
        if (platform === "global") return "USD";
        if (platform === "amazon-us") return "USD";
        if (platform === "amazon-ca") return "CAD";
        return "GBP";
    }, [platform]);

    const platformLabel = useMemo(() => {
        if (platform === "global") return "Amazon Global";
        if (platform === "amazon-uk") return "Amazon UK";
        if (platform === "amazon-us") return "Amazon US";
        if (platform === "amazon-ca") return "Amazon CA";
        return "Amazon";
    }, [platform]);

    /* ===================== PLATFORM → DISPLAY CURRENCY ===================== */
    const profileHomeCurrency = ((userData?.homeCurrency || "USD").toUpperCase() as CurrencyCode);

    const displayCurrency: CurrencyCode = useMemo(() => {
        switch (platform as PlatformId) {
            case "global":
                return profileHomeCurrency;
            case "amazon-uk":
                return "GBP";
            case "amazon-us":
                return "USD";
            case "amazon-ca":
                return "CAD";
            case "shopify":
                return "INR";
            default:
                return profileHomeCurrency;
        }
    }, [platform, profileHomeCurrency]);

    /* ===================== MONTHLY ADS (SP + SD) ===================== */
    // Used by fetchMonthlySp() and the monthly ads table UI.
    const [monthlySpRows, setMonthlySpRows] = useState<MonthlySpRow[]>([]);
    const [monthlySpLoading, setMonthlySpLoading] = useState(false);
    const [monthlySpError, setMonthlySpError] = useState<string | null>(null);
    const [monthlySpTotalSpend, setMonthlySpTotalSpend] = useState<number | null>(null);

    const [adsSeeded, setAdsSeeded] = useState(false);
    const [adsSeedError, setAdsSeedError] = useState<string | null>(null);

    // background-only state, not connected to Loader
    const [adsBackgroundLoading, setAdsBackgroundLoading] = useState(false);
    const [adsBackgroundError, setAdsBackgroundError] = useState<string | null>(null);

    const [adsLoading, setAdsLoading] = useState(false);
    const [invLoading, setInvLoading] = useState(false);
    const [invError, setInvError] = useState("");
    const [invRows, setInvRows] = useState<InventoryRow[]>([]);
    const [inventoryAlerts, setInventoryAlerts] = useState<InventoryAlertRecord>({});
    const [activeTab, setActiveTab] = useState<TopTab>("live");
    const [summaryLoading, setSummaryLoading] = useState(true);
    const [countryTime, setCountryTime] = useState<CountryTimezoneResponse | null>(null);
    const [countryTimeLoading, setCountryTimeLoading] = useState(false);
    const [countryTimeError, setCountryTimeError] = useState<string | null>(null);
    const [dismissedAlertIds, setDismissedAlertIds] = useState<Set<string>>(
        () => new Set()
    );

    type PlSortConfig = {
        key: string;
        direction: "asc" | "desc";
    };

    const [plSortConfig, setPlSortConfig] = useState<PlSortConfig>({
        key: "net_sales",
        direction: "desc",
    });



    const productwiseInitialCollapsed = useMemo(
        () => ({
            quantity: true,
            marketplace_fees: true,
            tax_and_credits: true,
            profit: true,
            cm2_profit: true,
            ads_spend: true,
        }),
        []
    );

    const [productwiseCollapsed, setProductwiseCollapsed] = useState<Record<string, boolean>>(
        productwiseInitialCollapsed
    );

    const [productwiseAnyGroupExpanded, setProductwiseAnyGroupExpanded] = useState(false);

    const [showAllMtdProductwiseRows, setShowAllMtdProductwiseRows] = useState(false);

    const [previousSkuwiseGlobalData, setPreviousSkuwiseGlobalData] = useState<any>(null);
    const [previousSkuwiseGlobalLoading, setPreviousSkuwiseGlobalLoading] = useState(false);

    type MetricItem = {
        label: string;
        value: string;
        color?: string;
    };

    type SelectedRec = {
        productName: string;
        metrics: MetricItem[];
        journeyPoints: string[];
        recommendationPoints: string[];
        advertisingPoints?: string[];
        inventoryPoints?: string[];
        showChart?: boolean;
    } | null;

    const [recDrawerOpen, setRecDrawerOpen] = useState(false);
    const [selectedRec, setSelectedRec] = useState<SelectedRec>(null);

    const normalizePieName = (name: any) =>
        normalizeProductDisplayName(String(name || "Unknown"))
            .trim()
            .toLowerCase();

    const safeDeltaPct = (current: number, previous: number) => {
        const c = Number(current || 0);
        const p = Number(previous || 0);
        if (!p) return null;
        return ((c - p) / Math.abs(p)) * 100;
    };

    const buildPreviousProfitMap = useCallback(
        (key: "profit" | "cm2_profit") => {
            const rows =
                platform === "global"
                    ? previousSkuwiseGlobalData?.skuwise_items_global
                    : [];

            const map = new Map<string, number>();

            if (!Array.isArray(rows)) return map;

            rows.forEach((row: any) => {
                const name = String(row?.product_name || "").trim();
                const sku = String(row?.sku || "").trim().toUpperCase();

                if (!name) return;
                if (sku === "TOTAL") return;
                if (name.toLowerCase() === "total") return;

                const normalizedName = normalizePieName(name);
                const value = Number(row?.[key] ?? 0);

                map.set(normalizedName, value);
            });

            return map;
        },
        [platform, previousSkuwiseGlobalData]
    );

    type GlobalMtdView = "global" | "uk" | "us";

    const [globalMtdView, setGlobalMtdView] = useState<GlobalMtdView>("global");

    const globalMtdCountry = useMemo<"uk" | "us">(() => {
        return globalMtdView === "us" ? "us" : "uk";
    }, [globalMtdView]);

    const [dismissedAlerts, setDismissedAlerts] = React.useState<string[]>([]);
    const [fxReady, setFxReady] = useState(false);
    const [globalCountryPayloads, setGlobalCountryPayloads] = useState<{
        uk?: any;
        us?: any;
    }>({});

    const timezoneCountry = useMemo<"uk" | "us">(() => {
        if (platform === "amazon-us") return "us";
        if (platform === "amazon-uk") return "uk";

        // For global, choose the currently selected MTD country if you use one.
        if (platform === "global") return globalMtdCountry;

        // Default fallback
        return "uk";
    }, [platform, globalMtdCountry]);

    const fetchCountryTime = useCallback(async () => {
        if (typeof window === "undefined") return null;

        const token = localStorage.getItem("jwtToken");

        if (!token) {
            setCountryTime(null);
            setCountryTimeError("Authorization token is missing");
            return null;
        }

        try {
            setCountryTimeLoading(true);
            setCountryTimeError(null);

            const res = await fetch(`${COUNTRY_TIMEZONE_ENDPOINT}/${timezoneCountry}`, {
                method: "GET",
                headers: {
                    Accept: "application/json",
                    Authorization: `Bearer ${token}`,
                },
                cache: "no-store",
            });

            const json = await res.json().catch(() => null);

            if (!res.ok) {
                throw new Error(json?.error || `Failed to fetch country time (${res.status})`);
            }

            setCountryTime(json);
            return json;
        } catch (err: any) {
            setCountryTime(null);
            setCountryTimeError(err?.message || "Failed to fetch country time");
            return null;
        } finally {
            setCountryTimeLoading(false);
        }
    }, [timezoneCountry]);

    useEffect(() => {
        if (typeof window === "undefined") return;

        const token = localStorage.getItem("jwtToken");

        if (!token) return;

        void fetchCountryTime();
    }, [fetchCountryTime]);

    const dismissAlert = useCallback((id?: string) => {
        if (!id) return;
        setDismissedAlertIds((prev) => {
            const next = new Set(prev);
            next.add(id);
            return next;
        });
    }, []);


    const isManualRefreshRef = useRef(false);
    const shouldPostCacheRef = useRef(false);
    const [cacheSaveTick, setCacheSaveTick] = useState(0);

    const triggerCachePost = useCallback(() => {
        shouldPostCacheRef.current = true;
        isManualRefreshRef.current = true;

        // Force the cache-save effect to run after manual refresh.
        setCacheSaveTick((x) => x + 1);
    }, []);

    const [pendingHash, setPendingHash] = useState<string>("");

    const inventoryAlertList = useMemo<UiAlert[]>(() => {
        return Object.entries(inventoryAlerts || {})
            .map(([id, v]) => ({
                id,
                title: id, // if id is SKU, this is perfect; otherwise change to product name
                message: v.alert ?? "",
                variant: toVariant(v.alert_type),
            }))
            .filter((a) => a.message.trim().length > 0)
            .filter((a) => !dismissedAlertIds.has(a.id));
    }, [inventoryAlerts, dismissedAlertIds]);

    const isInventoryTotalRow = (r: InventoryRow) => {
        const name = String(r["Product Name"] ?? "").trim().toLowerCase();
        const sku = String(r["SKU"] ?? "").trim().toLowerCase();
        if (!name && !sku) return false;
        return (
            name === "total" ||
            name === "grand total" ||
            name.includes("total") ||
            sku === "total" ||
            sku === "grand total" ||
            sku.includes("total")
        );
    };

    const skuToProductName = useMemo(() => {
        const map: Record<string, string> = {};
        for (const r of invRows || []) {
            const sku = normalizeSku((r as any)["SKU"]);
            const name = String((r as any)["Product Name"] || "").trim();
            if (sku && name) map[sku] = name;
        }
        return map;
    }, [invRows]);

    const top5Skus = useMemo(() => {
        if (!invRows?.length) return [];

        const usable = invRows.filter((r) => {
            const name = String((r as any)["Product Name"] ?? "").trim();
            const sku = String((r as any)["SKU"] ?? "").trim();
            if (!name && !sku) return false;
            if (isInventoryTotalRow(r)) return false;
            return true;
        });

        const calc = usable.map((r, idx) => {
            const mtdKey = Object.keys(r).find((k) =>
                k.toLowerCase().startsWith("current month units sold")
            );
            const mtdSales = toNumberSafe(mtdKey ? (r as any)[mtdKey] : 0);

            return { idx, sku: normalizeSku((r as any)["SKU"]), mtdSales };
        });

        return calc
            .sort((a, b) => b.mtdSales - a.mtdSales)
            .slice(0, 5)
            .map((x) => x.sku)
            .filter(Boolean);
    }, [invRows]);




    const normalizedInventoryAlerts = useMemo(() => {
        const next: Record<string, { alert?: string; alert_type?: string }> = {};

        Object.entries(inventoryAlerts || {}).forEach(([sku, value]) => {
            next[String(sku).trim().toUpperCase()] = value;
        });

        return next;
    }, [inventoryAlerts]);

    const top5InventoryAlerts = useMemo(() => {
        return Object.entries(normalizedInventoryAlerts || {})
            .filter(([sku, v]) => {
                return top5Skus.includes(sku) && v.alert === "High alert";
            })
            .map(([sku, v]) => ({
                id: sku,
                title: sku,
                message: v.alert || "",
                variant: "error" as const,
            }));
    }, [normalizedInventoryAlerts, top5Skus]);


    /* ===================== AMAZON / SHOPIFY STATE ===================== */
    const [loading, setLoading] = useState(false);
    const [unauthorized, setUnauthorized] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [data, setData] = useState<any>(null);
    const { connections: amazonConnections } = useAmazonConnections();
    const [shopifyLoading, setShopifyLoading] = useState(false);
    const [shopifyError, setShopifyError] = useState<string | null>(null);
    const [shopifyRows, setShopifyRows] = useState<any[]>([]);
    const shopify = shopifyRows?.[0] || null;
    const [shopifyPrevRows, setShopifyPrevRows] = useState<any[]>([]);
    const [shopifyStore, setShopifyStore] = useState<any | null>(null);
    const [amazonRegion, setAmazonRegion] = useState<RegionKey>("Global");
    const [graphRegion, setGraphRegion] = useState<RegionKey>("Global");
    const [biStatus, setBiStatus] = useState<
        "idle" | "loading" | "processing" | "ready" | "error"
    >("idle");
    const [closedAlerts, setClosedAlerts] = useState<string[]>([]);
    const chartRef = React.useRef<HTMLDivElement | null>(null);
    // const prevLabel = useMemo(() => getPrevMonthShortLabel(), []);

    const [lastRefreshAt, setLastRefreshAt] = useState<number | null>(null);
    const [loadingStartedAt, setLoadingStartedAt] = useState<number | null>(null);

    const forcedRegion: RegionKey = useMemo(() => {
        switch (platform) {
            case "amazon-uk":
                return "UK";
            case "amazon-us":
                return "US";
            case "amazon-ca":
                return "CA";
            default:
                return "Global";
        }
    }, [platform]);

    const graphRegionToUse: RegionKey = isCountryMode ? forcedRegion : graphRegion;
    const activeDateRegion = graphRegionToUse;



    const isUsAmazonConnected = useMemo(() => {
        return (amazonConnections || []).some(
            (connection: any) =>
                String(connection?.country || "").toLowerCase() === "us"
        );
    }, [amazonConnections]);

    const dashboardTimeRegion: RegionKey = useMemo(() => {
        // Keep dashboard data date aligned with the same timezone used by "Last Updated at"
        if (platform === "global" && isUsAmazonConnected) return "US";

        if (platform === "global" && globalMtdCountry === "us") return "US";
        if (platform === "global" && globalMtdCountry === "uk") return "UK";

        return activeDateRegion;
    }, [platform, isUsAmazonConnected, globalMtdCountry, activeDateRegion]);

    const getRegionISODateFromTimestamp = useCallback(
        (timestamp: number | string | Date, region: RegionKey) => {
            const date = new Date(timestamp);

            const parts = new Intl.DateTimeFormat("en-CA", {
                timeZone: getTimezoneForRegion(region),
                year: "numeric",
                month: "2-digit",
                day: "2-digit",
            }).formatToParts(date);

            const get = (type: Intl.DateTimeFormatPartTypes) =>
                parts.find((p) => p.type === type)?.value || "";

            return `${get("year")}-${get("month")}-${get("day")}`;
        },
        []
    );

    const getRegionDayFromTimestamp = useCallback(
        (timestamp: number | string | Date, region: RegionKey) => {
            const iso = getRegionISODateFromTimestamp(timestamp, region);
            return Number(iso.slice(8, 10)) || 1;
        },
        [getRegionISODateFromTimestamp]
    );

    const dashboardDateAnchor = lastRefreshAt ?? Date.now();

    const dashboardAllowedEndISO = useMemo(() => {
        return getRegionISODateFromTimestamp(dashboardDateAnchor, dashboardTimeRegion);
    }, [dashboardDateAnchor, dashboardTimeRegion, getRegionISODateFromTimestamp]);

    const dashboardAllowedDay = useMemo(() => {
        return getRegionDayFromTimestamp(dashboardDateAnchor, dashboardTimeRegion);
    }, [dashboardDateAnchor, dashboardTimeRegion, getRegionDayFromTimestamp]);

    const dashboardDaysInMonth = useMemo(() => {
        const anchorDate = new Date(dashboardDateAnchor);

        const parts = new Intl.DateTimeFormat("en-CA", {
            timeZone: getTimezoneForRegion(dashboardTimeRegion),
            year: "numeric",
            month: "2-digit",
        }).formatToParts(anchorDate);

        const year = Number(parts.find((p) => p.type === "year")?.value);
        const month = Number(parts.find((p) => p.type === "month")?.value);

        return new Date(year, month, 0).getDate();
    }, [dashboardDateAnchor, dashboardTimeRegion]);

    const isCurrentPointAllowed = useCallback(
        (point?: DailyPoint) => {
            if (!point?.date) return false;
            return point.date <= dashboardAllowedEndISO;
        },
        [dashboardAllowedEndISO]
    );

    const isPreviousPointAllowed = useCallback(
        (point?: DailyPoint) => {
            if (!point?.date) return false;
            const day = Number(point.date.slice(8, 10));
            return Number.isFinite(day) && day <= dashboardAllowedDay;
        },
        [dashboardAllowedDay]
    );

    const getBackendCountryDate = useCallback(() => {
        const dt = countryTime?.selected_country?.datetime;

        if (!dt) {
            return getRegionNow(activeDateRegion);
        }

        return new Date(dt.replace(" ", "T"));
    }, [countryTime, activeDateRegion]);

    const getBackendCountryYearMonth = useCallback(() => {
        const now = getBackendCountryDate();

        const monthName = now.toLocaleString("en-US", {
            month: "long",
        });

        return {
            monthName,
            year: now.getFullYear(),
        };
    }, [getBackendCountryDate]);

    const getPrevBackendCountryYearMonth = useCallback(() => {
        const now = getBackendCountryDate();
        now.setMonth(now.getMonth() - 1);

        const monthName = now.toLocaleString("en-US", {
            month: "long",
        });

        return {
            monthName,
            year: now.getFullYear(),
        };
    }, [getBackendCountryDate]);

    const getBackendCountryDayInfo = useCallback(() => {
        const now = getBackendCountryDate();

        const todayDay = now.getDate();
        const prevMonthDate = new Date(now.getFullYear(), now.getMonth(), 0);
        const daysInPrevMonth = prevMonthDate.getDate();

        return { todayDay, daysInPrevMonth };
    }, [getBackendCountryDate]);

    const getDaysInBackendCountryMonth = useCallback(() => {
        const now = getBackendCountryDate();
        return new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    }, [getBackendCountryDate]);


    const dashboardLabelAnchor = lastRefreshAt ?? Date.now();

    const currentDisplayMonth = useMemo(() => {
        return getRegionYearMonthFromTimestamp(activeDateRegion, dashboardLabelAnchor);
    }, [activeDateRegion, dashboardLabelAnchor]);

    const previousDisplayMonth = useMemo(() => {
        return getPrevRegionYearMonthFromTimestamp(activeDateRegion, dashboardLabelAnchor);
    }, [activeDateRegion, dashboardLabelAnchor]);

    const formattedMonthYear = useMemo(() => {
        return formatMonthYearShort(
            currentDisplayMonth.monthName,
            currentDisplayMonth.year
        );
    }, [currentDisplayMonth]);

    const prevLabel = useMemo(() => {
        return formatMonthYearShort(
            previousDisplayMonth.monthName,
            previousDisplayMonth.year
        );
    }, [previousDisplayMonth]);



    const lastUpdatedTimeText = useMemo(() => {
        if (!lastRefreshAt) return "";

        if (platform === "global" && isUsAmazonConnected) {
            return formatLastUpdatedDateTime(lastRefreshAt, "America/Los_Angeles");
        }

        return activeDateRegion === "US"
            ? formatLastUpdatedDateTime(lastRefreshAt, "America/Los_Angeles")
            : activeDateRegion === "CA"
                ? formatLastUpdatedDateTime(lastRefreshAt, "America/Toronto")
                : formatLastUpdatedDateTime(lastRefreshAt, "Europe/London");
    }, [lastRefreshAt, platform, isUsAmazonConnected, activeDateRegion]);

    const countryLastRefreshTimeText = useMemo(() => {
        const selected = countryTime?.selected_country;

        if (!selected?.time) {
            return "";
        }

        return `${selected.time} ${selected.abbreviation || ""}`.trim();
    }, [countryTime]);

    const globalMtdViewOptions = useMemo(() => {
        if (shouldShowDummyUi) {
            return [
                { value: "global" as const, label: "Global" },
                { value: "uk" as const, label: "UK" },
                { value: "us" as const, label: "US" },
            ];
        }

        const connected = new Set(
            (amazonConnections || [])
                .map((c: any) => String(c?.country || "").toLowerCase())
                .filter(Boolean)
        );

        const options: { value: GlobalMtdView; label: string }[] = [
            { value: "global", label: "Global" },
        ];

        if (connected.has("uk")) {
            options.push({ value: "uk", label: "UK" });
        }

        if (connected.has("us")) {
            options.push({ value: "us", label: "US" });
        }

        return options;
    }, [amazonConnections, shouldShowDummyUi]);

    useEffect(() => {
        if (!globalMtdViewOptions.length) return;

        const selectedStillAvailable = globalMtdViewOptions.some(
            (option) => option.value === globalMtdView
        );

        if (!selectedStillAvailable) {
            setGlobalMtdView(globalMtdViewOptions[0].value);
        }
    }, [globalMtdViewOptions, globalMtdView]);

    const [todaySalesRaw, setTodaySalesRaw] = useState<number>(0);

    const [prevTargetSummaries, setPrevTargetSummaries] = useState<{
        uk?: any;
        us?: any;
        ca?: any;
        global?: any;
    }>({});




    const fetchMonthlySp = useCallback(async (silent = false) => {
        if (isMonthYearNA) {
            setMonthlySpLoading(false);
            setMonthlySpError(null);
            setMonthlySpRows([]);
            setMonthlySpTotalSpend(null);
            return;
        }
        try {
            if (!silent) setMonthlySpLoading(true);
            setMonthlySpError(null);

            const token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

            if (!token) throw new Error("No token found. Please sign in.");

            const country =
                platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

            const { monthName, year } = getBackendCountryYearMonth();

            const res = await fetch(MONTHLY_SP_ENDPOINT, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    Accept: "application/json",
                    Authorization: `Bearer ${token}`,
                },
                body: JSON.stringify({
                    month: monthToNumber(monthName.toLowerCase()),
                    year,
                    country,
                    // month: 1,
                    // year: 2026,
                    // country: "UK"
                }),
            });

            const json = await res.json().catch(() => ({}));

            if (!res.ok) {
                const msg =
                    (json as any)?.message ||
                    (json as any)?.error ||
                    `Failed to load Monthly SP data (${res.status})`;
                throw new Error(msg);
            }

            const items: any[] = Array.isArray((json as any)?.items) ? (json as any).items : [];

            const grandTotalRow = items.find((r) => r?.products === "Grand Total");
            setMonthlySpTotalSpend(
                typeof grandTotalRow?.spend === "number" ? grandTotalRow.spend : null
            );

            const mapped: MonthlySpRow[] = items
                .filter((r) => r && r.products !== "Grand Total") // keep body clean
                .map((r) => ({
                    sno: r.sno ?? null,
                    products: r.products ?? null,
                    spend: r.spend ?? null,
                }));

            setMonthlySpRows(mapped);

        } catch (e: any) {
            setMonthlySpError(e?.message || "Failed to load Monthly SP data");
            setMonthlySpRows([]);
            setMonthlySpTotalSpend(null);
        } finally {
            if (!silent) setMonthlySpLoading(false);
        }
    }, [platform, isMonthYearNA, getBackendCountryYearMonth]);

    const adsBackgroundLoadingRef = useRef(false);
    const adsSeededRef = useRef(false);
    const adsSeedErrorRef = useRef<string | null>(null);
    const adsBackgroundErrorRef = useRef<string | null>(null);

    const runAdsBackgroundSync = useCallback(async () => {
        if (adsBackgroundLoadingRef.current) return;
        if (isMonthYearNA) return;
        if (platform === "shopify") return;

        const jwtToken =
            typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (!jwtToken) {
            adsBackgroundErrorRef.current = "No token found. Please sign in.";
            return;
        }

        const country =
            platform === "amazon-us"
                ? "US"
                : platform === "amazon-ca"
                    ? "CA"
                    : "UK";

        adsBackgroundLoadingRef.current = true;
        adsBackgroundErrorRef.current = null;

        try {
            await ensureSpReportSeedOncePerDay(baseURL, jwtToken, country);

            if (country === "UK" || country === "US") {
                await ensureSdReportSeedOncePerDay(baseURL, jwtToken, country);
            }

            await ensureSbKeywordReportSeedOncePerDay(baseURL, jwtToken, country);

            const { monthName, year } = getRegionYearMonth(activeDateRegion);
            const month = monthToNumber(monthName.toLowerCase());
            const include = country === "UK" || country === "US" ? ["SP", "SD", "SB"] : ["SP"];

            const res = await fetch(`${baseURL}/api/ads/monthly_sp_sd_to_db`, {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${jwtToken}`,
                    Accept: "application/json",
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ month, year, country, include }),
            });

            const json = await res.json().catch(() => ({}));
            const errMsg = String(json?.error || json?.message || json?.detail || "");

            const isNoRows404 =
                res.status === 404 && errMsg.toLowerCase().includes("no rows found");

            const isDuplicateOrInProgress =
                res.status === 425 ||
                errMsg.toLowerCase().includes("duplicate") ||
                errMsg.toLowerCase().includes("already exists") ||
                errMsg.toLowerCase().includes("request is a duplicate") ||
                errMsg.toLowerCase().includes("in progress");

            if (!res.ok && !isNoRows404 && !isDuplicateOrInProgress) {
                throw new Error(errMsg || "monthly_sp_sd_to_db failed");
            }

            adsSeededRef.current = true;
            adsSeedErrorRef.current = null;

            // IMPORTANT: do not update UI automatically here.
            // await fetchMonthlySp(true);
        } catch (e: any) {
            console.error("Background ads sync failed:", e);
            adsSeededRef.current = false;
            adsSeedErrorRef.current = e?.message || "Ads background sync failed";
            adsBackgroundErrorRef.current = e?.message || "Ads background sync failed";
        } finally {
            adsBackgroundLoadingRef.current = false;
        }
    }, [
        isMonthYearNA,
        platform,
        activeDateRegion,
    ]);

    useEffect(() => {
        if (activeTab !== "live") return;
        if (!pendingHash) return;

        const run = () => {
            const targetId = pendingHash.replace("#", "");
            const el = document.getElementById(targetId);

            if (el) {
                el.scrollIntoView({
                    behavior: "smooth",
                    block: "start",
                });
                setPendingHash("");
            }
        };

        const timer = setTimeout(run, 250);
        return () => clearTimeout(timer);
    }, [activeTab, pendingHash]);

    useEffect(() => {
        if (activeTab === "summary") {
            setSummaryLoading(true);
        }
    }, [activeTab]);

    // useEffect(() => {
    //     fetchMonthlySp();
    // }, [fetchMonthlySp]);

    useEffect(() => {
        const stored = localStorage.getItem("dismissedInventoryAlerts");
        if (stored) {
            setDismissedAlerts(JSON.parse(stored));
        }
    }, []);

    const handleDismiss = (sku: string) => {
        setDismissedAlerts((prev) => {
            if (prev.includes(sku)) return prev;

            const updated = [...prev, sku];
            localStorage.setItem("dismissedInventoryAlerts", JSON.stringify(updated));
            return updated;
        });

        toast.dismiss(sku);
    };

    /* ===================== ✅ SHARED RANGE STATE (PARENT) ===================== */
    const [selectedStartDay, setSelectedStartDay] = useState<number | null>(null);
    const [selectedEndDay, setSelectedEndDay] = useState<number | null>(null);
    const [biLoading, setBiLoading] = useState(false);
    const [biError, setBiError] = useState<string | null>(null);
    const [biDailySeries, setBiDailySeries] = useState<ApiDailySeries | null>(null);
    const [biPeriods, setBiPeriods] = useState<BiApiResponse["periods"] | null>(null);
    const [liveBiPayload, setLiveBiPayload] = useState<any>(null);
    const [biAlignedTotals, setBiAlignedTotals] = useState<BiAlignedTotals | null>(null);
    const [liveBiReady, setLiveBiReady] = useState(false);
    const retryRef = useRef(0);

    /* ===================== FX RATES ===================== */
    const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
    const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
    const [cadToUsd, setCadToUsd] = useState(CAD_TO_USD_ENV);
    const [fxLoading, setFxLoading] = useState(false);

    const biUiLoading = biLoading;

    const [dashboardBusy, setDashboardBusy] = useState(false);
    const [showDashboardStepLoader, setShowDashboardStepLoader] = useState(false);

    const [currentStep, setCurrentStep] = useState<number>(0); // 0 = idle
    const [completedSteps, setCompletedSteps] = useState<Set<number>>(new Set());

    const [stepProgress, setStepProgress] = useState<{
        active: boolean;
        label: string;
        percentage: number;
        detail?: string;
    }>({
        active: false,
        label: "",
        percentage: 0,
        detail: "",
    });

    const markStepComplete = (step: number) => {
        setCompletedSteps((prev) => new Set([...prev, step]));
    };

    const setStep = (
        step: number,
        label: string,
        percentage: number = 0,
        detail?: string
    ) => {
        setCurrentStep(step);
        setStepProgress({
            active: true,
            label,
            percentage: Math.min(100, Math.max(0, percentage)),
            detail,
        });
    };

    const resetStepState = () => {
        setCurrentStep(0);
        setCompletedSteps(new Set());
        setLoadingStartedAt(null);
        setStepProgress({
            active: false,
            label: "",
            percentage: 0,
            detail: "",
        });
    };

    const dashboardSteps = [
        { num: 1, label: "Live MTD" },
        { num: 2, label: "Current Inventory" },
        { num: 3, label: "Plotting Graph" },
    ];

    const pageLoading =
        dashboardBusy ||
        loading ||
        shopifyLoading ||
        biLoading ||
        invLoading ||
        monthlySpLoading ||
        fxLoading ||
        previousSkuwiseGlobalLoading;

    type CurrencyRateRow = {
        conversion_rate: number;
        country: string;
        month: string;
        selected_currency: string;
        user_currency: string;
        year: number;
    };

    const FX_RATES_GET_ENDPOINT = `${baseURL}/currency-rates`;

    const fetchFxRates = useCallback(async () => {
        try {
            setFxLoading(true);

            const token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

            const headers: HeadersInit = { Accept: "application/json" };
            if (token) (headers as any).Authorization = `Bearer ${token}`;

            const res = await fetch(FX_RATES_GET_ENDPOINT, { method: "GET", headers });
            if (!res.ok) throw new Error(`FX rates fetch failed: ${res.status}`);

            const rows: CurrencyRateRow[] = await res.json();

            const { monthName, year } = getRegionYearMonth(activeDateRegion);
            const month = monthName.toLowerCase();

            const cur = (rows || []).filter(
                (r) =>
                    String(r.month || "").toLowerCase() === month &&
                    Number(r.year) === Number(year)
            );

            const getRate = (from: string, to: string) => {
                const row = cur.find(
                    (r) =>
                        String(r.user_currency).toLowerCase() === from &&
                        String(r.selected_currency).toLowerCase() === to
                );
                const rate = Number(row?.conversion_rate);
                return Number.isFinite(rate) && rate > 0 ? rate : null;
            };

            const gbpUsd = getRate("gbp", "usd");
            const inrUsd = getRate("inr", "usd");
            const cadUsd = getRate("cad", "usd");

            if (gbpUsd != null) setGbpToUsd(gbpUsd);
            if (inrUsd != null) setInrToUsd(inrUsd);
            if (cadUsd != null) setCadToUsd(cadUsd);

        } catch (err) {
            console.error("Failed to fetch FX from DB, keeping env defaults", err);
        } finally {
            setFxLoading(false);
            setFxReady(true);
        }
    }, [activeDateRegion]);

    useEffect(() => {
        fetchFxRates();
    }, [fetchFxRates]);

    const waitForPaint = () =>
        new Promise<void>((resolve) => {
            requestAnimationFrame(() => {
                requestAnimationFrame(() => {
                    resolve();
                });
            });
        });

    useEffect(() => {
        if (activeTab !== "summary") return;
        if (liveBiPayload) {
            setSummaryLoading(false);
        }
    }, [activeTab, liveBiPayload]);

    useEffect(() => {
        const timezone = getTimezoneForRegion(activeDateRegion);
        const now = new Date();
    }, [activeDateRegion]);

    useEffect(() => {
        if (!isCountryMode) return;
        setGraphRegion(forcedRegion);
        setAmazonRegion(forcedRegion);
    }, [isCountryMode, forcedRegion]);

    const [targetRegion, setTargetRegion] = useState<RegionKey>(
        isCountryMode ? forcedRegion : "Global"
    );

    const [targetSummaries, setTargetSummaries] = useState<{
        uk?: any;
        us?: any;
        ca?: any;
        global?: any;
    }>({});

    useEffect(() => {
        if (isCountryMode) setTargetRegion(forcedRegion);
    }, [isCountryMode, forcedRegion]);

    const targetSummaryCountry = useMemo(() => {
        if (platform === "amazon-us") return "us";
        if (platform === "amazon-uk") return "uk";
        if (platform === "amazon-ca") return "ca";
        if (platform === "global") return "global";
        return "uk";
    }, [platform]);

    const fetchTargetSummary = useCallback(async () => {
        if (isMonthYearNA) {
            setTargetSummaries({});
            return;
        }

        const token =
            typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (!token) return;

        const { monthName, year } = getRegionYearMonth(activeDateRegion);

        const countries =
            platform === "global"
                ? ["uk", "us"]
                : [targetSummaryCountry];

        const results = await Promise.all(
            countries.map(async (country) => {
                const params = new URLSearchParams({
                    month: monthName,
                    year: String(year),
                    country,
                });

                const res = await fetch(`${baseURL}/target-summary?${params.toString()}`, {
                    method: "GET",
                    headers: {
                        Accept: "application/json",
                        Authorization: `Bearer ${token}`,
                    },
                });

                const json = await res.json().catch(() => null);

                return [country, json?.data ?? null] as const;
            })
        );

        const next = Object.fromEntries(results);


        setTargetSummaries(next);
    }, [activeDateRegion, targetSummaryCountry, isMonthYearNA, platform]);

    const fetchPrevTargetSummary = useCallback(async () => {
        if (isMonthYearNA) {
            setPrevTargetSummaries({});
            return;
        }

        const token =
            typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (!token) return;

        const { monthName, year } = getPrevBackendCountryYearMonth();

        const countries =
            platform === "global"
                ? ["uk", "us"]
                : [targetSummaryCountry];

        const results = await Promise.all(
            countries.map(async (country) => {
                const params = new URLSearchParams({
                    month: monthName,
                    year: String(year),
                    country,
                });

                const res = await fetch(`${baseURL}/target-summary?${params.toString()}`, {
                    method: "GET",
                    headers: {
                        Accept: "application/json",
                        Authorization: `Bearer ${token}`,
                    },
                });

                const json = await res.json().catch(() => null);

                return [country, json?.data ?? null] as const;
            })
        );

        const next = Object.fromEntries(results);
        setPrevTargetSummaries(next);
    }, [
        targetSummaryCountry,
        isMonthYearNA,
        platform,
        getPrevBackendCountryYearMonth,
    ]);

    // useEffect(() => {
    //     fetchTargetSummary();
    // }, [fetchTargetSummary]);

    // useEffect(() => {
    //     fetchTargetSummary();
    //     fetchPrevTargetSummary();
    // }, [fetchTargetSummary, fetchPrevTargetSummary]);

    useEffect(() => {
        if (platform === "global") return;
        fetchTargetSummary();
    }, [platform, fetchTargetSummary]);

    useEffect(() => {
        if (platform === "global") return;
        fetchTargetSummary();
        fetchPrevTargetSummary();
    }, [platform, fetchTargetSummary, fetchPrevTargetSummary]);


    const inventoryCountry = useMemo(() => {
        if (platform === "global") return "global";

        if (graphRegionToUse === "UK") return "uk";
        if (graphRegionToUse === "US") return "us";
        if (graphRegionToUse === "CA") return "ca";

        return "global";
    }, [platform, graphRegionToUse]);

    const invMonthYear = useMemo(() => {
        const { monthName, year } = getBackendCountryYearMonth();

        return {
            month: monthName.toLowerCase(),
            year: String(year),
        };
    }, [getBackendCountryYearMonth]);

    const fetchInventory = useCallback(async () => {
        if (isMonthYearNA) {
            setInvLoading(false);
            setInvError("");
            setInvRows([]);
            setInventoryAlerts({});
            return;
        }
        const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
        if (!token) {
            setInvError("Authorization token is missing");
            setInvRows([]);
            return;
        }
        setInvLoading(true);
        setInvError("");

        try {
            const { rows, alerts } = await fetchCurrentInventoryData({
                baseURL,
                token,
                country: inventoryCountry,
                month: invMonthYear.month,
                year: invMonthYear.year,
                XLSX,
            });

            setInvRows(rows);
            setInventoryAlerts(alerts);
        } catch (e: any) {
            setInvError(e?.message || "Unknown error");
            setInvRows([]);
            setInventoryAlerts({});
        } finally {
            setInvLoading(false);
        }
    }, [inventoryCountry, invMonthYear.month, invMonthYear.year, isMonthYearNA]);

    // useEffect(() => {
    //     fetchInventory();
    // }, [fetchInventory]);

    /* ===================== CONVERSION + FORMATTING (DISPLAY CURRENCY) ===================== */
    const convertToDisplayCurrency = useCallback(
        (value: number | null | undefined, from: CurrencyCode) => {
            const n = toNumberSafe(value ?? 0);
            if (!n) return 0;

            // from -> USD
            let usd = n;
            if (from === "GBP") usd = n * gbpToUsd;
            if (from === "INR") usd = n * inrToUsd;
            if (from === "CAD") usd = n * cadToUsd;

            // USD -> displayCurrency
            if (displayCurrency === "USD") return usd;
            if (displayCurrency === "GBP") return gbpToUsd ? usd / gbpToUsd : usd;
            if (displayCurrency === "INR") return inrToUsd ? usd / inrToUsd : usd;
            if (displayCurrency === "CAD") return cadToUsd ? usd / cadToUsd : usd;

            return usd;
        },
        [displayCurrency, gbpToUsd, inrToUsd, cadToUsd]
    );

    const userMonthlyTargetHome = useMemo(() => {
        if (platform === "global") {
            const ukTarget = toNumberSafe(targetSummaries.uk?.target_sales ?? 0);
            const usTarget = toNumberSafe(targetSummaries.us?.target_sales ?? 0);

            const globalTarget = ukTarget * gbpToUsd + usTarget;

            return globalTarget;
        }

        return toNumberSafe(
            targetSummaries[targetSummaryCountry as keyof typeof targetSummaries]?.target_sales ?? 0
        );
    }, [platform, targetSummaries, targetSummaryCountry, gbpToUsd]);


    const prevFullMonthNetSalesDisp = useMemo(() => {
        const v = liveBiPayload?.aligned_totals?.total_previous_net_sales_full_month;
        if (v == null) return 0;

        return platform === "global"
            ? convertToDisplayCurrency(Number(v) || 0, "USD")
            : convertToDisplayCurrency(Number(v) || 0, biSourceCurrency);
    }, [liveBiPayload, convertToDisplayCurrency, biSourceCurrency, platform]);


    /* ===================== INTEGRATION FLAGS ===================== */
    const shopifyDeriv = useMemo(() => {
        if (!shopify) return null;
        const totalOrders = toNumberSafe(shopify.total_orders);
        const netSales = toNumberSafe(shopify.net_sales);
        return { totalOrders, netSales };
    }, [shopify]);

    const shopifyPrevDeriv = useMemo(() => {
        const row = shopifyPrevRows?.[0];
        if (!row) return null;
        const netSales = toNumberSafe(row.net_sales);
        const totalOrders = toNumberSafe(row.total_orders);
        return { netSales, totalOrders };
    }, [shopifyPrevRows]);

    const globalPrevFullMonthNetSalesDisp = useMemo(() => {
        const amazonFull = prevFullMonthNetSalesDisp; // already in display currency
        const shopifyFull = convertToDisplayCurrency(shopifyPrevDeriv?.netSales ?? 0, "INR");
        return amazonFull + shopifyFull;
    }, [prevFullMonthNetSalesDisp, shopifyPrevDeriv?.netSales, convertToDisplayCurrency]);


    const formatDisplayAmount = useCallback(
        (value: number | null | undefined, label?: string) => {
            const n = toNumberSafe(value ?? 0);

            const shouldRound =
                label &&
                ["Gross Sales", "Net Sales", "Cost of Ads", "CM2 Profit"].includes(label);

            const options = shouldRound
                ? {
                    minimumFractionDigits: 0,
                    maximumFractionDigits: 0,
                }
                : {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                };

            switch (displayCurrency) {
                case "USD":
                    return new Intl.NumberFormat("en-US", {
                        style: "currency",
                        currency: "USD",
                        ...options,
                    }).format(n);

                case "GBP":
                    return new Intl.NumberFormat("en-GB", {
                        style: "currency",
                        currency: "GBP",
                        ...options,
                    }).format(n);

                case "CAD":
                    return new Intl.NumberFormat("en-CA", {
                        style: "currency",
                        currency: "CAD",
                        ...options,
                    }).format(n);

                case "INR":
                    return new Intl.NumberFormat("en-IN", {
                        style: "currency",
                        currency: "INR",
                        ...options,
                    }).format(n);

                default:
                    return n.toString();
            }
        },
        [displayCurrency]
    );

    const formatDisplayAmountNoDecimals = useCallback(
        (value: number | null | undefined) => {
            const n = Math.round(toNumberSafe(value ?? 0));

            switch (displayCurrency) {
                case "USD":
                    return new Intl.NumberFormat("en-US", {
                        style: "currency",
                        currency: "USD",
                        minimumFractionDigits: 0,
                        maximumFractionDigits: 0,
                    }).format(n);

                case "GBP":
                    return new Intl.NumberFormat("en-GB", {
                        style: "currency",
                        currency: "GBP",
                        minimumFractionDigits: 0,
                        maximumFractionDigits: 0,
                    }).format(n);

                case "CAD":
                    return new Intl.NumberFormat("en-CA", {
                        style: "currency",
                        currency: "CAD",
                        minimumFractionDigits: 0,
                        maximumFractionDigits: 0,
                    }).format(n);

                case "INR":
                    return new Intl.NumberFormat("en-IN", {
                        style: "currency",
                        currency: "INR",
                        minimumFractionDigits: 0,
                        maximumFractionDigits: 0,
                    }).format(n);

                default:
                    return n.toLocaleString(undefined, {
                        minimumFractionDigits: 0,
                        maximumFractionDigits: 0,
                    });
            }
        },
        [displayCurrency]
    );

    const formatAdsNumber = (value: number) =>
        Number.isFinite(value)
            ? value.toLocaleString("en-GB", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            })
            : "-";

    const formatDisplayK = useCallback(
        (value: number | null | undefined) => {
            const n = toNumberSafe(value ?? 0);
            const abs = Math.abs(n);
            const isK = abs >= 1000;

            const displayVal = isK ? n / 1000 : n;
            const suffix = isK ? "k" : "";

            return `${formatDisplayAmount(displayVal)}${suffix}`;
        },
        [formatDisplayAmount]
    );

    const currencySymbol =
        displayCurrency === "USD"
            ? "$"
            : displayCurrency === "GBP"
                ? "£"
                : displayCurrency === "CAD"
                    ? "CA$"
                    : displayCurrency === "INR"
                        ? "₹"
                        : "¤";

    const biDailySeriesHome = useMemo<GraphDailySeries | null>(() => {
        if (!biDailySeries) return null;

        const fromCurrency: CurrencyCode =
            platform === "global" ? "USD" : biDataCurrency;

        const filterSeriesByCountry = (
            rows: DailyPoint[] | undefined,
            country: "uk" | "us" | "ca"
        ) => {
            return (rows || []).filter((row: any) => {
                const rowCountry = String(
                    row?.source_country || row?.country || ""
                ).toLowerCase();

                return rowCountry === country;
            });
        };

        const getCurrentSource = (): DailyPoint[] => {
            if (platform !== "global") {
                return biDailySeries.current_mtd || [];
            }

            if (globalMtdView === "uk") {
                return (
                    biDailySeries.current_mtd_uk ||
                    filterSeriesByCountry(biDailySeries.current_mtd, "uk")
                );
            }

            if (globalMtdView === "us") {
                return (
                    biDailySeries.current_mtd_us ||
                    filterSeriesByCountry(biDailySeries.current_mtd, "us")
                );
            }

            return biDailySeries.current_mtd_global || biDailySeries.current_mtd || [];
        };

        const getPreviousSource = (): DailyPoint[] => {
            if (platform !== "global") {
                return biDailySeries.previous || [];
            }

            if (globalMtdView === "uk") {
                return (
                    biDailySeries.previous_uk ||
                    filterSeriesByCountry(biDailySeries.previous, "uk")
                );
            }

            if (globalMtdView === "us") {
                return (
                    biDailySeries.previous_us ||
                    filterSeriesByCountry(biDailySeries.previous, "us")
                );
            }

            return biDailySeries.previous_global || biDailySeries.previous || [];
        };

        const currentSource = getCurrentSource();
        const previousSource = getPreviousSource();

        const convPoint = (p: DailyPoint): DailyPoint => ({
            ...p,
            quantity: Number(p.quantity || 0),

            net_sales:
                p.net_sales != null
                    ? convertToDisplayCurrency(Number(p.net_sales || 0), fromCurrency)
                    : p.net_sales,

            gross_sales:
                p.gross_sales != null
                    ? convertToDisplayCurrency(Number(p.gross_sales || 0), fromCurrency)
                    : p.gross_sales,

            profit:
                p.profit != null
                    ? convertToDisplayCurrency(Number(p.profit || 0), fromCurrency)
                    : p.profit,

            cm2_profit:
                p.cm2_profit != null
                    ? convertToDisplayCurrency(Number(p.cm2_profit || 0), fromCurrency)
                    : p.cm2_profit,
        });

        const blankFutureCurrentPoint = (p: DailyPoint): DailyPoint => {
            const converted = convPoint(p);

            if (isCurrentPointAllowed(p)) {
                return converted;
            }

            return {
                ...converted,
                quantity: undefined,
                net_sales: undefined,
                gross_sales: undefined,
                profit: undefined,
                cm2_profit: undefined,
            };
        };

        return {
            previous: previousSource.map(convPoint),
            current_mtd: currentSource.map(blankFutureCurrentPoint),
        };
    }, [
        biDailySeries,
        convertToDisplayCurrency,
        biDataCurrency,
        platform,
        globalMtdView,
        isCurrentPointAllowed,
    ]);

    /* ===================== AMAZON FETCH ===================== */
    const fetchAmazon = useCallback(async () => {
        if (isMonthYearNA) {
            setLoading(false);
            setUnauthorized(false);
            setError(null);
            setData(null);
            return;
        }
        setLoading(true);
        setUnauthorized(false);
        setError(null);

        try {
            const token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

            if (!token) {
                setUnauthorized(true);
                throw new Error("No token found. Please sign in.");
            }

            const apiCountry =
                platform === "global"
                    ? "global"
                    : platform === "amazon-us"
                        ? "us"
                        : platform === "amazon-ca"
                            ? "ca"
                            : "uk";

            const marketplaceId =
                apiCountry === "uk"
                    ? (
                        amazonConnections?.find?.(
                            (c: any) => (c?.country || "").toLowerCase() === "uk"
                        )?.marketplace_id || "A1F83G8C2ARO7P"
                    )
                    : apiCountry === "us"
                        ? (
                            amazonConnections?.find?.(
                                (c: any) => (c?.country || "").toLowerCase() === "us"
                            )?.marketplace_id || "ATVPDKIKX0DER"
                        )
                        : apiCountry === "ca"
                            ? (
                                amazonConnections?.find?.(
                                    (c: any) => (c?.country || "").toLowerCase() === "ca"
                                )?.marketplace_id || "A2EUQ1WTGCTBG2"
                            )
                            : "";

            const params =
                apiCountry === "global"
                    ? new URLSearchParams({
                        country: "global",
                        store_in_db: "true",
                    })
                    : new URLSearchParams({
                        country: apiCountry,
                        marketplace_id: marketplaceId,
                        store_in_db: "true",
                        format: "json",
                    });

            const url = `${FIN_MTD_TX_ENDPOINT}?${params.toString()}`;


            const res = await fetch(url, {
                method: "GET",
                headers: {
                    Accept: "application/json",
                    Authorization: `Bearer ${token}`,
                },
                credentials: "omit",
            });

            if (res.status === 401) {
                setUnauthorized(true);
                throw new Error("Unauthorized — token missing/invalid/expired.");
            }

            const json = await res.json();
            if (!res.ok || json?.success === false) {
                throw new Error(json?.error || `Request failed: ${res.status}`);
            }

            setData(json); // ✅ data now matches your new response shape
        } catch (e: any) {
            setError(e?.message || "Failed to load data");
            setData(null);
        } finally {
            setLoading(false);
        }
    }, [platform, amazonConnections, isMonthYearNA]);

    const renderMoneyWithPerUnit = (amount: number, units: number, fmt: (v: number) => string) => {
        const totalText = fmt(amount);

        if (!units) return <span>{totalText}</span>;

        const perUnit = amount / units;
        const perUnitText = fmt(perUnit);

        return (
            <>
                <span>{totalText}</span>
                <span className="text-[10px] 2xl:text-xs text-charcoal-400 font-medium">
                    ({perUnitText}/unit)
                </span>
            </>
        );
    };

    /* ===================== SHOPIFY STORE INFO ===================== */
    useEffect(() => {
        const fetchShopifyStore = async () => {
            try {
                const token =
                    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
                if (!token) return;

                const res = await fetch(`${baseURL}/shopify/store`, {
                    headers: { Authorization: `Bearer ${token}` },
                });

                const ct = res.headers.get("content-type") || "";
                if (!ct.includes("application/json")) return;

                const d = await res.json();
                if (!res.ok || d?.error) return;

                setShopifyStore(d);
            } catch (err) {
                console.error("Error fetching Shopify store in Dashboard:", err);
            }
        };
        fetchShopifyStore();
    }, []);

    /* ===================== SHOPIFY CURRENT MONTH ===================== */
    const fetchShopify = useCallback(async () => {
        setShopifyLoading(true);
        setShopifyError(null);
        try {
            const user_token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
            if (!user_token) throw new Error("No token found. Please sign in.");

            if (!shopifyStore?.shop_name || !shopifyStore?.access_token) {
                throw new Error("Shopify store not connected.");
            }

            const { monthName, year } = getRegionYearMonth(activeDateRegion);

            const params = new URLSearchParams({
                range: "monthly",
                month: monthName.toLowerCase(),
                year: String(year),
                user_token,
                shop: shopifyStore.shop_name,
                token: shopifyStore.access_token,
            });

            const url = `${SHOPIFY_DROPDOWN_ENDPOINT}?${params.toString()}`;

            const res = await fetch(url, {
                method: "GET",
                headers: { Accept: "application/json", Authorization: `Bearer ${user_token}` },
                credentials: "omit",
            });

            if (res.status === 401) throw new Error("Unauthorized — token missing/invalid/expired.");
            if (!res.ok) throw new Error(`Shopify request failed: ${res.status}`);

            const json = await res.json();
            const row = json?.last_row_data ? json.last_row_data : null;
            setShopifyRows(row ? [row] : []);
        } catch (e: any) {
            setShopifyError(e?.message || "Failed to load Shopify data");
            setShopifyRows([]);
        } finally {
            setShopifyLoading(false);
        }
    }, [shopifyStore, activeDateRegion]);

    /* ===================== SHOPIFY PREVIOUS MONTH ===================== */
    const fetchShopifyPrev = useCallback(async () => {
        try {
            const user_token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
            if (!user_token) throw new Error("No token found. Please sign in.");

            if (!shopifyStore?.shop_name || !shopifyStore?.access_token) {
                throw new Error("Shopify store not connected.");
            }

            const { year, monthName } = getPrevRegionYearMonth(activeDateRegion);

            const params = new URLSearchParams({
                range: "monthly",
                month: monthName.toLowerCase(),
                year: String(year),
                user_token,
                shop: shopifyStore.shop_name,
                token: shopifyStore.access_token,
            });

            const url = `${SHOPIFY_DROPDOWN_ENDPOINT}?${params.toString()}`;

            const res = await fetch(url, {
                method: "GET",
                headers: { Accept: "application/json", Authorization: `Bearer ${user_token}` },
                credentials: "omit",
            });

            if (res.status === 401) throw new Error("Unauthorized — token missing/invalid/expired.");
            if (!res.ok) throw new Error(`Shopify (prev) request failed: ${res.status}`);

            const json = await res.json();
            const row = json?.last_row_data ? json.last_row_data : null;
            setShopifyPrevRows(row ? [row] : []);
        } catch (e: any) {
            console.warn("Shopify prev-month fetch failed:", e?.message);
            setShopifyPrevRows([]);
        }
    }, [shopifyStore, activeDateRegion]);

    /* ===================== ✅ SHARED BI FETCH (FOR CARDS + GRAPH) ===================== */
    const { monthName: currMonthName, year: currYear } =
        getBackendCountryYearMonth();
    const lastBiKeyRef = useRef<string>("");
    const aiRequestedRef = useRef<boolean>(false);

    // const fetchBiSeries = useCallback(
    //     async (startDay?: number | null, endDay?: number | null) => {
    //         if (isMonthYearNA) {
    //             setBiError(null);
    //             setBiLoading(false);
    //             setBiStatus("ready");
    //             setBiDailySeries(null);
    //             setBiPeriods(null);
    //             setBiAlignedTotals(null);
    //             setLiveBiPayload(null);
    //             return;
    //         }

    //         if (!showLiveBI) return;

    //         const normalized = (biCountryName || "").toLowerCase();

    //         if (!normalized) return;

    //         // Safety: only global page can call countryName=global
    //         if (normalized === "global" && platform !== "global") return;

    //         // Safety: countrywise pages should never accidentally call global
    //         if (platform !== "global" && normalized === "global") return;

    //         setBiError(null);
    //         setBiLoading(true);
    //         setBiStatus("loading");

    //         try {
    //             const token =
    //                 typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    //             const params = new URLSearchParams({
    //                 countryName: normalized,
    //                 ranged: "MTD",
    //                 month: currMonthName.toLowerCase(),
    //                 year: String(currYear),
    //                 generate_ai_insights: aiRequestedRef.current ? "true" : "false",
    //             });

    //             const finalStartDay = startDay ?? 1;
    //             const finalEndDay = Math.min(endDay ?? dashboardAllowedDay, dashboardAllowedDay);

    //             params.set("start_day", String(finalStartDay));
    //             params.set("end_day", String(finalEndDay));

    //             let attempts = 0;
    //             const maxAttempts = 10;

    //             while (attempts < maxAttempts) {
    //                 const res = await fetch(`${LIVE_MTD_BI_ENDPOINT}?${params.toString()}`, {
    //                     headers: token ? { Authorization: `Bearer ${token}` } : undefined,
    //                 });

    //                 if (res.status === 202) {
    //                     setBiStatus("processing");
    //                     attempts += 1;
    //                     await sleep(3000);
    //                     continue;
    //                 }

    //                 if (!res.ok) {
    //                     throw new Error(`BI failed: ${res.status}`);
    //                 }

    //                 const json: BiApiResponse = await res.json();

    //                 setLiveBiPayload(json);
    //                 setBiPeriods(json?.periods || null);
    //                 setBiDailySeries(json?.daily_series || null);

    //                 const alignedFromNested = json?.aligned_totals;
    //                 const alignedFromTopLevel: BiAlignedTotals = {
    //                     total_current_advertising: (json as any)?.total_current_advertising,
    //                     total_previous_advertising: (json as any)?.total_previous_advertising,

    //                     total_current_net_sales: (json as any)?.total_current_net_sales,
    //                     total_previous_net_sales: (json as any)?.total_previous_net_sales,
    //                     total_previous_net_sales_full_month:
    //                         (json as any)?.total_previous_net_sales_full_month,

    //                     total_current_platform_fees: (json as any)?.total_current_platform_fees,
    //                     total_previous_platform_fees: (json as any)?.total_previous_platform_fees,

    //                     total_current_profit: (json as any)?.total_current_profit,
    //                     total_previous_profit: (json as any)?.total_previous_profit,

    //                     // ✅ add these
    //                     current_cm2_profit: (json as any)?.current_cm2_profit,
    //                     previous_cm2_profit: (json as any)?.previous_cm2_profit,

    //                     total_current_profit_cm2: (json as any)?.total_current_profit_cm2,
    //                     total_previous_profit_cm2: (json as any)?.total_previous_profit_cm2,

    //                     total_current_profit_percentage: (json as any)?.total_current_profit_percentage,
    //                     total_previous_profit_percentage: (json as any)?.total_previous_profit_percentage,

    //                     total_current_rembursement_fee: (json as any)?.total_current_rembursement_fee,
    //                     total_previous_rembursement_fee: (json as any)?.total_previous_rembursement_fee,
    //                 };

    //                 setBiAlignedTotals(alignedFromNested ?? alignedFromTopLevel ?? null);
    //                 setBiStatus("ready");
    //                 return;
    //             }

    //             throw new Error("Live BI is still processing. Max retry limit reached.");
    //         } catch (e: any) {
    //             setBiPeriods(null);
    //             setBiDailySeries(null);
    //             setBiAlignedTotals(null);
    //             setBiStatus("error");
    //             setBiError(e?.message || "Failed to load BI series");
    //         } finally {
    //             setBiLoading(false);
    //         }
    //     },
    //     [showLiveBI, biCountryName, currMonthName, currYear, isMonthYearNA, platform, dashboardAllowedDay]
    // );

    const fetchPreviousSkuwiseGlobal = useCallback(async (
        startDay: number | null = selectedStartDay,
        endDay: number | null = selectedEndDay
    ) => {
        if (isMonthYearNA) {
            setPreviousSkuwiseGlobalData(null);
            return;
        }

        if (platform !== "global") {
            setPreviousSkuwiseGlobalData(null);
            return;
        }

        const token =
            typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (!token) return;

        try {
            setPreviousSkuwiseGlobalLoading(true);

            const finalStartDay = startDay ?? 1;
            const finalEndDay = Math.min(endDay ?? dashboardAllowedDay, dashboardAllowedDay);

            const params = new URLSearchParams({
                as_of: getGlobalPreviousSkuwiseAsOfISO(),
                start_day: String(finalStartDay),
                end_day: String(finalEndDay),
            });

            const res = await fetch(
                `${PREVIOUS_SKUWISE_GLOBAL_ENDPOINT}?${params.toString()}`,
                {
                    method: "GET",
                    headers: {
                        Accept: "application/json",
                        Authorization: `Bearer ${token}`,
                    },
                }
            );

            const json = await res.json().catch(() => null);

            if (!res.ok || json?.success === false) {
                throw new Error(json?.error || `Previous SKU-wise global failed: ${res.status}`);
            }

            setPreviousSkuwiseGlobalData(json);
        } catch (err) {
            console.error("Failed to fetch previous SKU-wise global:", err);
            setPreviousSkuwiseGlobalData(null);
        } finally {
            setPreviousSkuwiseGlobalLoading(false);
        }
    }, [
        isMonthYearNA,
        platform,
        selectedStartDay,
        selectedEndDay,
        dashboardAllowedDay
    ]);

    const fetchLiveBiPayload = useCallback(
        async ({
            startDay = selectedStartDay,
            endDay = selectedEndDay,
            generateInsights = false,
            skipLoader = false,
        }: FetchLiveBiPayloadArgs = {}) => {
            if (isMonthYearNA) return;

            const shouldShowBiLoader = !skipLoader && !generateInsights;

            setBiError(null);

            if (shouldShowBiLoader) {
                setBiLoading(true);
                setBiStatus("loading");
            }

            try {
                const token =
                    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

                const params = new URLSearchParams({
                    countryName: biCountryName.toLowerCase(),
                    ranged: "MTD",
                    month: currMonthName.toLowerCase(),
                    year: String(currYear),
                    generate_ai_insights: generateInsights ? "true" : "false",
                });

                const finalStartDay = startDay ?? 1;
                const finalEndDay = Math.min(endDay ?? dashboardAllowedDay, dashboardAllowedDay);

                params.set("start_day", String(finalStartDay));
                params.set("end_day", String(finalEndDay));

                const res = await fetch(`${LIVE_MTD_BI_ENDPOINT}?${params.toString()}`, {
                    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
                });

                if (!res.ok) {
                    throw new Error(`BI failed: ${res.status}`);
                }

                const json: BiApiResponse = await res.json();

                setLiveBiPayload(json);
                setBiPeriods(json?.periods || null);
                setBiDailySeries(json?.daily_series || null);
                setBiAlignedTotals(json?.aligned_totals || null);
                setLiveBiReady(true);

                if (!skipLoader) {
                    setBiStatus("ready");
                }

                return json;
            } catch (e: any) {
                setBiError(e?.message || "Failed to load BI data");

                if (!skipLoader) {
                    setBiStatus("error");
                }

                throw e;
            } finally {
                if (shouldShowBiLoader) {
                    setBiLoading(false);
                }
            }
        },
        [
            selectedStartDay,
            selectedEndDay,
            isMonthYearNA,
            biCountryName,
            currMonthName,
            currYear,
            dashboardAllowedDay,
        ]
    );

    const runDashboardLoadWithSteps = useCallback(async () => {
        if (isMonthYearNA) {
            resetStepState();
            return;
        }

        setShowDashboardStepLoader(true);
        setLoadingStartedAt(Date.now());
        setDashboardBusy(true);

        setError(null);
        setBiError(null);
        setInvError("");
        setMonthlySpError(null);
        setShopifyError(null);
        setAdsSeedError(null);

        setCurrentStep(1);
        setCompletedSteps(new Set());
        setStepProgress({
            active: true,
            label: "",
            percentage: 0,
            detail: "",
        });

        try {
            setStep(1, "MTD Fetching", 5, "Preparing dashboard refresh...");

            setStep(1, "MTD Fetching", 10, "Fetching currency rates...");
            await fetchFxRates();

            setStep(1, "MTD Fetching", 15, "Fetching target summary...");
            await fetchTargetSummary();
            await fetchPrevTargetSummary();

            if (platform === "global") {
                setStep(1, "MTD Fetching", 22, "Fetching SKU-wise global data...");
                await fetchPreviousSkuwiseGlobal(selectedStartDay, selectedEndDay);
            }

            const jwtToken =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

            if (platform !== "shopify" && jwtToken) {
                setStep(1, "MTD Fetching", 38, "Starting ads sync in background...");
                void runAdsBackgroundSync();
            } else {
                setStep(1, "MTD Fetching", 48, "Skipping ads fetch for Shopify-only mode...");
            }

            setStep(1, "MTD Fetching", 62, "Fetching Amazon MTD data...");
            await fetchAmazon();

            if (showLiveBI) {
                setStep(1, "MTD Fetching", 78, "Fetching Live BI data.");

                await fetchLiveBiPayload({
                    startDay: selectedStartDay,
                    endDay: selectedEndDay,
                    generateInsights: false,
                    skipLoader: true,
                });
            } else {
                setStep(1, "MTD Fetching", 78, "Live BI not enabled, skipping.");
            }

            if (shopifyStore?.shop_name && shopifyStore?.access_token) {
                setStep(1, "MTD Fetching", 90, "Fetching Shopify current month data...");
                await fetchShopify();

                setStep(1, "MTD Fetching", 96, "Fetching Shopify previous month data...");
                await fetchShopifyPrev();
            } else {
                setStep(1, "MTD Fetching", 96, "Shopify not connected, skipping Shopify fetch...");
            }

            setStep(1, "MTD Fetching", 100, "MTD data ready");
            markStepComplete(1);

            setStep(2, "Inventory Fetch", 20, "Fetching current inventory...");
            await fetchInventory();

            setStep(2, "Inventory Fetch", 100, "Inventory ready");
            markStepComplete(2);

            setStep(3, "Plotting Graph", 40, "Preparing charts and tables...");
            await waitForPaint();

            setStep(3, "Plotting Graph", 75, "Preparing charts and tables...");
            await waitForPaint();

            setStep(3, "Plotting Graph", 95, "Final render in progress...");
            await waitForPaint();

            setStep(3, "Plotting Graph", 100, "Dashboard ready");
            markStepComplete(3);

            await waitForPaint();

            setStepProgress((prev) => ({
                ...prev,
                active: false,
            }));

            setLoadingStartedAt(null);
            setDashboardBusy(false);
        } catch (e: any) {
            console.error("runDashboardLoadWithSteps failed:", e);
            setError(e?.message || "Failed to load dashboard");
            setDashboardBusy(false);
            setStepProgress((prev) => ({
                ...prev,
                active: false,
            }));
        } finally {
            setDashboardBusy(false);
            setShowDashboardStepLoader(false);
            setStepProgress((prev) => ({
                ...prev,
                active: false,
            }));
            setLoadingStartedAt(null);
        }
    }, [
        isMonthYearNA,
        platform,
        showLiveBI,
        selectedStartDay,
        selectedEndDay,
        shopifyStore,
        fetchFxRates,
        fetchTargetSummary,
        fetchPrevTargetSummary,
        fetchPreviousSkuwiseGlobal,
        fetchAmazon,
        // fetchBiSeries,
        fetchLiveBiPayload,
        fetchShopify,
        fetchShopifyPrev,
        fetchInventory,
        runAdsBackgroundSync,
    ]);

    const liveDashboardCountry = useMemo(() => {
        if (platform === "global") return "global";
        if (platform === "amazon-us") return "us";
        if (platform === "amazon-ca") return "ca";
        return "uk";
    }, [platform]);

    useEffect(() => {
        if (typeof window === "undefined") return;

        const key = "live-dashboard-cache-init";

        if (!localStorage.getItem(key)) {
            localStorage.setItem(key, "initialized");
        }
    }, []);

    const hasSavedRef = useRef(false);
    const didBootstrapRef = useRef<string | null>(null);

    const saveDashboardCacheToBackend = useCallback(
        async (payload: DashboardCachePayload): Promise<void> => {
            if (typeof window === "undefined") return;

            try {
                const token = localStorage.getItem("jwtToken");
                if (!token) return;

                const res = await fetch(LIVE_DASHBOARD_CACHE_ENDPOINT, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        Accept: "application/json",
                        Authorization: `Bearer ${token}`,
                    },
                    body: JSON.stringify({
                        country: liveDashboardCountry,
                        platform: String(platform || "").toLowerCase(),
                        region: String(activeDateRegion || ""),
                        startDay: selectedStartDay,
                        endDay: selectedEndDay,
                        cachePayload: payload,
                    }),
                });

                const json = await res.json().catch(() => null);

                if (!res.ok || !json?.success) {
                    throw new Error(
                        json?.error || `Failed to save dashboard cache (${res.status})`
                    );
                }

                return;
            } catch (err) {
                console.error(err);
                return;
            }
        },
        [
            liveDashboardCountry,
            platform,
            activeDateRegion,
            selectedStartDay,
            selectedEndDay,
        ]
    );

    const ensureLocalStorageThenSave = async (payload: DashboardCachePayload) => {
        if (typeof window === "undefined") return;

        localStorage.setItem(
            liveCacheKey,
            JSON.stringify({
                ...payload,
                savedAt: Date.now(),
            })
        );

        await saveDashboardCacheToBackend(payload);
    };

    const formatAppliedRangeLabel = (start: number | null, end: number | null) => {
        if (start == null || end == null) return "Select Date Range";

        const { monthName } = getRegionYearMonth(activeDateRegion);
        const shortMonth = monthName.slice(0, 3);

        return `${shortMonth} ${start}-${end}`;
    };

    type DashboardCachePayload = ReturnType<typeof buildDashboardCachePayload>;

    const applyDashboardCachePayload = useCallback((parsed: any) => {
        setData(parsed?.data ?? null);
        setBiDailySeries(parsed?.biDailySeries ?? null);
        setBiPeriods(parsed?.biPeriods ?? null);
        setLiveBiPayload(parsed?.liveBiPayload ?? null);
        setBiAlignedTotals(parsed?.biAlignedTotals ?? null);

        setInvRows(parsed?.invRows ?? []);
        setInventoryAlerts(parsed?.inventoryAlerts ?? {});
        setMonthlySpRows(parsed?.monthlySpRows ?? []);
        setMonthlySpTotalSpend(parsed?.monthlySpTotalSpend ?? null);

        setTargetSummaries(parsed?.targetSummaries ?? {});
        setPrevTargetSummaries(parsed?.prevTargetSummaries ?? {});
        setShopifyRows(parsed?.shopifyRows ?? []);
        setShopifyPrevRows(parsed?.shopifyPrevRows ?? []);
        setPreviousSkuwiseGlobalData(parsed?.previousSkuwiseGlobalData ?? null);
        setGlobalCountryPayloads(parsed?.globalCountryPayloads ?? {});

        setLiveBiReady(!!parsed?.liveBiReady);
        setBiStatus(parsed?.biStatus ?? (parsed?.biDailySeries ? "ready" : "idle"));
        setBiLoading(false);
        setBiError(null);
    }, []);

    const getDashboardCacheFromBackend = useCallback(async () => {
        if (typeof window === "undefined") return null;

        const token = localStorage.getItem("jwtToken");
        if (!token) return null;

        const params = new URLSearchParams({
            country: liveDashboardCountry,
            platform: String(platform || "").toLowerCase(),
            region: String(activeDateRegion || ""),
        });

        if (selectedStartDay != null) {
            params.set("start_day", String(selectedStartDay));
        }

        if (selectedEndDay != null) {
            params.set("end_day", String(selectedEndDay));
        }

        const res = await fetch(`${LIVE_DASHBOARD_CACHE_ENDPOINT}?${params.toString()}`, {
            method: "GET",
            headers: {
                Accept: "application/json",
                Authorization: `Bearer ${token}`,
            },
            cache: "no-store",
        });

        const json = await res.json().catch(() => null);

        if (!res.ok) {
            throw new Error(json?.error || `Failed to fetch dashboard cache (${res.status})`);
        }

        const payload =
            json?.data?.payload ??
            json?.payload ??
            null;

        if (!payload) {
            return {
                found: false,
                payload: null,
                updatedAt: null,
            };
        }

        return {
            found: true,
            payload,
            updatedAt:
                json?.data?.updated_at ??
                json?.data?.created_at ??
                json?.updated_at ??
                json?.created_at ??
                null,
        };
    }, [
        liveDashboardCountry,
        platform,
        activeDateRegion,
        selectedStartDay,
        selectedEndDay,
    ]);

    const liveCacheKey = useMemo(() => {
        const country =
            platform === "amazon-us" ? "us" :
                platform === "amazon-uk" ? "uk" :
                    platform === "amazon-ca" ? "ca" :
                        "global";

        return `live-dashboard-cache:${country}:${activeDateRegion}`;
    }, [platform, activeDateRegion]);

    const lastRefreshKey = useMemo(() => {
        return `${liveCacheKey}:last-updated-at`;
    }, [liveCacheKey]);

    const restoreLiveCacheFromLocalStorage = useCallback(() => {
        if (typeof window === "undefined") return false;

        const raw = localStorage.getItem(liveCacheKey);
        if (!raw) return false;

        try {
            const parsed = JSON.parse(raw);

            applyDashboardCachePayload(parsed);

            const normalizeRefreshTimestamp = (value: any): number | null => {
                if (!value) return null;

                const numeric = Number(value);
                if (Number.isFinite(numeric) && numeric > 0) {
                    return numeric;
                }

                const parsedDate = new Date(value).getTime();
                return Number.isFinite(parsedDate) && parsedDate > 0 ? parsedDate : null;
            };

            const restoredLastRefreshAt =
                normalizeRefreshTimestamp(parsed?.lastRefreshAt) ??
                normalizeRefreshTimestamp(parsed?.savedAt) ??
                normalizeRefreshTimestamp(localStorage.getItem(lastRefreshKey));

            setLastRefreshAt(restoredLastRefreshAt);

            if (restoredLastRefreshAt) {
                localStorage.setItem(lastRefreshKey, String(restoredLastRefreshAt));
            }

            return true;
        } catch (err) {
            console.error("Failed to restore live cache from localStorage:", err);
            return false;
        }
    }, [liveCacheKey, lastRefreshKey, applyDashboardCachePayload]);

    const getLiveCacheKey = useCallback(
        (country: "uk" | "us") =>
            `live-dashboard-cache:${country}:${activeDateRegion}:${selectedStartDay ?? "na"}:${selectedEndDay ?? "na"}`,
        [activeDateRegion, selectedStartDay, selectedEndDay]
    );

    const fetchDashboardCacheByCountry = useCallback(async (country: "uk" | "us") => {
        const token = localStorage.getItem("jwtToken");
        if (!token) return null;

        const params = new URLSearchParams({
            country,
            platform: country === "uk" ? "amazon-uk" : "amazon-us",
            region: country.toUpperCase(),
        });

        if (selectedStartDay != null) params.set("start_day", String(selectedStartDay));
        if (selectedEndDay != null) params.set("end_day", String(selectedEndDay));

        const res = await fetch(`${LIVE_DASHBOARD_CACHE_ENDPOINT}?${params}`, {
            headers: { Accept: "application/json", Authorization: `Bearer ${token}` },
        });

        const json = await res.json().catch(() => null);
        if (!res.ok || !json?.success || !json?.data?.payload) return null;

        return json.data.payload;
    }, [selectedStartDay, selectedEndDay]);

    const usdFrom = (value: any, country: "uk" | "us") => {
        const n = toNumberSafe(value);
        return country === "uk" ? n * gbpToUsd : n;
    };

    const mergeMoneyField = (
        ukObj: any,
        usObj: any,
        key: string
    ) => usdFrom(ukObj?.[key], "uk") + usdFrom(usObj?.[key], "us");

    const mergeNumberField = (
        ukObj: any,
        usObj: any,
        key: string
    ) => toNumberSafe(ukObj?.[key]) + toNumberSafe(usObj?.[key]);

    const mergeSeries = (ukSeries: any[] = [], usSeries: any[] = []) => {
        const map = new Map<string, any>();

        const addRows = (rows: any[], country: "uk" | "us") => {
            rows.forEach((row) => {
                const date = row?.date;
                if (!date) return;

                const prev = map.get(date) || { date };

                map.set(date, {
                    ...prev,
                    quantity: toNumberSafe(prev.quantity) + toNumberSafe(row.quantity),
                    gross_sales: toNumberSafe(prev.gross_sales) + usdFrom(row.gross_sales, country),
                    net_sales: toNumberSafe(prev.net_sales) + usdFrom(row.net_sales, country),
                    profit: toNumberSafe(prev.profit) + usdFrom(row.profit, country),
                    cm2_profit: toNumberSafe(prev.cm2_profit) + usdFrom(row.cm2_profit, country),
                    advertising: toNumberSafe(prev.advertising) + usdFrom(row.advertising, country),
                    platform_fee: toNumberSafe(prev.platform_fee) + usdFrom(row.platform_fee, country),
                });
            });
        };

        addRows(ukSeries, "uk");
        addRows(usSeries, "us");

        return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date));
    };

    const mergeDashboardPayloads = (ukPayload: any, usPayload: any) => {
        const ukData = ukPayload?.data || {};
        const usData = usPayload?.data || {};

        const ukDerived = ukData?.derived_totals || {};
        const usDerived = usData?.derived_totals || {};

        const firstNonZero = (...values: any[]) => {
            for (const v of values) {
                const n = toNumberSafe(v);
                if (n !== 0) return n;
            }
            return 0;
        };

        const getShipmentCharges = (payload: any, country: "uk" | "us") => {
            const data = payload?.data || {};

            const raw = firstNonZero(
                payload?.shipmentCharges,
                data?.summary?.shipment_charges,
                data?.summary?.shipping_charges,
                data?.summary?.shipment_fees,
                data?.pl_summary?.shipment_charges,
                data?.mtd_summary?.shipment_charges,
                data?.totals?.shipment_charges,
                data?.derived_totals?.shipment_charges
            );

            return country === "uk" ? raw * gbpToUsd : raw;
        };

        const ukCurrentAspUsd = toNumberSafe(ukDerived?.asp) * gbpToUsd;
        const usCurrentAspUsd = toNumberSafe(usDerived?.asp);

        const ukPrevAspUsd = toNumberSafe(ukData?.previous_period?.totals?.asp) * gbpToUsd;
        const usPrevAspUsd = toNumberSafe(usData?.previous_period?.totals?.asp);

        const globalMergedCurrentAsp = (ukCurrentAspUsd + usCurrentAspUsd) / 2;
        const globalMergedPrevAsp = (ukPrevAspUsd + usPrevAspUsd) / 2;

        const ukTotals = ukData?.totals || {};
        const usTotals = usData?.totals || {};

        const currentUnits = mergeNumberField(ukTotals, usTotals, "quantity");
        const currentNetSales = mergeMoneyField(ukDerived, usDerived, "net_sales");
        const currentGrossSales = mergeMoneyField(ukTotals, usTotals, "gross_sales");

        const getPayloadAdsTotal = (payload: any) => {
            if (payload?.adsSpendTotal != null) return toNumberSafe(payload.adsSpendTotal);
            if (payload?.data?.adsSpendTotal != null) return toNumberSafe(payload.data.adsSpendTotal);
            if (payload?.data?.rawAdsSpendTotal != null) return toNumberSafe(payload.data.rawAdsSpendTotal);
            return 0;
        };

        const currentAdvertising =
            getPayloadAdsTotal(ukPayload) * gbpToUsd +
            getPayloadAdsTotal(usPayload);

        const currentShipmentCharges =
            getShipmentCharges(ukPayload, "uk") + getShipmentCharges(usPayload, "us");

        const getPayloadCm2Profit = (payload: any) => {
            if (payload?.cm2Profit != null) return toNumberSafe(payload.cm2Profit);

            const profit = toNumberSafe(payload?.grandTotalProfit);
            const ads = toNumberSafe(payload?.adsSpendTotal);
            const platformFee = Math.abs(toNumberSafe(payload?.grandTotalPlatformFee));

            if (payload?.grandTotalProfit != null) {
                return profit - ads - platformFee;
            }

            return toNumberSafe(payload?.data?.derived_totals?.cm2_profit);
        };

        const ukCurrentCm2Local = getPayloadCm2Profit(ukPayload);
        const usCurrentCm2Local = getPayloadCm2Profit(usPayload);

        const currentCm2Profit = ukCurrentCm2Local * gbpToUsd + usCurrentCm2Local;
        const currentProfit = mergeMoneyField(ukDerived, usDerived, "profit");

        const ukPrevTotals = ukData?.previous_period?.totals || {};
        const usPrevTotals = usData?.previous_period?.totals || {};

        const prevGraphDebugRows = labels.map((label) => {
            const getRaw = (row: any) => {
                switch (label) {
                    case "Net Sales":
                        return row?.net_sales;
                    case "COGS":
                        return row?.cogs;
                    case "Marketplace Fees":
                        return row?.amazon_fees;
                    case "Tax & Credits":
                        return row?.tax_and_credits;
                    case "Advertisements":
                        return row?.advertising_fees;
                    case "Others":
                        return row?.platform_fee;
                    case "CM1 Profit":
                        return row?.profit;
                    case "CM2 Profit":
                        return row?.cm2_profit;
                    default:
                        return 0;
                }
            };

            const ukRaw = toNumberSafe(getRaw(ukPrevTotals));
            const usRaw = toNumberSafe(getRaw(usPrevTotals));
            const ukUsd = ukRaw * gbpToUsd;
            const usUsd = usRaw;

            return {
                label,
                ukRawGBP: ukRaw,
                ukConvertedUSD: ukUsd,
                usRawUSD: usRaw,
                mergedUSD: ukUsd + usUsd,
            };
        });

        // console.table(prevGraphDebugRows);

        const prevUnits = mergeNumberField(ukPrevTotals, usPrevTotals, "quantity");
        const prevNetSales = mergeMoneyField(ukPrevTotals, usPrevTotals, "net_sales");
        const prevGrossSales = mergeMoneyField(ukPrevTotals, usPrevTotals, "gross_sales");

        const prevAdvertising =
            toNumberSafe(ukPrevTotals?.advertising_fees) * gbpToUsd +
            toNumberSafe(usPrevTotals?.advertising_fees);

        const prevCm2Profit = mergeMoneyField(ukPrevTotals, usPrevTotals, "cm2_profit");
        const prevProfit = mergeMoneyField(ukPrevTotals, usPrevTotals, "profit");


        const prevCogs = mergeMoneyField(ukPrevTotals, usPrevTotals, "cogs");

        const prevMarketplaceFees =
            mergeMoneyField(ukPrevTotals, usPrevTotals, "amazon_fees") ||
            (
                mergeMoneyField(ukPrevTotals, usPrevTotals, "fba_fees") +
                mergeMoneyField(ukPrevTotals, usPrevTotals, "selling_fees")
            );

        const prevTaxAndCredits = mergeMoneyField(
            ukPrevTotals,
            usPrevTotals,
            "tax_and_credits"
        );

        const prevPlatformFee = mergeMoneyField(
            ukPrevTotals,
            usPrevTotals,
            "platform_fee"
        );

        const ukPrevFullMonthNetSalesGBP = toNumberSafe(
            ukPayload?.biAlignedTotals?.total_previous_net_sales_full_month
        );

        const usPrevFullMonthNetSalesUSD = toNumberSafe(
            usPayload?.biAlignedTotals?.total_previous_net_sales_full_month
        );

        const ukPrevFullMonthNetSalesUSD =
            ukPrevFullMonthNetSalesGBP * gbpToUsd;

        const prevFullMonthNetSales =
            ukPrevFullMonthNetSalesUSD + usPrevFullMonthNetSalesUSD;

        const currentTacos =
            currentNetSales > 0 ? (currentAdvertising / currentNetSales) * 100 : 0;

        const prevTacos =
            prevNetSales > 0 ? (prevAdvertising / prevNetSales) * 100 : 0;

        const mergedBiAlignedTotals = {
            ...ukPayload?.biAlignedTotals,

            total_current_net_sales: currentNetSales,
            total_previous_net_sales: prevNetSales,
            total_previous_net_sales_full_month: prevFullMonthNetSales,

            total_current_advertising: currentAdvertising,
            total_previous_advertising: prevAdvertising,

            total_current_tacos: currentTacos,
            total_previous_tacos: prevTacos,

            total_current_profit_cm2: currentCm2Profit,
            total_previous_profit_cm2: prevCm2Profit,

            total_current_profit: currentProfit,
            total_previous_profit: prevProfit,

            total_current_profit_percentage:
                currentNetSales > 0 ? (currentCm2Profit / currentNetSales) * 100 : 0,

            total_previous_profit_percentage:
                prevNetSales > 0 ? (prevCm2Profit / prevNetSales) * 100 : 0,
        };

        const mergedBiDailySeries = {
            current_mtd: mergeSeries(
                ukPayload?.biDailySeries?.current_mtd,
                usPayload?.biDailySeries?.current_mtd
            ),
            previous: mergeSeries(
                ukPayload?.biDailySeries?.previous,
                usPayload?.biDailySeries?.previous
            ),
        };

        const mergedSkuwiseItems = mergeProductwiseRowsGlobal(
            ukPayload?.data?.skuwise_items || [],
            usPayload?.data?.skuwise_items || []
        );

        return {
            ...ukPayload,

            biAlignedTotals: mergedBiAlignedTotals,
            biDailySeries: mergedBiDailySeries,

            liveBiPayload: {
                ...ukPayload?.liveBiPayload,
                aligned_totals: mergedBiAlignedTotals,
                daily_series: mergedBiDailySeries,
            },

            data: {
                ...ukData,

                summary: {
                    ...(ukData?.summary || {}),
                    shipment_charges: currentShipmentCharges,
                    shipping_charges: currentShipmentCharges,
                    shipment_fees: currentShipmentCharges,
                },

                skuwise_items: mergedSkuwiseItems,
                derived_totals: {
                    ...ukDerived,
                    net_sales: currentNetSales,
                    gross_sales: currentGrossSales,
                    asp: globalMergedCurrentAsp,
                    advertising_fees: currentAdvertising,
                    tacos: currentTacos,
                    acos: currentTacos,
                    cm2_profit: currentCm2Profit,
                    profit: currentProfit,
                    shipment_charges: currentShipmentCharges,
                    shipping_charges: currentShipmentCharges,
                    shipment_fees: currentShipmentCharges,
                },
                totals: {
                    ...ukTotals,
                    quantity: currentUnits,
                    gross_sales: currentGrossSales,
                },
                previous_period: {
                    ...ukData?.previous_period,
                    totals: {
                        ...ukPrevTotals,

                        quantity: prevUnits,
                        net_sales: prevNetSales,
                        gross_sales: prevGrossSales,
                        asp: globalMergedPrevAsp,

                        cogs: prevCogs,
                        amazon_fees: prevMarketplaceFees,
                        tax_and_credits: prevTaxAndCredits,
                        platform_fee: prevPlatformFee,

                        advertising_fees: prevAdvertising,
                        cm2_profit: prevCm2Profit,
                        profit: prevProfit,
                        profit_percentage:
                            prevNetSales > 0 ? (prevCm2Profit / prevNetSales) * 100 : 0,
                    },
                },
                previous_month_total_net_sales: {
                    ...(ukData?.previous_month_total_net_sales || {}),
                    total: prevFullMonthNetSales,
                },
            },
        };
    };



    const STEP_ESTIMATED_SECONDS: Record<number, number> = {
        1: 60,
        2: 60,
        3: 60,
    };

    // useEffect(() => {
    //     if (typeof window === "undefined") return;

    //     const saved = localStorage.getItem(lastRefreshKey);
    //     if (!saved) {
    //         setLastRefreshAt(null);
    //         return;
    //     }

    //     const ts = Number(saved);
    //     setLastRefreshAt(Number.isNaN(ts) ? null : ts);
    // }, [lastRefreshKey]);

    const getRelativeRefreshText = useCallback((ts: number | null) => {
        if (!ts) return "Never refreshed";

        const diffMs = Date.now() - ts;
        const diffSec = Math.floor(diffMs / 1000);
        const diffMin = Math.floor(diffSec / 60);
        const diffHr = Math.floor(diffMin / 60);
        const diffDay = Math.floor(diffHr / 24);

        if (diffSec < 60) return "just now";
        if (diffMin < 60) return `${diffMin} min ago`;
        if (diffHr < 24) return `${diffHr} hr ago`;
        return `${diffDay} day ago`;
    }, []);

    const handleHardRefresh = useCallback(async () => {
        if (typeof window === "undefined") return;

        resetStepState();

        try {
            await fetchCountryTime();

            await runDashboardLoadWithSteps();

            const refreshedAt = Date.now();

            localStorage.setItem(lastRefreshKey, String(refreshedAt));
            setLastRefreshAt(refreshedAt);

            triggerCachePost();
        } catch (err) {
            console.error("Hard refresh failed:", err);
            isManualRefreshRef.current = false;
            shouldPostCacheRef.current = false;
        }
    }, [
        fetchCountryTime,
        runDashboardLoadWithSteps,
        triggerCachePost,
        lastRefreshKey,
    ]);

    useEffect(() => {
        if (!fxReady) return;

        if (didBootstrapRef.current === liveCacheKey) return;

        let cancelled = false;

        const bootstrapDashboard = async () => {
            try {
                const cacheResult = await getDashboardCacheFromBackend();

                if (cancelled) return;

                didBootstrapRef.current = liveCacheKey;

                if (cacheResult?.found && cacheResult.payload) {
                    shouldPostCacheRef.current = false;
                    isManualRefreshRef.current = false;

                    applyDashboardCachePayload(cacheResult.payload);

                    const normalizeRefreshTimestamp = (value: any): number | null => {
                        if (!value) return null;

                        const numeric = Number(value);
                        if (Number.isFinite(numeric) && numeric > 0) {
                            return numeric;
                        }

                        const parsed = new Date(value).getTime();
                        return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
                    };

                    const payloadLastRefreshAt = normalizeRefreshTimestamp(
                        cacheResult.payload?.lastRefreshAt
                    );

                    const backendUpdatedAt = normalizeRefreshTimestamp(cacheResult.updatedAt);

                    const savedRefreshAt = localStorage.getItem(lastRefreshKey);
                    const savedTs = normalizeRefreshTimestamp(savedRefreshAt);

                    const finalRefreshAt =
                        payloadLastRefreshAt ??
                        backendUpdatedAt ??
                        savedTs ??
                        null;

                    setLastRefreshAt(finalRefreshAt);

                    if (finalRefreshAt) {
                        localStorage.setItem(lastRefreshKey, String(finalRefreshAt));
                    }

                    localStorage.setItem(
                        liveCacheKey,
                        JSON.stringify({
                            ...cacheResult.payload,
                            savedAt: Date.now(),
                        })
                    );

                    setDashboardBusy(false);
                    setShowDashboardStepLoader(false);
                    setStepProgress((prev) => ({
                        ...prev,
                        active: false,
                    }));

                    return;
                }

                const restoredFromLocal = restoreLiveCacheFromLocalStorage();

                if (restoredFromLocal) {
                    shouldPostCacheRef.current = false;
                    isManualRefreshRef.current = false;

                    setDashboardBusy(false);
                    setShowDashboardStepLoader(false);
                    setStepProgress((prev) => ({
                        ...prev,
                        active: false,
                    }));

                    return;
                }

                await runDashboardLoadWithSteps();

                if (cancelled) return;

                triggerCachePost();
            } catch (err) {
                console.error("Dashboard bootstrap failed:", err);

                didBootstrapRef.current = null;

                resetStepState();
                setDashboardBusy(false);
                setShowDashboardStepLoader(false);
            }
        };

        bootstrapDashboard();

        return () => {
            cancelled = true;
        };
    }, [
        fxReady,
        liveCacheKey,
        lastRefreshKey,
        getDashboardCacheFromBackend,
        applyDashboardCachePayload,
        restoreLiveCacheFromLocalStorage,
        runDashboardLoadWithSteps,
        triggerCachePost,
    ]);

    /* ===================== AMAZON DERIVED DATA ===================== */
    const totals = data?.totals || null;
    const derived = data?.derived_totals || null;

    const globalMergedCurrentUnits = toNumberSafe(totals?.quantity);
    const globalMergedCurrentGross = toNumberSafe(totals?.gross_sales);
    const globalMergedCurrentNet = toNumberSafe(derived?.net_sales);
    const globalMergedCurrentAsp = toNumberSafe(derived?.asp);

    const globalMergedPrevTotals = data?.previous_period?.totals || {};

    const globalMergedPrevUnits = toNumberSafe(globalMergedPrevTotals?.quantity);
    const globalMergedPrevGross = toNumberSafe(globalMergedPrevTotals?.gross_sales);
    const globalMergedPrevNet = toNumberSafe(globalMergedPrevTotals?.net_sales);
    const globalMergedPrevAsp = toNumberSafe(globalMergedPrevTotals?.asp);

    const globalMergedCostOfAds = toNumberSafe(biAlignedTotals?.total_current_advertising);
    const globalMergedPrevCostOfAds = toNumberSafe(biAlignedTotals?.total_previous_advertising);

    const globalMergedTacos =
        globalMergedCurrentNet > 0 ? (globalMergedCostOfAds / globalMergedCurrentNet) * 100 : 0;

    const globalMergedPrevTacos =
        globalMergedPrevNet > 0 ? (globalMergedPrevCostOfAds / globalMergedPrevNet) * 100 : 0;

    const globalMergedCm2Profit = toNumberSafe(
        biAlignedTotals?.total_current_profit_cm2 ??
        biAlignedTotals?.total_current_profit
    );

    const globalMergedPrevCm2Profit = toNumberSafe(
        biAlignedTotals?.total_previous_profit_cm2 ??
        biAlignedTotals?.total_previous_profit
    );

    const globalMergedCm2Pct =
        globalMergedCurrentNet > 0 ? (globalMergedCm2Profit / globalMergedCurrentNet) * 100 : 0;

    const globalMergedPrevCm2Pct =
        globalMergedPrevNet > 0 ? (globalMergedPrevCm2Profit / globalMergedPrevNet) * 100 : 0;

    const uk = useMemo(() => {
        const netSalesGBP = derived?.net_sales != null ? toNumberSafe(derived.net_sales) : null;
        const aspGBP = derived?.asp != null ? toNumberSafe(derived.asp) : null;
        const cm2ProfitGBP =
            derived?.cm2_profit != null ? toNumberSafe(derived.cm2_profit) : null;
        const cogsGBP = totals?.cogs != null ? toNumberSafe(totals.cogs) : 0;
        const fbaFeesGBP = totals?.fba_fees != null ? toNumberSafe(totals.fba_fees) : 0;
        const sellingFeesGBP = totals?.selling_fees != null ? toNumberSafe(totals.selling_fees) : 0;
        const amazonFeesGBP =
            derived?.amazon_fees != null
                ? toNumberSafe(derived.amazon_fees)
                : (fbaFeesGBP + sellingFeesGBP);
        const profitGBP = derived?.profit != null ? toNumberSafe(derived.profit) : null;
        const unitsGBP = totals?.quantity != null ? toNumberSafe(totals.quantity) : null;

        let profitPctGBP: number | null = null;
        if (cm2ProfitGBP !== null && netSalesGBP && netSalesGBP !== 0) {
            profitPctGBP = (cm2ProfitGBP / netSalesGBP) * 100;
        }

        const grossSalesGBP =
            totals?.gross_sales != null ? toNumberSafe(totals.gross_sales) : null;
        const advertisingGBP =
            derived?.advertising_fees != null ? toNumberSafe(derived.advertising_fees) : 0;
        const platformFeeGBP =
            derived?.platform_fee != null ? toNumberSafe(derived.platform_fee) : 0;

        return {
            unitsGBP,
            netSalesGBP,
            grossSalesGBP,
            aspGBP,
            profitGBP,
            cm2ProfitGBP,
            profitPctGBP,
            cogsGBP,
            amazonFeesGBP,
            advertisingGBP,
            platformFeeGBP,
        };
    }, [totals, derived]);

    // const safeDeltaPct = (current: number, previous: number) => {
    //     const c = Number(current) || 0;
    //     const p = Number(previous) || 0;

    //     if (p === 0) return null; // avoid wrong spikes
    //     return ((c - p) / Math.abs(p)) * 100;
    // };

    const prevTotals = data?.previous_period?.totals || null;

    const prev = useMemo(() => {
        return {
            quantity: toNumberSafe(prevTotals?.quantity ?? 0),
            netSales: toNumberSafe(prevTotals?.net_sales ?? 0),
            grossSales: toNumberSafe(prevTotals?.gross_sales ?? 0),
            asp: toNumberSafe(prevTotals?.asp ?? 0),
            profit: toNumberSafe(prevTotals?.profit ?? 0),
            cm2Profit: toNumberSafe(prevTotals?.cm2_profit ?? 0),
            profitPct: toNumberSafe(prevTotals?.profit_percentage ?? 0),
        };
    }, [prevTotals]);

    const amazonCurrAdsDisp = useMemo(() => {
        const ads = toNumberSafe(derived?.advertising_fees ?? 0);
        return convertToDisplayCurrency(ads, amazonDataCurrency);
    }, [derived?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

    const amazonPrevAdsDisp = useMemo(() => {
        const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
        return convertToDisplayCurrency(ads, amazonDataCurrency);
    }, [data?.previous_period?.totals?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

    const amazonAdsDeltaPct = useMemo(() => {
        return safeDeltaPct(amazonCurrAdsDisp, amazonPrevAdsDisp);
    }, [amazonCurrAdsDisp, amazonPrevAdsDisp]);

    const amazonCurrRoasPct = useMemo(() => {
        const sales = toNumberSafe(derived?.net_sales ?? 0);
        const ads = toNumberSafe(derived?.advertising_fees ?? 0);
        return sales > 0 ? (ads / sales) * 100 : 0;
    }, [derived?.net_sales, derived?.advertising_fees]);

    const amazonPrevRoasPct = useMemo(() => {
        const sales = toNumberSafe(data?.previous_period?.totals?.net_sales ?? 0);
        const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
        return sales > 0 ? (ads / sales) * 100 : 0;
    }, [data?.previous_period?.totals?.net_sales, data?.previous_period?.totals?.advertising_fees]);

    const curr = useMemo(() => {
        return {
            quantity: toNumberSafe(totals?.quantity ?? 0),
            netSales: toNumberSafe(derived?.net_sales ?? 0),
            asp: toNumberSafe(derived?.asp ?? 0),
            profit: toNumberSafe(derived?.profit ?? 0),
            profitPct: toNumberSafe(uk.profitPctGBP ?? 0),
        };
    }, [totals, derived, uk.profitPctGBP]);

    const deltas = useMemo(() => {
        return {
            quantityPct: safeDeltaPct(curr.quantity, prev.quantity),
            netSalesPct: safeDeltaPct(curr.netSales, prev.netSales),
            aspPct: safeDeltaPct(curr.asp, prev.asp),
            profitPct: safeDeltaPct(curr.profit, prev.profit),
            profitMarginPctPts:
                curr.profitPct != null && prev.profitPct != null
                    ? Number(curr.profitPct) - Number(prev.profitPct)
                    : null,
        };
    }, [curr, prev]);

    const deltaPctPoints = (currentPct: number, previousPct: number) => {
        const c = Number(currentPct) || 0;
        const p = Number(previousPct) || 0;
        return c - p;
    };

    const deltaPctAbs = (currentPct: number, previousPct: number) => {
        const c = Number(currentPct) || 0;
        const p = Number(previousPct) || 0;
        return c - p;
    };

    /* ===================== ✅ RANGE KPIs FOR CARDS (FROM SAME BI DATA AS GRAPH) ===================== */
    useEffect(() => {
        const pts = (biDailySeriesHome?.current_mtd || []).filter(isCurrentPointAllowed);

        if (!pts.length) {
            setTodaySalesRaw(0);
            return;
        }

        const exact = pts.find(
            (p) => Number(p.date?.slice(8, 10)) === dashboardAllowedDay
        );

        if (exact?.net_sales != null) {
            setTodaySalesRaw(Number(exact.net_sales) || 0);
            return;
        }

        const latest = [...pts].sort((a, b) => a.date.localeCompare(b.date)).at(-1);
        setTodaySalesRaw(Number(latest?.net_sales) || 0);
    }, [biDailySeriesHome, isCurrentPointAllowed, dashboardAllowedDay]);


    const biCardKpis = useMemo(() => {
        const currAll = (biDailySeriesHome?.current_mtd || []).filter(isCurrentPointAllowed);
        const prevAll = (biDailySeriesHome?.previous || []).filter(isPreviousPointAllowed);

        const currPts = sliceByDayRange(currAll, selectedStartDay, selectedEndDay);
        const prevPts = sliceByDayRange(prevAll, selectedStartDay, selectedEndDay);

        const sum = (arr: DailyPoint[], key: keyof DailyPoint) =>
            arr.reduce((a, d) => a + (Number(d[key]) || 0), 0);

        const curr = {
            units: sum(currPts, "quantity"),
            netSales: sum(currPts, "net_sales"),
            grossSales: sum(currPts, "gross_sales"),
            profit: sum(currPts, "profit"),
            cm2Profit: sum(currPts, "cm2_profit"),
        };

        const prev = {
            units: sum(prevPts, "quantity"),
            netSales: sum(prevPts, "net_sales"),
            grossSales: sum(prevPts, "gross_sales"),
            profit: sum(prevPts, "profit"),
            cm2Profit: sum(prevPts, "cm2_profit"),
        };

        const currAsp = curr.units > 0 ? curr.netSales / curr.units : 0;
        const prevAsp = prev.units > 0 ? prev.netSales / prev.units : 0;

        const currProfitPct = curr.netSales !== 0 ? (curr.cm2Profit / curr.netSales) * 100 : 0;
        const prevProfitPct = prev.netSales !== 0 ? (prev.cm2Profit / prev.netSales) * 100 : 0;

        const deltaPct = (c: number, p: number) => (p ? ((c - p) / p) * 100 : null);

        return {
            curr: { ...curr, asp: currAsp, profitPct: currProfitPct },
            prev: { ...prev, asp: prevAsp, profitPct: prevProfitPct },
            deltas: {
                units: deltaPct(curr.units, prev.units),
                netSales: deltaPct(curr.netSales, prev.netSales),
                grossSales: deltaPct(curr.grossSales, prev.grossSales),
                asp: deltaPct(currAsp, prevAsp),
                profit: deltaPct(curr.profit, prev.profit),
                profitPct: safeDeltaPct(currProfitPct, prevProfitPct),

            },
        };
    }, [biDailySeriesHome, selectedStartDay, selectedEndDay, isCurrentPointAllowed,
        isPreviousPointAllowed,]);

    const rangeActive = selectedStartDay != null && selectedEndDay != null;
    const useBiCm2 = showLiveBI && rangeActive;
    const cm2Ready = useBiCm2 && !biLoading && !!biAlignedTotals;
    const biCardsReady = rangeActive && !biLoading && !!biAlignedTotals;

    const cachedRangeTotals = useMemo(() => {
        const currAll = (biDailySeriesHome?.current_mtd || []).filter(isCurrentPointAllowed);
        const prevAll = (biDailySeriesHome?.previous || []).filter(isPreviousPointAllowed);

        const currPts = sliceByDayRange(currAll, selectedStartDay, selectedEndDay);
        const prevPts = sliceByDayRange(prevAll, selectedStartDay, selectedEndDay);

        const sum = (arr: DailyPoint[], key: keyof DailyPoint) =>
            arr.reduce((acc, row) => acc + Number(row?.[key] ?? 0), 0);

        const currentProfit = sum(currPts, "profit");
        const previousProfit = sum(prevPts, "profit");

        const currentAds = sum(currPts as any[], "advertising" as keyof DailyPoint);
        const previousAds = sum(prevPts as any[], "advertising" as keyof DailyPoint);

        const currentPlatformFees = sum(currPts as any[], "platform_fee" as keyof DailyPoint);
        const previousPlatformFees = sum(prevPts as any[], "platform_fee" as keyof DailyPoint);

        const currentNetSales = sum(currPts, "net_sales");
        const previousNetSales = sum(prevPts, "net_sales");

        const currentCm2Profit = currentProfit - currentAds - currentPlatformFees;
        const previousCm2Profit = previousProfit - previousAds - previousPlatformFees;

        const currentCm2Pct =
            currentNetSales > 0 ? (currentCm2Profit / currentNetSales) * 100 : 0;

        const previousCm2Pct =
            previousNetSales > 0 ? (previousCm2Profit / previousNetSales) * 100 : 0;

        return {
            currentProfit,
            previousProfit,
            currentAds,
            previousAds,
            currentPlatformFees,
            previousPlatformFees,
            currentNetSales,
            previousNetSales,
            currentCm2Profit,
            previousCm2Profit,
            currentCm2Pct,
            previousCm2Pct,
        };
    }, [
        biDailySeriesHome,
        selectedStartDay,
        selectedEndDay,
        isCurrentPointAllowed,
        isPreviousPointAllowed,
    ]);

    const biAlignedTotalsHome = useMemo(() => {
        if (!biCardsReady || !biAlignedTotals) return null;

        const conv = (v?: number) => {
            const n = Number(v || 0);

            // Global merged payload is already in USD
            if (platform === "global") {
                return convertToDisplayCurrency(n, "USD");
            }

            return convertToDisplayCurrency(n, biSourceCurrency);
        };

        return {
            total_current_net_sales: conv(biAlignedTotals.total_current_net_sales),
            total_previous_net_sales: conv(biAlignedTotals.total_previous_net_sales),
            total_previous_net_sales_full_month: conv(biAlignedTotals.total_previous_net_sales_full_month),

            total_current_advertising: conv(biAlignedTotals.total_current_advertising),
            total_previous_advertising: conv(biAlignedTotals.total_previous_advertising),

            total_current_platform_fees: conv(biAlignedTotals.total_current_platform_fees),
            total_previous_platform_fees: conv(biAlignedTotals.total_previous_platform_fees),

            total_current_profit: conv(biAlignedTotals.total_current_profit),
            total_previous_profit: conv(biAlignedTotals.total_previous_profit),

            total_current_rembursement_fee: conv(biAlignedTotals.total_current_rembursement_fee),
            total_previous_rembursement_fee: conv(biAlignedTotals.total_previous_rembursement_fee),
        };
    }, [biCardsReady, biAlignedTotals, convertToDisplayCurrency, biSourceCurrency, platform]);

    const globalUseBi = platform === "global" && showLiveBI && rangeActive;
    const globalCm2Ready = globalUseBi && !biLoading && !!biAlignedTotals;

    const shopifyNotConnected =
        !shopifyStore?.shop_name ||
        !shopifyStore?.access_token ||
        (shopifyError &&
            (shopifyError.toLowerCase().includes("shopify store not connected") ||
                shopifyError.toLowerCase().includes("no token")));

    const shopifyIntegrated = !shopifyNotConnected && !!shopify;

    const amazonIntegrated =
        Array.isArray(amazonConnections) && amazonConnections.length > 0;

    const noIntegrations = !amazonIntegrated && !shopifyIntegrated;

    /* ===================== GLOBAL / FX COMBINED (BASE USD DATA) ===================== */
    const amazonUK_USD = useMemo(() => {
        const amazonUK_GBP = toNumberSafe(uk.netSalesGBP);
        return amazonUK_GBP * gbpToUsd;
    }, [uk.netSalesGBP, gbpToUsd]);

    const combinedUSD = useMemo(() => {
        const aUK = amazonUK_USD;
        const shopifyUSD = toNumberSafe(shopifyDeriv?.netSales) * inrToUsd;
        return aUK + shopifyUSD;
    }, [amazonUK_USD, shopifyDeriv?.netSales, inrToUsd]);

    const prevAmazonMtdSalesGBP = toNumberSafe(data?.previous_period?.totals?.net_sales ?? 0);
    const prevAmazonMtdSalesUSD = prevAmazonMtdSalesGBP * gbpToUsd;

    const prevAmazonUKTotalUSD = useMemo(() => {
        const prevTotalGBP = toNumberSafe(data?.previous_month_total_net_sales?.total);
        if (prevTotalGBP > 0) return prevTotalGBP * gbpToUsd;
        const { todayDay, daysInPrevMonth } = getRegionDayInfo(activeDateRegion);
        if (!todayDay || !daysInPrevMonth) return 0;
        return (prevAmazonMtdSalesUSD * daysInPrevMonth) / todayDay;
    }, [data?.previous_month_total_net_sales?.total, gbpToUsd, prevAmazonMtdSalesUSD]);


    const amazonUK_Gross_USD = useMemo(() => {
        const grossGBP = toNumberSafe(totals?.gross_sales);
        return grossGBP * gbpToUsd;
    }, [totals?.gross_sales, gbpToUsd]);



    const combinedGrossUSD = useMemo(() => {
        const shopifyUSD = toNumberSafe(shopifyDeriv?.netSales) * inrToUsd;
        return amazonUK_Gross_USD + shopifyUSD;
    }, [amazonUK_Gross_USD, shopifyDeriv?.netSales, inrToUsd]);

    const prevAmazonGrossUSD = useMemo(() => {
        return toNumberSafe(prev.grossSales) * gbpToUsd;
    }, [prev.grossSales, gbpToUsd]);

    const prevGlobalGrossUSD = useMemo(() => {
        const prevShopifyUSD = toNumberSafe(shopifyPrevDeriv?.netSales) * inrToUsd;
        return prevAmazonGrossUSD + prevShopifyUSD;
    }, [prevAmazonGrossUSD, shopifyPrevDeriv?.netSales, inrToUsd]);


    const fallbackTargetUSD = useMemo(() => {
        return prevAmazonUKTotalUSD > 0 ? prevAmazonUKTotalUSD : 0;
    }, [prevAmazonUKTotalUSD]);


    const prevShopifyTotalUSD = useMemo(() => {
        const prevINRTotal = toNumberSafe(shopifyPrevDeriv?.netSales);
        return prevINRTotal * inrToUsd;
    }, [shopifyPrevDeriv, inrToUsd]);

    const globalPrevTotalUSD = prevShopifyTotalUSD + prevAmazonUKTotalUSD;

    const chooseLastMonthTotal = (manualUSD: number, computedUSD: number) =>
        USE_MANUAL_LAST_MONTH && manualUSD > 0 ? manualUSD : computedUSD;

    const prorateToDate = (lastMonthTotalUSD: number) => {
        const { todayDay, daysInPrevMonth } = getRegionDayInfo(activeDateRegion);
        return daysInPrevMonth > 0 ? (lastMonthTotalUSD * todayDay) / daysInPrevMonth : 0;
    };

    // ---------- NET SALES (DISPLAY CURRENCY) ----------

    const amazonCurrNetDisp = useMemo(
        () => convertToDisplayCurrency(uk.netSalesGBP ?? 0, amazonDataCurrency),
        [uk.netSalesGBP, amazonDataCurrency, convertToDisplayCurrency]
    );

    const amazonPrevNetDisp = useMemo(
        () => convertToDisplayCurrency(prev.netSales ?? 0, amazonDataCurrency),
        [prev.netSales, convertToDisplayCurrency]
    );

    const globalCurrNetDisp = useMemo(() => {
        const amazon = amazonCurrNetDisp;
        const shopify = convertToDisplayCurrency(
            shopifyDeriv?.netSales ?? 0,
            "INR"
        );
        return amazon + shopify;
    }, [amazonCurrNetDisp, shopifyDeriv?.netSales, convertToDisplayCurrency]);

    const globalPrevNetDisp = useMemo(() => {
        const amazon = amazonPrevNetDisp;
        const shopify = convertToDisplayCurrency(
            shopifyPrevDeriv?.netSales ?? 0,
            "INR"
        );
        return amazon + shopify;
    }, [amazonPrevNetDisp, shopifyPrevDeriv?.netSales, convertToDisplayCurrency]);



    const regions = useMemo(() => {
        const ukTargetGBP = toNumberSafe(targetSummaries.uk?.target_sales ?? 0);
        const usTargetUSD = toNumberSafe(targetSummaries.us?.target_sales ?? 0);

        const globalTargetFromRows = ukTargetGBP * gbpToUsd + usTargetUSD;

        const userMonthlyTargetForRegion =
            platform === "global"
                ? globalTargetFromRows
                : toNumberSafe(
                    targetSummaries[targetSummaryCountry as keyof typeof targetSummaries]?.target_sales ?? 0
                );

        const globalPrevFullMonthSales =
            globalPrevFullMonthNetSalesDisp > 0
                ? globalPrevFullMonthNetSalesDisp
                : globalPrevNetDisp;

        const globalTarget =
            userMonthlyTargetForRegion > 0 ? userMonthlyTargetForRegion : globalPrevFullMonthSales;

        const global: RegionMetrics = {
            mtdUSD: globalCurrNetDisp,
            lastMonthToDateUSD: globalPrevNetDisp,
            lastMonthTotalUSD: globalPrevFullMonthSales,
            targetUSD: globalTarget,
            decTargetUSD: globalTarget,
        };

        const ukPrevFullMonthSales =
            prevFullMonthNetSalesDisp > 0
                ? prevFullMonthNetSalesDisp
                : amazonPrevNetDisp;

        const ukTarget =
            userMonthlyTargetForRegion > 0 ? userMonthlyTargetForRegion : ukPrevFullMonthSales;

        const ukRegion: RegionMetrics = {
            mtdUSD: amazonCurrNetDisp,
            lastMonthToDateUSD: amazonPrevNetDisp, // prev MTD
            lastMonthTotalUSD: ukPrevFullMonthSales, // ✅ Dec/prev full-month SALES (as-is)
            targetUSD: ukTarget, // ✅ Target from userData
            decTargetUSD: ukTarget,
        };

        const usLastMonthTotal = chooseLastMonthTotal(MANUAL_LAST_MONTH_USD_US, 0);
        const usRegion: RegionMetrics = {
            mtdUSD: 0,
            lastMonthToDateUSD: prorateToDate(usLastMonthTotal),
            lastMonthTotalUSD: usLastMonthTotal,
            targetUSD: usLastMonthTotal,
            decTargetUSD: usLastMonthTotal,
        };

        const caLastMonthTotal = chooseLastMonthTotal(MANUAL_LAST_MONTH_USD_CA, 0);
        const caRegion: RegionMetrics = {
            mtdUSD: 0,
            lastMonthToDateUSD: prorateToDate(caLastMonthTotal),
            lastMonthTotalUSD: caLastMonthTotal,
            targetUSD: caLastMonthTotal,
            decTargetUSD: caLastMonthTotal,
        };

        return {
            Global: global,
            UK: ukRegion,
            US: usRegion,
            CA: caRegion,
        } as Record<RegionKey, RegionMetrics>;
    }, [
        globalCurrNetDisp,
        globalPrevNetDisp,
        amazonCurrNetDisp,
        amazonPrevNetDisp,
        prevFullMonthNetSalesDisp,
        globalPrevFullMonthNetSalesDisp,
        targetSummaries,
        gbpToUsd,
        platform,
        targetSummaryCountry,
        convertToDisplayCurrency,
        chooseLastMonthTotal,
        prorateToDate,
    ]);




    const PreviewLockedSection = ({
        enabled,
        children,
        title,
        description,
        buttonText,
        onAction,
    }: {
        enabled: boolean;
        children: React.ReactNode;
        title?: string;
        description?: string;
        buttonText?: string;
        onAction?: () => void;
    }) => {
        return (
            <div className="relative w-full">
                <div
                    className={
                        enabled
                            ? "pointer-events-none select-none opacity-45 transition-all duration-300"
                            : "opacity-100 transition-all duration-300"
                    }
                >
                    {children}
                </div>

                {enabled && (
                    <>
                        <div className="absolute inset-0 z-10 rounded-xl bg-white/45" />

                        <div className="absolute inset-0 z-20 pointer-events-none">
                            <div className="sticky top-[18vh] sm:top-[20vh] lg:top-[22vh] 2xl:top-[24vh] flex justify-center px-4">
                                <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 text-center">
                                    <div className="mb-4 flex justify-center">
                                        <div className="flex h-16 w-16 items-center justify-center rounded-full  bg-[#37455F]">
                                            <IoMdLock className="text-3xl text-[#F8EDCE]" />
                                        </div>
                                    </div>

                                    <h3 className="text-lg font-semibold text-[#414042]">
                                        {title}
                                    </h3>

                                    <p className="mt-2 text-sm text-gray-600 leading-6">
                                        {description}
                                    </p>

                                    <button
                                        onClick={onAction}
                                        className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
                                    >
                                        {buttonText}
                                    </button>

                                    {/* <p className="mt-3 text-xs text-gray-500">
                                        Demo data is shown for preview only.
                                    </p> */}
                                </div>
                            </div>
                        </div>
                    </>
                )}
            </div>
        );
    };

    const DashboardLoaderModal = React.memo(function DashboardLoaderModal({
        pageLoading,
        shouldShowDummyUi,
        currentStep,
        completedSteps,
        dashboardSteps,
        stepProgress,
        loadingStartedAt,
        estimatedSecondsMap,
    }: {
        pageLoading: boolean;
        shouldShowDummyUi: boolean;
        currentStep: number;
        completedSteps: Set<number>;
        dashboardSteps: { num: number; label: string }[];
        stepProgress: {
            active: boolean;
            label: string;
            percentage: number;
            detail?: string;
        };
        loadingStartedAt: number | null;
        estimatedSecondsMap: Record<number, number>;
    }) {
        const [timerNow, setTimerNow] = useState(Date.now());

        useEffect(() => {
            if (!pageLoading || !stepProgress.active || !loadingStartedAt) return;

            const interval = setInterval(() => {
                setTimerNow(Date.now());
            }, 1000);

            return () => clearInterval(interval);
        }, [pageLoading, stepProgress.active, loadingStartedAt]);

        const TOTAL_ESTIMATED_SECONDS = useMemo(() => {
            return dashboardSteps.reduce((sum, step) => {
                return sum + (estimatedSecondsMap[step.num] ?? 20);
            }, 0);
        }, [dashboardSteps, estimatedSecondsMap]);

        const estimatedTime = useMemo(() => {
            if (!stepProgress.active || !loadingStartedAt) return "00:00";

            const elapsedSec = Math.floor((timerNow - loadingStartedAt) / 1000);
            const remainingSec = Math.max(TOTAL_ESTIMATED_SECONDS - elapsedSec, 0);

            const mm = String(Math.floor(remainingSec / 60)).padStart(2, "0");
            const ss = String(remainingSec % 60).padStart(2, "0");

            return `${mm}:${ss}`;
        }, [timerNow, loadingStartedAt, stepProgress.active, TOTAL_ESTIMATED_SECONDS]);

        const completedLineWidth = useMemo(() => {
            if (!completedSteps.size) return "0%";

            const maxCompleted = Math.max(...Array.from(completedSteps));
            const denominator = Math.max(dashboardSteps.length - 1, 1);

            const pct =
                maxCompleted > 1
                    ? ((Math.min(
                        maxCompleted,
                        dashboardSteps[dashboardSteps.length - 1].num
                    ) - 1) /
                        denominator) *
                    100
                    : 0;

            return `${pct}%`;
        }, [completedSteps, dashboardSteps]);

        if (shouldShowDummyUi || !pageLoading) return null;

        return (
            <div className="fixed inset-0 z-[999] flex items-center justify-center px-4 pointer-events-none">
                <div className="absolute inset-0 bg-white/40" />

                <div className="relative pointer-events-auto w-full max-w-xl rounded-2xl border border-slate-200 bg-white p-5 md:p-6 shadow-md">
                    <div className="flex items-center justify-between mb-4">
                        <div className="flex items-center gap-3">
                            <div className="w-8 h-8 rounded-lg bg-[#E8F5F0] flex items-center justify-center flex-shrink-0">
                                <svg
                                    width="15"
                                    height="15"
                                    viewBox="0 0 24 24"
                                    fill="none"
                                    stroke="#5EA68E"
                                    strokeWidth="2.5"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                >
                                    <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
                                </svg>
                            </div>

                            <div>
                                <p className="text-lg font-semibold text-[#37455F] leading-tight">
                                    Syncing dashboard data
                                </p>
                            </div>
                        </div>

                        <span className="text-sm font-bold text-[#5EA68E] tabular-nums">
                            {Math.round(stepProgress.percentage)}%
                        </span>
                    </div>

                    <div className="h-[7px] w-full bg-slate-100 rounded-full overflow-hidden mb-6">
                        <div
                            className="h-full rounded-full transition-all duration-500 ease-in-out"
                            style={{
                                width: `${stepProgress.percentage}%`,
                                background:
                                    "linear-gradient(90deg, #5EA68E 0%, #37455F 100%)",
                            }}
                        />
                    </div>

                    <div className="relative flex items-start justify-between">
                        <div
                            className="absolute top-4 z-0 h-px bg-slate-200"
                            style={{ left: "calc(12.5% + 10px)", right: "calc(12.5% + 10px)" }}
                        >
                            {completedSteps.size > 0 && (() => {
                                const maxCompleted = Math.max(...Array.from(completedSteps));
                                const denominator = Math.max(dashboardSteps.length - 1, 1);
                                const pct =
                                    maxCompleted > 1
                                        ? ((Math.min(
                                            maxCompleted,
                                            dashboardSteps[dashboardSteps.length - 1].num
                                        ) - 1) /
                                            denominator) *
                                        100
                                        : 0;

                                return (
                                    <div
                                        className="h-full bg-[#5EA68E] transition-all duration-500"
                                        style={{ width: `${pct}%` }}
                                    />
                                );
                            })()}
                        </div>

                        {dashboardSteps.map((step) => {
                            const isCompleted = completedSteps.has(step.num);
                            const isActive = currentStep === step.num;

                            return (
                                <div
                                    key={step.num}
                                    className="flex flex-col items-center flex-1 relative z-10 gap-2"
                                >
                                    <div
                                        className={[
                                            "w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold border-2 transition-all duration-300",
                                            isCompleted
                                                ? "border-[#5EA68E] bg-[#5EA68E] text-white"
                                                : isActive
                                                    ? "border-[#5EA68E] bg-[#E8F5F0] text-[#37455F]"
                                                    : "border-slate-200 bg-white text-slate-400",
                                        ].join(" ")}
                                    >
                                        {isCompleted ? (
                                            <svg
                                                width="12"
                                                height="12"
                                                viewBox="0 0 24 24"
                                                fill="none"
                                                stroke="white"
                                                strokeWidth="3.5"
                                                strokeLinecap="round"
                                                strokeLinejoin="round"
                                            >
                                                <polyline points="20 6 9 17 4 12" />
                                            </svg>
                                        ) : isActive ? (
                                            <span
                                                className="w-3 h-3 rounded-full border-2 border-[#b8ddd4] border-t-[#5EA68E] animate-spin"
                                                style={{ display: "inline-block" }}
                                            />
                                        ) : (
                                            <span>{step.num}</span>
                                        )}
                                    </div>

                                    <p
                                        className={[
                                            "text-center text-[12px] sm:text-sm font-medium leading-tight",
                                            isCompleted || isActive
                                                ? "text-[#37455F]"
                                                : "text-slate-400",
                                        ].join(" ")}
                                    >
                                        {step.label}
                                    </p>

                                    <span
                                        className={[
                                            "text-[10px] sm:text-xs px-2 py-0.5 rounded-full font-medium",
                                            isCompleted
                                                ? "bg-[#E8F5F0] text-[#5EA68E]"
                                                : isActive
                                                    ? "bg-[#E8F5F0] text-[#5EA68E] animate-pulse"
                                                    : "bg-slate-100 text-slate-400",
                                        ].join(" ")}
                                    >
                                        {isCompleted ? "✓ Done" : isActive ? "In progress" : "Pending"}
                                    </span>
                                </div>
                            );
                        })}
                    </div>

                    <div className="mt-5 pt-4 border-t border-slate-100 grid grid-cols-3 items-center">
                        <p className="text-xs text-slate-400 truncate justify-self-start">
                            {stepProgress.detail || "Initialising dashboard…"}
                        </p>

                        <div className="justify-self-center flex items-center gap-1 px-2 py-1 bg-slate-100 rounded-full mx-3">
                            <span className="text-xs text-slate-400">Estimated Time:</span>
                            <span className="text-xs font-medium text-slate-600 tabular-nums min-w-[42px] text-right">
                                {estimatedTime}
                            </span>
                        </div>

                        <span className="text-xs text-slate-400 shrink-0 justify-self-end">
                            Step {Math.min(currentStep, dashboardSteps.length)} of {dashboardSteps.length}
                        </span>
                    </div>
                </div>
            </div>
        );
    });

    const amazonTabs = useMemo<RegionKey[]>(() => {
        const tabs: RegionKey[] = [];
        (["UK", "US", "CA"] as RegionKey[]).forEach((key) => {
            const r = regions[key];
            if (!r) return;
            if (r.mtdUSD || r.lastMonthToDateUSD || r.lastMonthTotalUSD || r.targetUSD) tabs.push(key);
        });
        return tabs;
    }, [regions]);

    useEffect(() => {
        if (amazonTabs.length && !amazonTabs.includes(amazonRegion)) setAmazonRegion(amazonTabs[0]);
    }, [amazonTabs, amazonRegion]);

    const graphRegions = useMemo<RegionKey[]>(() => {
        const list: RegionKey[] = ["Global"];
        (["UK", "US", "CA"] as RegionKey[]).forEach((key) => {
            const r = regions[key];
            if (!r) return;
            if (r.mtdUSD || r.lastMonthToDateUSD || r.lastMonthTotalUSD || r.targetUSD) list.push(key);
        });
        return list;
    }, [regions]);

    useEffect(() => {
        if (!graphRegions.includes(graphRegion)) setGraphRegion("Global");
    }, [graphRegions, graphRegion]);

    const onlyAmazon = amazonIntegrated && !shopifyIntegrated;
    const onlyShopify = shopifyIntegrated && !amazonIntegrated;

    const monthlySkuwiseRows = useMemo<MonthlySkuwiseRow[]>(() => {
        const items =
            platform === "global"
                ? (data as any)?.skuwise_items_global ?? []
                : (data as any)?.skuwise_items ?? [];

        if (!Array.isArray(items)) return [];

        const body =
            platform === "global"
                ? items.filter((r: any) => {
                    const sku = String(r?.sku || "").toUpperCase();
                    const name = String(r?.product_name || "").trim().toLowerCase();

                    return (
                        sku !== "GRAND_TOTAL" &&
                        name !== "grand total" &&
                        name !== "total"
                    );
                })
                : items.filter((r: any) => r?.sku && r.sku !== "GRAND_TOTAL");

        const total = items.find((r: any) => {
            const sku = String(r?.sku || "").toUpperCase();
            const name = String(r?.product_name || "").trim().toLowerCase();

            return sku === "GRAND_TOTAL" || name === "grand total";
        });

        const mapRow = (r: any, idx?: number, isTotal = false): MonthlySkuwiseRow => {
            const tax = Number(r.net_taxes ?? r.tax ?? 0);
            const credits = Number(r.credits ?? r.net_credits ?? 0);

            const taxAndCredits = Number(
                r.other_transactions ??
                r.tax_and_credits ??
                tax + credits
            );

            return {
                sno: isTotal ? undefined : (idx ?? 0) + 1,
                sku: String(r.sku ?? ""),
                product_name: String(r.product_name ?? ""),
                ad_type: String(r.ad_type ?? r.adType ?? r.ad_types ?? r.adTypes ?? ""),

                quantity: Number(r.quantity ?? 0),

                return_quantity: Number(
                    r.return_quantity ??
                    r.returns_quantity ??
                    r.return_qty ??
                    0
                ),

                total_quantity: Number(
                    r.total_quantity ??
                    r.net_quantity ??
                    (
                        Number(r.quantity ?? 0) -
                        Number(r.return_quantity ?? r.returns_quantity ?? r.return_qty ?? 0)
                    )
                ),

                asp: Number(r.asp ?? 0),
                net_sales: Number(r.net_sales ?? 0),

                cogs: Number(r.cogs ?? 0),
                fba_fees: Number(r.fba_fees ?? 0),
                selling_fees: Number(r.selling_fees ?? 0),
                ads_spend: Number(r.ads_spend ?? 0),
                acos: Number(r.acos ?? 0),
                cm2_profit: Number(r.cm2_profit ?? 0),

                tax,
                credits,
                tax_and_credits: taxAndCredits,
                net_taxes: tax,
                other_transactions: taxAndCredits,

                cm1_profit_per: Number(r.cm1_profit_per ?? 0),
                cm1_profit_per_unit: Number(r.cm1_profit_per_unit ?? 0),
                cm2_profit_per: Number(r.cm2_profit_per ?? 0),
                cm2_profit_per_unit: Number(r.cm2_profit_per_unit ?? 0),
                profit: Number(r.profit ?? 0),

                platform_fee: Number(r.platform_fee ?? 0),
                platform_fee_inventory_storage: Number(r.platform_fee_inventory_storage ?? 0),
                lost_total: Number(r.lost_total ?? 0),
                other: Number(r.other ?? 0),

                product_spend: Number(
                    r.product_spend ??
                    r.sponsored_product ??
                    r.Sponsored_Product ??
                    0
                ),

                display_spend: Number(
                    r.display_spend ??
                    r.sponsored_display ??
                    r.Sponsored_Display ??
                    0
                ),

                brand_spend: Number(
                    r.brand_spend ??
                    r.display_brand ??
                    r.Display_brand ??
                    r.sponsored_brand ??
                    r.Sponsored_Brand ??
                    0
                ),
                dealsvouchar_ads: Number(r.dealsvouchar_ads ?? 0),
                platformfeenew: Number(r.platformfeenew ?? 0),

                isTotal,
            };
        };

        const mapped = body.map((r: any, idx: number) => mapRow(r, idx, false));
        if (total) mapped.push(mapRow(total, undefined, true));
        return mapped;
    }, [data, platform]);


    const PRODUCTWISE_MONEY_KEYS: ProductwiseMoneyKey[] = [
        "asp",
        "net_sales",
        "net_taxes",
        "other_transactions",
        "cogs",
        "fba_fees",
        "selling_fees",
        "ads_spend",
        "cm2_profit",
        "tax",
        "credits",
        "tax_and_credits",
        "cm1_profit_per_unit",
        "cm2_profit_per_unit",
        "profit",
        "platform_fee",
        "platform_fee_inventory_storage",
        "lost_total",
        "other",
        "product_spend",
        "display_spend",
        "brand_spend",
        "dealsvouchar_ads",
        "platformfeenew",
        "debt_payment",
        "disbursement",
    ];

    const normalizeProductwiseRow = (raw: any): MonthlySkuwiseRow => {
        const tax = toNumberSafe(raw?.tax ?? raw?.net_taxes);
        const credits = toNumberSafe(raw?.credits ?? raw?.net_credits);

        const taxAndCredits = toNumberSafe(
            raw?.tax_and_credits ??
            raw?.taxes_and_credits ??
            raw?.tex_and_credits ??
            tax + credits
        );

        // ✅ Add this here, before return
        const productSpend = toNumberSafe(
            raw?.product_spend ??
            raw?.sponsored_product ??
            raw?.Sponsored_Product ??
            0
        );

        const displaySpend = toNumberSafe(
            raw?.display_spend ??
            raw?.sponsored_display ??
            raw?.Sponsored_Display ??
            0
        );

        const brandSpend = toNumberSafe(
            raw?.brand_spend ??
            raw?.display_brand ??
            raw?.Display_brand ??
            raw?.sponsored_brand ??
            raw?.Sponsored_Brand ??
            0
        );

        return {
            ...raw,

            quantity: toNumberSafe(raw?.quantity),

            return_quantity: toNumberSafe(
                raw?.return_quantity ??
                raw?.returns_quantity ??
                raw?.return_qty
            ),

            total_quantity: toNumberSafe(
                raw?.total_quantity ??
                raw?.net_quantity ??
                (
                    toNumberSafe(raw?.quantity) -
                    toNumberSafe(raw?.return_quantity ?? raw?.returns_quantity ?? raw?.return_qty)
                )
            ),

            asp: toNumberSafe(raw?.asp),
            net_sales: toNumberSafe(raw?.net_sales),

            tax,
            credits,
            tax_and_credits: taxAndCredits,
            net_taxes: tax,
            other_transactions: taxAndCredits,

            cogs: toNumberSafe(raw?.cogs),
            fba_fees: toNumberSafe(raw?.fba_fees),
            selling_fees: toNumberSafe(raw?.selling_fees),

            // ✅ Use normalized values here
            product_spend: productSpend,
            display_spend: displaySpend,
            brand_spend: brandSpend,

            // ✅ Total Ads = SP + SD + SB
            ads_spend: productSpend + displaySpend + brandSpend,

            acos: toNumberSafe(raw?.acos),
            cm2_profit: toNumberSafe(raw?.cm2_profit),
            cm1_profit_per: toNumberSafe(raw?.cm1_profit_per),
            cm1_profit_per_unit: toNumberSafe(raw?.cm1_profit_per_unit),
            cm2_profit_per: toNumberSafe(raw?.cm2_profit_per),
            cm2_profit_per_unit: toNumberSafe(raw?.cm2_profit_per_unit),
            profit: toNumberSafe(raw?.profit),

            platform_fee: toNumberSafe(raw?.platform_fee),
            platform_fee_inventory_storage: toNumberSafe(raw?.platform_fee_inventory_storage),
            lost_total: toNumberSafe(raw?.lost_total),
            other: toNumberSafe(raw?.other),

            dealsvouchar_ads: toNumberSafe(raw?.dealsvouchar_ads),
            platformfeenew: toNumberSafe(raw?.platformfeenew),
        };
    };

    const convertProductwiseRowToUsd = (
        rawRow: MonthlySkuwiseRow,
        country: "uk" | "us"
    ): MonthlySkuwiseRow => {
        const row = normalizeProductwiseRow(rawRow);
        const fx = country === "uk" ? gbpToUsd : 1;

        const converted: MonthlySkuwiseRow = {
            ...row,
            quantity: toNumberSafe(row.quantity),
            return_quantity: toNumberSafe(row.return_quantity),
            total_quantity: toNumberSafe(
                row.total_quantity ??
                (toNumberSafe(row.quantity) - toNumberSafe(row.return_quantity))
            ),
        };

        PRODUCTWISE_MONEY_KEYS.forEach((key) => {
            (converted as any)[key] = toNumberSafe((row as any)[key]) * fx;
        });

        converted.tax = toNumberSafe(row.tax) * fx;
        converted.credits = toNumberSafe(row.credits) * fx;
        converted.net_taxes = toNumberSafe(row.net_taxes ?? row.tax) * fx;

        converted.tax_and_credits = toNumberSafe(row.tax_and_credits) * fx;
        converted.other_transactions = toNumberSafe(row.other_transactions) * fx;

        return converted;
    };

    const mergeProductwiseRowsGlobal = (
        ukRows: MonthlySkuwiseRow[] = [],
        usRows: MonthlySkuwiseRow[] = []
    ): MonthlySkuwiseRow[] => {
        const map = new Map<string, MonthlySkuwiseRow>();

        const addRows = (rows: MonthlySkuwiseRow[], country: "uk" | "us") => {
            rows
                .filter((r) => !r?.isTotal && !r?.isOthers)
                .forEach((raw) => {
                    const row = convertProductwiseRowToUsd(raw, country);
                    const key = String(row.sku || row.product_name || "").trim();

                    if (!key) return;

                    const existing = map.get(key);

                    if (!existing) {
                        map.set(key, {
                            ...row,
                            sno: map.size + 1,
                        });
                        return;
                    }

                    const quantity =
                        toNumberSafe(existing.quantity) + toNumberSafe(row.quantity);

                    const merged: MonthlySkuwiseRow = {
                        ...existing,
                        quantity,
                        product_name: existing.product_name || row.product_name,
                        ad_type: existing.ad_type || row.ad_type,
                    };

                    PRODUCTWISE_MONEY_KEYS.forEach((moneyKey) => {
                        if (moneyKey === "asp") return;

                        (merged as any)[moneyKey] =
                            toNumberSafe((existing as any)[moneyKey]) +
                            toNumberSafe((row as any)[moneyKey]);
                    });

                    const existingAsp = toNumberSafe(existing.asp);
                    const rowAsp = toNumberSafe(row.asp);
                    const aspValues = [existingAsp, rowAsp].filter((v) => v > 0);

                    merged.asp =
                        aspValues.length > 0
                            ? aspValues.reduce((s, v) => s + v, 0) / aspValues.length
                            : 0;

                    merged.tax =
                        toNumberSafe(existing.tax) + toNumberSafe(row.tax);

                    merged.credits =
                        toNumberSafe(existing.credits) + toNumberSafe(row.credits);
                    merged.net_taxes =
                        toNumberSafe(existing.net_taxes) + toNumberSafe(row.net_taxes);

                    merged.tax_and_credits =
                        toNumberSafe(existing.tax_and_credits) + toNumberSafe(row.tax_and_credits);

                    merged.other_transactions =
                        toNumberSafe(existing.other_transactions) + toNumberSafe(row.other_transactions);

                    merged.acos =
                        toNumberSafe(merged.net_sales) > 0
                            ? (Math.abs(toNumberSafe(merged.ads_spend)) /
                                Math.abs(toNumberSafe(merged.net_sales))) * 100
                            : 0;

                    merged.cm1_profit_per =
                        toNumberSafe(merged.net_sales) > 0
                            ? (toNumberSafe(merged.profit) / toNumberSafe(merged.net_sales)) * 100
                            : 0;

                    merged.cm1_profit_per_unit =
                        quantity > 0 ? toNumberSafe(merged.profit) / quantity : 0;

                    merged.cm2_profit_per =
                        toNumberSafe(merged.net_sales) > 0
                            ? (toNumberSafe(merged.cm2_profit) / toNumberSafe(merged.net_sales)) * 100
                            : 0;

                    merged.cm2_profit_per_unit =
                        quantity > 0 ? toNumberSafe(merged.cm2_profit) / quantity : 0;

                    map.set(key, merged);
                });
        };

        addRows(ukRows, "uk");
        addRows(usRows, "us");

        const rows = Array.from(map.values()).map((row, idx) => ({
            ...row,
            sno: idx + 1,
        }));

        const totalQuantity = rows.reduce((s, r) => s + toNumberSafe(r.quantity), 0);
        const totalNetSales = rows.reduce((s, r) => s + toNumberSafe(r.net_sales), 0);
        const totalProfit = rows.reduce((s, r) => s + toNumberSafe(r.profit), 0);
        const totalCm2Profit = rows.reduce((s, r) => s + toNumberSafe(r.cm2_profit), 0);
        const totalAdsSpend = rows.reduce((s, r) => s + toNumberSafe(r.ads_spend), 0);
        const totalTax = rows.reduce((s, r) => s + toNumberSafe(r.tax), 0);
        const totalCredits = rows.reduce((s, r) => s + toNumberSafe(r.credits), 0);
        const totalTaxAndCredits = rows.reduce(
            (s, r) => s + toNumberSafe(r.tax_and_credits),
            0
        );

        const totalAspValues = rows
            .map((r) => toNumberSafe(r.asp))
            .filter((v) => v > 0);

        const totalRow: MonthlySkuwiseRow = {
            sno: undefined,
            sku: "GRAND_TOTAL",
            product_name: "Grand Total",
            quantity: totalQuantity,

            return_quantity: rows.reduce(
                (s, r) => s + toNumberSafe(r.return_quantity),
                0
            ),

            total_quantity: rows.reduce(
                (s, r) =>
                    s +
                    toNumberSafe(
                        r.total_quantity ??
                        (toNumberSafe(r.quantity) - toNumberSafe(r.return_quantity))
                    ),
                0
            ),

            asp:
                totalAspValues.length > 0
                    ? totalAspValues.reduce((s, v) => s + v, 0) / totalAspValues.length
                    : 0,
            net_sales: totalNetSales,
            cogs: rows.reduce((s, r) => s + toNumberSafe(r.cogs), 0),
            fba_fees: rows.reduce((s, r) => s + toNumberSafe(r.fba_fees), 0),
            selling_fees: rows.reduce((s, r) => s + toNumberSafe(r.selling_fees), 0),
            ads_spend: totalAdsSpend,
            acos:
                totalNetSales > 0
                    ? (Math.abs(totalAdsSpend) / Math.abs(totalNetSales)) * 100
                    : 0,
            cm2_profit: totalCm2Profit,

            tax: totalTax,
            credits: totalCredits,
            tax_and_credits: totalTaxAndCredits,
            net_taxes: totalTax,
            other_transactions: totalTaxAndCredits,

            cm1_profit_per:
                totalNetSales > 0 ? (totalProfit / totalNetSales) * 100 : 0,
            cm1_profit_per_unit:
                totalQuantity > 0 ? totalProfit / totalQuantity : 0,
            cm2_profit_per:
                totalNetSales > 0 ? (totalCm2Profit / totalNetSales) * 100 : 0,
            cm2_profit_per_unit:
                totalQuantity > 0 ? totalCm2Profit / totalQuantity : 0,
            profit: totalProfit,

            platform_fee: rows.reduce((s, r) => s + toNumberSafe(r.platform_fee), 0),
            platform_fee_inventory_storage: rows.reduce(
                (s, r) => s + toNumberSafe(r.platform_fee_inventory_storage),
                0
            ),
            lost_total: rows.reduce((s, r) => s + toNumberSafe(r.lost_total), 0),
            other: rows.reduce((s, r) => s + toNumberSafe(r.other), 0),
            product_spend: rows.reduce((s, r) => s + toNumberSafe(r.product_spend), 0),
            display_spend: rows.reduce((s, r) => s + toNumberSafe(r.display_spend), 0),
            brand_spend: rows.reduce((s, r) => s + toNumberSafe(r.brand_spend), 0),
            dealsvouchar_ads: rows.reduce((s, r) => s + toNumberSafe(r.dealsvouchar_ads), 0),
            platformfeenew: rows.reduce((s, r) => s + toNumberSafe(r.platformfeenew), 0),

            isTotal: true,
        };

        return [...rows, totalRow];
    };

    const monthlySkuwiseRowsDisplay = useMemo<MonthlySkuwiseRow[]>(() => {
        return (monthlySkuwiseRows || []).map((row: any) => ({
            ...row,
            product_name:
                platform === "global"
                    ? normalizeProductDisplayName(row?.product_name)
                    : row?.product_name,
        }));
    }, [monthlySkuwiseRows, platform]);

    const grandTotalRowDisplay = useMemo(() => {
        return (
            monthlySkuwiseRowsDisplay.find((item: any) => item.sku === "GRAND_TOTAL") ??
            monthlySkuwiseRowsDisplay.find((item: any) => item.isTotal) ??
            monthlySkuwiseRowsDisplay.find((item: any) =>
                ["grand total", "total"].includes(
                    String(item.product_name || "").trim().toLowerCase()
                )
            ) ??
            null
        );
    }, [monthlySkuwiseRowsDisplay]);

    const marketplaceFeesFromTable = useMemo(() => {
        const fba = Number(grandTotalRowDisplay?.fba_fees ?? 0);
        const selling = Number(grandTotalRowDisplay?.selling_fees ?? 0);
        return Math.abs(fba + selling);
    }, [grandTotalRowDisplay]);

    const grandTotalRowRaw = useMemo<GrandTotalSkuwiseRow | null>(() => {
        return (
            monthlySkuwiseRows.find((item: any) => item.sku === "GRAND_TOTAL") ??
            monthlySkuwiseRows.find((item: any) => item.isTotal) ??
            monthlySkuwiseRows.find((item: any) =>
                ["grand total", "total"].includes(
                    String(item.product_name || "").trim().toLowerCase()
                )
            ) ??
            null
        );
    }, [monthlySkuwiseRows]);

    const getNetUnits = (row: any) =>
        toNumber(
            row?.total_quantity ??
            row?.net_quantity ??
            (
                toNumber(row?.quantity) -
                toNumber(row?.return_quantity ?? row?.returns_quantity ?? row?.return_qty)
            )
        );

    const mtdUnitsCurrent = useMemo(() => {
        return getNetUnits(grandTotalRowRaw ?? grandTotalRowDisplay ?? {});
    }, [grandTotalRowRaw, grandTotalRowDisplay]);

    const mtdUnitsPrevious = useMemo(() => {
        return toNumber(
            data?.previous_period?.totals?.total_quantity ??
            data?.previous_period?.totals?.net_quantity ??
            prev.quantity
        );
    }, [data, prev.quantity]);

    const mtdUnitsDelta = safeDeltaPct(mtdUnitsCurrent, mtdUnitsPrevious);

    const plSummaryTotals = useMemo<PlSummaryTotals>(() => {
        return computePlSummaryTotals(data, monthlySkuwiseRows, platform);
    }, [data, monthlySkuwiseRows, platform]);

    const totalRowCm2Profit = useMemo(() => {
        return toNumber(
            grandTotalRowRaw?.total_cm2_profit ??
            grandTotalRowDisplay?.total_cm2_profit ??
            plSummaryTotals.cm2_profit ??
            grandTotalRowRaw?.cm2_profit ??
            grandTotalRowDisplay?.cm2_profit ??
            0
        );
    }, [
        grandTotalRowRaw?.total_cm2_profit,
        grandTotalRowDisplay?.total_cm2_profit,
        plSummaryTotals.cm2_profit,
        grandTotalRowRaw?.cm2_profit,
        grandTotalRowDisplay?.cm2_profit,
    ]);

    const totalRowCm2Margins = useMemo(() => {
        return toNumber(
            grandTotalRowRaw?.total_cm2_margins ??
            grandTotalRowDisplay?.total_cm2_margins ??
            plSummaryTotals.cm2_margins ??
            grandTotalRowRaw?.cm2_profit_per ??
            grandTotalRowDisplay?.cm2_profit_per ??
            0
        );
    }, [
        grandTotalRowRaw?.total_cm2_margins,
        grandTotalRowDisplay?.total_cm2_margins,
        plSummaryTotals.cm2_margins,
        grandTotalRowRaw?.cm2_profit_per,
        grandTotalRowDisplay?.cm2_profit_per,
    ]);

    // ===================== SKUWISE API FIELD MAPPING =====================
    // These replace the old frontend-calculated variables.
    // Source: backend skuwise_items / skuwise_items_global grand total row.

    const rawAdsSpend = toNumber(
        grandTotalRowRaw?.ads_spend ??
        grandTotalRowRaw?.total_ads ??
        grandTotalRowRaw?.advertising_total ??
        grandTotalRowRaw?.advertising_fees ??
        plSummaryTotals?.advertising_total ??
        0
    );

    const rawSponsoredProductsSpend = toNumber(
        grandTotalRowRaw?.product_spend ??
        plSummaryTotals?.visible_ads ??
        0
    );

    const rawSponsoredBrandSpend = toNumber(
        grandTotalRowRaw?.brand_spend ??
        0
    );

    const rawDealVouchers = toNumber(
        grandTotalRowRaw?.dealsvouchar_ads ??
        plSummaryTotals?.dealsvouchar_ads ??
        0
    );

    const rawPlatformFee = toNumber(
        grandTotalRowRaw?.platform_fee ??
        grandTotalRowRaw?.platformfeenew ??
        plSummaryTotals?.platform_fee ??
        0
    );

    const rawOtherPlatformFee = toNumber(
        grandTotalRowRaw?.platformfeenew ??
        grandTotalRowRaw?.platform_fee ??
        plSummaryTotals?.platform_fee ??
        0
    );

    const rawInventoryStorageFees = toNumber(
        grandTotalRowRaw?.platform_fee_inventory_storage ??
        plSummaryTotals?.platform_fee_inventory_storage ??
        0
    );

    const rawLostInventoryTotal = toNumber(
        grandTotalRowRaw?.lost_total ??
        plSummaryTotals?.lost_total ??
        plSummaryTotals?.reimbursement_lost_inventory_amount ??
        0
    );

    const rawProfit = toNumber(
        grandTotalRowRaw?.profit ??
        plSummaryTotals?.profit ??
        0
    );

    // Old UI logic used brand spend minus deals/vouchers as additional ad cost.
    const rawCostOfAds = Math.abs(rawSponsoredBrandSpend - rawDealVouchers);

    // Prefer backend total ad value first.
    // Fallback to ads_spend + costOfAds if total_ads is missing.
    const rawAdsSpendTotal = Math.abs(
        toNumber(
            grandTotalRowRaw?.total_ads ??
            grandTotalRowRaw?.advertising_total ??
            grandTotalRowRaw?.advertising_fees ??
            plSummaryTotals?.advertising_total ??
            0
        ) || (rawAdsSpend + rawCostOfAds)
    );

    // Prefer backend CM2 first.
    // Fallback to old formula only if backend CM2 is missing.
    const rawCm2Profit =
        toNumber(
            grandTotalRowRaw?.total_cm2_profit ??
            grandTotalRowDisplay?.total_cm2_profit ??
            plSummaryTotals?.cm2_profit ??
            grandTotalRowRaw?.cm2_profit ??
            grandTotalRowDisplay?.cm2_profit ??
            0
        ) || (rawProfit - rawAdsSpendTotal - Math.abs(rawPlatformFee));

    const rawCm2Margins = toNumber(
        grandTotalRowRaw?.total_cm2_margins ??
        grandTotalRowRaw?.cm2_profit_per ??
        plSummaryTotals?.cm2_margins ??
        0
    );

    const adsSpendTotal =
        platform === "global"
            ? convertToDisplayCurrency(rawAdsSpendTotal, amazonDataCurrency)
            : rawAdsSpendTotal;

    const costOfAds =
        platform === "global"
            ? convertToDisplayCurrency(rawCostOfAds, amazonDataCurrency)
            : rawCostOfAds;

    const cm2Profit =
        platform === "global"
            ? convertToDisplayCurrency(rawCm2Profit, amazonDataCurrency)
            : rawCm2Profit;

    const cm2Margins = rawCm2Margins;

    const platformFee =
        platform === "global"
            ? convertToDisplayCurrency(Math.abs(rawPlatformFee), amazonDataCurrency)
            : Math.abs(rawPlatformFee);

    const sponsoredProductsSpend =
        platform === "global"
            ? convertToDisplayCurrency(rawSponsoredProductsSpend, amazonDataCurrency)
            : rawSponsoredProductsSpend;

    const sponsoredBrandSpend =
        platform === "global"
            ? convertToDisplayCurrency(rawSponsoredBrandSpend, amazonDataCurrency)
            : rawSponsoredBrandSpend;

    const dealVoucher =
        platform === "global"
            ? convertToDisplayCurrency(rawDealVouchers, amazonDataCurrency)
            : rawDealVouchers;

    // Alias because your file uses both names.
    const dealVouchers = dealVoucher;

    const inventoryStorageFees =
        platform === "global"
            ? convertToDisplayCurrency(rawInventoryStorageFees, amazonDataCurrency)
            : rawInventoryStorageFees;

    const lost_inventory_total =
        platform === "global"
            ? convertToDisplayCurrency(rawLostInventoryTotal, amazonDataCurrency)
            : rawLostInventoryTotal;

    const otherPlatformFee =
        platform === "global"
            ? convertToDisplayCurrency(rawOtherPlatformFee, amazonDataCurrency)
            : rawOtherPlatformFee;


    const amazonPl = () => {
        const sourceCurrency = amazonDataCurrency;

        const toDisplay = (value: any) => {
            const n = toNumberSafe(value ?? 0);

            return platform === "global"
                ? n
                : convertToDisplayCurrency(n, sourceCurrency);
        };

        const sales = toDisplay(derived?.net_sales ?? 0);

        const cogs = toDisplay(totals?.cogs ?? grandTotalRowRaw?.cogs ?? 0);

        const fees = toDisplay(
            Math.abs(
                toNumberSafe(grandTotalRowRaw?.fba_fees ?? totals?.fba_fees ?? 0) +
                toNumberSafe(grandTotalRowRaw?.selling_fees ?? totals?.selling_fees ?? 0)
            )
        );

        const taxCredits = toDisplay(
            totals?.tax_and_credits ?? grandTotalRowRaw?.tax_and_credits ?? 0
        );

        const cm1 = toDisplay(
            derived?.profit ?? totals?.profit ?? grandTotalRowRaw?.profit ?? 0
        );

        const adv = toDisplay(
            derived?.advertising_fees ?? grandTotalRowRaw?.ads_spend ?? 0
        );

        const others = convertToDisplayCurrency(
            toNumberSafe(
                derived?.platform_fee ??
                grandTotalRowRaw?.platform_fee ??
                grandTotalRowRaw?.platform_fee_inventory_storage ??
                0
            ),
            sourceCurrency
        );

        const cm2 = convertToDisplayCurrency(
            toNumberSafe(
                grandTotalRowRaw?.total_cm2_profit ??
                grandTotalRowDisplay?.total_cm2_profit ??
                derived?.total_cm2_profit ??
                derived?.cm2_profit ??
                grandTotalRowRaw?.cm2_profit ??
                grandTotalRowDisplay?.cm2_profit ??
                0
            ),
            sourceCurrency
        );

        return [
            { label: "Net Sales", raw: sales, display: formatDisplayAmount(sales) },
            { label: "COGS", raw: cogs, display: formatDisplayAmount(cogs) },
            { label: "Marketplace Fees", raw: fees, display: formatDisplayAmount(fees) },
            { label: "Tax & Credits", raw: taxCredits, display: formatDisplayAmount(taxCredits) },
            { label: "CM1 Profit", raw: cm1, display: formatDisplayAmount(cm1) },
            { label: "Advertisements", raw: adv, display: formatDisplayAmount(adv) },
            { label: "Others", raw: others, display: formatDisplayAmount(others) },
            { label: "CM2 Profit", raw: cm2, display: formatDisplayAmount(cm2) },
        ];
    };

    /* ===================== P&L ITEMS (DISPLAY CURRENCY OUTPUT) ===================== */
    const plItems = useMemo(() => {
        const ukPl = () => {
            const sales = convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");
            const fees = marketplaceFeesFromTable;
            const cogs = convertToDisplayCurrency(uk.cogsGBP ?? 0, "GBP");
            const adv = convertToDisplayCurrency(uk.advertisingGBP ?? 0, "GBP");

            const others = convertToDisplayCurrency(uk.platformFeeGBP ?? 0, "GBP");
            const cm1 = convertToDisplayCurrency(uk.profitGBP ?? 0, "GBP");
            const cm2 = convertToDisplayCurrency(uk.cm2ProfitGBP ?? 0, "GBP");
            const taxCredits = convertToDisplayCurrency(
                toNumberSafe(totals?.tax_and_credits ?? 0),
                "GBP"
            );

            return [
                { label: "Net Sales", raw: sales, display: formatDisplayAmount(sales) },
                { label: "COGS", raw: cogs, display: formatDisplayAmount(cogs) },
                { label: "Marketplace Fees", raw: fees, display: formatDisplayAmount(Math.abs(fees)) },
                { label: "Tax & Credits", raw: taxCredits, display: formatDisplayAmount(taxCredits) },
                { label: "CM1 Profit", raw: cm1, display: formatDisplayAmount(cm1) },
                { label: "Advertisements", raw: adv, display: formatDisplayAmount(adv) },
                { label: "Others", raw: others, display: formatDisplayAmount(others) },
                { label: "CM2 Profit", raw: cm2, display: formatDisplayAmount(cm2) },
            ];
        };

        if (graphRegionToUse === "Global") {
            if (onlyAmazon) return ukPl();

            if (onlyShopify) {
                const sales = convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR");
                return [
                    { label: "Net Sales", raw: sales, display: formatDisplayAmount(sales) },
                    { label: "Marketplace Fees", raw: 0, display: formatDisplayAmount(0) },
                    { label: "COGS", raw: 0, display: formatDisplayAmount(0) },
                    { label: "Advertisements", raw: 0, display: formatDisplayAmount(0) },
                    { label: "Other Charges", raw: 0, display: formatDisplayAmount(0) },
                    { label: "Profit", raw: 0, display: formatDisplayAmount(0) },
                ];
            }

            const sales = convertToDisplayCurrency(combinedUSD, "USD");
            return [
                { label: "Sales", raw: sales, display: formatDisplayAmount(sales) },
                { label: "Marketplace Fees", raw: 0, display: formatDisplayAmount(0) },
                { label: "COGS", raw: 0, display: formatDisplayAmount(0) },
                { label: "Advertisements", raw: 0, display: formatDisplayAmount(0) },
                { label: "Other Charges", raw: 0, display: formatDisplayAmount(0) },
                { label: "Profit", raw: 0, display: formatDisplayAmount(0) },
            ];
        }

        // if (graphRegionToUse === "UK") return ukPl();

        if (["UK", "US", "CA"].includes(graphRegionToUse)) {
            return amazonPl();
        }

        const zero = formatDisplayAmount(0);
        return [
            { label: "Sales", raw: 0, display: zero },
            { label: "Marketplace Fees", raw: 0, display: zero },
            { label: "COGS", raw: 0, display: zero },
            { label: "Advertisements", raw: 0, display: zero },
            { label: "Other Charges", raw: 0, display: zero },
            { label: "Profit", raw: 0, display: zero },
        ];
    }, [
        graphRegionToUse,
        derived?.net_sales,
        derived?.advertising_fees,
        derived?.platform_fee,
        derived?.profit,
        derived?.cm2_profit,
        totals?.cogs,
        totals?.tax_and_credits,
        grandTotalRowRaw,
        amazonDataCurrency
    ]);

    const chartItems = useMemo(() => plItems || [], [plItems]);

    const labels = useMemo(() => chartItems.map((i) => i.label), [chartItems]);
    const values = useMemo(() => chartItems.map((i) => Number(i.raw ?? 0)), [chartItems]);

    const allValuesZero = useMemo(
        () => values.length === 0 || values.every((v) => Math.abs(v) < 1e-9),
        [values]
    );


    const formatAdType = (adType?: string | null) => {
        if (!adType) return "-";

        return String(adType)
            .split(",")
            .map((t) =>
                t.trim().replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
            )
            .filter(Boolean)
            .join(", ");
    };

    const monthlySkuwiseRowsForTable = useMemo<MonthlySkuwiseTableRow[]>(() => {
        if (!monthlySkuwiseRowsDisplay || monthlySkuwiseRowsDisplay.length === 0) return [];

        const isMtdTotalRow = (row: any) => {
            const name = String(row?.product_name || "").trim().toLowerCase();
            const sku = String(row?.sku || "").trim().toUpperCase();

            return (
                !!row?.isTotal ||
                sku === "GRAND_TOTAL" ||
                sku === "TOTAL" ||
                name === "grand total" ||
                name === "total"
            );
        };

        const normalizeTotalRow = (row: any): MonthlySkuwiseTableRow => ({
            ...row,
            sku: row?.sku || "GRAND_TOTAL",
            product_name: "Total",
            isTotal: true,
            isOthers: false,
            sno: undefined,
        });

        const totalRowRaw = monthlySkuwiseRowsDisplay.find(isMtdTotalRow) ?? null;
        const totalRow = totalRowRaw ? normalizeTotalRow(totalRowRaw) : null;

        const bodyRows = monthlySkuwiseRowsDisplay.filter((r) => !isMtdTotalRow(r));

        const getMarketplaceFees = (row: MonthlySkuwiseRow) => {
            const value =
                toNumber((row as any).amazon_fees) ||
                toNumber((row as any).marketplace_fees) ||
                toNumber(row.fba_fees) + toNumber(row.selling_fees);

            return Math.abs(value);
        };

        const getSortValue = (row: MonthlySkuwiseRow, key: string) => {
            switch (key) {
                case "marketplace_total":
                    return getMarketplaceFees(row);

                case "quantity":
                case "return_quantity":
                case "total_quantity":
                case "asp":
                case "net_sales":
                case "cogs":
                case "tax":
                case "credits":
                case "tax_and_credits":
                case "cm1_profit_per_unit":
                case "cm1_profit_per":
                case "profit":
                case "ads_spend":
                case "acos":
                case "cm2_profit_per_unit":
                case "cm2_profit_per":
                case "cm2_profit":
                    return toNumber((row as any)[key]);

                case "product_name":
                    return String(row.product_name || "").toLowerCase();

                case "sku":
                    return String(row.sku || "").toLowerCase();
                case "product_spend":
                case "display_spend":
                case "brand_spend":
                    return toNumber((row as any)[key]);

                default:
                    return toNumber((row as any)[key]);
            }
        };

        const sorted = [...bodyRows].sort((a, b) => {
            const aValue = getSortValue(a, plSortConfig.key);
            const bValue = getSortValue(b, plSortConfig.key);

            if (typeof aValue === "number" && typeof bValue === "number") {
                return plSortConfig.direction === "desc"
                    ? bValue - aValue
                    : aValue - bValue;
            }

            return plSortConfig.direction === "desc"
                ? String(bValue).localeCompare(String(aValue))
                : String(aValue).localeCompare(String(bValue));
        });

        if (showAllMtdProductwiseRows || sorted.length <= 9) {
            const out: MonthlySkuwiseTableRow[] = sorted.map((r, idx) => ({
                ...r,
                sno: idx + 1,
                isOthers: false,
                isTotal: false,
            }));

            if (totalRow) out.push(totalRow);

            return out;
        }

        const top9 = sorted.slice(0, 9).map((r, idx) => ({
            ...r,
            sno: idx + 1,
            isOthers: false,
            isTotal: false,
        }));

        const rest = sorted.slice(9);

        const sum = (key: keyof MonthlySkuwiseRow) =>
            rest.reduce((acc, r) => acc + (Number((r as any)[key]) || 0), 0);

        const othersQty = sum("quantity");
        const othersNetSales = sum("net_sales");

        const othersAsp =
            othersQty && Number.isFinite(othersQty) && othersQty !== 0
                ? othersNetSales / othersQty
                : rest.length
                    ? rest.reduce((acc, r) => acc + (Number(r.asp) || 0), 0) / rest.length
                    : 0;

        const othersProductSpend = rest.reduce(
            (acc, r) => acc + toNumberSafe(r.product_spend ?? 0),
            0
        );

        const othersDisplaySpend = rest.reduce(
            (acc, r) => acc + toNumberSafe(r.display_spend ?? 0),
            0
        );

        const othersAdsSpend = rest.reduce(
            (acc, r) => acc + toNumberSafe(r.ads_spend ?? 0),
            0
        );

        const othersRow: MonthlySkuwiseTableRow = {
            sno: 10,
            sku: "OTHERS",
            product_name: "Others",

            quantity: othersQty,

            return_quantity: sum("return_quantity" as keyof MonthlySkuwiseRow),

            total_quantity: rest.reduce(
                (acc, r) =>
                    acc +
                    toNumber(
                        (r as any).total_quantity ??
                        (toNumber((r as any).quantity) - toNumber((r as any).return_quantity))
                    ),
                0
            ),

            asp: othersAsp,
            net_sales: othersNetSales,

            cogs: sum("cogs"),
            fba_fees: sum("fba_fees"),
            selling_fees: sum("selling_fees"),

            tax: sum("tax"),
            credits: sum("credits"),
            tax_and_credits: sum("tax_and_credits"),

            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,

            ads_spend: othersAdsSpend,
            product_spend: othersProductSpend,
            display_spend: othersDisplaySpend,
            brand_spend: sum("brand_spend"),

            acos: othersNetSales
                ? (Math.abs(sum("ads_spend")) / Math.abs(othersNetSales)) * 100
                : 0,

            cm2_profit: sum("cm2_profit"),
            profit: sum("profit"),

            platform_fee: sum("platform_fee"),
            platform_fee_inventory_storage: sum("platform_fee_inventory_storage"),
            lost_total: sum("lost_total"),
            other: sum("other"),

            // product_spend: sum("product_spend"),
            // display_spend: sum("display_spend" as keyof MonthlySkuwiseRow),
            // brand_spend: sum("brand_spend"),
            dealsvouchar_ads: sum("dealsvouchar_ads"),
            platformfeenew: sum("platformfeenew"),

            total_ads: sum("total_ads"),
            advertising_fees: sum("advertising_fees"),
            amazon_fees: rest.reduce((acc, r) => acc + getMarketplaceFees(r), 0),

            isOthers: true,
            isTotal: false,
        };

        const out: MonthlySkuwiseTableRow[] = [...top9, othersRow];

        if (totalRow) out.push(totalRow);

        return out;
    }, [monthlySkuwiseRowsDisplay, plSortConfig, showAllMtdProductwiseRows]);

    const globalMtdCardData = useMemo(() => {
        const globalRows =
            platform === "global" && Array.isArray(data?.skuwise_items_global)
                ? data.skuwise_items_global
                : Array.isArray(data?.skuwise_items)
                    ? data.skuwise_items
                    : monthlySkuwiseRows || [];

        const globalGrand = getGrandTotalRow(globalRows) as GrandTotalSkuwiseRow;

        const prevDerived = previousSkuwiseGlobalData?.derived_totals_global || {};
        const prevAligned = previousSkuwiseGlobalData?.aligned_totals_global || {};

        return {
            units: getNetUnits(globalGrand),
            prevUnits: toNumber(prevDerived.total_quantity ?? prevDerived.net_quantity ?? prevDerived.quantity),

            grossSales: toNumber(globalGrand.gross_sales),
            prevGrossSales: toNumber(prevDerived.gross_sales),

            netSales: toNumber(globalGrand.net_sales),
            prevNetSales: toNumber(prevDerived.net_sales),

            asp: toNumber(globalGrand.asp),
            prevAsp: toNumber(prevDerived.asp),

            ads: toNumber(globalGrand.total_ads ?? globalGrand.advertising_fees ?? globalGrand.ads_spend),
            prevAds: toNumber(prevAligned.total_previous_advertising ?? prevDerived.advertising_fees),

            tacos: toNumber(globalGrand.tacos_total_advertising_cost_of_sale ?? globalGrand.acos),
            prevTacos: toNumber(
                prevDerived.net_sales
                    ? (toNumber(prevAligned.total_previous_advertising ?? prevDerived.advertising_fees) /
                        toNumber(prevDerived.net_sales)) * 100
                    : 0
            ),

            cm2Profit: toNumber(globalGrand.total_cm2_profit ?? globalGrand.cm2_profit),
            prevCm2Profit: toNumber(prevAligned.total_previous_profit_cm2 ?? prevDerived.cm2_profit),

            cm2Pct: toNumber(globalGrand.total_cm2_margins ?? globalGrand.profit_percentage ?? globalGrand.cm2_profit_per),
            prevCm2Pct: toNumber(
                prevAligned.total_previous_profit_percentage ??
                prevDerived.cm2_profit_percentage
            ),
        };
    }, [
        platform,
        data,
        monthlySkuwiseRows,
        previousSkuwiseGlobalData,
    ]);

    const stickyTableTotals = useMemo(() => {
        const row: GrandTotalSkuwiseRow = grandTotalRowRaw ?? grandTotalRowDisplay ?? {};

        const units = getNetUnits(row);
        const netSales = toNumber(row.net_sales);
        const asp = toNumber(row.asp);

        const costOfAds = toNumber(
            platform === "global"
                ? (
                    plSummaryTotals.advertising_total ??
                    row.total_ads ??
                    row.advertising_total ??
                    row.advertising_fees ??
                    row.ads_spend
                )
                : (
                    row.total_ads ??
                    row.ads_spend ??
                    row.advertising_total ??
                    row.advertising_fees
                )
        );

        const tacos = toNumber(
            platform === "global"
                ? (
                    plSummaryTotals.acos ??
                    row.tacos_total_advertising_cost_of_sale ??
                    row.acos
                )
                : (
                    row.tacos_total_advertising_cost_of_sale ??
                    row.acos
                )
        );

        const cm2Profit = toNumber(
            platform === "global"
                ? (
                    plSummaryTotals.cm2_profit ??
                    row.total_cm2_profit ??
                    row.cm2_profit
                )
                : (
                    row.total_cm2_profit ??
                    row.cm2_profit
                )
        );

        const cm2MarginPct = toNumber(
            row.total_cm2_margins ??
            row.cm2_profit_per
        );

        return {
            units,
            netSales,
            asp,
            costOfAds,
            tacos,
            cm2Profit,
            cm2MarginPct,
        };
    }, [
        platform,
        grandTotalRowRaw,
        grandTotalRowDisplay,
        plSummaryTotals.advertising_total,
        plSummaryTotals.acos,
        plSummaryTotals.cm2_profit,
    ]);

    const stickyPreviousTotals = useMemo(() => {
        const prevDerived = previousSkuwiseGlobalData?.derived_totals_global || {};
        const prevAligned = previousSkuwiseGlobalData?.aligned_totals_global || {};

        const prevNetSales = toNumber(prevDerived.net_sales);
        const prevAds = toNumber(
            prevAligned.total_previous_advertising ??
            prevDerived.advertising_fees
        );

        return {
            units: toNumber(prevDerived.quantity),
            netSales: prevNetSales,
            asp: toNumber(prevDerived.asp),

            costOfAds: prevAds,

            tacos: prevNetSales
                ? (prevAds / prevNetSales) * 100
                : 0,

            cm2Profit: toNumber(
                prevAligned.total_previous_profit_cm2 ??
                prevDerived.cm2_profit
            ),

            cm2MarginPct: toNumber(
                prevAligned.total_previous_profit_percentage ??
                prevDerived.cm2_profit_percentage
            ),
        };
    }, [previousSkuwiseGlobalData]);

    const globalTargetCardTotals = useMemo(() => {
        const prevAligned = previousSkuwiseGlobalData?.aligned_totals_global || {};
        const prevDerived = previousSkuwiseGlobalData?.derived_totals_global || {};

        const currentNetSales = toNumber(
            stickyTableTotals.netSales ??
            plSummaryTotals.net_sales ??
            (data as any)?.derived_totals?.net_sales
        );

        // Previous MTD / same selected period
        const previousNetSales = toNumber(
            prevAligned.total_previous_net_sales ??
            prevDerived.net_sales
        );

        // Previous full-month net sales
        const previousNetSalesFullMonth = toNumber(
            prevAligned.total_previous_net_sales_full_month ??
            previousSkuwiseGlobalData?.aligned_totals_global?.total_previous_net_sales_full_month ??
            prevDerived.total_previous_net_sales_full_month
        );

        const currentReimbursement = toNumber(
            plSummaryTotals.net_reimbursement ??
            (data as any)?.derived_totals?.current_net_reimbursement ??
            (data as any)?.current_net_reimbursement
        );

        const previousReimbursement = toNumber(
            prevAligned.total_previous_rembursement_fee ??
            prevAligned.total_previous_reimbursement_fee ??
            prevDerived.total_previous_rembursement_fee ??
            prevDerived.total_previous_reimbursement_fee
        );

        return {
            currentNetSales,
            previousNetSales,
            previousNetSalesFullMonth,
            currentReimbursement,
            previousReimbursement,
            reimbursementDeltaPct: safeDeltaPct(
                currentReimbursement,
                previousReimbursement
            ),
        };
    }, [
        data,
        stickyTableTotals.netSales,
        plSummaryTotals.net_sales,
        plSummaryTotals.net_reimbursement,
        previousSkuwiseGlobalData,
    ]);

    const salesTargetBiAlignedTotals = useMemo(() => {
        if (platform !== "global") {
            return biAlignedTotalsHome;
        }

        return {
            ...(biAlignedTotalsHome || {}),

            total_current_net_sales:
                globalTargetCardTotals.currentNetSales,

            total_previous_net_sales:
                globalTargetCardTotals.previousNetSales,

            total_previous_net_sales_full_month:
                globalTargetCardTotals.previousNetSalesFullMonth,

            total_current_rembursement_fee:
                globalTargetCardTotals.currentReimbursement,

            total_previous_rembursement_fee:
                globalTargetCardTotals.previousReimbursement,

            total_current_advertising:
                stickyTableTotals.costOfAds,

            total_previous_advertising:
                previousSkuwiseGlobalData?.aligned_totals_global?.total_previous_advertising ?? 0,

            total_current_profit:
                stickyTableTotals.cm2Profit,

            total_previous_profit:
                previousSkuwiseGlobalData?.aligned_totals_global?.total_previous_profit ?? 0,

            total_current_platform_fees: 0,

            total_previous_platform_fees:
                previousSkuwiseGlobalData?.aligned_totals_global?.total_previous_platform_fees ?? 0,
        };
    }, [
        platform,
        biAlignedTotalsHome,
        globalTargetCardTotals,
        stickyTableTotals.costOfAds,
        stickyTableTotals.cm2Profit,
        previousSkuwiseGlobalData,
    ]);

    const SKUWISE_LEFT_COLS: LeafCol<MonthlySkuwiseTableRow>[] = [
        {
            key: "sno",
            label: "S.No",
            align: "center",
            width: "6%",
        },
        {
            key: "product_name",
            label: "Product Name",
            align: "left",
            info: <InfoTip text={TERM_DEFINITIONS.product_name} />,
            width: "14%",
        },
    ];

    const SKUWISE_GROUPS = [
        {
            id: "marketplace_fees",
            label: "Marketplace Fees",
            // info: <InfoTip text={TERM_DEFINITIONS.marketplace_fees} />,
            collapsedCols: [
                {
                    key: "marketplace_total",
                    label: "Total",
                    width: "7%",
                    align: "center" as const,
                },
            ],

            expandedCols: [
                { key: "selling_fees", label: "Selling Fees", align: "center" as const },
                { key: "fba_fees", label: "FBA Fees", align: "center" as const },
                {
                    key: "marketplace_total",
                    label: "Total",
                    align: "center" as const,
                },
            ],
        },

        {
            id: "quantity",
            label: "Net Units Sold",
            info: <InfoTip text={TERM_DEFINITIONS.net_units_sold} />,

            collapsedCols: [
                {
                    key: "total_quantity",
                    label: "Total",
                    align: "center" as const,
                    width: "8%",
                    sortable: true,
                },
            ],

            expandedCols: [
                {
                    key: "sku",
                    label: "SKU",
                    align: "center" as const,
                    width: "7%",
                },
                {
                    key: "quantity",
                    label: "Units Sold",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
                {
                    key: "return_quantity",
                    label: "Return",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
                {
                    key: "total_quantity",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
            ],
        },

        {
            id: "profit",
            label: "CM1 Profit",
            // info: <InfoTip text={TERM_DEFINITIONS.cm1_profit} />,

            collapsedCols: [
                {
                    key: "profit",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
            ],

            expandedCols: [
                { key: "cm1_profit_per_unit", label: "Per Unit", align: "center" as const },
                { key: "cm1_profit_per", label: "%", align: "center" as const },
                {
                    key: "profit",
                    label: "Total",
                    align: "center" as const,
                },
            ],
        },
        {
            id: "tax_and_credits",
            label: "Other Transactions",

            collapsedCols: [
                {
                    key: "tax_and_credits",
                    label: "Total",
                    align: "center" as const,
                    width: "10%",
                },
            ],

            expandedCols: [
                {
                    key: "tax",
                    label: "Net Taxes",
                    info: <InfoTip text={TERM_DEFINITIONS.net_taxes} />,
                    align: "center" as const
                },
                {
                    key: "credits",
                    label: "Net Credits",
                    info: <InfoTip text={TERM_DEFINITIONS.net_credits} />,
                    align: "center" as const
                },
                {
                    key: "tax_and_credits",
                    label: "Total",
                    align: "center" as const,
                },
            ],
        },
        {
            id: "ads_spend",
            label: "Ads Spend",
            info: <InfoTip text={TERM_DEFINITIONS.ads_spend} />,

            collapsedCols: [
                {
                    key: "ads_spend",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                    render: (row: MonthlySkuwiseTableRow) =>
                        formatDisplayAmount(toNumberSafe(row.ads_spend)),
                },
            ],

            expandedCols: [
                {
                    key: "product_spend",
                    label: "Sponsored Product",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                },
                {
                    key: "display_spend",
                    label: "Sponsored Display",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                },
                {
                    key: "ads_spend",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                },
            ],
        },

        {
            id: "cm2_profit",
            label: "CM2 Profit",
            info: <InfoTip text={TERM_DEFINITIONS.cm2_profit} />,

            collapsedCols: [
                {
                    key: "cm2_profit",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
            ],

            expandedCols: [
                { key: "cm2_profit_per_unit", label: "Per Unit", align: "center" as const },
                { key: "cm2_profit_per", label: "%", align: "center" as const },
                {
                    key: "cm2_profit",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                },
            ],
        },
    ];

    const SKUWISE_SINGLE_COLS = [
        { key: "quantity", label: "Net Units Sold", align: "center" as const },
        {
            key: "asp",
            label: "ASP",
            info: <InfoTip text={TERM_DEFINITIONS.asp} />,
            align: "center" as const,
            width: "7%",
        },
        {
            key: "net_sales",
            label: "Net Sales",
            sortable: true,
            info: <InfoTip text={TERM_DEFINITIONS.net_sales} />,
            align: "center" as const,
            width: "7%",
        },
        { key: "cogs", label: "COGS", align: "center" as const, },
        { key: "profit", label: "CM1 Profit", align: "center" as const },
        { key: "ads_spend", label: "Ads Spend", align: "center" as const, },
        { key: "acos", label: "ACoS %", align: "center" as const, },
        { key: "cm2_profit", label: "CM2 Profit", align: "center" as const },
        { key: "cm1_profit_per", label: "CM1 Profit Per Unit", align: "center" as const },
        { key: "cm1_profit_per_unit", label: "CM1 Profit %", align: "center" as const },
        { key: "cm2_profit_per", label: "CM2 Profit Per Unit", align: "center" as const },
        { key: "cm2_profit_per_unit", label: "CM2 Profit %", align: "center" as const }

    ];

    const prevValues = useMemo(() => {
        const getPrev = (label: string) => {
            switch (label) {
                case "Net Sales":
                    return globalUseBi
                        ? (biCardKpis.prev.netSales ?? 0)
                        : convertToDisplayCurrency(prev.netSales ?? 0, amazonDataCurrency);

                case "COGS":
                    return convertToDisplayCurrency(
                        toNumberSafe(data?.previous_period?.totals?.cogs ?? 0),
                        amazonDataCurrency
                    );

                case "Marketplace Fees":
                    return convertToDisplayCurrency(
                        toNumberSafe(data?.previous_period?.totals?.amazon_fees ?? 0),
                        amazonDataCurrency
                    );

                case "Tax & Credits":
                    return convertToDisplayCurrency(
                        toNumberSafe(data?.previous_period?.totals?.tax_and_credits ?? 0),
                        amazonDataCurrency
                    );

                case "Advertisements":
                    return convertToDisplayCurrency(
                        toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0),
                        amazonDataCurrency
                    );

                case "Others":
                    return convertToDisplayCurrency(
                        toNumberSafe(data?.previous_period?.totals?.platform_fee ?? 0),
                        amazonDataCurrency
                    );

                case "CM1 Profit":
                    return convertToDisplayCurrency(
                        toNumberSafe(data?.previous_period?.totals?.profit ?? 0),
                        amazonDataCurrency
                    );

                case "CM2 Profit":
                    return globalUseBi
                        ? (cm2Ready ? convertToDisplayCurrency(biAlignedTotals?.previous_cm2_profit ?? 0, biSourceCurrency) : 0)
                        : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency);

                default:
                    return 0;
            }
        };

        // return labels.map(getPrev);

        const mappedPrevValues = labels.map(getPrev);

        return mappedPrevValues;

    }, [
        labels,
        data?.previous_period?.totals,
        prev.netSales,
        prev.cm2Profit,
        amazonDataCurrency,
        convertToDisplayCurrency,
        globalUseBi,
        biCardKpis,
        cm2Ready,
        biAlignedTotals,
        biSourceCurrency,
    ]);


    const colorMapping: Record<string, string> = {
        "Net Sales": "#75BBDA",
        "Marketplace Fees": "#B75A5A",
        COGS: "#FDD36F",
        Advertisements: "#C49466",
        "Tax & Credits": "#ED9F50",
        Others: "#3A8EA4",
        "CM1 Profit": "#7B9A6D",
        "CM2 Profit": "#B8C78C",

    };

    const colors = labels.map((label) => colorMapping[label] || "#75BBDA");

    /* ===================== EXCEL EXPORT (USES displayCurrency symbol) ===================== */

    const shortMonForGraph = new Date(`${currMonthName} 1, ${currYear}`).toLocaleString("en-US", {
        month: "short",
        timeZone: getTimezoneForRegion(activeDateRegion),
    });

    // const formattedMonthYear = `${shortMonForGraph}'${String(currYear).slice(-2)}`;

    const getFormattedMonthYearByRegion = (region: RegionKey) => {
        const tz = getTimezoneForRegion(region);

        const now = new Date(
            new Date().toLocaleString("en-US", { timeZone: tz })
        );

        const monthShort = now.toLocaleString("en-US", {
            month: "short",
            timeZone: tz,
        });

        const yearShort = String(now.getFullYear()).slice(-2);

        return `${monthShort}'${yearShort}`;
    };

    // const formattedMonthYear = getFormattedMonthYearByRegion(activeDateRegion);

    const countryNameForGraph =
        graphRegionToUse === "Global" ? "global" : graphRegionToUse.toLowerCase();

    const todaySalesFromBI = useMemo(() => {
        const points = biDailySeries?.current_mtd || [];
        if (!points.length) return 0;
        const pts = rangeActive
            ? sliceByDayRange(points, selectedStartDay, selectedEndDay)
            : points;

        if (!pts.length) return 0;

        const last = [...pts].sort((a, b) => a.date.localeCompare(b.date)).at(-1);

        return Number(last?.net_sales) || 0;
    }, [biDailySeries, rangeActive, selectedStartDay, selectedEndDay]);

    const useBiForAmazonCards =
        showLiveBI && rangeActive && (isCountryMode || platform === "global");

    const unitsToUse = useBiForAmazonCards ? (biCardKpis.curr.units ?? 0) : toNumberSafe(totals?.quantity ?? 0);

    const moneyPerUnitFormatter = useCallback(
        (v: number) => renderMoneyWithPerUnit(Number(v) || 0, unitsToUse, formatDisplayAmount),
        [unitsToUse, renderMoneyWithPerUnit, formatDisplayAmount]
    );


    /* ===================== ✅ GLOBAL CARD: prev/current + deltas ===================== */

    // Global Units
    const globalCurrUnits = useMemo(() => {
        return toNumberSafe(totals?.quantity ?? 0) + toNumberSafe(shopifyDeriv?.totalOrders ?? 0);
    }, [totals?.quantity, shopifyDeriv?.totalOrders]);

    const globalPrevUnits = useMemo(() => {
        return toNumberSafe(prev.quantity ?? 0) + toNumberSafe(shopifyPrevDeriv?.totalOrders ?? 0);
    }, [prev.quantity, shopifyPrevDeriv?.totalOrders]);

    const globalCurrSalesDisp = useMemo(() => {
        return convertToDisplayCurrency(combinedUSD, "USD");
    }, [combinedUSD, convertToDisplayCurrency]);

    const globalPrevSalesDisp = useMemo(() => {
        return convertToDisplayCurrency(globalPrevTotalUSD, "USD");
    }, [globalPrevTotalUSD, convertToDisplayCurrency]);

    const globalCurrAsp = useMemo(() => {
        return globalCurrUnits > 0 ? globalCurrSalesDisp / globalCurrUnits : 0;
    }, [globalCurrSalesDisp, globalCurrUnits]);

    const globalPrevAsp = useMemo(() => {
        return globalPrevUnits > 0 ? globalPrevSalesDisp / globalPrevUnits : 0;
    }, [globalPrevSalesDisp, globalPrevUnits]);


    const globalCurrNetSalesDisp = useMemo(() => {
        if (onlyAmazon) return convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");
        return convertToDisplayCurrency(combinedUSD, "USD");
    }, [onlyAmazon, uk.netSalesGBP, combinedUSD, convertToDisplayCurrency]);

    const globalPrevNetSalesDisp = useMemo(() => {
        if (onlyAmazon) return convertToDisplayCurrency(prev.netSales ?? 0, "GBP");

        return convertToDisplayCurrency(globalPrevTotalUSD, "USD");
    }, [onlyAmazon, prev.netSales, globalPrevTotalUSD, convertToDisplayCurrency]);

    const globalCurrAdsDisp = useMemo(() => {
        const ads = toNumberSafe(derived?.advertising_fees ?? 0);
        return convertToDisplayCurrency(ads, amazonDataCurrency);
    }, [derived?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

    const globalPrevAdsDisp = useMemo(() => {
        const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
        return convertToDisplayCurrency(ads, amazonDataCurrency);
    }, [data?.previous_period?.totals?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

    const globalAdsDeltaPct = useMemo(
        () => safeDeltaPct(globalCurrAdsDisp, globalPrevAdsDisp),
        [globalCurrAdsDisp, globalPrevAdsDisp]
    );

    const globalCurrRoasPct = useMemo(() => {
        const ads = toNumberSafe(derived?.advertising_fees ?? 0);
        const amazonSales = toNumberSafe(derived?.net_sales ?? 0);
        const shopifySales = toNumberSafe(shopifyDeriv?.netSales ?? 0);

        const amazonSalesUsd =
            amazonDataCurrency === "GBP" ? amazonSales * gbpToUsd :
                amazonDataCurrency === "CAD" ? amazonSales * cadToUsd :
                    amazonSales;

        const shopifySalesUsd = shopifySales * inrToUsd;
        const globalSalesUsd = onlyAmazon ? amazonSalesUsd : (amazonSalesUsd + shopifySalesUsd);

        const adsUsd =
            amazonDataCurrency === "GBP" ? ads * gbpToUsd :
                amazonDataCurrency === "CAD" ? ads * cadToUsd :
                    ads;

        return globalSalesUsd > 0 ? (adsUsd / globalSalesUsd) * 100 : 0;
    }, [
        derived?.advertising_fees,
        derived?.net_sales,
        shopifyDeriv?.netSales,
        onlyAmazon,
        amazonDataCurrency,
        gbpToUsd,
        cadToUsd,
        inrToUsd,
    ]);

    const costOfAdsForSummary = useMemo(() => {
        if (useBiForAmazonCards) {
            if (!cm2Ready) return 0;
            return convertToDisplayCurrency(
                biAlignedTotals?.total_current_advertising ?? 0,
                biSourceCurrency
            );
        }
        return toNumber(amazonCurrAdsDisp);
    }, [
        useBiForAmazonCards,
        cm2Ready,
        biAlignedTotals,
        biSourceCurrency,
        convertToDisplayCurrency,
        amazonCurrAdsDisp,
    ]);


    const globalCurrAspDisp = useMemo(() => {
        return globalCurrUnits > 0 ? globalCurrNetSalesDisp / globalCurrUnits : 0;
    }, [globalCurrUnits, globalCurrNetSalesDisp]);

    const globalPrevAspDisp = useMemo(() => {
        return globalPrevUnits > 0 ? globalPrevNetSalesDisp / globalPrevUnits : 0;
    }, [globalPrevUnits, globalPrevNetSalesDisp]);


    const globalCurrProfit = useMemo(() => {
        const pUsd = toNumberSafe(uk.profitGBP ?? 0) * gbpToUsd;
        return convertToDisplayCurrency(pUsd, "USD");
    }, [uk.profitGBP, gbpToUsd, convertToDisplayCurrency]);

    const globalPrevProfit = useMemo(() => {
        const prevProfitGbp = toNumberSafe(prev.profit ?? 0);
        const pUsd = prevProfitGbp * gbpToUsd;
        return convertToDisplayCurrency(pUsd, "USD");
    }, [prev.profit, gbpToUsd, convertToDisplayCurrency]);

    const globalDeltas = useMemo(() => {
        return {
            units: safeDeltaPct(globalCurrUnits, globalPrevUnits),
            sales: safeDeltaPct(globalCurrSalesDisp, globalPrevSalesDisp),
            asp: safeDeltaPct(globalCurrAsp, globalPrevAsp),
            profit: safeDeltaPct(globalCurrProfit, globalPrevProfit),
            profitPct: null as number | null,
        };
    }, [
        globalCurrUnits,
        globalPrevUnits,
        globalCurrSalesDisp,
        globalPrevSalesDisp,
        globalCurrAsp,
        globalPrevAsp,
        globalCurrProfit,
        globalPrevProfit,
    ]);

    const globalCurrGrossDisp = useMemo(() => {
        return convertToDisplayCurrency(combinedGrossUSD, "USD");
    }, [combinedGrossUSD, convertToDisplayCurrency]);

    const globalPrevGrossDisp = useMemo(() => {
        const prevAmazonGrossUSD = toNumberSafe(prev.grossSales) * gbpToUsd;
        const prevShopifyUSD = toNumberSafe(shopifyPrevDeriv?.netSales) * inrToUsd;
        return convertToDisplayCurrency(prevAmazonGrossUSD + prevShopifyUSD, "USD");
    }, [prev.grossSales, gbpToUsd, shopifyPrevDeriv?.netSales, inrToUsd, convertToDisplayCurrency]);

    /* ===================== RENDER FLAGS ===================== */
    const hasAnyGraphData = amazonIntegrated || shopifyIntegrated;
    const hasGlobalCard = shouldShowDummyUi ? true : !noIntegrations;
    const hasAmazonCard = shouldShowDummyUi ? true : amazonIntegrated;
    const hasShopifyCard = !shopifyNotConnected;
    const leftColumnHeightClass = !hasShopifyCard ? "lg:min-h-[520px]" : "";
    const prevShort = getShort(biPeriods?.previous?.label);
    const currShort = getShort(biPeriods?.current_mtd?.label);
    const rangeCurrency = currencyForCountry(countryName);
    const identityConvert = useCallback((v: number, _from?: any) => v, []);
    const reimbursementHome = useMemo(() => {
        const currRaw = toNumberSafe(derived?.current_net_reimbursement ?? 0);
        const prevRaw = toNumberSafe(
            data?.previous_period?.totals?.previous_net_reimbursement ?? 0
        );

        return {
            current: convertToDisplayCurrency(currRaw, amazonDataCurrency),
            previous: convertToDisplayCurrency(prevRaw, amazonDataCurrency),
            deltaPct: safeDeltaPct(
                convertToDisplayCurrency(currRaw, amazonDataCurrency),
                convertToDisplayCurrency(prevRaw, amazonDataCurrency)
            ),
        };
    }, [
        derived?.current_net_reimbursement,
        data?.previous_period?.totals?.previous_net_reimbursement,
        convertToDisplayCurrency,
        amazonDataCurrency,
    ]);

    // const targetData = regions[targetRegion] || regions.Global;

    // const stats_mtdHome = identityConvert(targetData.mtdUSD ?? 0);
    // const stats_lastMtdHome = identityConvert(targetData.lastMonthToDateUSD ?? 0);
    // const stats_lastMonthTotalHome = identityConvert(targetData.lastMonthTotalUSD ?? 0);
    // const stats_targetHome = identityConvert(targetData.targetUSD ?? 0);

    const selectedTargetRegionKey: RegionKey = isCountryMode ? forcedRegion : targetRegion;

    const selectedTargetRegion =
        regions[selectedTargetRegionKey] ||
        regions[forcedRegion] ||
        regions.Global;

    const targetData: RegionMetrics = {
        mtdUSD:
            selectedTargetRegion?.mtdUSD ||
            amazonCurrNetDisp ||
            0,

        lastMonthToDateUSD:
            selectedTargetRegion?.lastMonthToDateUSD ||
            amazonPrevNetDisp ||
            0,

        lastMonthTotalUSD:
            selectedTargetRegion?.lastMonthTotalUSD ||
            prevFullMonthNetSalesDisp ||
            amazonPrevNetDisp ||
            0,

        targetUSD:
            selectedTargetRegion?.targetUSD ||
            userMonthlyTargetHome ||
            0,

        decTargetUSD:
            selectedTargetRegion?.decTargetUSD ||
            selectedTargetRegion?.targetUSD ||
            userMonthlyTargetHome ||
            0,
    };

    const stats_mtdHome = targetData.mtdUSD;
    const stats_lastMtdHome = targetData.lastMonthToDateUSD;
    const stats_lastMonthTotalHome = targetData.lastMonthTotalUSD;
    const stats_targetHome = targetData.targetUSD;

    // const grandTotalRow = data?.skuwise_items?.find(
    //     (item: any) =>
    //         item.product_name === "Grand Total" ||
    //         item.sku === "GRAND_TOTAL"
    // );

    // const row = grandTotalRowRaw;

    // const rawAdsSpend = toNumber(row?.ads_spend ?? 0);
    // const rawBrandSpend = toNumber(row?.brand_spend ?? 0);
    // const rawDealVouchers = toNumber(row?.dealsvouchar_ads ?? 0);
    // const rawPlatformFee = toNumber(row?.platform_fee ?? 0);
    // const rawProfit = toNumber(row?.profit ?? 0);

    // const rawCostOfAds = Math.abs(rawBrandSpend - rawDealVouchers);
    // const rawAdsSpendTotal = Math.abs(rawAdsSpend + rawCostOfAds);

    // const rawCm2Profit = rawProfit - rawAdsSpendTotal - Math.abs(rawPlatformFee);

    const globalBottomCards = useMemo(() => {
        const currentCostOfAds = costOfAds;
        const currentCm2Profit = cm2Profit;

        const salesBase =
            platform === "global"
                ? globalCurrNetSalesDisp
                : convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");

        const currentTacos = salesBase > 0 ? (currentCostOfAds / salesBase) * 100 : 0;
        const currentCm2Pct = salesBase > 0 ? (currentCm2Profit / salesBase) * 100 : 0;

        return {
            currentCostOfAds,
            currentTacos,
            currentCm2Profit,
            currentCm2Pct,
        };
    }, [
        costOfAds,
        cm2Profit,
        platform,
        globalCurrNetSalesDisp,
        uk.netSalesGBP,
        convertToDisplayCurrency,
    ]);

    const tacosPctForSummary = useMemo(() => {
        const salesBase =
            platform === "global"
                ? globalCurrNetSalesDisp
                : convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");

        return salesBase > 0 ? (adsSpendTotal / salesBase) * 100 : 0;
    }, [
        platform,
        adsSpendTotal,
        globalCurrNetSalesDisp,
        uk.netSalesGBP,
        convertToDisplayCurrency,
    ]);

    const buildDashboardCachePayload = useCallback(() => {
        const storedLastRefreshAt =
            typeof window !== "undefined"
                ? Number(localStorage.getItem(lastRefreshKey))
                : NaN;

        const finalLastRefreshAt =
            Number.isFinite(storedLastRefreshAt)
                ? storedLastRefreshAt
                : lastRefreshAt;

        return {
            data,
            adsSpendTotal,
            cm2Profit,
            grandTotalProfit: toNumber(grandTotalRowDisplay?.profit),
            grandTotalPlatformFee: toNumber(grandTotalRowDisplay?.platform_fee),
            shipmentCharges: toNumber(plSummaryTotals?.shipment_charges ?? 0),

            biDailySeries,
            biPeriods,
            liveBiPayload,
            biAlignedTotals,

            invRows,
            inventoryAlerts,
            monthlySpRows,
            monthlySpTotalSpend,

            targetSummaries,
            prevTargetSummaries,
            shopifyRows,
            shopifyPrevRows,
            previousSkuwiseGlobalData,
            globalCountryPayloads,

            liveBiReady,
            biStatus,

            // ✅ this is the important part
            lastRefreshAt: finalLastRefreshAt,
            savedAt: Date.now(),
        };
    }, [
        data,
        adsSpendTotal,
        cm2Profit,
        grandTotalRowDisplay?.profit,
        grandTotalRowDisplay?.platform_fee,
        plSummaryTotals?.shipment_charges,
        biDailySeries,
        biPeriods,
        liveBiPayload,
        biAlignedTotals,
        invRows,
        inventoryAlerts,
        monthlySpRows,
        monthlySpTotalSpend,
        targetSummaries,
        prevTargetSummaries,
        shopifyRows,
        shopifyPrevRows,
        previousSkuwiseGlobalData,
        globalCountryPayloads,
        liveBiReady,
        biStatus,
        lastRefreshAt,
        lastRefreshKey,
    ]);

    const saveLiveCacheToLocalStorage = useCallback((cachePayload?: any) => {
        if (typeof window === "undefined") return;

        const payloadToSave = cachePayload ?? buildDashboardCachePayload();

        localStorage.setItem(
            liveCacheKey,
            JSON.stringify({
                ...payloadToSave,
                savedAt: Date.now(),
            })
        );
    }, [buildDashboardCachePayload, liveCacheKey]);

    // useEffect(() => {
    //     if (typeof window === "undefined") return;
    //     if (platform !== "global") return;

    //     localStorage.removeItem(liveCacheKey);
    //     localStorage.removeItem("live-dashboard-cache:global:Global");
    // }, [platform, liveCacheKey]);


    useEffect(() => {
        if (!shouldPostCacheRef.current || !isManualRefreshRef.current) return;
        if (!data && !liveBiPayload && !invRows.length) return;

        const shouldPersist =
            !pageLoading &&
            !dashboardBusy &&
            !loading &&
            !biLoading &&
            !invLoading &&
            !monthlySpLoading &&
            !shopifyLoading;

        if (!shouldPersist) return;

        const payload = buildDashboardCachePayload();

        try {
            localStorage.setItem(
                liveCacheKey,
                JSON.stringify({
                    ...payload,
                    savedAt: Date.now(),
                })
            );

            localStorage.setItem("live-dashboard-cache-init", "initialized");
        } catch (e) {
            console.error("Failed to write local cache:", e);
        }

        saveDashboardCacheToBackend(payload)
            .then(() => {
                shouldPostCacheRef.current = false;
                isManualRefreshRef.current = false;
            })
    }, [
        buildDashboardCachePayload,
        saveDashboardCacheToBackend,
        liveCacheKey,
        lastRefreshKey,
        pageLoading,
        dashboardBusy,
        loading,
        biLoading,
        invLoading,
        monthlySpLoading,
        shopifyLoading,
        data,
        liveBiPayload,
        invRows,
        monthlySpRows,
        monthlySpTotalSpend,
        cacheSaveTick,
    ]);

    const { todayDay: statsTodayDay } = getRegionDayInfo(activeDateRegion);

    const stats_todayHome =
        typeof todaySalesRaw === "number" && !Number.isNaN(todaySalesRaw)
            ? todaySalesRaw
            : statsTodayDay > 0
                ? stats_mtdHome / statsTodayDay
                : 0;

    const stats_salesTrendPct =
        stats_lastMtdHome > 0
            ? ((stats_mtdHome - stats_lastMtdHome) / stats_lastMtdHome) * 100
            : 0;

    const getDaysInMonthByRegion = (region: RegionKey) => {
        const now = getRegionNow(region);
        return new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    };

    const todayByRegion = dashboardAllowedDay;
    const daysInMonthByRegion = dashboardDaysInMonth;

    const proratedTargetToDate = (daysInMonthByRegion > 0)
        ? (todayByRegion / daysInMonthByRegion) * stats_targetHome  // x
        : 0;

    const stats_targetTrendPct =
        stats_targetHome > 0
            ? ((stats_mtdHome - proratedTargetToDate) / stats_targetHome) * 100
            : 0;

    const stats_targetTrendPrevPct = useMemo(() => {
        return stats_targetHome > 0
            ? ((stats_lastMtdHome - proratedTargetToDate) / stats_targetHome) * 100
            : 0;
    }, [stats_lastMtdHome, proratedTargetToDate, stats_targetHome]);

    const ADS_SIGN_PLUS = new Set(["net_sales", "credits", "tax_and_credits", "quantity", "total_quantity"]);

    const ADS_SIGN_MINUS = new Set([
        "return_quantity",
        "ads_spend",
        "product_spend",
        "display_spend",
        "brand_spend",
        "cogs",
        "fba_fees",
        "selling_fees",
        "marketplace_total",
        "tax",
    ]);

    const getAdsSignForCol = useCallback((colKey: string) => {
        if (ADS_SIGN_PLUS.has(colKey)) return { text: "(+)", className: "text-green-700" };
        if (ADS_SIGN_MINUS.has(colKey)) return { text: "(-)", className: "text-[#ff5c5c]" };
        return null;
    }, []);


    const skuwiseItems = useMemo(() => {
        const items = (data as any)?.skuwise_items;
        return Array.isArray(items) ? items : [];
    }, [data]);

    const idxAds = useMemo(() => labels.findIndex((l) => l === "Advertisements"), [labels]);
    const idxOthers = useMemo(() => labels.findIndex((l) => l === "Others"), [labels]);
    const idxCm2 = useMemo(() => labels.findIndex((l) => l === "CM2 Profit"), [labels]);

    const idxNetSales = useMemo(() => labels.findIndex((l) => l === "Net Sales"), [labels]);
    const idxCogs = useMemo(() => labels.findIndex((l) => l === "COGS"), [labels]);

    const idxTax = useMemo(() => labels.findIndex((l) => l === "Tax"), [labels]);
    const idxCredits = useMemo(() => labels.findIndex((l) => l === "Credits"), [labels]);
    const idxTaxAndCredits = useMemo(
        () => labels.findIndex((l) => l === "Tax & Credits" || l === "Taxes & Credits"),
        [labels]
    );
    const idxCm1Profit = useMemo(() => labels.findIndex((l) => l === "CM1 Profit"), [labels]);

    const valuesPatched = useMemo(() => {
        const copy = [...values];

        const totalRow =
            monthlySkuwiseRowsDisplay.find((r) => r.isTotal) ??
            monthlySkuwiseRowsDisplay.find((r) => String(r.sku || "").toUpperCase() === "GRAND_TOTAL") ??
            monthlySkuwiseRowsDisplay.find((r) => String(r.sku || "").toUpperCase() === "TOTAL") ??
            monthlySkuwiseRowsDisplay.find(
                (r) => String(r.product_name || "").trim().toLowerCase() === "grand total"
            ) ??
            monthlySkuwiseRowsDisplay.find(
                (r) => String(r.product_name || "").trim().toLowerCase() === "total"
            ) ??
            monthlySkuwiseRowsDisplay[monthlySkuwiseRowsDisplay.length - 1];

        if (idxNetSales !== -1) {
            copy[idxNetSales] = toNumberSafe(totalRow?.net_sales ?? 0);
        }

        if (idxCogs !== -1) {
            copy[idxCogs] = Math.abs(toNumberSafe(totalRow?.cogs ?? 0));
        }

        if (idxTax !== -1) {
            copy[idxTax] = Math.abs(toNumberSafe(totalRow?.tax ?? 0));
        }

        if (idxCredits !== -1) {
            copy[idxCredits] = Math.abs(toNumberSafe(totalRow?.credits ?? 0));
        }

        if (idxTaxAndCredits !== -1) {
            copy[idxTaxAndCredits] = Math.abs(toNumberSafe(totalRow?.tax_and_credits ?? 0));
        }

        if (idxCm1Profit !== -1) {
            copy[idxCm1Profit] = toNumberSafe(totalRow?.profit ?? 0);
        }

        if (idxAds !== -1) copy[idxAds] = Number(adsSpendTotal ?? 0);
        if (idxOthers !== -1) copy[idxOthers] = Math.abs(Number(platformFee ?? 0));
        if (idxCm2 !== -1) {
            copy[idxCm2] = Number(
                toNumberSafe(
                    totalRow?.total_cm2_profit ??
                    grandTotalRowRaw?.total_cm2_profit ??
                    grandTotalRowDisplay?.total_cm2_profit ??
                    cm2Profit ??
                    totalRow?.cm2_profit ??
                    0
                )
            );
        }

        return copy;
    }, [
        values,
        monthlySkuwiseRowsDisplay,
        idxNetSales,
        idxCogs,
        idxTax,
        idxCredits,
        idxTaxAndCredits,
        idxCm1Profit,
        idxAds,
        idxOthers,
        idxCm2,
        adsSpendTotal,
        platformFee,
        cm2Profit,
        grandTotalRowRaw?.total_cm2_profit,
        grandTotalRowDisplay?.total_cm2_profit,
    ]);

    const targetKpisFromBi = useMemo(() => {
        if (!rangeActive || !liveBiPayload) return null;

        return {
            todayHome: liveBiPayload.today_sales_home ?? 0,
            mtdHome: liveBiPayload.period_sales_home ?? 0, // range total (or MTD if no range)
            lastMonthTotalHome: liveBiPayload.last_month_total_home ?? 0,
            lastMonthToDateHome: liveBiPayload.last_month_to_date_home ?? 0,

            reimbursement: {
                current: liveBiPayload.reimbursement_current_home ?? 0,
                previous: liveBiPayload.reimbursement_previous_home ?? 0,
            },
        };
    }, [rangeActive, liveBiPayload]);

    const readCountryCache = useCallback(
        (country: "uk" | "us") => {
            const fromState = globalCountryPayloads[country];
            if (fromState) return fromState;

            if (typeof window === "undefined") return null;

            const key = `live-dashboard-cache:${country}:${country.toUpperCase()}:${selectedStartDay ?? "na"}:${selectedEndDay ?? "na"}`;
            const raw = localStorage.getItem(key);

            if (!raw) return null;

            try {
                return JSON.parse(raw);
            } catch {
                return null;
            }
        },
        [globalCountryPayloads, selectedStartDay, selectedEndDay]
    );

    // const targets_todayHome = stats_todayHome;
    // const targets_mtdHome = targetKpisFromBi ? targetKpisFromBi.mtdHome : stats_mtdHome;
    // const targets_lastMonthTotalHome = targetKpisFromBi ? targetKpisFromBi.lastMonthTotalHome : stats_lastMonthTotalHome;
    // const targets_lastMonthToDateHome = targetKpisFromBi ? targetKpisFromBi.lastMonthToDateHome : stats_lastMtdHome;

    // const targets_reimbursement = targetKpisFromBi ? targetKpisFromBi.reimbursement : reimbursementHome;

    const targets_todayHome = stats_todayHome;

    const targets_mtdHome =
        platform === "global"
            ? globalTargetCardTotals.currentNetSales
            : targetKpisFromBi?.mtdHome || stats_mtdHome;

    const targets_lastMonthTotalHome =
        platform === "global"
            ? globalTargetCardTotals.previousNetSalesFullMonth
            : targetKpisFromBi?.lastMonthTotalHome || stats_lastMonthTotalHome;

    const targets_lastMonthToDateHome =
        platform === "global"
            ? globalTargetCardTotals.previousNetSales
            : targetKpisFromBi?.lastMonthToDateHome || stats_lastMtdHome;

    const targets_reimbursement = useMemo(() => {
        if (platform === "global") {
            return {
                current: globalTargetCardTotals.currentReimbursement,
                previous: globalTargetCardTotals.previousReimbursement,
            };
        }

        return targetKpisFromBi?.reimbursement || reimbursementHome;
    }, [
        platform,
        targetKpisFromBi,
        reimbursementHome,
        globalTargetCardTotals.currentReimbursement,
        globalTargetCardTotals.previousReimbursement,
    ]);

    const cm2MarginPctForSummary = useMemo(() => {
        return totalRowCm2Margins;
    }, [totalRowCm2Margins]);

    const tacosFromDisplayedCardsForSummary = useMemo(() => {
        if (platform === "global") {
            return toNumber(
                plSummaryTotals.acos ??
                (data as any)?.derived_totals?.tacos_total_advertising_cost_of_sale ??
                (data as any)?.derived_totals?.tacos ??
                (data as any)?.tacos_total_advertising_cost_of_sale
            );
        }

        const netSalesFromCard = curr.netSales;
        const adsFromCard = adsSpendTotal;

        return netSalesFromCard > 0
            ? (Math.abs(adsFromCard) / Math.abs(netSalesFromCard)) * 100
            : 0;
    }, [
        platform,
        plSummaryTotals.acos,
        data,
        curr.netSales,
        adsSpendTotal,
    ]);

    const reimbursementForSummary = useMemo(() => {
        if (platform === "global") {
            return toNumber(
                plSummaryTotals.net_reimbursement ??
                (data as any)?.derived_totals?.current_net_reimbursement ??
                (data as any)?.current_net_reimbursement
            );
        }

        return toNumber(targets_reimbursement?.current);
    }, [
        platform,
        plSummaryTotals.net_reimbursement,
        data,
        targets_reimbursement?.current,
    ]);

    const reimbursementVsCm2PctForSummary = useMemo(() => {
        if (platform === "global") {
            return toNumber(
                plSummaryTotals.rembursment_vs_cm2_margins ??
                (data as any)?.derived_totals?.reimbursement_vs_cm2_margins ??
                (data as any)?.reimbursement_vs_cm2_margins
            );
        }

        const cm2 = toNumber(cm2Profit);
        return cm2 ? (reimbursementForSummary / cm2) * 100 : 0;
    }, [
        platform,
        plSummaryTotals.rembursment_vs_cm2_margins,
        data,
        reimbursementForSummary,
        cm2Profit,
    ]);

    const reimbursementVsSalesPctForSummary = useMemo(() => {
        if (platform === "global") {
            return toNumber(
                plSummaryTotals.reimbursement_vs_sales ??
                (data as any)?.derived_totals?.reimbursement_vs_sales ??
                (data as any)?.reimbursement_vs_sales
            );
        }

        const netSales = toNumber(plSummaryTotals.net_sales) || toNumber(stats_mtdHome);
        return netSales ? (reimbursementForSummary / netSales) * 100 : 0;
    }, [
        platform,
        plSummaryTotals.reimbursement_vs_sales,
        plSummaryTotals.net_sales,
        data,
        stats_mtdHome,
        reimbursementForSummary,
    ]);

    const rangeCompletedPct = useMemo(() => {
        if (selectedStartDay && selectedEndDay) {
            const safeEndDay = Math.min(selectedEndDay, dashboardAllowedDay);
            const safeStartDay = Math.min(selectedStartDay, safeEndDay);
            const completedDays = safeEndDay - safeStartDay + 1;

            return dashboardDaysInMonth > 0
                ? (completedDays / dashboardDaysInMonth) * 100
                : 0;
        }

        return dashboardDaysInMonth > 0
            ? (dashboardAllowedDay / dashboardDaysInMonth) * 100
            : 0;
    }, [
        selectedStartDay,
        selectedEndDay,
        dashboardAllowedDay,
        dashboardDaysInMonth,
    ]);

    type TopTab =
        | "live"
        | "summary"
        | "productwise"
        | "inventory";

    const TOP_TABS: { id: TopTab; label: string }[] = [
        { id: "live", label: "MTD Sales" },
        { id: "summary", label: "AI Insights & Recommendations" },
        { id: "productwise", label: "P&L Breakdown" },
        { id: "inventory", label: "Current Inventory" },
    ];

    const HASH_TO_TAB: Record<string, TopTab> = {
        "live-sales": "live",
        "ai-insights": "summary",
        // "mtd-pl": "productwise",
        "pnl-mtd": "productwise",
        "current-inventory": "inventory",
    };

    const TAB_TO_HASH: Record<TopTab, string> = {
        live: "live-sales",
        summary: "ai-insights",
        productwise: "pnl-mtd",
        inventory: "current-inventory",
    };

    const handleConnectAmazonPreview = () => {
        router.push(`/profile/${countryName}/NA/NA`);
    };

    const handleHashNavigation = useCallback((rawHash?: string) => {
        if (typeof window === "undefined") return;

        const hash = (rawHash ?? window.location.hash).replace("#", "");
        if (!hash) return;

        const targetTab = HASH_TO_TAB[hash];
        if (!targetTab) return;

        setPendingHash(hash);
        setActiveTab(targetTab);
    }, []);

    useEffect(() => {
        if (typeof window === "undefined") return;

        const onHashChange = () => {
            handleHashNavigation(window.location.hash);
        };

        const onCustomHashNavigate = (event: Event) => {
            const customEvent = event as CustomEvent<{ hash?: string }>;
            if (!customEvent.detail?.hash) return;
            handleHashNavigation(`#${customEvent.detail.hash}`);
        };

        handleHashNavigation(window.location.hash);

        window.addEventListener("hashchange", onHashChange);
        window.addEventListener("page-hash-navigate", onCustomHashNavigate as EventListener);

        return () => {
            window.removeEventListener("hashchange", onHashChange);
            window.removeEventListener("page-hash-navigate", onCustomHashNavigate as EventListener);
        };
    }, [handleHashNavigation]);

    useEffect(() => {
        if (!pendingHash) return;

        const timer = setTimeout(() => {
            const el = document.getElementById(pendingHash);
            if (el) {
                el.scrollIntoView({
                    behavior: "smooth",
                    block: "start",
                });
            }
            setPendingHash("");
        }, 250);

        return () => clearTimeout(timer);
    }, [activeTab, pendingHash]);

    const dummyStatData = {
        units: { current: 0, previous: 0, deltaPct: 0 },
        grossSales: { current: 0, previous: 0, deltaPct: 0 },
        netSales: { current: 0, previous: 0, deltaPct: 0 },
        asp: { current: 0, previous: 0, deltaPct: 0 },
        costOfAds: { current: 0, previous: 0, deltaPct: 0 },
        tacos: { current: 0, previous: 0, deltaPct: 0 },
        cm2Profit: { current: 0, previous: 0, deltaPct: 0 },
        cm2ProfitPct: { current: 0, previous: 0, deltaPct: 0 },
    };

    const dummyLiveBusinessClientData: ApiResponse & {
        portfolio_recommendation?: string;
        objective_context?: {
            growth_intent: string;
            inventory_clearance_priority: boolean;
            profit_priority: string;
        };
        ads_recommendation?: string;
        inventory_summary?: {
            alert_bullets: string[];
            summary_text: string;
        };
        recommended_actions_mtd?: Record<string, string>;
        remaining_skus_block?: string;
        categorized_growth?: any;
        ai_insights?: Record<string, any>;
    } = {
        periods: {
            previous: {
                label: "Feb MTD",
                start_date: "2026-02-01",
                end_date: "2026-02-05",
            },
            current_mtd: {
                label: "Mar MTD",
                start_date: "2026-03-01",
                end_date: "2026-03-05",
            },
        },

        overall_summary: [
            "Business delivered healthy month-to-date growth driven by stronger unit movement, better sales conversion, and stable contribution margins across core SKUs.",
        ],

        overall_actions: [
            "Increase focus on top-performing SKUs with strong unit velocity and stable profitability.",
            "Reduce inefficient ad spend on low-contribution SKUs.",
            "Monitor low-stock products to avoid losing momentum during the current period.",
        ],

        portfolio_recommendation:
            "Keep scaling winning SKUs while protecting margin on mid-tier products and correcting slow-moving inventory pockets.",

        objective_context: {
            growth_intent: "aggressive",
            inventory_clearance_priority: true,
            profit_priority: "protect_growth",
        },

        ads_recommendation:
            "Shift budget toward high-conversion SKUs and reduce exposure on products with weak sales-to-spend efficiency.",

        inventory_summary: {
            alert_bullets: [
                "Dummy Product 2 is approaching low stock threshold.",
                "Dummy Product 3 is in high alert and may require urgent replenishment.",
            ],
            summary_text:
                "Inventory health is mixed. A few hero SKUs are healthy, but 2 products need replenishment attention to avoid sales loss.",
        },

        recommended_actions_mtd: {
            "1": `Dummy Product 1
Units: 0
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
Dummy Product 1 has maintained steady momentum and gained share through stronger conversion and stable pricing.

Recommendation:
Continue scaling this SKU with controlled inventory support and sustained ad investment.

Advertising:
Increase spend moderately on top-performing keywords and branded placements.

Inventory:
Maintain healthy stock cover and avoid under-ordering during high velocity weeks.`,

            "2": `Dummy Product 2
Units: 0 (0%)
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
Dummy Product 2 is growing, though ASP softness indicates price pressure.

Recommendation:
Protect profitability by limiting discount dependency and improving listing conversion.

Advertising:
Reduce non-performing spend and prioritize campaigns with stronger ROAS.

Inventory:
Replenish conservatively and align purchase planning with recent sell-through.`,

            "3": `Dummy Product 3
Units: 0 (0%)
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
The remaining SKU portfolio is contributing moderate growth but with weaker profitability consistency.

Recommendation:
Clean up weak SKUs, focus on margin-positive products, and improve assortment efficiency.

Advertising:
Consolidate spend toward better converting products and pause weak ad groups.

Inventory:
Use tighter replenishment rules on slow-moving items to avoid inventory drag.`,
        },


        remaining_skus_block: `Other SKUs
Units: 0 (0%)
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
The remaining SKU portfolio is contributing moderate growth but with weaker profitability consistency.

Recommendation:
Clean up weak SKUs, focus on margin-positive products, and improve assortment efficiency.

Advertising:
Consolidate spend toward better converting products and pause weak ad groups.

Inventory:
Use tighter replenishment rules on slow-moving items to avoid inventory drag.`,

        categorized_growth: {
            top_80_skus: [
                {
                    product_name: "Dummy Product 1",
                    sku: "DUMMY-SKU-001",
                    quantity_month1: 0,
                    quantity_month2: 0,
                    asp_month1: 0,
                    asp_month2: 0,
                    product_sales_month1: 0,
                    product_sales_month2: 0,
                    net_sales_month1: 0,
                    net_sales_month2: 0,
                    sales_mix_month1: 0,
                    sales_mix_month2: 0,
                    unit_wise_profitability_month1: 0,
                    unit_wise_profitability_month2: 0,
                    profit_month1: 0,
                    profit_month2: 0,
                    profit_percentage_month1: 0,
                    profit_percentage_month2: 0,
                    "Sales Mix (Month2)": 0,
                    "Unit Growth": { category: "growth", value: 0 },
                    "ASP Growth": { category: "growth", value: 0 },
                    "Sales Growth": { category: "growth", value: 0 },
                    "Net Sales Growth": { category: "growth", value: 0 },
                    "Sales Mix Change": { category: "growth", value: 0 },
                    "Profit Per Unit": { category: "growth", value: 0 },
                    "CM1 Profit Impact": { category: "growth", value: 0 },
                },
                {
                    product_name: "Dummy Product 2",
                    sku: "DUMMY-SKU-002",
                    quantity_month1: 0,
                    quantity_month2: 0,
                    asp_month1: 0,
                    asp_month2: 0,
                    product_sales_month1: 0,
                    product_sales_month2: 0,
                    net_sales_month1: 0,
                    net_sales_month2: 0,
                    sales_mix_month1: 0,
                    sales_mix_month2: 0,
                    unit_wise_profitability_month1: 0,
                    unit_wise_profitability_month2: 0,
                    profit_month1: 0,
                    profit_month2: 0,
                    profit_percentage_month1: 0,
                    profit_percentage_month2: 0,
                    "Sales Mix (Month2)": 0,
                    "Unit Growth": { category: "growth", value: 0 },
                    "ASP Growth": { category: "growth", value: 0 },
                    "Sales Growth": { category: "growth", value: 0 },
                    "Net Sales Growth": { category: "growth", value: 0 },
                    "Sales Mix Change": { category: "growth", value: 0 },
                    "Profit Per Unit": { category: "growth", value: 0 },
                    "CM1 Profit Impact": { category: "growth", value: 0 },
                },
            ],

            new_or_reviving_skus: [
                {
                    product_name: "Dummy Product 3",
                    sku: "DUMMY-SKU-003",
                    quantity_month1: 0,
                    quantity_month2: 0,
                    asp_month1: 0,
                    asp_month2: 0,
                    product_sales_month1: 0,
                    product_sales_month2: 0,
                    net_sales_month1: 0,
                    net_sales_month2: 0,
                    sales_mix_month1: 0,
                    sales_mix_month2: 0,
                    unit_wise_profitability_month1: 0,
                    unit_wise_profitability_month2: 0,
                    profit_month1: 0,
                    profit_month2: 0,
                    profit_percentage_month1: 0,
                    profit_percentage_month2: 0,
                    "Sales Mix (Month2)": 0,
                },
            ],

            other_skus: [],

            top_80_total: {
                product_name: "Total",
                "Sales Mix (Month2)": 0,
                "Unit Growth": { category: "growth", value: 0 },
                "ASP Growth": { category: "growth", value: 0 },
                "Sales Growth": { category: "growth", value: 0 },
                "Net Sales Growth": { category: "growth", value: 0 },
                "Sales Mix Change": { category: "growth", value: 0 },
                "Profit Per Unit": { category: "growth", value: 0 },
                "CM1 Profit Impact": { category: "growth", value: 0 },
            },

            new_or_reviving_total: {
                product_name: "Total",
                "Sales Mix (Month2)": 0,
            },

            other_total: {
                product_name: "Total",
                "Sales Mix (Month2)": 0,
                "Unit Growth": { category: "growth", value: 0 },
                "ASP Growth": { category: "growth", value: 0 },
                "Sales Growth": { category: "growth", value: 0 },
                "Net Sales Growth": { category: "growth", value: 0 },
                "Sales Mix Change": { category: "growth", value: 0 },
                "Profit Per Unit": { category: "growth", value: 0 },
                "CM1 Profit Impact": { category: "growth", value: 0 },
            },

            all_skus_total: {
                product_name: "Total",
                "Sales Mix (Month2)": 0,
                "Unit Growth": { category: "growth", value: 0 },
                "ASP Growth": { category: "growth", value: 0 },
                "Sales Growth": { category: "growth", value: 0 },
                "Net Sales Growth": { category: "growth", value: 0 },
                "Sales Mix Change": { category: "growth", value: 0 },
                "Profit Per Unit": { category: "growth", value: 0 },
                "CM1 Profit Impact": { category: "growth", value: 0 },
            },
        },

        ai_insights: {
            "DUMMY-SKU-001": {
                product_name: "Dummy Product 1",
                sku: "DUMMY-SKU-001",
                insight: `Product Journey:
Dummy Product 1 has shown strong and stable growth with better unit movement and improved contribution.

Recommendation:
Scale this SKU further with controlled aggressiveness and maintain inventory readiness.

Advertising:
Increase bids on best-performing search terms and keep branded traffic protected.

Inventory:
Maintain healthy stock cover and monitor weekly velocity.`,
            },

            "DUMMY-SKU-002": {
                product_name: "Dummy Product 2",
                sku: "DUMMY-SKU-002",
                insight: `Product Journey:
Dummy Product 2 continues to grow, though ASP pressure suggests pricing sensitivity in the current period.

Recommendation:
Improve listing efficiency and protect margin instead of chasing aggressive discount-led growth.

Advertising:
Reduce weak campaign spend and reallocate to higher-conversion traffic.

Inventory:
Plan replenishment conservatively with close watch on stock aging.`,
            },

            "DUMMY-SKU-003": {
                product_name: "Dummy Product 3",
                sku: "DUMMY-SKU-003",
                insight: `Product Journey:
Dummy Product 3 is a new or reviving SKU and has started contributing positively to sales and profit.

Recommendation:
Support early momentum but validate consistency before scaling too aggressively.

Advertising:
Test campaigns in a measured way and double down only where conversion quality is stable.

Inventory:
Keep enough stock for validation but avoid over-committing too early.`,
            },
        },
    };

    const dummyBiDailySeriesHome: GraphDailySeries = {
        previous: [
            { date: "2026-03-01", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-02", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-03", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-04", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-05", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        ],
        current_mtd: [
            { date: "2026-03-01", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-02", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-03", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-04", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
            { date: "2026-03-05", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        ],
    };

    const dummyBiPeriods = {
        previous: {
            label: "Feb MTD",
            start_date: "2026-02-01",
            end_date: "2026-02-05",
        },
        current_mtd: {
            label: "Mar MTD",
            start_date: "2026-03-01",
            end_date: "2026-03-05",
        },
    };

    const dummyMonthlySkuwiseRowsForTable: MonthlySkuwiseTableRow[] = [
        {
            sno: 1,
            sku: "SKU-001",
            product_name: "Dummy Product 1",
            quantity: 0,
            asp: 0,
            net_sales: 0,
            cogs: 0,
            fba_fees: 0,
            selling_fees: 0,
            tax: 0,
            credits: 0,
            tax_and_credits: 0,
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: 0,
            acos: 0,
            cm2_profit: 0,
            profit: 0,
        },
        {
            sno: 2,
            sku: "SKU-002",
            product_name: "Dummy Product 2",
            quantity: 0,
            asp: 0,
            net_sales: 0,
            cogs: 0,
            fba_fees: 0,
            selling_fees: 0,
            tax: 0,
            credits: 0,
            tax_and_credits: 0,
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: 0,
            acos: 0,
            cm2_profit: 0,
            profit: 0,
        },
        {
            sno: 3,
            sku: "SKU-003",
            product_name: "Dummy Product 3",
            quantity: 0,
            asp: 0,
            net_sales: 0,
            cogs: 0,
            fba_fees: 0,
            selling_fees: 0,
            tax: 0,
            credits: 0,
            tax_and_credits: 0,
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: 0,
            acos: 0,
            cm2_profit: 0,
            profit: 0,
        },
        {
            sno: 4,
            sku: "SKU-004",
            product_name: "Dummy Product 4",
            quantity: 0,
            asp: 0,
            net_sales: 0,
            cogs: 0,
            fba_fees: 0,
            selling_fees: 0,
            tax: 0,
            credits: 0,
            tax_and_credits: 0,
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: 0,
            acos: 0,
            cm2_profit: 0,
            profit: 0,
        },
        {
            sno: 5,
            sku: "SKU-005",
            product_name: "Dummy Product 5",
            quantity: 0,
            asp: 0,
            net_sales: 0,
            cogs: 0,
            fba_fees: 0,
            selling_fees: 0,
            tax: 0,
            credits: 0,
            tax_and_credits: 0,
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: 0,
            acos: 0,
            cm2_profit: 0,
            profit: 0,
        },
        {
            sno: undefined,
            sku: "GRAND_TOTAL",
            product_name: "Total",
            quantity: 0,
            asp: 0,
            net_sales: 0,
            cogs: 0,
            fba_fees: 0,
            selling_fees: 0,
            tax: 0,
            credits: 0,
            tax_and_credits: 0,
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: 0,
            acos: 0,
            cm2_profit: 0,
            profit: 0,
            isTotal: true,
        },
    ];

    const dummySalesTargetStats = {
        todayHome: 0,
        mtdHome: 0,
        targetHome: 0,
        lastMonthTotalHome: 0,
        lastMonthToDateHome: 0,
        salesTrendPct: 0,
        targetTrendPct: 0,
        reimbursement: {
            current: 0,
            previous: 0,
        },
        periodCompletedPct: 0,
    };

    const dummyTargetData = [
        { label: "Amazon US", target: 0, achieved: 0 },
        { label: "Amazon UK", target: 0, achieved: 0 },
        { label: "Shopify", target: 0, achieved: 0 },
    ];

    const dummyCm1ProfitPieData: Cm1PieSlice[] = [
        { name: "Product A", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
        { name: "Product B", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
        { name: "Product C", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
        { name: "Product D", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
        { name: "Others", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
    ];

    const dummyInventoryRows: InventoryRow[] = [
        {
            "S.No": 1,
            "Product Name": "Dummy Product 1",
            "SKU": "DUMMY-SKU-001",
            "MTD Sales": 0,
            "Sales Last 30 Days": 0,
            "Sales Rank": 0,
            "Current Inventory": 0,
            "Inventory 180+ Days": 0,
            "Estimated Storage Cost ($)": 0,
            "Coverage Ratio (In Months)": 0,
            "Inventory Alerts": "High alert",
        } as InventoryRow,
        {
            "S.No": 2,
            "Product Name": "Dummy Product 2",
            "SKU": "DUMMY-SKU-002",
            "MTD Sales": 0,
            "Sales Last 30 Days": 0,
            "Sales Rank": 0,
            "Current Inventory": 0,
            "Inventory 180+ Days": 0,
            "Estimated Storage Cost ($)": 0,
            "Coverage Ratio (In Months)": 0,
            "Inventory Alerts": "High alert",
        } as InventoryRow,
        {
            "S.No": 3,
            "Product Name": "Dummy Product 3",
            "SKU": "DUMMY-SKU-003",
            "MTD Sales": 0,
            "Sales Last 30 Days": 0,
            "Sales Rank": 0,
            "Current Inventory": 0,
            "Inventory 180+ Days": 0,
            "Estimated Storage Cost ($)": 0,
            "Coverage Ratio (In Months)": 0,
            "Inventory Alerts": "High alert",
        } as InventoryRow,
        {
            "S.No": 4,
            "Product Name": "Dummy Product 4",
            "SKU": "DUMMY-SKU-004",
            "MTD Sales": 0,
            "Sales Last 30 Days": 0,
            "Sales Rank": 0,
            "Current Inventory": 0,
            "Inventory 180+ Days": 0,
            "Estimated Storage Cost ($)": 0,
            "Coverage Ratio (In Months)": 0,
            "Inventory Alerts": "High alert",
        } as InventoryRow,
        {
            "S.No": 5,
            "Product Name": "Dummy Product 5",
            "SKU": "DUMMY-SKU-005",
            "MTD Sales": 0,
            "Sales Last 30 Days": 0,
            "Sales Rank": 0,
            "Current Inventory": 0,
            "Inventory 180+ Days": 0,
            "Estimated Storage Cost ($)": 0,
            "Coverage Ratio (In Months)": 0,
            "Inventory Alerts": "High alert",
        } as InventoryRow,
    ];

    const dummyInventoryAlerts: InventoryAlertRecord = {
        "DUMMY-SKU-001": { alert: "High alert", alert_type: "warning" },
        "DUMMY-SKU-002": { alert: "High alert", alert_type: "warning" },
        "DUMMY-SKU-003": { alert: "High alert", alert_type: "error" },
        "DUMMY-SKU-004": { alert: "High alert", alert_type: "error" },
        "DUMMY-SKU-005": { alert: "High alert", alert_type: "error" },
    };

    const syncTabToHash = useCallback((tab: TopTab) => {
        if (typeof window === "undefined") return;

        const hash = TAB_TO_HASH[tab];
        if (!hash) return;

        const nextUrl = `${window.location.pathname}#${hash}`;
        window.history.pushState(null, "", nextUrl);

        window.dispatchEvent(
            new CustomEvent("page-hash-navigate", {
                detail: { hash },
            })
        );
    }, []);

    const isStickyGlobal = platform === "global";

    const roundedMoneyFormatter = (value: number) => {
        const rounded = Math.round(Number(value) || 0);
        return `$${rounded.toLocaleString()}`;
    };

    const stickyTargetHome =
        stats_targetHome && stats_targetHome > 0
            ? stats_targetHome
            : targets_lastMonthTotalHome && targets_lastMonthTotalHome > 0
                ? targets_lastMonthTotalHome
                : stats_lastMonthTotalHome && stats_lastMonthTotalHome > 0
                    ? stats_lastMonthTotalHome
                    : 0;

    const stickyTargetProratedToDate =
        daysInMonthByRegion > 0
            ? (todayByRegion / daysInMonthByRegion) * stickyTargetHome
            : 0;

    const stickyTargetTrendPct =
        shouldShowDummyUi
            ? dummySalesTargetStats.targetTrendPct
            : stickyTargetHome > 0
                ? ((targets_mtdHome / stickyTargetHome) * 100) - rangeCompletedPct
                : 0;

    const stickyTargetTrendPrevPct =
        shouldShowDummyUi
            ? 0
            : stickyTargetHome > 0
                ? ((targets_lastMonthToDateHome / stickyTargetHome) * 100) - rangeCompletedPct
                : 0;

    const prevMonthTargetHome = useMemo(() => {
        if (platform === "global") {
            const ukPrevTarget = toNumberSafe(prevTargetSummaries.uk?.target_sales ?? 0);
            const usPrevTarget = toNumberSafe(prevTargetSummaries.us?.target_sales ?? 0);

            return ukPrevTarget * gbpToUsd + usPrevTarget;
        }

        return toNumberSafe(
            prevTargetSummaries[
                targetSummaryCountry as keyof typeof prevTargetSummaries
            ]?.target_sales ?? 0
        );
    }, [platform, prevTargetSummaries, targetSummaryCountry, gbpToUsd]);

    const stickyPreviousTargetHome =
        prevMonthTargetHome && prevMonthTargetHome > 0
            ? prevMonthTargetHome
            : targets_lastMonthTotalHome && targets_lastMonthTotalHome > 0
                ? targets_lastMonthTotalHome
                : stats_lastMonthTotalHome && stats_lastMonthTotalHome > 0
                    ? stats_lastMonthTotalHome
                    : 0;

    const mtdCostOfAdsCurrentDisplay = shouldShowDummyUi
        ? dummyStatData.costOfAds.current
        : rangeActive
            ? cachedRangeTotals.currentAds
            : adsSpendTotal;

    const mtdCostOfAdsPreviousDisplay = shouldShowDummyUi
        ? dummyStatData.costOfAds.previous
        : rangeActive
            ? cachedRangeTotals.previousAds
            : amazonPrevAdsDisp;

    const mtdCostOfAdsDelta = shouldShowDummyUi
        ? dummyStatData.costOfAds.deltaPct
        : safeDeltaPct(
            mtdCostOfAdsCurrentDisplay,
            mtdCostOfAdsPreviousDisplay
        );

    const mtdTacosCurrent = shouldShowDummyUi
        ? dummyStatData.tacos.current
        : rangeActive
            ? (
                cachedRangeTotals.currentNetSales > 0
                    ? (cachedRangeTotals.currentAds / cachedRangeTotals.currentNetSales) * 100
                    : 0
            )
            : (
                Number(plSummaryTotals.net_sales ?? 0) > 0
                    ? (Number(adsSpendTotal ?? 0) / Number(plSummaryTotals.net_sales ?? 0)) * 100
                    : 0
            );

    const mtdTacosPrevious = shouldShowDummyUi
        ? dummyStatData.tacos.previous
        : rangeActive
            ? (
                cachedRangeTotals.previousNetSales > 0
                    ? (cachedRangeTotals.previousAds / cachedRangeTotals.previousNetSales) * 100
                    : 0
            )
            : (
                Number(prev.netSales ?? 0) > 0
                    ? (Number(amazonPrevAdsDisp ?? 0) / Number(prev.netSales ?? 0)) * 100
                    : 0
            );

    const mtdTacosDelta = shouldShowDummyUi
        ? dummyStatData.tacos.deltaPct
        : safeDeltaPct(mtdTacosCurrent, mtdTacosPrevious);

    const stickyKpiItems = [
        {
            label: "Units",
            current: shouldShowDummyUi
                ? dummyStatData.units.current
                : isStickyGlobal
                    ? stickyTableTotals.units
                    : (useBiForAmazonCards ? biCardKpis.curr.units : mtdUnitsCurrent),

            previous: shouldShowDummyUi
                ? dummyStatData.units.previous
                : isStickyGlobal
                    ? stickyPreviousTotals.units
                    : (useBiForAmazonCards ? biCardKpis.prev.units : mtdUnitsPrevious),

            deltaPct: shouldShowDummyUi
                ? dummyStatData.units.deltaPct
                : isStickyGlobal
                    ? safeDeltaPct(stickyTableTotals.units, stickyPreviousTotals.units)
                    : (useBiForAmazonCards ? biCardKpis.deltas.units : mtdUnitsDelta),

            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)
                    : (loading || biLoading)
            ),
            formatter: fmtInt,
            bottomLabel: prevLabel,
            className: "bg-white border-[#FDD36F] border-t-4 border-t-[#FDD36F]",
        },
        {
            label: "ASP",
            current: shouldShowDummyUi
                ? dummyStatData.asp.current
                : isStickyGlobal
                    ? stickyTableTotals.asp
                    : (showLiveBI && rangeActive
                        ? biCardKpis.curr.asp
                        : convertToDisplayCurrency(uk.aspGBP ?? 0, amazonDataCurrency)),

            previous: shouldShowDummyUi
                ? dummyStatData.asp.previous
                : isStickyGlobal
                    ? stickyPreviousTotals.asp
                    : (showLiveBI && rangeActive
                        ? biCardKpis.prev.asp
                        : convertToDisplayCurrency(prev.asp ?? 0, amazonDataCurrency)),

            deltaPct: shouldShowDummyUi
                ? dummyStatData.asp.deltaPct
                : isStickyGlobal
                    ? safeDeltaPct(stickyTableTotals.asp, stickyPreviousTotals.asp)
                    : (useBiForAmazonCards
                        ? biCardKpis.deltas.asp
                        : deltas.aspPct),

            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)
                    : (loading || biLoading)
            ),
            formatter: formatDisplayAmount,
            previousFormatter: formatDisplayAmount,
            bottomLabel: prevLabel,
            className: "bg-white border-[#B75A5A] border-t-4 border-t-[#B75A5A]",
        },
        {
            label: "Net Sales",
            current: shouldShowDummyUi
                ? dummyStatData.netSales.current
                : isStickyGlobal
                    ? stickyTableTotals.netSales
                    : (showLiveBI && rangeActive
                        ? biCardKpis.curr.netSales
                        : convertToDisplayCurrency(uk.netSalesGBP ?? 0, amazonDataCurrency)),

            previous: shouldShowDummyUi
                ? dummyStatData.netSales.previous
                : isStickyGlobal
                    ? stickyPreviousTotals.netSales
                    : (showLiveBI && rangeActive
                        ? biCardKpis.prev.netSales
                        : convertToDisplayCurrency(prev.netSales ?? 0, amazonDataCurrency)),

            deltaPct: shouldShowDummyUi
                ? dummyStatData.netSales.deltaPct
                : isStickyGlobal
                    ? safeDeltaPct(stickyTableTotals.netSales, stickyPreviousTotals.netSales)
                    : (useBiForAmazonCards
                        ? biCardKpis.deltas.netSales
                        : deltas.netSalesPct),

            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)
                    : (loading || biLoading)
            ),
            formatter: (val: number) => formatDisplayAmount(val, "Net Sales"),
            previousFormatter: (val: number) => formatDisplayAmount(val, "Net Sales"),
            bottomLabel: prevLabel,
            className: "bg-white border-[#75BBDA] border-t-4 border-t-[#75BBDA]",
        },
        {
            label: "Cost of Ads",
            current: shouldShowDummyUi
                ? dummyStatData.costOfAds.current
                : isStickyGlobal
                    ? stickyTableTotals.costOfAds
                    : mtdCostOfAdsCurrentDisplay,

            previous: shouldShowDummyUi
                ? dummyStatData.costOfAds.previous
                : isStickyGlobal
                    ? stickyPreviousTotals.costOfAds
                    : mtdCostOfAdsPreviousDisplay,

            deltaPct: shouldShowDummyUi
                ? dummyStatData.costOfAds.deltaPct
                : isStickyGlobal
                    ? safeDeltaPct(stickyTableTotals.costOfAds, stickyPreviousTotals.costOfAds)
                    : mtdCostOfAdsDelta,

            inverseDelta: true,
            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || previousSkuwiseGlobalLoading)
                    : (loading || (rangeActive ? biLoading : false))
            ),
            formatter: (val: number) => formatDisplayAmount(val, "Cost of Ads"),
            previousFormatter: (val: number) => formatDisplayAmount(val, "Cost of Ads"),
            bottomLabel: prevLabel,
            className: "bg-white border-[#C49466] border-t-4 border-t-[#C49466]",
        },

        {
            label: "TACoS",
            current: shouldShowDummyUi
                ? dummyStatData.tacos.current
                : isStickyGlobal
                    ? stickyTableTotals.tacos
                    : mtdTacosCurrent,

            previous: shouldShowDummyUi
                ? dummyStatData.tacos.previous
                : isStickyGlobal
                    ? stickyPreviousTotals.tacos
                    : mtdTacosPrevious,

            deltaPct: shouldShowDummyUi
                ? dummyStatData.tacos.deltaPct
                : isStickyGlobal
                    ? safeDeltaPct(stickyTableTotals.tacos, stickyPreviousTotals.tacos)
                    : mtdTacosDelta,

            inverseDelta: true,
            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || previousSkuwiseGlobalLoading)
                    : (loading || (rangeActive ? biLoading : false))
            ),
            formatter: fmtPct2,
            previousFormatter: fmtPct2,
            bottomLabel: prevLabel,
            className: "bg-white border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]",
        },

        {
            label: "CM2 Profit",
            current: shouldShowDummyUi
                ? dummyStatData.cm2Profit.current
                : isStickyGlobal
                    ? stickyTableTotals.cm2Profit
                    : totalRowCm2Profit,

            previous: shouldShowDummyUi
                ? dummyStatData.cm2Profit.previous
                : isStickyGlobal
                    ? stickyPreviousTotals.cm2Profit
                    : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency),

            deltaPct: shouldShowDummyUi
                ? dummyStatData.cm2Profit.deltaPct
                : isStickyGlobal
                    ? safeDeltaPct(stickyTableTotals.cm2Profit, stickyPreviousTotals.cm2Profit)
                    : safeDeltaPct(
                        totalRowCm2Profit,
                        convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency)
                    ),

            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || previousSkuwiseGlobalLoading)
                    : loading
            ),

            formatter: (val: number) => formatDisplayAmount(val, "CM2 Profit"),
            previousFormatter: (val: number) => formatDisplayAmount(val, "CM2 Profit"),
            bottomLabel: prevLabel,
            className: "bg-white border-[#B8C78C] border-t-4 border-t-[#B8C78C]",
        },

        {
            label: "Target",
            current: shouldShowDummyUi
                ? 0
                : stickyTargetHome,

            previous: shouldShowDummyUi
                ? 0
                : stickyPreviousTargetHome,

            deltaPct: shouldShowDummyUi
                ? safeDeltaPct(0, 0)
                : safeDeltaPct(stickyTargetHome, stickyPreviousTargetHome),

            loading: !shouldShowDummyUi && loading,

            formatter: (val: number) =>
                formatDisplayAmount(Math.round(val), "Net Sales"),

            previousFormatter: (val: number) =>
                formatDisplayAmount(Math.round(val), "Net Sales"),

            bottomLabel: "Last Month",
            className: "bg-white border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]",
        },

        {
            label: "Target Trend",

            current: stickyTargetTrendPct,

            previous: stickyTargetTrendPrevPct,

            deltaPct: shouldShowDummyUi
                ? deltaPctAbs(dummySalesTargetStats.targetTrendPct, 0)
                : deltaPctAbs(stickyTargetTrendPct, stickyTargetTrendPrevPct),

            loading: !shouldShowDummyUi && loading,

            formatter: fmtPct,
            previousFormatter: fmtPct,

            bottomLabel: "Last Month",
            className: "bg-white border-[#ED9F50] border-t-4 border-t-[#ED9F50]",
        },
    ];

    const dummyBarLabels = [
        "Net Sales",
        "COGS",
        "Marketplace Fees",
        "Tax & Credits",
        "CM1 Profit",
        "Advertisements",
        "Others",
        "CM2 Profit",
    ];

    const dummyBarValues = [16200, 6200, 2100, 420, 7480, 2100, 530, 3250];
    const dummyPrevBarValues = [13100, 5400, 1800, 360, 5540, 1760, 470, 2480];

    const finalBarLabels = shouldShowDummyUi ? dummyBarLabels : labels;

    const globalCurrentGraphValues = useMemo(() => {
        const rows =
            platform === "global" && Array.isArray(data?.skuwise_items_global)
                ? data.skuwise_items_global
                : Array.isArray(data?.skuwise_items)
                    ? data.skuwise_items
                    : monthlySkuwiseRows || [];

        const grand = getGrandTotalRow(rows) as GrandTotalSkuwiseRow;

        const currentPlatformFee = toNumber(
            grand?.platform_fee ??
            grand?.platformfeenew ??
            grand?.other_transactions ??
            grand?.other
        );

        return {
            "Net Sales": toNumber(grand?.net_sales),

            "COGS": toNumber(grand?.cogs),

            "Marketplace Fees": toNumber(
                grand?.amazon_fees ??
                (
                    toNumber(grand?.fba_fees) +
                    toNumber(grand?.selling_fees)
                )
            ),

            "Tax & Credits": toNumber(
                grand?.tax_and_credits ??
                (
                    toNumber(grand?.tax) +
                    toNumber(grand?.credits)
                )
            ),

            "Advertisements": toNumber(
                grand?.total_ads ??
                grand?.advertising_fees ??
                grand?.ads_spend
            ),

            // ✅ use platform_fee for current Other Transactions bar
            "Other Transactions": currentPlatformFee,
            "Others": currentPlatformFee,
            "Other Charges": currentPlatformFee,

            "CM1 Profit": toNumber(
                grand?.profit ??
                grand?.cm1_profit_per
            ),

            "CM2 Profit": toNumber(
                grand?.total_cm2_profit ??
                grand?.cm2_profit
            ),
        } as Record<string, number>;
    }, [
        platform,
        data,
        monthlySkuwiseRows,
    ]);

    const finalBarValues = useMemo(() => {
        if (shouldShowDummyUi) {
            return dummyBarValues;
        }

        if (platform === "global") {
            return finalBarLabels.map((label) =>
                Math.round(toNumber(globalCurrentGraphValues[label]))
            );
        }

        return valuesPatched;
    }, [
        shouldShowDummyUi,
        dummyBarValues,
        platform,
        finalBarLabels,
        globalCurrentGraphValues,
        valuesPatched,
    ]);

    const globalPreviousGraphValues = useMemo(() => {
        const prevAligned = previousSkuwiseGlobalData?.aligned_totals_global || {};
        const prevDerived = previousSkuwiseGlobalData?.derived_totals_global || {};

        const previousNetSales = toNumber(
            prevAligned.total_previous_net_sales ??
            prevDerived.net_sales
        );

        const previousAdvertising = toNumber(
            prevAligned.total_previous_advertising ??
            prevDerived.advertising_fees
        );

        const previousPlatformFees = toNumber(
            prevAligned.total_previous_platform_fees ??
            prevDerived.platform_fee ??
            prevDerived.platformfeenew
        );

        const previousCm1Profit = toNumber(
            prevAligned.total_previous_profit ??
            prevDerived.profit ??
            prevDerived.cm1_profit
        );

        const previousCm2Profit = toNumber(
            prevAligned.total_previous_profit_cm2 ??
            prevDerived.cm2_profit
        );

        const previousCogs = toNumber(
            prevDerived.cogs ??
            prevDerived.cost_of_unit_sold
        );

        const previousMarketplaceFees = toNumber(
            prevDerived.amazon_fees ??
            prevDerived.marketplace_fees ??
            (
                toNumber(prevDerived.fba_fees) +
                toNumber(prevDerived.selling_fees)
            )
        );

        const previousTaxAndCredits = toNumber(
            prevDerived.tax_and_credits ??
            prevDerived.tex_and_credits ??
            (
                toNumber(prevDerived.tax) +
                toNumber(prevDerived.credits)
            )
        );

        return {
            "Net Sales": previousNetSales,
            "COGS": previousCogs,
            "Marketplace Fees": previousMarketplaceFees,
            "Tax & Credits": previousTaxAndCredits,
            "Advertisements": previousAdvertising,
            "Others": previousPlatformFees,
            "Other Charges": previousPlatformFees,
            "CM1 Profit": previousCm1Profit,
            "CM2 Profit": previousCm2Profit,
        } as Record<string, number>;
    }, [previousSkuwiseGlobalData]);

    const finalPrevBarValues = useMemo(() => {
        if (shouldShowDummyUi) {
            return dummyPrevBarValues;
        }

        if (platform === "global") {
            return finalBarLabels.map((label) =>
                Math.round(toNumber(globalPreviousGraphValues[label]))
            );
        }

        return prevValues;
    }, [
        shouldShowDummyUi,
        dummyPrevBarValues,
        platform,
        finalBarLabels,
        globalPreviousGraphValues,
        prevValues,
    ]);

    const finalAllValuesZero = shouldShowDummyUi ? false : allValuesZero;

    const finalBiDailySeriesHome: GraphDailySeries | null = shouldShowDummyUi
        ? (dummyBiDailySeriesHome as GraphDailySeries)
        : biDailySeriesHome;

    const finalBiPeriods = shouldShowDummyUi ? dummyBiPeriods : biPeriods;

    // const finalMonthlySkuwiseRowsForTable = shouldShowDummyUi
    //     ? dummyMonthlySkuwiseRowsForTable
    //     : monthlySkuwiseRowsForTable;


    const liveBiGraphLoading =
        !shouldShowDummyUi &&
        biUiLoading &&
        !finalBiDailySeriesHome;

    const previousSkuwiseRowsForDelta = useMemo(() => {
        if (platform === "global") {
            return Array.isArray(previousSkuwiseGlobalData?.skuwise_items_global)
                ? previousSkuwiseGlobalData.skuwise_items_global
                : [];
        }

        return Array.isArray((data as any)?.previous_period?.sku_metrics)
            ? (data as any).previous_period.sku_metrics
            : [];
    }, [platform, previousSkuwiseGlobalData, data]);

    const finalMonthlySkuwiseRowsForTable = useMemo<MonthlySkuwiseTableRow[]>(() => {
        if (shouldShowDummyUi) {
            return dummyMonthlySkuwiseRowsForTable;
        }

        return attachNetSalesDeltaToLiveRows(
            monthlySkuwiseRowsForTable,
            previousSkuwiseRowsForDelta
        );
    }, [
        shouldShowDummyUi,
        dummyMonthlySkuwiseRowsForTable,
        monthlySkuwiseRowsForTable,
        previousSkuwiseRowsForDelta,
    ]);

    const handleDownloadPlProductwiseMtd = useCallback(() => {
        try {

            const rowsToExport = (monthlySkuwiseRowsDisplay || []).filter((r: any) => {
                const sku = String(r?.sku || "").trim().toUpperCase();
                const pn = String(r?.product_name || "").trim().toLowerCase();

                // remove grouped/summary "Others"
                if (r?.isOthers || sku === "OTHERS" || pn === "others") return false;

                // keep real items and total row
                return sku || pn || r?.isTotal;
            });

            if (!rowsToExport.length) return;

            const periodLabel = formattedMonthYear;
            const titleCountry = countryName === "global" ? "Global" : countryName.toUpperCase();

            const companyName =
                (userData as any)?.companyName ||
                (userData as any)?.company_name ||
                (userData as any)?.company ||
                "";

            const n = (v: any) => Number(v ?? 0) || 0;

            const totalRow: any =
                rowsToExport.find((r: any) => r?.isTotal) ||
                rowsToExport.find((r: any) => String(r?.sku || "").toUpperCase() === "GRAND_TOTAL") ||
                rowsToExport.find((r: any) => String(r?.product_name || "").toLowerCase() === "total") ||
                {};

            const dataRows = rowsToExport.map((r: any) => ({
                "S.No": r.isTotal ? "" : r.sno ?? "",
                "SKU": r.isOthers || r.isTotal ? "-" : r.sku || "-",
                "Product Name": r.isTotal ? "Total" : r.isOthers ? "Others" : r.product_name,
                // "Ad Type": r.isOthers || r.isTotal ? "-" : formatAdType(r.ad_type || "-"),

                "Units Sold": n(r.quantity),
                "Return": n(r.return_quantity),
                "Total Units": n(
                    r.total_quantity ??
                    (toNumber(r.quantity) - toNumber(r.return_quantity))
                ),
                "ASP": n(r.asp),
                "Net Sales": n(r.net_sales),
                "COGS": n(r.cogs),

                "Selling Fees": n(r.selling_fees),
                "FBA Fees": n(r.fba_fees),

                "Ads Spend": n(r.ads_spend),
                "ACOS %": n(r.acos),

                "Tax": n(r.tax),
                "Credits": n(r.credits),
                "Tax & Credits": n(r.tax_and_credits),

                "CM1 Profit": n(r.profit),
                "CM1 Profit %": n(r.cm1_profit_per),
                "CM1 Profit Per Unit": n(r.cm1_profit_per_unit),

                "CM2 Profit": n(r.cm2_profit),
                "CM2 Profit %": n(r.cm2_profit_per),
                "CM2 Profit Per Unit": n(r.cm2_profit_per_unit),
            }));

            const visibilityAds =
                n(totalRow?.brand_spend); // Visibility - Ads (-)

            const dealsVouchers =
                n(totalRow?.dealsvouchar_ads) ||
                n((plSummaryTotals as any)?.dealsvouchar_ads);

            const otherPlatformFees =
                n(totalRow?.platformfeenew) ||
                n(totalRow?.platform_fee) ||
                n((plSummaryTotals as any)?.platform_fee);

            const inventoryStorageFees =
                n(totalRow?.platform_fee_inventory_storage) ||
                n((plSummaryTotals as any)?.platform_fee_inventory_storage);

            const miscTransactions =
                n(totalRow?.misc_transaction) ||
                n(grandTotalRowRaw?.misc_transaction) ||
                n(grandTotalRowDisplay?.misc_transaction) ||
                n((plSummaryTotals as any)?.misc_transaction);

            const lostInventory =
                n(totalRow?.lost_total) ||
                n((plSummaryTotals as any)?.lost_total) ||
                n((plSummaryTotals as any)?.reimbursement_lost_inventory_amount);

            const shipmentCharges =
                n((plSummaryTotals as any)?.shipment_charges);

            const cm2ProfitLoss =
                n(grandTotalRowRaw?.total_cm2_profit) ||
                n(grandTotalRowDisplay?.total_cm2_profit) ||
                n((plSummaryTotals as any)?.cm2_profit) ||
                n(grandTotalRowRaw?.cm2_profit) ||
                n(grandTotalRowDisplay?.cm2_profit);

            const summaryRows: { label: string; value: any; indent?: number; bold?: boolean }[] = [
                { label: "Cost of Advertisement", value: "", bold: true },
                { label: "Visibility - Ads (-)", value: visibilityAds, indent: 1 },
                { label: "Visibility - Deals, Vouchers and Reviews (-)", value: dealsVouchers, indent: 1 },

                { label: "Other Transactions", value: "", bold: true },
                { label: "Other Platform Fees (-)", value: otherPlatformFees, indent: 1 },
                { label: "Inventory Storage Fees (-)", value: inventoryStorageFees, indent: 1 },
                {
                    label: "Misc. Transactions (+)",
                    value: formatSummaryValue(plSummaryTotals.misc_transaction, "misc_transaction"),
                    indent: 1
                },
                { label: "Reimbursement for lost Inventory (+)", value: lostInventory, indent: 1 },

                ...(countryName === "us" || countryName === "global"
                    ? [
                        {
                            label: "Shipment Charges (-)",
                            value: shipmentCharges,
                            bold: true
                        },
                    ]
                    : []),

                { label: "CM2 Profit/Loss", value: cm2ProfitLoss, bold: true },
                { label: "CM2 Margins", value: Number(cm2MarginPctForSummary ?? 0), bold: true },
                { label: "TACoS (Total Advertising Cost of Sale)", value: Number(tacosFromDisplayedCardsForSummary ?? 0), bold: true },
                { label: "Net Reimbursement", value: Number(reimbursementForSummary ?? 0), bold: true },
                { label: "Reimbursement vs CM2 Margins", value: Number(reimbursementVsCm2PctForSummary ?? 0), bold: true },
                { label: "Reimbursement vs Sales", value: Number(reimbursementVsSalesPctForSummary ?? 0), bold: true },
            ];

            exportPnLProductwiseBreakdownMtdExcel({
                filename: `Amazon-PnL-Productwise-MTD-${periodLabel}.xlsx`,
                titleLine: `Amazon ${titleCountry} - P&L Productwise Breakdown MTD - ${periodLabel}`,
                countryName,
                titleCountry,
                platformLabel: "Phormula",
                periodLabel,
                companyName,
                brandName: String(brandName || ""),
                homeCurrencyCode: profileHomeCurrency,
                dataRows,
                summaryRows,
            });
        } catch (err) {
            console.error("Error exporting P&L Productwise Breakdown MTD", err);
        }
    }, [
        monthlySkuwiseRowsDisplay,
        formattedMonthYear,
        countryName,
        plSummaryTotals,
        cm2MarginPctForSummary,
        tacosFromDisplayedCardsForSummary,
        reimbursementForSummary,
        reimbursementVsCm2PctForSummary,
        reimbursementVsSalesPctForSummary,
        userData,
        brandName,
        profileHomeCurrency,
    ]);


    const cm2ProfitPieData = useMemo<Cm1PieSlice[]>(() => {
        const prevCm2ByName = buildPreviousProfitMap("cm2_profit");

        const rows = (finalMonthlySkuwiseRowsForTable || [])
            .filter((r: any) => {
                const name = String(r?.product_name || "").trim().toLowerCase();
                const sku = String(r?.sku || "").trim().toUpperCase();

                return (
                    !r?.isTotal &&
                    !r?.isOthers &&
                    sku !== "TOTAL" &&
                    sku !== "GRAND_TOTAL" &&
                    name !== "total" &&
                    name !== "grand total"
                );
            })
            .map((r: any) => {
                const name = normalizeProductDisplayName(
                    r?.product_name || r?.sku || "Unknown"
                );

                const value = Number(r?.cm2_profit || 0);
                const prevValue = prevCm2ByName.get(normalizePieName(name)) ?? 0;

                return {
                    name,
                    value,
                    prevValue,
                    pct: 0,
                    deltaPct: safeDeltaPct(value, prevValue),
                };
            })
            .filter((r) => r.value !== 0 || r.prevValue !== 0);

        const total = rows.reduce((sum, r) => sum + Math.abs(r.value), 0) || 1;

        return rows
            .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
            .map((r) => ({
                ...r,
                pct: (Math.abs(r.value) / total) * 100,
            }));
    }, [
        finalMonthlySkuwiseRowsForTable,
        buildPreviousProfitMap,
    ]);


    const cm1ProfitPieData = useMemo<Cm1PieSlice[]>(() => {
        const isInvalidPieRow = (name: string, sku?: string) => {
            const n = String(name || "").trim().toLowerCase();
            const s = String(sku || "").trim().toUpperCase();

            return (
                !n ||
                n === "unknown" ||
                n === "total" ||
                n === "grand total" ||
                s === "TOTAL" ||
                s === "GRAND_TOTAL"
            );
        };

        const withPct = (rows: Cm1PieSlice[]) => {
            const total = rows.reduce((sum, r) => sum + Math.abs(Number(r.value || 0)), 0);

            if (!total) return [];

            return rows
                .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
                .map((r) => ({
                    ...r,
                    pct: (Math.abs(Number(r.value || 0)) / total) * 100,
                }));
        };

        // ✅ Countrywise: use the actual countrywise MTD response shape.
        // Your countrywise response has:
        // - data.skuwise_items[] for current profit
        // - data.previous_period.sku_metrics[] for previous profit
        if (platform !== "global") {
            const currentRows = Array.isArray((data as any)?.skuwise_items)
                ? (data as any).skuwise_items
                : finalMonthlySkuwiseRowsForTable || [];

            const previousRows = Array.isArray((data as any)?.previous_period?.sku_metrics)
                ? (data as any).previous_period.sku_metrics
                : [];

            const prevProfitByName = new Map<string, number>();
            const prevProfitBySku = new Map<string, number>();

            previousRows.forEach((row: any) => {
                const rawName = String(row?.product_name || row?.sku || "").trim();
                const sku = String(row?.sku || "").trim().toUpperCase();

                if (isInvalidPieRow(rawName, sku)) return;

                const profit = Number(row?.profit ?? row?.cm1_profit ?? 0);

                if (rawName) {
                    prevProfitByName.set(
                        normalizePieName(normalizeProductDisplayName(rawName)),
                        profit
                    );
                }

                if (sku) {
                    prevProfitBySku.set(sku, profit);
                }
            });

            const rows: Cm1PieSlice[] = currentRows
                .map((row: any) => {
                    const rawName = String(row?.product_name || row?.sku || "Unknown").trim();
                    const sku = String(row?.sku || "").trim().toUpperCase();

                    const name = normalizeProductDisplayName(rawName);

                    const value = Number(
                        row?.profit ??
                        row?.cm1_profit ??
                        row?.profit_curr ??
                        0
                    );

                    const prevValue =
                        prevProfitBySku.get(sku) ??
                        prevProfitByName.get(normalizePieName(name)) ??
                        Number(row?.profit_prev ?? 0);

                    return {
                        name,
                        value,
                        prevValue,
                        pct: 0,
                        deltaPct: safeDeltaPct(value, prevValue),
                    };
                })
                .filter((r: Cm1PieSlice) => {
                    return (
                        !isInvalidPieRow(r.name) &&
                        (Number(r.value || 0) !== 0 || Number(r.prevValue || 0) !== 0)
                    );
                });

            return withPct(rows);
        }

        // ✅ Global: keep your existing SKU-wise global previous map logic unchanged.
        const prevProfitByName = buildPreviousProfitMap("profit");

        const rows: Cm1PieSlice[] = (finalMonthlySkuwiseRowsForTable || [])
            .map((r: any) => {
                const name = normalizeProductDisplayName(
                    r?.product_name || r?.sku || "Unknown"
                );

                const value = Number(r?.profit ?? r?.cm1_profit ?? 0);
                const prevValue = prevProfitByName.get(normalizePieName(name)) ?? 0;

                return {
                    name,
                    value,
                    prevValue,
                    pct: 0,
                    deltaPct: safeDeltaPct(value, prevValue),
                };
            })
            .filter((r: Cm1PieSlice) => {
                return (
                    !isInvalidPieRow(r.name) &&
                    (Number(r.value || 0) !== 0 || Number(r.prevValue || 0) !== 0)
                );
            });

        return withPct(rows);
    }, [
        platform,
        data,
        finalMonthlySkuwiseRowsForTable,
        buildPreviousProfitMap,
    ]);

    const isUsingDummyData = shouldShowDummyUi;

    const finalCm1ProfitPieData = isUsingDummyData
        ? dummyCm1ProfitPieData
        : cm1ProfitPieData;



    const hasRealInventoryRows = Array.isArray(invRows) && invRows.length > 0;

    const finalInventoryRows = hasRealInventoryRows
        ? invRows
        : isUsingDummyData
            ? dummyInventoryRows
            : [];

    const finalInventoryAlerts = hasRealInventoryRows
        ? inventoryAlerts
        : isUsingDummyData
            ? dummyInventoryAlerts
            : {};

    const finalTargetData = shouldShowDummyUi ? dummyTargetData : targetData;

    const finalTargetsTodayHome = shouldShowDummyUi
        ? dummySalesTargetStats.todayHome
        : targets_todayHome;

    const finalTargetsMtdHome = shouldShowDummyUi
        ? dummySalesTargetStats.mtdHome
        : targets_mtdHome;

    const finalStatsTargetHome = shouldShowDummyUi
        ? dummySalesTargetStats.targetHome
        : stats_targetHome;

    const finalTargetsLastMonthTotalHome = shouldShowDummyUi
        ? dummySalesTargetStats.lastMonthTotalHome
        : targets_lastMonthTotalHome;

    const finalTargetsLastMonthToDateHome = shouldShowDummyUi
        ? dummySalesTargetStats.lastMonthToDateHome
        : targets_lastMonthToDateHome;

    const finalStatsSalesTrendPct = shouldShowDummyUi
        ? dummySalesTargetStats.salesTrendPct
        : stats_salesTrendPct;

    const finalStatsTargetTrendPct = stickyTargetTrendPct;

    const finalTargetsReimbursement = shouldShowDummyUi
        ? dummySalesTargetStats.reimbursement
        : targets_reimbursement;

    const finalRangeCompletedPct = shouldShowDummyUi
        ? dummySalesTargetStats.periodCompletedPct
        : rangeCompletedPct;

    const finalLiveBiPayload = shouldShowDummyUi
        ? dummyLiveBusinessClientData
        : liveBiPayload;

    const cleanDrawerPoint = (value: string) =>
        String(value || "")
            .replace(/^\s*[-•]\s*/, "")
            .replace(/^\s*\d+\.\s*-\s*/, "")
            .replace(/^\s*\d+\.\s*/, "")
            .trim();

    const toDrawerPoints = (value: unknown): string[] => {
        if (!value) return [];

        const rawItems = Array.isArray(value) ? value : [String(value)];

        return rawItems
            .flatMap((item) =>
                String(item || "")
                    .replace(/\\n/g, "\n")
                    .split(/\r?\n+/)
                    .flatMap((line) => line.split(/\s+(?=[-•]\s+)/g))
                    .flatMap((line) =>
                        line.split(
                            /(?<=[.!?])\s+(?=(?:From|Between|In|This|Long-term|ASP|Units|CM1|Net|Inventory)\b)/g
                        )
                    )
            )
            .map(cleanDrawerPoint)
            .filter(Boolean);
    };

    const extractDrawerSections = (text: string) => {
        const raw = String(text || "").trim();

        const getBlock = (label: string, nextLabels: string[]) => {
            const next = nextLabels
                .map((l) => l.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
                .join("|");

            const re = new RegExp(
                `${label}\\s*:\\s*([\\s\\S]*?)(?=\\n\\s*(?:${next})\\s*:|$)`,
                "i"
            );

            const match = raw.match(re);
            return (match?.[1] || "").trim();
        };

        const journeyText = getBlock("Product\\s*Journey", [
            "Recommendation",
            "Advertising",
            "Inventory",
        ]);

        const recText = getBlock("Recommendation", [
            "Advertising",
            "Inventory",
            "Product\\s*Journey",
        ]);

        const adsText = getBlock("Advertising", [
            "Inventory",
            "Recommendation",
            "Product\\s*Journey",
        ]);

        const invText = getBlock("Inventory", [
            "Advertising",
            "Recommendation",
            "Product\\s*Journey",
        ]);

        return {
            journeyPoints: toDrawerPoints(journeyText),
            recommendationPoints: toDrawerPoints(recText),
            advertisingPoints: toDrawerPoints(adsText),
            inventoryPoints: toDrawerPoints(invText),
        };
    };

    const normalizeDrawerKey = (value: any) =>
        String(value || "")
            .trim()
            .toLowerCase()
            .replace(/\s+/g, " ");

    const getDrawerGrowth = (row: any, ...keys: string[]) => {
        for (const key of keys) {
            const raw = row?.[key];
            const value = typeof raw === "object" ? raw?.value : raw;
            const n = Number(value);
            if (Number.isFinite(n)) return n;
        }
        return 0;
    };

    const formatDrawerGrowth = (value: number) => {
        const sign = value > 0 ? "+" : "";
        return `${sign}${value.toFixed(2)}%`;
    };

    const formatDrawerMetricValue = (
        actualValue: number,
        growthValue: number,
        type: "money" | "number" = "money",
        sourceCurrencyForValue: CurrencyCode = platform === "global" ? "USD" : biSourceCurrency,
        noDecimals: boolean = false
    ) => {
        const convertedValue = convertToDisplayCurrency(
            Number(actualValue || 0),
            sourceCurrencyForValue
        );

        const mainValue =
            type === "number"
                ? Number(actualValue || 0).toLocaleString()
                : noDecimals
                    ? formatDisplayAmountNoDecimals(convertedValue)
                    : formatDisplayAmount(convertedValue);

        return `${mainValue} (${formatDrawerGrowth(growthValue)})`;
    };

    const getLiveBiProductRows = useCallback(() => {
        const cat = (finalLiveBiPayload as any)?.categorized_growth || {};

        return [
            ...(cat.top_80_skus || []),
            ...(cat.new_skus || []),
            ...(cat.reviving_skus || []),
            ...(cat.new_or_reviving_skus || []),
            ...(cat.other_skus || []),
        ];
    }, [finalLiveBiPayload]);

    const findLiveBiRowForPnlRow = useCallback(
        (row: MonthlySkuwiseTableRow) => {
            const sku = normalizeDrawerKey(row?.sku);
            const name = normalizeDrawerKey(row?.product_name);

            return getLiveBiProductRows().find((item: any) => {
                const itemSku = normalizeDrawerKey(item?.sku);
                const itemName = normalizeDrawerKey(item?.product_name);

                return (
                    (!!sku && sku !== "others" && itemSku === sku) ||
                    (!!name && itemName === name) ||
                    (!!name && itemName.includes(name)) ||
                    (!!itemName && name.includes(itemName))
                );
            });
        },
        [getLiveBiProductRows]
    );

    const buildDrawerMetricsForPnlRow = useCallback(
        (pnlRow: MonthlySkuwiseTableRow, liveRow?: any): MetricItem[] => {
            const source = liveRow || pnlRow;

            const units =
                Number(
                    source?.quantity_curr ??
                    source?.quantity_month2 ??
                    source?.total_quantity ??
                    source?.quantity ??
                    0
                ) || 0;

            const netSales =
                Number(
                    source?.net_sales_curr ??
                    source?.net_sales_month2 ??
                    source?.net_sales ??
                    0
                ) || 0;

            const asp =
                Number(source?.asp_curr ?? source?.asp_month2 ?? source?.asp ?? 0) || 0;

            const cm1Profit =
                Number(
                    source?.profit_curr ??
                    source?.profit_month2 ??
                    source?.profit ??
                    source?.cm1_profit ??
                    0
                ) || 0;

            const cm1ProfitPerUnit =
                Number(
                    source?.unit_wise_profitability_curr ??
                    source?.unit_wise_profitability_month2 ??
                    source?.unit_wise_profitability ??
                    source?.cm1_profit_per_unit ??
                    0
                ) || 0;

            const unitGrowth = getDrawerGrowth(source, "Unit Growth", "Unit Growth (%)");
            const salesGrowth = getDrawerGrowth(
                source,
                "Sales Growth",
                "Net Sales Growth",
                "Net Sales Growth (%)"
            );
            const aspGrowth = getDrawerGrowth(source, "ASP Growth", "ASP Growth (%)");
            const profitGrowth = getDrawerGrowth(
                source,
                "CM1 Profit Impact",
                "CM1 Profit Impact (%)"
            );
            const profitPerUnitGrowth = getDrawerGrowth(
                source,
                "Profit Per Unit",
                "Profit Per Unit (%)"
            );

            const valueCurrency: CurrencyCode = platform === "global" ? "USD" : biSourceCurrency;

            return [
                {
                    label: "Units",
                    value: formatDrawerMetricValue(units, unitGrowth, "number", valueCurrency),
                    color: unitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                },
                {
                    label: "Net sales",
                    value: formatDrawerMetricValue(
                        netSales,
                        salesGrowth,
                        "money",
                        valueCurrency,
                        true
                    ),
                    color: salesGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                },
                {
                    label: "ASP",
                    value: formatDrawerMetricValue(asp, aspGrowth, "money", valueCurrency),
                    color: aspGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                },
                {
                    label: "CM1 profit",
                    value: formatDrawerMetricValue(
                        cm1Profit,
                        profitGrowth,
                        "money",
                        valueCurrency,
                        true
                    ),
                    color: profitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                },
                {
                    label: "CM1 profit per unit",
                    value: formatDrawerMetricValue(
                        cm1ProfitPerUnit,
                        profitPerUnitGrowth,
                        "money",
                        valueCurrency
                    ),
                    color: profitPerUnitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                },
            ];
        },
        [
            platform,
            biSourceCurrency,
            convertToDisplayCurrency,
            formatDisplayAmount,
            formatDisplayAmountNoDecimals,
        ]
    );

    const findLiveBiInsightForPnlRow = useCallback(
        (row: MonthlySkuwiseTableRow, liveRow?: any) => {
            const payload: any = finalLiveBiPayload || {};
            const insights = payload.ai_insights || payload.insights || {};

            const sku = normalizeDrawerKey(row?.sku || liveRow?.sku);
            const name = normalizeDrawerKey(row?.product_name || liveRow?.product_name);

            const directBySku = Object.entries(insights).find(([key]) => {
                return normalizeDrawerKey(key) === sku;
            });

            if (directBySku) return directBySku[1] as any;

            const byProductName = Object.values(insights).find((item: any) => {
                const product = normalizeDrawerKey(item?.product_name);
                return (
                    product === name ||
                    (!!product && product.includes(name)) ||
                    (!!name && name.includes(product))
                );
            });

            if (byProductName) return byProductName as any;

            return null;
        },
        [finalLiveBiPayload]
    );

    const findRecommendedActionTextForPnlRow = useCallback(
        (row: MonthlySkuwiseTableRow, liveRow?: any) => {
            const payload: any = finalLiveBiPayload || {};
            const recommended = payload.recommended_actions_mtd || {};

            const targetName = normalizeDrawerKey(
                row?.product_name || liveRow?.product_name || row?.sku
            );

            const targetSku = normalizeDrawerKey(row?.sku || liveRow?.sku);

            const flattenActionValues = (value: any): string[] => {
                if (!value) return [];

                if (typeof value === "string") return [value];

                if (Array.isArray(value)) {
                    return value.flatMap(flattenActionValues);
                }

                if (typeof value === "object") {
                    return Object.values(value).flatMap(flattenActionValues);
                }

                return [];
            };

            const actionTexts = flattenActionValues(recommended);

            return (
                actionTexts.find((text) => {
                    const firstLine = String(text || "")
                        .split(/\r?\n/)
                        .map((line) => line.trim())
                        .filter(Boolean)[0];

                    const normalizedFirstLine = normalizeDrawerKey(firstLine);
                    const normalizedFullText = normalizeDrawerKey(text);

                    return (
                        (!!targetName &&
                            (normalizedFirstLine === targetName ||
                                normalizedFirstLine.includes(targetName) ||
                                targetName.includes(normalizedFirstLine))) ||
                        (!!targetSku && normalizedFullText.includes(targetSku))
                    );
                }) || ""
            );
        },
        [finalLiveBiPayload]
    );

    const findGlobalJourneyForPnlRow = useCallback(
        (row: MonthlySkuwiseTableRow, liveRow?: any) => {
            const journeys = (finalLiveBiPayload as any)?.product_journey || {};
            const name = normalizeDrawerKey(row?.product_name || liveRow?.product_name);

            const direct = Object.entries(journeys).find(([key, value]: [string, any]) => {
                const keyName = normalizeDrawerKey(key);
                const productName = normalizeDrawerKey(value?.product_name);

                return (
                    keyName === name ||
                    productName === name ||
                    (!!productName && productName.includes(name)) ||
                    (!!name && productName && name.includes(productName))
                );
            });

            return direct?.[1] as any;
        },
        [finalLiveBiPayload]
    );

    const getFirstCountryActionForDrawer = (journey: any, country: "uk" | "us" | "ca" | "india") => {
        const blocks = journey?.[country] || {};
        return Object.values(blocks || {})[0] as any;
    };

    const openPnlSkuDrawer = useCallback(
        (row: MonthlySkuwiseTableRow) => {
            const name = String(row?.product_name || "").trim().toLowerCase();
            const sku = String(row?.sku || "").trim().toUpperCase();

            if (
                row.isTotal ||
                row.isOthers ||
                sku === "GRAND_TOTAL" ||
                sku === "TOTAL" ||
                name === "grand total" ||
                name === "total" ||
                name === "others"
            ) {
                return;
            }

            const liveRow = findLiveBiRowForPnlRow(row);
            const insight = findLiveBiInsightForPnlRow(row, liveRow);
            const productName =
                liveRow?.product_name ||
                insight?.product_name ||
                row.product_name ||
                row.sku ||
                "Details";

            let journeyPoints: string[] = [];
            let recommendationPoints: string[] = [];
            let advertisingPoints: string[] = [];
            let inventoryPoints: string[] = [];

            const recommendedActionText = findRecommendedActionTextForPnlRow(row, liveRow);
            const parsedRecommendedAction = extractDrawerSections(recommendedActionText);

            if (platform === "global") {
                const journey = insight?.raw_global_journey || findGlobalJourneyForPnlRow(row, liveRow);

                const countryActions = [
                    ["UK", getFirstCountryActionForDrawer(journey, "uk")],
                    ["US", getFirstCountryActionForDrawer(journey, "us")],
                    ["CA", getFirstCountryActionForDrawer(journey, "ca")],
                    ["India", getFirstCountryActionForDrawer(journey, "india")],
                ] as const;

                journeyPoints = Array.isArray(journey?.journey_comparison)
                    ? journey.journey_comparison
                    : toDrawerPoints(insight?.product_journey);

                recommendationPoints = countryActions
                    .map(([country, action]) =>
                        action?.recommendation ? `${country}: ${action.recommendation}` : ""
                    )
                    .filter(Boolean);

                advertisingPoints = countryActions
                    .map(([country, action]) =>
                        action?.ads_recommendation ? `${country}: ${action.ads_recommendation}` : ""
                    )
                    .filter(Boolean);

                inventoryPoints = countryActions
                    .map(([country, action]) =>
                        action?.inventory_recommendation
                            ? `${country}: ${action.inventory_recommendation}`
                            : ""
                    )
                    .filter(Boolean);

                // fallback from recommended_actions_mtd
                if (!journeyPoints.length) {
                    journeyPoints = parsedRecommendedAction.journeyPoints;
                }

                if (!recommendationPoints.length) {
                    recommendationPoints = parsedRecommendedAction.recommendationPoints;
                }

                if (!advertisingPoints.length) {
                    advertisingPoints = parsedRecommendedAction.advertisingPoints;
                }

                if (!inventoryPoints.length) {
                    inventoryPoints = parsedRecommendedAction.inventoryPoints;
                }
            } else {
                journeyPoints =
                    toDrawerPoints(insight?.product_journey).length > 0
                        ? toDrawerPoints(insight?.product_journey)
                        : parsedRecommendedAction.journeyPoints;

                recommendationPoints =
                    toDrawerPoints(insight?.recommendation).length > 0
                        ? toDrawerPoints(insight?.recommendation)
                        : parsedRecommendedAction.recommendationPoints;

                advertisingPoints =
                    toDrawerPoints(insight?.advertising).length > 0
                        ? toDrawerPoints(insight?.advertising)
                        : parsedRecommendedAction.advertisingPoints;

                inventoryPoints =
                    toDrawerPoints(insight?.inventory_recommendation).length > 0
                        ? toDrawerPoints(insight?.inventory_recommendation)
                        : parsedRecommendedAction.inventoryPoints;
            }

            setSelectedRec({
                productName,
                metrics: buildDrawerMetricsForPnlRow(row, liveRow),
                journeyPoints,
                recommendationPoints,
                advertisingPoints,
                inventoryPoints,
                showChart: true,
            });

            setRecDrawerOpen(true);
        },
        [
            platform,
            findLiveBiRowForPnlRow,
            findLiveBiInsightForPnlRow,
            findGlobalJourneyForPnlRow,
            findRecommendedActionTextForPnlRow,
            buildDrawerMetricsForPnlRow,
        ]
    );

    const currentInventoryExportRows = useMemo(() => {
        const rowsToUse = finalInventoryRows || [];

        return rowsToUse
            .filter((row) => {
                const productName = String((row as any)["Product Name"] ?? "").trim();
                const sku = String((row as any)["SKU"] ?? "").trim();
                return productName || sku;
            })
            .filter((row) => !isInventoryTotalRow(row))
            .map((row, index) => {
                const sku = String((row as any)["SKU"] ?? "").trim();

                const currentInventory =
                    Number(
                        (row as any)["Current Inventory"] ??
                        (row as any)["Available Inventory"] ??
                        (row as any)["Available Quantity"] ??
                        0
                    ) || 0;

                const currentMonthUnitsSoldKey = Object.keys(row || {}).find((k) =>
                    String(k).toLowerCase().startsWith("current month units sold")
                );

                const currentMonthUnitsSold =
                    Number(
                        (row as any)["Current Month Units Sold"] ??
                        (currentMonthUnitsSoldKey ? (row as any)[currentMonthUnitsSoldKey] : 0) ??
                        0
                    ) || 0;

                const explicitDaysInHand = (row as any)["Days in Hand"];
                const computedDaysInHand =
                    currentMonthUnitsSold > 0
                        ? Math.round(currentInventory / currentMonthUnitsSold)
                        : "";

                const rawAlert = String(inventoryAlerts?.[sku]?.alert ?? "").toLowerCase();

                let status =
                    (row as any)["Status"] ??
                    (rawAlert.includes("high")
                        ? "High Alert"
                        : rawAlert.includes("low")
                            ? "Low Stock"
                            : "Healthy");

                return {
                    "Sno.": index + 1,
                    "SKU": sku,
                    "Product Name": (row as any)["Product Name"] ?? "",
                    "Current Inventory": currentInventory,
                    "Current Month Units Sold": currentMonthUnitsSold,
                    "Days in Hand":
                        explicitDaysInHand !== undefined && explicitDaysInHand !== null && explicitDaysInHand !== ""
                            ? explicitDaysInHand
                            : computedDaysInHand,
                    "Status": status,
                };
            });
    }, [finalInventoryRows, inventoryAlerts, isInventoryTotalRow]);

    const handleCurrentInventoryExport = useCallback(() => {
        const titleCountry =
            graphRegionToUse === "Global" ? "Global" : graphRegionToUse;

        exportCurrentInventoryExcel({
            filename: `Current_Inventory_${titleCountry}_${formattedMonthYear}.xlsx`,
            titleLine: `Amazon ${titleCountry} - Current Inventory - ${formattedMonthYear}`,
            countryName: countryNameForGraph,
            titleCountry,
            platformLabel: "Phormula",
            periodLabel: formattedMonthYear,
            companyName,
            brandName: brandName || "",
            homeCurrencyCode: profileHomeCurrency,
            dataRows: currentInventoryExportRows,
        });
    }, [
        graphRegionToUse,
        formattedMonthYear,
        countryNameForGraph,
        companyName,
        brandName,
        profileHomeCurrency,
        currentInventoryExportRows,
    ]);




    const mtdCm2ProfitCurrent = shouldShowDummyUi
        ? dummyStatData.cm2Profit.current
        : useBiForAmazonCards
            ? (cm2Ready
                ? (
                    Number(biAlignedTotals?.total_current_net_sales ?? 0) !== 0
                        ? (
                            (
                                Number(biAlignedTotals?.total_current_profit_cm2 ?? 0) /
                                Number(biAlignedTotals?.total_current_net_sales ?? 0)
                            ) *
                            Number(biAlignedTotals?.total_current_net_sales ?? 0)
                        )
                        : 0
                )
                : 0)
            : Number(uk?.cm2ProfitGBP ?? 0);


    const mtdCm2ProfitPrevious = shouldShowDummyUi
        ? dummyStatData.cm2Profit.previous
        : useBiForAmazonCards
            ? (cm2Ready
                ? (
                    Number(biAlignedTotals?.total_previous_net_sales ?? 0) !== 0
                        ? (
                            (
                                Number(biAlignedTotals?.total_previous_profit_cm2 ?? 0) /
                                Number(biAlignedTotals?.total_previous_net_sales ?? 0)
                            ) *
                            Number(biAlignedTotals?.total_previous_net_sales ?? 0)
                        )
                        : 0
                )
                : 0)
            : Number(prev?.cm2Profit ?? 0);

    const rangeCm2ProfitCurrent = cachedRangeTotals.currentCm2Profit;

    const rangeCm2ProfitPrevious = cachedRangeTotals.previousCm2Profit;

    const rangeCm2ProfitPctCurrent = cachedRangeTotals.currentCm2Pct;

    const rangeCm2ProfitPctPrevious = cachedRangeTotals.previousCm2Pct;

    const mtdCm2ProfitCurrentDisplay = shouldShowDummyUi
        ? dummyStatData.cm2Profit.current
        : rangeActive
            ? rangeCm2ProfitCurrent
            : totalRowCm2Profit;

    const mtdCm2ProfitPreviousDisplay = shouldShowDummyUi
        ? dummyStatData.cm2Profit.previous
        : rangeActive
            ? rangeCm2ProfitPrevious
            : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency);

    const mtdCm2ProfitDelta = shouldShowDummyUi
        ? dummyStatData.cm2Profit.deltaPct
        : safeDeltaPct(
            mtdCm2ProfitCurrentDisplay,
            mtdCm2ProfitPreviousDisplay
        );

    const mtdCm2ProfitPctCurrent = shouldShowDummyUi
        ? dummyStatData.cm2ProfitPct.current
        : rangeActive
            ? rangeCm2ProfitPctCurrent
            : totalRowCm2Margins;

    const mtdCm2ProfitPctPrevious = shouldShowDummyUi
        ? dummyStatData.cm2ProfitPct.previous
        : rangeActive
            ? rangeCm2ProfitPctPrevious
            : Number(prev?.profitPct ?? 0);

    const mtdCm2ProfitPctDelta = shouldShowDummyUi
        ? dummyStatData.cm2ProfitPct.deltaPct
        : safeDeltaPct(
            mtdCm2ProfitPctCurrent,
            mtdCm2ProfitPctPrevious
        );

    const globalCm2ProfitCurrentRaw = globalUseBi
        ? (globalCm2Ready ? Number(biAlignedTotals?.total_current_profit_cm2 ?? 0) : 0)
        : Number(globalBottomCards.currentCm2Profit ?? 0);

    const globalCm2ProfitPreviousRaw = globalUseBi
        ? (globalCm2Ready ? Number(biAlignedTotals?.total_previous_profit_cm2 ?? 0) : 0)
        : Number(prev.cm2Profit ?? 0);

    const globalCm2ProfitCurrentDisplay = shouldShowDummyUi
        ? dummyStatData.cm2Profit.current
        : (globalUseBi
            ? convertToDisplayCurrency(globalCm2ProfitCurrentRaw, biSourceCurrency)
            : globalCm2ProfitCurrentRaw);

    const globalCm2ProfitPreviousDisplay = shouldShowDummyUi
        ? dummyStatData.cm2Profit.previous
        : (globalUseBi
            ? convertToDisplayCurrency(globalCm2ProfitPreviousRaw, biSourceCurrency)
            : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency));

    const globalCm2ProfitDelta = shouldShowDummyUi
        ? dummyStatData.cm2Profit.deltaPct
        : safeDeltaPct(globalCm2ProfitCurrentDisplay, globalCm2ProfitPreviousDisplay);

    const globalCm2ProfitPctCurrent = shouldShowDummyUi
        ? dummyStatData.cm2ProfitPct.current
        : (() => {
            const sales = globalUseBi
                ? Number(biAlignedTotals?.total_current_net_sales ?? 0)
                : Number(globalCurrNetSalesDisp ?? 0);

            const profit = globalUseBi
                ? Number(biAlignedTotals?.total_current_profit_cm2 ?? 0)
                : Number(globalCm2ProfitCurrentDisplay ?? 0);

            return sales > 0 ? (profit / sales) * 100 : 0;
        })();

    const globalCm2ProfitPctPrevious = shouldShowDummyUi
        ? dummyStatData.cm2ProfitPct.previous
        : (() => {
            const sales = globalUseBi
                ? Number(biAlignedTotals?.total_previous_net_sales ?? 0)
                : Number(globalPrevNetSalesDisp ?? 0);

            const profit = globalUseBi
                ? Number(biAlignedTotals?.total_previous_profit_cm2 ?? 0)
                : Number(globalCm2ProfitPreviousDisplay ?? 0);

            return sales > 0 ? (profit / sales) * 100 : 0;
        })();

    const globalCm2ProfitPctDelta = shouldShowDummyUi
        ? dummyStatData.cm2ProfitPct.deltaPct
        : safeDeltaPct(globalCm2ProfitPctCurrent, globalCm2ProfitPctPrevious);

    const remainingSteps = dashboardSteps.length - currentStep;

    const secondsLeft = remainingSteps * 30;

    const getCountryMtdCardData = useCallback((country: "uk" | "us") => {
        const currentRows =
            country === "uk"
                ? Array.isArray(data?.skuwise_items_uk)
                    ? data.skuwise_items_uk
                    : []
                : Array.isArray(data?.skuwise_items_us)
                    ? data.skuwise_items_us
                    : [];

        const currentGrand = getGrandTotalRow(currentRows) as GrandTotalSkuwiseRow;

        const prevDerived =
            country === "uk"
                ? previousSkuwiseGlobalData?.derived_totals_uk || {}
                : previousSkuwiseGlobalData?.derived_totals_us || {};

        const prevAligned =
            country === "uk"
                ? previousSkuwiseGlobalData?.aligned_totals_uk || {}
                : previousSkuwiseGlobalData?.aligned_totals_us || {};

        return {
            units: getNetUnits(currentGrand),
            prevUnits: toNumber(
                prevDerived.total_quantity ??
                prevDerived.net_quantity ??
                prevDerived.quantity
            ),

            grossSales: toNumber(currentGrand.gross_sales),
            prevGrossSales: toNumber(prevDerived.gross_sales),

            netSales: toNumber(currentGrand.net_sales),
            prevNetSales: toNumber(prevDerived.net_sales),

            asp: toNumber(currentGrand.asp),
            prevAsp: toNumber(prevDerived.asp),

            ads: toNumber(
                currentGrand.total_ads ??
                currentGrand.advertising_fees ??
                currentGrand.ads_spend
            ),
            prevAds: toNumber(
                prevAligned.total_previous_advertising ??
                prevDerived.advertising_fees
            ),

            tacos: toNumber(
                currentGrand.tacos_total_advertising_cost_of_sale ??
                currentGrand.acos
            ),
            prevTacos: toNumber(
                prevDerived.net_sales
                    ? (toNumber(prevAligned.total_previous_advertising ?? prevDerived.advertising_fees) /
                        toNumber(prevDerived.net_sales)) * 100
                    : 0
            ),

            cm2Profit: toNumber(
                currentGrand.total_cm2_profit ??
                currentGrand.cm2_profit
            ),
            prevCm2Profit: toNumber(
                prevAligned.total_previous_profit_cm2 ??
                prevDerived.cm2_profit
            ),

            cm2Pct: toNumber(
                currentGrand.total_cm2_margins ??
                currentGrand.profit_percentage ??
                currentGrand.cm2_profit_per
            ),
            prevCm2Pct: toNumber(
                prevAligned.total_previous_profit_percentage ??
                prevDerived.cm2_profit_percentage
            ),
        };
    }, [data, previousSkuwiseGlobalData]);

    const renderCountryMtdCards = (country: "uk" | "us") => {
        const c = getCountryMtdCardData(country);
        const title = country === "uk" ? "UK MTD Sales (USD)" : "US MTD Sales (USD)";

        return (
            <div className="">
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                    <AmazonStatCard
                        label="Units"
                        current={c.units}
                        previous={c.prevUnits}
                        deltaPct={safeDeltaPct(c.units, c.prevUnits)}
                        formatter={fmtInt}
                        bottomLabel={prevLabel}
                        className="border-[#FDD36F] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="Gross Sales"
                        current={c.grossSales}
                        previous={c.prevGrossSales}
                        deltaPct={safeDeltaPct(c.grossSales, c.prevGrossSales)}
                        formatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                        previousFormatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                        bottomLabel={prevLabel}
                        className="border-[#ED9F50] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="Net Sales"
                        current={c.netSales}
                        previous={c.prevNetSales}
                        deltaPct={safeDeltaPct(c.netSales, c.prevNetSales)}
                        formatter={(val) => formatDisplayAmount(val, "Net Sales")}
                        previousFormatter={(val) => formatDisplayAmount(val, "Net Sales")}
                        bottomLabel={prevLabel}
                        className="border-[#6BBDE3] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="ASP"
                        current={c.asp}
                        previous={c.prevAsp}
                        deltaPct={safeDeltaPct(c.asp, c.prevAsp)}
                        formatter={formatDisplayAmount}
                        previousFormatter={formatDisplayAmount}
                        bottomLabel={prevLabel}
                        className="border-[#B75A5A] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="Cost of Ads"
                        current={c.ads}
                        previous={c.prevAds}
                        deltaPct={safeDeltaPct(c.ads, c.prevAds)}
                        inverseDelta
                        formatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                        previousFormatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                        bottomLabel={prevLabel}
                        className="border-[#C58A5A] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="TACoS"
                        current={c.tacos}
                        previous={c.prevTacos}
                        deltaPct={safeDeltaPct(c.tacos, c.prevTacos)}
                        inverseDelta
                        formatter={fmtPct2}
                        bottomLabel={prevLabel}
                        className="border-[#3A8EA4] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="CM2 Profit"
                        current={c.cm2Profit}
                        previous={c.prevCm2Profit}
                        deltaPct={safeDeltaPct(c.cm2Profit, c.prevCm2Profit)}
                        formatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                        previousFormatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                        bottomLabel={prevLabel}
                        className="border-[#A8BE7A] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="CM2 Profit %"
                        current={c.cm2Pct}
                        previous={c.prevCm2Pct}
                        deltaPct={safeDeltaPct(c.cm2Pct, c.prevCm2Pct)}
                        formatter={fmtPct2}
                        previousFormatter={fmtPct2}
                        bottomLabel={prevLabel}
                        className="border-[#6D8F61] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />
                </div>
            </div>
        );
    };

    const renderGlobalMtdCards = () => {
        const c = globalMtdCardData;

        return (
            <div className="">
                <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
                    <AmazonStatCard
                        label="Units"
                        current={c.units}
                        previous={c.prevUnits}
                        deltaPct={null}
                        formatter={fmtInt}
                        bottomLabel={prevLabel}
                        className="border-[#FDD36F] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="Gross Sales"
                        current={c.grossSales}
                        previous={c.prevGrossSales}
                        deltaPct={null}
                        formatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                        previousFormatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                        bottomLabel={prevLabel}
                        className="border-[#ED9F50] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="Net Sales"
                        current={c.netSales}
                        previous={c.prevNetSales}
                        deltaPct={null}
                        formatter={(val) => formatDisplayAmount(val, "Net Sales")}
                        previousFormatter={(val) => formatDisplayAmount(val, "Net Sales")}
                        bottomLabel={prevLabel}
                        className="border-[#6BBDE3] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="ASP"
                        current={c.asp}
                        previous={c.prevAsp}
                        deltaPct={null}
                        formatter={formatDisplayAmount}
                        previousFormatter={formatDisplayAmount}
                        bottomLabel={prevLabel}
                        className="border-[#B75A5A] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="Cost of Ads"
                        current={c.ads}
                        previous={c.prevAds}
                        deltaPct={null}
                        inverseDelta
                        formatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                        previousFormatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                        bottomLabel={prevLabel}
                        className="border-[#C49466] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="TACoS"
                        current={c.tacos}
                        previous={c.prevTacos}
                        deltaPct={null}
                        inverseDelta
                        formatter={fmtPct2}
                        previousFormatter={fmtPct2}
                        bottomLabel={prevLabel}
                        className="border-[#3A8EA4] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="CM2 Profit"
                        current={c.cm2Profit}
                        previous={c.prevCm2Profit}
                        deltaPct={null}
                        formatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                        previousFormatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                        bottomLabel={prevLabel}
                        className="border-[#B8C78C] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="CM2 Profit %"
                        current={c.cm2Pct}
                        previous={c.prevCm2Pct}
                        deltaPct={null}
                        formatter={fmtPct2}
                        previousFormatter={fmtPct2}
                        bottomLabel={prevLabel}
                        className="border-[#7B9A6D] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />
                </div>
            </div>
        );
    };

    const renderLiveNetSalesDelta = (row: MonthlySkuwiseTableRow) => {
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
                className={`shrink-0 text-xs font-semibold ${isPositive ? "text-[#5EA68E]" : "text-[#FF5C5C]"
                    }`}
                title={`Previous Net Sales: ${Math.round(
                    Math.abs(toNumber(row.previous_net_sales))
                ).toLocaleString()}`}
            >
                {isPositive ? "▲" : "▼"} {Math.abs(rawPct).toFixed(2)}%
            </span>
        );
    };

    const MTD_VISIBLE_PRODUCT_ROWS = 15;

    const MTD_HEADER_ROW_HEIGHT = 60;
    const MTD_SIGN_ROW_HEIGHT = 45;
    const MTD_PRODUCT_ROW_HEIGHT = 45;

    const mtdProductRowCount = finalMonthlySkuwiseRowsForTable.filter((row) => {
        const name = String(row?.product_name || "").trim().toLowerCase();
        const sku = String(row?.sku || "").trim().toUpperCase();

        return (
            !row.isTotal &&
            !row.isOthers &&
            sku !== "GRAND_TOTAL" &&
            sku !== "TOTAL" &&
            name !== "grand total" &&
            name !== "total" &&
            name !== "others"
        );
    }).length;

    const shouldScrollMtdProductwiseTable =
        showAllMtdProductwiseRows && mtdProductRowCount > MTD_VISIBLE_PRODUCT_ROWS;

    const mtdProductwiseTableScrollHeight =
        MTD_HEADER_ROW_HEIGHT +
        MTD_SIGN_ROW_HEIGHT +
        MTD_PRODUCT_ROW_HEIGHT * MTD_VISIBLE_PRODUCT_ROWS;

    return (
        <div className="relative w-full">
            <Toaster
                position="top-right"
                richColors
                closeButton={false}
                toastOptions={{
                    duration: Infinity,
                }}
            />
            <HashScroll offset={80} />

            {!shouldShowDummyUi && showDashboardStepLoader && (
                <DashboardLoaderModal
                    pageLoading={showDashboardStepLoader}
                    shouldShowDummyUi={shouldShowDummyUi}
                    currentStep={currentStep}
                    completedSteps={completedSteps}
                    dashboardSteps={dashboardSteps}
                    stepProgress={stepProgress}
                    loadingStartedAt={loadingStartedAt}
                    estimatedSecondsMap={STEP_ESTIMATED_SECONDS}
                />
            )}

            {!shouldShowDummyUi && pageLoading && !showDashboardStepLoader && (
                <Loader
                    fullscreen
                    contained
                    backgroundClass="bg-white/40"
                />
            )}

            <div className="sticky top-0 z-40 bg-[#F7F7F7] ">
                <div className="flex items-center justify-between gap-2">

                    {/* LEFT SIDE */}
                    <div className="flex flex-col leading-tight min-w-0">
                        <p className="text-xs sm:text-sm 2xl:text-lg text-charcoal-500 mb-1 truncate">
                            Let&apos;s get started,{" "}
                            <span className="text-green-500">{brandName}!</span>
                        </p>

                        <div className="flex items-center gap-1 flex-wrap">
                            <PageBreadcrumb
                                pageTitle="Sales Dashboard - Amazon"
                                variant="page"
                                textSize="2xl"
                            />

                            <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                                {countryName === "global" ? "Global" : countryName.toUpperCase()}
                            </span>

                            <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                                - {formattedMonthYear}
                            </span>
                        </div>
                    </div>

                    <div className="flex flex-col items-end gap-1">
                        <button
                            onClick={handleHardRefresh}
                            disabled={pageLoading}
                            className={`shrink-0 rounded-md border shadow-sm
px-2 py-1 text-[10px]
sm:px-3 sm:py-1.5 sm:text-xs
2xl:text-sm
${pageLoading
                                    ? "cursor-not-allowed border-gray-200 bg-gray-100 text-gray-400"
                                    : "border-gray-300 bg-white hover:bg-gray-50"
                                }`}
                        >
                            {pageLoading ? "Refreshing…" : "Refresh"}
                        </button>

                        {lastRefreshAt != null && (
                            <span className="text-xs 2xl:text-sm text-gray-500 whitespace-nowrap">
                                Last Updated at{" "}
                                {lastUpdatedTimeText ||
                                    (activeDateRegion === "US"
                                        ? formatUSTime12hr(lastRefreshAt)
                                        : activeDateRegion === "CA"
                                            ? formatLastUpdatedDateTime(lastRefreshAt, "America/Toronto")
                                            : formatUKTime12hr(lastRefreshAt))}
                            </span>
                        )}
                    </div>
                </div>
            </div>

            <div className="sticky max-[480px]:top-[44px] max-[640px]:top-[44px] sm:top-[48px] md:top-[48px] 2xl:top-[56px] z-30 bg-[#F7F7F7] border-b border-gray-200 
    max-[480px]:py-1 max-[640px]:pb-2 sm:py-2">
                <SegmentedToggle<TopTab>
                    value={activeTab}
                    options={TOP_TABS.map((t) => ({ value: t.id, label: t.label }))}
                    onChange={(tab) => {
                        setActiveTab(tab);
                        syncTabToHash(tab);
                    }}
                    className="mt-2 w-full"
                    compact
                    textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
                />
            </div>

            <PreviewLockedSection
                enabled={isUsingDummyData}
                title="Preview Mode"
                description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
                buttonText="Complete Setup"
                onAction={handleConnectAmazonPreview}
            >
                {["summary", "productwise", "inventory"].includes(activeTab) && (
                    <DashboardStickyKpis items={stickyKpiItems} />
                )}

                {activeTab === "live" && (
                    <div
                        id="live-sales"
                        className="grid grid-cols-12 gap-4 mt-2 md:mt-4 scroll-mt-[80px] items-start"
                    >
                        {/* LEFT COLUMN */}
                        <div
                            className="col-span-12 lg:col-span-8 order-2 lg:order-1 flex flex-col gap-4 min-w-0 h-auto min-h-0"
                        >
                            {/* AMAZON SECTION */}
                            {hasAmazonCard && (

                                <div className="flex flex-col gap-4 2xl:gap-4">
                                    {/* Amazon KPI Box */}
                                    <div className="w-full rounded-xl border bg-white p-3 2xl:p-5 shadow-sm">
                                        <div className="mb-3 lg:mb-2 2xl:mb-4 flex items-center justify-between gap-2 sm:gap-3">
                                            <div className="min-w-0">
                                                <PageBreadcrumb pageTitle="MTD Sales" variant="page" align="left" />
                                            </div>

                                            {/* RIGHT: TOGGLE (only for global) */}
                                            {platform === "global" && globalMtdViewOptions.length > 1 && (
                                                <SegmentedToggle<GlobalMtdView>
                                                    value={globalMtdView}
                                                    options={globalMtdViewOptions}
                                                    onChange={setGlobalMtdView}
                                                    compact
                                                    textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
                                                />
                                            )}

                                            {showLiveBI && isCountryMode && (
                                                <div className="ml-auto shrink-0">
                                                    <RangePicker
                                                        selectedStartDay={selectedStartDay}
                                                        selectedEndDay={selectedEndDay}
                                                        label={formatAppliedRangeLabel(selectedStartDay, selectedEndDay)}
                                                        onSubmit={(s, e) => {
                                                            setSelectedStartDay(s);
                                                            setSelectedEndDay(e);
                                                            setBiError(null);
                                                        }}
                                                        onClear={() => {
                                                            setSelectedStartDay(null);
                                                            setSelectedEndDay(null);
                                                            setBiError(null);
                                                        }}
                                                        onCloseReset={() => {
                                                            setSelectedStartDay(null);
                                                            setSelectedEndDay(null);
                                                            setBiError(null);
                                                        }}
                                                    />
                                                </div>
                                            )}
                                        </div>

                                        {platform === "global" ? (
                                            globalMtdView === "global" ? (
                                                <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-2 lg:gap-2 2xl:gap-3 auto-rows-fr">
                                                    <AmazonStatCard
                                                        label="Units"
                                                        current={shouldShowDummyUi ? dummyStatData.units.current : globalMtdCardData.units}
                                                        previous={shouldShowDummyUi ? dummyStatData.units.previous : globalMtdCardData.prevUnits}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.units.current, dummyStatData.units.previous)
                                                                : safeDeltaPct(globalMtdCardData.units, globalMtdCardData.prevUnits)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={fmtInt}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                                                    />

                                                    <AmazonStatCard
                                                        label="Gross Sales"
                                                        current={shouldShowDummyUi ? dummyStatData.grossSales.current : globalMtdCardData.grossSales}
                                                        previous={shouldShowDummyUi ? dummyStatData.grossSales.previous : globalMtdCardData.prevGrossSales}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.grossSales.current, dummyStatData.grossSales.previous)
                                                                : safeDeltaPct(globalMtdCardData.grossSales, globalMtdCardData.prevGrossSales)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                                                    />

                                                    <AmazonStatCard
                                                        label="Net Sales"
                                                        current={shouldShowDummyUi ? dummyStatData.netSales.current : globalMtdCardData.netSales}
                                                        previous={shouldShowDummyUi ? dummyStatData.netSales.previous : globalMtdCardData.prevNetSales}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.netSales.current, dummyStatData.netSales.previous)
                                                                : safeDeltaPct(globalMtdCardData.netSales, globalMtdCardData.prevNetSales)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"
                                                    />

                                                    <AmazonStatCard
                                                        label="ASP"
                                                        current={shouldShowDummyUi ? dummyStatData.asp.current : globalMtdCardData.asp}
                                                        previous={shouldShowDummyUi ? dummyStatData.asp.previous : globalMtdCardData.prevAsp}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.asp.current, dummyStatData.asp.previous)
                                                                : safeDeltaPct(globalMtdCardData.asp, globalMtdCardData.prevAsp)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={formatDisplayAmount}
                                                        previousFormatter={formatDisplayAmount}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                                    />

                                                    <AmazonStatCard
                                                        label="Cost of Ads"
                                                        current={shouldShowDummyUi ? dummyStatData.costOfAds.current : globalMtdCardData.ads}
                                                        previous={shouldShowDummyUi ? dummyStatData.costOfAds.previous : globalMtdCardData.prevAds}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.costOfAds.current, dummyStatData.costOfAds.previous)
                                                                : safeDeltaPct(globalMtdCardData.ads, globalMtdCardData.prevAds)
                                                        }
                                                        inverseDelta
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                                    />

                                                    <AmazonStatCard
                                                        label="TACoS"
                                                        current={shouldShowDummyUi ? dummyStatData.tacos.current : globalMtdCardData.tacos}
                                                        previous={shouldShowDummyUi ? dummyStatData.tacos.previous : globalMtdCardData.prevTacos}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.tacos.current, dummyStatData.tacos.previous)
                                                                : safeDeltaPct(globalMtdCardData.tacos, globalMtdCardData.prevTacos)
                                                        }
                                                        inverseDelta
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={fmtPct2}
                                                        previousFormatter={fmtPct2}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                                    />

                                                    <AmazonStatCard
                                                        label="CM2 Profit"
                                                        current={shouldShowDummyUi ? dummyStatData.cm2Profit.current : globalMtdCardData.cm2Profit}
                                                        previous={shouldShowDummyUi ? dummyStatData.cm2Profit.previous : globalMtdCardData.prevCm2Profit}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.cm2Profit.current, dummyStatData.cm2Profit.previous)
                                                                : safeDeltaPct(globalMtdCardData.cm2Profit, globalMtdCardData.prevCm2Profit)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#A8BF7A] border-t-4 border-t-[#A8BF7A]"
                                                    />

                                                    <AmazonStatCard
                                                        label="CM2 Profit %"
                                                        current={shouldShowDummyUi ? dummyStatData.cm2ProfitPct.current : globalMtdCardData.cm2Pct}
                                                        previous={shouldShowDummyUi ? dummyStatData.cm2ProfitPct.previous : globalMtdCardData.prevCm2Pct}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? deltaPctPoints(dummyStatData.cm2ProfitPct.current, dummyStatData.cm2ProfitPct.previous)
                                                                : deltaPctPoints(globalMtdCardData.cm2Pct, globalMtdCardData.prevCm2Pct)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={fmtPct2}
                                                        previousFormatter={fmtPct2}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                                                    />
                                                </div>
                                            ) : (
                                                <div className="space-y-4">
                                                    {renderCountryMtdCards(globalMtdView)}
                                                </div>
                                            )
                                        ) : (
                                            <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-2 lg:gap-2 2xl:gap-3 auto-rows-fr">

                                                <AmazonStatCard
                                                    label="Units"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.units.current
                                                            : (useBiForAmazonCards ? biCardKpis.curr.units : mtdUnitsCurrent)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.units.previous
                                                            : (useBiForAmazonCards ? biCardKpis.prev.units : mtdUnitsPrevious)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.units.deltaPct
                                                            : (useBiForAmazonCards ? biCardKpis.deltas.units : mtdUnitsDelta)
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={fmtInt}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                                                />

                                                <AmazonStatCard
                                                    label="Gross Sales"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.grossSales.current
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.curr.grossSales
                                                                : convertToDisplayCurrency(uk.grossSalesGBP ?? 0, amazonDataCurrency)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.grossSales.previous
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.prev.grossSales
                                                                : convertToDisplayCurrency(prev.grossSales ?? 0, amazonDataCurrency)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.grossSales.deltaPct
                                                            : (useBiForAmazonCards
                                                                ? biCardKpis.deltas.grossSales
                                                                : safeDeltaPct(uk.grossSalesGBP ?? 0, prev.grossSales ?? 0))
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                                                />

                                                <AmazonStatCard
                                                    label="Net Sales"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.netSales.current
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.curr.netSales
                                                                : convertToDisplayCurrency(uk.netSalesGBP ?? 0, amazonDataCurrency)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.netSales.previous
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.prev.netSales
                                                                : convertToDisplayCurrency(prev.netSales, amazonDataCurrency)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.netSales.deltaPct
                                                            : (useBiForAmazonCards ? biCardKpis.deltas.netSales : deltas.netSalesPct)
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"
                                                />

                                                <AmazonStatCard
                                                    label="ASP"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.asp.current
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.curr.asp
                                                                : convertToDisplayCurrency(uk.aspGBP ?? 0, amazonDataCurrency)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.asp.previous
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.prev.asp
                                                                : convertToDisplayCurrency(prev.asp, amazonDataCurrency)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.asp.deltaPct
                                                            : (useBiForAmazonCards ? biCardKpis.deltas.asp : deltas.aspPct)
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={formatDisplayAmount}
                                                    previousFormatter={formatDisplayAmount}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                                />

                                                <AmazonStatCard
                                                    label="Cost of Ads"
                                                    current={mtdCostOfAdsCurrentDisplay}
                                                    previous={mtdCostOfAdsPreviousDisplay}
                                                    deltaPct={mtdCostOfAdsDelta}
                                                    inverseDelta
                                                    loading={!shouldShowDummyUi && (loading || (rangeActive ? biLoading : false))}
                                                    formatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                                />

                                                <AmazonStatCard
                                                    label="TACoS"
                                                    current={mtdTacosCurrent}
                                                    previous={mtdTacosPrevious}
                                                    deltaPct={mtdTacosDelta}
                                                    inverseDelta
                                                    loading={!shouldShowDummyUi && (loading || (rangeActive ? biLoading : false))}
                                                    formatter={fmtPct2}
                                                    previousFormatter={fmtPct2}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                                />

                                                <AmazonStatCard
                                                    label="CM2 Profit"
                                                    current={mtdCm2ProfitCurrentDisplay}
                                                    previous={mtdCm2ProfitPreviousDisplay}
                                                    deltaPct={mtdCm2ProfitDelta}
                                                    loading={!shouldShowDummyUi && loading}
                                                    formatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#B8C78C] border-t-4 border-t-[#B8C78C]"
                                                />

                                                <AmazonStatCard
                                                    label="CM2 Profit %"
                                                    current={mtdCm2ProfitPctCurrent}
                                                    previous={mtdCm2ProfitPctPrevious}
                                                    deltaPct={mtdCm2ProfitPctDelta}
                                                    loading={!shouldShowDummyUi && loading}
                                                    formatter={fmtPct2}
                                                    previousFormatter={fmtPct2}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                                                />
                                            </div>
                                        )}



                                    </div>



                                    {/* Live BI graph */}
                                    {/* {showLiveBI && isCountryMode && (
                                        <div className="w-full rounded-xl border bg-white p-3 lg:p-3 2xl:p-5 shadow-sm overflow-x-hidden">
                                            <div className="w-full max-w-full min-w-0">

                                                {!shouldShowDummyUi && biStatus === "processing" && (
                                                    <div className="flex justify-center items-center py-10">
                                                        <Loader className="bg-transparent" />
                                                    </div>
                                                )}

                                                {!shouldShowDummyUi && biStatus === "error" && (
                                                    <div className="text-center py-10 text-sm text-red-500">
                                                        Taking longer than expected. Please refresh once.
                                                    </div>
                                                )}

                                                {!shouldShowDummyUi && biStatus === "ready" && !biDailySeriesHome && (
                                                    <div className="text-center py-10 text-sm text-gray-500">
                                                        No data available for the selected period
                                                    </div>
                                                )}

                                              
                                                {(shouldShowDummyUi || biStatus === "ready") && finalBiDailySeriesHome && (

                                                    <LiveBiLineGraph
                                                        dailySeries={finalBiDailySeriesHome}
                                                        periods={finalBiPeriods}
                                                        loading={liveBiGraphLoading}
                                                        isRefreshing={!shouldShowDummyUi && biUiLoading && !!finalBiDailySeriesHome}
                                                        error={shouldShowDummyUi ? null : biError}
                                                        selectedStartDay={selectedStartDay}
                                                        selectedEndDay={selectedEndDay}
                                                        currencySymbol={currencySymbol}
                                                    />

                                                )}

                                            </div>
                                        </div>
                                    )} */}
                                </div>

                            )}

                            {/* Shopify Block */}
                            {!isCountryMode && hasShopifyCard && (
                                <div className="flex lg:flex-1">
                                    <div className="w-full rounded-xl border bg-white p-5 shadow-sm">
                                        <div className="mb-4 flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                                            <div className="flex flex-col">
                                                <div className="flex items-baseline gap-2">
                                                    <PageBreadcrumb
                                                        pageTitle="Shopify"
                                                        variant="page"
                                                        align="left"
                                                        textSize="2xl"
                                                    />
                                                </div>
                                            </div>
                                        </div>
                                        {/* Shopify Block */}
                                        {!isCountryMode && hasShopifyCard && (
                                            <div className="flex lg:flex-1">
                                                <div className="w-full rounded-xl border bg-white p-5 shadow-sm">
                                                    <div className="mb-4 flex flex-col gap-4 md:flex-row md:items-start md:justify-between ">
                                                        <div className="flex flex-col">
                                                            <div className="flex items-baseline gap-2">
                                                                <PageBreadcrumb
                                                                    pageTitle="Shopify"
                                                                    variant="page"
                                                                    align="left"
                                                                    textSize="2xl"
                                                                />
                                                            </div>
                                                        </div>
                                                    </div>

                                                    {shopifyLoading ? (
                                                        <div className="mt-3 text-sm text-gray-500">Loading Shopify…</div>
                                                    ) : shopify ? (
                                                        <div className="grid grid-cols-2 gap-3 sm:grid-cols-2 lg:grid-cols-3">

                                                            <AmazonStatCard
                                                                label="Units"
                                                                current={shopifyDeriv?.totalOrders ?? 0}
                                                                previous={shopifyPrevDeriv?.totalOrders ?? 0}
                                                                loading={shopifyLoading}
                                                                formatter={fmtInt}
                                                                bottomLabel={prevLabel}
                                                                // className="border-[#FDD36F] bg-[#FDD36F4D]"
                                                                className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                                                            />
                                                            <AmazonStatCard
                                                                label="Sales"
                                                                current={convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR")}
                                                                previous={convertToDisplayCurrency(shopifyPrevDeriv?.netSales ?? 0, "INR")}
                                                                loading={shopifyLoading}
                                                                formatter={formatDisplayAmount}
                                                                bottomLabel={prevLabel}
                                                                // className="border-[#75BBDA] bg-[#75BBDA4D]"
                                                                className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"

                                                            />
                                                            <AmazonStatCard
                                                                label="ASP"
                                                                current={(() => {
                                                                    const units = shopifyDeriv?.totalOrders ?? 0;
                                                                    if (!units) return 0;
                                                                    const net = convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR");
                                                                    return net / units;
                                                                })()}
                                                                previous={0}
                                                                loading={shopifyLoading}
                                                                formatter={formatDisplayAmount}
                                                                bottomLabel={prevLabel}
                                                                // className="border-[#B75A5A] bg-[#B75A5A4D]"
                                                                className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                                            />
                                                        </div>
                                                    ) : (
                                                        <div className="mt-2 text-sm text-gray-500">
                                                            No Shopify data for the current month.
                                                        </div>
                                                    )}
                                                </div>
                                            </div>
                                        )}
                                    </div>

                                    <AmazonStatCard
                                        label="ASP"
                                        current={(() => {
                                            const units = shopifyDeriv?.totalOrders ?? 0;
                                            if (!units) return 0;
                                            const net = convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR");
                                            return net / units;
                                        })()}
                                        previous={0}
                                        loading={shopifyLoading}
                                        formatter={formatDisplayAmount}
                                        bottomLabel={prevLabel}
                                        // className="border-[#B75A5A] bg-[#B75A5A4D] "
                                        className="border-[#B75A5A] bg-[#B75A5A4D] "

                                    />
                                </div>
                            )}

                            {showLiveBI && (
                                <div
                                    id="ai-insights"
                                    className="w-full max-w-full min-w-0 rounded-xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden scroll-mt-[80px]"
                                >
                                    <div className="w-full max-w-full min-w-0 h-full">
                                        <LiveBiLineGraph
                                            dailySeries={finalBiDailySeriesHome}
                                            periods={finalBiPeriods}
                                            loading={!shouldShowDummyUi && biUiLoading}
                                            error={shouldShowDummyUi ? null : biError}
                                            selectedStartDay={selectedStartDay}
                                            selectedEndDay={selectedEndDay}
                                            currencySymbol={currencySymbol}
                                        />
                                    </div>
                                </div>
                            )}
                        </div>

                        {/* RIGHT COLUMN – Sales Target */}
                        <aside className="col-span-12 lg:col-span-4 order-1 lg:order-2 h-auto min-h-0 self-start">
                            <div className="grid gap-4 h-auto">

                                {/* Top card */}
                                <div className="w-full self-start">

                                    <SalesTargetStatsCard
                                        regions={regions}
                                        value={targetRegion}
                                        onChange={setTargetRegion}
                                        hideTabs={isCountryMode}
                                        homeCurrency={displayCurrency}
                                        formatHomeK={formatDisplayK}
                                        todayHome={finalTargetsTodayHome}
                                        mtdHome={finalTargetsMtdHome}
                                        targetHome={finalStatsTargetHome}
                                        lastMonthTotalHome={finalTargetsLastMonthTotalHome}
                                        lastMonthToDateHome={targets_lastMonthToDateHome}
                                        salesTrendPct={finalStatsSalesTrendPct}
                                        targetTrendPct={finalStatsTargetTrendPct}
                                        currentReimbursement={finalTargetsReimbursement.current}
                                        previousReimbursement={finalTargetsReimbursement.previous}
                                        biAlignedTotals={shouldShowDummyUi ? null : biAlignedTotalsHome}
                                        biEnabled={shouldShowDummyUi ? false : biCardsReady}
                                        currentMonthLabel={formattedMonthYear}
                                        previousMonthLabel={prevLabel}
                                        currentMonthName={currentDisplayMonth.monthName}
                                        currentYear={currentDisplayMonth.year}
                                    />

                                </div>

                                {/* Bottom card */}
                                <div className="h-auto lg:h-full lg:sticky lg:top-4 2xl:top-6">

                                    <SalesTargetCard
                                        data={regions[targetRegion]}
                                        homeCurrency={displayCurrency}
                                        convertToHomeCurrency={(value, from) => convertToDisplayCurrency(value, from)}
                                        formatHomeK={formatDisplayK}
                                        todaySales={todaySalesRaw}
                                        targetHome={stickyTargetHome}
                                        mtdHome={targets_mtdHome}
                                        lastMonthTotalHome={targets_lastMonthTotalHome}
                                        lastMonthToDateHome={targets_lastMonthToDateHome}
                                        currentMonthLabel={formattedMonthYear}
                                        previousMonthLabel={prevLabel}
                                        currentReimbursement={targets_reimbursement.current}
                                        previousReimbursement={targets_reimbursement.previous}
                                        reimbursementDeltaPct={
                                            platform === "global"
                                                ? globalTargetCardTotals.reimbursementDeltaPct
                                                : safeDeltaPct(
                                                    targets_reimbursement.current,
                                                    targets_reimbursement.previous
                                                )
                                        }
                                        biAlignedTotals={shouldShowDummyUi ? null : salesTargetBiAlignedTotals}
                                        biEnabled={
                                            shouldShowDummyUi
                                                ? false
                                                : platform === "global"
                                                    ? true
                                                    : biCardsReady
                                        }

                                        // ✅ add these
                                        periodCompletedPct={finalRangeCompletedPct}
                                        periodCompletedLabel="Month"
                                    />
                                </div>
                            </div>
                        </aside>
                    </div >
                )}

                {activeTab === "summary" && (

                    <div className="w-full overflow-x-hidden">
                        {(!shouldShowDummyUi && (summaryLoading || !liveBiPayload)) ? (
                            <div className="flex min-h-[300px] items-center justify-center py-12 text-center">
                                <Loader />
                            </div>
                        ) : (
                            showLiveBI && (
                                <LiveBusinessClient
                                    countryName={countryName}
                                    sourceCountryName={countryName}
                                    ranged="MTD"
                                    month={(currMonthName || "").toLowerCase()}
                                    year={String(currYear)}
                                    initialData={finalLiveBiPayload}
                                    disableAutoFetch
                                    onGenerateInsights={async () => {
                                        if (shouldShowDummyUi) return;

                                        await fetchLiveBiPayload({
                                            generateInsights: true,
                                            skipLoader: true,
                                        });
                                    }}
                                />
                            )
                        )}
                    </div>
                )}

                {activeTab === "productwise" && (
                    <>
                        <div id="pnl-mtd" className="scroll-mt-[80px] mt-2 md:mt-4 w-full rounded-xl border bg-white p-4 sm:p-5 shadow-sm overflow-hidden">
                            <div className="mb-3 relative flex items-center justify-between gap-3">
                                {/* LEFT: Title */}
                                <div className="flex items-center gap-2">
                                    <PageBreadcrumb
                                        pageTitle="P&L Productwise Breakdown"
                                        variant="page"
                                        align="left"
                                        textSize="2xl"
                                    />
                                    <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl text-green-500 font-semibold">
                                        ({currencySymbol})
                                    </span>
                                </div>
                                {/* CENTER: Ads loading message */}
                                {adsLoading && (
                                    <div className="absolute left-1/2 -translate-x-1/2 flex items-center gap-2 text-sm 2xl:text-base text-charcoal-700 font-medium">
                                        <span className="inline-block h-2.5 w-2.5 rounded-full bg-charcoal-500 animate-pulse" />
                                        Ads data is being fetched, please wait…
                                    </div>
                                )}
                                {/* RIGHT: Download */}
                                {/* RIGHT: Expand + Download */}
                                <div className="flex items-center gap-2">
                                    <button
                                        type="button"
                                        onClick={() => setShowAllMtdProductwiseRows((prev) => !prev)}
                                        title={showAllMtdProductwiseRows ? "Collapse rows" : "Expand all rows"}
                                        aria-label={showAllMtdProductwiseRows ? "Collapse rows" : "Expand all rows"}
                                        className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                    >
                                        {showAllMtdProductwiseRows ? (
                                            <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                                        ) : (
                                            <RiExpandDiagonalFill size={18} className="font-extrabold" />
                                        )}
                                    </button>

                                    <DownloadIconButton
                                        onClick={handleDownloadPlProductwiseMtd}
                                        aria-label="Download P&L Productwise Breakdown MTD"
                                        className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                    />
                                </div>
                            </div>

                            {!shouldShowDummyUi && loading && monthlySkuwiseRows.length === 0 ? (
                                <div className="text-sm text-gray-500">Loading…</div>
                            ) : finalMonthlySkuwiseRowsForTable.length === 0 ? (
                                <div className="text-sm text-red-600">
                                    No P&L productwise rows available for this period.
                                </div>
                            ) : (
                                <div
                                    className={[
                                        "w-full rounded-xl border border-gray-300",
                                        productwiseAnyGroupExpanded ? "overflow-x-auto" : "overflow-hidden",
                                    ].join(" ")}
                                >
                                    <div className={productwiseAnyGroupExpanded ? "min-w-[1200px]" : "w-full"}>
                                        <GroupedCollapsibleTable<MonthlySkuwiseTableRow>
                                            rows={finalMonthlySkuwiseRowsForTable}
                                            onAnyGroupExpandedChange={setProductwiseAnyGroupExpanded}
                                            tableClassName={[
                                                "w-full border-collapse bg-white text-[#414042] text-[14px] lg:text-[12px] min-[1700px]:text-[14px]",
                                                productwiseAnyGroupExpanded
                                                    ? "table-auto min-w-[1200px]"
                                                    : "table-fixed",
                                            ].join(" ")}
                                            getRowKey={(row, idx) =>
                                                row.isTotal
                                                    ? "TOTAL"
                                                    : row.isOthers
                                                        ? "OTHERS"
                                                        : row.sku || String(idx)
                                            }
                                            leftCols={SKUWISE_LEFT_COLS}
                                            groups={SKUWISE_GROUPS}
                                            singleCols={SKUWISE_SINGLE_COLS}
                                            initialCollapsed={productwiseInitialCollapsed}
                                            collapsedState={productwiseCollapsed}
                                            onCollapsedChange={setProductwiseCollapsed}
                                            defaultSort={plSortConfig}
                                            onSortChange={setPlSortConfig}
                                            showSignRowInBody
                                            getSignForCol={getAdsSignForCol}
                                            bodyMaxHeight={
                                                shouldScrollMtdProductwiseTable
                                                    ? mtdProductwiseTableScrollHeight
                                                    : undefined
                                            }
                                            isTotalRow={(row) => {
                                                const name = String(row?.product_name || "").trim().toLowerCase();
                                                const sku = String(row?.sku || "").trim().toUpperCase();

                                                return (
                                                    !!row.isTotal ||
                                                    sku === "GRAND_TOTAL" ||
                                                    sku === "TOTAL" ||
                                                    name === "grand total" ||
                                                    name === "total"
                                                );
                                            }}

                                            layout={[
                                                { type: "group", id: "quantity" },
                                                { type: "single", key: "asp" },
                                                { type: "single", key: "net_sales" },
                                                { type: "single", key: "cogs" },
                                                { type: "group", id: "marketplace_fees" },
                                                { type: "group", id: "tax_and_credits" },
                                                { type: "group", id: "profit" },
                                                { type: "group", id: "ads_spend" },
                                                { type: "single", key: "acos" },
                                                { type: "group", id: "cm2_profit" },

                                            ]}

                                            // initialCollapsed={{ marketplace_fees: false }}
                                            getRowClassName={(row, index) => {
                                                if (row.isTotal) return "bg-[#EFEFEF] font-semibold";
                                                if (row.isOthers) return "";
                                                return index % 2 === 0 ? "bg-white" : "bg-gray-50";
                                            }}
                                            getValue={(row, colKey) => {
                                                if (colKey === "sno") return row.isTotal ? "" : row.sno ?? "";
                                                if (colKey === "sku") {
                                                    if (row.isOthers || row.isTotal) return "-";
                                                    return row.sku || "-";
                                                }
                                                if (colKey === "quantity") {
                                                    return fmtInt(toNumber((row as any).quantity));
                                                }

                                                if (colKey === "return_quantity") {
                                                    return fmtInt(toNumber((row as any).return_quantity));
                                                }

                                                if (colKey === "total_quantity") {
                                                    return fmtInt(
                                                        toNumber(
                                                            (row as any).total_quantity ??
                                                            (toNumber((row as any).quantity) - toNumber((row as any).return_quantity))
                                                        )
                                                    );
                                                }
                                                if (colKey === "product_name") {
                                                    if (row.isTotal) {
                                                        return (
                                                            <span className="inline-block w-full truncate font-semibold">
                                                                Total
                                                            </span>
                                                        );
                                                    }

                                                    if (row.isOthers) {
                                                        return (
                                                            <span
                                                                className="inline-block w-full truncate text-[#60a68e]"
                                                                title="Aggregated remaining products"
                                                            >
                                                                Others
                                                            </span>
                                                        );
                                                    }

                                                    return (
                                                        <button
                                                            type="button"
                                                            onClick={() => openPnlSkuDrawer(row)}
                                                            className="flex w-full items-center justify-between gap-3 text-left text-[#60a68e] "
                                                            title={String(row.product_name || "")}
                                                        >
                                                            <span className="min-w-0 truncate">
                                                                {row.product_name || "-"}
                                                            </span>

                                                            {renderLiveNetSalesDelta(row)}
                                                        </button>
                                                    );
                                                }

                                                if (colKey === "quantity")
                                                    return Math.round(Number(row.quantity || 0)).toLocaleString();

                                                if (colKey === "asp") return formatAdsNumber(row.asp);
                                                if (colKey === "net_sales") return Math.round(Number(row.net_sales || 0)).toLocaleString();

                                                if (colKey === "tax" || colKey === "credits" || colKey === "tax_and_credits") {
                                                    const v = Number((row as any)[colKey] ?? 0);
                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString();
                                                }
                                                if (colKey === "cm1_profit_per") {
                                                    const v = Number(row.cm1_profit_per ?? 0);
                                                    return `${formatAdsNumber(Math.abs(v))}%`;
                                                }

                                                if (colKey === "cm1_profit_per_unit") {
                                                    const v = Number(row.cm1_profit_per_unit ?? 0);
                                                    return formatAdsNumber(Math.abs(v));
                                                }

                                                if (colKey === "cm2_profit_per") {
                                                    const v = Number(row.cm2_profit_per ?? 0);
                                                    return `${formatAdsNumber(v)}%`;
                                                }

                                                // CM2 per unit (no %)
                                                if (colKey === "cm2_profit_per_unit") {
                                                    const v = Number(row.cm2_profit_per_unit ?? 0);
                                                    return formatAdsNumber(v);
                                                }
                                                if (colKey === "ad_type") {
                                                    if (row.isOthers || row.isTotal) return "-";
                                                    return formatAdType((row as any).ad_type);
                                                }
                                                if (
                                                    colKey === "product_spend" ||
                                                    colKey === "display_spend" ||
                                                    colKey === "brand_spend"
                                                ) {
                                                    const v = Number((row as any)[colKey] ?? 0);

                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString("en-GB", {
                                                        minimumFractionDigits: 0,
                                                        maximumFractionDigits: 0,
                                                    });
                                                }

                                                if (colKey === "ads_spend") {
                                                    const v = Number(row.ads_spend ?? 0);

                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString("en-GB", {
                                                        minimumFractionDigits: 0,
                                                        maximumFractionDigits: 0,
                                                    });
                                                }

                                                if (colKey === "ads_spend")
                                                    return Math.round(Math.abs(Number(row.ads_spend || 0))).toLocaleString();
                                                if (colKey === "acos") {
                                                    const v = Number(row.acos ?? 0);
                                                    return `${formatAdsNumber(v)}%`;
                                                }
                                                if (colKey === "cogs")
                                                    return Math.round(Math.abs(Number(row.cogs || 0))).toLocaleString();

                                                if (colKey === "fba_fees")
                                                    return Math.round(Math.abs(Number(row.fba_fees || 0))).toLocaleString();

                                                if (colKey === "selling_fees")
                                                    return Math.round(Math.abs(Number(row.selling_fees || 0))).toLocaleString();

                                                if (colKey === "marketplace_total")
                                                    return Math.round(
                                                        Math.abs(Number(row.fba_fees || 0)) + Math.abs(Number(row.selling_fees || 0))
                                                    ).toLocaleString();
                                                if (colKey === "cm2_profit")
                                                    return Math.round(Number(row.cm2_profit || 0)).toLocaleString();
                                                if (colKey === "profit")
                                                    return Math.round(Number(row.profit || 0)).toLocaleString();
                                                return (row as any)[colKey] ?? "";
                                            }}
                                            summary={{
                                                enabled: finalMonthlySkuwiseRowsForTable.length > 0,

                                                rows: [
                                                    {
                                                        type: "section",
                                                        id: "ads",
                                                        label: "Cost of Advertisement",
                                                        endValue: formatSummaryRounded(costOfAds),
                                                        defaultCollapsed: true,
                                                        children: [
                                                            {
                                                                id: "ads_1",
                                                                label: <>Visibility - Ads <strong className="text-[#ff5c5c]">(-)</strong></>,
                                                                midValue: formatSummaryRounded(sponsoredBrandSpend),
                                                            },
                                                            {
                                                                id: "ads_3",
                                                                label: <>Visibility - Deals, Vouchers and Reviews <strong className="text-[#ff5c5c]">(-)</strong></>,
                                                                midValue: formatSummaryRounded(dealVouchers),
                                                            },
                                                        ],
                                                    },

                                                    {
                                                        type: "section",
                                                        id: "other",
                                                        label: "Other Transactions",
                                                        endValue: formatSummaryRounded(platformFee),
                                                        defaultCollapsed: true,
                                                        children: [
                                                            {
                                                                id: "other_1",
                                                                label: <>Other Platform Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                                                                midValue: formatSummaryValue(otherPlatformFee, "platformfeenew"),
                                                            },
                                                            {
                                                                id: "other_2",
                                                                label: <>Inventory Storage Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                                                                midValue: formatSummaryRounded(inventoryStorageFees),
                                                            },
                                                            {
                                                                id: "other_misc",
                                                                label: <>Misc. Transactions <strong className="text-green-500">(+)</strong></>,
                                                                midValue: formatSummaryValue(
                                                                    plSummaryTotals.misc_transaction,
                                                                    "misc_transaction"
                                                                ),
                                                            },
                                                            {
                                                                id: "other_3",
                                                                label: (
                                                                    <>
                                                                        Reimbursement for lost Inventory{" "}
                                                                        <strong className="text-green-500">(+)</strong>
                                                                    </>
                                                                ),
                                                                midValue: formatSummaryValue(lost_inventory_total, "lost_total"),
                                                            },
                                                        ],
                                                    },

                                                    ...(countryName === "us" || countryName === "global"
                                                        ? [
                                                            {
                                                                type: "fixed" as const,
                                                                id: "ship",
                                                                label: (
                                                                    <>
                                                                        Shipment Charges <strong className="text-[#ff5c5c]">(-)</strong>
                                                                    </>
                                                                ),
                                                                endValue: formatSummaryValue(
                                                                    plSummaryTotals.shipment_charges,
                                                                    "shipment_charges"
                                                                ),
                                                            },
                                                        ]
                                                        : []),

                                                    {
                                                        type: "fixed",
                                                        id: "cm2_profit",
                                                        label: "CM2 Profit/Loss",
                                                        endValue: Math.round(totalRowCm2Profit).toLocaleString(),
                                                    },
                                                    {
                                                        type: "fixed",
                                                        id: "cm2_margins",
                                                        label: "CM2 Margins",
                                                        endValue: `${formatSummaryValue(totalRowCm2Margins, "cm2_margins")}%`,
                                                    },
                                                    {
                                                        type: "fixed",
                                                        id: "tacos",
                                                        label: "TACoS (Total Advertising Cost of Sale)",
                                                        endValue: `${formatSummaryValue(
                                                            tacosFromDisplayedCardsForSummary,
                                                            "acos"
                                                        )}%`,
                                                    },

                                                    // Net Reimbursement below TACoS and still collapsible
                                                    {
                                                        type: "section",
                                                        id: "net_reimbursement",
                                                        label: "Net Reimbursement",
                                                        endValue: formatSummaryValue(
                                                            reimbursementForSummary,
                                                            "net_reimbursement"
                                                        ),
                                                        defaultCollapsed: true,
                                                        children: [
                                                            {
                                                                id: "net_reimbursement_debt_payment",
                                                                label: (
                                                                    <>
                                                                        Debt Payment <strong className="text-[#ff5c5c]">(-)</strong>
                                                                    </>
                                                                ),
                                                                midValue: formatSummaryValue(
                                                                    plSummaryTotals.debt_payment,
                                                                    "debt_payment"
                                                                ),
                                                            },
                                                            {
                                                                id: "net_reimbursement_disbursement",
                                                                label: (
                                                                    <>
                                                                        Disbursement <strong className="text-green-500">(+)</strong>
                                                                    </>
                                                                ),
                                                                midValue: formatSummaryValue(
                                                                    plSummaryTotals.disbursement,
                                                                    "disbursement"
                                                                ),
                                                            },
                                                        ],
                                                    },

                                                    {
                                                        type: "fixed",
                                                        id: "rv_cm2",
                                                        label: "Reimbursement vs CM2 Margins",
                                                        endValue: `${formatSummaryValue(
                                                            reimbursementVsCm2PctForSummary,
                                                            "rembursment_vs_cm2_margins"
                                                        )}%`,
                                                    },
                                                    {
                                                        type: "fixed",
                                                        id: "rv_sales",
                                                        label: "Reimbursement vs Sales",
                                                        endValue: `${formatSummaryValue(
                                                            reimbursementVsSalesPctForSummary,
                                                            "reimbursement_vs_sales"
                                                        )}%`,
                                                    },
                                                ],

                                                valueCols: 2,
                                            }}
                                        />
                                    </div>
                                </div>
                            )}

                        </div>
                        <div id="mtd-pl" className="mt-4 scroll-mt-[80px]">
                            <div
                                className={[
                                    "grid grid-cols-1 gap-4 items-stretch",
                                    isMtdPlExpanded ? "lg:grid-cols-1" : "lg:grid-cols-2",
                                ].join(" ")}
                            >
                                <div className="rounded-xl border bg-white p-5 shadow-sm min-w-0">
                                    <div className="2xl:mb-3 flex items-center justify-between">
                                        <div className="text-sm text-charcoal-500">
                                            <div className="flex flex-wrap items-baseline gap-2 text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold">
                                                <PageBreadcrumb pageTitle="MTD P&L" align="left" textSize="2xl" variant="page" />
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            <span className="relative group shrink-0">
                                                <button
                                                    type="button"
                                                    className="
      rounded-md
      border
      border-gray-300
      bg-white
      text-blue-700
      p-1.5
      transition-all
      duration-200
      ease-out
      hover:-translate-y-[2px]
      hover:shadow-lg
      active:translate-y-0
      active:shadow-md
    "
                                                    onClick={() => setIsMtdPlExpanded((s) => !s)}
                                                    aria-label={isMtdPlExpanded ? "Collapse chart" : "Expand chart"}
                                                >
                                                    {isMtdPlExpanded ? (
                                                        <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                                                    ) : (
                                                        <RiExpandDiagonalFill size={18} className="font-extrabold" />
                                                    )}
                                                </button>

                                                {/* Chart.js-like tooltip */}
                                                <span
                                                    className="
      pointer-events-none
      absolute
      left-1/2
      -translate-x-1/2
      -top-9
      z-50
      whitespace-nowrap
      rounded-md
      border
      border-gray-200
      bg-white
      px-2
      py-1
      text-[11px]
      font-medium
      text-[#414042]
      shadow-sm
      opacity-0
      transition-opacity
      duration-150
      group-hover:opacity-100
    "
                                                >
                                                    {isMtdPlExpanded ? "Collapse" : "Expand"}
                                                    <span
                                                        className="
        absolute
        left-1/2
        top-full
        h-2
        w-2
        -translate-x-1/2
        -translate-y-1/2
        rotate-45
        border-r
        border-b
        border-gray-200
        bg-white
      "
                                                    />
                                                </span>
                                            </span>
                                        </div>
                                    </div>

                                    <div ref={chartRef} className="overflow-x-hidden flex-1 min-h-0">
                                        <div className="w-full max-w-full min-w-0 h-full">
                                            <DashboardBargraphCard
                                                countryName={countryNameForGraph}
                                                formattedMonthYear={formattedMonthYear}
                                                currencySymbol={currencySymbol}
                                                labels={finalBarLabels}
                                                values={finalBarValues}
                                                prevValues={finalPrevBarValues}
                                                expanded={isMtdPlExpanded}
                                                colors={colors}
                                                loading={!shouldShowDummyUi && loading}
                                                allValuesZero={finalAllValuesZero}
                                                previewMode={shouldShowDummyUi}
                                            />
                                        </div>
                                    </div>
                                </div>

                                {!isMtdPlExpanded && (
                                    <div
                                        className={
                                            isUsingDummyData
                                                ? "min-w-0 h-full flex flex-col  pointer-events-none select-none transition-opacity duration-300"
                                                : "min-w-0 h-full flex flex-col transition-opacity duration-300"
                                        }
                                    >
                                        <Cm1ProfitBreakdownPie
                                            data={finalCm1ProfitPieData}
                                            cm2Data={cm2ProfitPieData}
                                            currency={displayCurrency}
                                            noDataFound={shouldShowDummyUi}
                                            height={320}
                                        />
                                    </div>
                                )}
                            </div>
                        </div>
                    </>

                )}

                {/* {amazonIntegrated && graphRegionToUse !== "Global" && ( */}
                {activeTab === "inventory" && (
                    <div id="current-inventory" className="scroll-mt-[80px]">
                        <CurrentInventorySection
                            region={hasRealInventoryRows ? graphRegionToUse : "UK"}
                            invLoading={!hasRealInventoryRows && !shouldShowDummyUi && invLoading}
                            invError={hasRealInventoryRows ? "" : shouldShowDummyUi ? "" : invError}
                            invRows={finalInventoryRows}
                            inventoryAlerts={finalInventoryAlerts}
                            userData={userData}
                            convertToDisplayCurrency={convertToDisplayCurrency}
                            displayCurrency={displayCurrency}
                        />
                        {!invLoading && !invError && (
                            <InventoryAgeGraphSection
                                invRows={invRows}
                                region={graphRegionToUse}
                                selectedCountry="uk"
                            />
                        )}
                    </div>
                )}
            </PreviewLockedSection>

            <SkuRecommendationDrawer
                open={recDrawerOpen}
                onClose={() => setRecDrawerOpen(false)}
                selectedRec={selectedRec}
                objectiveContext={(finalLiveBiPayload as any)?.objective_context || null}
                countryName={countryName}
                sourceCountryName={countryName}
                displayCurrency={displayCurrency}
            />

        </div >

    );
}



