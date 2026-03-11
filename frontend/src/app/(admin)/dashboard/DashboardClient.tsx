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
import { useRouter } from "next/navigation";
import Cm1ProfitBreakdownPie from "@/components/dashboard/Cm1ProfitBreakdownPie";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import GroupedCollapsibleTable, { ColGroup } from "@/components/ui/table/GroupedCollapsibleTable";
import { exportPnLProductwiseBreakdownMtdExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import InfoTip from "@/components/ui/InfoTip";
import * as XLSX from "xlsx-js-style";
import { fetchCurrentInventoryData, InventoryRow } from "@/lib/inventory/fetchCurrentInventoryData";
import Alert from "@/components/ui/alert/Alert";
import { ApiResponse } from "@/components/businessInsight/types";

const TERM_DEFINITIONS: Record<string, string> = {
    asp: "Average Selling Price",
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
    ad_type?: string;
    product_name: string;
    quantity: number;
    asp: number;
    net_sales: number;
    cogs: number;
    fba_fees: number;
    selling_fees: number;
    tax: number;
    credits: number;
    tax_and_credits: number;
    cm1_profit_per: number;
    cm1_profit_per_unit: number;
    cm2_profit_per: number;
    cm2_profit_per_unit: number;
    ads_spend: number;
    cm2_profit: number;
    profit: number;
    isTotal?: boolean;
    platform_fee?: number;
    platform_fee_inventory_storage?: number;
    lost_total?: number;
    other?: number;
};

type MonthlySkuwiseTableRow = MonthlySkuwiseRow & {
    isOthers?: boolean;
    isTotal?: boolean;
};

type FetchLiveBiPayloadArgs = {
    startDay?: number | null;
    endDay?: number | null;
    generateInsights?: boolean;
};

/* ===================== ENV & ENDPOINTS ===================== */
const baseURL =
    process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// const API_URL = `${baseURL}/amazon_api/orders`;
const FIN_MTD_TX_ENDPOINT = `${baseURL}/amazon_api/finances/mtd_transactions`;
const SHOPIFY_DROPDOWN_ENDPOINT = `${baseURL}/shopify/dropdown`;
// const FX_RATES_GET_ENDPOINT = `${baseURL}/currency-rates`;

const LIVE_MTD_BI_ENDPOINT = `${baseURL}/live_mtd_bi`;


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

/* ===================== BI TYPES (for shared cards + graph) ===================== */
type ChartMetric = "net_sales" | "quantity";

type DailyPoint = {
    date: string;
    quantity?: number;
    net_sales?: number;
    gross_sales?: number;
    profit?: number;
    cm2_profit?: number; // ✅ add
};


type DailySeries = {
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
    daily_series?: DailySeries;

    aligned_totals?: BiAlignedTotals;

    categorized_growth?: any;
    insights?: Record<string, any>;
    ai_insights?: Record<string, any>;
    overall_summary?: string[];
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

const toVariant = (t?: string): UiAlert["variant"] => {
    const x = (t || "").toLowerCase();

    if (x.includes("error") || x.includes("danger") || x.includes("critical")) return "error";
    if (x.includes("warn")) return "warning";
    if (x.includes("success") || x.includes("ok")) return "success";
    return "info";
};

const normalizeSku = (v: any) => String(v || "").trim().toUpperCase();

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

    profit: number;
    net_sales: number;
};

const toNumber = (v: any) => {
    if (v === undefined || v === null || v === "") return 0;
    if (typeof v === "number") return Number.isFinite(v) ? v : 0;
    const n = Number(String(v).replace(/,/g, "").trim());
    return Number.isFinite(n) ? n : 0;
};

const INT_KEYS = new Set<string>(["reimbursement_lost_inventory_units"]);

const SIGNED_KEYS = new Set<string>([
    "cm2_profit",
    "cm2_margins",
    "rembursment_vs_cm2_margins",
    "reimbursement_vs_sales",
]);

function computePlSummaryTotalsFromSource(source: any): PlSummaryTotals {
    const platformFees = toNumber(source?.platformfeenew ?? source?.platform_fee_new ?? source?.platform_fee);
    const inventoryStorageFees = toNumber(
        source?.platform_fee_inventory_storage
    );

    const netReimbursement = toNumber(source?.rembursement_fee ?? source?.reimbursement_fee ?? source?.net_reimbursement);
    const reimbursementUnits = toNumber(
        source?.reimbursement_lost_inventory_units ?? source?.reimbursement_units ?? source?.lost_inventory_units
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
        advertising_total: toNumber(source?.advertising_total ?? source?.ads_total ?? source?.ads_spend_total),
        visible_ads: toNumber(source?.visible_ads ?? source?.ads_visibility),
        dealsvouchar_ads: toNumber(source?.dealsvouchar_ads ?? source?.deals_vouchers_ads),

        other_transactions: toNumber(source?.other_transactions ?? source?.platform_fee ?? source?.other_fees_total),
        platform_fee: platformFees,
        inventory_storage_fees: inventoryStorageFees,
        platform_fee_inventory_storage: toNumber(source?.platform_fee_inventory_storage),
        misc_transaction: toNumber(source?.misc_transaction ?? source?.misc_transactions),

        reimbursement_lost_inventory_amount: toNumber(
            source?.reimbursement_lost_inventory_amount ?? source?.lost_inventory_amount
        ),
        reimbursement_lost_inventory_units: reimbursementUnits,
        lost_total: toNumber(source?.lost_total),

        shipment_charges: toNumber(source?.shipment_charges ?? source?.shipping_charges),
        reimbursement_vs_sales: toNumber(source?.reimbursement_vs_sales ?? source?.reimbursement_vs_net_sales),

        cm2_profit: toNumber(source?.cm2_profit),
        cm2_margins: cm2MarginsValue,
        acos: toNumber(source?.acos ?? source?.tacos),

        rembursment_vs_cm2_margins: toNumber(source?.rembursment_vs_cm2_margins ?? source?.reimbursement_vs_cm2_margins),
        net_reimbursement: netReimbursement,

        profit: toNumber(source?.Profit ?? source?.profit ?? source?.cm1_profit),
        net_sales: toNumber(source?.Net_Sales ?? source?.net_sales),
    };
}

function computePlSummaryTotalsFromSkuwise(rows: any[]): PlSummaryTotals {
    // Fallback: use GRAND_TOTAL row from skuwise_items (or sum rows).
    const grand = rows?.find?.((r: any) => r?.isTotal) || rows?.[rows?.length - 1] || {};
    const netSales = toNumber(grand?.net_sales);
    const adsSpend = toNumber(grand?.ads_spend);
    const cm2Profit = toNumber(grand?.cm2_profit);

    const acos = netSales ? (Math.abs(adsSpend) / Math.abs(netSales)) * 100 : 0;
    const cm2Margins = netSales ? (cm2Profit / netSales) * 100 : 0;

    return {
        advertising_total: Math.abs(adsSpend),
        visible_ads: 0,
        dealsvouchar_ads: 0,

        other_transactions: 0,
        platform_fee: 0,
        inventory_storage_fees: 0,
        platform_fee_inventory_storage: 0,
        misc_transaction: 0,
        reimbursement_lost_inventory_amount: 0,
        reimbursement_lost_inventory_units: 0,
        lost_total: 0,

        shipment_charges: 0,
        reimbursement_vs_sales: 0,

        cm2_profit: cm2Profit,
        cm2_margins: cm2Margins,
        acos,

        rembursment_vs_cm2_margins: 0,
        net_reimbursement: 0,

        profit: toNumber(grand?.profit),
        net_sales: netSales,
    };
}

function computePlSummaryTotals(data: any, skuwiseRows: any[]): PlSummaryTotals {
    // Priority:
    // 1) If API returns a dedicated summary object, use it (common candidates).
    // 2) Else if API totals include the needed keys, use totals.
    // 3) Else fallback to GRAND_TOTAL skuwise row.
    const candidates = [
        data?.summary,
        data?.pl_summary,
        data?.mtd_summary,
        data?.totals, // sometimes contains extra finance breakdown fields
        data?.derived_totals,
    ].filter(Boolean);

    for (const c of candidates) {
        const t = computePlSummaryTotalsFromSource(c);
        // If it looks meaningful (net_sales or cm2_profit), accept it.
        if (t.net_sales !== 0 || t.cm2_profit !== 0 || t.advertising_total !== 0) return t;
    }
    return computePlSummaryTotalsFromSkuwise(skuwiseRows || []);
}

const formatSummaryValue = (value: unknown, key: string) => {
    if (value === undefined || value === null || value === "") return "-";

    const raw = toNumber(value);
    if (!Number.isFinite(raw)) return "-";

    // preserve sign for certain metrics; otherwise display as absolute value
    const n = SIGNED_KEYS.has(key) ? raw : Math.abs(raw);

    if (INT_KEYS.has(key)) return String(Math.trunc(n));

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
            countries: ["UK"],
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

    // ✅ IMPORTANT: if another tab/render is running it, don't error, just exit
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

const fmtPct2 = (v: number) => `${(Number(v) || 0).toFixed(2)}%`;

/* ===================== RANGE PICKER (moved above graph) ===================== */
function RangePicker({
    selectedStartDay,
    selectedEndDay,
    onSubmit,
    onClear,
    onCloseReset,
}: {
    selectedStartDay: number | null;
    selectedEndDay: number | null;
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
                {formatRangeLabel(pendingStartDate, pendingEndDate)}
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
    const [isMtdPlExpanded, setIsMtdPlExpanded] = useState(false);

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

    const biCountryName = useMemo(() => {
        if (platform === "global") return "uk";
        return countryName;
    }, [platform, countryName]);

    const biDataCurrency = useMemo(() => currencyForCountry(biCountryName), [biCountryName]);

    const biSourceCurrency: CurrencyCode = useMemo(
        () => currencyForCountry(biCountryName),
        [biCountryName]
    );

    const amazonDataCurrency: CurrencyCode = useMemo(() => {
        if (platform === "amazon-us") return "USD";
        if (platform === "amazon-ca") return "CAD";
        return "GBP"; // amazon-uk OR global default
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
    const [adsLoading, setAdsLoading] = useState(false);
    const [invLoading, setInvLoading] = useState(false);
    const [invError, setInvError] = useState("");
    const [invRows, setInvRows] = useState<InventoryRow[]>([]);
    const [inventoryAlerts, setInventoryAlerts] = useState<InventoryAlertRecord>({});
    const [activeTab, setActiveTab] = useState<TopTab>("live");
    const [summaryLoading, setSummaryLoading] = useState(true);
    const [dismissedAlertIds, setDismissedAlertIds] = useState<Set<string>>(
        () => new Set()
    );
    const [dismissedAlerts, setDismissedAlerts] = React.useState<string[]>([]);

    const dismissAlert = useCallback((id?: string) => {
        if (!id) return;
        setDismissedAlertIds((prev) => {
            const next = new Set(prev);
            next.add(id);
            return next;
        });
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


    const top5InventoryAlerts = useMemo(() => {
        return Object.entries(inventoryAlerts || {})
            .filter(([sku, v]) => {
                return (
                    top5Skus.includes(sku.toUpperCase()) &&
                    v.alert === "High alert" // only this specific alert
                );
            })
            .map(([sku, v]) => ({
                id: sku,
                title: sku,
                message: v.alert || "",
                variant: "error" as const, // High alert = red
            }));
    }, [inventoryAlerts, top5Skus]);

    const fetchMonthlySp = useCallback(async () => {
        try {
            setMonthlySpLoading(true);
            setMonthlySpError(null);

            const token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

            if (!token) throw new Error("No token found. Please sign in.");

            const country =
                platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

            const { monthName, year } = getISTYearMonth();

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
            setMonthlySpLoading(false);
        }
    }, [platform]);



    useEffect(() => {
        if (activeTab === "summary") {
            setSummaryLoading(true);
        }
    }, [activeTab]);


    useEffect(() => {
        fetchMonthlySp();
    }, [fetchMonthlySp]);

    useEffect(() => {
        const stored = localStorage.getItem("dismissedInventoryAlerts");
        if (stored) {
            setDismissedAlerts(JSON.parse(stored));
        }
    }, []);

    const handleDismiss = (sku: string) => {
        setDismissedAlerts((prev) => {
            const updated = [...prev, sku];
            localStorage.setItem(
                "dismissedInventoryAlerts",
                JSON.stringify(updated)
            );
            return updated;
        });
    };


    /* ===================== AMAZON / SHOPIFY STATE ===================== */
    const [loading, setLoading] = useState(false);
    const [unauthorized, setUnauthorized] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [data, setData] = useState<any>(null);

    const { connections: amazonConnections } = useAmazonConnections();

    // Shopify (current month)
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

    // const biUiLoading = biStatus === "loading" || biStatus === "processing";
    const [closedAlerts, setClosedAlerts] = useState<string[]>([]);


    const chartRef = React.useRef<HTMLDivElement | null>(null);
    const prevLabel = useMemo(() => getPrevMonthShortLabel(), []);

    const getDayOfMonthIST = () => {
        const now = new Date();
        const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
        return ist.getDate(); // 1..31
    };

    const [todaySalesRaw, setTodaySalesRaw] = useState<number>(0);


    /* ===================== ✅ SHARED RANGE STATE (PARENT) ===================== */
    const [selectedStartDay, setSelectedStartDay] = useState<number | null>(null);
    const [selectedEndDay, setSelectedEndDay] = useState<number | null>(null);

    const [biLoading, setBiLoading] = useState(false);
    const [biError, setBiError] = useState<string | null>(null);
    const [biDailySeries, setBiDailySeries] = useState<DailySeries | null>(null);
    const [biPeriods, setBiPeriods] = useState<BiApiResponse["periods"] | null>(null);
    // const [liveBiPayload, setLiveBiPayload] = useState<BiApiResponse | null>(null);
    const [liveBiPayload, setLiveBiPayload] = useState<any>(null);
    const [biAlignedTotals, setBiAlignedTotals] = useState<BiAlignedTotals | null>(null);
    const [liveBiReady, setLiveBiReady] = useState(false);
    const retryRef = useRef(0);



    /* ===================== FX RATES ===================== */
    const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
    const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
    const [cadToUsd, setCadToUsd] = useState(CAD_TO_USD_ENV);
    const [fxLoading, setFxLoading] = useState(false);

    const biUiLoading = biStatus === "loading" || biStatus === "processing";

    // Parent controls BI readiness now
    const pageLoading =
        loading ||
        shopifyLoading ||
        fxLoading ||
        adsLoading ||
        monthlySpLoading ||
        invLoading ||
        biUiLoading; // keep only this for BI

    // Optional: if you want to block until BI payload exists when showLiveBI:
    /// || (showLiveBI && !liveBiPayload && biStatus !== "error")

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

            const { monthName, year } = getISTYearMonth();
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

            // console.log("✅ FX (current month)", { month, year, gbpUsd, inrUsd, cadUsd });
        } catch (err) {
            console.error("Failed to fetch FX from DB, keeping env defaults", err);
        } finally {
            setFxLoading(false);
        }
    }, []);


    // useEffect(() => {
    //   console.log("📊 FINAL FX RATES IN USE", {
    //     GBP_TO_USD: gbpToUsd,
    //     INR_TO_USD: inrToUsd,
    //     CAD_TO_USD: cadToUsd,
    //     displayCurrency,
    //   });
    // }, [gbpToUsd, inrToUsd, cadToUsd, displayCurrency]);

    useEffect(() => {
        if (activeTab !== "summary") return;

        // when BI payload is ready, hide loader
        if (liveBiPayload) {
            setSummaryLoading(false);
        }
    }, [activeTab, liveBiPayload]);

    useEffect(() => {
        fetchFxRates();
    }, [fetchFxRates]);

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

    useEffect(() => {
        if (!isCountryMode) return;
        setGraphRegion(forcedRegion);
        setAmazonRegion(forcedRegion);
    }, [isCountryMode, forcedRegion]);

    // ✅ which region is selected in the Sales Target card
    const [targetRegion, setTargetRegion] = useState<RegionKey>(
        isCountryMode ? forcedRegion : "Global"
    );

    useEffect(() => {
        if (isCountryMode) setTargetRegion(forcedRegion);
    }, [isCountryMode, forcedRegion]);

    const didAdsManagerSeedRef = useRef(false);

    // ===================== EFFECTS =====================

    useEffect(() => {
        let cancelled = false;

        const run = async () => {
            try {
                setAdsLoading(true);

                if (platform === "shopify") {
                    if (!cancelled) setAdsSeeded(true);
                    return;
                }

                const jwtToken =
                    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

                if (!jwtToken) {
                    if (!cancelled) {
                        setAdsSeeded(false);
                        setAdsSeedError("No token found. Please sign in.");
                    }
                    return;
                }

                const country =
                    platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

                await ensureSpReportSeedOncePerDay(baseURL, jwtToken, country);

                // ✅ SD supports UK/US only
                if (country === "UK" || country === "US") {
                    await ensureSdReportSeedOncePerDay(baseURL, jwtToken, country);
                }

                if (!cancelled) {
                    setAdsSeedError(null);
                    setAdsSeeded(true);
                }
            } catch (e: any) {
                if (!cancelled) {
                    setAdsSeedError(e?.message || "Ads seed failed");
                    setAdsSeeded(false);
                }
            } finally {
                if (!cancelled) setAdsLoading(false);
            }
        };

        setAdsSeeded(false);
        run();

        return () => {
            cancelled = true;
        };
    }, [platform, baseURL]);



    const didMonthlyAdsSyncRef = useRef(false);


    useEffect(() => {
        if (!adsSeeded) return; // ✅ monthly runs only after seed succeeds
        if (platform === "shopify") return;

        let cancelled = false;

        const run = async () => {
            try {
                const jwtToken =
                    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
                if (!jwtToken) return;

                const country =
                    platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

                const { monthName, year } = getISTYearMonth();
                const month = monthToNumber(monthName.toLowerCase());

                const include = country === "UK" || country === "US" ? ["SP", "SD"] : ["SP"];

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

                if (res.status === 404 && String(json?.error || "").includes("No rows found")) {
                    console.warn(`No monthly ads rows for ${country} ${month}/${year}. Skipping.`);
                    return;
                }

                if (!res.ok) throw new Error(json?.error || "monthly_sp_sd_to_db failed");

                if (cancelled) return;

                await fetchMonthlySp();
            } catch (e) {
                console.error("monthly_sp_sd_to_db error:", e);
            }
        };

        run();

        return () => {
            cancelled = true;
        };
    }, [adsSeeded, platform, baseURL, fetchMonthlySp]);


    useEffect(() => {
        let cancelled = false;

        const run = async () => {
            try {
                setAdsLoading(true);

                if (platform === "shopify") {
                    if (!cancelled) setAdsSeeded(true);
                    return;
                }

                const jwtToken =
                    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

                if (!jwtToken) {
                    if (!cancelled) {
                        setAdsSeeded(false);
                        setAdsSeedError("No token found. Please sign in.");
                    }
                    return;
                }

                const country =
                    platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

                await ensureSpReportSeedOncePerDay(baseURL, jwtToken, country);

                // ✅ SD supports UK/US only
                if (country === "UK" || country === "US") {
                    await ensureSdReportSeedOncePerDay(baseURL, jwtToken, country);
                }

                // ✅ ADD THIS: SB Keyword seed (your new API)
                await ensureSbKeywordReportSeedOncePerDay(baseURL, jwtToken, country);

                if (!cancelled) {
                    setAdsSeedError(null);
                    setAdsSeeded(true);
                }
            } catch (e: any) {
                if (!cancelled) {
                    setAdsSeedError(e?.message || "Ads seed failed");
                    setAdsSeeded(false);
                }
            } finally {
                if (!cancelled) setAdsLoading(false);
            }
        };

        setAdsSeeded(false);
        run();

        return () => {
            cancelled = true;
        };
    }, [platform, baseURL]);



    // region -> backend country
    const inventoryCountry = useMemo(() => {
        const v = (graphRegionToUse || "").toString().trim().toLowerCase(); // or your region prop
        return v.length ? v : "global";
    }, [graphRegionToUse]);

    const invMonthYear = useMemo(() => {
        const { monthName, year } = getISTYearMonth();
        return { month: monthName.toLowerCase(), year: String(year) };
    }, []);

    const fetchInventory = useCallback(async () => {
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
    }, [inventoryCountry, invMonthYear.month, invMonthYear.year]);

    useEffect(() => {
        fetchInventory();
    }, [fetchInventory]);


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

    const userMonthlyTargetGBP = useMemo(() => {
        return toNumberSafe(userData?.target_sales ?? 0);
    }, [userData?.target_sales]);

    const userMonthlyTargetHome = useMemo(() => {
        if (!userMonthlyTargetGBP) return 0;
        return convertToDisplayCurrency(userMonthlyTargetGBP, "GBP");
    }, [userMonthlyTargetGBP, convertToDisplayCurrency]);


    const prevFullMonthNetSalesDisp = useMemo(() => {
        const v = liveBiPayload?.aligned_totals?.total_previous_net_sales_full_month;
        if (v == null) return 0;
        return convertToDisplayCurrency(Number(v) || 0, biSourceCurrency);
    }, [liveBiPayload, convertToDisplayCurrency, biSourceCurrency]);


    const cm1ProfitPieData = useMemo<Cm1PieSlice[]>(() => {
        const apiSlices = liveBiPayload?.cm1_profit_pie?.slices;
        if (apiSlices?.length) {
            const merged = new Map<string, { name: string; value: number; prevValue: number; deltaPct: number }>();

            for (const s of apiSlices) {
                const name = (s.name || "Others").trim();
                const value = Number(s.profit_curr || 0);
                const prevValue = Number(s.profit_prev || 0);

                const existing = merged.get(name);
                if (existing) {
                    existing.value += value;
                    existing.prevValue += prevValue;
                    // keep latest deltaPct (or you could compute weighted delta later if needed)
                    existing.deltaPct = Number(s.delta_pct ?? existing.deltaPct ?? 0);
                } else {
                    merged.set(name, {
                        name,
                        value,
                        prevValue,
                        deltaPct: Number(s.delta_pct ?? 0),
                    });
                }
            }

            const total = Array.from(merged.values()).reduce((sum, r) => sum + r.value, 0) || 1;

            return Array.from(merged.values())
                .map((r) => ({
                    ...r,
                    pct: (r.value / total) * 100,
                }))
                .sort((a, b) => b.value - a.value);
        }

        // ✅ 2) Fallback: your existing frontend logic (keep as backup)
        const cg = liveBiPayload?.categorized_growth;
        const top80 = cg?.top_80_skus ?? [];
        const other = cg?.other_skus ?? [];
        const combined = [...top80, ...other];

        if (!combined.length) return [];

        const sorted = combined
            .map((r: any) => ({
                name: String(r?.product_name ?? "Unknown"),
                profit_curr: Number(r?.profit_curr ?? 0),
                profit_prev: Number(r?.profit_prev ?? 0),
            }))
            .filter((x) => x.profit_curr !== 0 || x.profit_prev !== 0)
            .sort((a, b) => b.profit_curr - a.profit_curr);

        const total = sorted.reduce((s, x) => s + x.profit_curr, 0) || 1;

        // keep your previous “min 5 named + pareto” logic here if you want
        // (but ideally you won't hit this path once backend cm1_profit_pie exists)
        const top = sorted.slice(0, 5);
        const rest = sorted.slice(5);

        const named: Cm1PieSlice[] = top.map((x) => ({
            name: x.name,
            value: x.profit_curr,
            prevValue: x.profit_prev,
            pct: (x.profit_curr / total) * 100,
            deltaPct: x.profit_prev ? ((x.profit_curr - x.profit_prev) / Math.abs(x.profit_prev)) * 100 : 0,
        }));

        if (rest.length) {
            const restCurr = rest.reduce((s, x) => s + x.profit_curr, 0);
            const restPrev = rest.reduce((s, x) => s + x.profit_prev, 0);
            named.push({
                name: "Others",
                value: restCurr,
                prevValue: restPrev,
                pct: (restCurr / total) * 100,
                deltaPct: restPrev ? ((restCurr - restPrev) / Math.abs(restPrev)) * 100 : 0,
            });
        }

        return named.sort((a, b) => b.value - a.value);
    }, [liveBiPayload?.cm1_profit_pie, liveBiPayload?.categorized_growth]);

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

    // ✅ Global FULL month target = Amazon(previous full month from BI) + Shopify(previous month total)
    const globalPrevFullMonthNetSalesDisp = useMemo(() => {
        const amazonFull = prevFullMonthNetSalesDisp; // already in display currency
        const shopifyFull = convertToDisplayCurrency(shopifyPrevDeriv?.netSales ?? 0, "INR");
        return amazonFull + shopifyFull;
    }, [prevFullMonthNetSalesDisp, shopifyPrevDeriv?.netSales, convertToDisplayCurrency]);


    const formatDisplayAmount = useCallback(
        (value: number | null | undefined) => {
            const n = toNumberSafe(value ?? 0);

            switch (displayCurrency) {
                case "USD":
                    return fmtUSD(n);
                case "GBP":
                    return fmtGBP(n);
                case "CAD":
                    return new Intl.NumberFormat("en-CA", {
                        style: "currency",
                        currency: "CAD",
                    }).format(n);
                case "INR":
                    return new Intl.NumberFormat("en-IN", {
                        style: "currency",
                        currency: "INR",
                    }).format(n);
                default:
                    return fmtNum(n);
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

    const biDailySeriesHome = useMemo(() => {
        if (!biDailySeries) return null;

        const convPoint = (p: DailyPoint): DailyPoint => ({
            ...p,
            net_sales: p.net_sales != null ? convertToDisplayCurrency(p.net_sales, biDataCurrency) : p.net_sales,
            gross_sales: p.gross_sales != null ? convertToDisplayCurrency(p.gross_sales, biDataCurrency) : p.gross_sales,
            profit: p.profit != null ? convertToDisplayCurrency(p.profit, biDataCurrency) : p.profit,
            cm2_profit: p.cm2_profit != null ? convertToDisplayCurrency(p.cm2_profit, biDataCurrency) : p.cm2_profit,
        });

        return {
            previous: (biDailySeries.previous || []).map(convPoint),
            current_mtd: (biDailySeries.current_mtd || []).map(convPoint),
        };
    }, [biDailySeries, convertToDisplayCurrency, biDataCurrency]);


    /* ===================== AMAZON FETCH ===================== */
    const fetchAmazon = useCallback(async () => {
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

            // ✅ decide country from platform
            const uiCountry =
                platform === "amazon-us" ? "us" : platform === "amazon-ca" ? "ca" : "uk";

            // ✅ marketplace id (fallback to UK one you provided)
            const marketplaceId =
                (amazonConnections?.find?.((c: any) => (c?.country || "").toLowerCase() === uiCountry)
                    ?.marketplace_id) ||
                (uiCountry === "uk"
                    ? "A1F83G8C2ARO7P"
                    : uiCountry === "us"
                        ? "ATVPDKIKX0DER"
                        : uiCountry === "ca"
                            ? "A2EUQ1WTGCTBG2"
                            : "A1F83G8C2ARO7P");

            const params = new URLSearchParams({
                marketplace_id: marketplaceId,
                store_in_db: "true",
                country: uiCountry,
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
    }, [platform, amazonConnections]);

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

            const { monthName, year } = getISTYearMonth();

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
    }, [shopifyStore]);

    /* ===================== SHOPIFY PREVIOUS MONTH ===================== */
    const fetchShopifyPrev = useCallback(async () => {
        try {
            const user_token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
            if (!user_token) throw new Error("No token found. Please sign in.");

            if (!shopifyStore?.shop_name || !shopifyStore?.access_token) {
                throw new Error("Shopify store not connected.");
            }

            const { year, monthName } = getPrevISTYearMonth();

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
    }, [shopifyStore]);






    /* ===================== ✅ SHARED BI FETCH (FOR CARDS + GRAPH) ===================== */
    const { monthName: currMonthName, year: currYear } = getISTYearMonth();

    const lastBiKeyRef = useRef<string>("");
    const aiRequestedRef = useRef<boolean>(false);




    const fetchBiSeries = useCallback(
        async (startDay?: number | null, endDay?: number | null) => {
            if (!showLiveBI) return;

            const normalized = (biCountryName || "").toLowerCase();

            if (!normalized || normalized === "global") return;


            const rangeActive = startDay != null && endDay != null;

            const key = JSON.stringify({
                country: normalized,
                ranged: "MTD",
                month: currMonthName.toLowerCase(),
                year: currYear,
                startDay: rangeActive ? startDay : null,
                endDay: rangeActive ? endDay : null,
                ai: aiRequestedRef.current, // ✅ IMPORTANT
            });
            setBiError(null);
            setBiLoading(true);
            try {
                const token =
                    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

                const params = new URLSearchParams({
                    countryName: normalized,
                    ranged: "MTD",
                    month: currMonthName.toLowerCase(),
                    year: String(currYear),
                    generate_ai_insights: aiRequestedRef.current ? "true" : "false",
                });

                if (rangeActive) {
                    params.set("start_day", String(startDay));
                    params.set("end_day", String(endDay));
                }

                const res = await fetch(`${LIVE_MTD_BI_ENDPOINT}?${params.toString()}`, {
                    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
                });

                // ✅ HANDLE 202 FIRST
                if (res.status === 202) {
                    setBiStatus("processing");
                    // IMPORTANT
                    return;
                }

                if (!res.ok) {
                    throw new Error(`BI failed: ${res.status}`);
                }

                const json: BiApiResponse = await res.json();

                lastBiKeyRef.current = key;
                // aiRequestedRef.current = true;

                setLiveBiPayload(json);
                setBiPeriods(json?.periods || null);
                setBiDailySeries(json?.daily_series || null);
                setBiAlignedTotals(json?.aligned_totals || null);

                setBiStatus("idle");      // 🔥 reset first
                setTimeout(() => {
                    setBiStatus("ready");   // 🔥 force render
                }, 0);

                // setBiAlignedTotals(json?.aligned_totals || null);
                const alignedFromNested = (json as any)?.aligned_totals;

                const alignedFromTopLevel: BiAlignedTotals = {
                    total_current_advertising: (json as any)?.total_current_advertising,
                    total_previous_advertising: (json as any)?.total_previous_advertising,

                    total_current_net_sales: (json as any)?.total_current_net_sales,
                    total_previous_net_sales: (json as any)?.total_previous_net_sales,
                    total_previous_net_sales_full_month: (json as any)?.total_previous_net_sales_full_month,

                    total_current_platform_fees: (json as any)?.total_current_platform_fees,
                    total_previous_platform_fees: (json as any)?.total_previous_platform_fees,

                    total_current_profit: (json as any)?.total_current_profit,
                    total_previous_profit: (json as any)?.total_previous_profit,

                    total_current_rembursement_fee: (json as any)?.total_current_rembursement_fee,
                    total_previous_rembursement_fee: (json as any)?.total_previous_rembursement_fee,
                };

                setBiAlignedTotals(alignedFromNested ?? alignedFromTopLevel ?? null);

            } catch (e: any) {
                setBiPeriods(null);
                setBiDailySeries(null);
                setBiAlignedTotals(null);
                setBiError(e?.message || "Failed to load BI series");
            } finally {
                setBiLoading(false);
            }
        },
        [showLiveBI, biCountryName, currMonthName, currYear]

    );

    useEffect(() => {
        if (biStatus !== "processing") {
            retryRef.current = 0;
            return;
        }

        if (retryRef.current >= 10) {
            setBiStatus("error");
            return;
        }

        retryRef.current += 1;

        const timer = setTimeout(() => {
            fetchBiSeries(selectedStartDay, selectedEndDay);
        }, 3000);

        return () => clearTimeout(timer);
    }, [biStatus, fetchBiSeries, selectedStartDay, selectedEndDay]);

    // const fetchLiveBiPayload = useCallback(
    //     async ({
    //         startDay = selectedStartDay,
    //         endDay = selectedEndDay,
    //         generateInsights = false,
    //     }: FetchLiveBiPayloadArgs = {}) => {
    //         // toggle AI insights flag
    //         aiRequestedRef.current = !!generateInsights;

    //         // fetch using provided range (or current state range by default)
    //         await fetchBiSeries(startDay, endDay);
    //     },
    //     [fetchBiSeries, selectedStartDay, selectedEndDay]
    // );

    const fetchLiveBiPayload = useCallback(
        async ({
            startDay = selectedStartDay,
            endDay = selectedEndDay,
            generateInsights = false,
        }: FetchLiveBiPayloadArgs = {}) => {
            setSummaryLoading(true);

            // toggle AI insights flag
            aiRequestedRef.current = !!generateInsights;

            // fetch using provided range (or current state range by default)
            await fetchBiSeries(startDay, endDay);
        },
        [fetchBiSeries, selectedStartDay, selectedEndDay]
    );

    useEffect(() => {
        if (!showLiveBI) return;
        if (biStatus === "processing") return; // 🔥 ADD THIS
        fetchBiSeries(selectedStartDay, selectedEndDay);
    }, [showLiveBI, biStatus, fetchBiSeries, selectedStartDay, selectedEndDay]);

    /* ===================== REFRESH ALL ===================== */
    const refreshAll = useCallback(async () => {
        await fetchAmazon();
        if (shopifyStore?.shop_name && shopifyStore?.access_token) {
            await Promise.all([fetchShopify(), fetchShopifyPrev()]);
        }
    }, [
        fetchAmazon,
        fetchShopify,
        fetchShopifyPrev,
        shopifyStore,
    ]);

    const didRefreshRef = useRef(false);

    useEffect(() => {
        if (didRefreshRef.current) return;
        didRefreshRef.current = true;

        refreshAll();
    }, []);



    /* ===================== AMAZON DERIVED DATA ===================== */
    const totals = data?.totals || null;
    const derived = data?.derived_totals || null;

    const uk = useMemo(() => {
        const netSalesGBP = derived?.net_sales != null ? toNumberSafe(derived.net_sales) : null;
        const aspGBP = derived?.asp != null ? toNumberSafe(derived.asp) : null;
        const cm2ProfitGBP =
            derived?.cm2_profit != null ? toNumberSafe(derived.cm2_profit) : null;

        const cogsGBP = totals?.cogs != null ? toNumberSafe(totals.cogs) : 0;
        const fbaFeesGBP = totals?.fba_fees != null ? toNumberSafe(totals.fba_fees) : 0;
        const sellingFeesGBP = totals?.selling_fees != null ? toNumberSafe(totals.selling_fees) : 0;

        // ✅ your backend already computed amazon_fees = selling + fba, but we can compute too
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
            totals?.gross_sales != null ? toNumberSafe(totals.gross_sales) : null; // ✅ current gross

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

    const safeDeltaPct = (current: number, previous: number) => {
        const c = Number(current) || 0;
        const p = Number(previous) || 0;

        if (!p) return null;

        return ((c - p) / Math.abs(p)) * 100;
    };




    const prevTotals = data?.previous_period?.totals || null;

    const prev = useMemo(() => {
        return {
            quantity: toNumberSafe(prevTotals?.quantity ?? 0),
            netSales: toNumberSafe(prevTotals?.net_sales ?? 0),
            grossSales: toNumberSafe(prevTotals?.gross_sales ?? 0), // ✅ add
            asp: toNumberSafe(prevTotals?.asp ?? 0),
            profit: toNumberSafe(prevTotals?.profit ?? 0),
            cm2Profit: toNumberSafe(prevTotals?.cm2_profit ?? 0),
            profitPct: toNumberSafe(prevTotals?.profit_percentage ?? 0),
        };
    }, [prevTotals]);

    // ✅ AMAZON Ads (display currency)
    const amazonCurrAdsDisp = useMemo(() => {
        const ads = toNumberSafe(derived?.advertising_fees ?? 0);
        return convertToDisplayCurrency(ads, amazonDataCurrency);
    }, [derived?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

    const amazonPrevAdsDisp = useMemo(() => {
        const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
        return convertToDisplayCurrency(ads, amazonDataCurrency);
    }, [data?.previous_period?.totals?.advertising_fees, convertToDisplayCurrency, amazonDataCurrency]);

    const amazonAdsDeltaPct = useMemo(
        () => safeDeltaPct(amazonCurrAdsDisp, amazonPrevAdsDisp),
        [amazonCurrAdsDisp, amazonPrevAdsDisp]
    );

    // ✅ AMAZON ROAS% = (Ads / Net Sales) * 100
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

    const amazonRoasDeltaPct = useMemo(
        () => safeDeltaPct(amazonCurrRoasPct, amazonPrevRoasPct),
        [amazonCurrRoasPct, amazonPrevRoasPct]
    );



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

            // Profit % must be percentage-points (pp)
            profitMarginPctPts:
                curr.profitPct != null && prev.profitPct != null
                    ? Number(curr.profitPct) - Number(prev.profitPct)
                    : null,
        };
    }, [curr, prev]);

    const deltaPctPoints = (currentPct: number, previousPct: number) => {
        const c = Number(currentPct) || 0;
        const p = Number(previousPct) || 0;
        return c - p; // percentage points
    };

    const deltaPctAbs = (currentPct: number, previousPct: number) => {
        const c = Number(currentPct) || 0;
        const p = Number(previousPct) || 0;
        return c - p;
    };


    /* ===================== ✅ RANGE KPIs FOR CARDS (FROM SAME BI DATA AS GRAPH) ===================== */
    useEffect(() => {
        const pts = biDailySeriesHome?.current_mtd || [];
        if (!pts.length) return;

        const todayDay = getDayOfMonthIST();

        // try exact today
        const exact = pts.find((p) => Number(p.date?.slice(8, 10)) === todayDay);
        if (exact?.net_sales != null) {
            setTodaySalesRaw(Number(exact.net_sales) || 0);
            return;
        }

        // fallback: latest available day in series
        const latest = [...pts].sort((a, b) => a.date.localeCompare(b.date)).at(-1);
        setTodaySalesRaw(Number(latest?.net_sales) || 0);
    }, [biDailySeriesHome]);


    const biCardKpis = useMemo(() => {
        const currAll = biDailySeriesHome?.current_mtd || [];
        const prevAll = biDailySeriesHome?.previous || [];

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
                profitPct: safeDeltaPctFromPct(currProfitPct, prevProfitPct),

            },
        };
    }, [biDailySeriesHome, selectedStartDay, selectedEndDay]);

    // const rangeActive = selectedStartDay != null && selectedEndDay != null;
    const rangeActive = selectedStartDay != null && selectedEndDay != null;

    // use BI only when a range is active
    const useBiCm2 = showLiveBI && rangeActive;

    // BI values are usable only when rangeActive + finished loading + response present
    const cm2Ready = useBiCm2 && !biLoading && !!biAlignedTotals;


    // ✅ only when range is active + BI is ready
    const biCardsReady = rangeActive && !biLoading && !!biAlignedTotals;

    const biAlignedTotalsHome = useMemo(() => {
        if (!biCardsReady || !biAlignedTotals) return null;

        // convert BI source currency -> your display currency
        const conv = (v?: number) =>
            convertToDisplayCurrency(Number(v || 0), biSourceCurrency);

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
    }, [biCardsReady, biAlignedTotals, convertToDisplayCurrency, biSourceCurrency]);

    const globalRangeCurrency = currencyForCountry(biCountryName);
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

        // fallback: estimate full last-month total from last-month MTD
        const { todayDay, daysInPrevMonth } = getISTDayInfo();
        if (!todayDay || !daysInPrevMonth) return 0;

        // prevAmazonMtdSalesUSD is already last month MTD (USD)
        return (prevAmazonMtdSalesUSD * daysInPrevMonth) / todayDay;
    }, [data?.previous_month_total_net_sales?.total, gbpToUsd, prevAmazonMtdSalesUSD]);


    const amazonUK_Gross_USD = useMemo(() => {
        const grossGBP = toNumberSafe(totals?.gross_sales); // ✅ current gross
        return grossGBP * gbpToUsd;
    }, [totals?.gross_sales, gbpToUsd]);



    const combinedGrossUSD = useMemo(() => {
        const shopifyUSD = toNumberSafe(shopifyDeriv?.netSales) * inrToUsd;
        return amazonUK_Gross_USD + shopifyUSD;
    }, [amazonUK_Gross_USD, shopifyDeriv?.netSales, inrToUsd]);

    const prevAmazonGrossUSD = useMemo(() => {
        return toNumberSafe(prev.grossSales) * gbpToUsd; // prev gross in GBP → USD
    }, [prev.grossSales, gbpToUsd]);

    const prevGlobalGrossUSD = useMemo(() => {
        const prevShopifyUSD = toNumberSafe(shopifyPrevDeriv?.netSales) * inrToUsd; // shopify gross not available; using net like you do elsewhere
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
        const { todayDay, daysInPrevMonth } = getISTDayInfo();
        return daysInPrevMonth > 0 ? (lastMonthTotalUSD * todayDay) / daysInPrevMonth : 0;
    };




    // ---------- NET SALES (DISPLAY CURRENCY) ----------

    // Amazon current & prev net sales (already correct source)
    const amazonCurrNetDisp = useMemo(
        () => convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP"),
        [uk.netSalesGBP, convertToDisplayCurrency]
    );

    const amazonPrevNetDisp = useMemo(
        () => convertToDisplayCurrency(prev.netSales ?? 0, "GBP"),
        [prev.netSales, convertToDisplayCurrency]
    );

    // Global = Amazon + Shopify (NET SALES ONLY)
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
        const userMonthlyTargetGBP = toNumberSafe(userData?.target_sales ?? 0);
        const userMonthlyTargetHome =
            userMonthlyTargetGBP > 0
                ? convertToDisplayCurrency(userMonthlyTargetGBP, "GBP")
                : 0;

        const globalPrevFullMonthSales =
            globalPrevFullMonthNetSalesDisp > 0
                ? globalPrevFullMonthNetSalesDisp
                : globalPrevNetDisp;

        const globalTarget =
            userMonthlyTargetHome > 0 ? userMonthlyTargetHome : globalPrevFullMonthSales;

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
            userMonthlyTargetHome > 0 ? userMonthlyTargetHome : ukPrevFullMonthSales;

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
        // existing deps you already had
        globalCurrNetDisp,
        globalPrevNetDisp,
        amazonCurrNetDisp,
        amazonPrevNetDisp,
        prevFullMonthNetSalesDisp,
        globalPrevFullMonthNetSalesDisp,

        // ✅ add these because we use them inside now
        userData?.target_sales,
        convertToDisplayCurrency,

        // these are referenced by US/CA regions
        chooseLastMonthTotal,
        prorateToDate,
    ]);

    const anyLoading = loading || shopifyLoading;

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

    /* ===================== P&L ITEMS (DISPLAY CURRENCY OUTPUT) ===================== */
    const plItems = useMemo(() => {
        const ukPl = () => {
            const sales = convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");
            const fees = convertToDisplayCurrency(uk.amazonFeesGBP ?? 0, "GBP");
            const cogs = convertToDisplayCurrency(uk.cogsGBP ?? 0, "GBP");
            const adv = convertToDisplayCurrency(uk.advertisingGBP ?? 0, "GBP");

            const others = convertToDisplayCurrency(uk.platformFeeGBP ?? 0, "GBP"); // you renamed Platform Fees → Others
            const cm1 = convertToDisplayCurrency(uk.profitGBP ?? 0, "GBP");         // you renamed Profit → CM1 Profit
            const cm2 = convertToDisplayCurrency(uk.cm2ProfitGBP ?? 0, "GBP");

            // ✅ NEW: Tax & Credits from totals.tax_and_credits
            const taxCredits = convertToDisplayCurrency(
                toNumberSafe(totals?.tax_and_credits ?? 0),
                "GBP"
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

        if (graphRegionToUse === "UK") return ukPl();

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
        onlyAmazon,
        onlyShopify,
        combinedUSD,
        totals?.tax_and_credits,
        uk.netSalesGBP,
        uk.amazonFeesGBP,
        uk.cogsGBP,
        uk.advertisingGBP,
        uk.platformFeeGBP,
        uk.profitGBP,
        shopifyDeriv?.netSales,
        convertToDisplayCurrency,
        formatDisplayAmount,
    ]);

    const chartItems = useMemo(() => plItems || [], [plItems]);

    const labels = useMemo(() => chartItems.map((i) => i.label), [chartItems]);
    const values = useMemo(() => chartItems.map((i) => Number(i.raw ?? 0)), [chartItems]);

    // If you still need "no data" detection:
    const allValuesZero = useMemo(
        () => values.length === 0 || values.every((v) => Math.abs(v) < 1e-9),
        [values]
    );

    const monthlySkuwiseRows = useMemo<MonthlySkuwiseRow[]>(() => {
        const items = (data as any)?.skuwise_items ?? [];
        if (!Array.isArray(items)) return [];

        const body = items.filter((r: any) => r?.sku && r.sku !== "GRAND_TOTAL");
        const total = items.find((r: any) => r?.sku === "GRAND_TOTAL");

        const mapRow = (r: any, idx?: number, isTotal = false): MonthlySkuwiseRow => ({
            sno: isTotal ? undefined : (idx ?? 0) + 1,
            sku: String(r.sku ?? ""),
            product_name: String(r.product_name ?? ""),
            ad_type: String(
                r.ad_type ?? r.adType ?? r.ad_types ?? r.adTypes ?? ""
            ),
            quantity: Number(r.quantity ?? 0),
            asp: Number(r.asp ?? 0),
            net_sales: Number(r.net_sales ?? 0),
            cogs: Number(r.cogs ?? 0),

            fba_fees: Number(r.fba_fees ?? 0),
            selling_fees: Number(r.selling_fees ?? 0),

            ads_spend: Number(r.ads_spend ?? 0),
            cm2_profit: Number(r.cm2_profit ?? 0),

            tax: Number(r.tax ?? 0),
            credits: Number(r.credits ?? 0),
            tax_and_credits: Number(r.tax_and_credits ?? 0),

            cm1_profit_per: Number(r.cm1_profit_per ?? 0),
            cm1_profit_per_unit: Number(r.cm1_profit_per_unit ?? 0),

            cm2_profit_per: Number(r.cm2_profit_per ?? 0),
            cm2_profit_per_unit: Number(r.cm2_profit_per_unit ?? 0),
            profit: Number(r.profit ?? 0),
            isTotal,
            platform_fee: Number(r.platform_fee ?? 0),
            platform_fee_inventory_storage: Number(r.platform_fee_inventory_storage ?? 0),
            lost_total: Number(r.lost_total ?? 0),
            other: Number(r.other ?? 0),

        });

        const mapped = body.map((r: any, idx: number) => mapRow(r, idx, false));

        if (total) mapped.push(mapRow(total, undefined, true));

        return mapped;
    }, [data]);

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
        if (!monthlySkuwiseRows || monthlySkuwiseRows.length === 0) return [];

        const totalRow =
            monthlySkuwiseRows.find((r) => r.isTotal) ??
            monthlySkuwiseRows.find((r) => r.sku === "GRAND_TOTAL") ??
            null;

        const bodyRows = monthlySkuwiseRows.filter((r) => !r.isTotal && r.sku !== "GRAND_TOTAL");

        // If there are 9 or fewer body rows, show them as-is (plus total if present).
        if (bodyRows.length <= 9) {
            const out = [...bodyRows];
            if (totalRow) out.push(totalRow);
            // re-number S.No
            return out.map((r, idx) => (r.isTotal ? r : { ...r, sno: idx + 1 }));
        }

        const sorted = [...bodyRows].sort((a, b) => Math.abs(b.net_sales) - Math.abs(a.net_sales));
        const top9 = sorted.slice(0, 9).map((r, idx) => ({ ...r, sno: idx + 1 }));

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

        const othersRow: MonthlySkuwiseTableRow = {
            sno: 10,
            sku: "OTHERS",
            product_name: "Others",
            quantity: othersQty,
            asp: othersAsp,
            net_sales: othersNetSales,
            cogs: sum("cogs"),
            fba_fees: sum("fba_fees"),
            selling_fees: sum("selling_fees"),
            // Not shown in the current layout; keep them as 0 to avoid misleading sums.
            tax: 0,
            credits: 0,
            tax_and_credits: 0,
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: sum("ads_spend"),
            cm2_profit: sum("cm2_profit"),
            profit: sum("profit"),
            isOthers: true,
        };

        const out: MonthlySkuwiseRow[] = [...top9, othersRow];
        if (totalRow) out.push(totalRow);
        return out;
    }, [monthlySkuwiseRows]);


    const plSummaryTotals = useMemo<PlSummaryTotals>(() => {
        // ✅ Summary rows for "P&L Productwise Breakdown MTD"
        // Data source: mtd_transactions API response (data), with a safe fallback to GRAND_TOTAL in skuwise_items.
        return computePlSummaryTotals(data, monthlySkuwiseRows);
    }, [data, monthlySkuwiseRows]);


    const SKUWISE_LEFT_COLS = [
        { key: "sno", label: "S.No", align: "center" as const },
        { key: "product_name", label: "Product Name", align: "left" as const },
    ];

    const SKUWISE_GROUPS = [
        {
            id: "marketplace_fees",
            label: "Marketplace Fees",
            info: <InfoTip text={TERM_DEFINITIONS.marketplace_fees} />,
            // label: (
            //     <>
            //         Marketplace Fees <InfoTip text={TERM_DEFINITIONS.marketplace_fees} />
            //     </>
            // ),

            collapsedCols: [
                {
                    key: "marketplace_total",
                    label: "Total",
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

            collapsedCols: [
                {
                    key: "quantity",
                    label: "Total",
                    align: "center" as const,
                    width: 20,
                },
            ],

            expandedCols: [
                { key: "sku", label: "SKU", align: "center" as const },
                {
                    key: "quantity",
                    label: "Total",
                    align: "center" as const,
                },
            ],
        },

        {
            id: "profit",
            // label: (
            //     <>
            //         CM1 Profit <InfoTip text={TERM_DEFINITIONS.cm1_profit} />
            //     </>
            // ),
            label: "CM1 Profit",
            info: <InfoTip text={TERM_DEFINITIONS.cm1_profit} />,

            collapsedCols: [
                {
                    key: "profit",
                    label: "Total",
                    align: "center" as const,
                },
            ],

            expandedCols: [
                { key: "cm1_profit_per_unit", label: "CM1 Profit Per Unit", align: "center" as const },
                { key: "cm1_profit_per", label: "CM1 Profit %", align: "center" as const },
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
                },
            ],

            expandedCols: [
                {
                    key: "tax",
                    // label: (
                    // <>
                    //     Net Taxes <InfoTip text={TERM_DEFINITIONS.net_taxes} />
                    // </>
                    // ), 
                    label: "Net Taxes",
                    info: <InfoTip text={TERM_DEFINITIONS.net_taxes} />,
                    align: "center" as const
                },
                {
                    key: "credits",
                    //  label: (
                    //     <>
                    //         Net Credits <InfoTip text={TERM_DEFINITIONS.net_credits} />
                    //     </>
                    // ), 
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
            id: "cm2_profit",
            // label: (
            //     <>
            //         CM2 Profit <InfoTip text={TERM_DEFINITIONS.cm2_profit} />
            //     </>
            // ),
            label: "CM2 Profit",
            info: <InfoTip text={TERM_DEFINITIONS.cm2_profit} />,

            collapsedCols: [
                {
                    key: "cm2_profit",
                    label: "Total",
                    align: "center" as const,
                },
            ],

            expandedCols: [
                { key: "cm2_profit_per_unit", label: "CM2 Profit Per Unit", align: "center" as const },
                { key: "cm2_profit_per", label: "CM2 Profit %", align: "center" as const },
                {
                    key: "cm2_profit",
                    label: "Total",
                    align: "center" as const,
                },
            ],
        },
    ];

    const SKUWISE_SINGLE_COLS = [
        { key: "quantity", label: "Net Units Sold", align: "center" as const },
        {
            key: "asp",
            //  label: (
            //     <>
            //         ASP <InfoTip text={TERM_DEFINITIONS.asp} />
            //     </>
            // ), 
            label: "ASP",
            info: <InfoTip text={TERM_DEFINITIONS.asp} />,
            align: "center" as const
        },
        {
            key: "net_sales",
            //  label: (
            //     <>
            //         Net Sales <InfoTip text={TERM_DEFINITIONS.net_sales} />
            //     </>
            // ), 

            label: "Net Sales",
            info: <InfoTip text={TERM_DEFINITIONS.net_sales} />,
            align: "center" as const
        },
        { key: "cogs", label: "COGS", align: "center" as const },
        { key: "profit", label: "CM1 Profit", align: "center" as const },
        { key: "ads_spend", label: "Ads Spend", align: "center" as const },
        { key: "cm2_profit", label: "CM2 Profit", align: "center" as const },
        { key: "cm1_profit_per", label: "CM1 Profit Per Unit", align: "center" as const },
        { key: "cm1_profit_per_unit", label: "CM1 Profit %", align: "center" as const },
        { key: "cm2_profit_per", label: "CM2 Profit Per Unit", align: "center" as const },
        { key: "cm2_profit_per_unit", label: "CM2 Profit %", align: "center" as const }

    ];


    const monthlyAdsSpentTotal = useMemo<number>(() => {
        const items = (data as any)?.skuwise_items ?? [];
        const grand = items.find((r: any) => r?.sku === "GRAND_TOTAL");

        return Number(
            grand?.ad_spend ??
            grand?.advertising_spend ??
            grand?.advertising_fees ??
            grand?.advertising_cost ??
            (data as any)?.derived_totals?.advertising_fees ??
            (data as any)?.totals?.advertising_cost ??
            0
        );
    }, [data]);



    const prevValues = useMemo(() => {
        const getPrev = (label: string) => {
            // map label -> previous raw value (same currency basis as current raw)
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

        return labels.map(getPrev);
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
    const captureChartPng = useCallback(async () => {
        const container = chartRef.current;
        if (!container) return null;

        const canvas = container.querySelector("canvas") as HTMLCanvasElement | null;
        if (canvas) {
            try {
                const tmpCanvas = document.createElement("canvas");
                tmpCanvas.width = canvas.width;
                tmpCanvas.height = canvas.height;
                const ctx = tmpCanvas.getContext("2d");
                if (!ctx) return null;

                ctx.fillStyle = "#ffffff";
                ctx.fillRect(0, 0, tmpCanvas.width, tmpCanvas.height);
                ctx.drawImage(canvas, 0, 0);

                return tmpCanvas.toDataURL("image/png");
            } catch {
                return null;
            }
        }
        return null;
    }, []);

    const shortMonForGraph = new Date(`${currMonthName} 1, ${currYear}`).toLocaleString("en-US", {
        month: "short",
        timeZone: "Asia/Kolkata",
    });
    const formattedMonthYear = `${shortMonForGraph}'${String(currYear).slice(-2)}`;

    const countryNameForGraph =
        graphRegionToUse === "Global" ? "global" : graphRegionToUse.toLowerCase();

    const handleDownload = useCallback(async () => {
        try {
            const pngDataUrl = await captureChartPng();

            const workbook = new ExcelJS.Workbook();
            const sheet = workbook.addWorksheet("Amazon P&L");

            sheet.addRow([brandName || "Brand"]);
            sheet.addRow([`Amazon P&L - ${formattedMonthYear}`]);
            sheet.addRow([`Country: ${countryNameForGraph.toUpperCase()}`]);
            sheet.addRow([`Currency: ${currencySymbol}`]);
            sheet.addRow([""]);

            sheet.addRow(["Metric", "", `Amount (${currencySymbol})`]);

            const signs: Record<string, string> = {
                "Net Sales": "(+)",
                "Marketplace Fees": "(-)",
                COGS: "(-)",
                Advertisements: "(-)",
                "Tax & Credits": "(+/-)",
                "Other Charges": "(-)",
                Others: "(-)",
                "CM1 Profit": "",
                "CM2 Profit": "",
            };

            values.forEach((v, idx) => {
                const label = labels[idx];
                const sign = signs[label] || "";
                const num = Number(v || 0);
                sheet.addRow([label, sign, Number(num.toFixed(2))]);
            });

            const totalValue = values.reduce((acc, v) => acc + (Number(v) || 0), 0);
            sheet.addRow(["Total", "", Number(totalValue.toFixed(2))]);

            if (pngDataUrl) {
                const base64 = pngDataUrl.replace(/^data:image\/png;base64,/, "");
                const imageId = workbook.addImage({ base64, extension: "png" });

                sheet.addImage(
                    imageId,
                    { tl: { col: 0, row: 9 } as any, br: { col: 8, row: 28 } as any, editAs: "oneCell" } as any
                );
            }

            const buffer = await workbook.xlsx.writeBuffer();
            const blob = new Blob([buffer], {
                type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            });
            saveAs(blob, `Amazon-PnL-${formattedMonthYear}.xlsx`);
        } catch (err) {
            console.error("Error generating Excel with chart", err);
        }
    }, [
        brandName,
        formattedMonthYear,
        countryNameForGraph,
        currencySymbol,
        captureChartPng,
        labels,
        values,
    ]);

    const todaySalesFromBI = useMemo(() => {
        const points = biDailySeries?.current_mtd || [];
        if (!points.length) return 0;

        // if range active, use sliced series (so "today" = last day in range)
        const pts = rangeActive
            ? sliceByDayRange(points, selectedStartDay, selectedEndDay)
            : points;

        if (!pts.length) return 0;

        // pick last point by date (safe even if API order changes)
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

    // ✅ GLOBAL ROAS% (normalize to USD to avoid GBP+INR mixing)
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


    const tacosPctForSummary = useMemo(() => {
        if (globalUseBi) {
            if (!globalCm2Ready) return 0;
            const ads = biAlignedTotals?.total_current_advertising ?? 0;
            const sales = biAlignedTotals?.total_current_net_sales ?? 0;
            return sales > 0 ? (ads / sales) * 100 : 0;
        }

        return globalCurrRoasPct;
    }, [globalUseBi, globalCm2Ready, biAlignedTotals, globalCurrRoasPct]);

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


    const globalPrevRoasPct = useMemo(() => {
        const ads = toNumberSafe(data?.previous_period?.totals?.advertising_fees ?? 0);
        const amazonSales = toNumberSafe(data?.previous_period?.totals?.net_sales ?? 0);
        const shopifySales = toNumberSafe(shopifyPrevDeriv?.netSales ?? 0);

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
        data?.previous_period?.totals?.advertising_fees,
        data?.previous_period?.totals?.net_sales,
        shopifyPrevDeriv?.netSales,
        onlyAmazon,
        amazonDataCurrency,
        gbpToUsd,
        cadToUsd,
        inrToUsd,
    ]);

    const globalRoasDeltaPct = useMemo(
        () => safeDeltaPct(globalCurrRoasPct, globalPrevRoasPct),
        [globalCurrRoasPct, globalPrevRoasPct]
    );

    const globalCurrAspDisp = useMemo(() => {
        return globalCurrUnits > 0 ? globalCurrNetSalesDisp / globalCurrUnits : 0;
    }, [globalCurrUnits, globalCurrNetSalesDisp]);

    const globalPrevAspDisp = useMemo(() => {
        return globalPrevUnits > 0 ? globalPrevNetSalesDisp / globalPrevUnits : 0;
    }, [globalPrevUnits, globalPrevNetSalesDisp]);


    const globalCurrCm2Disp = useMemo(() => {
        return convertToDisplayCurrency(uk.cm2ProfitGBP ?? 0, "GBP");
    }, [uk.cm2ProfitGBP, convertToDisplayCurrency]);

    const globalPrevCm2Disp = useMemo(() => {
        return convertToDisplayCurrency(prev.cm2Profit ?? 0, "GBP");
    }, [prev.cm2Profit, convertToDisplayCurrency]);


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
        const prevAmazonGrossUSD = toNumberSafe(prev.grossSales) * gbpToUsd; // prev gross comes in GBP
        const prevShopifyUSD = toNumberSafe(shopifyPrevDeriv?.netSales) * inrToUsd;
        return convertToDisplayCurrency(prevAmazonGrossUSD + prevShopifyUSD, "USD");
    }, [prev.grossSales, gbpToUsd, shopifyPrevDeriv?.netSales, inrToUsd, convertToDisplayCurrency]);



    /* ===================== RENDER FLAGS ===================== */
    const hasAnyGraphData = amazonIntegrated || shopifyIntegrated;
    const hasGlobalCard = !noIntegrations;
    const hasAmazonCard = amazonIntegrated;
    const hasShopifyCard = !shopifyNotConnected;

    const leftColumnHeightClass = !hasShopifyCard ? "lg:min-h-[520px]" : "";

    const prevShort = getShort(biPeriods?.previous?.label);
    const currShort = getShort(biPeriods?.current_mtd?.label);

    const rangeCurrency = currencyForCountry(countryName);


    const identityConvert = useCallback((v: number, _from?: any) => v, []);

    const reimbursementHome = useMemo(() => {
        // current month reimbursement lives in derived_totals
        const currRaw = toNumberSafe(derived?.current_net_reimbursement ?? 0);

        // previous month reimbursement lives in previous_period.totals (as per your snippet)
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


    const targetData = regions[targetRegion] || regions.Global;

    const stats_mtdHome = identityConvert(targetData.mtdUSD ?? 0);
    const stats_lastMtdHome = identityConvert(targetData.lastMonthToDateUSD ?? 0);
    const stats_lastMonthTotalHome = identityConvert(targetData.lastMonthTotalUSD ?? 0);
    const stats_targetHome = identityConvert(targetData.targetUSD ?? 0);

    const grandTotalRow = data?.skuwise_items?.find(
        (item: any) =>
            item.product_name === "Grand Total" ||
            item.sku === "GRAND_TOTAL"
    );

    console.log("Grand Total Row:", grandTotalRow);

    const ads_spend = grandTotalRow?.ads_spend ?? 0;
    const sponsoredProductsSpend = grandTotalRow?.product_spend ?? 0;
    const sponsoredBrandSpend = grandTotalRow?.brand_spend ?? 0;


    const inventoryStorageFees = grandTotalRow?.platform_fee_inventory_storage ?? 0;
    const lost_inventory_total = grandTotalRow?.lost_total ?? 0;
    const otherPlatformFee = grandTotalRow?.platformfeenew ?? 0;
    const platformFee = grandTotalRow?.platform_fee ?? 0;
    const dealVouchers = grandTotalRow?.dealsvouchar_ads ?? 0;


    const costOfAds = Math.abs(
        toNumber(sponsoredBrandSpend - dealVouchers)
    );

    const adsSpendTotal = Math.abs(
        toNumber(ads_spend + costOfAds)
    );


    const cm2Profit = ((grandTotalRow?.profit) - adsSpendTotal - (Math.abs(grandTotalRow?.platform_fee)))



    const reimbursementForSummary = useMemo(() => {
        return toNumber(reimbursementHome?.current);
    }, [reimbursementHome?.current]);

    // CM2 Margin (%) = (CM2 Profit / Net Sales) * 100
    const cm2MarginPctForSummary = useMemo(() => {
        const cm2 = cm2Profit;
        const netSales = toNumber(plSummaryTotals.net_sales);
        return netSales ? (cm2 / netSales) * 100 : 0;
    }, [plSummaryTotals.cm2_profit, plSummaryTotals.net_sales]);


    const netReimbursementPctForSummary = useMemo(() => {
        const mtdSales = toNumber(stats_mtdHome);
        return mtdSales ? (reimbursementForSummary / mtdSales) * 100 : 0;
    }, [reimbursementForSummary, stats_mtdHome]);

    // Reimbursement vs CM2 Margin (%) = (Reimbursement / CM2 Profit/Loss) * 100
    const reimbursementVsCm2PctForSummary = useMemo(() => {
        const cm2 = cm2Profit;
        return cm2 ? (reimbursementForSummary / cm2) * 100 : 0;
    }, [reimbursementForSummary, plSummaryTotals.cm2_profit]);

    // Reimbursement vs Sales (%) = (Reimbursement / Net Sales) * 100
    // (Your text omitted *100; keeping % for consistent display with existing UI.)
    const reimbursementVsSalesPctForSummary = useMemo(() => {
        const netSales = toNumber(plSummaryTotals.net_sales) || toNumber(stats_mtdHome);
        return netSales ? (reimbursementForSummary / netSales) * 100 : 0;
    }, [reimbursementForSummary, plSummaryTotals.net_sales, stats_mtdHome]);


    const { todayDay: statsTodayDay } = getISTDayInfo();

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

    const getDaysInMonthIST = () => {
        const now = new Date();
        const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
        return new Date(ist.getFullYear(), ist.getMonth() + 1, 0).getDate(); // 28..31
    };

    const todayIST = getDayOfMonthIST();        // D
    const daysInMonthIST = getDaysInMonthIST(); // N

    const proratedTargetToDate = (daysInMonthIST > 0)
        ? (todayIST / daysInMonthIST) * stats_targetHome  // x
        : 0;

    const stats_targetTrendPct =
        stats_targetHome > 0
            ? ((stats_mtdHome - proratedTargetToDate) / stats_targetHome) * 100
            : 0;

    const ADS_SIGN_PLUS = new Set(["net_sales", "credits", "tax_and_credits"]);
    const ADS_SIGN_MINUS = new Set([
        "ads_spend",
        "cogs",
        "fba_fees",
        "selling_fees",
        "marketplace_total",
        "tax"
    ]);

    const getAdsSignForCol = useCallback((colKey: string) => {
        if (ADS_SIGN_PLUS.has(colKey)) return { text: "(+)", className: "text-green-700" };
        if (ADS_SIGN_MINUS.has(colKey)) return { text: "(-)", className: "text-[#ff5c5c]" };
        return null;
    }, []);

    const handleDownloadPlProductwiseMtd = useCallback(() => {
        try {
            const rows = (monthlySkuwiseRows || []).filter((r) => {
                const sku = String(r.sku || "").toUpperCase();
                const pn = String((r as any).product_name || "").toLowerCase();
                const isOthers = (r as any).isOthers === true;
                return !isOthers && !(sku === "OTHERS" && pn === "others");
            });

            if (!rows.length) return;

            const periodLabel = formattedMonthYear;
            const titleCountry = countryName === "global" ? "Global" : countryName.toUpperCase();
            const companyName =
                (userData as any)?.companyName ||
                (userData as any)?.company_name ||
                (userData as any)?.company ||
                "";

            const dataRows = rows.map((r) => {
                const marketplaceTotal =
                    Math.abs(Number(r.fba_fees || 0)) + Math.abs(Number(r.selling_fees || 0));

                return {
                    "S.No": r.isTotal ? "" : (r.sno ?? ""),
                    "Product Name": r.isTotal ? "Total" : (r.product_name ?? ""),
                    SKU: r.isTotal ? "" : (r.sku ?? ""),
                    "Net Units Sold": Number(r.quantity || 0),
                    ASP: Number(r.asp || 0),
                    "Net Sales": Number(r.net_sales || 0),
                    COGS: Number(r.cogs || 0),
                    "FBA Fees": Number(r.fba_fees || 0),
                    "Selling Fees": Number(r.selling_fees || 0),
                    "Marketplace Fees Total": marketplaceTotal,
                    "Net Taxes": Number(r.tax || 0),
                    "Net Credits": Number(r.credits || 0),
                    "Tax & Credits": Number(r.tax_and_credits || 0),
                    "CM1 Profit %": Number(r.cm1_profit_per || 0),
                    "CM1 Profit Per Unit": Number(r.cm1_profit_per_unit || 0),
                    "CM1 Profit": Number(r.profit || 0),
                    "Ads Spend": Number(r.ads_spend || 0),
                    "CM2 Profit": Number(r.cm2_profit || 0),
                    "CM2 Profit %": Number(r.cm2_profit_per || 0),
                    "CM2 Profit Per Unit": Number(r.cm2_profit_per_unit || 0),
                };
            });

            // ✅ IMPORTANT: pass percents as numbers (NOT "12.3%") so export can format them properly
            const summaryRows: { label: string; value: any; indent?: number; bold?: boolean }[] = [
                ...(countryName === "us" || countryName === "global"
                    ? [
                        {
                            label: "Shipment Charges (-)",
                            value: Number((plSummaryTotals as any)?.shipment_charges ?? 0),
                            bold: true,
                        },
                    ]
                    : []),

                // ---- Cost of Advertisement (parent + children)
                { label: "Cost of Advertisement", value: "", bold: true },
                { label: "Visibility - Ads (-)", value: "", indent: 1 },
                { label: "Visibility - Deals, Vouchers and Reviews (-)", value: "", indent: 1 },

                // ---- Other Transactions (parent + children)
                { label: "Other Transactions", value: "", bold: true },
                { label: "Other Platform Fees (-)", value: "", indent: 1 },
                { label: "Inventory Storage Fees (-)", value: Number((plSummaryTotals as any)?.platform_fee_inventory_storage ?? 0), indent: 1 },
                { label: "Misc. Transactions (+)", value: "", indent: 1 },
                { label: "Reimbursement for lost Inventory (+)", value: "", indent: 1 },

                // ---- Fixed rows (same as your table fixedRows)
                { label: "CM2 Profit/Loss", value: Number((plSummaryTotals as any)?.cm2_profit ?? 0), bold: true },
                { label: "CM2 Margins", value: Number(cm2MarginPctForSummary ?? 0), bold: true }, // percent
                { label: "TACoS (Total Advertising Cost of Sale)", value: Number(tacosPctForSummary ?? 0), bold: true }, // percent
                { label: "Net Reimbursement", value: Number(reimbursementForSummary ?? 0), bold: true },
                { label: "Reimbursement vs CM2 Margins", value: Number(reimbursementVsCm2PctForSummary ?? 0), bold: true }, // percent
                { label: "Reimbursement vs Sales", value: Number(reimbursementVsSalesPctForSummary ?? 0), bold: true }, // percent
            ];

            exportPnLProductwiseBreakdownMtdExcel({
                filename: `Amazon-PnL-Productwise-MTD-${periodLabel}.xlsx`,
                titleLine: `Amazon ${titleCountry} - P&L Productwise Breakdown MTD - ${periodLabel}`,
                countryName,
                titleCountry,
                platformLabel: "Amazon",
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
        monthlySkuwiseRows,
        formattedMonthYear,
        countryName,
        plSummaryTotals,
        costOfAdsForSummary,
        cm2MarginPctForSummary,
        tacosPctForSummary,
        reimbursementForSummary,
        reimbursementVsCm2PctForSummary,
        reimbursementVsSalesPctForSummary,
        userData,
        brandName,
        profileHomeCurrency,
    ]);

    const skuwiseItems = useMemo(() => {
        const items = (data as any)?.skuwise_items;
        return Array.isArray(items) ? items : [];
    }, [data]);

    const grandTotalSkuRow = useMemo(() => {
        return (
            skuwiseItems.find((r: any) => String(r?.sku || "").toUpperCase() === "GRAND_TOTAL") ||
            skuwiseItems.find((r: any) => String(r?.product_name || "").toLowerCase() === "grand total") ||
            null
        );
    }, [skuwiseItems]);


    const mtdExtraTotals = useMemo(() => {
        const g = grandTotalSkuRow || {};
        return {
            lost_total: toNumber(g.lost_total),
            platform_fee: toNumber(g.platform_fee),
            platform_fee_inventory_storage: toNumber(g.platform_fee_inventory_storage),
        };
    }, [grandTotalSkuRow]);


    const adsIdx = useMemo(
        () => labels.findIndex((l) => l === "Advertisements"),
        [labels]
    );

    const valuesWithAds = useMemo(() => {
        if (adsIdx === -1) return values;
        const copy = [...values];
        copy[adsIdx] = Number(adsSpendTotal ?? 0);
        return copy;
    }, [adsIdx, values, adsSpendTotal]);

    // ✅ SAFE: only apply prev replacement if you actually have a number
    const prevValuesWithAds = useMemo(() => {
        if (adsIdx === -1) return prevValues;

        // Put your prev ads var here ONLY if it exists, else keep prevValues unchanged
        const prevAds = undefined as unknown as number; // <-- replace if you have one (or leave as undefined)

        if (!Number.isFinite(Number(prevAds))) return prevValues;

        const copy = [...prevValues];
        copy[adsIdx] = Number(prevAds);
        return copy;
    }, [adsIdx, prevValues]);


    // indexes for the 3 bars we want to override
    const idxAds = useMemo(() => labels.findIndex((l) => l === "Advertisements"), [labels]);
    const idxOthers = useMemo(() => labels.findIndex((l) => l === "Others"), [labels]);
    const idxCm2 = useMemo(() => labels.findIndex((l) => l === "CM2 Profit"), [labels]);

    const valuesPatched = useMemo(() => {
        const copy = [...values];

        // Ads
        if (idxAds !== -1) copy[idxAds] = Number(adsSpendTotal ?? 0);

        // Others (platform fee) — choose ABS so it renders as a positive bar like your UI
        if (idxOthers !== -1) copy[idxOthers] = Math.abs(Number(platformFee ?? 0));

        // CM2
        if (idxCm2 !== -1) copy[idxCm2] = Number(cm2Profit ?? 0);

        return copy;
    }, [values, idxAds, idxOthers, idxCm2, adsSpendTotal, platformFee, cm2Profit]);

    const targetKpisFromBi = useMemo(() => {
        if (!rangeActive || !liveBiPayload) return null;

        // TODO: Replace these keys with your real /live-bi fields:
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

    const targets_todayHome = stats_todayHome;
    const targets_mtdHome = targetKpisFromBi ? targetKpisFromBi.mtdHome : stats_mtdHome;
    const targets_lastMonthTotalHome = targetKpisFromBi ? targetKpisFromBi.lastMonthTotalHome : stats_lastMonthTotalHome;
    const targets_lastMonthToDateHome = targetKpisFromBi ? targetKpisFromBi.lastMonthToDateHome : stats_lastMtdHome;

    const targets_reimbursement = targetKpisFromBi ? targetKpisFromBi.reimbursement : reimbursementHome;

    // Calculate the percentage of the month completed (for range display)
    const rangeCompletedPct = useMemo(() => {
        if (!selectedStartDay || !selectedEndDay) return 0;
        const daysInMonth = getDaysInMonthIST();
        const completedDays = selectedEndDay - selectedStartDay + 1;
        return (completedDays / daysInMonth) * 100;
    }, [selectedStartDay, selectedEndDay]);

    type TopTab =
        | "live"
        | "summary"
        // | "mtd_pl"
        | "productwise"
        | "inventory";

    const TOP_TABS: { id: TopTab; label: string }[] = [
        { id: "live", label: "Live Sales" },
        { id: "summary", label: "AI Insights and Recommendations" },
        // { id: "mtd_pl", label: "MTD P&L Breakdown" },
        { id: "productwise", label: "P&L Productwise Breakdown" },
        { id: "inventory", label: "Current Inventory" },
    ];

    const HASH_TO_TAB: Record<string, TopTab> = {
        "live-sales": "live",
        "targets-action-items": "summary",
        "mtd-pl": "productwise",
        "pnl-mtd": "productwise",
        "current-inventory": "inventory",
    };

    const scrollToHashSection = useCallback((hash?: string) => {
        if (typeof window === "undefined") return;

        const rawHash = hash ?? window.location.hash;
        if (!rawHash) return;

        const id = rawHash.replace("#", "");
        if (!id) return;

        // wait a bit so the tab content mounts first
        setTimeout(() => {
            const el = document.getElementById(id);
            if (!el) return;

            el.scrollIntoView({
                behavior: "smooth",
                block: "start",
            });
        }, 120);
    }, []);

    const handleHashNavigation = useCallback(
        (hash?: string) => {
            if (typeof window === "undefined") return;

            const rawHash = hash ?? window.location.hash;
            if (!rawHash) return;

            const id = rawHash.replace("#", "");
            const nextTab = HASH_TO_TAB[id];

            if (nextTab && nextTab !== activeTab) {
                setActiveTab(nextTab);
                return; // scrolling will happen in the next effect after tab changes
            }

            scrollToHashSection(rawHash);
        },
        [activeTab, scrollToHashSection, setActiveTab]
    );

    useEffect(() => {
        if (typeof window === "undefined") return;

        const applyHash = () => {
            const hash = window.location.hash.replace("#", "");
            if (!hash) return;

            const targetTab = HASH_TO_TAB[hash];

            if (targetTab) {
                setPendingHash(hash);
                setActiveTab(targetTab);
                return;
            }

            const el = document.getElementById(hash);
            if (el) {
                el.scrollIntoView({ behavior: "smooth", block: "start" });
            }
        };

        applyHash();
        window.addEventListener("hashchange", applyHash);

        return () => window.removeEventListener("hashchange", applyHash);
    }, []);

    useEffect(() => {
        if (!pendingHash) return;

        const el = document.getElementById(pendingHash);
        if (!el) return;

        const timer = setTimeout(() => {
            el.scrollIntoView({
                behavior: "smooth",
                block: "start",
            });
            setPendingHash("");
        }, 120);

        return () => clearTimeout(timer);
    }, [activeTab, pendingHash]);

    return (
        <div className="relative w-full">
            <HashScroll offset={80} />
            {(loading || shopifyLoading || biLoading) && !data && !shopify && !liveBiPayload && (
                <Loader fullscreen backgroundClass="bg-white/80" />
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

                            {countryName !== "global" && (
                                <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                                    {countryName.toUpperCase()}
                                </span>
                            )}

                            <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                                - {formattedMonthYear}
                            </span>
                        </div>
                    </div>

                    {/* RIGHT SIDE BUTTON */}
                    <button
                        onClick={refreshAll}
                        disabled={loading || shopifyLoading || biLoading}
                        className={`shrink-0 rounded-md border shadow-sm
        px-2 py-1 text-[10px]
        sm:px-3 sm:py-1.5 sm:text-xs
        2xl:text-sm
        ${loading || shopifyLoading || biLoading
                                ? "cursor-not-allowed border-gray-200 bg-gray-100 text-gray-400"
                                : "border-gray-300 bg-white hover:bg-gray-50"
                            }`}
                    >
                        {loading || shopifyLoading || biLoading ? "Refreshing…" : "Refresh"}
                    </button>

                </div>
            </div>

            <div className="sticky max-[480px]:top-[44px] max-[640px]:top-[44px] sm:top-[48px] md:top-[48px] 2xl:top-[56px] z-30 bg-[#F7F7F7] border-b border-gray-200 
    max-[480px]:py-1 max-[640px]:pb-2 sm:py-2">
                <SegmentedToggle<TopTab>
                    value={activeTab}
                    options={TOP_TABS.map((t) => ({ value: t.id, label: t.label }))}
                    onChange={setActiveTab}
                    className="mt-2"
                    compact
                    textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
                />
            </div>

            {/* Top 5 alerts */}
            {/* <div className="my-2 md:my-4 space-y-3">
                {top5Skus
                    .map((sku) => ({
                        sku,
                        productName: skuToProductName[sku] || sku,
                        alert: inventoryAlerts?.[sku]?.alert || "",
                    }))
                    .filter(
                        (x) =>
                            x.alert.trim().toLowerCase() === "high alert" &&
                            !dismissedAlerts.includes(x.sku) // ✅ don't show dismissed
                    )
                    .map(({ sku, productName }) => (
                        <Alert
                            key={sku}
                            variant="error"
                            title={`Inventory Alert - ${productName}`}
                            message="This product is in your Top 5 and requires attention."
                            showLink={false}
                            closable
                            onClose={() => handleDismiss(sku)} // ✅ persist dismissal
                        />
                    ))}
            </div> */}

            {activeTab === "live" && (
                <div
                    id="live-sales"
                    className="grid grid-cols-12 gap-4 mt-2 md:mt-4 scroll-mt-[80px] items-stretch auto-rows-fr"
                >
                    {/* LEFT COLUMN */}
                    <div
                        className={`col-span-12 lg:col-span-8 order-2 lg:order-1 flex flex-col gap-4 h-full min-h-full ${leftColumnHeightClass ?? ""}`}
                    >


                        {/* GLOBAL CARD */}
                        {!isCountryMode && hasGlobalCard && (
                            <div className="flex">
                                <div className="w-full rounded-xl border bg-white p-4 lg:p-3 2xl:p-5 shadow-sm">
                                    <div className="mb-4 flex items-start justify-between gap-3">
                                        <div className="flex items-baseline gap-2">
                                            <PageBreadcrumb pageTitle="Global" variant="page" align="left" />
                                        </div>

                                        {showLiveBI && platform === "global" && (
                                            <RangePicker
                                                selectedStartDay={selectedStartDay}
                                                selectedEndDay={selectedEndDay}
                                                onSubmit={(s, e) => {
                                                    setSelectedStartDay(s);
                                                    setSelectedEndDay(e);
                                                    fetchLiveBiPayload({ startDay: s, endDay: e, generateInsights: false });
                                                }}
                                                onClear={() => {
                                                    setSelectedStartDay(null);
                                                    setSelectedEndDay(null);
                                                    fetchLiveBiPayload({ startDay: null, endDay: null, generateInsights: false });
                                                }}

                                                onCloseReset={() => {
                                                    setSelectedStartDay(null);
                                                    setSelectedEndDay(null);

                                                    // ✅ Add this too (same as clear)
                                                    fetchLiveBiPayload({
                                                        startDay: null,
                                                        endDay: null,
                                                        generateInsights: false,
                                                    });
                                                }}
                                            />

                                        )}
                                    </div>

                                    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-3 auto-rows-fr">

                                        <AmazonStatCard
                                            label="Units"
                                            current={globalUseBi ? biCardKpis.curr.units : globalCurrUnits}
                                            previous={globalUseBi ? biCardKpis.prev.units : globalPrevUnits}
                                            deltaPct={globalUseBi ? biCardKpis.deltas.units : globalDeltas.units}
                                            loading={loading || shopifyLoading || biLoading}
                                            formatter={fmtInt}
                                            bottomLabel={prevLabel}
                                            // className="border-[#FDD36F] bg-[#FDD36F4D]"
                                            className="border-[#FDD36F] border-t-4 border-t-[#75BBDA]"
                                        />

                                        <AmazonStatCard
                                            label="Gross Sales"
                                            current={globalUseBi ? biCardKpis.curr.grossSales : globalCurrGrossDisp}
                                            previous={globalUseBi ? biCardKpis.prev.grossSales : globalPrevGrossDisp}

                                            deltaPct={globalUseBi ? biCardKpis.deltas.grossSales : safeDeltaPct(combinedGrossUSD, prevGlobalGrossUSD)}
                                            loading={loading || shopifyLoading || biLoading}
                                            formatter={moneyPerUnitFormatter}
                                            previousFormatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}

                                            // className="border-[#ED9F50] bg-[#ED9F504D]"
                                            className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                                        />


                                        <AmazonStatCard
                                            label="Net Sales"
                                            current={globalUseBi ? biCardKpis.curr.netSales : globalCurrNetSalesDisp}
                                            previous={globalUseBi ? biCardKpis.prev.netSales : globalPrevNetSalesDisp}

                                            deltaPct={globalUseBi ? biCardKpis.deltas.netSales : safeDeltaPct(globalCurrNetSalesDisp, globalPrevNetSalesDisp)}
                                            loading={loading || shopifyLoading || biLoading}
                                            formatter={moneyPerUnitFormatter}
                                            previousFormatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#75BBDA] bg-[#75BBDA4D]"
                                            className="border-[#75BBDA] border-t-4 border-t-[#75BBDA4D]"
                                        />

                                        <AmazonStatCard
                                            label="ASP"
                                            current={globalUseBi ? biCardKpis.curr.asp : globalCurrAspDisp}
                                            previous={globalUseBi ? biCardKpis.prev.asp : globalPrevAspDisp}
                                            deltaPct={
                                                globalUseBi
                                                    ? biCardKpis.deltas.asp
                                                    : safeDeltaPct(globalCurrAspDisp, globalPrevAspDisp)
                                            }
                                            loading={loading || shopifyLoading || biLoading}
                                            formatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#B75A5A] bg-[#B75A5A4D]"
                                            className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                        />

                                        <AmazonStatCard
                                            label="Cost of Ads"
                                            current={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? convertToDisplayCurrency(
                                                            biAlignedTotals?.total_current_advertising ?? 0,
                                                            biSourceCurrency
                                                        )
                                                        : 0)
                                                    : globalCurrAdsDisp
                                            }
                                            previous={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? convertToDisplayCurrency(
                                                            biAlignedTotals?.total_previous_advertising ?? 0,
                                                            biSourceCurrency
                                                        )
                                                        : 0)
                                                    : globalPrevAdsDisp
                                            }
                                            deltaPct={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? safeDeltaPct(
                                                            // delta should be computed on converted values (home currency) to avoid FX noise
                                                            convertToDisplayCurrency(
                                                                biAlignedTotals?.total_current_advertising ?? 0,
                                                                biSourceCurrency
                                                            ),
                                                            convertToDisplayCurrency(
                                                                biAlignedTotals?.total_previous_advertising ?? 0,
                                                                biSourceCurrency
                                                            )
                                                        )
                                                        : null)
                                                    : globalAdsDeltaPct
                                            }
                                            loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                                            formatter={formatDisplayAmount}
                                            previousFormatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#C49466] bg-[#C494664D]"
                                            className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                        />

                                        <AmazonStatCard
                                            label="TACoS"
                                            current={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? (() => {
                                                            const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                                            const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                                            return sales > 0 ? (ads / sales) * 100 : 0;
                                                        })()
                                                        : 0)
                                                    : globalCurrRoasPct
                                            }
                                            previous={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? (() => {
                                                            const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                                            const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                                            return sales > 0 ? (ads / sales) * 100 : 0;
                                                        })()
                                                        : 0)
                                                    : globalPrevRoasPct
                                            }
                                            deltaPct={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? deltaPctAbs(
                                                            (() => {
                                                                const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                                                const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                                                return sales > 0 ? (ads / sales) * 100 : 0;
                                                            })(),
                                                            (() => {
                                                                const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                                                const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                                                return sales > 0 ? (ads / sales) * 100 : 0;
                                                            })()
                                                        )
                                                        : null)
                                                    : deltaPctAbs(globalCurrRoasPct, globalPrevRoasPct)
                                            }
                                            inverseDelta
                                            loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                                            formatter={fmtPct2}
                                            bottomLabel={prevLabel}
                                            // className="border-[#3A8EA4] bg-[#3A8EA44D]"
                                            className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                        />


                                        <AmazonStatCard
                                            label="CM2 Profit"
                                            current={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? convertToDisplayCurrency(biAlignedTotals?.total_current_profit_cm2 ?? 0, biSourceCurrency)
                                                        : 0)
                                                    : globalCurrCm2Disp
                                            }
                                            previous={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? convertToDisplayCurrency(biAlignedTotals?.previous_cm2_profit ?? 0, biSourceCurrency)
                                                        : 0)
                                                    : globalPrevCm2Disp
                                            }

                                            deltaPct={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? safeDeltaPct(
                                                            biAlignedTotals?.total_current_profit_cm2 ?? 0,
                                                            biAlignedTotals?.previous_cm2_profit ?? 0
                                                        )
                                                        : null)
                                                    : safeDeltaPct(globalCurrCm2Disp, globalPrevCm2Disp)
                                            }
                                            loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                                            formatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#B8C78C] bg-[#B8C78C4D]"
                                            className="border-[#B8C78C] border-t-4 border-t-[#B8C78C]"
                                        />


                                        <AmazonStatCard
                                            label="CM2 Profit %"
                                            current={
                                                globalUseBi
                                                    ? (globalCm2Ready ? (biAlignedTotals?.total_current_profit_percentage ?? 0) : 0)
                                                    : curr.profitPct
                                            }
                                            previous={
                                                globalUseBi
                                                    ? (globalCm2Ready ? (biAlignedTotals?.total_previous_profit_percentage ?? 0) : 0)
                                                    : prev.profitPct
                                            }
                                            deltaPct={
                                                globalUseBi
                                                    ? (globalCm2Ready
                                                        ? deltaPctPoints(
                                                            biAlignedTotals?.total_current_profit_percentage ?? 0,
                                                            biAlignedTotals?.total_previous_profit_percentage ?? 0
                                                        )
                                                        : null)
                                                    : deltaPctPoints(curr.profitPct ?? 0, prev.profitPct ?? 0)
                                            }


                                            loading={loading || shopifyLoading || (globalUseBi ? biLoading : false)}
                                            formatter={fmtPct}
                                            bottomLabel={prevLabel}
                                            // className="border-[#7B9A6D] bg-[#7B9A6D4D]"
                                            className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                                        />


                                    </div>
                                </div>
                            </div>
                        )}

                        {/* AMAZON SECTION */}
                        {hasAmazonCard && (
                            <div className="flex flex-col lg:flex-1 gap-4 2xl:gap-4">
                                {/* Amazon KPI Box */}
                                <div className="w-full rounded-xl border bg-white p-3 2xl:p-5 shadow-sm">
                                    <div className="mb-3 lg:mb-2 2xl:mb-4 flex flex-row gap-3 items-start md:items-start md:justify-between">
                                        <div className="flex flex-col flex-1 min-w-0">
                                        </div>
                                        <div className="flex items-center gap-2">
                                            {showLiveBI && isCountryMode && (
                                                <RangePicker
                                                    selectedStartDay={selectedStartDay}
                                                    selectedEndDay={selectedEndDay}
                                                    onSubmit={(s, e) => {
                                                        setSelectedStartDay(s);
                                                        setSelectedEndDay(e);
                                                    }}
                                                    onClear={() => {
                                                        setSelectedStartDay(null);
                                                        setSelectedEndDay(null);
                                                    }}
                                                    onCloseReset={() => {
                                                        setSelectedStartDay(null);
                                                        setSelectedEndDay(null);
                                                    }}
                                                />
                                            )}

                                        </div>
                                    </div>

                                    <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-2 lg:gap-2 2xl:gap-3 auto-rows-fr">

                                        <AmazonStatCard
                                            label="Units"
                                            current={useBiForAmazonCards ? biCardKpis.curr.units : (totals?.quantity ?? 0)}
                                            previous={useBiForAmazonCards ? biCardKpis.prev.units : prev.quantity}
                                            deltaPct={useBiForAmazonCards ? biCardKpis.deltas.units : deltas.quantityPct}
                                            loading={loading || biLoading}
                                            formatter={fmtInt}
                                            bottomLabel={prevLabel}
                                            // className="border-[#FDD36F] bg-[#FDD36F4D]"
                                            className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"

                                        />

                                        <AmazonStatCard
                                            label="Gross Sales"
                                            current={
                                                showLiveBI && rangeActive
                                                    ? biCardKpis.curr.grossSales                 // ✅ no conversion
                                                    : convertToDisplayCurrency(uk.grossSalesGBP ?? 0, amazonDataCurrency)
                                            }
                                            previous={
                                                showLiveBI && rangeActive
                                                    ? biCardKpis.prev.grossSales                 // ✅ no conversion
                                                    : convertToDisplayCurrency(prev.grossSales ?? 0, amazonDataCurrency)
                                            }
                                            deltaPct={useBiForAmazonCards ? biCardKpis.deltas.grossSales : safeDeltaPct(uk.grossSalesGBP ?? 0, prev.grossSales ?? 0)}
                                            loading={loading || biLoading}
                                            formatter={moneyPerUnitFormatter}
                                            previousFormatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#ED9F50] bg-[#ED9F504D]"
                                            className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                                        />

                                        <AmazonStatCard
                                            label="Net Sales"
                                            current={
                                                showLiveBI && rangeActive
                                                    ? biCardKpis.curr.netSales                   // ✅ no conversion
                                                    : convertToDisplayCurrency(uk.netSalesGBP ?? 0, amazonDataCurrency)
                                            }
                                            previous={
                                                showLiveBI && rangeActive
                                                    ? biCardKpis.prev.netSales                   // ✅ no conversion
                                                    : convertToDisplayCurrency(prev.netSales, amazonDataCurrency)
                                            }
                                            deltaPct={useBiForAmazonCards ? biCardKpis.deltas.netSales : deltas.netSalesPct}
                                            loading={loading || biLoading}
                                            formatter={moneyPerUnitFormatter}
                                            previousFormatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#75BBDA] bg-[#75BBDA4D]"
                                            className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"
                                        />

                                        <AmazonStatCard
                                            label="ASP"
                                            current={
                                                showLiveBI && rangeActive
                                                    ? biCardKpis.curr.asp
                                                    : convertToDisplayCurrency(uk.aspGBP ?? 0, amazonDataCurrency)
                                            }
                                            previous={
                                                showLiveBI && rangeActive
                                                    ? biCardKpis.prev.asp
                                                    : convertToDisplayCurrency(prev.asp, amazonDataCurrency)
                                            }
                                            deltaPct={useBiForAmazonCards ? biCardKpis.deltas.asp : deltas.aspPct}
                                            loading={loading || biLoading}
                                            formatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#B75A5A] bg-[#B75A5A4D]"
                                            className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                        />

                                        <AmazonStatCard
                                            label="Cost of Ads"
                                            current={
                                                useBiForAmazonCards
                                                    ?
                                                    (cm2Ready
                                                        ? convertToDisplayCurrency(
                                                            biAlignedTotals?.total_current_advertising ?? 0,
                                                            biSourceCurrency
                                                        )
                                                        : 0)

                                                    : adsSpendTotal

                                            }
                                            previous={
                                                useBiForAmazonCards
                                                    ? (cm2Ready
                                                        ? convertToDisplayCurrency(
                                                            biAlignedTotals?.total_previous_advertising ?? 0,
                                                            biSourceCurrency
                                                        )
                                                        : 0)
                                                    : amazonPrevAdsDisp
                                            }
                                            deltaPct={
                                                useBiForAmazonCards
                                                    ? (cm2Ready
                                                        ? safeDeltaPct(
                                                            convertToDisplayCurrency(
                                                                biAlignedTotals?.total_current_advertising ?? 0,
                                                                biSourceCurrency
                                                            ),
                                                            convertToDisplayCurrency(
                                                                biAlignedTotals?.total_previous_advertising ?? 0,
                                                                biSourceCurrency
                                                            )
                                                        )
                                                        : null)
                                                    : amazonAdsDeltaPct
                                            }
                                            loading={loading || (useBiForAmazonCards ? biLoading : false)}
                                            formatter={(v) => renderMoneyWithPerUnit(Number(v) || 0, unitsToUse, formatDisplayAmount)}
                                            previousFormatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#C49466] bg-[#C494664D]"
                                            className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                        />

                                        <AmazonStatCard
                                            label="TACoS"
                                            current={
                                                useBiForAmazonCards
                                                    ? (cm2Ready
                                                        ? (() => {
                                                            // const ads = biAlignedTotals?.total_current_advertising ?? 0;

                                                            const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                                            return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                                                        })()
                                                        : 0)
                                                    : amazonCurrRoasPct
                                            }
                                            previous={
                                                useBiForAmazonCards
                                                    ? (cm2Ready
                                                        ? (() => {
                                                            const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                                            const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                                            return sales > 0 ? (ads / sales) * 100 : 0;
                                                        })()
                                                        : 0)
                                                    : amazonPrevRoasPct
                                            }
                                            deltaPct={
                                                useBiForAmazonCards
                                                    ? (cm2Ready
                                                        ? deltaPctAbs(
                                                            (() => {
                                                                const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                                                const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                                                return sales > 0 ? (ads / sales) * 100 : 0;
                                                            })(),
                                                            (() => {
                                                                const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                                                const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                                                return sales > 0 ? (ads / sales) * 100 : 0;
                                                            })()
                                                        )
                                                        : null)
                                                    : deltaPctAbs(amazonCurrRoasPct, amazonPrevRoasPct)
                                            }
                                            inverseDelta
                                            loading={loading || (useBiForAmazonCards ? biLoading : false)}
                                            formatter={fmtPct2}
                                            bottomLabel={prevLabel}
                                            // className="border-[#3A8EA4] bg-[#3A8EA44D]"
                                            className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                        />


                                        <AmazonStatCard
                                            label="CM2 Profit"
                                            current={
                                                useBiCm2
                                                    ?
                                                    (cm2Ready
                                                        ? convertToDisplayCurrency(biAlignedTotals?.total_current_profit_cm2 ?? 0, rangeCurrency)
                                                        : 0)
                                                    : cm2Profit
                                                // convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency) // ✅ MTD Transactions prev
                                                // cm2Profit
                                            }
                                            previous={
                                                useBiCm2
                                                    ? (cm2Ready
                                                        ? convertToDisplayCurrency(biAlignedTotals?.total_previous_profit_cm2 ?? 0, rangeCurrency)
                                                        : 0)
                                                    : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency) // ✅ MTD Transactions prev
                                            }
                                            deltaPct={
                                                useBiCm2
                                                    ? (cm2Ready
                                                        ? safeDeltaPct(
                                                            biAlignedTotals?.total_current_profit_cm2 ?? 0,
                                                            biAlignedTotals?.total_previous_profit_cm2 ?? 0
                                                        )
                                                        : null)
                                                    : safeDeltaPct(uk.cm2ProfitGBP ?? 0, prev.cm2Profit ?? 0) // ✅ MTD Transactions delta
                                            }
                                            loading={loading || (useBiCm2 ? biLoading : false)}
                                            formatter={(v) => renderMoneyWithPerUnit(Number(v) || 0, unitsToUse, formatDisplayAmount)}
                                            previousFormatter={formatDisplayAmount}
                                            bottomLabel={prevLabel}
                                            // className="border-[#B8C78C] bg-[#B8C78C4D]"
                                            className="border-[#B8C78C] border-t-4 border-t-[#B8C78C]"
                                        />

                                        <AmazonStatCard
                                            label="CM2 Profit %"
                                            current={
                                                useBiCm2
                                                    ? (cm2Ready ? (biAlignedTotals?.total_current_profit_percentage ?? 0) : 0)
                                                    :
                                                    // (prev.profitPct ?? 0)
                                                    cm2MarginPctForSummary

                                            }
                                            previous={
                                                useBiCm2
                                                    ? (cm2Ready ? (biAlignedTotals?.total_previous_profit_percentage ?? 0) : 0)
                                                    : (prev.profitPct ?? 0)
                                            }
                                            deltaPct={
                                                useBiCm2
                                                    ? (cm2Ready
                                                        ? deltaPctPoints(
                                                            biAlignedTotals?.total_current_profit_percentage ?? 0,
                                                            biAlignedTotals?.total_previous_profit_percentage ?? 0
                                                        )
                                                        : null)
                                                    : deltaPctPoints(curr.profitPct ?? 0, prev.profitPct ?? 0)
                                            }

                                            loading={loading || (useBiCm2 ? biLoading : false)}
                                            formatter={fmtPct}
                                            bottomLabel={prevLabel}
                                            // className="border-[#7B9A6D] bg-[#7B9A6D4D]"
                                            className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                                        />
                                    </div>
                                </div>



                                {/* Live BI graph */}
                                {showLiveBI && isCountryMode && (
                                    <div className="w-full rounded-xl border bg-white p-3 lg:p-3 2xl:p-5 shadow-sm overflow-x-hidden">
                                        <div className="w-full max-w-full min-w-0">

                                            {/* ✅ CASE 1: 202 → processing */}
                                            {biStatus === "processing" && (
                                                <div className="flex justify-center items-center py-10">
                                                    <Loader label="Preparing your Amazon data, this may take a moment…" />
                                                </div>
                                            )}

                                            {biStatus === "error" && (
                                                <div className="text-center py-10 text-sm text-red-500">
                                                    Taking longer than expected. Please refresh once.
                                                </div>
                                            )}

                                            {/* ✅ CASE 2: 200 but empty */}
                                            {biStatus === "ready" && !biDailySeriesHome && (
                                                <div className="text-center py-10 text-sm text-gray-500">
                                                    No data available for the selected period
                                                </div>
                                            )}

                                            {/* ✅ CASE 3: 200 + data */}
                                            {biStatus === "ready" && biDailySeriesHome && (
                                                <LiveBiLineGraph
                                                    dailySeries={biDailySeriesHome}
                                                    periods={biPeriods}
                                                    loading={biUiLoading}
                                                    error={biError}
                                                    selectedStartDay={selectedStartDay}
                                                    selectedEndDay={selectedEndDay}
                                                    currencySymbol={currencySymbol}
                                                />
                                            )}

                                        </div>
                                    </div>
                                )}


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
                    </div>

                    {/* RIGHT COLUMN – Sales Target */}
                    <aside className="col-span-12 lg:col-span-4 order-1 lg:order-2 h-full min-h-full self-stretch">
                        <div className="h-full grid grid-rows-[auto_minmax(0,1fr)] gap-4">
                            {/* Top card = only as tall as content */}
                            <div className="w-full self-start">
                                <SalesTargetStatsCard
                                    regions={regions}
                                    value={targetRegion}
                                    onChange={setTargetRegion}
                                    hideTabs={isCountryMode}
                                    homeCurrency={displayCurrency}
                                    formatHomeK={formatDisplayK}
                                    todayHome={targets_todayHome}
                                    mtdHome={targets_mtdHome}
                                    targetHome={stats_targetHome}
                                    lastMonthTotalHome={targets_lastMonthTotalHome}
                                    salesTrendPct={stats_salesTrendPct}
                                    targetTrendPct={stats_targetTrendPct}
                                    currentReimbursement={targets_reimbursement.current}
                                    previousReimbursement={targets_reimbursement.previous}
                                    biAlignedTotals={biAlignedTotalsHome}
                                    biEnabled={biCardsReady}
                                />
                            </div>

                            {/* Bottom card = fills remaining height */}
                            <div className="w-full min-h-0">
                                <div className="h-full lg:sticky lg:top-4 2xl:top-6">
                                    <SalesTargetCard
                                        data={targetData}
                                        homeCurrency={displayCurrency}
                                        convertToHomeCurrency={identityConvert}
                                        formatHomeK={formatDisplayK}
                                        todaySales={targets_todayHome}
                                        targetHome={stats_targetHome}
                                        mtdHome={targets_mtdHome}
                                        lastMonthTotalHome={targets_lastMonthTotalHome}
                                        lastMonthToDateHome={targets_lastMonthToDateHome}
                                        currentReimbursement={targets_reimbursement.current}
                                        previousReimbursement={targets_reimbursement.previous}
                                        biAlignedTotals={biAlignedTotalsHome}
                                        biEnabled={biCardsReady}
                                        periodCompletedPct={rangeCompletedPct}
                                        periodCompletedLabel="Range"
                                    />
                                </div>
                            </div>
                        </div>
                    </aside>
                </div >

            )}


            {
                platform === "global" && showLiveBI && (
                    // <div className="mt-6 w-full rounded-2xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden">
                    <div
                        id="targets-action-items"
                        className="mt-6 w-full rounded-xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden scroll-mt-[80px]"
                    >
                        <div className="w-full max-w-full min-w-0">
                            <LiveBiLineGraph
                                dailySeries={biDailySeriesHome}
                                periods={biPeriods}
                                loading={biUiLoading}
                                error={biError}
                                selectedStartDay={selectedStartDay}
                                selectedEndDay={selectedEndDay}
                                currencySymbol={currencySymbol}
                            />
                        </div>
                    </div>
                )
            }


            {/* {activeTab === "summary" && (
                <div className="w-full overflow-x-hidden">
                    {showLiveBI && liveBiPayload && (
                        <LiveBusinessClient
                            countryName={countryName}
                            ranged="MTD"
                            month={(currMonthName || "").toLowerCase()}
                            year={String(currYear)}
                            initialData={liveBiPayload}
                            disableAutoFetch
                            onGenerateInsights={() => fetchLiveBiPayload({ generateInsights: true })}
                        />
                    )}
                </div>
            )} */}

            {activeTab === "summary" && (
                <div className="w-full overflow-x-hidden">
                    {summaryLoading || !liveBiPayload ? (
                        <div className="flex min-h-[300px] items-center justify-center py-12 text-center">
                            <Loader />
                        </div>
                    ) : (
                        showLiveBI && (
                            <LiveBusinessClient
                                countryName={countryName}
                                ranged="MTD"
                                month={(currMonthName || "").toLowerCase()}
                                year={String(currYear)}
                                initialData={liveBiPayload}
                                disableAutoFetch
                                onGenerateInsights={() => fetchLiveBiPayload({ generateInsights: true })}
                            />
                        )
                    )}
                </div>
            )}

            {activeTab === "productwise" && (
                <>
                    <div id="pnl-mtd" className="scroll-mt-[80px] mt-2 md:mt-4 w-full rounded-xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-auto">
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
                            <div className="flex items-center gap-2">
                                <DownloadIconButton
                                    onClick={handleDownloadPlProductwiseMtd}
                                    aria-label="Download P&L Productwise Breakdown MTD"
                                    className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                />
                            </div>

                        </div>


                        {error ? (
                            <div className="text-sm text-red-600">{error}</div>
                        ) : loading && monthlySkuwiseRows.length === 0 ? (
                            <div className="text-sm text-gray-500">Loading…</div>
                        ) : (
                            <div className="w-full overflow-x-auto rounded-xl border border-gray-300">
                                <div className="min-w-full">
                                    <GroupedCollapsibleTable<MonthlySkuwiseTableRow>
                                        rows={monthlySkuwiseRowsForTable}
                                        getRowKey={(row, idx) => (row.isTotal ? "GRAND_TOTAL" : row.isOthers ? "OTHERS" : row.sku || String(idx))}
                                        leftCols={SKUWISE_LEFT_COLS}
                                        groups={SKUWISE_GROUPS}
                                        singleCols={SKUWISE_SINGLE_COLS}
                                        showSignRowInBody
                                        getSignForCol={getAdsSignForCol}
                                        layout={[
                                            { type: "group", id: "quantity" },
                                            { type: "single", key: "asp" },
                                            { type: "single", key: "net_sales" },
                                            { type: "single", key: "cogs" },
                                            { type: "group", id: "marketplace_fees" },
                                            { type: "group", id: "tax_and_credits" },
                                            { type: "group", id: "profit" },
                                            { type: "single", key: "ads_spend" },
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
                                            if (colKey === "product_name") return row.isTotal ? "Total" : row.isOthers ? "Others" : row.product_name;

                                            if (colKey === "quantity") return row.quantity;

                                            if (colKey === "asp") return formatAdsNumber(row.asp);
                                            if (colKey === "net_sales") return formatAdsNumber(row.net_sales);

                                            if (colKey === "tax" || colKey === "credits" || colKey === "tax_and_credits" || colKey === "cm1_profit_per" || colKey === "cm1_profit_per_unit") {
                                                const v = Number((row as any)[colKey] ?? 0);
                                                return formatAdsNumber(Math.abs(Number.isFinite(v) ? v : 0));
                                            }

                                            if (colKey === "cm2_profit_per" || colKey === "cm2_profit_per_unit") {
                                                const v = Number((row as any)[colKey] ?? 0);
                                                return formatAdsNumber(Number.isFinite(v) ? v : 0);
                                            }

                                            if (colKey === "ad_type") {
                                                if (row.isOthers || row.isTotal) return "-";
                                                return formatAdType((row as any).ad_type);
                                            }


                                            if (colKey === "ads_spend")
                                                return formatAdsNumber(Math.abs(row.ads_spend));

                                            if (colKey === "cogs")
                                                return formatAdsNumber(Math.abs(row.cogs));

                                            if (colKey === "fba_fees")
                                                return formatAdsNumber(Math.abs(row.fba_fees));

                                            if (colKey === "selling_fees")
                                                return formatAdsNumber(Math.abs(row.selling_fees));

                                            if (colKey === "marketplace_total")
                                                return formatAdsNumber(
                                                    Math.abs(row.fba_fees) + Math.abs(row.selling_fees)
                                                );

                                            if (colKey === "cm2_profit")
                                                return formatAdsNumber(row.cm2_profit);

                                            if (colKey === "profit")
                                                return formatAdsNumber(row.profit);

                                            return (row as any)[colKey] ?? "";
                                        }}
                                        summary={{
                                            enabled: monthlySkuwiseRowsForTable.length > 0,

                                            sections: [
                                                {
                                                    id: "ads",
                                                    label: "Cost of Advertisement",
                                                    endValue: formatSummaryValue(costOfAds, "advertising_total"),
                                                    defaultCollapsed: true,
                                                    children: [
                                                        {
                                                            id: "ads_1",
                                                            label: <>Visibility - Ads <strong className="text-[#ff5c5c]">(-)</strong></>,
                                                            midValue: formatSummaryValue(sponsoredBrandSpend, "advertising_total"),
                                                            // midValue: formatSummaryValue(sponsoredProductsSpend, "advertising_total"),
                                                        },
                                                        // {
                                                        //     id: "ads_2",
                                                        //     label: <>Sponsored Display <strong className="text-[#ff5c5c]">(-)</strong></>,
                                                        //     midValue: formatSummaryValue(sponsoredDisplaySpend, "advertising_total"),
                                                        // },
                                                        {
                                                            id: "ads_3",
                                                            label: <>Visibility - Deals, Vouchers and Reviews <strong className="text-[#ff5c5c]">(-)</strong></>,
                                                            midValue: formatSummaryValue(dealVouchers, "advertising_total"),
                                                        },
                                                    ],
                                                },

                                                {
                                                    id: "other",
                                                    label: "Other Transactions",
                                                    endValue: formatSummaryValue(platformFee, "platform_fee"),
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
                                                            midValue: formatSummaryValue(inventoryStorageFees, "inventory_storage_fees"),
                                                        },
                                                        {
                                                            id: "other_misc",
                                                            label: <>Misc. Transactions <strong className="text-green-500">(+)</strong></>,
                                                            midValue: "-",
                                                        },
                                                        {
                                                            id: "other_3",
                                                            label: (
                                                                <>
                                                                    Reimbursement for lost Inventory
                                                                    {/* {totals.reimbursement_lost_inventory_units
                                                                ? ` - ${totals.reimbursement_lost_inventory_units} Units `
                                                                : " "} */}
                                                                    <strong className="text-green-500">(+)</strong>
                                                                </>
                                                            ),
                                                            midValue: formatSummaryValue(lost_inventory_total, "lost_total"),
                                                        },
                                                    ],
                                                },
                                            ],

                                            fixedRows: [
                                                ...(countryName === "us" || countryName === "global"
                                                    ? [
                                                        {
                                                            id: "ship",
                                                            label: (
                                                                <>
                                                                    Shipment Charges <strong>(-)</strong>
                                                                </>
                                                            ),
                                                            endValue: formatSummaryValue(plSummaryTotals.shipment_charges, "shipment_charges"),
                                                        },
                                                    ]
                                                    : []),

                                                {
                                                    id: "cm2_profit",
                                                    label: "CM2 Profit/Loss",
                                                    endValue: Number(cm2Profit.toFixed(2)),
                                                },
                                                {
                                                    id: "cm2_margins",
                                                    label: "CM2 Margins",
                                                    endValue: `${formatSummaryValue(cm2MarginPctForSummary, "cm2_margins")}%`,
                                                },

                                                {
                                                    id: "tacos",
                                                    label: "TACoS (Total Advertising Cost of Sale)",
                                                    endValue: `${formatSummaryValue(tacosPctForSummary, "acos")}%`,
                                                },

                                                {
                                                    id: "net_reimb",
                                                    label: "Net Reimbursement",
                                                    endValue: `${formatSummaryValue(reimbursementForSummary, "net_reimbursement")}`,
                                                },
                                                {
                                                    id: "rv_cm2",
                                                    label: "Reimbursement vs CM2 Margins",
                                                    endValue: `${formatSummaryValue(
                                                        reimbursementVsCm2PctForSummary, "rembursment_vs_cm2_margins")}%`,
                                                },
                                                {
                                                    id: "rv_sales",
                                                    label: "Reimbursement vs Sales",
                                                    endValue: `${formatSummaryValue(
                                                        reimbursementVsSalesPctForSummary, "reimbursement_vs_sales")}%`,
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
                                "grid grid-cols-1 gap-4 items-start min-[1700px]:items-stretch",
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
                                        {/* Only this part depends on isCountryMode */}
                                        {!isCountryMode && (
                                            <>
                                                <SegmentedToggle<RegionKey>
                                                    value={graphRegion}
                                                    options={graphRegions.map((r) => ({ value: r }))}
                                                    onChange={setGraphRegion}
                                                />
                                                {/* <DownloadIconButton onClick={handleDownload} /> */}
                                            </>
                                        )}
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
                                            labels={labels}
                                            values={valuesPatched}
                                            prevValues={prevValues}
                                            expanded={isMtdPlExpanded}
                                            colors={colors}
                                            loading={loading}
                                            allValuesZero={allValuesZero}
                                        />
                                    </div>
                                </div>
                            </div>

                            {/* RIGHT: CM1 Profit Breakdown Pie (hide when expanded) */}
                            {!isMtdPlExpanded && (
                                <div className="min-w-0 h-full flex flex-col">
                                    <Cm1ProfitBreakdownPie
                                        title="CM1 Profit Breakdown"
                                        data={cm1ProfitPieData}
                                        currency={displayCurrency}
                                        height={320}
                                    />
                                </div>
                            )}
                        </div>
                    </div>
                </>
            )}

            {/* {amazonIntegrated && graphRegionToUse !== "Global" && ( */}
            {activeTab === "inventory" && amazonIntegrated && graphRegionToUse !== "Global" && (
                <div id="current-inventory" className="scroll-mt-[80px] ">
                    {/* <CurrentInventorySection region={graphRegionToUse} /> */}
                    <CurrentInventorySection
                        region={graphRegionToUse}
                        invLoading={invLoading}
                        invError={invError}
                        invRows={invRows}
                        inventoryAlerts={inventoryAlerts}
                        userData={userData}
                    />

                </div>
            )}

        </div >

    );
}
