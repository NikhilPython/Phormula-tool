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
import GroupedCollapsibleTable, { ColGroup } from "@/components/ui/table/GroupedCollapsibleTable";
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
    product_name: string;
    ad_type?: string;

    quantity: number;
    asp: number;
    net_sales: number;
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
    brand_spend?: number;
    dealsvouchar_ads?: number;
    platformfeenew?: number;
    isTotal?: boolean;
    isOthers?: boolean;
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

type ProductwiseMoneyKey =
    | "asp"
    | "net_sales"
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
    | "platform_fee"
    | "platform_fee_inventory_storage"
    | "lost_total"
    | "other"
    | "product_spend"
    | "brand_spend"
    | "dealsvouchar_ads"
    | "platformfeenew";


/* ===================== ENV & ENDPOINTS ===================== */
const baseURL =
    process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";
const FIN_MTD_TX_ENDPOINT = `${baseURL}/amazon_api/finances/mtd_transactions`;
const SHOPIFY_DROPDOWN_ENDPOINT = `${baseURL}/shopify/dropdown`;
// const FX_RATES_GET_ENDPOINT = `${baseURL}/currency-rates`;

const LIVE_MTD_BI_ENDPOINT = `${baseURL}/live_mtd_bi`;
const LIVE_DASHBOARD_CACHE_ENDPOINT = `${baseURL}/amazon_api/live-dashboard/save`;

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

        shipment_charges: toNumber(
            source?.shipment_charges ??
            source?.shipping_charges ??
            source?.shipment_fees
        ),
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

const REGION_TIMEZONE: Record<RegionKey, string> = {
    Global: "Asia/Kolkata", // or keep a default of your choice
    UK: "Europe/London",
    US: "America/New_York",
    CA: "America/Toronto",
};

const getTimezoneForRegion = (region: RegionKey) => {
    return REGION_TIMEZONE[region] || "Asia/Kolkata";
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

const getRegionNow = (region: RegionKey) => {
    const tz = getTimezoneForRegion(region);
    return new Date(new Date().toLocaleString("en-US", { timeZone: tz }));
};

const getRegionYearMonth = (region: RegionKey) => {
    const now = getRegionNow(region);

    const monthName = now.toLocaleString("en-US", {
        month: "long",
        timeZone: getTimezoneForRegion(region),
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
        timeZone: getTimezoneForRegion(region),
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
            countries: ["UK"],
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

    const formatUKTime12hr = (
        timestamp: string | number | Date | null | undefined
    ) => {
        if (timestamp == null) return "";

        // If backend already sends UK local time as plain string,
        // do NOT create a Date and do NOT apply timezone conversion.
        if (typeof timestamp === "string") {
            const match = timestamp.match(
                /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/
            );

            if (!match) return "";

            const hour24 = Number(match[4]);
            const minute = match[5];

            const hour12 = hour24 % 12 || 12;
            const ampm = hour24 >= 12 ? "PM" : "AM";

            return `${hour12}:${minute} ${ampm} BST`;
        }

        // Fallback only if you actually receive a real Date/UTC timestamp
        const date = timestamp instanceof Date ? timestamp : new Date(timestamp);
        if (Number.isNaN(date.getTime())) return "";

        return new Intl.DateTimeFormat("en-GB", {
            timeZone: "Europe/London",
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

        return new Intl.DateTimeFormat("en-US", {
            timeZone: "America/New_York", // or LA if needed
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
    const prevLabel = useMemo(() => getPrevMonthShortLabel(), []);
    // const getDayOfMonthIST = () => {
    //     const now = new Date();
    //     const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    //     return ist.getDate(); // 1..31
    // };

    const [todaySalesRaw, setTodaySalesRaw] = useState<number>(0);

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


    const fetchMonthlySp = useCallback(async () => {
        if (isMonthYearNA) {
            setMonthlySpLoading(false);
            setMonthlySpError(null);
            setMonthlySpRows([]);
            setMonthlySpTotalSpend(null);
            return;
        }
        try {
            setMonthlySpLoading(true);
            setMonthlySpError(null);

            const token =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

            if (!token) throw new Error("No token found. Please sign in.");

            const country =
                platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

            const { monthName, year } = getRegionYearMonth(activeDateRegion);

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
    }, [platform, isMonthYearNA, activeDateRegion]);


    const showInventoryToast = ({
        sku,
        productName,
        onDismiss,
        currentParams,
    }: {
        sku: string;
        productName: string;
        onDismiss: () => void;
        currentParams: {
            countryName: string;
            month: string;
            year: string;
        };
    }) => {
        toast.custom(
            (toastId) => (
                <div
                    style={{
                        background: "#FFFFFF", // deep black
                        color: "#ffffff",
                        padding: "16px 18px",
                        borderRadius: "12px",
                        fontSize: "14px",
                        fontWeight: 500,
                        // boxShadow: "0 10px 25px rgba(0,0,0,0.4)",
                        display: "flex",
                        flexDirection: "column",
                        gap: "10px",
                        minWidth: "300px",
                        border: "1px solid #414042",
                    }}
                >
                    <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
                        <div
                            style={{
                                width: "4px",
                                height: "20px",
                                background: "#ef4444", // red accent
                                borderRadius: "2px",
                            }}
                        />
                        <div style={{ fontSize: "15px", fontWeight: 600, color: "#414042" }}>
                            {productName}
                        </div>
                    </div>

                    <div
                        style={{
                            fontSize: "12px",
                            background: "rgba(239, 68, 68, 0.08)", // soft red tint
                            color: "#414042",
                            padding: "8px 10px",
                            borderRadius: "6px",
                            lineHeight: 1.5,
                            border: "1px solid rgba(239, 68, 68, 0.25)",
                        }}
                    >
                        Top-selling item is running low. Restock recommended.
                    </div>

                    <div style={{ display: "flex", gap: "8px", marginTop: "6px" }}>
                        <button
                            onClick={() => {
                                const targetUrl = `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#current-inventory`;

                                // make sure the dashboard is on the live tab
                                setActiveTab("live");
                                setPendingHash("#current-inventory");

                                // navigate
                                router.push(targetUrl);

                                // after tab/page updates, force scroll
                                setTimeout(() => {
                                    const el = document.getElementById("current-inventory");
                                    if (el) {
                                        el.scrollIntoView({
                                            behavior: "smooth",
                                            block: "start",
                                        });
                                    } else {
                                        window.location.hash = "current-inventory";
                                    }
                                }, 250);

                                toast.dismiss(toastId);
                            }}
                            style={{
                                background: "#FFFFFF",
                                color: "#414042",
                                padding: "6px 10px",
                                borderRadius: "6px",
                                fontSize: "12px",
                                border: "1px solid #414042",
                                cursor: "pointer",
                            }}
                        >
                            View
                        </button>

                        <button
                            onClick={() => {
                                onDismiss();
                                toast.dismiss(toastId);
                            }}
                            style={{
                                background: "#FFFFFF",
                                color: "#414042",
                                padding: "6px 10px",
                                borderRadius: "6px",
                                fontSize: "12px",
                                border: "1px solid #414042",
                                cursor: "pointer",
                            }}
                        >
                            Dismiss
                        </button>
                    </div>
                </div>
            ),
            {
                id: sku,
                duration: Infinity,
            }
        );
    };

    // useEffect(() => {
    //     const top5HighAlerts = top5Skus
    //         .map((sku) => ({
    //             sku,
    //             productName: skuToProductName[sku] || sku,
    //             alert: inventoryAlerts?.[sku]?.alert || "",
    //             alertType: inventoryAlerts?.[sku]?.alert_type || "error",
    //         }))
    //         .filter(
    //             (item) =>
    //                 item.alert.trim().toLowerCase() === "high alert" &&
    //                 !dismissedAlerts.includes(item.sku)
    //         );

    //     top5HighAlerts.forEach(({ sku, productName }) => {
    //         if (shownInventoryToastIdsRef.current.has(sku)) return;

    //         shownInventoryToastIdsRef.current.add(sku);

    //         showInventoryToast({
    //             sku,
    //             productName,
    //             onDismiss: () => handleDismiss(sku),
    //             currentParams: {
    //                 countryName: countryName,
    //                 month: urlMonthParam || "",
    //                 year: urlYearParam || "",
    //             },
    //         });
    //     });
    // }, [top5Skus, skuToProductName, inventoryAlerts, dismissedAlerts]);



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
    const [biDailySeries, setBiDailySeries] = useState<DailySeries | null>(null);
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
        adsLoading;

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
        }
    }, [activeDateRegion]);

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

        console.log({
            activeDateRegion,
            timezone,
            regionDateTime: now.toLocaleString("en-GB", {
                timeZone: timezone,
                year: "numeric",
                month: "2-digit",
                day: "2-digit",
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                hour12: false,
            }),
        });
    }, [activeDateRegion]);

    useEffect(() => {
        if (!isCountryMode) return;
        setGraphRegion(forcedRegion);
        setAmazonRegion(forcedRegion);
    }, [isCountryMode, forcedRegion]);

    const [targetRegion, setTargetRegion] = useState<RegionKey>(
        isCountryMode ? forcedRegion : "Global"
    );

    useEffect(() => {
        if (isCountryMode) setTargetRegion(forcedRegion);
    }, [isCountryMode, forcedRegion]);

    // const didAdsManagerSeedRef = useRef(false);

    // ===================== EFFECTS =====================

    // useEffect(() => {
    //     let cancelled = false;

    //     const run = async () => {
    //         try {
    //             setAdsLoading(true);

    //             if (platform === "shopify") {
    //                 if (!cancelled) setAdsSeeded(true);
    //                 return;
    //             }

    //             const jwtToken =
    //                 typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    //             if (!jwtToken) {
    //                 if (!cancelled) {
    //                     setAdsSeeded(false);
    //                     setAdsSeedError("No token found. Please sign in.");
    //                 }
    //                 return;
    //             }

    //             const country =
    //                 platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

    //             await ensureSpReportSeedOncePerDay(baseURL, jwtToken, country);

    //             if (country === "UK" || country === "US") {
    //                 await ensureSdReportSeedOncePerDay(baseURL, jwtToken, country);
    //             }

    //             if (!cancelled) {
    //                 setAdsSeedError(null);
    //                 setAdsSeeded(true);
    //             }
    //         } catch (e: any) {
    //             if (!cancelled) {
    //                 setAdsSeedError(e?.message || "Ads seed failed");
    //                 setAdsSeeded(false);
    //             }
    //         } finally {
    //             if (!cancelled) setAdsLoading(false);
    //         }
    //     };

    //     setAdsSeeded(false);
    //     run();

    //     return () => {
    //         cancelled = true;
    //     };
    // }, [platform, baseURL]);

    // const didMonthlyAdsSyncRef = useRef(false);
    // useEffect(() => {
    //     if (!adsSeeded) return;
    //     if (platform === "shopify") return;

    //     let cancelled = false;

    //     const run = async () => {
    //         try {
    //             const jwtToken =
    //                 typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
    //             if (!jwtToken) return;

    //             const country =
    //                 platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

    //             const { monthName, year } = getRegionYearMonth(activeDateRegion);
    //             const month = monthToNumber(monthName.toLowerCase());

    //             const include = country === "UK" || country === "US" ? ["SP", "SD"] : ["SP"];

    //             const res = await fetch(`${baseURL}/api/ads/monthly_sp_sd_to_db`, {
    //                 method: "POST",
    //                 headers: {
    //                     Authorization: `Bearer ${jwtToken}`,
    //                     Accept: "application/json",
    //                     "Content-Type": "application/json",
    //                 },
    //                 body: JSON.stringify({ month, year, country, include }),
    //             });

    //             const json = await res.json().catch(() => ({}));

    //             if (res.status === 404 && String(json?.error || "").includes("No rows found")) {
    //                 console.warn(`No monthly ads rows for ${country} ${month}/${year}. Skipping.`);
    //                 return;
    //             }

    //             if (!res.ok) throw new Error(json?.error || "monthly_sp_sd_to_db failed");

    //             if (cancelled) return;

    //             await fetchMonthlySp();
    //         } catch (e) {
    //             console.error("monthly_sp_sd_to_db error:", e);
    //         }
    //     };
    //     run();
    //     return () => {
    //         cancelled = true;
    //     };
    // }, [adsSeeded, platform, baseURL, fetchMonthlySp]);


    // useEffect(() => {
    //     let cancelled = false;
    //     const run = async () => {
    //         try {
    //             setAdsLoading(true);

    //             if (platform === "shopify") {
    //                 if (!cancelled) setAdsSeeded(true);
    //                 return;
    //             }
    //             const jwtToken =
    //                 typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    //             if (!jwtToken) {
    //                 if (!cancelled) {
    //                     setAdsSeeded(false);
    //                     setAdsSeedError("No token found. Please sign in.");
    //                 }
    //                 return;
    //             }
    //             const country =
    //                 platform === "amazon-us" ? "US" : platform === "amazon-ca" ? "CA" : "UK";

    //             await ensureSpReportSeedOncePerDay(baseURL, jwtToken, country);

    //             if (country === "UK" || country === "US") {
    //                 await ensureSdReportSeedOncePerDay(baseURL, jwtToken, country);
    //             }
    //             await ensureSbKeywordReportSeedOncePerDay(baseURL, jwtToken, country);

    //             if (!cancelled) {
    //                 setAdsSeedError(null);
    //                 setAdsSeeded(true);
    //             }
    //         } catch (e: any) {
    //             if (!cancelled) {
    //                 setAdsSeedError(e?.message || "Ads seed failed");
    //                 setAdsSeeded(false);
    //             }
    //         } finally {
    //             if (!cancelled) setAdsLoading(false);
    //         }
    //     };

    //     setAdsSeeded(false);
    //     run();

    //     return () => {
    //         cancelled = true;
    //     };
    // }, [platform, baseURL]);



    const inventoryCountry = useMemo(() => {
        // Country pages
        if (graphRegionToUse === "UK") return "uk";
        if (graphRegionToUse === "US") return "us";
        if (graphRegionToUse === "CA") return "ca";

        // Global page:
        // inventory is unit-based, so fetch a real marketplace instead of "global"
        return "uk";
    }, [graphRegionToUse]);

    const invMonthYear = useMemo(() => {
        const { monthName, year } = getRegionYearMonth(activeDateRegion);
        return { month: monthName.toLowerCase(), year: String(year) };
    }, [activeDateRegion]);

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

    const userMonthlyTargetGBP = useMemo(() => {
        return toNumberSafe(userData?.target_sales ?? 0);
    }, [userData?.target_sales]);

    const userMonthlyTargetHome = useMemo(() => {
        if (!userMonthlyTargetGBP) return 0;

        // ✅ If US → already USD, no conversion
        if (platform === "amazon-us") {
            return userMonthlyTargetGBP;
        }

        // UK / others → convert from GBP
        return convertToDisplayCurrency(userMonthlyTargetGBP, "GBP");
    }, [userMonthlyTargetGBP, convertToDisplayCurrency, platform]);


    const prevFullMonthNetSalesDisp = useMemo(() => {
        const v = liveBiPayload?.aligned_totals?.total_previous_net_sales_full_month;
        if (v == null) return 0;
        return convertToDisplayCurrency(Number(v) || 0, biSourceCurrency);
    }, [liveBiPayload, convertToDisplayCurrency, biSourceCurrency]);

    // const cm1ProfitPieData = useMemo<Cm1PieSlice[]>(() => {
    //     const toPieCurrency = (v: number) => {
    //         const n = Number(v || 0);
    //         if (!n) return 0;

    //         // Global tab => convert source currency to user's home/display currency
    //         if (platform === "global") {
    //             return convertToDisplayCurrency(n, biSourceCurrency);
    //         }

    //         // Country tabs => keep native/source currency
    //         return n;
    //     };

    //     const apiSlices = liveBiPayload?.cm1_profit_pie?.slices;

    //     if (apiSlices?.length) {
    //         const merged = new Map<
    //             string,
    //             { name: string; value: number; prevValue: number; deltaPct: number }
    //         >();

    //         for (const s of apiSlices) {
    //             const name = (s.name || "Others").trim();

    //             const value = toPieCurrency(Number(s.profit_curr || 0));
    //             const prevValue = toPieCurrency(Number(s.profit_prev || 0));

    //             const existing = merged.get(name);
    //             if (existing) {
    //                 existing.value += value;
    //                 existing.prevValue += prevValue;
    //                 existing.deltaPct = Number(s.delta_pct ?? existing.deltaPct ?? 0);
    //             } else {
    //                 merged.set(name, {
    //                     name,
    //                     value,
    //                     prevValue,
    //                     deltaPct: Number(s.delta_pct ?? 0),
    //                 });
    //             }
    //         }

    //         const total =
    //             Array.from(merged.values()).reduce((sum, r) => sum + r.value, 0) || 1;

    //         return Array.from(merged.values())
    //             .map((r) => ({
    //                 ...r,
    //                 pct: (r.value / total) * 100,
    //             }))
    //             .sort((a, b) => b.value - a.value);
    //     }

    //     const cg = liveBiPayload?.categorized_growth;
    //     const top80 = cg?.top_80_skus ?? [];
    //     const other = cg?.other_skus ?? [];
    //     const combined = [...top80, ...other];

    //     if (!combined.length) return [];

    //     const sorted = combined
    //         .map((r: any) => {
    //             const curr = toPieCurrency(Number(r?.profit_curr ?? 0));
    //             const prev = toPieCurrency(Number(r?.profit_prev ?? 0));

    //             return {
    //                 name: String(r?.product_name ?? "Unknown"),
    //                 profit_curr: curr,
    //                 profit_prev: prev,
    //             };
    //         })
    //         .filter((x) => x.profit_curr !== 0 || x.profit_prev !== 0)
    //         .sort((a, b) => b.profit_curr - a.profit_curr);

    //     const total = sorted.reduce((s, x) => s + x.profit_curr, 0) || 1;

    //     const top = sorted.slice(0, 5);
    //     const rest = sorted.slice(5);

    //     const named: Cm1PieSlice[] = top.map((x) => ({
    //         name: x.name,
    //         value: x.profit_curr,
    //         prevValue: x.profit_prev,
    //         pct: (x.profit_curr / total) * 100,
    //         deltaPct: x.profit_prev
    //             ? ((x.profit_curr - x.profit_prev) / Math.abs(x.profit_prev)) * 100
    //             : 0,
    //     }));

    //     if (rest.length) {
    //         const restCurr = rest.reduce((s, x) => s + x.profit_curr, 0);
    //         const restPrev = rest.reduce((s, x) => s + x.profit_prev, 0);

    //         named.push({
    //             name: "Others",
    //             value: restCurr,
    //             prevValue: restPrev,
    //             pct: (restCurr / total) * 100,
    //             deltaPct: restPrev
    //                 ? ((restCurr - restPrev) / Math.abs(restPrev)) * 100
    //                 : 0,
    //         });
    //     }

    //     return named.sort((a, b) => b.value - a.value);
    // }, [liveBiPayload?.cm1_profit_pie, liveBiPayload?.categorized_growth, platform, biSourceCurrency, convertToDisplayCurrency]);

    const cm1ProfitPieData = useMemo<Cm1PieSlice[]>(() => {
        const toPieCurrency = (v: number) => {
            const n = Number(v || 0);
            if (!n) return 0;

            if (platform === "global") {
                return convertToDisplayCurrency(n, biSourceCurrency);
            }

            return n;
        };

        const buildFinalRows = (
            rows: Array<{
                name: string;
                value: number;
                prevValue: number;
                deltaPct?: number | null;
            }>
        ): Cm1PieSlice[] => {
            const cleaned = rows
                .map((r: {
                    name: string;
                    value: number;
                    prevValue: number;
                    deltaPct?: number | null;
                }) => ({
                    name: String(r.name || "Unknown").trim() || "Unknown",
                    value: Number(r.value || 0),
                    prevValue: Number(r.prevValue || 0),
                    deltaPct:
                        r.deltaPct == null
                            ? Number(r.prevValue || 0) !== 0
                                ? ((Number(r.value || 0) - Number(r.prevValue || 0)) /
                                    Math.abs(Number(r.prevValue || 0))) * 100
                                : null
                            : r.deltaPct,
                }))
                .filter((r: {
                    value: number;
                    prevValue: number;
                }) => r.value !== 0 || r.prevValue !== 0);

            if (!cleaned.length) return [];

            const merged = new Map<
                string,
                { name: string; value: number; prevValue: number; deltaPct: number | null }
            >();

            for (const row of cleaned) {
                const key = row.name;
                const existing = merged.get(key);

                if (existing) {
                    existing.value += row.value;
                    existing.prevValue += row.prevValue;

                    existing.deltaPct =
                        existing.prevValue !== 0
                            ? ((existing.value - existing.prevValue) /
                                Math.abs(existing.prevValue)) * 100
                            : null;
                } else {
                    merged.set(key, { ...row });
                }
            }

            const mergedRows = Array.from(merged.values()).sort(
                (a: { value: number }, b: { value: number }) => b.value - a.value
            );

            const total = mergedRows.reduce(
                (sum: number, r: { value: number }) => sum + r.value,
                0
            ) || 1;

            return mergedRows.map((r: {
                name: string;
                value: number;
                prevValue: number;
                deltaPct: number | null;
            }) => ({
                name: r.name,
                value: r.value,
                prevValue: r.prevValue,
                pct: (r.value / total) * 100,
                deltaPct: r.deltaPct,
            }));
        };

        const apiSlices = liveBiPayload?.cm1_profit_pie?.slices;

        if (Array.isArray(apiSlices) && apiSlices.length) {
            return buildFinalRows(
                apiSlices.map((s: any) => ({
                    name: String(s?.name || "Others").trim(),
                    value: toPieCurrency(Number(s?.profit_curr || 0)),
                    prevValue: toPieCurrency(Number(s?.profit_prev || 0)),
                    deltaPct: s?.delta_pct == null ? null : Number(s.delta_pct),
                }))
            );
        }

        const cg = liveBiPayload?.categorized_growth;
        const top80 = Array.isArray(cg?.top_80_skus) ? cg.top_80_skus : [];
        const other = Array.isArray(cg?.other_skus) ? cg.other_skus : [];
        const combinedGrowth = [...top80, ...other];

        if (combinedGrowth.length) {
            const sorted = combinedGrowth
                .map((r: any) => ({
                    name: String(r?.product_name ?? r?.name ?? "Unknown"),
                    value: toPieCurrency(Number(r?.profit_curr ?? 0)),
                    prevValue: toPieCurrency(Number(r?.profit_prev ?? 0)),
                    deltaPct:
                        Number(r?.profit_prev ?? 0) !== 0
                            ? ((Number(r?.profit_curr ?? 0) - Number(r?.profit_prev ?? 0)) /
                                Math.abs(Number(r?.profit_prev ?? 0))) * 100
                            : null,
                }))
                .filter((x: {
                    value: number;
                    prevValue: number;
                }) => x.value !== 0 || x.prevValue !== 0)
                .sort((a: { value: number }, b: { value: number }) => b.value - a.value);

            const top = sorted.slice(0, 5);
            const rest = sorted.slice(5);

            const rows = [...top];

            if (rest.length) {
                const restCurr = rest.reduce(
                    (s: number, x: { value: number }) => s + x.value,
                    0
                );

                const restPrev = rest.reduce(
                    (s: number, x: { prevValue: number }) => s + x.prevValue,
                    0
                );

                rows.push({
                    name: "Others",
                    value: restCurr,
                    prevValue: restPrev,
                    deltaPct:
                        restPrev !== 0
                            ? ((restCurr - restPrev) / Math.abs(restPrev)) * 100
                            : null,
                });
            }

            return buildFinalRows(rows);
        }

        const skuwiseItems = Array.isArray((data as any)?.skuwise_items)
            ? (data as any).skuwise_items
            : [];

        if (skuwiseItems.length) {
            const bodyRows = skuwiseItems.filter((r: any) =>
                r?.sku &&
                r.sku !== "GRAND_TOTAL" &&
                String(r?.product_name || "").trim() !== ""
            );

            if (!bodyRows.length) return [];

            const mapped = bodyRows
                .map((r: any) => ({
                    name: String(r?.product_name || r?.sku || "Unknown"),
                    value: toPieCurrency(Number(r?.profit ?? r?.cm1_profit ?? 0)),
                    prevValue: toPieCurrency(
                        Number(r?.profit_prev ?? r?.previous_profit ?? r?.prev_profit ?? 0)
                    ),
                    deltaPct: null,
                }))
                .filter((r: { value: number; prevValue: number }) =>
                    r.value !== 0 || r.prevValue !== 0
                )
                .sort((a: { value: number }, b: { value: number }) => b.value - a.value);

            if (!mapped.length) return [];

            const top = mapped.slice(0, 5);
            const rest = mapped.slice(5);

            const rows = [...top];

            if (rest.length) {
                rows.push({
                    name: "Others",
                    value: rest.reduce(
                        (s: number, x: { value: number }) => s + x.value,
                        0
                    ),
                    prevValue: rest.reduce(
                        (s: number, x: { prevValue: number }) => s + x.prevValue,
                        0
                    ),
                    deltaPct: null,
                });
            }

            return buildFinalRows(rows);
        }

        return [];
    }, [
        liveBiPayload?.cm1_profit_pie,
        liveBiPayload?.categorized_growth,
        data,
        platform,
        biSourceCurrency,
        convertToDisplayCurrency,
    ]);

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
            const uiCountry =
                platform === "amazon-us" ? "us" : platform === "amazon-ca" ? "ca" : "uk";

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
    const { monthName: currMonthName, year: currYear } = getRegionYearMonth(activeDateRegion);
    const lastBiKeyRef = useRef<string>("");
    const aiRequestedRef = useRef<boolean>(false);

    const fetchBiSeries = useCallback(
        async (startDay?: number | null, endDay?: number | null) => {
            if (isMonthYearNA) {
                setBiError(null);
                setBiLoading(false);
                setBiStatus("ready");
                setBiDailySeries(null);
                setBiPeriods(null);
                setBiAlignedTotals(null);
                setLiveBiPayload(null);
                return;
            }

            if (!showLiveBI) return;

            const normalized = (biCountryName || "").toLowerCase();
            if (!normalized || normalized === "global") return;

            setBiError(null);
            setBiLoading(true);
            setBiStatus("loading");

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

                const rangeActive = startDay != null && endDay != null;
                if (rangeActive) {
                    params.set("start_day", String(startDay));
                    params.set("end_day", String(endDay));
                }

                let attempts = 0;
                const maxAttempts = 10;

                while (attempts < maxAttempts) {
                    const res = await fetch(`${LIVE_MTD_BI_ENDPOINT}?${params.toString()}`, {
                        headers: token ? { Authorization: `Bearer ${token}` } : undefined,
                    });

                    if (res.status === 202) {
                        setBiStatus("processing");
                        attempts += 1;
                        await sleep(3000);
                        continue;
                    }

                    if (!res.ok) {
                        throw new Error(`BI failed: ${res.status}`);
                    }

                    const json: BiApiResponse = await res.json();

                    setLiveBiPayload(json);
                    setBiPeriods(json?.periods || null);
                    setBiDailySeries(json?.daily_series || null);

                    const alignedFromNested = json?.aligned_totals;
                    const alignedFromTopLevel: BiAlignedTotals = {
                        total_current_advertising: (json as any)?.total_current_advertising,
                        total_previous_advertising: (json as any)?.total_previous_advertising,
                        total_current_net_sales: (json as any)?.total_current_net_sales,
                        total_previous_net_sales: (json as any)?.total_previous_net_sales,
                        total_previous_net_sales_full_month:
                            (json as any)?.total_previous_net_sales_full_month,
                        total_current_platform_fees: (json as any)?.total_current_platform_fees,
                        total_previous_platform_fees: (json as any)?.total_previous_platform_fees,
                        total_current_profit: (json as any)?.total_current_profit,
                        total_previous_profit: (json as any)?.total_previous_profit,
                        total_current_rembursement_fee: (json as any)?.total_current_rembursement_fee,
                        total_previous_rembursement_fee: (json as any)?.total_previous_rembursement_fee,
                    };

                    setBiAlignedTotals(alignedFromNested ?? alignedFromTopLevel ?? null);
                    setBiStatus("ready");
                    return;
                }

                throw new Error("Live BI is still processing. Max retry limit reached.");
            } catch (e: any) {
                setBiPeriods(null);
                setBiDailySeries(null);
                setBiAlignedTotals(null);
                setBiStatus("error");
                setBiError(e?.message || "Failed to load BI series");
            } finally {
                setBiLoading(false);
            }
        },
        [showLiveBI, biCountryName, currMonthName, currYear, isMonthYearNA]
    );

    const fetchLiveBiPayload = useCallback(
        async ({
            startDay = selectedStartDay,
            endDay = selectedEndDay,
            generateInsights = false,
        }: FetchLiveBiPayloadArgs = {}) => {
            setSummaryLoading(true);
            aiRequestedRef.current = !!generateInsights;
            await fetchBiSeries(startDay, endDay);
        },
        [fetchBiSeries, selectedStartDay, selectedEndDay]
    );

    const runDashboardLoadWithSteps = useCallback(async () => {
        if (isMonthYearNA) {
            resetStepState();
            return;
        }

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

            // STEP 2: MTD Fetching
            setStep(1, "MTD Fetching", 5, "Preparing MTD fetch...");

            const jwtToken =
                typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

            const country =
                platform === "amazon-us"
                    ? "US"
                    : platform === "amazon-ca"
                        ? "CA"
                        : "UK";

            if (platform !== "shopify" && jwtToken) {
                setAdsLoading(true);
                setAdsSeeded(false);

                setStep(1, "MTD Fetching", 10);
                // await ensureSpReportSeedOncePerDay(baseURL, jwtToken, country);

                // if (country === "UK" || country === "US") {
                //     setStep(1, "MTD Fetching", 18);
                //     await ensureSdReportSeedOncePerDay(baseURL, jwtToken, country);
                // }

                setStep(1, "MTD Fetching", 26);
                // await ensureSbKeywordReportSeedOncePerDay(baseURL, jwtToken, country);

                setAdsSeeded(true);
                setAdsSeedError(null);
                setAdsLoading(false);

                setStep(1, "MTD Fetching", 38, "Fetching Monthly Ads data...");
                const { monthName, year } = getRegionYearMonth(activeDateRegion);
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

                setStep(1, "MTD Fetching", 48, "Fetching Monthly Ads summary...");
                await fetchMonthlySp();
            } else {
                setStep(1, "MTD Fetching", 48, "Skipping ads fetch for Shopify-only mode...");
            }

            setStep(1, "MTD Fetching", 62);
            await fetchAmazon();

            if (showLiveBI) {
                setStep(1, "MTD Fetching", 78);
                await fetchBiSeries(selectedStartDay, selectedEndDay);
            } else {
                setStep(1, "MTD Fetching", 78, "Live BI not enabled, skipping...");
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

            // STEP 3: Inventory Fetch
            setStep(2, "Inventory Fetch", 20);
            await fetchInventory();
            setStep(2, "Inventory Fetch", 100, "Inventory ready");
            markStepComplete(2);

            // STEP 4: Plotting Graph
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
        } finally {
            setAdsLoading(false);
        }
    }, [
        isMonthYearNA,
        platform,
        baseURL,
        activeDateRegion,
        showLiveBI,
        selectedStartDay,
        selectedEndDay,
        shopifyStore,
        fetchFxRates,
        fetchMonthlySp,
        fetchAmazon,
        fetchBiSeries,
        fetchShopify,
        fetchShopifyPrev,
        fetchInventory,
    ]);

    const liveDashboardCountry = useMemo(() => {
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

    const saveDashboardCacheToBackend = useCallback(
        async (payload: DashboardCachePayload): Promise<number | null> => {
            if (typeof window === "undefined") return null;

            try {
                const token = localStorage.getItem("jwtToken");
                if (!token) return null;

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

                if (json?.data?.updated_at) {
                    const ts = new Date(json.data.updated_at).getTime();
                    if (!Number.isNaN(ts)) {
                        return ts;
                    }
                }

                return null;
            } catch (err) {
                console.error(err);
                return null;
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

    const buildDashboardCachePayload = useCallback(() => {
        return {
            data,
            biDailySeries,
            biPeriods,
            liveBiPayload,
            biAlignedTotals,
            invRows,
            inventoryAlerts,
            monthlySpRows,
            monthlySpTotalSpend,
            liveBiReady,
            biStatus,
            savedAt: Date.now(),
        };
    }, [
        data,
        biDailySeries,
        biPeriods,
        liveBiPayload,
        biAlignedTotals,
        invRows,
        inventoryAlerts,
        monthlySpRows,
        monthlySpTotalSpend,
        liveBiReady,
        biStatus,
    ]);

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
        });

        const json = await res.json().catch(() => null);

        if (!res.ok || !json?.success) {
            throw new Error(json?.error || `Failed to fetch dashboard cache (${res.status})`);
        }

        if (json?.found === false) {
            return { found: false, payload: null, updatedAt: null };
        }

        if (!json?.data?.payload) {
            return { found: false, payload: null, updatedAt: null };
        }

        return {
            found: true,
            payload: json.data.payload,
            updatedAt: json.data.updated_at ?? null,
        };
    }, [
        liveDashboardCountry,
        platform,
        activeDateRegion,
        selectedStartDay,
        selectedEndDay,
    ]);

    const liveCacheKey = useMemo(() => {
        return `live-dashboard-cache:${platform}:${activeDateRegion}:${selectedStartDay ?? "na"}:${selectedEndDay ?? "na"}`;
    }, [platform, activeDateRegion, selectedStartDay, selectedEndDay]);


    // const saveDashboardCacheToBackend = useCallback(async (cachePayload?: any) => {
    //     if (typeof window === "undefined") return;

    //     const token = localStorage.getItem("jwtToken");
    //     if (!token) return;

    //     const payloadToSave = cachePayload ?? buildDashboardCachePayload();

    //     const res = await fetch(LIVE_DASHBOARD_CACHE_ENDPOINT, {
    //         method: "POST",
    //         headers: {
    //             "Content-Type": "application/json",
    //             Accept: "application/json",
    //             Authorization: `Bearer ${token}`,
    //         },
    //         body: JSON.stringify({
    //             country: liveDashboardCountry,
    //             platform: String(platform || "").toLowerCase(),
    //             region: String(activeDateRegion || ""),
    //             startDay: selectedStartDay,
    //             endDay: selectedEndDay,
    //             savedAt: Date.now(),
    //             cachePayload: payloadToSave,
    //         }),
    //     });

    //     const json = await res.json().catch(() => null);

    //     if (!res.ok || !json?.success) {
    //         throw new Error(json?.error || `Failed to save dashboard cache (${res.status})`);
    //     }
    // }, [
    //     buildDashboardCachePayload,
    //     liveDashboardCountry,
    //     platform,
    //     activeDateRegion,
    //     selectedStartDay,
    //     selectedEndDay,
    // ]);

    // const saveDashboardCacheToBackend = useCallback(async (cachePayload?: any) => {
    //     if (typeof window === "undefined") return;

    //     const token = localStorage.getItem("jwtToken");
    //     if (!token) return;

    //     const payloadToSave = cachePayload ?? buildDashboardCachePayload();

    //     // keep browser cache in sync too
    //     localStorage.setItem(
    //         liveCacheKey,
    //         JSON.stringify({
    //             ...payloadToSave,
    //             savedAt: Date.now(),
    //         })
    //     );

    //     const res = await fetch(LIVE_DASHBOARD_CACHE_ENDPOINT, {
    //         method: "POST",
    //         headers: {
    //             "Content-Type": "application/json",
    //             Accept: "application/json",
    //             Authorization: `Bearer ${token}`,
    //         },
    //         body: JSON.stringify({
    //             country: liveDashboardCountry,
    //             platform: String(platform || "").toLowerCase(),
    //             region: String(activeDateRegion || ""),
    //             startDay: selectedStartDay,
    //             endDay: selectedEndDay,
    //             savedAt: Date.now(),
    //             cachePayload: payloadToSave,
    //         }),
    //     });

    //     const json = await res.json().catch(() => null);

    //     if (!res.ok || !json?.success) {
    //         throw new Error(json?.error || `Failed to save dashboard cache (${res.status})`);
    //     }

    //     if (json?.data?.updated_at) {
    //         const ts = new Date(json.data.updated_at).getTime();
    //         if (!Number.isNaN(ts)) {
    //             setDbUpdatedAt(ts);
    //         }
    //     }
    // }, [
    //     buildDashboardCachePayload,
    //     liveCacheKey,
    //     liveDashboardCountry,
    //     platform,
    //     activeDateRegion,
    //     selectedStartDay,
    //     selectedEndDay,
    // ]);


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

    const restoreLiveCacheFromLocalStorage = useCallback(() => {
        if (typeof window === "undefined") return false;

        const raw = localStorage.getItem(liveCacheKey);
        if (!raw) return false;

        try {
            const parsed = JSON.parse(raw);
            applyDashboardCachePayload(parsed);
            return true;
        } catch (err) {
            console.error("Failed to restore live cache from localStorage:", err);
            return false;
        }
    }, [liveCacheKey, applyDashboardCachePayload]);

    // const restoreLiveCache = useCallback(() => {
    //     if (typeof window === "undefined") return false;

    //     const raw = localStorage.getItem(liveCacheKey);
    //     if (!raw) return false;

    //     try {
    //         const parsed = JSON.parse(raw);

    //         setData(parsed.data ?? null);
    //         setBiDailySeries(parsed.biDailySeries ?? null);
    //         setBiPeriods(parsed.biPeriods ?? null);
    //         setLiveBiPayload(parsed.liveBiPayload ?? null);
    //         setBiAlignedTotals(parsed.biAlignedTotals ?? null);
    //         setInvRows(parsed.invRows ?? []);
    //         setInventoryAlerts(parsed.inventoryAlerts ?? {});
    //         setMonthlySpRows(parsed.monthlySpRows ?? []);
    //         setMonthlySpTotalSpend(parsed.monthlySpTotalSpend ?? null);
    //         setLiveBiReady(!!parsed.liveBiReady);

    //         setBiStatus(parsed.biStatus ?? (parsed.biDailySeries ? "ready" : "idle"));
    //         setBiLoading(false);
    //         setBiError(null);

    //         return true;
    //     } catch {
    //         return false;
    //     }
    // }, [liveCacheKey]);

    // const saveLiveCache = useCallback(() => {
    //     if (typeof window === "undefined") return;

    //     localStorage.setItem(
    //         liveCacheKey,
    //         JSON.stringify({
    //             data,
    //             biDailySeries,
    //             biPeriods,
    //             liveBiPayload,
    //             biAlignedTotals,
    //             invRows,
    //             inventoryAlerts,
    //             monthlySpRows,
    //             monthlySpTotalSpend,
    //             liveBiReady,
    //             biStatus,
    //             savedAt: Date.now(),
    //         })
    //     );
    // }, [
    //     liveCacheKey,
    //     data,
    //     biDailySeries,
    //     biPeriods,
    //     liveBiPayload,
    //     biAlignedTotals,
    //     invRows,
    //     inventoryAlerts,
    //     monthlySpRows,
    //     monthlySpTotalSpend,
    //     liveBiReady,
    // ]);


    const [lastRefreshAt, setLastRefreshAt] = useState<number | null>(null);
    const [dbUpdatedAt, setDbUpdatedAt] = useState<number | null>(null);
    const [refreshNow, setRefreshNow] = useState(Date.now());
    const [loadingStartedAt, setLoadingStartedAt] = useState<number | null>(null);


    const STEP_ESTIMATED_SECONDS: Record<number, number> = {
        1: 60,
        2: 60,
        3: 60,
    };

    useEffect(() => {
        if (typeof window === "undefined") return;

        const saved = localStorage.getItem(LAST_REFRESH_KEY);
        if (saved) {
            const ts = Number(saved);
            if (!Number.isNaN(ts)) {
                setLastRefreshAt(ts);
            }
        }
    }, []);

    useEffect(() => {
        const interval = setInterval(() => {
            setRefreshNow(Date.now());
        }, 30000);

        return () => clearInterval(interval);
    }, []);

    const getRelativeRefreshText = useCallback((ts: number | null) => {
        if (!ts) return "Never refreshed";

        const diffMs = refreshNow - ts;
        const diffSec = Math.floor(diffMs / 1000);
        const diffMin = Math.floor(diffSec / 60);
        const diffHr = Math.floor(diffMin / 60);
        const diffDay = Math.floor(diffHr / 24);

        if (diffSec < 60) return "just now";
        if (diffMin < 60) return `${diffMin} min ago`;
        if (diffHr < 24) return `${diffHr} hr ago`;
        return `${diffDay} day ago`;
    }, [refreshNow]);

    useEffect(() => {
        if (!lastRefreshAt) return;

        const interval = setInterval(() => {
            setLastRefreshAt((prev) => (prev ? prev : null));
        }, 30000);

        return () => clearInterval(interval);
    }, [lastRefreshAt]);

    const isManualRefreshRef = useRef(false);

    const shouldPostCacheRef = useRef(false);

    const handleHardRefresh = useCallback(async () => {
        if (typeof window === "undefined") return;

        resetStepState();
        isManualRefreshRef.current = true;
        shouldPostCacheRef.current = true;

        try {
            await runDashboardLoadWithSteps(); // fetch everything and update state
            // persistence effect will automatically:
            // 1. write localStorage
            // 2. POST to backend
        } catch (err) {
            console.error("Hard refresh failed:", err);
            isManualRefreshRef.current = false;
            shouldPostCacheRef.current = false;
        }
    }, [runDashboardLoadWithSteps]);


    useEffect(() => {
        let cancelled = false;

        const bootstrapDashboard = async () => {
            try {
                const cacheResult = await getDashboardCacheFromBackend();

                if (cancelled) return;

                if (cacheResult?.found && cacheResult.payload) {
                    shouldPostCacheRef.current = false;
                    isManualRefreshRef.current = false;

                    applyDashboardCachePayload(cacheResult.payload);

                    // keep localStorage synced from DB too
                    localStorage.setItem(
                        liveCacheKey,
                        JSON.stringify({
                            ...cacheResult.payload,
                            savedAt: Date.now(),
                        })
                    );

                    const backendUpdatedAt = cacheResult.updatedAt
                        ? new Date(cacheResult.updatedAt).getTime()
                        : null;

                    setDbUpdatedAt(
                        backendUpdatedAt != null && !Number.isNaN(backendUpdatedAt)
                            ? backendUpdatedAt
                            : null
                    );

                    return;
                }

                // fallback: try browser cache before refetching
                const restoredFromLocal = restoreLiveCacheFromLocalStorage();
                if (restoredFromLocal) {
                    shouldPostCacheRef.current = false;
                    isManualRefreshRef.current = false;
                    return;
                }

                shouldPostCacheRef.current = true;
                isManualRefreshRef.current = true;
                await runDashboardLoadWithSteps();

            } catch (err) {
                console.error("Dashboard bootstrap failed:", err);

                if (cancelled) return;

                shouldPostCacheRef.current = true;
                isManualRefreshRef.current = true;
                await runDashboardLoadWithSteps();
            }
        };

        bootstrapDashboard();

        return () => {
            cancelled = true;
        };
    }, [liveCacheKey, restoreLiveCacheFromLocalStorage, getDashboardCacheFromBackend, applyDashboardCachePayload, runDashboardLoadWithSteps]);

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
            // 1) always store browser cache first
            localStorage.setItem(
                liveCacheKey,
                JSON.stringify({
                    ...payload,
                    savedAt: Date.now(),
                })
            );

            // optional init marker if you still want it
            localStorage.setItem("live-dashboard-cache-init", "initialized");
        } catch (e) {
            console.error("Failed to write local cache:", e);
        }

        // saveDashboardCacheToBackend(payload)
        //     .then(() => {
        //         const now = Date.now();

        //         setDbUpdatedAt(now);
        //         setLastRefreshAt(now);
        //         localStorage.setItem(LAST_REFRESH_KEY, String(now));

        //         shouldPostCacheRef.current = false;
        //         isManualRefreshRef.current = false;
        //     })
        //     .catch((err) => {
        //         console.error("Failed to persist dashboard cache:", err);
        //         shouldPostCacheRef.current = false;
        //         isManualRefreshRef.current = false;
        //     });
        saveDashboardCacheToBackend(payload)
            .then((serverUpdatedAt) => {
                if (serverUpdatedAt != null) {
                    setDbUpdatedAt(serverUpdatedAt);
                }

                shouldPostCacheRef.current = false;
                isManualRefreshRef.current = false;
            })
            .catch((err) => {
                console.error("Failed to persist dashboard cache:", err);
                shouldPostCacheRef.current = false;
                isManualRefreshRef.current = false;
            });
    }, [
        buildDashboardCachePayload,
        saveDashboardCacheToBackend,
        liveCacheKey,
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
    ]);

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

    const safeDeltaPct = (current: number, previous: number) => {
        const c = Number(current) || 0;
        const p = Number(previous) || 0;

        if (p === 0) return null; // avoid wrong spikes
        return ((c - p) / Math.abs(p)) * 100;
    };

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
        const pts = biDailySeriesHome?.current_mtd || [];
        if (!pts.length) return;

        const todayDay = getDayOfMonthByRegion(activeDateRegion);
        const exact = pts.find((p) => Number(p.date?.slice(8, 10)) === todayDay);
        if (exact?.net_sales != null) {
            setTodaySalesRaw(Number(exact.net_sales) || 0);
            return;
        }

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
                profitPct: safeDeltaPct(currProfitPct, prevProfitPct),

            },
        };
    }, [biDailySeriesHome, selectedStartDay, selectedEndDay]);

    const rangeActive = selectedStartDay != null && selectedEndDay != null;
    const useBiCm2 = showLiveBI && rangeActive;
    const cm2Ready = useBiCm2 && !biLoading && !!biAlignedTotals;
    const biCardsReady = rangeActive && !biLoading && !!biAlignedTotals;

    const biAlignedTotalsHome = useMemo(() => {
        if (!biCardsReady || !biAlignedTotals) return null;
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
        const userMonthlyTargetGBP = toNumberSafe(userData?.target_sales ?? 0);
        const userMonthlyTargetRaw = toNumberSafe(userData?.target_sales ?? 0);

        const userMonthlyTargetForRegion =
            userMonthlyTargetRaw > 0
                ? platform === "amazon-us"
                    ? userMonthlyTargetRaw
                    : convertToDisplayCurrency(userMonthlyTargetRaw, "GBP")
                : 0;

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
        userData?.target_sales,
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
        const items = (data as any)?.skuwise_items ?? [];
        if (!Array.isArray(items)) return [];

        const body = items.filter((r: any) => r?.sku && r.sku !== "GRAND_TOTAL");
        const total = items.find((r: any) => r?.sku === "GRAND_TOTAL");

        const mapRow = (r: any, idx?: number, isTotal = false): MonthlySkuwiseRow => ({
            sno: isTotal ? undefined : (idx ?? 0) + 1,
            sku: String(r.sku ?? ""),
            product_name: String(r.product_name ?? ""),
            ad_type: String(r.ad_type ?? r.adType ?? r.ad_types ?? r.adTypes ?? ""),
            quantity: Number(r.quantity ?? 0),
            asp: Number(r.asp ?? 0),
            net_sales: Number(r.net_sales ?? 0),
            cogs: Number(r.cogs ?? 0),
            fba_fees: Number(r.fba_fees ?? 0),
            selling_fees: Number(r.selling_fees ?? 0),
            ads_spend: Number(r.ads_spend ?? 0),
            acos: Number(r.acos ?? 0),
            cm2_profit: Number(r.cm2_profit ?? 0),
            tax: Number(r.tax ?? 0),
            credits: Number(r.credits ?? 0),
            tax_and_credits: Number(r.tax_and_credits ?? 0),
            cm1_profit_per: Number(r.cm1_profit_per ?? 0),
            cm1_profit_per_unit: Number(r.cm1_profit_per_unit ?? 0),
            cm2_profit_per: Number(r.cm2_profit_per ?? 0),
            cm2_profit_per_unit: Number(r.cm2_profit_per_unit ?? 0),
            profit: Number(r.profit ?? 0),

            platform_fee: Number(r.platform_fee ?? 0),
            platform_fee_inventory_storage: Number(r.platform_fee_inventory_storage ?? 0),
            lost_total: Number(r.lost_total ?? 0),
            other: Number(r.other ?? 0),

            product_spend: Number(r.product_spend ?? 0),
            brand_spend: Number(r.brand_spend ?? 0),
            dealsvouchar_ads: Number(r.dealsvouchar_ads ?? 0),
            platformfeenew: Number(r.platformfeenew ?? 0),

            isTotal,
        });

        const mapped = body.map((r: any, idx: number) => mapRow(r, idx, false));
        if (total) mapped.push(mapRow(total, undefined, true));
        return mapped;
    }, [data]);


    const PRODUCTWISE_MONEY_KEYS: ProductwiseMoneyKey[] = [
        "asp",
        "net_sales",
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
        "brand_spend",
        "dealsvouchar_ads",
        "platformfeenew",
    ];


    const convertProductwiseRowToDisplay = useCallback(
        (row: MonthlySkuwiseRow): MonthlySkuwiseRow => {
            if (platform !== "global") return row;

            const next: MonthlySkuwiseRow = { ...row };

            PRODUCTWISE_MONEY_KEYS.forEach((key) => {
                next[key] = convertToDisplayCurrency(
                    Number(row[key] ?? 0),
                    amazonDataCurrency
                );
            });

            return next;
        },
        [platform, convertToDisplayCurrency, amazonDataCurrency]
    );


    const monthlySkuwiseRowsDisplay = useMemo<MonthlySkuwiseRow[]>(() => {
        return (monthlySkuwiseRows || []).map(convertProductwiseRowToDisplay);
    }, [monthlySkuwiseRows, convertProductwiseRowToDisplay]);

    const grandTotalRowDisplay = useMemo(() => {
        return monthlySkuwiseRowsDisplay.find(
            (item: any) =>
                item.product_name === "Grand Total" || item.sku === "GRAND_TOTAL"
        );
    }, [monthlySkuwiseRowsDisplay]);

    const marketplaceFeesFromTable = useMemo(() => {
        const fba = Number(grandTotalRowDisplay?.fba_fees ?? 0);
        const selling = Number(grandTotalRowDisplay?.selling_fees ?? 0);
        return Math.abs(fba + selling);
    }, [grandTotalRowDisplay]);

    const grandTotalRowRaw = useMemo(() => {
        return monthlySkuwiseRows.find(
            (item: any) =>
                item.product_name === "Grand Total" || item.sku === "GRAND_TOTAL"
        );
    }, [monthlySkuwiseRows]);

    // 👇 ADD THIS HERE
    const amazonPl = () => {
        const sourceCurrency = amazonDataCurrency;

        const sales = convertToDisplayCurrency(
            toNumberSafe(derived?.net_sales ?? 0),
            sourceCurrency
        );

        const cogs = convertToDisplayCurrency(
            toNumberSafe(totals?.cogs ?? grandTotalRowRaw?.cogs ?? 0),
            sourceCurrency
        );

        const fees = convertToDisplayCurrency(
            Math.abs(
                toNumberSafe(grandTotalRowRaw?.fba_fees ?? totals?.fba_fees ?? 0) +
                toNumberSafe(grandTotalRowRaw?.selling_fees ?? totals?.selling_fees ?? 0)
            ),
            sourceCurrency
        );

        const taxCredits = convertToDisplayCurrency(
            toNumberSafe(totals?.tax_and_credits ?? grandTotalRowRaw?.tax_and_credits ?? 0),
            sourceCurrency
        );

        const cm1 = convertToDisplayCurrency(
            toNumberSafe(derived?.profit ?? totals?.profit ?? grandTotalRowRaw?.profit ?? 0),
            sourceCurrency
        );

        const adv = convertToDisplayCurrency(
            toNumberSafe(derived?.advertising_fees ?? 0),
            sourceCurrency
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
            toNumberSafe(derived?.cm2_profit ?? grandTotalRowRaw?.cm2_profit ?? 0),
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

        const totalRow =
            monthlySkuwiseRowsDisplay.find((r) => r.isTotal) ??
            monthlySkuwiseRowsDisplay.find((r) => r.sku === "GRAND_TOTAL") ??
            null;

        const bodyRows = monthlySkuwiseRowsDisplay.filter(
            (r) => !r.isTotal && r.sku !== "GRAND_TOTAL"
        );

        if (bodyRows.length <= 9) {
            const out = [...bodyRows];
            if (totalRow) out.push(totalRow);
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
            tax: sum("tax"),
            credits: sum("credits"),
            tax_and_credits: sum("tax_and_credits"),
            cm1_profit_per: 0,
            cm1_profit_per_unit: 0,
            cm2_profit_per: 0,
            cm2_profit_per_unit: 0,
            ads_spend: sum("ads_spend"),
            acos: othersNetSales ? (Math.abs(sum("ads_spend")) / Math.abs(othersNetSales)) * 100 : 0,
            cm2_profit: sum("cm2_profit"),
            profit: sum("profit"),
            platform_fee: sum("platform_fee"),
            platform_fee_inventory_storage: sum("platform_fee_inventory_storage"),
            lost_total: sum("lost_total"),
            other: sum("other"),
            isOthers: true,
        };

        const out: MonthlySkuwiseRow[] = [...top9, othersRow];
        if (totalRow) out.push(totalRow);
        return out;
    }, [monthlySkuwiseRowsDisplay]);


    const plSummaryTotals = useMemo<PlSummaryTotals>(() => {
        if (platform === "global") {
            return computePlSummaryTotals(null, monthlySkuwiseRowsDisplay);
        }
        return computePlSummaryTotals(data, monthlySkuwiseRowsDisplay);
    }, [platform, data, monthlySkuwiseRowsDisplay]);

    const SKUWISE_LEFT_COLS = [
        { key: "sno", label: "S.No", align: "center" as const },
        { key: "product_name", label: "Product Name", align: "left" as const },
    ];

    const SKUWISE_GROUPS = [
        {
            id: "marketplace_fees",
            label: "Marketplace Fees",
            info: <InfoTip text={TERM_DEFINITIONS.marketplace_fees} />,
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
                    width: 150,
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
            id: "cm2_profit",
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
                { key: "cm2_profit_per_unit", label: "Per Unit", align: "center" as const },
                { key: "cm2_profit_per", label: "%", align: "center" as const },
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
            label: "ASP",
            info: <InfoTip text={TERM_DEFINITIONS.asp} />,
            align: "center" as const
        },
        {
            key: "net_sales",
            label: "Net Sales",
            info: <InfoTip text={TERM_DEFINITIONS.net_sales} />,
            align: "center" as const
        },
        { key: "cogs", label: "COGS", align: "center" as const },
        { key: "profit", label: "CM1 Profit", align: "center" as const },
        { key: "ads_spend", label: "Ads Spend", align: "center" as const },
        { key: "acos", label: "ACoS %", align: "center" as const },
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

    const formattedMonthYear = getFormattedMonthYearByRegion(activeDateRegion);

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



    // const tacosPctForSummary = useMemo(() => {
    //     if (globalUseBi) {
    //         if (!globalCm2Ready) return 0;
    //         const ads = biAlignedTotals?.total_current_advertising ?? 0;
    //         const sales = biAlignedTotals?.total_current_net_sales ?? 0;
    //         return sales > 0 ? (ads / sales) * 100 : 0;
    //     }

    //     return globalCurrRoasPct;
    // }, [globalUseBi, globalCm2Ready, biAlignedTotals, globalCurrRoasPct]);

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

    const grandTotalRow = data?.skuwise_items?.find(
        (item: any) =>
            item.product_name === "Grand Total" ||
            item.sku === "GRAND_TOTAL"
    );

    const row = grandTotalRowRaw;

    const rawAdsSpend = toNumber(row?.ads_spend ?? 0);
    const rawBrandSpend = toNumber(row?.brand_spend ?? 0);
    const rawDealVouchers = toNumber(row?.dealsvouchar_ads ?? 0);
    const rawPlatformFee = toNumber(row?.platform_fee ?? 0);
    const rawProfit = toNumber(row?.profit ?? 0);

    const rawCostOfAds = Math.abs(rawBrandSpend - rawDealVouchers);
    const rawAdsSpendTotal = Math.abs(rawAdsSpend + rawCostOfAds);
    const rawCm2Profit = rawProfit - rawAdsSpendTotal - Math.abs(rawPlatformFee);

    console.log("rawAdsSpendTotal", rawAdsSpendTotal);


    const globalBottomCards = useMemo(() => {


        const currentCostOfAds =
            platform === "global"
                ? convertToDisplayCurrency(rawAdsSpendTotal, amazonDataCurrency)
                : rawAdsSpendTotal;

        const currentCm2Profit =
            platform === "global"
                ? convertToDisplayCurrency(rawCm2Profit, amazonDataCurrency)
                : rawCm2Profit;

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
        grandTotalRowRaw,
        platform,
        amazonDataCurrency,
        convertToDisplayCurrency,
        globalCurrNetSalesDisp,
        uk.netSalesGBP,
    ]);

    // const tacosPctForSummary = useMemo(() => {
    //     if (globalUseBi) {
    //         if (!globalCm2Ready) return 0;
    //         const ads = rawAdsSpendTotal;
    //         const sales = biAlignedTotals?.total_current_net_sales ?? 0;
    //         console.log("tacos",ads, sales)
    //         return sales > 0 ? (ads / sales) * 100 : 0;
    //     }

    //     return globalCurrRoasPct;
    // }, [globalUseBi, globalCm2Ready, biAlignedTotals, globalCurrRoasPct]);

    const tacosPctForSummary = useMemo(() => {
        const salesBase =
            platform === "global"
                ? globalCurrNetSalesDisp
                : convertToDisplayCurrency(uk.netSalesGBP ?? 0, "GBP");

        const adsBase =
            platform === "global"
                ? convertToDisplayCurrency(rawAdsSpendTotal, amazonDataCurrency)
                : rawAdsSpendTotal;

        return salesBase > 0 ? (adsBase / salesBase) * 100 : 0;
    }, [
        platform,
        rawAdsSpendTotal,
        globalCurrNetSalesDisp,
        uk.netSalesGBP,
        convertToDisplayCurrency,
        amazonDataCurrency,
    ]);

    console.log(tacosPctForSummary)

    const ads_spend = grandTotalRowDisplay?.ads_spend ?? 0;
    const sponsoredProductsSpend = grandTotalRowDisplay?.product_spend ?? 0;
    const sponsoredBrandSpend = grandTotalRowDisplay?.brand_spend ?? 0;
    const inventoryStorageFees = grandTotalRowDisplay?.platform_fee_inventory_storage ?? 0;
    const lost_inventory_total = grandTotalRowDisplay?.lost_total ?? 0;
    const otherPlatformFee = grandTotalRowDisplay?.platformfeenew ?? 0;
    const platformFee = grandTotalRowDisplay?.platform_fee ?? 0;
    const dealVouchers = grandTotalRowDisplay?.dealsvouchar_ads ?? 0;

    const costOfAds = Math.abs(toNumber(sponsoredBrandSpend - dealVouchers));
    const adsSpendTotal = Math.abs(toNumber(ads_spend + costOfAds));
    const cm2Profit =
        toNumber(grandTotalRowDisplay?.profit) -
        adsSpendTotal -
        Math.abs(toNumber(grandTotalRowDisplay?.platform_fee));

    console.log("cm2Profit", cm2Profit);

    const reimbursementForSummary = useMemo(() => {
        return toNumber(reimbursementHome?.current);
    }, [reimbursementHome?.current]);

    const cm2MarginPctForSummary = useMemo(() => {
        const cm2 = cm2Profit;
        const netSales = toNumber(plSummaryTotals.net_sales);
        return netSales ? (cm2 / netSales) * 100 : 0;
    }, [plSummaryTotals.cm2_profit, plSummaryTotals.net_sales]);


    const reimbursementVsCm2PctForSummary = useMemo(() => {
        const cm2 = cm2Profit;
        return cm2 ? (reimbursementForSummary / cm2) * 100 : 0;
    }, [reimbursementForSummary, plSummaryTotals.cm2_profit]);

    const reimbursementVsSalesPctForSummary = useMemo(() => {
        const netSales = toNumber(plSummaryTotals.net_sales) || toNumber(stats_mtdHome);
        return netSales ? (reimbursementForSummary / netSales) * 100 : 0;
    }, [reimbursementForSummary, plSummaryTotals.net_sales, stats_mtdHome]);

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

    // const todayIST = getDayOfMonthIST();        // D
    // const daysInMonthIST = getDaysInMonthIST(); // N

    const todayByRegion = getDayOfMonthByRegion(activeDateRegion);
    const daysInMonthByRegion = getDaysInMonthByRegion(activeDateRegion);

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
            const rows = (monthlySkuwiseRowsDisplay || []).filter((r) => {
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

            const dataRows = monthlySkuwiseRows.map((r) => ({
                "S.No": r.isTotal ? "" : r.sno ?? "",
                "SKU": r.isOthers || r.isTotal ? "-" : r.sku || "-",
                "Product Name": r.isTotal ? "Total" : r.isOthers ? "Others" : r.product_name,
                "Ad Type": r.isOthers || r.isTotal ? "-" : (r.ad_type || "-"),
                "Net Units Sold": Number(r.quantity || 0),
                "ASP": Number(r.asp || 0),
                "Net Sales": Number(r.net_sales || 0),
                "COGS": Number(r.cogs || 0),
                "Selling Fees": Number(r.selling_fees || 0),
                "FBA Fees": Number(r.fba_fees || 0),
                "Ads Spend": Number(r.ads_spend || 0),

                // add this
                "ACOS %": Number(r.acos || 0),

                "Tax": Number(r.tax || 0),
                "Credits": Number(r.credits || 0),
                "Tax & Credits": Number(r.tax_and_credits || 0),
                "CM1 Profit": Number(r.profit || 0),
                "CM1 Profit %": Number(r.cm1_profit_per || 0),
                "CM1 Profit Per Unit": Number(r.cm1_profit_per_unit || 0),
                "CM2 Profit": Number(r.cm2_profit || 0),
                "CM2 Profit %": Number(r.cm2_profit_per || 0),
                "CM2 Profit Per Unit": Number(r.cm2_profit_per_unit || 0),
            }));

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

                { label: "Cost of Advertisement", value: "", bold: true },
                { label: "Visibility - Ads (-)", value: "", indent: 1 },
                { label: "Visibility - Deals, Vouchers and Reviews (-)", value: "", indent: 1 },
                { label: "Other Transactions", value: "", bold: true },
                { label: "Other Platform Fees (-)", value: "", indent: 1 },
                { label: "Inventory Storage Fees (-)", value: Number((plSummaryTotals as any)?.platform_fee_inventory_storage ?? 0), indent: 1 },
                { label: "Misc. Transactions (+)", value: "", indent: 1 },
                { label: "Reimbursement for lost Inventory (+)", value: "", indent: 1 },
                { label: "CM2 Profit/Loss", value: Number((plSummaryTotals as any)?.cm2_profit ?? 0), bold: true },
                { label: "CM2 Margins", value: Number(cm2MarginPctForSummary ?? 0), bold: true },
                { label: "TACoS (Total Advertising Cost of Sale)", value: Number(tacosPctForSummary ?? 0), bold: true },
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

    // const targets_todayHome = stats_todayHome;
    // const targets_mtdHome = targetKpisFromBi ? targetKpisFromBi.mtdHome : stats_mtdHome;
    // const targets_lastMonthTotalHome = targetKpisFromBi ? targetKpisFromBi.lastMonthTotalHome : stats_lastMonthTotalHome;
    // const targets_lastMonthToDateHome = targetKpisFromBi ? targetKpisFromBi.lastMonthToDateHome : stats_lastMtdHome;

    // const targets_reimbursement = targetKpisFromBi ? targetKpisFromBi.reimbursement : reimbursementHome;

    const targets_todayHome = stats_todayHome;

    const targets_mtdHome =
        targetKpisFromBi?.mtdHome || stats_mtdHome;

    const targets_lastMonthTotalHome =
        targetKpisFromBi?.lastMonthTotalHome || stats_lastMonthTotalHome;

    const targets_lastMonthToDateHome =
        targetKpisFromBi?.lastMonthToDateHome || stats_lastMtdHome;

    const targets_reimbursement =
        targetKpisFromBi?.reimbursement || reimbursementHome;


    const rangeCompletedPct = useMemo(() => {
        if (!selectedStartDay || !selectedEndDay) return 0;
        const daysInMonth = getDaysInMonthByRegion(activeDateRegion);
        const completedDays = selectedEndDay - selectedStartDay + 1;
        return (completedDays / daysInMonth) * 100;
    }, [selectedStartDay, selectedEndDay, activeDateRegion]);

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
        const connectCountry = countryName === "global" ? "uk" : countryName;
        router.push(`/profile/${connectCountry}/NA/NA`);
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

    const dummyBiDailySeriesHome: DailySeries = {
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

    const stickyKpiItems = [
        {
            label: "Units",
            current: shouldShowDummyUi
                ? dummyStatData.units.current
                : isStickyGlobal
                    ? (globalUseBi ? biCardKpis.curr.units : globalCurrUnits)
                    : (useBiForAmazonCards ? biCardKpis.curr.units : (totals?.quantity ?? 0)),
            previous: shouldShowDummyUi
                ? dummyStatData.units.previous
                : isStickyGlobal
                    ? (globalUseBi ? biCardKpis.prev.units : globalPrevUnits)
                    : (useBiForAmazonCards ? biCardKpis.prev.units : prev.quantity),
            deltaPct: shouldShowDummyUi
                ? dummyStatData.units.deltaPct
                : isStickyGlobal
                    ? (globalUseBi ? biCardKpis.deltas.units : globalDeltas.units)
                    : (useBiForAmazonCards ? biCardKpis.deltas.units : deltas.quantityPct),
            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || biLoading)
                    : (loading || biLoading)
            ),
            formatter: fmtInt,
            bottomLabel: prevLabel,
            className: "bg-white border-[#FDD36F] border-t-4 border-t-[#FDD36F]",
        },

        {
            label: "Net Sales",
            current: shouldShowDummyUi
                ? dummyStatData.netSales.current
                : isStickyGlobal
                    ? (globalUseBi ? biCardKpis.curr.netSales : globalCurrNetSalesDisp)
                    : (showLiveBI && rangeActive
                        ? biCardKpis.curr.netSales
                        : convertToDisplayCurrency(uk.netSalesGBP ?? 0, amazonDataCurrency)),
            previous: shouldShowDummyUi
                ? dummyStatData.netSales.previous
                : isStickyGlobal
                    ? (globalUseBi ? biCardKpis.prev.netSales : globalPrevNetSalesDisp)
                    : (showLiveBI && rangeActive
                        ? biCardKpis.prev.netSales
                        : convertToDisplayCurrency(prev.netSales ?? 0, amazonDataCurrency)),
            deltaPct: shouldShowDummyUi
                ? dummyStatData.netSales.deltaPct
                : isStickyGlobal
                    ? (globalUseBi
                        ? biCardKpis.deltas.netSales
                        : safeDeltaPct(globalCurrNetSalesDisp, globalPrevNetSalesDisp))
                    : (useBiForAmazonCards
                        ? biCardKpis.deltas.netSales
                        : deltas.netSalesPct),
            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || biLoading)
                    : (loading || biLoading)
            ),
            formatter: moneyPerUnitFormatter,
            previousFormatter: formatDisplayAmount,
            bottomLabel: prevLabel,
            className: "bg-white border-[#75BBDA] border-t-4 border-t-[#75BBDA]",
        },

        {
            label: "ASP",
            current: shouldShowDummyUi
                ? dummyStatData.asp.current
                : isStickyGlobal
                    ? (globalUseBi ? biCardKpis.curr.asp : globalCurrAspDisp)
                    : (showLiveBI && rangeActive
                        ? biCardKpis.curr.asp
                        : convertToDisplayCurrency(uk.aspGBP ?? 0, amazonDataCurrency)),
            previous: shouldShowDummyUi
                ? dummyStatData.asp.previous
                : isStickyGlobal
                    ? (globalUseBi ? biCardKpis.prev.asp : globalPrevAspDisp)
                    : (showLiveBI && rangeActive
                        ? biCardKpis.prev.asp
                        : convertToDisplayCurrency(prev.asp ?? 0, amazonDataCurrency)),
            deltaPct: shouldShowDummyUi
                ? dummyStatData.asp.deltaPct
                : isStickyGlobal
                    ? (globalUseBi
                        ? biCardKpis.deltas.asp
                        : safeDeltaPct(globalCurrAspDisp, globalPrevAspDisp))
                    : (useBiForAmazonCards
                        ? biCardKpis.deltas.asp
                        : deltas.aspPct),
            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || biLoading)
                    : (loading || biLoading)
            ),
            formatter: formatDisplayAmount,
            bottomLabel: prevLabel,
            className: "bg-white border-[#B75A5A] border-t-4 border-t-[#B75A5A]",
        },

        {
            label: "Cost of Ads",
            current: shouldShowDummyUi
                ? dummyStatData.costOfAds.current
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_current_advertising ?? 0,
                                biSourceCurrency
                            )
                            : 0)
                        : adsSpendTotal)
                    : (useBiForAmazonCards
                        ? (cm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_current_advertising ?? 0,
                                biSourceCurrency
                            )
                            : 0)
                        : adsSpendTotal),

            previous: shouldShowDummyUi
                ? dummyStatData.costOfAds.previous
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_previous_advertising ?? 0,
                                biSourceCurrency
                            )
                            : 0)
                        : amazonPrevAdsDisp)
                    : (useBiForAmazonCards
                        ? (cm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_previous_advertising ?? 0,
                                biSourceCurrency
                            )
                            : 0)
                        : amazonPrevAdsDisp),

            deltaPct: shouldShowDummyUi
                ? dummyStatData.costOfAds.deltaPct
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
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
                        : safeDeltaPct(
                            adsSpendTotal,
                            amazonPrevAdsDisp
                        ))
                    : (useBiForAmazonCards
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
                        : safeDeltaPct(
                            adsSpendTotal,
                            amazonPrevAdsDisp
                        )),
            inverseDelta: true,
            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || (globalUseBi ? biLoading : false))
                    : (loading || (useBiForAmazonCards ? biLoading : false))
            ),
            formatter: isStickyGlobal
                ? formatDisplayAmount
                : (v: any) => renderMoneyWithPerUnit(Number(v) || 0, unitsToUse, formatDisplayAmount),
            previousFormatter: formatDisplayAmount,
            bottomLabel: prevLabel,
            className: "bg-white border-[#C49466] border-t-4 border-t-[#C49466]",
        },
        {
            label: "TACoS",
            current: shouldShowDummyUi
                ? dummyStatData.tacos.current
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? (() => {
                                const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                        : (() => {
                            const sales = toNumber(plSummaryTotals.net_sales);
                            return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                        })())
                    : (useBiForAmazonCards
                        ? (cm2Ready
                            ? (() => {
                                const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                        : (() => {
                            const sales = toNumber(plSummaryTotals.net_sales);
                            return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                        })()),

            previous: shouldShowDummyUi
                ? dummyStatData.tacos.previous
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? (() => {
                                const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                        : amazonPrevRoasPct)
                    : (useBiForAmazonCards
                        ? (cm2Ready
                            ? (() => {
                                const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                return sales > 0 ? (ads / sales) * 100 : 0;
                            })()
                            : 0)
                        : amazonPrevRoasPct),

            deltaPct: shouldShowDummyUi
                ? dummyStatData.tacos.deltaPct
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? safeDeltaPct(
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
                        : safeDeltaPct(
                            (() => {
                                const sales = toNumber(plSummaryTotals.net_sales);
                                return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                            })(),
                            amazonPrevRoasPct
                        ))
                    : (useBiForAmazonCards
                        ? (cm2Ready
                            ? safeDeltaPct(
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
                        : safeDeltaPct(
                            (() => {
                                const sales = toNumber(plSummaryTotals.net_sales);
                                return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                            })(),
                            amazonPrevRoasPct
                        )),

            inverseDelta: true,
            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || (globalUseBi ? biLoading : false))
                    : (loading || (useBiForAmazonCards ? biLoading : false))
            ),
            formatter: fmtPct2,
            bottomLabel: prevLabel,
            className: "bg-white border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]",
        },

        {
            label: "CM2 Profit",
            current: shouldShowDummyUi
                ? dummyStatData.cm2Profit.current
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_current_profit_cm2 ?? 0,
                                biSourceCurrency
                            )
                            : 0)
                        : cm2Profit)
                    : (useBiCm2
                        ? (cm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_current_profit_cm2 ?? 0,
                                rangeCurrency
                            )
                            : 0)
                        : cm2Profit),

            previous: shouldShowDummyUi
                ? dummyStatData.cm2Profit.previous
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_previous_profit_cm2 ?? 0,
                                biSourceCurrency
                            )
                            : 0)
                        : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency))
                    : (useBiCm2
                        ? (cm2Ready
                            ? convertToDisplayCurrency(
                                biAlignedTotals?.total_previous_profit_cm2 ?? 0,
                                rangeCurrency
                            )
                            : 0)
                        : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency)),

            deltaPct: shouldShowDummyUi
                ? dummyStatData.cm2Profit.deltaPct
                : isStickyGlobal
                    ? (globalUseBi
                        ? (globalCm2Ready
                            ? safeDeltaPct(
                                convertToDisplayCurrency(
                                    biAlignedTotals?.total_current_profit_cm2 ?? 0,
                                    biSourceCurrency
                                ),
                                convertToDisplayCurrency(
                                    biAlignedTotals?.total_previous_profit_cm2 ?? 0,
                                    biSourceCurrency
                                )
                            )
                            : null)
                        : safeDeltaPct(
                            cm2Profit ?? 0,
                            convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency)
                        ))
                    : (useBiCm2
                        ? (cm2Ready
                            ? safeDeltaPct(
                                convertToDisplayCurrency(
                                    biAlignedTotals?.total_current_profit_cm2 ?? 0,
                                    rangeCurrency
                                ),
                                convertToDisplayCurrency(
                                    biAlignedTotals?.total_previous_profit_cm2 ?? 0,
                                    rangeCurrency
                                )
                            )
                            : null)
                        : safeDeltaPct(
                            cm2Profit ?? 0,
                            convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency)
                        )),

            loading: !shouldShowDummyUi && (
                isStickyGlobal
                    ? (loading || shopifyLoading || (globalUseBi ? biLoading : false))
                    : (loading || (useBiCm2 ? biLoading : false))
            ),
            formatter: formatDisplayAmount,
            bottomLabel: prevLabel,
            className: "bg-white border-[#B8C78C] border-t-4 border-t-[#B8C78C]",
        },
        {
            label: "Target",
            current: shouldShowDummyUi
                ? 0
                : (stats_targetHome ?? 0),
            previous: shouldShowDummyUi
                ? 0
                : (targets_lastMonthTotalHome ?? 0),
            deltaPct: shouldShowDummyUi
                ? safeDeltaPct(0, 0)
                : safeDeltaPct(stats_targetHome ?? 0, targets_lastMonthTotalHome ?? 0),
            loading: !shouldShowDummyUi && loading,
            formatter: formatDisplayAmount,
            previousFormatter: formatDisplayAmount,
            bottomLabel: "Last Month",
            className: "bg-white border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]",
        },
        {
            label: "Target Trend",
            current: shouldShowDummyUi
                ? 0
                : (stats_targetTrendPct ?? 0),
            previous: shouldShowDummyUi
                ? 0
                : (stats_targetTrendPrevPct ?? 0),
            deltaPct: shouldShowDummyUi
                ? deltaPctAbs(0, 0)
                : deltaPctAbs(stats_targetTrendPct ?? 0, stats_targetTrendPrevPct ?? 0),
            loading: !shouldShowDummyUi && loading,
            formatter: fmtPct,
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
    const finalBarValues = shouldShowDummyUi ? dummyBarValues : valuesPatched;
    const finalPrevBarValues = shouldShowDummyUi ? dummyPrevBarValues : prevValues;
    const finalAllValuesZero = shouldShowDummyUi ? false : allValuesZero;


    const finalBiDailySeriesHome = shouldShowDummyUi ? dummyBiDailySeriesHome : biDailySeriesHome;
    const finalBiPeriods = shouldShowDummyUi ? dummyBiPeriods : biPeriods;
    const finalMonthlySkuwiseRowsForTable = shouldShowDummyUi
        ? dummyMonthlySkuwiseRowsForTable
        : monthlySkuwiseRowsForTable;


    const isUsingDummyData = shouldShowDummyUi;

    const finalCm1ProfitPieData = isUsingDummyData
        ? dummyCm1ProfitPieData
        : cm1ProfitPieData;

    const finalInventoryRows = isUsingDummyData
        ? dummyInventoryRows
        : invRows;

    const finalInventoryAlerts = isUsingDummyData
        ? dummyInventoryAlerts
        : inventoryAlerts;

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

    const finalStatsTargetTrendPct = shouldShowDummyUi
        ? dummySalesTargetStats.targetTrendPct
        : stats_targetTrendPct;

    const finalTargetsReimbursement = shouldShowDummyUi
        ? dummySalesTargetStats.reimbursement
        : targets_reimbursement;

    const finalRangeCompletedPct = shouldShowDummyUi
        ? dummySalesTargetStats.periodCompletedPct
        : rangeCompletedPct;

    const finalLiveBiPayload = shouldShowDummyUi
        ? dummyLiveBusinessClientData
        : liveBiPayload;


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
    const mtdCm2ProfitCurrentDisplay = shouldShowDummyUi
        ? dummyStatData.cm2Profit.current
        : useBiCm2
            ? (cm2Ready
                ? convertToDisplayCurrency(
                    biAlignedTotals?.total_current_profit_cm2 ?? 0,
                    rangeCurrency
                )
                : 0)
            : Number(cm2Profit ?? 0);

    const mtdCm2ProfitPreviousDisplay = shouldShowDummyUi
        ? dummyStatData.cm2Profit.previous
        : useBiCm2
            ? (cm2Ready
                ? convertToDisplayCurrency(
                    biAlignedTotals?.total_previous_profit_cm2 ?? 0,
                    rangeCurrency
                )
                : 0)
            : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency);

    const mtdCm2ProfitDelta = shouldShowDummyUi
        ? dummyStatData.cm2Profit.deltaPct
        : safeDeltaPct(
            mtdCm2ProfitCurrentDisplay,
            mtdCm2ProfitPreviousDisplay
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

            {/* {!shouldShowDummyUi && pageLoading && (
                <div className="absolute inset-0 z-[999] bg-white/80 flex items-center justify-center px-4 rounded-xl">
                    <div className="w-full max-w-xl rounded-2xl border border-slate-200 bg-white p-5 md:p-6 shadow-md">
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
                                {stepProgress.percentage}%
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
                                                "text-center text-[10px] sm:text-xs font-medium leading-tight",
                                                isCompleted || isActive
                                                    ? "text-[#37455F]"
                                                    : "text-slate-400",
                                            ].join(" ")}
                                        >
                                            {step.label}
                                        </p>

                                        <span
                                            className={[
                                                "text-[9px] sm:text-[10px] px-2 py-0.5 rounded-full font-medium",
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

                        <div className="mt-5 pt-4 border-t border-slate-100 flex items-center justify-between">

                            <p className="text-xs text-slate-400 truncate">
                                {stepProgress.detail || "Initialising dashboard…"}
                            </p>

                            <div className="flex items-center gap-1 px-2 py-1 bg-slate-100 rounded-full mx-3">
                                <span className="text-xs text-slate-400">Estimated:</span>
                                <span className="text-xs font-medium text-slate-600">
                                    {estimatedTime}
                                </span>
                            </div>

                            <span className="text-xs text-slate-400 shrink-0">
                                Step {Math.min(currentStep, dashboardSteps.length)} of {dashboardSteps.length}
                            </span>
                        </div>
                    </div>
                </div>

            )
            } */}

            <DashboardLoaderModal
                pageLoading={pageLoading}
                shouldShowDummyUi={shouldShowDummyUi}
                currentStep={currentStep}
                completedSteps={completedSteps}
                dashboardSteps={dashboardSteps}
                stepProgress={stepProgress}
                loadingStartedAt={loadingStartedAt}
                estimatedSecondsMap={STEP_ESTIMATED_SECONDS}
            />

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

                    {/* RIGHT SIDE BUTTON */}
                    {/* <button
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
                    </button> */}

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
                        {dbUpdatedAt && (
                            <span className="text-sm text-gray-500">
                                Last Updated at{" "}
                                {activeDateRegion === "US"
                                    ? formatUSTime12hr(dbUpdatedAt)
                                    : formatUKTime12hr(dbUpdatedAt)}
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
                        className="grid grid-cols-12 gap-4 mt-2 md:mt-4 scroll-mt-[80px] items-start lg:items-stretch lg:auto-rows-fr"
                    >
                        {/* LEFT COLUMN */}
                        <div
                            className={`col-span-12 lg:col-span-8 order-2 lg:order-1 flex flex-col gap-4 h-auto min-h-0 lg:h-full lg:min-h-full ${leftColumnHeightClass ?? ""}`}
                        >
                            {!isCountryMode && hasGlobalCard && (
                                <div className="flex">
                                    <div className="w-full rounded-xl border bg-white p-4 lg:p-3 2xl:p-5 shadow-sm">
                                        <div className="mb-4 flex items-center justify-between gap-3">
                                            <div className="min-w-0 shrink-0">
                                                <PageBreadcrumb pageTitle="Global MTD Sales" variant="page" align="left" />
                                            </div>

                                            {showLiveBI && platform === "global" && (
                                                <div className="shrink-0 ml-auto">
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
                                                            fetchLiveBiPayload({
                                                                startDay: null,
                                                                endDay: null,
                                                                generateInsights: false,
                                                            });
                                                        }}
                                                    />
                                                </div>
                                            )}
                                        </div>

                                        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-3 auto-rows-fr">

                                            <AmazonStatCard
                                                label="Units"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.units.current
                                                        : (globalUseBi ? biCardKpis.curr.units : globalCurrUnits)
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.units.previous
                                                        : (globalUseBi ? biCardKpis.prev.units : globalPrevUnits)
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.units.deltaPct
                                                        : (globalUseBi ? biCardKpis.deltas.units : globalDeltas.units)
                                                }
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading)}
                                                formatter={fmtInt}
                                                bottomLabel={prevLabel}
                                                className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                                            />

                                            <AmazonStatCard
                                                label="Gross Sales"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.grossSales.current
                                                        : (globalUseBi ? biCardKpis.curr.grossSales : globalCurrGrossDisp)
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.grossSales.previous
                                                        : (globalUseBi ? biCardKpis.prev.grossSales : globalPrevGrossDisp)
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.grossSales.deltaPct
                                                        : (globalUseBi
                                                            ? biCardKpis.deltas.grossSales
                                                            : safeDeltaPct(combinedGrossUSD, prevGlobalGrossUSD))
                                                }
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading)}
                                                formatter={moneyPerUnitFormatter}
                                                previousFormatter={formatDisplayAmount}
                                                bottomLabel={prevLabel}
                                                className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                                            />

                                            <AmazonStatCard
                                                label="Net Sales"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.netSales.current
                                                        : (globalUseBi ? biCardKpis.curr.netSales : globalCurrNetSalesDisp)
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.netSales.previous
                                                        : (globalUseBi ? biCardKpis.prev.netSales : globalPrevNetSalesDisp)
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.netSales.deltaPct
                                                        : (globalUseBi
                                                            ? biCardKpis.deltas.netSales
                                                            : safeDeltaPct(globalCurrNetSalesDisp, globalPrevNetSalesDisp))
                                                }
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading)}
                                                formatter={moneyPerUnitFormatter}
                                                previousFormatter={formatDisplayAmount}
                                                bottomLabel={prevLabel}
                                                className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"
                                            />

                                            <AmazonStatCard
                                                label="ASP"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.asp.current
                                                        : (globalUseBi ? biCardKpis.curr.asp : globalCurrAspDisp)
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.asp.previous
                                                        : (globalUseBi ? biCardKpis.prev.asp : globalPrevAspDisp)
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.asp.deltaPct
                                                        : (globalUseBi
                                                            ? biCardKpis.deltas.asp
                                                            : safeDeltaPct(globalCurrAspDisp, globalPrevAspDisp))
                                                }
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading)}
                                                formatter={formatDisplayAmount}
                                                bottomLabel={prevLabel}
                                                className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                            />

                                            <AmazonStatCard
                                                label="Cost of Ads"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.costOfAds.current
                                                        : globalUseBi
                                                            ? (globalCm2Ready
                                                                ? convertToDisplayCurrency(
                                                                    biAlignedTotals?.total_current_advertising ?? 0,
                                                                    biSourceCurrency
                                                                )
                                                                : 0)
                                                            : globalBottomCards.currentCostOfAds
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.costOfAds.previous
                                                        : globalUseBi
                                                            ? (globalCm2Ready
                                                                ? convertToDisplayCurrency(
                                                                    biAlignedTotals?.total_previous_advertising ?? 0,
                                                                    biSourceCurrency
                                                                )
                                                                : 0)
                                                            : amazonPrevAdsDisp
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.costOfAds.deltaPct
                                                        : globalUseBi
                                                            ? (globalCm2Ready
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
                                                            : safeDeltaPct(
                                                                globalBottomCards.currentCostOfAds,
                                                                amazonPrevAdsDisp
                                                            )
                                                }
                                                inverseDelta
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || (globalUseBi ? biLoading : false))}
                                                formatter={formatDisplayAmount}
                                                previousFormatter={formatDisplayAmount}
                                                bottomLabel={prevLabel}
                                                className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                            />

                                            <AmazonStatCard
                                                label="TACoS"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.tacos.current
                                                        : globalUseBi
                                                            ? (globalCm2Ready
                                                                ? (() => {
                                                                    const ads = biAlignedTotals?.total_current_advertising ?? 0;
                                                                    const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                                                    return sales > 0 ? (ads / sales) * 100 : 0;
                                                                })()
                                                                : 0)
                                                            : globalBottomCards.currentTacos
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.tacos.previous
                                                        : globalUseBi
                                                            ? (globalCm2Ready
                                                                ? (() => {
                                                                    const ads = biAlignedTotals?.total_previous_advertising ?? 0;
                                                                    const sales = biAlignedTotals?.total_previous_net_sales ?? 0;
                                                                    return sales > 0 ? (ads / sales) * 100 : 0;
                                                                })()
                                                                : 0)
                                                            : amazonPrevRoasPct
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.tacos.deltaPct
                                                        : globalUseBi
                                                            ? (globalCm2Ready
                                                                ? safeDeltaPct(
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
                                                            : safeDeltaPct(
                                                                globalBottomCards.currentTacos,
                                                                amazonPrevRoasPct
                                                            )
                                                }
                                                inverseDelta
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || (globalUseBi ? biLoading : false))}
                                                formatter={fmtPct2}
                                                bottomLabel={prevLabel}
                                                className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                            />

                                            <AmazonStatCard
                                                label="CM2 Profit"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2Profit.current
                                                        : cm2Profit
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2Profit.previous
                                                        : convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency)
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2Profit.deltaPct
                                                        : safeDeltaPct(
                                                            cm2Profit,
                                                            convertToDisplayCurrency(prev.cm2Profit ?? 0, amazonDataCurrency)
                                                        )
                                                }
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading)}
                                                formatter={formatDisplayAmount}
                                                bottomLabel={prevLabel}
                                                className="border-[#B8C78C] border-t-4 border-t-[#B8C78C]"
                                            />

                                            <AmazonStatCard
                                                label="CM2 Profit %"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2ProfitPct.current
                                                        : (
                                                            Number(curr?.netSales ?? 0) !== 0
                                                                ? (Number(uk?.cm2ProfitGBP ?? 0) / Number(curr.netSales)) * 100
                                                                : 0
                                                        )
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2ProfitPct.previous
                                                        : (
                                                            Number(prev?.netSales ?? 0) !== 0
                                                                ? (Number(prev?.cm2Profit ?? 0) / Number(prev.netSales)) * 100
                                                                : 0
                                                        )
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2ProfitPct.deltaPct
                                                        : safeDeltaPct(
                                                            Number(curr?.netSales ?? 0) !== 0
                                                                ? (Number(uk?.cm2ProfitGBP ?? 0) / Number(curr.netSales)) * 100
                                                                : 0,
                                                            Number(prev?.netSales ?? 0) !== 0
                                                                ? (Number(prev?.cm2Profit ?? 0) / Number(prev.netSales)) * 100
                                                                : 0
                                                        )
                                                }
                                                loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading)}
                                                formatter={fmtPct}
                                                bottomLabel={prevLabel}
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
                                        <div className="mb-3 lg:mb-2 2xl:mb-4 flex items-center justify-between gap-2 sm:gap-3">
                                            <div className="min-w-0">
                                                <PageBreadcrumb pageTitle="MTD Sales" variant="page" align="left" />
                                            </div>

                                            {showLiveBI && isCountryMode && (
                                                <div className="ml-auto shrink-0">
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
                                                </div>
                                            )}
                                        </div>

                                        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-2 lg:gap-2 2xl:gap-3 auto-rows-fr">

                                            <AmazonStatCard
                                                label="Units"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.units.current
                                                        : (useBiForAmazonCards ? biCardKpis.curr.units : (totals?.quantity ?? 0))
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.units.previous
                                                        : (useBiForAmazonCards ? biCardKpis.prev.units : prev.quantity)
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.units.deltaPct
                                                        : (useBiForAmazonCards ? biCardKpis.deltas.units : deltas.quantityPct)
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
                                                formatter={moneyPerUnitFormatter}
                                                previousFormatter={formatDisplayAmount}
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
                                                formatter={moneyPerUnitFormatter}
                                                previousFormatter={formatDisplayAmount}
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
                                                bottomLabel={prevLabel}
                                                className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                            />

                                            <AmazonStatCard
                                                label="Cost of Ads"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.costOfAds.current
                                                        : useBiForAmazonCards
                                                            ? (cm2Ready
                                                                ? convertToDisplayCurrency(
                                                                    biAlignedTotals?.total_current_advertising ?? 0,
                                                                    biSourceCurrency
                                                                )
                                                                : 0)
                                                            : adsSpendTotal
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.costOfAds.previous
                                                        : useBiForAmazonCards
                                                            ? (cm2Ready
                                                                ? convertToDisplayCurrency(
                                                                    biAlignedTotals?.total_previous_advertising ?? 0,
                                                                    biSourceCurrency
                                                                )
                                                                : 0)
                                                            : amazonPrevAdsDisp
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.costOfAds.deltaPct
                                                        : useBiForAmazonCards
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
                                                            : safeDeltaPct(
                                                                adsSpendTotal,
                                                                amazonPrevAdsDisp
                                                            )
                                                }
                                                inverseDelta
                                                loading={!shouldShowDummyUi && (loading || (useBiForAmazonCards ? biLoading : false))}
                                                formatter={(v) => renderMoneyWithPerUnit(Number(v) || 0, unitsToUse, formatDisplayAmount)}
                                                previousFormatter={formatDisplayAmount}
                                                bottomLabel={prevLabel}
                                                className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                            />

                                            <AmazonStatCard
                                                label="TACoS"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.tacos.current
                                                        : useBiForAmazonCards
                                                            ? (cm2Ready
                                                                ? (() => {
                                                                    const sales = biAlignedTotals?.total_current_net_sales ?? 0;
                                                                    return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                                                                })()
                                                                : 0)
                                                            : (() => {
                                                                const sales = toNumber(plSummaryTotals.net_sales);
                                                                return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                                                            })()
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.tacos.previous
                                                        : useBiForAmazonCards
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
                                                    shouldShowDummyUi
                                                        ? dummyStatData.tacos.deltaPct
                                                        : useBiForAmazonCards
                                                            ? (cm2Ready
                                                                ? safeDeltaPct(
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
                                                            : safeDeltaPct(
                                                                (() => {
                                                                    const sales = toNumber(plSummaryTotals.net_sales);
                                                                    return sales > 0 ? (adsSpendTotal / sales) * 100 : 0;
                                                                })(),
                                                                amazonPrevRoasPct
                                                            )
                                                }
                                                inverseDelta
                                                loading={!shouldShowDummyUi && (loading || (useBiForAmazonCards ? biLoading : false))}
                                                formatter={fmtPct2}
                                                bottomLabel={prevLabel}
                                                className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                            />

                                            <AmazonStatCard
                                                label="CM2 Profit"
                                                current={mtdCm2ProfitCurrentDisplay}
                                                previous={mtdCm2ProfitPreviousDisplay}
                                                deltaPct={mtdCm2ProfitDelta}
                                                loading={!shouldShowDummyUi && (loading || (useBiCm2 ? biLoading : false))}
                                                formatter={(v) => renderMoneyWithPerUnit(Number(v) || 0, unitsToUse, formatDisplayAmount)}
                                                previousFormatter={formatDisplayAmount}
                                                bottomLabel={prevLabel}
                                                className="border-[#B8C78C] border-t-4 border-t-[#B8C78C]"
                                            />

                                            <AmazonStatCard
                                                label="CM2 Profit %"
                                                current={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2ProfitPct.current
                                                        : useBiForAmazonCards
                                                            ? (cm2Ready
                                                                ? (
                                                                    Number(biAlignedTotals?.total_current_net_sales ?? 0) !== 0
                                                                        ? (
                                                                            Number(biAlignedTotals?.total_current_profit_cm2 ?? 0) /
                                                                            Number(biAlignedTotals?.total_current_net_sales ?? 0)
                                                                        ) * 100
                                                                        : 0
                                                                )
                                                                : 0)
                                                            : (
                                                                Number(curr?.netSales ?? 0) !== 0
                                                                    ? (Number(uk?.cm2ProfitGBP ?? 0) / Number(curr.netSales)) * 100
                                                                    : 0
                                                            )
                                                }
                                                previous={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2ProfitPct.previous
                                                        : useBiForAmazonCards
                                                            ? (cm2Ready
                                                                ? (
                                                                    Number(biAlignedTotals?.total_previous_net_sales ?? 0) !== 0
                                                                        ? (
                                                                            Number(biAlignedTotals?.total_previous_profit_cm2 ?? 0) /
                                                                            Number(biAlignedTotals?.total_previous_net_sales ?? 0)
                                                                        ) * 100
                                                                        : 0
                                                                )
                                                                : 0)
                                                            : (
                                                                Number(prev?.netSales ?? 0) !== 0
                                                                    ? (Number(prev?.cm2Profit ?? 0) / Number(prev.netSales)) * 100
                                                                    : 0
                                                            )
                                                }
                                                deltaPct={
                                                    shouldShowDummyUi
                                                        ? dummyStatData.cm2ProfitPct.deltaPct
                                                        : useBiForAmazonCards
                                                            ? (cm2Ready
                                                                ? safeDeltaPct(
                                                                    Number(biAlignedTotals?.total_current_net_sales ?? 0) !== 0
                                                                        ? (
                                                                            Number(biAlignedTotals?.total_current_profit_cm2 ?? 0) /
                                                                            Number(biAlignedTotals?.total_current_net_sales ?? 0)
                                                                        ) * 100
                                                                        : 0,
                                                                    Number(biAlignedTotals?.total_previous_net_sales ?? 0) !== 0
                                                                        ? (
                                                                            Number(biAlignedTotals?.total_previous_profit_cm2 ?? 0) /
                                                                            Number(biAlignedTotals?.total_previous_net_sales ?? 0)
                                                                        ) * 100
                                                                        : 0
                                                                )
                                                                : null)
                                                            : safeDeltaPct(
                                                                Number(curr?.netSales ?? 0) !== 0
                                                                    ? (Number(uk?.cm2ProfitGBP ?? 0) / Number(curr.netSales)) * 100
                                                                    : 0,
                                                                Number(prev?.netSales ?? 0) !== 0
                                                                    ? (Number(prev?.cm2Profit ?? 0) / Number(prev.netSales)) * 100
                                                                    : 0
                                                            )
                                                }
                                                loading={!shouldShowDummyUi && (loading || (useBiForAmazonCards ? biLoading : false))}
                                                formatter={(v) => `${Number(v || 0).toFixed(2)}%`}
                                                previousFormatter={(v) => `${Number(v || 0).toFixed(2)}%`}
                                                bottomLabel={prevLabel}
                                                className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                                            />
                                        </div>

                                    </div>



                                    {/* Live BI graph */}
                                    {showLiveBI && isCountryMode && (
                                        <div className="w-full rounded-xl border bg-white p-3 lg:p-3 2xl:p-5 shadow-sm overflow-x-hidden">
                                            <div className="w-full max-w-full min-w-0">

                                                {/* ✅ CASE 1: 202 → processing */}
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

                                                {/* ✅ CASE 2: 200 but empty */}
                                                {!shouldShowDummyUi && biStatus === "ready" && !biDailySeriesHome && (
                                                    <div className="text-center py-10 text-sm text-gray-500">
                                                        No data available for the selected period
                                                    </div>
                                                )}

                                                {/* ✅ CASE 3: 200 + data */}
                                                {(shouldShowDummyUi || biStatus === "ready") && finalBiDailySeriesHome && (

                                                    <LiveBiLineGraph
                                                        dailySeries={finalBiDailySeriesHome}
                                                        periods={finalBiPeriods}
                                                        loading={!shouldShowDummyUi && biUiLoading}
                                                        error={shouldShowDummyUi ? null : biError}
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
                        <aside className="col-span-12 lg:col-span-4 order-1 lg:order-2 h-auto min-h-0 self-auto lg:h-full lg:min-h-full lg:self-stretch">
                            <div className="grid gap-4 h-auto lg:h-full lg:grid-rows-[auto_minmax(0,1fr)]">

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
                                        salesTrendPct={finalStatsSalesTrendPct}
                                        targetTrendPct={finalStatsTargetTrendPct}
                                        currentReimbursement={finalTargetsReimbursement.current}
                                        previousReimbursement={finalTargetsReimbursement.previous}
                                        biAlignedTotals={shouldShowDummyUi ? null : biAlignedTotalsHome}
                                        biEnabled={shouldShowDummyUi ? false : biCardsReady}
                                    />

                                </div>

                                {/* Bottom card */}
                                <div className="h-auto lg:h-full lg:sticky lg:top-4 2xl:top-6">

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
                        </aside>
                    </div >
                )}

                {activeTab === "live" && platform === "global" && showLiveBI && (
                    <div
                        id="ai-insights"
                        className="mt-2 md:mt-4 w-full rounded-xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden scroll-mt-[80px]"
                    >
                        <div className="w-full max-w-full min-w-0">
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
                )
                }

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
                                        await fetchLiveBiPayload({ generateInsights: true });
                                    }}
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

                            {!shouldShowDummyUi && loading && monthlySkuwiseRows.length === 0 ? (
                                <div className="text-sm text-gray-500">Loading…</div>
                            ) : finalMonthlySkuwiseRowsForTable.length === 0 ? (
                                <div className="text-sm text-red-600">
                                    No P&L productwise rows available for this period.
                                </div>
                            ) : (
                                <div className="w-full overflow-x-auto rounded-xl border border-gray-300">
                                    <div className="min-w-full">
                                        <GroupedCollapsibleTable<MonthlySkuwiseTableRow>
                                            rows={finalMonthlySkuwiseRowsForTable}
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
                                                if (colKey === "product_name") return row.isTotal ? "Total" : row.isOthers ? "Others" : row.product_name;

                                                if (colKey === "quantity") return row.quantity;

                                                if (colKey === "asp") return formatAdsNumber(row.asp);
                                                if (colKey === "net_sales") return formatAdsNumber(row.net_sales);

                                                if (colKey === "tax" || colKey === "credits" || colKey === "tax_and_credits") {
                                                    const v = Number((row as any)[colKey] ?? 0);
                                                    return formatAdsNumber(Math.abs(Number.isFinite(v) ? v : 0));
                                                }
                                                // CM1 %
                                                if (colKey === "cm1_profit_per") {
                                                    const v = Number(row.cm1_profit_per ?? 0);
                                                    return `${formatAdsNumber(Math.abs(v))}%`;
                                                }

                                                // CM1 per unit (no %)
                                                if (colKey === "cm1_profit_per_unit") {
                                                    const v = Number(row.cm1_profit_per_unit ?? 0);
                                                    return formatAdsNumber(Math.abs(v));
                                                }

                                                // if (colKey === "cm2_profit_per" || colKey === "cm2_profit_per_unit") {
                                                //     const v = Number((row as any)[colKey] ?? 0);
                                                //     return formatAdsNumber(Number.isFinite(v) ? v : 0);
                                                // }
                                                // CM2 %
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
                                                if (colKey === "ads_spend")
                                                    return formatAdsNumber(Math.abs(row.ads_spend));
                                                if (colKey === "acos") {
                                                    const v = Number(row.acos ?? 0);
                                                    return `${formatAdsNumber(v)}%`;
                                                }
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
                                                enabled: finalMonthlySkuwiseRowsForTable.length > 0,

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
                                                            },
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
                                                        endValue: Number(reimbursementForSummary.toFixed(2)),
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
                                            title="CM1 Profit Breakdown"
                                            data={finalCm1ProfitPieData}
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
                {activeTab === "inventory" &&
                    (isUsingDummyData || amazonIntegrated) && (
                        <div id="current-inventory" className="scroll-mt-[80px]">
                            <CurrentInventorySection
                                region={isUsingDummyData ? "UK" : graphRegionToUse}
                                invLoading={!shouldShowDummyUi && invLoading}
                                invError={shouldShowDummyUi ? "" : invError}
                                invRows={finalInventoryRows}
                                inventoryAlerts={finalInventoryAlerts}
                                userData={userData}
                                convertToDisplayCurrency={convertToDisplayCurrency}
                                displayCurrency={displayCurrency}
                            />

                        </div>
                    )}
            </PreviewLockedSection>
        </div >

    );
}



