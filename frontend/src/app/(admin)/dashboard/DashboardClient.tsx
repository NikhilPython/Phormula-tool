"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSelector } from "react-redux";
import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import Loader from "@/components/loader/Loader";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
// import CurrentInventorySection from "@/components/dashboard/CurrentInventorySection";
import { useCurrentInventoryExcelExport } from "@/lib/inventory/useCurrentInventoryExcelExport";
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
import LiveBusinessClient from "@/app/(admin)/live-business-insight/[ranged]/[countryName]/[month]/[year]/liveBusinessClient";
import { useRouter, useParams } from "next/navigation";
import type { ColGroup, LeafCol } from "@/components/ui/table/GroupedCollapsibleTable";
import {
    exportPnLProductwiseBreakdownMtdExcel,
    exportCurrentInventoryExcel,
    exportAgeingRiskHeatmapExcel,
    exportGlobalAgeingRiskHeatmapExcel,
} from "@/lib/excel/exportCurrentInventoryExcel";
import * as XLSX from "xlsx-js-style";
import { fetchCurrentInventoryData, InventoryRow } from "@/lib/inventory/fetchCurrentInventoryData";
import Alert from "@/components/ui/alert/Alert";
import DashboardStickyKpis from "./DashboardStickyKpis";
import { buildSkuwiseTableColumns } from "./DashboardSkuwiseColumns";
import {
    dummyBarLabels,
    dummyBarValues,
    dummyBiDailySeriesHome,
    dummyBiPeriods,
    dummyCm1ProfitPieData,
    dummyInventoryAlerts,
    dummyInventoryRows,
    dummyLiveBusinessClientData,
    dummyMonthlySkuwiseRowsForTable,
    dummyPrevBarValues,
    dummySalesTargetStats,
    dummyStatData,
    dummyTargetData,
} from "./DashboardPreviewData";
import DashboardPageHeader from "./DashboardPageHeader";
import { DashboardLoaderModal, PreviewLockedSection } from "./DashboardAccessPanels";
import DashboardLiveSalesTab from "./DashboardLiveSalesTab";
import DashboardProductwisePnlSection from "./DashboardProductwisePnlSection";
import DashboardMtdPlSection from "./DashboardMtdPlSection";
import DashboardInventoryInsightsTab from "./DashboardInventoryInsightsTab";
import {
    AGEING_TREND_BUCKET_OPTIONS,
    buildAgeingTrendDataFromInventoryCurrent,
    buildAgeingTrendDataFromSummary,
    buildInventoryInsightsFromResponses,
    getSelectedCountryAgeSummaryResponses,
    getSelectedCountryInventoryResponse,
    inventoryMonthIndexMap,
    sliceByDayRange,
} from "./DashboardInventoryInsightsUtils";
import type {
    ApiDailySeries,
    BiAlignedTotals,
    BiApiResponse,
    Cm1PieSlice,
    CountryTimezoneResponse,
    CurrencyCode,
    DailyPoint,
    FetchLiveBiPayloadArgs,
    GraphDailySeries,
    GrandTotalSkuwiseRow,
    InventoryAgeSummaryApiResponse,
    InventoryAlertRecord,
    InventoryCurrentApiResponse,
    InventoryCurrentRow,
    InventoryInsightsData,
    MonthlySkuwiseRow,
    MonthlySkuwiseTableRow,
    MonthlySpRow,
    PlSummaryTotals,
    ProductwiseMoneyKey,
    UiAlert,
} from "./DashboardTypes";
import { Toaster, toast } from "sonner";
import { useHeaderNotifications } from "@/components/context/NotificationContext";
import InventoryAgeGraphSection from "@/components/dashboard/InventoryAgeGraphSection";
import SkuRecommendationDrawer from "@/components/dashboard/SkuRecommendationDrawer";

import type {
    AgeingBucket,
    AgeingRiskHeatmapRow,
    AgeingRiskUnitSalesDataKey,
} from "@/components/common/inventory/AgeingRiskHeatmap";

import type {
    DonutChartItem,
} from "@/components/common/inventory/SkuAgeingDonutChart";

import type {
    AgeingTrendItem,
    AgeingTrendAllSeriesItem,
} from "@/components/common/inventory/AgeingTrendChart";

import type {
    ActionCardItem,
    ActionLogicItem,
} from "@/components/common/inventory/ActionBasedDashboard";



const ROUND_LABELS = [
    "Gross Sales",
    "Net Sales",
    "Cost of Ads",
    "CM2 Profit",
    "Promotions",
];


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
const DAILY_SP_SD_SB_ENDPOINT = `${baseURL}/api/ads/daily_sp_sd_sb_to_db`;
const GBP_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_GBP_TO_USD || "1.25");
const INR_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_INR_TO_USD || "0.01128");
const CAD_TO_USD_ENV = Number(process.env.NEXT_PUBLIC_CAD_TO_USD || "0.74");
const SB_KEYWORD_ENDPOINT = `${baseURL}/api/ads/manager/sb_keyword_report`;

const DEFAULT_INVENTORY_MARKETPLACE_IDS: Record<string, string> = {
    uk: "A1F83G8C2ARO7P",
    us: "ATVPDKIKX0DER",
    ca: "A2EUQ1WTGCTBG2",
};

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

const isMissingDisplayName = (value: any) => {
    if (value === undefined || value === null) return true;

    const clean = String(value).trim().toLowerCase();

    return (
        clean === "" ||
        clean === "-" ||
        clean === "0" ||
        clean === "nan" ||
        clean === "none" ||
        clean === "null" ||
        clean === "undefined" ||
        clean === "unknown"
    );
};

const getSkuwiseDisplayProductName = (row: any) => {
    const productName = row?.product_name;
    const sku = row?.sku;

    if (!isMissingDisplayName(productName)) {
        return normalizeProductDisplayName(productName);
    }

    if (!isMissingDisplayName(sku)) {
        return String(sku).trim();
    }

    return "-";
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
    "inventory_charges_and_reimbursement",
    "others",
    "other_adjustment",
]);

const ROUNDED_SUMMARY_KEYS = new Set<string>([
    "misc_transaction",
    "lost_total",
    "net_reimbursement",
    "debt_payment",
    "disbursement",

    "short_term_storage_fee",
    "long_term_storage_fee",
    "fba_disposal",
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
    "others",
    "other_adjustment",
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
        placement_fee: toNumber(source?.placement_fee),
        placement_fees: toNumber(source?.placement_fees ?? source?.placement_fee),
        shipping_charges: toNumber(source?.shipping_charges),
        shipment_fees: toNumber(source?.shipment_fees),
        customs_fee: toNumber(source?.customs_fee),
        customs_fees: toNumber(source?.customs_fees ?? source?.customs_fee),
        storage_fee: toNumber(source?.storage_fee),
        storage_fees: toNumber(source?.storage_fees ?? source?.storage_fee),
        platform_management_fees: toNumber(source?.platform_management_fees),
        other_adjustment: toNumber(source?.other_adjustment),

        other_transactions: toNumber(source?.other_transactions ?? source?.platform_fee ?? source?.other_fees_total),
        platform_fee: platformFees,
        inventory_storage_fees: inventoryStorageFees,
        platform_fee_inventory_storage: inventoryStorageFees,

        short_term_storage_fee: toNumber(source?.short_term_storage_fee),
        long_term_storage_fee: toNumber(source?.long_term_storage_fee),
        fba_disposal: toNumber(source?.fba_disposal),

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
        rows.find((r) => ["GRAND_TOTAL", "TOTAL"].includes(String(r?.sku || "").toUpperCase())) ||
        rows.find((r) => ["grand total", "total"].includes(String(r?.product_name || "").toLowerCase())) ||
        rows.find((r) => r?.isTotal) ||
        rows[rows.length - 1] ||
        {}
    );
}

const pickFirstNonZeroNumber = (...values: any[]) => {
    let fallback = 0;
    let hasFallback = false;

    for (const value of values) {
        if (value === undefined || value === null || value === "") continue;

        const n = toNumber(value);

        if (!hasFallback) {
            fallback = n;
            hasFallback = true;
        }

        if (n !== 0) return n;
    }

    return fallback;
};

const pickPromotionalRebates = (...sources: any[]) =>
    pickFirstNonZeroNumber(
        ...sources.map((source) =>
            source?.promotional_rebates ??
            source?.total_promotional_rebates ??
            source?.total_previous_promotional_rebates ??
            source?.promotions
        )
    );

const calculatePromotionalRebatesPct = (
    promotionalRebates: number,
    netSales: number,
    ...sources: any[]
) => {
    const explicitPct = pickFirstNonZeroNumber(
        ...sources.map((source) =>
            source?.promotional_rebates_percentage ??
            source?.total_promotional_rebates_percentage ??
            source?.total_previous_promotional_rebates_percentage ??
            source?.promotions_percentage
        )
    );

    return Math.abs(
        netSales
            ? (promotionalRebates / netSales) * 100
            : explicitPct
    );
};

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
        placement_fee: toNumber(grand?.placement_fee),
        placement_fees: toNumber(grand?.placement_fees ?? grand?.placement_fee),
        shipping_charges: toNumber(grand?.shipping_charges),
        shipment_fees: toNumber(grand?.shipment_fees),
        customs_fee: toNumber(grand?.customs_fee),
        customs_fees: toNumber(grand?.customs_fees ?? grand?.customs_fee),
        storage_fee: toNumber(grand?.storage_fee),
        storage_fees: toNumber(grand?.storage_fees ?? grand?.storage_fee),
        platform_management_fees: toNumber(grand?.platform_management_fees),
        other_adjustment: toNumber(grand?.other_adjustment),

        other_transactions: toNumber(grand?.other),
        platform_fee: toNumber(grand?.platformfeenew ?? grand?.platform_fee),
        inventory_storage_fees: toNumber(grand?.platform_fee_inventory_storage),
        platform_fee_inventory_storage: toNumber(grand?.platform_fee_inventory_storage),

        short_term_storage_fee: toNumber(grand?.short_term_storage_fee),
        long_term_storage_fee: toNumber(grand?.long_term_storage_fee),
        fba_disposal: toNumber(grand?.fba_disposal),

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

    const skuTotals = computePlSummaryTotalsFromSkuwise(apiRows);
    const derivedSource =
        data?.derived_totals ||
        data?.summary ||
        data?.pl_summary ||
        data?.mtd_summary ||
        data?.totals;

    const derivedRecord =
        derivedSource && typeof derivedSource === "object"
            ? (derivedSource as Record<string, unknown>)
            : null;

    if (!derivedRecord) return skuTotals;

    const has = (key: string) => Object.prototype.hasOwnProperty.call(derivedRecord, key);
    const value = (key: string) => toNumber(derivedRecord[key]);

    return {
        ...skuTotals,
        placement_fee: has("placement_fee") ? value("placement_fee") : skuTotals.placement_fee,
        placement_fees: has("placement_fees")
            ? value("placement_fees")
            : has("placement_fee")
                ? value("placement_fee")
                : skuTotals.placement_fees,
        shipping_charges: has("shipping_charges")
            ? value("shipping_charges")
            : skuTotals.shipping_charges,
        shipment_fees: has("shipment_fees") ? value("shipment_fees") : skuTotals.shipment_fees,
        customs_fee: has("customs_fee") ? value("customs_fee") : skuTotals.customs_fee,
        customs_fees: has("customs_fees")
            ? value("customs_fees")
            : has("customs_fee")
                ? value("customs_fee")
                : skuTotals.customs_fees,
        storage_fee: has("storage_fee") ? value("storage_fee") : skuTotals.storage_fee,
        storage_fees: has("storage_fees")
            ? value("storage_fees")
            : has("storage_fee")
                ? value("storage_fee")
                : skuTotals.storage_fees,
        short_term_storage_fee: has("short_term_storage_fee")
            ? value("short_term_storage_fee")
            : skuTotals.short_term_storage_fee,
        long_term_storage_fee: has("long_term_storage_fee")
            ? value("long_term_storage_fee")
            : skuTotals.long_term_storage_fee,
        platform_management_fees: has("platform_management_fees")
            ? value("platform_management_fees")
            : skuTotals.platform_management_fees,
        other_adjustment: has("other_adjustment")
            ? value("other_adjustment")
            : skuTotals.other_adjustment,
    };
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
    country: string, // "UK" | "US" | "CA"
    force = false
) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";
    const { start_date, end_date } = getIstMonthToTodayRangeISO();

    // once per user + country + day
    const storageKey = `sp_report_seed_daily_${userId}_${country}_${end_date}`;
    if (!force && localStorage.getItem(storageKey) === "1") return;

    const lockKey = `${storageKey}_lock`;

    const didRun = await withLocalStorageLock(lockKey, async () => {
        // re-check after lock to avoid race
        if (!force && localStorage.getItem(storageKey) === "1") return;

        const body = {
            start_date,
            end_date,
            time_unit: "DAILY",
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
    country: string, // "UK" | "US"
    force = false
) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";
    const { start_date, end_date } = getIstMonthToTodayRangeISO();

    // once per user + country + day
    const storageKey = `sd_report_seed_daily_${userId}_${country}_${end_date}`;
    if (!force && localStorage.getItem(storageKey) === "1") return;

    const lockKey = `${storageKey}_lock`;

    const didRun = await withLocalStorageLock(lockKey, async () => {
        // re-check after lock to avoid race
        if (!force && localStorage.getItem(storageKey) === "1") return;

        // ✅ BODY EXACTLY AS REQUESTED (same keys/shape)
        const body = {
            start_date,
            end_date,
            time_unit: "DAILY",
            countries: [country], // ["UK"] or ["US"]
            max_wait_seconds: 1800,
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
    country: string, // "UK" | "US" | "CA"
    force = false
) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";
    const { start_date, end_date } = getIstMonthToTodayRangeISO();

    // once per user + country + day
    const storageKey = `sb_keyword_report_seed_daily_${userId}_${country}_${end_date}`;
    if (!force && localStorage.getItem(storageKey) === "1") return;

    const lockKey = `${storageKey}_lock`;

    const didRun = await withLocalStorageLock(lockKey, async () => {
        // re-check after lock to avoid race
        if (!force && localStorage.getItem(storageKey) === "1") return;

        // ✅ BODY EXACTLY AS REQUESTED
        const body = {
            start_date,
            end_date,
            time_unit: "DAILY",
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
    const [activeTab, setActiveTab] = useState<TopTab>("summary");
    const [summaryLoading, setSummaryLoading] = useState(true);
    const [countryTime, setCountryTime] = useState<CountryTimezoneResponse | null>(null);
    const [countryTimeLoading, setCountryTimeLoading] = useState(false);
    const [countryTimeError, setCountryTimeError] = useState<string | null>(null);
    const [dismissedAlertIds, setDismissedAlertIds] = useState<Set<string>>(
        () => new Set()
    );
    const pageTopRef = useRef<HTMLDivElement | null>(null);
    const tabTopRef = useRef<HTMLDivElement | null>(null);
    const shouldScrollTabTopRef = useRef(false);

    const [selectedAgeingTrendBucket, setSelectedAgeingTrendBucket] =
        useState<string>("365+ days");

    const [selectedGlobalInventoryCountry, setSelectedGlobalInventoryCountry] =
        useState<"uk" | "us">("uk");

    const [inventoryInsightsData, setInventoryInsightsData] =
        useState<InventoryInsightsData | null>(null);

    const [inventoryInsightResponses, setInventoryInsightResponses] = useState<
        InventoryCurrentApiResponse[]
    >([]);

    const [inventoryAgeSummaryResponses, setInventoryAgeSummaryResponses] = useState<
        InventoryAgeSummaryApiResponse[]
    >([]);

    const [inventoryInsightsLoading, setInventoryInsightsLoading] =
        useState(false);

    const [inventoryInsightsError, setInventoryInsightsError] =
        useState<string | null>(null);

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
            net_sales: true,
            promotions: true,
            marketplace_fees: true,
            other_transactions: true,
            profit: true,
            cm2_profit: true,
            ads_spend: true,
        }),
        []
    );

    const [productwiseCollapsed, setProductwiseCollapsed] = useState<Record<string, boolean>>(
        productwiseInitialCollapsed
    );

    const [productwiseAllColumnsExpanded, setProductwiseAllColumnsExpanded] = useState(false);

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

            const toNullableNumber = (value: any): number | null => {
                if (value === undefined || value === null || value === "") return null;

                const n = Number(String(value).replace(/,/g, "").trim());
                return Number.isFinite(n) ? n : null;
            };

            const getFirstNonZeroNumber = (...values: any[]): number | null => {
                for (const value of values) {
                    const n = toNullableNumber(value);
                    if (n !== null && n !== 0) return n;
                }

                return null;
            };

            rows.forEach((row: any) => {
                const rawName = String(row?.product_name || "").trim();
                const rawSku = String(row?.sku || "").trim().toUpperCase();

                if (!rawName && !rawSku) return;
                if (rawSku === "TOTAL" || rawSku === "GRAND_TOTAL") return;

                const normalizedName = normalizePieName(rawName || rawSku);

                if (
                    !normalizedName ||
                    normalizedName === "total" ||
                    normalizedName === "grand total"
                ) {
                    return;
                }

                /**
                 * IMPORTANT FOR GLOBAL:
                 * /previous_skuwise_global currently does not send SKU-level cm2_profit.
                 * It sends SKU-level profit only.
                 * So for cm2_profit previous map, fallback to profit.
                 */
                const value =
                    key === "cm2_profit"
                        ? getFirstNonZeroNumber(
                            row?.cm2_profit,
                            row?.total_cm2_profit,
                            row?.profit
                        )
                        : getFirstNonZeroNumber(
                            row?.profit,
                            row?.cm1_profit
                        );

                if (value === null) return;

                map.set(normalizedName, value);

                if (rawSku) {
                    map.set(rawSku.toLowerCase(), value);
                }
            });

            return map;
        },
        [platform, previousSkuwiseGlobalData, normalizePieName]
    );

    type GlobalMtdView = "global" | "uk" | "us";

    const [globalMtdView, setGlobalMtdView] = useState<GlobalMtdView>("global");

    const globalMtdCountry = useMemo<"uk" | "us">(() => {
        return globalMtdView === "us" ? "us" : "uk";
    }, [globalMtdView]);

    const isUsPnlSkuLayout = useMemo(() => {
        return countryName === "us" || (platform === "global" && globalMtdView === "us");
    }, [countryName, platform, globalMtdView]);

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

    const dashboardCompletedTimeZone = useMemo(() => {
        if (platform === "global" && isUsAmazonConnected) {
            return "America/Los_Angeles";
        }

        if (activeDateRegion === "US") return "America/Los_Angeles";
        if (activeDateRegion === "CA") return "America/Toronto";
        if (activeDateRegion === "UK") return "Europe/London";

        return "Asia/Kolkata";
    }, [platform, isUsAmazonConnected, activeDateRegion]);

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

    const runAdsBackgroundSync = useCallback(async ({
        forceReportSync = false,
    }: {
        forceReportSync?: boolean;
    } = {}) => {
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
            const adsSeedTasks = [
                ensureSpReportSeedOncePerDay(baseURL, jwtToken, country, forceReportSync),
                ensureSbKeywordReportSeedOncePerDay(baseURL, jwtToken, country, forceReportSync),
            ];

            if (country === "UK" || country === "US") {
                adsSeedTasks.push(
                    ensureSdReportSeedOncePerDay(baseURL, jwtToken, country, forceReportSync)
                );
            }

            await Promise.all(adsSeedTasks);

            const { monthName, year } = getRegionYearMonth(activeDateRegion);
            const month = monthToNumber(monthName.toLowerCase());
            const include = country === "UK" || country === "US" ? ["SP", "SD", "SB"] : ["SP"];

            const adsDbPayload = {
                month,
                year,
                country,
                include,
            };

            const callAdsDbSync = async (url: string, fallbackName: string) => {
                const res = await fetch(url, {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${jwtToken}`,
                        Accept: "application/json",
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify(adsDbPayload),
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
                    throw new Error(errMsg || `${fallbackName} failed`);
                }

                return json;
            };

            await callAdsDbSync(MONTHLY_SP_ENDPOINT, "monthly_sp_sd_to_db");

            await callAdsDbSync(DAILY_SP_SD_SB_ENDPOINT, "daily_sp_sd_sb_to_db");


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
        if (!pendingHash) return;

        setPendingHash("");
    }, [pendingHash]);

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
        { num: 2, label: "Inventory Data" },
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

        const countries = [targetSummaryCountry];

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

        const countries = [targetSummaryCountry];

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

    const inventoryMarketplaceIds = useMemo(() => {
        const inventoryConnections = (amazonConnections || []) as Array<{
            country?: string;
            marketplace_id?: string;
        }>;

        const marketplaceForCountry = (countryKey: "uk" | "us" | "ca") => {
            const connectedMarketplace = inventoryConnections.find(
                (connection) =>
                    String(connection?.country || "").toLowerCase() === countryKey
            )?.marketplace_id;

            return (
                connectedMarketplace ||
                DEFAULT_INVENTORY_MARKETPLACE_IDS[countryKey]
            );
        };

        if (inventoryCountry === "global") {
            return [marketplaceForCountry("uk"), marketplaceForCountry("us")];
        }

        if (inventoryCountry === "uk" || inventoryCountry === "us" || inventoryCountry === "ca") {
            return [marketplaceForCountry(inventoryCountry)];
        }

        return [];
    }, [amazonConnections, inventoryCountry]);

    const invMonthYear = useMemo(() => {
        const { monthName, year } = getBackendCountryYearMonth();

        return {
            month: monthName.toLowerCase(),
            year: String(year),
        };
    }, [getBackendCountryYearMonth]);

    const inventoryInsightsReportCountry =
        platform === "global" ? selectedGlobalInventoryCountry : countryName;

    const showUsCurrentInventoryTable = useMemo(() => {
        return ["us", "usa", "united states"].includes(
            String(inventoryInsightsReportCountry || "").trim().toLowerCase()
        );
    }, [inventoryInsightsReportCountry]);

    const inventoryHeatmapUnitSalesDataKey: AgeingRiskUnitSalesDataKey =
        "salesLast30Days";

    const inventoryInsightsSalesLabel = "Sales Last 30 Days";

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
                marketplaceIds: inventoryMarketplaceIds,
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
    }, [
        inventoryCountry,
        inventoryMarketplaceIds,
        invMonthYear.month,
        invMonthYear.year,
        isMonthYearNA,
    ]);



    const fetchSingleMonthInventoryCurrentForInsights = useCallback(
        async (
            monthName: string,
            yearValue: string,
            countryValue: string,
            signal?: AbortSignal
        ): Promise<InventoryCurrentApiResponse> => {
            const token =
                typeof window !== "undefined"
                    ? localStorage.getItem("jwtToken")
                    : null;

            if (!token) {
                throw new Error("Missing token");
            }

            const url = new URL(`${baseURL}/inventory_current`);

            url.searchParams.set("country_key", String(countryValue).toLowerCase());
            url.searchParams.set("month_name", String(monthName).toLowerCase());
            url.searchParams.set("year", String(yearValue));

            const res = await fetch(url.toString(), {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${token}`,
                },
                cache: "no-store",
                signal,
            });

            const json = await res.json().catch(() => ({}));

            if (!res.ok) {
                throw new Error(
                    json?.message ||
                    json?.error ||
                    `Failed to fetch inventory for ${monthName} ${yearValue}`
                );
            }

            return json;
        },
        []
    );

    const fetchSingleMonthInventoryAgeSummaryForInsights = useCallback(
        async (
            monthName: string,
            yearValue: string,
            countryValue: string,
            signal?: AbortSignal
        ): Promise<InventoryAgeSummaryApiResponse> => {
            const token =
                typeof window !== "undefined"
                    ? localStorage.getItem("jwtToken")
                    : null;

            if (!token) {
                throw new Error("Missing token");
            }

            const url = new URL(`${baseURL}/inventory_current_age_summary`);

            url.searchParams.set("country_key", String(countryValue).toLowerCase());
            url.searchParams.set("month_name", String(monthName).toLowerCase());
            url.searchParams.set("year", String(yearValue));

            const res = await fetch(url.toString(), {
                method: "GET",
                headers: {
                    Authorization: `Bearer ${token}`,
                },
                cache: "no-store",
                signal,
            });

            const json = await res.json().catch(() => ({}));

            if (!res.ok) {
                throw new Error(
                    json?.message ||
                    json?.error ||
                    `Failed to fetch inventory age summary for ${monthName} ${yearValue}`
                );
            }

            return json;
        },
        []
    );

    const getInventoryInsightMonthRange = (
        currentMonth: string,
        currentYear: string,
        count = 6
    ) => {
        const monthIndex = inventoryMonthIndexMap[String(currentMonth).toLowerCase()];

        if (monthIndex == null || monthIndex < 0) {
            return [];
        }

        const startDate = new Date(Number(currentYear), monthIndex, 1);

        return Array.from({ length: count }, (_, index) => {
            const d = new Date(startDate);
            d.setMonth(startDate.getMonth() - (count - 1 - index));

            const monthName = d.toLocaleString("en-US", {
                month: "long",
            }).toLowerCase();

            return {
                month: monthName,
                year: String(d.getFullYear()),
            };
        });
    };

    const fetchInventoryInsights = useCallback(async () => {
        if (isMonthYearNA) {
            setInventoryInsightsData(null);
            setInventoryInsightsError(null);
            setInventoryInsightsLoading(false);
            return;
        }

        const ac = new AbortController();

        try {
            setInventoryInsightsLoading(true);
            setInventoryInsightsError(null);

            const country = inventoryCountry;
            const currentMonth = invMonthYear.month;
            const currentYear = invMonthYear.year;

            // ✅ Backend age-summary already returns Jan → selected month in month_summary.
            // So frontend should call only selected/current month.
            const selectedInventoryMonth = {
                month: currentMonth,
                year: currentYear,
            };

            const inventoryResults = await Promise.allSettled([
                fetchSingleMonthInventoryCurrentForInsights(
                    selectedInventoryMonth.month,
                    selectedInventoryMonth.year,
                    country,
                    ac.signal
                ),
            ]);

            const ageSummaryResults = await Promise.allSettled([
                fetchSingleMonthInventoryAgeSummaryForInsights(
                    selectedInventoryMonth.month,
                    selectedInventoryMonth.year,
                    country,
                    ac.signal
                ),
            ]);

            const inventoryResponses = inventoryResults
                .filter(
                    (result): result is PromiseFulfilledResult<InventoryCurrentApiResponse> =>
                        result.status === "fulfilled" && result.value?.success
                )
                .map((result) => result.value);

            const ageSummaryResponses = ageSummaryResults
                .filter(
                    (result): result is PromiseFulfilledResult<InventoryAgeSummaryApiResponse> =>
                        result.status === "fulfilled" && result.value?.success
                )
                .map((result) => result.value);

            if (inventoryResponses.length === 0) {
                throw new Error("No inventory insights data found");
            }

            // ✅ Save all months so dropdown/chart rebuild keeps June + previous months
            setInventoryInsightResponses(inventoryResponses);
            setInventoryAgeSummaryResponses(ageSummaryResponses);

            const builtInventoryInsights = buildInventoryInsightsFromResponses(
                inventoryResponses,
                ageSummaryResponses,
                country,
                profileHomeCurrency,
                selectedAgeingTrendBucket,
                selectedGlobalInventoryCountry
            );

            setInventoryInsightsData(builtInventoryInsights);
        } catch (e: any) {
            if (e?.name === "AbortError") return;

            setInventoryInsightsData(null);
            setInventoryInsightsError(
                e?.message || "Failed to fetch inventory insights"
            );
        } finally {
            setInventoryInsightsLoading(false);
        }
    }, [
        isMonthYearNA,
        inventoryCountry,
        invMonthYear.month,
        invMonthYear.year,
        profileHomeCurrency,
        selectedAgeingTrendBucket,
        selectedGlobalInventoryCountry,
        fetchSingleMonthInventoryCurrentForInsights,
        fetchSingleMonthInventoryAgeSummaryForInsights,
    ]);

    useEffect(() => {
        if (platform !== "global") return;
        if (!inventoryInsightResponses.length) return;

        const builtInventoryInsights = buildInventoryInsightsFromResponses(
            inventoryInsightResponses,
            inventoryAgeSummaryResponses,
            inventoryCountry,
            profileHomeCurrency,
            selectedAgeingTrendBucket,
            selectedGlobalInventoryCountry
        );

        setInventoryInsightsData(builtInventoryInsights);
    }, [
        platform,
        inventoryInsightResponses,
        inventoryAgeSummaryResponses,
        inventoryCountry,
        profileHomeCurrency,
        selectedAgeingTrendBucket,
        selectedGlobalInventoryCountry,
    ]);

    const handleAgeingTrendBucketChange = useCallback(
        (bucketValue: string) => {
            setSelectedAgeingTrendBucket(bucketValue);

            setInventoryInsightsData((prev) => {
                if (!prev) return prev;

                const isAllTrendSelected = bucketValue === "all";

                const selectedBucket =
                    AGEING_TREND_BUCKET_OPTIONS.find(
                        (bucket) => bucket.value === bucketValue
                    ) ||
                    AGEING_TREND_BUCKET_OPTIONS.find(
                        (bucket) => bucket.value === "365+ days"
                    ) ||
                    AGEING_TREND_BUCKET_OPTIONS[0];

                const selectedAgeSummaryResponses =
                    platform === "global"
                        ? getSelectedCountryAgeSummaryResponses(
                            inventoryAgeSummaryResponses,
                            selectedGlobalInventoryCountry
                        )
                        : inventoryAgeSummaryResponses;

                const selectedInventoryResponses =
                    platform === "global"
                        ? inventoryInsightResponses
                            .map((res) =>
                                getSelectedCountryInventoryResponse(
                                    res,
                                    selectedGlobalInventoryCountry
                                )
                            )
                            .filter(Boolean) as InventoryCurrentApiResponse[]
                        : inventoryInsightResponses;

                const trendDataFromSummary = isAllTrendSelected
                    ? []
                    : buildAgeingTrendDataFromSummary(
                        selectedAgeSummaryResponses,
                        selectedBucket.column
                    );

                const trendDataFromInventoryCurrent = isAllTrendSelected
                    ? []
                    : buildAgeingTrendDataFromInventoryCurrent(
                        selectedInventoryResponses,
                        selectedBucket.column
                    );

                const trendData =
                    trendDataFromSummary.length > 0
                        ? trendDataFromSummary
                        : trendDataFromInventoryCurrent;

                const trendAllSeriesData: AgeingTrendAllSeriesItem[] =
                    AGEING_TREND_BUCKET_OPTIONS.map((bucket) => {
                        const dataFromSummary = buildAgeingTrendDataFromSummary(
                            selectedAgeSummaryResponses,
                            bucket.column
                        );

                        const dataFromInventoryCurrent =
                            buildAgeingTrendDataFromInventoryCurrent(
                                selectedInventoryResponses,
                                bucket.column
                            );

                        return {
                            bucketValue: bucket.value,
                            bucketLabel: bucket.label,
                            color: bucket.color,
                            data:
                                dataFromSummary.length > 0
                                    ? dataFromSummary
                                    : dataFromInventoryCurrent,
                        };
                    });

                return {
                    ...prev,
                    trendSelectedBucket: isAllTrendSelected
                        ? "all"
                        : selectedBucket.value,

                    trendData,

                    trendLineColor: isAllTrendSelected
                        ? "#B75A5A"
                        : selectedBucket.color,

                    trendAllSeriesData,
                };
            });
        },
        [inventoryAgeSummaryResponses, inventoryInsightResponses, platform,
            selectedGlobalInventoryCountry,]
    );

    useEffect(() => {
        if (activeTab !== "inventory") return;
        if (inventoryInsightsData) return;
        if (inventoryInsightsLoading) return;
        if (isMonthYearNA) return;

        void fetchInventoryInsights().then(() => {
            triggerCachePost();
        });
    }, [
        activeTab,
        inventoryInsightsData,
        inventoryInsightsLoading,
        isMonthYearNA,
        fetchInventoryInsights,
        triggerCachePost,
    ]);

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
        return toNumberSafe(
            targetSummaries[targetSummaryCountry as keyof typeof targetSummaries]?.target_sales ?? 0
        );
    }, [targetSummaries, targetSummaryCountry]);


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
                ["Gross Sales", "Net Sales", "Cost of Ads", "CM2 Profit", "Promotions"].includes(label);

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

    const formatAmountWithPct = useCallback(
        (
            amount: number | null | undefined,
            pct: number | null | undefined,
            amountLabel: string,
            absoluteAmount = false
        ) => {
            const amountToShow = absoluteAmount
                ? Math.abs(toNumberSafe(amount ?? 0))
                : toNumberSafe(amount ?? 0);
            const pctToShow = Math.abs(toNumberSafe(pct ?? 0));

            return `${formatDisplayAmount(amountToShow, amountLabel)} (${fmtPct2(pctToShow)})`;
        },
        [formatDisplayAmount]
    );

    const formatCurrentAmountWithPct = useCallback(
        (
            amount: number | null | undefined,
            pct: number | null | undefined,
            amountLabel: string,
            absoluteAmount = false
        ) => {
            const amountToShow = absoluteAmount
                ? Math.abs(toNumberSafe(amount ?? 0))
                : toNumberSafe(amount ?? 0);
            const pctToShow = Math.abs(toNumberSafe(pct ?? 0));

            return (
                <span className="inline-flex items-baseline gap-1 leading-tight">
                    <span>{formatDisplayAmount(amountToShow, amountLabel)}</span>
                    <span className="text-[10px] 2xl:text-xs text-charcoal-400 font-medium">
                        ({fmtPct2(pctToShow)})
                    </span>
                </span>
            );
        },
        [formatDisplayAmount]
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

    const buildAdsMetric = (row: any) => {
        const adsSpend = toNumberSafe(
            row?.ads_spend ??
            row?.total_ads ??
            row?.advertising_fees ??
            row?.advertising_total ??
            0
        );

        const prevAdsSpend = toNumberSafe(
            row?.previous_ads_spend ??
            row?.prev_ads_spend ??
            row?.ads_spend_prev ??
            0
        );

        const growthPct =
            row?.ads_spend_growth_pct != null
                ? Number(row.ads_spend_growth_pct)
                : prevAdsSpend
                    ? safeDeltaPct(adsSpend, prevAdsSpend)
                    : 0;

        const sign = Number(growthPct) > 0 ? "+" : "";
        const growthText = `${sign}${Number(growthPct || 0).toFixed(2)}%`;

        return {
            label: "Ads",
            value: `${formatDisplayAmount(adsSpend)} (${growthText})`,
            color: "#414042",
        };
    };

    const buildDrawerMetricsWithAds = (
        metrics: { label: string; value: string; color?: string }[] = [],
        sourceRow: any
    ) => {
        const baseMetrics = metrics.filter((m) => {
            const label = m.label.trim().toLowerCase();

            return label !== "ads";
        });

        return [
            ...baseMetrics,
            ...(sourceRow ? [buildAdsMetric(sourceRow)] : []),
        ];
    };

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
            dataOnlyRefresh = false,

            // ✅ ADD THIS
            manualAiRefresh = false,
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

                    // ✅ Manual refresh should hit backend as true
                    manual_ai_refresh: manualAiRefresh ? "true" : "false",

                    // ✅ Manual AI refresh must never behave like data-only refresh
                    data_only_refresh: manualAiRefresh ? "false" : dataOnlyRefresh ? "true" : "false",
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

                setLiveBiPayload((prev: any) => {
                    // ✅ Manual AI refresh must replace parent payload completely.
                    // Otherwise page reload will hydrate old Business Summary again.
                    if (manualAiRefresh || !dataOnlyRefresh || !prev) {
                        return json;
                    }

                    return {
                        ...json,

                        // keep existing Business Summary + AI recommendation data
                        // ONLY for top-right data-only refresh
                        overall_summary: prev.overall_summary,
                        overall_actions: prev.overall_actions,
                        recommended_actions_mtd: prev.recommended_actions_mtd,
                        portfolio_recommendation: prev.portfolio_recommendation,
                        remaining_skus_recommendation: prev.remaining_skus_recommendation,
                        remaining_skus_ads_recommendation: prev.remaining_skus_ads_recommendation,
                        remaining_skus_inventory_recommendation: prev.remaining_skus_inventory_recommendation,
                        remaining_skus_journey_summary: prev.remaining_skus_journey_summary,
                        remaining_skus_block: prev.remaining_skus_block,
                        sku_strategy_actions: prev.sku_strategy_actions,
                        ai_insights: prev.ai_insights,
                        product_journey: prev.product_journey,
                        ai_last_refreshed_at: prev.ai_last_refreshed_at,
                    };
                });
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

    const runDashboardLoadWithSteps = useCallback(async ({
        dataOnlyRefresh = false,
        forceAdsReportSync = false,
    }: {
        dataOnlyRefresh?: boolean;
        forceAdsReportSync?: boolean;
    } = {}) => {
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
                void runAdsBackgroundSync({ forceReportSync: forceAdsReportSync });
            } else {
                setStep(1, "MTD Fetching", 48, "Skipping ads fetch for Shopify-only mode...");
            }

            setStep(1, "MTD Fetching", 62, "Fetching Amazon MTD data...");
            await fetchAmazon();

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

            setStep(2, "Inventory Fetch", 20, "Fetching aged, AWD, and current inventory...");
            await fetchInventory();

            setStep(2, "Inventory Fetch", 60, "Fetching inventory insights.");
            await fetchInventoryInsights();

            setStep(2, "Inventory Fetch", 100, "Inventory ready");
            markStepComplete(2);

            // ✅ Now /live_mtd_bi runs AFTER all inventory APIs
            if (showLiveBI) {
                setStep(3, "Plotting Graph", 20, "Fetching Live BI data.");

                await fetchLiveBiPayload({
                    startDay: selectedStartDay,
                    endDay: selectedEndDay,
                    generateInsights: false,
                    skipLoader: true,

                    // ✅ Browser/page reload must fetch backend cached AI summary.
                    // Do not preserve stale parent AI summary here.
                    dataOnlyRefresh,
                });
            } else {
                setStep(3, "Plotting Graph", 20, "Live BI not enabled, skipping.");
            }

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
        fetchInventoryInsights,
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

    // useEffect(() => {
    //     if (typeof window === "undefined") return;

    //     const key = "live-dashboard-cache-init";

    //     if (!localStorage.getItem(key)) {
    //         localStorage.setItem(key, "initialized");
    //     }
    // }, []);

    const hasSavedRef = useRef(false);
    const didBootstrapRef = useRef<string | null>(null);
    const isRangeChangeRef = useRef(false);

    const saveDashboardCacheToBackend = useCallback(
        async (payload: DashboardCachePayload): Promise<void> => {
            if (typeof window === "undefined") return;

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
                    savedAt: Date.now(),
                    cachePayload: payload,
                }),
            });

            const json = await res.json().catch(() => null);

            if (!res.ok || !json?.success) {
                throw new Error(
                    json?.error || `Failed to save dashboard cache (${res.status})`
                );
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

    // const ensureLocalStorageThenSave = async (payload: DashboardCachePayload) => {
    //     if (typeof window === "undefined") return;

    //     localStorage.setItem(
    //         liveCacheKey,
    //         JSON.stringify({
    //             ...payload,
    //             savedAt: Date.now(),
    //         })
    //     );

    //     await saveDashboardCacheToBackend(payload);
    // };

    const formatAppliedRangeLabel = (start: number | null, end: number | null) => {
        if (start == null || end == null) return "Select Date Range";

        const { monthName } = getRegionYearMonth(activeDateRegion);
        const shortMonth = monthName.slice(0, 3);

        return `${shortMonth} ${start}-${end}`;
    };

    type DashboardCachePayload = ReturnType<typeof buildDashboardCachePayload>;

    const normalizeRefreshTimestamp = useCallback((value: any): number | null => {
        if (value === undefined || value === null || value === "") return null;

        const numeric = Number(value);
        if (Number.isFinite(numeric) && numeric > 0) {
            // Support both milliseconds and Unix seconds.
            return numeric < 10_000_000_000 ? numeric * 1000 : numeric;
        }

        const parsedDate = new Date(value).getTime();
        return Number.isFinite(parsedDate) && parsedDate > 0 ? parsedDate : null;
    }, []);

    const applyDashboardCachePayload = useCallback((parsed: any) => {
        setData(parsed?.data ?? null);
        setBiDailySeries(parsed?.biDailySeries ?? null);
        setBiPeriods(parsed?.biPeriods ?? null);
        setLiveBiPayload(parsed?.liveBiPayload ?? null);

        setBiAlignedTotals(parsed?.biAlignedTotals ?? null);

        setInvRows(parsed?.invRows ?? []);
        setInventoryAlerts(parsed?.inventoryAlerts ?? {});
        setInventoryInsightsData(parsed?.inventoryInsightsData ?? null);
        setInventoryInsightsError(parsed?.inventoryInsightsError ?? null);
        setSelectedAgeingTrendBucket(parsed?.selectedAgeingTrendBucket ?? "365+ days");
        setInventoryInsightsLoading(false);
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
        const payloadRefreshAt =
            normalizeRefreshTimestamp(parsed?.lastUpdatedAt) ??
            normalizeRefreshTimestamp(parsed?.lastRefreshAt) ??
            normalizeRefreshTimestamp(parsed?.savedAt);

        if (payloadRefreshAt) {
            setLastRefreshAt(payloadRefreshAt);
        }

        setBiLoading(false);
        setSummaryLoading(false);
        setBiError(null);
    }, [normalizeRefreshTimestamp]);


    const getDashboardCacheFromBackend = useCallback(
        async (
            rangeStartDay: number | null = selectedStartDay,
            rangeEndDay: number | null = selectedEndDay
        ) => {
            if (typeof window === "undefined") return null;

            const token = localStorage.getItem("jwtToken");
            if (!token) return null;

            const params = new URLSearchParams({
                country: liveDashboardCountry,
                platform: String(platform || "").toLowerCase(),
                region: String(activeDateRegion || ""),
            });
            if (rangeStartDay != null) {
                params.set("start_day", String(rangeStartDay));
            }

            if (rangeEndDay != null) {
                params.set("end_day", String(rangeEndDay));
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

            const payload = json?.data?.payload ?? null;

            return {
                found: Boolean(json?.found && payload),
                payload,
                updatedAt: json?.data?.updated_at ?? json?.data?.created_at ?? null,
            };
        }, [
        liveDashboardCountry,
        platform,
        activeDateRegion,
        selectedStartDay,
        selectedEndDay,
    ]);

    const loadRangeFromCache = useCallback(
        async (
            startDay: number | null,
            endDay: number | null
        ) => {

            const cacheResult = await getDashboardCacheFromBackend(
                startDay,
                endDay
            );

            if (cacheResult?.found && cacheResult.payload) {

                applyDashboardCachePayload(
                    cacheResult.payload
                );

                const cacheRefreshAt =
                    normalizeRefreshTimestamp(cacheResult.payload?.lastUpdatedAt) ??
                    normalizeRefreshTimestamp(cacheResult.payload?.lastRefreshAt) ??
                    normalizeRefreshTimestamp(cacheResult.payload?.savedAt) ??
                    normalizeRefreshTimestamp(cacheResult.updatedAt);

                if (cacheRefreshAt) {
                    setLastRefreshAt(cacheRefreshAt);
                }

                setBiLoading(false);
                setSummaryLoading(false);
                setBiError(null);

                return true;
            }

            return false;
        },
        [
            getDashboardCacheFromBackend,
            applyDashboardCachePayload,
            normalizeRefreshTimestamp
        ]
    );

    // const liveCacheKey = useMemo(() => {
    //     const country =
    //         platform === "amazon-us" ? "us" :
    //             platform === "amazon-uk" ? "uk" :
    //                 platform === "amazon-ca" ? "ca" :
    //                     "global";

    //     return `live-dashboard-cache:${country}:${activeDateRegion}`;
    // }, [platform, activeDateRegion]);

    // const lastRefreshKey = useMemo(() => {
    //     return `${liveCacheKey}:last-updated-at`;
    // }, [liveCacheKey]);

    // const restoreLiveCacheFromLocalStorage = useCallback(() => {
    //     if (typeof window === "undefined") return false;

    //     const raw = localStorage.getItem(liveCacheKey);
    //     if (!raw) return false;

    //     try {
    //         const parsed = JSON.parse(raw);

    //         applyDashboardCachePayload(parsed);

    //         const normalizeRefreshTimestamp = (value: any): number | null => {
    //             if (!value) return null;

    //             const numeric = Number(value);
    //             if (Number.isFinite(numeric) && numeric > 0) {
    //                 return numeric;
    //             }

    //             const parsedDate = new Date(value).getTime();
    //             return Number.isFinite(parsedDate) && parsedDate > 0 ? parsedDate : null;
    //         };

    //         const restoredLastRefreshAt =
    //             normalizeRefreshTimestamp(parsed?.lastRefreshAt) ??
    //             normalizeRefreshTimestamp(parsed?.savedAt) ??
    //             normalizeRefreshTimestamp(localStorage.getItem(lastRefreshKey));

    //         setLastRefreshAt(restoredLastRefreshAt);

    //         if (restoredLastRefreshAt) {
    //             localStorage.setItem(lastRefreshKey, String(restoredLastRefreshAt));
    //         }

    //         return true;
    //     } catch (err) {
    //         console.error("Failed to restore live cache from localStorage:", err);
    //         return false;
    //     }
    // }, [liveCacheKey, lastRefreshKey, applyDashboardCachePayload]);

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
        try {
            isManualRefreshRef.current = true;
            shouldPostCacheRef.current = false;

            await fetchCountryTime();

            await runDashboardLoadWithSteps({
                dataOnlyRefresh: true,
                forceAdsReportSync: true,
            });

            const refreshedAt = Date.now();
            setLastRefreshAt(refreshedAt);

            shouldPostCacheRef.current = true;
            setCacheSaveTick((x) => x + 1);
        } catch (err) {
            console.error("Hard refresh failed:", err);
            isManualRefreshRef.current = false;
            shouldPostCacheRef.current = false;
        }
    }, [
        fetchCountryTime,
        runDashboardLoadWithSteps,
    ]);

    useEffect(() => {

        if (isRangeChangeRef.current) {
            isRangeChangeRef.current = false;
            return;
        }

        if (!fxReady) return;

        const bootstrapKey = [
            liveDashboardCountry,
            platform,
            activeDateRegion,
            selectedStartDay ?? "na",
            selectedEndDay ?? "na",
        ].join(":");

        if (didBootstrapRef.current === bootstrapKey) return;

        let cancelled = false;

        const bootstrapDashboard = async () => {
            try {
                const cacheResult = await getDashboardCacheFromBackend();

                if (cancelled) return;

                didBootstrapRef.current = bootstrapKey;

                if (cacheResult?.found && cacheResult.payload) {
                    shouldPostCacheRef.current = false;
                    isManualRefreshRef.current = false;

                    applyDashboardCachePayload(cacheResult.payload);

                    const cacheRefreshAt =
                        normalizeRefreshTimestamp(cacheResult.payload?.lastUpdatedAt) ??
                        normalizeRefreshTimestamp(cacheResult.payload?.lastRefreshAt) ??
                        normalizeRefreshTimestamp(cacheResult.payload?.savedAt) ??
                        normalizeRefreshTimestamp(cacheResult.updatedAt);

                    setLastRefreshAt(cacheRefreshAt);

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

                isManualRefreshRef.current = true;
                shouldPostCacheRef.current = true;
                setCacheSaveTick((x) => x + 1);
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
        liveDashboardCountry,
        platform,
        activeDateRegion,
        getDashboardCacheFromBackend,
        applyDashboardCachePayload,
        normalizeRefreshTimestamp,
        runDashboardLoadWithSteps,
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
            promotions: Math.abs(toNumberSafe(prevTotals?.promotional_rebates ?? 0)),
            promotionsPct: Math.abs(
                toNumberSafe(prevTotals?.net_sales ?? 0)
                    ? (toNumberSafe(prevTotals?.promotional_rebates ?? 0) /
                        toNumberSafe(prevTotals?.net_sales ?? 0)) * 100
                    : toNumberSafe(prevTotals?.promotional_rebates_percentage ?? 0)
            ),
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
        const userMonthlyTargetForRegion = toNumberSafe(
            targetSummaries[
                targetSummaryCountry as keyof typeof targetSummaries
            ]?.target_sales ?? 0
        );

        const globalPrevFullMonthSales =
            globalPrevFullMonthNetSalesDisp > 0
                ? globalPrevFullMonthNetSalesDisp
                : globalPrevNetDisp;

        const globalTarget = userMonthlyTargetForRegion;

        const global: RegionMetrics = {
            mtdUSD: globalCurrNetDisp,
            lastMonthToDateUSD: globalPrevNetDisp,
            lastMonthTotalUSD: globalPrevFullMonthSales,
            targetUSD: globalTarget,
            decTargetUSD: globalTarget,
        };

        const prevTargetSummaryForRegion =
            prevTargetSummaries[
            targetSummaryCountry as keyof typeof prevTargetSummaries
            ];

        const prevTargetSummaryNetSales = toNumberSafe(
            prevTargetSummaryForRegion?.net_sales_total ?? 0
        );

        const prevTargetSummaryNetSalesDisp =
            prevTargetSummaryNetSales > 0
                ? convertToDisplayCurrency(
                    prevTargetSummaryNetSales,
                    currencyForCountry(targetSummaryCountry)
                )
                : 0;

        const ukPrevFullMonthSales =
            prevTargetSummaryNetSalesDisp > 0
                ? prevTargetSummaryNetSalesDisp
                : prevFullMonthNetSalesDisp > 0
                    ? prevFullMonthNetSalesDisp
                    : amazonPrevNetDisp;

        const ukTarget =
            userMonthlyTargetForRegion > 0
                ? userMonthlyTargetForRegion
                : ukPrevFullMonthSales;

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
        prevTargetSummaries,
        gbpToUsd,
        platform,
        targetSummaryCountry,
        convertToDisplayCurrency,
        chooseLastMonthTotal,
        prorateToDate,
    ]);



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
            const miscTransaction = Number(r.misc_transaction ?? r.misc_transactions ?? 0);

            const taxAndCredits = Number(
                r.other_transactions ??
                r.other_transaction_fees ??
                r.tax_and_credits ??
                tax + credits
            );

            return {
                sno: isTotal ? undefined : (idx ?? 0) + 1,
                sku: String(r.sku ?? ""),
                product_name: getSkuwiseDisplayProductName(r),
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
                gross_sales: Number(r.gross_sales ?? 0),
                refund_sales: Number(r.refund_sales ?? 0),
                net_sales_tax_and_credits: Number(r.tax_and_credits ?? 0),
                net_sales: Number(r.net_sales ?? 0),

                promotional_rebates: Number(r.promotional_rebates ?? 0),
                promotional_rebates_percentage: Number(r.promotional_rebates_percentage ?? 0),

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
                misc_transaction: miscTransaction,

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
        "gross_sales",
        "refund_sales",
        "net_sales",
        "promotional_rebates",
        "promotional_rebates_percentage",
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
        const miscTransaction = toNumberSafe(raw?.misc_transaction ?? raw?.misc_transactions);

        const taxAndCredits = toNumberSafe(
            raw?.tax_and_credits ??
            raw?.taxes_and_credits ??
            raw?.tex_and_credits ??
            tax + credits
        );
        const otherTransactions = toNumberSafe(
            raw?.other_transactions ??
            raw?.other_transaction_fees ??
            taxAndCredits
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
            gross_sales: toNumberSafe(raw?.gross_sales),
            refund_sales: toNumberSafe(raw?.refund_sales),
            net_sales_tax_and_credits: taxAndCredits,
            net_sales: toNumberSafe(raw?.net_sales),

            promotional_rebates: toNumberSafe(raw?.promotional_rebates),
            promotional_rebates_percentage: toNumberSafe(raw?.promotional_rebates_percentage),

            tax,
            credits,
            tax_and_credits: taxAndCredits,
            net_taxes: tax,
            other_transactions: otherTransactions,
            misc_transaction: miscTransaction,

            cogs: toNumberSafe(raw?.cogs),
            fba_fees: toNumberSafe(raw?.fba_fees),
            selling_fees: toNumberSafe(raw?.selling_fees),

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
        const totalMiscTransaction = rows.reduce(
            (s, r) => s + toNumberSafe(r.misc_transaction),
            0
        );
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
            misc_transaction: totalMiscTransaction,

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
                row?.isTotal ||
                    String(row?.sku || "").toUpperCase() === "GRAND_TOTAL" ||
                    String(row?.product_name || "").trim().toLowerCase() === "grand total"
                    ? "Total"
                    : getSkuwiseDisplayProductName(row),
        }));
    }, [monthlySkuwiseRows]);

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

    const getProductwiseOtherTransactionsTotal = useCallback(
        (row: Partial<MonthlySkuwiseRow>) => {
            const fallback = toNumber(
                (row as any).other_transactions ??
                (row as any).other_transaction_fees ??
                (row as any).tax_and_credits
            );

            if (!isUsPnlSkuLayout) return fallback;

            const computed =
                toNumber((row as any).credits ?? (row as any).net_credits) +
                toNumber((row as any).misc_transaction ?? (row as any).misc_transactions) -
                Math.abs(toNumber((row as any).tax ?? (row as any).net_taxes));

            return computed !== 0 || fallback === 0 ? computed : fallback;
        },
        [isUsPnlSkuLayout]
    );

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
                case "gross_sales":
                case "refund_sales":
                case "net_sales_tax_and_credits":
                case "net_sales":
                case "promotional_rebates":
                case "promotional_rebates_percentage":
                case "cogs":
                case "tax":
                case "credits":
                case "tax_and_credits":
                case "misc_transaction":
                case "cm1_profit_per_unit":
                case "cm1_profit_per":
                case "profit":
                case "ads_spend":
                case "acos":
                case "cm2_profit_per_unit":
                case "cm2_profit_per":
                case "cm2_profit":
                    return toNumber((row as any)[key]);

                case "other_transactions":
                    return getProductwiseOtherTransactionsTotal(row);

                case "product_name":
                    return getSkuwiseDisplayProductName(row).toLowerCase();

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

        const average = (key: keyof MonthlySkuwiseRow) =>
            rest.length
                ? rest.reduce((acc, r) => acc + (Number((r as any)[key]) || 0), 0) / rest.length
                : 0;

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
            gross_sales: sum("gross_sales"),
            refund_sales: sum("refund_sales"),
            net_sales: othersNetSales,
            promotional_rebates: sum("promotional_rebates"),
            promotional_rebates_percentage: average("promotional_rebates_percentage"),

            cogs: sum("cogs"),
            fba_fees: sum("fba_fees"),
            selling_fees: sum("selling_fees"),

            tax: sum("tax"),
            credits: sum("credits"),
            tax_and_credits: sum("tax_and_credits"),
            misc_transaction: sum("misc_transaction"),
            other_transactions: rest.reduce(
                (acc, r) => acc + getProductwiseOtherTransactionsTotal(r),
                0
            ),

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
    }, [
        monthlySkuwiseRowsDisplay,
        plSortConfig,
        showAllMtdProductwiseRows,
        getProductwiseOtherTransactionsTotal,
    ]);

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
        const previousGlobalRows = Array.isArray(previousSkuwiseGlobalData?.skuwise_items_global)
            ? previousSkuwiseGlobalData.skuwise_items_global
            : [];
        const previousGlobalGrand = getGrandTotalRow(previousGlobalRows);
        const currentPromotionsRaw = pickPromotionalRebates(
            globalGrand,
            (data as any)?.derived_totals_global,
            (data as any)?.derived_totals
        );
        const previousPromotionsRaw = pickPromotionalRebates(
            prevDerived,
            prevAligned,
            previousGlobalGrand,
            data?.previous_period?.totals
        );
        const previousPromotionsNetSales = pickFirstNonZeroNumber(
            prevDerived.net_sales,
            prevAligned.total_previous_net_sales,
            previousGlobalGrand.net_sales,
            data?.previous_period?.totals?.net_sales
        );

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

            promotions: Math.abs(currentPromotionsRaw),
            prevPromotions: Math.abs(previousPromotionsRaw),
            promotionsPct: calculatePromotionalRebatesPct(
                currentPromotionsRaw,
                toNumber(globalGrand.net_sales),
                globalGrand,
                (data as any)?.derived_totals_global,
                (data as any)?.derived_totals
            ),
            prevPromotionsPct: calculatePromotionalRebatesPct(
                previousPromotionsRaw,
                previousPromotionsNetSales,
                prevDerived,
                prevAligned,
                previousGlobalGrand,
                data?.previous_period?.totals
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

        const promotions = Math.abs(toNumber(row.promotional_rebates));
        const promotionsPct = Math.abs(
            netSales
                ? (toNumber(row.promotional_rebates) / netSales) * 100
                : toNumber(row.promotional_rebates_percentage)
        );

        return {
            units,
            netSales,
            asp,
            costOfAds,
            tacos,
            cm2Profit,
            cm2MarginPct,
            promotions,
            promotionsPct,
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
        const previousGlobalRows = Array.isArray(previousSkuwiseGlobalData?.skuwise_items_global)
            ? previousSkuwiseGlobalData.skuwise_items_global
            : [];
        const previousGlobalGrand = getGrandTotalRow(previousGlobalRows);

        const prevNetSales = pickFirstNonZeroNumber(
            prevDerived.net_sales,
            prevAligned.total_previous_net_sales,
            previousGlobalGrand.net_sales
        );
        const prevAds = toNumber(
            prevAligned.total_previous_advertising ??
            prevDerived.advertising_fees
        );
        const prevPromotionsRaw = pickPromotionalRebates(
            prevDerived,
            prevAligned,
            previousGlobalGrand
        );
        const prevPromotions = Math.abs(prevPromotionsRaw);

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

            promotions: prevPromotions,
            promotionsPct: calculatePromotionalRebatesPct(
                prevPromotionsRaw,
                prevNetSales,
                prevDerived,
                prevAligned,
                previousGlobalGrand
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

    const { SKUWISE_LEFT_COLS, SKUWISE_GROUPS, SKUWISE_SINGLE_COLS } = useMemo(
        () => buildSkuwiseTableColumns(formatDisplayAmount, { isUsSkuLayout: isUsPnlSkuLayout }),
        [formatDisplayAmount, isUsPnlSkuLayout]
    );

    const PRODUCTWISE_GROUP_IDS = useMemo(
        () => [
            "quantity",
            "net_sales",
            ...(isUsPnlSkuLayout ? [] : ["promotions"]),
            "marketplace_fees",
            "other_transactions",
            "profit",
            "ads_spend",
            "cm2_profit",
        ],
        [isUsPnlSkuLayout]
    );

    const productwiseHasExpandedGroups = useMemo(() => {
        return PRODUCTWISE_GROUP_IDS.some(
            (groupId) => productwiseCollapsed[groupId] === false
        );
    }, [productwiseCollapsed, PRODUCTWISE_GROUP_IDS]);

    const buildProductwiseCollapsedState = useCallback(
        (collapsedValue: boolean) => {
            return PRODUCTWISE_GROUP_IDS.reduce<Record<string, boolean>>(
                (acc, groupId) => {
                    acc[groupId] = collapsedValue;
                    return acc;
                },
                {}
            );
        },
        [PRODUCTWISE_GROUP_IDS]
    );

    const handleToggleProductwiseAllColumns = useCallback(() => {
        setProductwiseCollapsed((prev) => {
            const currentlyAllExpanded =
                PRODUCTWISE_GROUP_IDS.length > 0 &&
                PRODUCTWISE_GROUP_IDS.every((groupId) => prev[groupId] === false);

            const nextExpanded = !currentlyAllExpanded;
            const next = buildProductwiseCollapsedState(!nextExpanded);

            setProductwiseAllColumnsExpanded(nextExpanded);
            return next;
        });
    }, [PRODUCTWISE_GROUP_IDS, buildProductwiseCollapsedState]);

    useEffect(() => {
        setProductwiseCollapsed(buildProductwiseCollapsedState(true));
        setProductwiseAllColumnsExpanded(false);
    }, [platform, globalMtdView, buildProductwiseCollapsedState]);

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

    const resolvedMtdHome =
        selectedTargetRegion?.mtdUSD ||
        amazonCurrNetDisp ||
        0;

    const resolvedLastMonthToDateHome =
        selectedTargetRegion?.lastMonthToDateUSD ||
        amazonPrevNetDisp ||
        0;

    const resolvedLastMonthTotalHome =
        selectedTargetRegion?.lastMonthTotalUSD ||
        prevFullMonthNetSalesDisp ||
        amazonPrevNetDisp ||
        0;

    // ✅ Main fix: if current target is missing, use previous full-month net sales
    const resolvedTargetHome =
        selectedTargetRegion?.targetUSD && selectedTargetRegion.targetUSD > 0
            ? selectedTargetRegion.targetUSD
            : userMonthlyTargetHome && userMonthlyTargetHome > 0
                ? userMonthlyTargetHome
                : resolvedLastMonthTotalHome;

    const targetData: RegionMetrics = {
        mtdUSD: resolvedMtdHome,
        lastMonthToDateUSD: resolvedLastMonthToDateHome,
        lastMonthTotalUSD: resolvedLastMonthTotalHome,
        targetUSD: resolvedTargetHome,
        decTargetUSD: resolvedTargetHome,
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
        const finalLastRefreshAt = lastRefreshAt ?? Date.now();

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

            inventoryInsightsData,
            inventoryInsightsError,
            selectedAgeingTrendBucket,

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
        inventoryInsightsData,
        inventoryInsightsError,
        selectedAgeingTrendBucket,
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
    ]);

    // const saveLiveCacheToLocalStorage = useCallback((cachePayload?: any) => {
    //     if (typeof window === "undefined") return;

    //     const payloadToSave = cachePayload ?? buildDashboardCachePayload();

    //     localStorage.setItem(
    //         liveCacheKey,
    //         JSON.stringify({
    //             ...payloadToSave,
    //             savedAt: Date.now(),
    //         })
    //     );
    // }, [buildDashboardCachePayload, liveCacheKey]);

    // useEffect(() => {
    //     if (typeof window === "undefined") return;
    //     if (platform !== "global") return;

    //     localStorage.removeItem(liveCacheKey);
    //     localStorage.removeItem("live-dashboard-cache:global:Global");
    // }, [platform, liveCacheKey]);


    // useEffect(() => {
    //     if (!shouldPostCacheRef.current || !isManualRefreshRef.current) return;
    //     if (!data && !liveBiPayload && !invRows.length) return;

    //     const shouldPersist =
    //         !pageLoading &&
    //         !dashboardBusy &&
    //         !loading &&
    //         !biLoading &&
    //         !invLoading &&
    //         !monthlySpLoading &&
    //         !shopifyLoading;

    //     if (!shouldPersist) return;

    //     const payload = buildDashboardCachePayload();

    //     try {
    //         localStorage.setItem(
    //             liveCacheKey,
    //             JSON.stringify({
    //                 ...payload,
    //                 savedAt: Date.now(),
    //             })
    //         );

    //         localStorage.setItem("live-dashboard-cache-init", "initialized");
    //     } catch (e) {
    //         console.error("Failed to write local cache:", e);
    //     }

    //     saveDashboardCacheToBackend(payload)
    //         .then(() => {
    //             shouldPostCacheRef.current = false;
    //             isManualRefreshRef.current = false;
    //         })
    // }, [
    //     buildDashboardCachePayload,
    //     saveDashboardCacheToBackend,
    //     liveCacheKey,
    //     lastRefreshKey,
    //     pageLoading,
    //     dashboardBusy,
    //     loading,
    //     biLoading,
    //     invLoading,
    //     monthlySpLoading,
    //     shopifyLoading,
    //     data,
    //     liveBiPayload,
    //     invRows,
    //     monthlySpRows,
    //     monthlySpTotalSpend,
    //     cacheSaveTick,
    // ]);


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

        saveDashboardCacheToBackend(payload)
            .then(() => {
                shouldPostCacheRef.current = false;
                isManualRefreshRef.current = false;
            })
            .catch((err) => {
                console.error("Failed to save dashboard cache to backend:", err);
            });
    }, [
        buildDashboardCachePayload,
        saveDashboardCacheToBackend,
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

    const ADS_SIGN_PLUS = new Set([
        "gross_sales",
        "net_sales",
        "credits",
        "tax_and_credits",
        "misc_transaction",
        "other_transactions",
        "quantity",
        "total_quantity",
    ]);

    const ADS_SIGN_MINUS = new Set([
        "return_quantity",
        "ads_spend",
        "product_spend",
        "display_spend",
        "brand_spend",
        "refund_sales",
        "net_sales_tax_and_credits",
        "promotional_rebates",
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
        | "productwise"
        | "summary"
        | "inventory";

    const TOP_TABS: { id: TopTab; label: string }[] = [
        { id: "summary", label: "AI Insights & Recommendations" },
        { id: "live", label: "MTD Sales" },
        { id: "productwise", label: "P&L Breakdown" },
        { id: "inventory", label: "Inventory Insights" },
    ];

    const HASH_TO_TAB: Record<string, TopTab> = {
        "live-sales": "live",
        "ai-insights": "summary",
        // "mtd-pl": "productwise",
        "pnl-mtd": "productwise",
        "inventory-insights": "inventory",
    };

    const TAB_TO_HASH: Record<TopTab, string> = {
        live: "live-sales",
        summary: "ai-insights",
        productwise: "pnl-mtd",
        inventory: "inventory-insights",
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



    const syncTabToHash = useCallback((tab: TopTab) => {
        if (typeof window === "undefined") return;

        const hash = TAB_TO_HASH[tab];
        if (!hash) return;

        const nextUrl = `${window.location.pathname}#${hash}`;

        // Only update URL. Do not trigger scroll on manual tab switch.
        window.history.replaceState(null, "", nextUrl);
    }, []);

    const scrollDashboardPageToTop = useCallback(() => {
        if (typeof window === "undefined") return;

        const target = tabTopRef.current || pageTopRef.current;

        const scrollParents: HTMLElement[] = [];

        let parent = target?.parentElement || null;

        while (parent) {
            const style = window.getComputedStyle(parent);
            const overflowY = style.overflowY;

            const canScroll =
                (overflowY === "auto" || overflowY === "scroll") &&
                parent.scrollHeight > parent.clientHeight;

            if (canScroll) {
                scrollParents.push(parent);
            }

            parent = parent.parentElement;
        }

        window.scrollTo({
            top: 0,
            left: 0,
            behavior: "auto",
        });

        document.documentElement.scrollTop = 0;
        document.body.scrollTop = 0;

        scrollParents.forEach((el) => {
            el.scrollTop = 0;
        });
    }, []);

    useEffect(() => {
        if (!shouldScrollTabTopRef.current) return;

        shouldScrollTabTopRef.current = false;

        const scrollNow = () => {
            scrollDashboardPageToTop();
        };

        scrollNow();

        const r1 = requestAnimationFrame(scrollNow);
        const t1 = window.setTimeout(scrollNow, 50);
        const t2 = window.setTimeout(scrollNow, 150);
        const t3 = window.setTimeout(scrollNow, 350);

        return () => {
            cancelAnimationFrame(r1);
            window.clearTimeout(t1);
            window.clearTimeout(t2);
            window.clearTimeout(t3);
        };
    }, [activeTab, scrollDashboardPageToTop]);

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
        return toNumberSafe(
            prevTargetSummaries[
                targetSummaryCountry as keyof typeof prevTargetSummaries
            ]?.target_sales ?? 0
        );
    }, [prevTargetSummaries, targetSummaryCountry]);

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

            formatter: (val: number) =>
                formatCurrentAmountWithPct(
                    val,
                    shouldShowDummyUi
                        ? dummyStatData.cm2ProfitPct.current
                        : isStickyGlobal
                            ? stickyTableTotals.cm2MarginPct
                            : totalRowCm2Margins,
                    "CM2 Profit"
                ),
            previousFormatter: (val: number) =>
                formatAmountWithPct(
                    val,
                    shouldShowDummyUi
                        ? dummyStatData.cm2ProfitPct.previous
                        : isStickyGlobal
                            ? stickyPreviousTotals.cm2MarginPct
                            : prev.profitPct,
                    "CM2 Profit"
                ),
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

            bottomLabel: prevLabel,
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

            bottomLabel: prevLabel,
            className: "bg-white border-[#ED9F50] border-t-4 border-t-[#ED9F50]",
        },
    ];


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

            const dataRows = rowsToExport.map((r: any) => {
                const netUnitsSold = n(
                    r.total_quantity ??
                    (toNumber(r.quantity) - toNumber(r.return_quantity))
                );
                const marketplaceTotal =
                    Math.abs(n(r.fba_fees)) + Math.abs(n(r.selling_fees));
                const otherTransactionsTotal = getProductwiseOtherTransactionsTotal(r);

                if (isUsPnlSkuLayout) {
                    return {
                        "Sno.": r.isTotal ? "" : r.sno ?? "",
                        "Product Name": r.isTotal ? "Total" : r.isOthers ? "Others" : r.product_name,
                        "SKU": r.isOthers || r.isTotal ? "-" : r.sku || "-",

                        "Units Sold": n(r.quantity),
                        "Return": n(r.return_quantity),
                        "Net Units Sold": netUnitsSold,
                        "ASP": n(r.asp),

                        "Gross Sales": n(r.gross_sales),
                        "Sales - Refund": n(r.refund_sales),
                        "Promotions": Math.abs(n(r.promotional_rebates)),
                        "Net Sales": n(r.net_sales),

                        "Promotions %": Math.abs(n(r.promotional_rebates_percentage)),

                        "COGS": n(r.cogs),

                        "Selling Fees": n(r.selling_fees),
                        "FBA Fees": n(r.fba_fees),
                        "Total Fees": marketplaceTotal,

                        "Net Taxes": n(r.tax ?? r.net_taxes),
                        "Net Credits": n(r.credits ?? r.net_credits),
                        "Misc. Transactions": n(r.misc_transaction),
                        "Other Transactions": otherTransactionsTotal,

                        "CM1 Profit": n(r.profit),
                        "CM1 Profit Per Unit": n(r.cm1_profit_per_unit),
                        "CM1 Profit %": n(r.cm1_profit_per),

                        "Sponsored Product": n(r.product_spend),
                        "Sponsored Display": n(r.display_spend),
                        "Ads Spend": n(r.ads_spend),

                        "ACOS %": n(r.acos),

                        "CM2 Profit": n(r.cm2_profit),
                        "CM2 Profit Per Unit": n(r.cm2_profit_per_unit),
                        "CM2 Profit %": n(r.cm2_profit_per),
                    };
                }

                return {
                    "S.No": r.isTotal ? "" : r.sno ?? "",
                    "Product Name": r.isTotal ? "Total" : r.isOthers ? "Others" : r.product_name,
                    "SKU": r.isOthers || r.isTotal ? "-" : r.sku || "-",

                    "Units Sold": n(r.quantity),
                    "Return": n(r.return_quantity),
                    "Total Units": netUnitsSold,
                    "ASP": n(r.asp),

                    "Gross Sales": n(r.gross_sales),
                    "Sales - Refund": n(r.refund_sales),
                    "Taxes and Credits": n(r.net_sales_tax_and_credits ?? r.tax_and_credits),
                    "Net Sales": n(r.net_sales),

                    "Promotions": Math.abs(n(r.promotional_rebates)),
                    "Promotions %": Math.abs(n(r.promotional_rebates_percentage)),

                    "COGS": n(r.cogs),

                    "Selling Fees": n(r.selling_fees),
                    "FBA Fees": n(r.fba_fees),

                    "Tax": n(r.tax),
                    "Credits": n(r.credits),
                    "Tax & Credits": n(r.tax_and_credits),

                    "CM1 Profit Per Unit": n(r.cm1_profit_per_unit),
                    "CM1 Profit %": n(r.cm1_profit_per),
                    "CM1 Profit": n(r.profit),

                    "Sponsored Product": n(r.product_spend),
                    "Sponsored Display": n(r.display_spend),
                    "Ads Spend": n(r.ads_spend),

                    "ACOS %": n(r.acos),

                    "CM2 Profit Per Unit": n(r.cm2_profit_per_unit),
                    "CM2 Profit %": n(r.cm2_profit_per),
                    "CM2 Profit": n(r.cm2_profit),
                };
            });

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

            const shortTermStorageFee =
                n(totalRow?.short_term_storage_fee) ||
                n(grandTotalRowRaw?.short_term_storage_fee) ||
                n(grandTotalRowDisplay?.short_term_storage_fee) ||
                n((plSummaryTotals as any)?.short_term_storage_fee);

            const longTermStorageFee =
                n(totalRow?.long_term_storage_fee) ||
                n(grandTotalRowRaw?.long_term_storage_fee) ||
                n(grandTotalRowDisplay?.long_term_storage_fee) ||
                n((plSummaryTotals as any)?.long_term_storage_fee);

            const fbaDisposal =
                n(totalRow?.fba_disposal) ||
                n(grandTotalRowRaw?.fba_disposal) ||
                n(grandTotalRowDisplay?.fba_disposal) ||
                n((plSummaryTotals as any)?.fba_disposal);

            const exportSources = [
                totalRow,
                grandTotalRowRaw,
                grandTotalRowDisplay,
                plSummaryTotals,
            ];

            const getOptionalExportNumber = (keys: string[]) => {
                for (const source of exportSources) {
                    const record =
                        source && typeof source === "object"
                            ? (source as Record<string, unknown>)
                            : null;

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

                        const parsed =
                            typeof normalized === "number" ? normalized : Number(normalized);

                        if (Number.isFinite(parsed)) return parsed;
                    }
                }

                return null;
            };

            const exportNonZeroOrNull = (value: unknown) => {
                const parsed = Number(String(value ?? "").replace(/,/g, "").trim());
                return Number.isFinite(parsed) && parsed !== 0 ? parsed : null;
            };

            const summaryExportValue = (value: number | null) => value === null ? "-" : value;
            const spacerSummaryRow = () => ({ label: "", value: "" });

            const exportPlacementFees = getOptionalExportNumber(["placement_fee"]);
            const exportShippingCharges = getOptionalExportNumber(["shipment_fees"]);
            const exportCustomsFees = getOptionalExportNumber(["customs_fee"]);
            const exportShippingChargesTotal = getOptionalExportNumber(["shipping_charges"]);

            const exportShortTermStorage =
                getOptionalExportNumber(["short_term_storage_fee", "short_term_storage"]) ??
                exportNonZeroOrNull(shortTermStorageFee);
            const exportLongTermStorage =
                getOptionalExportNumber(["long_term_storage_fee", "long_term_storage"]) ??
                exportNonZeroOrNull(longTermStorageFee);
            const exportStorageFees = getOptionalExportNumber(["storage_fee"]);

            const exportInventoryCharges =
                getOptionalExportNumber([
                    "inventory_charges",
                    "inventory_charge",
                    "inventory_fees",
                    "inventory_fee",
                    "fba_disposal",
                ]) ?? exportNonZeroOrNull(fbaDisposal);
            const exportReimbursementForLostInventory =
                getOptionalExportNumber([
                    "reimbursement_lost_inventory_amount",
                    "lost_inventory_reimbursement",
                    "lost_total",
                ]) ?? exportNonZeroOrNull(lostInventory);
            const exportInventoryChargesAndReimbursement =
                getOptionalExportNumber([
                    "inventory_charges_and_reimbursement",
                    "inventory_charges_reimbursement",
                    "inventory_charges_reimbursement_total",
                ]) ??
                (exportInventoryCharges !== null && exportReimbursementForLostInventory !== null
                    ? exportInventoryCharges - exportReimbursementForLostInventory
                    : null);

            const exportPlatformManagementFees = getOptionalExportNumber(["platform_management_fees"]);
            const exportOthers = getOptionalExportNumber(["other_adjustment"]);
            const reimbursementUnits = getOptionalExportNumber([
                "reimbursement_lost_inventory_units",
                "reimbursement_units",
                "lost_inventory_units",
            ]);

            const legacySummaryRows: { label: string; value: any; indent?: number; bold?: boolean }[] = [
                { label: "Cost of Advertisement", value: "" },
                { label: "Visibility - Ads (-)", value: visibilityAds, indent: 1 },
                { label: "Visibility - Deals, Vouchers and Reviews (-)", value: dealsVouchers, indent: 1 },

                { label: "Other Transactions", value: "" },
                { label: "Short Term Storage Fee (-)", value: shortTermStorageFee, indent: 1 },
                { label: "Long Term Storage Fee (-)", value: longTermStorageFee, indent: 1 },
                { label: "FBA Disposal (-)", value: fbaDisposal, indent: 1 },
                { label: "Reimbursement for lost Inventory (+)", value: lostInventory, indent: 1 },
                {
                    label: "Misc. Transactions (+)",
                    value: formatSummaryValue(plSummaryTotals.misc_transaction, "misc_transaction"),
                    indent: 1
                },
                { label: "Other Platform Fees (-)", value: otherPlatformFees, indent: 1 },

                ...(countryName === "us" || countryName === "global"
                    ? [
                        {
                            label: "Shipment Charges (-)",
                            value: shipmentCharges,
                        },
                    ]
                    : []),

                { label: "CM2 Profit/Loss", value: cm2ProfitLoss },
                { label: "CM2 Margins", value: Number(cm2MarginPctForSummary ?? 0) },
                { label: "TACoS (Total Advertising Cost of Sale)", value: Number(tacosFromDisplayedCardsForSummary ?? 0) },
                { label: "Net Reimbursement", value: Number(reimbursementForSummary ?? 0) },
                { label: "Reimbursement vs CM2 Margins", value: Number(reimbursementVsCm2PctForSummary ?? 0) },
                { label: "Reimbursement vs Sales", value: Number(reimbursementVsSalesPctForSummary ?? 0) },
            ];

            const usSummaryRows: { label: string; value: any; indent?: number; bold?: boolean }[] = [
                { label: "Cost of Advertisement (-)", value: costOfAds },
                { label: "Visibility - Ads (-)", value: visibilityAds, indent: 1 },
                { label: "Visibility - Deals, Vouchers and Reviews (-)", value: dealsVouchers, indent: 1 },
                spacerSummaryRow(),

                { label: "Shipping Charges (-)", value: summaryExportValue(exportShippingChargesTotal) },
                { label: "Placement Fees (-)", value: summaryExportValue(exportPlacementFees), indent: 1 },
                { label: "Shipping Charges (-)", value: summaryExportValue(exportShippingCharges), indent: 1 },
                { label: "Customs Fees (-)", value: summaryExportValue(exportCustomsFees), indent: 1 },
                spacerSummaryRow(),

                { label: "Storage Fees (-)", value: summaryExportValue(exportStorageFees) },
                { label: "Short Term Storage (-)", value: summaryExportValue(exportShortTermStorage), indent: 1 },
                { label: "Long Term Storage (-)", value: summaryExportValue(exportLongTermStorage), indent: 1 },
                spacerSummaryRow(),

                {
                    label: "Inventory Charges and Reimbursement",
                    value: summaryExportValue(exportInventoryChargesAndReimbursement),
                },
                { label: "Inventory Charges (-)", value: summaryExportValue(exportInventoryCharges), indent: 1 },
                {
                    label: `Reimbursement for lost Inventory${reimbursementUnits
                        ? ` - ${reimbursementUnits} Units`
                        : ""} (+)`,
                    value: summaryExportValue(exportReimbursementForLostInventory),
                    indent: 1,
                },
                spacerSummaryRow(),

                {
                    label: "Platform Management Fees",
                    value: summaryExportValue(exportPlatformManagementFees),
                },
                { label: "Others", value: summaryExportValue(exportOthers) },
                spacerSummaryRow(),

                { label: "CM2 Profit", value: cm2ProfitLoss },
                spacerSummaryRow(),
                { label: "CM2 Profit %", value: Number(cm2MarginPctForSummary ?? 0) },
                { label: "TACoS (Total Advertising Cost of Sale)", value: Number(tacosFromDisplayedCardsForSummary ?? 0) },
                { label: "Net Reimbursement", value: Number(reimbursementForSummary ?? 0) },
                { label: "Reimbursement vs Sales", value: Number(reimbursementVsSalesPctForSummary ?? 0) },
                { label: "Reimbursement vs CM2 Margins", value: Number(reimbursementVsCm2PctForSummary ?? 0) },
            ];

            const summaryRows = isUsPnlSkuLayout ? usSummaryRows : legacySummaryRows;

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
                isUsLayout: isUsPnlSkuLayout,
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
        isUsPnlSkuLayout,
        getProductwiseOtherTransactionsTotal,
        plSummaryTotals,
        cm2MarginPctForSummary,
        tacosFromDisplayedCardsForSummary,
        reimbursementForSummary,
        reimbursementVsCm2PctForSummary,
        reimbursementVsSalesPctForSummary,
        costOfAds,
        grandTotalRowDisplay,
        grandTotalRowRaw,
        userData,
        brandName,
        profileHomeCurrency,
    ]);


    const cm2ProfitPieData = useMemo<Cm1PieSlice[]>(() => {
        const prevCm2ByName = buildPreviousProfitMap("cm2_profit");

        const toNullableNumber = (value: any): number | null => {
            if (value === undefined || value === null || value === "") return null;

            const n = Number(String(value).replace(/,/g, "").trim());
            return Number.isFinite(n) ? n : null;
        };

        const getFirstNumber = (...values: any[]): number | null => {
            for (const value of values) {
                const n = toNullableNumber(value);
                if (n !== null) return n;
            }

            return null;
        };

        const getFirstNonZeroNumber = (...values: any[]): number | null => {
            for (const value of values) {
                const n = toNullableNumber(value);
                if (n !== null && n !== 0) return n;
            }

            return null;
        };

        const isValidPieRow = (r: any) => {
            const name = String(r?.product_name || "").trim().toLowerCase();
            const sku = String(r?.sku || "").trim().toUpperCase();

            return (
                !r?.isTotal &&
                !r?.isOthers &&
                name !== "total" &&
                name !== "grand total" &&
                sku !== "GRAND_TOTAL" &&
                sku !== "TOTAL" &&
                sku !== "TOTAL_SEGMENT"
            );
        };

        const liveCm2Rows =
            Array.isArray(liveBiPayload?.all_action_rows) && liveBiPayload.all_action_rows.length > 0
                ? liveBiPayload.all_action_rows
                : Array.isArray(liveBiPayload?.focus_sku_rows) && liveBiPayload.focus_sku_rows.length > 0
                    ? liveBiPayload.focus_sku_rows
                    : [];

        const sourceRows: any[] =
            liveCm2Rows.length > 0
                ? liveCm2Rows
                : finalMonthlySkuwiseRowsForTable || [];

        const rows: Cm1PieSlice[] = sourceRows
            .filter(isValidPieRow)
            .map((r: any): Cm1PieSlice => {
                const name = normalizeProductDisplayName(
                    r?.product_name || r?.sku || "Unknown"
                );

                const normalizedName = normalizePieName(name);
                const normalizedSku = String(r?.sku || "").trim().toLowerCase();

                const currentValue =
                    getFirstNumber(
                        r?.cm2_profit_curr,
                        r?.cm2_profit,
                        r?.total_cm2_profit
                    ) ?? 0;

                /**
                 * Priority:
                 * 1. Real SKU-level cm2_profit_prev from /live_mtd_bi if backend sends it
                 * 2. /previous_skuwise_global map for global
                 * 3. profit_prev fallback for countrywise/current live response
                 */
                const apiPrevCm2 = getFirstNonZeroNumber(
                    r?.cm2_profit_prev,
                    r?.previous_cm2_profit
                );

                const prevFromGlobalMap =
                    prevCm2ByName.get(normalizedName) ??
                    (normalizedSku ? prevCm2ByName.get(normalizedSku) : undefined);

                const fallbackPrevProfit = getFirstNonZeroNumber(
                    r?.profit_prev,
                    r?.cm1_profit_prev
                );

                const prevValue =
                    apiPrevCm2 !== null
                        ? apiPrevCm2
                        : typeof prevFromGlobalMap === "number" &&
                            Number.isFinite(prevFromGlobalMap) &&
                            prevFromGlobalMap !== 0
                            ? prevFromGlobalMap
                            : fallbackPrevProfit ?? 0;

                const apiDelta = getFirstNonZeroNumber(
                    r?.cm2_profit_growth_pct,
                    r?.cm2_delta_pct,
                    r?.cm2_profit_delta_percentage
                );

                const deltaPct =
                    prevValue !== 0
                        ? apiDelta !== null
                            ? apiDelta
                            : safeDeltaPct(currentValue, prevValue)
                        : null;

                return {
                    name,
                    value: currentValue,
                    prevValue,
                    pct: 0,
                    deltaPct,
                };
            })
            .filter(
                (row: Cm1PieSlice) =>
                    Number(row.value || 0) !== 0 ||
                    Number(row.prevValue || 0) !== 0
            );

        const totalAbs = rows.reduce(
            (sum: number, row: Cm1PieSlice) =>
                sum + Math.abs(Number(row.value || 0)),
            0
        );

        return rows
            .sort(
                (a: Cm1PieSlice, b: Cm1PieSlice) =>
                    Math.abs(Number(b.value || 0)) -
                    Math.abs(Number(a.value || 0))
            )
            .map((row: Cm1PieSlice): Cm1PieSlice => ({
                ...row,
                pct: totalAbs ? (Math.abs(row.value) / totalAbs) * 100 : 0,
            }));
    }, [
        liveBiPayload,
        finalMonthlySkuwiseRowsForTable,
        buildPreviousProfitMap,
        normalizePieName,
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

    const netSalesPieData = useMemo<Cm1PieSlice[]>(() => {
        const rows = Array.isArray(liveBiPayload?.compare_top5_net_sales)
            ? liveBiPayload.compare_top5_net_sales
            : [];

        const toNullableNumber = (value: any): number | null => {
            if (value === undefined || value === null || value === "") return null;

            const n = Number(String(value).replace(/,/g, "").trim());
            return Number.isFinite(n) ? n : null;
        };

        const getFirstNumber = (...values: any[]): number | null => {
            for (const value of values) {
                const n = toNullableNumber(value);
                if (n !== null) return n;
            }

            return null;
        };

        const isInvalidNetSalesPieRow = (name: string) => {
            const n = String(name || "").trim().toLowerCase();

            return (
                !n ||
                n === "unknown" ||
                n === "total" ||
                n === "grand total"
            );
        };

        type NetSalesPieRow = Cm1PieSlice & { hasPctFromApi: boolean };

        const mappedRows: NetSalesPieRow[] = rows
            .map((row: any): NetSalesPieRow => {
                const rawName = String(
                    row?.product ||
                    row?.name ||
                    row?.product_name ||
                    row?.sku ||
                    "Unknown"
                ).trim();

                const name = normalizeProductDisplayName(rawName);
                const value =
                    getFirstNumber(row?.current_net_sales, row?.net_sales_curr, row?.value) ?? 0;
                const prevValue =
                    getFirstNumber(row?.previous_net_sales, row?.net_sales_prev, row?.prevValue) ?? 0;
                const pctFromApi = getFirstNumber(
                    row?.current_sales_mix_percentage,
                    row?.sales_mix_curr,
                    row?.pct
                );
                const deltaPct =
                    getFirstNumber(row?.net_sales_delta_percentage, row?.deltaPct, row?.delta_pct) ??
                    safeDeltaPct(value, prevValue);

                return {
                    name,
                    value,
                    prevValue,
                    pct: pctFromApi ?? 0,
                    deltaPct,
                    hasPctFromApi: pctFromApi !== null,
                };
            })
            .filter((row: NetSalesPieRow) => {
                return (
                    !isInvalidNetSalesPieRow(row.name) &&
                    (Number(row.value || 0) !== 0 || Number(row.prevValue || 0) !== 0)
                );
            });

        const total = mappedRows.reduce(
            (sum: number, row: NetSalesPieRow) => sum + Math.abs(Number(row.value || 0)),
            0
        );

        return mappedRows
            .sort((a: NetSalesPieRow, b: NetSalesPieRow) =>
                Math.abs(Number(b.value || 0)) - Math.abs(Number(a.value || 0))
            )
            .map((row: NetSalesPieRow): Cm1PieSlice => {
                const { hasPctFromApi, ...pieRow } = row;

                return {
                    ...pieRow,
                    pct: hasPctFromApi
                        ? Number(pieRow.pct || 0)
                        : total
                            ? (Math.abs(Number(pieRow.value || 0)) / total) * 100
                            : 0,
                };
            });
    }, [liveBiPayload]);

    const finalNetSalesPieData = isUsingDummyData
        ? dummyCm1ProfitPieData
        : netSalesPieData;



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

    const {
        downloadInventoryExcel,
        canDownloadInventoryExcel,
    } = useCurrentInventoryExcelExport({
        region: hasRealInventoryRows ? graphRegionToUse : "UK",
        invRows: finalInventoryRows,
        inventoryAlerts: finalInventoryAlerts,
        userData,
        convertToDisplayCurrency,
        selectedInventoryCountry: globalMtdCountry,
    });

    const finalTargetData = shouldShowDummyUi ? dummyTargetData : targetData;

    const finalTargetsTodayHome = shouldShowDummyUi
        ? dummySalesTargetStats.todayHome
        : targets_todayHome;

    const finalTargetsMtdHome = shouldShowDummyUi
        ? dummySalesTargetStats.mtdHome
        : targets_mtdHome;

    const finalStatsTargetHome = shouldShowDummyUi
        ? dummySalesTargetStats.targetHome
        : stickyTargetHome;

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
            .replace(/\s*\+\s*/g, " + ")
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
            const name = normalizeDrawerKey(getSkuwiseDisplayProductName(row));

            const rows = getLiveBiProductRows();

            // 1. Exact SKU match first
            if (sku && sku !== "others") {
                const bySku = rows.find((item: any) => {
                    const itemSku = normalizeDrawerKey(item?.sku);
                    return itemSku === sku;
                });

                if (bySku) return bySku;
            }

            // 2. Exact product name match only
            if (name) {
                const byName = rows.find((item: any) => {
                    const itemName = normalizeDrawerKey(item?.product_name);
                    return itemName === name;
                });

                if (byName) return byName;
            }

            return undefined;
        },
        [getLiveBiProductRows]
    );

    const getPnlDrawerInventoryValues = (source: any) => {
        const currentInventory = Number(
            source?.current_inventory ??
            source?.currentInventory ??
            source?.["Current Inventory"] ??
            source?.["Available Inventory"] ??
            source?.["Available Quantity"] ??
            0
        );

        const stockCover = Number(
            source?.coverage_ratio_months ??
            source?.coverageRatioMonths ??
            source?.["Coverage Ratio (In Months)"] ??
            source?.["Stock Cover"] ??
            0
        );

        return {
            currentInventory: Number.isFinite(currentInventory) ? currentInventory : 0,
            stockCover: Number.isFinite(stockCover) ? stockCover : 0,
        };
    };

    const buildPnlDrawerInventoryMetrics = (source: any): MetricItem[] => {
        const { currentInventory, stockCover } = getPnlDrawerInventoryValues(source);

        return [
            {
                label: "Current Inventory",
                value: `${Math.round(currentInventory).toLocaleString()} units`,
            },
            {
                label: "Stock Cover (Months)",
                value: stockCover.toFixed(2),
            },
        ];
    };

    const buildDrawerMetricsForPnlRow = useCallback(
        (pnlRow: MonthlySkuwiseTableRow, liveRow?: any): MetricItem[] => {
            const source = {
                ...(pnlRow || {}),
                ...(liveRow || {}),
            };

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

            const cm2Profit =
                Number(
                    source?.cm2_profit_curr ??
                    source?.total_cm2_profit ??
                    source?.cm2_profit_total ??
                    source?.cm2_profit ??
                    0
                ) || 0;

            const cm2ProfitPerUnit =
                Number(
                    source?.cm2_profit_per_unit_curr ??
                    source?.cm2_profit_per_unit ??
                    source?.cm2_profit_per ??
                    source?.cm2_profit_unit ??
                    0
                ) || (units > 0 ? cm2Profit / units : 0);

            const hasCm2 =
                source?.cm2_profit_curr !== undefined ||
                source?.total_cm2_profit !== undefined ||
                source?.cm2_profit_total !== undefined ||
                source?.cm2_profit !== undefined;

            const unitGrowth = getDrawerGrowth(source, "Unit Growth", "Unit Growth (%)");
            const salesGrowth = getDrawerGrowth(
                source,
                "Sales Growth",
                "Net Sales Growth",
                "Net Sales Growth (%)"
            );
            const aspGrowth = getDrawerGrowth(source, "ASP Growth", "ASP Growth (%)");
            const cm1ProfitGrowth = getDrawerGrowth(
                source,
                "CM1 Profit Impact",
                "CM1 Profit Impact (%)"
            );

            const cm1ProfitPerUnitGrowth = getDrawerGrowth(
                source,
                "Profit Per Unit",
                "Profit Per Unit (%)"
            );

            const cm2ProfitPrev = Number(
                source?.cm2_profit_prev ??
                source?.previous_cm2_profit ??
                0
            ) || 0;

            const cm2ProfitPerUnitPrev = Number(
                source?.cm2_profit_per_unit_prev ??
                0
            ) || 0;

            const cm2ProfitGrowth =
                source?.cm2_profit_growth_pct !== undefined
                    ? Number(source.cm2_profit_growth_pct)
                    : cm2ProfitPrev
                        ? ((cm2Profit - cm2ProfitPrev) / Math.abs(cm2ProfitPrev)) * 100
                        : 0;

            const cm2ProfitPerUnitGrowth =
                source?.cm2_profit_per_unit_growth_pct !== undefined
                    ? Number(source.cm2_profit_per_unit_growth_pct)
                    : cm2ProfitPerUnitPrev
                        ? ((cm2ProfitPerUnit - cm2ProfitPerUnitPrev) / Math.abs(cm2ProfitPerUnitPrev)) * 100
                        : 0;

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
                ...(hasCm2
                    ? [
                        {
                            label: "CM2 profit",
                            value: formatDrawerMetricValue(
                                cm2Profit,
                                cm2ProfitGrowth,
                                "money",
                                valueCurrency,
                                true
                            ),
                            color: cm2ProfitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                        },
                        {
                            label: "CM2 profit per unit",
                            value: formatDrawerMetricValue(
                                cm2ProfitPerUnit,
                                cm2ProfitPerUnitGrowth,
                                "money",
                                valueCurrency
                            ),
                            color: cm2ProfitPerUnitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                        },
                    ]
                    : [
                        {
                            label: "CM1 profit",
                            value: formatDrawerMetricValue(
                                cm1Profit,
                                cm1ProfitGrowth,
                                "money",
                                valueCurrency,
                                true
                            ),
                            color: cm1ProfitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                        },
                        {
                            label: "CM1 profit per unit",
                            value: formatDrawerMetricValue(
                                cm1ProfitPerUnit,
                                cm1ProfitPerUnitGrowth,
                                "money",
                                valueCurrency
                            ),
                            color: cm1ProfitPerUnitGrowth < 0 ? "#FF5C5C" : "#5EA68E",
                        },
                    ]),
                ...buildPnlDrawerInventoryMetrics(source),
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
            const name = normalizeDrawerKey(
                getSkuwiseDisplayProductName(row) || liveRow?.product_name
            );

            const directBySku = Object.entries(insights).find(([key]) => {
                return normalizeDrawerKey(key) === sku;
            });

            if (directBySku) return directBySku[1] as any;

            const byProductName = Object.values(insights).find((item: any) => {
                const product = normalizeDrawerKey(item?.product_name);
                return product === name;
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
                        (!!targetName && normalizedFirstLine === targetName) ||
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

                return keyName === name || productName === name;
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
                getSkuwiseDisplayProductName(row) ||
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

            const rowAny = row as any;
            const liveRowAny = liveRow as any;

            setSelectedRec({
                productName,
                metrics: buildDrawerMetricsWithAds(
                    buildDrawerMetricsForPnlRow(row, liveRow),
                    {
                        ...liveRowAny,
                        ...rowAny,
                        ads_spend:
                            rowAny?.ads_spend ??
                            rowAny?.total_ads ??
                            rowAny?.advertising_fees ??
                            rowAny?.advertising_total ??
                            liveRowAny?.ads_spend ??
                            liveRowAny?.ads_spend_curr ??
                            liveRowAny?.total_ads ??
                            liveRowAny?.advertising_fees,
                        ads_spend_growth_pct:
                            rowAny?.ads_spend_growth_pct ??
                            liveRowAny?.ads_spend_growth_pct,
                    }
                ),
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

    const handleHeatmapProductClick = useCallback(
        (heatmapRow: AgeingRiskHeatmapRow) => {
            if (!heatmapRow || heatmapRow.isTotalRow || heatmapRow.isOthersRow) {
                return;
            }

            const productName = String(heatmapRow.productName || "").trim();
            const sku = String(heatmapRow.sku || "").trim();

            if (!productName && !sku) {
                return;
            }

            openPnlSkuDrawer({
                product_name: productName,
                sku,
            } as MonthlySkuwiseTableRow);
        },
        [openPnlSkuDrawer]
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
            filename: `Inventory_Insights_${titleCountry}_${formattedMonthYear}.xlsx`,
            titleLine: `Amazon ${titleCountry} - Inventory_Insights - ${formattedMonthYear}`,
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

    const formatHeatmapCell = (value: any, showZero = false) => {
        if (value === null || value === undefined || value === "") return "-";

        const n = Number(String(value).replace(/,/g, ""));
        if (Number.isFinite(n)) {
            if (n === 0 && !showZero) return "-";
            return n;
        }

        return value;
    };

    const canDownloadInventoryInsightsExcel =
        !!inventoryInsightsData?.heatmapData?.length;

    const buildInventoryInsightsExcelRows = (
        rows: AgeingRiskHeatmapRow[],
        buckets: AgeingBucket[],
        inventoryAgeSummary?: InventoryCurrentApiResponse["inventory_age_summary"]
    ) => {
        const percentageRow = rows.find((row) => row.isPercentageRow);
        const productRows = rows.filter((row) => !row.isPercentageRow && !row.isTotalRow);

        const sortedRows = [...productRows].sort((a, b) => {
            const aUnitsSold = Number(a.salesLast30Days || 0);
            const bUnitsSold = Number(b.salesLast30Days || 0);
            return bUnitsSold - aUnitsSold;
        });

        const totalRow: AgeingRiskHeatmapRow = {
            productName: "Total",
            sku: "-",
            isTotalRow: true,
            zeroToNinety: 0,
            ninetyOneToOneEighty: 0,
            zeroToOneEighty: 0,
            oneEightyOneToTwoSeventy: 0,
            twoSeventyOneToThreeSixtyFive: 0,
            threeSixtyFivePlus: 0,
            available: 0,
            totalUnits: 0,
            inboundUnits: 0,
            unsellableUnits: 0,
            unitsSold: 0,
            salesRank: "",
            coverageRatio: undefined,
            inventoryAlert: "",
        };

        buckets.forEach((bucket) => {
            const apiColumnMap: Record<string, string> = {
                zeroToNinety: "inv-age-0-to-90-days",
                ninetyOneToOneEighty: "inv-age-91-to-180-days",
                zeroToOneEighty: "inv-age-0-to-180-days",
                oneEightyOneToTwoSeventy: "inv-age-181-to-270-days",
                twoSeventyOneToThreeSixtyFive: "inv-age-271-to-365-days",
                threeSixtyFivePlus: "inv-age-365-plus-days",
            };

            const apiColumn = apiColumnMap[bucket.key];

            totalRow[bucket.key] =
                inventoryAgeSummary?.columns?.[apiColumn]?.total ??
                sortedRows.reduce((sum, row) => sum + Number(row[bucket.key] || 0), 0);
        });

        totalRow.available =
            inventoryAgeSummary?.sellable_total ??
            sortedRows.reduce((sum, row) => sum + Number(row.available || 0), 0);

        totalRow.totalUnits = totalRow.available;

        totalRow.inboundUnits = sortedRows.reduce(
            (sum, row) => sum + Number(row.inboundUnits || 0),
            0
        );

        totalRow.unsellableUnits =
            inventoryAgeSummary?.unfulfillable_total ??
            sortedRows.reduce((sum, row) => sum + Number(row.unsellableUnits || 0), 0);

        totalRow.unitsSold =
            inventoryAgeSummary?.current_month_units_sold_total ??
            sortedRows.reduce((sum, row) => sum + Number(row.unitsSold || 0), 0);

        totalRow.salesLast30Days =
            sortedRows.reduce(
                (sum, row) => sum + Number(row.salesLast30Days || 0),
                0
            );

        return percentageRow
            ? [...sortedRows, totalRow, percentageRow]
            : [...sortedRows, totalRow];
    };

    const handleInventoryInsightsExcelDownload = useCallback(() => {
        const titleCountry =
            platform === "global"
                ? "GLOBAL"
                : countryName.toUpperCase();

        if (platform === "global") {
            const ukInventoryInsights = buildInventoryInsightsFromResponses(
                inventoryInsightResponses,
                inventoryAgeSummaryResponses,
                "global",
                profileHomeCurrency,
                selectedAgeingTrendBucket,
                "uk"
            );

            const usInventoryInsights = buildInventoryInsightsFromResponses(
                inventoryInsightResponses,
                inventoryAgeSummaryResponses,
                "global",
                profileHomeCurrency,
                selectedAgeingTrendBucket,
                "us"
            );

            exportGlobalAgeingRiskHeatmapExcel({
                filename: `Inventory_Insights_Global_${formattedMonthYear}.xlsx`,
                titleLine: `Amazon Global - Inventory Insights - ${formattedMonthYear}`,
                platformLabel: "Phormula",
                periodLabel: formattedMonthYear,
                companyName,
                brandName: brandName || "",
                homeCurrencyCode: profileHomeCurrency,
                buckets: inventoryInsightsData?.heatmapBuckets || [],
                ukRows: ukInventoryInsights.heatmapData || [],
                usRows: usInventoryInsights.heatmapData || [],
                showInventoryAlerts: true,
                salesLast30DaysLabel: inventoryInsightsSalesLabel,
                unitSalesDataKey: inventoryHeatmapUnitSalesDataKey,
                storageCostCurrencySymbol: currencySymbol,
            });

            return;
        }

        exportAgeingRiskHeatmapExcel({
            filename: `Inventory_Insights_${titleCountry}_${formattedMonthYear}.xlsx`,
            titleLine: `Amazon ${titleCountry} - Inventory Insights - ${formattedMonthYear}`,
            countryName,
            titleCountry,
            countryLabel: titleCountry,
            platformLabel: "Phormula",
            periodLabel: formattedMonthYear,
            companyName,
            brandName: brandName || "",
            homeCurrencyCode: profileHomeCurrency,
            buckets: inventoryInsightsData?.heatmapBuckets || [],
            dataRows: buildInventoryInsightsExcelRows(
                inventoryInsightsData?.heatmapData || [],
                inventoryInsightsData?.heatmapBuckets || [],
                inventoryInsightsData?.inventoryAgeSummary
            ),
            showInventoryAlerts: true,

            // ✅ ADD same label as UI
            salesLast30DaysLabel: inventoryInsightsSalesLabel,
            useCurrentInventoryTableLayout: showUsCurrentInventoryTable,
            unitSalesDataKey: inventoryHeatmapUnitSalesDataKey,
            storageCostCurrencySymbol: currencySymbol,
        });
    }, [
        platform,
        countryName,
        formattedMonthYear,
        companyName,
        brandName,
        profileHomeCurrency,
        inventoryInsightsData?.heatmapBuckets,
        inventoryInsightsData?.heatmapData,
        inventoryInsightResponses,
        inventoryAgeSummaryResponses,
        selectedAgeingTrendBucket,
        inventoryInsightsSalesLabel,
        showUsCurrentInventoryTable,
        inventoryHeatmapUnitSalesDataKey,
        currencySymbol,
    ]);

    const formatExcelDash = (value: any) => {
        if (value === null || value === undefined || value === "") return "-";
        if (Number(value) === 0) return "-";
        return value;
    };

    const inventoryInsightsExcelRows = useMemo(() => {
        const rows = inventoryInsightsData?.heatmapData || [];

        const bodyRows = rows
            .filter((row: any) => {
                const productName = String(row?.productName || "").trim();
                const sku = String(row?.sku || "").trim();

                const isTotal =
                    productName.toLowerCase() === "total" ||
                    sku.toLowerCase() === "total";

                return (productName || sku) && !isTotal;
            })
            .map((row: any, index: number) => ({
                "S.No.": index + 1,
                "Product Name": row.productName || "-",
                "SKU": row.sku || "-",
                "0–90 Days": formatExcelDash(row.zeroToNinety),
                "91–180 Days": formatExcelDash(row.ninetyOneToOneEighty),
                "181–270 Days": formatExcelDash(row.oneEightyOneToTwoSeventy),
                "271–365 Days": formatExcelDash(row.twoSeventyOneToThreeSixtyFive),
                "365+ Days": formatExcelDash(row.threeSixtyFivePlus),
                "Sellable Units": formatExcelDash(row.available ?? row.totalUnits),
                "Inbound Units": formatExcelDash(row.inboundUnits),
                "Sales Rank": formatExcelDash(row.salesRank),
                "Unfulfillable Units": formatExcelDash(row.unsellableUnits),
                "Units Sold": formatExcelDash(row.unitsSold),
                "Coverage Ratio (in Months)": formatExcelDash(row.coverageRatio),
                "Inventory Alerts": row.inventoryAlert || "-",
            }));

        const totals = bodyRows.reduce(
            (acc: any, row: any) => {
                acc.zeroToNinety += Number(row["0–90 Days"]) || 0;
                acc.ninetyOneToOneEighty += Number(row["91–180 Days"]) || 0;
                acc.oneEightyOneToTwoSeventy += Number(row["181–270 Days"]) || 0;
                acc.twoSeventyOneToThreeSixtyFive += Number(row["271–365 Days"]) || 0;
                acc.threeSixtyFivePlus += Number(row["365+ Days"]) || 0;
                acc.sellable += Number(row["Sellable Units"]) || 0;
                acc.inbound += Number(row["Inbound Units"]) || 0;
                acc.unfulfillable += Number(row["Unfulfillable Units"]) || 0;
                acc.unitsSold += Number(row["Units Sold"]) || 0;
                return acc;
            },
            {
                zeroToNinety: 0,
                ninetyOneToOneEighty: 0,
                oneEightyOneToTwoSeventy: 0,
                twoSeventyOneToThreeSixtyFive: 0,
                threeSixtyFivePlus: 0,
                sellable: 0,
                inbound: 0,
                unfulfillable: 0,
                unitsSold: 0,
            }
        );

        const ageingTotal =
            totals.zeroToNinety +
            totals.ninetyOneToOneEighty +
            totals.oneEightyOneToTwoSeventy +
            totals.twoSeventyOneToThreeSixtyFive +
            totals.threeSixtyFivePlus;

        return [
            ...bodyRows,
            {
                "S.No.": "",
                "Product Name": "Total",
                "SKU": "",
                "0–90 Days": formatExcelDash(totals.zeroToNinety),
                "91–180 Days": formatExcelDash(totals.ninetyOneToOneEighty),
                "181–270 Days": formatExcelDash(totals.oneEightyOneToTwoSeventy),
                "271–365 Days": formatExcelDash(totals.twoSeventyOneToThreeSixtyFive),
                "365+ Days": formatExcelDash(totals.threeSixtyFivePlus),
                "Sellable Units": formatExcelDash(totals.sellable),
                "Inbound Units": formatExcelDash(totals.inbound),
                "Sales Rank": "",
                "Unfulfillable Units": formatExcelDash(totals.unfulfillable),
                "Units Sold": formatExcelDash(totals.unitsSold),
                "Coverage Ratio (in Months)": "",
                "Inventory Alerts": "",
            },
            {
                "S.No.": "",
                "Product Name": "% of Total",
                "SKU": "",
                "0–90 Days": ageingTotal ? `${((totals.zeroToNinety / ageingTotal) * 100).toFixed(2)}%` : "-",
                "91–180 Days": ageingTotal ? `${((totals.ninetyOneToOneEighty / ageingTotal) * 100).toFixed(2)}%` : "-",
                "181–270 Days": ageingTotal ? `${((totals.oneEightyOneToTwoSeventy / ageingTotal) * 100).toFixed(2)}%` : "-",
                "271–365 Days": ageingTotal ? `${((totals.twoSeventyOneToThreeSixtyFive / ageingTotal) * 100).toFixed(2)}%` : "-",
                "365+ Days": ageingTotal ? `${((totals.threeSixtyFivePlus / ageingTotal) * 100).toFixed(2)}%` : "-",
                "Sellable Units": ageingTotal ? `${((totals.sellable / ageingTotal) * 100).toFixed(2)}%` : "-",
                "Inbound Units": "",
                "Sales Rank": "",
                "Unfulfillable Units": ageingTotal ? `${((totals.unfulfillable / ageingTotal) * 100).toFixed(2)}%` : "-",
                "Units Sold": "",
                "Coverage Ratio (in Months)": "",
                "Inventory Alerts": "",
            },
        ];
    }, [inventoryInsightsData?.heatmapData]);



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

    const rawMtdPromotionsCurrent = Math.abs(
        toNumber(
            derived?.promotional_rebates ??
            grandTotalRowRaw?.promotional_rebates ??
            grandTotalRowDisplay?.promotional_rebates ??
            totals?.promotional_rebates ??
            0
        )
    );

    const rawMtdPromotionsPctCurrent = Math.abs(
        (
            toNumber(derived?.net_sales ?? grandTotalRowRaw?.net_sales ?? grandTotalRowDisplay?.net_sales)
                ? (
                    toNumber(
                        derived?.promotional_rebates ??
                        grandTotalRowRaw?.promotional_rebates ??
                        grandTotalRowDisplay?.promotional_rebates ??
                        totals?.promotional_rebates ??
                        0
                    ) /
                    toNumber(derived?.net_sales ?? grandTotalRowRaw?.net_sales ?? grandTotalRowDisplay?.net_sales)
                ) * 100
                : toNumber(
                    derived?.promotional_rebates_percentage ??
                    grandTotalRowRaw?.promotional_rebates_percentage ??
                    grandTotalRowDisplay?.promotional_rebates_percentage
                )
        )
    );

    const mtdPromotionsCurrentDisplay = shouldShowDummyUi
        ? dummyStatData.promotions.current
        : rawMtdPromotionsCurrent;

    const mtdPromotionsPreviousDisplay = shouldShowDummyUi
        ? dummyStatData.promotions.previous
        : convertToDisplayCurrency(prev.promotions ?? 0, amazonDataCurrency);

    const mtdPromotionsDelta = shouldShowDummyUi
        ? dummyStatData.promotions.deltaPct
        : safeDeltaPct(
            mtdPromotionsCurrentDisplay,
            mtdPromotionsPreviousDisplay
        );

    const mtdPromotionsPctCurrent = shouldShowDummyUi
        ? dummyStatData.promotionsPct.current
        : rawMtdPromotionsPctCurrent;

    const mtdPromotionsPctPrevious = shouldShowDummyUi
        ? dummyStatData.promotionsPct.previous
        : prev.promotionsPct;

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

        const previousRows =
            country === "uk"
                ? Array.isArray(previousSkuwiseGlobalData?.skuwise_items_uk)
                    ? previousSkuwiseGlobalData.skuwise_items_uk
                    : []
                : Array.isArray(previousSkuwiseGlobalData?.skuwise_items_us)
                    ? previousSkuwiseGlobalData.skuwise_items_us
                    : [];
        const previousGrand = getGrandTotalRow(previousRows);
        const currentDerived =
            country === "uk"
                ? (data as any)?.derived_totals_uk || {}
                : (data as any)?.derived_totals_us || {};
        const currentPromotionsRaw = pickPromotionalRebates(
            currentGrand,
            currentDerived
        );
        const previousPromotionsRaw = pickPromotionalRebates(
            prevDerived,
            prevAligned,
            previousGrand
        );
        const previousPromotionsNetSales = pickFirstNonZeroNumber(
            prevDerived.net_sales,
            prevAligned.total_previous_net_sales,
            previousGrand.net_sales
        );

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

            promotions: Math.abs(currentPromotionsRaw),
            prevPromotions: Math.abs(previousPromotionsRaw),
            promotionsPct: calculatePromotionalRebatesPct(
                currentPromotionsRaw,
                toNumber(currentGrand.net_sales),
                currentGrand,
                currentDerived
            ),
            prevPromotionsPct: calculatePromotionalRebatesPct(
                previousPromotionsRaw,
                previousPromotionsNetSales,
                prevDerived,
                prevAligned,
                previousGrand
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
                        formatter={(val) => formatCurrentAmountWithPct(val, c.cm2Pct, "CM2 Profit")}
                        previousFormatter={(val) => formatAmountWithPct(val, c.prevCm2Pct, "CM2 Profit")}
                        bottomLabel={prevLabel}
                        className="border-[#A8BE7A] border-t-4"
                        loading={!shouldShowDummyUi && (loading || biLoading)}
                    />

                    <AmazonStatCard
                        label="Promotions"
                        current={c.promotions}
                        previous={c.prevPromotions}
                        deltaPct={safeDeltaPct(c.promotions, c.prevPromotions)}
                        inverseDelta
                        formatter={(val) => formatCurrentAmountWithPct(val, c.promotionsPct, "Promotions", true)}
                        previousFormatter={(val) => formatAmountWithPct(val, c.prevPromotionsPct, "Promotions", true)}
                        bottomLabel={prevLabel}
                        className="border-[#7B9A6D] border-t-4"
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
                        formatter={(val) => formatCurrentAmountWithPct(val, c.cm2Pct, "CM2 Profit")}
                        previousFormatter={(val) => formatAmountWithPct(val, c.prevCm2Pct, "CM2 Profit")}
                        bottomLabel={prevLabel}
                        className="border-[#B8C78C] border-t-4"
                        loading={!shouldShowDummyUi && loading}
                    />

                    <AmazonStatCard
                        label="Promotions"
                        current={c.promotions}
                        previous={c.prevPromotions}
                        deltaPct={null}
                        inverseDelta
                        formatter={(val) => formatCurrentAmountWithPct(val, c.promotionsPct, "Promotions", true)}
                        previousFormatter={(val) => formatAmountWithPct(val, c.prevPromotionsPct, "Promotions", true)}
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

    // const MTD_VISIBLE_PRODUCT_ROWS = 8.92;

    // const MTD_HEADER_ROW_HEIGHT = 60;
    // const MTD_SIGN_ROW_HEIGHT = 45;
    // const MTD_PRODUCT_ROW_HEIGHT = 45;

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

    // const shouldScrollMtdProductwiseTable =
    //     showAllMtdProductwiseRows && mtdProductRowCount > MTD_VISIBLE_PRODUCT_ROWS;

    // const mtdProductwiseTableScrollHeight =
    //     MTD_HEADER_ROW_HEIGHT +
    //     MTD_SIGN_ROW_HEIGHT +
    //     MTD_PRODUCT_ROW_HEIGHT * MTD_VISIBLE_PRODUCT_ROWS;


    const MTD_PRODUCTWISE_VISIBLE_PRODUCT_ROWS = 4.85;
    // 9 product rows + Others row

    const MTD_PRODUCTWISE_SIGN_ROW_HEIGHT = 46;
    const MTD_PRODUCTWISE_ROW_HEIGHT = 46;
    const MTD_PRODUCTWISE_TOTAL_ROW_HEIGHT = 52;
    const MTD_PRODUCTWISE_SUMMARY_ROW_HEIGHT = 48;

    const MTD_PRODUCTWISE_SUMMARY_ROW_COUNT =
        isUsPnlSkuLayout
            ? 12
            : platform === "global"
                ? 9
                : 8;

    const shouldScrollMtdProductwiseTable =
        mtdProductRowCount > MTD_PRODUCTWISE_VISIBLE_PRODUCT_ROWS;

    const mtdProductwiseTableScrollHeight =
        MTD_PRODUCTWISE_SIGN_ROW_HEIGHT +
        MTD_PRODUCTWISE_ROW_HEIGHT * MTD_PRODUCTWISE_VISIBLE_PRODUCT_ROWS +
        MTD_PRODUCTWISE_TOTAL_ROW_HEIGHT +
        MTD_PRODUCTWISE_SUMMARY_ROW_HEIGHT * MTD_PRODUCTWISE_SUMMARY_ROW_COUNT;

    return (
        <div ref={pageTopRef} className="relative w-full">
            <div ref={tabTopRef} />
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

            <DashboardPageHeader
                brandName={brandName}
                countryName={countryName}
                formattedMonthYear={formattedMonthYear}
                handleHardRefresh={handleHardRefresh}
                pageLoading={pageLoading}
                lastRefreshAt={lastRefreshAt}
                lastUpdatedTimeText={lastUpdatedTimeText}
                activeDateRegion={activeDateRegion}
                formatUSTime12hr={formatUSTime12hr}
                formatLastUpdatedDateTime={formatLastUpdatedDateTime}
                formatUKTime12hr={formatUKTime12hr}
                activeTab={activeTab}
                TOP_TABS={TOP_TABS}
                shouldScrollTabTopRef={shouldScrollTabTopRef}
                setActiveTab={setActiveTab}
                syncTabToHash={syncTabToHash}
            />

            <PreviewLockedSection
                enabled={isUsingDummyData}
                title="Preview Mode"
                description="To view your real business data and analytics, please complete your profile and connect your Amazon account. This will unlock your performance dashboard and insights."
                buttonText="Complete Setup"
                onAction={handleConnectAmazonPreview}
            >
                {["summary", "productwise"].includes(activeTab) && (
                    <DashboardStickyKpis items={stickyKpiItems} />
                )}

                {activeTab === "live" && (
                    <DashboardLiveSalesTab
                        hasAmazonCard={hasAmazonCard}
                        platform={platform}
                        globalMtdViewOptions={globalMtdViewOptions}
                        globalMtdView={globalMtdView}
                        setGlobalMtdView={setGlobalMtdView}
                        showLiveBI={showLiveBI}
                        isCountryMode={isCountryMode}
                        selectedStartDay={selectedStartDay}
                        selectedEndDay={selectedEndDay}
                        formatAppliedRangeLabel={formatAppliedRangeLabel}
                        isRangeChangeRef={isRangeChangeRef}
                        setSelectedStartDay={setSelectedStartDay}
                        setSelectedEndDay={setSelectedEndDay}
                        setBiError={setBiError}
                        loadRangeFromCache={loadRangeFromCache}
                        fetchLiveBiPayload={fetchLiveBiPayload}
                        shouldShowDummyUi={shouldShowDummyUi}
                        dummyStatData={dummyStatData}
                        globalMtdCardData={globalMtdCardData}
                        safeDeltaPct={safeDeltaPct}
                        loading={loading}
                        shopifyLoading={shopifyLoading}
                        biLoading={biLoading}
                        previousSkuwiseGlobalLoading={previousSkuwiseGlobalLoading}
                        fmtInt={fmtInt}
                        formatDisplayAmount={formatDisplayAmount}
                        prevLabel={prevLabel}
                        fmtPct2={fmtPct2}
                        renderCountryMtdCards={renderCountryMtdCards}
                        useBiForAmazonCards={useBiForAmazonCards}
                        biCardKpis={biCardKpis}
                        mtdUnitsCurrent={mtdUnitsCurrent}
                        mtdUnitsPrevious={mtdUnitsPrevious}
                        mtdUnitsDelta={mtdUnitsDelta}
                        rangeActive={rangeActive}
                        convertToDisplayCurrency={convertToDisplayCurrency}
                        uk={uk}
                        amazonDataCurrency={amazonDataCurrency}
                        prev={prev}
                        deltas={deltas}
                        mtdCostOfAdsCurrentDisplay={mtdCostOfAdsCurrentDisplay}
                        mtdCostOfAdsPreviousDisplay={mtdCostOfAdsPreviousDisplay}
                        mtdCostOfAdsDelta={mtdCostOfAdsDelta}
                        mtdTacosCurrent={mtdTacosCurrent}
                        mtdTacosPrevious={mtdTacosPrevious}
                        mtdTacosDelta={mtdTacosDelta}
                        mtdCm2ProfitCurrentDisplay={mtdCm2ProfitCurrentDisplay}
                        mtdCm2ProfitPreviousDisplay={mtdCm2ProfitPreviousDisplay}
                        mtdCm2ProfitDelta={mtdCm2ProfitDelta}
                        mtdCm2ProfitPctCurrent={mtdCm2ProfitPctCurrent}
                        mtdCm2ProfitPctPrevious={mtdCm2ProfitPctPrevious}
                        mtdPromotionsCurrentDisplay={mtdPromotionsCurrentDisplay}
                        mtdPromotionsPreviousDisplay={mtdPromotionsPreviousDisplay}
                        mtdPromotionsDelta={mtdPromotionsDelta}
                        mtdPromotionsPctCurrent={mtdPromotionsPctCurrent}
                        mtdPromotionsPctPrevious={mtdPromotionsPctPrevious}
                        hasShopifyCard={hasShopifyCard}
                        shopify={shopify}
                        shopifyDeriv={shopifyDeriv}
                        shopifyPrevDeriv={shopifyPrevDeriv}
                        finalBiDailySeriesHome={finalBiDailySeriesHome}
                        finalBiPeriods={finalBiPeriods}
                        dashboardAllowedEndISO={dashboardAllowedEndISO}
                        biUiLoading={biUiLoading}
                        biError={biError}
                        currencySymbol={currencySymbol}
                        regions={regions}
                        targetRegion={targetRegion}
                        setTargetRegion={setTargetRegion}
                        displayCurrency={displayCurrency}
                        formatDisplayK={formatDisplayK}
                        finalTargetsTodayHome={finalTargetsTodayHome}
                        finalTargetsMtdHome={finalTargetsMtdHome}
                        finalStatsTargetHome={finalStatsTargetHome}
                        finalTargetsLastMonthTotalHome={finalTargetsLastMonthTotalHome}
                        targets_lastMonthToDateHome={targets_lastMonthToDateHome}
                        finalStatsSalesTrendPct={finalStatsSalesTrendPct}
                        finalStatsTargetTrendPct={finalStatsTargetTrendPct}
                        finalTargetsReimbursement={finalTargetsReimbursement}
                        biAlignedTotalsHome={biAlignedTotalsHome}
                        biCardsReady={biCardsReady}
                        formattedMonthYear={formattedMonthYear}
                        currentDisplayMonth={currentDisplayMonth}
                        todaySalesRaw={todaySalesRaw}
                        stickyTargetHome={stickyTargetHome}
                        targets_mtdHome={targets_mtdHome}
                        targets_lastMonthTotalHome={targets_lastMonthTotalHome}
                        targets_reimbursement={targets_reimbursement}
                        globalTargetCardTotals={globalTargetCardTotals}
                        salesTargetBiAlignedTotals={salesTargetBiAlignedTotals}
                        finalRangeCompletedPct={finalRangeCompletedPct}
                        lastRefreshAt={lastRefreshAt}
                        dashboardCompletedTimeZone={dashboardCompletedTimeZone}
                    />
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
                                    formattedMonthYear={formattedMonthYear}
                                    onGenerateInsights={async () => {
                                        if (shouldShowDummyUi) return;

                                        await fetchLiveBiPayload({
                                            generateInsights: true,
                                            skipLoader: true,
                                        });
                                    }}

                                    // ✅ ADD THIS
                                    onManualAiRefresh={async () => {
                                        if (shouldShowDummyUi) return null;

                                        const freshPayload = await fetchLiveBiPayload({
                                            startDay: selectedStartDay,
                                            endDay: selectedEndDay,
                                            generateInsights: false,
                                            skipLoader: true,

                                            // ✅ IMPORTANT
                                            manualAiRefresh: true,

                                            // ✅ IMPORTANT
                                            dataOnlyRefresh: false,
                                        });

                                        return freshPayload;
                                    }}
                                />
                            )
                        )}
                    </div>
                )}

                {activeTab === "productwise" && (
                    <>
                        <DashboardProductwisePnlSection
                            currencySymbol={currencySymbol}
                            adsLoading={adsLoading}
                            showAllMtdProductwiseRows={showAllMtdProductwiseRows}
                            setShowAllMtdProductwiseRows={setShowAllMtdProductwiseRows}
                            productwiseAllColumnsExpanded={productwiseAllColumnsExpanded}
                            handleToggleProductwiseAllColumns={handleToggleProductwiseAllColumns}
                            handleDownloadPlProductwiseMtd={handleDownloadPlProductwiseMtd}
                            shouldShowDummyUi={shouldShowDummyUi}
                            loading={loading}
                            monthlySkuwiseRows={monthlySkuwiseRows}
                            finalMonthlySkuwiseRowsForTable={finalMonthlySkuwiseRowsForTable}
                            shouldScrollMtdProductwiseTable={shouldScrollMtdProductwiseTable}
                            productwiseHasExpandedGroups={productwiseHasExpandedGroups}
                            setProductwiseAnyGroupExpanded={setProductwiseAnyGroupExpanded}
                            SKUWISE_LEFT_COLS={SKUWISE_LEFT_COLS}
                            SKUWISE_GROUPS={SKUWISE_GROUPS}
                            SKUWISE_SINGLE_COLS={SKUWISE_SINGLE_COLS}
                            productwiseInitialCollapsed={productwiseInitialCollapsed}
                            productwiseCollapsed={productwiseCollapsed}
                            setProductwiseCollapsed={setProductwiseCollapsed}
                            setProductwiseAllColumnsExpanded={setProductwiseAllColumnsExpanded}
                            PRODUCTWISE_GROUP_IDS={PRODUCTWISE_GROUP_IDS}
                            plSortConfig={plSortConfig}
                            setPlSortConfig={setPlSortConfig}
                            getAdsSignForCol={getAdsSignForCol}
                            mtdProductwiseTableScrollHeight={mtdProductwiseTableScrollHeight}
                            toNumber={toNumber}
                            getSkuwiseDisplayProductName={getSkuwiseDisplayProductName}
                            openPnlSkuDrawer={openPnlSkuDrawer}
                            renderLiveNetSalesDelta={renderLiveNetSalesDelta}
                            formatAdsNumber={formatAdsNumber}
                            formatAdType={formatAdType}
                            costOfAds={costOfAds}
                            formatSummaryRounded={formatSummaryRounded}
                            sponsoredBrandSpend={sponsoredBrandSpend}
                            dealVouchers={dealVouchers}
                            platformFee={platformFee}
                            plSummaryTotals={plSummaryTotals}
                            formatSummaryValue={formatSummaryValue}
                            lost_inventory_total={lost_inventory_total}
                            otherPlatformFee={otherPlatformFee}
                            countryName={countryName}
                            isUsPnlSkuLayout={isUsPnlSkuLayout}
                            getProductwiseOtherTransactionsTotal={getProductwiseOtherTransactionsTotal}
                            totalRowCm2Profit={totalRowCm2Profit}
                            totalRowCm2Margins={totalRowCm2Margins}
                            tacosFromDisplayedCardsForSummary={tacosFromDisplayedCardsForSummary}
                            reimbursementForSummary={reimbursementForSummary}
                            reimbursementVsCm2PctForSummary={reimbursementVsCm2PctForSummary}
                            reimbursementVsSalesPctForSummary={reimbursementVsSalesPctForSummary}
                        />

                        <DashboardMtdPlSection
                            isMtdPlExpanded={isMtdPlExpanded}
                            setIsMtdPlExpanded={setIsMtdPlExpanded}
                            chartRef={chartRef}
                            countryNameForGraph={countryNameForGraph}
                            formattedMonthYear={formattedMonthYear}
                            currencySymbol={currencySymbol}
                            finalBarLabels={finalBarLabels}
                            finalBarValues={finalBarValues}
                            finalPrevBarValues={finalPrevBarValues}
                            colors={colors}
                            shouldShowDummyUi={shouldShowDummyUi}
                            loading={loading}
                            finalAllValuesZero={finalAllValuesZero}
                            isUsingDummyData={isUsingDummyData}
                            netSalesPieData={finalNetSalesPieData}
                            finalCm1ProfitPieData={finalCm1ProfitPieData}
                            cm2ProfitPieData={cm2ProfitPieData}
                            displayCurrency={displayCurrency}
                        />
                    </>
                )}
                {activeTab === "inventory" && (
                    <DashboardInventoryInsightsTab
                        inventoryInsightsLoading={inventoryInsightsLoading}
                        inventoryInsightsError={inventoryInsightsError}
                        inventoryInsightsData={inventoryInsightsData}
                        platform={platform}
                        selectedGlobalInventoryCountry={selectedGlobalInventoryCountry}
                        setSelectedGlobalInventoryCountry={setSelectedGlobalInventoryCountry}
                        selectedAgeingTrendBucket={selectedAgeingTrendBucket}
                        handleAgeingTrendBucketChange={handleAgeingTrendBucketChange}
                        handleInventoryInsightsExcelDownload={handleInventoryInsightsExcelDownload}
                        canDownloadInventoryInsightsExcel={canDownloadInventoryInsightsExcel}
                        handleHeatmapProductClick={handleHeatmapProductClick}
                        countryName={countryName}
                        salesLast30DaysLabel={inventoryInsightsSalesLabel}
                        unitSalesDataKey={inventoryHeatmapUnitSalesDataKey}
                        useCurrentInventoryTableLayout={showUsCurrentInventoryTable}
                        storageCostCurrencySymbol={currencySymbol}
                    />
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
                formattedMonthYear={formattedMonthYear}
            />
        </div >

    );
}



