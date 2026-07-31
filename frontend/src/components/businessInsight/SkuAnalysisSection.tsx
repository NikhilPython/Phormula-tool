"use client";
import React, { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { BsStars } from "react-icons/bs";
import { FaArrowUp, FaArrowDown } from "react-icons/fa";
import DataTable, { ColumnDef, Row } from "@/components/ui/table/DataTable";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import axios from "axios";
import Drawer from "@mui/material/Drawer";
import IconButton from "@mui/material/IconButton";
import { FaThumbsUp, FaThumbsDown } from "react-icons/fa";
import Productinfoinpopup from "@/components/businessInsight/Productinfoinpopup";
import { AnimatePresence, motion } from "framer-motion";
import { exportSkuAnalysisMtdExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";

export type TabKey =
    | "top_80_skus"
    | "new_skus"
    | "reviving_skus"
    | "other_skus"
    | "all_skus";

export interface GrowthCategory {
    category: string;
    value: number;
}

export interface SkuItem {
    product_name: string;
    sku?: string;
    "Sales Mix (Month2)"?: number;

    total_quantity_month1?: number;
    total_quantity_month2?: number;
    asp_month1?: number;
    asp_month2?: number;
    net_sales_month1?: number;
    net_sales_month2?: number;
    gross_sales_month1?: number;
    gross_sales_month2?: number;
    sales_mix_month1?: number;
    sales_mix_month2?: number;
    profit_percentage_month1?: number;
    profit_percentage_month2?: number;
    unit_wise_profitability_month1?: number;
    unit_wise_profitability_month2?: number;
    profit_month1?: number;
    profit_month2?: number;

    [key: string]: any;
}

export interface CategorizedGrowth {
    top_80_skus: SkuItem[];

    // keep this for charts/export/backward compatibility
    new_or_reviving_skus: SkuItem[];

    // new individual table tabs
    new_skus?: SkuItem[];
    reviving_skus?: SkuItem[];

    other_skus: SkuItem[];
    all_skus?: SkuItem[];

    top_80_total?: SkuItem | null;
    new_or_reviving_total?: SkuItem | null;
    new_total?: SkuItem | null;
    reviving_total?: SkuItem | null;
    other_total?: SkuItem | null;
    all_skus_total?: SkuItem | null;
}

export interface SkuInsight {
    product_name: string;
    insight?: string;
    inventory_recommendation?: string;
    recommendation?: string;
    product_journey?: string[];
    objective?:
    | {
        growth_intent?: string;
        inventory_clearance_priority?: string;
        profit_priority?: string;
    }
    | string;
}

type TableRow = Row & {
    ai?: SkuItem;
    __isTotal?: boolean;
    __isOthers?: boolean;
};

type BestPerformanceMetric = {
    month?: string;
    year?: string | number;

    units?: number;
    net_sales?: number;
    asp?: number;
    cm1_profit?: number;
    unit_wise_profitability?: number;
};

type ProductBestPerformanceData = {
    units?: BestPerformanceMetric;
    net_sales?: BestPerformanceMetric;
    asp?: BestPerformanceMetric;
    cm1_profit?: BestPerformanceMetric;
    unit_wise_profitability?: BestPerformanceMetric;
};

type MetricItem = {
    label: string;
    value: string;
    color?: string;
};

type Props = {
    categorizedGrowth: CategorizedGrowth;
    month1: string;
    year1: string;
    month2: string;
    year2: string;
    month2Label: string;
    countryName?: string;
    isGlobalData: () => boolean;

    // old single-sheet export can stay for fallback, but we won't use it for the new button
    exportToExcel: (rows: SkuItem[], filename?: string) => void;
    getAllSkusForExport: () => SkuItem[];

    getAbbr: (m: string | number) => string;
    isPreviewMode: boolean;

    // optional header values for Excel helper header block
    companyName?: string;
    brandName?: string;
    homeCurrencyCode?: string;
    platformLabel?: string;
};


const API_BASE = `${process.env.NEXT_PUBLIC_API_BASE_URL}`;

const api = axios.create({ baseURL: API_BASE });

api.interceptors.request.use((cfg) => {
    const t = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
    if (t) cfg.headers.Authorization = `Bearer ${t}`;
    return cfg;
});

const INSIGHTS_KEY = "bi_sku_insights";

type ObjectiveObj = {
    growth_intent?: string;
    inventory_clearance_priority?: string;
    profit_priority?: string;
};

const isObjectiveObj = (v: unknown): v is ObjectiveObj =>
    !!v && typeof v === "object" && !Array.isArray(v);

const normalizeBullets = (raw?: string) => {
    if (!raw) return [];

    let parts = raw
        .split(/\r?\n+/g)
        .map((s: string) => s.trim())
        .filter(Boolean);

    if (parts.length <= 1) {
        parts = raw
            .split(/(?<=[.!?])\s+/g)
            .map((s: string) => s.trim())
            .filter(Boolean);
    }

    return parts
        .map((s: string) =>
            s
                .replace(/^[-•*]+\s*/, "")
                .replace(/^\d+[\).\]]\s*/, "")
                .trim()
        )
        .filter(Boolean);
};

const metricColors = [
    "border border-[#FDD36F] border-t-[#FDD36F]",
    "border border-[#75BBDA] border-t-[#75BBDA]",
    "border border-[#B75A5A] border-t-[#B75A5A]",
    "border border-[#7B9A6D] border-t-[#7B9A6D]",
    "border border-[#C49466] border-t-[#C49466]",
];

const metricOrder = [
    "units",
    "net sales",
    "asp",
    "cm1 profit",
    "cm1 profit per unit",
];

const toNum = (v: any) => {
    if (v === null || v === undefined) return 0;
    if (typeof v === "number") return Number.isFinite(v) ? v : 0;

    const n = Number(String(v).replace(/,/g, "").trim());
    return Number.isFinite(n) ? n : 0;
};

const getCurrencySymbolFromCodeLocal = (code?: string) => {
    const c = String(code || "").toUpperCase();

    if (c === "GBP") return "£";
    if (c === "USD") return "$";
    if (c === "CAD") return "C$";
    if (c === "EUR") return "€";
    if (c === "INR") return "₹";

    return c || "$";
};

const formatMoneyNoDecimal = (value: any, currencyCode?: string) => {
    const symbol = getCurrencySymbolFromCodeLocal(currencyCode);
    const n = Math.round(toNum(value));

    return `${symbol}${n.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
    })}`;
};

const formatMoneyTwoDecimal = (value: any, currencyCode?: string) => {
    const symbol = getCurrencySymbolFromCodeLocal(currencyCode);
    const n = toNum(value);

    return `${symbol}${n.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    })}`;
};

const formatUnitsNoDecimal = (value: any) => {
    return Math.round(toNum(value)).toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
    });
};

const formatBestPerformancePeriod = (
    month?: string,
    year?: string | number
) => {
    if (!month) return "-";

    const monthMap: Record<string, string> = {
        january: "Jan",
        february: "Feb",
        march: "Mar",
        april: "Apr",
        may: "May",
        june: "Jun",
        july: "Jul",
        august: "Aug",
        september: "Sep",
        october: "Oct",
        november: "Nov",
        december: "Dec",
    };

    const shortMonth =
        monthMap[String(month).toLowerCase()] || String(month).slice(0, 3);

    const shortYear = year ? String(year).slice(-2) : "";

    return shortYear ? `${shortMonth}'${shortYear}` : shortMonth;
};

const splitMetricValue = (value: string) => {
    const v = String(value || "").trim();
    const match = v.match(/^([^\(]+)\s*(\(.+\))?$/);

    const main = match?.[1]?.trim() || v;
    const delta = match?.[2] || "";
    const isNegative = delta.includes("-");

    return {
        main,
        delta,
        deltaColor: isNegative ? "#FF5C5C" : "#5EA68E",
    };
};

const getMetricBorderColorByLabel = (label: string, fallbackIndex = 0) => {
    const normalizedLabel = label.trim().toLowerCase();
    const metricIndex = metricOrder.indexOf(normalizedLabel);

    return metricColors[
        metricIndex !== -1 ? metricIndex : fallbackIndex % metricColors.length
    ];
};

const SkuAnalysisSection: React.FC<Props> = ({
    categorizedGrowth,
    month1,
    year1,
    month2,
    year2,
    month2Label,
    countryName,
    isGlobalData,
    exportToExcel,
    getAllSkusForExport,
    getAbbr,
    isPreviewMode,
    homeCurrencyCode = "",
    platformLabel = "Phormula",
}) => {
    const [activeTab, setActiveTab] = useState<TabKey>("all_skus");
    const [expandAllSkusOthers, setExpandAllSkusOthers] = useState(false);
    const [selectedInsightItem, setSelectedInsightItem] = useState<SkuItem | null>(null);

    const [bestPerformanceLoading, setBestPerformanceLoading] = useState(false);
    const [bestPerformanceError, setBestPerformanceError] = useState<string | null>(null);
    const [bestPerformanceData, setBestPerformanceData] =
        useState<ProductBestPerformanceData | null>(null);

    const [loadingInsight, setLoadingInsight] = useState(false);
    const [skuInsights, setSkuInsights] = useState<Record<string, SkuInsight>>({});
    const [selectedSku, setSelectedSku] = useState<string | null>(null);
    const [modalOpen, setModalOpen] = useState(false);

    const [fbType, setFbType] = useState<"like" | "dislike" | null>(null);
    const [fbText, setFbText] = useState("");
    const [fbSuccess, setFbSuccess] = useState(false);

    const saveInsightsToStorage = (insights: Record<string, SkuInsight>) => {
        if (typeof window === "undefined") return;
        localStorage.setItem(INSIGHTS_KEY, JSON.stringify(insights || {}));
    };

    const [mounted, setMounted] = useState(false);

    React.useEffect(() => {
        setMounted(true);
    }, []);

    const analyzeSkus = async (
        e?: React.MouseEvent<HTMLButtonElement> | React.FormEvent
    ) => {
        e?.preventDefault?.();
        e?.stopPropagation?.();

        if (loadingInsight) return;

        setLoadingInsight(true);

        try {
            const allSkus: SkuItem[] = [
                ...(categorizedGrowth.top_80_skus || []),
                ...(categorizedGrowth.new_skus || []),
                ...(categorizedGrowth.reviving_skus || []),
                ...(categorizedGrowth.other_skus || []),
            ];

            const res = await api.post<{ insights: Record<string, SkuInsight> }>(
                "/analyze_skus",
                {
                    month1,
                    year1,
                    month2,
                    year2,
                    country: countryName,
                    skus: allSkus,
                }
            );

            const insights = res.data?.insights || {};
            setSkuInsights(insights);
            saveInsightsToStorage(insights);
        } catch (err: any) {
            console.error("analyze_skus error:", err?.response?.data || err.message);
        } finally {
            setLoadingInsight(false);
        }
    };

    const getInsightByProductName = (
        productName: string
    ): [string, SkuInsight] | null => {
        if (!productName) return null;

        const needle = productName.toLowerCase().trim();

        let entry = Object.entries(skuInsights).find(
            ([, d]) => d.product_name?.toLowerCase().trim() === needle
        );

        if (!entry && isGlobalData()) {
            entry = Object.entries(skuInsights).find(([, d]) => {
                const n = d.product_name?.toLowerCase().trim();
                return n && (n.includes(needle) || needle.includes(n));
            });
        }

        return entry ? (entry as [string, SkuInsight]) : null;
    };

    const getInsightForItem = (item: SkuItem): [string, SkuInsight] | null => {
        if (isGlobalData()) return getInsightByProductName(item.product_name);
        if (item.sku && skuInsights[item.sku]) return [item.sku, skuInsights[item.sku]];
        return getInsightByProductName(item.product_name);
    };

    const tabOptions = useMemo(
        () => [
            { value: "all_skus" as const, label: "All SKUs" },
            { value: "top_80_skus" as const, label: "Top 80% SKUs" },
            { value: "new_skus" as const, label: "New SKUs" },
            { value: "reviving_skus" as const, label: "Reviving SKUs" },
            { value: "other_skus" as const, label: "Other SKUs" },
        ],
        []
    );

    const { data: userData } = useGetUserDataQuery();
    const companyName =
        (userData as any)?.companyName ||
        (userData as any)?.company_name ||
        (userData as any)?.company ||
        "";

    const brandName =
        (userData as any)?.brandName ||
        (userData as any)?.brand_name ||
        (userData as any)?.brand ||
        "";


    const handleDownloadSkuAnalysisTabsExcel = () => {
        if (isPreviewMode) return;

        const titleCountry =
            String(countryName || "").toLowerCase() === "global"
                ? "Global"
                : String(countryName || "").toUpperCase();

        const periodLabel = `${getAbbr(month2)}'${String(year2).slice(2)}`;

        exportSkuAnalysisMtdExcel({
            filename: `SKU-Analysis-MTD-${titleCountry}-${periodLabel}.xlsx`,

            titleLine: `${titleCountry} - SKU Analysis MTD - ${periodLabel}`,
            countryName: String(countryName || ""),
            titleCountry,
            platformLabel,

            periodLabel,
            companyName,
            brandName,
            homeCurrencyCode,

            month2Label: month2Label || periodLabel,
            categorizedGrowth,
        });
    };

    const currentTabData = useMemo(() => {
        const fullCurrent = (
            activeTab === "new_skus"
                ? categorizedGrowth.new_skus || []
                : activeTab === "reviving_skus"
                    ? categorizedGrowth.reviving_skus || []
                    : categorizedGrowth?.[activeTab] || []
        ) as SkuItem[];

        if (activeTab !== "all_skus") return fullCurrent;

        const sorted = [...fullCurrent].sort((a, b) => {
            const am = Number(a?.["Sales Mix (Month2)"] ?? -Infinity);
            const bm = Number(b?.["Sales Mix (Month2)"] ?? -Infinity);
            return bm - am;
        });

        if (expandAllSkusOthers) {
            return sorted;
        }

        const top5 = sorted.slice(0, 5);
        const rest = sorted.slice(5);

        if (!rest.length) return top5;

        const sumNum = (arr: any[], key: string) =>
            arr.reduce((acc, r) => acc + (Number(r?.[key]) || 0), 0);

        const wAvgGrowth = (arr: any[], field: string) => {
            const wKey = "net_sales_month2";
            const totalW = arr.reduce((s, r) => s + (Number(r?.[wKey]) || 0), 0);
            if (!totalW) return null;

            const val =
                arr.reduce((s, r) => {
                    const g = r?.[field];
                    const v = Number(g?.value ?? 0);
                    const w = Number(r?.[wKey] ?? 0);
                    return s + v * w;
                }, 0) / totalW;

            return { category: "Low Growth", value: val } as GrowthCategory;
        };

        const others: any = {
            product_name: "Others",
            "Sales Mix (Month2)": sumNum(rest, "Sales Mix (Month2)"),
            "Unit Growth": wAvgGrowth(rest, "Unit Growth"),
            "ASP Growth": wAvgGrowth(rest, "ASP Growth"),
            "Net Sales Growth": wAvgGrowth(rest, "Net Sales Growth"),
            "Sales Mix Change": wAvgGrowth(rest, "Sales Mix Change"),
            "CM1 Profit Impact": wAvgGrowth(rest, "CM1 Profit Impact"),
            "Profit Per Unit": wAvgGrowth(rest, "Profit Per Unit"),
        };

        return [...top5, others];
    }, [categorizedGrowth, activeTab, expandAllSkusOthers]);

    const formatMetricValueWithGrowth = (
        actualValue: any,
        growthValue: any,
        type: "money" | "number" = "money"
    ) => {
        const growth = Number(
            typeof growthValue === "object" ? growthValue?.value : growthValue
        );

        const safeGrowth = Number.isFinite(growth) ? growth : 0;
        const sign = safeGrowth > 0 ? "+" : "";

        const main =
            type === "number"
                ? formatUnitsNoDecimal(actualValue)
                : formatMoneyNoDecimal(actualValue, homeCurrencyCode);

        return `${main} (${sign}${safeGrowth.toFixed(2)}%)`;
    };

    const buildMetricsForSku = (item?: SkuItem | null): MetricItem[] => {
        if (!item) return [];

        return [
            {
                label: "Units",
                value: formatMetricValueWithGrowth(
                    item.total_quantity_month2,
                    item["Unit Growth"],
                    "number"
                ),
            },
            {
                label: "Net sales",
                value: formatMetricValueWithGrowth(
                    item.net_sales_month2,
                    item["Net Sales Growth"],
                    "money"
                ),
            },
            {
                label: "ASP",
                value: formatMetricValueWithGrowth(
                    item.asp_month2,
                    item["ASP Growth"],
                    "money"
                ),
            },
            {
                label: "CM1 profit",
                value: formatMetricValueWithGrowth(
                    item.profit_month2,
                    item["CM1 Profit Impact"],
                    "money"
                ),
            },
            {
                label: "CM1 profit per unit",
                value: formatMetricValueWithGrowth(
                    item.unit_wise_profitability_month2,
                    item["Profit Per Unit"],
                    "money"
                ),
            },
        ];
    };

    const renderGrowthCell = (growth: any) => {
        const isObj = typeof growth === "object" && growth !== null && "value" in growth;
        const rawVal = isObj ? growth.value : growth;

        const val = Number.isFinite(Number(rawVal)) ? Number(rawVal) : 0;

        const sign = val > 0 ? "+" : "";
        const text = `${sign}${val.toFixed(2)}%`;

        // ✅ NEW: handle zero case separately (NO arrow, NO color)
        if (val === 0) {
            return (
                <span className="flex items-center justify-center w-full font-semibold text-[#414042]">
                    <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-center">
                        {text}
                    </span>
                </span>
            );
        }

        const category =
            isObj && growth?.category
                ? growth.category
                : val >= 5
                    ? "High Growth"
                    : val < 0
                        ? "Negative Growth"
                        : "Low Growth";

        if (category === "High Growth") {
            return (
                <span className="flex items-center justify-center gap-2 w-full font-semibold text-[#5EA68E]">
                    <span className="w-4 flex justify-center shrink-0">
                        <FaArrowUp size={12} />
                    </span>
                    <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-center">
                        {text}
                    </span>
                </span>
            );
        }

        if (category === "Negative Growth") {
            return (
                <span className="flex items-center justify-center gap-2 w-full font-semibold text-[#FF5C5C]">
                    <span className="w-4 flex justify-center shrink-0">
                        <FaArrowDown size={12} />
                    </span>
                    <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-center">
                        {text}
                    </span>
                </span>
            );
        }

        return (
            <span className="flex items-center justify-center gap-2 w-full font-semibold text-[#414042]">
                <span className="w-4 flex justify-center shrink-0">
                    {val > 0 ? (
                        <FaArrowUp size={12} />
                    ) : val < 0 ? (
                        <FaArrowDown size={12} />
                    ) : null}
                </span>
                <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-center">
                    {text}
                </span>
            </span>
        );
    };

    const buildTableRows = useMemo<TableRow[]>(() => {
        const rows = (currentTabData || []) as any[];

        const tableRows: TableRow[] = rows.map((item, idx) => {
            const isOthers =
                activeTab === "all_skus" &&
                String(item?.product_name ?? "").toLowerCase().trim() === "others";

            return {
                sno: idx + 1,
                product_name:
                    String(item.product_name).trim() === "0"
                        ? item.sku || "N/A"
                        : item.product_name,
                sales_mix: `${Number(item["Sales Mix (Month2)"] ?? 0).toFixed(2)}%`,
                sales_mix_change: item["Sales Mix Change"],
                unit_growth: item["Unit Growth"],
                asp_growth: item["ASP Growth"],
                net_sales_growth: item["Net Sales Growth"],
                cm1_profit_impact: item["CM1 Profit Impact"],
                profit_per_unit: item["Profit Per Unit"],
                ai: item,
                __isOthers: isOthers,
            };
        });

        if ((categorizedGrowth?.[activeTab] as any[])?.length) {
            const rawRows = (categorizedGrowth[activeTab] || []) as any[];

            const sum = (k: string) =>
                rawRows.reduce((s, r) => s + Number(r?.[k] ?? 0), 0);

            const pct = (m1: number, m2: number) =>
                m1 === 0 ? 0 : ((m2 - m1) / m1) * 100;

            const totalSalesMixSum = rawRows.reduce(
                (s, r) => s + Number(r?.["Sales Mix (Month2)"] ?? 0),
                0
            );
            const rounded = Number(totalSalesMixSum.toFixed(2));
            const fixed = Math.abs(rounded - 100) < 0.05 ? 100 : rounded;

            tableRows.push({
                sno: "",
                product_name: "Total",
                sales_mix: `${fixed.toFixed(2)}%`,
                sales_mix_change:
                    activeTab !== "new_skus" && activeTab !== "reviving_skus" ? 0 : undefined,
                unit_growth: pct(
                    sum("total_quantity_month1"),
                    sum("total_quantity_month2")
                ),
                asp_growth: pct(sum("asp_month1"), sum("asp_month2")),
                net_sales_growth: pct(
                    sum("net_sales_month1"),
                    sum("net_sales_month2")
                ),
                cm1_profit_impact: pct(
                    sum("unit_wise_profitability_month1"),
                    sum("unit_wise_profitability_month2")
                ),
                profit_per_unit: pct(sum("profit_month1"), sum("profit_month2")),
                __isTotal: true,
            });
        }

        return tableRows;
    }, [currentTabData, categorizedGrowth, activeTab]);

    const columns = useMemo<ColumnDef<TableRow>[]>(() => {
        const showInsight = Object.keys(skuInsights).length > 0;

        const cols: ColumnDef<TableRow>[] = [
            { key: "sno", header: "S.No.", width: "60px" },
            {
                key: "product_name",
                header: "Product Name",
                width: "260px",
                cellClassName: "text-left",
                render: (row, value) => (
                    <span className={row.__isTotal ? "font-bold" : ""}>{value}</span>
                ),
            },
            {
                key: "sales_mix",
                header: `Sales Mix (${month2Label || "Month 2"})`,
                width: "150px",
            },
        ];

        if (activeTab !== "new_skus" && activeTab !== "reviving_skus") {
            cols.push({
                key: "sales_mix_change",
                header: "Sales Mix Change (%)",
                width: "170px",
                render: (_row, value) => renderGrowthCell(value),
            });
        }

        cols.push(
            {
                key: "unit_growth",
                header: "Unit Growth (%)",
                width: "150px",
                render: (_row, v) => renderGrowthCell(v),
            },
            {
                key: "asp_growth",
                header: "ASP Growth (%)",
                width: "150px",
                render: (_row, v) => renderGrowthCell(v),
            },
            {
                key: "net_sales_growth",
                header: "Net Sales Growth (%)",
                width: "170px",
                render: (_row, v) => renderGrowthCell(v),
            },
            {
                key: "cm1_profit_impact",
                header: "CM1 Profit Impact (%)",
                width: "190px",
                render: (_row, v) => renderGrowthCell(v),
            },
            {
                key: "profit_per_unit",
                header: "CM1 Profit Per Unit (%)",
                width: "200px",
                render: (_row, v) => renderGrowthCell(v),
            }
        );

        if (showInsight) {
            cols.push({
                key: "ai",
                header: "AI Insight",
                width: "140px",
                render: (row) => {
                    if (row.__isTotal) return "";

                    if (row.__isOthers) return "";

                    const item = row.ai as SkuItem;
                    const entry = getInsightForItem(item);

                    if (!entry) return <em style={{ color: "#888" }}>--</em>;

                    return (
                        <button
                            type="button"
                            className="font-semibold underline text-[#414042]"
                            onClick={() => {
                                setSelectedSku(entry[0]);
                                setSelectedInsightItem(item);
                                setModalOpen(true);
                                setFbType(null);
                                setFbText("");
                                setFbSuccess(false);
                                setBestPerformanceData(null);
                                setBestPerformanceError(null);
                            }}
                        >
                            View Insights
                        </button>
                    );
                },
            });
        }

        return cols;
    }, [
        activeTab,
        month2Label,
        skuInsights,
        getInsightForItem,
        setSelectedSku,
        setModalOpen,
        setFbType,
        setFbText,
        setFbSuccess,
    ]);

    const hasAnyRows = (
        ["all_skus", "top_80_skus", "new_skus", "reviving_skus", "other_skus"] as TabKey[]
    ).some((k) => (categorizedGrowth[k] || []).length > 0);

    useEffect(() => {
        if (!modalOpen) return;

        const productName = String(
            selectedInsightItem?.product_name || selectedSku || ""
        ).trim();

        if (!productName) return;

        const lowerName = productName.toLowerCase();

        if (
            lowerName === "total" ||
            lowerName === "grand total" ||
            lowerName === "others" ||
            lowerName === "other skus"
        ) {
            setBestPerformanceData(null);
            setBestPerformanceError(null);
            setBestPerformanceLoading(false);
            return;
        }

        const ac = new AbortController();

        const fetchBestPerformance = async () => {
            try {
                setBestPerformanceLoading(true);
                setBestPerformanceError(null);
                setBestPerformanceData(null);

                const token =
                    typeof window !== "undefined"
                        ? localStorage.getItem("jwtToken")
                        : null;

                if (!token) throw new Error("Missing token");

                const res = await fetch(
                    `${process.env.NEXT_PUBLIC_API_BASE_URL}/ProductBestPerformance`,
                    {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json",
                            Authorization: `Bearer ${token}`,
                        },
                        body: JSON.stringify({
                            product_name: productName,
                            country: countryName || "global",
                            home_currency: homeCurrencyCode || "USD",
                        }),
                        cache: "no-store",
                        signal: ac.signal,
                    }
                );

                const json = await res.json().catch(() => ({}));

                if (!res.ok) {
                    throw new Error(json?.error || "Failed to fetch best performance");
                }

                setBestPerformanceData(json?.best_performance ?? null);
            } catch (e: any) {
                if (e?.name === "AbortError") return;
                setBestPerformanceError(e?.message || "Failed to load best performance");
            } finally {
                setBestPerformanceLoading(false);
            }
        };

        fetchBestPerformance();

        return () => ac.abort();
    }, [
        modalOpen,
        selectedSku,
        selectedInsightItem?.product_name,
        countryName,
        homeCurrencyCode,
    ]);

    if (!hasAnyRows) return null;

    const insightData: SkuInsight | null =
        selectedSku && skuInsights[selectedSku]
            ? skuInsights[selectedSku]
            : selectedSku
                ? getInsightByProductName(selectedSku)?.[1] || null
                : null;

    const getSkuEmptyMessage = () => {
        if (activeTab === "new_skus") {
            return "No new SKU has been launched within the last 6 months.";
        }

        if (activeTab === "reviving_skus") {
            return "No reviving SKU was identified.";
        }

        return "No data found.";
    };

    return (
        <>
            <div className="mt-4 overflow-hidden rounded-xl border bg-white p-4 shadow-sm sm:p-5">
                <div className="flex min-w-0 flex-col gap-4">
                    <div className="flex min-w-0 items-center justify-between gap-2 xl:hidden">
                        <PageBreadcrumb pageTitle="SKU Analysis MTD" variant="page" align="left" />

                        <div className="flex shrink-0 items-center gap-2">
                            <button
                                type="button"
                                onClick={(e) => {
                                    e.preventDefault();
                                    e.stopPropagation();
                                    analyzeSkus(e);
                                }}
                                disabled={
                                    loadingInsight ||
                                    isPreviewMode ||
                                    ![
                                        "top_80_skus",
                                        "new_skus",
                                        "reviving_skus",
                                        "other_skus",
                                    ].some(
                                        (k) =>
                                            (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])?.length > 0
                                    )
                                }
                                className="inline-flex h-9 min-w-[120px] items-center justify-center gap-1 whitespace-nowrap rounded-sm bg-custom-effect px-4 text-xs text-[#F8EDCE] transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50 disabled:shadow-none disabled:transform-none 2xl:text-sm"
                                style={{ boxShadow: "0px 4px 4px 0px #00000040" }}
                            >
                                <BsStars
                                    className="shrink-0"
                                    style={{ fontSize: "12px", color: "#F8EDCE" }}
                                />
                                {loadingInsight ? "Generating..." : "AI Insights"}
                            </button>

                            {activeTab === "all_skus" && (
                                <button
                                    type="button"
                                    onClick={() => setExpandAllSkusOthers((prev) => !prev)}
                                    title={expandAllSkusOthers ? "Collapse rows" : "Expand all rows"}
                                    aria-label={expandAllSkusOthers ? "Collapse rows" : "Expand all rows"}
                                    className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                >
                                    {expandAllSkusOthers ? (
                                        <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                                    ) : (
                                        <RiExpandDiagonalFill size={18} className="font-extrabold" />
                                    )}
                                </button>
                            )}

                            <DownloadIconButton
                                disabled={isPreviewMode}
                                onClick={handleDownloadSkuAnalysisTabsExcel}
                                className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                            />
                        </div>
                    </div>

                    <div className="w-full min-w-0 overflow-hidden xl:hidden">
                        <div className="no-scrollbar w-full max-w-full overflow-x-auto overflow-y-hidden">
                            <div className="inline-flex min-w-max">
                                <SegmentedToggle<TabKey>
                                    value={activeTab}
                                    options={tabOptions}
                                    onChange={setActiveTab}
                                    className="bg-white"
                                    textSizeClass="text-xs 2xl:text-sm"
                                />
                            </div>
                        </div>
                    </div>

                    <div className="hidden min-w-0 xl:flex xl:items-center xl:justify-between xl:gap-6">
                        <PageBreadcrumb pageTitle="SKU Analysis MTD" variant="page" align="left" />

                        <div className="flex min-w-0 flex-1 items-center justify-end gap-3">
                            <div className="min-w-0 max-w-full overflow-hidden">
                                <div className="no-scrollbar max-w-full overflow-x-auto overflow-y-hidden">
                                    <div className="inline-flex min-w-max">
                                        <SegmentedToggle<TabKey>
                                            value={activeTab}
                                            options={tabOptions}
                                            onChange={setActiveTab}
                                            className="bg-white"
                                            textSizeClass="text-xs 2xl:text-sm"
                                        />
                                    </div>
                                </div>
                            </div>

                            <button
                                type="button"
                                onClick={(e) => {
                                    e.preventDefault();
                                    e.stopPropagation();
                                    analyzeSkus(e);
                                }}
                                disabled={
                                    loadingInsight ||
                                    isPreviewMode ||
                                    ![
                                        "top_80_skus",
                                        "new_skus",
                                        "reviving_skus",
                                        "other_skus",
                                    ].some(
                                        (k) =>
                                            (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])
                                                ?.length > 0
                                    )
                                }
                                className="inline-flex h-9 min-w-[120px] shrink-0 items-center justify-center gap-1 whitespace-nowrap rounded-sm bg-custom-effect px-4 text-xs text-[#F8EDCE] transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50 disabled:shadow-none disabled:transform-none 2xl:text-sm"
                                style={{ boxShadow: "0px 4px 4px 0px #00000040" }}
                            >
                                <BsStars
                                    className="shrink-0"
                                    style={{ fontSize: "12px", color: "#F8EDCE" }}
                                />
                                {loadingInsight ? "Generating..." : "AI Insights"}
                            </button>

                            {activeTab === "all_skus" && (
                                <button
                                    type="button"
                                    onClick={() => setExpandAllSkusOthers((prev) => !prev)}
                                    title={expandAllSkusOthers ? "Collapse rows" : "Expand all rows"}
                                    aria-label={expandAllSkusOthers ? "Collapse rows" : "Expand all rows"}
                                    className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                >
                                    {expandAllSkusOthers ? (
                                        <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                                    ) : (
                                        <RiExpandDiagonalFill size={18} className="font-extrabold" />
                                    )}
                                </button>
                            )}

                            <DownloadIconButton
                                disabled={isPreviewMode}
                                onClick={handleDownloadSkuAnalysisTabsExcel}
                                className="shrink-0 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                            />
                        </div>
                    </div>
                </div>

                <div className="pt-6">
                    <DataTable<TableRow>
                        columns={columns}
                        data={buildTableRows}
                        stickyHeader
                        zebra
                        paginate={false}
                        scrollY={false}
                        maxHeight="none"
                        loading={false}
                        headerMaxWidth={140}
                        emptyMessage={getSkuEmptyMessage()}
                        rowClassName={(row) =>
                            row.__isTotal
                                ? "bg-[#EFEFEF] font-semibold"
                                : row.__isOthers && activeTab === "all_skus" && !expandAllSkusOthers
                                    ? "cursor-pointer"
                                    : ""
                        }
                        onRowClick={(row) => {
                            if (
                                activeTab === "all_skus" &&
                                !expandAllSkusOthers &&
                                row.__isOthers
                            ) {
                                setExpandAllSkusOthers(true);
                            }
                        }}
                        isTotalRow={(row) => !!row.__isTotal}
                        bodyMaxHeight={
                            buildTableRows.filter((row) => !row.__isTotal).length > 15
                                ? 40 * 15
                                : undefined
                        }
                    />

                </div>

                <div className="mt-2 flex justify-center">
                    <div className="mt-1 grid grid-cols-2 justify-items-start gap-x-6 gap-y-2 text-xs text-[#414042] sm:grid-cols-4 lg:flex lg:flex-wrap lg:items-center lg:gap-10 2xl:text-sm">
                        <span className="inline-flex items-center gap-2">
                            <span className="inline-flex items-center gap-2 font-bold text-[#5EA68E]">
                                <FaArrowUp className="text-[10px] 2xl:text-xs" /> High growth
                            </span>
                        </span>

                        <span className="inline-flex items-center gap-2">
                            <span className="inline-flex items-center gap-2 font-bold text-[#FF5C5C]">
                                <FaArrowDown className="text-[10px] 2xl:text-xs" /> Negative growth
                            </span>
                        </span>

                        <span className="inline-flex items-center gap-2 whitespace-nowrap font-bold">
                            <span className="inline-flex items-center gap-1 text-[#414042]">
                                <FaArrowUp className="text-[10px] 2xl:text-xs" /> + /
                                <FaArrowDown className="text-[10px] 2xl:text-xs" /> -
                            </span>
                            Low growth
                        </span>

                        <span className="inline-flex items-center gap-2 font-bold">
                            <span className="text-sm leading-none 2xl:text-base">-</span>
                            Past data for SKU is not available
                        </span>
                    </div>
                </div>
            </div>

            {mounted &&
                createPortal(
                    (() => {
                        if (!modalOpen || !selectedSku) return null;

                        const insightData =
                            skuInsights[selectedSku as keyof typeof skuInsights] ||
                            getInsightByProductName(selectedSku as string)?.[1];

                        if (!insightData) return null;

                        const objectiveObj = isObjectiveObj(insightData.objective)
                            ? insightData.objective
                            : undefined;

                        const toBullets = (raw?: string) => normalizeBullets(raw);
                        const recoBullets = toBullets(insightData.recommendation);
                        const inventoryRecoBullets = toBullets(insightData.inventory_recommendation);

                        const journeyBullets = (insightData.product_journey || [])
                            .map((s: string) => String(s || "").trim())
                            .filter(Boolean);

                        return (
                            <AnimatePresence>
                                {modalOpen && (
                                    <>
                                        <motion.div
                                            className="fixed inset-0 z-[999999] h-full bg-black/40"
                                            initial={{ opacity: 0 }}
                                            animate={{ opacity: 1 }}
                                            exit={{ opacity: 0 }}
                                            onClick={() => {
                                                setModalOpen(false);
                                                setSelectedInsightItem(null);
                                                setBestPerformanceData(null);
                                                setBestPerformanceError(null);
                                            }}
                                        />

                                        <motion.aside
                                            className="fixed right-0 top-0 z-[1000000] h-screen w-[95vw] sm:w-[75vw] md-[60vw] lg:w-[50vw] min-[1700px]:w-[50vw] bg-white shadow-2xl"
                                            initial={{ x: 520 }}
                                            animate={{ x: 0 }}
                                            exit={{ x: 520 }}
                                            transition={{ type: "tween", duration: 0.25 }}
                                        >
                                            <div className="flex h-full flex-col gap-4">
                                                <div className="shrink-0 border-b border-slate-200 px-4 py-3 flex items-center justify-between gap-3">
                                                    <div className="min-w-0">
                                                        <div className="truncate text-2xl font-semibold leading-tight text-[#414042]">
                                                            <div className="flex items-center gap-1 flex-wrap">
                                                                <PageBreadcrumb
                                                                    pageTitle="Detailed View - "
                                                                    variant="page"
                                                                    textSize="2xl"
                                                                />
                                                                <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                                                                    {insightData.product_name || selectedSku}
                                                                </span>
                                                            </div>

                                                        </div>
                                                    </div>

                                                    <button
                                                        onClick={() => {
                                                            setModalOpen(false);
                                                            setSelectedInsightItem(null);
                                                            setBestPerformanceData(null);
                                                            setBestPerformanceError(null);
                                                        }}
                                                        className="shrink-0 rounded-lg border border-slate-200 px-3 py-1.5 text-lg leading-none text-slate-500 hover:bg-slate-50"
                                                    >
                                                        ×
                                                    </button>
                                                </div>

                                                <div className="flex-1 overflow-y-auto px-4  space-y-6">

                                                    {(() => {
                                                        const sortedMetrics = buildMetricsForSku(selectedInsightItem).sort((a, b) => {
                                                            const aIndex = metricOrder.indexOf(a.label.trim().toLowerCase());
                                                            const bIndex = metricOrder.indexOf(b.label.trim().toLowerCase());

                                                            const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
                                                            const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

                                                            return safeAIndex - safeBIndex;
                                                        });

                                                        if (!sortedMetrics.length) return null;

                                                        return (
                                                            <div>
                                                                <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg">
                                                                    Metrics
                                                                </div>

                                                                <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                                                                    {sortedMetrics.map((m, i) => {
                                                                        const { main, delta, deltaColor } = splitMetricValue(m.value);

                                                                        return (
                                                                            <div
                                                                                key={`${m.label}-${i}`}
                                                                                className={`rounded-lg border border-t-4 ${getMetricBorderColorByLabel(m.label, i)
                                                                                    } px-3 py-2`}
                                                                            >
                                                                                <div className="text-[10px] text-charcoal-400 2xl:text-xs">
                                                                                    {m.label
                                                                                        .replace(/\b\w/g, (char) => char.toUpperCase())
                                                                                        .replace("Cm1", "CM1")}
                                                                                </div>

                                                                                <div className="flex flex-col leading-tight">
                                                                                    <span className="text-sm font-bold 2xl:text-lg text-[#414042]">
                                                                                        {main}
                                                                                    </span>

                                                                                    {delta ? (
                                                                                        <span
                                                                                            className="text-[10px] 2xl:text-xs font-semibold"
                                                                                            style={{ color: deltaColor }}
                                                                                        >
                                                                                            {delta}
                                                                                        </span>
                                                                                    ) : null}
                                                                                </div>
                                                                            </div>
                                                                        );
                                                                    })}
                                                                </div>
                                                            </div>
                                                        );
                                                    })()}
                                                    <div>
                                                        <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg">
                                                            Overall Best Performance
                                                        </div>
                                                        <div className="mb-2 text-[11px] text-charcoal-400 2xl:text-xs">
                                                            Best performance is calculated from overall historical data, not just the selected period.
                                                        </div>

                                                        {bestPerformanceLoading ? (
                                                            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                                                                Loading best performance...
                                                            </div>
                                                        ) : bestPerformanceError ? (
                                                            <div className="rounded-lg border border-red-100 bg-red-50 px-3 py-3 text-xs text-red-600 2xl:text-sm">
                                                                {bestPerformanceError}
                                                            </div>
                                                        ) : bestPerformanceData ? (
                                                            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3 xl:grid-cols-5">
                                                                {[
                                                                    {
                                                                        label: "Units",
                                                                        value: formatUnitsNoDecimal(bestPerformanceData?.units?.units),
                                                                        period: formatBestPerformancePeriod(
                                                                            bestPerformanceData?.units?.month,
                                                                            bestPerformanceData?.units?.year
                                                                        ),
                                                                    },
                                                                    {
                                                                        label: "Net Sales",
                                                                        value: formatMoneyNoDecimal(
                                                                            bestPerformanceData?.net_sales?.net_sales,
                                                                            homeCurrencyCode
                                                                        ),
                                                                        period: formatBestPerformancePeriod(
                                                                            bestPerformanceData?.net_sales?.month,
                                                                            bestPerformanceData?.net_sales?.year
                                                                        ),
                                                                    },
                                                                    {
                                                                        label: "ASP",
                                                                        value: formatMoneyTwoDecimal(
                                                                            bestPerformanceData?.asp?.asp,
                                                                            homeCurrencyCode
                                                                        ),
                                                                        period: formatBestPerformancePeriod(
                                                                            bestPerformanceData?.asp?.month,
                                                                            bestPerformanceData?.asp?.year
                                                                        ),
                                                                    },
                                                                    {
                                                                        label: "CM1 Profit",
                                                                        value: formatMoneyNoDecimal(
                                                                            bestPerformanceData?.cm1_profit?.cm1_profit,
                                                                            homeCurrencyCode
                                                                        ),
                                                                        period: formatBestPerformancePeriod(
                                                                            bestPerformanceData?.cm1_profit?.month,
                                                                            bestPerformanceData?.cm1_profit?.year
                                                                        ),
                                                                    },
                                                                    {
                                                                        label: "CM1 Profit Per Unit",
                                                                        value: formatMoneyTwoDecimal(
                                                                            bestPerformanceData?.unit_wise_profitability?.unit_wise_profitability,
                                                                            homeCurrencyCode
                                                                        ),
                                                                        period: formatBestPerformancePeriod(
                                                                            bestPerformanceData?.unit_wise_profitability?.month,
                                                                            bestPerformanceData?.unit_wise_profitability?.year
                                                                        ),
                                                                    },
                                                                ].map((card, index) => (
                                                                    <div
                                                                        key={card.label}
                                                                        className={`rounded-lg border border-t-4 ${getMetricBorderColorByLabel(card.label, index)
                                                                            } px-3 py-2`}
                                                                    >
                                                                        <div className="text-[10px] 2xl:text-xs text-charcoal-400">
                                                                            {card.label}
                                                                        </div>

                                                                        <div className="flex flex-col leading-tight">
                                                                            <span className="mt-1 text-[10px] 2xl:text-xs text-[#414042]">
                                                                                {card.period}
                                                                            </span>

                                                                            <span className="mt-2 text-sm 2xl:text-lg font-bold text-[#414042]">
                                                                                {card.value}
                                                                            </span>
                                                                        </div>
                                                                    </div>
                                                                ))}
                                                            </div>
                                                        ) : (
                                                            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-3 text-xs text-charcoal-500 2xl:text-sm">
                                                                —
                                                            </div>
                                                        )}
                                                    </div>
                                                    {objectiveObj && (
                                                        <div className="space-y-2">
                                                            <div className="mb-2 text-xs font-semibold text-charcoal-700 sm:text-sm 2xl:text-lg text-charcoal-700">Objectives</div>

                                                            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
                                                                <div className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
                                                                    <div className="2xl:text-sm text-xs text-slate-500">Primary Focus</div>
                                                                    <div className="2xl:text-base text-sm font-semibold text-[#414042] mt-1">
                                                                        {(objectiveObj?.growth_intent || "balanced")
                                                                            .replaceAll("_", " ")
                                                                            .toLowerCase()
                                                                            .replace(/^\w/, (c) => c.toUpperCase())}
                                                                    </div>
                                                                </div>

                                                                <div className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
                                                                    <div className="2xl:text-sm text-xs text-slate-500">Profit Strategy</div>
                                                                    <div className="2xl:text-base text-sm font-semibold text-[#414042] mt-1">
                                                                        {(objectiveObj?.profit_priority || "protect growth")
                                                                            .replaceAll("_", " ")
                                                                            .toLowerCase()
                                                                            .replace(/^\w/, (c) => c.toUpperCase())}
                                                                    </div>
                                                                </div>

                                                                <div className="rounded-lg border border-slate-200 bg-slate-50 px-4 py-3">
                                                                    <div className="2xl:text-sm text-xs text-slate-500">Inventory Dilution</div>
                                                                    <div className="2xl:text-base text-sm font-semibold text-[#414042] mt-1">
                                                                        {objectiveObj?.inventory_clearance_priority ? "Yes" : "No"}
                                                                    </div>
                                                                </div>
                                                            </div>
                                                        </div>
                                                    )}

                                                    <div className="space-y-2">
                                                        <div className="mb-2 text-xs font-semibold text-charcoal-500 sm:text-sm 2xl:text-lg">
                                                            Recommendations
                                                        </div>

                                                        {recoBullets.length > 0 && (
                                                            <div>
                                                                <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                                                                    Action
                                                                </div>
                                                                <ul className="list-disc space-y-1 pl-5 2xl:text-sm text-xs text-[#414042]">
                                                                    {recoBullets.map((pt, i) => (
                                                                        <li key={i}>{pt}</li>
                                                                    ))}
                                                                </ul>
                                                            </div>
                                                        )}

                                                        {inventoryRecoBullets.length > 0 && (
                                                            <div>
                                                                <div className="text-xs font-semibold text-charcoal-500 2xl:text-sm">
                                                                    Inventory
                                                                </div>
                                                                <ul className="list-disc space-y-1 pl-5 2xl:text-sm text-xs text-[#414042]">
                                                                    {inventoryRecoBullets.map((pt, i) => (
                                                                        <li key={i}>{pt}</li>
                                                                    ))}
                                                                </ul>
                                                            </div>
                                                        )}

                                                        {!recoBullets.length && !inventoryRecoBullets.length && (
                                                            <div className="text-sm text-slate-500">
                                                                No recommendation available.
                                                            </div>
                                                        )}
                                                    </div>

                                                    <div className="space-y-2">
                                                        <div className="">
                                                            <Productinfoinpopup
                                                                productname={insightData.product_name}
                                                                countryName={countryName}
                                                            />
                                                        </div>
                                                    </div>

                                                    {journeyBullets.length > 0 && (
                                                        <div className="space-y-2">
                                                            <div className="flex items-center gap-1 flex-wrap">
                                                                <PageBreadcrumb
                                                                    pageTitle="Product Journey"
                                                                    variant="page"
                                                                    textSize="lg"
                                                                />
                                                            </div>

                                                            <ol className="list-decimal list-outside space-y-2 pl-3 text-xs text-[#414042] 2xl:text-sm marker:font-semibold">
                                                                {journeyBullets.map((j, i) => (
                                                                    <li key={i}>
                                                                        <span>{j}</span>
                                                                    </li>
                                                                ))}
                                                            </ol>
                                                        </div>
                                                    )}
                                                </div>
                                            </div>

                                        </motion.aside>
                                    </>
                                )}
                            </AnimatePresence>
                        );
                    })(),
                    document.body
                )}
        </>
    );
};

export default SkuAnalysisSection;
