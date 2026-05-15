"use client";
import React, { useMemo, useState } from "react";
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

export type TabKey =
    | "top_80_skus"
    | "new_or_reviving_skus"
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
    new_or_reviving_skus: SkuItem[];
    other_skus: SkuItem[];
    all_skus?: SkuItem[];

    top_80_total?: SkuItem | null;
    new_or_reviving_total?: SkuItem | null;
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
                ...(categorizedGrowth.new_or_reviving_skus || []),
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
            { value: "new_or_reviving_skus" as const, label: "New/Reviving SKUs" },
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
        const fullCurrent = (categorizedGrowth?.[activeTab] || []) as SkuItem[];

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
                    activeTab !== "new_or_reviving_skus" ? 0 : undefined,
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

        if (activeTab !== "new_or_reviving_skus") {
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

                    if (row.__isOthers) {
                        return (
                            <button
                                type="button"
                                className="font-semibold underline text-[#414042]"
                                onClick={() => setExpandAllSkusOthers(true)}
                            >
                                Expand SKUs
                            </button>
                        );
                    }

                    const item = row.ai as SkuItem;
                    const entry = getInsightForItem(item);

                    if (!entry) return <em style={{ color: "#888" }}>--</em>;

                    return (
                        <button
                            type="button"
                            className="font-semibold underline text-[#414042]"
                            onClick={() => {
                                setSelectedSku(entry[0]);
                                setModalOpen(true);
                                setFbType(null);
                                setFbText("");
                                setFbSuccess(false);
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
        ["all_skus", "top_80_skus", "new_or_reviving_skus", "other_skus"] as TabKey[]
    ).some((k) => (categorizedGrowth[k] || []).length > 0);

    if (!hasAnyRows) return null;

    const insightData: SkuInsight | null =
        selectedSku && skuInsights[selectedSku]
            ? skuInsights[selectedSku]
            : selectedSku
                ? getInsightByProductName(selectedSku)?.[1] || null
                : null;


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
                                    !["top_80_skus", "new_or_reviving_skus", "other_skus"].some(
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
                                    !["top_80_skus", "new_or_reviving_skus", "other_skus"].some(
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
                        paginate
                        pageSize={10}
                        maxHeight="60vh"
                        loading={false}
                        headerMaxWidth={140}
                        rowClassName={(row) => (row.__isTotal ? "bg-[#D9D9D933] font-bold" : "")}
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
                                            className="fixed inset-0 z-[999999] bg-black/40"
                                            initial={{ opacity: 0 }}
                                            animate={{ opacity: 1 }}
                                            exit={{ opacity: 0 }}
                                            onClick={() => setModalOpen(false)}
                                        />

                                        <motion.aside
                                            className="fixed right-0 top-0 z-[1000000] flex h-screen w-[95vw] max-w-[720px] flex-col bg-white shadow-2xl"
                                            initial={{ x: 520 }}
                                            animate={{ x: 0 }}
                                            exit={{ x: 520 }}
                                            transition={{ type: "tween", duration: 0.25 }}
                                        >
                                            <div className="flex shrink-0 items-start justify-between gap-3 border-b border-slate-200 p-4">
                                                <div>
                                                    <div className="flex items-center gap-2">
                                                        <div className="text-sm text-slate-500">Detailed View</div>

                                                        <span className="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] text-slate-700">
                                                            {countryName ? String(countryName).toUpperCase() : "—"}
                                                        </span>
                                                    </div>

                                                    <div className="text-lg font-semibold text-slate-900">
                                                        {insightData.product_name || selectedSku}
                                                    </div>
                                                </div>

                                                <button
                                                    onClick={() => setModalOpen(false)}
                                                    className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
                                                >
                                                    ✕
                                                </button>
                                            </div>

                                            <div className="flex-1 space-y-5 overflow-y-auto p-4">
                                                {objectiveObj && (
                                                    <div className="space-y-2">
                                                        <div className="text-sm font-semibold text-slate-800">Objectives</div>

                                                        <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
                                                            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                                                                <div className="text-xs text-slate-500">Primary Focus</div>
                                                                <div className="mt-1 text-sm font-bold text-slate-800">
                                                                    {objectiveObj?.growth_intent || "balanced"}
                                                                </div>
                                                            </div>

                                                            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                                                                <div className="text-xs text-slate-500">Profit Strategy</div>
                                                                <div className="mt-1 text-sm font-bold text-slate-800">
                                                                    {objectiveObj?.profit_priority?.replaceAll("_", " ") ||
                                                                        "protect growth"}
                                                                </div>
                                                            </div>

                                                            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                                                                <div className="text-xs text-slate-500">Inventory Dilution</div>
                                                                <div className="mt-1 text-sm font-bold text-slate-800">
                                                                    {objectiveObj?.inventory_clearance_priority ? "Yes" : "No"}
                                                                </div>
                                                            </div>
                                                        </div>
                                                    </div>
                                                )}

                                                <div className="space-y-2">
                                                    <div className="text-sm font-semibold text-slate-800">
                                                        Recommendations
                                                    </div>

                                                    {recoBullets.length > 0 && (
                                                        <div>
                                                            <div className="mb-1 text-xs font-semibold text-blue-900">
                                                                💡 Action
                                                            </div>
                                                            <ul className="list-disc space-y-1 pl-5 text-xs text-slate-700">
                                                                {recoBullets.map((pt, i) => (
                                                                    <li key={i}>{pt}</li>
                                                                ))}
                                                            </ul>
                                                        </div>
                                                    )}

                                                    {inventoryRecoBullets.length > 0 && (
                                                        <div>
                                                            <div className="mb-1 text-xs font-semibold text-amber-900">
                                                                📦 Inventory
                                                            </div>
                                                            <ul className="list-disc space-y-1 pl-5 text-xs text-slate-700">
                                                                {inventoryRecoBullets.map((pt, i) => (
                                                                    <li key={i}>{pt}</li>
                                                                ))}
                                                            </ul>
                                                        </div>
                                                    )}

                                                    {!recoBullets.length && !inventoryRecoBullets.length && (
                                                        <div className="text-xs text-slate-500">
                                                            No recommendation available.
                                                        </div>
                                                    )}
                                                </div>

                                                <div className="space-y-2">
                                                    <div className="rounded-lg border border-[#e5e7eb] p-2">
                                                        <Productinfoinpopup
                                                            productname={insightData.product_name}
                                                            countryName={countryName}
                                                        />
                                                    </div>
                                                </div>

                                                {journeyBullets.length > 0 && (
                                                    <div className="space-y-2">
                                                        <div className="text-sm font-semibold text-slate-800">
                                                            Product Journey
                                                        </div>

                                                        <ul className="space-y-2 text-xs text-slate-700 2xl:text-sm">
                                                            {journeyBullets.map((j, i) => (
                                                                <li key={i} className="flex gap-2">
                                                                    <span className="mt-[2px] text-slate-400">→</span>
                                                                    <span>{j}</span>
                                                                </li>
                                                            ))}
                                                        </ul>
                                                    </div>
                                                )}
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