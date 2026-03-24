"use client";

import React, { useMemo, useState } from "react";
import { BsStars } from "react-icons/bs";
import { FaArrowUp, FaArrowDown } from "react-icons/fa";

import DataTable, { ColumnDef, Row } from "@/components/ui/table/DataTable";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SegmentedToggle from "@/components/ui/SegmentedToggle";

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
    skuInsights: Record<string, SkuInsight>;
    loadingInsight: boolean;
    analyzeSkus: () => void;
    exportToExcel: (rows: SkuItem[], filename?: string) => void;
    getAllSkusForExport: () => SkuItem[];
    getAbbr: (m: string | number) => string;
    getInsightForItem: (item: SkuItem) => [string, SkuInsight] | null;
    setSelectedSku: (sku: string | null) => void;
    setModalOpen: (open: boolean) => void;
    setFbType: (t: "like" | "dislike" | null) => void;
    setFbText: (v: string) => void;
    setFbSuccess: (v: boolean) => void;
    isPreviewMode: boolean;
};

const SkuAnalysisSection: React.FC<Props> = ({
    categorizedGrowth,
    month1,
    year1,
    month2,
    year2,
    month2Label,
    skuInsights,
    loadingInsight,
    analyzeSkus,
    exportToExcel,
    getAllSkusForExport,
    getAbbr,
    getInsightForItem,
    setSelectedSku,
    setModalOpen,
    setFbType,
    setFbText,
    setFbSuccess,
    isPreviewMode,
}) => {
    const [activeTab, setActiveTab] = useState<TabKey>("all_skus");
    const [expandAllSkusOthers, setExpandAllSkusOthers] = useState(false);

    const tabOptions = useMemo(
        () => [
            { value: "all_skus" as const, label: "All SKUs" },
            { value: "top_80_skus" as const, label: "Top 80% SKUs" },
            { value: "new_or_reviving_skus" as const, label: "New/Reviving SKUs" },
            { value: "other_skus" as const, label: "Other SKUs" },
        ],
        []
    );

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
        if (growth == null) return "N/A";

        const isObj = typeof growth === "object" && "value" in growth;
        const val = Number(isObj ? growth.value : growth);

        if (!Number.isFinite(val)) return "N/A";

        const sign = val > 0 ? "+" : "";
        const text = `${sign}${val.toFixed(2)}%`;

        const category = isObj
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
                    <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-right">
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
                    <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-right">
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
                <span className="tabular-nums inline-block w-[50px] 2xl:w-[60px] text-right">
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
                sales_mix:
                    item["Sales Mix (Month2)"] != null
                        ? `${Number(item["Sales Mix (Month2)"]).toFixed(2)}%`
                        : "N/A",
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

    return (
        <div className="mt-4 overflow-hidden rounded-xl border bg-white p-4 shadow-sm sm:p-5">
            <div className="flex min-w-0 flex-col gap-4">
                <div className="flex min-w-0 items-center justify-between gap-2 xl:hidden">
                    <PageBreadcrumb pageTitle="SKU Analysis MTD" variant="page" align="left" />

                    <div className="flex shrink-0 items-center gap-2">
                        <button
                            type="button"
                            onClick={analyzeSkus}
                            disabled={
                                isPreviewMode ||
                                !["top_80_skus", "new_or_reviving_skus", "other_skus"].some(
                                    (k) =>
                                        (categorizedGrowth[k as keyof CategorizedGrowth] as SkuItem[])
                                            ?.length > 0
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
                            onClick={() => {
                                const file = `AllSKUs-${getAbbr(month1)}'${String(year1).slice(
                                    2
                                )}vs${getAbbr(month2)}'${String(year2).slice(2)}.xlsx`;
                                exportToExcel(getAllSkusForExport(), file);
                            }}
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
                            onClick={analyzeSkus}
                            disabled={
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
                            onClick={() => {
                                const file = `AllSKUs-${getAbbr(month1)}'${String(year1).slice(
                                    2
                                )}vs${getAbbr(month2)}'${String(year2).slice(2)}.xlsx`;
                                exportToExcel(getAllSkusForExport(), file);
                            }}
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
    );
};

export default SkuAnalysisSection;