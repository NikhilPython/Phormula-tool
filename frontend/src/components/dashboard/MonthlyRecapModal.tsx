"use client";

import React, { useEffect, useRef } from "react";
import {
    ExternalLink,
    Mail,
    Plus,
    X,
} from "lucide-react";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import PageBreadcrumb from "../common/PageBreadCrumb";
import { createPortal } from "react-dom";
import DataTable, { type ColumnDef, type Row } from "@/components/ui/table/DataTable";

const ACCENT_CLASS = {
    units: "border-[#FDD36F]", // Gold/Yellow
    asp: "border-[#ED9F50]", // Orange
    netSales: "border-[#75BBDA]", // Light Blue
    marketplaceFees: "border-[#B75A5A]", // Red
    tacos: "border-[#3A8EA4]", // Teal
    cm2Profit: "border-[#B8C78C]", // Light Green
} as const;

export type MonthlyRecapMetric = {
    label: string;
    value: number | null | undefined;
    formatter?: (value: number) => React.ReactNode;
    loading?: boolean;
    /** Previous month's value. Shows the "Last month" row and change % on the card. Leave empty to hide both. */
    previous?: number | null;
    /** Percentage change supplied by the dashboard card_metrics API response. */
    deltaPct?: number | null;
    /** Set true for costs/rates where a decrease is good (Cost of Ads, TACoS). Passed to AmazonStatCard. */
    inverseDelta?: boolean;
    /** Optional top-border colour override. */
    accent?: keyof typeof ACCENT_CLASS;
};

export type MonthlyRecapData = {
    monthLabel: string;
    target: number;
    actual: number;
    skuHighlights: Array<{
        title: string;
        name: string;
        sku?: string;
        netSales: number;
        netSalesChange: number | null;
        netSalesDeltaPct: number | null;
        units: number;
        cm2Profit: number | null;
        asp?: number | null;
        acos?: number | null;
    }>;
    objectives: string[];
    formatMoney: (val: number) => string;
    /** Values for the completed month, rather than the current MTD values. */
    metrics: MonthlyRecapMetric[];
};

type Props = {
    open: boolean;
    data: MonthlyRecapData | null;
    onClose: () => void;
    onViewBreakdown?: () => void;
    onSetTarget?: () => void;
    onEmailRecap?: () => Promise<void> | void;
    country: string;
};

/* ----------------------------- Helpers ----------------------------- */

const METRIC_ORDER = ["units", "asp", "net sales", "marketplace fees", "tacos", "cm2 profit"];

const metricRank = (label: string) => {
    const i = METRIC_ORDER.indexOf(label.trim().toLowerCase());
    return i === -1 ? METRIC_ORDER.length : i;
};

const ACCENT_BY_LABEL: Record<string, keyof typeof ACCENT_CLASS> = {
    units: "units",
    asp: "asp",
    "net sales": "netSales",
    "marketplace fees": "marketplaceFees",
    tacos: "tacos",
    "cm2 profit": "cm2Profit",
};

const getDeltaPct = (value: number, previous: number | null | undefined): number | null => {
    if (previous == null) return null;
    const prev = Number(previous);
    if (!Number.isFinite(prev) || prev === 0) return null;
    return ((value - prev) / Math.abs(prev)) * 100;
};

/* ----------------------------- Gauge ----------------------------- */

const GAUGE_W = 170;
const GAUGE_H = 88;
const GAUGE_CX = GAUGE_W / 2;
const GAUGE_CY = 84;
const TARGET_R = 74;
const ACTUAL_R = 64;

const gaugePoint = (radius: number, pct: number) => {
    const clamped = Math.min(Math.max(pct, 0), 99.99);
    const angle = (180 - clamped * 1.8) * (Math.PI / 180);
    return {
        x: GAUGE_CX + radius * Math.cos(angle),
        y: GAUGE_CY - radius * Math.sin(angle),
    };
};

const gaugeArc = (radius: number, pct: number) => {
    const end = gaugePoint(radius, pct);
    return `M ${GAUGE_CX - radius} ${GAUGE_CY} A ${radius} ${radius} 0 0 1 ${end.x} ${end.y}`;
};

function TargetGauge({
    actual,
    target,
    pct,
    formatMoney,
}: {
    actual: number;
    target: number;
    pct: number;
    formatMoney: (val: number) => string;
}) {
    const max = Math.max(actual, target, 1);
    const targetPct = (target / max) * 100;
    const actualPct = (actual / max) * 100;
    const targetEnd = gaugePoint(TARGET_R, targetPct);
    const actualEnd = gaugePoint(ACTUAL_R, actualPct);

    return (
        <div className="flex flex-col items-center">
            <svg
                width={GAUGE_W}
                height={GAUGE_H}
                viewBox={`0 0 ${GAUGE_W} ${GAUGE_H}`}
                role="img"
                aria-label={`${pct.toFixed(1)}% of target achieved`}
            >
                <title>{`Net sales ${formatMoney(actual)} · Target ${formatMoney(target)}`}</title>
                <path
                    d={gaugeArc(TARGET_R, targetPct)}
                    fill="none"
                    stroke="#5EA68E"
                    strokeWidth="5"
                    strokeLinecap="round"
                />
                <path
                    d={gaugeArc(ACTUAL_R, actualPct)}
                    fill="none"
                    stroke="#ED9F50"
                    strokeWidth="8"
                    strokeLinecap="round"
                />
                <circle cx={targetEnd.x} cy={targetEnd.y} r="4.5" fill="#5EA68E" stroke="white" strokeWidth="2" />
                <circle cx={actualEnd.x} cy={actualEnd.y} r="4.5" fill="#ED9F50" stroke="white" strokeWidth="2" />
            </svg>
            <div className="-mt-7 text-center leading-none">
                <div className="text-2xl font-semibold text-[#37455F]">{pct.toFixed(1)}%</div>
                <div className="mt-1 text-xs font-medium text-[#50627A]">Target Achieved</div>
            </div>
        </div>
    );
}

/* ----------------------------- Component ----------------------------- */

export default function MonthlyRecapModal({
    open,
    data,
    onClose,
    onViewBreakdown,
    onSetTarget,
    onEmailRecap,
    country,
}: Props) {
    const primaryBtnRef = useRef<HTMLButtonElement | null>(null);
    const [emailing, setEmailing] = React.useState(false);

    useEffect(() => {
        if (!open) return;
        primaryBtnRef.current?.focus();
        const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [open, onClose]);

    if (!open || !data) return null;

    const { monthLabel, target, actual, skuHighlights, objectives, formatMoney, metrics } = data;
    const normalizedCountry = country.toLowerCase();
    const skuAspCurrency = normalizedCountry.includes("uk") || normalizedCountry.includes("gb")
        ? "GBP"
        : "USD";
    const formatSkuAsp = (value: number) => new Intl.NumberFormat(
        skuAspCurrency === "GBP" ? "en-GB" : "en-US",
        { style: "currency", currency: skuAspCurrency, minimumFractionDigits: 2, maximumFractionDigits: 2 }
    ).format(value);
    const hasTarget = target > 0;
    const pct = hasTarget ? (actual / target) * 100 : null;
    const met = pct !== null && pct >= 100;
    const gap = Math.abs(actual - target);

    const orderedMetrics = [...metrics].sort((a, b) => metricRank(a.label) - metricRank(b.label));

    const topSellingHighlight = skuHighlights.find((highlight) => highlight.title === "Top Selling SKU");
    const highestGrowthHighlight = skuHighlights.find((highlight) => highlight.title === "Highest Net Sales Growth");
    const highlightIdentity = (highlight: NonNullable<typeof topSellingHighlight>) =>
        highlight.sku?.trim().toLowerCase() || highlight.name.trim().toLowerCase();
    const topSellingMatchesHighestGrowth = !!topSellingHighlight && !!highestGrowthHighlight &&
        highlightIdentity(topSellingHighlight) === highlightIdentity(highestGrowthHighlight);
    type SkuHighlightRow = Row & { category: string; mergeSkuCells: boolean; hideMergedSkuCells: boolean; productName: React.ReactNode; units: string; asp: string; netSales: string; acos: string; cm2Profit: string };
    const skuTableRows: SkuHighlightRow[] = skuHighlights.map((highlight) => ({
        category: highlight.title === "Largest Net Sales Decline" ? "Lowest Net Sales Growth" : highlight.title,
        mergeSkuCells: topSellingMatchesHighestGrowth && highlight.title === "Top Selling SKU",
        hideMergedSkuCells: topSellingMatchesHighestGrowth && highlight.title === "Highest Net Sales Growth",
        productName: <div className="min-w-0 text-[11px] leading-tight"><div className="truncate font-semibold" title={highlight.name}>{highlight.name}</div></div>,
        units: Math.round(highlight.units || 0).toLocaleString(),
        asp: highlight.asp != null ? formatSkuAsp(highlight.asp) : "—",
        netSales: formatMoney(highlight.netSales),
        acos: highlight.acos != null ? `${highlight.acos.toFixed(2)}%` : "—",
        cm2Profit: highlight.cm2Profit != null ? formatMoney(highlight.cm2Profit) : "—",
    }));
    const skuTableColumns: ColumnDef<SkuHighlightRow>[] = [
        {
            key: "category",
            header: "Category",
            width: "162px",
            render: (row) => <div className="whitespace-normal">{row.category}</div>,
            cellClassName: "!px-1 !py-1 text-[11px] font-semibold",
            headerClassName: "!px-1 !py-1 !text-[11px]",
        },
        { key: "productName", header: "Product Name", width: "110px", rowSpan: (row) => row.mergeSkuCells ? 2 : 1, hideCell: (row) => row.hideMergedSkuCells, cellClassName: "!px-1 !py-1 text-[11px]", headerClassName: "!px-1 !py-1 !text-[11px]" },
        { key: "units", header: "Units", width: "48px", rowSpan: (row) => row.mergeSkuCells ? 2 : 1, hideCell: (row) => row.hideMergedSkuCells, cellClassName: "!px-1 !py-1 text-[11px]", headerClassName: "!px-1 !py-1 !text-[11px]" },
        { key: "asp", header: "ASP", width: "58px", rowSpan: (row) => row.mergeSkuCells ? 2 : 1, hideCell: (row) => row.hideMergedSkuCells, cellClassName: "!px-1 !py-1 text-[11px]", headerClassName: "!px-1 !py-1 !text-[11px]" },
        { key: "netSales", header: "Net Sales", width: "72px", rowSpan: (row) => row.mergeSkuCells ? 2 : 1, hideCell: (row) => row.hideMergedSkuCells, cellClassName: "!px-1 !py-1 text-[11px]", headerClassName: "!px-1 !py-1 !text-[11px]" },
        { key: "acos", header: "ACoS", width: "52px", rowSpan: (row) => row.mergeSkuCells ? 2 : 1, hideCell: (row) => row.hideMergedSkuCells, cellClassName: "!px-1 !py-1 text-[11px]", headerClassName: "!px-1 !py-1 !text-[11px]" },
        { key: "cm2Profit", header: "CM2 Profit", width: "80px", rowSpan: (row) => row.mergeSkuCells ? 2 : 1, hideCell: (row) => row.hideMergedSkuCells, cellClassName: "!px-1 !py-1 text-[11px]", headerClassName: "!px-1 !py-1 !text-[11px]" },
    ];

    const handleEmailRecap = async () => {
        if (!onEmailRecap || emailing) return;
        setEmailing(true);
        try {
            await onEmailRecap();
        } finally {
            setEmailing(false);
        }
    };

    const formatRecapMonth = (label: string) => {
        if (!label) return "";
        const parts = label.trim().split(" ");
        if (parts.length >= 2) {
            const month = parts[0].substring(0, 3); // "Sep"
            const year = parts[parts.length - 1].substring(2); // "26" from "2026"
            return `${month}'${year}`;
        }
        return label;
    };

    const getPnlDateParams = () => {
        const monthMap: Record<string, string> = {
            jan: "January", feb: "February", mar: "March", apr: "April", may: "May", jun: "June",
            jul: "July", aug: "August", sep: "September", oct: "October", nov: "November", dec: "December"
        };

        let month = new Date().toLocaleString('default', { month: 'long' });
        let year = new Date().getFullYear();

        if (monthLabel) {
            const parts = monthLabel.trim().split(" ");
            if (parts.length >= 2) {
                const shortMonth = parts[0].substring(0, 3).toLowerCase();
                month = monthMap[shortMonth] || month;

                const parsedYear = parseInt(parts[parts.length - 1], 10);
                if (!isNaN(parsedYear)) {
                    year = parsedYear;
                }
            }
        }
        return { month, year };
    };

    const handleOpenPnl = () => {
        if (onViewBreakdown) {
            onViewBreakdown();
            return;
        }

        const { month, year } = getPnlDateParams();
        const url = `/pnl-dashboard/MTD/${country.toLowerCase()}/${month}/${year}`;
        window.open(url, "_blank", "noopener,noreferrer");
    };

    return createPortal(
        <div
            className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/40 px-4 py-6"
            onClick={onClose}
        >
            <div
                role="dialog"
                aria-modal="true"
                aria-labelledby="monthly-recap-title"
                onClick={(e) => e.stopPropagation()}
                className="relative max-h-full w-full max-w-2xl overflow-y-auto rounded-2xl bg-white p-5 shadow-2xl sm:p-6"
            >
                <button
                    type="button"
                    onClick={onClose}
                    aria-label="Close"
                    className="absolute right-3 top-3 z-10 rounded-full p-1.5 text-slate-400 transition hover:bg-slate-100 hover:text-slate-600"
                >
                    <X className="h-4 w-4" />
                </button>

                {/* Header */}
                <div id="monthly-recap-title" className="pr-8">
                    <PageBreadcrumb
                        pageTitle={
                            <>
                                Here&rsquo;s how your{" "}
                                <span className="text-[#5EA68E]">{formatRecapMonth(monthLabel)}</span>{" "}
                                went
                            </>
                        }
                        align="left"
                        variant="page"
                        textSize="xl"
                        className="!text-[#37455F]"
                    />
                </div>
                <p className="mt-1.5 text-xs leading-4 text-slate-600">
                    A snapshot of your sales, profitability, and overall performance for the month.
                </p>

                {/* Sales vs target */}
                <section className="mt-3 rounded-xl border border-[#E6ECEF] px-4 py-2.5">
                    <h3 className="mb-1 text-lg text-center font-semibold text-[#37455F]">
                        Target and Objectives
                    </h3>

                    <div className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2">
                        {/* Net sales */}
                        <div className="flex min-w-0 flex-col items-center justify-center text-center">
                            <p className="mb-0.5 flex items-center justify-center gap-1 text-sm text-[#50627A]">
                                {hasTarget && <span className="h-1.5 w-1.5 rounded-full bg-[#ED9F50]" />}
                                Net sales
                            </p>
                            <p className="max-w-full truncate text-2xl font-bold leading-tight text-[#37455F]">
                                {formatMoney(actual)}
                            </p>
                        </div>

                        {/* Gauge (replaces "vs") */}
                        <div className="flex shrink-0 items-center justify-center">
                            {hasTarget && pct !== null ? (
                                <TargetGauge
                                    actual={actual}
                                    target={target}
                                    pct={pct}
                                    formatMoney={formatMoney}
                                />
                            ) : (
                                <span className="text-base font-bold uppercase tracking-wider text-slate-400">
                                    vs
                                </span>
                            )}
                        </div>

                        {/* Target */}
                        <div className="flex min-w-0 flex-col items-center justify-center text-center">
                            <p className="mb-0.5 flex items-center justify-center gap-1 text-sm text-[#50627A]">
                                {hasTarget && <span className="h-1.5 w-1.5 rounded-full bg-[#5EA68E]" />}
                                Target
                            </p>
                            {hasTarget ? (
                                <p className="max-w-full truncate text-2xl font-bold leading-tight text-[#37455F]">
                                    {formatMoney(target)}
                                </p>
                            ) : (
                                <>
                                    <p className="text-2xl font-semibold leading-tight text-slate-400">Not set</p>
                                    {onSetTarget && (
                                        <button
                                            type="button"
                                            onClick={onSetTarget}
                                            className="mt-1 inline-flex items-center gap-1 text-[11px] font-semibold text-[#5EA68E] hover:underline"
                                        >
                                            <Plus className="h-3 w-3" />
                                            Set a target
                                        </button>
                                    )}
                                </>
                            )}
                        </div>
                    </div>

                    {objectives.length > 0 && (
                        <div className="mt-3 flex flex-wrap justify-center gap-1.5">
                            {objectives.map((o, i) => (
                                <span
                                    key={i}
                                    className="inline-flex items-center capitalize gap-1 rounded-full bg-slate-100 px-2 py-0.5 text-[11px] text-[#37455F]"
                                >
                                    <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-[#5EA68E]" />
                                    {o}
                                </span>
                            ))}
                        </div>
                    )}
                </section>

                {/* Key metrics */}
                <section className="mt-3" aria-labelledby="monthly-recap-metrics">
                    <h3 id="monthly-recap-metrics" className="sr-only">
                        Key metrics for {monthLabel}
                    </h3>
                    <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
                        {orderedMetrics.map((metric) => {
                            const value = Number(metric.value);
                            const hasValue = Number.isFinite(value);
                            const accentKey =
                                metric.accent ?? ACCENT_BY_LABEL[metric.label.trim().toLowerCase()] ?? "netSales";
                            const hasPrevious = hasValue && metric.previous != null;

                            return (
                                <AmazonStatCard
                                    key={metric.label}
                                    label={metric.label}
                                    current={hasValue ? value : null}
                                    previous={metric.previous ?? null}
                                    deltaPct={
                                        hasValue
                                            ? metric.deltaPct ?? getDeltaPct(value, metric.previous)
                                            : null
                                    }
                                    inverseDelta={metric.inverseDelta}
                                    bottomLabel="Last month"
                                    hidePrevious={!hasPrevious && metric.deltaPct == null}
                                    hidePreviousValue={!hasPrevious}
                                    loading={!!metric.loading}
                                    formatter={
                                        hasValue
                                            ? (v) => metric.formatter?.(v) ?? Number(v).toLocaleString()
                                            : () => "\u2014"
                                    }
                                    className={`border-t-4 ${ACCENT_CLASS[accentKey]}`}
                                />
                            );
                        })}
                    </div>
                </section>

                {/* SKU sales highlights */}
                <section className="mt-3 space-y-2" aria-label="SKU sales highlights">
                    {skuHighlights.length > 0 ? (
                        <DataTable<SkuHighlightRow>
                            columns={skuTableColumns}
                            data={skuTableRows}
                            paginate={false}
                            scrollY={false}
                            stickyHeader={false}
                            zebra={false}
                            headerMaxWidth={60}
                            rowClassName={() => "!h-9"}
                            className="overflow-x-hidden border-[#E6ECEF] shadow-none"
                            tableClassName="!w-full !min-w-0 text-[11px]"
                        />
                    ) : (
                        <div className="rounded-xl border border-t-4 border-[#7B9A6D] p-3">
                            <p className="text-xs text-charcoal-500">
                                No SKU sales recorded for this month.
                            </p>
                        </div>
                    )}
                </section>

                {/* Footer */}
                <div className="mt-5 flex items-center justify-between gap-3">
                    {onViewBreakdown || country ? (
                        <button
                            type="button"
                            onClick={handleOpenPnl}
                            className="inline-flex items-center gap-1.5 text-xs font-semibold text-[#5EA68E] hover:underline"
                        >
                            Open P&amp;L Breakdown
                            <ExternalLink className="h-3.5 w-3.5" />
                        </button>
                    ) : (
                        <span />
                    )}
                    <div className="flex items-center gap-2">
                        {onEmailRecap && (
                            <button
                                type="button"
                                onClick={handleEmailRecap}
                                disabled={emailing}
                                className="inline-flex items-center gap-1.5 rounded-md border border-[#5EA68E] px-3 py-2 text-xs font-semibold text-[#438A73] transition hover:bg-[#F1F8F5] disabled:cursor-wait disabled:opacity-60"
                            >
                                <Mail className="h-3.5 w-3.5" />
                                {emailing ? "Sending…" : "Email recap"}
                            </button>
                        )}
                        <button
                            ref={primaryBtnRef}
                            type="button"
                            onClick={onClose}
                            className="rounded-md bg-[#2E3A59] px-5 py-2 text-xs font-semibold text-white transition hover:bg-[#1f2943] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#5EA68E] focus-visible:ring-offset-2"
                        >
                            Got it
                        </button>
                    </div>
                </div>
            </div>
        </div>,
        document.body
    );
}
