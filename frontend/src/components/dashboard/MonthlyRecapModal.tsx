"use client";

import React, { useEffect, useRef } from "react";
import {
    AlertCircle,
    CheckCircle2,
    ExternalLink,
    Plus,
    X,
} from "lucide-react";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import PageBreadcrumb from "../common/PageBreadCrumb";
import { createPortal } from "react-dom";

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
        netSalesDeltaPct: number | null;
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
    country: string;
};

/* ----------------------------- Helpers ----------------------------- */

// Updated sequence as requested
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

/* ----------------------------- Component ----------------------------- */

export default function MonthlyRecapModal({
    open,
    data,
    onClose,
    onViewBreakdown,
    onSetTarget,
    country,
}: Props) {
    const primaryBtnRef = useRef<HTMLButtonElement | null>(null);

    useEffect(() => {
        if (!open) return;
        primaryBtnRef.current?.focus();
        const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [open, onClose]);

    if (!open || !data) return null;

    const { monthLabel, target, actual, skuHighlights, objectives, formatMoney, metrics } = data;
    const hasTarget = target > 0;
    const pct = hasTarget ? (actual / target) * 100 : null;
    const met = pct !== null && pct >= 100;
    const gap = Math.abs(actual - target);

    const orderedMetrics = [...metrics].sort((a, b) => metricRank(a.label) - metricRank(b.label));

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
        // If the parent passed a custom onViewBreakdown function, use that instead
        if (onViewBreakdown) {
            onViewBreakdown();
            return;
        }

        const { month, year } = getPnlDateParams();

        const url = `/pnl-dashboard/MTD/${country.toLowerCase()}/${month}/${year}`;

        // Open in a new tab
        window.open(url, "_blank", "noopener,noreferrer");
    };

    return createPortal(
        <div
            className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/40 px-4 py-6" // Increased z-index to be safe
            onClick={onClose}
        >
            <div
                role="dialog"
                aria-modal="true"
                aria-labelledby="monthly-recap-title"
                onClick={(e) => e.stopPropagation()}
                className="relative max-h-full w-full max-w-xl overflow-y-auto rounded-2xl bg-white p-6 shadow-2xl sm:p-8"
            >
                <button
                    type="button"
                    onClick={onClose}
                    aria-label="Close"
                    className="absolute right-4 top-4 rounded-full p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-600 z-10"
                >
                    <X className="h-5 w-5" />
                </button>

                {/* Header */}
                <div id="monthly-recap-title" className="pr-10">
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
                        textSize="2xl"
                        className="!text-[#37455F]"
                    />
                </div>
                <p className="mt-2 text-sm leading-5 text-slate-600">
                    A snapshot of your sales, profitability, and overall performance for the month.
                </p>

                {/* Objectives as chips */}
                {objectives.length > 0 && (
                    <div className="mt-3 flex flex-wrap justify-center gap-2">
                        {objectives.map((o, i) => (
                            <span
                                key={i}
                                className="inline-flex items-center gap-1.5 rounded-full bg-slate-100 px-2.5 py-1 text-xs text-[#37455F]"
                            >
                                <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-[#5EA68E]" />
                                {o}
                            </span>
                        ))}
                    </div>
                )}

                {/* Sales vs target */}
                <section className="mt-5 rounded-xl border border-[#E6ECEF] px-5 py-3">
                    {/* Centered Status Heading */}
                    {hasTarget && (
                        <div className="mb-2 flex w-full items-center justify-center gap-1.5">
                            <PageBreadcrumb
                                pageTitle={met ? "Target Achieved" : "Target Not Achieved"}
                                align="center2"
                                variant="page"
                                textSize="xl"
                                className={`font-bold ${met ? "!text-[#5EA68E]" : "!text-[#B75A5A]"}`}
                            />
                        </div>
                    )}

                    <div className="flex items-end justify-between gap-2">
                        <div className="flex-1">
                            <p className="text-xs text-[#50627A] mb-1">Net sales</p>
                            <p className="text-2xl font-bold leading-tight text-[#37455F]">{formatMoney(actual)}</p>
                        </div>

                        <div className="flex shrink-0 items-center justify-center pb-1.5">
                            <span className="text-lg font-bold uppercase tracking-wider text-slate-400">vs</span>
                        </div>

                        <div className="flex-1 text-right">
                            <p className="text-xs text-[#50627A] mb-1">Target</p>
                            {hasTarget ? (
                                <p className="text-2xl font-bold leading-tight text-[#37455F]">{formatMoney(target)}</p>
                            ) : (
                                <>
                                    <p className="text-2xl font-semibold leading-tight text-slate-400">Not set</p>
                                    {onSetTarget && (
                                        <button
                                            type="button"
                                            onClick={onSetTarget}
                                            className="mt-1 inline-flex items-center gap-1 text-xs font-semibold text-[#5EA68E] hover:underline"
                                        >
                                            <Plus className="h-3.5 w-3.5" />
                                            Set a target
                                        </button>
                                    )}
                                </>
                            )}
                        </div>
                    </div>

                    {pct !== null ? (
                        <div className="mt-4">
                            <div className="h-1.5 w-full overflow-hidden rounded-full bg-gray-200">
                                <div
                                    className={`h-full rounded-full ${met ? "bg-[#5EA68E]" : "bg-[#ED9F50]"}`}
                                    style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
                                />
                            </div>
                            <div className="mt-2 flex items-center justify-between gap-3 text-xs">
                                <span className="font-semibold text-[#37455F]">{pct.toFixed(1)}% achieved</span>
                                <span className={met ? "font-semibold text-[#5EA68E]" : "font-semibold text-[#B75A5A]"}>
                                    {met ? `Target beat by ${formatMoney(gap)}` : `${formatMoney(gap)} short of target`}
                                </span>
                            </div>
                        </div>
                    ) : (
                        <p className="mt-3 text-xs text-[#50627A]">
                            No target was set for {monthLabel}, so there&rsquo;s nothing to compare against.
                        </p>
                    )}
                </section>

                {/* Key metrics */}
                <section className="mt-4" aria-labelledby="monthly-recap-metrics">
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
                <section className="mt-4 space-y-2" aria-label="SKU sales highlights">
                    {skuHighlights.length > 0 ? skuHighlights.map((highlight) => {
                        const delta = highlight.netSalesDeltaPct;
                        return (
                            <article key={highlight.title} className="rounded-xl border border-t-4 border-[#7B9A6D] px-4 py-3">
                                <p className="mb-2 text-xs text-charcoal-500">{highlight.title}</p>
                                <div className="grid grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)_auto] items-center gap-3">
                                    <div className="min-w-0">
                                        <p className="truncate font-bold text-charcoal-500" title={highlight.name}>{highlight.name}</p>
                                        {highlight.sku && <p className="mt-0.5 truncate text-xs text-charcoal-500">{highlight.sku}</p>}
                                    </div>
                                    <div className="min-w-0">
                                        <p className="text-xs text-charcoal-500">Net sales</p>
                                        <p className="mt-0.5 truncate text-base font-bold text-charcoal-500">{formatMoney(highlight.netSales)}</p>
                                    </div>
                                    <p className={`whitespace-nowrap text-xs font-semibold ${delta == null ? "text-slate-400" : delta >= 0 ? "text-emerald-600" : "text-red-600"}`}>
                                        {delta == null ? "\u2014" : `${delta >= 0 ? "\u25B2" : "\u25BC"} ${Math.abs(delta).toFixed(2)}%`}
                                    </p>
                                </div>
                            </article>
                        );
                    }) : (
                        <div className="rounded-xl border border-t-4 border-[#7B9A6D] p-4">
                            <p className="text-sm text-charcoal-500">No SKU sales recorded for this month.</p>
                        </div>
                    )}
                </section>

                {/* Footer */}
                {/* Footer */}
                <div className="mt-6 flex items-center justify-between gap-3">
                    {onViewBreakdown || country ? ( // Ensure it shows if either is provided
                        <button
                            type="button"
                            onClick={handleOpenPnl} // <--- Changed to new handler
                            className="inline-flex items-center gap-1.5 text-sm font-semibold text-[#5EA68E] hover:underline"
                        >
                            Open P&amp;L Breakdown
                            <ExternalLink className="h-4 w-4" />
                        </button>
                    ) : (
                        <span />
                    )}
                    <button
                        ref={primaryBtnRef}
                        type="button"
                        onClick={onClose}
                        className="rounded-md bg-[#2E3A59] px-6 py-2.5 text-sm font-semibold text-white transition hover:bg-[#1f2943] focus:outline-none focus-visible:ring-2 focus-visible:ring-[#5EA68E] focus-visible:ring-offset-2"
                    >
                        Got it
                    </button>
                </div>
            </div>
        </div>,
        document.body
    );
}
