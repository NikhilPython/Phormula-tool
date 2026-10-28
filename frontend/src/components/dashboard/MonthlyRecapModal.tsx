"use client";

import React, { useEffect, useRef } from "react";
import { Flag, Target, Trophy, X } from "lucide-react";

export type MonthlyRecapMetric = {
    label: string;
    value: number | null | undefined;
    formatter?: (value: number) => React.ReactNode;
    loading?: boolean;
};

export type MonthlyRecapData = {
    monthLabel: string;
    target: number;
    actual: number;
    topSku: {
        name: string;
        sku?: string;
        units: number;
        netSales: number;
        sharePct?: number;
    } | null;
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
};

export default function MonthlyRecapModal({ open, data, onClose, onViewBreakdown }: Props) {
    const primaryBtnRef = useRef<HTMLButtonElement | null>(null);

    useEffect(() => {
        if (!open) return;
        primaryBtnRef.current?.focus();
        const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [open, onClose]);

    if (!open || !data) return null;

    const { monthLabel, target, actual, topSku, objectives, formatMoney, metrics } = data;
    const hasTarget = target > 0;
    const pct = hasTarget ? (actual / target) * 100 : null;
    const met = pct !== null && pct >= 100;
    const gap = Math.abs(actual - target);

    return (
        <div
            className="fixed inset-0 z-[80] flex items-center justify-center bg-[#0F172A]/45 px-4 py-6"
            onClick={onClose}
        >
            <div
                role="dialog"
                aria-modal="true"
                aria-labelledby="monthly-recap-title"
                onClick={(e) => e.stopPropagation()}
                className="relative max-h-full w-full max-w-[560px] overflow-y-auto rounded-xl border border-[#E6ECEF] bg-white shadow-[0_20px_50px_rgba(15,23,42,0.25)]"
            >
                {/* Header */}
                <div className="flex items-start justify-between gap-4 border-b border-[#E6ECEF] px-6 pb-4 pt-5">
                    <div>
                        <p className="text-xs font-semibold uppercase tracking-wide text-[#5EA68E]">
                            Monthly recap
                        </p>
                        <h2 id="monthly-recap-title" className="mt-1 text-xl font-bold text-[#37455F]">
                            Here&rsquo;s how {monthLabel} went
                        </h2>
                    </div>
                    <button
                        type="button"
                        onClick={onClose}
                        aria-label="Close"
                        className="rounded-md p-1 text-[#50627A] transition hover:bg-gray-100"
                    >
                        <X className="h-5 w-5" />
                    </button>
                </div>

                <div className="space-y-4 px-6 py-5">
                    {/* Target vs Actual */}
                    <section className="rounded-lg border border-[#E6ECEF] p-4">
                        <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-[#37455F]">
                            <Target className="h-4 w-4 text-[#5EA68E]" />
                            Sales target vs actual net sales
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <p className="text-xs text-[#50627A]">Target</p>
                                <p className="text-lg font-bold text-[#37455F]">
                                    {hasTarget ? formatMoney(target) : "Not set"}
                                </p>
                            </div>
                            <div>
                                <p className="text-xs text-[#50627A]">Actual net sales</p>
                                <p className="text-lg font-bold text-[#37455F]">{formatMoney(actual)}</p>
                            </div>
                        </div>

                        {pct !== null && (
                            <div className="mt-4">
                                <div className="h-2 w-full overflow-hidden rounded-full bg-gray-100">
                                    <div
                                        className={`h-full rounded-full ${met ? "bg-[#5EA68E]" : "bg-[#ED9F50]"}`}
                                        style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
                                    />
                                </div>
                                <div className="mt-2 flex items-center justify-between text-xs">
                                    <span className="font-semibold text-[#37455F]">{pct.toFixed(1)}% achieved</span>
                                    <span className={met ? "font-semibold text-[#5EA68E]" : "font-semibold text-[#B75A5A]"}>
                                        {met ? `Target beat by ${formatMoney(gap)}` : `${formatMoney(gap)} short of target`}
                                    </span>
                                </div>
                            </div>
                        )}
                    </section>

                    {/* Key metrics */}
                    <section>
                        <div className="mb-3 flex items-center justify-between gap-3">
                            <h3 className="text-sm font-semibold text-[#37455F]">Your key metrics</h3>
                            <span className="text-xs text-[#50627A]">{monthLabel}</span>
                        </div>
                        <div className="grid grid-cols-2 gap-3 sm:grid-cols-3">
                            {metrics.map((metric) => {
                                const value = Number(metric.value);
                                const hasValue = Number.isFinite(value);

                                return (
                                    <div
                                        key={metric.label}
                                        className="min-w-0 rounded-lg border border-[#E6ECEF] border-t-4 border-t-[#75BBDA] bg-[#FAFCFD] p-3"
                                    >
                                        <p className="truncate text-xs font-medium text-[#50627A]" title={metric.label}>
                                            {metric.label}
                                        </p>
                                        <p className="mt-1 truncate text-base font-bold text-[#37455F]" title={hasValue ? String(metric.formatter?.(value) ?? value) : undefined}>
                                            {metric.loading ? "Loading…" : hasValue ? metric.formatter?.(value) ?? value.toLocaleString() : "—"}
                                        </p>
                                    </div>
                                );
                            })}
                        </div>
                    </section>

                    {/* Hero SKU */}
                    <section className="rounded-lg border border-[#E6ECEF] p-4">
                        <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-[#37455F]">
                            <Trophy className="h-4 w-4 text-[#ED9F50]" />
                            Hero SKU
                        </div>
                        {topSku ? (
                            <div className="flex items-end justify-between gap-4">
                                <div className="min-w-0">
                                    <p className="truncate text-base font-bold text-[#37455F]" title={topSku.name}>
                                        {topSku.name}
                                    </p>
                                    {topSku.sku && <p className="text-xs text-[#50627A]">{topSku.sku}</p>}
                                </div>
                                <div className="shrink-0 text-right">
                                    <p className="text-base font-bold text-[#37455F]">{formatMoney(topSku.netSales)}</p>
                                    <p className="text-xs text-[#50627A]">
                                        {Math.round(topSku.units).toLocaleString()} units
                                        {topSku.sharePct != null && ` · ${topSku.sharePct.toFixed(0)}% of sales`}
                                    </p>
                                </div>
                            </div>
                        ) : (
                            <p className="text-sm text-[#50627A]">No SKU sales recorded for this month.</p>
                        )}
                    </section>

                    {/* Objectives */}
                    <section className="rounded-lg border border-[#E6ECEF] p-4">
                        <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-[#37455F]">
                            <Flag className="h-4 w-4 text-[#3A8EA4]" />
                            Your objectives
                        </div>
                        {objectives.length > 0 ? (
                            <ul className="space-y-2">
                                {objectives.map((o, i) => (
                                    <li key={i} className="flex gap-2 text-sm leading-5 text-[#50627A]">
                                        <span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-[#5EA68E]" />
                                        <span>{o}</span>
                                    </li>
                                ))}
                            </ul>
                        ) : (
                            <p className="text-sm text-[#50627A]">No objectives were set for this month.</p>
                        )}
                    </section>
                </div>

                {/* Footer */}
                <div className="flex items-center justify-end gap-3 border-t border-[#E6ECEF] px-6 py-4">
                    {onViewBreakdown && (
                        <button
                            type="button"
                            onClick={onViewBreakdown}
                            className="rounded-md px-3 py-2 text-sm font-semibold text-[#37455F] transition hover:bg-gray-100"
                        >
                            Open P&amp;L Breakdown
                        </button>
                    )}
                    <button
                        ref={primaryBtnRef}
                        type="button"
                        onClick={onClose}
                        className="rounded-md bg-[#37455F] px-4 py-2 text-sm font-semibold text-white shadow-md transition hover:bg-[#2f3a50] focus:outline-none focus:ring-2 focus:ring-[#5EA68E] focus:ring-offset-2"
                    >
                        Got it
                    </button>
                </div>
            </div>
        </div>
    );
}
