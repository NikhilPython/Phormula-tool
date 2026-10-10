"use client";

import React from "react";

export type ActionDiagnosisMetric = {
    label: string;
    value: React.ReactNode;
    helper?: string;
};

export type ActionDiagnosisRiskItem = {
    name: string;
    detail: string;
    secondary?: string;
};

type ActionDiagnosisPanelProps = {
    title: string;
    description: string;
    metrics: ActionDiagnosisMetric[];
    whyItMatters: string;
    recommendedAction: string;
    triggerRule: string;
    riskTitle?: string;
    riskItems?: ActionDiagnosisRiskItem[];
    evidenceNote?: string;
};

const metricAccentClasses = [
    "border-[#E7B54A]", // amber
    "border-[#C96B6B]", // red
    "border-[#6BAED6]", // blue
    "border-[#C98952]", // orange
    "border-[#4F9C88]", // green
    "border-[#D08A43]", // warm orange
];

export default function ActionDiagnosisPanel({
    title,
    description,
    metrics,
    whyItMatters,
    recommendedAction,
    triggerRule,
    riskTitle = "Highest-risk products",
    riskItems = [],
    evidenceNote = "Calculated from the affected rows shown in this focused view.",
}: ActionDiagnosisPanelProps) {
    return (
        <section className="mb-4 overflow-hidden rounded-2xl border border-[#D9E8E3] bg-gradient-to-br from-[#F8FCFA] via-white to-[#F4FAF7] shadow-[0_8px_28px_rgba(31,92,76,0.08)]">
            <div className="flex flex-wrap items-start justify-between gap-3 border-b border-[#E4EFEB] px-4 py-3.5 sm:px-5">
                <div className="flex min-w-0 items-start gap-3">
                    <div className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-[#CFE5DD] bg-[#EAF6F1] text-[#2F806B] shadow-sm">
                        <svg
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="1.8"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            className="h-[18px] w-[18px]"
                            aria-hidden="true"
                        >
                            <path d="M4 19V9" />
                            <path d="M10 19V5" />
                            <path d="M16 19v-7" />
                            <path d="M22 19V3" />
                        </svg>
                    </div>

                    <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                            <h3 className="text-sm font-semibold text-[#233C36] sm:text-[15px]">
                                {title}
                            </h3>
                            <span className="inline-flex items-center rounded-full border border-[#F2D7A5] bg-[#FFF8E8] px-2 py-0.5 text-[10px] font-semibold text-[#B77A10]">
                                Focused diagnosis
                            </span>
                        </div>
                        <p className="mt-1 max-w-[900px] text-[11px] leading-5 text-[#60756F] sm:text-xs">
                            {description}
                        </p>
                    </div>
                </div>

                <span className="inline-flex shrink-0 items-center gap-1.5 rounded-full border border-[#D7E8E2] bg-white/85 px-2.5 py-1 text-[10px] font-medium text-[#628078]">
                    <span className="h-1.5 w-1.5 rounded-full bg-[#5EA68E]" />
                    Evidence-based view
                </span>
            </div>

            {metrics.length > 0 && (
                <div className="grid grid-cols-2 gap-3 px-3 py-3 sm:grid-cols-3 sm:px-4 xl:grid-cols-6">
                    {metrics.slice(0, 6).map((metric, index) => (
                        <div
                            key={metric.label}
                            className={`
    flex min-w-0 flex-col justify-between
    rounded-2xl
    border
    border-t-[3px]
    bg-white
    p-3
    shadow-[0_2px_5px_rgba(0,0,0,0.08)]
    transition-all duration-200
    ${metricAccentClasses[index % metricAccentClasses.length]}
`}
                        >
                            <p
                                className="truncate text-[10px] font-medium leading-tight text-charcoal-500 2xl:text-xs"
                                title={metric.label}
                            >
                                {metric.label}
                            </p>

                            <div
                                className="mt-1 min-w-0 truncate text-sm font-semibold leading-tight tabular-nums text-charcoal-500 2xl:text-lg"
                                title={typeof metric.value === "string" ? metric.value : undefined}
                            >
                                {metric.value}
                            </div>

                            <div className="mt-2 min-h-[24px]">
                                {metric.helper ? (
                                    <p
                                        className="truncate text-[9.5px] leading-tight text-charcoal-400 sm:text-[10px] 2xl:text-xs"
                                        title={metric.helper}
                                    >
                                        {metric.helper}
                                    </p>
                                ) : (
                                    <span className="block h-[12px]" />
                                )}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            <div className="grid gap-3 border-t border-[#E4EFEB] p-3 sm:p-4 lg:grid-cols-3">
                <div className="rounded-xl border border-[#E2ECE8] bg-white/80 p-3.5">
                    <p className="text-[10px] font-semibold uppercase tracking-[0.08em] text-[#779087]">
                        Why this matters
                    </p>
                    <p className="mt-2 text-[11px] leading-5 text-[#4F6861] sm:text-xs">
                        {whyItMatters}
                    </p>
                </div>

                <div className="rounded-xl border border-[#D7E9E2] bg-[#F3FAF7] p-3.5">
                    <p className="text-[10px] font-semibold uppercase tracking-[0.08em] text-[#4F8C7C]">
                        Recommended next step
                    </p>
                    <p className="mt-2 text-[11px] leading-5 text-[#365E54] sm:text-xs">
                        {recommendedAction}
                    </p>
                </div>

                <div className="rounded-xl border border-[#E2ECE8] bg-white/80 p-3.5">
                    <p className="text-[10px] font-semibold uppercase tracking-[0.08em] text-[#779087]">
                        {riskItems.length ? riskTitle : "Trigger rule"}
                    </p>

                    {riskItems.length ? (
                        <div className="mt-2 space-y-2">
                            {riskItems.slice(0, 3).map((item, index) => (
                                <div key={`${item.name}-${index}`} className="flex items-start justify-between gap-3 rounded-lg bg-[#F8FBFA] px-2.5 py-2">
                                    <div className="min-w-0">
                                        <p className="truncate text-[11px] font-semibold text-[#34554C]" title={item.name}>
                                            {item.name}
                                        </p>
                                        {item.secondary && (
                                            <p className="mt-0.5 truncate text-[10px] text-[#82968F]" title={item.secondary}>
                                                {item.secondary}
                                            </p>
                                        )}
                                    </div>
                                    <span className="shrink-0 text-[11px] font-semibold tabular-nums text-[#4E7C70]">
                                        {item.detail}
                                    </span>
                                </div>
                            ))}
                        </div>
                    ) : (
                        <p className="mt-2 text-[11px] leading-5 text-[#4F6861] sm:text-xs">
                            {triggerRule}
                        </p>
                    )}
                </div>
            </div>


        </section>
    );
}
