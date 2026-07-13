"use client";

import React, { useEffect, useMemo, useState } from "react";
import { IoMdLock } from "react-icons/io";

export function PreviewLockedSection({
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
}) {
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
}

export const DashboardLoaderModal = React.memo(function DashboardLoaderModal({
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
