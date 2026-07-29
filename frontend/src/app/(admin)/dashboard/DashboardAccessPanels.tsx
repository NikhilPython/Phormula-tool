"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
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

type DashboardEtaStep = {
    num: number;
    label: string;
    estimatedSeconds: number;
    startedAt?: number;
    completedAt?: number;
    actualSeconds?: number;
};

type DashboardEtaRun = {
    startedAt: number;
    estimatedTotalSeconds: number;
    steps: DashboardEtaStep[];
};

type DashboardEtaHistoryEntry = {
    avgSeconds: number;
    samples: number;
    updatedAt: number;
};

type DashboardEtaHistory = Record<string, DashboardEtaHistoryEntry>;

const DASHBOARD_ETA_HISTORY_KEY = "dashboardLoaderEtaHistory:v1";
const DASHBOARD_STEP_FALLBACK_SECONDS = 20;
const MAX_DASHBOARD_ETA_HISTORY_SAMPLES = 20;
const MIN_ACTIVE_DASHBOARD_ETA_SECONDS = 8;
const MAX_DASHBOARD_STEP_ETA_SECONDS = 30 * 60;
const SHORT_DISPLAY_DASHBOARD_STEP_NUMS = new Set([3]);

const clampDashboardEtaSeconds = (seconds: number) => {
    if (!Number.isFinite(seconds)) return DASHBOARD_STEP_FALLBACK_SECONDS;
    return Math.min(
        MAX_DASHBOARD_STEP_ETA_SECONDS,
        Math.max(1, Math.ceil(seconds))
    );
};

const formatDashboardEtaDuration = (seconds: number) => {
    const safeSeconds = Math.max(0, Math.ceil(seconds));
    const mm = String(Math.floor(safeSeconds / 60)).padStart(2, "0");
    const ss = String(safeSeconds % 60).padStart(2, "0");

    return `${mm}:${ss}`;
};

const getDashboardEtaKey = (step: { num: number; label: string }) =>
    `${step.num}:${step.label}`;

const readDashboardEtaHistory = (): DashboardEtaHistory => {
    if (typeof window === "undefined") return {};

    try {
        const raw = window.localStorage.getItem(DASHBOARD_ETA_HISTORY_KEY);
        if (!raw) return {};

        const parsed = JSON.parse(raw) as DashboardEtaHistory;
        return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
        return {};
    }
};

const getStoredDashboardEtaSeconds = (
    step: { num: number; label: string },
    fallbackSeconds: number
) => {
    if (SHORT_DISPLAY_DASHBOARD_STEP_NUMS.has(step.num)) {
        return clampDashboardEtaSeconds(fallbackSeconds);
    }

    const history = readDashboardEtaHistory();
    const saved = history[getDashboardEtaKey(step)]?.avgSeconds;

    return clampDashboardEtaSeconds(saved ?? fallbackSeconds);
};

const updateStoredDashboardEtaSeconds = (
    step: { num: number; label: string },
    actualSeconds: number
) => {
    if (typeof window === "undefined") return;
    if (SHORT_DISPLAY_DASHBOARD_STEP_NUMS.has(step.num)) return;

    const history = readDashboardEtaHistory();
    const key = getDashboardEtaKey(step);
    const previous = history[key];
    const previousSamples = Math.min(
        previous?.samples ?? 0,
        MAX_DASHBOARD_ETA_HISTORY_SAMPLES - 1
    );
    const nextSamples = Math.min(
        previousSamples + 1,
        MAX_DASHBOARD_ETA_HISTORY_SAMPLES
    );
    const clampedActual = clampDashboardEtaSeconds(actualSeconds);
    const previousAverage = previous?.avgSeconds ?? clampedActual;
    const nextAverage =
        (previousAverage * previousSamples + clampedActual) /
        Math.max(previousSamples + 1, 1);

    history[key] = {
        avgSeconds: clampDashboardEtaSeconds(nextAverage),
        samples: nextSamples,
        updatedAt: Date.now(),
    };

    try {
        window.localStorage.setItem(
            DASHBOARD_ETA_HISTORY_KEY,
            JSON.stringify(history)
        );
    } catch {
        // localStorage can fail in private mode; the current run still uses live timing.
    }
};

const buildDashboardEtaRun = (
    dashboardSteps: { num: number; label: string }[],
    estimatedSecondsMap: Record<number, number>,
    startedAt: number
): DashboardEtaRun => {
    const steps = dashboardSteps.map((step) => {
        const fallbackSeconds =
            estimatedSecondsMap[step.num] ?? DASHBOARD_STEP_FALLBACK_SECONDS;

        return {
            ...step,
            estimatedSeconds: getStoredDashboardEtaSeconds(step, fallbackSeconds),
        };
    });

    const estimatedTotalSeconds = Math.max(
        1,
        steps.reduce((sum, step) => sum + step.estimatedSeconds, 0)
    );

    return {
        startedAt,
        estimatedTotalSeconds,
        steps,
    };
};

export const DashboardLoaderModal = React.memo(function DashboardLoaderModal({
    pageLoading,
    shouldShowDummyUi,
    currentStep,
    completedSteps,
    dashboardSteps,
    stepProgress,
    loadingStartedAt,
    estimatedSecondsMap,
    onCancel,
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
    onCancel?: () => void;
}) {
    const [timerNow, setTimerNow] = useState(Date.now());
    const [dynamicProgress, setDynamicProgress] = useState(0);
    const etaRunRef = useRef<DashboardEtaRun | null>(null);

    useEffect(() => {
        if (!pageLoading || !stepProgress.active || !loadingStartedAt) {
            etaRunRef.current = null;
            setDynamicProgress(0);
            return;
        }

        if (etaRunRef.current?.startedAt !== loadingStartedAt) {
            etaRunRef.current = buildDashboardEtaRun(
                dashboardSteps,
                estimatedSecondsMap,
                loadingStartedAt
            );
            setTimerNow(Date.now());
            setDynamicProgress(0);
        }
    }, [
        dashboardSteps,
        estimatedSecondsMap,
        loadingStartedAt,
        pageLoading,
        stepProgress.active,
    ]);

    useEffect(() => {
        if (!pageLoading || !stepProgress.active || !loadingStartedAt) return;

        const interval = setInterval(() => {
            setTimerNow(Date.now());
        }, 1000);

        return () => clearInterval(interval);
    }, [pageLoading, stepProgress.active, loadingStartedAt]);

    useEffect(() => {
        const run = etaRunRef.current;
        if (!pageLoading || !stepProgress.active || !loadingStartedAt || !run || !currentStep) {
            return;
        }

        const etaStep = run.steps.find((step) => step.num === currentStep);
        if (!etaStep || etaStep.completedAt || etaStep.startedAt) return;

        const firstStepNum = run.steps[0]?.num;
        const startedAt = currentStep === firstStepNum ? loadingStartedAt : Date.now();

        etaStep.startedAt = startedAt;
        setTimerNow(startedAt);
    }, [currentStep, loadingStartedAt, pageLoading, stepProgress.active]);

    useEffect(() => {
        const run = etaRunRef.current;
        if (!pageLoading || !loadingStartedAt || !run) return;

        const completedAt = Date.now();
        let changed = false;

        completedSteps.forEach((stepNum) => {
            const etaStep = run.steps.find((step) => step.num === stepNum);
            if (!etaStep || etaStep.completedAt) return;

            const firstStepNum = run.steps[0]?.num;
            const startedAt =
                etaStep.startedAt ??
                (stepNum === firstStepNum ? loadingStartedAt : completedAt);
            const actualSeconds = clampDashboardEtaSeconds(
                (completedAt - startedAt) / 1000
            );

            etaStep.startedAt = startedAt;
            etaStep.completedAt = completedAt;
            etaStep.actualSeconds = actualSeconds;
            updateStoredDashboardEtaSeconds(etaStep, actualSeconds);
            changed = true;
        });

        if (changed) {
            setTimerNow(completedAt);
        }
    }, [completedSteps, loadingStartedAt, pageLoading]);

    const dynamicRemainingSeconds = useMemo(() => {
        if (!pageLoading || !stepProgress.active || !loadingStartedAt) return null;

        const run = etaRunRef.current;
        if (!run) return null;

        const remaining = run.steps.reduce((total, step) => {
            if (step.completedAt || completedSteps.has(step.num)) return total;

            if (!step.startedAt) {
                return total + step.estimatedSeconds;
            }

            const elapsed = Math.max((timerNow - step.startedAt) / 1000, 0);

            if (SHORT_DISPLAY_DASHBOARD_STEP_NUMS.has(step.num)) {
                const shortRemaining = step.estimatedSeconds - elapsed;
                return total + Math.max(1, Math.min(step.estimatedSeconds, shortRemaining));
            }

            if (elapsed < step.estimatedSeconds) {
                return total + (step.estimatedSeconds - elapsed);
            }

            const overrunElapsed = elapsed - step.estimatedSeconds;
            const softOverrunBuffer =
                step.estimatedSeconds * 0.2 - overrunElapsed * 0.2;

            return total + Math.max(MIN_ACTIVE_DASHBOARD_ETA_SECONDS, softOverrunBuffer);
        }, 0);

        return Math.max(1, Math.ceil(remaining));
    }, [
        completedSteps,
        loadingStartedAt,
        pageLoading,
        stepProgress.active,
        timerNow,
    ]);

    const activeStepIsOverdue = useMemo(() => {
        const run = etaRunRef.current;
        if (!run) return false;

        return run.steps.some((step) => {
            if (!step.startedAt || step.completedAt || completedSteps.has(step.num)) {
                return false;
            }
            if (SHORT_DISPLAY_DASHBOARD_STEP_NUMS.has(step.num)) return false;

            const elapsed = Math.max((timerNow - step.startedAt) / 1000, 0);
            return elapsed >= step.estimatedSeconds;
        });
    }, [completedSteps, timerNow]);

    const allDashboardStepsComplete = useMemo(() => {
        return (
            dashboardSteps.length > 0 &&
            dashboardSteps.every((step) => completedSteps.has(step.num))
        );
    }, [completedSteps, dashboardSteps]);

    const calculatedDynamicProgress = useMemo(() => {
        const run = etaRunRef.current;
        if (!pageLoading || !stepProgress.active || !run) return 0;
        if (allDashboardStepsComplete) return 100;
        if (dynamicRemainingSeconds === null || !run.estimatedTotalSeconds) return 0;

        const progress =
            ((run.estimatedTotalSeconds - dynamicRemainingSeconds) /
                run.estimatedTotalSeconds) *
            100;

        return Math.min(99, Math.max(0, Math.round(progress)));
    }, [
        allDashboardStepsComplete,
        dynamicRemainingSeconds,
        pageLoading,
        stepProgress.active,
    ]);

    useEffect(() => {
        if (!pageLoading || !stepProgress.active) {
            setDynamicProgress(0);
            return;
        }

        setDynamicProgress((prev) => Math.max(prev, calculatedDynamicProgress));
    }, [calculatedDynamicProgress, pageLoading, stepProgress.active]);

    const estimatedTime = useMemo(() => {
        if (!stepProgress.active || !loadingStartedAt) return "00:00";

        if (dynamicRemainingSeconds === null) return "Estimating...";
        if (activeStepIsOverdue) return "Still syncing...";
        if (dynamicRemainingSeconds <= 5) return "Finishing up...";

        return formatDashboardEtaDuration(dynamicRemainingSeconds);
    }, [
        activeStepIsOverdue,
        dynamicRemainingSeconds,
        loadingStartedAt,
        stepProgress.active,
    ]);

    const displayProgressPercentage = allDashboardStepsComplete ? 100 : dynamicProgress;

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
                        {displayProgressPercentage}%
                    </span>
                </div>

                <div className="h-[7px] w-full bg-slate-100 rounded-full overflow-hidden mb-6">
                    <div
                        className="h-full rounded-full transition-all duration-500 ease-in-out"
                        style={{
                            width: `${displayProgressPercentage}%`,
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

                <div className="mt-5 pt-4 border-t border-slate-100 grid grid-cols-1 gap-3 sm:grid-cols-3 sm:items-center">
                    <p className="text-xs text-slate-400 truncate justify-self-start w-full sm:w-auto">
                        {stepProgress.detail || "Initialising dashboard…"}
                    </p>

                    <div className="justify-self-start sm:justify-self-center flex items-center gap-1 px-2 py-1 bg-slate-100 rounded-full sm:mx-3 whitespace-nowrap">
                        <span className="text-xs text-slate-400">Estimated Time:</span>
                        <span className="text-xs font-medium text-slate-600 tabular-nums min-w-[72px] text-right">
                            {estimatedTime}
                        </span>
                    </div>

                    <div className="justify-self-start sm:justify-self-end flex items-center">
                        <span className="text-xs text-slate-400 shrink-0">
                            Step {Math.min(currentStep, dashboardSteps.length)} of {dashboardSteps.length}
                        </span>
                    </div>

                    <div className="sm:col-span-3 flex justify-center">
                        <button
                            type="button"
                            onClick={onCancel}
                            className="rounded-md border border-slate-200 bg-white px-4 py-1.5 text-xs font-medium text-[#37455F] shadow-sm transition hover:border-[#5EA68E] hover:bg-[#E8F5F0] hover:text-[#2f6f5f] focus:outline-none focus:ring-2 focus:ring-[#5EA68E]/30"
                        >
                            Cancel
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
});
