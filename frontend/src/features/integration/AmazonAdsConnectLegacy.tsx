"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FaLink } from "react-icons/fa";
import { TiTick } from "react-icons/ti";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
// import { usePlatform } from "@/components/context/PlatformContext";

const API_BASE =
    process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const getAuthToken = () =>
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

type ApiErrorResponse = {
    error?: string;
    message?: string;
};

type AdsCountry = "UK" | "US" | "CA";

type AdsEtaUnitType =
    | "spReport"
    | "sdReport"
    | "sbReport"
    | "monthlyAdsDb"
    | "dailyAdsDb";

type AdsEtaUnit = {
    id: string;
    type: AdsEtaUnitType;
    label: string;
    fallbackSeconds: number;
    startedAt?: number;
    completedAt?: number;
    actualSeconds?: number;
};

type AdsEtaGroup = {
    id: string;
    mode: "parallel" | "sequential";
    units: AdsEtaUnit[];
};

type AdsEtaPlan = {
    startedAt: number;
    estimatedTotalSeconds: number;
    groups: AdsEtaGroup[];
};

type AdsEtaHistory = Record<
    string,
    {
        avgSeconds: number;
        samples: number;
        updatedAt: number;
    }
>;

const ADS_ETA_HISTORY_STORAGE_KEY = "amazonAdsFetchEtaHistory:v2";
const MAX_ADS_ETA_HISTORY_SAMPLES = 20;
const MIN_ACTIVE_ADS_ETA_SECONDS = 8;

const DEFAULT_ADS_ETA_SECONDS: Record<AdsEtaUnitType, number> = {
    spReport: 480,
    sdReport: 480,
    sbReport: 480,
    monthlyAdsDb: 60,
    dailyAdsDb: 60,
};

function clampAdsEtaSeconds(value: number) {
    if (!Number.isFinite(value) || value <= 0) return MIN_ACTIVE_ADS_ETA_SECONDS;
    return Math.min(Math.max(value, 1), 60 * 60);
}

function adsEtaHistoryKey(scope: string, type: AdsEtaUnitType) {
    return `${scope}:${type}`;
}

function readAdsEtaHistory(): AdsEtaHistory {
    if (typeof window === "undefined") return {};

    try {
        const raw = window.localStorage.getItem(ADS_ETA_HISTORY_STORAGE_KEY);
        if (!raw) return {};

        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
        return {};
    }
}

function getStoredAdsEtaSeconds(scope: string, type: AdsEtaUnitType) {
    const history = readAdsEtaHistory();
    const entry = history[adsEtaHistoryKey(scope, type)];

    if (!entry || !Number.isFinite(entry.avgSeconds)) return null;
    return clampAdsEtaSeconds(entry.avgSeconds);
}

function updateStoredAdsEtaSeconds(
    scope: string,
    type: AdsEtaUnitType,
    actualSeconds: number
) {
    if (typeof window === "undefined") return;

    try {
        const history = readAdsEtaHistory();
        const key = adsEtaHistoryKey(scope, type);
        const previous = history[key];
        const boundedActual = clampAdsEtaSeconds(actualSeconds);
        const previousWeight = previous
            ? Math.min(previous.samples, MAX_ADS_ETA_HISTORY_SAMPLES - 1)
            : 0;
        const avgSeconds =
            previousWeight > 0
                ? (previous.avgSeconds * previousWeight + boundedActual) /
                (previousWeight + 1)
                : boundedActual;

        history[key] = {
            avgSeconds: Math.round(clampAdsEtaSeconds(avgSeconds)),
            samples: Math.min(previousWeight + 1, MAX_ADS_ETA_HISTORY_SAMPLES),
            updatedAt: Date.now(),
        };

        window.localStorage.setItem(
            ADS_ETA_HISTORY_STORAGE_KEY,
            JSON.stringify(history)
        );
    } catch {
        // ETA history is optional; ads sync should continue even if storage fails.
    }
}

function formatAdsEtaDuration(totalSeconds: number) {
    const safeSeconds = Math.max(0, Math.ceil(totalSeconds));
    const minutes = Math.floor(safeSeconds / 60);
    const seconds = safeSeconds % 60;

    return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

const mapCountry = (country?: string): AdsCountry => {
    const upper = (country || "").toUpperCase();

    if (upper === "US") return "US";
    if (upper === "CA") return "CA";
    if (upper === "UK") return "UK";

    return "UK";
};

async function apiJson<T = unknown>(
    path: string,
    options: RequestInit = {}
): Promise<T> {
    const token = getAuthToken();
    const res = await fetch(`${API_BASE}${path}`, {
        ...options,
        headers: {
            Accept: "application/json",
            ...(options.headers || {}),
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
    });

    const data = (await res.json().catch(() => ({}))) as ApiErrorResponse;

    if (!res.ok) {
        throw new Error(data.error || data.message || `HTTP ${res.status}`);
    }

    return data as T;
}
const ICONS = {
    amazonAdsLogo: "/amazon_ads.png",
    secure: "/secure_black.png",
};

// type Props = {
//     onClose?: () => void;
//     onConnected?: () => void | Promise<void>;
//     /** if you want auto-redirect after connect */
//     redirectUrl?: string;
//     /** optional override */
//     pollIntervalMs?: number; // default 1500
//     /** optional override */
//     maxWaitMs?: number; // default 2 min
// };

const getIstTodayISO = () => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    const y = ist.getFullYear();
    const m = ist.getMonth() + 1;
    const d = ist.getDate();
    return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
};

const getIstMonthStartISO = () => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));
    const y = ist.getFullYear();
    const m = ist.getMonth() + 1;
    return `${y}-${String(m).padStart(2, "0")}-01`;
};

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

const getCurrentMonthYearIST = () => {
    const now = new Date();
    const ist = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" }));

    return {
        month: ist.toLocaleString("en-US", {
            month: "long",
            timeZone: "Asia/Kolkata",
        }),
        year: ist.getFullYear(),
    };
};

async function postJson<TBody extends Record<string, unknown>, TResponse = unknown>(
    path: string,
    body: TBody
): Promise<TResponse> {
    return apiJson<TResponse>(path, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
    });
}

async function ensureSpReportOncePerDay(country: "UK" | "US" | "CA") {
    const token = getAuthToken();
    if (!token) throw new Error("Missing auth token");

    const userId = decodeJwtUserId(token) || "unknown";
    const end_date = getIstTodayISO();
    const start_date = getIstMonthStartISO();

    const storageKey = `sp_report_seed_daily_${userId}_${country}_${end_date}`;
    if (localStorage.getItem(storageKey) === "1") return;

    await postJson(`/api/ads/manager/sp_advertised_product_report`, {
        start_date,
        end_date,
        time_unit: "DAILY",
        countries: [country],
        return_excel: false,
    });

    localStorage.setItem(storageKey, "1");
}

// async function seedAdsReportsOnConnect(country: "UK" | "US" | "CA") {
//     const start_date = getIstMonthStartISO();
//     const end_date = getIstTodayISO();

//     await ensureSpReportOncePerDay(country);

//     await postJson(`/api/ads/manager/sb_keyword_report`, {
//         start_date,
//         end_date,
//         time_unit: "SUMMARY",
//         countries: ["UK"],
//         return_excel: false,
//     });

//     if (country === "UK" || country === "US") {
//         await postJson(`/api/ads/manager/sd_advertised_product_report/sync`, {
//             start_date,
//             end_date,
//             time_unit: "SUMMARY",
//             countries: [country],
//             max_wait_seconds: 900,
//             poll_every_seconds: 10,
//         });
//     }

//     const { month, year } = getCurrentMonthYearIST();
//     const include =
//         country === "UK" || country === "US"
//             ? ["SP", "SD"]
//             : ["SP"];

//     console.log("PAYLOAD:", {
//         month: monthToNumber(month),
//         year,
//         country,
//         include,
//     });


//     await postJson(`/api/ads/monthly_sp_sd_to_db`, {
//         month: monthToNumber(month),
//         year,
//         country: country || "UK",
//         include,
//     });
// }

async function seedAdsReportsOnConnect(
    country: AdsCountry,
    hooks?: {
        onStep?: (
            step: number,
            label: string,
            percentage?: number,
            detail?: string
        ) => void;
        onCompleteStep?: (step: number) => void;
        onActiveSteps?: (steps: number[]) => void;
        runEtaUnit?: <T>(unitId: string, fn: () => Promise<T>) => Promise<T>;
    }
) {
    const onStep = hooks?.onStep;
    const onCompleteStep = hooks?.onCompleteStep;
    const onActiveSteps = hooks?.onActiveSteps;
    const runEtaUnit =
        hooks?.runEtaUnit ?? (async <T,>(_unitId: string, fn: () => Promise<T>) => fn());

    const start_date = getIstMonthStartISO();
    const end_date = getIstTodayISO();

    onActiveSteps?.([1, 2, 3]);
    onStep?.(1, "Amazon Ads reports", 20, "Starting Sponsored Product, Display, and Brand sync...");
    const spSyncTask = runEtaUnit("spReport", () =>
        postJson(`/api/ads/manager/sp_advertised_product_report`, {
            start_date,
            end_date,
            time_unit: "DAILY",
            countries: [country],
            return_excel: false,
        })
    ).then(() => {
        onCompleteStep?.(1);
    });

    const sdSyncTask = runEtaUnit("sdReport", () =>
        postJson(`/api/ads/manager/sd_advertised_product_report/sync`, {
            start_date,
            end_date,
            time_unit: "DAILY",
            countries: [country],
            max_wait_seconds: 1800,
            poll_every_seconds: 10,
        })
    ).then(() => {
        onCompleteStep?.(2);
    });

    const sbSyncTask = runEtaUnit("sbReport", () =>
        postJson(`/api/ads/manager/sb_keyword_report`, {
            start_date,
            end_date,
            time_unit: "SUMMARY",
            countries: [country],
            return_excel: false,
        })
    );

    await Promise.all([spSyncTask, sdSyncTask, sbSyncTask]);

    onActiveSteps?.([3]);
    onStep?.(3, "Ads summary", 75, "Building monthly and daily ads tables...");

    const { month, year } = getCurrentMonthYearIST();

    const adsDbPayload = {
        month: monthToNumber(month),
        year,
        country,
        include: ["SP", "SD", "SB"],
    };

    await runEtaUnit("monthlyAdsDb", () =>
        postJson(`/api/ads/monthly_sp_sd_to_db`, adsDbPayload)
    );

    await runEtaUnit("dailyAdsDb", () =>
        postJson(`/api/ads/daily_sp_sd_sb_to_db`, adsDbPayload)
    );

    onStep?.(3, "Sponsored Brand", 100, "Sponsored Brand and monthly sync complete");
    onCompleteStep?.(3);
}

const AdsSyncLoaderModal = React.memo(function AdsSyncLoaderModal({
    open,
    currentStep,
    activeSteps,
    completedSteps,
    dashboardSteps,
    stepProgress,
    progressPercentage,
    estimatedTime,
    onDismiss,
}: {
    open: boolean;
    currentStep: number;
    activeSteps: Set<number>;
    completedSteps: Set<number>;
    dashboardSteps: { num: number; label: string }[];
    stepProgress: {
        active: boolean;
        label: string;
        percentage: number;
        detail?: string;
    };
    progressPercentage: number;
    estimatedTime: string;
    onDismiss?: () => void;
}) {
    const displayActiveSteps = useMemo(() => {
        if (
            activeSteps.size === 0 &&
            currentStep === 3 &&
            completedSteps.size === 0 &&
            progressPercentage <= 25
        ) {
            return new Set(dashboardSteps.map((step) => step.num));
        }

        return activeSteps;
    }, [activeSteps, completedSteps.size, currentStep, dashboardSteps, progressPercentage]);

    if (!open) return null;

    return (
        <div
            className="fixed inset-0 z-[100000] bg-black/40 flex items-center justify-center px-4"
            onClick={(e) => {
                e.stopPropagation();
                onDismiss?.();
            }}
        >
            <div
                className="w-full max-w-xl rounded-2xl border border-slate-200 bg-white p-5 md:p-6 shadow-md"
                onClick={(e) => e.stopPropagation()}
            >
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
                                Syncing Amazon Ads data
                            </p>
                        </div>
                    </div>

                    <span className="text-sm font-bold text-[#5EA68E] tabular-nums">
                        {progressPercentage}%
                    </span>
                </div>

                <div className="h-[7px] w-full bg-slate-100 rounded-full overflow-hidden mb-6">
                    <div
                        className="h-full rounded-full transition-all duration-500 ease-in-out"
                        style={{
                            width: `${progressPercentage}%`,
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
                        const isActive =
                            !isCompleted && (displayActiveSteps.has(step.num) || currentStep === step.num);

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
                                        "text-center text-[10px] sm:text-xs font-medium leading-tight",
                                        isCompleted || isActive
                                            ? "text-[#37455F]"
                                            : "text-slate-400",
                                    ].join(" ")}
                                >
                                    {step.label}
                                </p>

                                <span
                                    className={[
                                        "text-[9px] sm:text-[10px] px-2 py-0.5 rounded-full font-medium",
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

                <div className="mt-5 pt-4 border-t border-slate-100 flex justify-center">
                    {/* <p className="text-xs text-slate-400 truncate">
                        {stepProgress.detail || "Initialising sync…"}
                    </p> */}

                    <div className="flex items-center gap-1 px-3 py-1 bg-slate-100 rounded-full">
                        <span className="text-xs text-slate-400">Estimated:</span>
                        <span className="text-xs font-medium text-slate-600">
                            {estimatedTime}
                        </span>
                    </div>

                    {/* <span className="text-xs text-slate-400 shrink-0">
                        {(() => {
                            const runningCount = dashboardSteps.filter(
                                (step) => displayActiveSteps.has(step.num) && !completedSteps.has(step.num)
                            ).length;

                            if (runningCount > 1) return `${runningCount} tasks running`;

                            return `Step ${Math.min(currentStep, dashboardSteps.length)} of ${dashboardSteps.length}`;
                        })()}
                    </span> */}
                </div>
               
            </div>
        </div>
    );
});

type Props = {
    onClose?: () => void;
    onConnected?: () => void | Promise<void>;
    country?: AdsCountry;
    redirectUrl?: string;
    pollIntervalMs?: number;
    maxWaitMs?: number;
};

export default function AmazonAdsConnect({
    onClose,
    onConnected,
    country,
    pollIntervalMs = 1500,
    maxWaitMs = 2 * 60 * 1000,
}: Props) {
    // usePlatform();

    // const resolvedCountry: "UK" | "US" | "CA" =
    //     platform === "amazon-us" ? "US" :
    //         platform === "amazon-ca" ? "CA" :
    //             "UK";

    const resolvedCountry = mapCountry(country);

    const [isConnecting, setIsConnecting] = useState(false);
    const [error, setError] = useState("");
    const [message, setMessage] = useState("");
    const [isOpen, setIsOpen] = useState(true);
    const [showSuccessPopup, setShowSuccessPopup] = useState(false);
    const [isSyncModalDismissed, setIsSyncModalDismissed] = useState(false);
    const popupRef = useRef<Window | null>(null);
    const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
    const startedAtRef = useRef<number>(0);


    const [currentStep, setCurrentStep] = useState<number>(0);
    const [activeSteps, setActiveSteps] = useState<Set<number>>(new Set());
    const [completedSteps, setCompletedSteps] = useState<Set<number>>(new Set());
    const [remainingSeconds, setRemainingSeconds] = useState<number | null>(null);
    const [dynamicProgress, setDynamicProgress] = useState(0);
    const etaPlanRef = useRef<AdsEtaPlan | null>(null);
    const etaSamplesRef = useRef<Partial<Record<AdsEtaUnitType, number[]>>>({});
    const etaScope = useMemo(() => `amazon-ads:${resolvedCountry}`, [resolvedCountry]);

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

    const stepLabels: Record<number, string> = {
        1: "Sponsored Product",
        2: "Sponsored Display",
        3: "Sponsored Brand",
    };

    const getEtaEstimate = useCallback(
        (
            type: AdsEtaUnitType,
            fallbackSeconds: number = DEFAULT_ADS_ETA_SECONDS[type]
        ) => {
            const currentSamples = etaSamplesRef.current[type];

            if (currentSamples?.length) {
                const avg =
                    currentSamples.reduce((sum, value) => sum + value, 0) /
                    currentSamples.length;
                return clampAdsEtaSeconds(avg);
            }

            return getStoredAdsEtaSeconds(etaScope, type) ?? clampAdsEtaSeconds(fallbackSeconds);
        },
        [etaScope]
    );

    const makeEtaUnit = useCallback(
        (
            id: string,
            type: AdsEtaUnitType,
            label: string,
            fallbackSeconds: number = DEFAULT_ADS_ETA_SECONDS[type]
        ): AdsEtaUnit => ({
            id,
            type,
            label,
            fallbackSeconds: getEtaEstimate(type, fallbackSeconds),
        }),
        [getEtaEstimate]
    );

    const buildAdsEtaGroups = useCallback(
        (): AdsEtaGroup[] => [
            {
                id: "reports",
                mode: "parallel",
                units: [
                    makeEtaUnit("spReport", "spReport", "Sponsored Product report"),
                    makeEtaUnit("sdReport", "sdReport", "Sponsored Display report"),
                    makeEtaUnit("sbReport", "sbReport", "Sponsored Brand report"),
                ],
            },
            {
                id: "adsTables",
                mode: "sequential",
                units: [
                    makeEtaUnit("monthlyAdsDb", "monthlyAdsDb", "Monthly ads table"),
                    makeEtaUnit("dailyAdsDb", "dailyAdsDb", "Daily ads table"),
                ],
            },
        ],
        [makeEtaUnit]
    );

    const calculateUnitRemainingSeconds = useCallback(
        (unit: AdsEtaUnit) => {
            if (unit.completedAt) return 0;

            const estimate = getEtaEstimate(unit.type, unit.fallbackSeconds);

            if (!unit.startedAt) {
                return estimate;
            }

            const elapsed = Math.max((Date.now() - unit.startedAt) / 1000, 0);

            if (elapsed < estimate) {
                return estimate - elapsed;
            }

            const overrunElapsed = elapsed - estimate;
            const softOverrunBuffer = estimate * 0.2 - overrunElapsed * 0.2;

            return Math.max(MIN_ACTIVE_ADS_ETA_SECONDS, softOverrunBuffer);
        },
        [getEtaEstimate]
    );

    const calculateDynamicRemainingSeconds = useCallback(() => {
        const plan = etaPlanRef.current;
        if (!plan) return null;

        const remaining = plan.groups.reduce((total, group) => {
            const unitRemaining = group.units.map(calculateUnitRemainingSeconds);

            if (group.mode === "parallel") {
                return total + Math.max(0, ...unitRemaining);
            }

            return total + unitRemaining.reduce((sum, value) => sum + value, 0);
        }, 0);

        return Math.max(1, Math.ceil(remaining));
    }, [calculateUnitRemainingSeconds]);

    const calculateDynamicProgressPercentage = useCallback(() => {
        const plan = etaPlanRef.current;
        if (!plan) return 0;

        const allUnits = plan.groups.flatMap((group) => group.units);
        if (allUnits.length > 0 && allUnits.every((unit) => unit.completedAt)) {
            return 100;
        }

        const remaining = plan.groups.reduce((total, group) => {
            const unitRemaining = group.units.map(calculateUnitRemainingSeconds);

            if (group.mode === "parallel") {
                return total + Math.max(0, ...unitRemaining);
            }

            return total + unitRemaining.reduce((sum, value) => sum + value, 0);
        }, 0);

        if (!plan.estimatedTotalSeconds) return 0;

        const progress =
            ((plan.estimatedTotalSeconds - remaining) / plan.estimatedTotalSeconds) * 100;

        return Math.min(99, Math.max(0, Math.round(progress)));
    }, [calculateUnitRemainingSeconds]);

    const startEtaPlan = useCallback(
        (groups: AdsEtaGroup[]) => {
            etaSamplesRef.current = {};
            const initialEstimate = groups.reduce((total, group) => {
                const unitEstimates = group.units.map((unit) =>
                    getEtaEstimate(unit.type, unit.fallbackSeconds)
                );

                if (group.mode === "parallel") {
                    return total + Math.max(0, ...unitEstimates);
                }

                return total + unitEstimates.reduce((sum, value) => sum + value, 0);
            }, 0);

            const estimatedTotalSeconds = Math.max(1, Math.ceil(initialEstimate));

            etaPlanRef.current = {
                startedAt: Date.now(),
                estimatedTotalSeconds,
                groups,
            };

            setRemainingSeconds(estimatedTotalSeconds);
            setDynamicProgress(0);
        },
        [getEtaEstimate]
    );

    const findEtaUnit = useCallback((unitId: string) => {
        const plan = etaPlanRef.current;
        if (!plan) return null;

        for (const group of plan.groups) {
            const unit = group.units.find((item) => item.id === unitId);
            if (unit) return unit;
        }

        return null;
    }, []);

    const startEtaUnit = useCallback(
        (unitId: string) => {
            const unit = findEtaUnit(unitId);

            if (unit && !unit.startedAt && !unit.completedAt) {
                unit.startedAt = Date.now();
            }

            setRemainingSeconds(calculateDynamicRemainingSeconds());
            setDynamicProgress((prev) =>
                Math.max(prev, calculateDynamicProgressPercentage())
            );
        },
        [calculateDynamicProgressPercentage, calculateDynamicRemainingSeconds, findEtaUnit]
    );

    const completeEtaUnit = useCallback(
        (unitId: string) => {
            const unit = findEtaUnit(unitId);
            if (!unit || unit.completedAt) return;

            const completedAt = Date.now();
            const startedAt = unit.startedAt ?? completedAt;
            const actualSeconds = clampAdsEtaSeconds((completedAt - startedAt) / 1000);

            unit.startedAt = startedAt;
            unit.completedAt = completedAt;
            unit.actualSeconds = actualSeconds;

            const samples = etaSamplesRef.current[unit.type] ?? [];
            etaSamplesRef.current[unit.type] = [
                ...samples.slice(-(MAX_ADS_ETA_HISTORY_SAMPLES - 1)),
                actualSeconds,
            ];

            updateStoredAdsEtaSeconds(etaScope, unit.type, actualSeconds);
            setRemainingSeconds(calculateDynamicRemainingSeconds());
            setDynamicProgress((prev) =>
                Math.max(prev, calculateDynamicProgressPercentage())
            );
        },
        [calculateDynamicProgressPercentage, calculateDynamicRemainingSeconds, etaScope, findEtaUnit]
    );

    const runEtaUnit = useCallback(
        async <T,>(unitId: string, fn: () => Promise<T>): Promise<T> => {
            startEtaUnit(unitId);

            try {
                return await fn();
            } finally {
                completeEtaUnit(unitId);
            }
        },
        [completeEtaUnit, startEtaUnit]
    );

    useEffect(() => {
        if (!isConnecting || !stepProgress.active) {
            etaPlanRef.current = null;
            setRemainingSeconds(null);
            setDynamicProgress(0);
            return;
        }

        const interval = window.setInterval(() => {
            setRemainingSeconds(calculateDynamicRemainingSeconds());
            setDynamicProgress((prev) =>
                Math.max(prev, calculateDynamicProgressPercentage())
            );
        }, 1000);

        setRemainingSeconds(calculateDynamicRemainingSeconds());
        setDynamicProgress((prev) =>
            Math.max(prev, calculateDynamicProgressPercentage())
        );

        return () => window.clearInterval(interval);
    }, [
        calculateDynamicProgressPercentage,
        calculateDynamicRemainingSeconds,
        isConnecting,
        stepProgress.active,
    ]);

    const markStepComplete = (step: number) => {
        setActiveSteps((prev) => {
            const next = new Set(prev);
            next.delete(step);
            if (next.size > 0) {
                setCurrentStep(Math.min(...Array.from(next)));
            }
            return next;
        });

        setCompletedSteps((prev) => {
            const next = new Set([...prev, step]);
            const reportPct = Math.min(65, 20 + next.size * 15);

            setStepProgress((progress) => {
                if (!progress.active || progress.percentage >= 100) return progress;

                return {
                    ...progress,
                    percentage: Math.max(progress.percentage, reportPct),
                    detail: `${stepLabels[step] || "Ads report"} synced. Waiting for remaining work...`,
                };
            });

            return next;
        });
    };

    const setRunningSteps = (steps: number[]) => {
        setActiveSteps(new Set(steps));
        if (steps.length > 0) {
            setCurrentStep(Math.min(...steps));
        }
    };

    const setStep = (
        step: number,
        label: string,
        percentage: number = 0,
        detail?: string
    ) => {
        setCurrentStep(step);
        setActiveSteps((prev) => new Set([...prev, step]));
        setStepProgress({
            active: true,
            label,
            percentage: Math.min(100, Math.max(0, percentage)),
            detail,
        });
    };

    const resetStepState = () => {
        setCurrentStep(0);
        setActiveSteps(new Set());
        setCompletedSteps(new Set());
        etaPlanRef.current = null;
        setRemainingSeconds(null);
        setDynamicProgress(0);
        setStepProgress({
            active: false,
            label: "",
            percentage: 0,
            detail: "",
        });
    };

    const handleCloseModal = () => {
        if (isConnecting || stepProgress.active) {
            setIsSyncModalDismissed(true);
            return;
        }

        setIsOpen(false);
        onClose?.();
    };

    const dashboardSteps = [
        { num: 1, label: "Sponsored Product" },
        { num: 2, label: "Sponsored Display" },
        { num: 3, label: "Sponsored Brand" },
    ];

    const estimatedTimeText =
        remainingSeconds === null
            ? "Estimating..."
            : etaPlanRef.current?.groups.some((group) =>
                group.units.some((unit) => {
                    if (!unit.startedAt || unit.completedAt) return false;

                    const estimate = getEtaEstimate(unit.type, unit.fallbackSeconds);
                    const elapsed = (Date.now() - unit.startedAt) / 1000;

                    return elapsed >= estimate;
                })
            )
                ? "Still syncing..."
                : remainingSeconds <= 5
                ? "Finishing up..."
                : formatAdsEtaDuration(remainingSeconds);


    const stopPolling = () => {
        if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
        }
    };

    const closePopup = () => {
        try {
            if (popupRef.current && !popupRef.current.closed) popupRef.current.close();
        } catch { }
        popupRef.current = null;
    };

    const finalizedRef = useRef(false);

    const finalizeConnection = async () => {
        if (finalizedRef.current) return;
        finalizedRef.current = true;

        setMessage("Connected to Amazon Ads ✅");
        setError("");

        stopPolling();
        closePopup();

        try {
            setIsOpen(true);
            setIsConnecting(true);
            setShowSuccessPopup(false);
            setIsSyncModalDismissed(false);
            setCurrentStep(1);
            setActiveSteps(new Set([1]));
            setCompletedSteps(new Set());
            setStepProgress({
                active: true,
                label: "",
                percentage: 0,
                detail: "",
            });
            startEtaPlan(buildAdsEtaGroups());

            await seedAdsReportsOnConnect(resolvedCountry, {
                onStep: setStep,
                onCompleteStep: markStepComplete,
                onActiveSteps: setRunningSteps,
                runEtaUnit,
            });

            console.log("Ads sync complete, showing success popup");

            setDynamicProgress(100);
            setStepProgress((prev) => ({
                ...prev,
                active: false,
            }));

            setIsConnecting(false);
            setIsSyncModalDismissed(false);
            setIsOpen(true);
            setShowSuccessPopup(false);

            window.dispatchEvent(
                new CustomEvent("amazonAdsSyncSuccess", {
                    detail: {
                        message: "Amazon Ads data has been synced successfully.",
                    },
                })
            );

            setTimeout(async () => {
                try {
                    await onConnected?.();
                } catch { }

                window.location.reload();
            }, 1500);
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : "Amazon Ads connected, but ads sync failed.");
            setIsConnecting(false);
            finalizedRef.current = false;
            resetStepState();
        }
    };

    const checkStatusOnce = async () => {
        // backend: { status: "connected" } or { status: "not_connected" } etc.
        const s = await apiJson<{ status?: string }>(`/api/ads/status`, { method: "GET" });
        if (String(s?.status).toLowerCase() === "connected") {
            await finalizeConnection();
            return true;
        }
        return false;
    };

    const startPolling = () => {
        stopPolling();
        startedAtRef.current = Date.now();

        pollingRef.current = setInterval(async () => {
            try {
                // stop if popup closed by user
                if (popupRef.current && popupRef.current.closed) {
                    stopPolling();
                    setIsConnecting(false);
                    setError("Popup closed. Please try again.");
                    return;
                }

                // timeout
                if (Date.now() - startedAtRef.current > maxWaitMs) {
                    stopPolling();
                    setIsConnecting(false);
                    closePopup();
                    setError("Connection timed out. Please try again.");
                    return;
                }

                await checkStatusOnce();
            } catch {
                // ignore transient errors while polling
            }
        }, pollIntervalMs);
    };

    const handleAmazonAdsLogin = async () => {
        setError("");
        setMessage("");

        const token = getAuthToken();
        if (!token) {
            setError("You are not logged in. Please login again.");
            return;
        }

        setIsConnecting(true);

        try {
            // 1) if already connected -> finish
            try {
                const already = await checkStatusOnce();
                if (already) {
                    setIsConnecting(false);
                    return;
                }
            } catch {
                // ignore
            }

            // 2) open popup first (better chance to avoid popup blockers)
            const popup = window.open(
                "about:blank",
                "amazonAdsConnect",
                "width=720,height=820,menubar=no,toolbar=no,location=yes,status=no,scrollbars=yes,resizable=yes"
            );

            if (!popup) {
                // fallback: navigate in same tab
                const cu = await apiJson<{ url?: string }>(`/api/ads/connect_url`, { method: "GET" });
                const url = cu?.url;
                if (!url) throw new Error("No url returned from /api/ads/connect_url");
                window.location.assign(url);
                return;
            }

            popupRef.current = popup;

            // 3) get connect url and navigate popup
            const cu = await apiJson<{ url?: string }>(`/api/ads/connect_url`, { method: "GET" });
            const connectUrl = cu?.url;
            if (!connectUrl) throw new Error("No url returned from /api/ads/connect_url");

            popup.location.href = connectUrl;
            popup.focus();

            // 4) poll status
            startPolling();
        } catch (e: unknown) {
            setError(e instanceof Error ? e.message : "Error connecting to Amazon Ads.");
            stopPolling();
            closePopup();
        }
        // finally {
        //     setIsConnecting(false);
        // }
    };

    useEffect(() => {
        const onMsg = (e: MessageEvent) => {
            const d = e.data as {
                type?: string;
                ok?: boolean;
                error?: string;
            };
            if (!d || typeof d !== "object") return;
            if (d.type !== "amazon_ads_connected") return;

            if (d.ok) {
                finalizeConnection();
            } else {
                stopPolling();
                closePopup();
                setIsConnecting(false);
                setError(d.error || "Amazon Ads connection failed.");
            }
        };

        window.addEventListener("message", onMsg);
        return () => window.removeEventListener("message", onMsg);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);


    // Cleanup
    useEffect(() => {
        return () => {
            stopPolling();
            closePopup();
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    if (!isOpen && !showSuccessPopup && !isConnecting) return null;

    if (isConnecting && stepProgress.active && isSyncModalDismissed) {
        return null;
    }

    return (
        <div
            className="fixed inset-0 z-[99999] flex items-center justify-center bg-black/50 p-3 sm:p-4 md:p-6"
            role="dialog"
            aria-modal="true"
            onClick={handleCloseModal}
        >
            {isConnecting && stepProgress.active ? (
                <AdsSyncLoaderModal
                    open={true}
                    currentStep={currentStep}
                    activeSteps={activeSteps}
                    completedSteps={completedSteps}
                    dashboardSteps={dashboardSteps}
                    stepProgress={stepProgress}
                    progressPercentage={dynamicProgress}
                    estimatedTime={estimatedTimeText}
                    onDismiss={handleCloseModal}
                />
            ) : (
                <div
                    className="relative w-full max-w-md sm:max-w-lg md:max-w-xl bg-white rounded-2xl shadow-xl p-4 sm:p-6 md:p-8"
                    onClick={(e) => e.stopPropagation()}
                >
                    <div className="flex justify-center w-full mt-1 sm:mt-2">
                        <img
                            src={ICONS.amazonAdsLogo}
                            alt="Amazon Ads"
                            className="w-12 sm:w-14 md:w-44 mx-auto mb-2"
                        />
                    </div>

                    <PageBreadcrumb
                        pageTitle="Connect Amazon Ads"
                        align="center"
                        variant="table"
                        textSize="2xl"
                    />

                    <p className="text-center text-xs sm:text-sm md:text-base text-[#414042] font-bold mt-2 mb-4 sm:mb-5 md:mb-6 w-[90%] sm:w-[80%] m-auto">
                        Authorize access to your Amazon Ads account to view campaign performance and advertising spend insights.
                    </p>

                    <div className="w-full border-t border-gray-300 mb-4 sm:mb-6" />

                    <div className="rounded-lg border border-[#5EA68E26] bg-emerald-50/50 p-3 sm:p-4 mb-4 sm:mb-5">
                        <div className="flex items-center gap-3 mb-2 sm:mb-3">
                            <div className="flex items-center justify-center w-9 h-9 sm:w-10 sm:h-10 rounded-lg bg-[#D9D9D9]">
                                <FaLink size={16} color="#5EA68E" />
                            </div>

                            <div className="flex flex-col">
                                <h3 className="font-semibold text-[#414042] text-xs sm:text-sm md:text-base leading-tight">
                                    Link your Amazon Ads Account
                                </h3>

                                <p className="text-[10px] sm:text-xs md:text-sm text-[#5EA68E] mt-0.5">
                                    Setup in 30 seconds
                                </p>
                            </div>
                        </div>

                        <ul className="text-xs sm:text-sm text-[#414042] space-y-2 sm:space-y-3 mb-3 sm:mb-4 ml-3 sm:ml-4">
                            {[
                                "Automatic campaign data synchronization",
                                "Accurate, real-time performance metrics",
                                "Secure, encrypted authorization",
                            ].map((text) => (
                                <li key={text} className="flex items-start gap-2">
                                    <TiTick className="w-4 h-4 sm:w-5 sm:h-5 text-green-500 mt-[1px]" />
                                    <span>{text}</span>
                                </li>
                            ))}
                        </ul>

                        <Button
                            type="button"
                            variant="primary"
                            size="sm"
                            onClick={handleAmazonAdsLogin}
                            disabled={isConnecting}
                            className="w-full bg-blue-700"
                        >
                            <FaLink size={14} />

                            <span className="text-[#F8EDCE] text-xs sm:text-sm">
                                Connect
                            </span>
                        </Button>
                    </div>

                    <div className="w-full border-t border-gray-300 mb-4 sm:mb-5" />

                    <div className="flex items-center justify-center gap-2 text-[10px] sm:text-xs md:text-sm text-[#414042]">
                        <img
                            src={ICONS.secure}
                            alt="Secure"
                            className="w-3 h-3 sm:w-4 sm:h-4 opacity-70"
                        />

                        <span>Your credentials are encrypted and stored securely</span>
                    </div>

                    {error && (
                        <div className="mt-3 sm:mt-4 text-center text-xs sm:text-sm text-red-700 bg-red-50 border border-red-200 rounded-md p-2">
                            {error}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
