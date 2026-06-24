"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
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
    }
) {
    const onStep = hooks?.onStep;
    const onCompleteStep = hooks?.onCompleteStep;

    const start_date = getIstMonthStartISO();
    const end_date = getIstTodayISO();

    // STEP 1 — Sponsored Product
    onStep?.(1, "Sponsored Product", 15, "Starting Sponsored Product sync...");
    await postJson(`/api/ads/manager/sp_advertised_product_report`, {
        start_date,
        end_date,
        time_unit: "DAILY",
        countries: [country],
        return_excel: false,
    });
    onStep?.(1, "Sponsored Product", 100, "Sponsored Product synced");
    onCompleteStep?.(1);

    // STEP 2 — Sponsored Display
    onStep?.(2, "Sponsored Display", 15, "Starting Sponsored Display sync...");
    await postJson(`/api/ads/manager/sd_advertised_product_report/sync`, {
        start_date,
        end_date,
        time_unit: "DAILY",
        countries: [country],
        max_wait_seconds: 900,
        poll_every_seconds: 10,
    });
    onStep?.(2, "Sponsored Display", 100, "Sponsored Display synced");
    onCompleteStep?.(2);

    // STEP 3 — Sponsored Brand + Monthly Sync
    onStep?.(3, "Sponsored Brand", 20, "Starting Sponsored Brand sync...");
    await postJson(`/api/ads/manager/sb_keyword_report`, {
        start_date,
        end_date,
        time_unit: "SUMMARY",
        countries: [country],
        return_excel: false,
    });

    onStep?.(3, "Sponsored Brand", 65, "Building monthly SP/SD summary...");

    const { month, year } = getCurrentMonthYearIST();

    await postJson(`/api/ads/monthly_sp_sd_to_db`, {
        month: monthToNumber(month),
        year,
        country,
        include: ["SP", "SD", "SB"],
    });

    onStep?.(3, "Sponsored Brand", 100, "Sponsored Brand and monthly sync complete");
    onCompleteStep?.(3);
}

const AdsSyncLoaderModal = React.memo(function AdsSyncLoaderModal({
    open,
    currentStep,
    completedSteps,
    dashboardSteps,
    stepProgress,
    loadingStartedAt,
    estimatedSecondsMap,
    onDismiss,
}: {
    open: boolean;
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
    onDismiss?: () => void;
}) {
    const [timerNow, setTimerNow] = useState(Date.now());

    useEffect(() => {
        if (!open || !stepProgress.active || !loadingStartedAt) return;

        const interval = setInterval(() => {
            setTimerNow(Date.now());
        }, 1000);

        return () => clearInterval(interval);
    }, [open, stepProgress.active, loadingStartedAt]);

    const totalEstimatedSeconds = useMemo(() => {
        return dashboardSteps.reduce((sum, step) => {
            return sum + (estimatedSecondsMap[step.num] ?? 20);
        }, 0);
    }, [dashboardSteps, estimatedSecondsMap]);

    const estimatedTime = useMemo(() => {
        if (!stepProgress.active || !loadingStartedAt) return "00:00";

        const elapsedSec = Math.floor((timerNow - loadingStartedAt) / 1000);
        const remainingSec = Math.max(totalEstimatedSeconds - elapsedSec, 0);

        const mm = String(Math.floor(remainingSec / 60)).padStart(2, "0");
        const ss = String(remainingSec % 60).padStart(2, "0");

        return `${mm}:${ss}`;
    }, [timerNow, loadingStartedAt, stepProgress.active, totalEstimatedSeconds]);

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
                        {stepProgress.percentage}%
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

                <div className="mt-5 pt-4 border-t border-slate-100 flex items-center justify-between">
                    <p className="text-xs text-slate-400 truncate">
                        {stepProgress.detail || "Initialising sync…"}
                    </p>

                    <div className="flex items-center gap-1 px-2 py-1 bg-slate-100 rounded-full mx-3">
                        <span className="text-xs text-slate-400">Estimated:</span>
                        <span className="text-xs font-medium text-slate-600">
                            {estimatedTime}
                        </span>
                    </div>

                    <span className="text-xs text-slate-400 shrink-0">
                        Step {Math.min(currentStep, dashboardSteps.length)} of {dashboardSteps.length}
                    </span>
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
    const [completedSteps, setCompletedSteps] = useState<Set<number>>(new Set());
    const [loadingStartedAt, setLoadingStartedAt] = useState<number | null>(null);

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

    const markStepComplete = (step: number) => {
        setCompletedSteps((prev) => new Set([...prev, step]));
    };

    const setStep = (
        step: number,
        label: string,
        percentage: number = 0,
        detail?: string
    ) => {
        setCurrentStep(step);
        setStepProgress({
            active: true,
            label,
            percentage: Math.min(100, Math.max(0, percentage)),
            detail,
        });
    };

    const resetStepState = () => {
        setCurrentStep(0);
        setCompletedSteps(new Set());
        setLoadingStartedAt(null);
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

    const STEP_ESTIMATED_SECONDS: Record<number, number> = {
        1: 200,
        2: 200,
        3: 200,
    };


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
            setLoadingStartedAt(Date.now());
            setCurrentStep(1);
            setCompletedSteps(new Set());
            setStepProgress({
                active: true,
                label: "",
                percentage: 0,
                detail: "",
            });

            await seedAdsReportsOnConnect(resolvedCountry, {
                onStep: setStep,
                onCompleteStep: markStepComplete,
            });

            console.log("Ads sync complete, showing success popup");

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
                    completedSteps={completedSteps}
                    dashboardSteps={dashboardSteps}
                    stepProgress={stepProgress}
                    loadingStartedAt={loadingStartedAt}
                    estimatedSecondsMap={STEP_ESTIMATED_SECONDS}
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
