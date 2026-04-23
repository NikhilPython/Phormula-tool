"use client";

import React, { useEffect, useRef, useState } from "react";
import { FaLink } from "react-icons/fa";
import { TiTick } from "react-icons/ti";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import { usePlatform } from "@/components/context/PlatformContext";

const API_BASE =
    process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const getAuthToken = () =>
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

async function apiJson(path: string, options: RequestInit = {}) {
    const token = getAuthToken();
    const res = await fetch(`${API_BASE}${path}`, {
        ...options,
        headers: {
            Accept: "application/json",
            ...(options.headers || {}),
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
        throw new Error(
            (data as any)?.error || (data as any)?.message || `HTTP ${res.status}`
        );
    }
    return data as any;
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

async function postJson(path: string, body: any) {
    return apiJson(path, {
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
        time_unit: "SUMMARY",
        countries: [country],
        return_excel: false,
    });

    localStorage.setItem(storageKey, "1");
}

async function seedAdsReportsOnConnect(country: "UK" | "US" | "CA") {
    const start_date = getIstMonthStartISO();
    const end_date = getIstTodayISO();

    await ensureSpReportOncePerDay(country);

    await postJson(`/api/ads/manager/sb_keyword_report`, {
        start_date,
        end_date,
        time_unit: "SUMMARY",
        countries: ["UK"],
        return_excel: false,
    });

    if (country === "UK" || country === "US") {
        await postJson(`/api/ads/manager/sd_advertised_product_report/sync`, {
            start_date,
            end_date,
            time_unit: "SUMMARY",
            countries: [country],
            max_wait_seconds: 900,
            poll_every_seconds: 10,
        });
    }

    const { month, year } = getCurrentMonthYearIST();
    const include =
        country === "UK" || country === "US"
            ? ["SP", "SD"]
            : ["SP"];

    console.log("PAYLOAD:", {
        month: monthToNumber(month),
        year,
        country,
        include,
    });


    await postJson(`/api/ads/monthly_sp_sd_to_db`, {
        month: monthToNumber(month),
        year,
        country: country || "UK",
        include,
    });
}

type Props = {
    onClose?: () => void;
    onConnected?: () => void | Promise<void>;
    redirectUrl?: string;
    pollIntervalMs?: number;
    maxWaitMs?: number;
};

export default function AmazonAdsConnect({
    onClose,
    onConnected,
    redirectUrl,
    pollIntervalMs = 1500,
    maxWaitMs = 2 * 60 * 1000,
}: Props) {
    const { platform } = usePlatform();

    const resolvedCountry: "UK" | "US" | "CA" =
        platform === "amazon-us" ? "US" :
            platform === "amazon-ca" ? "CA" :
                "UK";
    const [isConnecting, setIsConnecting] = useState(false);
    const [error, setError] = useState("");
    const [message, setMessage] = useState("");

    const popupRef = useRef<Window | null>(null);
    const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
    const startedAtRef = useRef<number>(0);

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

    // const finalizeConnection = async () => {
    //     if (finalizedRef.current) return;
    //     finalizedRef.current = true;

    //     setMessage("Connected to Amazon Ads ✅");
    //     setError("");

    //     stopPolling();
    //     closePopup();

    //     try {
    //         await onConnected?.();
    //     } catch { }

    //     // ✅ This is what you wanted: page refresh immediately after connect
    //     window.location.reload();
    // };

    const finalizeConnection = async () => {
        if (finalizedRef.current) return;
        finalizedRef.current = true;

        setMessage("Connected to Amazon Ads ✅");
        setError("");

        stopPolling();
        closePopup();

        try {
            setIsConnecting(true);

            await seedAdsReportsOnConnect(resolvedCountry);
            await onConnected?.();
        } catch (e: any) {
            setError(e?.message || "Amazon Ads connected, but ads sync failed.");
            setIsConnecting(false);
            finalizedRef.current = false;
            return;
        }

        window.location.reload();
    };

    const checkStatusOnce = async () => {
        // backend: { status: "connected" } or { status: "not_connected" } etc.
        const s = await apiJson(`/api/ads/status`, { method: "GET" });
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
                const cu = await apiJson(`/api/ads/connect_url`, { method: "GET" });
                const url = cu?.url;
                if (!url) throw new Error("No url returned from /api/ads/connect_url");
                window.location.assign(url);
                return;
            }

            popupRef.current = popup;

            // 3) get connect url and navigate popup
            const cu = await apiJson(`/api/ads/connect_url`, { method: "GET" });
            const connectUrl = cu?.url;
            if (!connectUrl) throw new Error("No url returned from /api/ads/connect_url");

            popup.location.href = connectUrl;
            popup.focus();

            // 4) poll status
            startPolling();
        } catch (e: any) {
            setError(e?.message || "Error connecting to Amazon Ads.");
            stopPolling();
            closePopup();
        }
        // finally {
        //     setIsConnecting(false);
        // }
    };

    useEffect(() => {
        const onMsg = (e: MessageEvent) => {
            const d: any = e?.data;
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

    return (
        <div
            className="fixed inset-0 z-[99999] flex items-center justify-center bg-black/50 p-3 sm:p-4 md:p-6"
            role="dialog"
            aria-modal="true"
            onClick={onClose}
        >
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
                            // "No manual work required",
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
                        className={`w-full ${isConnecting ? "bg-blue-700 cursor-not-allowed" : "bg-blue-700"
                            }`}
                    >
                        <FaLink size={14} />
                        <span className="text-[#F8EDCE] text-xs sm:text-sm">
                            {isConnecting ? "Connecting..." : "Connect"}
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

                {/* {message && (
                    <div className="mt-3 sm:mt-4 text-center text-xs sm:text-sm text-emerald-700 bg-emerald-50 border border-emerald-200 rounded-md p-2">
                        {message}
                    </div>
                )} */}
                {error && (
                    <div className="mt-3 sm:mt-4 text-center text-xs sm:text-sm text-red-700 bg-red-50 border border-red-200 rounded-md p-2">
                        {error}
                    </div>
                )}
            </div>
        </div>
    );
}
