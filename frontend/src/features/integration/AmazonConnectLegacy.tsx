"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import AmazonFinancialDashboard from "./AmazonFinancialDashboard";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { buildCountryMarketplaceMap } from "@/lib/utils/countryMarketplace";



const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://13.126.252.94:5000";
const CALLBACK_ORIGIN = process.env.NEXT_PUBLIC_CALLBACK_ORIGIN || ""; // e.g. https://your-ngrok.ngrok-free.app

const getAuthToken = () =>
  typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
const getRefreshToken = () =>
  typeof window !== "undefined" ? localStorage.getItem("amazonRefreshToken") : null;

function getOrigin(url: string) {
  try {
    const u = new URL(url);
    return `${u.protocol}//${u.host}`;
  } catch {
    return null;
  }
}

const apiOrigin = getOrigin(API_BASE);
const callbackOrigin = CALLBACK_ORIGIN ? getOrigin(CALLBACK_ORIGIN) : null;

async function api(path: string, options: RequestInit = {}) {
  const token = getAuthToken();
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok)
    throw new Error(
      (data as any)?.error || (data as any)?.message || `HTTP ${res.status}`
    );
  return data;
}



const MARKETPLACE_BY_REGION: Record<string, string> = {
  "us-east-1": "ATVPDKIKX0DER",
  "eu-west-1": "A1F83G8C2ARO7P",
  "ap-southeast-1": "A1VC38T7YXB528",
};

const REGION_LABELS: Record<string, string> = {
  "us-east-1": "US",
  "eu-west-1": "UK",
  "ap-southeast-1": "Canada",
};

const ICONS = {
  amazonLogo: "/amazon.png",
  back: "/BackArrow.png",
  shield: "/secure.png",
  caret: "/caret-down.svg",
  link: "/link.png",
};

type Props = {
  onClose?: () => void;
  onConnected?: (refreshToken?: string) => void;
};

export default function AmazonConnectLegacy({ onClose, onConnected }: Props) {
  const [region, setRegion] = useState("eu-west-1");
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [showDashboard, setShowDashboard] = useState(false);

  const [isStep3Unlocked, setIsStep3Unlocked] = useState(!!getRefreshToken());

  const popupRef = useRef<Window | null>(null);
  const pollingRef = useRef<number | null>(null);
  const loginStateRef = useRef<string | null>(null);

  const { data: user } = useGetUserDataQuery();

  const countryMarketplaceMap = useMemo(() => {
    if (!user?.country || !user?.marketplace_id) return {};
    return {
      [user.country]: user.marketplace_id,
    };
  }, [user]);

  const country =
  REGION_LABELS[region]?.toLowerCase(); // "uk" | "us"

const marketplaceId = useMemo(() => {
  if (!country) return undefined;
  return countryMarketplaceMap[country];
}, [country, countryMarketplaceMap]);

  const stopPolling = () => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  };

  const closePopup = () => {
    try {
      if (popupRef.current && !popupRef.current.closed) popupRef.current.close();
    } catch {}
    popupRef.current = null;
  };

  const unlockStep3 = (refreshToken: string) => {
    localStorage.setItem("amazonRefreshToken", refreshToken);
    localStorage.setItem("amazonRefreshTokenStoredAt", String(Date.now()));
    localStorage.setItem("amazonMarketplaceRegion", region);
    localStorage.setItem("amazonMarketplaceId", marketplaceId);

    
    localStorage.setItem("amazonIntegrationStep3Unlocked", "true");
    setIsStep3Unlocked(true);

    window.dispatchEvent(
      new CustomEvent("amazon:tokenSaved", {
        detail: {
          refreshToken,
          region,
          marketplaceId,
          unlockedStep: 3,
          at: Date.now(),
        },
      })
    );

    const t = refreshToken;
    const masked = t.length > 14 ? `${t.slice(0, 6)}…${t.slice(-4)}` : "********";
    console.log("Stored Amazon refresh token (masked):", masked);
  };

  // const handleStatusPollWinAndRoute = async () => {
  //   try {
  //     const qs = new URLSearchParams({ region }).toString();
  //     const s = await api(`/amazon_api/status?${qs}`);

  //     if ((s as any)?.success) {
  //       stopPolling();
  //       closePopup();
  //       setMessage("Connected to Amazon ✅");

  //       localStorage.setItem("amazonConnected", "true");
  //       if ((s as any)?.payload) {
  //         localStorage.setItem(
  //           "amazonParticipations",
  //           JSON.stringify((s as any).payload, null, 2)
  //         );
  //       }

  //       setShowDashboard(true);
  //       onConnected?.();
  //     }
  //   } catch (err: any) {
  //     console.warn("Amazon status poll failed:", err.message);
  //   }
  // };

const handleStatusPollWinAndRoute = async () => {
  try {
    const qs = new URLSearchParams({ region }).toString();
    const s = await api(`/amazon_api/status?${qs}`) as any;

    const hasRefreshToken = !!s?.has_refresh_token;

    if (s?.success && hasRefreshToken) {
      stopPolling();
      closePopup();
      setMessage("Connected to Amazon ✅");

      localStorage.setItem("amazonConnected", "true");
      if (s?.payload) {
        localStorage.setItem(
          "amazonParticipations",
          JSON.stringify(s.payload, null, 2)
        );
      }

      setShowDashboard(true);
      onConnected?.();
    } else {
      // s.status might be "no_record" | "pending" | "sp_api_error"
      // You can optionally show different messages based on that.
    }
  } catch (err: any) {
    console.warn("Amazon status poll failed:", err.message);
  }
};


  const handleAmazonLogin = async () => {
    setError("");
    setMessage("");
    setIsConnecting(true);

    try {
      const qs = new URLSearchParams({
        region,
        marketplace_id: marketplaceId,
      }).toString();
      const data = await api(`/amazon_api/login?${qs}`);

      loginStateRef.current = (data as any)?.state || null;

      const msg = String((data as any)?.message || "").toLowerCase();
      if (
        (data as any)?.success &&
        (msg.includes("refresh token saved") || msg.includes("refresh"))
      ) {
        await handleStatusPollWinAndRoute();
        return;
      }

      const authUrl = (data as any)?.auth_url;
      if (!authUrl)
        throw new Error(
          (data as any)?.error ||
            (data as any)?.message ||
            "Failed to get Amazon login URL"
        );

      const w = window.open(authUrl, "amazon_oauth", "width=720,height=800");
      if (!w) throw new Error("Popup blocked. Please allow popups for this site.");
      popupRef.current = w;

      stopPolling();
      pollingRef.current = window.setInterval(handleStatusPollWinAndRoute, 2000) as unknown as number;
    } catch (e: any) {
      setError(e.message || "Error connecting to Amazon login.");
      stopPolling();
      closePopup();
    } finally {
      setIsConnecting(false);
    }
  };

  useEffect(() => {
    const onMessage = async (event: MessageEvent) => {
      const { data } = event || {};
      if (!data || typeof data !== "object") return;

      const allowed = new Set(
        [apiOrigin, callbackOrigin, window.location.origin].filter(Boolean)
      );
      if (allowed.size && !allowed.has(event.origin)) {
        console.warn("Ignored message from unknown origin:", event.origin);
        return;
      }

      if ((data as any).type === "amazon_oauth_success") {
        try {
          if (
            loginStateRef.current &&
            (data as any).state &&
            (data as any).state !== loginStateRef.current
          ) {
            setError("State mismatch during Amazon OAuth.");
            stopPolling();
            closePopup();
            return;
          }

          if ((data as any).refresh_token) {
            unlockStep3((data as any).refresh_token);
          }

          await handleStatusPollWinAndRoute();
        } catch (e: any) {
          setError(e.message || "Post-auth follow-up failed.");
        }
      }

      if ((data as any).type === "amazon_oauth_error") {
        stopPolling();
        closePopup();
        setError((data as any).error || "Amazon connection failed.");
      }
    };

    window.addEventListener("message", onMessage);
    return () => {
      window.removeEventListener("message", onMessage);
      stopPolling();
      closePopup();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [region, marketplaceId]);

  // if (showDashboard) {
  //   const country = REGION_LABELS[region];
  //   const refreshToken = getRefreshToken() || undefined;

  //   return (
  //     <AmazonFinancialDashboard
  //       country={country}
  //       // @ts-expect-error keeping prop for your outer flow; safe to ignore
  //       refreshToken={refreshToken}
  //       isStep3Unlocked={isStep3Unlocked}
  //       onClose={onClose}
  //     />
  //   );
  // }

  if (showDashboard) {
  const country = REGION_LABELS[region];
  const refreshToken = getRefreshToken() || undefined;

  if (!refreshToken) {
    // Defensive: if somehow we toggled showDashboard but don't have a token, don't render it.
    return null;
  }

  return (
    <AmazonFinancialDashboard
      country={country}
      // @ts-expect-error keeping prop for your outer flow; safe to ignore
      refreshToken={refreshToken}
      isStep3Unlocked={isStep3Unlocked}
      onClose={onClose}
    />
  );
}


  return (
    <div
      className="fixed inset-0 z-99999 flex items-center justify-center bg-black/50 p-3 sm:p-4 md:p-6"
      role="dialog"
      aria-modal="true"
      aria-labelledby="amazon-connect-title"
       onClick={onClose} 
    >
      <div className="relative w-11/12 sm:w-full max-w-sm sm:max-w-md md:max-w-lg lg:max-w-xl rounded-xl bg-white shadow-xl"
      onClick={(e) => e.stopPropagation()}
      >
        {/* <button
          type="button"
          onClick={onClose}
          className="absolute left-2 top-2 sm:left-3 sm:top-3 inline-flex h-9 w-9 sm:h-10 sm:w-10 items-center justify-center rounded-full hover:bg-gray-100 focus:outline-none"
          aria-label="Close"
          title="Close"
        >
          <img src={ICONS.back} alt="Back" className="h-6 w-6 sm:h-8 sm:w-8" />
        </button> */}

        <div className="max-h-[90vh] overflow-y-auto px-4 sm:px-6 md:px-8 pb-6 pt-5 sm:pt-8">
          {/* <div className="mx-auto mb-3 sm:mb-4 flex h-10 w-10 items-center justify-center">
            <img src={ICONS.amazonLogo} alt="Amazon" className="h-10 w-10 sm:h-12 sm:w-12 mx-auto" />
          </div> */}

          <div className="flex items-center justify-between relative">
          <div className="flex justify-center w-full mt-1 sm:mt-2">
            <img
              src={ICONS.amazonLogo}
              alt="Amazon"
              className="w-12 h-12 sm:w-14 sm:h-14 md:w-16 md:h-16 mx-auto"
            />
          </div>
        </div>

          {/* <h2
            id="amazon-connect-title"
            className="mb-1 text-center text-2xl sm:text-3xl md:text-4xl font-semibold text-[#5EA68E]"
          >
            Connect Amazon Account
          </h2> */}
          <PageBreadcrumb pageTitle="Connect Amazon Account" align="center" variant="table" textSize="2xl" />
          <p className="mb-5 text-center text-xs sm:text-sm md:text-base text-[#414042]">
            Link your Amazon Seller Central to sync your sales data
          </p>

          {message && (
            <div className="mb-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs sm:text-sm text-emerald-700">
              {message}
            </div>
          )}
          {error && (
            <div className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs sm:text-sm text-red-700">
              {error}
            </div>
          )}

          <div className="mb-5 rounded-md border border-[#5EA68E] border-l-[5px] bg-[#D9D9D94D] px-3 py-3 sm:py-4">
            <div className="flex items-center gap-2">
              <img src={ICONS.shield} alt="Secure" className="h-4 w-4 sm:h-5 sm:w-5" />
              <span className="text-xs sm:text-sm font-medium text-[#5EA68E]">
                Secure Connection
              </span>
            </div>
            <p className="mt-1 ml-1 text-xs sm:text-sm text-[#414042]">
              Your Amazon credentials are encrypted and stored securely. We only access data
              necessary for analytics.
            </p>
          </div>

          <label className="mb-1 block text-xs sm:text-sm font-semibold text-charcoal-500">
            Select your marketplace <span className="text-rose-500">*</span>
          </label>
          <div className="relative">
            <select
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              className="mb-4 w-full rounded-md border border-gray-300 bg-white px-3 py-2 pr-10 text-sm md:text-base text-gray-800 outline-none transition focus:border-green-500 focus:ring-2 focus:ring-green-500"
            >
              <option value="eu-west-1">{REGION_LABELS["eu-west-1"]}</option>
              <option value="us-east-1">{REGION_LABELS["us-east-1"]}</option>
              <option value="ap-southeast-1">{REGION_LABELS["ap-southeast-1"]}</option>
            </select>
            <div className="pointer-events-none absolute inset-y-0 right-3 flex items-center">
              <img src={ICONS.caret} alt="" className="h-4 w-4 opacity-70" />
            </div>
          </div>

          <div className="mt-2 flex w-full justify-center">
            <Button
              variant="primary"
              size="sm"
              onClick={handleAmazonLogin}
              disabled={isConnecting}
              className={` w-full
                ${isConnecting ? "bg-blue-700 cursor-not-allowed" : "bg-blue-700"}`}
            >
              <img src={ICONS.link} alt="" className="h-5 w-5 sm:h-6 sm:w-6 opacity-90" />
              {isConnecting ? "Working..." : "Connect"}
            </Button>
          </div>

          {/* <div className="mt-4 text-sm text-gray-700">
            <span
              className={`inline-flex items-center gap-2 ${
                isStep3Unlocked ? "text-emerald-600" : "text-gray-500"
              }`}
            >
              {isStep3Unlocked ? "✅" : "⬜️"} Step 3: Amazon Integration Unlocked
            </span>
          </div> */}
        </div>
      </div>
    </div>
  );
}
