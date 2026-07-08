"use client";

import React, { useEffect, useRef, useState } from "react";
import AmazonFinancialDashboard from "./AmazonFinancialDashboard";
import Button from "@/components/ui/button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import { useSubmitSelectFormMutation } from "@/lib/api/onboardingApi";
import { FaLink, FaLock } from "react-icons/fa6";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";
const CALLBACK_ORIGIN = process.env.NEXT_PUBLIC_CALLBACK_ORIGIN || "";

const getAuthToken = () =>
  typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

const isAmazonConnected = (region?: string) =>
  typeof window !== "undefined"
    ? localStorage.getItem(region ? `amazonConnected_${region}` : "amazonConnected") ===
      "true"
    : false;

const getMarketplaceId = (region?: string) =>
  typeof window !== "undefined"
    ? region
      ? localStorage.getItem(`amazonMarketplaceId_${region}`)
      : localStorage.getItem("amazonMarketplaceId")
    : null;

const REGION_LABELS: Record<string, string> = {
  "us-east-1": "US",
  "eu-west-1": "UK",
  "ap-southeast-1": "Canada",
};

const REGION_TO_COUNTRY_NAME: Record<string, string> = {
  "us-east-1": "us",
  "eu-west-1": "uk",
  "ap-southeast-1": "canada",
};

const REGION_TO_MARKETPLACE_ID: Record<string, string> = {
  "us-east-1": "ATVPDKIKX0DER",
  "eu-west-1": "A1F83G8C2ARO7P",
  "ap-southeast-1": "A2EUQ1WTGCTBG2",
};

function saveAmazonConnectionLocal(region: string, marketplaceId?: string) {
  if (typeof window === "undefined") return;

  localStorage.setItem("amazonConnected", "true");
  localStorage.setItem(`amazonConnected_${region}`, "true");
  localStorage.setItem(`amazonConnectedAt_${region}`, String(Date.now()));

  localStorage.setItem(`amazonMarketplaceRegion_${region}`, region);
  localStorage.setItem("amazonMarketplaceRegion", region);

  localStorage.setItem(
    "amazonSelectedCountry",
    REGION_TO_COUNTRY_NAME[region] || ""
  );

  localStorage.setItem(
    `amazonSelectedCountry_${region}`,
    REGION_TO_COUNTRY_NAME[region] || ""
  );

  if (marketplaceId) {
    localStorage.setItem(`amazonMarketplaceId_${region}`, marketplaceId);
    localStorage.setItem("amazonMarketplaceId", marketplaceId);
  }
}

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

  if (!res.ok) {
    throw new Error(
      (data as any)?.error ||
        (data as any)?.message ||
        (data as any)?.detail ||
        `HTTP ${res.status}`
    );
  }

  return data;
}

type Props = {
  onClose?: () => void;
  onConnected?: () => void;
};

export default function AmazonConnectLegacy({ onClose, onConnected }: Props) {
  const [region, setRegion] = useState("eu-west-1");
  const [isConnecting, setIsConnecting] = useState(false);
  const [isSavingProfile, setIsSavingProfile] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [showDashboard, setShowDashboard] = useState(false);
  const [selectedMarketplaceId, setSelectedMarketplaceId] = useState("");

  const [stockUnit, setStockUnit] = useState("");
  const [transitTime, setTransitTime] = useState("");
  const [fieldErrors, setFieldErrors] = useState<{
    stockUnit?: string;
    transitTime?: string;
  }>({});

  const [isStep3Unlocked, setIsStep3Unlocked] = useState(
    isAmazonConnected(region)
  );

  const [resolvedMarketplaceId, setResolvedMarketplaceId] = useState(
    getMarketplaceId(region) || REGION_TO_MARKETPLACE_ID[region] || ""
  );

  const [showProfileFields, setShowProfileFields] = useState(false);

  const popupRef = useRef<Window | null>(null);
  const pollingRef = useRef<number | null>(null);
  const loginStateRef = useRef<string | null>(null);

  const [submitSelectForm] = useSubmitSelectFormMutation();

  const country = REGION_TO_COUNTRY_NAME[region] || "";

  const stopPolling = () => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
  };

  const closePopup = () => {
    try {
      if (popupRef.current && !popupRef.current.closed) {
        popupRef.current.close();
      }
    } catch {
      // ignore
    }

    popupRef.current = null;
  };

  const resetProfileStateForRegionChange = (nextRegion: string) => {
    setShowProfileFields(false);
    setStockUnit("");
    setTransitTime("");
    setFieldErrors({});
    setMessage("");
    setError("");
    setSelectedMarketplaceId("");

    const nextMarketplaceId =
      getMarketplaceId(nextRegion) || REGION_TO_MARKETPLACE_ID[nextRegion] || "";

    setResolvedMarketplaceId(nextMarketplaceId);
  };

  const saveMarketplaceSelection = async (
    selectedCountry: string,
    marketplaceId?: string
  ) => {
    const canonicalMarketplaceId =
      REGION_TO_MARKETPLACE_ID[region] || marketplaceId || "";

    if (!selectedCountry || !canonicalMarketplaceId) return;

    try {
      await submitSelectForm({
        country: selectedCountry,
        marketplace_id: canonicalMarketplaceId,
      }).unwrap();

      const finalMarketplaceId =
        REGION_TO_MARKETPLACE_ID[region] ||
        selectedMarketplaceId ||
        resolvedMarketplaceId ||
        getMarketplaceId(region) ||
        "";

      setSelectedMarketplaceId(finalMarketplaceId);
      setResolvedMarketplaceId(finalMarketplaceId);

      localStorage.setItem(`amazonMarketplaceId_${region}`, finalMarketplaceId);
      localStorage.setItem("amazonMarketplaceId", finalMarketplaceId);
    } catch (err) {
      console.error("submitSelectForm marketplace save error:", err);
    }
  };

  const unlockStep3 = (marketplaceId?: string) => {
    saveAmazonConnectionLocal(region, marketplaceId);

    localStorage.setItem("amazonIntegrationStep3Unlocked", "true");
    setIsStep3Unlocked(true);

    window.dispatchEvent(
      new CustomEvent("amazon:connected", {
        detail: {
          region,
          marketplaceId: marketplaceId || "",
          unlockedStep: 3,
          at: Date.now(),
        },
      })
    );
  };

  const validateProfileFields = () => {
    const nextErrors: { stockUnit?: string; transitTime?: string } = {};

    const parsedStockUnit = Number(stockUnit);
    const parsedTransitTime = Number(transitTime);

    if (
      stockUnit === "" ||
      Number.isNaN(parsedStockUnit) ||
      parsedStockUnit < 0
    ) {
      nextErrors.stockUnit = "Stock unit must be a non-negative integer";
    } else if (!Number.isInteger(parsedStockUnit)) {
      nextErrors.stockUnit = "Stock unit must be an integer";
    }

    if (
      transitTime === "" ||
      Number.isNaN(parsedTransitTime) ||
      parsedTransitTime <= 0
    ) {
      nextErrors.transitTime = "Transit time must be a positive integer";
    } else if (!Number.isInteger(parsedTransitTime)) {
      nextErrors.transitTime = "Transit time must be an integer";
    }

    setFieldErrors(nextErrors);

    return {
      isValid: Object.keys(nextErrors).length === 0,
      parsedStockUnit,
      parsedTransitTime,
    };
  };

  const loadCountryProfile = async (
    selectedCountry: string,
    marketplaceId: string
  ) => {
    if (!selectedCountry || !marketplaceId) return;

    try {
      const qs = new URLSearchParams({
        country: selectedCountry,
        marketplace: marketplaceId,
      }).toString();

      const data = (await api(`/country-profile?${qs}`)) as any;

      if (data?.exists && data?.profile) {
        setStockUnit(String(data.profile.stock_unit ?? ""));
        setTransitTime(String(data.profile.transit_time ?? ""));
      } else {
        setStockUnit("");
        setTransitTime("");
      }
    } catch (err) {
      console.error("Failed to load country profile:", err);
      setStockUnit("");
      setTransitTime("");
    }
  };

  const handleStatusPollWinAndRoute = async () => {
    try {
      const qs = new URLSearchParams({ region }).toString();
      const s = (await api(`/amazon_api/status?${qs}`)) as any;

      const hasRefreshToken = !!s?.has_refresh_token;

      if (s?.success && hasRefreshToken) {
        const payload = Array.isArray(s?.payload) ? s.payload : [];

        const marketplaceIdFromStatus = REGION_TO_MARKETPLACE_ID[region] || "";

        stopPolling();
        closePopup();

        localStorage.setItem(
          "amazonParticipations",
          JSON.stringify(payload, null, 2)
        );

        setResolvedMarketplaceId(marketplaceIdFromStatus);
        setSelectedMarketplaceId(marketplaceIdFromStatus);
        setShowProfileFields(true);
        setMessage("Connected to Amazon ✅. Please enter stock unit and transit time.");

        if (!marketplaceIdFromStatus) {
          setError(
            "Amazon connected, but marketplace ID could not be resolved for the selected country."
          );
          return;
        }

        localStorage.setItem(`amazonOAuthConnected_${region}`, "true");
        localStorage.setItem(`amazonMarketplaceId_${region}`, marketplaceIdFromStatus);
        localStorage.setItem("amazonMarketplaceId", marketplaceIdFromStatus);

        await loadCountryProfile(country, marketplaceIdFromStatus);

        return;
      }
    } catch (err: any) {
      console.warn("Amazon status poll failed:", err.message);
    }
  };

  const handleAmazonLogin = async () => {
    setError("");
    setMessage("");
    setFieldErrors({});
    setIsConnecting(true);

    try {
      const qs = new URLSearchParams({ region }).toString();
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

      if (!authUrl) {
        throw new Error(
          (data as any)?.error ||
            (data as any)?.message ||
            "Failed to get Amazon login URL"
        );
      }

      const w = window.open(authUrl, "amazon_oauth", "width=720,height=800");

      if (!w) {
        throw new Error("Popup blocked. Please allow popups for this site.");
      }

      popupRef.current = w;

      stopPolling();

      pollingRef.current = window.setInterval(
        handleStatusPollWinAndRoute,
        2000
      ) as unknown as number;
    } catch (e: any) {
      setError(e.message || "Error connecting to Amazon login.");
      stopPolling();
      closePopup();
    } finally {
      setIsConnecting(false);
    }
  };

  const handleSaveCountryProfile = async () => {
    setError("");
    setMessage("");

    if (!country) {
      setError("Country is missing.");
      return;
    }

    const finalMarketplaceId =
      selectedMarketplaceId ||
      resolvedMarketplaceId ||
      getMarketplaceId(region) ||
      REGION_TO_MARKETPLACE_ID[region] ||
      "";

    if (!finalMarketplaceId) {
      setError("Marketplace ID is not available yet.");
      return;
    }

    const { isValid, parsedStockUnit, parsedTransitTime } =
      validateProfileFields();

    if (!isValid) return;

    setIsSavingProfile(true);

    try {
      await api("/country-profile", {
        method: "POST",
        body: JSON.stringify({
          country,
          marketplace: finalMarketplaceId,
          stock_unit: parsedStockUnit,
          transit_time: parsedTransitTime,
        }),
      });

      await saveMarketplaceSelection(country, finalMarketplaceId);

      unlockStep3(finalMarketplaceId);

      setMessage("Country profile saved successfully ✅");
      setShowDashboard(true);
      onConnected?.();
    } catch (e: any) {
      setError(e.message || "Failed to save country profile.");
    } finally {
      setIsSavingProfile(false);
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
  }, [region, country]);

  useEffect(() => {
    resetProfileStateForRegionChange(region);
    setIsStep3Unlocked(isAmazonConnected(region));
  }, [region]);

  if (showDashboard) {
    return (
      <AmazonFinancialDashboard
        country={country}
        region={region}
        marketplaceId={
          REGION_TO_MARKETPLACE_ID[region] ||
          selectedMarketplaceId ||
          resolvedMarketplaceId
        }
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
      <div
        className="relative w-11/12 sm:w-full max-w-sm sm:max-w-md md:max-w-lg lg:max-w-xl rounded-xl bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="max-h-[90vh] overflow-y-auto px-4 sm:px-6 md:px-8 pb-6 pt-5 sm:pt-8">
          <div className="flex items-center justify-between relative">
            <div className="flex justify-center w-full mt-1 sm:mt-2">
              <img
                src="/amazon.png"
                alt="Amazon"
                className="w-12 h-12 sm:w-14 sm:h-14 md:w-16 md:h-16 mx-auto"
              />
            </div>
          </div>

          <PageBreadcrumb
            pageTitle="Connect Amazon Account"
            align="center"
            variant="table"
            textSize="2xl"
          />

          <p className="mb-5 text-center text-xs sm:text-sm md:text-base text-[#414042]">
            Link your Amazon Seller Central to sync your sales data
          </p>

          {error && (
            <div className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs sm:text-sm text-red-700">
              {error}
            </div>
          )}

          {message && (
            <div className="mb-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs sm:text-sm text-green-700">
              {message}
            </div>
          )}

          <div className="mb-5 rounded-md border border-[#5EA68E] border-l-[5px] bg-[#D9D9D94D] px-3 py-3 sm:py-4">
            <div className="flex items-center gap-2">
              <div className="flex items-center justify-center w-4 h-4 rounded-full bg-[#5EA68E]">
                <FaLock className="text-white text-[8px]" />
              </div>

              <span className="text-xs sm:text-sm font-medium text-[#5EA68E]">
                Secure Connection
              </span>
            </div>

            <p className="mt-1 ml-1 text-xs sm:text-sm text-[#414042]">
              Your Amazon credentials are encrypted and stored securely. We only
              access data necessary for analytics.
            </p>
          </div>

          <label className="mb-1 block text-xs sm:text-sm font-semibold text-charcoal-500">
            Select your marketplace <span className="text-rose-500">*</span>
          </label>

          <div className="relative">
            <select
              value={region}
              onChange={(e) => {
                const nextRegion = e.target.value;
                setRegion(nextRegion);

                const nextMarketplaceId =
                  REGION_TO_MARKETPLACE_ID[nextRegion] ||
                  getMarketplaceId(nextRegion) ||
                  "";

                setResolvedMarketplaceId(nextMarketplaceId);
                setSelectedMarketplaceId(nextMarketplaceId);
              }}
              className="mb-4 w-full rounded-md border border-gray-300 bg-white px-3 py-2 pr-10 text-sm md:text-base text-gray-800 outline-none transition focus:border-green-500 focus:ring-2 focus:ring-green-500"
            >
              <option value="eu-west-1">{REGION_LABELS["eu-west-1"]}</option>
              <option value="us-east-1">{REGION_LABELS["us-east-1"]}</option>
              {/* <option value="ap-southeast-1">
                {REGION_LABELS["ap-southeast-1"]}
              </option> */}
            </select>
          </div>

          {!showProfileFields && (
            <div className="mt-2 flex w-full justify-center">
              <Button
                variant="primary"
                size="sm"
                onClick={handleAmazonLogin}
                disabled={isConnecting}
                className={`w-full ${
                  isConnecting ? "bg-blue-700 cursor-not-allowed" : "bg-blue-700"
                }`}
              >
                <FaLink className="h-4 w-4 opacity-90" />
                {isConnecting ? "Working..." : "Connect"}
              </Button>
            </div>
          )}

          {showProfileFields && resolvedMarketplaceId && (
            <>
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                <div>
                  <label className="mb-1 block text-xs sm:text-sm font-semibold text-charcoal-500">
                    Stock Unit (in months)
                    <span className="text-rose-500">*</span>
                  </label>

                  <input
                    type="number"
                    min="0"
                    step="1"
                    value={stockUnit}
                    onChange={(e) => setStockUnit(e.target.value)}
                    className="mb-2 w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm md:text-base text-gray-800 outline-none transition focus:border-green-500 focus:ring-2 focus:ring-green-500"
                    placeholder="Enter stock unit"
                  />

                  {fieldErrors.stockUnit && (
                    <p className="text-xs text-red-600">
                      {fieldErrors.stockUnit}
                    </p>
                  )}
                </div>

                <div>
                  <label className="mb-1 block text-xs sm:text-sm font-semibold text-charcoal-500">
                    Transit Time (in months)
                    <span className="text-rose-500">*</span>
                  </label>

                  <input
                    type="number"
                    min="1"
                    step="1"
                    value={transitTime}
                    onChange={(e) => setTransitTime(e.target.value)}
                    className="mb-2 w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm md:text-base text-gray-800 outline-none transition focus:border-green-500 focus:ring-2 focus:ring-green-500"
                    placeholder="Enter transit time"
                  />

                  {fieldErrors.transitTime && (
                    <p className="text-xs text-red-600">
                      {fieldErrors.transitTime}
                    </p>
                  )}
                </div>
              </div>

              <div className="mt-4 flex w-full justify-center">
                <Button
                  variant="primary"
                  size="sm"
                  onClick={handleSaveCountryProfile}
                  disabled={isSavingProfile}
                  className={`w-full ${
                    isSavingProfile
                      ? "bg-blue-700 cursor-not-allowed"
                      : "bg-blue-700"
                  }`}
                >
                  {isSavingProfile ? "Saving..." : "Submit"}
                </Button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}