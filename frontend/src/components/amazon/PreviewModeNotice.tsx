"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter, usePathname } from "next/navigation";
import { useGetUserDataQuery } from "@/lib/api/profileApi";

const DISMISS_KEY = "preview_mode_notice_dismissed_until";
const DISMISS_MS = 30 * 1000; // 30 sec
const AUTO_COLLAPSE_MS = 5000; // 5 sec after showing, collapse into mini toast

export default function PreviewModeNotice() {
  const params = useParams();
  const router = useRouter();
  const pathname = usePathname();

  const [dismissedUntil, setDismissedUntil] = useState<number | null>(null);
  const [now, setNow] = useState<number>(Date.now());
  const [expanded, setExpanded] = useState(true);

  const { data: userData, isLoading: userLoading } = useGetUserDataQuery();
  const amazonUserExist = userData?.amazon_user_exist === true;

  const isPreviewMode = useMemo(() => {
    return params?.month === "NA" && params?.year === "NA";
  }, [params]);

  const routeCountry = useMemo(() => {
    const raw = String(params?.countryName || "").toLowerCase();
    if (!raw || raw === "na") return "uk";
    return raw;
  }, [params]);

  const isPnlPreviewRoute = useMemo(() => {
    return pathname === `/pnl-dashboard/QTD/${routeCountry}/NA/NA`;
  }, [pathname, routeCountry]);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const stored = localStorage.getItem(DISMISS_KEY);
    if (stored) {
      const ts = Number(stored);
      if (Number.isFinite(ts)) {
        setDismissedUntil(ts);
      }
    }
  }, []);

  useEffect(() => {
    if (!dismissedUntil) return;

    const remaining = dismissedUntil - Date.now();

    if (remaining <= 0) {
      localStorage.removeItem(DISMISS_KEY);
      setDismissedUntil(null);
      setNow(Date.now());
      return;
    }

    const timer = setTimeout(() => {
      localStorage.removeItem(DISMISS_KEY);
      setDismissedUntil(null);
      setNow(Date.now());
    }, remaining);

    return () => clearTimeout(timer);
  }, [dismissedUntil]);

  useEffect(() => {
    const handleStorage = (e: StorageEvent) => {
      if (e.key !== DISMISS_KEY) return;

      if (!e.newValue) {
        setDismissedUntil(null);
        setNow(Date.now());
        return;
      }

      const ts = Number(e.newValue);
      if (Number.isFinite(ts)) {
        setDismissedUntil(ts);
        setNow(Date.now());
      }
    };

    window.addEventListener("storage", handleStorage);
    return () => window.removeEventListener("storage", handleStorage);
  }, []);

  const isTemporarilyDismissed = dismissedUntil != null && now < dismissedUntil;

  const shouldShow =
    !userLoading &&
    isPreviewMode &&
    !amazonUserExist &&
    !isTemporarilyDismissed &&
    !isPnlPreviewRoute;

  useEffect(() => {
    if (!shouldShow) return;

    setExpanded(true);

    const timer = setTimeout(() => {
      setExpanded(false);
    }, AUTO_COLLAPSE_MS);

    return () => clearTimeout(timer);
  }, [shouldShow]);

  const handleDismiss = () => {
    const until = Date.now() + DISMISS_MS;
    localStorage.setItem(DISMISS_KEY, String(until));
    setDismissedUntil(until);
    setNow(Date.now());
  };

  const handleConnectAmazon = () => {
    router.push(`/pnl-dashboard/QTD/${routeCountry}/NA/NA`);
  };

  if (!shouldShow) return null;

  return (
    <div className="fixed top-16 right-4 z-[9999]">
      {expanded ? (
        <div
          className="
            w-[92vw] max-w-sm sm:max-w-md
            rounded-2xl border border-amber-300 bg-white/95 backdrop-blur
            shadow-2xl p-4
            transition-all duration-300 ease-out
            animate-in slide-in-from-top-2 fade-in
          "
        >
          <div className="flex items-start gap-3">
            <button
              onClick={() => setExpanded(false)}
              className="mt-0.5 text-lg leading-none text-amber-600"
              aria-label="Collapse notification"
            >
              ⚠️
            </button>

            <div className="flex-1">
              <div className="flex items-start justify-between gap-3">
                <h4 className="text-sm font-semibold text-red-700">
                  Preview mode active
                </h4>

                <button
                  onClick={() => setExpanded(false)}
                  className="text-xs text-gray-400 hover:text-gray-600"
                >
                  Hide
                </button>
              </div>

              <p className="mt-1 text-xs sm:text-sm text-[#414042] leading-5">
                You&apos;re viewing dummy data. Connect your Amazon account and fetch
                real data to unlock full insights.
              </p>

              <div className="mt-3 flex flex-wrap gap-2">
                <button
                  onClick={handleConnectAmazon}
                  className="rounded-md bg-[#37455F] px-3 py-1.5 text-xs sm:text-sm font-medium text-[#F8EDCE] hover:opacity-90 transition"
                >
                  Connect Amazon
                </button>

                <button
                  onClick={handleDismiss}
                  className="rounded-md bg-[#D9D9D94D] px-3 py-1.5 text-xs sm:text-sm font-medium text-[#414042] hover:bg-[#d9d9d980] transition"
                >
                  Dismiss
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : (
        <button
          onClick={() => setExpanded(true)}
          className="
            flex items-center gap-2
            rounded-full border border-amber-300 bg-white/95 backdrop-blur
            px-3 py-2 shadow-lg
            text-sm font-medium text-[#414042]
            hover:shadow-xl transition-all duration-300
          "
        >
          <span className="text-amber-600">⚠️</span>
          <span>Preview mode</span>
        </button>
      )}
    </div>
  );
}