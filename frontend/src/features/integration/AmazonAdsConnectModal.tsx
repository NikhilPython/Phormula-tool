"use client";

import React, { useEffect } from "react";
import { createPortal } from "react-dom";

type Props = {
  isOpen: boolean;
  onClose: () => void;

  adsStatusLoading: boolean;
  adsStatus: any | null;

  adsConnecting: boolean;
  adsError: string | null;

  onConnectOrSync: () => void;
  disabled?: boolean;
};

export default function AmazonAdsConnectModal({
  isOpen,
  onClose,
  adsStatusLoading,
  adsStatus,
  adsConnecting,
  adsError,
  onConnectOrSync,
  disabled,
}: Props) {
  const connected = adsStatus?.status === "connected";

  // Escape key close (same behavior as your Modal)
  useEffect(() => {
    if (!isOpen) return;

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };

    document.addEventListener("keydown", handleEscape);
    return () => document.removeEventListener("keydown", handleEscape);
  }, [isOpen, onClose]);

  // Lock scroll (same behavior as your Modal)
  useEffect(() => {
    if (!isOpen) return;

    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    return () => {
      document.body.style.overflow = prev || "unset";
    };
  }, [isOpen]);

  if (!isOpen) return null;

  const content = (
    <div
      className="fixed inset-0 flex items-center justify-center p-4"
      style={{ zIndex: 99999 }}
      role="dialog"
      aria-modal="true"
      onClick={onClose}
    >
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/40"
        style={{ zIndex: 99999 }}
      />

      {/* Modal Card */}
      <div
        className="relative w-full max-w-xl rounded-3xl bg-white p-6 shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9] dark:bg-gray-900"
        style={{ zIndex: 100000 }}
        onClick={(e) => e.stopPropagation()}
      >
        <h3 className="text-xl font-semibold text-charcoal-500">
          Amazon Ads Integration
        </h3>

        <p className="mt-2 text-sm text-gray-600">
          Connect Amazon Ads to load Sponsored Products &amp; Sponsored Display
          reports.
        </p>

        <div className="mt-4 text-sm">
          {adsStatusLoading ? (
            <span className="text-gray-500">Checking status…</span>
          ) : connected ? (
            <span className="text-emerald-700 font-medium">
              Status: connected
            </span>
          ) : (
            <span className="text-gray-700">Status: not connected</span>
          )}

          {adsStatus?.saved?.amazon_ads_refresh_token_updated_at ? (
            <span className="ml-2 text-gray-500">
              (token updated:{" "}
              {new Date(
                adsStatus.saved.amazon_ads_refresh_token_updated_at
              ).toLocaleString()}
              )
            </span>
          ) : null}
        </div>

        {adsError ? (
          <div className="mt-3 text-sm text-red-600">{adsError}</div>
        ) : null}

        <div className="mt-6 flex items-center justify-end gap-3">
          <button
            type="button"
            onClick={onClose}
            className="rounded-xl px-4 py-2 text-sm font-semibold border border-gray-300 text-gray-700 hover:bg-gray-50 dark:border-gray-700 dark:text-gray-200 dark:hover:bg-gray-800"
          >
            Close
          </button>

          <button
            type="button"
            onClick={onConnectOrSync}
            disabled={disabled || adsConnecting}
            className={`rounded-xl px-5 py-2 text-sm font-semibold text-white transition-all ${
              disabled || adsConnecting
                ? "bg-gray-400 cursor-not-allowed"
                : "bg-[#5EA68E] hover:opacity-90"
            }`}
          >
            {adsConnecting
              ? "Connecting…"
              : connected
              ? "Sync Ads Data"
              : "Connect Amazon Ads"}
          </button>
        </div>
      </div>
    </div>
  );

  // Portal ensures it’s not trapped under other stacking contexts
  return createPortal(content, document.body);
}
