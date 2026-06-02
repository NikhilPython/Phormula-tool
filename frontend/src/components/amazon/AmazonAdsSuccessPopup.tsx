"use client";

import React, { useEffect, useRef, useState } from "react";

export default function AmazonAdsSuccessPopup() {
  const [open, setOpen] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const handleSuccess = () => {
      setOpen(true);

      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }

      timeoutRef.current = setTimeout(() => {
        setOpen(false);
      }, 4500);
    };

    window.addEventListener("amazonAdsSyncSuccess", handleSuccess);

    return () => {
      window.removeEventListener("amazonAdsSyncSuccess", handleSuccess);

      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[999999] bg-black/70 flex items-center justify-center px-4"
      role="dialog"
      aria-modal="true"
    >
      <div
        className="w-full max-w-sm rounded-2xl bg-white p-6 shadow-xl text-center"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="mx-auto mb-4 w-14 h-14 rounded-full bg-[#E8F5F0] flex items-center justify-center">
          <svg
            width="28"
            height="28"
            viewBox="0 0 24 24"
            fill="none"
            stroke="#5EA68E"
            strokeWidth="3"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <polyline points="20 6 9 17 4 12" />
          </svg>
        </div>

        <h3 className="text-lg font-semibold text-[#37455F]">
          Ads fetched successfully
        </h3>

        <p className="mt-2 text-sm text-slate-500">
          Amazon Ads data has been synced successfully.
        </p>
      </div>
    </div>
  );
}