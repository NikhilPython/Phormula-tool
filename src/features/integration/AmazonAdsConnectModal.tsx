"use client";

import React, { useEffect, useState } from "react";
import { Modal } from "@/components/ui/modal";
import { useSelector } from "react-redux";

type Props = {
  isOpen: boolean;
  onClose: () => void;

  // ✅ controlled props from parent (optional)
  adsStatusLoading?: boolean;
  adsStatus?: any | null;
  adsConnecting?: boolean;
  adsError?: string | null;
  onConnectOrSync?: () => void | Promise<void>;
};


export default function AmazonAdsConnectModal({
  isOpen,
  onClose,
  adsStatusLoading,
  adsStatus,
  adsConnecting,
  adsError,

}: Props) {

  const token = useSelector((state: any) => state.auth?.token);

  const [loadingStatus, setLoadingStatus] = useState(false);
  const [status, setStatus] = useState<any | null>(null);

  const [connecting, setConnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  // (Optional) fetch status on open
  useEffect(() => {
    if (!isOpen) return;
    if (!token) return;

    const run = async () => {
      try {
        setLoadingStatus(true);
        setError(null);

        const res = await fetch(`${baseUrl}/amazon-ads/status`, {
          headers: { Authorization: `Bearer ${token}` },
        });

        const contentType = res.headers.get("content-type") || "";
        if (!contentType.includes("application/json")) return;

        const data = await res.json();
        if (!res.ok) throw new Error(data?.error || "Failed to load Ads status");

        setStatus(data);
      } catch (e: any) {
        setError(e?.message || "Failed to load Amazon Ads status");
      } finally {
        setLoadingStatus(false);
      }
    };

    run();
  }, [isOpen, token, baseUrl]);

  const onConnectOrSync = async () => {
    if (!token) {
      setError("Missing auth token");
      return;
    }

    try {
      setConnecting(true);
      setError(null);

      // ✅ hit your backend route here
      const res = await fetch(`${baseUrl}/amazon-ads/connect`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}), // add payload if needed
      });

      const contentType = res.headers.get("content-type") || "";
      const data = contentType.includes("application/json") ? await res.json() : null;

      if (!res.ok) throw new Error(data?.error || "Amazon Ads connect failed");

      // optionally refresh status after success
      setStatus(data || { connected: true });
    } catch (e: any) {
      setError(e?.message || "Amazon Ads connect failed");
    } finally {
      setConnecting(false);
    }
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      className="m-4 max-w-xl"
      showCloseButton
    >
      <div className="rounded-3xl bg-white p-6 dark:bg-gray-900">
        <h2 className="text-xl font-semibold">Amazon Ads Integration</h2>

        <div className="mt-4 text-sm">
          {loadingStatus ? (
            <div>Loading status…</div>
          ) : status ? (
            <pre className="rounded-lg bg-gray-100 p-3 text-xs dark:bg-gray-800 overflow-auto">
              {JSON.stringify(status, null, 2)}
            </pre>
          ) : (
            <div className="text-gray-600 dark:text-gray-300">
              Connect your Amazon Ads account to sync campaigns and spend.
            </div>
          )}
        </div>

        {error && (
          <div className="mt-3 rounded-lg bg-red-50 p-3 text-sm text-red-700">
            {error}
          </div>
        )}

        <div className="mt-6 flex gap-3 justify-end">
          <button
            type="button"
            className="rounded-xl border px-4 py-2"
            onClick={onClose}
            disabled={connecting}
          >
            Close
          </button>

          <button
            type="button"
            className="rounded-xl bg-emerald-600 px-4 py-2 text-white disabled:opacity-60"
            onClick={onConnectOrSync}
            disabled={connecting}
          >
            {connecting ? "Working…" : "Connect / Sync"}
          </button>
        </div>
      </div>
    </Modal>
  );
}
