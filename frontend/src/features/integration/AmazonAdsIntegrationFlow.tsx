"use client";

import React, { useEffect, useState } from "react";
import AmazonAdsConnectModal from "./AmazonAdsConnectModal";

export default function AmazonAdsIntegrationFlow() {
  const [open, setOpen] = useState(false);

  // keep minimal props so it opens regardless of backend wiring
  const [adsConnecting, setAdsConnecting] = useState(false);
  const [adsError, setAdsError] = useState<string | null>(null);

  useEffect(() => {
    const handler = (e: Event) => {
      const custom = e as CustomEvent<{ provider?: string }>;
      const provider = custom.detail?.provider;
      if (provider === "amazon_ads") {
        setOpen(true);
      }
    };

    window.addEventListener("integration:choose", handler as EventListener);
    return () =>
      window.removeEventListener("integration:choose", handler as EventListener);
  }, []);

  const onConnectOrSync = async () => {
    try {
      setAdsConnecting(true);
      setAdsError(null);
    } catch (err) {
      console.error(err);
      setAdsError("Amazon Ads action failed");
    } finally {
      setAdsConnecting(false);
    }
  };

  return (
    <AmazonAdsConnectModal
      isOpen={open}
      onClose={() => setOpen(false)}
      adsStatusLoading={false}
      adsStatus={null}
      adsConnecting={adsConnecting}
      adsError={adsError}
      onConnectOrSync={onConnectOrSync}
    />
  );
}
