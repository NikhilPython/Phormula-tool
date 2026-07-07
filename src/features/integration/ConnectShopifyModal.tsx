"use client";

// import React, { useCallback, useEffect, useState } from "react";

import React, { useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Button from "@/components/ui/button/Button";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const ICONS = {
  back: "/BackArrow.png",
  shopify: "/shopify.png",
  shield: "/secure.png",
  link: "/link.png",
  info: "/info.png",
};

function sanitizeShopName(raw: string) {
  let s = (raw || "").trim().toLowerCase();
  s = s.replace(/^https?:\/\//, "") // remove protocol
    .replace(/\/.*$/, "")        // strip path
    .replace(/\.myshopify\.com$/, ""); // drop domain if pasted
  s = s.replace(/[^a-z0-9-]/g, ""); // keep only valid characters
  return s;
}

// type Props = {
//   onClose?: () => void;
// };

type Props = {
  onClose?: () => void;
  isAlreadyConnected?: boolean;
};


// export default function ConnectShopifyModal({ onClose }: Props) {
//   const [shopName, setShopName] = useState("");
//   const [isConnecting, setIsConnecting] = useState(false);
//   const [error, setError] = useState("");


export default function ConnectShopifyModal({ onClose }: Props) {
  const [shopName, setShopName] = useState("");
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState("");


  const handleConnectShopify = useCallback(() => {
    const cleaned = sanitizeShopName(shopName);
    if (!cleaned) {
      setError("Please enter your Shopify store name.");
      return;
    }
    setError("");
    setIsConnecting(true);

    const frontendBase = window.location.origin;
    const redirectUri = `${frontendBase}/(admin)/shopify?shop=${encodeURIComponent(`${cleaned}.myshopify.com`)}`;


    const user_token =
      (typeof window !== "undefined" && localStorage.getItem("jwtToken")) || "";

    // const installUrl = `${API_BASE}/shopify/install?shop=${encodeURIComponent(
    //   `${cleaned}.myshopify.com`
    // )}&user_token=${encodeURIComponent(user_token)}`;

    // window.location.href = installUrl;

    const installUrl = `${API_BASE}/shopify/install?shop=${encodeURIComponent(
      `${cleaned}.myshopify.com`
    )}&user_token=${encodeURIComponent(user_token)}&redirect_uri=${encodeURIComponent(redirectUri)}`;

    window.location.href = installUrl;
  }, [shopName]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose?.();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-99999 flex items-center justify-center bg-black/50 p-3 sm:p-4 md:p-6"
      role="dialog"
      aria-modal="true"
      aria-labelledby="shopify-connect-title"
      onClick={onClose} // click on backdrop closes
    >
      <div
        className="relative w-11/12 sm:w-full max-w-sm sm:max-w-md md:max-w-lg lg:max-w-xl rounded-xl bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}        // prevent backdrop close on inner clicks
        onMouseDown={(e) => e.stopPropagation()}    // guard focus edge cases
      >
        {/* Back / Close */}
        {/* <button
          type="button"
          onClick={onClose}
          className="absolute left-2 top-2 sm:left-3 sm:top-3 inline-flex h-9 w-9 sm:h-10 sm:w-10 items-center justify-center rounded-full hover:bg-gray-100 focus:outline-none"
          aria-label="Close"
          title="Close"
        >
          <img src={ICONS.back} alt="Back" className="h-6 w-6 sm:h-8 sm:w-8" />
        </button> */}

        {/* Body */}
        <div className="max-h-[90vh] overflow-y-auto px-4 sm:px-6 md:px-8 pb-6 pt-5 sm:pt-8">
          {/* Logo */}
          <div className="mx-auto mb-3 sm:mb-4 flex h-10 w-10 items-center justify-center">
            <img
              src={ICONS.shopify}
              alt="Shopify"
              className="h-10 w-10 sm:h-12 sm:w-12 mx-auto"
            />
          </div>

          {/* Title & subtitle */}
          <h2
            id="shopify-connect-title"
            className="mb-1 text-center text-2xl sm:text-3xl md:text-4xl font-semibold text-[#5EA68E]"
          >
            Connect Shopify Store
          </h2>
          <p className="mb-5 mx-auto w-[65%] text-center text-xs sm:text-sm font-bold text-[#414042]">
            Link your Shopify Store to sync products, orders, and financial data
            seamlessly
          </p>

          {/* Secure callout */}
          <div className="mb-5 rounded-md border border-[#5EA68E] border-l-[5px] bg-[#D9D9D94D] px-3 py-3 sm:py-4">
            <div className="flex items-center gap-2">
              <img
                src={ICONS.shield}
                alt="Secure"
                className="h-4 w-4 sm:h-5 sm:w-5"
              />
              <span className="text-xs sm:text-sm font-medium text-[#5EA68E]">
                Secure Connection
              </span>
            </div>
            <p className="mt-1 ml-1 text-xs sm:text-sm text-[#414042]">
              Your credentials are encrypted and stored securely. We only access
              data necessary for analytics.
            </p>
          </div>

          {/* Field */}
          <label className="mb-1 block text-xs sm:text-sm font-bold text-gray-700">
            Store Name <span className="text-rose-500">*</span>
          </label>
          <input
            type="text"
            inputMode="text"
            placeholder="Your Store Name"
            value={shopName}
            onChange={(e) => setShopName(e.target.value)}
            className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm md:text-base text-gray-800 outline-none transition focus:border-emerald-500 focus:ring-2 focus:ring-emerald-200"
          />
          <div className="mt-1 flex items-start gap-1">
            <img
              src={ICONS.info}
              alt=""
              aria-hidden="true"
              className="h-4 w-4 object-contain"
            />
            <p className="text-[10px] sm:text-xs text-[#414042]">
              Enter the name from your Shopify admin URL.
            </p>
          </div>

          {/* Error */}
          {error && (
            <div className="mt-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs sm:text-sm text-red-700">
              {error}
            </div>
          )}

          {/* Connect button */}
          <div className="mt-4 flex w-full justify-center">
            <Button
              // type="button"
              variant="primary"
              size="sm"
              onClick={handleConnectShopify}
              disabled={isConnecting}
              className={`w-full
                ${isConnecting
                  ? "bg-blue-700 cursor-not-allowed"
                  : "bg-blue-700 "
                }`}
            >
              <img
                src={ICONS.link}
                alt=""
                className="h-5 w-5 sm:h-8 sm:w-8 opacity-90"
              />
              {isConnecting ? "Working..." : "Connect"}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
