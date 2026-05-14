// lib/dashboard/useFx.ts
"use client";

import { useCallback, useEffect, useState } from "react";
import { getISTYearMonth } from "@/lib/dashboard/date";
import { fmtGBP, fmtUSD, fmtNum } from "@/lib/dashboard/format";

/**
 * Home currency can now be:
 * - USD
 * - GBP
 * - INR
 * - CAD
 */
export type HomeCurrency = "USD" | "GBP" | "INR" | "CAD";

/**
 * Source currency codes we might receive from backends
 * (Amazon US, UK, India, Canada etc.).
 */
export type FromCurrency = "USD" | "GBP" | "INR" | "CAD";

/* ===================== ENV & ENDPOINTS ===================== */

const baseURL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const FX_RATES_GET_ENDPOINT = `${baseURL}/currency-rates`;

export function useFx() {
  // 👇 Default homeCurrency – will usually be overridden by profile
  const [homeCurrency, setHomeCurrency] = useState<HomeCurrency>("USD");

  // FX store: all vs USD
  // 👉 start with 1 (no-op conversion) until backend gives us real rates
  const [gbpToUsd, setGbpToUsd] = useState(1);
  const [inrToUsd, setInrToUsd] = useState(1);
  const [cadToUsd, setCadToUsd] = useState(1);

  const [fxLoading, setFxLoading] = useState(false);

  /* =========================================================
   *  Helpers: convert between currencies
   * =======================================================*/

  // Safely coerce to number
  const toNumberSafe = (value: any): number => {
    if (value == null || value === "" || isNaN(Number(value))) return 0;
    return Number(value);
  };

  /**
   * Convert from any “fromCurrency” into USD,
   * using the stored FX rates.
   */
  const toUSD = useCallback(
    (value: number, from: FromCurrency): number => {
      const n = toNumberSafe(value);
      if (!n) return 0;

      switch (from) {
        case "USD":
          return n;
        case "GBP": {
          const rate = gbpToUsd || 1;
          return n * rate;
        }
        case "INR": {
          const rate = inrToUsd || 1;
          return n * rate;
        }
        case "CAD": {
          const rate = cadToUsd || 1;
          return n * rate;
        }
        default:
          return n;
      }
    },
    [gbpToUsd, inrToUsd, cadToUsd]
  );

  /**
   * Convert a USD amount into the current homeCurrency.
   */
  const fromUSDToHome = useCallback(
    (usdAmount: number): number => {
      const n = toNumberSafe(usdAmount);
      if (!n) return 0;

      switch (homeCurrency) {
        case "USD":
          return n;
        case "GBP": {
          const rate = gbpToUsd || 1;
          return rate ? n / rate : n;
        }
        case "INR": {
          const rate = inrToUsd || 1;
          return rate ? n / rate : n;
        }
        case "CAD": {
          const rate = cadToUsd || 1;
          return rate ? n / rate : n;
        }
        default:
          return n;
      }
    },
    [homeCurrency, gbpToUsd, inrToUsd, cadToUsd]
  );

  /**
   * Main converter that pages should use.
   * Example:
   *  - Amazon UK net_sales is in GBP → convertToHomeCurrency(value, "GBP")
   *  - Amazon US net_sales is in USD → convertToHomeCurrency(value, "USD")
   *  - Shopify India is in INR → convertToHomeCurrency(value, "INR")
   */
  const convertToHomeCurrency = useCallback(
    (value: number | null | undefined, from: FromCurrency) => {
      const n = toNumberSafe(value ?? 0);
      if (!n) return 0;

      const usd = toUSD(n, from);
      return fromUSDToHome(usd);
    },
    [toUSD, fromUSDToHome]
  );

  /**
   * Format an amount *already in homeCurrency* for display.
   * You should call this after convertToHomeCurrency,
   * or on any number that is known to already be in homeCurrency.
   */
  const formatHomeAmount = useCallback(
    (value: number | null | undefined) => {
      const n = toNumberSafe(value ?? 0);

      if (homeCurrency === "USD") {
        return fmtUSD(n);
      }
      if (homeCurrency === "GBP") {
        return fmtGBP(n);
      }
      if (homeCurrency === "INR") {
        return new Intl.NumberFormat("en-IN", {
          style: "currency",
          currency: "INR",
          maximumFractionDigits: 0,
        }).format(n);
      }
      if (homeCurrency === "CAD") {
        return new Intl.NumberFormat("en-CA", {
          style: "currency",
          currency: "CAD",
          maximumFractionDigits: 0,
        }).format(n);
      }

      // Fallback
      return fmtNum(n);
    },
    [homeCurrency]
  );

  /* =========================================================
   *  Fetch FX rates from backend
   * =======================================================*/


const fetchFxRates = useCallback(async () => {
  try {
    setFxLoading(true);

    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    const headers: HeadersInit = { Accept: "application/json" };
    if (token) (headers as any).Authorization = `Bearer ${token}`;

    const res = await fetch(FX_RATES_GET_ENDPOINT, {
      method: "GET",
      headers,
    });

    if (!res.ok) {
      console.error("FX rates fetch failed:", res.status);
      return;
    }

    const rows: {
      user_currency: string;
      selected_currency: string;
      conversion_rate: number;
      month: string;
      year: number;
    }[] = await res.json();

    // helper
    const findRate = (from: string, to: string) =>
      rows.find(
        (r) =>
          r.user_currency === from &&
          r.selected_currency === to &&
          Number.isFinite(Number(r.conversion_rate))
      )?.conversion_rate;

    const gbp = findRate("gbp", "usd");
    const inr = findRate("inr", "usd");
    const cad = findRate("cad", "usd");

    if (gbp != null) setGbpToUsd(Number(gbp));
    if (inr != null) setInrToUsd(Number(inr));
    if (cad != null) setCadToUsd(Number(cad));

  } catch (err) {
    console.error("Failed to fetch FX rates", err);
  } finally {
    setFxLoading(false);
  }
}, []);


  useEffect(() => {
    fetchFxRates();
  }, [fetchFxRates]);

  return {
    homeCurrency,
    setHomeCurrency,
    fxLoading,
    // raw FX rates
    gbpToUsd,
    inrToUsd,
    cadToUsd,
    // helpers
    convertToHomeCurrency,
    formatHomeAmount,
  };
}
