// // // lib/dashboard/useFx.ts
// // "use client";

// // import { useCallback, useEffect, useState } from "react";
// // import { getISTYearMonth } from "@/lib/dashboard/date";
// // import { fmtGBP, fmtUSD, fmtNum, toNumberSafe } from "@/lib/dashboard/format";

// // export type HomeCurrency = "USD" | "GBP";

// // /* ===================== ENV & ENDPOINTS ===================== */

// // const baseURL =
// //   process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// // // Flask route path for FX
// // const FX_ENDPOINT = `${baseURL}/currency-rate`;

// // /** FX defaults (used until backend answers) */
// // const GBP_TO_USD_ENV = Number(
// //   process.env.NEXT_PUBLIC_GBP_TO_USD || ""
// // );
// // const INR_TO_USD_ENV = Number(
// //   process.env.NEXT_PUBLIC_INR_TO_USD || "0.01128"
// // );

// // type FromCurrency = "USD" | "GBP" | "INR";

// // export function useFx() {
// //   const [homeCurrency, setHomeCurrency] = useState<HomeCurrency>("USD");

// //   // FX rates: GBP→USD (Amazon UK) and INR→USD (Shopify India)
// //   const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
// //   const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
// //   const [fxLoading, setFxLoading] = useState(false);

// //   /* ----- converters ----- */

// //   const convertToHomeCurrency = useCallback(
// //     (value: number | null | undefined, from: FromCurrency) => {
// //       const n = toNumberSafe(value ?? 0);
// //       if (!n) return 0;

// //       // Home currency = USD
// //       if (homeCurrency === "USD") {
// //         switch (from) {
// //           case "USD":
// //             return n;
// //           case "GBP":
// //             return n * gbpToUsd; // GBP → USD
// //           case "INR":
// //             return n * inrToUsd; // INR → USD
// //           default:
// //             return n;
// //         }
// //       }

// //       // Home currency = GBP
// //       if (homeCurrency === "GBP") {
// //         switch (from) {
// //           case "GBP":
// //             return n;
// //           case "USD":
// //             return gbpToUsd ? n / gbpToUsd : n; // USD → GBP
// //           case "INR": {
// //             const usd = n * inrToUsd;            // INR → USD
// //             return gbpToUsd ? usd / gbpToUsd : usd; // USD → GBP
// //           }
// //           default:
// //             return n;
// //         }
// //       }

// //       return n;
// //     },
// //     [homeCurrency, gbpToUsd, inrToUsd]
// //   );

// //   const formatHomeAmount = useCallback(
// //     (value: number | null | undefined) => {
// //       const n = toNumberSafe(value ?? 0);
// //       switch (homeCurrency) {
// //         case "USD":
// //           return fmtUSD(n);
// //         case "GBP":
// //           return fmtGBP(n);
// //         default:
// //           return fmtNum(n);
// //       }
// //     },
// //     [homeCurrency]
// //   );

// //   /* ----- FX API call (same logic as Dashboard) ----- */

// //   const fetchFxRates = useCallback(async () => {
// //     try {
// //       setFxLoading(true);

// //       const token =
// //         typeof window !== "undefined"
// //           ? localStorage.getItem("jwtToken")
// //           : null;

// //       const headers: HeadersInit = {
// //         "Content-Type": "application/json",
// //         Accept: "application/json",
// //       };
// //       if (token) {
// //         (headers as any).Authorization = `Bearer ${token}`;
// //       }

// //       const { monthName, year } = getISTYearMonth();
// //       const month = monthName.toLowerCase();

// //       const commonBody = {
// //         month,
// //         year,
// //         fetch_if_missing: true,
// //       };

// //       const [ukRes, inrRes] = await Promise.all([
// //         fetch(FX_ENDPOINT, {
// //           method: "POST",
// //           headers,
// //           body: JSON.stringify({
// //             ...commonBody,
// //             user_currency: "GBP",
// //             country: "uk",
// //             selected_currency: "USD",
// //           }),
// //         }),
// //         fetch(FX_ENDPOINT, {
// //           method: "POST",
// //           headers,
// //           body: JSON.stringify({
// //             ...commonBody,
// //             user_currency: "INR",
// //             country: "india",
// //             selected_currency: "USD",
// //           }),
// //         }),
// //       ]);

// //       if (ukRes.ok) {
// //         const json = await ukRes.json();
// //         const rate = json?.record?.conversion_rate;
// //         if (json?.success && rate != null) {
// //           setGbpToUsd(Number(rate));
// //         }
// //       } else {
// //         console.warn("UK FX fetch failed:", ukRes.status);
// //       }

// //       if (inrRes.ok) {
// //         const json = await inrRes.json();
// //         const rate = json?.record?.conversion_rate;
// //         if (json?.success && rate != null) {
// //           setInrToUsd(Number(rate));
// //         }
// //       } else {
// //         console.warn("INR FX fetch failed:", inrRes.status);
// //       }
// //     } catch (err) {
// //       console.error("Failed to fetch FX rates", err);
// //     } finally {
// //       setFxLoading(false);
// //     }
// //   }, []);

// //   useEffect(() => {
// //     fetchFxRates();
// //   }, [fetchFxRates]);

// //   return {
// //     homeCurrency,
// //     setHomeCurrency,
// //     gbpToUsd,
// //     inrToUsd,
// //     fxLoading,
// //     convertToHomeCurrency,
// //     formatHomeAmount,
// //   };
// // }























// // lib/dashboard/useFx.ts
// "use client";

// import { useCallback, useEffect, useState } from "react";
// import { getISTYearMonth } from "@/lib/dashboard/date";
// import { fmtGBP, fmtUSD, fmtNum, toNumberSafe } from "@/lib/dashboard/format";
// import { useGetUserDataQuery } from "@/lib/api/profileApi";

// export type HomeCurrency = "USD" | "GBP";

// /* ===================== ENV & ENDPOINTS ===================== */

// const baseURL =
//   process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// // Flask route path for FX
// const FX_ENDPOINT = `${baseURL}/currency-rate`;

// /** FX defaults (used until backend answers) */
// const GBP_TO_USD_ENV = Number(
//   process.env.NEXT_PUBLIC_GBP_TO_USD || ""
// );
// const INR_TO_USD_ENV = Number(
//   process.env.NEXT_PUBLIC_INR_TO_USD || "0.01128"
// );

// type FromCurrency = "USD" | "GBP" | "INR";

// export function useFx() {
//   // ---- get homeCurrency from profile ----
//   const { data: userData } = useGetUserDataQuery();

//   const [homeCurrency, setHomeCurrency] = useState<HomeCurrency>("USD");

//   // whenever profile data arrives / changes, sync our state
//   useEffect(() => {
//     const profileCurrency = (userData?.homeCurrency || "").toString().toUpperCase();
//     if (profileCurrency === "GBP" || profileCurrency === "USD") {
//       setHomeCurrency(profileCurrency as HomeCurrency);
//     }
//   }, [userData?.homeCurrency]);

//   // FX rates: GBP→USD (Amazon UK) and INR→USD (Shopify India)
//   const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
//   const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
//   const [fxLoading, setFxLoading] = useState(false);

//   /* ----- converters ----- */

//   const convertToHomeCurrency = useCallback(
//     (value: number | null | undefined, from: FromCurrency) => {
//       const n = toNumberSafe(value ?? 0);
//       if (!n) return 0;

//       // Home currency = USD
//       if (homeCurrency === "USD") {
//         switch (from) {
//           case "USD":
//             return n;
//           case "GBP":
//             return n * gbpToUsd; // GBP → USD
//           case "INR":
//             return n * inrToUsd; // INR → USD
//           default:
//             return n;
//         }
//       }

//       // Home currency = GBP
//       if (homeCurrency === "GBP") {
//         switch (from) {
//           case "GBP":
//             return n;
//           case "USD":
//             return gbpToUsd ? n / gbpToUsd : n; // USD → GBP
//           case "INR": {
//             const usd = n * inrToUsd; // INR → USD
//             return gbpToUsd ? usd / gbpToUsd : usd; // USD → GBP
//           }
//           default:
//             return n;
//         }
//       }

//       return n;
//     },
//     [homeCurrency, gbpToUsd, inrToUsd]
//   );

//   const formatHomeAmount = useCallback(
//     (value: number | null | undefined) => {
//       const n = toNumberSafe(value ?? 0);
//       switch (homeCurrency) {
//         case "USD":
//           return fmtUSD(n);
//         case "GBP":
//           return fmtGBP(n);
//         default:
//           return fmtNum(n);
//       }
//     },
//     [homeCurrency]
//   );

//   /* ----- FX API call (same logic as Dashboard) ----- */

//   const fetchFxRates = useCallback(async () => {
//     try {
//       setFxLoading(true);

//       const token =
//         typeof window !== "undefined"
//           ? localStorage.getItem("jwtToken")
//           : null;

//       const headers: HeadersInit = {
//         "Content-Type": "application/json",
//         Accept: "application/json",
//       };
//       if (token) {
//         (headers as any).Authorization = `Bearer ${token}`;
//       }

//       const { monthName, year } = getISTYearMonth();
//       const month = monthName.toLowerCase();

//       const commonBody = {
//         month,
//         year,
//         fetch_if_missing: true,
//       };

//       const [ukRes, inrRes] = await Promise.all([
//         fetch(FX_ENDPOINT, {
//           method: "POST",
//           headers,
//           body: JSON.stringify({
//             ...commonBody,
//             user_currency: "GBP",
//             country: "uk",
//             selected_currency: "USD",
//           }),
//         }),
//         fetch(FX_ENDPOINT, {
//           method: "POST",
//           headers,
//           body: JSON.stringify({
//             ...commonBody,
//             user_currency: "INR",
//             country: "india",
//             selected_currency: "USD",
//           }),
//         }),
//       ]);

//       if (ukRes.ok) {
//         const json = await ukRes.json();
//         const rate = json?.record?.conversion_rate;
//         if (json?.success && rate != null) {
//           setGbpToUsd(Number(rate));
//         }
//       } else {
//         console.warn("UK FX fetch failed:", ukRes.status);
//       }

//       if (inrRes.ok) {
//         const json = await inrRes.json();
//         const rate = json?.record?.conversion_rate;
//         if (json?.success && rate != null) {
//           setInrToUsd(Number(rate));
//         }
//       } else {
//         console.warn("INR FX fetch failed:", inrRes.status);
//       }
//     } catch (err) {
//       console.error("Failed to fetch FX rates", err);
//     } finally {
//       setFxLoading(false);
//     }
//   }, []);

//   useEffect(() => {
//     fetchFxRates();
//   }, [fetchFxRates]);

//   return {
//     homeCurrency,        // driven by profile, but can still be overridden
//     setHomeCurrency,     // keep this to avoid breaking existing pages
//     gbpToUsd,
//     inrToUsd,
//     fxLoading,
//     convertToHomeCurrency,
//     formatHomeAmount,
//   };
// }








































// // lib/dashboard/useFx.ts
// "use client";

// import { useCallback, useEffect, useState } from "react";
// import { getISTYearMonth } from "@/lib/dashboard/date";
// import { fmtGBP, fmtUSD, fmtNum } from "@/lib/dashboard/format";

// /**
//  * Home currency can now be:
//  * - USD
//  * - GBP
//  * - INR
//  * - CAD
//  */
// export type HomeCurrency = "USD" | "GBP" | "INR" | "CAD";

// /* ===================== ENV & ENDPOINTS ===================== */

// const baseURL =
//   process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// const FX_ENDPOINT = `${baseURL}/currency-rate`;

// /**
//  * FX defaults (used until backend answers).
//  * All rates are “X → USD”.
//  */
// const GBP_TO_USD_ENV = Number(
//   process.env.NEXT_PUBLIC_GBP_TO_USD || "1.27"
// );
// const INR_TO_USD_ENV = Number(
//   process.env.NEXT_PUBLIC_INR_TO_USD || "0.01128"
// );
// const CAD_TO_USD_ENV = Number(
//   process.env.NEXT_PUBLIC_CAD_TO_USD || "0.73"
// );

// /**
//  * Source currency codes we might receive from backends
//  * (Amazon US, UK, India, Canada etc.).
//  */
// export type FromCurrency = "USD" | "GBP" | "INR" | "CAD";

// export function useFx() {
//   // 👇 Default homeCurrency – will usually be overridden by profile
//   const [homeCurrency, setHomeCurrency] = useState<HomeCurrency>("USD");

//   // FX store: all vs USD
//   const [gbpToUsd, setGbpToUsd] = useState(GBP_TO_USD_ENV);
//   const [inrToUsd, setInrToUsd] = useState(INR_TO_USD_ENV);
//   const [cadToUsd, setCadToUsd] = useState(CAD_TO_USD_ENV);

//   const [fxLoading, setFxLoading] = useState(false);

//   /* =========================================================
//    *  Helpers: convert between currencies
//    * =======================================================*/

//   // Safely coerce to number
//   const toNumberSafe = (value: any): number => {
//     if (value == null || value === "" || isNaN(Number(value))) return 0;
//     return Number(value);
//   };

//   /**
//    * Convert from any “fromCurrency” into USD,
//    * using the stored FX rates.
//    */
//   const toUSD = useCallback(
//     (value: number, from: FromCurrency): number => {
//       const n = toNumberSafe(value);
//       if (!n) return 0;

//       switch (from) {
//         case "USD":
//           return n;
//         case "GBP":
//           return n * (gbpToUsd || GBP_TO_USD_ENV || 1);
//         case "INR":
//           return n * (inrToUsd || INR_TO_USD_ENV || 1);
//         case "CAD":
//           return n * (cadToUsd || CAD_TO_USD_ENV || 1);
//         default:
//           return n;
//       }
//     },
//     [gbpToUsd, inrToUsd, cadToUsd]
//   );

//   /**
//    * Convert a USD amount into the current homeCurrency.
//    */
//   const fromUSDToHome = useCallback(
//     (usdAmount: number): number => {
//       const n = toNumberSafe(usdAmount);
//       if (!n) return 0;

//       switch (homeCurrency) {
//         case "USD":
//           return n;
//         case "GBP": {
//           const rate = gbpToUsd || GBP_TO_USD_ENV || 1;
//           return rate ? n / rate : n;
//         }
//         case "INR": {
//           const rate = inrToUsd || INR_TO_USD_ENV || 1;
//           return rate ? n / rate : n;
//         }
//         case "CAD": {
//           const rate = cadToUsd || CAD_TO_USD_ENV || 1;
//           return rate ? n / rate : n;
//         }
//         default:
//           return n;
//       }
//     },
//     [homeCurrency, gbpToUsd, inrToUsd, cadToUsd]
//   );

//   /**
//    * Main converter that pages should use.
//    * Example:
//    *  - Amazon UK net_sales is in GBP → convertToHomeCurrency(value, "GBP")
//    *  - Amazon US net_sales is in USD → convertToHomeCurrency(value, "USD")
//    *  - Shopify India is in INR → convertToHomeCurrency(value, "INR")
//    */
//   const convertToHomeCurrency = useCallback(
//     (value: number | null | undefined, from: FromCurrency) => {
//       const n = toNumberSafe(value ?? 0);
//       if (!n) return 0;

//       const usd = toUSD(n, from);
//       return fromUSDToHome(usd);
//     },
//     [toUSD, fromUSDToHome]
//   );

//   /**
//    * Format an amount *already in homeCurrency* for display.
//    * You should call this after convertToHomeCurrency,
//    * or on any number that is known to already be in homeCurrency.
//    */
//   const formatHomeAmount = useCallback(
//     (value: number | null | undefined) => {
//       const n = toNumberSafe(value ?? 0);

//       if (homeCurrency === "USD") {
//         return fmtUSD(n);
//       }
//       if (homeCurrency === "GBP") {
//         return fmtGBP(n);
//       }
//       if (homeCurrency === "INR") {
//         return new Intl.NumberFormat("en-IN", {
//           style: "currency",
//           currency: "INR",
//           maximumFractionDigits: 0,
//         }).format(n);
//       }
//       if (homeCurrency === "CAD") {
//         return new Intl.NumberFormat("en-CA", {
//           style: "currency",
//           currency: "CAD",
//           maximumFractionDigits: 0,
//         }).format(n);
//       }

//       // Fallback
//       return fmtNum(n);
//     },
//     [homeCurrency]
//   );

//   /* =========================================================
//    *  Fetch FX rates from backend
//    * =======================================================*/

//   const fetchFxRates = useCallback(async () => {
//     try {
//       setFxLoading(true);

//       const token =
//         typeof window !== "undefined"
//           ? localStorage.getItem("jwtToken")
//           : null;

//       const headers: HeadersInit = {
//         "Content-Type": "application/json",
//         Accept: "application/json",
//       };
//       if (token) {
//         (headers as any).Authorization = `Bearer ${token}`;
//       }

//       const { monthName, year } = getISTYearMonth();
//       const month = monthName.toLowerCase();

//       const commonBody = {
//         month,
//         year,
//         fetch_if_missing: true,
//       };

//       // 🔹 Ask backend for:
//       //    GBP → USD (country: "uk")
//       //    INR → USD (country: "india")
//       //    CAD → USD (country: "canada")
//       const [ukRes, inrRes, cadRes] = await Promise.all([
//         fetch(FX_ENDPOINT, {
//           method: "POST",
//           headers,
//           body: JSON.stringify({
//             ...commonBody,
//             user_currency: "GBP",
//             country: "uk",
//             selected_currency: "USD",
//           }),
//         }),
//         fetch(FX_ENDPOINT, {
//           method: "POST",
//           headers,
//           body: JSON.stringify({
//             ...commonBody,
//             user_currency: "INR",
//             country: "india",
//             selected_currency: "USD",
//           }),
//         }),
//         fetch(FX_ENDPOINT, {
//           method: "POST",
//           headers,
//           body: JSON.stringify({
//             ...commonBody,
//             user_currency: "CAD",
//             country: "canada",
//             selected_currency: "USD",
//           }),
//         }),
//       ]);

//       if (ukRes.ok) {
//         const json = await ukRes.json();
//         const rate = json?.record?.conversion_rate;
//         if (json?.success && rate != null) {
//           setGbpToUsd(Number(rate));
//         }
//       } else {
//         console.warn("GBP→USD FX fetch failed:", ukRes.status);
//       }

//       if (inrRes.ok) {
//         const json = await inrRes.json();
//         const rate = json?.record?.conversion_rate;
//         if (json?.success && rate != null) {
//           setInrToUsd(Number(rate));
//         }
//       } else {
//         console.warn("INR→USD FX fetch failed:", inrRes.status);
//       }

//       if (cadRes.ok) {
//         const json = await cadRes.json();
//         const rate = json?.record?.conversion_rate;
//         if (json?.success && rate != null) {
//           setCadToUsd(Number(rate));
//         }
//       } else {
//         console.warn("CAD→USD FX fetch failed:", cadRes.status);
//       }
//     } catch (err) {
//       console.error("Failed to fetch FX rates", err);
//     } finally {
//       setFxLoading(false);
//     }
//   }, []);

//   useEffect(() => {
//     fetchFxRates();
//   }, [fetchFxRates]);

//   return {
//     homeCurrency,
//     setHomeCurrency,
//     fxLoading,
//     // raw FX rates
//     gbpToUsd,
//     inrToUsd,
//     cadToUsd,
//     // helpers
//     convertToHomeCurrency,
//     formatHomeAmount,
//   };
// }



















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

const FX_ENDPOINT = `${baseURL}/currency-rate`;

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
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      const headers: HeadersInit = {
        "Content-Type": "application/json",
        Accept: "application/json",
      };
      if (token) {
        (headers as any).Authorization = `Bearer ${token}`;
      }

      const { monthName, year } = getISTYearMonth();
      const month = monthName.toLowerCase();

      const commonBody = {
        month,
        year,
        fetch_if_missing: true,
      };

      // 🔹 Ask backend for:
      //    GBP → USD (country: "uk")
      //    INR → USD (country: "india")
      //    CAD → USD (country: "canada")
      const [ukRes, inrRes, cadRes] = await Promise.all([
        fetch(FX_ENDPOINT, {
          method: "POST",
          headers,
          body: JSON.stringify({
            ...commonBody,
            user_currency: "GBP",
            country: "uk",
            selected_currency: "USD",
          }),
        }),
        fetch(FX_ENDPOINT, {
          method: "POST",
          headers,
          body: JSON.stringify({
            ...commonBody,
            user_currency: "INR",
            country: "india",
            selected_currency: "USD",
          }),
        }),
        fetch(FX_ENDPOINT, {
          method: "POST",
          headers,
          body: JSON.stringify({
            ...commonBody,
            user_currency: "CAD",
            country: "canada",
            selected_currency: "USD",
          }),
        }),
      ]);

      if (ukRes.ok) {
        const json = await ukRes.json();
        const rate = json?.record?.conversion_rate;
        if (json?.success && rate != null) {
          setGbpToUsd(Number(rate));
        }
      } else {
        console.warn("GBP→USD FX fetch failed:", ukRes.status);
      }

      if (inrRes.ok) {
        const json = await inrRes.json();
        const rate = json?.record?.conversion_rate;
        if (json?.success && rate != null) {
          setInrToUsd(Number(rate));
        }
      } else {
        console.warn("INR→USD FX fetch failed:", inrRes.status);
      }

      if (cadRes.ok) {
        const json = await cadRes.json();
        const rate = json?.record?.conversion_rate;
        if (json?.success && rate != null) {
          setCadToUsd(Number(rate));
        }
      } else {
        console.warn("CAD→USD FX fetch failed:", cadRes.status);
      }
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
