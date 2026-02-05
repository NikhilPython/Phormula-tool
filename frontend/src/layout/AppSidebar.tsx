"use client";

import React, { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import Image from "next/image";
import { useParams, usePathname, useRouter } from "next/navigation";
import { useSidebar } from "../context/SidebarContext";
import { FaChevronDown, FaTimes } from "react-icons/fa";
import RegionSelect, { RegionOption } from "@/components/sidebar/RegionSelect";
import { LuLayoutDashboard } from "react-icons/lu";
import {
  useGetProfileCountriesQuery,
  useGetUploadHistoryQuery,
} from "@/lib/api/feePreviewApi";

import {
  buildPlatformOptions,
  platformToCountryName,
  PlatformId,
} from "@/lib/utils/platforms";

import { useConnectedPlatforms } from "@/lib/utils/useConnectedPlatforms";
import { useShopifyStore } from "@/lib/utils/useShopifyStore";
import { usePlatform } from "@/components/context/PlatformContext";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { buildCountryMarketplaceMap } from "@/lib/utils/countryMarketplace";

type NavSubItem = {
  name: string;
  path:
  | string
  | ((params: { ranged: string; countryName: string; month: string; year: string }) => string);
  onClick?: () => void | Promise<void>;
};


type NavSection = {
  key: string;
  name: string;
  icon: React.ReactNode;
  subItems: NavSubItem[];
};

const AppSidebar: React.FC = () => {
  const {
    isExpanded,
    isMobileOpen,
    isHovered,
    setIsHovered,
    setIsMobileOpen,
    toggleSidebar,
    toggleMobileSidebar,
  } = useSidebar();

  const pathname = usePathname();
  const router = useRouter();
  const routeParams = useParams();
  const { data: user } = useGetUserDataQuery();

  const isPreviewMode =
  routeParams?.month === "NA" &&
  routeParams?.year === "NA";

  //   useEffect(() => {
  //   if (typeof window === "undefined") return;

  //   // 🔑 country URL se aayega (uk / us / global)
  //   const countryFromRoute = routeParams?.countryName as string | undefined;

  //   // ✅ CASE 1: URL me country present hai
  //   if (countryFromRoute) {
  //     let platformFromRoute = "global";

  //     if (countryFromRoute === "uk") platformFromRoute = "amazon_uk";
  //     if (countryFromRoute === "us") platformFromRoute = "amazon_us";

  //     setSelectedPlatform(platformFromRoute);
  //     localStorage.setItem("selectedPlatform", platformFromRoute);
  //     return;
  //   }

  //   // ✅ CASE 2: URL me kuch nahi → localStorage fallback
  //   const saved = localStorage.getItem("selectedPlatform");
  //   if (saved) {
  //     setSelectedPlatform(saved);
  //   }
  // }, [routeParams]);

  // ✅ Smaller / laptop friendly typography
  const textMain = "text-[11px] sm:text-[12px] lg:text-[12.5px] xl:text-[13px]";
  const textSection =
    "text-[10px] sm:text-[11px] lg:text-[11.5px] xl:text-[12px] tracking-wide";
  const padItem = "px-2 py-1 sm:py-1.5";
  const padHeader = "px-2 py-1.5 sm:py-2";
  const iconSize = "h-[18px] w-[18px] sm:h-5 sm:w-5 lg:h-[22px] lg:w-[22px]";

  useEffect(() => {
    setIsMobileOpen(false);
  }, [pathname, setIsMobileOpen]);

  useEffect(() => {
    if (typeof document === "undefined") return;

    const originalOverflow = document.body.style.overflow;
    document.body.style.overflow = isMobileOpen ? "hidden" : originalOverflow || "";

    return () => {
      document.body.style.overflow = originalOverflow || "";
    };
  }, [isMobileOpen]);

  const { shopifyStore } = useShopifyStore();

  const handleToggle = () => {
    if (typeof window !== "undefined" && window.innerWidth >= 1024) {
      toggleSidebar();
    } else {
      toggleMobileSidebar();
    }
  };

  // ===== Data from RTK Query =====
  useGetProfileCountriesQuery();
  useGetUploadHistoryQuery();

  // ===== Platform data =====
  const connectedPlatforms = useConnectedPlatforms();
  const rawOptions: RegionOption[] =
    buildPlatformOptions(connectedPlatforms);

  const countryFromRoute = routeParams?.countryName as string | undefined;

  const regionOptions: RegionOption[] = React.useMemo(() => {
  const opts = buildPlatformOptions(connectedPlatforms);

  const countryFromRoute = routeParams?.countryName as string | undefined;

  // ✅ ONLY force Amazon country if NOT global
  if (countryFromRoute && countryFromRoute !== "global") {
    const forcedValue = `amazon-${countryFromRoute}`;

    const exists = opts.some(o => o.value === forcedValue);
    if (!exists) {
      opts.unshift({
        value: forcedValue,
        label: `Amazon ${countryFromRoute.toUpperCase()}`,
      });
    }
  }

  return opts;
}, [connectedPlatforms, routeParams?.countryName]);



  // ===== Selected platform =====

 const [selectedPlatform, setSelectedPlatform] = useState<string>(() => {
  if (isPreviewMode) return "global";

  if (!countryFromRoute || countryFromRoute === "global") {
    return "global";
  }

  return `amazon-${countryFromRoute}`;
});
  const decodeJwtUserId = (jwt: string): string | null => {
    try {
      const payloadPart = jwt.split(".")[1];
      if (!payloadPart) return null;

      const base64 = payloadPart.replace(/-/g, "+").replace(/_/g, "/");
      const json = decodeURIComponent(
        atob(base64)
          .split("")
          .map((c) => "%" + c.charCodeAt(0).toString(16).padStart(2, "0"))
          .join("")
      );

      const payload = JSON.parse(json);
      return payload?.user_id != null ? String(payload.user_id) : null;
    } catch {
      return null;
    }
  };


  // const ensureAdsSeedOnce = async (baseUrl: string, jwtToken: string) => {
  //   const userId = decodeJwtUserId(jwtToken) || "unknown";
  //   const storageKey = `ads_sp_seed_done_user_${userId}`;

  //   // Already seeded once -> do nothing
  //   if (localStorage.getItem(storageKey) === "1") return;

  //   const { start_date, end_date } = getAdsSeedDatesIST();

  //   // If it's the 1st of the month, yesterday belongs to previous month -> skip
  //   if (end_date < start_date) return;

  //   const body = {
  //     start_date,
  //     end_date,
  //     time_unit: "SUMMARY",
  //     countries: ["UK"],      // ✅ as requested
  //     return_excel: false,    // ✅ as requested
  //   };

  //   const res = await fetch(
  //     `${baseUrl}/api/ads/manager/sp_advertised_product_report`,
  //     {
  //       method: "POST",
  //       headers: {
  //         Authorization: `Bearer ${jwtToken}`,
  //         Accept: "application/json",
  //         "Content-Type": "application/json",
  //       },
  //       body: JSON.stringify(body),
  //     }
  //   );

  //   if (!res.ok) {
  //     const err = await res.json().catch(() => ({}));
  //     throw new Error(err?.error || "Failed to seed Sponsored Products report");
  //   }

  //   // Mark as done forever (per user)
  //   localStorage.setItem(storageKey, "1");
  // };

  // const monthToNumber = (monthStr: string) => {
  //   const idx = monthNames.indexOf(monthStr.toLowerCase());
  //   return idx === -1 ? null : idx + 1; // 1..12
  // };


  // const ensureAdsMonthlySeedOnce = async (
  //   baseUrl: string,
  //   jwtToken: string,
  //   monthStr: string,
  //   yearStr: string,
  //   country: string = "UK"
  // ) => {
  //   const userId = decodeJwtUserId(jwtToken) || "unknown";

  //   const monthNum = monthToNumber(monthStr);
  //   const yearNum = Number(yearStr);

  //   if (!monthNum || Number.isNaN(yearNum)) return;

  //   // ✅ run once per user+month+year+country (recommended)
  //   const storageKey = `ads_monthly_sp_seed_${userId}_${country}_${yearNum}_${monthNum}`;

  //   if (localStorage.getItem(storageKey) === "1") return;

  //   const body = {
  //     month: monthNum,
  //     year: yearNum,
  //     country,
  //   };

  //   const res = await fetch(`${baseUrl}/api/ads/monthly_sp_sd_to_db`, {
  //     method: "POST",
  //     headers: {
  //       Authorization: `Bearer ${jwtToken}`,
  //       Accept: "application/json",
  //       "Content-Type": "application/json",
  //     },
  //     body: JSON.stringify(body),
  //   });
  //   console.log("TOken", jwtToken)

  //   if (!res.ok) {
  //     const err = await res.json().catch(() => ({}));
  //     throw new Error(err?.error || "Failed to sync monthly ads data");
  //   }

  //   localStorage.setItem(storageKey, "1");
  // };


  useEffect(() => {
  // 🔥 PREVIEW MODE: always GLOBAL
  if (isPreviewMode) {
    setSelectedPlatform("global");
    setPlatformCtx("global" as PlatformId);
    localStorage.setItem("selectedPlatform", "global");
    return;
  }

  // 🔹 existing logic untouched
  const country = routeParams?.countryName as string | undefined;
if (country) {
  if (country === "global") {
    setSelectedPlatform("global");
    setPlatformCtx("global" as PlatformId);
    localStorage.setItem("selectedPlatform", "global");
  } else {
    const platform = `amazon-${country}` as PlatformId;
    setSelectedPlatform(platform);
    setPlatformCtx(platform);
    localStorage.setItem("selectedPlatform", platform);
  }
  return;
}
}, [routeParams?.countryName, isPreviewMode]);



  const monthNames = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
  ];

  const [initialPeriod] = useState(() => {
    const today = new Date();
    let ranged = "QTD";
    let month = monthNames[today.getMonth()];
    let year = String(today.getFullYear());

    if (typeof window !== "undefined") {
      try {
        const raw = localStorage.getItem("latestFetchedPeriod");
        if (raw) {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          if (parsed.month && parsed.year) {
            month = String(parsed.month).toLowerCase();
            year = String(parsed.year);
          }
        }
      } catch { }
    }
    return { ranged, month, year };
  });



  const currentCountryName =
    (routeParams?.countryName as string) || "global";

  const currentParams = {
    ranged: (routeParams?.ranged as string) || initialPeriod.ranged,
    countryName: currentCountryName,
    month: (routeParams?.month as string) || initialPeriod.month,
    year: (routeParams?.year as string) || initialPeriod.year,
  };


  const { setPlatform: setPlatformCtx } = usePlatform();

  const handleFetchAgedInventory = async () => {
    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      if (!token) {
        console.error("No auth token found");
        return;
      }

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/inventory/aged`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        }
      );

      if (!res.ok) {
        throw new Error(`API Error: ${res.status}`);
      }

      const data = await res.json();

    } catch (err) {
      console.error("Aged Inventory API Error:", err);
    }
  };

  const handleInventoryForecastFetch = async () => {
    try {
      const token =
        typeof window !== "undefined"
          ? localStorage.getItem("jwtToken")
          : null;

      if (!token) {
        console.error("No auth token found");
        return;
      }

      const baseUrl =
        process.env.NEXT_PUBLIC_API_BASE_URL;

      const months = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
      ];

      const monthIndex = months.indexOf(currentParams.month.toLowerCase());
      if (monthIndex === -1) {
        console.error("Invalid month");
        return;
      }

      const year = Number(currentParams.year);
      const lastDay = new Date(year, monthIndex + 1, 0).getDate();

      const lastDateISO = `${year}-${String(monthIndex + 1).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;

      const url =
        `${baseUrl}/amazon_api/inventory/ledger-summary` +
        `?start_date=${encodeURIComponent(lastDateISO)}` +
        `&end_date=${encodeURIComponent(lastDateISO)}` +
        `&store_in_db=true`;

      const res = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data?.error || "Ledger API failed");

      console.log("✅ Inventory Forecast API Response:", data);
    } catch (err) {
      console.error("❌ Inventory Forecast API Error:", err);
    }
  };

  // const handleConnectAmazonAds = async () => {
  //   const adsAnchorUrl = `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#advertisements`;

  //   const token =
  //     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  //   if (!token) return;

  //   const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  //   // 0) Check status first — if connected, go straight to table
  //   try {
  //     const statusRes = await fetch(`${baseUrl}/api/ads/status`, {
  //       method: "GET",
  //       headers: {
  //         Authorization: `Bearer ${token}`,
  //         Accept: "application/json",
  //       },
  //     });

  //     const statusJson = await statusRes.json();

  //     if (statusJson?.status === "connected") {
  //       router.push(adsAnchorUrl);
  //       return;
  //     }
  //   } catch {
  //     // ignore and continue to connect flow
  //   }

  //   // 1) Not connected -> open popup
  //   const popup = window.open(
  //     "about:blank",
  //     "amazonAdsConnect",
  //     "width=720,height=820,menubar=no,toolbar=no,location=yes,status=no,scrollbars=yes,resizable=yes"
  //   );

  //   try {
  //     // 2) fetch connect url
  //     const res = await fetch(`${baseUrl}/api/ads/connect_url`, {
  //       method: "GET",
  //       headers: {
  //         Authorization: `Bearer ${token}`,
  //         Accept: "application/json",
  //       },
  //     });

  //     const data = await res.json();
  //     if (!res.ok) throw new Error(data?.error || "Failed to get ads connect URL");

  //     const connectUrl = data?.url;
  //     if (!connectUrl) throw new Error("No url returned from /api/ads/connect_url");

  //     // 3) navigate popup to amazon consent
  //     if (popup && !popup.closed) {
  //       popup.location.href = connectUrl;
  //       popup.focus();
  //     } else {
  //       window.location.href = connectUrl;
  //       return;
  //     }

  //     // 4) Poll /api/ads/status until connected, then close popup + redirect
  //     const start = Date.now();
  //     const MAX_MS = 2 * 60 * 1000; // 2 minutes
  //     const INTERVAL_MS = 1500;

  //     const interval = window.setInterval(async () => {
  //       try {
  //         if (!popup || popup.closed) {
  //           window.clearInterval(interval);
  //           return;
  //         }

  //         const elapsed = Date.now() - start;
  //         if (elapsed > MAX_MS) {
  //           window.clearInterval(interval);
  //           return;
  //         }

  //         const pollRes = await fetch(`${baseUrl}/api/ads/status`, {
  //           method: "GET",
  //           headers: {
  //             Authorization: `Bearer ${token}`,
  //             Accept: "application/json",
  //           },
  //         });

  //         const pollJson = await pollRes.json();

  //         if (pollJson?.status === "connected") {
  //           window.clearInterval(interval);
  //           try {
  //             popup.close();
  //           } catch {}

  //           router.push(adsAnchorUrl);
  //         }
  //       } catch {
  //         // ignore transient errors while polling
  //       }
  //     }, INTERVAL_MS);
  //   } catch (err) {
  //     try {
  //       popup?.close();
  //     } catch {}
  //     console.error("Amazon Ads connect error:", err);
  //   }
  // };

  // const monthNames = [
  //   "january", "february", "march", "april", "may", "june",
  //   "july", "august", "september", "october", "november", "december",
  // ];

  const monthToNumber = (monthStr: string) => {
    const idx = monthNames.indexOf(monthStr.toLowerCase());
    return idx === -1 ? null : idx + 1; // 1..12
  };

  const getMonthRangeISO = (monthStr: string, yearStr: string) => {
    const m = monthToNumber(monthStr);
    const y = Number(yearStr);
    if (!m || Number.isNaN(y)) return null;

    const start = new Date(Date.UTC(y, m - 1, 1));
    const end = new Date(Date.UTC(y, m, 0)); // last day of month

    const toISO = (d: Date) =>
      `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(
        d.getUTCDate()
      ).padStart(2, "0")}`;

    return { start_date: toISO(start), end_date: toISO(end) };
  };

  const ensureSpReportSeedOnce = async (
    baseUrl: string,
    jwtToken: string,
    monthStr: string,
    yearStr: string,
    country: string = "UK"
  ) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";

    const m = monthToNumber(monthStr);
    const y = Number(yearStr);
    if (!m || Number.isNaN(y)) return;

    const storageKey = `sp_report_seed_${userId}_${country}_${y}_${m}`;
    if (localStorage.getItem(storageKey) === "1") return;

    const range = getMonthRangeISO(monthStr, yearStr);
    if (!range) return;

    const body = {
      start_date: range.start_date,
      end_date: range.end_date,
      time_unit: "SUMMARY",
      countries: [country],     // backend expects list
      return_excel: false,
    };

    const res = await fetch(`${baseUrl}/api/ads/manager/sp_advertised_product_report`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${jwtToken}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err?.error || "Failed to seed Sponsored Products report");
    }

    localStorage.setItem(storageKey, "1");
  };

  const getCurrentMonthToYesterdayRangeISO = () => {
    const today = new Date();

    // yesterday in local time
    const y = new Date(today);
    y.setDate(today.getDate() - 1);

    // If today is 1st, yesterday is previous month -> skip
    if (today.getDate() === 1) return null;

    const year = y.getFullYear();
    const month = y.getMonth(); // 0..11

    const start = new Date(year, month, 1);

    const toISO = (d: Date) =>
      `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;

    return { start_date: toISO(start), end_date: toISO(y) };
  };

  const ensureSdReportSeedOnce = async (
    baseUrl: string,
    jwtToken: string
  ) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";

    const range = getCurrentMonthToYesterdayRangeISO();
    if (!range) return;

    // ✅ once per user per month
    const ym = range.start_date.slice(0, 7); // "YYYY-MM"
    const storageKey = `sd_report_seed_${userId}_${ym}`;
    if (localStorage.getItem(storageKey) === "1") return;

    const body = {
      start_date: range.start_date,
      end_date: range.end_date,
      time_unit: "SUMMARY",
      max_wait_seconds: 20,
      poll_every_seconds: 5,
    };

    const res = await fetch(
      `${baseUrl}/api/ads/manager/sd_advertised_product_report/sync`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${jwtToken}`,
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      }
    );

    // 202 means pending in your backend logic — treat it as "seed started"
    if (res.status === 202) {
      localStorage.setItem(storageKey, "1");
      return;
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err?.error || "Failed to seed SD advertised product report");
    }

    localStorage.setItem(storageKey, "1");
  };


  // const handleConnectAmazonAds = async () => {
  //   const adsAnchorUrl = `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#advertisements`;

  //   const jwtToken =
  //     typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

  //   if (!jwtToken) return;

  //   const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

  //   const fetchAdsStatus = async () => {
  //     const res = await fetch(`${baseUrl}/api/ads/status`, {
  //       method: "GET",
  //       headers: {
  //         Authorization: `Bearer ${jwtToken}`,
  //         Accept: "application/json",
  //       },
  //     });
  //     return res.json();
  //   };

  //   // A) Already connected -> seed once -> redirect
  //   try {
  //     const statusJson = await fetchAdsStatus();
  //     if (statusJson?.status === "connected") {
  //       await ensureSpReportSeedOnce(
  //         baseUrl!,
  //         jwtToken,
  //         currentParams.month,
  //         currentParams.year,
  //         "UK"
  //       );
  //       await ensureSdReportSeedOnce(baseUrl!, jwtToken);
  //       router.push(adsAnchorUrl);
  //       return;
  //     }

  //   } catch {
  //     // ignore and continue
  //   }

  //   // B) Not connected -> open popup
  //   const popup = window.open(
  //     "about:blank",
  //     "amazonAdsConnect",
  //     "width=720,height=820,menubar=no,toolbar=no,location=yes,status=no,scrollbars=yes,resizable=yes"
  //   );

  //   try {
  //     // get connect URL
  //     const res = await fetch(`${baseUrl}/api/ads/connect_url`, {
  //       method: "GET",
  //       headers: {
  //         Authorization: `Bearer ${jwtToken}`,
  //         Accept: "application/json",
  //       },
  //     });

  //     const data = await res.json();
  //     if (!res.ok) throw new Error(data?.error || "Failed to get ads connect URL");

  //     const connectUrl = data?.url;
  //     if (!connectUrl) throw new Error("No url returned from /api/ads/connect_url");

  //     // navigate popup
  //     if (popup && !popup.closed) {
  //       popup.location.href = connectUrl;
  //       popup.focus();
  //     } else {
  //       window.location.href = connectUrl;
  //       return;
  //     }

  //     // poll status until connected -> seed once -> close popup -> redirect
  //     const start = Date.now();
  //     const MAX_MS = 2 * 60 * 1000;
  //     const INTERVAL_MS = 1500;

  //     const interval = window.setInterval(async () => {
  //       try {
  //         const elapsed = Date.now() - start;

  //         if (!popup || popup.closed) {
  //           window.clearInterval(interval);
  //           return;
  //         }

  //         if (elapsed > MAX_MS) {
  //           window.clearInterval(interval);
  //           return;
  //         }

  //         const pollJson = await fetchAdsStatus();
  //         if (pollJson?.status === "connected") {
  //           window.clearInterval(interval);

  //           await ensureSpReportSeedOnce(
  //             baseUrl!,
  //             jwtToken,
  //             currentParams.month,
  //             currentParams.year,
  //             "UK"
  //           );
  //           await ensureSdReportSeedOnce(baseUrl!, jwtToken);
  //           try {
  //             popup.close();
  //           } catch { }

  //           router.push(adsAnchorUrl);
  //         }
  //       } catch {
  //         // ignore transient errors
  //       }
  //     }, INTERVAL_MS);
  //   } catch (err) {
  //     try {
  //       popup?.close();
  //     } catch { }
  //     console.error("Amazon Ads connect error:", err);
  //   }
  // };


  const onRegionChange = (val: string) => {
    const platform = val as PlatformId;

    if (val === "add_more_countries") {
      router.push("/settings/countries");
      return;
    }

    setSelectedPlatform(val);
    setPlatformCtx(platform);

    if (platform === "shopify") {
      if (shopifyStore?.shop && shopifyStore?.token && shopifyStore?.email) {
        const params = new URLSearchParams({
          shop: shopifyStore.shop,
          token: shopifyStore.token,
          email: shopifyStore.email,
        });
        router.push(`/orders?${params.toString()}`);
      } else {
        router.push("/orders");
      }
      return;
    }

    if (typeof window !== "undefined") {
      localStorage.setItem("selectedPlatform", val);
      localStorage.removeItem("chatHistory");
    }

    const newCountryName = platformToCountryName(platform);
    const segments = pathname.split("/").filter(Boolean);
    const params: any = routeParams;

    let newPath: string | null = null;
    const ranged = (params.ranged as string) || currentParams.ranged;
    const month = (params.month as string) || currentParams.month;
    const year = (params.year as string) || currentParams.year;

    if ((params as any).countryName || segments[0] === "country") {
      switch (segments[0]) {
        case "pnl-dashboard":
          newPath = `/pnl-dashboard/${ranged}/${newCountryName}/${month}/${year}`;
          break;
        case "live-business-insight":
          newPath = `/live-business-insight/${ranged}/${newCountryName}/${month}/${year}`;
          break;
        case "ai-insight":
          newPath = `/ai-insight/${ranged}/${newCountryName}/${month}/${year}`;
          break;
        case "chatbot":
          newPath = `/chatbot/${ranged}/${newCountryName}/${month}/${year}`;
          break;
      }
    }

    if (!newPath) {
      switch (segments[0]) {
        case "inventory-forecast":
          newPath = `/inventory-forecast/${newCountryName}/${month}/${year}`;
          break;
        case "pnlforecast":
          newPath = `/pnlforecast/${newCountryName}/${month}/${year}`;
          break;
        case "inventory-reconciliation":
          newPath = `/inventory-reconciliation/${newCountryName}/${month}/${year}`;
          break;
        case "currentInventory":
          newPath = `/currentInventory/${newCountryName}/${month}/${year}`;
          break;
        case "dispatch":
          newPath = `/dispatch/${newCountryName}/${month}/${year}`;
          break;
        case "purchase-order":
          newPath = `/purchase-order/${newCountryName}/${month}/${year}`;
          break;
        case "cashflow":
          newPath = `/cashflow/${newCountryName}/${month}/${year}`;
          break;
        case "expense-reconciliation":
          newPath = `/expense-reconciliation/${newCountryName}/${month}/${year}`;
          break;
        case "fba":
          newPath = `/fba/${newCountryName}/${month}/${year}`;
          break;
        case "skuwiseprofit": {
          const productname = segments[1] ?? "Classic";
          newPath = `/skuwiseprofit/${productname}/${newCountryName}/${month}/${year}`;
          break;
        }
      }
    }

    if (newPath && newPath !== pathname) router.push(newPath);
  };

  const sections: NavSection[] = [
    // A) Live Dashboard (real-time page sections only)
    {
      key: "live-dashboard",
      name: "LIVE DASHBOARD",
      icon: (
        <Image
          src="/images/brand/live.png"
          alt="Logo"
          width={18}
          height={18}
          className="w-[16px] h-[16px] sm:w-[18px] sm:h-[18px] lg:w-[20px] lg:h-[20px]"
        />
      ),
      subItems: [
        {
          name: "Live Sales",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#live-sales`,
        },
        {
          name: "Targets and Action Items",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#targets-action-items`,
        },
        {
          name: "MTD P&L",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#mtd-pl`,
        },
        {
          name: "P&L Productwise Breakdown",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#pnl-mtd`,
        },
        {
          name: "Current Inventory",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#current-inventory`,
          onClick: handleFetchAgedInventory,
        },
        // {
        //   name: "Advertisements",
        //   path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#advertisements`,
        //   // onClick: handleConnectAmazonAds,
        // },

      ],
    },

    // B) Finance Dashboards
    {
      key: "finance-dashboards",
      name: "FINANCE DASHBOARDS",
      icon: <LuLayoutDashboard className={iconSize} />,
      subItems: [
        {
          name: "P&L Dashboard",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${ranged}/${countryName}/${month}/${year}`,
        },
        {
          name: "Business Summary",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${encodeURIComponent(ranged)}/${encodeURIComponent(
              countryName
            )}/${encodeURIComponent(month)}/${encodeURIComponent(year)}#business-summary`,
        },

        {
          name: "Cash Flow",
          path: ({ countryName, month, year }) =>
            `/cashflow/${encodeURIComponent(countryName)}/${encodeURIComponent(
              month
            )}/${encodeURIComponent(year)}`,
        },
        {
          name: "SKU wise Profit",
          path: (params: {
            productname?: string;
            countryName: string;
            month: string;
            year: string;
          }) =>
            `/skuwiseprofit/${params.productname ?? "Classic"}/${params.countryName}/${params.month}/${params.year}`,
        },
        {
          name: "Expense Reconcilliation",
          path: ({ countryName, month, year }) =>
            `/expense-reconciliation/${encodeURIComponent(countryName)}/${encodeURIComponent(month)}/${encodeURIComponent(year)}` // your Amazon/Referral Fees page
        },
      ],
    },

    // C) Business Intelligence
    {
      key: "business-intelligence",
      name: "BUSINESS INTELLIGENCE",
      icon: (
        <Image
          src="/images/brand/business.png"
          alt="Logo"
          width={18}
          height={18}
          className="w-[16px] h-[16px] sm:w-[18px] sm:h-[18px] lg:w-[20px] lg:h-[20px]"
        />
      ),
      subItems: [
        {
          name: "AI Insights",
          path: `/ai-insight/${currentParams.ranged}/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
        },
        {
          name: "Chatbot",
          path: `/chatbot/${currentParams.ranged}/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
        },
        {
          name: "Inventory Forecast",
          path: `/inventory-forecast/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
          onClick: handleInventoryForecastFetch,
        },
        {
          name: "P&L Forecast",
          path: `/pnlforecast/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
        },
      ],
    },

    // D) Inventory Planning
    {
      key: "inventory-planning",
      name: "INVENTORY PLANNING",
      icon: (
        <Image
          src="/images/brand/inventory.png"
          alt="Logo"
          width={18}
          height={18}
          className="w-[16px] h-[16px] sm:w-[18px] sm:h-[18px] lg:w-[20px] lg:h-[20px]"
        />
      ),
      subItems: [
        {
          name: "Inventory Reconcilliation",
          path: `/inventory-reconciliation/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`, // ✅ Current Inventory
        },

        {
          name: "Dispatch Planning",
          path: `/dispatch/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
        },
        {
          name: "Purchase Order (PO) Planning",
          path: `/purchase-order/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
        },
      ],
    },
  ];


  // const [openSections, setOpenSections] = useState<Record<string, boolean>>({
  //   "Live Analytics": true,
  //   dashboard: true,
  //   "business-intelligence": true,
  //   inventory: true,
  //   recon: true,
  //   integrations: false,
  // });

  const [openSections, setOpenSections] = useState<Record<string, boolean>>({
    "live-dashboard": true,
    "finance-dashboards": true,
    "business-intelligence": true,
    "inventory-planning": true,
  });


  const toggleSection = useCallback((key: string) => {
    setOpenSections((prev) => ({ ...prev, [key]: !prev[key] }));
  }, []);

  const isActive = useCallback(
    (path: string | ((params: typeof currentParams) => string)) => {
      const resolvedPath = typeof path === "function" ? path(currentParams) : path;
      return pathname === resolvedPath;
    },
    [pathname, currentParams]
  );

  const showText = isExpanded || isMobileOpen;

  const safeSelectedPlatform =
    regionOptions.find((o) => o.value === selectedPlatform)?.value ??
    regionOptions[0]?.value ??
    "global";

  return (
    <aside
      className={`fixed mt-16 flex flex-col lg:mt-0 top-0 left-0 bg-white text-gray-900 h-screen overflow-y-auto transition-all duration-300 ease-in-out z-[1100]
        px-3 sm:px-4 lg:px-3 xl:px-4
        ${isMobileOpen
          ? "w-full"
          : showText
            ? "w-[clamp(155px,13vw,210px)] xl:w-[clamp(180px,16vw,250px)]"
            : "w-[56px] sm:w-[64px] xl:w-[72px]"
        }
        ${isMobileOpen ? "translate-x-0" : "-translate-x-full"}
        lg:translate-x-0 font-lato
      `}
    >
      {/* Logo + toggle */}
      <div
  className={`py-4 sm:py-5 lg:py-6 flex items-center justify-between`}
>
  {/* Logo */}
  <Link
    href={`/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`}
    className="flex items-center gap-2"
  >
    {showText ? (
      <Image
        className="dark:hidden hidden lg:block"
        src="/images/logo/Logo_Phormula.png"
        alt="Logo"
        width={132}
        height={36}
      />
    ) : null}
  </Link>

  {/* 🔥 TOGGLE BUTTON (always visible) */}
  <button
    type="button"
    onClick={handleToggle}
    className="flex items-center justify-center w-8 h-8 lg:w-9 lg:h-9 rounded-lg border border-gray-200"
    aria-label="Toggle sidebar"
  >
    {showText ? (
      <Image
        src="/images/icons/sidebarin.png"
        alt="Close sidebar"
        width={20}
        height={20}
      />
    ) : (
      <Image
        src="/images/icons/hamburger.png"
        alt="Open sidebar"
        width={20}
        height={20}
      />
    )}
  </button>
</div>


      {/* Platform Select */}
      {showText && regionOptions.length > 0 && (
        <RegionSelect
          label="Platform"
          selectedCountry={safeSelectedPlatform}   // ✅ yahin use
          options={regionOptions}
          onChange={onRegionChange}
          className={`mb-2 rounded bg-transparent text-gray-800 focus:outline-none focus:ring-1 focus:ring-[#5EA68E]
      px-2 py-1 ${textMain}`}
        />
      )}

      {/* Navigation Sections */}
      <div className="flex flex-col overflow-y-auto duration-300 ease-linear no-scrollbar">
        <nav className="mb-6">
          <div className="flex flex-col gap-1">
            {sections.map((section) => {
              const resolvedSubPaths = section.subItems.map((sub) =>
                typeof sub.path === "function" ? sub.path(currentParams) : sub.path
              );
              const isSectionActive = resolvedSubPaths.some((p) =>
                isActive(p as any)
              );

              return (
                <div key={section.key} className="flex flex-col">
                  <button
                    onClick={() => toggleSection(section.key)}
                    className={`w-full rounded hover:bg-[#5EA68E]/15 transition-colors cursor-pointer group
                      ${padHeader} ${textSection} text-left text-[#5EA68E] font-semibold
                      flex items-center ${showText ? "justify-between" : "justify-center"
                      }
                      ${isSectionActive ? "bg-[#5EA68E]/10" : ""}`}
                  >
                    <div className="flex items-center">
                      {section.icon}
                      {showText && <span className="ml-2">{section.name}</span>}
                    </div>

                    {showText && (
                      <FaChevronDown
                        className={`h-3 w-3 sm:h-3.5 sm:w-3.5 transition-transform duration-200 ${openSections[section.key] ? "rotate-0" : "rotate-90"
                          }`}
                      />
                    )}
                  </button>
                  {openSections[section.key] && showText && (
                    <div className="ml-4 sm:ml-5 lg:ml-6 mt-1 space-y-1 overflow-hidden">
                      {section.subItems.map((subItem, idx) => {
                        const resolvedPath =
                          typeof subItem.path === "function"
                            ? (subItem.path as any)(currentParams)
                            : subItem.path;

                        return (
                          <Link
                            key={idx}
                            href={resolvedPath}
                            // onClick={() => subItem.onClick?.()}
                            onClick={async (e) => {
                              if (!subItem.onClick) return;
                              e.preventDefault();
                              try {
                                await subItem.onClick();
                              } catch (err) {
                                console.error(err);
                                router.push(resolvedPath); // optional fallback
                              }
                            }}

                            className={`block rounded transition-colors
                              ${padItem} ${textMain} text-gray-700 hover:bg-[#5EA68E]/15
                              ${isActive(subItem.path as any)
                                ? "bg-[#5EA68E]/20 text-[#5EA68E] font-medium"
                                : ""
                              }`}
                          >
                            {subItem.name}
                          </Link>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </nav>
      </div>

      {isMobileOpen && (
        <button
          onClick={() => setIsMobileOpen(false)}
          className="absolute top-4 right-4 text-gray-500 hover:text-gray-700 md:hidden"
        >
          <FaTimes className="h-5 w-5" />
        </button>
      )}
    </aside>
  );
};

export default AppSidebar;
