"use client";

import React, { useState, useEffect, useCallback } from "react";
import Link from "next/link";
import Image from "next/image";
import {
  useParams,
  usePathname,
  useRouter,
  useSearchParams,
} from "next/navigation";
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
import { useAppSelector } from "@/lib/hooks";
import { FiChevronRight } from "react-icons/fi";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/animate-ui/primitives/radix/collapsible";

type NavSubItem = {
  name: string;
  path:
  | string
  | ((
    params: {
      ranged: string;
      countryName: string;
      month: string;
      year: string;
    }
  ) => string);
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
  const searchParams = useSearchParams();


  const [currentHash, setCurrentHash] = useState("");

  useEffect(() => {
    const updateHash = () => {
      if (typeof window === "undefined") return;

      const hash = window.location.hash || "";
      setCurrentHash(hash);
    };

    updateHash();

    window.addEventListener("hashchange", updateHash);
    window.addEventListener("page-hash-navigate", updateHash as EventListener);

    return () => {
      window.removeEventListener("hashchange", updateHash);
      window.removeEventListener(
        "page-hash-navigate",
        updateHash as EventListener
      );
    };
  }, [pathname]);

  const [lastNonGlobalPlatform, setLastNonGlobalPlatform] = useState<
    string | null
  >(null);

  const authUser = useAppSelector((s: any) => s.auth.user);
  const token = useAppSelector((s: any) => s.auth.token);

  const getJwtPayload = (jwt?: string | null) => {
    try {
      if (!jwt) return null;

      const payloadPart = jwt.split(".")[1];

      if (!payloadPart) return null;

      const base64 = payloadPart.replace(/-/g, "+").replace(/_/g, "/");

      return JSON.parse(atob(base64));
    } catch {
      return null;
    }
  };

  const tokenPayload = React.useMemo(() => getJwtPayload(token), [token]);

  const isMember = !!authUser?.is_member || tokenPayload?.is_member === true;

  const { data: userData } = useGetUserDataQuery(undefined, {
    skip: !token,
    refetchOnMountOrArgChange: true,
    refetchOnFocus: true,
    refetchOnReconnect: true,
  });

  const countryAccess: Record<string, string[]> = React.useMemo(() => {
    const freshAccess = (userData as any)?.country_access;
    const authAccess = authUser?.country_access;
    const tokenAccess = tokenPayload?.country_access;

    const rawAccess =
      freshAccess && typeof freshAccess === "object"
        ? freshAccess
        : authAccess && typeof authAccess === "object"
          ? authAccess
          : tokenAccess && typeof tokenAccess === "object"
            ? tokenAccess
            : {};

    return Object.fromEntries(
      Object.entries(rawAccess).map(([country, modules]) => [
        String(country).toUpperCase(),
        Array.isArray(modules) ? modules.map(String) : [],
      ])
    );
  }, [userData, authUser?.country_access, tokenPayload?.country_access]);

  const currentCountry = String(
    routeParams?.countryName || "global"
  ).toUpperCase();

  const allowedModulesForCurrentCountry = React.useMemo(() => {
    if (!isMember) return [];

    if (currentCountry === "GLOBAL") {
      return Array.from(new Set(Object.values(countryAccess).flat()));
    }

    return countryAccess[currentCountry] || [];
  }, [isMember, currentCountry, countryAccess]);

  const user = userData;

  const isPreviewMode =
    routeParams?.month === "NA" && routeParams?.year === "NA";

  const textMain =
    "text-[11px] sm:text-[12px] lg:text-[12.5px] xl:text-[12px] 2xl:text-[13px]";
  const textSection =
    "text-[10px] sm:text-[11px] lg:text-[11.5px] xl:text-[13px] 2xl:text-[14px] tracking-wide";
  const padItem = "px-2 py-1 sm:py-1.5";
  const padHeader = "px-2 py-1.5 sm:py-2";
  const iconSize =
    "h-[18px] w-[18px] sm:h-5 sm:w-5 lg:h-[22px] lg:w-[22px]";

  useEffect(() => {
    setIsMobileOpen(false);
  }, [pathname, setIsMobileOpen]);

  useEffect(() => {
    if (typeof document === "undefined") return;

    const originalOverflow = document.body.style.overflow;

    document.body.style.overflow = isMobileOpen
      ? "hidden"
      : originalOverflow || "";

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

  useGetProfileCountriesQuery(undefined, { skip: !token || isMember });
  useGetUploadHistoryQuery(undefined, { skip: !token || isMember });

  const connectedPlatforms = useConnectedPlatforms();

  const countryFromRoute = routeParams?.countryName as string | undefined;

  const regionOptions: RegionOption[] = React.useMemo(() => {
    const opts = buildPlatformOptions(connectedPlatforms);
    const countryFromRoute = routeParams?.countryName as string | undefined;

    const ensureOption = (value: string) => {
      if (!value.startsWith("amazon-")) return;
      if (opts.some((o) => o.value === value)) return;

      const country = value.replace("amazon-", "").toUpperCase();

      opts.unshift({
        value,
        label: `Amazon ${country}`,
      });
    };

    if (countryFromRoute && countryFromRoute !== "global") {
      ensureOption(`amazon-${countryFromRoute}`);
    }

    if (lastNonGlobalPlatform) {
      ensureOption(lastNonGlobalPlatform);
    }

    return opts;
  }, [connectedPlatforms, routeParams?.countryName, lastNonGlobalPlatform]);

  const allowedCountriesForMember = React.useMemo(() => {
    return Object.keys(countryAccess).map((country) => country.toLowerCase());
  }, [countryAccess]);

  const filteredRegionOptions = React.useMemo(() => {
    if (!isMember) return regionOptions;

    return regionOptions.filter((option) => {
      if (!option.value.startsWith("amazon-")) return false;

      const country = option.value.replace("amazon-", "").toLowerCase();

      return allowedCountriesForMember.includes(country);
    });
  }, [isMember, regionOptions, allowedCountriesForMember]);

  const [selectedPlatform, setSelectedPlatform] = useState<string>(() => {
    if (isPreviewMode) return "global";

    if (!countryFromRoute || countryFromRoute === "global") {
      return "global";
    }

    return `amazon-${countryFromRoute}`;
  });

  useEffect(() => {
    if (typeof window === "undefined") return;

    const saved = localStorage.getItem("lastNonGlobalPlatform");

    if (saved && saved.startsWith("amazon-")) {
      setLastNonGlobalPlatform(saved);
    }
  }, []);

  useEffect(() => {
    if (
      selectedPlatform &&
      selectedPlatform !== "global" &&
      selectedPlatform !== "shopify" &&
      selectedPlatform.startsWith("amazon-")
    ) {
      setLastNonGlobalPlatform(selectedPlatform);
      localStorage.setItem("lastNonGlobalPlatform", selectedPlatform);
    }
  }, [selectedPlatform]);

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

      return payload?.user_id != null
        ? String(payload.user_id)
        : payload?.owner_user_id != null
          ? String(payload.owner_user_id)
          : null;
    } catch {
      return null;
    }
  };

  const { setPlatform: setPlatformCtx } = usePlatform();

  useEffect(() => {
    if (isPreviewMode) {
      setSelectedPlatform("global");
      setPlatformCtx("global" as PlatformId);
      localStorage.setItem("selectedPlatform", "global");
      return;
    }

    const country = routeParams?.countryName as string | undefined;

    if (!country) return;

    if (country === "global") {
      if (isMember && filteredRegionOptions.length > 0) {
        const firstAllowedPlatform =
          filteredRegionOptions[0].value as PlatformId;

        setSelectedPlatform(firstAllowedPlatform);
        setPlatformCtx(firstAllowedPlatform);
        localStorage.setItem("selectedPlatform", firstAllowedPlatform);
        return;
      }

      setSelectedPlatform("global");
      setPlatformCtx("global" as PlatformId);
      localStorage.setItem("selectedPlatform", "global");
      return;
    }

    const platform = `amazon-${country}` as PlatformId;

    if (isMember) {
      const isAllowed = filteredRegionOptions.some(
        (option) => option.value === platform
      );

      if (!isAllowed && filteredRegionOptions.length > 0) {
        const firstAllowedPlatform =
          filteredRegionOptions[0].value as PlatformId;

        setSelectedPlatform(firstAllowedPlatform);
        setPlatformCtx(firstAllowedPlatform);
        localStorage.setItem("selectedPlatform", firstAllowedPlatform);
        return;
      }
    }

    setSelectedPlatform(platform);
    setPlatformCtx(platform);
    localStorage.setItem("selectedPlatform", platform);
  }, [
    routeParams?.countryName,
    isPreviewMode,
    setPlatformCtx,
    isMember,
    filteredRegionOptions,
  ]);

  const monthNames = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
  ];

  const capitalizeMonth = (month?: string) => {
    if (!month) return "";

    const normalized = String(month).trim().toLowerCase();

    if (normalized === "na") return "NA";

    return normalized.charAt(0).toUpperCase() + normalized.slice(1);
  };

  const [initialPeriod] = useState(() => {
    const today = new Date();
    const currentYear = String(today.getFullYear());

    let ranged = "YTD";
    let month = monthNames[today.getMonth()];
    let year = currentYear;

    if (typeof window !== "undefined") {
      try {
        const raw = localStorage.getItem("latestFetchedPeriod");

        if (raw) {
          const parsed = JSON.parse(raw) as { month?: string; year?: string };
          const savedYear = String(parsed?.year || "").trim();

          if (parsed.month && savedYear === currentYear) {
            month = String(parsed.month).toLowerCase();
            year = currentYear;
          } else if (savedYear && savedYear !== currentYear) {
            localStorage.removeItem("latestFetchedPeriod");
          }
        }
      } catch {
        localStorage.removeItem("latestFetchedPeriod");
      }
    }

    return { ranged, month, year };
  });

  const currentCountryName =
    (routeParams?.countryName as string) || "global";

  const currentParams = {
    ranged: (routeParams?.ranged as string) || initialPeriod.ranged,
    countryName: currentCountryName,
    month: capitalizeMonth(
      (routeParams?.month as string) || initialPeriod.month
    ),
    year: (routeParams?.year as string) || initialPeriod.year,
  };

  useEffect(() => {
    const routeMonth = routeParams?.month as string | undefined;

    if (!routeMonth) return;
    if (routeMonth.toUpperCase() === "NA") return;

    const capitalizedMonth = capitalizeMonth(routeMonth);

    if (routeMonth === capitalizedMonth) return;

    const segments = pathname.split("/").filter(Boolean);

    const monthIndex = segments.findIndex(
      (segment) => segment.toLowerCase() === routeMonth.toLowerCase()
    );

    if (monthIndex === -1) return;

    segments[monthIndex] = capitalizedMonth;

    const queryString = searchParams.toString();
    const nextUrl = `/${segments.join("/")}${queryString ? `?${queryString}` : ""}`;

    router.replace(nextUrl, { scroll: false });
  }, [routeParams?.month, pathname, router, searchParams]);

  useEffect(() => {
    if (!isMember) return;
    if (!filteredRegionOptions.length) return;

    const currentCountry = String(
      routeParams?.countryName || ""
    ).toLowerCase();

    if (!currentCountry || currentCountry === "global") return;

    const isAllowedCountry =
      allowedCountriesForMember.includes(currentCountry);

    if (isAllowedCountry) return;

    const firstAllowedCountry = filteredRegionOptions[0].value.replace(
      "amazon-",
      ""
    );

    const segments = pathname.split("/").filter(Boolean);

    const ranged = (routeParams?.ranged as string) || currentParams.ranged;
    const month = currentParams.month;
    const year = (routeParams?.year as string) || currentParams.year;

    let redirectPath: string | null = null;

    switch (segments[0]) {
      case "live-dashboard":
        redirectPath = `/live-dashboard/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "pnl-dashboard":
        redirectPath = `/pnl-dashboard/${ranged}/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "live-business-insight":
        redirectPath = `/live-business-insight/${ranged}/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "ai-insight":
        redirectPath = `/ai-insight/${ranged}/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "chatbot":
        redirectPath = `/chatbot/${ranged}/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "inventory-forecast":
        redirectPath = `/inventory-forecast/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "pnlforecast":
        redirectPath = `/pnlforecast/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "inventory-reconciliation":
        redirectPath = `/inventory-reconciliation/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "currentInventory":
        redirectPath = `/currentInventory/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "purchase-order":
        redirectPath = `/purchase-order/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "cashflow":
        redirectPath = `/cashflow/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "expense-reconciliation":
        redirectPath = `/expense-reconciliation/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "fba":
        redirectPath = `/fba/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "inputCost":
        redirectPath = `/inputCost/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "objectives-targets":
        redirectPath = `/objectives-targets/${firstAllowedCountry}/${month}/${year}`;
        break;

      case "skuwiseprofit": {
        const productname = segments[1] ?? "Classic";
        redirectPath = `/skuwiseprofit/${productname}/${firstAllowedCountry}/${month}/${year}`;
        break;
      }
    }

    if (redirectPath && redirectPath !== pathname) {
      const queryString = searchParams.toString();
      const nextUrl = `${redirectPath}${queryString ? `?${queryString}` : ""}`;

      router.replace(nextUrl, { scroll: false });
    }
  }, [
    isMember,
    filteredRegionOptions,
    allowedCountriesForMember,
    routeParams?.countryName,
    routeParams?.ranged,
    routeParams?.month,
    routeParams?.year,
    searchParams,
    pathname,
    router,
    currentParams.ranged,
    currentParams.month,
    currentParams.year,
  ]);

  const monthToNumber = (monthStr: string) => {
    const idx = monthNames.indexOf(monthStr.toLowerCase());

    return idx === -1 ? null : idx + 1;
  };

  const getMonthRangeISO = (monthStr: string, yearStr: string) => {
    const m = monthToNumber(monthStr);
    const y = Number(yearStr);

    if (!m || Number.isNaN(y)) return null;

    const start = new Date(Date.UTC(y, m - 1, 1));
    const end = new Date(Date.UTC(y, m, 0));

    const toISO = (d: Date) =>
      `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(
        2,
        "0"
      )}-${String(d.getUTCDate()).padStart(2, "0")}`;

    return { start_date: toISO(start), end_date: toISO(end) };
  };

  const getCurrentMonthYear = () => {
    const now = new Date();

    return {
      month: monthNames[now.getMonth()],
      year: String(now.getFullYear()),
    };
  };

  const currentMonthYear = getCurrentMonthYear();

  const getNextMonthYearFromRoute = (month: string, year: string) => {
    const idx = monthNames.indexOf(String(month).toLowerCase());
    const y = Number(year);

    if (idx === -1 || Number.isNaN(y)) {
      return { month, year };
    }

    const nextIdx = (idx + 1) % 12;
    const nextYear = idx === 11 ? String(y + 1) : String(y);

    return {
      month: monthNames[nextIdx],
      year: nextYear,
    };
  };

  const forecastParams = getNextMonthYearFromRoute(
    currentParams.month,
    currentParams.year
  );

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
      countries: [country],
      return_excel: false,
    };

    const res = await fetch(
      `${baseUrl}/api/ads/manager/sp_advertised_product_report`,
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

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));

      throw new Error(
        err?.error || "Failed to seed Sponsored Products report"
      );
    }

    localStorage.setItem(storageKey, "1");
  };

  const getCurrentMonthToYesterdayRangeISO = () => {
    const today = new Date();

    const y = new Date(today);
    y.setDate(today.getDate() - 1);

    if (today.getDate() === 1) return null;

    const year = y.getFullYear();
    const month = y.getMonth();

    const start = new Date(year, month, 1);

    const toISO = (d: Date) =>
      `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(
        2,
        "0"
      )}-${String(d.getDate()).padStart(2, "0")}`;

    return { start_date: toISO(start), end_date: toISO(y) };
  };

  const ensureSdReportSeedOnce = async (
    baseUrl: string,
    jwtToken: string
  ) => {
    const userId = decodeJwtUserId(jwtToken) || "unknown";

    const range = getCurrentMonthToYesterdayRangeISO();

    if (!range) return;

    const ym = range.start_date.slice(0, 7);
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

    if (res.status === 202) {
      localStorage.setItem(storageKey, "1");
      return;
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));

      throw new Error(
        err?.error || "Failed to seed SD advertised product report"
      );
    }

    localStorage.setItem(storageKey, "1");
  };

  const triggerPurchaseOrderApi = async (
    country: string,
    month: string,
    year: string
  ) => {
    const jwtToken =
      token ||
      (typeof window !== "undefined"
        ? localStorage.getItem("jwtToken")
        : null);

    if (!jwtToken) throw new Error("Missing jwt token");

    const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL;

    if (!baseUrl) throw new Error("Missing NEXT_PUBLIC_API_BASE_URL");

    const safeMonth =
      month.charAt(0).toUpperCase() + month.slice(1).toLowerCase();

    const formData = new FormData();

    formData.append("month", safeMonth);
    formData.append("year", year);
    formData.append("country", country.toLowerCase());

    const res = await fetch(`${baseUrl}/purchase_order`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${jwtToken}`,
        Accept: "application/json",
      },
      body: formData,
    });

    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(json?.error || "Purchase order API failed");
    }

    return json;
  };

  const getTopRouteForMemberCountry = React.useCallback(
    (countryName: string) => {
      const countryCode = countryName.toUpperCase();
      const allowedModules = countryAccess[countryCode] || [];

      const ranged = currentParams.ranged;
      const month = currentParams.month;
      const year = currentParams.year;

      if (allowedModules.includes("LIVE_DASHBOARD")) {
        return `/live-dashboard/${countryName}/${month}/${year}#live-sales`;
      }

      if (allowedModules.includes("FINANCE_DASHBOARDS")) {
        return `/pnl-dashboard/${ranged}/${countryName}/${month}/${year}#finance-dashboard`;
      }

      if (allowedModules.includes("BUSINESS_INTELLIGENCE")) {
        return `/ai-insight/${ranged}/${countryName}/${month}/${year}`;
      }

      if (allowedModules.includes("INVENTORY_PLANNING")) {
        return `/inputCost/${countryName}/${month}/${year}`;
      }

      return null;
    },
    [
      countryAccess,
      currentParams.ranged,
      currentParams.month,
      currentParams.year,
    ]
  );

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

    if (isMember) {
      const topRoute = getTopRouteForMemberCountry(newCountryName);

      if (topRoute) {
        router.push(topRoute);
      }

      return;
    }

    const segments = pathname.split("/").filter(Boolean);
    const params: any = routeParams;

    let newPath: string | null = null;

    const ranged = (params.ranged as string) || currentParams.ranged;
    const month = currentParams.month;
    const year = (params.year as string) || currentParams.year;

    if ((params as any).countryName || segments[0] === "country") {
      switch (segments[0]) {
        case "live-dashboard":
          newPath = `/live-dashboard/${newCountryName}/${month}/${year}`;
          break;

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

        case "dispatch": {
          const { month: currentMonth, year: currentYear } =
            getCurrentMonthYear();

          newPath = `/dispatch/${newCountryName}/${currentMonth}/${currentYear}`;
          break;
        }

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

        case "inputCost":
          newPath = `/inputCost/${newCountryName}/${month}/${year}`;
          break;

        case "objectives-targets":
          newPath = `/objectives-targets/${newCountryName}/${month}/${year}`;
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
          name: "AI Insights",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#ai-insights`,
        },
        {
          name: "MTD Sales",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#live-sales`,
        },
        {
          name: "P&L Breakdown",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#pnl-mtd`,
        },
        {
          name: "Inventory Insights",
          path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#inventory-insights`,
        },
      ],
    },
    {
      key: "finance-dashboards",
      name: "FINANCIAL METRICS",
      icon: <LuLayoutDashboard className={iconSize} />,
      subItems: [
        {
          name: "AI Insights",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${encodeURIComponent(
              ranged
            )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
              month
            )}/${encodeURIComponent(year)}#ai-insights`,
        },
        {
          name: "Finance Dashboard",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${encodeURIComponent(
              ranged
            )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
              month
            )}/${encodeURIComponent(year)}#finance-dashboard`,
        },
        {
          name: "P&L Breakdown",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${encodeURIComponent(
              ranged
            )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
              month
            )}/${encodeURIComponent(year)}#pnl-breakdown`,
        },
        {
          name: "Cash Flow",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${encodeURIComponent(
              ranged
            )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
              month
            )}/${encodeURIComponent(year)}#cash-flow`,
        },
        {
          name: "SKU Journey",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${encodeURIComponent(
              ranged
            )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
              month
            )}/${encodeURIComponent(year)}#skuwise-profit`,
        },
        {
          name: "Inventory Insights",
          path: ({ ranged, countryName, month, year }) =>
            `/pnl-dashboard/${encodeURIComponent(
              ranged
            )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
              month
            )}/${encodeURIComponent(year)}#inventory-insights`,
        },
      ],
    },
    {
  key: "business-intelligence",
  name: "INVENTORY PLANNING",
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
    // {
    //   name: "AI Insights",
    //   path: `/ai-insight/${currentParams.ranged}/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
    // },
    {
      name: "Inventory Forecast",
      path: ({ countryName, month, year }) =>
        `/inventory-forecast/${encodeURIComponent(
          countryName
        )}/${encodeURIComponent(month)}/${encodeURIComponent(
          year
        )}#inventory-forecast`,
    },
        {
          name: "Dispatch Planning",
          path: ({ countryName, month, year }) =>
            `/inventory-forecast/${encodeURIComponent(
              countryName
            )}/${encodeURIComponent(month)}/${encodeURIComponent(
              year
            )}#dispatch`,
        },
        {
          name: "Purchase Order (PO) Planning",
          path: ({ countryName, month, year }) =>
            `/inventory-forecast/${encodeURIComponent(
              countryName
            )}/${encodeURIComponent(month)}/${encodeURIComponent(
              year
            )}#purchase-order`,
        },
        {
          name: "P&L Forecast",
          path: ({ countryName, month, year }) =>
            `/pnlforecast/${encodeURIComponent(
              countryName
            )}/${encodeURIComponent(month)}/${encodeURIComponent(
              year
            )}#purchase-order`,
        },
      ],
    },
    {
  key: "inventory-planning",
  name: "CURRENT INVENTORY",
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
    name: "Inventory Insights",
    path: `/inputCost/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#inventory-insights`,
  },
  {
    name: "SKU Information",
    path: `/inputCost/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#sku-info`,
  },
   {
    name: "Upload Warehouse Data",
    path: `/inputCost/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#extra`,
  },
  {
    name: "Recon Table",
    path: `/inputCost/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#recon-table`,
  },
  {
    name: "Lost vs Compensation",
    path: `/inputCost/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#lost-compensation`,
  },
 

  // abhi ke liye comment
  // {
  //   name: "Inventory Reconciliation",
  //   path: `/inventory-reconciliation/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
  // },
  // {
  //   name: "Expense Reconciliation",
  //   path: ({ ranged, countryName, month, year }) =>
  //     `/expense-reconciliation/${encodeURIComponent(
  //       ranged
  //     )}/${encodeURIComponent(countryName)}/${encodeURIComponent(
  //       month
  //     )}/${encodeURIComponent(year)}`,
  // },
],
    },
  ];

  const SECTION_TO_MODULE: Record<string, string> = {
    "live-dashboard": "LIVE_DASHBOARD",
    "finance-dashboards": "FINANCE_DASHBOARDS",
    "business-intelligence": "BUSINESS_INTELLIGENCE",
    "inventory-planning": "INVENTORY_PLANNING",
  };

  const visibleSections = React.useMemo(() => {
    if (!isMember) return sections;

    return sections.filter((s) => {
      const moduleKey = SECTION_TO_MODULE[s.key];

      return moduleKey
        ? allowedModulesForCurrentCountry.includes(moduleKey)
        : false;
    });
  }, [isMember, allowedModulesForCurrentCountry, sections]);

  const [openSections, setOpenSections] = useState<Record<string, boolean>>({
    "live-dashboard": true,
    "finance-dashboards": true,
    "business-intelligence": true,
    "inventory-planning": true,
  });

  const isActive = useCallback(
  (path: string | ((params: typeof currentParams) => string)) => {
    const resolvedPath =
      typeof path === "function" ? path(currentParams) : path;

    const [targetPath, targetHash = ""] = resolvedPath.split("#");

    if (targetHash) {
      return pathname === targetPath && currentHash === `#${targetHash}`;
    }

    const [resolvedPathOnly] = resolvedPath.split("#");
    return pathname === resolvedPathOnly && !currentHash;
  },
  [pathname, currentHash, currentParams]
);

  const showText = isExpanded || isMobileOpen;

  const safeSelectedPlatform =
    filteredRegionOptions.find((o) => o.value === selectedPlatform)?.value ??
    filteredRegionOptions[0]?.value ??
    "global";

  return (
    <aside
      className={`fixed mt-16 flex flex-col lg:mt-0 top-0 left-0 bg-white text-gray-900 h-screen overflow-y-auto transition-all duration-300 ease-in-out z-[1100]
     ${showText ? "px-3 sm:px-4 lg:px-3 xl:px-4" : "px-0"}
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
        className={`py-4 sm:py-5 lg:py-6 flex items-center ${showText ? "justify-between" : "justify-center"
          }`}
      >
        {showText && (
          <Link
            href={`/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`}
            className="flex items-center gap-2"
          >
            <Image
              className="dark:hidden hidden lg:block"
              src="/images/logo/Logo_Phormula.png"
              alt="Logo"
              width={132}
              height={36}
            />
          </Link>
        )}

        <button
          type="button"
          onClick={handleToggle}
          className="flex shrink-0 items-center justify-center w-8 h-8 2xl:w-9 2xl:h-9 rounded-lg border border-gray-200"
          aria-label="Toggle sidebar"
        >
          {showText ? (
            <Image
              src="/images/icons/sidebarin.png"
              alt="Close sidebar"
              width={20}
              height={20}
              className="2xl:w-[20px] 2xl:h-[20px] w-[18px] h-[18px]"
            />
          ) : (
            <Image
              src="/images/icons/hamburger.png"
              alt="Open sidebar"
              width={20}
              height={20}
              className="2xl:w-[20px] 2xl:h-[20px] w-[18px] h-[18px]"
            />
          )}
        </button>
      </div>

      {/* Platform Select */}
      {showText && filteredRegionOptions.length > 0 && (
        <RegionSelect
          label="Platform"
          selectedCountry={safeSelectedPlatform}
          options={filteredRegionOptions}
          onChange={onRegionChange}
          className={`mb-2 rounded bg-transparent text-gray-800 focus:outline-none focus:ring-1 focus:ring-[#5EA68E]
 py-1 ${textMain}`}
        />
      )}

      {/* Navigation Sections */}
      <div className="flex flex-col overflow-y-auto duration-300 ease-linear no-scrollbar">
        <nav className="mb-6">
          <div className="flex flex-col gap-4 min-[1700px]:gap-1">
            {visibleSections.map((section) => {
              const resolvedSubPaths = section.subItems.map((sub) =>
                typeof sub.path === "function"
                  ? sub.path(currentParams)
                  : sub.path
              );

              const isSectionActive = resolvedSubPaths.some((p) =>
                isActive(p as any)
              );

              const isOpen = !!openSections[section.key];

              return (
                <Collapsible
                  key={section.key}
                  open={showText && isOpen}
                  onOpenChange={(open) => {
                    if (!showText) return;

                    setOpenSections((prev) => ({
                      ...prev,
                      [section.key]: open,
                    }));
                  }}
                  className="group/collapsible flex flex-col"
                >
                  <CollapsibleTrigger asChild>
                    <button
                      type="button"
                      className={`w-full rounded cursor-pointer group
                        ${padHeader} ${textSection} text-left text-[#5EA68E] font-semibold
                        flex items-center ${showText ? "justify-between" : "justify-center"
                        }
                        transition-all duration-300 ease-out
                        hover:bg-[#5EA68E]/15 hover:shadow-sm hover:translate-x-[2px]
                        active:scale-[0.98]
                        ${isSectionActive ? "bg-[#5EA68E]/10" : ""}`}
                    >
                      <div className="flex items-center">
                        {section.icon}
                        {showText && (
                          <span className="ml-2">{section.name}</span>
                        )}
                      </div>

                      {showText && (
                        <FiChevronRight
                          className="h-3 w-3 sm:h-4.5 sm:w-4.5 font-bold transition-transform duration-300 group-data-[state=open]/collapsible:rotate-90"
                        />
                      )}
                    </button>
                  </CollapsibleTrigger>

                  <CollapsibleContent>
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
                            onClick={async (e) => {
                              const targetUrl = resolvedPath;

                              const [targetPathOnly, targetHash = ""] =
                                targetUrl.split("#");

                              const currentPathOnly = pathname;

                              if (subItem.onClick) {
                                e.preventDefault();

                                try {
                                  await subItem.onClick();
                                } catch (err) {
                                  console.error(err);
                                  router.push(resolvedPath);
                                }

                                return;
                              }

                              if (
                                targetHash &&
                                targetPathOnly === currentPathOnly
                              ) {
                                e.preventDefault();

                                const nextHash = `#${targetHash}`;

                                window.history.pushState(
                                  null,
                                  "",
                                  `${targetPathOnly}${nextHash}`
                                );

                                setCurrentHash(nextHash);

                                window.dispatchEvent(
                                  new CustomEvent("page-hash-navigate", {
                                    detail: { hash: targetHash },
                                  })
                                );

                                const el =
                                  document.getElementById(targetHash);

                                if (el) {
                                  el.scrollIntoView({
                                    behavior: "smooth",
                                    block: "start",
                                  });
                                }

                                return;
                              }
                            }}
                            className={`block rounded
  ${padItem} ${textMain}
  transition-all duration-300 ease-out
  hover:bg-[#5EA68E]/15 hover:text-[#5EA68E] hover:translate-x-[3px] hover:shadow-sm
  active:scale-[0.98]
  ${isActive(subItem.path as any)
                                ? "bg-[#5EA68E]/20 text-[#5EA68E] font-medium"
                                : "text-gray-700"
                              }`}
                          >
                            {subItem.name}
                          </Link>
                        );
                      })}
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              );
            })}
          </div>
        </nav>
      </div>

      {isMobileOpen && (
        <button
          type="button"
          onClick={() => setIsMobileOpen(false)}
          className="absolute top-4 right-4 text-gray-500 hover:text-gray-700 md:hidden"
          aria-label="Close mobile sidebar"
        >
          <FaTimes className="h-5 w-5" />
        </button>
      )}
    </aside>
  );
};

export default AppSidebar;