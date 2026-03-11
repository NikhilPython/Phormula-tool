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
import { useAppSelector } from "@/lib/hooks"; // ✅ add

type NavSubItem = {
  name: string;
  path:
  | string
  | ((
    params: { ranged: string; countryName: string; month: string; year: string }
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

  // ✅ Auth info (client vs member)
  const authUser = useAppSelector((s: any) => s.auth.user);
  const token = useAppSelector((s: any) => s.auth.token);
  // ✅ token payload read (refresh pe user null hota hai)
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

  // ✅ member detect: store OR token
  const isMember = !!authUser?.is_member || tokenPayload?.is_member === true;

  // ✅ modules: store OR token
  const allowedModules: string[] =
    authUser?.modules || tokenPayload?.modules || [];

  // ✅ Skip client-only user-data call for members (prevents 500 spam)
  const { data: user } = useGetUserDataQuery(undefined, {
    skip: !token || isMember,
  });

  const isPreviewMode = routeParams?.month === "NA" && routeParams?.year === "NA";

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
  // ✅ Skip for members (these endpoints usually expect client token)
  useGetProfileCountriesQuery(undefined, { skip: !token || isMember });
  useGetUploadHistoryQuery(undefined, { skip: !token || isMember });

  // ===== Platform data =====
  const connectedPlatforms = useConnectedPlatforms();
  const rawOptions: RegionOption[] = buildPlatformOptions(connectedPlatforms);

  const countryFromRoute = routeParams?.countryName as string | undefined;

  const regionOptions: RegionOption[] = React.useMemo(() => {
    const opts = buildPlatformOptions(connectedPlatforms);

    const countryFromRoute = routeParams?.countryName as string | undefined;

    // ✅ ONLY force Amazon country if NOT global
    if (countryFromRoute && countryFromRoute !== "global") {
      const forcedValue = `amazon-${countryFromRoute}`;

      const exists = opts.some((o) => o.value === forcedValue);
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

      // ✅ client token uses user_id; member token uses owner_user_id
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
  }, [routeParams?.countryName, isPreviewMode, setPlatformCtx]);

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

  const currentCountryName = (routeParams?.countryName as string) || "global";

  const currentParams = {
    ranged: (routeParams?.ranged as string) || initialPeriod.ranged,
    countryName: currentCountryName,
    month: (routeParams?.month as string) || initialPeriod.month,
    year: (routeParams?.year as string) || initialPeriod.year,
  };

 

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
      `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(
        2,
        "0"
      )}-${String(d.getUTCDate()).padStart(2, "0")}`;

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
      throw new Error(err?.error || "Failed to seed Sponsored Products report");
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

  const ensureSdReportSeedOnce = async (baseUrl: string, jwtToken: string) => {
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
      throw new Error(err?.error || "Failed to seed SD advertised product report");
    }

    localStorage.setItem(storageKey, "1");
  };

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
        // {
        //   name: "Targets and Action Items",
        //   path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#targets-action-items`,
        // },
        // {
        //   name: "MTD P&L",
        //   path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#mtd-pl`,
        // },
        // {
        //   name: "P&L Productwise Breakdown",
        //   path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#pnl-mtd`,
        // },
        // {
        //   name: "Current Inventory",
        //   path: `/live-dashboard/${currentParams.countryName}/${currentParams.month}/${currentParams.year}#current-inventory`,
        // },
      ],
    },

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
        // {
        //   name: "Business Summary",
        //   path: ({ ranged, countryName, month, year }) =>
        //     `/pnl-dashboard/${encodeURIComponent(ranged)}/${encodeURIComponent(
        //       countryName
        //     )}/${encodeURIComponent(month)}/${encodeURIComponent(
        //       year
        //     )}#business-summary`,
        // },
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
            `/expense-reconciliation/${encodeURIComponent(
              countryName
            )}/${encodeURIComponent(month)}/${encodeURIComponent(year)}`,
        },
      ],
    },

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
        },
        {
          name: "P&L Forecast",
          path: `/pnlforecast/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
        },
      ],
    },

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
          name: "Input Cost",
          path: `/inputCost/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
        },
        {
          name: "Inventory Reconcilliation",
          path: `/inventory-reconciliation/${currentParams.countryName}/${currentParams.month}/${currentParams.year}`,
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

  // ✅ module mapping for member permissions
  const SECTION_TO_MODULE: Record<string, string> = {
    "live-dashboard": "LIVE_DASHBOARD",
    "finance-dashboards": "FINANCE_DASHBOARDS",
    "business-intelligence": "BUSINESS_INTELLIGENCE",
    "inventory-planning": "INVENTORY_PLANNING",
  };

  // ✅ filtered sections for member
  const visibleSections = React.useMemo(() => {
    if (!isMember) return sections; // client sees all
    return sections.filter((s) => {
      const moduleKey = SECTION_TO_MODULE[s.key];
      return moduleKey ? allowedModules.includes(moduleKey) : false;
    });
  }, [isMember, allowedModules, sections]);

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
      <div className={`py-4 sm:py-5 lg:py-6 flex items-center justify-between`}>
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
          selectedCountry={safeSelectedPlatform}
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
            {visibleSections.map((section) => {
              const resolvedSubPaths = section.subItems.map((sub) =>
                typeof sub.path === "function" ? sub.path(currentParams) : sub.path
              );
              const isSectionActive = resolvedSubPaths.some((p) => isActive(p as any));

              return (
                <div key={section.key} className="flex flex-col">
                  <button
                    onClick={() => toggleSection(section.key)}
                    className={`w-full rounded hover:bg-[#5EA68E]/15 transition-colors cursor-pointer group
                      ${padHeader} ${textSection} text-left text-[#5EA68E] font-semibold
                      flex items-center ${showText ? "justify-between" : "justify-center"}
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
                            onClick={async (e) => {
                              if (!subItem.onClick) return;
                              e.preventDefault();
                              try {
                                await subItem.onClick();
                              } catch (err) {
                                console.error(err);
                                router.push(resolvedPath);
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