"use client";

import React, { useEffect, useMemo } from "react";
import { usePathname, useRouter } from "next/navigation";
import { useGetUserDataQuery } from "@/lib/api/profileApi";

const MODULE_HOME_ROUTES: Record<string, string> = {
  LIVE_DASHBOARD: "live-dashboard",
  FINANCE_DASHBOARDS: "pnl-dashboard",
  BUSINESS_INTELLIGENCE: "ai-insight",
  INVENTORY_PLANNING: "inputCost",
};

const ROUTE_MODULE_MAP: Record<string, string> = {
  // LIVE DASHBOARD
  "live-dashboard": "LIVE_DASHBOARD",
  currentInventory: "LIVE_DASHBOARD",

  // FINANCIAL METRICS
  "pnl-dashboard": "FINANCE_DASHBOARDS",
  cashflow: "FINANCE_DASHBOARDS",
  skuwiseprofit: "FINANCE_DASHBOARDS",

  // BUSINESS INTELLIGENCE
  "ai-insight": "BUSINESS_INTELLIGENCE",
  chatbot: "BUSINESS_INTELLIGENCE",
  "inventory-forecast": "BUSINESS_INTELLIGENCE",
  pnlforecast: "BUSINESS_INTELLIGENCE",
  "purchase-order": "BUSINESS_INTELLIGENCE",

  // INVENTORY PLANNING
  inputCost: "INVENTORY_PLANNING",
  "inventory-reconciliation": "INVENTORY_PLANNING",
  "expense-reconciliation": "INVENTORY_PLANNING",
};

const ROUTES_WITH_RANGED = new Set([
  "pnl-dashboard",
  "live-business-insight",
  "ai-insight",
  "chatbot",
  "expense-reconciliation",
]);

const COUNTRY_NAME_TO_CODE: Record<string, string> = {
  us: "US",
  usa: "US",
  "amazon-us": "US",
  "amazon us": "US",
  "united-states": "US",
  "united states": "US",

  uk: "UK",
  "amazon-uk": "UK",
  "amazon uk": "UK",
  "united-kingdom": "UK",
  "united kingdom": "UK",

  ca: "CA",
  canada: "CA",
  "amazon-ca": "CA",
  "amazon ca": "CA",

  de: "DE",
  germany: "DE",
  "amazon-de": "DE",
  "amazon de": "DE",
};

const COUNTRY_CODE_TO_PLATFORM: Record<string, string> = {
  US: "amazon-us",
  UK: "amazon-uk",
  CA: "amazon-ca",
  DE: "amazon-de",
};

const ALLOWED_COUNTRIES = ["US", "UK", "CA", "DE"];

const MONTH_NAMES = [
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

function normalizeCountry(value?: string | null) {
  if (!value) return "";

  const key = decodeURIComponent(value).trim().toLowerCase();

  return COUNTRY_NAME_TO_CODE[key] || key.toUpperCase();
}

function normalizeCountryAccess(rawCountryAccess: any): Record<string, string[]> {
  if (
    !rawCountryAccess ||
    typeof rawCountryAccess !== "object" ||
    Array.isArray(rawCountryAccess)
  ) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(rawCountryAccess).map(([country, modules]) => [
      String(country).toUpperCase(),
      Array.isArray(modules) ? modules.map(String) : [],
    ])
  );
}

function getPathParts(pathname: string) {
  const cleanPath = pathname.split("?")[0].split("#")[0];

  return cleanPath.split("/").filter(Boolean);
}

function getCurrentMonthYear() {
  const now = new Date();

  return {
    month: MONTH_NAMES[now.getMonth()],
    year: String(now.getFullYear()),
  };
}

function getRouteModule(pathname: string) {
  const parts = getPathParts(pathname);
  const routeKey = parts[0];

  return ROUTE_MODULE_MAP[routeKey] || "";
}

function getCountryFromPath(pathname: string) {
  const parts = getPathParts(pathname);

  const possibleCountry = parts.find((part) => {
    const normalized = normalizeCountry(part);
    return ALLOWED_COUNTRIES.includes(normalized);
  });

  return normalizeCountry(possibleCountry);
}

function getRangedFromPath(pathname: string) {
  const parts = getPathParts(pathname);

  const possibleRanged = parts.find((part) =>
    ["MTD", "YTD"].includes(String(part).toUpperCase())
  );

  return possibleRanged || "YTD";
}

function getMonthYearFromPath(pathname: string) {
  const parts = getPathParts(pathname);

  const monthFromPath = parts.find((part) =>
    MONTH_NAMES.includes(String(part).toLowerCase())
  );

  const yearFromPath = parts.find((part) => /^\d{4}$/.test(String(part)));

  const fallback = getCurrentMonthYear();

  return {
    month: monthFromPath ? String(monthFromPath).toLowerCase() : fallback.month,
    year: yearFromPath ? String(yearFromPath) : fallback.year,
  };
}

function syncPlatformStorage(countryCode: string) {
  if (typeof window === "undefined") return;

  const platform = COUNTRY_CODE_TO_PLATFORM[countryCode];

  if (!platform) return;

  localStorage.setItem("selectedPlatform", platform);
  localStorage.setItem("lastNonGlobalPlatform", platform);
  localStorage.setItem("countryName", countryCode.toLowerCase());
  localStorage.setItem("selectedCountry", countryCode);
  localStorage.setItem("selectedCountryCode", countryCode);
}

function buildRedirectRoute({
  module,
  country,
  pathname,
}: {
  module: string;
  country: string;
  pathname: string;
}) {
  const countryName = country.toLowerCase();
  const ranged = getRangedFromPath(pathname);
  const { month, year } = getMonthYearFromPath(pathname);

  if (module === "LIVE_DASHBOARD") {
    return `/live-dashboard/${countryName}/${month}/${year}#live-sales`;
  }

  if (module === "FINANCE_DASHBOARDS") {
    return `/pnl-dashboard/${ranged}/${countryName}/${month}/${year}#finance-dashboard`;
  }

  if (module === "BUSINESS_INTELLIGENCE") {
    return `/ai-insight/${ranged}/${countryName}/${month}/${year}`;
  }

  if (module === "INVENTORY_PLANNING") {
    return `/inputCost/${countryName}/${month}/${year}`;
  }

  return "/profile/global/NA/NA";
}

function getAllowedCountryForModule(
  countryAccess: Record<string, string[]>,
  module: string
) {
  return Object.keys(countryAccess).find((country) =>
    countryAccess[country]?.includes(module)
  );
}

function getFirstAllowedRoute(
  countryAccess: Record<string, string[]>,
  pathname: string
) {
  for (const [country, modules] of Object.entries(countryAccess)) {
    if (Array.isArray(modules) && modules.length > 0) {
      return buildRedirectRoute({
        module: modules[0],
        country,
        pathname,
      });
    }
  }

  return "/profile/global/NA/NA";
}

export default function MemberRouteGuard({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const router = useRouter();

  const isPublicRoute =
    pathname.startsWith("/login") ||
    pathname.startsWith("/signup") ||
    pathname.startsWith("/forgot-password") ||
    pathname.startsWith("/reset-password");

  const { data, isLoading } = useGetUserDataQuery(undefined, {
    skip: isPublicRoute,
    refetchOnMountOrArgChange: true,
    refetchOnFocus: true,
    refetchOnReconnect: true,
  });

  const permissionState = useMemo(() => {
    const userData = data as any;

    return {
      isMember: !!userData?.is_member,
      countryAccess: normalizeCountryAccess(userData?.country_access),
    };
  }, [data]);

  useEffect(() => {
    if (isPublicRoute) return;
    if (isLoading) return;

    const { isMember, countryAccess } = permissionState;

    if (!isMember) return;

    const currentModule = getRouteModule(pathname);

    // Profile/settings/account pages are allowed.
    if (!currentModule) return;

    const currentCountry = getCountryFromPath(pathname);

    const allowedModulesForCurrentCountry = currentCountry
      ? countryAccess[currentCountry] || []
      : [];

    const hasAccess =
      !!currentCountry &&
      allowedModulesForCurrentCountry.includes(currentModule);

    if (hasAccess) {
      syncPlatformStorage(currentCountry);
      return;
    }

    /**
     * Example:
     * Current route: /inputCost/us/may/2026
     * Access: UK -> INVENTORY_PLANNING, US -> FINANCE_DASHBOARDS
     *
     * Result:
     * Redirect to /inputCost/uk/may/2026
     */
    const allowedCountryForSameModule = getAllowedCountryForModule(
      countryAccess,
      currentModule
    );

    if (allowedCountryForSameModule) {
      syncPlatformStorage(allowedCountryForSameModule);

      const redirectRoute = buildRedirectRoute({
        module: currentModule,
        country: allowedCountryForSameModule,
        pathname,
      });

      if (redirectRoute !== pathname) {
        router.replace(redirectRoute);
      }

      return;
    }

    /**
     * Example:
     * Current route: /ai-insight/MTD/uk/may/2026
     * Access: UK -> INVENTORY_PLANNING
     *
     * Result:
     * Redirect to /inputCost/uk/may/2026
     */
    const redirectRoute = getFirstAllowedRoute(countryAccess, pathname);
    const redirectCountry = getCountryFromPath(redirectRoute);

    if (redirectCountry) {
      syncPlatformStorage(redirectCountry);
    }

    if (redirectRoute !== pathname) {
      router.replace(redirectRoute);
    }
  }, [isPublicRoute, isLoading, permissionState, pathname, router]);

  if (!isPublicRoute && isLoading) {
    return null;
  }

  return <>{children}</>;
}