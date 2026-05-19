// src/components/common/CurrentMonthRouteGuard.tsx

"use client";

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";

const MONTHS = [
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

const ROUTES_WITH_MONTH_YEAR = new Set([
  "live-dashboard",
  "pnl-dashboard",
  "live-business-insight",
  "ai-insight",
  "chatbot",
  "inventory-forecast",
  "pnlforecast",
  "inventory-reconciliation",
  "currentInventory",
  "purchase-order",
  "cashflow",
  "expense-reconciliation",
  "fba",
  "inputCost",
  "objectives-targets",
  "skuwiseprofit",
]);

const getCurrentMonthYear = () => {
  const now = new Date();

  return {
    month: now.toLocaleString("en-US", { month: "long" }).toLowerCase(),
    year: String(now.getFullYear()),
  };
};

const isYear = (value: string) => /^\d{4}$/.test(value);

export default function CurrentMonthRouteGuard() {
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    if (!pathname) return;

    const segments = pathname.split("/").filter(Boolean);
    const routeName = segments[0];

    if (!ROUTES_WITH_MONTH_YEAR.has(routeName)) return;

    const monthIndex = segments.findIndex((segment, index) => {
      const month = segment.toLowerCase();
      const year = segments[index + 1];

      return MONTHS.includes(month) && !!year && isYear(year);
    });

    if (monthIndex === -1) return;

    const yearIndex = monthIndex + 1;

    const routeMonth = segments[monthIndex].toLowerCase();
    const routeYear = segments[yearIndex];

    const { month: currentMonth, year: currentYear } = getCurrentMonthYear();

    if (routeMonth === currentMonth && routeYear === currentYear) return;

    segments[monthIndex] = currentMonth;
    segments[yearIndex] = currentYear;

    const search = window.location.search || "";
    const hash = window.location.hash || "";

    router.replace(`/${segments.join("/")}${search}${hash}`);
  }, [pathname, router]);

  return null;
}