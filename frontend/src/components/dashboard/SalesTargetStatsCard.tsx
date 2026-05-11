"use client";

import React, { useMemo } from "react";
import { FiEdit } from "react-icons/fi";
import { useRouter } from "next/navigation";
import { getISTYearMonth, getPrevMonthShortLabel } from "@/lib/dashboard/date";
import PageBreadcrumb from "../common/PageBreadCrumb";
import type { RegionKey, RegionMetrics } from "@/lib/dashboard/types";
import SegmentedToggle from "../ui/SegmentedToggle";

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

type BiAlignedTotalsCard = {
  total_current_advertising: number;
  total_current_net_sales: number;
  total_current_platform_fees: number;
  total_current_profit: number;

  total_previous_advertising: number;
  total_previous_net_sales: number;
  total_previous_net_sales_full_month: number;
  total_previous_platform_fees: number;
  total_previous_profit: number;
};

type Props = {
  regions: Record<RegionKey, RegionMetrics>;
  value: RegionKey;
  onChange: (r: RegionKey) => void;
  hideTabs?: boolean;

  homeCurrency: CurrencyCode;
  formatHomeK: (value: number) => string;

  todayHome: number;
  mtdHome: number;
  targetHome: number;
  lastMonthTotalHome: number;
  salesTrendPct: number;
  targetTrendPct: number;

  currentReimbursement?: number;
  previousReimbursement?: number;

  biEnabled?: boolean;
  biAlignedTotals?: BiAlignedTotalsCard | null;
  lastMonthToDateHome?: number;
};



export default function SalesTargetStatsCard({
  regions,
  value,
  onChange,
  hideTabs,
  homeCurrency,
  formatHomeK,
  todayHome,
  mtdHome,
  targetHome,
  lastMonthTotalHome,
  salesTrendPct,
  targetTrendPct,
  currentReimbursement,
  previousReimbursement,
  biEnabled,
  biAlignedTotals,
  lastMonthToDateHome,
}: Props) {

  const prevLabel = getPrevMonthShortLabel();
  const router = useRouter();
  const { monthName, year } = getISTYearMonth();
  const monthYearLabel = `${new Date(`${monthName} 1, ${year}`).toLocaleString("en-US", {
    month: "short",
  })}'${String(year).slice(-2)}`;

  // 🔥 ADD THIS
  const activeRegion: RegionKey =
    regions[value]?.mtdUSD ||
      regions[value]?.targetUSD ||
      regions[value]?.lastMonthToDateUSD
      ? value
      : "Global";

  const active = regions[activeRegion];

  // ✅ define availableRegions properly (fixes availableRegions + implicit any)
  const availableRegions = useMemo<RegionKey[]>(() => {
    const list: RegionKey[] = ["Global"];
    (["UK", "US", "CA"] as RegionKey[]).forEach((key) => {
      const r = regions[key];
      if (!r) return;

      if (r.mtdUSD || r.lastMonthToDateUSD || r.lastMonthTotalUSD || r.targetUSD) {
        list.push(key);
      }
    });

    return list;
  }, [regions]);

  // const currMtd = biEnabled && biAlignedTotals
  //   ? biAlignedTotals.total_current_net_sales
  //   : mtdHome;

  // // last month MTD-to-date (same day range)
  // const prevMtd = biEnabled && biAlignedTotals
  //   ? biAlignedTotals.total_previous_net_sales
  //   : (regions[value]?.lastMonthToDateUSD ?? 0);

  // // full last month total
  // const prevFullMonth = biEnabled && biAlignedTotals
  //   ? biAlignedTotals.total_previous_net_sales_full_month
  //   : lastMonthTotalHome;

  const currMtd =
    biEnabled && biAlignedTotals
      ? biAlignedTotals.total_current_net_sales
      : mtdHome || active?.mtdUSD || 0;

  const prevMtdFromBi =
    biEnabled && biAlignedTotals
      ? Number(biAlignedTotals.total_previous_net_sales || 0)
      : 0;

  const prevMtd =
    prevMtdFromBi > 0
      ? prevMtdFromBi
      : Number(lastMonthToDateHome || active?.lastMonthToDateUSD || 0);

  const prevFullMonth =
    biEnabled && biAlignedTotals
      ? biAlignedTotals.total_previous_net_sales_full_month
      : lastMonthTotalHome || active?.lastMonthTotalUSD || 0;

  const targetToUse = targetHome || active?.targetUSD || 0;

  // Get today's date and total days in current month (IST aligned to your existing helpers)
  const today = new Date();
  const currentDay = today.getDate();

  // Get total days in current month
  const totalDaysInMonth = new Date(
    today.getFullYear(),
    today.getMonth() + 1,
    0
  ).getDate();

  // X = (Current Date / Total Days in Month) * Target
  const expectedSalesTillDate =
    totalDaysInMonth > 0
      ? (currentDay / totalDaysInMonth) * targetToUse
      : 0;

  // Sales Trend (unchanged – vs last month MTD)
  const salesTrendPctToUse =
    prevMtd > 0 ? ((currMtd - prevMtd) / prevMtd) * 100 : 0;

  // ✅ New Target Trend formula
  const targetTrendPctToUse =
    targetToUse > 0
      ? ((currMtd - expectedSalesTillDate) / targetToUse) * 100
      : 0;


  return (
    <div className="rounded-xl border p-3 2xl:p-5 shadow-sm h-auto lg:h-full flex flex-col bg-white">

      <div className="relative flex flex-col items-center gap-2 font-bold text-charcoal-500">
        {value !== "Global" && (
          <button
            type="button"
            onClick={() => {
              const country = value.toLowerCase(); // uk, us, ca
              const month = monthName.toLowerCase(); // february
              const fullYear = String(year); // 2026

              router.push(
                `/objectives-targets/${country}/${month}/${fullYear}?tab=targets_and_objectives`
              );
            }}
            className="absolute right-0 top-0 inline-flex h-9 w-9 items-center justify-center rounded-md text-gray-700 hover:bg-gray-100"
            aria-label="Edit targets and objectives"
            title="Edit targets and objectives"
          >
            <FiEdit className="text-lg" />
          </button>
        )}

        <div className="flex items-center gap-1">
          <PageBreadcrumb
            pageTitle="Sales Metrics"
            textSize="2xl"
            variant="page"
            align="center"
          />
        </div>

        {/* {!hideTabs && (
          <SegmentedToggle<RegionKey>
            value={value}
            options={availableRegions.map((r: RegionKey) => ({ value: r }))}
            onChange={onChange}
            className="mt-1"
          />
        )} */}
      </div>

      <div className="pt-4 lg:flex-1">
        <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-3 gap-3 text-sm h-auto lg:h-full">
          {[
            { title: "Today", value: formatHomeK(todayHome), helper: "\u00A0" },
            { title: "MTD Sales", value: formatHomeK(currMtd), helper: "\u00A0" },
            { title: "Target", value: formatHomeK(targetToUse), helper: "\u00A0" },
            { title: `${prevLabel} MTD`, value: formatHomeK(prevMtd), helper: "\u00A0" },
            {
              title: "Sales Trend",
              value: `${salesTrendPctToUse >= 0 ? "+" : ""}${salesTrendPctToUse.toFixed(2)}%`,
              helper: `vs ${prevLabel} MTD`,
            },
            {
              title: "Target Trend",
              value: `${targetTrendPctToUse >= 0 ? "+" : ""}${targetTrendPctToUse.toFixed(2)}%`,
              helper: `MTD vs Target`,
            },
          ].map((t) => (
            <div
              key={t.title}
              className="rounded-xl 2xl:p-1.5 text-center h-full flex flex-col items-center justify-start"
            >
              <div className="text-charcoal-500 whitespace-nowrap leading-none  text-[10px] 2xl:text-xs">{t.title}</div>
              <div className="mt-2 text-sm 2xl:text-lg font-semibold whitespace-nowrap leading-none">{t.value}</div>
              <div
                className={`mt-1 text-[10px] text-charcoal-500 2xl:text-xs leading-none ${t.helper === "\u00A0" ? "text-transparent select-none" : "text-charcoal-500"
                  }`}
              >
                {t.helper}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
