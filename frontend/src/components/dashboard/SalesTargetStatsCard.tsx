"use client";

import React, { useMemo } from "react";
import { getISTYearMonth, getPrevMonthShortLabel } from "@/lib/dashboard/date";
import PageBreadcrumb from "../common/PageBreadCrumb";
import type { RegionKey, RegionMetrics } from "@/lib/dashboard/types";
import SegmentedToggle from "../ui/SegmentedToggle";

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

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
}: Props) {

  const prevLabel = getPrevMonthShortLabel();

  const { monthName, year } = getISTYearMonth();
  const monthYearLabel = `${new Date(`${monthName} 1, ${year}`).toLocaleString("en-US", {
    month: "short",
  })}'${String(year).slice(-2)}`;


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

  return (
    <div className="rounded-2xl border p-3 2xl:p-5 shadow-sm h-full flex flex-col bg-[#D9D9D933]">
      <div className="relative flex flex-col items-center gap-2 font-bold text-charcoal-500">
        <div className="flex items-center gap-1">
          <PageBreadcrumb
            pageTitle="Sales Metrics -"
            textSize="2xl"
            variant="page"
            align="center"
          />

          <span className="text-green-500 text-base sm:text-xl lg:text-lg 2xl:text-2xl ">
            {monthYearLabel}
          </span>

          <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl">
            MTD
          </span>
        </div>


        {/* ✅ Toggle moved here */}
        {!hideTabs && (
          <SegmentedToggle<RegionKey>
            value={value}
            options={availableRegions.map((r: RegionKey) => ({ value: r }))}
            onChange={onChange}
            className="mt-1"
          />
        )}
      </div>

      <div className="pt-4 flex-1">
        <div className="grid grid-cols-2 xl:grid-cols-3 gap-3 text-sm h-full">
          {[
            { title: "Today", value: formatHomeK(todayHome), helper: "\u00A0" },
            { title: "MTD Sales", value: formatHomeK(mtdHome), helper: "\u00A0" },
            { title: "Target", value: formatHomeK(targetHome), helper: "\u00A0" },
            { title: prevLabel, value: formatHomeK(lastMonthTotalHome), helper: "\u00A0" },
            {
              title: "Sales Trend",
              value: `${salesTrendPct >= 0 ? "+" : ""}${salesTrendPct.toFixed(2)}%`,
              helper: `vs ${prevLabel} MTD`,
            },
            {
              title: "Target Trend",
              value: `${targetTrendPct >= 0 ? "+" : ""}${targetTrendPct.toFixed(2)}%`,
              helper: `Target vs MTD Trend`,
            },
          ].map((t) => (
            <div
              key={t.title}
              className="rounded-xl 2xl:p-3 text-center h-full flex flex-col items-center justify-start"
            >
              <div className="text-charcoal-500 whitespace-nowrap leading-none  text-[10px] 2xl:text-xs">{t.title}</div>
              <div className="mt-2 text-sm 2xl:text-lg font-semibold whitespace-nowrap leading-none">{t.value}</div>
              <div
                className={`mt-1 text-[10px] 2xl:text-xs leading-none ${t.helper === "\u00A0" ? "text-transparent select-none" : "text-gray-500"
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
