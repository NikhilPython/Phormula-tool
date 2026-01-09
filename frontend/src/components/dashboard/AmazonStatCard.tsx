"use client";

import React from "react";
import ValueOrSkeleton from "@/components/common/ValueOrSkeleton";
import { calcDeltaPct, fmtGBP, toNumberSafe } from "@/lib/dashboard/format";

export type AmazonStatCardProps = {
  label: string;
  current: number | null | undefined;
  previous: number | null | undefined;
  loading: boolean;
  formatter?: (v: any) => string;
  bottomLabel: string;
  className?: string;
  deltaPct?: number | null;
  inverseDelta?: boolean;
};

export default function AmazonStatCard({
  label,
  current,
  previous,
  loading,
  formatter = fmtGBP,
  bottomLabel,
  className = "",
  deltaPct,
  inverseDelta
}: AmazonStatCardProps) {
  const currVal = toNumberSafe(current);
  const prevVal = previous != null ? toNumberSafe(previous) : 0;

  // default delta logic
  const computedDelta = calcDeltaPct(currVal, prevVal);

  // override if provided
  const deltaToShow =
    deltaPct === undefined ? computedDelta : deltaPct == null ? null : Number(deltaPct);

const visualIsUp =
  deltaToShow != null &&
  (inverseDelta ? deltaToShow < 0 : deltaToShow >= 0);

  const isPercentLabel = label.toLowerCase().includes("%");
  const suffix = "%";


  let deltaContent: React.ReactNode = "—";
  if (deltaToShow != null) {
    deltaContent = (
      <>
        <span className="mr-0.5">{visualIsUp  ? "▲" : "▼"}</span>
        {Math.abs(deltaToShow).toFixed(2)}
        {suffix}
      </>
    );
  }

  // const deltaColor =
  //   deltaToShow == null
  //     ? "text-gray-400"
  //     : isUp
  //       ? "text-emerald-600"
  //       : "text-rose-600";

const goodClass = "text-emerald-600";
const badClass = "text-red-600"; 

const deltaColor =
  deltaToShow == null
    ? "text-gray-400"
    : (visualIsUp ? goodClass : badClass);



  return (
    <div
      className={`w-full min-w-0 rounded-xl border p-2.5 sm:p-3 shadow-sm flex flex-col justify-between ${className}`}
    >
      {/* Label */}
      <div className="text-[10px] sm:text-[10px] 2xl:text-xs leading-tight font-medium text-charcoal-500 truncate">
        {label}
      </div>

      <div className="mt-1 text-sm 2xl:text-lg font-semibold leading-tight min-w-0 truncate">
        <ValueOrSkeleton loading={loading} mode="inline" compact>
          <span className="block min-w-0 truncate">
            {formatter(currVal)}
          </span>
        </ValueOrSkeleton>
      </div>

      {/* Bottom row */}
      <div className="mt-2 flex items-end justify-between gap-2 text-[9.5px] sm:text-[10px] 2xl:text-xs leading-tight text-charcoal-500 min-w-0">
        {/* Previous value */}
        <div className="flex flex-col min-w-0">
          <span className="truncate">{bottomLabel}:</span>
          <span className="font-medium truncate">
            {previous == null ? "—" : formatter(prevVal)}
          </span>
        </div>

        {/* Delta */}
        <div className={`shrink-0 inline-flex items-center font-semibold ${deltaColor}`}>
          {deltaContent}
        </div>
      </div>
    </div>
  );
}
