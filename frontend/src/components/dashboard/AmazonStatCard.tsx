"use client";

import React from "react";
import ValueOrSkeleton from "@/components/common/ValueOrSkeleton";
import { fmtGBP, toNumberSafe } from "@/lib/dashboard/format";

export type AmazonStatCardProps = {
  label: string;
  current: number | null | undefined;
  previous: number | null | undefined;
  loading: boolean;
  formatter?: (v: any) => React.ReactNode;
  previousFormatter?: (v: number) => React.ReactNode;
  bottomLabel: string;
  className?: string;
  deltaPct: number | null;
  inverseDelta?: boolean;
  currentPerUnit?: number | null;
  previousPerUnit?: number | null;
  perUnitFormatter?: (v: number) => React.ReactNode;
  hidePrevious?: boolean;
  hidePreviousValue?: boolean;
};

export default function AmazonStatCard({
  label,
  current,
  previous,
  loading,
  formatter = fmtGBP,
  previousFormatter = formatter,
  bottomLabel,
  className = "text-charcoal-500",
  deltaPct,
  inverseDelta = false,
  currentPerUnit,
  previousPerUnit,
  perUnitFormatter = formatter,
  hidePrevious = false,
  hidePreviousValue = false,
}: AmazonStatCardProps) {
  const currVal = toNumberSafe(current);
  const prevVal = previous != null ? toNumberSafe(previous) : 0;

  const deltaToShow = deltaPct == null ? null : Number(deltaPct);

  // Automatically apply inverse logic (lower is better) for cost-related metrics
  const normalizedLabel = label.trim().toLowerCase();
  const isInverseMetric =
    inverseDelta ||
    normalizedLabel === "marketplace fees" ||
    normalizedLabel === "amazon fees" ||
    normalizedLabel === "tacos" ||
    normalizedLabel === "cost of ads" ||
    normalizedLabel === "cost of advertisement";

  const isIncrease = deltaToShow != null && deltaToShow >= 0;
  const isGood =
    deltaToShow != null &&
    (isInverseMetric ? deltaToShow < 0 : deltaToShow >= 0);

  const arrow = deltaToShow == null ? "" : isIncrease ? "\u25B2" : "\u25BC";

  const deltaColor =
    deltaToShow == null
      ? "text-gray-400"
      : isGood
        ? "text-emerald-600"
        : "text-red-600";

  const deltaContent =
    deltaToShow == null ? (
      "\u2014"
    ) : (
      <>
        <span className="mr-0.5">{arrow}</span>
        {Math.abs(deltaToShow).toFixed(2)}%
      </>
    );

  const showInlineDelta = !hidePrevious && hidePreviousValue;
  const showPreviousRow = !hidePrevious && !hidePreviousValue;

  return (
    <div
      className={`w-full min-w-0 rounded-xl border p-2.5 sm:p-3 shadow-sm flex flex-col ${
        showInlineDelta ? "justify-start" : "justify-between"
      } ${className}`}
    >
      <div className="text-[10px] sm:text-[10px] 2xl:text-xs leading-tight font-medium text-charcoal-500 truncate">
        {label}
      </div>

      {/* Current value + inline delta */}
      <div className="mt-1 flex items-baseline justify-between gap-2 min-w-0">
        <div className="min-w-0 flex-1 text-sm 2xl:text-lg font-semibold leading-tight">
          <ValueOrSkeleton loading={loading} mode="inline" compact>
            <span className="inline-flex text-charcoal-500 items-baseline gap-1 min-w-0 truncate">
              {formatter(currVal)}
              {currentPerUnit != null && (
                <span className="text-[10px] 2xl:text-xs text-charcoal-400 font-medium">
                  ({perUnitFormatter(toNumberSafe(currentPerUnit))}/unit)
                </span>
              )}
            </span>
          </ValueOrSkeleton>
        </div>

        {/* Inline delta for Monthly Recap cards */}
        {showInlineDelta && (
          <div
            className={`shrink-0 whitespace-nowrap inline-flex items-center text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold ${deltaColor}`}
          >
            {deltaContent}
          </div>
        )}
      </div>

      {/* Previous month row - existing layout preserved */}
      {showPreviousRow && (
        <div className="mt-2 flex items-end justify-between gap-2 text-[9.5px] sm:text-[10px] 2xl:text-xs leading-tight text-charcoal-500 min-w-0">
          {!hidePreviousValue && (
            <div className="flex flex-col min-w-0">
              <span className="truncate">{bottomLabel}:</span>
              <span className="font-medium inline-flex items-baseline gap-1 truncate">
                {previous == null ? "\u2014" : previousFormatter(prevVal)}
                {previous != null && previousPerUnit != null && (
                  <span className="text-charcoal-400 font-normal">
                    ({perUnitFormatter(toNumberSafe(previousPerUnit))}/unit)
                  </span>
                )}
              </span>
            </div>
          )}

          {/* Keep bottom delta for cards displaying previous values */}
          {!hidePreviousValue && (
            <div className={`shrink-0 inline-flex items-center font-semibold ${deltaColor}`}>
              {deltaContent}
            </div>
          )}
        </div>
      )}
    </div>
  );
}