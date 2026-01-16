"use client";

import React from "react";

type ComparisonRow = {
  label: string;
  valueText: string;     // already formatted (e.g. "$1,234.00", "12%")
  deltaText: string;     // already formatted (e.g. "▲ 2.5%" or "-")
  deltaClassName?: string; // e.g. "text-emerald-600" / "text-red-600"
};

type SummaryMetricCardProps = {
  title: string;
  valueText: string;
  className?: string;     // container styles (bg/border)
  valueClassName?: string;
  comparisons?: ComparisonRow[];
};

const SummaryMetricCard: React.FC<SummaryMetricCardProps> = ({
  title,
  valueText,
  className = "",
  valueClassName = "",
  comparisons = [],
}) => {
  return (
    <div
      className={[
        "w-full rounded-2xl shadow-sm p-3 2xl:p-4 flex flex-col justify-between",
        className,
      ].join(" ")}
    >
      <div className="flex justify-between items-center 2xl:mb-2">
        <span className="text-[10px] 2xl:text-xs text-charcoal-500">{title}</span>
      </div>

      <div
        className={[
          "mt-1 text-sm 2xl:text-lg font-semibold text-charcoal-500 leading-tight tabular-nums",
          valueClassName,
        ].join(" ")}
      >
        {valueText}
      </div>

      {!!comparisons.length && (
        <div className="2xl:mt-3 space-y-2">
          {comparisons.map((row, idx) => (
            <div
              key={`${row.label}-${idx}`}
              className="mt-2 flex items-end text-charcoal-500 justify-between gap-3 text-[10px] 2xl:text-xs leading-tight tabular-nums"
            >
              <div className="min-w-0">
                <div className="whitespace-nowrap">{row.label}:</div>
                <div className="whitespace-nowrap">{row.valueText}</div>
              </div>

              <div className={`font-bold whitespace-nowrap ${row.deltaClassName ?? "text-gray-400"}`}>
                {row.deltaText}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default SummaryMetricCard;