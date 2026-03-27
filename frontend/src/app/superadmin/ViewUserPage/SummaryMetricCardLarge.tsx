"use client";

import React from "react";

type ComparisonRow = {
  label: string;
  valueText: string;
  deltaText: string;
  deltaClassName?: string;
};

type SummaryMetricCardLargeProps = {
  title: string;
  value: React.ReactNode;
  className?: string;
  valueClassName?: string;
  comparisons?: ComparisonRow[];
};

const SummaryMetricCardLarge: React.FC<SummaryMetricCardLargeProps> = ({
  title,
  value,
  className = "",
  valueClassName = "",
  comparisons = [],
}) => {
  return (
    <div
      className={[
        "w-full rounded-2xl shadow-sm p-4 flex flex-col justify-between",
        className,
      ].join(" ")}
    >
      {/* Title */}
      <div className="flex justify-between items-center">
        <span className="text-xs text-charcoal-500 font-medium">
          {title}
        </span>
      </div>

      {/* Main Value */}
      <div
        className={[
          "mt-2 text-lg 2xl:text-xl font-semibold text-charcoal-600 leading-tight tabular-nums",
          valueClassName,
        ].join(" ")}
      >
        {value}
      </div>

      {/* Comparisons */}
      {!!comparisons.length && (
        <div className="space-y-2 mt-2">
          {comparisons.map((row, idx) => (
            <div
              key={`${row.label}-${idx}`}
              className="flex items-end justify-between gap-3 text-xs text-charcoal-500 leading-tight tabular-nums"
            >
              <div className="min-w-0">
                <div className="whitespace-nowrap font-medium">
                  {row.label}:
                </div>
                <div className="whitespace-nowrap">
                  {row.valueText}
                </div>
              </div>

              <div
                className={`font-bold whitespace-nowrap ${
                  row.deltaClassName ?? "text-gray-400"
                }`}
              >
                {row.deltaText}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default SummaryMetricCardLarge;