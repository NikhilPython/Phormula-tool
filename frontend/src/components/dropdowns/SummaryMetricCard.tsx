
"use client";

import React from "react";

type ComparisonRow = {
  label: string;
  valueText: string;
  deltaText: React.ReactNode;
  deltaClassName?: string;
};

type SummaryMetricCardProps = {
  title: string;
  value: React.ReactNode;
  className?: string;
  titleClassName?: string;
  valueClassName?: string;
  comparisons?: ComparisonRow[];
};

const SummaryMetricCard: React.FC<SummaryMetricCardProps> = ({
  title,
  value,
  className = "",
  titleClassName = "",
  valueClassName = "",
  comparisons = [],
}) => {
  return (
    <div
      className={[
        // Smaller laptop: current compact layout
        "box-border flex h-full min-h-0 min-w-0 w-full flex-col",
        "rounded-xl border p-3 sm:p-3.5 xl:p-4",
        "shadow-sm",

        // Desktop: restore previous card layout
        "2xl:justify-between 2xl:p-3",

        className,
      ].join(" ")}
    >
      <div className="flex shrink-0 items-center justify-between">
        <span
          className={[
            "text-[11px] font-medium leading-tight sm:text-xs",
            // Previous desktop title size
            "2xl:text-xs",
            titleClassName || "text-charcoal-500",
          ].join(" ")}
        >
          {title}
        </span>
      </div>

      <div
        className={[
          "mt-1.5 min-w-0 shrink-0",
          "text-sm font-semibold leading-tight tabular-nums",
          "sm:text-base xl:text-lg",
          // Previous desktop value size
          "2xl:text-lg",
          "text-charcoal-500",
          valueClassName,
        ].join(" ")}
      >
        {value}
      </div>

      {!!comparisons.length && (
        <div className="mt-auto min-w-0 pt-2 xl:pt-3 2xl:mt-2 2xl:space-y-2 2xl:pt-0">
          {comparisons.map((row, idx) => (
            <div
              key={`${row.label}-${idx}`}
              className={[
                "flex min-w-0 items-center justify-between gap-2",
                "text-[10px] leading-tight tabular-nums",
                "xl:text-[11px]",
                // Previous desktop comparison layout
                "2xl:mt-2 2xl:items-end 2xl:gap-3 2xl:text-xs",
              ].join(" ")}
            >
              <div className="flex min-w-0 flex-1 flex-wrap items-baseline gap-x-1 gap-y-0.5 text-charcoal-500 2xl:block">
                <span className="shrink-0 2xl:block">
                  {row.label}:
                </span>

                <span className="min-w-0 break-words font-medium 2xl:whitespace-nowrap">
                  {row.valueText}
                </span>
              </div>

              <div
                className={[
                  "shrink-0 whitespace-nowrap font-semibold",
                  row.deltaClassName ?? "text-gray-400",
                ].join(" ")}
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

export default SummaryMetricCard;
