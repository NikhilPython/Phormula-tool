"use client";

import React from "react";
import { FaAngleDown } from "react-icons/fa";

export type MetricSortKey =
  | "units"
  | "sales"
  | "profit"
  | "cm2_profit"
  | "marketplace_fees";

export type SortDirection = "desc" | "asc";

export type MetricSortOption = `${MetricSortKey}_${SortDirection}`;

type MetricConfig = {
  key: MetricSortKey;
  label: string;
};

const METRIC_CONFIGS: Record<MetricSortKey, MetricConfig> = {
  units: {
    key: "units",
    label: "Units",
  },
  sales: {
    key: "sales",
    label: "Sales",
  },
  profit: {
    key: "profit",
    label: "CM1 Profit",
  },
  cm2_profit: {
    key: "cm2_profit",
    label: "CM2 Profit",
  },
  marketplace_fees: {
    key: "marketplace_fees",
    label: "Marketplace Fees",
  },
};

type MetricSortDropdownProps = {
  value: MetricSortOption;
  onChange: (value: MetricSortOption) => void;

  /**
   * Pass only the metrics available on that page/table.
   * Example:
   * metrics={["units", "sales", "profit"]}
   */
  metrics: MetricSortKey[];

  className?: string;
};

const MetricSortDropdown: React.FC<MetricSortDropdownProps> = ({
  value,
  onChange,
  metrics,
  className = "",
}) => {
  const options = metrics.flatMap((metric) => {
    const config = METRIC_CONFIGS[metric];

    return [
      {
        value: `${metric}_desc` as MetricSortOption,
        label: `${config.label}: High to Low`,
      },
      {
        value: `${metric}_asc` as MetricSortOption,
        label: `${config.label}: Low to High`,
      },
    ];
  });

  const wrapCls =
    "relative inline-flex items-center rounded-md sm:rounded-lg border border-gray-300 bg-white " +
    "px-2 py-1 " +
    "text-[10px] sm:text-xs shadow-sm";

  const selectCls =
    "appearance-none bg-transparent text-center text-[#414042] focus:outline-none cursor-pointer " +
    "px-1 py-0.5 pr-5 sm:px-1.5 sm:py-1 sm:pr-7 " +
    "text-[10px] sm:text-xs ";

  return (
    <div className={[wrapCls, className].join(" ")}>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as MetricSortOption)}
        className={selectCls}
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>

      <span className="pointer-events-none absolute inset-y-0 right-3 sm:right-5 flex items-center text-[9px] sm:text-[10px]">
        <FaAngleDown />
      </span>
    </div>
  );
};

export default MetricSortDropdown;