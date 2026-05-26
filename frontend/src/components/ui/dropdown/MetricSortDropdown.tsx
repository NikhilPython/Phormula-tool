// components/ui/MetricSortDropdown.tsx

"use client";

import React from "react";

export type MetricSortOption =
  | "units_desc"
  | "units_asc"
  | "sales_desc"
  | "sales_asc"
  | "profit_desc"
  | "profit_asc"
  | "marketplace_fees_desc"
  | "marketplace_fees_asc";

type MetricSortDropdownProps = {
  value: MetricSortOption;
  onChange: (value: MetricSortOption) => void;
  className?: string;
};

const MetricSortDropdown: React.FC<MetricSortDropdownProps> = ({
  value,
  onChange,
  className = "",
}) => {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value as MetricSortOption)}
      className={[
        "h-9 rounded-lg border border-slate-300 bg-white px-3",
        "text-xs font-medium text-slate-700 outline-none",
        "focus:border-[#5EA68E] focus:ring-1 focus:ring-[#5EA68E]",
        className,
      ].join(" ")}
    >
      <option value="units_desc">Units: High to Low</option>
      <option value="units_asc">Units: Low to High</option>

      <option value="sales_desc">Sales: High to Low</option>
      <option value="sales_asc">Sales: Low to High</option>

      <option value="profit_desc">CM1 Profit: High to Low</option>
      <option value="profit_asc">CM1 Profit: Low to High</option>

      <option value="marketplace_fees_desc">Marketplace Fees: High to Low</option>
      <option value="marketplace_fees_asc">Marketplace Fees: Low to High</option>
    </select>
  );
};

export default MetricSortDropdown;