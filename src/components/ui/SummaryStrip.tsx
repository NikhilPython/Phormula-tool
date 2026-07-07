// components/ui/SummaryStrip.tsx
"use client";

import React from "react";
import CustomTable from "@/components/ui/CustomTable";

export type Summary = {
  unit_sold: number;
  total_sales: number;
  total_expense: number;
  cm2_profit: number;
};

export default function SummaryStrip({
  summary,
  currencySymbol,
}: {
  summary: Summary;
  currencySymbol: string;
}) {
  const cm2Pct =
    summary.total_sales > 0
      ? `${((summary.cm2_profit / summary.total_sales) * 100).toFixed(2)}%`
      : "0%";

  const faded =
    summary.unit_sold === 0 &&
    summary.total_sales === 0 &&
    summary.total_expense === 0 &&
    summary.cm2_profit === 0;

  const headers = [
    "Units",
    "Sales",
    "Expense",
    "CM2 Profit",
    "CM2 Profit (%)",
  ];

  const rows = [
    [
      summary.unit_sold,
      `${currencySymbol} ${Number(summary.total_sales).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      `${currencySymbol} ${Number(summary.total_expense).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      `${currencySymbol} ${Number(summary.cm2_profit).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
      cm2Pct,
    ],
  ];

  return (
    <div className={faded ? "opacity-40" : undefined}>
      <CustomTable headers={headers} rows={rows} />
    </div>
  );
}
