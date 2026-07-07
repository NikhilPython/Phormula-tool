// components/tables/SKUtable.tsx
"use client";
import React, { useMemo } from "react";
import CustomTable from "@/components/ui/CustomTable";
import { RangeApi, useGetSkuTableQuery } from "@/lib/api/dashboardApi";

export default function SKUtable({ rangeApi, country, monthApi, quarter, year }:
  { rangeApi: RangeApi; country: string; monthApi?: string; quarter?: string; year: string }) {
  const enabled = !!year && (!!monthApi || !!quarter || rangeApi === "YTD");
  const { data, isFetching, error } = useGetSkuTableQuery(
    { rangeApi, country, monthApi, quarter, year },
    { skip: !enabled }
  );

  if (!enabled) return null;
  if (isFetching) return <div className="text-sm text-slate-500">Loading…</div>;
  if (error) return <div className="text-sm text-red-600">Failed to fetch</div>;

  const rowsArr = Array.isArray(data) ? data : data?.rows || data?.data || [];
  const headers = rowsArr.length ? Object.keys(rowsArr[0]) : ["No data"];
  const rows = rowsArr.length ? rowsArr.map((r: any) => headers.map((h) => String(r[h] ?? ""))) : [["No rows"]];

  return (
    <div>
      <h3 className="text-sm font-semibold text-slate-700 mb-3">SKU Details</h3>
      <CustomTable headers={headers} rows={rows} />
    </div>
  );
}
