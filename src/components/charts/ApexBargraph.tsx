// components/charts/ApexBargraph.tsx
"use client";
import dynamic from "next/dynamic";
import React from "react";
import { RangeApi, useGetChartBarQuery } from "@/lib/api/dashboardApi";
const ReactApexChart = dynamic(() => import("react-apexcharts"), { ssr: false });

export default function ApexBargraph({
  rangeApi, monthApi, quarter, year, country,
}: { rangeApi: RangeApi; monthApi?: string; quarter?: string; year: string; country: string }) {
  const enabled = !!year && (!!monthApi || !!quarter || rangeApi === "YTD");
  const { data, error, isFetching } = useGetChartBarQuery(
    { rangeApi, monthApi, quarter, year, country },
    { skip: !enabled }
  );

  if (!enabled) return null;
  if (isFetching) return <div className="text-sm text-slate-500">Loading…</div>;
  if (error) return <div className="text-sm text-red-600">Failed to fetch</div>;
  if (!data?.series?.length) return <div className="text-sm text-slate-500">No data.</div>;

  return (
    <div className="bg-white rounded-xl p-4 shadow-sm">
      <ReactApexChart
        type="bar"
        height={340}
        series={data.series}
        options={{ chart: { toolbar: { show: false } }, xaxis: { categories: data.categories || [] }, dataLabels: { enabled: false } }}
      />
    </div>
  );
}
