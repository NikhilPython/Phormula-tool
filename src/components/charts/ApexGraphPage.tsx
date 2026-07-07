// components/charts/ApexGraphPage.tsx
"use client";
import dynamic from "next/dynamic";
import React from "react";
import { RangeApi, useGetChartLineQuery } from "@/lib/api/dashboardApi";
const ReactApexChart = dynamic(() => import("react-apexcharts"), { ssr: false });

export default function ApexGraphPage(props: { rangeApi: RangeApi; monthApi?: string; quarter?: string; year: string; country: string }) {
  const enabled = !!props.year && (!!props.monthApi || !!props.quarter || props.rangeApi !== "MTD");
  const { data, error, isFetching } = useGetChartLineQuery(props, { skip: !enabled });

  if (!enabled) return null;
  if (isFetching) return <div className="text-sm text-slate-500">Loading…</div>;
  if (error) return <div className="text-sm text-red-600">Failed to fetch</div>;
  if (!data?.series?.length) return <div className="text-sm text-slate-500">No data.</div>;

  return (
    <div className="bg-white rounded-xl p-4 shadow-sm">
      <ReactApexChart
        type="area"
        height={340}
        series={data.series}
        options={{ chart: { toolbar: { show: false } }, xaxis: { categories: data.categories || [] }, dataLabels: { enabled: false }, stroke: { curve: "smooth", width: 3 } }}
      />
    </div>
  );
}
