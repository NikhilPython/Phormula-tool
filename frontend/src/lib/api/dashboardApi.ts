"use client";

import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";

/** —— Helpers —— */
export type RangeApi = "MTD" | "QTD" | "YTD";
export const rangeToApi = (r: string): RangeApi =>
  r === "monthly" ? "MTD" : r === "quarterly" ? "QTD" : "YTD";

export const titleCase = (s?: string) => {
  if (!s) return "";
  return s[0].toUpperCase() + s.slice(1).toLowerCase();
};

const buildParams = (obj: Record<string, any>) => {
  const sp = new URLSearchParams();
  Object.entries(obj).forEach(([k, v]) => {
    if (v !== undefined && v !== null && String(v) !== "") sp.set(k, String(v));
  });
  return `?${sp.toString()}`;
};

/** —— Base URL —— */
const baseUrl =
  process.env.NEXT_PUBLIC_API_BASE?.replace(/\/$/, "") ||
  process.env.FLASK_API_ORIGIN?.replace(/\/$/, "") ||
  "http://13.126.252.94:5000";

/** —— Types —— */
export type Summary = {
  unit_sold: number;
  total_sales: number;
  total_expense: number;
  cm2_profit: number;
};

export type SummaryResp = { summary?: Summary };
export type TableResp = any[] | { rows?: any[]; data?: any[] };
export type ChartResp = { series: any[]; categories?: string[]; labels?: string[] };

/** —— API Slice —— */
export const dashboardApi = createApi({
  reducerPath: "dashboardApi",
  baseQuery: fetchBaseQuery({
    baseUrl,
    prepareHeaders: (headers) => {
      if (typeof window !== "undefined") {
        const token = localStorage.getItem("jwtToken");
        if (token) headers.set("authorization", `Bearer ${token}`);
      }
      return headers;
    },
  }),
  endpoints: (builder) => ({
    /** Summary totals */
    getSummary: builder.query<
      SummaryResp,
      { rangeApi: RangeApi; country: string; year: string; monthApi?: string; quarter?: string }
    >({
      query: ({ rangeApi, country, year, monthApi, quarter }) =>
        `/upload_history2${buildParams({
          range: rangeApi,
          country: country.toLowerCase(),
          year,
          month: monthApi,
          quarter,
        })}`,
    }),

    /** Overview table */
    getOverviewTable: builder.query<
      TableResp,
      { rangeApi: RangeApi; country: string; year: string; monthApi?: string; quarter?: string }
    >({
      query: ({ rangeApi, country, year, monthApi, quarter }) =>
        `/upload_table${buildParams({
          range: rangeApi,
          country: country.toLowerCase(),
          year,
          month: monthApi,
          quarter,
        })}`,
    }),

    /** SKU table */
    getSkuTable: builder.query<
      TableResp,
      { rangeApi: RangeApi; country: string; year: string; monthApi?: string; quarter?: string }
    >({
      query: ({ rangeApi, country, year, monthApi, quarter }) =>
        `/upload_table_sku${buildParams({
          range: rangeApi,
          country: country.toLowerCase(),
          year,
          month: monthApi,
          quarter,
        })}`,
    }),

    /** Charts */
    getChartBar: builder.query<
      ChartResp,
      { rangeApi: RangeApi; country: string; year: string; monthApi?: string; quarter?: string }
    >({
      query: ({ rangeApi, country, year, monthApi, quarter }) =>
        `/charts/bar${buildParams({
          range: rangeApi,
          country: country.toLowerCase(),
          year,
          month: monthApi,
          quarter,
        })}`,
    }),

    getChartPie: builder.query<
      ChartResp,
      { rangeApi: RangeApi; country: string; year: string; monthApi?: string; quarter?: string }
    >({
      query: ({ rangeApi, country, year, monthApi, quarter }) =>
        `/charts/pie${buildParams({
          range: rangeApi,
          country: country.toLowerCase(),
          year,
          month: monthApi,
          quarter,
        })}`,
    }),

    getChartCM2: builder.query<
      ChartResp,
      { rangeApi: RangeApi; country: string; year: string; monthApi?: string; quarter?: string }
    >({
      query: ({ rangeApi, country, year, monthApi, quarter }) =>
        `/charts/cm2${buildParams({
          range: rangeApi,
          country: country.toLowerCase(),
          year,
          month: monthApi,
          quarter,
        })}`,
    }),

    getChartLine: builder.query<
      ChartResp,
      { rangeApi: RangeApi; country: string; year: string; monthApi?: string; quarter?: string }
    >({
      query: ({ rangeApi, country, year, monthApi, quarter }) =>
        `/charts/line${buildParams({
          range: rangeApi,
          country: country.toLowerCase(),
          year,
          month: monthApi,
          quarter,
        })}`,
    }),
  }),
});

export const {
  useGetSummaryQuery,
  useGetOverviewTableQuery,
  useGetSkuTableQuery,
  useGetChartBarQuery,
  useGetChartPieQuery,
  useGetChartCM2Query,
  useGetChartLineQuery,
} = dashboardApi;
