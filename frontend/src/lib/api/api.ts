// lib/api.ts
"use client";

export const API = {
  history: `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history2`,
  tableOverview: `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_table`,
  tableSku: `${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_table_sku`,
  charts: {
    line: `${process.env.NEXT_PUBLIC_API_BASE_URL}/charts/line`,
    cm2: `${process.env.NEXT_PUBLIC_API_BASE_URL}/charts/cm2`,
    bar: `${process.env.NEXT_PUBLIC_API_BASE_URL}/charts/bar`,
    pie: `${process.env.NEXT_PUBLIC_API_BASE_URL}/charts/pie`,
  },
};

export const rangeToApi = (range: string) =>
  range === "monthly" ? "MTD" : range === "quarterly" ? "QTD" : "YTD";

export const titleCase = (s?: string) => {
  const str = s ?? "";
  return str.length ? str[0].toUpperCase() + str.slice(1).toLowerCase() : "";
};

export function buildUrl(endpoint: string, params: Record<string, any>) {
  const url = new URL(endpoint);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && String(v) !== "") {
      url.searchParams.set(k, String(v));
    }
  });
  return url.toString();
}

export async function authedFetchJson<T = any>(url: string): Promise<T> {
  const token =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
  const res = await fetch(url, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} ${text}`);
  }
  return res.json();
}
