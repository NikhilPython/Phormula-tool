"use client";

import React, { useMemo, useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import {
  FaCheckCircle as CheckCircle2,
  FaExclamationCircle as AlertCircle,
} from "react-icons/fa";
import { ImInfinite } from "react-icons/im";

import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Button from "@/components/ui/button/Button";
import { useGetUserDataQuery } from "@/lib/api/profileApi";
import { buildCountryMarketplaceMap } from "@/lib/utils/countryMarketplace";
import { StepBadge } from "@/components/common/StepBadge";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const getAuthToken = () =>
  typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

const COUNTRY_TO_MARKETPLACE: Record<string, string> = {
  uk: "A1F83G8C2ARO7P",
  us: "ATVPDKIKX0DER",
  canada: "A2EUQ1WTGCTBG2",
};

const COUNTRY_TO_REGION: Record<string, string> = {
  uk: "eu-west-1",
  us: "us-east-1",
  canada: "us-east-1",
};

/** ======= HARD OVERRIDE FOR TESTING ======= **/
const FORCE = {
  enabled: false,
  country: "uk",
  region: "eu-west-1",
  marketplaceId: "A1F83G8C2ARO7P",
};
/** ======================================== **/

const fullMonthNames = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];



const monthSlugOrder = fullMonthNames.map((m) => m.toLowerCase());
const two = (n: number | string) => String(n).padStart(2, "0");


function updateLatestFetchedPeriod(monthSlug: string, yearStr: string) {
  if (typeof window === "undefined") return;

  const key = "latestFetchedPeriod";

  const newMonthIdx = monthSlugOrder.indexOf(monthSlug.toLowerCase());
  if (newMonthIdx === -1) return;

  const newYear = parseInt(yearStr, 10);
  if (Number.isNaN(newYear)) return;

  const newValue = newYear * 12 + newMonthIdx;

  const existingRaw = window.localStorage.getItem(key);
  if (!existingRaw) {
    window.localStorage.setItem(
      key,
      JSON.stringify({ month: monthSlug, year: yearStr })
    );
    return;
  }

  try {
    const existing = JSON.parse(existingRaw);
    const existingMonthIdx = monthSlugOrder.indexOf(
      String(existing.month || "").toLowerCase()
    );
    const existingYear = parseInt(existing.year, 10);

    if (
      Number.isNaN(existingYear) ||
      existingMonthIdx === -1 ||
      newValue >= existingYear * 12 + existingMonthIdx
    ) {
      window.localStorage.setItem(
        key,
        JSON.stringify({ month: monthSlug, year: yearStr })
      );
    }
  } catch {
    window.localStorage.setItem(
      key,
      JSON.stringify({ month: monthSlug, year: yearStr })
    );
  }
}

/** ---------------- JSON API helper ---------------- */
async function apiJson(path: string, options: RequestInit = {}) {
  const token = getAuthToken();

  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  const text = await res.text().catch(() => "");
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const msg =
      data?.error || data?.message || text || `Request failed: ${res.status}`;
    throw new Error(msg);
  }

  return data;
}

async function fetchInventoryLedgerSummary(params: {
  marketplace_id: string;
  month?: string;
  start_date?: string;
  end_date?: string;
  store_in_db?: boolean;
  keep_first_last?: boolean;
}) {
  const qs = new URLSearchParams();

  qs.set("marketplace_id", params.marketplace_id);
  qs.set("store_in_db", String(params.store_in_db ?? true));
  qs.set("keep_first_last", String(params.keep_first_last ?? false));

  if (params.month) {
    qs.set("month", params.month);
  }

  if (params.start_date && params.end_date) {
    qs.set("start_date", params.start_date);
    qs.set("end_date", params.end_date);
  }

  return apiJson(`/amazon_api/inventory/ledger-summary?${qs.toString()}`, {
    method: "GET",
  });
}

/** ---------------- localStorage run-once guards ---------------- */
function lsKeyFees(country: string) {
  return `feesSynced:${country}`;
}

function lsKeyFeeUpload(country: string) {
  return `feeUploadReady:${country}`;
}

/** ✅ NEW: run-once key for inventory aged (per country) */
function lsKeyInventoryAged(country: string) {
  return `inventoryAgedSynced:${country}`;
}

function wasDone(key: string) {
  if (typeof window === "undefined") return false;
  return window.localStorage.getItem(key) === "1";
}
function markDone(key: string) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(key, "1");
}

/** ✅ NEW: hit /amazon_api/inventory/aged with NO params, once per country */
async function syncInventoryAgedOnce(country: string) {
  const key = lsKeyInventoryAged(country);
  if (wasDone(key)) return;

  await apiJson(`/amazon_api/inventory/aged`, { method: "GET" });

  markDone(key);
}

/**
 * Ensure we hit:
 * 1) /amazon_api/fees/sync_and_upload   (one-time per country)
 * 2) /amazon_api/inventory/aged         (one-time per country)   ✅ NEW (no params)
 * 3) /fetch_fees                       (one-time per country+year+month)
 */
async function ensureFeesPrimedOnce(params: {
  country: string;
  regionUsed?: string;
  marketplaceId: string;
  year: number;
  month: number; // 1-12
}) {
  const { country, regionUsed, marketplaceId, year, month } = params;

  const uploadKey = lsKeyFeeUpload(country);
  if (!wasDone(uploadKey)) {
    await apiJson(`/amazon_api/fees/sync_and_upload`, {
      method: "POST",
      body: JSON.stringify({
        country,
        marketplace_id: marketplaceId,
        region: regionUsed,
        transit_time: 0,
        stock_unit: 0,
      }),
    });

    // ✅ NEW: Immediately after fees sync/upload, call inventory aged with NO params
    // (run-once per country; does not affect existing flow)
    await syncInventoryAgedOnce(country);

    markDone(uploadKey);
  }

  const feesKey = lsKeyFees(country);
  if (!wasDone(feesKey)) {
    // backend requires a month/year, so we send the current selection
    // but we only do this ONCE per country
    const monthParam = `${year}-${two(month)}`;
    await apiJson(`/fetch_fees`, {
      method: "POST",
      body: JSON.stringify({
        region: regionUsed,
        marketplace_id: marketplaceId,
        month: monthParam,
        year: String(year),
        country,
      }),
    });

    markDone(feesKey);
  }

}


/** -------- Better error parsing for monthly_transactions -------- */
async function readErrorMessage(res: Response) {
  const ct = res.headers.get("content-type") || "";
  const raw = await res.text().catch(() => "");
  if (!raw) return `API ${res.status} ${res.statusText}`;

  if (ct.includes("application/json")) {
    try {
      const j = JSON.parse(raw);
      const e = j?.error || j?.message || j;
      return typeof e === "string" ? e : JSON.stringify(e, null, 2);
    } catch {
      return raw;
    }
  }
  return raw;
}

/** ---------------- Monthly transactions Excel fetch (no download) ---------------- */
async function fetchMonthlyTransactionsExcel(params: {
  year: number;
  month: number; // 1-12
  marketplace_id: string;
  country: string;
  run_upload_pipeline: boolean;
  store_in_db: boolean;
}) {
  const token = getAuthToken();

  if (params.month < 1 || params.month > 12) {
    throw new Error(`Invalid month: ${params.month}. Expected 1-12.`);
  }

  const qs = new URLSearchParams({
    year: String(params.year),
    month: String(params.month),
    marketplace_id: params.marketplace_id,
    run_upload_pipeline: String(params.run_upload_pipeline),
    country: params.country,
    format: "excel",
    store_in_db: String(params.store_in_db),
  });

  const url = `${API_BASE}/amazon_api/finances/monthly_transactions?${qs.toString()}`;
  const res = await fetch(url, {
    method: "GET",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  if (!res.ok) {
    const msg = await readErrorMessage(res);
    throw new Error(`API ${res.status} ${res.statusText}\nURL: ${url}\n\n${msg}`);
  }

  await res.arrayBuffer();
  return { ok: true, url };
}

/** ---------------- Forecast fetch (backend expects month name, not 03) ---------------- */
async function runForecast(params: {
  country: string;
  year: number | string;
  month: number | string; // 1-12, "03", or month name
}) {
  const token = getAuthToken();

  let monthValue = String(params.month).trim().toLowerCase();

  // If numeric month is provided, convert to full month name expected by backend
  const numericMonth = parseInt(monthValue, 10);
  if (!Number.isNaN(numericMonth) && numericMonth >= 1 && numericMonth <= 12) {
    monthValue = fullMonthNames[numericMonth - 1].toLowerCase();
  }

  const qs = new URLSearchParams({
    country: params.country,
    year: String(params.year),
    month: monthValue,
  });

  const url = `${API_BASE}/api/forecast?${qs.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  if (!res.ok) {
    const msg = await readErrorMessage(res);
    throw new Error(`Forecast API ${res.status} ${res.statusText}\nURL: ${url}\n\n${msg}`);
  }

  const blob = await res.blob();

  return {
    ok: true,
    url,
    blob,
    filename: res.headers.get("Content-Disposition") || "forecast-file",
  };
}

async function fetchForecastMonthRange(params: { country: string }) {
  const token = getAuthToken();

  const qs = new URLSearchParams({
    country: params.country,
  });

  const url = `${API_BASE}/api/forecast_monthrange?${qs.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  const text = await res.text().catch(() => "");
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const msg =
      data?.error || data?.message || text || `Forecast month range failed: ${res.status}`;
    throw new Error(msg);
  }

  return data;
}

async function fetchDispatchFile(params: {
  country: string;
  month: string; // full month name
  year: number | string;
}) {
  const token = getAuthToken();

  const qs = new URLSearchParams({
    country: params.country,
    month: params.month,
    year: String(params.year),
  });

  const url = `${API_BASE}/getDispatchfile?${qs.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  if (!res.ok) {
    const msg = await readErrorMessage(res);
    throw new Error(`Dispatch file API ${res.status} ${res.statusText}\nURL: ${url}\n\n${msg}`);
  }

  const blob = await res.blob();
  return { ok: true, url, blob };
}

// async function runPurchaseOrder(params: {
//   country: string;
//   year: number | string;
//   month: number | string;
// }) {
//   const token = getAuthToken();

//   // let monthValue = String(params.month).trim().toLowerCase();
//   let monthValue = "april"

//   const numericMonth = parseInt(monthValue, 10);
//   if (!Number.isNaN(numericMonth) && numericMonth >= 1 && numericMonth <= 12) {
//     monthValue = fullMonthNames[numericMonth - 1].toLowerCase();
//   }

//   const formData = new FormData();
//   formData.append("month", monthValue);
//   formData.append("year", String(params.year));
//   formData.append("country", params.country.toLowerCase());

//   const url = `${API_BASE}/purchase_order`;

//   const res = await fetch(url, {
//     method: "POST",
//     headers: {
//       ...(token ? { Authorization: `Bearer ${token}` } : {}),
//     },
//     body: formData,
//   });

//   const text = await res.text().catch(() => "");
//   let data: any = {};
//   try {
//     data = text ? JSON.parse(text) : {};
//   } catch {
//     data = { raw: text };
//   }

//   if (!res.ok) {
//     const msg =
//       data?.error || data?.message || text || `Purchase order failed: ${res.status}`;
//     throw new Error(msg);
//   }

//   return data;
// }

async function fetchGeneratedPOFile(params: {
  country: string;
  month: string;
  year: number | string;
}) {
  const token = getAuthToken();

  const qs = new URLSearchParams({
    country: params.country,
    month: String(params.month).trim().toLowerCase(),
    year: String(params.year),
  });

  const url = `${API_BASE}/getDispatchfile2?${qs.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  return res;
}

async function runPurchaseOrder(params: {
  country: string;
  year: number | string;
  month: number | string;
}) {
  const token = getAuthToken();

  let monthValue = String(params.month).trim().toLowerCase();

  const numericMonth = parseInt(monthValue, 10);
  if (!Number.isNaN(numericMonth) && numericMonth >= 1 && numericMonth <= 12) {
    monthValue = fullMonthNames[numericMonth - 1].toLowerCase();
  }

  const formData = new FormData();
  formData.append("month", monthValue);
  formData.append("year", String(params.year));
  formData.append("country", params.country.toLowerCase());

  const url = `${API_BASE}/purchase_order`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      Accept: "application/json",
    },
    body: formData,
  });

  const text = await res.text().catch(() => "");
  let data: any = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const msg =
      data?.error || data?.message || text || `Purchase order failed: ${res.status}`;
    throw new Error(msg);
  }

  return data;
}

async function fetchPurchaseOrderFile(params: {
  country: string;
  month: string; // must match stored month, e.g. "april"
  year: number | string;
}) {
  const token = getAuthToken();

  const qs = new URLSearchParams({
    country: params.country,
    month: params.month.toLowerCase(),
    year: String(params.year),
  });

  const url = `${API_BASE}/getDispatchfile2?${qs.toString()}`;

  const res = await fetch(url, {
    method: "GET",
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });

  if (!res.ok) {
    const msg = await readErrorMessage(res);
    throw new Error(`Purchase order file API ${res.status} ${res.statusText}\nURL: ${url}\n\n${msg}`);
  }

  const blob = await res.blob();
  return { ok: true, url, blob };
}

// function getMonthNameFromInput(month: number | string) {
//   const raw = String(month).trim();
//   const numericMonth = parseInt(raw, 10);

//   if (!Number.isNaN(numericMonth) && numericMonth >= 1 && numericMonth <= 12) {
//     return fullMonthNames[numericMonth - 1];
//   }

//   const normalized = raw.toLowerCase();
//   const idx = fullMonthNames.findIndex(
//     (m) => m.toLowerCase() === normalized
//   );

//   if (idx !== -1) return fullMonthNames[idx];

//   return raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase();
// }

function getMonthNameFromInput(month: number | string) {
  const raw = String(month).trim();
  const numericMonth = parseInt(raw, 10);

  if (!Number.isNaN(numericMonth) && numericMonth >= 1 && numericMonth <= 12) {
    return fullMonthNames[numericMonth - 1];
  }

  const normalized = raw.toLowerCase();
  const idx = fullMonthNames.findIndex((m) => m.toLowerCase() === normalized);

  if (idx !== -1) return fullMonthNames[idx];

  return raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase();
}

function getNextMonthAndYear(month: number | string, year: number | string) {
  const monthName = getMonthNameFromInput(month);
  const monthIndex = fullMonthNames.findIndex(
    (m) => m.toLowerCase() === monthName.toLowerCase()
  );

  if (monthIndex === -1) {
    return {
      month: monthName,
      year: String(year),
    };
  }

  const nextIndex = (monthIndex + 1) % 12;
  const nextYear =
    monthIndex === 11 ? String(Number(year) + 1) : String(year);

  return {
    month: fullMonthNames[nextIndex],
    year: nextYear,
  };
}


function parseForecastLabelToMonthName(label?: string) {
  if (!label) return null;

  const cleaned = String(label).trim().replace(/ Sold$/i, "");
  const match = cleaned.match(/^([A-Za-z]{3})'(\d{2})$/);

  if (!match) return null;

  const short = match[1].toLowerCase();
  const shortMonths = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
  const idx = shortMonths.indexOf(short);

  if (idx === -1) return null;
  return fullMonthNames[idx];
}

async function runForecastAndPoSequence(params: {
  country: string;
  year: number | string;
  month: number | string;
  setStep: (step: number, label: string, percentage?: number, detail?: string) => void;
}) {
  const selectedMonthName = getMonthNameFromInput(params.month);
  const selectedYearStr = String(params.year);

  // current going month = selected month + 1
  const currentGoingPeriod = getNextMonthAndYear(selectedMonthName, selectedYearStr);
  const currentGoingMonthName = currentGoingPeriod.month;
  const currentGoingMonthLower = currentGoingMonthName.toLowerCase();
  const currentGoingYearStr = currentGoingPeriod.year;

  // dispatch month = current going month + 1
  const dispatchPeriod = getNextMonthAndYear(
    currentGoingMonthName,
    currentGoingYearStr
  );

  params.setStep(
    7,
    "Forecast",
    0,
    `Running inventory forecast for ${currentGoingMonthName} ${currentGoingYearStr}...`
  );
  await runForecast({
    country: params.country,
    year: currentGoingYearStr,
    month: currentGoingMonthName,
  });

  params.setStep(
    7,
    "Forecast",
    25,
    `Fetching dispatch file for ${dispatchPeriod.month} ${dispatchPeriod.year}...`
  );
  await fetchDispatchFile({
    country: params.country,
    month: dispatchPeriod.month,
    year: dispatchPeriod.year,
  });

  params.setStep(
    7,
    "Forecast",
    55,
    `Checking purchase order file for ${currentGoingMonthName} ${currentGoingYearStr}...`
  );
  let poRes = await fetchGeneratedPOFile({
    country: params.country,
    month: currentGoingMonthLower,
    year: currentGoingYearStr,
  });

  if (!poRes.ok && poRes.status === 404) {
    params.setStep(
      7,
      "Forecast",
      75,
      `Generating purchase order for ${currentGoingMonthName} ${currentGoingYearStr}...`
    );

    await runPurchaseOrder({
      country: params.country,
      year: currentGoingYearStr,
      month: currentGoingMonthLower,
    });

    params.setStep(7, "Forecast", 90, "Fetching purchase order file...");
    poRes = await fetchGeneratedPOFile({
      country: params.country,
      month: currentGoingMonthLower,
      year: currentGoingYearStr,
    });
  }

  if (!poRes.ok) {
    const msg = await readErrorMessage(poRes);
    throw new Error(
      `Purchase order file API ${poRes.status} ${poRes.statusText}\n\n${msg}`
    );
  }

  await poRes.blob();

  params.setStep(7, "Forecast", 100, "Forecast, dispatch, and purchase order ready");
}

function monthValue(y: number, m1: number) {
  // m1: 1..12
  return y * 12 + (m1 - 1);
}

function addMonthsUTC(year: number, month1to12: number, delta: number) {
  const base = monthValue(year, month1to12);
  const v = base + delta;
  const y = Math.floor(v / 12);
  const m1 = (v % 12) + 1;
  return { y, m1 };
}

function formatYMDUTC(d: Date) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function startOfMonthUTC(year: number, month1: number) {
  return new Date(Date.UTC(year, month1 - 1, 1));
}

function endOfMonthUTC(year: number, month1: number) {
  return new Date(Date.UTC(year, month1, 0));
}

function buildLedgerRange(period: number | "lifetime") {
  const now = new Date();
  const currentYear = now.getUTCFullYear();
  const currentMonth1 = now.getUTCMonth() + 1;

  const totalMonths = period === "lifetime" ? 24 : Number(period);

  const start = addMonthsUTC(currentYear, currentMonth1, -(totalMonths - 1));
  const startDate = startOfMonthUTC(start.y, start.m1);
  const endDate = endOfMonthUTC(currentYear, currentMonth1);

  return {
    start_date: formatYMDUTC(startDate),
    end_date: formatYMDUTC(endDate),
  };
}

function getEarliestAllowedMonthUTC() {
  const now = new Date();

  // cutoff = now - 2 years (UTC)
  const cutoff = new Date(now.getTime());
  cutoff.setUTCFullYear(cutoff.getUTCFullYear() - 2);

  // monthStart = first day of cutoff's month (00:00Z)
  const monthStart = new Date(Date.UTC(cutoff.getUTCFullYear(), cutoff.getUTCMonth(), 1));

  // If cutoff is AFTER the month start, that month is not fully allowed (because postedAfter would be the 1st),
  // so earliest month must be NEXT month.
  const cutoffIsAfterMonthStart = cutoff.getTime() > monthStart.getTime();

  const earliest = cutoffIsAfterMonthStart
    ? new Date(Date.UTC(cutoff.getUTCFullYear(), cutoff.getUTCMonth() + 1, 1))
    : monthStart;

  return {
    y: earliest.getUTCFullYear(),
    m1: earliest.getUTCMonth() + 1, // 1..12
  };
}


function getLatestAllowedMonthUTC() {
  // latest allowed = previous UTC month (avoid current ongoing)
  const now = new Date();
  const y = now.getUTCFullYear();
  const m1 = now.getUTCMonth() + 1;
  return addMonthsUTC(y, m1, -1);
}

function clampToAllowedRangeUTC(year: number, month1to12: number) {
  const earliest = getEarliestAllowedMonthUTC();
  const latest = getLatestAllowedMonthUTC();

  const sel = monthValue(year, month1to12);
  const minV = monthValue(earliest.y, earliest.m1);
  const maxV = monthValue(latest.y, latest.m1);

  if (sel < minV) return { y: earliest.y, m1: earliest.m1, clamped: true };
  if (sel > maxV) return { y: latest.y, m1: latest.m1, clamped: true };
  return { y: year, m1: month1to12, clamped: false };
}

function isSelectableUTC(year: number, month1to12: number) {
  const earliest = getEarliestAllowedMonthUTC();
  const latest = getLatestAllowedMonthUTC();
  const v = monthValue(year, month1to12);
  return (
    v >= monthValue(earliest.y, earliest.m1) &&
    v <= monthValue(latest.y, latest.m1)
  );
}

/** Build range ending at previous month (latest allowed) */
function buildMonthRange(count: number) {
  const latest = getLatestAllowedMonthUTC();
  const anchor = new Date(Date.UTC(latest.y, latest.m1 - 1, 1));

  const out: { y: number; mIdx: number; mNum: number }[] = [];
  for (let i = 0; i < count; i++) {
    const d = new Date(Date.UTC(anchor.getUTCFullYear(), anchor.getUTCMonth() - i, 1));
    const y = d.getUTCFullYear();
    const mIdx = d.getUTCMonth();
    out.push({ y, mIdx, mNum: mIdx + 1 });
  }
  return out.reverse();
}

/** Build "Lifetime" range from earliest allowed month to latest allowed month */
function buildLifetimeRange() {
  const earliest = getEarliestAllowedMonthUTC();
  const latest = getLatestAllowedMonthUTC();

  const startY = earliest.y;
  const startMIdx = earliest.m1 - 1;

  const endY = latest.y;
  const endMIdx = latest.m1 - 1;

  const startValue = startY * 12 + startMIdx;
  const endValue = endY * 12 + endMIdx;
  if (endValue < startValue) return [];

  const out: { y: number; mIdx: number; mNum: number }[] = [];
  for (let v = startValue; v <= endValue; v++) {
    const y = Math.floor(v / 12);
    const mIdx = v % 12;
    out.push({ y, mIdx, mNum: mIdx + 1 });
  }
  return out;
}

type Props = {
  region?: string;
  country?: string;
  onClose?: () => void;
};

const AmazonFinancialDashboard: React.FC<Props> = ({ region, country, onClose }) => {
  const router = useRouter();

  const { data: user } = useGetUserDataQuery();

  const countryMarketplaceMap = useMemo(() => {
    if (!user?.country || !user?.marketplace_id) return {};
    return {
      [user.country]: user.marketplace_id,
    };
  }, [user]);

  let countryUsed = (country || "uk").toLowerCase();

  // normalize dropdown values just in case
  if (countryUsed === "united kingdom") countryUsed = "uk";
  if (countryUsed === "united states") countryUsed = "us";

  let marketplaceIdUsed = COUNTRY_TO_MARKETPLACE[countryUsed];
  let regionUsed = COUNTRY_TO_REGION[countryUsed];

  if (FORCE.enabled) {
    countryUsed = FORCE.country;
    marketplaceIdUsed = FORCE.marketplaceId;
    regionUsed = FORCE.region;
  }

  if (!marketplaceIdUsed) {
    throw new Error(`Marketplace not configured for ${countryUsed}`);
  }

  if (!regionUsed) {
    throw new Error(`Region not configured for ${countryUsed}`);
  }

  const earliest = useMemo(() => getEarliestAllowedMonthUTC(), []);
  const latest = useMemo(() => getLatestAllowedMonthUTC(), []);

  // Default selection = latest allowed month (previous month)
  const [selYear, setSelYear] = useState(String(latest.y));
  const [selMonth, setSelMonth] = useState(two(latest.m1));

  const [error, setError] = useState<string>("");
  const [message, setMessage] = useState<string>("");

  const [busy, setBusy] = useState(false);

  const TOTAL_FETCH_SECONDS = 15 * 60;

  const [remainingSeconds, setRemainingSeconds] = useState<number | null>(null);

  // 6-step progress tracking
  const [currentStep, setCurrentStep] = useState<number>(0); // 0 = not started, 1-6 = active step
  const [completedSteps, setCompletedSteps] = useState<Set<number>>(new Set());
  const [stepProgress, setStepProgress] = useState<{
    active: boolean;
    label: string;
    percentage: number;
    detail?: string;
  }>({
    active: false,
    label: "",
    percentage: 0,
    detail: "",
  });

  const [rangeProgress, setRangeProgress] = useState<{
    currentMonth: number;
    totalMonths: number;
    ok: number;
    fail: number;
  }>({
    currentMonth: 0,
    totalMonths: 0,
    ok: 0,
    fail: 0,
  });

  useEffect(() => {
    if (!busy) {
      setRemainingSeconds(null);
      return;
    }

    setRemainingSeconds(TOTAL_FETCH_SECONDS);

    const interval = setInterval(() => {
      setRemainingSeconds((prev) => {
        if (!prev || prev <= 1) {
          clearInterval(interval);
          return 0;
        }
        return prev - 1;
      });
    }, 1000);

    return () => clearInterval(interval);
  }, [busy]);

  const markStepComplete = (step: number) => {
    setCompletedSteps((prev) => new Set([...prev, step]));
  };

  const setStep = (step: number, label: string, percentage: number = 0, detail?: string) => {
    setCurrentStep(step);
    setStepProgress({
      active: true,
      label,
      percentage: Math.min(100, Math.max(0, percentage)),
      detail,
    });
  };

  const [selectedPeriod, setSelectedPeriod] = useState<number | null>(24);

  const wrap = async (fn: () => Promise<void>) => {
    try {
      setBusy(true);
      setError("");
      setMessage("");
      await fn();
    } catch (e: any) {
      setError(e?.message || "Unknown error");
    } finally {
      setBusy(false);
    }
  };

  /** Years allowed in dropdown: earliest.y ... latest.y */
  const allowedYears = useMemo(() => {
    const ys: number[] = [];
    for (let y = earliest.y; y <= latest.y; y++) ys.push(y);
    return ys;
  }, [earliest.y, latest.y]);

  const platform = `amazon_${countryUsed}`;

  const handleFetchByMonth = () =>
    wrap(async () => {
      const yRaw = parseInt(selYear, 10);
      const mRaw = parseInt(selMonth, 10);

      const clamped = clampToAllowedRangeUTC(yRaw, mRaw);
      const y = clamped.y;
      const mNum = clamped.m1;

      if (clamped.clamped) {
        setMessage(
          `Amazon allows ~2 years of finance transactions. Adjusted selection to ${y}-${two(mNum)}.`
        );
        setSelYear(String(y));
        setSelMonth(two(mNum));
      }

      // Reset progress
      setCurrentStep(1);
      setCompletedSteps(new Set());
      setStepProgress({ active: true, label: "", percentage: 0 });

      // Step 1: Currency Conversion
      setStep(1, "Currency Conversion", 0, "Converting currency rates...");
      await new Promise((resolve) => setTimeout(resolve, 500)); // Simulate currency conversion
      markStepComplete(1);

      // Step 2: Category Fees (takes ~4 minutes)
      setStep(2, "Category Fees", 0, "Syncing category fees...");

      // Simulate progress for ~4 minutes (240000ms)
      const progressInterval = setInterval(() => {
        setStepProgress((prev) => {
          if (prev.percentage < 95) {
            return { ...prev, percentage: prev.percentage + 1 };
          }
          return prev;
        });
      }, 2400); // Update every 2.4 seconds to reach ~95% in ~4 minutes

      try {
        await ensureFeesPrimedOnce({
          country: countryUsed,
          regionUsed,
          marketplaceId: marketplaceIdUsed,
          year: y,
          month: mNum,
        });
        clearInterval(progressInterval);
        setStep(2, "Category Fees", 100, "Category fees synced successfully");
        await new Promise((resolve) => setTimeout(resolve, 500));
        markStepComplete(2);
      } catch (e) {
        clearInterval(progressInterval);
        throw e;
      }

      // Step 3: Fee Preview
      setStep(3, "Fee Preview", 0, "Preparing fee preview...");
      await new Promise((resolve) => setTimeout(resolve, 1000)); // Simulate fee preview
      markStepComplete(3);

      // Step 4: Inventory
      // Step 4: Inventory
      setStep(4, "Inventory", 0, "Syncing inventory data...");

      // ye existing API as-is hit hogi
      await syncInventoryAgedOnce(countryUsed);

      // iske alawa ledger-summary bhi hit hogi
      await fetchInventoryLedgerSummary({
        marketplace_id: marketplaceIdUsed,
        month: `${y}-${two(mNum)}`,
        store_in_db: true,
        keep_first_last: false,
      });

      markStepComplete(4);

      // Step 5: Historic Data (per month, ~20 seconds)
      setStep(5, "Historic Data", 0, `Fetching data for ${y}-${two(mNum)}...`);
      await fetchMonthlyTransactionsExcel({
        year: y,
        month: mNum,
        marketplace_id: marketplaceIdUsed,
        country: countryUsed,
        run_upload_pipeline: true,
        store_in_db: true,
      });
      markStepComplete(5);

      // Step 6: Live Data
      setStep(6, "Live Data", 100, "Finalizing data sync...");
      await new Promise((resolve) => setTimeout(resolve, 500));
      markStepComplete(6);

      // Step 7: Inventory Forecast + Purchase Order
      if (selectedPeriod && selectedPeriod >= 6) {
        await runForecastAndPoSequence({
          country: countryUsed,
          year: y,
          month: mNum,
          setStep,
        });
        markStepComplete(7);
      } else {
        setStep(7, "Inventory Forecast", 100, "Skipped (requires ≥ 6 months data)");
        markStepComplete(7);
      }

      // Step 8: Plotting Graph
      setStep(8, "Plotting Graph", 0, "Preparing charts...");
      await new Promise((resolve) => setTimeout(resolve, 1000));
      markStepComplete(8);

      await new Promise((r) => setTimeout(r, 600));

      const monthSlug = fullMonthNames[mNum - 1].toLowerCase();
      updateLatestFetchedPeriod(monthSlug, String(y));

      setMessage(
        `Fetched ${countryUsed}: ${y}-${two(mNum)}. Inventory forecast and purchase order generated successfully.`
      );

      localStorage.setItem("selectedPlatform", `amazon-${countryUsed}`);
      window.dispatchEvent(new Event("storage"));

      if (onClose) onClose();
      router.push(`/pnl-dashboard/MTD/${countryUsed}/${monthSlug}/${y}`);
    });

  const handleFetchRange = () =>
    wrap(async () => {
      const isLifetime = selectedPeriod === 24;

      // Note: buildLifetimeRange already clamps to allowed window
      const months = isLifetime
        ? buildLifetimeRange()
        : buildMonthRange((selectedPeriod as number) || 0);

      if (!months.length) {
        setMessage("No months available to fetch.");
        return;
      }

      if (![3, 6, 12, 24].includes(selectedPeriod as number)) {
        setMessage("Please select 3, 6, 12, or Lifetime.");
        return;
      }

      // Reset progress
      setCurrentStep(1);
      setCompletedSteps(new Set());
      setRangeProgress({ currentMonth: 0, totalMonths: months.length, ok: 0, fail: 0 });

      // Step 1: Currency Conversion
      setStep(1, "Currency Conversion", 0, "Converting currency rates...");
      await new Promise((resolve) => setTimeout(resolve, 500));
      markStepComplete(1);

      // Step 2: Category Fees (takes ~4 minutes, only once)
      setStep(2, "Category Fees", 0, "Syncing category fees...");

      // Simulate progress for ~4 minutes
      const progressInterval = setInterval(() => {
        setStepProgress((prev) => {
          if (prev.percentage < 95) {
            return { ...prev, percentage: prev.percentage + 1 };
          }
          return prev;
        });
      }, 2400); // Update every 2.4 seconds to reach ~95% in ~4 minutes

      try {
        await ensureFeesPrimedOnce({
          country: countryUsed,
          regionUsed,
          marketplaceId: marketplaceIdUsed,
          year: months[0].y,
          month: months[0].mNum,
        });
        clearInterval(progressInterval);
        setStep(2, "Category Fees", 100, "Category fees synced successfully");
        await new Promise((resolve) => setTimeout(resolve, 500));
        markStepComplete(2);
      } catch (e) {
        clearInterval(progressInterval);
        console.error("fees priming failed", e);
      }

      // Step 3: Fee Preview
      setStep(3, "Fee Preview", 0, "Preparing fee preview...");
      await new Promise((resolve) => setTimeout(resolve, 1000));
      markStepComplete(3);

      // Step 4: Inventory
      // Step 4: Inventory
      setStep(4, "Inventory", 0, "Syncing inventory data...");
      try {
        // existing API as-is
        await syncInventoryAgedOnce(countryUsed);

        // additional ledger-summary API
        const ledgerRange = buildLedgerRange(
          selectedPeriod === 24 ? "lifetime" : Number(selectedPeriod)
        );

        await fetchInventoryLedgerSummary({
          marketplace_id: marketplaceIdUsed,
          start_date: ledgerRange.start_date,
          end_date: ledgerRange.end_date,
          store_in_db: true,
          keep_first_last: false,
        });

        markStepComplete(4);
      } catch (e) {
        console.error("inventory sync failed", e);
        throw e;
      }

      // Step 5: Historic Data (per month, ~20 seconds each)
      setStep(5, "Historic Data", 0, `Fetching data for ${months.length} months...`);
      let ok = 0;
      let fail = 0;

      for (let i = 0; i < months.length; i++) {
        const { y, mNum, mIdx } = months[i];
        const monthProgress = Math.round(((i + 1) / months.length) * 100);

        setStep(5, "Historic Data", monthProgress, `Month ${i + 1}/${months.length}: ${y}-${two(mNum)}`);
        setRangeProgress({ currentMonth: i + 1, totalMonths: months.length, ok, fail });

        try {
          await fetchMonthlyTransactionsExcel({
            year: y,
            month: mNum,
            marketplace_id: marketplaceIdUsed,
            country: countryUsed,
            run_upload_pipeline: true,
            store_in_db: true,
          });
          ok++;
          setRangeProgress((prev) => ({ ...prev, ok }));
        } catch (e: any) {
          console.error("monthly_transactions failed for", y, mNum, e?.message || e);
          fail++;
          setRangeProgress((prev) => ({ ...prev, fail }));
        }

        const monthSlug = fullMonthNames[mIdx].toLowerCase();
        updateLatestFetchedPeriod(monthSlug, String(y));
      }
      markStepComplete(5);

      // Step 6: Live Data
      setStep(6, "Live Data", 100, "Finalizing data sync...");
      await new Promise((resolve) => setTimeout(resolve, 500));
      markStepComplete(6);

      // Step 7: Inventory Forecast + Purchase Order
      const lastMonth = months[months.length - 1];

      setStep(
        7,
        "Forecast",
        0,
        `Running inventory forecast for ${lastMonth.y}-${two(lastMonth.mNum)}...`
      );

      if (selectedPeriod && selectedPeriod >= 6) {
        await runForecastAndPoSequence({
          country: countryUsed,
          year: lastMonth.y,
          month: lastMonth.mNum,
          setStep,
        });
        markStepComplete(7);
      } else {
        setStep(
          7,
          "Inventory Forecast",
          100,
          "Skipped (requires ≥ 6 months data)"
        );
        markStepComplete(7);
      }

      // Step 8: Plotting Graph
      setStep(8, "Plotting Graph", 0, "Preparing charts...");
      await new Promise((resolve) => setTimeout(resolve, 1000));
      markStepComplete(8);

      await new Promise((r) => setTimeout(r, 600));

      const last = months[months.length - 1];
      const latestMonthSlug = fullMonthNames[last.mIdx].toLowerCase();

      setMessage(
        `Fetch complete for ${countryUsed}: ${isLifetime ? "lifetime (allowed window)" : `${selectedPeriod} months`
        }, ok ${ok}, failed ${fail}. Inventory forecast and purchase order generated successfully for ${last.y}-${two(last.mNum)}.`
      );

      localStorage.setItem("selectedPlatform", `amazon-${countryUsed}`);
      window.dispatchEvent(new Event("storage"));

      if (onClose) onClose();
      router.push(`/pnl-dashboard/MTD/${countryUsed}/${latestMonthSlug}/${last.y}`);
    });

  const selectedIsValid =
    isSelectableUTC(parseInt(selYear, 10), parseInt(selMonth, 10));

  const handleQuickForecastTest = () =>
    wrap(async () => {
      if (!selectedPeriod || selectedPeriod < 6) {
        throw new Error("Quick Forecast Test requires at least 6 months selected.");
      }

      const months = buildMonthRange(selectedPeriod);
      if (months.length < 5) {
        throw new Error("Forecast requires at least 5 months of historic data.");
      }

      console.log("🚀 QUICK TEST START");

      for (let i = 0; i < months.length; i++) {
        const { y, mNum } = months[i];

        console.log(`📦 Fetching historic data for ${y}-${two(mNum)}...`);
        await fetchMonthlyTransactionsExcel({
          year: y,
          month: mNum,
          marketplace_id: marketplaceIdUsed,
          country: countryUsed,
          run_upload_pipeline: true,
          store_in_db: true,
        });
      }

      const lastMonth = months[months.length - 1];

      await runForecastAndPoSequence({
        country: countryUsed,
        year: lastMonth.y,
        month: lastMonth.mNum,
        setStep,
      });

      setMessage(
        `⚡ Quick forecast + purchase order completed for ${selectedPeriod} months using ${lastMonth.y}-${two(lastMonth.mNum)}.`
      );
    });

  const shouldShowForecastStep = selectedPeriod !== null && selectedPeriod >= 6;

  const steps = shouldShowForecastStep
    ? [
      { num: 1, label: "Currency Conversion" },
      { num: 2, label: "Category Fees" },
      { num: 3, label: "Fee Preview" },
      { num: 4, label: "Inventory Data" },
      { num: 5, label: "Historic Data" },
      { num: 6, label: "Live Data" },
      { num: 7, label: "Inventory Forecast" },
      { num: 8, label: "Plotting Graph" },
    ]
    : [
      { num: 1, label: "Currency Conversion" },
      { num: 2, label: "Category Fees" },
      { num: 3, label: "Fee Preview" },
      { num: 4, label: "Inventory Data" },
      { num: 5, label: "Historic Data" },
      { num: 6, label: "Live Data" },
      { num: 8, label: "Plotting Graph" },
    ];


  return (
    <div className="w-full">
      <div className="rounded-xl bg-white">
        {/* Header */}
        <div className="items-center mb-2 p-4">
          <div className="text-center">
            <PageBreadcrumb pageTitle="Select Data Fetch Period" textSize="2xl" variant="table" />
            <p className="text-charcoal-500 text-sm mt-1">
              Link your Amazon Seller Central to sync your sales data
            </p>
            {/* <p className="text-xs text-slate-500 mt-2">
              Allowed window: {earliest.y}-{two(earliest.m1)} to {latest.y}-{two(latest.m1)}
            </p> */}
          </div>
        </div>

        <p className="text-charcoal-500 text-center font-bold text-md mt-1">
          Data Fetch Period
        </p>

        {/* Period Buttons */}
        <div className="mt-2 grid grid-cols-5 gap-2 sm:grid-cols-5 sm:gap-3 max-w-2xl mx-auto">
          {[1, 3, 6, 12, 24].map((m) => {
            const isActive = selectedPeriod === m;

            return (
              <div key={String(m)} className="relative w-full">
                {m === 24 && (
                  <div
                    className={[
                      "absolute -top-2 left-1/2 -translate-x-1/2",
                      "text-[10px] px-2 py-0.5 rounded-full z-10",
                      isActive
                        ? "bg-[#FEF2D4] text-charcoal-500"
                        : "bg-gray-200 text-charcoal-500",
                    ].join(" ")}
                  >
                    Recommended
                  </div>
                )}

                <button
                  type="button"
                  onClick={() => setSelectedPeriod(m)}
                  className={[
                    "w-full rounded-lg border p-2 sm:p-3 text-center transition",
                    isActive
                      ? "border-blue-700 bg-blue-700 text-yellow-200"
                      : "border-slate-200 bg-slate-50 hover:bg-white text-charcoal-500",
                  ].join(" ")}
                >
                  <div className="w-full text-base sm:text-lg font-semibold flex items-center justify-center tabular-nums">
                    <span className="inline-flex w-[1.6em] justify-center">{m}</span>
                  </div>

                  <div className="text-[10px] sm:text-xs uppercase tracking-wide mt-1">
                    {m === 1 ? "Month" : "Months"}
                  </div>
                </button>
              </div>
            );
          })}
        </div>

        {/* Note Section */}
        <div
          className="mt-4 max-w-2xl mx-auto rounded-lg bg-[#FDD36F4D] p-2 text-[12px] sm:p-3 sm:text-sm border border-[#FDD36F]"
          style={{ borderLeft: "6px solid #FDD36F" }}
        >
          Note:&nbsp; Your Amazon credentials are encrypted and stored securely. We only access data necessary for
          analytics.
        </div>

        {/* Progress UI with 6-Step Stepper */}


        {/* 1 month controls */}
        {selectedPeriod === 1 && (
          <div className="mt-6">
            <div className="flex flex-wrap items-center gap-3 justify-center">
              {/* Month */}
              <div className="flex items-center gap-2">
                <label className="text-xs text-slate-500">Month</label>
                <select
                  value={selMonth}
                  onChange={(e) => setSelMonth(e.target.value)}
                  className="rounded-lg border-2 border-slate-200 bg-white px-2 py-2 text-sm outline-none focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20"
                >
                  {Array.from({ length: 12 }, (_, i) => {
                    const m1 = i + 1;
                    const label = fullMonthNames[i].slice(0, 3);
                    const y = parseInt(selYear, 10);
                    const disabled = !Number.isFinite(y) || !isSelectableUTC(y, m1);
                    return (
                      <option key={m1} value={two(m1)} disabled={disabled}>
                        {label}
                      </option>
                    );
                  })}
                </select>
              </div>

              {/* Year */}
              <div className="flex items-center gap-2">
                <label className="text-xs text-slate-500">Year</label>
                <select
                  value={selYear}
                  onChange={(e) => {
                    const newYearStr = e.target.value;
                    setSelYear(newYearStr);

                    const newYear = parseInt(newYearStr, 10);
                    const currentMonthNum = parseInt(selMonth, 10);

                    // If current selected month becomes invalid for this year, clamp it.
                    if (Number.isFinite(newYear)) {
                      const clamped = clampToAllowedRangeUTC(newYear, currentMonthNum);
                      if (clamped.clamped) {
                        setSelYear(String(clamped.y));
                        setSelMonth(two(clamped.m1));
                      }
                    }
                  }}
                  className="rounded-lg border-2 border-slate-200 bg-white px-2 py-2 text-sm outline-none focus:border-[#5EA68E] focus:ring-4 focus:ring-[#5EA68E]/20"
                >
                  {allowedYears.map((y) => (
                    <option key={y} value={y}>
                      {y}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="w-full flex justify-center gap-3 mt-4">
              <Button onClick={onClose} variant="outline" size="sm">
                Cancel
              </Button>
              <Button
                onClick={handleFetchByMonth}
                variant="primary"
                size="sm"
                disabled={busy}
              >
                {busy ? "Fetching..." : "Continue"}
              </Button>
            </div>
          </div>
        )}



        {/* >1 month controls (includes Lifetime) */}
        {selectedPeriod && selectedPeriod > 1 && (
          <div className="w-full flex justify-center gap-3 mt-4">
            <Button onClick={onClose} variant="outline" size="sm">
              Cancel
            </Button>

            <Button onClick={handleFetchRange} variant="primary" size="sm" disabled={busy}>
              {busy ? "Fetching..." : `Continue`}
            </Button>
            {/* {selectedPeriod && selectedPeriod >= 6 && (
              <Button
                onClick={handleQuickForecastTest}
                variant="primary"
                size="sm"
                disabled={busy}
              >
                ⚡ Quick Forecast Test
              </Button>
            )} */}

          </div>
        )}

        {busy && remainingSeconds !== null && (
          <div className="mt-2 text-center text-xs text-slate-500">
            Estimated time remaining:{" "}
            <span className="font-medium tabular-nums">
              {Math.floor(remainingSeconds / 60)}:
              {String(remainingSeconds % 60).padStart(2, "0")}
            </span>
          </div>
        )}

        {busy && stepProgress.active && currentStep > 0 && (
          <div className="mt-6 max-w-4xl mx-auto">
            {/* Current Step Progress Bar */}
            <div className="rounded-lg border border-slate-200 bg-white p-4 mb-4">
              <div className="flex items-center justify-between gap-3 mb-2">
                <div className="text-sm font-semibold text-slate-700">{stepProgress.label}</div>
                <div className="text-xs text-slate-500 tabular-nums">
                  {stepProgress.percentage}%
                </div>
              </div>

              <div className="h-3 w-full overflow-hidden rounded-full bg-[#D9D9D9]">
                <div
                  className="h-full  transition-all duration-300 shin"
                  style={{
                    width: `${stepProgress.percentage}%`,
                    background: "linear-gradient(90deg, #5EA68E 0%, #37455F 100%)",

                  }}
                />
              </div>

              {stepProgress.detail && (
                <div className="mt-2 text-xs text-slate-500">{stepProgress.detail}</div>
              )}

              {/* Range progress for Historic Data step */}

            </div>

            {/* 6-Step Stepper */}

            <div className="flex items-center justify-between relative">
              {/* Connecting lines - spans from first to last step */}
              <div className="absolute top-6 left-[4%] right-[4%] h-1.5 bg-[#D9D9D9] -z-10">
                {completedSteps.size > 0 && (() => {
                  const maxCompleted = Math.max(...Array.from(completedSteps));
                  const visibleStepCount = steps.length;
                  const denominator = Math.max(visibleStepCount - 1, 1);

                  const progressPercent =
                    maxCompleted > 1 ? ((Math.min(maxCompleted, steps[steps.length - 1].num) - 1) / denominator) * 100 : 0;
                  return (
                    <div
                      className="h-full bg-[#5EA68E] transition-all duration-500"
                      style={{ width: `${progressPercent}%` }}
                    />
                  );
                })()}
              </div>

              {steps.map((step) => {
                const isCompleted = completedSteps.has(step.num);
                const isActive = currentStep === step.num;
                const isPast = currentStep > step.num;

                return (
                  <div key={step.num} className="flex flex-col items-center flex-1 relative z-10">
                    <StepBadge
                      completed={isCompleted}
                      label={step.num}
                    />
                    <div className="mt-2 text-center">
                      <div
                        className={`text-[10px] sm:text-xs font-medium leading-tight text-center w-20 h-8 flex flex-col justify-center ${isActive || isCompleted ? "text-[#5EA68E]" : "text-[#414042]"
                          }`}
                      >
                        {step.label.split(" ").map((word, i) => (
                          <span key={i} className="block">
                            {word}
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>

      {/* Messages */}
      <div className="mt-4 space-y-4">
        {message && (
          <div className="flex items-center gap-2 rounded-md bg-emerald-50 border border-emerald-200 p-3 text-emerald-800 text-sm">
            <CheckCircle2 />
            <span>{message}</span>
          </div>
        )}
        {error && (
          <div className="flex items-center gap-2 rounded-md bg-red-50 border border-red-200 p-3 text-red-700 text-sm">
            <AlertCircle />
            <span style={{ whiteSpace: "pre-wrap" }}>{error}</span>
          </div>
        )}
      </div>
    </div>
  );
};

export default AmazonFinancialDashboard;
