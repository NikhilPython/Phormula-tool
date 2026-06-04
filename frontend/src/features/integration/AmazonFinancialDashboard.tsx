"use client";

import React, { useMemo, useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import {
  FaCheckCircle as CheckCircle2,
  FaTimesCircle as XCircle,
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
const MARKETPLACE_TO_COUNTRY: Record<string, string> = {
  A1F83G8C2ARO7P: "uk",
  ATVPDKIKX0DER: "us",
  A2EUQ1WTGCTBG2: "canada",
};

const COUNTRY_TO_REGION: Record<string, string> = {
  uk: "eu-west-1",
  us: "us-east-1",
  canada: "ap-southeast-1",
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

function formatFetchMonth(year: number, monthNum: number) {
  const shortMonth = fullMonthNames[monthNum - 1].slice(0, 3);
  const shortYear = String(year).slice(-2);

  return `${shortMonth}'${shortYear}`;
}

function getHistoricFetchMessage(params: {
  year: number;
  monthNum: number;
  currentMonthIndex?: number;
  totalMonths?: number;
}) {
  const formattedMonth = formatFetchMonth(params.year, params.monthNum);

  if (params.totalMonths && params.totalMonths > 1 && params.currentMonthIndex) {
    return `Fetching ${formattedMonth} (Month ${params.currentMonthIndex}/${params.totalMonths})`;
  }

  return `Fetching ${formattedMonth}`;
}

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

  // return apiJson(`/amazon_api/inventory/ledger-summary?${qs.toString()}`, {
  //   method: "GET",
  // });
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

/** ✅ MTD EMAIL (run once per country in this browser) */
// function lsKeyMtdEmail(country: string) {
//   return `mtdEmailSent:${country}`;
// }

async function sendMtdReportEmail(country: string) {
  return apiJson(`/send-report-email`, {
    method: "POST",
    body: JSON.stringify({ country }),
  });
}

async function ensureMtdEmailSentOnce(country: string) {
  // const key = lsKeyMtdEmail(country);

  // if (wasDone(key)) return;

  await sendMtdReportEmail(country);

  // markDone(key);
}

/** ✅ NEW: hit /amazon_api/inventory/aged with NO params, once per country */
async function syncInventoryAgedOnce(country: string) {
  const key = lsKeyInventoryAged(country);
  if (wasDone(key)) return;

  await apiJson(`/amazon_api/inventory/aged`, { method: "GET" });

  markDone(key);
}


async function fetchProductInformation(params: {
  marketplace_id: string;
  store_in_db?: boolean;
  full_details?: boolean;
  limit?: number;
}) {
  const qs = new URLSearchParams();

  qs.set("marketplace_id", params.marketplace_id);
  qs.set("store_in_db", String(params.store_in_db ?? true));
  qs.set("full_details", String(params.full_details ?? true));

  if (params.limit) {
    qs.set("limit", String(params.limit));
  }

  return apiJson(`/amazon_api/skus?${qs.toString()}`, {
    method: "GET",
  });
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

  // US needs both RELEASED + DEFERRED; UK needs only RELEASED
  transaction_status:
    params.country.toLowerCase() === "us" ? "RELEASED,DEFERRED" : "RELEASED",
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

async function fetchLiveMtdBi(params: {
  country: string;
  month: string;
  year: number;
}) {
  const token = getAuthToken();

  const qs = new URLSearchParams({
    countryName: params.country,
    ranged: "MTD",
    month: params.month.toLowerCase(),
    year: String(params.year),
    generate_ai_insights: "false",
  });

  const url = `${API_BASE}/live_mtd_bi?${qs.toString()}`;

  let attempts = 0;
  const maxAttempts = 10;

  while (attempts < maxAttempts) {
    const res = await fetch(url, {
      headers: token ? { Authorization: `Bearer ${token}` } : undefined,
    });

    // ⏳ Processing state
    if (res.status === 202) {
      await new Promise((r) => setTimeout(r, 3000));
      attempts++;
      continue;
    }

    if (!res.ok) {
      const msg = await readErrorMessage(res);
      throw new Error(`Live BI failed: ${msg}`);
    }

    return await res.json();
  }

  throw new Error("Live BI processing timeout");
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
    8,
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
    8,
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
    8,
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
      8,
      "Forecast",
      75,
      `Generating purchase order for ${currentGoingMonthName} ${currentGoingYearStr}...`
    );

    await runPurchaseOrder({
      country: params.country,
      year: currentGoingYearStr,
      month: currentGoingMonthLower,
    });

    params.setStep(8, "Forecast", 90, "Fetching purchase order file...");

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

  params.setStep(
    8,
    "Forecast",
    100,
    "Forecast, dispatch, and purchase order ready"
  );

  return {
    redirectMonthSlug: currentGoingMonthLower,
    redirectYear: currentGoingYearStr,
  };
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

// function buildLedgerRange(period: number | "lifetime") {
//   const now = new Date();
//   const currentYear = now.getUTCFullYear();
//   const currentMonth1 = now.getUTCMonth() + 1;

//   const totalMonths = period === "lifetime" ? 24 : Number(period);

//   const start = addMonthsUTC(currentYear, currentMonth1, -(totalMonths - 1));
//   const startDate = startOfMonthUTC(start.y, start.m1);
//   const endDate = endOfMonthUTC(currentYear, currentMonth1);

//   return {
//     start_date: formatYMDUTC(startDate),
//     end_date: formatYMDUTC(endDate),
//   };
// }

function buildLedgerRange(period: number | "lifetime") {
  // Anchor to the latest completed month, not the current ongoing month
  const latest = getLatestAllowedMonthUTC(); // already returns previous month

  // ledger-summary supports only up to 18 past months
  // so lifetime/24 should fetch 18 completed months max
  const totalMonths = period === "lifetime" ? 18 : Number(period);

  // Start = first day of the month (totalMonths - 1) before latest completed month
  const start = addMonthsUTC(latest.y, latest.m1, -(totalMonths - 1));

  const startDate = startOfMonthUTC(start.y, start.m1);
  const endDate = endOfMonthUTC(latest.y, latest.m1);

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
  marketplaceId?: string;
  onClose?: () => void;
};

const AmazonFinancialDashboard: React.FC<Props> = ({
  region,
  country,
  marketplaceId,
  onClose,
}) => {
  const router = useRouter();

  const { data: user } = useGetUserDataQuery();

  const countryMarketplaceMap = useMemo(() => {
    if (!user?.country || !user?.marketplace_id) return {};
    return {
      [user.country]: user.marketplace_id,
    };
  }, [user]);

  let countryUsed = (country || "").toLowerCase().trim();

  if (countryUsed === "united kingdom" || countryUsed === "gb") countryUsed = "uk";
  if (countryUsed === "united states" || countryUsed === "usa") countryUsed = "us";
  if (countryUsed === "ca") countryUsed = "canada";

  const storedCountry =
    typeof window !== "undefined"
      ? (localStorage.getItem("amazonSelectedCountry") || "").toLowerCase().trim()
      : "";

  const storedRegion =
    typeof window !== "undefined"
      ? localStorage.getItem("amazonMarketplaceRegion") || ""
      : "";

  const storedMarketplaceId =
    typeof window !== "undefined"
      ? localStorage.getItem("amazonMarketplaceId") || ""
      : "";

  // first resolve marketplace
  let marketplaceIdUsed =
    marketplaceId ||
    storedMarketplaceId ||
    COUNTRY_TO_MARKETPLACE[countryUsed] ||
    "";

  // then resolve country
  if (!countryUsed) {
    countryUsed =
      storedCountry ||
      MARKETPLACE_TO_COUNTRY[marketplaceIdUsed] ||
      "us";
  }

  // re-resolve marketplace if country was filled from fallback
  if (!marketplaceIdUsed) {
    marketplaceIdUsed = COUNTRY_TO_MARKETPLACE[countryUsed] || "";
  }

  let regionUsed =
    region || storedRegion || COUNTRY_TO_REGION[countryUsed];

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

  // const TOTAL_FETCH_SECONDS = 15 * 60;

  const [remainingSeconds, setRemainingSeconds] = useState<number | null>(null);
  const [selectedPeriod, setSelectedPeriod] = useState<number | null>(24);

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

  const getEstimatedFetchSeconds = (period: number | null) => {
    switch (period) {
      case 1:
        return 12 * 60;
      case 3:
        return 15 * 60;
      case 6:
        return 20 * 60;
      case 12:
        return 25 * 60;
      case 24:
        return 30 * 60;
      default:
        return 15 * 60;
    }
  };

  // useEffect(() => {
  //   if (!busy) {
  //     setRemainingSeconds(null);
  //     return;
  //   }

  //   setRemainingSeconds(TOTAL_FETCH_SECONDS);

  //   const interval = setInterval(() => {
  //     setRemainingSeconds((prev) => {
  //       if (!prev || prev <= 1) {
  //         clearInterval(interval);
  //         return 0;
  //       }
  //       return prev - 1;
  //     });
  //   }, 1000);

  //   return () => clearInterval(interval);
  // }, [busy]);

  useEffect(() => {
    if (!busy) {
      setRemainingSeconds(null);
      return;
    }

    const estimatedSeconds = getEstimatedFetchSeconds(selectedPeriod);
    setRemainingSeconds(estimatedSeconds);

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
  }, [busy, selectedPeriod]);

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
      await new Promise((resolve) => setTimeout(resolve, 1000));
      markStepComplete(3);

      // Step 4: Product Information
      setStep(4, "Product Information", 0, "Fetching Amazon product information...");

      await fetchProductInformation({
        marketplace_id: marketplaceIdUsed,
        store_in_db: true,
        full_details: true,
      });

      setStep(4, "Product Information", 100, "Product information synced successfully");
      markStepComplete(4);

      // Step 5: Inventory
      setStep(5, "Inventory", 0, "Syncing inventory data...");

      await syncInventoryAgedOnce(countryUsed);

      await fetchInventoryLedgerSummary({
        marketplace_id: marketplaceIdUsed,
        month: `${y}-${two(mNum)}`,
        store_in_db: true,
        keep_first_last: false,
      });

      markStepComplete(5);

      // Step 6: Historic Data (per month, ~20 seconds)
      setStep(
        6,
        "Historic Data",
        0,
        getHistoricFetchMessage({
          year: y,
          monthNum: mNum,
        })
      );
      await fetchMonthlyTransactionsExcel({
        year: y,
        month: mNum,
        marketplace_id: marketplaceIdUsed,
        country: countryUsed,
        run_upload_pipeline: true,
        store_in_db: true,
      });
      markStepComplete(6);

      // Step 7: Live Data (LIVE MTD BI)
      setStep(7, "Live Data", 0, "Fetching live MTD BI data...");

      const monthName = fullMonthNames[mNum - 1]; // convert 3 → March

      await fetchLiveMtdBi({
        country: countryUsed,
        month: monthName,
        year: y,
      });

      setStep(7, "Live Data", 100, "Live BI data ready");
      markStepComplete(7);


      // Step 8: Inventory Forecast + Purchase Order
      const latestMonthSlug = fullMonthNames[mNum - 1].toLowerCase();

      let redirectMonthSlug = latestMonthSlug;
      let redirectYear = String(y);

      if (selectedPeriod && selectedPeriod >= 6) {
        const forecastResult = await runForecastAndPoSequence({
          country: countryUsed,
          year: y,
          month: mNum,
          setStep,
        });

        redirectMonthSlug = forecastResult.redirectMonthSlug;
        redirectYear = forecastResult.redirectYear;

        markStepComplete(8);
      }

      // Plotting Graph
      const plottingGraphStep = selectedPeriod && selectedPeriod >= 6 ? 9 : 8;

      setStep(plottingGraphStep, "Plotting Graph", 0, "Preparing charts...");
      await new Promise((resolve) => setTimeout(resolve, 1000));

      setStep(plottingGraphStep, "Plotting Graph", 100, "Charts ready");
      await new Promise((resolve) => setTimeout(resolve, 500));

      markStepComplete(plottingGraphStep);

      await new Promise((r) => setTimeout(r, 600));

      const monthSlug = fullMonthNames[mNum - 1].toLowerCase();
      updateLatestFetchedPeriod(monthSlug, String(y));

      setMessage(
        selectedPeriod && selectedPeriod >= 6
          ? `Fetched ${countryUsed}: ${y}-${two(mNum)}. Inventory forecast and purchase order generated successfully.`
          : `Fetched ${countryUsed}: ${y}-${two(mNum)}. Dashboard graphs are ready.`
      );

      localStorage.setItem("selectedPlatform", `amazon-${countryUsed}`);
      window.dispatchEvent(new Event("storage"));

      // ✅ Send MTD email for first-time users
      try {
        await sendMtdReportEmail(countryUsed);
      } catch (e) {
        console.error("MTD email send failed", e);
      }

      if (onClose) onClose();
      router.push(
        `/pnl-dashboard/MTD/${countryUsed}/${redirectMonthSlug}/${redirectYear}?amazonFetch=success&promptAmazonAds=1`
      );
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

      // Step 4: Product Information
      setStep(4, "Product Information", 0, "Fetching Amazon product information...");

      await fetchProductInformation({
        marketplace_id: marketplaceIdUsed,
        store_in_db: true,
        full_details: true,
      });

      setStep(4, "Product Information", 100, "Product information synced successfully");
      markStepComplete(4);

      // Step 5: Inventory
      setStep(5, "Inventory", 0, "Syncing inventory data...");
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

        markStepComplete(5);
      } catch (e) {
        console.error("inventory sync failed", e);
        throw e;
      }

      // Step 6: Historic Data (per month, ~20 seconds each)
      setStep(6, "Historic Data", 0, `Fetching data for ${months.length} months...`);
      let ok = 0;
      let fail = 0;

      for (let i = 0; i < months.length; i++) {
        const { y, mNum, mIdx } = months[i];
        const monthProgress = Math.round(((i + 1) / months.length) * 100);

        setStep(
          6,
          "Historic Data",
          monthProgress,
          getHistoricFetchMessage({
            year: y,
            monthNum: mNum,
            currentMonthIndex: i + 1,
            totalMonths: months.length,
          })
        );
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
      markStepComplete(6);

      // Step 7: Live Data
      setStep(7, "Live Data", 100, "Finalizing data sync...");
      await new Promise((resolve) => setTimeout(resolve, 500));
      markStepComplete(7);

      // Step 8: Inventory Forecast + Purchase Order

      const last = months[months.length - 1];
      const latestMonthSlug = fullMonthNames[last.mIdx].toLowerCase();

      let redirectMonthSlug = latestMonthSlug;
      let redirectYear = String(last.y);

      if (selectedPeriod && selectedPeriod >= 6) {
        const forecastResult = await runForecastAndPoSequence({
          country: countryUsed,
          year: last.y,
          month: last.mNum,
          setStep,
        });

        redirectMonthSlug = forecastResult.redirectMonthSlug;
        redirectYear = forecastResult.redirectYear;

        markStepComplete(8);
      }

      // Plotting Graph
      const plottingGraphStep = selectedPeriod && selectedPeriod >= 6 ? 9 : 8;

      setStep(plottingGraphStep, "Plotting Graph", 0, "Preparing charts...");
      await new Promise((resolve) => setTimeout(resolve, 1000));

      setStep(plottingGraphStep, "Plotting Graph", 100, "Charts ready");
      await new Promise((resolve) => setTimeout(resolve, 500));

      markStepComplete(plottingGraphStep);

      await new Promise((r) => setTimeout(r, 600));

      setMessage(
        selectedPeriod && selectedPeriod >= 6
          ? `Fetch complete for ${countryUsed}: ${isLifetime ? "lifetime (allowed window)" : `${selectedPeriod} months`
          }, ok ${ok}, failed ${fail}. Inventory forecast and purchase order generated successfully for ${last.y}-${two(last.mNum)}.`
          : `Fetch complete for ${countryUsed}: ${isLifetime ? "lifetime (allowed window)" : `${selectedPeriod} months`
          }, ok ${ok}, failed ${fail}. Dashboard graphs are ready.`
      );

      localStorage.setItem("selectedPlatform", `amazon-${countryUsed}`);
      window.dispatchEvent(new Event("storage"));

      // ✅ Send MTD email for first-time users
      try {
        await ensureMtdEmailSentOnce(countryUsed);
      } catch (e) {
        console.error("MTD email send failed", e);
      }

      if (onClose) onClose();
      router.push(
        `/pnl-dashboard/MTD/${countryUsed}/${redirectMonthSlug}/${redirectYear}?amazonFetch=success&promptAmazonAds=1`
      );
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

      for (let i = 0; i < months.length; i++) {
        const { y, mNum } = months[i];

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
      { num: 4, label: "Product Information" },
      { num: 5, label: "Inventory Data" },
      { num: 6, label: "Historic Data" },
      { num: 7, label: "Live Data" },
      { num: 8, label: "Inventory Forecast" },
      { num: 9, label: "Plotting Graph" },
    ]
    : [
      { num: 1, label: "Currency Conversion" },
      { num: 2, label: "Category Fees" },
      { num: 3, label: "Fee Preview" },
      { num: 4, label: "Product Information" },
      { num: 5, label: "Inventory Data" },
      { num: 6, label: "Historic Data" },
      { num: 7, label: "Live Data" },
      { num: 8, label: "Plotting Graph" },
    ];

  const periodFeatureMap: Record<number, {
    title: string;
    comparisons: { label: string; available: boolean }[];
    analytics: { label: string; available: boolean }[];
  }> = {
    1: {
      title: "Features",
      comparisons: [
        { label: "Monthly comparison", available: false },
        { label: "Quarterly comparison", available: false },
        { label: "Yearly comparison", available: false },
      ],
      analytics: [
        { label: "Current month analytics only", available: true },
        { label: "Inventory forecast", available: false },
        { label: "Purchase planning insights", available: false },
      ],
    },
    3: {
      title: "Features",
      comparisons: [
        { label: "2 months comparison", available: true },
        { label: "Quarterly comparison", available: false },
        { label: "Yearly comparison", available: false },
      ],
      analytics: [
        { label: "Short trend visibility", available: true },
        { label: "Inventory forecast", available: false },
        { label: "Purchase planning insights", available: false },
      ],
    },
    6: {
      title: "Features",
      comparisons: [
        { label: "5 months comparison", available: true },
        { label: "1 Quarterly comparison", available: true },
        { label: "Yearly comparison", available: false },
      ],
      analytics: [
        { label: "Mid-term trend visibility", available: true },
        { label: "Inventory forecast", available: true },
        { label: "Purchase planning insights", available: true },
      ],
    },
    12: {
      title: "Features",
      comparisons: [
        { label: "11 Months comparison", available: true },
        { label: "3 Quarterly comparisons", available: true },
        { label: "Yearly comparison", available: false },
      ],
      analytics: [
        { label: "Strong seasonal trend visibility", available: true },
        { label: "Inventory forecast", available: true },
        { label: "Purchase planning insights", available: true },
      ],
    },
    24: {
      title: "Features",
      comparisons: [
        { label: "23 months comparison", available: true },
        { label: "7 quarterly comparisons", available: true },
        { label: "Yearly comparison", available: true },
      ],
      analytics: [
        { label: "Full historical trend visibility", available: true },
        { label: "Inventory forecast", available: true },
        { label: "Purchase planning insights", available: true },
      ],
    },
  };

  const selectedFeatures = useMemo(() => {
    if (selectedPeriod === null) return null;
    return periodFeatureMap[selectedPeriod] ?? null;
  }, [selectedPeriod]);

  const visibleSteps = steps;

  const splitStepLabel = (label: string) => {
    const words = label.trim().split(/\s+/);

    if (words.length <= 1) {
      return [label, "\u00A0"];
    }

    return [words[0], words.slice(1).join(" ")];
  };

  return (
    <div className="w-full">
      <div className="rounded-xl bg-white max-h-[85vh] overflow-y-auto">
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
        {/* <div
          className="mt-4 max-w-2xl mx-auto rounded-lg bg-[#FDD36F4D] p-2 text-[12px] sm:p-3 sm:text-sm border border-[#FDD36F]"
          style={{ borderLeft: "6px solid #FDD36F" }}
        >
          Note:&nbsp; Your Amazon credentials are encrypted and stored securely. We only access data necessary for
          analytics.
        </div> */}

        {selectedFeatures && (
          <div className="mt-4 max-w-2xl mx-auto rounded-xl border border-[#D9E7E1] bg-emerald-50/50 p-4 shadow-sm">
            {/* <h3 className="text-base font-semibold text-[#37455F] mb-3 text-center">
              {selectedFeatures.title}
            </h3> */}

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div className="rounded-lg bg-white border border-slate-200 p-3">
                <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2">
                  Comparisons
                </p>

                <div className="space-y-2">
                  {selectedFeatures.comparisons.map((item) => (
                    <div key={item.label} className="flex items-start gap-2">
                      <span className={`mt-0.5 ${item.available ? "text-[#5EA68E]" : "text-[#E16D6D]"}`}>
                        {item.available ? (
                          <CheckCircle2 className="h-4 w-4" />
                        ) : (
                          <XCircle className="h-4 w-4" />
                        )}
                      </span>
                      <p className={`text-sm leading-5 ${item.available ? "text-slate-700" : "text-slate-500"}`}>
                        {item.label}
                      </p>
                    </div>
                  ))}
                </div>
              </div>

              <div className="rounded-lg bg-white border border-slate-200 p-3">
                <p className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-2">
                  Insights
                </p>

                <div className="space-y-2">
                  {selectedFeatures.analytics.map((item) => (
                    <div key={item.label} className="flex items-start gap-2">
                      <span className={`mt-0.5 ${item.available ? "text-[#5EA68E]" : "text-[#E16D6D]"}`}>
                        {item.available ? (
                          <CheckCircle2 className="h-4 w-4" />
                        ) : (
                          <XCircle className="h-4 w-4" />
                        )}
                      </span>
                      <p className={`text-sm leading-5 ${item.available ? "text-slate-700" : "text-slate-500"}`}>
                        {item.label}
                      </p>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}


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

        {busy && stepProgress.active && currentStep > 0 && (
          <div className="absolute inset-0 z-30 flex items-center justify-center bg-white/80 backdrop-blur-[1px] px-4">
            <div className="w-full max-w-4xl rounded-2xl border border-slate-200 bg-white p-5 md:p-6 shadow-md">
              {/* Header */}
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg bg-[#E8F5F0] flex items-center justify-center flex-shrink-0">
                    <svg
                      width="15"
                      height="15"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="#5EA68E"
                      strokeWidth="2.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
                    </svg>
                  </div>

                  <div>
                    <p className="text-lg font-semibold text-[#37455F] leading-tight">
                      Syncing dashboard data
                    </p>
                  </div>
                </div>

                <span className="text-sm font-bold text-[#5EA68E] tabular-nums">
                  {stepProgress.percentage}%
                </span>
              </div>

              {/* Progress bar */}
              <div className="h-[7px] w-full bg-slate-100 rounded-full overflow-hidden mb-6">
                <div
                  className="h-full rounded-full transition-all duration-500 ease-in-out"
                  style={{
                    width: `${stepProgress.percentage}%`,
                    background: "linear-gradient(90deg, #5EA68E 0%, #37455F 100%)",
                  }}
                />
              </div>

              {/* Steps row */}
              <div className="relative flex items-start justify-between">
                <div
                  className="absolute top-4 z-0 h-px bg-slate-200"
                  style={{ left: "calc(9% )", right: "calc(5%)" }}
                >
                  {completedSteps.size > 0 &&
                    (() => {
                      const maxCompleted = Math.max(...Array.from(completedSteps));
                      const denominator = Math.max(steps.length - 1, 1);
                      const pct =
                        maxCompleted > 1
                          ? ((Math.min(maxCompleted, steps[steps.length - 1].num) - 1) /
                            denominator) *
                          100
                          : 0;

                      return (
                        <div
                          className="h-full bg-[#5EA68E] transition-all duration-500"
                          style={{ width: `${pct}%` }}
                        />
                      );
                    })()}
                </div>

                {visibleSteps.map((step) => {
                  const isCompleted = completedSteps.has(step.num);
                  const isActive = currentStep === step.num;

                  return (
                    <div
                      key={step.num}
                      className="flex flex-col items-center flex-1 min-w-0 relative z-10"
                    >
                      <div
                        className={[
                          "w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold border-2 transition-all duration-300",
                          isCompleted
                            ? "border-[#5EA68E] bg-[#5EA68E] text-white"
                            : isActive
                              ? "border-[#5EA68E] bg-[#E8F5F0] text-[#37455F]"
                              : "border-slate-200 bg-white text-slate-400",
                        ].join(" ")}
                      >
                        {isCompleted ? (
                          <svg
                            width="12"
                            height="12"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="white"
                            strokeWidth="3.5"
                            strokeLinecap="round"
                            strokeLinejoin="round"
                          >
                            <polyline points="20 6 9 17 4 12" />
                          </svg>
                        ) : isActive ? (
                          <span
                            className="w-3 h-3 rounded-full border-2 border-[#b8ddd4] border-t-[#5EA68E] animate-spin"
                            style={{ display: "inline-block" }}
                          />
                        ) : (
                          <span>{step.num}</span>
                        )}
                      </div>

                      {/* <p
                        className={[
                          "text-center text-[10px] sm:text-xs font-medium leading-tight",
                          isCompleted || isActive ? "text-[#37455F]" : "text-slate-400",
                        ].join(" ")}
                      >
                        {step.label}
                      </p>

                      <span
                        className={[
                          "text-[9px] sm:text-[10px] px-2 py-0.5 rounded-full font-medium",
                          isCompleted
                            ? "bg-[#E8F5F0] text-[#5EA68E]"
                            : isActive
                              ? "bg-[#E8F5F0] text-[#5EA68E] animate-pulse"
                              : "bg-slate-100 text-slate-400",
                        ].join(" ")}
                      >
                        {isCompleted ? "✓ Done" : isActive ? "In progress" : "Pending"}
                      </span> */}

                      <div
                        className={[
                          "h-8 flex flex-col items-center justify-start text-center",
                          "text-[10px] sm:text-xs font-medium leading-[14px]",
                          "whitespace-normal break-normal",
                          isCompleted || isActive ? "text-[#37455F]" : "text-slate-400",
                        ].join(" ")}
                      >
                        {(() => {
                          const [line1, line2] = splitStepLabel(step.label);

                          return (
                            <>
                              <span className="block whitespace-nowrap">{line1}</span>
                              <span className="block whitespace-nowrap">{line2}</span>
                            </>
                          );
                        })()}
                      </div>

                      <span
                        className={[
                          "mt-1 h-5 inline-flex items-center justify-center",
                          "text-[9px] sm:text-[10px] px-2 rounded-full font-medium whitespace-nowrap",
                          isCompleted
                            ? "bg-[#E8F5F0] text-[#5EA68E]"
                            : isActive
                              ? "bg-[#E8F5F0] text-[#5EA68E] animate-pulse"
                              : "bg-slate-100 text-slate-400",
                        ].join(" ")}
                      >
                        {isCompleted ? "✓ Done" : isActive ? "In progress" : "Pending"}
                      </span>
                    </div>
                  );
                })}
              </div>

              <div className="mt-5 pt-4 border-t border-slate-100">
                <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-3">
                  {/* Left */}
                  <p className="text-[13px] text-slate-400 truncate text-left">
                    {stepProgress.detail || "Initialising dashboard..."}
                  </p>

                  {/* Center */}
                  {remainingSeconds !== null && (
                    <div className="inline-flex items-center justify-center rounded-full bg-slate-100 px-4 py-1.5 text-[13px] text-slate-500 leading-none whitespace-nowrap">
                      <span className="mr-2 font-medium text-slate-400">Estimated Time:</span>
                      <span className="font-medium tabular-nums text-slate-600">
                        {Math.floor(remainingSeconds / 60)}:
                        {String(remainingSeconds % 60).padStart(2, "0")}
                      </span>
                    </div>
                  )}

                  {/* Right */}
                  <span className="text-[13px] text-slate-400 text-right whitespace-nowrap">
                    Step {Math.max(
                      visibleSteps.findIndex((s) => s.num === currentStep) + 1,
                      1
                    )} of {visibleSteps.length}
                  </span>
                </div>
              </div>

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
