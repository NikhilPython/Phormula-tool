"use client";

import React, { useCallback, useEffect, useState } from "react";
import { useSelector } from "react-redux";
import { FiDownload, FiRefreshCw, FiTrendingUp } from "react-icons/fi";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

type CurrencyRate = {
  user_currency?: string;
  country?: string;
  selected_currency?: string;
  conversion_rate?: number | string | null;
  month?: string;
  year?: number | string;
};

export default function CurrencyRatesTab() {
  const reduxToken = useSelector((state: any) => state.auth?.token);
  const token =
    reduxToken ||
    (typeof window !== "undefined" ? localStorage.getItem("jwtToken") || "" : "");

  const [rates, setRates] = useState<CurrencyRate[]>([]);
  const [month, setMonth] = useState("");
  const [year, setYear] = useState<number | string>("");
  const [loading, setLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState("");

  const fetchRates = useCallback(async () => {
    if (!token) {
      setError("Authentication token is missing. Please sign in again.");
      return;
    }

    try {
      setLoading(true);
      setError("");
      const response = await fetch(`${API_BASE}/currency-rates`, {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` },
        cache: "no-store",
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(result?.error || result?.message || "Failed to load currency rates.");
      }

      const rows = Array.isArray(result)
        ? result
        : Array.isArray(result?.currency_rates)
          ? result.currency_rates
          : [];
      // Hide rows where Home Currency and Selected Currency are identical.
      const visibleRows = rows.filter((rate: CurrencyRate) => {
        const homeCurrency = String(rate.user_currency || "").trim().toUpperCase();
        const selectedCurrency = String(rate.selected_currency || "").trim().toUpperCase();
        return !homeCurrency || !selectedCurrency || homeCurrency !== selectedCurrency;
      });
      setRates(visibleRows);
      setMonth(
        result?.current_month ||
          rows[0]?.month ||
          new Date().toLocaleString("en-US", { month: "long" })
      );
      setYear(result?.current_year || rows[0]?.year || new Date().getFullYear());
    } catch (err: any) {
      setRates([]);
      setError(err?.message || "Failed to load currency rates.");
    } finally {
      setLoading(false);
    }
  }, [token]);

  useEffect(() => {
    void fetchRates();
  }, [fetchRates]);

  const downloadHistory = async () => {
    if (!token) {
      setError("Authentication token is missing. Please sign in again.");
      return;
    }

    try {
      setDownloading(true);
      setError("");
      const response = await fetch(`${API_BASE}/currency-rates?export=excel`, {
        method: "GET",
        headers: { Authorization: `Bearer ${token}` },
        cache: "no-store",
      });
      if (!response.ok) {
        const result = await response.json().catch(() => ({}));
        throw new Error(result?.error || result?.message || "Unable to download currency history.");
      }

      const contentType = response.headers.get("content-type") || "";
      if (!contentType.includes("spreadsheet") && !contentType.includes("octet-stream")) {
        const message = await response.text();
        let apiMessage = message;
        try {
          const parsed = JSON.parse(message);
          apiMessage = parsed?.message || parsed?.error || message;
        } catch {
          // Keep the response text as the error message.
        }
        throw new Error(apiMessage || "The server did not return a valid Excel file.");
      }

      const blob = await response.blob();
      // XLSX files are ZIP containers and must begin with the PK signature.
      const signature = new Uint8Array(await blob.slice(0, 2).arrayBuffer());
      if (signature[0] !== 0x50 || signature[1] !== 0x4b) {
        throw new Error("Downloaded file is not a valid .xlsx workbook. Please check the currency-rates API response.");
      }
      const objectUrl = window.URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = objectUrl;
      anchor.download = "Currency_Rates_History.xlsx";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.setTimeout(() => window.URL.revokeObjectURL(objectUrl), 1000);
    } catch (err: any) {
      setError(err?.message || "Unable to download currency history.");
    } finally {
      setDownloading(false);
    }
  };

  return (
    <section className="w-full overflow-hidden rounded-2xl border border-[#E1E7E3] bg-white shadow-sm">
      <div className="flex flex-col gap-4 border-b border-[#E8ECEA] px-5 py-5 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-[#EAF5EF] text-[#438B70]">
              <FiTrendingUp size={17} />
            </span>
            <h2 className="text-base font-bold text-[#37455F]">Currency Rates</h2>
            <span className="rounded-full bg-[#EAF7F1] px-2.5 py-1 text-[11px] font-semibold text-[#438B70]">
              {month ? `${month.charAt(0).toUpperCase()}${month.slice(1)} ${year}` : "Current month"}
            </span>
          </div>
          <p className="mt-2 text-xs text-[#78847E]">
            View the current month&apos;s currency conversion rates for your workspace.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => void fetchRates()}
            disabled={loading}
            className="inline-flex h-9 items-center gap-2 rounded-lg border border-[#DDE5E1] bg-white px-3 text-xs font-semibold text-[#37455F] transition hover:bg-[#F5F8F6] disabled:cursor-not-allowed disabled:opacity-60"
          >
            <FiRefreshCw className={loading ? "animate-spin" : ""} size={13} />
            {loading ? "Refreshing..." : "Refresh"}
          </button>
          <button
            type="button"
            onClick={downloadHistory}
            disabled={downloading}
            className="inline-flex h-9 items-center gap-2 rounded-lg bg-[#5EA68E] px-3 text-xs font-semibold text-white shadow-[0px_4px_4px_0px_#00000040] transition hover:bg-[#4A927A] disabled:cursor-not-allowed disabled:opacity-60"
          >
            <FiDownload size={14} />
            {downloading ? "Downloading..." : "Download full history"}
          </button>
        </div>
      </div>

      <div className="p-5">
        <div className="mb-4 rounded-xl border border-[#DDEBE4] bg-[#F8FCFA] px-4 py-3">
          <p className="text-xs text-[#557267]">
            This tab displays only the current month. Download the Excel file to view the complete historical currency rates.
          </p>
        </div>

        {error ? (
          <div className="flex flex-col gap-3 rounded-xl border border-[#EAC9C9] bg-[#FFF6F6] px-4 py-4 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-xs text-[#A94E4E]">{error}</p>
            <button type="button" onClick={() => void fetchRates()} className="text-xs font-semibold text-[#0E8558]">
              Try again
            </button>
          </div>
        ) : loading ? (
          <div className="grid min-h-[180px] place-items-center text-xs text-[#78847E]">
            Loading currency rates...
          </div>
        ) : rates.length === 0 ? (
          <div className="grid min-h-[160px] place-items-center rounded-xl border border-dashed border-[#DDE5E1] bg-[#FBFDFC] px-4 text-center">
            <p className="text-xs font-medium text-[#78847E]">No currency rates found for the current month.</p>
          </div>
        ) : (
          <div className="w-full overflow-x-auto rounded-xl border border-[#E5EBE7]">
            <table className="w-full min-w-[520px] border-collapse text-left">
              <thead>
                <tr className="bg-[#5EA68E] text-white">
                  <th className="border-r border-[#DCE5E0] px-4 py-3 text-[11px] font-semibold">Home Currency</th>
                  <th className="border-r border-[#DCE5E0] px-4 py-3 text-[11px] font-semibold">Selected Currency</th>
                  <th className="px-4 py-3 text-right text-[11px] font-semibold">Conversion Rate</th>
                </tr>
              </thead>
              <tbody>
                {rates.map((rate, index) => (
                  <tr
                    key={`${rate.user_currency || "currency"}-${rate.selected_currency || index}-${index}`}
                    className="bg-white"
                  >
                    <td className="border-r border-t border-[#DCE5E0] bg-white px-4 py-3 text-xs font-semibold uppercase text-[#33413A]">{rate.user_currency || "-"}</td>
                    <td className="border-r border-t border-[#DCE5E0] bg-white px-4 py-3 text-xs font-semibold uppercase text-[#33413A]">{rate.selected_currency || "-"}</td>
                    <td className="border-t border-[#DCE5E0] bg-white px-4 py-3 text-right text-xs font-semibold tabular-nums text-[#33413A]">
                      {rate.conversion_rate === null || rate.conversion_rate === undefined
                        ? "-"
                        : Number(rate.conversion_rate).toLocaleString("en-US", { maximumFractionDigits: 8 })}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}
