"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useSelector } from "react-redux";
import { FiDownload, FiRefreshCw, FiTrendingUp } from "react-icons/fi";
import PageBreadcrumb from "../common/PageBreadCrumb";
import { exportCurrencyRatesExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from '@/lib/api/profileApi';

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

  const { data: userData } = useGetUserDataQuery();
  const companyName = (userData as any)?.companyName || (userData as any)?.company_name || (userData as any)?.company || "";
  const brandName = (userData as any)?.brandName || (userData as any)?.brand_name || (userData as any)?.brand || "";

  const [rates, setRates] = useState<CurrencyRate[]>([]);
  const [month, setMonth] = useState("");
  const [year, setYear] = useState<number | string>("");
  const [loading, setLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState("");

  const groupedRates = useMemo(() => {
    const groups = new Map<string, CurrencyRate[]>();

    rates.forEach((rate) => {
      const currency = String(rate.user_currency || "-")
        .trim()
        .toUpperCase();

      if (!groups.has(currency)) {
        groups.set(currency, []);
      }

      groups.get(currency)!.push(rate);
    });

    return Array.from(groups.entries());
  }, [rates]);


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
        throw new Error(
          result?.error || result?.message || "Failed to load currency rates."
        );
      }

      const rows: CurrencyRate[] = Array.isArray(result)
        ? result
        : Array.isArray(result?.currency_rates)
          ? result.currency_rates
          : [];

      // Determine the current month and year.
      const currentMonth =
        result?.current_month ||
        rows[0]?.month ||
        new Date().toLocaleString("en-US", { month: "long" });

      const currentYear =
        result?.current_year ||
        rows[0]?.year ||
        new Date().getFullYear();

      setMonth(currentMonth);
      setYear(currentYear);

      // UI: Show only current month and year.
      const currentMonthRows = rows.filter((rate) => {
        const rowMonth = String(rate.month || "").trim().toLowerCase();
        const rowYear = String(rate.year || "").trim();

        return (
          rowMonth === String(currentMonth).trim().toLowerCase() &&
          rowYear === String(currentYear).trim()
        );
      });

      // Hide identical Home Currency and Selected Currency.
      const visibleRows = currentMonthRows.filter((rate) => {
        const home = String(rate.user_currency || "").trim().toUpperCase();
        const selected = String(rate.selected_currency || "").trim().toUpperCase();

        return !home || !selected || home !== selected;
      });

      // Deduplicate current month's UI rows.
      const uniqueRowsMap = new Map<string, CurrencyRate>();

      visibleRows.forEach((rate) => {
        const home = String(rate.user_currency || "").trim().toUpperCase();
        const country = String(rate.country || "").trim().toUpperCase();
        const selected = String(rate.selected_currency || "").trim().toUpperCase();

        const uniqueKey = `${home}-${country}-${selected}`;

        if (!uniqueRowsMap.has(uniqueKey)) {
          uniqueRowsMap.set(uniqueKey, rate);
        }
      });

      setRates(Array.from(uniqueRowsMap.values()));

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




  // const downloadHistory = async () => {
  //   if (!token) {
  //     setError("Authentication token is missing. Please sign in again.");
  //     return;
  //   }

  //   try {
  //     setDownloading(true);
  //     setError("");

  //     // Fetch ALL historical currency rates.
  //     const response = await fetch(
  //       `${API_BASE}/currency-rates?history=true`,
  //       {
  //         method: "GET",
  //         headers: {
  //           Authorization: `Bearer ${token}`,
  //         },
  //         cache: "no-store",
  //       }
  //     );

  //     const result = await response.json().catch(() => ({}));

  //     if (!response.ok) {
  //       throw new Error(
  //         result?.error ||
  //         result?.message ||
  //         "Failed to fetch currency history."
  //       );
  //     }

  //     const rows: CurrencyRate[] = Array.isArray(result)
  //       ? result
  //       : Array.isArray(result?.currency_rates)
  //         ? result.currency_rates
  //         : [];

  //     if (!rows.length) {
  //       throw new Error("No historical currency rates found.");
  //     }

  //     // Map backend fields to your Excel utility headers.
  //     // No current-month filtering.
  //     const excelData = rows.map((rate) => ({
  //       "Month": rate.month || "-",
  //       "Year": rate.year || "-",
  //       "Home Currency": String(rate.user_currency || "-").toUpperCase(),
  //       "Country": rate.country || "-",
  //       "Selected Currency": String(
  //         rate.selected_currency || "-"
  //       ).toUpperCase(),
  //       "Conversion Rate":
  //         rate.conversion_rate === null ||
  //           rate.conversion_rate === undefined
  //           ? "-"
  //           : rate.conversion_rate,
  //     }));

  //     // Generate Excel using your existing custom utility.
  //     exportCurrencyRatesExcel({
  //       filename: "Currency_Rates_History_All_Time.xlsx",
  //       titleLine: "Currency Rates History",
  //       titleCountry: "Global",
  //       platformLabel: "Phormula",
  //       periodLabel: "All Time",
  //       companyName,
  //       brandName,
  //       homeCurrencyCode: "USD",
  //       dataRows: excelData,
  //     });

  //   } catch (err: any) {
  //     setError(
  //       err?.message || "Unable to download currency history."
  //     );
  //   } finally {
  //     setDownloading(false);
  //   }
  // };

  const downloadHistory = async () => {
    if (!token) {
      setError("Authentication token is missing. Please sign in again.");
      return;
    }

    try {
      setDownloading(true);
      setError("");

      // Fetch ALL historical currency rates.
      const response = await fetch(
        `${API_BASE}/currency-rates?history=true`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
          },
          cache: "no-store",
        }
      );

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(
          result?.error ||
          result?.message ||
          "Failed to fetch currency history."
        );
      }

      const rows: CurrencyRate[] = Array.isArray(result)
        ? result
        : Array.isArray(result?.currency_rates)
          ? result.currency_rates
          : [];

      if (!rows.length) {
        throw new Error("No historical currency rates found.");
      }

      // --- DEDUPLICATION LOGIC START ---
      // Use a Map to keep only unique combinations of Month, Year, Home Currency, Country, and Selected Currency
      const uniqueRowsMap = new Map<string, any>();

      rows.forEach((rate) => {
        const rawMonth = String(rate.month || "-").trim();
        const monthStr = rawMonth !== "-"
          ? rawMonth.charAt(0).toUpperCase() + rawMonth.slice(1).toLowerCase()
          : "-";
        const yearStr = rate.year || "-";
        const home = String(rate.user_currency || "-").toUpperCase();
        const country = String(rate.country || "-").toUpperCase();
        const selected = String(rate.selected_currency || "-").toUpperCase();

        // Create a unique key based on the columns we want to deduplicate by
        const uniqueKey = `${monthStr}-${yearStr}-${home}-${country}-${selected}`;

        if (!uniqueRowsMap.has(uniqueKey)) {
          uniqueRowsMap.set(uniqueKey, {
            "Month": monthStr,
            "Year": yearStr,
            "Home Currency": home,
            "Country": country,
            "Selected Currency": selected,
            "Conversion Rate":
              rate.conversion_rate === null ||
                rate.conversion_rate === undefined
                ? "-"
                : rate.conversion_rate,
          });
        }
      });

      // Convert the Map values back to an array for the Excel export
      const excelData = Array.from(uniqueRowsMap.values());
      // --- DEDUPLICATION LOGIC END ---

      // Generate Excel using your existing custom utility.
      exportCurrencyRatesExcel({
        filename: "Currency Rates.xlsx",
        titleLine: "Currency Rates",
        titleCountry: "Global",
        platformLabel: "Phormula",
        periodLabel: "All Time",
        companyName,
        brandName,
        homeCurrencyCode: "USD",
        dataRows: excelData,
      });

    } catch (err: any) {
      setError(
        err?.message || "Unable to download currency history."
      );
    } finally {
      setDownloading(false);
    }
  };

  return (
    <section className="w-full overflow-hidden rounded-2xl border border-[#E1E7E3] bg-white shadow-sm">
      <div className="flex flex-col gap-4 px-5 pt-5 pb-2 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            {/* <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-[#EAF5EF] text-[#438B70]">
              <FiTrendingUp size={17} />
            </span> */}
            {/* <h2 className="text-base font-bold text-[#37455F]">Currency Rates</h2> */}
            <PageBreadcrumb
              pageTitle={
                <span className="inline-flex items-center gap-2">
                  <span>Currency Rates</span>
                  {month && year && (
                    <span className="leading-none text-[#438B70] whitespace-nowrap">
                      {`${month.slice(0, 3).charAt(0).toUpperCase()}${month.slice(1, 3).toLowerCase()}'${String(year).slice(-2)}`}
                    </span>
                  )}
                </span>
              }
              variant="page"
              align="left"
              textSize="2xl"
            />
            {/* 
            <span className="rounded-full bg-[#EAF7F1] px-2.5 py-1 text-[11px] font-semibold text-[#438B70]">
              {month ? `${month.charAt(0).toUpperCase()}${month.slice(1)} ${year}` : "Current month"}
            </span> */}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => void fetchRates()}
            disabled={loading}
            className="inline-flex h-9 items-center gap-2 rounded-lg border border-[#DDE5E1] bg-white px-3 text-xs font-semibold text-[#37455F] transition hover:bg-[#F5F8F6] disabled:cursor-not-allowed disabled:opacity-60"
          >
            <FiRefreshCw className={loading ? "animate-spin" : ""} size={13} />
            {/* {loading ? "Refreshing..." : "Refresh"} */}
          </button>
          <button
            type="button"
            onClick={downloadHistory}
            disabled={downloading}
            className="inline-flex h-9 items-center gap-2 rounded-lg bg-[#5EA68E] px-3 text-xs font-semibold text-white shadow-[0px_4px_4px_0px_#00000040] transition hover:bg-[#4A927A] disabled:cursor-not-allowed disabled:opacity-60"
          >
            <FiDownload size={14} />
            {/* {downloading ? "Downloading..." : "Download full history"} */}
          </button>
        </div>
      </div>

      <div className="px-5 pt-0 pb-5">
        {/* <div className="mb-4 rounded-xl border border-[#DDEBE4] bg-[#F8FCFA] px-4 py-3">
          <p className="text-xs text-[#557267]">
            This tab displays only the current month. Download the Excel file to view the complete historical currency rates.
          </p>
        </div> */}

        {error ? (
          <div className="flex flex-col gap-3 rounded-xl border border-[#EAC9C9] bg-[#FFF6F6] px-4 py-4 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-xs text-[#A94E4E]">{error}</p>
            <button
              type="button"
              onClick={() => void fetchRates()}
              className="text-xs font-semibold text-[#0E8558]"
            >
              Try again
            </button>
          </div>
        ) : loading ? (
          <div className="grid min-h-[180px] place-items-center text-xs text-[#78847E]">
            Loading currency rates...
          </div>
        ) : (
          <div className="w-full overflow-x-auto rounded-xl border border-[#E5EBE7]">
            <table className="w-full min-w-[600px] border-collapse text-xs 2xl:text-sm text-charcoal-500 text-left">
              <thead>
                <tr className="bg-[#5EA68E]">
                  <th className="w-[25%] border-b border-r border-gray-300 px-3 py-2 text-center align-middle whitespace-nowrap font-bold text-yellow-200">
                    Home Currency
                  </th>

                  <th className="w-[20%] border-b border-r border-gray-300 px-3 py-2 text-center align-middle whitespace-nowrap font-bold text-yellow-200">
                    Country
                  </th>

                  <th className="w-[27.5%] border-b border-r border-gray-300 px-3 py-2 text-center align-middle whitespace-nowrap font-bold text-yellow-200">
                    Selected Currency
                  </th>

                  <th className="w-[27.5%] border-b border-gray-300 px-3 py-2 text-center align-middle whitespace-nowrap font-bold text-yellow-200">
                    Conversion Rate
                  </th>
                </tr>
              </thead>

              <tbody>
                {groupedRates.length === 0 ? (
                  <tr>
                    <td
                      colSpan={4}
                      className="px-4 py-10 text-center text-xs text-[#78847E]"
                    >
                      No currency rates found for the current month.
                    </td>
                  </tr>
                ) : (
                  groupedRates.map(([currency, currencyRates]) =>
                    currencyRates.map((rate, index) => (
                      <tr
                        key={`${currency}-${rate.country}-${rate.selected_currency}-${index}`}
                        className="transition-colors"
                      >
                        {/* Merged Home Currency */}
                        {index === 0 && (
                          <td
                            rowSpan={currencyRates.length}
                            className="border-r border-b border-[#DCE5E0] px-4 py-3 text-center align-middle"
                          >
                            <div className="flex flex-col items-center justify-center gap-1.5">
                              <span className="text-sm font-bold uppercase tracking-wide text-charcoal-500">
                                {currency}
                              </span>


                            </div>
                          </td>
                        )}

                        {/* Country */}
                        <td className="border-r border-b border-[#E5EBE7] px-4 py-3 text-center text-xs font-semibold text-charcoal-500">
                          {rate.country || "-"}
                        </td>

                        {/* Selected Currency */}
                        <td className="border-r border-b border-[#E5EBE7] px-4 py-3 text-center text-xs font-semibold uppercase text-charcoal-500">
                          {rate.selected_currency || "-"}
                        </td>

                        {/* Conversion Rate */}
                        <td className="border-b border-[#E5EBE7] px-4 py-3 text-center text-xs font-semibold tabular-nums text-charcoal-500">
                          {rate.conversion_rate === null ||
                            rate.conversion_rate === undefined
                            ? "-"
                            : Number(rate.conversion_rate).toLocaleString("en-US", {
                              maximumFractionDigits: 8,
                            })}
                        </td>
                      </tr>
                    ))
                  )
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}
