// app/orders/page.tsx
"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import GraphPageforShopify from "@/components/charts/GraphPageforShopify";
import PeriodFiltersTable from "@/components/filters/PeriodFiltersTable";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

// ---- Types ----
interface LastRowData {
  currency: string;
  total_discounts: number;
  total_orders: number;
  total_tax: number;
  net_sales: number;
  // If your API returns more summary fields, add them here.
}

interface DropdownResponse {
  last_row_data?: LastRowData;
  [key: string]: unknown;
}

type RangeOption = "monthly" | "quarterly" | "yearly";

export default function OrdersPage() {
  // Query params we expect: ?shop=&token=&email=
  const searchParams = useSearchParams();
  const shop = searchParams.get("shop") || "";
  const tokenFromUrl = searchParams.get("token") || "";
  const email = searchParams.get("email") || "";

  // (optional) fallback to a stored token if URL one is missing
  const tokenStored =
    typeof window !== "undefined" ? localStorage.getItem("shopifyToken") || "" : "";
  const token = tokenFromUrl || tokenStored;

  const user_token =
    typeof window !== "undefined" ? localStorage.getItem("jwtToken") || "" : "";

  // ---- State ----
  const [orders, setOrders] = useState<unknown[]>([]);
  const [range, setRange] = useState<RangeOption>("monthly");

  // PeriodFiltersTable contract:
  // selectedMonth is a lowercase slug ("january"..."december")
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  // selectedQuarter is "Q1" | "Q2" | "Q3" | "Q4" | ""
  const [selectedQuarter, setSelectedQuarter] = useState<string>("");
  const [selectedYear, setSelectedYear] = useState<string>("");

  const [yearOptions, setYearOptions] = useState<(string | number)[]>([]);
  const [responseData, setResponseData] = useState<DropdownResponse | null>(null);

  // Build year options (current year and 6 previous)
  useEffect(() => {
    const currentYear = new Date().getFullYear();
    const years: number[] = [];
    for (let i = 0; i < 7; i++) years.push(currentYear - i);
    setYearOptions(years);
  }, []);

  // Optional prefetch (not used by charts, but keeping parity with your original behavior)
  useEffect(() => {
    const fetchOrders = async () => {
      try {
        const url = new URL(`${process.env.NEXT_PUBLIC_API_BASE_URL}/shopify/get_orders`);
        url.searchParams.set("shop", shop);
        url.searchParams.set("token", token);
        url.searchParams.set("user_token", user_token);

        const res = await fetch(url.toString());
        const json = await res.json();
        setOrders(Array.isArray(json?.orders) ? json.orders : []);
      } catch (e) {
        console.error("Error fetching orders:", e);
      }
    };

    if (shop && token && user_token) fetchOrders();
  }, [shop, token, user_token]);

  // Reset + change range
  const handleRangeChange = (v: RangeOption) => {
    setRange(v);
    setSelectedMonth("");
    setSelectedQuarter("");
    setSelectedYear("");
    setResponseData(null);
  };

  const shouldFetch = useMemo(() => {
    return (
      (range === "monthly" && !!selectedMonth && !!selectedYear) ||
      (range === "quarterly" && !!selectedQuarter && !!selectedYear) ||
      (range === "yearly" && !!selectedYear)
    );
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

  // Fetch dropdown summary for the current filters
  useEffect(() => {
    if (!shop || !token || !user_token) return;
    if (!shouldFetch) return;

    (async () => {
      try {
        // Backend expects quarter as "1".."4". Our UI stores "Q1".."Q4".
        const quarterForApi = selectedQuarter ? selectedQuarter.replace(/^Q/i, "") : "";

        const params = new URLSearchParams({
          range,
          month: selectedMonth,          // lowercase slug ("january" ... "december")
          quarter: quarterForApi,        // "1".."4"
          year: selectedYear,            // string
          user_token,
          shop,
          token,
        });

        const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/shopify/dropdown?${params.toString()}`);
        const text = await res.text();

        // Replace bare-word NaN with 0.0 to avoid JSON.parse errors
        const sanitized = text.replace(/\bNaN\b/g, "0.0");
        const data: DropdownResponse = JSON.parse(sanitized);

        if (data?.last_row_data) setResponseData(data);
        else setResponseData(null);
      } catch (err) {
        console.error("dropdown fetch error:", err);
        setResponseData(null);
      }
    })();
  }, [range, selectedMonth, selectedQuarter, selectedYear, shop, token, user_token, shouldFetch]);

  // -------- Summary prep (inline, no separate component) --------

  // Currency symbol helper
  const currencySymbol = useMemo(() => {
    const code = (responseData?.last_row_data?.currency ?? "") as string;
    const upper = code?.toUpperCase?.() ?? "";
    const map: Record<string, string> = {
      USD: "$",
      EUR: "€",
      GBP: "£",
      INR: "₹",
      AUD: "A$",
      CAD: "C$",
      JPY: "¥",
    };
    return map[upper] || upper || "";
  }, [responseData]);

  // Build displayData the table expects (unit_sold, total_sales, total_expense, cm2_profit)
  // Assumptions:
  // - unit_sold ~= total_orders (count)
  // - total_sales <- net_sales
  // - total_expense, cm2_profit default to 0 unless provided elsewhere
  const displayData = useMemo(() => {
    const lr = responseData?.last_row_data ?? ({} as Partial<LastRowData>);
    return {
      unit_sold: Number(lr?.total_orders ?? 0),
      total_sales: Number(lr?.net_sales ?? 0),
      total_expense: 0,
      cm2_profit: 0,
    };
  }, [responseData]);

  const hasSummary = Boolean(responseData?.last_row_data) && shouldFetch;
  const headerTitle = "Shopify Financial Metrics";

  const numberOrZero = (v: unknown, fractionDigits = 2) => {
    const n = typeof v === "number" ? v : Number(v);
    if (Number.isNaN(n)) return (0).toFixed(fractionDigits);
    return n.toFixed(fractionDigits);
  };

  // Helper to capitalize month for PeriodFiltersTable display
  const capMonth = (s: string) => (s ? s[0].toUpperCase() + s.slice(1) : "");

  return (
    <div className="space-y-4">
      {/* <h2 className="mb-4 text-2xl font-semibold text-neutral-800">{headerTitle}</h2> */}

      <div className="flex gap-2">
        <PageBreadcrumb pageTitle={headerTitle} variant="page" align="left" textSize="2xl" />

      </div>

      {/* Filters + Summary */}
      <div className="flex flex-col md:flex-row md:items-center md:gap-16 gap-4 w-full md:justify-start justify-center">
        {/* Period Filters (your component) */}
        <PeriodFiltersTable
          range={range}
          selectedMonth={capMonth(selectedMonth)} // component displays capitalized month
          selectedQuarter={selectedQuarter}       // "Q1" | "" etc
          selectedYear={selectedYear}
          yearOptions={yearOptions}
          onRangeChange={handleRangeChange}
          onMonthChange={(v) => setSelectedMonth(v)}      // receives lowercase id from component
          onQuarterChange={(v) => setSelectedQuarter(v)}  // expects "Q1".."Q4"
          onYearChange={(v) => setSelectedYear(v)}
        />

        {/* Summary Table (inline) */}
        {(hasSummary || responseData?.last_row_data) &&
          (() => {
            const summary = displayData;
            const isSummaryZero =
              summary.unit_sold === 0 &&
              summary.total_sales === 0 &&
              summary.total_expense === 0 &&
              summary.cm2_profit === 0;

            return (
              <table
                className={[
                  "w-full md:w-[18vw] border-collapse rounded-md text-[clamp(12px,0.729vw,16px)] font-[Lato]",
                  isSummaryZero ? "opacity-30" : "opacity-100",
                ].join(" ")}
              >
                <thead>
                  <tr className="border border-[#414042] bg-white text-[#5EA68E]">
                    <th className="border border-[#414042] px-[0.9vw] py-[1vh] text-center">Units</th>
                    <th className="border border-[#414042] px-[0.9vw] py-[1vh] text-center">Sales</th>
                    <th className="border border-[#414042] px-[0.9vw] py-[1vh] text-center">Expense</th>
                    <th className="whitespace-nowrap border border-[#414042] px-[0.9vw] py-[1vh] text-center">
                      CM2 Profit
                    </th>
                    <th className="whitespace-nowrap border border-[#414042] px-[0.9vw] py-[1vh] text-center">
                      CM2 Profit (%)
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="border border-[#414042] px-[0.9vw] py-[1vh] text-center">
                      {summary.unit_sold}
                    </td>
                    <td className="whitespace-nowrap border border-[#414042] px-[0.9vw] py-[1vh] text-center">
                      {currencySymbol}{" "}
                      {summary.total_sales.toLocaleString(undefined, {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      })}
                    </td>
                    <td className="whitespace-nowrap border border-[#414042] px-[0.9vw] py-[1vh] text-center">
                      {currencySymbol}{" "}
                      {summary.total_expense.toLocaleString(undefined, {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      })}
                    </td>
                    <td className="whitespace-nowrap border border-[#414042] px-[0.9vw] py-[1vh] text-center">
                      {currencySymbol}{" "}
                      {summary.cm2_profit.toLocaleString(undefined, {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      })}
                    </td>
                    <td className="border border-[#414042] px-[0.9vw] py-[1vh] text-center">
                      {summary.total_sales > 0
                        ? `${((summary.cm2_profit / summary.total_sales) * 100).toFixed(2)}%`
                        : "0%"}
                    </td>
                  </tr>
                </tbody>
              </table>
            );
          })()}
      </div>

      {/* Charts */}
      {range === "monthly" &&
        selectedMonth &&
        selectedYear &&
        responseData?.last_row_data && (
          <GraphPageforShopify
            range="monthly"
            selectedMonth={selectedMonth}
            selectedYear={selectedYear}
          />
        )}

      {range === "quarterly" &&
        selectedQuarter &&
        selectedYear &&
        responseData?.last_row_data && (
          <GraphPageforShopify
            range="quarterly"
            selectedQuarter={selectedQuarter} // "Q1" etc (GraphPageforShopify expects "Q1")
            selectedYear={selectedYear}
          />
        )}

      {range === "yearly" && selectedYear && responseData?.last_row_data && (
        <GraphPageforShopify range="yearly" selectedYear={selectedYear} />
      )}
    </div>
  );
}
