"use client";

import React, { useEffect, useState } from "react";
import { useParams } from "next/navigation";


type AnyRecord = Record<string, any>;

type UploadHistoryRow = {
  id: number | string;
  country: string;
  month: string | number;
  year: string | number;
  total_sales: number;
  total_profit: number;
  total_expense: number;
};

type CountryProfileRow = {
  id: number | string;
  country: string;
  stock_unit: string | number;
  transit_time: string | number;
};

type SkuWiseTable = {
  table?: string;
  rows?: AnyRecord[];
  error?: string;
};

type ViewUserData = {
  user_id?: number | string;
  brand_name?: string;
  name?: string;
  annual_sales_range?: string;
  related_upload_history?: UploadHistoryRow[];
  related_country_profiles?: CountryProfileRow[];
  skuwise_tables?: SkuWiseTable[];
};

type SummaryData = {
  costOfAdvertisement: number;
  platformFees: number;
  cm2ProfitLoss: number;
  cm2Margins: string;
  acos: string;
  netReimbursement: number;
  reimbursementVsCM2Margins: string;
  reimbursementVsSales: string;
};

export default function ViewUserPage() {
  const params = useParams<{ email: string }>();
  const email = params?.email ? decodeURIComponent(params.email) : "";

  const [data, setData] = useState<ViewUserData | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string>("");
  const [openRegions, setOpenRegions] = useState<Record<string, boolean>>({});
  const [openTables, setOpenTables] = useState<Record<string, boolean>>({});
  const [openSubsections, setOpenSubsections] = useState<Record<string, boolean>>({});

  const preferredColumns = [
    "product_name",
    "sku",
    "quantity",
    "asp",
    "net_sales",
    "cost_of_unit_sold",
    "amazon_fee",
    "selling_fees",
    "fba_fees",
    "net_credits",
    "net_taxes",
    "profit",
    "profit_percentage",
    "sales_mix",
    "profit_mix",
    "price_in_gbp",
  ].filter(Boolean);

  const toggleRegion = (region: string) =>
    setOpenRegions((prev) => ({ ...prev, [region]: !prev[region] }));

  const toggleTable = (key: string) =>
    setOpenTables((prev) => ({ ...prev, [key]: !prev[key] }));

  const toggleSubsection = (region: string, type: string) => {
    const key = `${region}-${type}`;
    setOpenSubsections((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const extractSummaryData = (rows?: AnyRecord[]): SummaryData | null => {
    if (!rows || rows.length === 0) return null;
    const lastRow = rows[rows.length - 1];

    const pct = (v: any) => {
      const n = Number(v);
      return Number.isNaN(n) ? "0.00%" : `${n.toFixed(2)}%`;
    };

    return {
      costOfAdvertisement: Number(lastRow.advertising_total) || 0,
      platformFees: Number(lastRow.platform_fee) || 0,
      cm2ProfitLoss: Number(lastRow.cm2_profit) || 0,
      cm2Margins: pct(lastRow.cm2_margins),
      acos: pct(lastRow.acos),
      netReimbursement: Number(lastRow.rembursement_fee) || 0,
      reimbursementVsCM2Margins: pct(lastRow.rembursment_vs_cm2_margins),
      reimbursementVsSales: pct(lastRow.reimbursement_vs_sales),
    };
  };

  useEffect(() => {
    const fetchUserDetails = async () => {
      const token = localStorage.getItem("superadmin_token");
      const admin_token = localStorage.getItem("admin_token");

      if (!token && !admin_token) {
        setError("You are not authorized to view this page.");
        setLoading(false);
        return;
      }

      try {
        let result: ViewUserData | null = null;

        if (token) {
          const res = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/dashboard?email=${encodeURIComponent(email)}`,
            { method: "GET", headers: { Authorization: `Bearer ${token}` } }
          );
          const json = (await res.json()) as any;
          if (!res.ok) throw new Error(json.message || "Superadmin fetch failed");
          result = json as ViewUserData;
        }

        if (admin_token) {
          const res = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/admin/dashboard?email=${encodeURIComponent(email)}`,
            { method: "GET", headers: { Authorization: `Bearer ${admin_token}` } }
          );
          const json = (await res.json()) as any;
          if (!res.ok) throw new Error(json.message || "Admin fetch failed");
          result = result ? ({ ...result, ...json } as ViewUserData) : (json as ViewUserData);
        }

        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    };

    if (email) fetchUserDetails();
  }, [email]);

  const groupedTables: Record<
    "UK" | "US" | "GLOBAL",
    { MTDs: SkuWiseTable[]; QTDs: SkuWiseTable[]; YTDs: SkuWiseTable[] }
  > = {
    UK: { MTDs: [], QTDs: [], YTDs: [] },
    US: { MTDs: [], QTDs: [], YTDs: [] },
    GLOBAL: { MTDs: [], QTDs: [], YTDs: [] },
  };

  if (Array.isArray(data?.skuwise_tables)) {
    const seen: Record<
      "UK" | "US" | "GLOBAL",
      { MTDs: Set<string>; QTDs: Set<string>; YTDs: Set<string> }
    > = {
      UK: { MTDs: new Set(), QTDs: new Set(), YTDs: new Set() },
      US: { MTDs: new Set(), QTDs: new Set(), YTDs: new Set() },
      GLOBAL: { MTDs: new Set(), QTDs: new Set(), YTDs: new Set() },
    };

    data.skuwise_tables.forEach((table) => {
      const name = String(table.table || "").toUpperCase();
      const hasUK = name.includes("UK");
      const hasUS = name.includes("US");
      const hasGLOBAL = name.includes("GLOBAL");

      let type: "MTDs" | "QTDs" | "YTDs" = "MTDs";
      if (name.includes("YEARLY")) type = "YTDs";
      else if (name.includes("QUARTER")) type = "QTDs";

      let region: "UK" | "US" | "GLOBAL" | null = null;
      if (hasUK && !hasGLOBAL) region = "UK";
      else if (hasUS && !hasGLOBAL) region = "US";
      else if (hasGLOBAL) region = "GLOBAL";

      if (region && !seen[region][type].has(name)) {
        seen[region][type].add(name);
        groupedTables[region][type].push(table);
      }
    });
  }

  const renderSummarySection = (summaryData: SummaryData | null, colCount: number) => {
    if (!summaryData) return null;

    const labelCell = (label: string) => (
      <td
        colSpan={Math.max(colCount - 1, 1)}
        className="bg-slate-50 px-3 py-2 text-xs sm:text-sm font-medium text-slate-600 text-right"
      >
        {label}
      </td>
    );

    const valCell = (val: string | number) => (
      <td className="bg-slate-50 px-3 py-2 text-xs sm:text-sm font-semibold text-slate-800">
        {typeof val === "number" ? val.toLocaleString() : val}
      </td>
    );

    return (
      <tfoot className="border-t">
        <tr>{labelCell("Cost of Advertisement")}{valCell(summaryData.costOfAdvertisement)}</tr>
        <tr>{labelCell("Platform Fees")}{valCell(summaryData.platformFees)}</tr>
        <tr>{labelCell("CM2 Profit/Loss")}{valCell(summaryData.cm2ProfitLoss)}</tr>
        <tr>{labelCell("CM2 Margins")}{valCell(summaryData.cm2Margins)}</tr>
        <tr>{labelCell("ACOS (Average Cost of Sales)")}{valCell(summaryData.acos)}</tr>
        <tr>{labelCell("Net Reimbursement during the month")}{valCell(summaryData.netReimbursement)}</tr>
        <tr>{labelCell("Reimbursement vs CM2 Margins")}{valCell(summaryData.reimbursementVsCM2Margins)}</tr>
        <tr>{labelCell("Reimbursement vs Sales")}{valCell(summaryData.reimbursementVsSales)}</tr>
      </tfoot>
    );
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-emerald-50 to-slate-100">
        <div className="h-10 w-10 animate-spin rounded-full border-2 border-[#1f5274]/30 border-t-[#1f5274]" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen p-4 sm:p-6 bg-gradient-to-br from-emerald-50 to-slate-100">
        <div className="max-w-full mx-auto">
          <div className="rounded-lg border border-red-200 bg-red-50 text-red-700 px-4 py-3">
            {error}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen w-full bg-gradient-to-br from-emerald-50 to-slate-100 p-4 sm:p-6">
      <div className="w-full mb-6 rounded-xl bg-gradient-to-r from-[#5EA68E] to-[#1f5274] text-white shadow-lg">
        <div className="px-4 sm:px-6 py-4">
          <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-3">
            <h2 className="text-xl sm:text-2xl font-semibold">User Details for: {email}</h2>
            {data && (
              <div className="grid sm:grid-cols-3 gap-2 sm:gap-6 text-white/90 text-sm sm:text-base">
               
                <p><strong className="text-white">Brand Name:</strong> {data.brand_name}</p>
                <p><strong className="text-white">Annual Sales Range:</strong> {data.annual_sales_range}</p>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="space-y-8">
        {data?.related_upload_history?.length ? (
          <section className="space-y-3">
            <h3 className="text-lg font-semibold text-[#1f5274]">
              Uploaded History of User — {email}
            </h3>
            <div className="overflow-x-auto rounded-xl bg-white shadow ring-1 ring-slate-200">
              <table className="w-full table-auto">
                <thead className="bg-slate-50">
                  <tr>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Country</th>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Month</th>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Year</th>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Total Sales</th>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Total Profit</th>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Total Expense</th>
                  </tr>
                </thead>
                <tbody>
                  {data.related_upload_history?.map((hist) => (
                    <tr key={hist.id} className="border-t">
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {hist.country.toUpperCase()}
                      </td>
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {hist.month}
                      </td>
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {hist.year}
                      </td>
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {Number(hist.total_sales).toFixed(2)}
                      </td>
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {Number(hist.total_profit).toFixed(2)}
                      </td>
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {Number(hist.total_expense).toFixed(2)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}

        {data?.related_country_profiles?.length ? (
          <section className="space-y-3">
            <h3 className="text-lg font-semibold text-[#1f5274]">
              Country Profiles of User — {email}
            </h3>
            <div className="overflow-x-auto rounded-xl bg-white shadow ring-1 ring-slate-200">
              <table className="w-full table-auto">
                <thead className="bg-slate-50">
                  <tr>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Country</th>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Stock Unit</th>
                    <th className="px-3 sm:px-4 py-2 sm:py-3 text-left text-xs sm:text-sm font-semibold text-slate-600">Transit Time</th>
                  </tr>
                </thead>
                <tbody>
                  {data.related_country_profiles?.map((p) => (
                    <tr key={p.id} className="border-t">
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {p.country.toUpperCase()}
                      </td>
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {p.stock_unit}
                      </td>
                      <td className="px-3 sm:px-4 py-2 sm:py-3 text-xs sm:text-sm text-slate-700">
                        {p.transit_time}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}

        {Array.isArray(data?.skuwise_tables) && data.skuwise_tables.length > 0 && (
          <section className="space-y-6">
            {Object.entries(groupedTables).map(([region, tables]) => (
              <div key={region} className="space-y-3">
                <button
                  onClick={() => toggleRegion(region)}
                  className="w-full text-left px-4 py-3 rounded-lg bg-slate-100 hover:bg-slate-200 text-[#1f5274] font-semibold border border-slate-200 flex items-center gap-2"
                >
                  <i
                    className={`fa-solid ${
                      openRegions[region] ? "fa-chevron-down" : "fa-chevron-right"
                    } text-slate-500`}
                  />
                  {region} REGION
                </button>

                {openRegions[region] && (
                  <div className="space-y-3 pl-0 sm:pl-4">
                    {(["MTDs", "QTDs", "YTDs"] as const).map((type) =>
                      tables[type].length > 0 ? (
                        <div key={type} className="space-y-2">
                          <button
                            onClick={() => toggleSubsection(region, type)}
                            className="w-full text-left px-3 py-2 rounded-md bg-white hover:bg-slate-50 text-[#5EA68E] font-semibold border border-slate-200 flex items-center gap-2"
                          >
                            <i
                              className={`fa-solid ${
                                openSubsections[`${region}-${type}`]
                                  ? "fa-chevron-down"
                                  : "fa-chevron-right"
                              } text-slate-500`}
                            />
                            {type}
                          </button>

                          {openSubsections[`${region}-${type}`] &&
                            tables[type].map((table, index) => {
                              const key = `${region}-${type}-${index}`;
                              const allColumns =
                                Array.isArray(table.rows) && table.rows.length > 0
                                  ? Object.keys(table.rows[0])
                                  : [];

                              const columnOrder = [
                                ...preferredColumns,
                                ...allColumns.filter((c) => !preferredColumns.includes(c)),
                              ];

                              const summaryData = extractSummaryData(table.rows);

                              return (
                                <div key={key} className="space-y-2 pl-0 sm:pl-6">
                                  <button
                                    onClick={() => toggleTable(key)}
                                    className="w-full text-left px-3 py-2 rounded-md bg-white hover:bg-slate-50 text-slate-700 font-medium border border-slate-200 flex items-center gap-2"
                                  >
                                    <i
                                      className={`fa-solid ${
                                        openTables[key] ? "fa-chevron-down" : "fa-chevron-right"
                                      } text-slate-500`}
                                    />
                                    {String(table.table || "")
                                      .replaceAll("_", " ")
                                      .toUpperCase()}
                                  </button>

                                  {openTables[key] &&
                                    (table.error ? (
                                      <p className="text-amber-600 text-sm px-3">
                                        ⚠️ {table.error}
                                      </p>
                                    ) : (
                                      <div className="overflow-x-auto rounded-xl bg-white shadow ring-1 ring-slate-200">
                                        <table className="w-full table-auto">
                                          <thead className="bg-slate-50">
                                            <tr>
                                              {columnOrder.map((col) => (
                                                <th
                                                  key={col}
                                                  className="px-3 sm:px-4 py-2 sm:py-3 text-left text-[11px] sm:text-xs md:text-sm font-semibold text-slate-600 whitespace-nowrap"
                                                >
                                                  {col.replaceAll("_", " ").toUpperCase()}
                                                </th>
                                              ))}
                                            </tr>
                                          </thead>

                                          <tbody>
                                            {table.rows?.map((row, i) => (
                                              <tr key={i} className="border-t hover:bg-slate-50/60">
                                                {columnOrder.map((col) => (
                                                  <td
                                                    key={col}
                                                    className="px-3 sm:px-4 py-2 sm:py-3 text-[11px] sm:text-xs md:text-sm text-slate-700"
                                                  >
                                                    {row?.[col]}
                                                  </td>
                                                ))}
                                              </tr>
                                            ))}
                                          </tbody>

                                          {renderSummarySection(summaryData, columnOrder.length)}
                                        </table>
                                      </div>
                                    ))}
                                </div>
                              );
                            })}
                        </div>
                      ) : null
                    )}
                  </div>
                )}
              </div>
            ))}
          </section>
        )}

       
      </div>
    </div>
  );
}