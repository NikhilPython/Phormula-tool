"use client";

import MonthYearPickerTable from "@/components/filters/MonthYearPickerTable";
import { useParams, useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx";

type TableRow = Record<string, any>;

export default function ErrorStatusPage() {
  // Route: /error-status/[countryName]/[month]/[year]
  const params = useParams<{ countryName?: string; month?: string; year?: string }>();
  const router = useRouter();

  // URL params (raw)
  const countryParam = (params?.countryName || "").toString();
  const monthParam = (params?.month || "").toString();
  const yearParam = (params?.year || "").toString();

  // Canonical country in lowercase for API
  const effectiveCountry = useMemo(() => (countryParam || "").toLowerCase(), [countryParam]);

  // Local state (drives fetching)
  const [month, setMonth] = useState<string>(monthParam ? monthParam.toLowerCase() : "");
  const [year, setYear] = useState<string>(yearParam || "");

  // If params change externally (navigation), mirror into local state
  useEffect(() => {
    const nextMonth = monthParam ? monthParam.toLowerCase() : "";
    if (nextMonth !== month) setMonth(nextMonth);
    if ((yearParam || "") !== year) setYear(yearParam || "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [monthParam, yearParam]);

  const years = useMemo(
    () => Array.from({ length: 2 }, (_, i) => new Date().getFullYear() - i),
    []
  );

  const [tableData, setTableData] = useState<TableRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [errMsg, setErrMsg] = useState<string | null>(null);

  const selectedColumns = [
    "order_id",
    "sku",
    "description",
    "product_sales",
    "product_sales_tax",
    "postage_credits",
    "shipping_credits_tax",
    "gift_wrap_credits",
    "giftwrap_credits_tax",
    "promotional_rebates",
    "promotional_rebates_tax",
    "marketplace_facilitator_tax",
    "selling_fees",
    "fba_fees",
    "other_transaction_fees",
    "errorstatus",
    "answer",
    "difference",
  ];

  // Compute displayed columns from data
  const columns = useMemo(() => {
    if (tableData.length === 0) return selectedColumns;
    const present = new Set(Object.keys(tableData[0] || {}));
    return selectedColumns.filter((c) => present.has(c));
  }, [tableData]);

  // Keep the URL in sync with the current selections (no button needed)
  useEffect(() => {
    if (!effectiveCountry || !month || !year) return;
    const urlMonth = monthParam ? monthParam.toLowerCase() : "";
    const urlYear = yearParam || "";
    if (urlMonth !== month || urlYear !== year) {
      router.replace(
        `/error-status/${encodeURIComponent(effectiveCountry)}/${encodeURIComponent(
          month
        )}/${encodeURIComponent(year)}`
      );
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [effectiveCountry, month, year]);

  // Fetch whenever state is complete (mirrors Referral Fees behavior)
  useEffect(() => {
    const run = async () => {
      if (!effectiveCountry || !month || !year) return;
      setLoading(true);
      setErrMsg(null);
      setTableData([]);

      try {
        const token =
          typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        const resp = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/get_error_file/${effectiveCountry}/${month}/${year}`,
          { method: "GET", headers: token ? { Authorization: `Bearer ${token}` } : {} }
        );

        if (!resp.ok) {
          if (resp.status === 404) {
            setErrMsg("No error file found for this month/year yet.");
          } else {
            let err: any = {};
            try {
              err = await resp.json();
            } catch {
              /* ignore */
            }
            console.error("Error fetching file:", err?.error || resp.statusText);
            setErrMsg("Error fetching file");
          }
          setTableData([]);
          return;
        }

        const blob = await resp.blob();
        const arrayBuf = await blob.arrayBuffer();
        const workbook = XLSX.read(new Uint8Array(arrayBuf), { type: "array" });
        const ws = workbook.Sheets[workbook.SheetNames[0]];
        const json = (XLSX.utils.sheet_to_json(ws) as TableRow[]) || [];
        setTableData(json);
        setErrMsg(null);
      } catch (e) {
        console.error("Fetch error:", e);
        setErrMsg("An unexpected error occurred");
        setTableData([]);
      } finally {
        setLoading(false);
      }
    };
    run();
  }, [effectiveCountry, month, year]);

  const handleDownload = async () => {
    if (!effectiveCountry || !month || !year) return;
    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    try {
      const resp = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/get_error_file/${effectiveCountry}/${month}/${year}`,
        { method: "GET", headers: token ? { Authorization: `Bearer ${token}` } : {} }
      );
      if (!resp.ok) {
        if (resp.status === 404) setErrMsg("No error file available to download.");
        else setErrMsg("Error downloading file");
        return;
      }
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `error_file_${effectiveCountry}_${month}_${year}.xlsx`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (e) {
      console.error("Download error:", e);
      setErrMsg("Error downloading file");
    }
  };

  return (
    <div className="font-[Lato] text-[#414042]">
      <h2 className="text-base md:text-xl font-semibold bg-white rounded-md mb-3">
        Error Status Recon –{" "}
        <span className="text-[#60a68e]">{(effectiveCountry || "").toUpperCase()}</span>
      </h2>

      <MonthYearPickerTable
        month={month}                 // emits lowercase (valueMode="lower")
        year={year}
        yearOptions={years}
        onMonthChange={(v) => setMonth(v)}
        onYearChange={(v) => setYear(v)}
        valueMode="lower"
      />

      {loading && <div className="mt-2 text-sm">Loading…</div>}

      {!loading && month && year && effectiveCountry && (
        <>
          <div className="w-full max-w-full max-h-[65vh] overflow-auto mt-3">
            <table className="w-full text-sm border-collapse">
              <thead>
                <tr className="bg-[#5EA68E] text-[#f8edcf] font-bold">
                  {columns.map((key) => (
                    <th
                      key={key}
                      className="px-4 py-3 border border-[#747070] text-center first:text-left"
                    >
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tableData.length > 0 ? (
                  tableData.map((row, idx) => (
                    <tr
                      key={idx}
                      className={`${idx % 2 === 0 ? "bg-white" : "bg-[#f9f9f9]"} hover:bg-[rgb(72,168,135)] hover:shadow-inner`}
                    >
                      {columns.map((key) => (
                        <td
                          key={key}
                          className="px-4 py-3 border border-[#747070] max-w-[240px] whitespace-nowrap overflow-hidden text-ellipsis hover:whitespace-normal hover:overflow-visible first:text-left"
                        >
                          {row[key]}
                        </td>
                      ))}
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td
                      className="text-center py-6 text-slate-500"
                      colSpan={Math.max(columns.length, 1)}
                    >
                      No data to display.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {tableData.length > 0 && (
            <button
              onClick={handleDownload}
              className="mt-4 inline-flex items-center gap-2 rounded-md px-3 py-2 text-sm md:text-base font-bold bg-[#2c3e50] text-[#f8edcf] shadow hover:bg-[#34495e] active:translate-y-[1px]"
            >
              <i className="fa-solid fa-download fa-beat" />
              Download
            </button>
          )}
        </>
      )}

      {!loading && errMsg && (
        <div className="mt-3 text-sm md:text-base text-red-600">Error: {errMsg}</div>
      )}
    </div>
  );
}
