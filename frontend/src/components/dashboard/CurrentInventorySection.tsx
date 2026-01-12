// "use client";

// import React, { useCallback, useEffect, useMemo, useState } from "react";
// import * as XLSX from "xlsx";
// import PageBreadcrumb from "@/components/common/PageBreadCrumb";
// import Loader from "@/components/loader/Loader";
// import type { RegionKey } from "@/lib/dashboard/types";
// import DataTable, { ColumnDef } from "../ui/table/DataTable";
// import DownloadIconButton from "../ui/button/DownloadIconButton";
// import { saveAs } from "file-saver";

// const baseURL =
//   process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// type InventoryRow = Record<string, string | number>;

// /** Row shape for the DataTable */
// type InventoryUiRow = {
//   sno: React.ReactNode;
//   productName?: React.ReactNode;
//   currentInventory?: React.ReactNode;
//   /** ✅ NEW: 180+ days inventory */
//   inventory180Plus?: React.ReactNode;

//   salesRank?: React.ReactNode;
//   estStorage?: React.ReactNode;
//   mtdSales?: React.ReactNode;
//   sales30?: React.ReactNode;
//   coverageMonths?: React.ReactNode;
//   alert?: React.ReactNode;

//   /** "normal" | "others" | "total" – for styling */
//   rowType?: "normal" | "others" | "total";
// } & Record<string, React.ReactNode>;

// type CurrentInventorySectionProps = {
//   region: RegionKey; // "Global" | "UK" | "US" | "CA"
// };


// /* ========= Shared helpers ========= */

// function getISTYearMonth() {
//   const optsMonth: Intl.DateTimeFormatOptions = {
//     timeZone: "Asia/Kolkata",
//     month: "long",
//   };
//   const optsYear: Intl.DateTimeFormatOptions = {
//     timeZone: "Asia/Kolkata",
//     year: "numeric",
//   };
//   const now = new Date();
//   const monthName = now.toLocaleString("en-US", optsMonth);
//   const yearStr = now.toLocaleString("en-US", optsYear);
//   return { monthName, year: Number(yearStr) };
// }

// const toNumberSafe = (v: any) => {
//   if (v === null || v === undefined) return 0;
//   if (typeof v === "number") return v;
//   const s = String(v).replace(/[, ]+/g, "");
//   const n = Number(s);
//   return isNaN(n) ? 0 : n;
// };

// const isInventoryTotalRow = (r: InventoryRow) => {
//   const name = String(r["Product Name"] ?? "").trim().toLowerCase();
//   const sku = String(r["SKU"] ?? "").trim().toLowerCase();

//   if (!name && !sku) return false;

//   return (
//     name === "total" ||
//     name === "grand total" ||
//     name.includes("total") ||
//     sku === "total" ||
//     sku === "grand total" ||
//     sku.includes("total")
//   );
// };

// const formatInt = (n: number | null | undefined) => {
//   const v = Number(n ?? 0);
//   if (!v) return "0";
//   return v.toLocaleString("en-IN", { maximumFractionDigits: 0 });
// };

// const formatRatio = (n: number | null | undefined) => {
//   const v = Number(n ?? 0);
//   if (!v || !Number.isFinite(v)) return "—";
//   return v.toFixed(1);
// };

// const normalizeSku = (v: any) =>
//   String(v || "")
//     .trim()
//     .toUpperCase();

// const normKey = (s: string) =>
//   String(s || "")
//     .toLowerCase()
//     .replace(/\s+/g, "")
//     .replace(/[_-]+/g, "");

// const getNumberByPossibleKeys = (row: InventoryRow, possible: string[]) => {
//   const wanted = possible.map(normKey);

//   const foundKey = Object.keys(row).find((k) => wanted.includes(normKey(k)));
//   return foundKey ? toNumberSafe(row[foundKey]) : 0;
// };


// /* ===================== COMPONENT ===================== */

// export default function CurrentInventorySection({
//   region,
// }: CurrentInventorySectionProps) {
//   const [invLoading, setInvLoading] = useState(false);
//   const [invError, setInvError] = useState<string>("");
//   const [invRows, setInvRows] = useState<InventoryRow[]>([]);
//   const [inventoryAlerts, setInventoryAlerts] = useState<Record<
//     string,
//     { alert?: string; alert_type?: string }
//   >>({});

//   // ✅ follow graphRegion, but send lowercase/“global” to backend
//   const inventoryCountry = useMemo(() => {
//     const v = (region || "").toString().trim().toLowerCase();
//     return v.length ? v : "global";
//   }, [region]);

//   // ✅ current month/year in IST
//   const invMonthYear = useMemo(() => {
//     const { monthName, year } = getISTYearMonth();
//     return { month: monthName.toLowerCase(), year: String(year) };
//   }, []);

//   const getCurrentInventoryEndpoint = useCallback(() => {
//     return inventoryCountry === "global"
//       ? `${baseURL}/current_inventory_global`
//       : `${baseURL}/current_inventory`;
//   }, [inventoryCountry]);

//   // backend month column: "Current Month Units Sold (MonthName)" (dynamic)
//   const findMtdKey = useCallback((row: InventoryRow) => {
//     const key = Object.keys(row).find((k) =>
//       k.toLowerCase().startsWith("current month units sold")
//     );
//     return key || "";
//   }, []);

//   // ✅ Sales for past 30 days: backend may call it "Others" or something else
//   const findSales30Key = useCallback((row: InventoryRow) => {
//     const keys = Object.keys(row);

//     const exactOthers = keys.find((k) => k.trim().toLowerCase() === "others");
//     if (exactOthers) return exactOthers;

//     const past30 = keys.find((k) => k.toLowerCase().includes("past 30"));
//     if (past30) return past30;

//     const days30 = keys.find((k) => k.toLowerCase().includes("30 days"));
//     if (days30) return days30;

//     const same = keys.find(
//       (k) => k.trim().toLowerCase() === "sales for past 30 days"
//     );
//     if (same) return same;

//     return "";
//   }, []);

//   // Single backend total row (if any)
//   const inventoryTotalRow = useMemo(
//     () => invRows.find((r) => isInventoryTotalRow(r)) || null,
//     [invRows]
//   );

//   // 🔁 Fetch inventory from backend
//   const fetchCurrentInventory = useCallback(async () => {
//     const token =
//       typeof window !== "undefined"
//         ? localStorage.getItem("jwtToken")
//         : null;

//     if (!token) {
//       setInvError("Authorization token is missing");
//       setInvRows([]);
//       return;
//     }

//     setInvLoading(true);
//     setInvError("");

//     try {
//       const endpoint = getCurrentInventoryEndpoint();
//       const { month, year } = invMonthYear;

//       const res = await fetch(endpoint, {
//         method: "POST",
//         headers: {
//           Authorization: `Bearer ${token}`,
//           "Content-Type": "application/json",
//         },
//         body: JSON.stringify({ month, year, country: inventoryCountry }),
//       });

//       if (!res.ok) {
//         const errJson = await res.json().catch(() => ({}));
//         throw new Error(errJson?.error || "Failed to fetch CurrentInventory data");
//       }

//       const json = await res.json();
//       const rawAlerts = json?.inventory_alerts || {};
//       const normalizedAlerts: Record<
//         string,
//         { alert?: string; alert_type?: string }
//       > = {};

//       Object.keys(rawAlerts).forEach((k) => {
//         normalizedAlerts[normalizeSku(k)] = rawAlerts[k];
//       });

//       setInventoryAlerts(normalizedAlerts);
//       const fileData: string | undefined = json?.data;
//       if (!fileData) throw new Error(json?.message || "Empty file received from server");

//       // Decode base64 → Blob → read via XLSX
//       const byteCharacters = atob(fileData);
//       const buffers: ArrayBuffer[] = [];
//       for (let offset = 0; offset < byteCharacters.length; offset += 1024) {
//         const slice = byteCharacters.slice(offset, offset + 1024);
//         const byteNumbers = new Array(slice.length);
//         for (let i = 0; i < slice.length; i++) byteNumbers[i] = slice.charCodeAt(i);
//         buffers.push(new Uint8Array(byteNumbers).buffer as ArrayBuffer);
//       }

//       const blob = new Blob(buffers, {
//         type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
//       });

//       const reader = new FileReader();
//       reader.onload = (e) => {
//         const arr = new Uint8Array(e.target?.result as ArrayBuffer);
//         const wb = XLSX.read(arr, { type: "array" });
//         const sheetName = wb.SheetNames[0];
//         const sheet = wb.Sheets[sheetName];
//         const jsonData = XLSX.utils.sheet_to_json<InventoryRow>(sheet, { defval: "" });
//         setInvRows(jsonData);
//       };

//       reader.readAsArrayBuffer(blob);
//     } catch (e: any) {
//       setInvError(e?.message || "Unknown error");
//       setInvRows([]);
//     } finally {
//       setInvLoading(false);
//     }
//   }, [getCurrentInventoryEndpoint, invMonthYear, inventoryCountry]);


//   const downloadInventoryExcel = useCallback(() => {
//     if (!invRows?.length) return;

//     const rowsToExport = invRows.filter((r) => {
//       const name = String(r["Product Name"] ?? "").trim();
//       const sku = String(r["SKU"] ?? "").trim();
//       if (!name && !sku) return false;
//       if (isInventoryTotalRow(r)) return false;
//       return true;
//     });


//     const wb = XLSX.utils.book_new();
//     const ws = XLSX.utils.json_to_sheet(rowsToExport, { skipHeader: false });

//     // Optional: make headers bold-ish by widening columns (xlsx doesn't style much without pro libs)
//     // Auto column widths
//     const headers = Object.keys(rowsToExport[0] || {});
//     ws["!cols"] = headers.map((h) => ({ wch: Math.min(Math.max(h.length + 2, 12), 40) }));

//     XLSX.utils.book_append_sheet(wb, ws, "Current Inventory");

//     const fileName = `Current-Inventory_${inventoryCountry.toUpperCase()}_${invMonthYear.month}_${invMonthYear.year}.xlsx`;

//     const out = XLSX.write(wb, { bookType: "xlsx", type: "array" });
//     const blob = new Blob([out], {
//       type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
//     });

//     saveAs(blob, fileName);
//   }, [invRows, inventoryCountry, invMonthYear]);


//   useEffect(() => {
//     fetchCurrentInventory();
//   }, [fetchCurrentInventory]);

//   /* -------- Transform backend rows → UI rows for DataTable -------- */

//   const tableRows: InventoryUiRow[] = useMemo(() => {
//     if (!invRows?.length) return [];

//     // 1) Filter out empty + backend total rows
//     const usable = invRows.filter((r) => {
//       const name = String(r["Product Name"] ?? "").trim();
//       const sku = String(r["SKU"] ?? "").trim();
//       const isEmpty = !name && !sku;
//       if (isEmpty) return false;
//       if (isInventoryTotalRow(r)) return false;
//       return true;
//     });





//     type CalcRow = {
//       index: number;
//       row: InventoryRow;
//       currentInventory: number;
//       mtdSales: number;
//       sales30: number;
//       coverage: number;
//       salesRank: number;
//       estStorage: number;
//       /** ✅ NEW: 180+ */
//       inventory180Plus: number;
//     };

//     // 2) Pre-calculate numeric metrics for each row
//     const calcRows: CalcRow[] = usable.map((r, idx) => {
//       const mtdKey = findMtdKey(r);
//       const sales30Key = findSales30Key(r);

//       const currentInventory = toNumberSafe(r["Inventory at the end of the month"]);
//       const mtdSales = toNumberSafe(mtdKey ? r[mtdKey] : 0);
//       const sales30 = toNumberSafe(sales30Key ? r[sales30Key] : 0);

//       const age181to270 = getNumberByPossibleKeys(r, [
//         "inv-age-181-to-270-days",
//         "inv_age_181_to_270_days",
//         "Inventory Age 181 to 270 Days",
//         "inv age 181 to 270 days",
//       ]);

//       const age271to365 = getNumberByPossibleKeys(r, [
//         "inv-age-271-to-365-days",
//         "inv_age_271_to_365_days",
//         "Inventory Age 271 to 365 Days",
//         "inv age 271 to 365 days",
//       ]);

//       const age365plus = getNumberByPossibleKeys(r, [
//         "inv-age-365-plus-days",
//         "inv_age_365_plus_days",
//         "Inventory Age 365+ Days",
//         "inv age 365 plus days",
//         "inv-age-365+-days",
//       ]);

//       const inventory180Plus = age181to270 + age271to365 + age365plus;

//       const denom = mtdSales + sales30;
//       const coverage = denom > 0 ? currentInventory / denom : 0;

//       const salesRank = toNumberSafe(r["sales-rank"]);
//       const estStorage = toNumberSafe(r["estimated-storage-cost-next-month"]);

//       return {
//         index: idx,
//         row: r,
//         currentInventory,
//         mtdSales,
//         sales30,
//         coverage,
//         salesRank,
//         estStorage,
//         inventory180Plus,
//       };
//     });

//     if (!calcRows.length) return [];

//     // 3) Top 5 by MTD Sales (desc)
//     const sortedByMtd = [...calcRows].sort((a, b) => b.mtdSales - a.mtdSales);
//     const top5 = sortedByMtd.slice(0, 5);
//     const top5Indices = new Set(top5.map((r) => r.index));

//     // 4) Others = all remaining rows
//     const othersRows = calcRows.filter((r) => !top5Indices.has(r.index));

//     // 5) Build UI rows for top 5 (keep sorted by MTD desc)
//     const uiRows: InventoryUiRow[] = top5.map((c, idx) => {
//       const {
//         row,
//         currentInventory,
//         mtdSales,
//         sales30,
//         coverage,
//         salesRank,
//         estStorage,
//         inventory180Plus,
//       } = c;

//       return {
//         rowType: "normal",
//         sno: idx + 1,
//         productName: row["Product Name"] || "",
//         currentInventory: formatInt(currentInventory),
//         inventory180Plus: formatInt(inventory180Plus),

//         salesRank: salesRank ? formatInt(salesRank) : "—",
//         estStorage: estStorage ? formatInt(estStorage) : "—",
//         mtdSales: formatInt(mtdSales),
//         sales30: formatInt(mtdSales + sales30),
//         coverageMonths: formatRatio(coverage),
//         alert: inventoryAlerts[normalizeSku(row["SKU"])]?.alert || "",

//       };
//     });

//     // 6) "Others" aggregate row
//     if (othersRows.length > 0) {
//       const agg = othersRows.reduce(
//         (acc, r) => {
//           acc.currentInventory += r.currentInventory;
//           acc.mtdSales += r.mtdSales;
//           acc.sales30 += r.sales30;
//           acc.salesRank += r.salesRank;
//           acc.estStorage += r.estStorage;
//           acc.inventory180Plus += r.inventory180Plus;
//           return acc;
//         },
//         {
//           currentInventory: 0,
//           mtdSales: 0,
//           sales30: 0,
//           salesRank: 0,
//           estStorage: 0,
//           inventory180Plus: 0,
//         }
//       );

//       const denom = agg.mtdSales + agg.sales30;
//       const coverage = denom > 0 ? agg.currentInventory / denom : 0;

//       uiRows.push({
//         rowType: "others",
//         sno: 6,
//         productName: "Others",
//         currentInventory: formatInt(agg.currentInventory),
//         inventory180Plus: formatInt(agg.inventory180Plus),

//         estStorage: formatInt(agg.estStorage),

//         mtdSales: formatInt(agg.mtdSales),
//         sales30: formatInt(agg.mtdSales + agg.sales30),
//         coverageMonths: formatRatio(coverage),
//         alert: "",
//       });
//     }

//     // 7) Backend grand total row
//     if (inventoryTotalRow) {
//       const mtdKey = findMtdKey(inventoryTotalRow);
//       const sales30Key = findSales30Key(inventoryTotalRow);

//       const currentInventory = toNumberSafe(
//         inventoryTotalRow["Inventory at the end of the month"]
//       );
//       const mtdSales = toNumberSafe(mtdKey ? inventoryTotalRow[mtdKey] : 0);
//       const sales30 = toNumberSafe(sales30Key ? inventoryTotalRow[sales30Key] : 0);

//       const age181to270 = toNumberSafe(inventoryTotalRow["inv-age-181-to-270-days"]);
//       const age271to365 = toNumberSafe(inventoryTotalRow["inv-age-271-to-365-days"]);
//       const age365plus = toNumberSafe(inventoryTotalRow["inv-age-365-plus-days"]);
//       const inventory180Plus = age181to270 + age271to365 + age365plus;

//       const denom = mtdSales + sales30;
//       const coverage = denom > 0 ? currentInventory / denom : 0;

//       uiRows.push({
//         rowType: "total",
//         sno: "",
//         productName: <span className="font-semibold">Total</span>,
//         currentInventory: <span className="font-semibold">{formatInt(currentInventory)}</span>,
//         inventory180Plus: <span className="font-semibold">{formatInt(inventory180Plus)}</span>,

//         salesRank: "",
//         estStorage: <span className="font-semibold">{formatInt(toNumberSafe(inventoryTotalRow["estimated-storage-cost-next-month"]))}</span>,
//         mtdSales: <span className="font-semibold">{formatInt(mtdSales)}</span>,
//         sales30: <span className="font-semibold">{formatInt(mtdSales + sales30)}</span>,
//         coverageMonths: <span className="font-semibold">{formatRatio(coverage)}</span>,
//         alert: "",
//       });
//     }

//     return uiRows;
//   }, [invRows, findMtdKey, findSales30Key, inventoryTotalRow]);

//   /* -------- Build DataTable columns -------- */

//   const columns: ColumnDef<InventoryUiRow>[] = useMemo(() => {
//     const cols: ColumnDef<InventoryUiRow>[] = [];

//     cols.push({
//       key: "sno",
//       header: "Sno.",
//       width: "60px",
//       cellClassName: "text-center",
//     });

//     cols.push({
//       key: "productName",
//       header: "Product Name",
//       cellClassName: "text-left",
//       headerClassName: "text-left",
//       // width: "220px",
//     });

//     cols.push({
//       key: "currentInventory",
//       header: "Current Inventory",
//       cellClassName: "text-center",
//     });

//     // ✅ Single ageing column
//     cols.push({
//       key: "inventory180Plus",
//       header: "Inventory 180+ Days",
//       cellClassName: "text-center",
//     });

//     cols.push(
//       {
//         key: "salesRank",
//         header: "Sales Rank",
//         cellClassName: "text-center",
//       },
//       {
//         key: "estStorage",
//         header: "Est Storage Cost Next Month",
//         cellClassName: "text-center",
//       },
//       {
//         key: "mtdSales",
//         header: "MTD Sales",
//         cellClassName: "text-center",
//       },
//       {
//         key: "sales30",
//         header: "Sales for past 30 days",
//         cellClassName: "text-center",
//       },
//       {
//         key: "coverageMonths",
//         header: "Inventory Coverage Ratio (In Months)",
//         cellClassName: "text-center",
//       },
//       {
//         key: "alert",
//         header: "Inventory Alerts",
//         cellClassName: "text-center",
//       }
//     );

//     return cols;
//   }, []);

//   return (
//     <div
//       className="
//         mt-4 rounded-2xl border bg-white p-4 shadow-sm
//         w-full max-w-full overflow-hidden
//         flex flex-col
//       "
//     >
//       {/* <div className="mb-3">
//         <div className="flex items-baseline gap-2">
//           <PageBreadcrumb pageTitle="Current Inventory" variant="page" align="left" />
//         </div>
//       </div> */}

//       <div className="mb-3 flex items-center justify-between">
//         <div className="flex items-baseline gap-2">
//           <PageBreadcrumb pageTitle="Current Inventory" variant="page" align="left" />
//         </div>

//         <DownloadIconButton
//           onClick={downloadInventoryExcel}
//           disabled={invLoading || !invRows?.length}
//         />
//       </div>


//       {invLoading ? (
//         <div className="py-10 flex justify-center">
//           <Loader
//             src="/infinity-unscreen.gif"
//             size={40}
//             transparent
//             roundedClass="rounded-full"
//             backgroundClass="bg-transparent"
//             respectReducedMotion
//           />
//         </div>
//       ) : invError ? (
//         <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
//           {invError}
//         </div>
//       ) : (
//         <div className="mt-2 flex-1 w-full max-w-full overflow-x-auto">
//           <div className="min-w-max [&_table]:w-auto">
//             <DataTable
//               columns={columns}
//               data={tableRows}
//               loading={false}
//               paginate={true}
//               pageSize={15}
//               scrollY={false}
//               maxHeight="none"
//               emptyMessage="No inventory data."
//               rowClassName={(row) => {
//                 if (row.rowType === "total") {
//                   return "bg-[#EFEFEF] font-semibold";
//                 }
//                 if (row.rowType === "others") {
//                   return "!bg-[#FFFFFF]";
//                 }
//                 return "bg-white";
//               }}
//             />
//           </div>
//         </div>
//       )}
//     </div>
//   );
// }













"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import type { RegionKey } from "@/lib/dashboard/types";
import DataTable, { ColumnDef } from "../ui/table/DataTable";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import { saveAs } from "file-saver";

const baseURL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

type InventoryRow = Record<string, string | number>;

/** Row shape for the DataTable */
type InventoryUiRow = {
  sno: React.ReactNode;
  productName?: React.ReactNode;
  currentInventory?: React.ReactNode;
  /** ✅ 180+ days inventory */
  inventory180Plus?: React.ReactNode;

  salesRank?: React.ReactNode;
  estStorage?: React.ReactNode;
  mtdSales?: React.ReactNode;
  sales30?: React.ReactNode;
  coverageMonths?: React.ReactNode;
  alert?: React.ReactNode;

  /** "normal" | "others" | "total" – for styling */
  rowType?: "normal" | "others" | "total";
} & Record<string, React.ReactNode>;

type CurrentInventorySectionProps = {
  region: RegionKey; // "Global" | "UK" | "US" | "CA"
};

/* ========= Shared helpers ========= */

function getISTYearMonth() {
  const optsMonth: Intl.DateTimeFormatOptions = {
    timeZone: "Asia/Kolkata",
    month: "long",
  };
  const optsYear: Intl.DateTimeFormatOptions = {
    timeZone: "Asia/Kolkata",
    year: "numeric",
  };
  const now = new Date();
  const monthName = now.toLocaleString("en-US", optsMonth);
  const yearStr = now.toLocaleString("en-US", optsYear);
  return { monthName, year: Number(yearStr) };
}

const toNumberSafe = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return v;
  const s = String(v).replace(/[, ]+/g, "");
  const n = Number(s);
  return isNaN(n) ? 0 : n;
};

const isInventoryTotalRow = (r: InventoryRow) => {
  const name = String(r["Product Name"] ?? "").trim().toLowerCase();
  const sku = String(r["SKU"] ?? "").trim().toLowerCase();

  if (!name && !sku) return false;

  return (
    name === "total" ||
    name === "grand total" ||
    name.includes("total") ||
    sku === "total" ||
    sku === "grand total" ||
    sku.includes("total")
  );
};

const formatInt = (n: number | null | undefined) => {
  const v = Number(n ?? 0);
  if (!v) return "0";
  return v.toLocaleString("en-IN", { maximumFractionDigits: 0 });
};

const formatRatio = (n: number | null | undefined) => {
  const v = Number(n ?? 0);
  if (!v || !Number.isFinite(v)) return "—";
  return v.toFixed(1);
};

const normalizeSku = (v: any) =>
  String(v || "")
    .trim()
    .toUpperCase();

const normKey = (s: string) =>
  String(s || "")
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[_-]+/g, "");

/** Find a numeric value in a row given multiple possible key spellings */
const getNumberByPossibleKeys = (row: InventoryRow, possible: string[]) => {
  const wanted = possible.map(normKey);
  const foundKey = Object.keys(row).find((k) => wanted.includes(normKey(k)));
  return foundKey ? toNumberSafe(row[foundKey]) : 0;
};

/* ========= Export Header Cleanup ========= */

/**
 * 1) remove synced_at
 * 2) rename known columns to proper names
 * 3) strip "-" everywhere for unknown keys (fallback)
 */
const HEADER_RENAME_MAP: Record<string, string | null> = {
  synced_at: null, // ❌ remove

  available: "Available Inventory",

  "inv-age-0-to-90-days": "Inventory Age 0 to 90 Days",
  "inv-age-91-to-180-days": "Inventory Age 91 to 180 Days",
  "inv-age-181-to-270-days": "Inventory Age 181 to 270 Days",
  "inv-age-271-to-365-days": "Inventory Age 271 to 365 Days",
  "inv-age-365-plus-days": "Inventory Age 365 Plus Days",

  "sales-rank": "Sales Rank",
  "estimated-storage-cost-next-month": "Estimated Storage Cost Next Month",
};

const toTitleCase = (s: string) =>
  String(s || "")
    .replace(/[_-]+/g, " ") // strip "-" and "_" globally
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());

/** Normalizes backend rows into clean export rows */
const normalizeExportRows = (rows: InventoryRow[]) => {
  return rows.map((row) => {
    const cleanRow: Record<string, any> = {};

    Object.entries(row).forEach(([key, value]) => {
      const normalizedKey = key.trim().toLowerCase();

      // ❌ remove synced_at (and any other null-mapped keys)
      if (normalizedKey === "synced_at") return;

      const mapped = HEADER_RENAME_MAP[normalizedKey];

      // if explicitly removed by mapping
      if (mapped === null) return;

      // rename if mapping exists, else fallback to cleaned Title Case
      const newKey = mapped ?? toTitleCase(normalizedKey);

      cleanRow[newKey] = value;
    });

    return cleanRow;
  });
};

/* ===================== COMPONENT ===================== */

export default function CurrentInventorySection({
  region,
}: CurrentInventorySectionProps) {
  const [invLoading, setInvLoading] = useState(false);
  const [invError, setInvError] = useState<string>("");
  const [invRows, setInvRows] = useState<InventoryRow[]>([]);
  const [inventoryAlerts, setInventoryAlerts] = useState<
    Record<string, { alert?: string; alert_type?: string }>
  >({});

  // ✅ follow graphRegion, but send lowercase/“global” to backend
  const inventoryCountry = useMemo(() => {
    const v = (region || "").toString().trim().toLowerCase();
    return v.length ? v : "global";
  }, [region]);

  // ✅ current month/year in IST
  const invMonthYear = useMemo(() => {
    const { monthName, year } = getISTYearMonth();
    return { month: monthName.toLowerCase(), year: String(year) };
  }, []);

  const getCurrentInventoryEndpoint = useCallback(() => {
    return inventoryCountry === "global"
      ? `${baseURL}/current_inventory_global`
      : `${baseURL}/current_inventory`;
  }, [inventoryCountry]);

  // backend month column: "Current Month Units Sold (MonthName)" (dynamic)
  const findMtdKey = useCallback((row: InventoryRow) => {
    const key = Object.keys(row).find((k) =>
      k.toLowerCase().startsWith("current month units sold")
    );
    return key || "";
  }, []);

  // ✅ Sales for past 30 days: backend may call it "Others" or something else
  const findSales30Key = useCallback((row: InventoryRow) => {
    const keys = Object.keys(row);

    const exactOthers = keys.find((k) => k.trim().toLowerCase() === "others");
    if (exactOthers) return exactOthers;

    const past30 = keys.find((k) => k.toLowerCase().includes("past 30"));
    if (past30) return past30;

    const days30 = keys.find((k) => k.toLowerCase().includes("30 days"));
    if (days30) return days30;

    const same = keys.find(
      (k) => k.trim().toLowerCase() === "sales for past 30 days"
    );
    if (same) return same;

    return "";
  }, []);

  // Single backend total row (if any)
  const inventoryTotalRow = useMemo(
    () => invRows.find((r) => isInventoryTotalRow(r)) || null,
    [invRows]
  );

  // 🔁 Fetch inventory from backend
  const fetchCurrentInventory = useCallback(async () => {
    const token =
      typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    if (!token) {
      setInvError("Authorization token is missing");
      setInvRows([]);
      return;
    }

    setInvLoading(true);
    setInvError("");

    try {
      const endpoint = getCurrentInventoryEndpoint();
      const { month, year } = invMonthYear;

      const res = await fetch(endpoint, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ month, year, country: inventoryCountry }),
      });

      if (!res.ok) {
        const errJson = await res.json().catch(() => ({}));
        throw new Error(
          errJson?.error || "Failed to fetch CurrentInventory data"
        );
      }

      const json = await res.json();

      // alerts
      const rawAlerts = json?.inventory_alerts || {};
      const normalizedAlerts: Record<string, { alert?: string; alert_type?: string }> =
        {};
      Object.keys(rawAlerts).forEach((k) => {
        normalizedAlerts[normalizeSku(k)] = rawAlerts[k];
      });
      setInventoryAlerts(normalizedAlerts);

      // file
      const fileData: string | undefined = json?.data;
      if (!fileData)
        throw new Error(json?.message || "Empty file received from server");

      // Decode base64 → Blob → read via XLSX
      const byteCharacters = atob(fileData);
      const buffers: ArrayBuffer[] = [];
      for (let offset = 0; offset < byteCharacters.length; offset += 1024) {
        const slice = byteCharacters.slice(offset, offset + 1024);
        const byteNumbers = new Array(slice.length);
        for (let i = 0; i < slice.length; i++)
          byteNumbers[i] = slice.charCodeAt(i);
        buffers.push(new Uint8Array(byteNumbers).buffer as ArrayBuffer);
      }

      const blob = new Blob(buffers, {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });

      const reader = new FileReader();
      reader.onload = (e) => {
        const arr = new Uint8Array(e.target?.result as ArrayBuffer);
        const wb = XLSX.read(arr, { type: "array" });
        const sheetName = wb.SheetNames[0];
        const sheet = wb.Sheets[sheetName];
        const jsonData = XLSX.utils.sheet_to_json<InventoryRow>(sheet, {
          defval: "",
        });
        setInvRows(jsonData);
      };

      reader.readAsArrayBuffer(blob);
    } catch (e: any) {
      setInvError(e?.message || "Unknown error");
      setInvRows([]);
    } finally {
      setInvLoading(false);
    }
  }, [getCurrentInventoryEndpoint, invMonthYear, inventoryCountry]);

  useEffect(() => {
    fetchCurrentInventory();
  }, [fetchCurrentInventory]);

  /* ===================== DOWNLOAD EXCEL (FULL DATA) ===================== */

  const downloadInventoryExcel = useCallback(() => {
    if (!invRows?.length) return;

    // export ALL rows except empty rows + backend totals
    const rowsToExport = invRows.filter((r) => {
      const name = String(r["Product Name"] ?? "").trim();
      const sku = String(r["SKU"] ?? "").trim();
      if (!name && !sku) return false;
      if (isInventoryTotalRow(r)) return false;
      return true;
    });

    if (!rowsToExport.length) return;

    // ✅ normalize headers: remove synced_at, rename, strip hyphens
    const cleanedRows = normalizeExportRows(rowsToExport);

    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet(cleanedRows, { skipHeader: false });

    // auto column widths
    const headers = Object.keys(cleanedRows[0] || {});
    ws["!cols"] = headers.map((h) => ({
      wch: Math.min(Math.max(h.length + 2, 12), 48),
    }));

    XLSX.utils.book_append_sheet(wb, ws, "Current Inventory");

    const fileName = `Current-Inventory_${inventoryCountry.toUpperCase()}_${invMonthYear.month}_${invMonthYear.year}.xlsx`;

    const out = XLSX.write(wb, { bookType: "xlsx", type: "array" });
    const blob = new Blob([out], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });

    saveAs(blob, fileName);
  }, [invRows, inventoryCountry, invMonthYear]);

  /* -------- Transform backend rows → UI rows for DataTable -------- */

  const tableRows: InventoryUiRow[] = useMemo(() => {
    if (!invRows?.length) return [];

    // 1) Filter out empty + backend total rows
    const usable = invRows.filter((r) => {
      const name = String(r["Product Name"] ?? "").trim();
      const sku = String(r["SKU"] ?? "").trim();
      const isEmpty = !name && !sku;
      if (isEmpty) return false;
      if (isInventoryTotalRow(r)) return false;
      return true;
    });

    type CalcRow = {
      index: number;
      row: InventoryRow;
      currentInventory: number;
      mtdSales: number;
      sales30: number;
      coverage: number;
      salesRank: number;
      estStorage: number;
      inventory180Plus: number;
    };

    // 2) Pre-calculate numeric metrics for each row
    const calcRows: CalcRow[] = usable.map((r, idx) => {
      const mtdKey = findMtdKey(r);
      const sales30Key = findSales30Key(r);

      const currentInventory = toNumberSafe(r["Inventory at the end of the month"]);
      const mtdSales = toNumberSafe(mtdKey ? r[mtdKey] : 0);
      const sales30 = toNumberSafe(sales30Key ? r[sales30Key] : 0);

      const age181to270 = getNumberByPossibleKeys(r, [
        "inv-age-181-to-270-days",
        "inv_age_181_to_270_days",
        "Inventory Age 181 to 270 Days",
        "inv age 181 to 270 days",
      ]);

      const age271to365 = getNumberByPossibleKeys(r, [
        "inv-age-271-to-365-days",
        "inv_age_271_to_365_days",
        "Inventory Age 271 to 365 Days",
        "inv age 271 to 365 days",
      ]);

      const age365plus = getNumberByPossibleKeys(r, [
        "inv-age-365-plus-days",
        "inv_age_365_plus_days",
        "Inventory Age 365+ Days",
        "inv age 365 plus days",
        "inv-age-365+-days",
      ]);

      const inventory180Plus = age181to270 + age271to365 + age365plus;

      const denom = mtdSales + sales30;
      const coverage = denom > 0 ? currentInventory / denom : 0;

      const salesRank = toNumberSafe(r["sales-rank"]);
      const estStorage = toNumberSafe(r["estimated-storage-cost-next-month"]);

      return {
        index: idx,
        row: r,
        currentInventory,
        mtdSales,
        sales30,
        coverage,
        salesRank,
        estStorage,
        inventory180Plus,
      };
    });

    if (!calcRows.length) return [];

    // 3) Top 5 by MTD Sales (desc)
    const sortedByMtd = [...calcRows].sort((a, b) => b.mtdSales - a.mtdSales);
    const top5 = sortedByMtd.slice(0, 5);
    const top5Indices = new Set(top5.map((r) => r.index));

    // 4) Others = all remaining rows
    const othersRows = calcRows.filter((r) => !top5Indices.has(r.index));

    // 5) Build UI rows for top 5
    const uiRows: InventoryUiRow[] = top5.map((c, idx) => {
      const {
        row,
        currentInventory,
        mtdSales,
        sales30,
        coverage,
        salesRank,
        estStorage,
        inventory180Plus,
      } = c;

      return {
        rowType: "normal",
        sno: idx + 1,
        productName: row["Product Name"] || "",
        currentInventory: formatInt(currentInventory),
        inventory180Plus: formatInt(inventory180Plus),

        salesRank: salesRank ? formatInt(salesRank) : "—",
        estStorage: estStorage ? formatInt(estStorage) : "—",
        mtdSales: formatInt(mtdSales),
        sales30: formatInt(mtdSales + sales30),
        coverageMonths: formatRatio(coverage),
        alert: inventoryAlerts[normalizeSku(row["SKU"])]?.alert || "",
      };
    });

    // 6) "Others" aggregate row
    if (othersRows.length > 0) {
      const agg = othersRows.reduce(
        (acc, r) => {
          acc.currentInventory += r.currentInventory;
          acc.mtdSales += r.mtdSales;
          acc.sales30 += r.sales30;
          acc.salesRank += r.salesRank;
          acc.estStorage += r.estStorage;
          acc.inventory180Plus += r.inventory180Plus;
          return acc;
        },
        {
          currentInventory: 0,
          mtdSales: 0,
          sales30: 0,
          salesRank: 0,
          estStorage: 0,
          inventory180Plus: 0,
        }
      );

      const denom = agg.mtdSales + agg.sales30;
      const coverage = denom > 0 ? agg.currentInventory / denom : 0;

      uiRows.push({
        rowType: "others",
        sno: 6,
        productName: "Others",
        currentInventory: formatInt(agg.currentInventory),
        inventory180Plus: formatInt(agg.inventory180Plus),
        estStorage: formatInt(agg.estStorage),
        mtdSales: formatInt(agg.mtdSales),
        sales30: formatInt(agg.mtdSales + agg.sales30),
        coverageMonths: formatRatio(coverage),
        alert: "",
      });
    }

    // 7) Backend grand total row
    const inventoryTotalRow = invRows.find((r) => isInventoryTotalRow(r)) || null;
    if (inventoryTotalRow) {
      const mtdKey = findMtdKey(inventoryTotalRow);
      const sales30Key = findSales30Key(inventoryTotalRow);

      const currentInventory = toNumberSafe(
        inventoryTotalRow["Inventory at the end of the month"]
      );
      const mtdSales = toNumberSafe(mtdKey ? inventoryTotalRow[mtdKey] : 0);
      const sales30 = toNumberSafe(sales30Key ? inventoryTotalRow[sales30Key] : 0);

      const age181to270 = toNumberSafe(inventoryTotalRow["inv-age-181-to-270-days"]);
      const age271to365 = toNumberSafe(inventoryTotalRow["inv-age-271-to-365-days"]);
      const age365plus = toNumberSafe(inventoryTotalRow["inv-age-365-plus-days"]);
      const inventory180Plus = age181to270 + age271to365 + age365plus;

      const denom = mtdSales + sales30;
      const coverage = denom > 0 ? currentInventory / denom : 0;

      uiRows.push({
        rowType: "total",
        sno: "",
        productName: <span className="font-semibold">Total</span>,
        currentInventory: (
          <span className="font-semibold">{formatInt(currentInventory)}</span>
        ),
        inventory180Plus: (
          <span className="font-semibold">{formatInt(inventory180Plus)}</span>
        ),
        salesRank: "",
        estStorage: (
          <span className="font-semibold">
            {formatInt(toNumberSafe(inventoryTotalRow["estimated-storage-cost-next-month"]))}
          </span>
        ),
        mtdSales: <span className="font-semibold">{formatInt(mtdSales)}</span>,
        sales30: (
          <span className="font-semibold">{formatInt(mtdSales + sales30)}</span>
        ),
        coverageMonths: (
          <span className="font-semibold">{formatRatio(coverage)}</span>
        ),
        alert: "",
      });
    }

    return uiRows;
  }, [invRows, findMtdKey, findSales30Key, inventoryAlerts]);

  /* -------- Build DataTable columns -------- */

  const columns: ColumnDef<InventoryUiRow>[] = useMemo(() => {
    const cols: ColumnDef<InventoryUiRow>[] = [];

    cols.push({
      key: "sno",
      header: "Sno.",
      width: "60px",
      cellClassName: "text-center",
    });

    cols.push({
      key: "productName",
      header: "Product Name",
      cellClassName: "text-left",
      headerClassName: "text-left",
    });

    cols.push({
      key: "currentInventory",
      header: "Current Inventory",
      cellClassName: "text-center",
    });

    cols.push({
      key: "inventory180Plus",
      header: "Inventory 180+ Days",
      cellClassName: "text-center",
    });

    cols.push(
      {
        key: "salesRank",
        header: "Sales Rank",
        cellClassName: "text-center",
      },
      {
        key: "estStorage",
        header: "Est Storage Cost Next Month",
        cellClassName: "text-center",
      },
      {
        key: "mtdSales",
        header: "MTD Sales",
        cellClassName: "text-center",
      },
      {
        key: "sales30",
        header: "Sales for past 30 days",
        cellClassName: "text-center",
      },
      {
        key: "coverageMonths",
        header: "Inventory Coverage Ratio (In Months)",
        cellClassName: "text-center",
      },
      {
        key: "alert",
        header: "Inventory Alerts",
        cellClassName: "text-center",
      }
    );

    return cols;
  }, []);

  return (
    <div
      className="
        mt-4 rounded-2xl border bg-white p-4 shadow-sm
        w-full max-w-full overflow-hidden
        flex flex-col
      "
    >
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-baseline gap-2">
          <PageBreadcrumb pageTitle="Current Inventory" variant="page" align="left" />
        </div>

        <DownloadIconButton
          onClick={downloadInventoryExcel}
          disabled={invLoading || !invRows?.length}
        />
      </div>

      {invLoading ? (
        <div className="py-10 flex justify-center">
          <Loader
            src="/infinity-unscreen.gif"
            size={40}
            transparent
            roundedClass="rounded-full"
            backgroundClass="bg-transparent"
            respectReducedMotion
          />
        </div>
      ) : invError ? (
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
          {invError}
        </div>
      ) : (
        <div className="mt-2 flex-1 w-full max-w-full overflow-x-auto">
          <div className="min-w-max [&_table]:w-auto">
            <DataTable
              columns={columns}
              data={tableRows}
              loading={false}
              paginate={true}
              pageSize={15}
              scrollY={false}
              maxHeight="none"
              emptyMessage="No inventory data."
              rowClassName={(row) => {
                if (row.rowType === "total") return "bg-[#EFEFEF] font-semibold";
                if (row.rowType === "others") return "!bg-[#FFFFFF]";
                return "bg-white";
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
}
