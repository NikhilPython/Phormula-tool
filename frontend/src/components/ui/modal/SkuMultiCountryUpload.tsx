
"use client";

import React from "react";
import Papa, { type ParseResult } from "papaparse";
import * as XLSX from "xlsx";
import { FiDownload } from "react-icons/fi";
import { useUploadSkuMultiCountryMutation } from "@/lib/api/skuApi";
import DataTable, { Row as TableRow, ColumnDef } from "../table/DataTable";
import Button from "../button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

type Row = Record<string, string | number | null | undefined>;
type Props = { onClose: () => void; onComplete: () => void };

function pad2(n: number) {
  return String(n).padStart(2, "0");
}

function formatToMMYYYY(value: unknown): string | number | null | undefined {
  // Keep empties as-is
  if (value == null || value === "") return value as any;

  // If already a Date
  if (value instanceof Date && !isNaN(value.getTime())) {
    return `${pad2(value.getMonth() + 1)}/${value.getFullYear()}`;
  }

  // If XLSX gave a number (Excel serial) - normalize to date
  // Excel's epoch is 1899-12-30 (with the 1900 leap-year bug baked in).
  if (typeof value === "number" && isFinite(value)) {
    const dt = XLSX.SSF.parse_date_code(value);
    if (dt && dt.y && dt.m) {
      return `${pad2(dt.m)}/${dt.y}`;
    }
    return value; // fallback
  }

  // If string, normalize common patterns:
  // "MM/YYYY", "M/YYYY", "MM-YYYY", "YYYY-MM", "YYYY/MM", "YYYY-MM-DD"
  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return "";

    // already MM/YYYY
    let m = s.match(/^(\d{1,2})[\/\-](\d{4})$/);
    if (m) {
      const mm = Math.max(1, Math.min(12, parseInt(m[1], 10)));
      const yyyy = parseInt(m[2], 10);
      return `${pad2(mm)}/${yyyy}`;
    }

    // YYYY-MM or YYYY/MM
    m = s.match(/^(\d{4})[\/\-](\d{1,2})$/);
    if (m) {
      const yyyy = parseInt(m[1], 10);
      const mm = Math.max(1, Math.min(12, parseInt(m[2], 10)));
      return `${pad2(mm)}/${yyyy}`;
    }

    // YYYY-MM-DD / YYYY/MM/DD
    m = s.match(/^(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})$/);
    if (m) {
      const yyyy = parseInt(m[1], 10);
      const mm = Math.max(1, Math.min(12, parseInt(m[2], 10)));
      return `${pad2(mm)}/${yyyy}`;
    }

    return s; // leave other strings unchanged
  }

  return value as any;
}

export default function SkuMultiCountryUpload({ onClose, onComplete }: Props) {
  const [error, setError] = React.useState<string>("");
  const [file, setFile] = React.useState<File | null>(null);
  const [fileName, setFileName] = React.useState<string>("No File Chosen");

  const [showConfirm, setShowConfirm] = React.useState<boolean>(false);
  const [columns, setColumns] = React.useState<ColumnDef<TableRow>[]>([]);
  const [rows, setRows] = React.useState<TableRow[]>([]);

  const [uploadSku, { isLoading: isUploading }] = useUploadSkuMultiCountryMutation();

  // ---------- helpers ----------
  const cleanParsedData = React.useCallback((data: unknown[]): Row[] => {
    if (!Array.isArray(data) || data.length === 0) return [];

    return (data as Row[])
      .filter((row) => row && Object.values(row).some((v) => v !== "" && v != null))
      .map((row) => {
        const out: Row = {};
        Object.keys(row as object).forEach((k) => {
          const normalizedKey = k.trim().toLowerCase().replace(/\s+/g, "_");
          let value = (row as Row)[k];

          // trim strings
          if (typeof value === "string") value = value.trim();
          if (value === "undefined" || value === "NaN") value = "";

          // ✅ Normalize single date column if present (key could be date / month_year / mm_yyyy etc.)
          // You said you want one column "MM/YYYY" — this ensures display + payload is consistent.
          if (
            normalizedKey === "date" ||
            normalizedKey === "month_year" ||
            normalizedKey === "mm/yyyy" ||
            normalizedKey === "mm_yyyy" ||
            normalizedKey === "month/year"
          ) {
            value = formatToMMYYYY(value) as any;
          }

          out[normalizedKey] = value;
        });
        return out;
      });
  }, []);

  const buildColumns = (r: Row[]): ColumnDef<TableRow>[] => {
    const keys = r.length ? Object.keys(r[0]) : [];
    return keys.map((k) => ({
      key: k,
      header: k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()),
    }));
  };

  const parseCSVFile = (f: File) => {
    Papa.parse<Row>(f, {
      complete: (result) => {
        const cleaned = cleanParsedData(result.data as unknown[]);
        setRows(cleaned as TableRow[]);
        setColumns(buildColumns(cleaned));
        setShowConfirm(true);
      },
      header: true,
      skipEmptyLines: true,
    });
  };

  const parseXLSXFile = (f: File) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const wb = XLSX.read(e.target?.result as ArrayBuffer, {
        type: "array",
        cellDates: true, // ✅ helps keep dates as Date where possible
      });

      const sheet = wb.Sheets[wb.SheetNames[0]];

      // ✅ IMPORTANT:
      // raw:false => apply Excel number/date formatting instead of returning serial numbers
      // dateNF => if a cell is recognized as a date, output in mm/yyyy
      const json = XLSX.utils.sheet_to_json(sheet, {
        defval: "",
        raw: false,
        dateNF: "mm/yyyy",
      }) as unknown[];

      const cleaned = cleanParsedData(json);
      setRows(cleaned as TableRow[]);
      setColumns(buildColumns(cleaned));
      setShowConfirm(true);
    };
    reader.readAsArrayBuffer(f);
  };

  // ---------- events ----------
  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0] || null;
    setError("");
    setShowConfirm(false);
    setRows([]);
    setColumns([]);

    if (!selected) {
      setFile(null);
      setFileName("No File Chosen");
      return;
    }

    const isValidType =
      selected.type === "application/vnd.ms-excel" ||
      selected.type === "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      selected.name.toLowerCase().endsWith(".csv");

    if (!isValidType) {
      setError("Invalid file type. Please upload a CSV or XLSX file.");
      setFile(null);
      setFileName("No File Chosen");
      return;
    }

    setFile(selected);
    setFileName(selected.name);

    if (selected.name.toLowerCase().endsWith(".csv")) parseCSVFile(selected);
    else parseXLSXFile(selected);
  };

  const onDownloadTemplate = () => {
    const a = document.createElement("a");
    a.href = `/SKU%20Information%20global%20file.xlsx`;
    a.download = "SKU Information Global file format.xlsx";
    document.body.appendChild(a);
    a.click();
    a.remove();
  };

  // FRONTEND: only required change (prevents false "Upload failed" + better error visibility)
  // Replace ONLY your onConfirmUpload with this:

  const onConfirmUpload = async () => {
    if (!file) return setError("Please select a file first.");

    setError(""); // ✅ clear any old error before uploading

    try {
      const res = await uploadSku({ file }).unwrap();
      console.log("Upload success:", res);

      setError(""); // ✅ clear on success

      // reset internal UI state
      setShowConfirm(false);
      setRows([]);
      setColumns([]);
      setFile(null);
      setFileName("No File Chosen");

      onComplete();
    } catch (e: any) {
      console.log("Upload failed (RTK error):", e);
      const msg =
        e?.data?.error ||
        e?.data?.message ||
        e?.error ||        // RTK Query parse/cors errors often appear here
        "Upload failed.";
      setError(msg);
    }
  };


  // ---------- UI ----------
  return (
    <div className="w-full ">
      {/* Step 1: uploader */}
      {!showConfirm && (
        <div className="w-full max-w-[520px] mx-auto flex flex-col gap-3">
          <PageBreadcrumb pageTitle="Upload SKU Data" variant="table" align="center2" />

          <div className="rounded-2xl p-3 ">
            <div className="flex items-center gap-2 rounded-md border border-gray-300 bg-white px-2 py-1.5">
              <label
                htmlFor="sku-file"
                className="shrink-0 cursor-pointer rounded-md bg-gray-100 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-200 "
              >
                Upload File
              </label>
              <input
                id="sku-file"
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={onFileChange}
                className="hidden "
              />
              <span className="block w-full truncate px-2 text-xs text-gray-500 ">
                {fileName}
              </span>
            </div>

            <button
              type="button"
              onClick={onDownloadTemplate}
              className="mx-auto mt-6 flex items-center gap-1 text-[13px] font-medium text-[#5EA68E] hover:text-[#4a907a]"
            >
              Download format here <FiDownload className="relative top-[1px]" />
            </button>
          </div>

          {error && <p className="mt-3 text-center text-sm text-red-600">{error}</p>}
        </div>
      )}

      {/* Step 2: confirmation modal with reusable DataTable */}
      {/* {showConfirm && (
        <div className="fixed inset-0 z-[1000] flex items-center justify-center p-4">
          <div
            className="w- max-w-4xl rounded-xl bg-white p-5 shadow-[6px_6px_7px_0px_#00000026]  border border-[#D9D9D9]"
            onClick={(e) => e.stopPropagation()}
          >
            <PageBreadcrumb pageTitle="Confirm SKU Data" variant="table" />
            
            <DataTable
              columns={columns}
              data={rows}
              pageSize={10}
              maxHeight="60vh"
              stickyHeader
              zebra
              emptyMessage="No parsed rows."
              className="my-4 "
            />

            {error && <p className="mt-2 text-sm text-red-600 text-center">{error}</p>}

            <div className="mt-4 flex justify-center gap-3">
              <Button onClick={onConfirmUpload} disabled={isUploading || !file} size="sm" variant="primary">
                {isUploading ? "Uploading…" : "Confirm & Upload"}
              </Button>

              <Button onClick={() => setShowConfirm(false)} disabled={isUploading} size="sm" variant="outline">
                Cancel
              </Button>
            </div>
          </div>
        </div>
      )} */}

      {showConfirm && (
        <div className="fixed inset-0 z-[1000] flex items-center justify-center p-2 sm:p-4">
          <div
            className="
        w-full max-w-4xl
        rounded-xl bg-white p-3 sm:p-5
        shadow-[6px_6px_7px_0px_#00000026]
        border border-[#D9D9D9]
        flex flex-col
        min-w-0
      "
            onClick={(e) => e.stopPropagation()}
          >
            <div className="mb-3 sm:mb-4 shrink-0">
              <PageBreadcrumb pageTitle="Confirm SKU Data" variant="table" />
            </div>

            {/* ✅ IMPORTANT: min-w-0 allows DataTable to shrink (prevents 768px blank/clipping) */}
            <div className="flex-1 min-w-0 w-full">
              <DataTable
                columns={columns}
                data={rows}
                pageSize={10}
                maxHeight="60vh"
                stickyHeader
                zebra
                emptyMessage="No parsed rows."
                className="my-4 w-full max-w-full min-w-0"
                tableClassName="
    [&_th]:whitespace-nowrap
    [&_td]:whitespace-nowrap
    [&_th]:overflow-hidden
    [&_th]:text-ellipsis
    [&_td]:overflow-hidden
    [&_td]:text-ellipsis
  "
              />

            </div>

            {error && <p className="mt-2 text-sm text-red-600 text-center">{error}</p>}

            <div className="mt-4 flex justify-center gap-3 shrink-0">
              <Button
                onClick={onConfirmUpload}
                disabled={isUploading || !file}
                size="sm"
                variant="primary"
              >
                {isUploading ? "Uploading…" : "Confirm & Upload"}
              </Button>

              <Button
                onClick={() => setShowConfirm(false)}
                disabled={isUploading}
                size="sm"
                variant="outline"
              >
                Cancel
              </Button>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}
