"use client";

import React from "react";
import Papa from "papaparse";
import * as XLSX from "xlsx";
import { FiDownload } from "react-icons/fi";
import { useUploadSkuMultiCountryMutation } from "@/lib/api/skuApi";
import DataTable, { Row as TableRow, ColumnDef } from "../table/DataTable";
import Button from "../button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

type Row = Record<string, string | number | null | undefined>;
type Props = { onClose: () => void; onComplete: () => void };

// Updated to match your new Excel format
const FIXED_COLUMNS = [
  "s_no",
  "product_name",
  "product_barcode",
  "asin",
  "country",
  "sku",
  "landing_cost",
  "currency",
  "local_stock",
] as const;

const HEADER_LABELS: Record<string, string> = {
  s_no: "S. No.",
  product_name: "Product Name",
  product_barcode: "Product Barcode",
  asin: "ASIN",
  country: "Country",
  sku: "SKU",
  landing_cost: "Landing Cost",
  currency: "Currency",
  local_stock: "Local Stock",
};

function toTitleCase(value: string) {
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function normalizeHeader(key: string): string {
  const raw = key.trim().toLowerCase().replace(/\s+/g, "_");

  const aliases: Record<string, string> = {
    "s._no.": "s_no",
    "s._no": "s_no",
    "s.no.": "s_no",
    "s.no": "s_no",
    "s_no": "s_no",
    "serial_no": "s_no",
    "serial_number": "s_no",

    product_name: "product_name",
    product: "product_name",

    product_barcode: "product_barcode",
    barcode: "product_barcode",

    asin: "asin",

    country: "country",

    sku: "sku",

    landing_cost: "landing_cost",
    cost: "landing_cost",

    currency: "currency",

    local_stock: "local_stock",
    stock: "local_stock",
  };

  return aliases[raw] || raw;
}

export default function SkuMultiCountryUpload({ onClose, onComplete }: Props) {
  const [error, setError] = React.useState<string>("");
  const [file, setFile] = React.useState<File | null>(null);
  const [fileName, setFileName] = React.useState<string>("No File Chosen");

  const [showConfirm, setShowConfirm] = React.useState<boolean>(false);
  const [columns, setColumns] = React.useState<ColumnDef<TableRow>[]>([]);
  const [rows, setRows] = React.useState<TableRow[]>([]);

  const [uploadSku, { isLoading: isUploading }] =
    useUploadSkuMultiCountryMutation();

  const resetPreview = React.useCallback(() => {
    setShowConfirm(false);
    setRows([]);
    setColumns([]);
  }, []);

  const buildColumns = React.useCallback((): ColumnDef<TableRow>[] => {
    return FIXED_COLUMNS.map((key) => {
      const header = HEADER_LABELS[key] || toTitleCase(key);
      if (key === "s_no") return { key, header, width: "64px" };
      if (key === "product_name") {
        return {
          key,
          header,
          width: "180px",
          headerClassName: "!text-left",
          cellClassName: "!text-left",
        };
      }
      return { key, header };
    });
  }, []);

  const validateHeadersFromRawRow = React.useCallback((row: Row) => {
    const normalizedHeaders = Object.keys(row).map((key) => normalizeHeader(key));

    const missingFixed = FIXED_COLUMNS.filter(
      (col) => !normalizedHeaders.includes(col)
    );

    if (missingFixed.length) {
      return `Missing required columns: ${missingFixed
        .map((col) => HEADER_LABELS[col] || toTitleCase(col))
        .join(", ")}`;
    }

    return "";
  }, []);

  const cleanParsedData = React.useCallback((data: unknown[]): Row[] => {
    if (!Array.isArray(data) || data.length === 0) return [];

    return (data as Row[])
      .filter(
        (row) => row && Object.values(row).some((v) => v !== "" && v != null)
      )
      .map((row, index) => {
        const out: Row = {};

        const allColumns = [...FIXED_COLUMNS];

        allColumns.forEach((col) => {
          out[col] = "";
        });

        Object.keys(row as object).forEach((k) => {
          const normalizedKey = normalizeHeader(k);
          let value = (row as Row)[k];

          if (typeof value === "string") value = value.trim();
          if (value === "undefined" || value === "NaN") value = "";

          if ((allColumns as string[]).includes(normalizedKey)) {
            out[normalizedKey] = value;
          }
        });

        if (!out.s_no) {
          out.s_no = index + 1;
        }

        return out;
      });
  }, []);

  const processParsedRows = React.useCallback(
    (rawRows: Row[]) => {
      if (!rawRows.length) {
        setError("The uploaded file is empty.");
        resetPreview();
        return;
      }

      const firstNonEmptyRow = rawRows.find(
        (row) => row && Object.values(row).some((v) => v !== "" && v != null)
      );

      if (!firstNonEmptyRow) {
        setError("The uploaded file is empty.");
        resetPreview();
        return;
      }

      const headerError = validateHeadersFromRawRow(firstNonEmptyRow);
      if (headerError) {
        setError(
          `Invalid file format. Please upload a file using the provided template. (${headerError})`
        );
        resetPreview();
        return;
      }

      const cleaned = cleanParsedData(rawRows);

      if (!cleaned.length) {
        setError("No valid rows found in the uploaded file.");
        resetPreview();
        return;
      }

      setRows(cleaned as TableRow[]);
      setColumns(buildColumns());
      setShowConfirm(true);
    },
    [buildColumns, cleanParsedData, resetPreview, validateHeadersFromRawRow]
  );

  const parseCSVFile = React.useCallback(
    (f: File) => {
      Papa.parse<Row>(f, {
        header: true,
        skipEmptyLines: true,
        error: (err) => {
          setError(`CSV parse error: ${err.message}`);
          resetPreview();
        },
        complete: (result) => {
          processParsedRows(result.data as Row[]);
        },
      });
    },
    [processParsedRows, resetPreview]
  );

  const parseXLSXFile = React.useCallback(
    (f: File) => {
      const reader = new FileReader();

      reader.onload = (e) => {
        try {
          const wb = XLSX.read(e.target?.result as ArrayBuffer, {
            type: "array",
            cellDates: true,
          });

          if (!wb.SheetNames.length) {
            setError("The uploaded Excel file has no sheets.");
            resetPreview();
            return;
          }

          const sheet = wb.Sheets[wb.SheetNames[0]];
          if (!sheet) {
            setError("Could not read the first worksheet.");
            resetPreview();
            return;
          }

          const json = XLSX.utils.sheet_to_json(sheet, {
            defval: "",
            raw: false,
          }) as Row[];

          processParsedRows(json);
        } catch (err: any) {
          setError(err?.message || "Failed to read the Excel file.");
          resetPreview();
        }
      };

      reader.onerror = () => {
        setError("Failed to read the selected file.");
        resetPreview();
      };

      reader.readAsArrayBuffer(f);
    },
    [processParsedRows, resetPreview]
  );

  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0] || null;

    setError("");
    resetPreview();

    if (!selected) {
      setFile(null);
      setFileName("No File Chosen");
      return;
    }

    const lowerName = selected.name.toLowerCase();
    const isValidType =
      selected.type === "application/vnd.ms-excel" ||
      selected.type ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      lowerName.endsWith(".csv") ||
      lowerName.endsWith(".xls") ||
      lowerName.endsWith(".xlsx");

    if (!isValidType) {
      setError("Invalid file type. Please upload a CSV or XLSX file.");
      setFile(null);
      setFileName("No File Chosen");
      return;
    }

    setFile(selected);
    setFileName(selected.name);

    if (lowerName.endsWith(".csv")) {
      parseCSVFile(selected);
    } else {
      parseXLSXFile(selected);
    }
  };

  const onDownloadTemplate = () => {
    const link = document.createElement("a");
    link.href = "/sku-information-template.xlsx";
    link.download = "SKU Information.xlsx";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const onConfirmUpload = async () => {
    if (!file) {
      setError("Please select a file first.");
      return;
    }

    setError("");

    try {
      // 👇 Transform long format into wide format for the backend
      const grouped = new Map();

      rows.forEach((row: any) => {
        const key = row.product_barcode || row.product_name;
        
        if (!grouped.has(key)) {
          grouped.set(key, {
            "S. No.": row.s_no,
            "Product Name": row.product_name,
            "Product Barcode": row.product_barcode,
            "ASIN": row.asin,
            "Landing Cost": row.landing_cost,
            "Currency": row.currency,
          });
        }
        
        const item = grouped.get(key);
        const country = String(row.country || "").toLowerCase();
        
        if (country) {
          item[`sku_${country}`] = row.sku;
          item[`asin_${country}`] = row.asin;
          item[`product_barcode_${country}`] = row.product_barcode;
          item[`local_stock_${country}`] = row.local_stock;
        }
      });

      const wideData = Array.from(grouped.values());
      const ws = XLSX.utils.json_to_sheet(wideData);
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "Sheet1");
      
      // Create the file object to upload
      const wbout = XLSX.write(wb, { bookType: "xlsx", type: "array" });
      const newFile = new File([wbout], file.name, {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });

      await uploadSku({ file: newFile }).unwrap();

      setError("");
      resetPreview();
      setFile(null);
      setFileName("No File Chosen");

      onComplete();
    } catch (e: any) {
      const msg =
        e?.data?.error || e?.data?.message || e?.error || "Upload failed.";
      setError(msg);
    }
  };

  return (
    <div className="w-full">
      {!showConfirm && (
        <div className="w-full max-w-[520px] mx-auto flex flex-col gap-3">
          <PageBreadcrumb
            pageTitle="Upload SKU Data"
            variant="table"
            align="center2"
          />

          <div className="rounded-2xl p-3">
            <div className="flex items-center gap-2 rounded-md border border-gray-300 bg-white px-2 py-1.5">
              <label
                htmlFor="sku-file"
                className="shrink-0 cursor-pointer rounded-md bg-gray-100 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-200"
              >
                Upload File
              </label>

              <input
                id="sku-file"
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={onFileChange}
                className="hidden"
              />

              <span className="block w-full truncate px-2 text-xs text-gray-500">
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

          {error && (
            <p className="mt-3 text-center text-sm text-red-600">{error}</p>
          )}
        </div>
      )}

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
              <PageBreadcrumb
                pageTitle="Confirm SKU Data"
                variant="table"
                align="center2"
              />
            </div>

            <div
              className="flex-1 min-w-0 w-full"
              style={{ "--table-sticky-top": "0px" } as React.CSSProperties}
            >
              <DataTable
                columns={columns}
                data={rows}
                stickyColumnKeys={["s_no", "product_name"]}
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

            {error && (
              <p className="mt-2 text-sm text-red-600 text-center">{error}</p>
            )}

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
                onClick={() => {
                  setError("");
                  resetPreview();
                  onClose();
                }}
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
