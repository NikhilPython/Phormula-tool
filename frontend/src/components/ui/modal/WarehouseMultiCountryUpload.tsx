"use client";

import React from "react";
import * as XLSX from "xlsx";
import { FiDownload } from "react-icons/fi";
import DataTable, { Row as TableRow, ColumnDef } from "../table/DataTable";
import Button from "../button/Button";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

type Row = Record<string, string | number | null | undefined>;

type Props = {
    onClose: () => void;
    onComplete: () => void;
    countryName: string;
};

const FIXED_COLUMNS = [
    "s_no",
    "sku_us",
    "sku_uk",
    "local_stock",
    "in_transit_units",
    "month",
    "year",
] as const;

const HEADER_LABELS: Record<string, string> = {
    s_no: "S No",
    sku_us: "SKU_US",
    sku_uk: "SKU_UK",
    local_stock: "Local Stock",
    in_transit_units: "In Transit Units",
    month: "Month",
    year: "Year",
};

function toTitleCase(value: string) {
    return value
        .replace(/_/g, " ")
        .replace(/\b\w/g, (c) => c.toUpperCase());
}

function normalizeHeader(key: string): string {
    const raw = key.trim().toLowerCase().replace(/\s+/g, "_");

    const aliases: Record<string, string> = {
        "s.no": "s_no",
        "s.no.": "s_no",
        "s._no": "s_no",
        "s._no.": "s_no",
        s_no: "s_no",
        serial_no: "s_no",
        serial_number: "s_no",

        sku_us: "sku_us",
        "sku_(us)": "sku_us",
        "sku-us": "sku_us",
        "sku us": "sku_us",

        sku_uk: "sku_uk",
        "sku_(uk)": "sku_uk",
        "sku-uk": "sku_uk",
        "sku uk": "sku_uk",

        stock: "local_stock",
        local_stock: "local_stock",
        "local stock": "local_stock",

        in_transit: "in_transit_units",
        transit_units: "in_transit_units",
        in_transit_units: "in_transit_units",
        "in transit units": "in_transit_units",

        month: "month",
        year: "year",
    };

    return aliases[raw] || raw;
}

export default function WarehouseMultiCountryUpload({
    onClose,
    onComplete,
    countryName,
}: Props) {
    const [error, setError] = React.useState("");
    const [file, setFile] = React.useState<File | null>(null);
    const [fileName, setFileName] = React.useState("No File Chosen");
    const [showConfirm, setShowConfirm] = React.useState(false);
    const [columns, setColumns] = React.useState<ColumnDef<TableRow>[]>([]);
    const [rows, setRows] = React.useState<TableRow[]>([]);
    const [isUploading, setIsUploading] = React.useState(false);

    const resetPreview = React.useCallback(() => {
        setShowConfirm(false);
        setRows([]);
        setColumns([]);
    }, []);

    const buildColumns = React.useCallback((): ColumnDef<TableRow>[] => {
        return FIXED_COLUMNS.map((key) => ({
            key,
            header: HEADER_LABELS[key] || toTitleCase(key),
        }));
    }, []);

    const validateHeadersFromRawRow = React.useCallback((row: Row) => {
        const normalizedHeaders = Object.keys(row).map((key) => normalizeHeader(key));

        const missing = FIXED_COLUMNS.filter((col) => !normalizedHeaders.includes(col));

        if (missing.length) {
            return "Invalid file format. Please upload a file using the provided template.";
        }

        return "";
    }, []);

    const cleanParsedData = React.useCallback((data: Row[]): Row[] => {
        if (!Array.isArray(data) || data.length === 0) return [];

        return data
            .filter((row) => row && Object.values(row).some((v) => v !== "" && v != null))
            .map((row, index) => {
                const out: Row = {
                    s_no: "",
                    sku_us: "",
                    sku_uk: "",
                    local_stock: "",
                    in_transit_units: "",
                    month: "",
                    year: "",
                };

                Object.keys(row).forEach((k) => {
                    const normalizedKey = normalizeHeader(k);
                    let value = row[k];

                    if (typeof value === "string") value = value.trim();
                    if (value === "undefined" || value === "NaN") value = "";

                    if ((FIXED_COLUMNS as readonly string[]).includes(normalizedKey)) {
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
                setError(headerError);
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

    const parseXLSXFile = React.useCallback(
        (selectedFile: File) => {
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

            reader.readAsArrayBuffer(selectedFile);
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
            lowerName.endsWith(".xls") ||
            lowerName.endsWith(".xlsx");

        if (!isValidType) {
            setError("Invalid file type. Please upload an XLS or XLSX file.");
            setFile(null);
            setFileName("No File Chosen");
            return;
        }

        setFile(selected);
        setFileName(selected.name);
        parseXLSXFile(selected);
    };

    const onDownloadTemplate = () => {
        const link = document.createElement("a");
        link.href = "/warehouse-information-template.xlsx";
        link.download = "Warehouse Information.xlsx";
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    };

    const onConfirmUpload = async () => {
        if (!file) {
            setError("Please select a file first.");
            return;
        }

        const token =
            typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

        if (!token) {
            setError("Authorization token is missing.");
            return;
        }

        try {
            setIsUploading(true);
            setError("");

            const formData = new FormData();
            formData.append("file", file);
            formData.append("country", countryName);

            const response = await fetch(
                `${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData`,
                {
                    method: "POST",
                    headers: {
                        Authorization: `Bearer ${token}`,
                    },
                    body: formData,
                }
            );

            const result = await response.json().catch(() => ({}));

            if (!response.ok) {
                throw new Error(
                    result?.error || result?.message || "Failed to upload warehouse file."
                );
            }

            setError("");
            resetPreview();
            setFile(null);
            setFileName("No File Chosen");
            onComplete();
        } catch (e: any) {
            setError(e?.message || "Upload failed.");
        } finally {
            setIsUploading(false);
        }
    };

    return (
        <div className="w-full">
            {!showConfirm && (
                <div className="w-full max-w-[520px] mx-auto flex flex-col gap-3">
                    <PageBreadcrumb
                        pageTitle="Upload Warehouse Data"
                        variant="table"
                        align="center2"
                    />

                    <div className="rounded-2xl p-3">
                        <div className="flex items-center gap-2 rounded-md border border-gray-300 bg-white px-2 py-1.5">
                            <label
                                htmlFor="warehouse-file"
                                className="shrink-0 cursor-pointer rounded-md bg-gray-100 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-200"
                            >
                                Upload File
                            </label>

                            <input
                                id="warehouse-file"
                                type="file"
                                accept=".xlsx,.xls"
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
                        className="w-full max-w-4xl rounded-xl bg-white p-3 sm:p-5 shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9] flex flex-col min-w-0"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <div className="mb-3 sm:mb-4 shrink-0">
                            <PageBreadcrumb
                                pageTitle="Confirm Warehouse Data"
                                variant="table"
                                align="center2"
                            />
                        </div>

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
                                {isUploading ? "Uploading..." : "Confirm & Upload"}
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