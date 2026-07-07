"use client";

import React from "react";
import { useRouter } from "next/navigation";
import * as XLSX from "xlsx";
import {
  useGetFeePreviewConfirmationTextQuery,
  useUploadFeePreviewMutation,
} from "@/lib/api/feePreviewApi";
import Modalmsg from "./Modalmsg";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import DataTable, { ColumnDef } from "../table/DataTable";
import Button from "../button/Button";

type Props = {
  country: string;
  marketplace: string;
  file: File | null;
  transitTime: string;
  stockUnit: string;
  onBack?: () => void;
};

type Cell = string | number | null | undefined;
type RowArr = Cell[];
type TableData = RowArr[];

// normalize header names
const norm = (s: unknown): string =>
  String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/_/g, "-");

const countryMap: Record<string, string> = {
  CA: "Canada",
  SG: "Singapore",
  IN: "India",
  GB: "United Kingdom",
  UK: "United Kingdom",
  US: "United States",
  Canada: "CA",
  Singapore: "SG",
  India: "IN",
  "United Kingdom": "GB",
  "United States": "US",
};

export default function ConfirmationFeepreview({
  country,
  marketplace,
  file,
  transitTime,
  stockUnit,
  onBack: onBackProp
}: Props) {
  const router = useRouter();
  const { data: confirmText } = useGetFeePreviewConfirmationTextQuery();
  const [uploadFeePreview, { isLoading: isUploading }] = useUploadFeePreviewMutation();

  const [error, setError] = React.useState<string>("");
  const [tableData, setTableData] = React.useState<TableData>([]);
  const [showModal, setShowModal] = React.useState<boolean>(false);
  const [modalMessage, setModalMessage] = React.useState<string>("");

  React.useEffect(() => {
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e: ProgressEvent<FileReader>) => {
      try {
        const result = e.target?.result;
        if (!(result instanceof ArrayBuffer)) {
          setError("Failed to read file data.");
          return;
        }

        const data = new Uint8Array(result);
        const wb = XLSX.read(data, { type: "array" });
        const first = wb.SheetNames[0];
        const ws = wb.Sheets[first];
        const json = XLSX.utils.sheet_to_json(ws, { header: 1 }) as TableData;

        if (!json.length) {
          setError("Uploaded sheet is empty.");
          return;
        }

        const headers = (json[0] ?? []) as RowArr;
        const normalized = headers.map(norm);

        const aliases = ["amazon-store", "amazon store", "store", "marketplace"].map(norm);
        const storeIdx = normalized.findIndex((h) => aliases.includes(h));
        if (storeIdx === -1) {
          setError('Store/country column not found (expected something like "amazon-store").');
          return;
        }

        const upper = String(country || "").toUpperCase();
        const mappedCountry = countryMap[upper] || countryMap[country] || country || "";

        const body = json.slice(1);
        const filtered = body.filter((row) => {
          const cellUp = String(row[storeIdx] ?? "").toUpperCase();
          const rowMap = countryMap[cellUp] || cellUp;
          return rowMap.toUpperCase() === String(mappedCountry).toUpperCase();
        });

        if (!filtered.length) {
          setError(
            "No rows matched the selected country in the Fee Preview file. Please verify your file and country."
          );
          return;
        }

        // Pad rows to same length
        const maxCols = Math.max(...filtered.map((r) => r.length));
        const padded = filtered.map((r) =>
          r.length < maxCols ? [...r, ...Array(maxCols - r.length).fill("")] : r
        );

        setTableData([headers, ...padded]);
        setError("");
      } catch (err) {
        console.error("Failed to parse Excel:", err);
        setError("Failed to parse the Excel file. Please check the format (.xlsx/.xls).");
      }
    };

    reader.readAsArrayBuffer(file);
  }, [file, country]);

  const onUpload = async () => {
    if (!country) return setError("Country is required.");
    if (!marketplace) return setError("Marketplace is required.");
    if (!file) return setError("Please choose a file.");
    if (tableData.length <= 1) {
      return setError("No rows to upload after filtering. Please verify your file and country.");
    }

    const t = Number.parseInt(String(transitTime), 10);
    const s = Number.parseInt(String(stockUnit), 10);
    if (!Number.isFinite(t) || t <= 0) return setError("Enter a valid Transit Time (months).");
    if (!Number.isFinite(s) || s <= 0) return setError("Enter a valid Stock Keeping Unit (months).");

    try {
      // Build a workbook from filtered rows and turn into a File
      const wb = XLSX.utils.book_new();
      const filteredSheet = XLSX.utils.aoa_to_sheet(tableData);
      XLSX.utils.book_append_sheet(wb, filteredSheet, "Filtered Data");

      const base64 = XLSX.write(wb, { bookType: "xlsx", type: "base64" });
      const bin = window.atob(base64);
      const buf = new ArrayBuffer(bin.length);
      const view = new Uint8Array(buf);
      for (let i = 0; i < bin.length; i++) view[i] = bin.charCodeAt(i);

      const blob = new Blob([buf], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      const filteredFile = new File([blob], "filtered_data.xlsx", {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });

      const res = await uploadFeePreview({
        country,
        marketplace,
        file: filteredFile,
        transit_time: t,
        stock_unit: s,
      }).unwrap();

      setModalMessage("File uploaded successfully.");
      setShowModal(true);

      if (res?.profile_id) localStorage.setItem("profileId", String(res.profile_id));
      if (res?.country) localStorage.setItem("country", String(res.country));
      localStorage.setItem("transitTime", String(t));
      localStorage.setItem("stockUnit", String(s));
    } catch (e) {
      console.error(e);
      setError("Upload failed. Please try again.");
    }
  };

  const onBack = () => {
    if (onBackProp) return onBackProp();        // ✅ use parent's callback
    if (typeof window !== "undefined" && window.history.length > 1) {
      router.back();                            // fallback if used via routing
    } else {
      router.push(`/pnl-dashboard/QTD/${country}/NA/NA`);
    }
  };


  /** ---------- Build DataTable columns + rows from AOA ---------- */
  const columns = React.useMemo<ColumnDef<Record<string, React.ReactNode>>[]>(() => {
    if (tableData.length === 0) return [];
    const headers = tableData[0];
    return headers.map((h, i) => ({
      key: (norm(h) || `col-${i}`) as string,
      header: String(h || `Column ${i + 1}`),
      // Optional per-column width or classes:
      // width: i === 0 ? "220px" : undefined,
      // headerClassName: "capitalize",
      // cellClassName: "truncate",
    }));
  }, [tableData]);

  const dataRows = React.useMemo<Record<string, React.ReactNode>[]>(() => {
    if (tableData.length <= 1) return [];
    const headerKeys = columns.map((c) => String(c.key));
    return tableData.slice(1).map((row) => {
      const obj: Record<string, React.ReactNode> = {};
      headerKeys.forEach((k, i) => {
        obj[k] = row[i] ?? "\u00A0";
      });
      return obj;
    });
  }, [tableData, columns]);

  return (
    <div className="w-full">
      <div className="mx-auto max-w-[95vw]">
        <PageBreadcrumb pageTitle="Amazon Fee Preview Information" variant="table" />

        {/* ✅ DataTable replaces the manual <table> */}
        <div className="mt-5">
          <DataTable
            columns={columns}
            data={dataRows}
            maxHeight="60vh"
            stickyHeader
            zebra
            tableClassName="text-sm"
            emptyMessage="No rows after filtering for the selected country."
          
          />
        </div>

        {error && <p className="mt-3 text-sm text-red-600">{error}</p>}

        <div className="mx-auto mt-6 flex w-full max-w-md items-center justify-center gap-3">

          <div className=" flex items-center justify-end gap-3 ">
            <button
              type="button"
              onClick={onBack}
              disabled={isUploading}
              className="inline-flex justify-center rounded-lg bg-gray-200 px-4 py-2 text-sm font-medium text-gray-800 hover:bg-gray-300 dark:bg-white/10 dark:text-gray-200 dark:hover:bg-white/15"
            >
              Back
            </button>
            <button
              type="button"
              onClick={onUpload}
              disabled={isUploading}
              className="inline-flex justify-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95 disabled:opacity-60"
            >
              {isUploading ? "Uploading…" : "Upload"}
            </button>
          </div>
        </div>
      </div>

      <Modalmsg
        show={showModal}
        message={modalMessage}
        onClose={() => {
          setShowModal(false);
          router.push(`/pnl-dashboard/QTD/${country}/NA/NA`);
        }}
      />
    </div>
  );
}





























// "use client";

// import React from "react";
// import { useRouter } from "next/navigation";
// import * as XLSX from "xlsx";
// import {
//   useGetFeePreviewConfirmationTextQuery,
//   useUploadFeePreviewMutation,
// } from "@/lib/api/feePreviewApi";
// import Modalmsg from "./Modalmsg";
// import PageBreadcrumb from "@/components/common/PageBreadCrumb";
// import DataTable, { ColumnDef } from "../table/DataTable";
// import Button from "../button/Button";

// type Props = {
//   country: string;
//   marketplace: string;
//   file: File | null;
//   transitTime: string;
//   stockUnit: string;
//   onBack?: () => void;
// };

// type Cell = string | number | null | undefined;
// type RowArr = Cell[];
// type TableData = RowArr[];

// const norm = (s: unknown): string =>
//   String(s ?? "")
//     .trim()
//     .toLowerCase()
//     .replace(/\s+/g, "-")
//     .replace(/_/g, "-");

// const countryMap: Record<string, string> = {
//   CA: "Canada",
//   SG: "Singapore",
//   IN: "India",
//   GB: "United Kingdom",
//   UK: "United Kingdom",
//   US: "United States",
//   Canada: "CA",
//   Singapore: "SG",
//   India: "IN",
//   "United Kingdom": "GB",
//   "United States": "US",
// };

// export default function ConfirmationFeepreview({
//   country,
//   marketplace,
//   file,
//   transitTime,
//   stockUnit,
//   onBack: onBackProp,
// }: Props) {
//   const router = useRouter();
//   const { data: confirmText } = useGetFeePreviewConfirmationTextQuery();
//   const [uploadFeePreview, { isLoading: isUploading }] =
//     useUploadFeePreviewMutation();

//   const [error, setError] = React.useState<string>("");
//   const [tableData, setTableData] = React.useState<TableData>([]);
//   const [showModal, setShowModal] = React.useState<boolean>(false);
//   const [modalMessage, setModalMessage] = React.useState<string>("");

//   React.useEffect(() => {
//     if (!file) return;

//     const reader = new FileReader();
//     reader.onload = (e: ProgressEvent<FileReader>) => {
//       try {
//         const result = e.target?.result;
//         if (!(result instanceof ArrayBuffer)) {
//           setError("Failed to read file data.");
//           return;
//         }

//         const data = new Uint8Array(result);
//         const wb = XLSX.read(data, { type: "array" });
//         const first = wb.SheetNames[0];
//         const ws = wb.Sheets[first];
//         const json = XLSX.utils.sheet_to_json(ws, { header: 1 }) as TableData;

//         if (!json.length) {
//           setError("Uploaded sheet is empty.");
//           return;
//         }

//         const headers = (json[0] ?? []) as RowArr;
//         const normalized = headers.map(norm);

//         const aliases = ["amazon-store", "amazon store", "store", "marketplace"].map(norm);
//         const storeIdx = normalized.findIndex((h) => aliases.includes(h));
//         if (storeIdx === -1) {
//           setError(
//             'Store/country column not found (expected something like "amazon-store").'
//           );
//           return;
//         }

//         const upper = String(country || "").toUpperCase();
//         const mappedCountry =
//           countryMap[upper] || countryMap[country] || country || "";

//         const body = json.slice(1);
//         const filtered = body.filter((row) => {
//           const cellUp = String(row[storeIdx] ?? "").toUpperCase();
//           const rowMap = countryMap[cellUp] || cellUp;
//           return rowMap.toUpperCase() === String(mappedCountry).toUpperCase();
//         });

//         if (!filtered.length) {
//           setError(
//             "No rows matched the selected country in the Fee Preview file. Please verify your file and country."
//           );
//           return;
//         }

//         // Pad rows to same length
//         const maxCols = Math.max(...filtered.map((r) => r.length));
//         const padded = filtered.map((r) =>
//           r.length < maxCols
//             ? [...r, ...Array(maxCols - r.length).fill("")]
//             : r
//         );

//         setTableData([headers, ...padded]);
//         setError("");
//       } catch (err) {
//         console.error("Failed to parse Excel:", err);
//         setError(
//           "Failed to parse the Excel file. Please check the format (.xlsx/.xls)."
//         );
//       }
//     };

//     reader.readAsArrayBuffer(file);
//   }, [file, country]);

//   const onUpload = async () => {
//     if (!country) return setError("Country is required.");
//     if (!marketplace) return setError("Marketplace is required.");
//     if (!file) return setError("Please choose a file.");
//     if (tableData.length <= 1) {
//       return setError(
//         "No rows to upload after filtering. Please verify your file and country."
//       );
//     }

//     const t = Number.parseInt(String(transitTime), 10);
//     const s = Number.parseInt(String(stockUnit), 10);
//     if (!Number.isFinite(t) || t <= 0)
//       return setError("Enter a valid Transit Time (months).");
//     if (!Number.isFinite(s) || s <= 0)
//       return setError("Enter a valid Stock Keeping Unit (months).");

//     try {
//       // Build a workbook from filtered rows and turn into a File
//       const wb = XLSX.utils.book_new();
//       const filteredSheet = XLSX.utils.aoa_to_sheet(tableData);
//       XLSX.utils.book_append_sheet(wb, filteredSheet, "Filtered Data");

//       const base64 = XLSX.write(wb, { bookType: "xlsx", type: "base64" });
//       const bin = window.atob(base64);
//       const buf = new ArrayBuffer(bin.length);
//       const view = new Uint8Array(buf);
//       for (let i = 0; i < bin.length; i++) view[i] = bin.charCodeAt(i);

//       const blob = new Blob([buf], {
//         type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
//       });
//       const filteredFile = new File([blob], "filtered_data.xlsx", {
//         type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
//       });

//       const res = await uploadFeePreview({
//         country,
//         marketplace,
//         file: filteredFile,
//         transit_time: t,
//         stock_unit: s,
//       }).unwrap();

//       setModalMessage("File uploaded successfully.");
//       setShowModal(true);

//       if (res?.profile_id)
//         localStorage.setItem("profileId", String(res.profile_id));
//       if (res?.country)
//         localStorage.setItem("country", String(res.country));
//       localStorage.setItem("transitTime", String(t));
//       localStorage.setItem("stockUnit", String(s));
//     } catch (e) {
//       console.error(e);
//       setError("Upload failed. Please try again.");
//     }
//   };

//   const onBack = () => {
//     if (onBackProp) return onBackProp();
//     if (typeof window !== "undefined" && window.history.length > 1) {
//       router.back();
//     } else {
//       router.push(`/country/QTD/${country}/NA/NA`);
//     }
//   };

//   const columns = React.useMemo<
//     ColumnDef<Record<string, React.ReactNode>>[]
//   >(() => {
//     if (tableData.length === 0) return [];
//     const headers = tableData[0];
//     return headers.map((h, i) => ({
//       key: (norm(h) || `col-${i}`) as string,
//       header: String(h || `Column ${i + 1}`),
//     }));
//   }, [tableData]);

//   const dataRows = React.useMemo<Record<string, React.ReactNode>[]>(() => {
//     if (tableData.length <= 1) return [];
//     const headerKeys = columns.map((c) => String(c.key));
//     return tableData.slice(1).map((row) => {
//       const obj: Record<string, React.ReactNode> = {};
//       headerKeys.forEach((k, i) => {
//         obj[k] = row[i] ?? "\u00A0";
//       });
//       return obj;
//     });
//   }, [tableData, columns]);

//   return (
//     <div className="w-full">
//       <div className="mx-auto max-w-[95vw]">
//         <PageBreadcrumb
//           pageTitle="Amazon Fee Preview Information"
//           variant="table"
//         />

//         <div className="mt-2">
//           <DataTable
//             columns={columns}
//             data={dataRows}
//             maxHeight="60vh"
//             stickyHeader
//             zebra
//             tableClassName="text-sm"
//             emptyMessage="No rows after filtering for the selected country."
//           />
//         </div>

//         {error && <p className="mt-3 text-sm text-red-600">{error}</p>}

//         <div className="mx-auto mt-6 flex w-full max-w-md items-center justify-center gap-3">
//           <Button
//             onClick={onUpload}
//             disabled={isUploading}
//             type="submit"
//             variant="primary"
//             size="sm"
//           >
//             {isUploading ? "Uploading…" : "Upload"}
//           </Button>

//           <Button onClick={onBack} disabled={isUploading} variant="outline" size="sm">
//             Back
//           </Button>
//         </div>
//       </div>

//       <Modalmsg
//         show={showModal}
//         message={modalMessage}
//         onClose={() => {
//           setShowModal(false);
//           router.push(`/country/QTD/${country}/NA/NA`);
//         }}
//       />
//     </div>
//   );
// }
