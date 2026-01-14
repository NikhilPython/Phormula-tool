"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx";

import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import { getISTYearMonth } from "@/lib/dashboard/date";

const baseURL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";
const INVENTORY_AGED_ENDPOINT = `${baseURL}/amazon_api/inventory/aged`;

/** Columns taken from your attached Excel, excluding the `id` column.
 *  We prepend a "Sno." column in the UI.
 */
const AGED_COLUMNS: string[] = [
  "snapshot-date",
  "sku",
  "fnsku",
  "asin",
  "product-name",
  "condition",
  "available",
  "pending-removal-quantity",
  "inv-age-0-to-90-days",
  "inv-age-91-to-180-days",
  "inv-age-181-to-270-days",
  "inv-age-271-to-365-days",
  "inv-age-365-plus-days",
  "currency",
  "units-shipped-t7",
  "units-shipped-t30",
  "units-shipped-t60",
  "units-shipped-t90",
  "alert",
  "your-price",
  "sales-price",
  "lowest-price-new-plus-shipping",
  "lowest-price-used",
  "recommended-action",
  "healthy-inventory-level",
  "recommended-sales-price",
  "recommended-sale-duration-days",
  "recommended-removal-quantity",
  "estimated-cost-savings-of-recommended-actions",
  "sell-through",
  "item-volume",
  "volume-unit-measurement",
  "storage-type",
  "storage-volume",
  "marketplace",
  "product-group",
  "sales-rank",
  "days-of-supply",
  "estimated-excess-quantity",
  "weeks-of-cover-t30",
  "weeks-of-cover-t90",
  "featuredoffer-price",
  "sales-shipped-last-7-days",
  "sales-shipped-last-30-days",
  "sales-shipped-last-60-days",
  "sales-shipped-last-90-days",
  "inv-age-0-to-30-days",
  "inv-age-31-to-60-days",
  "inv-age-61-to-90-days",
  "inv-age-181-to-330-days",
  "inv-age-331-to-365-days",
  "estimated-storage-cost-next-month",
  "inbound-quantity",
  "inbound-working",
  "inbound-shipped",
  "inbound-received",
  "Total Reserved Quantity",
  "unfulfillable-quantity",
  "quantity-to-be-charged-ais-241-270-days",
  "estimated-ais-241-270-days",
  "quantity-to-be-charged-ais-271-300-days",
  "estimated-ais-271-300-days",
  "quantity-to-be-charged-ais-301-330-days",
  "estimated-ais-301-330-days",
  "quantity-to-be-charged-ais-331-365-days",
  "estimated-ais-331-365-days",
  "quantity-to-be-charged-ais-365-plus-days",
  "estimated-ais-365-plus-days",
  "historical-days-of-supply",
  "Recommended ship-in quantity",
  "Recommended ship-in date",
  "Last updated date for Historical Days of Supply",
  "Short term historical days of supply",
  "Long term historical days of supply",
  "Inventory age snapshot date",
  "Inventory Supply at FBA",
  "Reserved FC Transfer",
  "Reserved FC Processing",
  "Reserved Customer Order",
  "Total Days of Supply (including units from open shipments)",
];

type AgedInventoryRow = Record<string, string | number>;

export default function AgeingInventorySection() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>("");
  const [rows, setRows] = useState<AgedInventoryRow[]>([]);

  const monthLabel = useMemo(() => {
    const { monthName, year } = getISTYearMonth();
    const shortMon = new Date(`${monthName} 1, ${year}`).toLocaleString(
      "en-US",
      {
        month: "short",
        timeZone: "Asia/Kolkata",
      }
    );
    return `${shortMon} '${String(year).slice(-2)}`;
  }, []);

  const fetchAgeingInventory = useCallback(async () => {
    const token =
      typeof window !== "undefined"
        ? localStorage.getItem("jwtToken")
        : null;

    if (!token) {
      setError("Authorization token is missing");
      setRows([]);
      return;
    }

    setLoading(true);
    setError("");

    try {
      const res = await fetch(INVENTORY_AGED_ENDPOINT, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/json",
        },
      });

      if (!res.ok) {
        const errJson = await res.json().catch(() => ({}));
        throw new Error(
          errJson?.error || "Failed to fetch aged inventory data"
        );
      }

      const json = await res.json();
      const fileData: string | undefined = json?.data;

      if (!fileData) {
        throw new Error(
          json?.message ||
            "Aged inventory endpoint did not return Excel data"
        );
      }

      // base64 → ArrayBuffer (same pattern as CurrentInventorySection)
      const byteCharacters = atob(fileData);
      const buffers: ArrayBuffer[] = [];
      for (
        let offset = 0;
        offset < byteCharacters.length;
        offset += 1024
      ) {
        const slice = byteCharacters.slice(offset, offset + 1024);
        const byteNumbers = new Array(slice.length);
        for (let i = 0; i < slice.length; i++) {
          byteNumbers[i] = slice.charCodeAt(i);
        }
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
        const jsonData = XLSX.utils.sheet_to_json<AgedInventoryRow>(sheet, {
          defval: "",
        });
        setRows(jsonData);
      };

      reader.readAsArrayBuffer(blob);
    } catch (e: any) {
      console.error("Error fetching aged inventory:", e);
      setError(e?.message || "Unknown error fetching aged inventory");
      setRows([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAgeingInventory();
  }, [fetchAgeingInventory]);

  return (
    <div className="mt-6 rounded-2xl border bg-white p-4 shadow-sm">
      {/* Header */}
      <div className="mb-3">
        <div className="flex items-baseline gap-2">
          <PageBreadcrumb
            pageTitle="Ageing Inventory -"
            variant="page"
            align="left"
          />
          <span className="text-[#5EA68E] text-lg font-semibold">
            {monthLabel}
          </span>
        </div>
        <p className="mt-1 text-sm text-charcoal-500">
          Aged inventory snapshot from Amazon (FBA Manage Inventory Health
          Report)
        </p>
      </div>

      {/* Content */}
      {loading ? (
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
      ) : error ? (
        <div className="rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
          {error}
        </div>
      ) : rows.length === 0 ? (
        <div className="text-sm text-gray-500">
          No ageing inventory data.
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg">
          {/* Wide table – lots of columns, so give plenty of min width */}
          <table className="min-w-[1600px] w-full border-collapse border border-gray-300">
            <thead>
              <tr>
                {/* Serial number column */}
                <th className="px-3 py-2 text-center text-sm font-semibold border border-gray-300 bg-[#5EA68E] text-[#f8edcf]">
                  Sno.
                </th>

                {AGED_COLUMNS.map((col) => (
                  <th
                    key={col}
                    className="px-3 py-2 text-center text-xs font-semibold border border-gray-300 bg-[#5EA68E] text-[#f8edcf]"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>

            <tbody>
              {rows.map((row, index) => (
                <tr key={index} className="bg-white">
                  {/* Serial number */}
                  <td className="px-3 py-2 text-center text-sm text-gray-800 border border-gray-300">
                    {index + 1}
                  </td>

                  {AGED_COLUMNS.map((col) => (
                    <td
                      key={col}
                      className="px-3 py-2 text-center text-xs text-gray-800 border border-gray-300"
                    >
                      {(() => {
                        const v = row[col];
                        if (v === null || v === undefined) return "";
                        if (typeof v === "number") {
                          return Number.isFinite(v)
                            ? v.toLocaleString()
                            : v;
                        }
                        return String(v);
                      })()}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
