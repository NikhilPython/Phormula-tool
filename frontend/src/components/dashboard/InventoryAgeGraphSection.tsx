// components/dashboard/InventoryAgeGraphSection.tsx

"use client";

import React, { useMemo, useState } from "react";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import type { InventoryRow } from "@/lib/inventory/fetchCurrentInventoryData";
import InventoryAgeStackedBarChart from "@/components/dashboard/InventoryAgeStackedBarChart";
import PageBreadcrumb from "../common/PageBreadCrumb";

type Props = {
    invRows: InventoryRow[];
    region: string;
    selectedCountry?: "uk" | "us";
    showAllInventoryRows?: boolean;
};

const toNumberSafe = (v: any) => {
    if (v === null || v === undefined) return 0;
    if (typeof v === "number") return v;
    const s = String(v).replace(/[, ]+/g, "");
    const n = Number(s);
    return isNaN(n) ? 0 : n;
};

const normalizeSku = (v: any) => String(v || "").trim().toUpperCase();

const normKey = (s: string) =>
    String(s || "")
        .toLowerCase()
        .replace(/\s+/g, "")
        .replace(/[_-]+/g, "");

const getNumberByPossibleKeys = (row: InventoryRow, possible: string[]) => {
    const wanted = possible.map(normKey);
    const foundKey = Object.keys(row).find((k) => wanted.includes(normKey(k)));
    return foundKey ? toNumberSafe(row[foundKey]) : 0;
};

const isInventoryTotalRow = (row: InventoryRow) => {
    const name = String(row["Product Name"] ?? "").trim().toLowerCase();
    const sku = String(row["SKU"] ?? "").trim().toLowerCase();

    return (
        name === "total" ||
        name === "grand total" ||
        name.includes("total") ||
        sku === "total" ||
        sku === "grand total" ||
        sku.includes("total")
    );
};

export default function InventoryAgeGraphSection({
    invRows,
    region,
    selectedCountry = "uk",
    showAllInventoryRows,
}: Props) {
    const isGlobalInventory = region === "Global";
    const [showAllGraphSkus, setShowAllGraphSkus] = useState(false);

    const visibleInvRows = useMemo(() => {
        if (!Array.isArray(invRows)) return [];

        if (!isGlobalInventory) return invRows;

        return invRows.filter((row) => {
            const rowCountry = String(
                (row as any).country || (row as any).Country || ""
            ).toLowerCase();

            return rowCountry === selectedCountry;
        });
    }, [invRows, isGlobalInventory, selectedCountry]);

    const inventoryAgeChartData = useMemo(() => {
        if (!visibleInvRows?.length) return [];

        return visibleInvRows
            .filter((row) => {
                const name = String(row["Product Name"] ?? "").trim();
                const sku = String(row["SKU"] ?? "").trim();

                if (!name && !sku) return false;
                if (isInventoryTotalRow(row)) return false;

                return true;
            })
            .map((row) => {
                const sku = normalizeSku((row as any)["SKU"]);

                const age0to90 = getNumberByPossibleKeys(row, [
                    "inv-age-0-to-90-days",
                    "inv_age_0_to_90_days",
                    "Inventory Age 0 to 90 Days",
                    "inv age 0 to 90 days",
                ]);

                const age91to180 = getNumberByPossibleKeys(row, [
                    "inv-age-91-to-180-days",
                    "inv_age_91_to_180_days",
                    "Inventory Age 91 to 180 Days",
                    "inv age 91 to 180 days",
                ]);

                const age181to270 = getNumberByPossibleKeys(row, [
                    "inv-age-181-to-270-days",
                    "inv_age_181_to_270_days",
                    "Inventory Age 181 to 270 Days",
                    "inv age 181 to 270 days",
                ]);

                const age271to365 = getNumberByPossibleKeys(row, [
                    "inv-age-271-to-365-days",
                    "inv_age_271_to_365_days",
                    "Inventory Age 271 to 365 Days",
                    "inv age 271 to 365 days",
                ]);

                const age365plus = getNumberByPossibleKeys(row, [
                    "inv-age-365-plus-days",
                    "inv_age_365_plus_days",
                    "Inventory Age 365+ Days",
                    "inv age 365 plus days",
                    "inv-age-365+-days",
                ]);

                const total =
                    age0to90 +
                    age91to180 +
                    age181to270 +
                    age271to365 +
                    age365plus;

                return {
                    sku,
                    productName: String((row as any)["Product Name"] || sku),
                    age0to90,
                    age91to180,
                    age181to270,
                    age271to365,
                    age365plus,
                    total,
                };
            })
            .filter((row) => row.sku && row.total > 0)
            .sort((a, b) => b.total - a.total)
            .slice(0, showAllGraphSkus ? undefined : 10);
    }, [visibleInvRows, showAllGraphSkus]);

    if (!inventoryAgeChartData.length) return null;

    return (
        <div className="mt-4 w-full rounded-2xl border bg-white p-4 shadow-sm">
            <div className="mb-3 flex items-start justify-between gap-3">
                <div>
                    <PageBreadcrumb
                        pageTitle="Inventory Age by SKU"
                        variant="page"
                        textSize="2xl"
                    />
                </div>

                {visibleInvRows.length > 10 && (
                    <button
                        type="button"
                        onClick={() => setShowAllGraphSkus((prev) => !prev)}
                        title={showAllGraphSkus ? "Collapse SKUs" : "View all SKUs"}
                        aria-label={showAllGraphSkus ? "Collapse SKUs" : "View all SKUs"}
                        className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                    >
                        {showAllGraphSkus ? (
                            <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                        ) : (
                            <RiExpandDiagonalFill size={18} className="font-extrabold" />
                        )}
                    </button>
                )}
            </div>

            <div
                className="
        w-full h-[46vh] sm:h-[48vh] md:h-[50vh]
        transition-opacity duration-300 text-[10px] 2xl:text-xs
      "
            >
                <InventoryAgeStackedBarChart data={inventoryAgeChartData} />
            </div>
        </div>
    );
}