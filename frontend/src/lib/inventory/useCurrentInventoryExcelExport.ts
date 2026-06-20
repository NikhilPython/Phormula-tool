import { useCallback, useMemo } from "react";
import type { RegionKey } from "@/lib/dashboard/types";
import type { InventoryRow } from "@/lib/inventory/fetchCurrentInventoryData";
import {
  exportCurrentInventoryExcel,
  exportGlobalCurrentInventoryExcel,
} from "@/lib/excel/exportCurrentInventoryExcel";

type CurrencyCode = "USD" | "GBP" | "CAD" | "INR";

type UseCurrentInventoryExcelExportArgs = {
  region: RegionKey;
  invRows: InventoryRow[];
  inventoryAlerts: Record<string, { alert?: string; alert_type?: string }>;
  userData: any;
  convertToDisplayCurrency: (
    value: number | null | undefined,
    from: CurrencyCode
  ) => number;
  selectedInventoryCountry?: "uk" | "us";
};

function getISTYearMonth() {
  const now = new Date();

  const monthName = now.toLocaleString("en-US", {
    timeZone: "Asia/Kolkata",
    month: "long",
  });

  const year = Number(
    now.toLocaleString("en-US", {
      timeZone: "Asia/Kolkata",
      year: "numeric",
    })
  );

  return { monthName, year };
}

const toNumberSafe = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;

  const n = Number(String(v).replace(/[, ]+/g, ""));
  return Number.isFinite(n) ? n : 0;
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

const getNumberByPossibleKeys = (row: InventoryRow, possible: string[]) => {
  const wanted = possible.map(normKey);
  const foundKey = Object.keys(row).find((k) => wanted.includes(normKey(k)));

  return foundKey ? toNumberSafe(row[foundKey]) : 0;
};

const getInventoryAgeBucketsForExport = (row: InventoryRow) => {
  const inventory0To90 = getNumberByPossibleKeys(row, [
    "inv-age-0-to-90-days",
    "inv_age_0_to_90_days",
    "Inventory Age 0 to 90 Days",
    "inv age 0 to 90 days",
  ]);

  const inventory91To180 = getNumberByPossibleKeys(row, [
    "inv-age-91-to-180-days",
    "inv_age_91_to_180_days",
    "Inventory Age 91 to 180 Days",
    "inv age 91 to 180 days",
  ]);

  const inventory181To270 = getNumberByPossibleKeys(row, [
    "inv-age-181-to-270-days",
    "inv_age_181_to_270_days",
    "Inventory Age 181 to 270 Days",
    "inv age 181 to 270 days",
  ]);

  const inventory271To365 = getNumberByPossibleKeys(row, [
    "inv-age-271-to-365-days",
    "inv_age_271_to_365_days",
    "Inventory Age 271 to 365 Days",
    "inv age 271 to 365 days",
  ]);

  const inventory365Plus = getNumberByPossibleKeys(row, [
    "inv-age-365-plus-days",
    "inv_age_365_plus_days",
    "Inventory Age 365+ Days",
    "inv age 365 plus days",
    "inv-age-365+-days",
  ]);

  return {
    inventory0To90,
    inventory91To180,
    inventory181To270,
    inventory271To365,
    inventory365Plus,
  };
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

const hasAnyAgeingInventoryForExport = (row: Record<string, any>) => {
  return (
    toNumberSafe(row["Inventory 0-90 Days"]) > 0 ||
    toNumberSafe(row["Inventory 91-180 Days"]) > 0 ||
    toNumberSafe(row["Inventory 181-270 Days"]) > 0 ||
    toNumberSafe(row["Inventory 271-365 Days"]) > 0 ||
    toNumberSafe(row["Inventory 365+ Days"]) > 0
  );
};

export function useCurrentInventoryExcelExport({
  region,
  invRows,
  inventoryAlerts,
  userData,
  convertToDisplayCurrency,
  selectedInventoryCountry = "uk",
}: UseCurrentInventoryExcelExportArgs) {
  const isGlobalInventory = region === "Global";

  const displayRegion = useMemo(() => {
    if (region === "UK") return "UK";
    if (region === "US") return "US";
    if (region === "CA") return "CA";
    return "Global";
  }, [region]);

  const homeCurrencyCodeForExcel = useMemo(
    () =>
      (userData as any)?.homeCurrency ||
      (userData as any)?.home_currency ||
      "",
    [userData]
  );

  const visibleInvRows = useMemo(() => {
    if (!Array.isArray(invRows)) return [];

    if (!isGlobalInventory) return invRows;

    return invRows.filter((row) => {
      const rowCountry = String(
        (row as any).country || (row as any).Country || ""
      ).toLowerCase();

      return rowCountry === selectedInventoryCountry;
    });
  }, [invRows, isGlobalInventory, selectedInventoryCountry]);

  const visibleInventoryAlerts = useMemo(() => {
    if (!isGlobalInventory) return inventoryAlerts;

    const prefix = `${selectedInventoryCountry.toUpperCase()}-`;
    const next: Record<string, { alert?: string; alert_type?: string }> = {};

    Object.entries(inventoryAlerts || {}).forEach(([key, value]) => {
      const normalizedKey = String(key).trim().toUpperCase();

      if (normalizedKey.startsWith(prefix)) {
        const skuOnly = normalizedKey.slice(prefix.length);
        next[skuOnly] = value;
        next[normalizedKey] = value;
      }
    });

    return next;
  }, [inventoryAlerts, isGlobalInventory, selectedInventoryCountry]);

  const findMtdKey = useCallback((row: InventoryRow) => {
    return (
      Object.keys(row).find((k) =>
        k.toLowerCase().startsWith("current month units sold")
      ) || ""
    );
  }, []);

  const findSales30Key = useCallback((row: InventoryRow) => {
    const keys = Object.keys(row);

    return (
      keys.find((k) => k.trim().toLowerCase() === "sales last 30 days") ||
      keys.find((k) => k.toLowerCase().includes("last 30 days")) ||
      keys.find((k) => k.toLowerCase().includes("past 30")) ||
      keys.find(
        (k) => k.trim().toLowerCase() === "sales for past 30 days"
      ) ||
      ""
    );
  }, []);

  const exportDataRows = useMemo(() => {
    if (!visibleInvRows?.length) return [];

    const normalRows: InventoryRow[] = [];

    visibleInvRows.forEach((r) => {
      const name = String(r["Product Name"] ?? "").trim();
      const sku = String(r["SKU"] ?? "").trim();

      if (!name && !sku) return;
      if (isInventoryTotalRow(r)) return;

      normalRows.push(r);
    });

    const buildRow = (row: InventoryRow, index: number) => {
      const sku = normalizeSku((row as any)["SKU"]);
      const mtdKey = findMtdKey(row);
      const sales30Key = findSales30Key(row);

      const currentInventory =
        toNumberSafe((row as any)["Current Inventory"]) ||
        toNumberSafe((row as any)["Inventory at the end of the month"]) ||
        toNumberSafe((row as any)["Available Inventory"]) ||
        toNumberSafe((row as any)["available"]);

      const mtdSales = toNumberSafe(mtdKey ? (row as any)[mtdKey] : 0);
      const salesLast30Days = toNumberSafe(
        sales30Key ? (row as any)[sales30Key] : 0
      );

      // const inventory180Plus =
      //   getNumberByPossibleKeys(row, [
      //     "inv-age-181-to-270-days",
      //     "inv_age_181_to_270_days",
      //     "Inventory Age 181 to 270 Days",
      //   ]) +
      //   getNumberByPossibleKeys(row, [
      //     "inv-age-271-to-365-days",
      //     "inv_age_271_to_365_days",
      //     "Inventory Age 271 to 365 Days",
      //   ]) +
      //   getNumberByPossibleKeys(row, [
      //     "inv-age-365-plus-days",
      //     "inv_age_365_plus_days",
      //     "Inventory Age 365+ Days",
      //     "inv age 365 plus days",
      //   ]);

      const {
  inventory0To90,
  inventory91To180,
  inventory181To270,
  inventory271To365,
  inventory365Plus,
} = getInventoryAgeBucketsForExport(row);

      const estimatedStorage = toNumberSafe(
        (row as any)["estimated-storage-cost-next-month"] ||
          (row as any)["Estimated Storage Cost"] ||
          (row as any)["Estimated Storage Cost ($)"]
      );

      const salesRank = toNumberSafe(
        (row as any)["sales-rank"] || (row as any)["Sales Rank"]
      );

      return {
        "S.No.": index + 1,
        "Product Name": (row as any)["Product Name"] ?? "",
        SKU: sku,
        "MTD Sales": mtdSales,
        "Sales Last 30 Days": salesLast30Days,
        "Sales Rank": salesRank,
        "Current Inventory": currentInventory,
        // "Inventory 180+ Days": inventory180Plus,
        "Inventory 0-90 Days": inventory0To90,
"Inventory 91-180 Days": inventory91To180,
"Inventory 181-270 Days": inventory181To270,
"Inventory 271-365 Days": inventory271To365,
"Inventory 365+ Days": inventory365Plus,
        "Estimated Storage Cost": estimatedStorage,
        "Inventory Coverage Ratio":
          salesLast30Days > 0 ? currentInventory / salesLast30Days : 0,
        "Inventory Alerts": visibleInventoryAlerts?.[sku]?.alert || "",
      };
    };

    const sortedRows = [...normalRows].sort((a, b) => {
      const aMtdKey = findMtdKey(a);
      const bMtdKey = findMtdKey(b);

      return (
        toNumberSafe(bMtdKey ? (b as any)[bMtdKey] : 0) -
        toNumberSafe(aMtdKey ? (a as any)[aMtdKey] : 0)
      );
    });

  const finalRows = sortedRows
  .map((r, i) => buildRow(r, i))
  .filter((row) => hasAnyAgeingInventoryForExport(row))
  .map((row, index) => ({
    ...row,
    "S.No.": index + 1,
  }));

    const totalRow = finalRows.reduce<Record<string, any>>(
      (acc, row) => {
        acc["MTD Sales"] += toNumberSafe(row["MTD Sales"]);
        acc["Sales Last 30 Days"] += toNumberSafe(row["Sales Last 30 Days"]);
        acc["Current Inventory"] += toNumberSafe(row["Current Inventory"]);
        // acc["Inventory 180+ Days"] += toNumberSafe(row["Inventory 180+ Days"]);
        acc["Inventory 0-90 Days"] += toNumberSafe(row["Inventory 0-90 Days"]);
acc["Inventory 91-180 Days"] += toNumberSafe(row["Inventory 91-180 Days"]);
acc["Inventory 181-270 Days"] += toNumberSafe(row["Inventory 181-270 Days"]);
acc["Inventory 271-365 Days"] += toNumberSafe(row["Inventory 271-365 Days"]);
acc["Inventory 365+ Days"] += toNumberSafe(row["Inventory 365+ Days"]);
        acc["Estimated Storage Cost"] += toNumberSafe(
          row["Estimated Storage Cost"]
        );

        return acc;
      },
      {
  "S.No.": "",
  "Product Name": "Total",
  SKU: "",
  "MTD Sales": 0,
  "Sales Last 30 Days": 0,
  "Sales Rank": "",
  "Current Inventory": 0,
  // "Inventory 180+ Days": 0,
  "Inventory 0-90 Days": 0,
"Inventory 91-180 Days": 0,
"Inventory 181-270 Days": 0,
"Inventory 271-365 Days": 0,
"Inventory 365+ Days": 0,
  "Estimated Storage Cost": 0,
  "Inventory Coverage Ratio": 0,
  "Inventory Alerts": "",
}
    );

    totalRow["Inventory Coverage Ratio"] =
      totalRow["Sales Last 30 Days"] > 0
        ? totalRow["Current Inventory"] / totalRow["Sales Last 30 Days"]
        : 0;

    return [...finalRows, totalRow];
  }, [visibleInvRows, visibleInventoryAlerts, findMtdKey, findSales30Key]);

  const buildExportRowsForCountry = useCallback(
    (country: "uk" | "us") => {
      const countryRows = (invRows || []).filter((row) => {
        const rowCountry = String(
          (row as any).country || (row as any).Country || ""
        ).toLowerCase();

        return rowCountry === country;
      });

      if (!countryRows.length) return [];

      const countryAlerts: Record<
        string,
        { alert?: string; alert_type?: string }
      > = {};

      const prefix = `${country.toUpperCase()}-`;

      Object.entries(inventoryAlerts || {}).forEach(([key, value]) => {
        const normalizedKey = String(key).trim().toUpperCase();

        if (normalizedKey.startsWith(prefix)) {
          const skuOnly = normalizedKey.slice(prefix.length);
          countryAlerts[skuOnly] = value;
        }
      });

      const usableRows = countryRows.filter((r) => {
        const name = String(r["Product Name"] ?? "").trim();
        const sku = String(r["SKU"] ?? "").trim();

        return (name || sku) && !isInventoryTotalRow(r);
      });

      const sourceCurrency: CurrencyCode = country === "us" ? "USD" : "GBP";

      const mappedRows = usableRows
        .map((row) => {
          const sku = normalizeSku((row as any)["SKU"]);
          const mtdKey = findMtdKey(row);
          const sales30Key = findSales30Key(row);

          const currentInventory =
            toNumberSafe((row as any)["Current Inventory"]) ||
            toNumberSafe((row as any)["Inventory at the end of the month"]) ||
            toNumberSafe((row as any)["Available Inventory"]) ||
            toNumberSafe((row as any)["available"]);

          const mtdSales = toNumberSafe(mtdKey ? (row as any)[mtdKey] : 0);

          const salesLast30Days = toNumberSafe(
            sales30Key ? (row as any)[sales30Key] : 0
          );

          // const inventory180Plus =
          //   getNumberByPossibleKeys(row, [
          //     "inv-age-181-to-270-days",
          //     "inv_age_181_to_270_days",
          //     "Inventory Age 181 to 270 Days",
          //   ]) +
          //   getNumberByPossibleKeys(row, [
          //     "inv-age-271-to-365-days",
          //     "inv_age_271_to_365_days",
          //     "Inventory Age 271 to 365 Days",
          //   ]) +
          //   getNumberByPossibleKeys(row, [
          //     "inv-age-365-plus-days",
          //     "inv_age_365_plus_days",
          //     "Inventory Age 365+ Days",
          //     "inv age 365 plus days",
          //   ]);

          const {
  inventory0To90,
  inventory91To180,
  inventory181To270,
  inventory271To365,
  inventory365Plus,
} = getInventoryAgeBucketsForExport(row);

          const estimatedStorageRaw = toNumberSafe(
            (row as any)["estimated-storage-cost-next-month"] ||
              (row as any)["Estimated Storage Cost"] ||
              (row as any)["Estimated Storage Cost ($)"]
          );

          return {
            sku,
            mtdSales,
            exportRow: {
              "S.No.": 0,
              "Product Name": (row as any)["Product Name"] ?? "",
              SKU: sku,
              "MTD Sales": mtdSales,
              "Sales Last 30 Days": salesLast30Days,
              "Sales Rank": toNumberSafe(
                (row as any)["sales-rank"] || (row as any)["Sales Rank"]
              ),
              "Current Inventory": currentInventory,
              // "Inventory 180+ Days": inventory180Plus,
              "Inventory 0-90 Days": inventory0To90,
"Inventory 91-180 Days": inventory91To180,
"Inventory 181-270 Days": inventory181To270,
"Inventory 271-365 Days": inventory271To365,
"Inventory 365+ Days": inventory365Plus,
              "Estimated Storage Cost": convertToDisplayCurrency(
                estimatedStorageRaw,
                sourceCurrency
              ),
              "Inventory Coverage Ratio":
                salesLast30Days > 0
                  ? currentInventory / salesLast30Days
                  : 0,
              "Inventory Alerts": countryAlerts?.[sku]?.alert || "",
            },
          };
        })
        .sort((a, b) => b.mtdSales - a.mtdSales);

const finalRows = mappedRows
  .map((item) => item.exportRow)
  .filter((row) => hasAnyAgeingInventoryForExport(row))
  .map((row, index) => ({
    ...row,
    "S.No.": index + 1,
  }));

      const totalRow = finalRows.reduce<Record<string, any>>(
        (acc, row) => {
          acc["MTD Sales"] += toNumberSafe(row["MTD Sales"]);
          acc["Sales Last 30 Days"] += toNumberSafe(row["Sales Last 30 Days"]);
          acc["Current Inventory"] += toNumberSafe(row["Current Inventory"]);
          // acc["Inventory 180+ Days"] += toNumberSafe(row["Inventory 180+ Days"]);
          acc["Inventory 0-90 Days"] += toNumberSafe(row["Inventory 0-90 Days"]);
acc["Inventory 91-180 Days"] += toNumberSafe(row["Inventory 91-180 Days"]);
acc["Inventory 181-270 Days"] += toNumberSafe(row["Inventory 181-270 Days"]);
acc["Inventory 271-365 Days"] += toNumberSafe(row["Inventory 271-365 Days"]);
acc["Inventory 365+ Days"] += toNumberSafe(row["Inventory 365+ Days"]);
          acc["Estimated Storage Cost"] += toNumberSafe(
            row["Estimated Storage Cost"]
          );

          return acc;
        },
        {
          "S.No.": "",
          "Product Name": "Total",
          SKU: "",
          "MTD Sales": 0,
          "Sales Last 30 Days": 0,
          "Sales Rank": "",
          "Current Inventory": 0,
          // "Inventory 180+ Days": 0,
          "Inventory 0-90 Days": 0,
"Inventory 91-180 Days": 0,
"Inventory 181-270 Days": 0,
"Inventory 271-365 Days": 0,
"Inventory 365+ Days": 0,
          "Estimated Storage Cost": 0,
          "Inventory Coverage Ratio": 0,
          "Inventory Alerts": "",
        }
      );

      totalRow["Inventory Coverage Ratio"] =
        totalRow["Sales Last 30 Days"] > 0
          ? totalRow["Current Inventory"] / totalRow["Sales Last 30 Days"]
          : 0;

      return [...finalRows, totalRow];
    },
    [
      invRows,
      inventoryAlerts,
      findMtdKey,
      findSales30Key,
      convertToDisplayCurrency,
    ]
  );

  const downloadInventoryExcel = useCallback(() => {
    const { monthName, year } = getISTYearMonth();
    const abbr = monthName.slice(0, 3);

    const periodLabel = `${abbr.charAt(0).toUpperCase()}${abbr.slice(
      1
    )}'${String(year).slice(2)}`;

    const companyNameForExcel = String(
      (userData as any)?.company_name || (userData as any)?.companyName || ""
    )
      .trim()
      .toLowerCase()
      .replace(/\b\w/g, (c) => c.toUpperCase());

    const brandNameForExcel = String(
      (userData as any)?.brand_name || (userData as any)?.brandName || ""
    )
      .trim()
      .toLowerCase()
      .replace(/\b\w/g, (c) => c.toUpperCase());

    if (isGlobalInventory) {
      const ukRows = buildExportRowsForCountry("uk");
      const usRows = buildExportRowsForCountry("us");

      void exportGlobalCurrentInventoryExcel({
        filename: `Current-Inventory_Global_${monthName}_${year}.xlsx`,
        titleLine: `Amazon Global - Current Inventory - ${periodLabel}`,
        platformLabel: "Phormula",
        periodLabel,
        companyName: companyNameForExcel,
        brandName: brandNameForExcel,
        homeCurrencyCode: homeCurrencyCodeForExcel,
        ukRows,
        usRows,
      });

      return;
    }

    if (!exportDataRows.length) return;

    exportCurrentInventoryExcel({
      filename: `Current-Inventory_${displayRegion}_${monthName}_${year}.xlsx`,
      titleLine: `Amazon ${displayRegion} - Current Inventory - ${periodLabel}`,
      countryName: displayRegion.toLowerCase(),
      titleCountry: displayRegion,
      platformLabel: "Phormula",
      periodLabel,
      companyName: companyNameForExcel,
      brandName: brandNameForExcel,
      homeCurrencyCode: homeCurrencyCodeForExcel,
      dataRows: exportDataRows,
    });
  }, [
    isGlobalInventory,
    buildExportRowsForCountry,
    exportDataRows,
    displayRegion,
    userData,
    homeCurrencyCodeForExcel,
  ]);

  return {
    downloadInventoryExcel,
    canDownloadInventoryExcel:
      isGlobalInventory ||
      exportDataRows.length > 0 ||
      buildExportRowsForCountry("uk").length > 0 ||
      buildExportRowsForCountry("us").length > 0,
  };
}