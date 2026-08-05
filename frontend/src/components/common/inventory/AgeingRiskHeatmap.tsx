import React, { useMemo, useState } from "react";
import {
    RiCollapseDiagonalFill,
    RiExpandDiagonalFill,
    RiLayoutColumnFill,
    RiLayoutColumnLine,
} from "react-icons/ri";
import PageBreadcrumb from "../PageBreadCrumb";
import GroupedCollapsibleTable, {
    type ColGroup,
    type LeafCol,
} from "@/components/ui/table/GroupedCollapsibleTable";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import { exportAgeingRiskHeatmapExcel } from "@/lib/excel/exportCurrentInventoryExcel";

export type AgeingBucket = {
    key: string;
    label: string;
    color: string;
};

export type AgeingRiskHeatmapRow = {
    productName: string;
    sku?: string;

    // Older inventory payloads used "available"; current inventory maps FBA from "Sellable Units".
    available?: number;
    "Sellable Units"?: number | string;

    // FC Transfer units from backend "fc-transfer"
    fcTransfer?: number;

    // keep this only if used elsewhere
    totalUnits?: number;

    inboundUnits?: number;
    unsellableUnits?: number;
    currentFba?: number;
    currentAwd?: number;
    transitFba?: number;
    transitAwd?: number;
    totalInStock?: number;
    totalInTransit?: number;
    unsellableFba?: number;
    unsellableAwd?: number;
    storageCostUsd?: number;
    coverageCurrentAndTransit?: number;
    unitsSold?: number;
    salesRank?: number | string;
    previousSalesRank?: number | string;

    // ✅ For Others coverage ratio
    salesLast30Days?: number;

    coverageRatio?: number;

    inventoryAlert?: string;

    isOthersRow?: boolean;
    isTotalRow?: boolean;
    isPercentageRow?: boolean;
    isDummyRow?: boolean;

    [bucketKey: string]: string | number | boolean | undefined;
};

export type AgeingRiskUnitSalesDataKey = "salesLast30Days" | "unitsSold";

type AgeingRiskHeatmapProps = {
    title?: string;
    subtitle?: string;
    data: AgeingRiskHeatmapRow[];
    buckets: AgeingBucket[];
    defaultVisibleRows?: number;
    salesLast30DaysLabel?: string;
    unitSalesDataKey?: AgeingRiskUnitSalesDataKey;
    onProductClick?: (row: AgeingRiskHeatmapRow) => void;

    onDownloadInventoryExcel?: () => void;
    canDownloadInventoryExcel?: boolean;
    showInventoryAlerts?: boolean;
    useCurrentInventoryTableLayout?: boolean;
    storageCostCurrencySymbol?: string;

    showExcelDownload?: boolean;
    excelFilename?: string;
    excelTitleLine?: string;
    excelCountryLabel?: string;
    excelPlatformLabel?: string;
    excelPeriodLabel?: string;
    excelCompanyName?: string;
    excelBrandName?: string;
    inventoryAgeSummary?: {
        total?: number;
        current_month_units_sold_total?: number;
        percentage_base_total?: number;
        sellable_total?: number;
        unfulfillable_total?: number;
        total_units_summary?: {
            current_month_units_sold?: {
                total?: number;
                percentage_share?: number;
            };
            sellable?: {
                total?: number;
                percentage_share?: number;
            };
            unfulfillable?: {
                total?: number;
                percentage_share?: number;
            };
        };
        columns?: Record<
            string,
            {
                total?: number;
                percentage_share?: number;
            }
        >;
    };
};

type HeatmapTableRow = AgeingRiskHeatmapRow;

const parseSalesRankNumber = (value: unknown) => {
    if (
        value === null ||
        value === undefined ||
        value === "" ||
        String(value).trim().toLowerCase() === "nan"
    ) {
        return null;
    }

    const n = Number(String(value).replace(/,/g, "").trim());

    return Number.isFinite(n) && n > 0 ? n : null;
};

const getSalesRankMovement = (currentRankValue: unknown, previousRankValue: unknown) => {
    const currentRank = parseSalesRankNumber(currentRankValue);
    const previousRank = parseSalesRankNumber(previousRankValue);

    if (!currentRank || !previousRank) return null;

    // Sales Rank me lower rank better hota hai.
    const rankDifference = previousRank - currentRank;

    if (rankDifference === 0) return null;

    return {
        value: rankDifference,
        isGood: rankDifference > 0,
    };
};

const getHeatColor = (
    bucketColor: string,
    value: number,
    rowTotal: number
): string => {
    if (!value || !rowTotal) return "#fff";

    const percentage = value / rowTotal;
    const opacity = Math.min(0.85, Math.max(0.15, percentage * 2.2));

    const hex = bucketColor.replace("#", "");
    const r = parseInt(hex.substring(0, 2), 16);
    const g = parseInt(hex.substring(2, 4), 16);
    const b = parseInt(hex.substring(4, 6), 16);

    return `rgba(${r}, ${g}, ${b}, ${opacity})`;
};

const getHeatmapColorBaseTotal = (
    row: AgeingRiskHeatmapRow,
    buckets: AgeingBucket[]
) => {
    const bucketTotal = buckets.reduce(
        (sum, bucket) => sum + Number(row[bucket.key] || 0),
        0
    );

    const available = Number(row.available ?? 0);
    const totalUnits = Number(row.totalUnits ?? 0);

    // For current month, available is fine.
    // For history months, available/totalUnits can be 0,
    // so use bucketTotal to keep heatmap colors working.
    if (totalUnits > 0) return totalUnits;
    if (available > 0) return available;

    return bucketTotal;
};

const getCurrentFbaValue = (row: AgeingRiskHeatmapRow) =>
    row.currentFba ?? row["Sellable Units"] ?? row.available;

const getCurrentFbaNumberValue = (row: AgeingRiskHeatmapRow) =>
    Number(getCurrentFbaValue(row) ?? 0);

const hasSellableBreakdown = (rows: AgeingRiskHeatmapRow[]) => {
    return rows.some(
        (row) =>
            Number(row.available || 0) > 0 ||
            Number(row.fcTransfer || 0) > 0
    );
};

const getUnitSalesValue = (
    row: AgeingRiskHeatmapRow,
    unitSalesDataKey: AgeingRiskUnitSalesDataKey = "salesLast30Days"
) => {
    const primaryValue =
        unitSalesDataKey === "unitsSold"
            ? row.unitsSold
            : row.salesLast30Days;

    const fallbackValue =
        unitSalesDataKey === "unitsSold"
            ? row.salesLast30Days
            : row.unitsSold;

    const hasPrimaryValue =
        primaryValue !== null &&
        primaryValue !== undefined;

    const value = hasPrimaryValue ? primaryValue : fallbackValue;

    const n = Number(value ?? 0);

    return Number.isFinite(n) ? n : 0;
};

const buildAggregateRow = (
    label: string,
    rows: AgeingRiskHeatmapRow[],
    buckets: AgeingBucket[],
    flags?: {
        isOthersRow?: boolean;
        isTotalRow?: boolean;
    },
    unitSalesDataKey: AgeingRiskUnitSalesDataKey = "salesLast30Days"
): AgeingRiskHeatmapRow => {
    const aggregate: AgeingRiskHeatmapRow = {
        productName: label,
        sku: "-",
        isOthersRow: flags?.isOthersRow,
        isTotalRow: flags?.isTotalRow,
    };

    buckets.forEach((bucket) => {
        aggregate[bucket.key] = rows.reduce(
            (sum, row) => sum + Number(row[bucket.key] || 0),
            0
        );
    });

    aggregate.available = rows.reduce(
        (sum, row) => sum + Number(row.available || 0),
        0
    );

    aggregate.fcTransfer = rows.reduce(
        (sum, row) => sum + Number(row.fcTransfer || 0),
        0
    );

    aggregate.totalUnits = rows.reduce(
        (sum, row) =>
            sum + Number(row.totalUnits ?? (Number(row.available || 0) + Number(row.fcTransfer || 0))),
        0
    );

    aggregate.inboundUnits = rows.reduce(
        (sum, row) => sum + Number(row.inboundUnits || 0),
        0
    );

    aggregate.unsellableUnits = rows.reduce(
        (sum, row) => sum + Number(row.unsellableUnits || 0),
        0
    );

    aggregate.currentFba = rows.reduce(
        (sum, row) => sum + getCurrentFbaNumberValue(row),
        0
    );

    aggregate.currentAwd = rows.reduce(
        (sum, row) => sum + Number(row.currentAwd || 0),
        0
    );

    aggregate.transitFba = rows.reduce(
        (sum, row) => sum + Number(row.transitFba || row.fcTransfer || 0),
        0
    );

    aggregate.transitAwd = rows.reduce(
        (sum, row) => sum + Number(row.transitAwd || row.inboundUnits || 0),
        0
    );

    aggregate.totalInStock = rows.reduce(
        (sum, row) =>
            sum +
            Number(
                row.totalInStock ??
                row.totalUnits ??
                getCurrentFbaNumberValue(row) +
                Number(row.currentAwd || 0)
            ),
        0
    );

    aggregate.totalInTransit = rows.reduce(
        (sum, row) =>
            sum +
            Number(
                row.totalInTransit ??
                Number(row.transitFba || row.fcTransfer || 0) +
                Number(row.transitAwd || row.inboundUnits || 0)
            ),
        0
    );

    aggregate.unsellableFba = rows.reduce(
        (sum, row) => sum + Number(row.unsellableFba || row.unsellableUnits || 0),
        0
    );

    aggregate.unsellableAwd = rows.reduce(
        (sum, row) => sum + Number(row.unsellableAwd || 0),
        0
    );

    aggregate.storageCostUsd = rows.reduce(
        (sum, row) => sum + Number(row.storageCostUsd || 0),
        0
    );

    aggregate.salesRank = "";

    aggregate.unitsSold = rows.some(
        (row) => row.unitsSold !== null && row.unitsSold !== undefined
    )
        ? rows.reduce((sum, row) => sum + Number(row.unitsSold || 0), 0)
        : undefined;

    aggregate.salesLast30Days = rows.some(
        (row) => row.salesLast30Days !== null && row.salesLast30Days !== undefined
    )
        ? rows.reduce((sum, row) => sum + Number(row.salesLast30Days || 0), 0)
        : undefined;

    aggregate.inventoryAlert = "";

    // ✅ For Others only:
    // Coverage Ratio = aggregated available / selected unit-sales value
    if (flags?.isOthersRow) {
        const totalAvailable = Number(aggregate.available || 0);
        const totalUnitSales = getUnitSalesValue(aggregate, unitSalesDataKey);

        aggregate.coverageRatio =
            totalUnitSales > 0
                ? Number(aggregate.totalInStock || totalAvailable) / totalUnitSales
                : 0;

        aggregate.coverageCurrentAndTransit =
            totalUnitSales > 0
                ? (Number(aggregate.totalInStock || 0) +
                    Number(aggregate.totalInTransit || 0)) /
                totalUnitSales
                : 0;

        return aggregate;
    }

    // Existing logic for Total or any other aggregate row
    const weightedCoverageTotal = rows.reduce((sum, row) => {
        const calculatedTotal = buckets.reduce(
            (bucketSum, bucket) => bucketSum + Number(row[bucket.key] || 0),
            0
        );

        const rowTotal = Number(row.available ?? row.totalUnits ?? calculatedTotal);
        const coverageRatio = Number(row.coverageRatio ?? 0);

        if (!rowTotal || !Number.isFinite(coverageRatio)) return sum;

        return sum + coverageRatio * rowTotal;
    }, 0);

    aggregate.coverageRatio = aggregate.totalUnits
        ? weightedCoverageTotal / aggregate.totalUnits
        : 0;

    const aggregateUnitSales = getUnitSalesValue(aggregate, unitSalesDataKey);

    aggregate.coverageCurrentAndTransit =
        aggregateUnitSales > 0
            ? (Number(aggregate.totalInStock || aggregate.totalUnits || 0) +
                Number(aggregate.totalInTransit || 0)) /
            aggregateUnitSales
            : undefined;

    return aggregate;
};

const bucketKeyToApiColumn: Record<string, string> = {
    zeroToNinety: "inv-age-0-to-90-days",
    ninetyOneToOneEighty: "inv-age-91-to-180-days",
    zeroToOneEighty: "inv-age-0-to-180-days",
    oneEightyOneToTwoSeventy: "inv-age-181-to-270-days",
    twoSeventyOneToThreeSixtyFive: "inv-age-271-to-365-days",
    threeSixtyFivePlus: "inv-age-365-plus-days",
};

const buildPercentageRow = (
    totalRow: AgeingRiskHeatmapRow,
    buckets: AgeingBucket[],
    inventoryAgeSummary?: AgeingRiskHeatmapProps["inventoryAgeSummary"]
): AgeingRiskHeatmapRow => {
    const percentageBaseTotal = Number(
        inventoryAgeSummary?.percentage_base_total || 0
    );

    const percentageRow: AgeingRiskHeatmapRow = {
        productName: "% of Total",
        sku: "-",
        available: 0,
        totalUnits: 0,
        inboundUnits: undefined,
        unsellableUnits: 0,
        unitsSold: undefined,
        coverageRatio: undefined,
        inventoryAlert: "",
        isPercentageRow: true,
    };

    if (percentageBaseTotal > 0) {
        buckets.forEach((bucket) => {
            const apiColumn = bucketKeyToApiColumn[bucket.key];

            const backendPercentage =
                inventoryAgeSummary?.columns?.[apiColumn]?.percentage_share;

            const backendTotal =
                inventoryAgeSummary?.columns?.[apiColumn]?.total;

            const frontendSplitTotal = Number(totalRow[bucket.key] || 0);

            percentageRow[bucket.key] =
                typeof backendPercentage === "number"
                    ? backendPercentage
                    : typeof backendTotal === "number"
                        ? (backendTotal / percentageBaseTotal) * 100
                        : frontendSplitTotal > 0
                            ? (frontendSplitTotal / percentageBaseTotal) * 100
                            : 0;
        });

        percentageRow.available =
            inventoryAgeSummary?.total_units_summary?.sellable?.percentage_share ??
            (
                inventoryAgeSummary?.sellable_total
                    ? (Number(inventoryAgeSummary.sellable_total) / percentageBaseTotal) * 100
                    : 0
            );

        percentageRow.fcTransfer =
            percentageBaseTotal > 0
                ? (Number(totalRow.fcTransfer || 0) / percentageBaseTotal) * 100
                : 0;

        percentageRow.totalUnits = percentageRow.available;

        percentageRow.unsellableUnits =
            inventoryAgeSummary?.total_units_summary?.unfulfillable?.percentage_share ??
            (
                inventoryAgeSummary?.unfulfillable_total
                    ? (Number(inventoryAgeSummary.unfulfillable_total) / percentageBaseTotal) * 100
                    : 0
            );

        return percentageRow;
    }

    // fallback only when backend summary is missing
    const sellableUnits = Number(totalRow.available ?? totalRow.totalUnits ?? 0);
    const unfulfillableUnits = Number(totalRow.unsellableUnits || 0);
    const fallbackBase = sellableUnits + unfulfillableUnits;

    buckets.forEach((bucket) => {
        const value = Number(totalRow[bucket.key] || 0);
        percentageRow[bucket.key] =
            fallbackBase > 0 ? (value / fallbackBase) * 100 : 0;
    });

    percentageRow.available =
        fallbackBase > 0 ? (sellableUnits / fallbackBase) * 100 : 0;

    percentageRow.totalUnits = percentageRow.available;

    percentageRow.unsellableUnits =
        fallbackBase > 0 ? (unfulfillableUnits / fallbackBase) * 100 : 0;

    return percentageRow;
};

const getUnitSalesSortValue = (
    row: AgeingRiskHeatmapRow,
    unitSalesDataKey: AgeingRiskUnitSalesDataKey = "salesLast30Days"
) => {
    return getUnitSalesValue(row, unitSalesDataKey);
};

const AgeingRiskHeatmap: React.FC<AgeingRiskHeatmapProps> = ({
    title = "Ageing Risk Heatmap",
    subtitle = "Quickly identify products with old inventory",
    data,
    buckets,
    defaultVisibleRows = 9,
    salesLast30DaysLabel = "Unit Sales in Last 30 Days",
    unitSalesDataKey = "salesLast30Days",
    onProductClick,
    onDownloadInventoryExcel,
    canDownloadInventoryExcel = false,
    showInventoryAlerts = true,
    useCurrentInventoryTableLayout = false,
    storageCostCurrencySymbol = "$",
    showExcelDownload = true,
    excelFilename = "ageing-risk-heatmap.xlsx",
    excelTitleLine,
    excelPlatformLabel = "Phormula",
    excelCountryLabel = "",
    excelPeriodLabel = "",
    excelCompanyName = "",
    excelBrandName = "",
    inventoryAgeSummary,
}) => {
    const [isExpanded, setIsExpanded] = useState(false);
    const [isInventoryDetailsExpanded, setIsInventoryDetailsExpanded] =
        useState(false);
    const [isSalesCoverageDetailsExpanded, setIsSalesCoverageDetailsExpanded] =
        useState(false);
    const storageCostHeaderLabel = `Est. Storage Cost (${storageCostCurrencySymbol || "$"})`;
    const areAllCurrentInventoryColumnsExpanded =
        isInventoryDetailsExpanded && isSalesCoverageDetailsExpanded;

    const handleToggleAllCurrentInventoryColumns = () => {
        const nextExpanded = !areAllCurrentInventoryColumnsExpanded;

        setIsInventoryDetailsExpanded(nextExpanded);
        setIsSalesCoverageDetailsExpanded(nextExpanded);
    };

    const showSellableBreakdown = useMemo(() => {
        return data.some((row) => {
            if (row.isPercentageRow) return false;

            const available = Number(row.available || 0);
            const fcTransfer = Number(row.fcTransfer || 0);

            return available > 0 || fcTransfer > 0;
        });
    }, [data]);

    const canCollapse = data.length > defaultVisibleRows;

    const displayRows = useMemo<HeatmapTableRow[]>(() => {
        const backendPercentageRow = data.find((row) => {
            const productName = String(row.productName || "").trim().toLowerCase();
            const sku = String(row.sku || "").trim().toLowerCase();

            return (
                row.isPercentageRow === true ||
                (row as any).is_percentage_row === true ||
                productName === "% of total" ||
                productName === "percentage" ||
                sku === "% of total" ||
                sku === "percentage"
            );
        });

        const hasAnyDisplayBucketValue = (row: AgeingRiskHeatmapRow) => {
            return buckets.some((bucket) => Number(row[bucket.key] || 0) > 0);
        };

        const backendTotalRow = data.find((row) => {
            const productName = String(row.productName || "").trim().toLowerCase();
            const sku = String(row.sku || "").trim().toLowerCase();

            return (
                row.isTotalRow === true ||
                productName === "total" ||
                productName === "grand total" ||
                sku === "total" ||
                sku === "grand total"
            );
        });

        const productRows = data.filter((row) => {
            const productName = String(row.productName || "").trim().toLowerCase();
            const sku = String(row.sku || "").trim().toLowerCase();
            const isPercentageRow =
                row.isPercentageRow === true ||
                (row as any).is_percentage_row === true ||
                productName === "% of total" ||
                productName === "percentage" ||
                sku === "% of total" ||
                sku === "percentage";

            return (
                !isPercentageRow &&
                !row.isTotalRow &&
                productName !== "total" &&
                productName !== "grand total" &&
                sku !== "total" &&
                sku !== "grand total" &&
                (row.isDummyRow === true || hasAnyDisplayBucketValue(row))
            );
        });

        const sortSalesKey = unitSalesDataKey;

        const sortedData = [...productRows].sort((a, b) => {
            const aUnitSales = getUnitSalesSortValue(a, sortSalesKey);
            const bUnitSales = getUnitSalesSortValue(b, sortSalesKey);

            return bUnitSales - aUnitSales;
        });

        const calculatedTotalRow = buildAggregateRow("Total", sortedData, buckets, {
            isTotalRow: true,
        }, unitSalesDataKey);

        const totalRow = backendTotalRow
            ? {
                ...calculatedTotalRow,
                ...backendTotalRow,

                // ✅ keep backend coverage ratio only
                coverageRatio: Number(backendTotalRow.coverageRatio ?? 0),
                coverageCurrentAndTransit: Number(
                    backendTotalRow.coverageCurrentAndTransit ?? 0
                ),
                isTotalRow: true,
                productName: "Total",
                sku: "-",
            }
            : calculatedTotalRow;

        if (inventoryAgeSummary) {
            buckets.forEach((bucket) => {
                const apiColumn = bucketKeyToApiColumn[bucket.key];
                const backendTotal = inventoryAgeSummary.columns?.[apiColumn]?.total;

                if (typeof backendTotal === "number") {
                    totalRow[bucket.key] = backendTotal;
                }
            });

            // ✅ Do NOT use inventoryAgeSummary.sellable_total for Available.
            // Backend "Total" row already has separate available / fc-transfer / Sellable Units.
            // inventoryAgeSummary.sellable_total can include different sellable base and causes wrong UI total.

            const backendTotalRow = data.find((row) => {
                const productName = String(row.productName || "").trim().toLowerCase();
                const sku = String(row.sku || "").trim().toLowerCase();

                return (
                    row.isTotalRow === true ||
                    productName === "total" ||
                    productName === "grand total" ||
                    sku === "total" ||
                    sku === "grand total"
                );
            });

            if (backendTotalRow) {
                totalRow.available = Number(backendTotalRow.available || 0);
                totalRow.fcTransfer = Number(backendTotalRow.fcTransfer || 0);
                totalRow.totalUnits = Number(
                    backendTotalRow.totalUnits ??
                    Number(backendTotalRow.available || 0) + Number(backendTotalRow.fcTransfer || 0)
                );

                totalRow.currentFba = Number(
                    getCurrentFbaValue(backendTotalRow) ?? 0
                );
                totalRow.currentAwd = Number(backendTotalRow.currentAwd || 0);
                totalRow.transitFba = Number(
                    backendTotalRow.transitFba ?? backendTotalRow.fcTransfer ?? 0
                );
                totalRow.transitAwd = Number(
                    backendTotalRow.transitAwd ?? backendTotalRow.inboundUnits ?? 0
                );
                totalRow.totalInStock = Number(
                    backendTotalRow.totalInStock ?? backendTotalRow.totalUnits ?? 0
                );
                totalRow.totalInTransit = Number(
                    backendTotalRow.totalInTransit ?? backendTotalRow.inboundUnits ?? 0
                );
                totalRow.storageCostUsd = Number(backendTotalRow.storageCostUsd || 0);
            }

            if (typeof inventoryAgeSummary.unfulfillable_total === "number") {
                totalRow.unsellableUnits = inventoryAgeSummary.unfulfillable_total;
                totalRow.unsellableFba = inventoryAgeSummary.unfulfillable_total;
            }

            if (typeof inventoryAgeSummary.current_month_units_sold_total === "number") {
                totalRow.unitsSold = inventoryAgeSummary.current_month_units_sold_total;
            }
        }

        const percentageRow = backendPercentageRow
            ? ({
                ...backendPercentageRow,
                productName: "% of Total",
                sku: backendPercentageRow.sku || "-",
                isPercentageRow: true,
            } as HeatmapTableRow)
            : buildPercentageRow(totalRow, buckets, inventoryAgeSummary);

        if (!canCollapse || isExpanded) {
            return [...sortedData, totalRow, percentageRow] as HeatmapTableRow[];
        }

        const mainRows = sortedData.slice(0, defaultVisibleRows);
        const otherRows = sortedData.slice(defaultVisibleRows);

        if (!otherRows.length) {
            return [...mainRows, totalRow, percentageRow] as HeatmapTableRow[];
        }

        const othersRow = buildAggregateRow("Others", otherRows, buckets, {
            isOthersRow: true,
        }, unitSalesDataKey);

        return [...mainRows, othersRow, totalRow, percentageRow] as HeatmapTableRow[];
    }, [data, buckets, canCollapse, isExpanded, defaultVisibleRows, inventoryAgeSummary, unitSalesDataKey, useCurrentInventoryTableLayout]);

    const bucketMaxValues = useMemo(() => {
        const maxMap: Record<string, number> = {};

        buckets.forEach((bucket) => {
            maxMap[bucket.key] = Math.max(
                ...displayRows
                    .filter((row) => !row.isPercentageRow && !row.isTotalRow)
                    .map((row) => Number(row[bucket.key] || 0)),
                0
            );
        });

        return maxMap;
    }, [buckets, displayRows]);


    const hasAnySalesRankMovement = useMemo(() => {
        return displayRows.some((row) => {
            if (row.isTotalRow || row.isPercentageRow || row.isOthersRow) {
                return false;
            }

            return !!getSalesRankMovement(row.salesRank, row.previousSalesRank);
        });
    }, [displayRows]);

    const tableConfig = useMemo(() => {
        const heatmapHeaderClassName =
            "!z-20 !px-1 !py-2 !h-auto !whitespace-normal !break-words !text-center !leading-tight !overflow-hidden !text-[12px] min-[1700px]:!text-[14px]";
        const defaultTdClassName =
            "!text-[12px] min-[1700px]:!text-[14px] text-charcoal-500 whitespace-nowrap overflow-hidden truncate";

        const percentageRowTextClassName =
            "min-[1700px]:!text-[14px] min-[1700px]:!font-semibold";

        const numberDisplay = (value: any) => {
            const n = Number(value || 0);
            return Number.isFinite(n) && n > 0 ? n.toLocaleString() : "-";
        };

        const percentDisplay = (value: any) => {
            const n = Number(value || 0);
            return n > 0
                ? `${n.toLocaleString(undefined, {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                })}%`
                : "-";
        };

        const makeCol = (
            key: string,
            label: React.ReactNode,
            width: string,
            align: "left" | "center" | "right" = "center",
            tdClassName = defaultTdClassName
        ): LeafCol<HeatmapTableRow> => ({
            key,
            label,
            width,
            align,
            thClassName: heatmapHeaderClassName,
            tdClassName,
        });

        const snoCol = makeCol("sno", useCurrentInventoryTableLayout ? "Sno." : "S.No.", "48px");
        const productNameCol = makeCol("productName", "Product Name", useCurrentInventoryTableLayout ? "170px" : "145px", "left");
        const skuCol = makeCol("sku", "SKU", "120px", "left");
        const salesRankCol = makeCol("salesRank", "Sales Rank", hasAnySalesRankMovement ? "140px" : "90px");

        const leftCols: LeafCol<HeatmapTableRow>[] = [
            snoCol,
            productNameCol,
        ];

        const scrollableIdentityCols: LeafCol<HeatmapTableRow>[] = [
            skuCol,
            salesRankCol,
        ];

        const ageBucketCols: LeafCol<HeatmapTableRow>[] = buckets.map((bucket) => ({
            key: bucket.key,
            label: bucket.label,
            width: "72px",
            align: "center",
            thClassName: heatmapHeaderClassName,
            tdClassName:
                "relative !p-0 overflow-hidden text-center text-charcoal-500 text-[12px] min-[1700px]:text-[14px] whitespace-normal break-words",
        }));

        const showAlertsColumn = showInventoryAlerts;

        const currentInventoryGroups: ColGroup<HeatmapTableRow>[] = [
            {
                id: "currentInventory",
                label: "Current Inventory",
                expandable: false,
                collapsedCols: [],
                expandedCols: [
                    makeCol("currentFba", "FBA", "84px"),
                    makeCol("currentAwd", "AWD", "84px"),
                ],
            },
            {
                id: "transitInventory",
                label: "In Transit Inventory",
                expandable: false,
                collapsedCols: [],
                expandedCols: [
                    makeCol("transitFba", "FBA", "84px"),
                    makeCol("transitAwd", "AWD", "84px"),
                ],
            },
            {
                id: "totalSellableInventory",
                label: "Total Sellable Inventory",
                collapsedCols: [
                    makeCol("totalInStock", "In Stock", "92px"),
                    makeCol("totalInTransit", "In transit", "92px"),
                ],
                expandedCols: [
                    makeCol("totalInStock", "In Stock", "92px"),
                    makeCol("totalInTransit", "In Transit", "92px"),
                ],
            },
            {
                id: "unsellableInventory",
                label: "Unsellable Inventory",
                expandable: false,
                collapsedCols: [],
                expandedCols: [
                    makeCol("unsellableFba", "FBA", "84px"),
                    makeCol("unsellableAwd", "AWD", "84px"),
                ],
            },
            {
                id: "fbaBreakup",
                label: "Breakup - FBA Inventory",
                expandable: false,
                collapsedCols: [],
                expandedCols: ageBucketCols,
            },
            {
                id: "salesCoverage",
                label: "Sales & Coverage Ratio",
                collapsedCols: [
                    makeCol("salesLast30Days", salesLast30DaysLabel, "120px"),
                    makeCol("coverageRatio", "Coverage Ratio (Current Inventory)", "130px"),
                    makeCol(
                        "coverageCurrentAndTransit",
                        "Coverage Ratio (Current + In transit)",
                        "136px"
                    ),
                ],
                expandedCols: [
                    makeCol("salesLast30Days", salesLast30DaysLabel, "120px"),
                    makeCol("coverageRatio", "Coverage Ratio (Current Inventory)", "130px"),
                    makeCol(
                        "coverageCurrentAndTransit",
                        "Coverage Ratio (Current + In transit)",
                        "136px"
                    ),
                ],
            },
        ];

        const sellableGroup: ColGroup<HeatmapTableRow> = {
            id: "sellable",
            label: "Sellable Units",

            expandable: showSellableBreakdown,

            collapsedCols: [
                {
                    key: "totalUnits",
                    label: "Sellable Units",
                    width: "95px",
                    align: "center",
                    thClassName: heatmapHeaderClassName,
                    tdClassName: defaultTdClassName,
                },
            ],

            expandedCols: showSellableBreakdown
                ? [
                    {
                        key: "available",
                        label: "Available",
                        width: "110px",
                        align: "center",
                        thClassName: heatmapHeaderClassName,
                        tdClassName: defaultTdClassName,
                    },
                    {
                        key: "fcTransfer",
                        label: "FC Transfer",
                        width: "110px",
                        align: "center",
                        thClassName: heatmapHeaderClassName,
                        tdClassName: defaultTdClassName,
                    },
                    {
                        key: "totalUnits",
                        label: "Total",
                        width: "110px",
                        align: "center",
                        thClassName: heatmapHeaderClassName,
                        tdClassName: defaultTdClassName,
                    },
                ]
                : [
                    {
                        key: "totalUnits",
                        label: "Sellable Units",
                        width: "95px",
                        align: "center",
                        thClassName: heatmapHeaderClassName,
                        tdClassName: defaultTdClassName,
                    },
                ],
        };

        const singleCols: LeafCol<HeatmapTableRow>[] = useCurrentInventoryTableLayout
            ? [
                ...scrollableIdentityCols,
                makeCol("storageCostUsd", storageCostHeaderLabel, "120px"),
                ...(showAlertsColumn
                    ? [
                        {
                            ...makeCol("inventoryAlert", "Alerts", "170px"),
                            align: "center" as const,
                        },
                    ]
                    : []),
            ]
            : [
                ...scrollableIdentityCols,
                ...ageBucketCols,
                {
                    key: "inboundUnits",
                    label: "Inbound Units",
                    width: "85px",
                    align: "center",
                    thClassName: heatmapHeaderClassName,
                    tdClassName: defaultTdClassName,
                },
                {
                    key: "unsellableUnits",
                    label: "Unfulfillable Units",
                    width: "95px",
                    align: "center",
                    thClassName: heatmapHeaderClassName,
                    tdClassName: defaultTdClassName,
                },
            // {
            //     key: "unitsSold",
            //     label: "Units Sold",
            //     width: "85px",
            //     align: "center",
            //     thClassName: heatmapHeaderClassName,
            //     tdClassName: defaultTdClassName,
            // },
                {
                    key: "salesLast30Days",
                    label: salesLast30DaysLabel,
                    width: "115px",
                    align: "center",
                    thClassName: heatmapHeaderClassName,
                    tdClassName: defaultTdClassName,
                },
                {
                    key: "coverageRatio",
                    label: "Coverage Ratio (in Months)",
                    width: "110px",
                    align: "center",
                    thClassName: heatmapHeaderClassName,
                    tdClassName: defaultTdClassName,
                },
                ...(showInventoryAlerts
                    ? [
                        {
                            key: "inventoryAlert",
                            label: "Inventory Alerts",
                            width: "175px",
                            align: "center" as const,
                            thClassName: heatmapHeaderClassName,
                        },
                    ]
                    : []),
            ];

        const layout = useCurrentInventoryTableLayout
            ? [
                { type: "single" as const, key: "sku" },
                { type: "single" as const, key: "salesRank" },
                { type: "group" as const, id: "currentInventory" },
                { type: "group" as const, id: "transitInventory" },
                { type: "group" as const, id: "totalSellableInventory" },
                { type: "group" as const, id: "unsellableInventory" },
                { type: "group" as const, id: "fbaBreakup" },
                { type: "group" as const, id: "salesCoverage" },
                { type: "single" as const, key: "storageCostUsd" },
                ...(showAlertsColumn
                    ? [{ type: "single" as const, key: "inventoryAlert" }]
                    : []),
            ]
            : [
                { type: "single" as const, key: "sku" },
                { type: "single" as const, key: "salesRank" },
                ...ageBucketCols.map((col) => ({
                    type: "single" as const,
                    key: col.key,
                })),
                {
                    type: "group" as const,
                    id: "sellable",
                },
                {
                    type: "single" as const,
                    key: "inboundUnits",
                },
                {
                    type: "single" as const,
                    key: "unsellableUnits",
                },
                {
                    type: "single" as const,
                    key: "salesLast30Days",
                },
                {
                    type: "single" as const,
                    key: "coverageRatio",
                },
                ...(showInventoryAlerts
                    ? [
                        {
                            type: "single" as const,
                            key: "inventoryAlert",
                        },
                    ]
                    : []),
            ];

        const getValue = (
            row: HeatmapTableRow,
            colKey: string,
            rowIndex: number
        ): React.ReactNode => {
            const textClass = "text-charcoal-500";
            const hasPercentageValue = (value: any) =>
                value !== null && value !== undefined && value !== "";
            const percentageCell = (value: any) =>
                hasPercentageValue(value) ? (
                    <span className={percentageRowTextClassName}>
                        {percentDisplay(value)}
                    </span>
                ) : (
                    ""
                );

            if (colKey === "sno") {
                if (row.isTotalRow || row.isPercentageRow) return "";
                return rowIndex + 1;
            }

            if (colKey === "productName") {
                const canClick =
                    !!onProductClick &&
                    !row.isTotalRow &&
                    !row.isPercentageRow &&
                    !row.isOthersRow &&
                    !!row.productName;

                if (!canClick) {
                    return (
                        <span
                            className={[
                                "block max-w-full truncate text-charcoal-500",
                                row.isOthersRow ? "text-green-500" : "",
                                row.isPercentageRow ? percentageRowTextClassName : "",
                            ].join(" ")}
                            title={row.productName}
                        >
                            {row.productName}
                        </span>
                    );
                }

                return (
                    <button
                        type="button"
                        onClick={() => onProductClick(row)}
                        title={row.productName}
                        className="block text-[12px] min-[1700px]:text-[14px] truncate text-left text-green-500 underline-offset-2"
                    >
                        {row.productName}
                    </button>
                );
            }

            if (colKey === "sku") {
                if (row.isTotalRow || row.isPercentageRow) return "";
                return (
                    <span
                        title={String(row.sku || "-")}
                        className="block w-full max-w-full truncate whitespace-nowrap text-left tabular-nums text-charcoal-500"
                    >
                        {row.sku || "-"}
                    </span>
                );
            }

            if (colKey === "salesRank") {
                if (row.isTotalRow || row.isPercentageRow || row.isOthersRow) return "";

                const rankNumber = parseSalesRankNumber(row.salesRank);
                if (!rankNumber) return "-";

                const rankMovement = getSalesRankMovement(row.salesRank, row.previousSalesRank);

                if (!rankMovement) {
                    return (
                        <div className="flex w-full items-center justify-center whitespace-nowrap tabular-nums">
                            {rankNumber.toLocaleString()}
                        </div>
                    );
                }

                return (
                    <div className="flex w-full items-center justify-between gap-2 whitespace-nowrap px-2">
                        <span className="tabular-nums text-charcoal-500">
                            {rankNumber.toLocaleString()}
                        </span>

                        <span
                            className={[
                                "inline-flex min-w-[52px] items-center justify-end text-right text-[10px] min-[1700px]:text-xs font-semibold tabular-nums",
                                rankMovement.isGood ? "text-[#5EA68E]" : "text-[#FF5C5C]",
                            ].join(" ")}
                            title="Sales rank movement from previous month"
                        >
                            <span className="w-3 shrink-0 text-center">
                                {rankMovement.isGood ? "▲" : "▼"}
                            </span>
                            <span>
                                {Math.abs(rankMovement.value).toLocaleString(undefined, {
                                    maximumFractionDigits: 0,
                                })}
                            </span>
                        </span>
                    </div>
                );
            }

            const bucket = buckets.find((b) => b.key === colKey);
            if (bucket) {
                const value = Number(row[bucket.key] || 0);

                const colorBaseTotal = row.isTotalRow
                    ? getHeatmapColorBaseTotal(row, buckets)
                    : bucketMaxValues[bucket.key] || value;

                if (row.isPercentageRow) {
                    const displayValue = percentDisplay(value);

                    return (
                        <div
                            title={`${bucket.label}: ${displayValue} of total`}
                            className={[
                                "absolute inset-0 flex h-full w-full items-center justify-center px-1 text-center text-xs font-semibold",
                                percentageRowTextClassName,
                                value > 0 ? "text-charcoal-500" : "text-charcoal-400",
                            ].join(" ")}
                            style={{ backgroundColor: "#F8F8F8" }}
                        >
                            {displayValue}
                        </div>
                    );
                }

                const percentage = colorBaseTotal ? (value / colorBaseTotal) * 100 : 0;

                return (
                    <div
                        title={`${row.productName} - ${bucket.label}: ${value.toLocaleString()} units (${percentage.toFixed(
                            1
                        )}%)`}
                        className="absolute inset-0 flex h-full w-full items-center justify-center px-1 text-center text-charcoal-500"
                        style={{
                            backgroundColor:
                                row.isTotalRow && value === 0
                                    ? "#EFEFEF"
                                    : getHeatColor(bucket.color, value, colorBaseTotal),
                        }}
                    >
                        {value === 0 ? "-" : value.toLocaleString()}
                    </div>
                );
            }

            if (colKey === "available") {
                if (row.isPercentageRow) return "";

                return (
                    <span className="text-charcoal-500">
                        {numberDisplay(row.available)}
                    </span>
                );
            }

            if (colKey === "fcTransfer") {
                if (row.isPercentageRow) return "";

                return (
                    <span className="text-charcoal-500">
                        {numberDisplay(row.fcTransfer)}
                    </span>
                );
            }

            if (colKey === "totalUnits") {
                if (row.isPercentageRow) {
                    return (
                        <span className={percentageRowTextClassName}>
                            {percentDisplay(row.totalUnits)}
                        </span>
                    );
                }

                const totalUnits = Number(
                    row.totalUnits ??
                    Number(row.available || 0) + Number(row.fcTransfer || 0)
                );

                // return totalUnits > 0 ? totalUnits.toLocaleString() : "0";
                return (
                    <span className="text-charcoal-500">
                        {totalUnits > 0 ? totalUnits.toLocaleString() : "0"}
                    </span>
                );
            }

            if (colKey === "inboundUnits") {
                if (row.isPercentageRow) return "";
                return (
                    <span className="text-charcoal-500">
                        {numberDisplay(row.inboundUnits)}
                    </span>
                );
            }

            if (colKey === "unsellableUnits") {
                if (row.isPercentageRow) {
                    return (
                        <span className={percentageRowTextClassName}>
                            {percentDisplay(row.unsellableUnits)}
                        </span>
                    );
                }

                return (
                    <span className="text-charcoal-500">
                        {numberDisplay(row.unsellableUnits)}
                    </span>
                );
            }

            if (colKey === "unitsSold") {
                if (row.isPercentageRow) return "";
                return (
                    <span className="text-charcoal-500">
                        {numberDisplay(row.unitsSold)}
                    </span>
                );
            }

            if (colKey === "salesLast30Days") {
                if (row.isPercentageRow) return "";

                return numberDisplay(getUnitSalesValue(row, unitSalesDataKey));
            }

            if (colKey === "coverageRatio") {
                if (row.isPercentageRow) return "";

                const coverageRatio = Number(row.coverageRatio ?? 0);

                return Number.isFinite(coverageRatio) && coverageRatio > 0
                    ? coverageRatio.toFixed(2)
                    : "-";
            }

            if (colKey === "coverageCurrentAndTransit") {
                if (row.isPercentageRow) return "";

                const coverageRatio = Number(row.coverageCurrentAndTransit ?? 0);

                if (Number.isFinite(coverageRatio) && coverageRatio > 0) {
                    return coverageRatio.toFixed(2);
                }

                const totalInStock = Number(
                    row.totalInStock ??
                    row.totalUnits ??
                    getCurrentFbaNumberValue(row) + Number(row.currentAwd || 0)
                );
                const totalInTransit = Number(
                    row.totalInTransit ??
                    Number(row.transitFba || row.fcTransfer || 0) +
                    Number(row.transitAwd || row.inboundUnits || 0)
                );
                const sales = getUnitSalesValue(row, unitSalesDataKey);

                return sales > 0
                    ? ((totalInStock + totalInTransit) / sales).toFixed(2)
                    : "-";
            }

            if (colKey === "currentFba") {
                if (row.isPercentageRow) return percentageCell(row.currentFba);
                return numberDisplay(getCurrentFbaValue(row));
            }

            if (colKey === "currentAwd") {
                if (row.isPercentageRow) return percentageCell(row.currentAwd);
                return numberDisplay(row.currentAwd);
            }

            if (colKey === "transitFba") {
                if (row.isPercentageRow) return percentageCell(row.transitFba);
                return numberDisplay(row.transitFba ?? row.fcTransfer);
            }

            if (colKey === "transitAwd") {
                if (row.isPercentageRow) return percentageCell(row.transitAwd);
                return numberDisplay(row.transitAwd ?? row.inboundUnits);
            }

            if (colKey === "totalInStock") {
                if (row.isPercentageRow) {
                    return percentageCell(
                        row.totalInStock ?? row.totalUnits ?? getCurrentFbaValue(row)
                    );
                }
                return numberDisplay(
                    row.totalInStock ??
                    row.totalUnits ??
                    getCurrentFbaNumberValue(row) + Number(row.currentAwd || 0)
                );
            }

            if (colKey === "totalInTransit") {
                if (row.isPercentageRow) return percentageCell(row.totalInTransit);
                return numberDisplay(
                    row.totalInTransit ??
                    Number(row.transitFba || row.fcTransfer || 0) +
                    Number(row.transitAwd || row.inboundUnits || 0)
                );
            }

            if (colKey === "unsellableFba") {
                if (row.isPercentageRow) {
                    return percentageCell(row.unsellableFba ?? row.unsellableUnits);
                }
                return numberDisplay(row.unsellableFba ?? row.unsellableUnits);
            }

            if (colKey === "unsellableAwd") {
                if (row.isPercentageRow) return percentageCell(row.unsellableAwd);
                return numberDisplay(row.unsellableAwd);
            }

            if (colKey === "storageCostUsd") {
                if (row.isPercentageRow) return "";

                const storageCost = Number(row.storageCostUsd || 0);

                return Number.isFinite(storageCost) && storageCost > 0
                    ? storageCost.toLocaleString(undefined, {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                    })
                    : "-";
            }

            if (colKey === "inventoryAlert") {
                if (row.isOthersRow || row.isTotalRow || row.isPercentageRow) return "";

                const alert = String(row.inventoryAlert || "").trim();

                if (!alert) return "-";

                const normalized = alert.toLowerCase();

                const badgeClassName = normalized.includes("high alert")
                    ? "bg-red-50 text-red-700 border-red-200"
                    : normalized.includes("high inventory coverage")
                        ? "bg-orange-50 text-orange-700 border-orange-200"
                        : normalized.includes("ageing")
                            ? "bg-yellow-50 text-yellow-700 border-yellow-200"
                            : "bg-slate-50 text-slate-700 border-slate-200";

                return (
                    <span
                        title={alert}
                        className={[
                            "inline-flex max-w-full whitespace-normal break-words text-center items-center justify-center rounded-md border px-2 py-1 text-xs leading-tight",
                            badgeClassName,
                        ].join(" ")}
                        style={{
                            overflowWrap: "anywhere",
                            wordBreak: "break-word",
                        }}
                    >
                        {alert}
                    </span>
                );
            }

            return "";
        };

        return {
            leftCols,
            groups: useCurrentInventoryTableLayout
                ? currentInventoryGroups
                : [sellableGroup],
            singleCols,
            layout,
            getValue,
        };
    }, [
        buckets,
        onProductClick,
        showInventoryAlerts,
        hasAnySalesRankMovement,
        bucketMaxValues,
        showSellableBreakdown,
        salesLast30DaysLabel,
        unitSalesDataKey,
        useCurrentInventoryTableLayout,
        storageCostHeaderLabel,
    ]);

    const currentInventoryCollapsedState = useMemo(
        () => ({
            currentInventory: !isInventoryDetailsExpanded,
            transitInventory: !isInventoryDetailsExpanded,
            totalSellableInventory: false,
            unsellableInventory: !isSalesCoverageDetailsExpanded,
            fbaBreakup: !isSalesCoverageDetailsExpanded,
            salesCoverage: false,
        }),
        [isInventoryDetailsExpanded, isSalesCoverageDetailsExpanded]
    );

    const handleCurrentInventoryCollapsedChange = (
        next: Record<string, boolean>
    ) => {
        if (next.totalSellableInventory !== false) {
            setIsInventoryDetailsExpanded((prev) => !prev);
        }

        if (next.salesCoverage !== false) {
            setIsSalesCoverageDetailsExpanded((prev) => !prev);
        }
    };

    const handleDownloadExcel = () => {
        if (onDownloadInventoryExcel) {
            onDownloadInventoryExcel();
            return;
        }

        const excelSourceRows = useCurrentInventoryTableLayout ? data : displayRows;
        const excelRows =
            unitSalesDataKey === "salesLast30Days"
                ? excelSourceRows
                : excelSourceRows.map((row) => ({
                    ...row,
                    salesLast30Days: getUnitSalesValue(row, unitSalesDataKey),
                }));

        exportAgeingRiskHeatmapExcel({
            filename: excelFilename,
            titleLine: excelTitleLine || title,
            countryLabel: excelCountryLabel,
            platformLabel: excelPlatformLabel,
            periodLabel: excelPeriodLabel,
            companyName: excelCompanyName,
            brandName: excelBrandName,
            buckets,
            dataRows: excelRows,
            showInventoryAlerts,
            salesLast30DaysLabel,
            useCurrentInventoryTableLayout,
            unitSalesDataKey,
            storageCostCurrencySymbol,
        });
    };

    return (
        <div className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
            <div className="mb-5 flex items-start justify-between gap-4">
                <div>
                    <PageBreadcrumb
                        pageTitle={title}
                        variant="page"
                        align="left"
                        textSize="2xl"
                    />
                </div>

                <div className="flex items-center gap-2">
                    {canCollapse && (
                        <button
                            type="button"
                            onClick={() => setIsExpanded((prev) => !prev)}
                            title={isExpanded ? "Collapse rows" : "Expand rows"}
                            aria-label={isExpanded ? "Collapse rows" : "Expand rows"}
                            className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-600 shadow-sm transition hover:bg-slate-50 hover:text-slate-900"
                        >
                            {isExpanded ? (
                                <RiCollapseDiagonalFill className="h-4 w-4" />
                            ) : (
                                <RiExpandDiagonalFill className="h-4 w-4" />
                            )}
                        </button>
                    )}

                    {useCurrentInventoryTableLayout && (
                        <button
                            type="button"
                            onClick={handleToggleAllCurrentInventoryColumns}
                            title={
                                areAllCurrentInventoryColumnsExpanded
                                    ? "Collapse all columns"
                                    : "Expand all columns"
                            }
                            aria-label={
                                areAllCurrentInventoryColumnsExpanded
                                    ? "Collapse all columns"
                                    : "Expand all columns"
                            }
                            className="flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-600 shadow-sm transition hover:bg-slate-50 hover:text-slate-900"
                        >
                            {areAllCurrentInventoryColumnsExpanded ? (
                                <RiLayoutColumnLine className="h-4 w-4" />
                            ) : (
                                <RiLayoutColumnFill className="h-4 w-4" />
                            )}
                        </button>
                    )}

                    {/* {onDownloadInventoryExcel && (
                        <DownloadIconButton
                            onClick={onDownloadInventoryExcel}
                            disabled={!canDownloadInventoryExcel}
                        />
                    )} */}

                    {showExcelDownload && (
                        <DownloadIconButton
                            onClick={handleDownloadExcel}
                            disabled={
                                onDownloadInventoryExcel
                                    ? !canDownloadInventoryExcel
                                    : !data.length || !buckets.length
                            }
                        />
                    )}
                </div>
            </div>

            <div className="rounded-xl w-full overflow-x-auto">
                <GroupedCollapsibleTable<HeatmapTableRow>
                    rows={displayRows}
                    leftCols={tableConfig.leftCols}
                    groups={tableConfig.groups}
                    singleCols={tableConfig.singleCols}
                    layout={tableConfig.layout}
                    initialCollapsed={
                        useCurrentInventoryTableLayout
                            ? undefined
                            : {
                                sellable: true,
                            }
                    }
                    collapsedState={
                        useCurrentInventoryTableLayout
                            ? currentInventoryCollapsedState
                            : undefined
                    }
                    onCollapsedChange={
                        useCurrentInventoryTableLayout
                            ? handleCurrentInventoryCollapsedChange
                            : undefined
                    }
                    getGroupToggleCollapsedState={
                        useCurrentInventoryTableLayout
                            ? (groupId, defaultIsCollapsed) =>
                                groupId === "totalSellableInventory"
                                    ? !isInventoryDetailsExpanded
                                    : groupId === "salesCoverage"
                                        ? !isSalesCoverageDetailsExpanded
                                        : defaultIsCollapsed
                            : undefined
                    }
                    getValue={tableConfig.getValue}
                    getRowKey={(row, index) =>
                        `${row.sku || row.productName || "row"}-${index}`
                    }
                    tableClassName="ageing-risk-heatmap-table w-full table-fixed border-collapse bg-white text-sm text-charcoal-500"
                    headerRow1ClassName="bg-[#5EA68E] text-[#f8edcf] !text-[12px] min-[1700px]:!text-[14px]"
                    headerRow2ClassName="bg-[#5EA68E] text-[#f8edcf] !text-[12px] min-[1700px]:!text-[14px]"
                    getRowClassName={(row) =>
                        row.isTotalRow
                            ? "bg-[#EFEFEF] font-semibold"
                            : row.isPercentageRow
                                ? "bg-[#F8F8F8] font-semibold"
                                : row.isOthersRow && !isExpanded
                                    ? "cursor-pointer"
                                : ""
                    }
                    onRowClick={(row) => {
                        if (row.isOthersRow && !isExpanded) {
                            setIsExpanded(true);
                        }
                    }}
                    preserveColumnWidths={
                        useCurrentInventoryTableLayout ? "responsive" : false
                    }
                    stickyLeftWidthMode="declared"
                    stickyLeftDividerMode="leading"
                    showStickyLeftOuterBorder
                />
            </div>
        </div>
    );
};

export default AgeingRiskHeatmap;
