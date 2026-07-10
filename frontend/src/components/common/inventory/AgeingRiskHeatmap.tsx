import React, { useMemo, useState } from "react";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
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

    // Sellable Units should come from backend "available" column
    available?: number;

    // FC Transfer units from backend "fc-transfer"
    fcTransfer?: number;

    // keep this only if used elsewhere
    totalUnits?: number;

    inboundUnits?: number;
    unsellableUnits?: number;
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

    [bucketKey: string]: string | number | boolean | undefined;
};

type AgeingRiskHeatmapProps = {
    title?: string;
    subtitle?: string;
    data: AgeingRiskHeatmapRow[];
    buckets: AgeingBucket[];
    defaultVisibleRows?: number;
    salesLast30DaysLabel?: string;
    onProductClick?: (row: AgeingRiskHeatmapRow) => void;

    onDownloadInventoryExcel?: () => void;
    canDownloadInventoryExcel?: boolean;
    showInventoryAlerts?: boolean;

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

const parseSalesRankNumber = (value: any) => {
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

const getSalesRankDelta = (currentRankValue: any, previousRankValue: any) => {
    const currentRank = parseSalesRankNumber(currentRankValue);
    const previousRank = parseSalesRankNumber(previousRankValue);

    if (!currentRank || !previousRank) return null;

    // Sales Rank me lower rank better hota hai.
    // Example: previous 300, current 200 => improvement +33.33%
    const deltaPct = ((previousRank - currentRank) / Math.abs(previousRank)) * 100;

    return {
        value: deltaPct,
        isGood: deltaPct > 0,
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

const hasSellableBreakdown = (rows: AgeingRiskHeatmapRow[]) => {
    return rows.some(
        (row) =>
            Number(row.available || 0) > 0 ||
            Number(row.fcTransfer || 0) > 0
    );
};

const buildAggregateRow = (
    label: string,
    rows: AgeingRiskHeatmapRow[],
    buckets: AgeingBucket[],
    flags?: {
        isOthersRow?: boolean;
        isTotalRow?: boolean;
    }
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
    aggregate.salesRank = "";

    aggregate.unitsSold = rows.reduce(
        (sum, row) => sum + Number(row.unitsSold || 0),
        0
    );

    aggregate.salesLast30Days = rows.reduce(
        (sum, row) => sum + Number(row.salesLast30Days || 0),
        0
    );

    aggregate.inventoryAlert = "";

    // ✅ For Others only:
    // Coverage Ratio = aggregated available / aggregated Sales Last 30 Days
    if (flags?.isOthersRow) {
        const totalAvailable = Number(aggregate.available || 0);
        const totalSalesLast30Days = Number(aggregate.salesLast30Days || 0);

        aggregate.coverageRatio =
            totalSalesLast30Days > 0
                ? totalAvailable / totalSalesLast30Days
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

const getUnitSalesSortValue = (row: AgeingRiskHeatmapRow) => {
    return Number(row.salesLast30Days ?? row.unitsSold ?? 0);
};

const AgeingRiskHeatmap: React.FC<AgeingRiskHeatmapProps> = ({
    title = "Ageing Risk Heatmap",
    subtitle = "Quickly identify products with old inventory",
    data,
    buckets,
    defaultVisibleRows = 9,
    salesLast30DaysLabel = "Unit Sales in Last 30 Days",
    onProductClick,
    onDownloadInventoryExcel,
    canDownloadInventoryExcel = false,
    showInventoryAlerts = true,
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
        const backendPercentageRow = data.find((row) => row.isPercentageRow);

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

            return (
                !row.isPercentageRow &&
                !row.isTotalRow &&
                productName !== "total" &&
                productName !== "grand total" &&
                sku !== "total" &&
                sku !== "grand total" &&
                hasAnyDisplayBucketValue(row)
            );
        });

        const sortedData = [...productRows].sort((a, b) => {
            const aUnitSales = getUnitSalesSortValue(a);
            const bUnitSales = getUnitSalesSortValue(b);

            return bUnitSales - aUnitSales;
        });

        const calculatedTotalRow = buildAggregateRow("Total", sortedData, buckets, {
            isTotalRow: true,
        });

        const totalRow = backendTotalRow
            ? {
                ...calculatedTotalRow,
                ...backendTotalRow,

                // ✅ keep backend coverage ratio only
                coverageRatio: Number(backendTotalRow.coverageRatio ?? 0),
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
            }

            if (typeof inventoryAgeSummary.unfulfillable_total === "number") {
                totalRow.unsellableUnits = inventoryAgeSummary.unfulfillable_total;
            }

            if (typeof inventoryAgeSummary.current_month_units_sold_total === "number") {
                totalRow.unitsSold = inventoryAgeSummary.current_month_units_sold_total;
            }
        }

        const percentageRow =
            backendPercentageRow ||
            ({
                productName: "% of Total",
                sku: "-",
                isPercentageRow: true,
            } as HeatmapTableRow);

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
        });

        return [...mainRows, othersRow, totalRow, percentageRow] as HeatmapTableRow[];
    }, [data, buckets, canCollapse, isExpanded, defaultVisibleRows, inventoryAgeSummary]);

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


    const hasAnySalesRankDelta = useMemo(() => {
        return displayRows.some((row) => {
            if (row.isTotalRow || row.isPercentageRow || row.isOthersRow) {
                return false;
            }

            return !!getSalesRankDelta(row.salesRank, row.previousSalesRank);
        });
    }, [displayRows]);

    const tableConfig = useMemo(() => {
        const heatmapHeaderClassName =
            "!px-1 !py-2 !h-auto !whitespace-normal !break-words !text-center !leading-tight !overflow-visible !text-[12px] min-[1700px]:!text-[14px]";
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

        const leftCols: LeafCol<HeatmapTableRow>[] = [
            {
                key: "sno",
                label: "S.No.",
                width: "48px",
                align: "center",
                thClassName: heatmapHeaderClassName,
                tdClassName: defaultTdClassName,
            },
            {
                key: "productName",
                label: "Product Name",
                width: "145px",
                align: "left",
                thClassName: heatmapHeaderClassName,
                tdClassName: defaultTdClassName,
            },
            {
                key: "sku",
                label: "SKU",
                width: "120px",
                align: "left",
                thClassName: heatmapHeaderClassName,
                tdClassName: defaultTdClassName,

            },
            {
                key: "salesRank",
                label: "Sales Rank",
                width: hasAnySalesRankDelta ? "140px" : "90px",
                align: "center",
                thClassName: heatmapHeaderClassName,
                tdClassName: defaultTdClassName,
            },
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

        const singleCols: LeafCol<HeatmapTableRow>[] = [
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

        const layout = [
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

                const delta = getSalesRankDelta(row.salesRank, row.previousSalesRank);

                if (!delta) {
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
                                delta.isGood ? "text-[#5EA68E]" : "text-[#FF5C5C]",
                            ].join(" ")}
                            title="Compared with previous month sales rank"
                        >
                            <span className="w-3 shrink-0 text-center">
                                {delta.isGood ? "▲" : "▼"}
                            </span>
                            <span>{Math.abs(delta.value).toFixed(2)}%</span>
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

                return numberDisplay(row.salesLast30Days);
            }

            if (colKey === "coverageRatio") {
                if (row.isPercentageRow) return "";

                const coverageRatio = Number(row.coverageRatio ?? 0);

                return Number.isFinite(coverageRatio) && coverageRatio > 0
                    ? coverageRatio.toFixed(2)
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
            groups: [sellableGroup],
            singleCols,
            layout,
            getValue,
        };
    }, [
        buckets,
        onProductClick,
        showInventoryAlerts,
        hasAnySalesRankDelta,
        bucketMaxValues,
        showSellableBreakdown,
        salesLast30DaysLabel,
    ]);

    const handleDownloadExcel = () => {
        if (onDownloadInventoryExcel) {
            onDownloadInventoryExcel();
            return;
        }

        exportAgeingRiskHeatmapExcel({
            filename: excelFilename,
            titleLine: excelTitleLine || title,
            countryLabel: excelCountryLabel,
            platformLabel: excelPlatformLabel,
            periodLabel: excelPeriodLabel,
            companyName: excelCompanyName,
            brandName: excelBrandName,
            buckets,
            dataRows: displayRows,
            showInventoryAlerts,
            salesLast30DaysLabel,
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
                    initialCollapsed={{
                        sellable: true,
                    }}
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
                                : ""
                    }
                />
            </div>
        </div>
    );
};

export default AgeingRiskHeatmap;