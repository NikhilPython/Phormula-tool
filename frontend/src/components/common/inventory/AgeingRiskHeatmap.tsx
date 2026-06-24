import React, { useMemo, useState } from "react";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import PageBreadcrumb from "../PageBreadCrumb";
import DataTable, { ColumnDef, Row } from "@/components/ui/table/DataTable";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";

export type AgeingBucket = {
    key: string;
    label: string;
    color: string;
};

export type AgeingRiskHeatmapRow = {
    productName: string;
    sku?: string;
    totalUnits?: number;
    unsellableUnits?: number;
    coverageRatio?: number;
    isOthersRow?: boolean;
    isTotalRow?: boolean;
    [bucketKey: string]: string | number | boolean | undefined;
};

type AgeingRiskHeatmapProps = {
    title?: string;
    subtitle?: string;
    data: AgeingRiskHeatmapRow[];
    buckets: AgeingBucket[];
    defaultVisibleRows?: number;

    onProductClick?: (row: AgeingRiskHeatmapRow) => void;

    onDownloadInventoryExcel?: () => void;
    canDownloadInventoryExcel?: boolean;
};

type HeatmapTableRow = AgeingRiskHeatmapRow & Row;

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

    aggregate.totalUnits = buckets.reduce(
        (sum, bucket) => sum + Number(aggregate[bucket.key] || 0),
        0
    );

    aggregate.unsellableUnits = rows.reduce(
        (sum, row) => sum + Number(row.unsellableUnits || 0),
        0
    );

    const weightedCoverageTotal = rows.reduce((sum, row) => {
        const calculatedTotal = buckets.reduce(
            (bucketSum, bucket) => bucketSum + Number(row[bucket.key] || 0),
            0
        );

        const rowTotal = Number(row.totalUnits ?? calculatedTotal);
        const coverageRatio = Number(row.coverageRatio ?? 0);

        if (!rowTotal || !Number.isFinite(coverageRatio)) return sum;

        return sum + coverageRatio * rowTotal;
    }, 0);

    aggregate.coverageRatio = aggregate.totalUnits
        ? weightedCoverageTotal / aggregate.totalUnits
        : 0;

    return aggregate;
};

const AgeingRiskHeatmap: React.FC<AgeingRiskHeatmapProps> = ({
    title = "Ageing Risk Heatmap",
    subtitle = "Quickly identify products with old inventory",
    data,
    buckets,
    defaultVisibleRows = 9,
    onProductClick,
    onDownloadInventoryExcel,
    canDownloadInventoryExcel = false,
}) => {
    const [isExpanded, setIsExpanded] = useState(false);

    const canCollapse = data.length > defaultVisibleRows;

    const displayRows = useMemo<HeatmapTableRow[]>(() => {
        const sortedData = [...data].sort((a, b) => {
            const aTotal =
                Number(a.totalUnits) ||
                buckets.reduce((sum, bucket) => sum + Number(a[bucket.key] || 0), 0);

            const bTotal =
                Number(b.totalUnits) ||
                buckets.reduce((sum, bucket) => sum + Number(b[bucket.key] || 0), 0);

            return bTotal - aTotal; // descending
        });

        const totalRow = buildAggregateRow("Total", sortedData, buckets, {
            isTotalRow: true,
        });

        if (!canCollapse || isExpanded) {
            return [...sortedData, totalRow] as HeatmapTableRow[];
        }

        const mainRows = sortedData.slice(0, defaultVisibleRows);
        const otherRows = sortedData.slice(defaultVisibleRows);

        if (!otherRows.length) {
            return [...mainRows, totalRow] as HeatmapTableRow[];
        }

        const othersRow = buildAggregateRow("Others", otherRows, buckets, {
            isOthersRow: true,
        });

        return [...mainRows, othersRow, totalRow] as HeatmapTableRow[];
    }, [data, buckets, canCollapse, isExpanded, defaultVisibleRows]);

    const columns = useMemo<ColumnDef<HeatmapTableRow>[]>(() => {
        const heatmapHeaderClassName =
            "!px-1 !py-2 !h-auto !whitespace-normal !break-words !text-center !leading-tight !overflow-visible";

        const bucketColumns: ColumnDef<HeatmapTableRow>[] = buckets.map((bucket) => ({
            key: bucket.key,
            width: "72px",
            header: bucket.label,
            headerClassName: heatmapHeaderClassName,
            cellClassName: "!p-0",
            render: (row) => {
                const calculatedTotal = buckets.reduce(
                    (sum, b) => sum + Number(row[b.key] || 0),
                    0
                );

                const totalUnits = Number(row.totalUnits ?? calculatedTotal);
                const value = Number(row[bucket.key] || 0);
                const percentage = totalUnits ? (value / totalUnits) * 100 : 0;

                return (
                    <div
                        title={`${row.productName} - ${bucket.label}: ${value.toLocaleString()} units (${percentage.toFixed(
                            1
                        )}%)`}
                        className="flex h-10 items-center justify-center px-1 text-center text-xs text-charcoal-500"
                        style={{
                            background:
                                row.isTotalRow && value === 0
                                    ? "#EFEFEF"
                                    : getHeatColor(bucket.color, value, totalUnits),
                        }}
                    >
                        {value === 0 ? "-" : value.toLocaleString()}
                    </div>
                );
            },
        }));

        return [
            {
                key: "sno",
                header: "S.No.",
                width: "48px",
                headerClassName: heatmapHeaderClassName,
                render: (row, _value, rowIndex) => {
                    if (row.isTotalRow) return "";
                    return <span>{rowIndex + 1}</span>;
                },
            },
            {
                key: "productName",
                header: "Product Name",
                width: "135px",
                headerClassName: heatmapHeaderClassName,
                cellClassName:
                    "text-left text-sm text-charcoal-500 overflow-hidden",
                render: (row) => {
                    const canClick =
                        !!onProductClick &&
                        !row.isTotalRow &&
                        !row.isOthersRow &&
                        !!row.productName;

                    if (!canClick) {
                        return (
                            <span
                                className="block max-w-full truncate"
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
                            className="block text-xs 2xl:text-sm  truncate text-left font-medium text-green-500 underline-offset-2"
                        >
                            {row.productName}
                        </button>
                    );
                },
            },
            {
                key: "sku",
                header: "SKU",
                width: "105px",
                headerClassName: heatmapHeaderClassName,
                cellClassName: "text-center text-xs 2xl:text-sm text-charcoal-500 whitespace-normal break-words",
                render: (row) => {
                    if (row.isTotalRow) return "";

                    return <span>{row.sku || "-"}</span>;
                },
            },
            ...bucketColumns,
            {
                key: "totalUnits",
                header: "Sellable Units",
                width: "85px",
                headerClassName: heatmapHeaderClassName,
                render: (row) => {
                    const calculatedTotal = buckets.reduce(
                        (sum, bucket) => sum + Number(row[bucket.key] || 0),
                        0
                    );

                    const totalUnits = Number(row.totalUnits ?? calculatedTotal);

                    return <span>{totalUnits.toLocaleString()}</span>;
                },
            },
            {
                key: "unsellableUnits",
                header: "Unfulfillable Units",
                width: "95px",
                headerClassName: heatmapHeaderClassName,
                render: (row) => {
                    const unsellableUnits = Number(row.unsellableUnits || 0);

                    return (
                        <span>
                            {unsellableUnits > 0
                                ? unsellableUnits.toLocaleString()
                                : "-"}
                        </span>
                    );
                },
            },
            {
                key: "coverageRatio",
                header: "Coverage Ratio (in Months)",
                width: "110px",
                headerClassName: heatmapHeaderClassName,
                render: (row) => {
                    const coverageRatio = Number(row.coverageRatio ?? 0);

                    return (
                        <span>
                            {Number.isFinite(coverageRatio) && coverageRatio > 0
                                ? coverageRatio.toFixed(2)
                                : "-"}
                        </span>
                    );
                },
            },
        ];
    }, [buckets, onProductClick]);

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

                    {onDownloadInventoryExcel && (
                        <DownloadIconButton
                            onClick={onDownloadInventoryExcel}
                            disabled={!canDownloadInventoryExcel}
                        />
                    )}
                </div>
            </div>

            <DataTable<HeatmapTableRow>
                columns={columns}
                data={displayRows}
                paginate={false}
                scrollY={false}
                stickyHeader
                zebra={false}
                showCellTitle={false}
                tableClassName="ageing-risk-heatmap-table w-full table-fixed text-sm"
                rowClassName={(row) =>
                    row.isTotalRow
                        ? "bg-[#EFEFEF] font-semibold"
                        : row.isOthersRow
                            ? ""
                            : ""
                }
            />
        </div>
    );
};

export default AgeingRiskHeatmap;