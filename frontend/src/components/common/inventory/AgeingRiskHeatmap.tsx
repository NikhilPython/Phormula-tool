import React, { useMemo, useState } from "react";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import PageBreadcrumb from "../PageBreadCrumb";

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
        sku: label.toUpperCase().replace(/\s+/g, "_"),
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

    // Weighted average coverage ratio for aggregate rows.
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
    defaultVisibleRows = 10,
}) => {
    const [isExpanded, setIsExpanded] = useState(false);

    const canCollapse = data.length > defaultVisibleRows;

    const displayRows = useMemo(() => {
        const totalRow = buildAggregateRow("Total", data, buckets, {
            isTotalRow: true,
        });

        if (!canCollapse || isExpanded) {
            return [...data, totalRow];
        }

        const mainRows = data.slice(0, defaultVisibleRows);
        const otherRows = data.slice(defaultVisibleRows);

        if (!otherRows.length) {
            return [...mainRows, totalRow];
        }

        const othersRow = buildAggregateRow("Others", otherRows, buckets, {
            isOthersRow: true,
        });

        return [...mainRows, othersRow, totalRow];
    }, [data, buckets, canCollapse, isExpanded, defaultVisibleRows]);

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
                </div>
            </div>

            <div className="overflow-x-auto">
                <table className="w-full min-w-[760px] border-separate border-spacing-0 text-xs">
                    <thead>
                        <tr className="text-slate-600">
                            <th className="px-3 py-2 text-left font-semibold">
                                Product Name
                            </th>

                            {buckets.map((bucket) => (
                                <th
                                    key={bucket.key}
                                    className="px-3 py-2 text-center font-semibold"
                                >
                                    <div className="flex items-center justify-center gap-1.5 whitespace-nowrap">
                                        <span
                                            className="h-2 w-2 rounded-full"
                                            style={{ backgroundColor: bucket.color }}
                                        />
                                        {bucket.label}
                                    </div>
                                </th>
                            ))}

                            <th className="whitespace-nowrap px-3 py-2 text-center font-semibold">
                                Sellable Units
                            </th>

                            <th className="whitespace-nowrap px-3 py-2 text-center font-semibold">
                                Unsellable Units
                            </th>

                            <th className="whitespace-nowrap px-3 py-2 text-center font-semibold">
                                Coverage Ratio(in Months)
                            </th>
                        </tr>
                    </thead>

                    <tbody>
                        {displayRows.map((row) => {
                            const calculatedTotal = buckets.reduce(
                                (sum, bucket) => sum + Number(row[bucket.key] || 0),
                                0
                            );

                            const totalUnits = Number(row.totalUnits ?? calculatedTotal);
                            const coverageRatio = Number(row.coverageRatio ?? 0);

                            const isAggregateRow = row.isOthersRow || row.isTotalRow;

                            return (
                                <tr
                                    key={row.sku || row.productName}
                                    className={
                                        row.isTotalRow
                                            ? "bg-slate-100"
                                            : row.isOthersRow
                                                ? ""
                                                : ""
                                    }
                                >
                                    <td
                                        className={`border-t border-slate-100 px-3 py-2 text-left text-slate-900 ${isAggregateRow ? "font-bold" : "font-semibold"
                                            }`}
                                    >
                                        {row.productName}
                                    </td>

                                    {buckets.map((bucket) => {
                                        const value = Number(row[bucket.key] || 0);
                                        const percentage = totalUnits
                                            ? (value / totalUnits) * 100
                                            : 0;

                                        return (
                                            <td
                                                key={bucket.key}
                                                className="border-t border-slate-100 px-0 py-0 text-center"
                                                title={`${row.productName} - ${bucket.label}: ${value.toLocaleString()} units (${percentage.toFixed(1)}%)`}
                                            >
                                                <div
                                                    className={`flex h-12 items-center justify-center text-[11px] text-slate-900 ${isAggregateRow ? "font-bold" : "font-semibold"
                                                        }`}
                                                    style={{
                                                        background: getHeatColor(
                                                            bucket.color,
                                                            value,
                                                            totalUnits
                                                        ),
                                                    }}
                                                >
                                                    {value === 0 ? "-" : value.toLocaleString()}
                                                </div>
                                            </td>
                                        );
                                    })}

                                    <td className="border-t border-slate-100 px-3 py-2 text-center font-bold text-slate-900">
                                        {totalUnits.toLocaleString()}
                                    </td>

                                    <td className="border-t border-slate-100 px-3 py-2 text-center font-bold text-slate-900">
                                        {Number(row.unsellableUnits || 0) > 0
                                            ? Number(row.unsellableUnits || 0).toLocaleString()
                                            : "-"}
                                    </td>

                                    <td className="border-t border-slate-100 px-3 py-2 text-center font-semibold text-slate-900">
                                        {Number.isFinite(coverageRatio) && coverageRatio > 0
                                            ? coverageRatio.toFixed(2)
                                            : "-"}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default AgeingRiskHeatmap;