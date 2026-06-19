import React from "react";

export type AgeingBucket = {
    key: string;
    label: string;
    color: string;
};

export type AgeingRiskHeatmapRow = {
    productName: string;
    sku?: string;
    totalUnits?: number;
    coverageRatio?: number;
    [bucketKey: string]: string | number | undefined;
};

type AgeingRiskHeatmapProps = {
    title?: string;
    subtitle?: string;
    data: AgeingRiskHeatmapRow[];
    buckets: AgeingBucket[];
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

const AgeingRiskHeatmap: React.FC<AgeingRiskHeatmapProps> = ({
    title = "Ageing Risk Heatmap",
    subtitle = "Quickly identify products with old inventory",
    data,
    buckets,
}) => {
    return (
        <div className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
            <div className="mb-5 flex items-start justify-between gap-4">
                <div>
                    <h3 className="text-lg font-extrabold uppercase text-slate-900">
                        {title}
                    </h3>
                    <p className="mt-1 text-sm text-slate-600">{subtitle}</p>
                </div>

                <div className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                    {data.length} Products
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
                                Total Units
                            </th>

                            <th className="whitespace-nowrap px-3 py-2 text-center font-semibold">
                                Coverage Ratio
                            </th>
                        </tr>
                    </thead>

                    <tbody>
                        {data.map((row) => {
                            const calculatedTotal = buckets.reduce(
                                (sum, bucket) => sum + Number(row[bucket.key] || 0),
                                0
                            );

                            const totalUnits = Number(row.totalUnits ?? calculatedTotal);
                            const coverageRatio = Number(row.coverageRatio ?? 0);

                            return (
                                <tr key={row.sku || row.productName}>
                                    <td className="border-t border-slate-100 px-3 py-2 text-left font-semibold text-slate-900">
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
                                                    className="flex h-12 items-center justify-center text-[11px] font-semibold text-slate-900"
                                                    style={{
                                                        background: getHeatColor(
                                                            bucket.color,
                                                            value,
                                                            totalUnits
                                                        ),
                                                    }}
                                                >
                                                    {value.toLocaleString()}
                                                </div>
                                            </td>
                                        );
                                    })}

                                    <td className="border-t border-slate-100 px-3 py-2 text-center font-bold text-slate-900">
                                        {totalUnits.toLocaleString()}
                                    </td>

                                    <td className="border-t border-slate-100 px-3 py-2 text-center font-semibold text-slate-900">
                                        {Number.isFinite(coverageRatio) && coverageRatio > 0
                                            ? `${coverageRatio.toFixed(2)} Months`
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