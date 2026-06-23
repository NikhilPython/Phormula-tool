import React, { useMemo } from "react";
import clsx from "clsx";

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
  isOthersRow?: boolean;
  isTotalRow?: boolean;
  [bucketKey: string]: string | number | boolean | undefined;
};

type AgeingRiskHeatmapTableProps = {
  data: AgeingRiskHeatmapRow[];
  buckets: AgeingBucket[];
  defaultVisibleRows?: number;
  isExpanded?: boolean;
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

const AgeingRiskHeatmapTable: React.FC<AgeingRiskHeatmapTableProps> = ({
  data,
  buckets,
  defaultVisibleRows = 10,
  isExpanded = false,
}) => {
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
    <div className="w-full overflow-x-auto [-webkit-overflow-scrolling:touch]">
      <table
        className={clsx(
          "border-separate border-spacing-0",
          "text-xs 2xl:text-sm text-slate-700",
          "min-w-max w-max lg:w-full",
          "table-fixed"
        )}
      >
        <colgroup>
          <col className="min-w-[200px]" />

          {buckets.map((bucket) => (
            <col key={bucket.key} className="min-w-[150px]" />
          ))}

          <col className="min-w-[130px]" />
          <col className="min-w-[190px]" />
        </colgroup>

        <thead>
          <tr>
            <th
              className={clsx(
                "sticky top-0 z-20",
                "bg-[#5EA68E] text-yellow-200 font-bold",
                "border-b border-r border-gray-300",
                "px-3 py-2 text-left align-middle whitespace-nowrap"
              )}
            >
              Product Name
            </th>

            {buckets.map((bucket) => (
              <th
                key={bucket.key}
                className={clsx(
                  "sticky top-0 z-20",
                  "bg-[#5EA68E] text-yellow-200 font-bold",
                  "border-b border-r border-gray-300",
                  "px-3 py-2 text-center align-middle whitespace-nowrap"
                )}
              >
                <div className="flex items-center justify-center gap-1.5">
                  <span
                    className="h-2 w-2 rounded-full"
                    style={{ backgroundColor: bucket.color }}
                  />
                  <span>{bucket.label}</span>
                </div>
              </th>
            ))}

            <th
              className={clsx(
                "sticky top-0 z-20",
                "bg-[#5EA68E] text-yellow-200 font-bold",
                "border-b border-r border-gray-300",
                "px-3 py-2 text-center align-middle whitespace-nowrap"
              )}
            >
              Total Units
            </th>

            <th
              className={clsx(
                "sticky top-0 z-20",
                "bg-[#5EA68E] text-yellow-200 font-bold",
                "border-b border-r-0 border-gray-300",
                "px-3 py-2 text-center align-middle whitespace-nowrap"
              )}
            >
              Coverage Ratio(in Months)
            </th>
          </tr>
        </thead>

        <tbody>
          {displayRows.length === 0 && (
            <tr>
              <td
                colSpan={buckets.length + 3}
                className="px-3 py-8 text-center text-slate-400"
              >
                No data found.
              </td>
            </tr>
          )}

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
                className={clsx(
                  "h-[40px] transition-colors",
                  row.isTotalRow && "bg-slate-100",
                  row.isOthersRow && "bg-slate-50"
                )}
              >
                <td
                  className={clsx(
                    "border-b border-r border-[#e1e5ea]",
                    "px-3 py-2 align-middle text-left min-w-0",
                    "whitespace-normal break-words text-slate-900",
                    isAggregateRow ? "font-bold" : "font-semibold"
                  )}
                >
                  <div className="leading-snug max-w-[220px] sm:max-w-[280px] lg:max-w-none">
                    {row.productName}
                  </div>
                </td>

                {buckets.map((bucket) => {
                  const value = Number(row[bucket.key] || 0);
                  const percentage = totalUnits
                    ? (value / totalUnits) * 100
                    : 0;

                  return (
                    <td
                      key={bucket.key}
                      className="border-b border-r border-[#e1e5ea] p-0 align-middle text-center min-w-0 whitespace-nowrap"
                      title={`${row.productName} - ${
                        bucket.label
                      }: ${value.toLocaleString()} units (${percentage.toFixed(
                        1
                      )}%)`}
                    >
                      <div
                        className={clsx(
                          "flex h-[40px] items-center justify-center px-3 py-2",
                          "text-slate-900",
                          isAggregateRow ? "font-bold" : "font-semibold"
                        )}
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

                <td className="border-b border-r border-[#e1e5ea] px-3 py-2 align-middle text-center whitespace-nowrap font-bold text-slate-900">
                  {totalUnits.toLocaleString()}
                </td>

                <td className="border-b border-r-0 border-[#e1e5ea] px-3 py-2 align-middle text-center whitespace-nowrap font-semibold text-slate-900">
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
  );
};

export default AgeingRiskHeatmapTable;