"use client";

import * as React from "react";
import clsx from "clsx";

export type Row = Record<string, React.ReactNode>;

type BaseColumn<T extends Row> = {
    key: keyof T | string;
    header: React.ReactNode;
    width?: string;
    render?: (row: T, value: React.ReactNode, rowIndex: number) => React.ReactNode;
    cellClassName?: string;
    headerClassName?: string; // ✅ ADD THIS
};

type GroupColumn<T extends Row> = {
    groupHeader: React.ReactNode;
    columns: Array<{
        key: keyof T | string;
        header: React.ReactNode;
        width?: string;
        render?: (row: T, value: React.ReactNode, rowIndex: number) => React.ReactNode;
        cellClassName?: string;
        headerClassName?: string; // ✅ ADD THIS (optional but good)
    }>;
};

type GroupedDataTableProps<T extends Row> = {
    baseColumns: BaseColumn<T>[];     // left columns (rowSpan=2)
    groupedColumns: GroupColumn<T>[]; // grouped columns (colSpan)
    data: T[];

    className?: string;
    tableClassName?: string;

    zebra?: boolean;
    rowClassName?: (row: T, rowIndex: number) => string;

    showCellTitle?: boolean;
    emptyMessage?: string;

    stickyHeader?: boolean;
};

export default function GroupedDataTable<T extends Row>({
    baseColumns,
    groupedColumns,
    data,
    className,
    tableClassName,
    zebra = true,
    rowClassName,
    showCellTitle = false,
    emptyMessage = "No data found.",
    stickyHeader = true,
}: GroupedDataTableProps<T>) {
    const hasData = Array.isArray(data) && data.length > 0;

    const allLeafColumns = React.useMemo(() => {
        const grouped = groupedColumns.flatMap((g) => g.columns);
        return [...baseColumns, ...grouped];
    }, [baseColumns, groupedColumns]);

    const colWidthStyle = (w?: string): React.CSSProperties | undefined =>
        w ? { width: w, maxWidth: w } : undefined;

    return (
        <div
            className={clsx(
                "relative w-full max-w-full overflow-x-auto overflow-y-auto max-h-[520px] rounded-xl border border-slate-200 bg-white shadow-sm",
                className
            )}
        >
            <table
                className={clsx(
                    "w-max min-w-full table-auto border-collapse text-xs text-slate-700",
                    tableClassName
                )}
            >

                <thead className={clsx(stickyHeader && "sticky top-0 z-10")}>
                    {/* Row 1: Group headers */}
                    <tr className="bg-green-500 text-yellow-200">
                        {baseColumns.map((col, i) => (
                            <th
                                key={`base-${String(col.key)}-${i}`}
                                rowSpan={2}
                                className={clsx(
                                    "border border-gray-300 px-2 py-2 text-center font-semibold whitespace-nowrap",
                                    col.headerClassName
                                )}

                                style={colWidthStyle(col.width)}
                            >
                                {col.header}
                            </th>
                        ))}

                        {groupedColumns.map((g, gi) => (
                            <th
                                key={`group-${gi}`}
                                colSpan={g.columns.length}
                                className="border border-gray-300 px-2 py-2 text-center font-semibold whitespace-nowrap"
                            >
                                {g.groupHeader}
                            </th>
                        ))}
                    </tr>

                    {/* Row 2: Sub headers (Applicable / Charged) */}
                    <tr className="bg-green-500 text-yellow-200">
                        {groupedColumns.flatMap((g, gi) =>
                            g.columns.map((c, ci) => (
                                <th
                                    key={`sub-${gi}-${ci}-${String(c.key)}`}
                                    className={clsx(
                                        "border border-gray-300 px-2 py-2 text-center font-semibold whitespace-nowrap",
                                        c.headerClassName
                                    )}
                                    style={colWidthStyle(c.width)}
                                >
                                    {c.header}
                                </th>
                            ))
                        )}
                    </tr>

                </thead>

                <tbody>
                    {!hasData && (
                        <tr>
                            <td
                                className="px-3 py-10 text-center text-slate-400"
                                colSpan={allLeafColumns.length}
                            >
                                {emptyMessage}
                            </td>
                        </tr>
                    )}

                    {hasData &&
                        data.map((row, ri) => (
                            <tr
                                key={ri}
                                className={clsx(
                                    "transition-colors",
                                    // zebra && ri % 2 === 1 && "bg-slate-50",
                                    rowClassName?.(row, ri)
                                )}
                            >
                                {/* Base cells */}
                                {baseColumns.map((col, ci) => {
                                    const value = (row as any)[String(col.key)];
                                    return (
                                        <td
                                            key={`b-${ri}-${ci}-${String(col.key)}`}
                                            className={clsx(
                                                "border border-gray-300 px-2 py-3 text-center whitespace-nowrap",
                                                col.cellClassName
                                            )}
                                            title={showCellTitle ? String(value ?? "\u00A0") : undefined}
                                        >
                                            {col.render ? col.render(row, value, ri) : value ?? "\u00A0"}
                                        </td>
                                    );
                                })}

                                {/* Grouped leaf cells */}
                                {groupedColumns.flatMap((g) =>
                                    g.columns.map((col, ci) => {
                                        const value = (row as any)[String(col.key)];
                                        return (
                                            <td
                                                key={`g-${ri}-${String(col.key)}-${ci}`}
                                                className={clsx(
                                                    "border border-gray-300 px-2 py-3 text-center whitespace-nowrap",
                                                    col.cellClassName
                                                )}
                                                style={colWidthStyle(col.width)}   // ✅ REQUIRED
                                                title={showCellTitle ? String(value ?? "\u00A0") : undefined}
                                            >
                                                {col.render ? col.render(row, value, ri) : value ?? "\u00A0"}
                                            </td>

                                        );
                                    })
                                )}
                            </tr>
                        ))}
                </tbody>
            </table>
        </div>
    );
}
