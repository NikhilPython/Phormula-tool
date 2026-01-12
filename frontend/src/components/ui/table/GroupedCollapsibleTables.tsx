"use client";

import React, { useMemo, useState } from "react";

export type Align = "left" | "center" | "right";

export type LeafCol<RowT> = {
  key: string;
  label: string;
  align?: Align;
  tooltip?: React.ReactNode;
  thClassName?: string;
  tdClassName?: string;
};

export type ColGroup<RowT> = {
  id: string;
  label: string;
  headerClassName?: string;

  // columns always visible when collapsed (usually 0 or 1, you will decide)
  collapsedCols: LeafCol<RowT>[];

  // columns visible when expanded (the “children” columns)
  expandedCols: LeafCol<RowT>[];
};

type Props<RowT> = {
  rows: RowT[];
  getRowKey?: (row: RowT, index: number) => string | number;

  // RowSpan=2 columns on left (like Product Name, Net Units Sold, ASP...)
  leftCols: LeafCol<RowT>[];

  // Groups that expand/collapse (like Sales, Promotions, Amazon Fees, Others)
  groups: ColGroup<RowT>[];

  // RowSpan=2 single columns that are not in groups (like Net Sales, COGS, Other Transactions, CM1 Profit Margin)
  singleCols: LeafCol<RowT>[];

  initialCollapsed?: Record<string, boolean>;

  getValue: (row: RowT, colKey: string, rowIndex: number) => React.ReactNode;

  getRowClassName?: (row: RowT, index: number) => string;

  // Optional: sign row not in THEAD (keeps header strictly 2 rows)
  showSignRowInBody?: boolean;
  getSignForCol?: (colKey: string) => { text: string; className?: string } | null;
  toggleGroupByColKey?: Record<string, string>;
  tableClassName?: string;
  headerRow1ClassName?: string;
  headerRow2ClassName?: string;
};

const alignClass = (align?: Align) => {
  if (align === "left") return "text-left";
  if (align === "right") return "text-right";
  return "text-center";
};

export default function GroupedCollapsibleTables<RowT>({
  rows,
  getRowKey,
  leftCols,
  groups,
  singleCols,
  initialCollapsed,
  getValue,
  getRowClassName,

  showSignRowInBody = false,
  getSignForCol,
toggleGroupByColKey,
  tableClassName = "min-w-[800px] w-full table-auto border-collapse text-[#414042]",
  headerRow1ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  headerRow2ClassName = "bg-[#5EA68E] text-[#f8edcf]",
}: Props<RowT>) {
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>(() => {
    const base: Record<string, boolean> = {};
    for (const g of groups) base[g.id] = true;
    return { ...base, ...(initialCollapsed || {}) };
  });

  const toggleGroup = (id: string) => setCollapsed((p) => ({ ...p, [id]: !p[id] }));

  // Leaf columns that will actually render in the body (order matters)
  const visibleLeafCols = useMemo(() => {
    const out: LeafCol<RowT>[] = [];
    out.push(...leftCols);

    for (const g of groups) {
      const isCollapsed = !!collapsed[g.id];
      out.push(...(isCollapsed ? g.collapsedCols : g.expandedCols));
    }

    out.push(...singleCols);
    return out;
  }, [leftCols, groups, singleCols, collapsed]);

  // Row2 leaf headers are ONLY group columns (left + single are rowSpan=2 and do not appear in row2)
  const row2GroupLeafCols = useMemo(() => {
    const out: LeafCol<RowT>[] = [];
    for (const g of groups) {
      const isCollapsed = !!collapsed[g.id];
      out.push(...(isCollapsed ? g.collapsedCols : g.expandedCols));
    }
    return out;
  }, [groups, collapsed]);

  const thBase =
    "whitespace-nowrap border border-gray-300 px-2 py-2 text-xs 2xl:text-sm";

  return (
    <table className={tableClassName}>
      <thead className="sticky top-0 z-10 font-bold">
        {/* ✅ Row 1: leftCols(rowSpan=2) + group headers(colSpan) + singleCols(rowSpan=2) */}
        <tr className={headerRow1ClassName}>
          {leftCols.map((c) => (
            <th
              key={c.key}
              rowSpan={2}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
            >
              <div className="flex items-center justify-center gap-1">
                {c.label}
                {c.tooltip ? c.tooltip : null}
              </div>
            </th>
          ))}

          {/* {groups.map((g) => {
            const isCollapsed = !!collapsed[g.id];
            const cols = isCollapsed ? g.collapsedCols : g.expandedCols;
            const colSpan = Math.max(cols.length, 1);

            return (
              <th
                key={g.id}
                colSpan={colSpan}
                className={`${thBase} relative text-center ${g.headerClassName || ""}`}
              >
                <button
                  type="button"
                  onClick={() => toggleGroup(g.id)}
                  className="absolute left-2 top-1/2 -translate-y-1/2 rounded border border-white/60 bg-white/10 px-1 text-xs"
                  aria-label={isCollapsed ? `Expand ${g.label}` : `Collapse ${g.label}`}
                  title={isCollapsed ? "Expand" : "Collapse"}
                >
                  {isCollapsed ? "+" : "−"}
                </button>
                <span className="px-6">{g.label}</span>
              </th>
            );
          })} */}

          {groups.map((g) => {
            const isCollapsed = !!collapsed[g.id];
            const cols = isCollapsed ? g.collapsedCols : g.expandedCols;
            const colSpan = cols.length;              // ✅ no Math.max

            if (colSpan === 0) return null;           // ✅ hide group header when collapsed has 0 cols

            return (
              <th key={g.id} colSpan={colSpan} className={`${thBase} relative text-center ${g.headerClassName || ""}`}>
                <button
                  type="button"
                  onClick={() => toggleGroup(g.id)}
                  className="absolute left-2 top-1/2 -translate-y-1/2 rounded border border-white/60 bg-white/10 px-1 text-xs"
                  aria-label={isCollapsed ? `Expand ${g.label}` : `Collapse ${g.label}`}
                  title={isCollapsed ? "Expand" : "Collapse"}
                >
                  {isCollapsed ? "+" : "−"}
                </button>
                <span className="px-6">{g.label}</span>
              </th>
            );
          })}

          {/* 
          {singleCols.map((c) => (
            <th
              key={c.key}
              rowSpan={2}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
            >
              <div className="flex items-center justify-center gap-1">
                {c.label}
                {c.tooltip ? c.tooltip : null}
              </div>
            </th>
          ))} */}

          {singleCols.map((c) => {
            const targetGroupId = toggleGroupByColKey?.[c.key];

            return (
              <th
                key={c.key}
                rowSpan={2}
                className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
                onClick={targetGroupId ? () => toggleGroup(targetGroupId) : undefined}
                role={targetGroupId ? "button" : undefined}
                title={targetGroupId ? "Click to expand/collapse" : undefined}
              >
                <div className="flex items-center justify-center gap-1">
                  {c.label}
                  {c.tooltip ? c.tooltip : null}
                </div>
              </th>
            );
          })}

        </tr>

        {/* ✅ Row 2: ONLY group leaf headers */}
        <tr className={headerRow2ClassName}>
          {row2GroupLeafCols.map((c) => (
            <th
              key={c.key}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
            >
              <div className="flex items-center justify-center gap-1">
                {c.label}
                {c.tooltip ? c.tooltip : null}
              </div>
            </th>
          ))}
        </tr>
      </thead>

      <tbody>
        {/* Optional sign row as FIRST BODY ROW (keeps header strictly 2 rows) */}
        {showSignRowInBody && (
          <tr className="bg-white font-bold text-center">
            {visibleLeafCols.map((c) => {
              const sign = getSignForCol?.(c.key);
              return (
                <td
                  key={c.key}
                  className={`whitespace-nowrap border border-gray-300 px-2 py-2 text-xs 2xl:text-sm ${sign?.className || ""
                    }`}
                >
                  {sign?.text || ""}
                </td>
              );
            })}
          </tr>
        )}

        {rows.map((row, idx) => {
          const rowKey = getRowKey ? getRowKey(row, idx) : idx;
          const rowClass = getRowClassName ? getRowClassName(row, idx) : "";

          return (
            <tr key={rowKey} className={rowClass}>
              {visibleLeafCols.map((c) => (
                <td
                  key={c.key}
                  className={`whitespace-nowrap border border-gray-300 px-2 py-2 text-xs 2xl:text-sm ${alignClass(
                    c.align
                  )} ${c.tdClassName || ""}`}
                >
                  {getValue(row, c.key, idx)}
                </td>
              ))}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
