"use client";

import React, { useEffect, useMemo, useState } from "react";
import { FaArrowDownLong } from "react-icons/fa6";
import { LuArrowUpDown } from "react-icons/lu";

export type Align = "left" | "center" | "right";

export type LeafCol<RowT> = {
  key: string;
  label: React.ReactNode;
  align?: Align;
  tooltip?: React.ReactNode;
  thClassName?: string;
  tdClassName?: string;
  sortable?: boolean;
};

export type ColGroup<RowT> = {
  id: string;
  label: React.ReactNode;
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
  onCollapsedChange?: (collapsed: Record<string, boolean>) => void;

  getValue: (row: RowT, colKey: string, rowIndex: number) => React.ReactNode;

  getRowClassName?: (row: RowT, index: number) => string;
  onRowClick?: (
    row: RowT,
    rowIndex: number,
    event: React.MouseEvent<HTMLTableRowElement>
  ) => void;

  // Optional: sign row not in THEAD (keeps header strictly 2 rows)
  showSignRowInBody?: boolean;
  getSignForCol?: (
    colKey: string,
  ) => { text: string; className?: string } | null;
  toggleGroupByColKey?: Record<string, string>;
  tableClassName?: string;
  headerRow1ClassName?: string;
  headerRow2ClassName?: string;

  getSortValue?: (
    row: RowT,
    colKey: string,
  ) => string | number | null | undefined;
  isTotalRow?: (row: RowT) => boolean;
  defaultSort?: {
    key: string;
    direction: "asc" | "desc";
  } | null;
  onSortChange?: (sort: { key: string; direction: "asc" | "desc" }) => void;
  bodyMaxHeight?: number | string;
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
  onCollapsedChange,
  getValue,
  getRowClassName,
  onRowClick,

  showSignRowInBody = false,
  getSignForCol,
  toggleGroupByColKey,
  tableClassName = "min-w-[800px] w-full table-auto border-collapse text-[#414042]",
  headerRow1ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  headerRow2ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  getSortValue,
  isTotalRow,
  defaultSort = null,
  onSortChange,
  bodyMaxHeight,
}: Props<RowT>) {
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>(() => {
    const base: Record<string, boolean> = {};
    for (const g of groups) base[g.id] = true;
    return { ...base, ...(initialCollapsed || {}) };
  });

  const toggleGroup = (id: string) =>
    setCollapsed((p) => {
      const next = { ...p, [id]: !p[id] };
      onCollapsedChange?.(next);
      return next;
    });

  type SortDirection = "asc" | "desc";

  const [sortConfig, setSortConfig] = useState<{
    key: string;
    direction: SortDirection;
  } | null>(defaultSort);

  useEffect(() => {
    setSortConfig(defaultSort);
  }, [defaultSort]);

  const handleSort = (colKey: string) => {
    setSortConfig((prev) => {
      const next =
        prev?.key === colKey
          ? {
            key: colKey,
            direction: (prev.direction === "desc"
              ? "asc"
              : "desc") as SortDirection,
          }
          : {
            key: colKey,
            direction: "asc" as const,
          };

      onSortChange?.(next);
      return next;
    });
  };

  const renderSortArrow = (colKey: string) => {
    const isSorted = sortConfig?.key === colKey;

    if (!isSorted) {
      return (
        <span
          className="inline-flex shrink-0 items-center text-sm opacity-80"
          title="Click to sort"
        >
          <LuArrowUpDown size={15} strokeWidth={2.25} />
        </span>
      );
    }

    return (
      <span
        className={[
          "inline-flex shrink-0 items-center text-[10px]",
          "transition-transform duration-300 ease-in-out",
          sortConfig.direction === "asc" ? "rotate-180" : "rotate-0",
        ].join(" ")}
        title={
          sortConfig.direction === "desc"
            ? "Currently high to low"
            : "Currently low to high"
        }
      >
        <FaArrowDownLong size={16} />
      </span>
    );
  };

  const normalizeSortValue = (value: unknown) => {
    if (value === null || value === undefined) return "";
    if (typeof value === "number") return value;

    const raw = String(value).trim();
    const numeric = Number(raw.replace(/[^0-9.-]/g, ""));

    if (Number.isFinite(numeric) && raw.match(/[0-9]/)) {
      return numeric;
    }

    return raw.toLowerCase();
  };

  const sortedRows = useMemo(() => {
    if (!sortConfig) return rows;

    // When the parent owns sorting through onSortChange, do not sort again here.
    // This lets pages recalculate rows like "Others" after each sort.
    if (onSortChange) return rows;

    const normalRows: RowT[] = [];
    const pinnedRows: RowT[] = [];

    rows.forEach((row) => {
      if (isTotalRow?.(row)) pinnedRows.push(row);
      else normalRows.push(row);
    });

    const sorted = [...normalRows].sort((a, b) => {
      const aValue = normalizeSortValue(getSortValue?.(a, sortConfig.key));
      const bValue = normalizeSortValue(getSortValue?.(b, sortConfig.key));

      if (typeof aValue === "number" && typeof bValue === "number") {
        return sortConfig.direction === "asc"
          ? aValue - bValue
          : bValue - aValue;
      }

      return sortConfig.direction === "asc"
        ? String(aValue).localeCompare(String(bValue))
        : String(bValue).localeCompare(String(aValue));
    });

    return [...sorted, ...pinnedRows];
  }, [rows, sortConfig, getSortValue, isTotalRow, onSortChange]);

  const shouldPinRows = bodyMaxHeight !== undefined;

  const scrollRows = useMemo(() => {
    if (!shouldPinRows) return sortedRows;
    return sortedRows.filter((row) => !isTotalRow?.(row));
  }, [sortedRows, shouldPinRows, isTotalRow]);

  const pinnedRows = useMemo(() => {
    if (!shouldPinRows) return [];
    return sortedRows.filter((row) => isTotalRow?.(row));
  }, [sortedRows, shouldPinRows, isTotalRow]);

  const bodyScrollStyle: React.CSSProperties = {
    maxHeight:
      bodyMaxHeight === undefined
        ? undefined
        : typeof bodyMaxHeight === "number"
          ? `${bodyMaxHeight}px`
          : bodyMaxHeight,
  };

  const renderHeaderContent = (
    col: LeafCol<RowT>,
    options?: {
      onToggle?: (e: React.MouseEvent) => void;
      isCollapsed?: boolean;
    },
  ) => (
    <div className="flex items-center justify-center gap-2">
      <button
        type="button"
        onMouseDown={(e) => {
          if (col.sortable) e.stopPropagation();
        }}
        onClick={
          col.sortable
            ? (e) => {
              e.preventDefault();
              e.stopPropagation();
              handleSort(col.key);
            }
            : undefined
        }
        className={`inline-flex items-center justify-center gap-1 ${col.sortable ? "cursor-pointer select-none" : "cursor-default"
          }`}
        title={col.sortable ? "Click to sort" : undefined}
      >
        <span>{col.label}</span>
        {col.sortable ? renderSortArrow(col.key) : null}
      </button>

      {col.tooltip ? col.tooltip : null}

      {options?.onToggle ? (
        <button
          type="button"
          onClick={options.onToggle}
          className="inline-flex h-4 min-w-4 shrink-0 items-center justify-center rounded border border-white/60 bg-white/10 px-1 text-xs leading-none"
          title="Click to expand/collapse"
        >
          {options.isCollapsed ? "+" : "−"}
        </button>
      ) : null}
    </div>
  );

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

  const renderHeader = () => (
    <thead className="font-bold">
      <tr className={headerRow1ClassName}>
        {leftCols.map((c) => (
          <th
            key={c.key}
            rowSpan={2}
            className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""} ${c.sortable ? "cursor-pointer select-none" : ""
              }`}
          >
            {renderHeaderContent(c)}
          </th>
        ))}

        {groups.map((g) => {
          const isCollapsed = !!collapsed[g.id];
          const cols = isCollapsed ? g.collapsedCols : g.expandedCols;
          const colSpan = cols.length;

          if (colSpan === 0) return null;

          return (
            <th
              key={g.id}
              colSpan={colSpan}
              className={`${thBase} relative text-center ${g.headerClassName || ""}`}
            >
              <div className="flex items-center justify-center gap-2">
                <span className="px-2">{g.label}</span>

                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    toggleGroup(g.id);
                  }}
                  className="inline-flex h-4 min-w-4 shrink-0 items-center justify-center rounded border border-white/60 bg-white/10 px-1 text-xs leading-none"
                  aria-label={
                    isCollapsed ? `Expand ${g.label}` : `Collapse ${g.label}`
                  }
                  title={isCollapsed ? "Expand" : "Collapse"}
                >
                  {isCollapsed ? "+" : "−"}
                </button>
              </div>
            </th>
          );
        })}

        {singleCols.map((c) => {
          const targetGroupId = toggleGroupByColKey?.[c.key];

          return (
            <th
              key={c.key}
              rowSpan={2}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""} ${c.sortable ? "cursor-pointer select-none" : ""
                }`}
            >
              {renderHeaderContent(
                c,
                targetGroupId
                  ? {
                    isCollapsed: !!collapsed[targetGroupId],
                    onToggle: (e) => {
                      e.stopPropagation();
                      toggleGroup(targetGroupId);
                    },
                  }
                  : undefined,
              )}
            </th>
          );
        })}
      </tr>

      <tr className={headerRow2ClassName}>
        {row2GroupLeafCols.map((c) => (
          <th
            key={c.key}
            className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
          >
            {renderHeaderContent(c)}
          </th>
        ))}
      </tr>
    </thead>
  );

  const renderSignRow = () =>
    showSignRowInBody ? (
      <tr className="h-[40px] bg-white font-bold text-center">
        {visibleLeafCols.map((c) => {
          const sign = getSignForCol?.(c.key);

          return (
            <td
              key={c.key}
              className={`bg-inherit whitespace-nowrap border border-gray-300 px-2 py-2 text-xs 2xl:text-sm ${sign?.className || ""
                }`}
            >
              {sign?.text || ""}
            </td>
          );
        })}
      </tr>
    ) : null;

  const renderRows = (rowsToRender: RowT[], startIndex = 0) =>
    rowsToRender.map((row, idx) => {
      const realIndex = startIndex + idx;
      const rowKey = getRowKey ? getRowKey(row, realIndex) : realIndex;
      const rowClass = getRowClassName ? getRowClassName(row, realIndex) : "";

      return (
        <tr
          key={rowKey}
          onClick={(event) => onRowClick?.(row, realIndex, event)}
          className={`h-[40px] ${rowClass || "bg-white"}`}
        >
          {visibleLeafCols.map((c) => (
            <td
              key={c.key}
              className={`bg-inherit whitespace-nowrap border border-gray-300 px-2 py-2 text-xs 2xl:text-sm ${alignClass(
                c.align,
              )} ${c.tdClassName || ""}`}
            >
              {getValue(row, c.key, realIndex)}
            </td>
          ))}
        </tr>
      );
    });

  if (shouldPinRows) {
    return (
      <div className="w-full">
        <table className={tableClassName}>{renderHeader()}</table>

        <div
          className="overflow-y-auto overflow-x-hidden [scrollbar-gutter:stable]"
          style={bodyScrollStyle}
        >
          <table className={tableClassName}>
            <tbody className="bg-white">
              {renderSignRow()}
              {renderRows(scrollRows)}
            </tbody>
          </table>
        </div>

        {pinnedRows.length > 0 && (
          <table className={tableClassName}>
            <tbody className="bg-white">
              {renderRows(
                pinnedRows,
                scrollRows.length + (showSignRowInBody ? 1 : 0),
              )}
            </tbody>
          </table>
        )}
      </div>
    );
  }

  return (
    <table className={tableClassName}>
      {renderHeader()}

      <tbody className="bg-white">
        {renderSignRow()}
        {renderRows(sortedRows)}
      </tbody>
    </table>
  );
}
