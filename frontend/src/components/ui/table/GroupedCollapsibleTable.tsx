"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { FaArrowDownLong } from "react-icons/fa6";
import { LuArrowUpDown } from "react-icons/lu";

/* ---------------- Types ---------------- */

export type Align = "left" | "center" | "right";

export type LeafCol<RowT> = {
  key: string;
  label: React.ReactNode;
  excelLabel?: string;
  align?: Align;
  width?: number | string;
  tooltip?: React.ReactNode;
  info?: React.ReactNode;
  thClassName?: string;
  tdClassName?: string;
  noWrap?: boolean;

  sortable?: boolean;
};

export type ColGroup<RowT> = {
  id: string;
  label: React.ReactNode;
  info?: React.ReactNode;
  headerClassName?: string;
  collapsedCols: LeafCol<RowT>[];
  expandedCols: LeafCol<RowT>[];
};

type LayoutItem<RowT> =
  | { type: "group"; id: string }
  | { type: "single"; key: string };

type Props<RowT> = {
  rows: RowT[];
  getRowKey?: (row: RowT, index: number) => string | number;
  onAnyGroupExpandedChange?: (expanded: boolean) => void;
  leftCols: LeafCol<RowT>[];
  groups: ColGroup<RowT>[];
  singleCols: LeafCol<RowT>[];
  layout?: LayoutItem<RowT>[];
  initialCollapsed?: Record<string, boolean>;
  onVisibleColCountChange?: (n: number) => void;
  getValue: (row: RowT, colKey: string, rowIndex: number) => React.ReactNode;
  getRowClassName?: (row: RowT, index: number) => string;
  showSignRowInBody?: boolean;
  getSignForCol?: (colKey: string) => { text: string; className?: string } | null;
  toggleGroupByColKey?: Record<string, string>;
  tableClassName?: string;
  headerRow1ClassName?: string;
  headerRow2ClassName?: string;
  summary?: SummaryBlock<RowT>;

  getSortValue?: (row: RowT, colKey: string) => string | number | null | undefined;
  isTotalRow?: (row: RowT) => boolean;
  defaultSort?: {
    key: string;
    direction: "asc" | "desc";
  };
  bodyMaxHeight?: number;
  collapsedState?: Record<string, boolean>;
  onCollapsedChange?: (next: Record<string, boolean>) => void;

  onSortChange?: (sort: { key: string; direction: "asc" | "desc" }) => void;
};

export type SummaryRow<RowT> = {
  id: string;
  label: React.ReactNode;
  midValue?: React.ReactNode;
  endValue?: React.ReactNode;
  labelAlign?: Align;
};

export type SummarySection<RowT> = {
  id: string;
  label: React.ReactNode;
  endValue: React.ReactNode;
  children: SummaryRow<RowT>[];
  defaultCollapsed?: boolean;
};

export type SummaryBlock<RowT> = {
  enabled?: boolean;
  sections?: SummarySection<RowT>[];
  fixedRows?: SummaryRow<RowT>[];
  valueCols?: 2;
};


/* ---------------- Utils ---------------- */

const alignClass = (align?: Align) =>
  align === "left" ? "text-left" : align === "right" ? "text-right" : "text-center";

/* ---------------- Component ---------------- */

export default function GroupedCollapsibleTable<RowT>({
  rows,
  getRowKey,
  onAnyGroupExpandedChange,
  leftCols,
  groups,
  singleCols,
  layout,
  initialCollapsed,
  collapsedState,
  onCollapsedChange,
  getValue,
  getRowClassName,
  showSignRowInBody = false,
  getSignForCol,
  toggleGroupByColKey,
  onVisibleColCountChange,
  tableClassName = "min-w-[800px] w-full table-auto border-collapse bg-white text-[#414042] text-xs 2xl:text-sm",
  headerRow1ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  headerRow2ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  summary,
  getSortValue,
  isTotalRow,
  defaultSort,
  onSortChange,
  bodyMaxHeight,

}: Props<RowT>) {
  /* ---------------- State ---------------- */

  const buildDefaultCollapsed = () => {
    const base: Record<string, boolean> = {};

    groups.forEach((g) => {
      base[g.id] = true;
    });

    return {
      ...base,
      ...(initialCollapsed || {}),
    };
  };

  const [internalCollapsed, setInternalCollapsed] = useState<Record<string, boolean>>(
    buildDefaultCollapsed
  );

  const collapsed = collapsedState ?? internalCollapsed;

  const updateCollapsed = (
    updater:
      | Record<string, boolean>
      | ((prev: Record<string, boolean>) => Record<string, boolean>)
  ) => {
    const next =
      typeof updater === "function"
        ? updater(collapsed)
        : updater;

    if (!collapsedState) {
      setInternalCollapsed(next);
    }

    onCollapsedChange?.(next);
  };

  const toggleGroup = (id: string) => {
    updateCollapsed((prev) => ({
      ...prev,
      [id]: !(prev[id] ?? true),
    }));
  };

  const [summaryCollapsed, setSummaryCollapsed] = useState<Record<string, boolean>>(() => {
    const base: Record<string, boolean> = {};
    (summary?.sections || []).forEach((s) => {
      base[s.id] = s.defaultCollapsed ?? true;
    });
    return base;
  });

  const toggleSummary = (id: string) =>
    setSummaryCollapsed((p) => ({ ...p, [id]: !p[id] }));



  type SortDirection = "asc" | "desc";

  const [sortConfig, setSortConfig] = useState<{
    key: string;
    direction: SortDirection;
  } | null>(
    defaultSort ?? {
      key: "net_sales",
      direction: "desc",
    }
  );

  const handleSort = (colKey: string) => {
    setSortConfig((prev) => {
      const next =
        prev?.key === colKey
          ? {
            key: colKey,
            direction: (prev.direction === "desc" ? "asc" : "desc") as SortDirection,
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

    // handles values like "9.52%", "£2,095", "-4", "15.89%"
    const numeric = Number(raw.replace(/[^0-9.-]/g, ""));

    if (Number.isFinite(numeric) && raw.match(/[0-9]/)) {
      return numeric;
    }

    return raw.toLowerCase();
  };

  const sortedRows = useMemo(() => {
    if (!sortConfig) return rows;

    // When parent owns sorting through onSortChange, do not sort again here.
    // Dashboard P&L table needs parent sorting so "Others" can be recalculated.
    if (onSortChange) {
      return rows;
    }

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

  const scrollRows = bodyMaxHeight
    ? sortedRows.filter((row) => !isTotalRow?.(row))
    : sortedRows;

  const pinnedRows = bodyMaxHeight
    ? sortedRows.filter((row) => isTotalRow?.(row))
    : [];

  /* ---------------- Maps ---------------- */

  const groupMap = useMemo(() => {
    const m = new Map<string, ColGroup<RowT>>();
    groups.forEach((g) => m.set(g.id, g));
    return m;
  }, [groups]);

  const singleMap = useMemo(() => {
    const m = new Map<string, LeafCol<RowT>>();
    singleCols.forEach((c) => m.set(c.key, c));
    return m;
  }, [singleCols]);

  const resolvedLayout: LayoutItem<RowT>[] = useMemo(
    () =>
      layout?.length
        ? layout
        : [
          ...groups.map((g) => ({ type: "group" as const, id: g.id })),
          ...singleCols.map((c) => ({ type: "single" as const, key: c.key })),
        ],
    [layout, groups, singleCols]
  );

  const renderHeaderContent = (
    col: LeafCol<RowT>,
    options?: {
      isExpandable?: boolean;
      isCollapsed?: boolean;
      onToggle?: (e: React.MouseEvent) => void;
    }
  ) => {
    const onSortClick = col.sortable
      ? (e: React.MouseEvent) => {
        e.preventDefault();
        e.stopPropagation();

        handleSort(col.key);
      }
      : undefined;

    return (
      <div className="flex w-full items-center justify-center">
        <div className="inline-flex items-center justify-center gap-2 leading-tight">
          <button
            type="button"
            data-sort-control={col.sortable ? "true" : undefined}
            onClick={onSortClick}
            className={`inline-flex items-center justify-center ${col.noWrap ? "whitespace-nowrap" : "whitespace-normal break-words"
              } ${col.sortable ? "cursor-pointer select-none" : "cursor-default"}`}
            title={col.sortable ? "Click to sort" : undefined}
          >
            <span className="inline-flex items-center">{col.label}</span>
          </button>

          {col.info && (
            <span
              className="inline-flex shrink-0 items-center"
              onClick={(e) => e.stopPropagation()}
            >
              {col.info}
            </span>
          )}

          {col.sortable && (
            <button
              type="button"
              data-sort-control="true"
              onClick={onSortClick}
              className="inline-flex shrink-0 items-center"
              title="Click to sort"
            >
              {renderSortArrow(col.key)}
            </button>
          )}

          {options?.isExpandable && (
            <button
              type="button"
              onClick={options.onToggle}
              className="inline-flex h-4 min-w-4 shrink-0 items-center justify-center rounded border border-white/60 bg-white/10 px-1 text-xs leading-none"
              title="Click to expand/collapse"
            >
              {options.isCollapsed ? "+" : "−"}
            </button>
          )}
        </div>
      </div>
    );
  };

  /* ---------------- Visible Columns ---------------- */

  const visibleLeafCols = useMemo(() => {
    const out: LeafCol<RowT>[] = [];
    out.push(...leftCols);

    for (const item of resolvedLayout) {
      if (item.type === "group") {
        const g = groupMap.get(item.id);
        if (!g) continue;
        const isCollapsed = collapsed[g.id];
        out.push(...(isCollapsed ? g.collapsedCols : g.expandedCols));
      } else {
        const c = singleMap.get(item.key);
        if (c) out.push(c);
      }
    }

    return out;
  }, [leftCols, resolvedLayout, collapsed, groupMap, singleMap]);

  useEffect(() => {
    onVisibleColCountChange?.(visibleLeafCols.length);
  }, [visibleLeafCols.length, onVisibleColCountChange]);


  /* ---------------- Row 2 Headers ---------------- */
  type Row2Cell<RowT> =
    | { kind: "col"; col: LeafCol<RowT>; colSpan: 1 }
    | { kind: "blank"; key: string; colSpan: number };

  const anyGroupExpanded = useMemo(
    () => groups.some((g) => collapsed[g.id] === false),
    [groups, collapsed]
  );

  useEffect(() => {
    onAnyGroupExpandedChange?.(anyGroupExpanded);
  }, [anyGroupExpanded, onAnyGroupExpandedChange]);


  const row2Cells = useMemo<LeafCol<RowT>[]>(() => {
    if (!anyGroupExpanded) return [];

    const cols: LeafCol<RowT>[] = [];
    for (const item of resolvedLayout) {
      if (item.type !== "group") continue;
      const g = groupMap.get(item.id);
      if (!g) continue;

      if (collapsed[g.id] === false) {
        cols.push(...g.expandedCols);
      }
    }
    return cols;
  }, [anyGroupExpanded, resolvedLayout, collapsed, groupMap]);

  const visibleCount = visibleLeafCols.length;
  const valueCols = summary?.valueCols ?? 2;
  const labelColSpan = Math.max(visibleCount - valueCols, 1);
  const midColSpan = 1;
  const endColSpan = 1;

  const cellPadding = "px-2 sm:px-3 py-3";
  const thBase =
    `whitespace-normal break-words leading-tight border border-gray-300 ${cellPadding}`;

  /* ---------------- Render ---------------- */

  /* ---------------- Render ---------------- */

  const renderColGroup = () => (
    <colgroup>
      {visibleLeafCols.map((c) => (
        <col
          key={c.key}
          style={c.width ? { width: c.width } : undefined}
        />
      ))}
    </colgroup>
  );

  const renderTableHead = () => (
    <thead className="sticky top-0 z-10 font-bold">
      {/* -------- Header Row 1 -------- */}
      <tr className={headerRow1ClassName}>
        {leftCols.map((c) => (
          <th
            key={c.key}
            rowSpan={anyGroupExpanded ? 2 : 1}
            className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""} ${c.sortable ? "cursor-pointer select-none" : ""
              }`}
          >
            {renderHeaderContent(c)}
          </th>
        ))}

        {resolvedLayout.map((item) => {
          if (item.type === "group") {
            const g = groupMap.get(item.id);
            if (!g) return null;

            const isCollapsed = collapsed[g.id];
            const cols = isCollapsed ? g.collapsedCols : g.expandedCols;
            if (cols.length === 0) return null;

            const groupRowSpan = anyGroupExpanded ? (isCollapsed ? 2 : 1) : 1;

            const sortCol = isCollapsed
              ? cols.find((col) => col.sortable)
              : null;

            return (
              <th
                key={g.id}
                colSpan={cols.length}
                rowSpan={groupRowSpan}
                className={`${thBase} text-center ${g.headerClassName || ""}`}
              >
                <div className="flex w-full justify-center">
                  <div className="inline-flex items-center justify-center gap-2 whitespace-normal break-words leading-tight">
                    <span className="inline-flex items-center">
                      {g.label}
                    </span>

                    {g.info && (
                      <span className="inline-flex shrink-0 items-center">
                        {g.info}
                      </span>
                    )}

                    {sortCol && (
                      <button
                        type="button"
                        data-sort-control="true"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();

                          handleSort(sortCol.key);
                        }}
                        className="inline-flex shrink-0 items-center"
                        title="Click to sort"
                      >
                        {renderSortArrow(sortCol.key)}
                      </button>
                    )}

                    <button
                      type="button"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        toggleGroup(g.id);
                      }}
                      className="inline-flex h-4 min-w-4 shrink-0 items-center justify-center rounded border border-white/60 bg-white/10 px-1 text-xs leading-none"
                      title="Click to expand/collapse"
                    >
                      {isCollapsed ? "+" : "−"}
                    </button>
                  </div>
                </div>
              </th>
            );
          }

          const c = singleMap.get(item.key);
          if (!c) return null;

          const targetGroupId = toggleGroupByColKey?.[c.key];
          const isExpandable = Boolean(targetGroupId);
          const isTargetCollapsed = targetGroupId ? collapsed[targetGroupId] : true;

          return (
            <th
              key={c.key}
              rowSpan={anyGroupExpanded ? 2 : 1}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""} ${c.sortable ? "cursor-pointer select-none" : ""
                }`}
            >
              {renderHeaderContent(c, {
                isExpandable,
                isCollapsed: isTargetCollapsed,
                onToggle: (e) => {
                  e.stopPropagation();
                  if (targetGroupId) toggleGroup(targetGroupId);
                },
              })}
            </th>
          );
        })}
      </tr>

      {/* -------- Header Row 2 -------- */}
      {anyGroupExpanded && (
        <tr className={headerRow2ClassName}>
          {row2Cells.map((c) => (
            <th
              key={c.key}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
            >
              {renderHeaderContent(c)}
            </th>
          ))}
        </tr>
      )}
    </thead>
  );

  const renderSignRow = () =>
    showSignRowInBody ? (
      <tr className="font-bold text-center">
        {visibleLeafCols.map((c) => {
          const sign = getSignForCol?.(c.key);

          return (
            <td
              key={c.key}
              className={`border ${cellPadding} ${sign?.className || ""}`}
            >
              {sign?.text || ""}
            </td>
          );
        })}
      </tr>
    ) : null;

  const renderBodyRows = (rowsToRender: RowT[], startIndex = 0) =>
    rowsToRender.map((row, idx) => {
      const realIndex = startIndex + idx;

      return (
        <tr
          key={getRowKey?.(row, realIndex) ?? realIndex}
          className={getRowClassName?.(row, realIndex)}
        >
          {visibleLeafCols.map((c) => (
            <td
              key={c.key}
              className={`border ${cellPadding} ${alignClass(c.align)} ${c.tdClassName || ""}`}
            >
              {getValue(row, c.key, realIndex)}
            </td>
          ))}
        </tr>
      );
    });

  const renderSummaryFooter = () =>
    summary?.enabled !== false &&
      (summary?.sections?.length || summary?.fixedRows?.length) ? (
      <tfoot>
        {(summary.sections || []).map((sec) => {
          const isCollapsed = summaryCollapsed[sec.id] ?? true;

          return (
            <React.Fragment key={sec.id}>
              <tr
                onClick={() => toggleSummary(sec.id)}
                role="button"
                className="cursor-pointer font-semibold bg-gray-50"
                title="Click to expand/collapse"
              >
                <td
                  colSpan={labelColSpan}
                  className="border border-gray-300 px-2 sm:px-3 py-3 text-left"
                >
                  <span className="inline-flex items-center gap-2">
                    <span className="rounded border border-gray-400 px-1 text-xs">
                      {isCollapsed ? "+" : "−"}
                    </span>
                    {sec.label}
                  </span>
                </td>

                <td
                  colSpan={midColSpan}
                  className="border border-gray-300 px-2 sm:px-3 py-3"
                />

                <td
                  colSpan={endColSpan}
                  className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center"
                >
                  {sec.endValue}
                </td>
              </tr>

              {!isCollapsed &&
                sec.children.map((ch) => (
                  <tr key={ch.id}>
                    <td
                      colSpan={labelColSpan}
                      className="border border-gray-300 px-2 sm:px-3 py-3 pl-8 text-right"
                    >
                      {ch.label}
                    </td>

                    <td
                      colSpan={midColSpan}
                      className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center"
                    >
                      {ch.midValue ?? ""}
                    </td>

                    <td
                      colSpan={endColSpan}
                      className="border border-gray-300 px-2 sm:px-3 py-3"
                    />
                  </tr>
                ))}
            </React.Fragment>
          );
        })}

        {(summary.fixedRows || []).map((r) => (
          <tr key={r.id}>
            <td
              colSpan={labelColSpan}
              className="border border-gray-300 px-2 sm:px-3 py-3 text-left"
            >
              {r.label}
            </td>

            <td
              colSpan={midColSpan}
              className="border border-gray-300 px-2 sm:px-3 py-3"
            />

            <td
              colSpan={endColSpan}
              className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center"
            >
              {r.endValue ?? ""}
            </td>
          </tr>
        ))}
      </tfoot>
    ) : null;

  /**
   * Scroll mode:
   * - Header + product rows scroll
   * - Total row + summary rows stay visible
   */
  if (bodyMaxHeight) {
    return (
      <div className="w-full">
        <div
          className="w-full overflow-y-auto"
          style={{ maxHeight: `${bodyMaxHeight}px` }}
        >
          <table className={tableClassName}>
            {renderColGroup()}
            {renderTableHead()}

            <tbody>
              {renderSignRow()}
              {renderBodyRows(scrollRows)}
            </tbody>
          </table>
        </div>

        <table className={tableClassName}>
          {renderColGroup()}

          {pinnedRows.length > 0 && (
            <tbody>
              {renderBodyRows(pinnedRows, scrollRows.length)}
            </tbody>
          )}

          {renderSummaryFooter()}
        </table>
      </div>
    );
  }

  /**
   * Normal mode:
   * Everything stays in one table
   */
  return (
    <table className={tableClassName}>
      {renderColGroup()}
      {renderTableHead()}

      <tbody>
        {renderSignRow()}
        {renderBodyRows(sortedRows)}
      </tbody>

      {renderSummaryFooter()}
    </table>
  );
}
