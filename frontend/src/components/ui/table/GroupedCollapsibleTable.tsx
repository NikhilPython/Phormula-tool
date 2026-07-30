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

  expandable?: boolean;

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
  preserveColumnWidths?: boolean | "responsive";
  stickyLeftCols?: boolean;
  hideStickyLeftColsWhileScrolling?: boolean;
  stickyLeftBorderMode?: "transparent" | "shadow-only";
  stickyLeftDividerMode?: "trailing" | "leading";
  stickyLeftHorizontalBorderMode?: "shadow" | "border";
  stickyLeftWidthMode?: "fallback" | "declared";
  showStickyLeftOuterBorder?: boolean;
  getGroupToggleCollapsedState?: (
    groupId: string,
    defaultIsCollapsed: boolean
  ) => boolean;

  onSortChange?: (sort: { key: string; direction: "asc" | "desc" }) => void;
};

export type SummaryRow<RowT> = {
  id: string;
  label: React.ReactNode;
  midValue?: React.ReactNode;
  endValue?: React.ReactNode;
  labelAlign?: Align;
  bold?: boolean;
};

export type SummarySection<RowT> = {
  id: string;
  label: React.ReactNode;
  endValue: React.ReactNode;
  children: SummaryRow<RowT>[];
  defaultCollapsed?: boolean;
  bold?: boolean;
};

export type SummaryFixedItem<RowT> = SummaryRow<RowT> & {
  type: "fixed";
};

export type SummarySectionItem<RowT> = SummarySection<RowT> & {
  type: "section";
};

export type SummaryOrderedItem<RowT> =
  | SummaryFixedItem<RowT>
  | SummarySectionItem<RowT>;

export type SummaryBlock<RowT> = {
  enabled?: boolean;

  // old support
  sections?: SummarySection<RowT>[];
  fixedRows?: SummaryRow<RowT>[];

  // new ordered support
  rows?: SummaryOrderedItem<RowT>[];

  valueCols?: 2;
  boldSectionsByDefault?: boolean;
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
  tableClassName = "w-full table-fixed border-collapse bg-white text-[#414042] text-[12px] lg:text-[12px] min-[1700px]:text-[14px]",
  headerRow1ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  headerRow2ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  summary,
  getSortValue,
  isTotalRow,
  defaultSort,
  onSortChange,
  bodyMaxHeight,
  preserveColumnWidths = false,
  stickyLeftCols = true,
  hideStickyLeftColsWhileScrolling = true,
  stickyLeftBorderMode = "shadow-only",
  stickyLeftDividerMode = "trailing",
  stickyLeftHorizontalBorderMode = "shadow",
  stickyLeftWidthMode = "fallback",
  showStickyLeftOuterBorder = false,
  getGroupToggleCollapsedState,

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

    const sectionRows =
      summary?.rows?.filter((row): row is SummarySectionItem<RowT> => row.type === "section") ??
      summary?.sections ??
      [];

    sectionRows.forEach((s) => {
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

  const iconButtonClass =
    "inline-flex h-4 min-w-4 shrink-0 items-center justify-center rounded border border-white/60 bg-white/10 px-1 text-xs leading-none";

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
      <div className="flex w-full min-w-0 flex-col items-center justify-center gap-1 text-center leading-tight">
        {/* Title row */}
        <button
          type="button"
          data-sort-control={col.sortable ? "true" : undefined}
          onClick={onSortClick}
          className={[
            "block w-full min-w-0 text-center",
            col.noWrap ? "whitespace-nowrap" : "whitespace-normal break-words",
            col.sortable ? "cursor-pointer select-none" : "cursor-default",
          ].join(" ")}
          title={col.sortable ? "Click to sort" : undefined}
        >
          {col.label}
        </button>

        {/* Icons row */}
        {(col.info || col.sortable || options?.isExpandable) && (
          <div className="flex items-center justify-center gap-1">
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
                className={iconButtonClass}
                title="Click to expand/collapse"
              >
                {options.isCollapsed ? "+" : "−"}
              </button>
            )}
          </div>
        )}
      </div>
    );
  };

  /* ---------------- Visible Columns ---------------- */

  const anyGroupExpanded = useMemo(
    () => groups.some((g) => (collapsed[g.id] ?? true) === false),
    [groups, collapsed]
  );

  const visibleLeafCols = useMemo(() => {
    const out: LeafCol<RowT>[] = [];
    out.push(...leftCols);

    for (const item of resolvedLayout) {
      if (item.type === "group") {
        const g = groupMap.get(item.id);
        if (!g) continue;
        const isCollapsed = collapsed[g.id] ?? true;
        out.push(...(isCollapsed ? g.collapsedCols : g.expandedCols));
      } else {
        const c = singleMap.get(item.key);
        if (c) out.push(c);
      }
    }

    return out;
  }, [leftCols, resolvedLayout, collapsed, groupMap, singleMap]);



  const getMinWidthForCol = (col: LeafCol<RowT>) => {

    if (col.key === "available") return 110;

    if (col.key === "fcTransfer") return 110;

    if (col.key === "totalUnits") return 110;

    if (col.key === "productName") return 180;

    if (col.key === "sku") return 120;

    if (col.key === "salesRank") return 130;
    if (col.key === "sno") return 58;
    if (col.key === "product_name") return 220;
    if (col.key === "sku") return 135;

    if (
      col.key === "quantity" ||
      col.key === "return_quantity" ||
      col.key === "total_quantity" ||
      col.key === "net_units_sold"
    ) {
      return 125;
    }

    if (
      col.key === "asp" ||
      col.key === "net_sales" ||
      col.key === "cogs"
    ) {
      return 135;
    }

    if (
      col.key === "selling_fees" ||
      col.key === "fba_fees" ||
      col.key === "marketplace_total" ||
      col.key === "amazon_fee"
    ) {
      return 120;
    }

    if (
      col.key === "tax" ||
      col.key === "credits" ||
      col.key === "tax_and_credits" ||
      col.key === "net_taxes" ||
      col.key === "net_credits" ||
      col.key === "other_transactions"
    ) {
      return 120;
    }

    if (
      col.key === "product_spend" ||
      col.key === "display_spend" ||
      col.key === "brand_spend" ||
      col.key === "ads_spend"
    ) {
      return 170;
    }

    if (
      col.key === "acos" ||
      col.key.includes("percentage") ||
      col.key.includes("margins") ||
      col.key.includes("_per")
    ) {
      return 120;
    }

    if (col.key.includes("profit")) {
      return 135;
    }

    if (col.noWrap) return 130;

    return 110;
  };

  const getDeclaredWidthPx = (width: LeafCol<RowT>["width"]) => {
    if (typeof width === "number") return width;
    if (typeof width !== "string") return undefined;

    const trimmedWidth = width.trim();
    if (!trimmedWidth.endsWith("px")) return undefined;

    const numericWidth = Number.parseFloat(trimmedWidth);
    return Number.isFinite(numericWidth) ? numericWidth : undefined;
  };

  const getStickyLeftWidthForCol = (col: LeafCol<RowT>) =>
    stickyLeftWidthMode === "declared"
      ? getDeclaredWidthPx(col.width) ?? getMinWidthForCol(col)
      : getMinWidthForCol(col);

  const [isNarrowViewport, setIsNarrowViewport] = useState(false);

  useEffect(() => {
    const mediaQuery = window.matchMedia("(max-width: 1199px)");
    const updateViewportMatch = () => setIsNarrowViewport(mediaQuery.matches);

    updateViewportMatch();
    mediaQuery.addEventListener("change", updateViewportMatch);

    return () => mediaQuery.removeEventListener("change", updateViewportMatch);
  }, []);

  const shouldPreserveColumnWidths =
    anyGroupExpanded ||
    preserveColumnWidths === true ||
    (preserveColumnWidths === "responsive" && isNarrowViewport);

  const requiredTableWidth = useMemo(() => {
    const width = visibleLeafCols.reduce(
      (sum, col) => sum + getMinWidthForCol(col),
      0
    );

    return Math.max(width, 1200);
  }, [visibleLeafCols]);

  const tableStyle: React.CSSProperties = {
    tableLayout: "fixed",
    width: "100%",
    minWidth: shouldPreserveColumnWidths ? `${requiredTableWidth}px` : "100%",
  };

  useEffect(() => {
    onVisibleColCountChange?.(visibleLeafCols.length);
  }, [visibleLeafCols.length, onVisibleColCountChange]);


  const scrollContainerRef = useRef<HTMLDivElement | null>(null);
  const summaryScrollContainerRef = useRef<HTMLDivElement | null>(null);
  const tableRef = useRef<HTMLTableElement | null>(null);
  const scrollStopTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastScrollLeftRef = useRef(0);
  const [summaryEndColumnWidth, setSummaryEndColumnWidth] = useState<number | null>(null);
  const [scrollbarWidth, setScrollbarWidth] = useState(0);
  const [isStickyLeftDrawerHidden, setIsStickyLeftDrawerHidden] = useState(false);

  useEffect(() => {
    return () => {
      if (scrollStopTimeoutRef.current) {
        clearTimeout(scrollStopTimeoutRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (!bodyMaxHeight) return;

    const measureScrollbarWidth = () => {
      const el = scrollContainerRef.current;
      if (!el) return;

      const width = el.offsetWidth - el.clientWidth;
      setScrollbarWidth(width > 0 ? width : 0);
    };

    measureScrollbarWidth();

    const el = scrollContainerRef.current;
    const resizeObserver =
      typeof ResizeObserver !== "undefined" && el
        ? new ResizeObserver(measureScrollbarWidth)
        : null;

    if (resizeObserver && el) {
      resizeObserver.observe(el);
    }
    window.addEventListener("resize", measureScrollbarWidth);

    return () => {
      resizeObserver?.disconnect();
      window.removeEventListener("resize", measureScrollbarWidth);
    };
  }, [bodyMaxHeight, sortedRows.length, visibleLeafCols.length]);

  /* ---------------- Row 2 Headers ---------------- */
  type Row2Cell<RowT> =
    | { kind: "col"; col: LeafCol<RowT>; colSpan: 1 }
    | { kind: "blank"; key: string; colSpan: number };



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

  useEffect(() => {
    const table = tableRef.current;

    if (!table || visibleCount === 0) {
      setSummaryEndColumnWidth(null);
      return;
    }

    const measureSummaryEndColumn = () => {
      const endCell =
        table.querySelector<HTMLElement>("[data-summary-main-value-cell='true']") ??
        table.querySelector<HTMLElement>("tfoot tr td:last-child") ??
        table.querySelector<HTMLElement>("tbody tr td:last-child") ??
        table.querySelector<HTMLElement>("thead tr:last-child th:last-child");
      const measuredWidth = endCell?.getBoundingClientRect().width;

      if (!measuredWidth || measuredWidth <= 0) return;

      const roundedWidth = Math.round(measuredWidth * 100) / 100;

      setSummaryEndColumnWidth((currentWidth) =>
        currentWidth !== null && Math.abs(currentWidth - roundedWidth) < 0.1
          ? currentWidth
          : roundedWidth
      );
    };

    measureSummaryEndColumn();

    const resizeObserver =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(measureSummaryEndColumn)
        : null;

    resizeObserver?.observe(table);
    window.addEventListener("resize", measureSummaryEndColumn);

    return () => {
      resizeObserver?.disconnect();
      window.removeEventListener("resize", measureSummaryEndColumn);
    };
  }, [visibleCount, shouldPreserveColumnWidths, requiredTableWidth, anyGroupExpanded]);

  const cellPadding = "px-2 sm:px-3 py-3";
  const thBase =
    `whitespace-normal break-words leading-tight border border-gray-300 ${cellPadding}`;

  const stickyLeftCount = stickyLeftCols ? leftCols.length : 0;
  const stickyLeftDrawerWidth = visibleLeafCols
    .slice(0, stickyLeftCount)
    .reduce((sum, col) => sum + getStickyLeftWidthForCol(col), 0);
  const shouldHideStickyLeftDrawer =
    hideStickyLeftColsWhileScrolling &&
    isStickyLeftDrawerHidden &&
    stickyLeftDrawerWidth > 0;

  const getStickyLeftOffset = (colIndex: number) =>
    visibleLeafCols
      .slice(0, colIndex)
      .reduce((sum, col) => sum + getStickyLeftWidthForCol(col), 0);

  const getStickyLeftStyle = (
    colIndex: number,
    surface: "header" | "body" | "sign" = "body"
  ): React.CSSProperties | undefined => {
    if (colIndex >= stickyLeftCount) return undefined;

    const dividerColor =
      surface === "header" ? "rgb(209 213 219)" : "rgb(229 231 235)";
    const verticalDividerColor = dividerColor;
    const outerLeftDivider =
      showStickyLeftOuterBorder && colIndex === 0
        ? `inset 1px 0 0 ${verticalDividerColor}`
        : "";
    const horizontalDivider =
      stickyLeftHorizontalBorderMode === "shadow"
        ? `inset 0 -1px 0 ${dividerColor}`
        : "";
    const stickyDividers =
      stickyLeftDividerMode === "leading"
        ? [
          outerLeftDivider,
          colIndex > 0 ? `inset 1px 0 0 ${verticalDividerColor}` : "",
          colIndex === stickyLeftCount - 1
            ? `inset -1px 0 0 ${verticalDividerColor}`
            : "",
          horizontalDivider,
        ].filter(Boolean)
        : [
          outerLeftDivider,
          `inset -1px 0 0 ${verticalDividerColor}`,
          horizontalDivider,
        ].filter(Boolean);

    return {
      left: `${getStickyLeftOffset(colIndex)}px`,
      boxShadow: stickyDividers.join(", "),
      ...(stickyLeftBorderMode === "shadow-only"
        ? { borderWidth: 0 }
        : { borderColor: "transparent" }),
      transform: shouldHideStickyLeftDrawer
        ? `translateX(-${stickyLeftDrawerWidth}px)`
        : "translateX(0)",
      transition: "transform 180ms ease",
      willChange: "transform",
    };
  };

  const getStickyBoundaryNeighborStyle = (
    colIndex: number
  ): React.CSSProperties | undefined => {
    if (
      stickyLeftCount === 0 ||
      colIndex !== stickyLeftCount ||
      shouldHideStickyLeftDrawer
    ) {
      return undefined;
    }

    return stickyLeftBorderMode === "shadow-only"
      ? { borderLeftWidth: 0 }
      : { borderLeftColor: "transparent" };
  };

  const getStickyLeftClassName = (
    colIndex: number,
    surface: "header" | "body" | "sign" = "body"
  ) => {
    if (colIndex >= stickyLeftCount) return "";

    return [
      "sticky",
      surface === "header" ? "z-30" : "z-10",
    ]
      .filter(Boolean)
      .join(" ");
  };

  const renderStickyLeftHorizontalDivider = (
    colIndex: number,
    surface: "header" | "body" | "sign" = "body"
  ) => {
    if (
      colIndex >= stickyLeftCount ||
      stickyLeftHorizontalBorderMode !== "border"
    ) {
      return null;
    }

    const dividerColor =
      surface === "header" ? "rgb(209 213 219)" : "rgb(229 231 235)";

    return (
      <span
        aria-hidden="true"
        style={{
          position: "absolute",
          left: 0,
          right: 0,
          bottom: 0,
          height: 1,
          backgroundColor: dividerColor,
          pointerEvents: "none",
          zIndex: 1,
        }}
      />
    );
  };

  const getStickyBodyBackgroundClassName = (rowClassName?: string) => {
    if (rowClassName?.includes("bg-[#EFEFEF]")) return "bg-[#EFEFEF]";
    if (rowClassName?.includes("bg-gray-50")) return "bg-gray-50";
    if (rowClassName?.includes("bg-gray-100")) return "bg-gray-100";
    if (rowClassName?.includes("bg-white")) return "bg-white";

    return "bg-white";
  };

  const getSummaryLabelContentStyle = (
    leftOffsetPx = 12
  ): React.CSSProperties => ({
    position: "sticky",
    left: `${leftOffsetPx}px`,
    zIndex: 12,
    maxWidth: `min(520px, calc(100vw - ${leftOffsetPx + 260}px))`,
  });

  const summaryChildLabelContentStyle: React.CSSProperties = {
    position: "sticky",
    left: "150px",
    zIndex: 12,
    maxWidth: "min(420px, calc(100vw - 620px))",
  };

  const getSummaryValueStyle = (
    backgroundColor: string,
    rightOffset: number | string = 0,
    showRightDivider = false
  ): React.CSSProperties => ({
    position: "sticky",
    right: typeof rightOffset === "number" ? `${rightOffset}px` : rightOffset,
    zIndex: 14,
    backgroundColor,
    borderLeftWidth: 0,
    borderRightWidth: showRightDivider ? 0 : undefined,
    boxShadow: [
      "inset 1px 0 0 rgb(209 213 219)",
      showRightDivider ? "inset -1px 0 0 rgb(209 213 219)" : "",
    ].filter(Boolean).join(", "),
  });

  const summaryBeforeValueCellStyle: React.CSSProperties = {
    borderRightWidth: 0,
  };

  const summaryAfterChildValueCellStyle: React.CSSProperties = {
    borderLeftWidth: 0,
  };

  const handleScroll = (event: React.UIEvent<HTMLDivElement>) => {
    const nextScrollLeft = event.currentTarget.scrollLeft;

    if (
      summaryScrollContainerRef.current &&
      Math.abs(summaryScrollContainerRef.current.scrollLeft - nextScrollLeft) > 0.5
    ) {
      summaryScrollContainerRef.current.scrollLeft = nextScrollLeft;
    }

    const didScrollHorizontally =
      Math.abs(nextScrollLeft - lastScrollLeftRef.current) > 0.5;

    lastScrollLeftRef.current = nextScrollLeft;

    if (!hideStickyLeftColsWhileScrolling || stickyLeftCount === 0) return;
    if (!didScrollHorizontally) return;

    if (scrollStopTimeoutRef.current) {
      clearTimeout(scrollStopTimeoutRef.current);
    }

    const shouldHideDrawer = nextScrollLeft >= Math.max(stickyLeftDrawerWidth - 8, 0);

    setIsStickyLeftDrawerHidden(shouldHideDrawer);

    scrollStopTimeoutRef.current = setTimeout(() => {
      setIsStickyLeftDrawerHidden(false);
    }, 220);
  };

  /* ---------------- Render ---------------- */

  /* ---------------- Render ---------------- */

  const renderColGroup = () => (
    <colgroup>
      {visibleLeafCols.map((c, index) => {
        const fixedWidth = getMinWidthForCol(c);

        const widthStyle: React.CSSProperties = shouldPreserveColumnWidths
          ? {
            width: `${fixedWidth}px`,
            minWidth: `${fixedWidth}px`,
          }
          : c.width
            ? { width: c.width }
            : {};

        return (
          <col
            key={`${c.key}-${index}`}
            style={widthStyle}
          />
        );
      })}
    </colgroup>
  );

  const renderTableHead = () => (
    <thead className={bodyMaxHeight ? "sticky top-0 z-20 font-bold" : "font-bold"}>
      {/* -------- Header Row 1 -------- */}
      <tr className={headerRow1ClassName}>
        {leftCols.map((c, colIndex) => (
          <th
            key={c.key}
            rowSpan={anyGroupExpanded ? 2 : 1}
            style={getStickyLeftStyle(colIndex, "header")}
            className={`${thBase} ${getStickyLeftClassName(colIndex, "header")} ${colIndex < stickyLeftCount ? "bg-[#5EA68E]" : ""} ${alignClass(c.align)} ${c.thClassName || ""} ${c.sortable ? "cursor-pointer select-none" : ""
              }`}
          >
            {renderHeaderContent(c)}
            {renderStickyLeftHorizontalDivider(colIndex, "header")}
          </th>
        ))}

        {resolvedLayout.map((item, itemIndex) => {
          if (item.type === "group") {
            const g = groupMap.get(item.id);
            if (!g) return null;
            const isCollapsed = collapsed[g.id] ?? true;
            const toggleIsCollapsed =
              getGroupToggleCollapsedState?.(g.id, isCollapsed) ?? isCollapsed;
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
                style={itemIndex === 0 ? getStickyBoundaryNeighborStyle(stickyLeftCount) : undefined}
                className={`${thBase} text-center ${g.headerClassName || ""}`}
              >
                <div className="flex w-full min-w-0 flex-col items-center justify-center gap-1 text-center leading-tight">
                  {/* Title row */}
                  <span className="block w-full min-w-0 whitespace-normal break-words text-center">
                    {g.label}
                  </span>

                  {/* Icons row */}
                  <div className="flex items-center justify-center gap-1">
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

                    {/* <button
                      type="button"
                      onClick={(e) => {
                        e.preventDefault();
                        e.stopPropagation();
                        toggleGroup(g.id);
                      }}
                      className={iconButtonClass}
                      title="Click to expand/collapse"
                    >
                      {isCollapsed ? "+" : "−"}
                    </button> */}
                    {g.expandable !== false && (
                      <button
                        type="button"
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          toggleGroup(g.id);
                        }}
                        className={iconButtonClass}
                        title="Click to expand/collapse"
                      >
                        {toggleIsCollapsed ? "+" : "−"}
                      </button>
                    )}
                  </div>
                </div>
              </th>
            );
          }

          const c = singleMap.get(item.key);
          if (!c) return null;

          const targetGroupId = toggleGroupByColKey?.[c.key];
          const isExpandable = Boolean(targetGroupId);
          const isTargetCollapsed = targetGroupId ? (collapsed[targetGroupId] ?? true) : true;

          return (
            <th
              key={c.key}
              rowSpan={anyGroupExpanded ? 2 : 1}
              style={itemIndex === 0 ? getStickyBoundaryNeighborStyle(stickyLeftCount) : undefined}
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
          {row2Cells.map((c, colIndex) => (
            <th
              key={c.key}
              style={colIndex === 0 ? getStickyBoundaryNeighborStyle(stickyLeftCount) : undefined}
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
      <tr className="bg-white font-bold text-center">
        {visibleLeafCols.map((c, colIndex) => {
          const sign = getSignForCol?.(c.key);

          return (
            <td
              key={c.key}
              style={getStickyLeftStyle(colIndex, "sign") ?? getStickyBoundaryNeighborStyle(colIndex)}
              className={`border ${cellPadding} ${getStickyLeftClassName(colIndex, "sign")} ${colIndex < stickyLeftCount ? "bg-white" : ""} ${sign?.className || ""}`}
            >
              {sign?.text || ""}
              {renderStickyLeftHorizontalDivider(colIndex, "sign")}
            </td>
          );
        })}
      </tr>
    ) : null;

  const renderBodyRows = (rowsToRender: RowT[], startIndex = 0) =>
    rowsToRender.map((row, idx) => {
      const realIndex = startIndex + idx;
      const rowClassName = getRowClassName?.(row, realIndex);
      const stickyBodyBackgroundClassName =
        getStickyBodyBackgroundClassName(rowClassName);

      return (
        <tr
          key={getRowKey?.(row, realIndex) ?? realIndex}
          className={rowClassName}
        >
          {visibleLeafCols.map((c, colIndex) => (
            <td
              key={c.key}
              style={getStickyLeftStyle(colIndex, "body") ?? getStickyBoundaryNeighborStyle(colIndex)}
              className={[
                "border",
                cellPadding,
                getStickyLeftClassName(colIndex, "body"),
                colIndex < stickyLeftCount ? stickyBodyBackgroundClassName : "",
                c.key === "sku" ? "text-left" : alignClass(c.align),
                "overflow-hidden truncate",
                c.tdClassName || "",
              ].join(" ")}
            >
              {getValue(row, c.key, realIndex)}
              {renderStickyLeftHorizontalDivider(colIndex, "body")}
            </td>
          ))}
        </tr>
      );
    });

  const renderSummaryFooter = () => {
    const summaryRows =
      summary?.rows ??
      [
        ...(summary?.sections || []).map((section) => ({
          ...section,
          type: "section" as const,
        })),
        ...(summary?.fixedRows || []).map((row) => ({
          ...row,
          type: "fixed" as const,
        })),
      ];

    if (summary?.enabled === false || summaryRows.length === 0) {
      return null;
    }

    const boldSectionsByDefault = summary?.boldSectionsByDefault ?? true;
    const summaryLabelColSpan = labelColSpan + midColSpan;
    const summaryEndCol = visibleLeafCols[visibleCount - 1];
    const fallbackSummaryEndValueOffset =
      summaryEndCol && !shouldPreserveColumnWidths && summaryEndCol.width
        ? typeof summaryEndCol.width === "number"
          ? `${summaryEndCol.width}px`
          : summaryEndCol.width
        : summaryEndCol
          ? getMinWidthForCol(summaryEndCol)
          : 0;
    const summaryEndValueOffset =
      summaryEndColumnWidth ?? fallbackSummaryEndValueOffset;
    const summaryChildValueRightOffset =
      typeof summaryEndValueOffset === "number"
        ? Math.max(summaryEndValueOffset - 1, 0)
        : `calc(${summaryEndValueOffset} - 1px)`;

    return (
      <tfoot>
        {summaryRows.map((item) => {
          if (item.type === "section") {
            const sec = item;
            const isCollapsed = summaryCollapsed[sec.id] ?? true;
            const isBold = sec.bold ?? boldSectionsByDefault;

            return (
              <React.Fragment key={sec.id}>
                <tr
                  onClick={() => toggleSummary(sec.id)}
                  role="button"
                  className={[
                    "cursor-pointer bg-gray-50",
                    isBold ? "font-semibold" : "",
                  ].join(" ")}
                  title="Click to expand/collapse"
                >
                  <td
                    colSpan={summaryLabelColSpan}
                    style={summaryBeforeValueCellStyle}
                    className="border border-gray-300 px-2 sm:px-3 py-3 text-left"
                  >
                    <span
                      style={getSummaryLabelContentStyle()}
                      className="inline-flex items-center gap-2 overflow-hidden whitespace-nowrap bg-gray-50 pr-3"
                    >
                      <span className="rounded border border-gray-400 px-1 text-xs">
                        {isCollapsed ? "+" : "−"}
                      </span>
                      <span className="min-w-0 truncate">{sec.label}</span>
                    </span>
                  </td>

                  <td
                    data-summary-main-value-cell="true"
                    colSpan={endColSpan}
                    style={getSummaryValueStyle("#ffffff")}
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
                        style={summaryBeforeValueCellStyle}
                        className="border border-gray-300 px-2 sm:px-3 py-3 text-left"
                      >
                        <span
                          style={summaryChildLabelContentStyle}
                          className="inline-block overflow-hidden truncate whitespace-nowrap bg-white pr-4 text-right"
                        >
                          {ch.label}
                        </span>
                      </td>

                      <td
                        colSpan={midColSpan}
                        style={getSummaryValueStyle("#ffffff", summaryChildValueRightOffset, true)}
                        className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center"
                      >
                        {ch.midValue ?? ""}
                      </td>

                      <td
                        colSpan={endColSpan}
                        style={summaryAfterChildValueCellStyle}
                        className="border border-gray-300 px-2 sm:px-3 py-3"
                      />
                    </tr>
                  ))}
              </React.Fragment>
            );
          }

          const r = item;
          const isBold = r.bold === true;

          return (
            <tr key={r.id} className={isBold ? "font-semibold" : undefined}>
              <td
                colSpan={summaryLabelColSpan}
                style={summaryBeforeValueCellStyle}
                className="border border-gray-300 px-2 sm:px-3 py-3 text-left"
              >
                <span
                  style={getSummaryLabelContentStyle()}
                  className="inline-flex items-center overflow-hidden whitespace-nowrap bg-white pr-3"
                >
                  <span className="min-w-0 truncate">{r.label}</span>
                </span>
              </td>

              <td
                data-summary-main-value-cell="true"
                colSpan={endColSpan}
                style={getSummaryValueStyle("#ffffff")}
                className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center"
              >
                {r.endValue ?? ""}
              </td>
            </tr>
          );
        })}
      </tfoot>
    );
  };

  /**
   * Scroll mode:
   * - Header, product rows, and total row share the visible horizontal scrollbar
   * - Summary rows render below that scrollbar without adding another one
   */
  if (bodyMaxHeight) {
    const summaryFooter = renderSummaryFooter();
    const summaryScrollbarCompensationStyle: React.CSSProperties = scrollbarWidth
      ? {
        paddingRight: `${scrollbarWidth}px`,
        boxSizing: "border-box",
      }
      : {};

    return (
      <div className="w-full">
        <div
          ref={scrollContainerRef}
          onScroll={handleScroll}
          className="w-full overflow-auto"
          style={{
            maxHeight: `${bodyMaxHeight}px`,
            scrollbarGutter: "stable",
          }}
        >
          <table ref={tableRef} className={tableClassName} style={tableStyle}>
            {renderColGroup()}

            <thead className="sticky top-0 z-20 font-bold">
              {renderTableHead().props.children}
            </thead>

            <tbody>
              {renderSignRow()}
              {renderBodyRows(scrollRows)}
              {pinnedRows.length > 0 &&
                renderBodyRows(pinnedRows, scrollRows.length)}
            </tbody>
          </table>
        </div>

        {summaryFooter && (
          <div
            ref={summaryScrollContainerRef}
            className="w-full overflow-x-hidden"
            style={summaryScrollbarCompensationStyle}
          >
            <table className={tableClassName} style={tableStyle}>
              {renderColGroup()}
              {summaryFooter}
            </table>
          </div>
        )}
      </div>
    );
  }
  /**
   * Normal mode:
   * Everything stays in one table
   */
  return (
    <div
      ref={scrollContainerRef}
      onScroll={handleScroll}
      className="w-full overflow-x-auto"
    >
      <table ref={tableRef} className={tableClassName} style={tableStyle}>
        {renderColGroup()}
        {renderTableHead()}

        <tbody>
          {renderSignRow()}
          {renderBodyRows(scrollRows)}
          {pinnedRows.length > 0 && renderBodyRows(pinnedRows, scrollRows.length)}
        </tbody>

        {renderSummaryFooter()}
      </table>
    </div>
  );
}
