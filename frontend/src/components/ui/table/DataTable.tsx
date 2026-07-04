"use client";

import * as React from "react";
import clsx from "clsx";
import Loader from "@/components/loader/Loader";
import { FaChevronLeft, FaChevronRight } from "react-icons/fa";

export type Row = Record<string, React.ReactNode>;

export type ColumnDef<T extends Row> = {
  key: keyof T | string;
  header: React.ReactNode;
  render?: (row: T, value: React.ReactNode, rowIndex: number) => React.ReactNode;
  width?: string; // optional fixed width (e.g. "140px")
  cellClassName?: string;
  headerClassName?: string;
  onHeaderClick?: () => void;
};

type DataTableProps<T extends Row> = {
  columns: ColumnDef<T>[];
  data: T[];

  className?: string;
  tableClassName?: string;
  maxHeight?: number | string;
  stickyHeader?: boolean;
  zebra?: boolean;
  emptyMessage?: string;
  showCellTitle?: boolean;

  pageSize?: number;
  initialPage?: number;
  paginate?: boolean;
  scrollY?: boolean;

  rowClassName?: (row: T, rowIndex: number) => string;
  onPageChange?: (page: number) => void;
  isTotalRow?: (row: T) => boolean;
  bodyMaxHeight?: number | string;
  loading?: boolean;
  loaderHeight?: number | string;

  headerMaxWidth?: number; // default 140
};

export default function DataTable<T extends Row>({
  columns,
  data,
  className,
  tableClassName,
  maxHeight,
  stickyHeader = true,
  zebra = true,
  emptyMessage = "No data found.",
  showCellTitle = false,
  pageSize = 10,
  initialPage = 1,
  paginate = true,
  scrollY = true,
  rowClassName,
  isTotalRow,
  bodyMaxHeight,
  onPageChange,
  loading = false,
  loaderHeight = 260,
  headerMaxWidth = 130,
}: DataTableProps<T>) {
  const containerStyle: React.CSSProperties = {
    maxHeight: scrollY
      ? typeof maxHeight === "number"
        ? `${maxHeight}px`
        : maxHeight
      : undefined,
  };

  const hasData = Array.isArray(data) && data.length > 0;

  const loaderStyleHeight =
    typeof loaderHeight === "number" ? `${loaderHeight}px` : loaderHeight;

  const [page, setPage] = React.useState<number>(Math.max(1, initialPage));

  React.useEffect(() => {
    const totalPages = Math.max(1, Math.ceil((data?.length ?? 0) / pageSize));
    if (page > totalPages) setPage(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, pageSize]);

  const total = data?.length ?? 0;
  const totalPages = paginate ? Math.max(1, Math.ceil(total / pageSize)) : 1;

  const pageRows = React.useMemo(() => {
    if (!hasData) return [];
    if (!paginate) return data;
    const start = (page - 1) * pageSize;
    return data.slice(start, start + pageSize);
  }, [data, page, pageSize, hasData, paginate]);

  const shouldPinTotalRows = bodyMaxHeight !== undefined;

  const scrollRows = React.useMemo(() => {
    if (!shouldPinTotalRows) return pageRows;
    return pageRows.filter((row) => !isTotalRow?.(row));
  }, [pageRows, shouldPinTotalRows, isTotalRow]);

  const pinnedRows = React.useMemo(() => {
    if (!shouldPinTotalRows) return [];
    return pageRows.filter((row) => isTotalRow?.(row));
  }, [pageRows, shouldPinTotalRows, isTotalRow]);

  const bodyScrollRef = React.useRef<HTMLDivElement | null>(null);
  const [scrollbarWidth, setScrollbarWidth] = React.useState(0);

  React.useEffect(() => {
    if (!shouldPinTotalRows) return;

    const measure = () => {
      const el = bodyScrollRef.current;
      if (!el) return;

      const width = el.offsetWidth - el.clientWidth;
      setScrollbarWidth(width > 0 ? width : 0);
    };

    measure();

    const el = bodyScrollRef.current;
    const resizeObserver =
      typeof ResizeObserver !== "undefined" && el
        ? new ResizeObserver(measure)
        : null;

    if (resizeObserver && el) {
      resizeObserver.observe(el);
    }

    window.addEventListener("resize", measure);

    return () => {
      resizeObserver?.disconnect();
      window.removeEventListener("resize", measure);
    };
  }, [shouldPinTotalRows, bodyMaxHeight, pageRows.length, columns.length]);

  const scrollbarCompensationStyle: React.CSSProperties =
    shouldPinTotalRows && scrollbarWidth
      ? {
        paddingRight: `${scrollbarWidth}px`,
        boxSizing: "border-box",
      }
      : {};

  const bodyScrollStyle: React.CSSProperties = {
    maxHeight:
      bodyMaxHeight === undefined
        ? undefined
        : typeof bodyMaxHeight === "number"
          ? `${bodyMaxHeight}px`
          : bodyMaxHeight,
  };

  if (loading) {
    return (
      <div
        className={clsx(
          "flex items-center justify-center rounded-xl border border-slate-200 bg-white shadow-sm",
          className
        )}
        style={{ minHeight: loaderStyleHeight }}
      >
        <Loader fullscreen transparent />
      </div>
    );
  }

  const goToPage = (p: number) => {
    const next = Math.min(Math.max(1, p), totalPages);
    setPage(next);
    onPageChange?.(next);
  };

  const onPrev = () => goToPage(page - 1);
  const onNext = () => goToPage(page + 1);

  const getPageItems = (current: number, totalP: number): (number | "dots")[] => {
    const items: (number | "dots")[] = [];
    if (totalP <= 7) {
      for (let i = 1; i <= totalP; i += 1) items.push(i);
      return items;
    }
    if (current <= 3) items.push(1, 2, 3, "dots", totalP - 1, totalP);
    else if (current >= totalP - 2) items.push(1, 2, "dots", totalP - 2, totalP - 1, totalP);
    else items.push(1, "dots", current - 1, current, current + 1, "dots", totalP);
    return items;
  };

  const pageItems = getPageItems(page, totalPages);

  const formatHeader = (header: React.ReactNode) => {
    if (typeof header !== "string") return header;

    const words = header.split(" ");
    return words
      .map((w, idx) => {
        const lower = w.toLowerCase();
        if (lower === "sku") return "SKU";

        if (
          idx > 0 &&
          words[idx - 1].toLowerCase() === "sku" &&
          ["uk", "us", "canada"].includes(lower)
        ) {
          return w.toUpperCase();
        }

        return w.charAt(0).toUpperCase() + w.slice(1);
      })
      .join(" ");
  };

  const thStyle = (col: ColumnDef<T>): React.CSSProperties => {
    if (col.width) {
      return {
        width: col.width,
        minWidth: col.width,
        maxWidth: col.width,
      };
    }

    return {
      minWidth: `${headerMaxWidth}px`, // 👈 default min width for headers
    };
  };

  const isCssSize = (v?: string) => {
    if (!v) return false;
    // accept: 120px, 12rem, 20%, 10vw, etc.
    return /^(\d+(\.\d+)?)(px|rem|em|%|vw|vh)$/.test(v.trim());
  };

  const getWidthStyle = (col: ColumnDef<T>): React.CSSProperties => {
    if (typeof col.width === "string" && isCssSize(col.width)) {
      return { width: col.width, minWidth: col.width, maxWidth: col.width };
    }
    // default min width so headers don't crush on mobile
    return { minWidth: `${headerMaxWidth}px` };
  };

  const getWidthClass = (col: ColumnDef<T>) => {
    if (typeof col.width !== "string") return "";
    // if it looks like tailwind width tokens, treat as className not style
    if (/\bw-|\bmin-w-|\bmax-w-/.test(col.width)) return col.width;
    return "";
  };

  const colGroup = (
    <colgroup>
      {columns.map((col, i) => (
        <col
          key={String(col.key) + i}
          style={getWidthStyle(col)}
          className={getWidthClass(col)}
        />
      ))}
    </colgroup>
  );

  return (
    <div
      className={clsx(
        "relative w-full max-w-full border border-slate-200 bg-white shadow-sm",
        "rounded-xl overflow-hidden",
        scrollY && !shouldPinTotalRows && "overflow-y-auto",
        className
      )}
      style={shouldPinTotalRows ? undefined : containerStyle}
    >
      {/* Horizontal scroll wrapper for header + body + pinned total */}
      <div className="w-full overflow-x-auto [-webkit-overflow-scrolling:touch]">
        {/* Header table */}
        <div style={scrollbarCompensationStyle}>
          <table
            className={clsx(
              "border-separate border-spacing-0",
              "text-xs 2xl:text-sm text-slate-700",
              "min-w-max w-max lg:w-full",
              "table-fixed",
              tableClassName
            )}
          >
            {colGroup}

            <thead>
              <tr>
                {columns.map((col, i) => (
                  <th
                    key={String(col.key) + i}
                    onClick={col.onHeaderClick}
                    style={getWidthStyle(col)}
                    className={clsx(
                      stickyHeader && "sticky top-0 z-20",
                      "bg-[#5EA68E] text-yellow-200 font-bold",
                      "border-b border-r border-gray-300",
                      i === columns.length - 1 && "border-r-0",
                      "px-3 py-2 text-center align-middle whitespace-nowrap",
                      getWidthClass(col),
                      col.headerClassName,
                      col.onHeaderClick && "cursor-pointer select-none"
                    )}
                  >
                    <div
                      className="
                      overflow-hidden text-ellipsis
                      [display:-webkit-box]
                      [-webkit-box-orient:vertical]
                      [-webkit-line-clamp:2]
                    "
                    >
                      {formatHeader(col.header)}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
          </table>
        </div>

        {/* Scrollable body table */}
        <div
          ref={bodyScrollRef}
          className={clsx(
            shouldPinTotalRows &&
            "overflow-y-auto overflow-x-hidden [scrollbar-gutter:stable]"
          )}
          style={bodyScrollStyle}
        >
          <table
            className={clsx(
              "border-separate border-spacing-0",
              "text-xs 2xl:text-sm text-slate-700",
              "min-w-max w-max lg:w-full",
              "table-fixed",
              tableClassName
            )}
          >
            {colGroup}

            <tbody>
              {!hasData && (
                <tr>
                  <td
                    className="px-3 py-8 text-center text-slate-400"
                    colSpan={columns.length}
                  >
                    {emptyMessage}
                  </td>
                </tr>
              )}

              {hasData &&
                scrollRows.map((row, ri) => (
                  <tr
                    key={ri}
                    className={clsx(
                      rowClassName?.(row, (page - 1) * pageSize + ri),
                      "h-[40px] transition-colors"
                    )}
                  >
                    {columns.map((col, ci) => {
                      const value =
                        (row as Record<string, React.ReactNode>)[String(col.key)];

                      const keyStr = String(col.key);
                      const isTextCol =
                        keyStr === "productName" ||
                        keyStr === "product_name" ||
                        keyStr === "alert";

                      return (
                        <td
                          key={String(col.key) + ci}
                          style={getWidthStyle(col)}
                          className={clsx(
                            "border-b border-r border-[#e1e5ea]",
                            ci === columns.length - 1 && "border-r-0",
                            "px-3 py-2 align-middle text-center min-w-0",
                            isTextCol
                              ? "whitespace-normal break-words"
                              : "whitespace-nowrap",
                            getWidthClass(col),
                            col.cellClassName
                          )}
                          title={
                            showCellTitle ? String(value ?? "\u00A0") : undefined
                          }
                        >
                          {isTextCol ? (
                            <div className="leading-snug max-w-[220px] sm:max-w-[280px] lg:max-w-none">
                              {col.render
                                ? col.render(
                                  row as any,
                                  value,
                                  (page - 1) * pageSize + ri
                                )
                                : value ?? "\u00A0"}
                            </div>
                          ) : col.render ? (
                            col.render(
                              row as any,
                              value,
                              (page - 1) * pageSize + ri
                            )
                          ) : (
                            value ?? "\u00A0"
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
            </tbody>
          </table>
        </div>

        {/* Fixed total row table */}
        {pinnedRows.length > 0 && (
          <div style={scrollbarCompensationStyle}>
            <table
              className={clsx(
                "border-separate border-spacing-0",
                "text-xs 2xl:text-sm text-slate-700",
                "min-w-max w-max lg:w-full",
                "table-fixed",
                tableClassName
              )}
            >
              {colGroup}

              <tbody>
                {pinnedRows.map((row, ri) => {
                  const realIndex = scrollRows.length + ri;

                  return (
                    <tr
                      key={`pinned-${ri}`}
                      className={clsx(
                        rowClassName?.(
                          row,
                          (page - 1) * pageSize + realIndex
                        ),
                        "h-[40px] transition-colors"
                      )}
                    >
                      {columns.map((col, ci) => {
                        const value =
                          (row as Record<string, React.ReactNode>)[String(col.key)];

                        const keyStr = String(col.key);
                        const isTextCol =
                          keyStr === "productName" ||
                          keyStr === "product_name" ||
                          keyStr === "alert";

                        return (
                          <td
                            key={String(col.key) + ci}
                            style={getWidthStyle(col)}
                            className={clsx(
                              "border-b border-r border-[#e1e5ea]",
                              ci === columns.length - 1 && "border-r-0",
                              "px-3 py-2 align-middle text-center min-w-0",
                              isTextCol
                                ? "whitespace-normal break-words"
                                : "whitespace-nowrap",
                              getWidthClass(col),
                              col.cellClassName
                            )}
                            title={
                              showCellTitle ? String(value ?? "\u00A0") : undefined
                            }
                          >
                            {isTextCol ? (
                              <div className="leading-snug max-w-[220px] sm:max-w-[280px] lg:max-w-none">
                                {col.render
                                  ? col.render(
                                    row as any,
                                    value,
                                    (page - 1) * pageSize + realIndex
                                  )
                                  : value ?? "\u00A0"}
                              </div>
                            ) : col.render ? (
                              col.render(
                                row as any,
                                value,
                                (page - 1) * pageSize + realIndex
                              )
                            ) : (
                              value ?? "\u00A0"
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Pagination outside horizontal scroll */}
      {paginate && totalPages > 1 && (
        <div className="border-t border-slate-200 bg-slate-50 px-4 py-3">
          <div className="flex items-center justify-between gap-4 text-[10px] 2xl:text-xs">
            <button
              onClick={onPrev}
              disabled={page <= 1}
              className={clsx(
                "inline-flex items-center rounded-md border border-slate-200 bg-white px-2 py-1.5 text-slate-700 shadow-sm hover:bg-slate-100",
                "disabled:cursor-not-allowed disabled:opacity-50"
              )}
            >
              <FaChevronLeft />
            </button>

            <div className="flex items-center justify-center gap-1 sm:gap-1.5">
              {pageItems.map((item, idx) =>
                item === "dots" ? (
                  <span
                    key={`dots-${idx}`}
                    className="px-1 text-slate-400 select-none"
                  >
                    …
                  </span>
                ) : (
                  <button
                    key={item}
                    onClick={() => goToPage(item)}
                    className={clsx(
                      "h-7 w-7 sm:h-8 sm:w-8 rounded-full",
                      "flex items-center justify-center transition-colors",
                      item === page
                        ? "bg-slate-200 text-slate-900 font-semibold"
                        : "text-slate-700 hover:bg-slate-100"
                    )}
                  >
                    {item}
                  </button>
                )
              )}
            </div>

            <button
              onClick={onNext}
              disabled={page >= totalPages}
              className={clsx(
                "inline-flex items-center rounded-md border border-slate-200 bg-white px-2 py-1.5 text-slate-700 shadow-sm hover:bg-slate-100",
                "disabled:cursor-not-allowed disabled:opacity-50"
              )}
            >
              <FaChevronRight />
            </button>
          </div>
        </div>
      )}
    </div>
  );

}
