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

  loading?: boolean;
  loaderHeight?: number | string;

  /**
   * ✅ New: header wrap control
   * This sets the default max-width for headers so they wrap naturally
   * instead of forcing horizontal scroll.
   */
  headerMaxWidth?: number; // default 140
};

export default function DataTable<T extends Row>({
  columns,
  data,
  className,
  tableClassName,
  maxHeight = "60vh",
  stickyHeader = true,
  zebra = true,
  emptyMessage = "No data found.",
  showCellTitle = false,
  pageSize = 10,
  initialPage = 1,
  paginate = true,
  scrollY = true,
  rowClassName,
  onPageChange,
  loading = false,
  loaderHeight = 260,
  headerMaxWidth = 140, // ✅ tune this (120–180 usually best)
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

  if (loading) {
    return (
      <div
        className={clsx(
          "flex items-center justify-center rounded-xl border border-slate-200 bg-white shadow-sm",
          className
        )}
        style={{ minHeight: loaderStyleHeight }}
      >
        <Loader
          src="/infinity-unscreen.gif"
          size={150}
          transparent
          roundedClass="rounded-full"
          backgroundClass="bg-transparent"
          respectReducedMotion
        />
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

  // keep your existing capitalization behavior (but NO "()" splitting)
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

  // ✅ If a column has width, use it, else use headerMaxWidth
  const thStyle = (col: ColumnDef<T>): React.CSSProperties => {
    const w = col.width ? col.width : `${headerMaxWidth}px`;
    // With table-fixed, width is respected. maxWidth helps wrapping if widths are not strict.
    return { width: w, maxWidth: w };
  };

  return (
    <div
      className={clsx(
        "relative w-full max-w-full border border-slate-200 bg-white shadow-sm",
        // ✅ allow scroll ONLY when truly needed
        "overflow-x-auto",
        scrollY && "overflow-y-auto",
        className
      )}
      style={containerStyle}
    >
      <table
        className={clsx(
          "w-full table-fixed border-collapse text-[10px] 2xl:text-xs text-slate-700",
          tableClassName
        )}
      >
        <thead
          className={clsx(
            "bg-[#5EA68E] text-yellow-200 font-bold",
            stickyHeader && "sticky top-0 z-10"
          )}
        >
          <tr>
            {columns.map((col, i) => (
              <th
                key={String(col.key) + i}
                onClick={col.onHeaderClick}
                className={clsx(
                  // "border border-[#e1e5ea] px-2 sm:px-3 py-2 text-center align-middle",
                  // "whitespace-normal break-words leading-tight",
                  "whitespace-nowrap border border-gray-300 px-2 py-2 text-center",
                  col.headerClassName,
                  col.onHeaderClick && "cursor-pointer select-none"
                )}
                style={thStyle(col)}
              >
                {formatHeader(col.header)}
              </th>
            ))}
          </tr>
        </thead>

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
            pageRows.map((row, ri) => (
              <tr
                key={ri}
                className={clsx(
                  rowClassName?.(row, (page - 1) * pageSize + ri),
                  "transition-colors",
                  (row as any).__isTotal
                    ? "bg-[#EFEFEF] font-semibold"
                    : zebra && ri % 2 === 1
                      ? ""
                      : ""
                )}
              >

                {columns.map((col, ci) => {
                  const value = (row as Record<string, React.ReactNode>)[String(col.key)];

                  return (
                    <td
                      key={String(col.key) + ci}
                      className={clsx(
                        "border border-[#e1e5ea] px-2 sm:px-3 py-3 align-middle text-center",
                        "whitespace-nowrap",
                        col.cellClassName
                      )}
                      title={showCellTitle ? String(value ?? "\u00A0") : undefined}
                    >
                      {col.render
                        ? col.render(row as any, value, (page - 1) * pageSize + ri)
                        : value ?? "\u00A0"}
                    </td>
                  );
                })}
              </tr>
            ))}
        </tbody>
      </table>

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
                  <span key={`dots-${idx}`} className="px-1 text-slate-400 select-none">
                    …
                  </span>
                ) : (
                  <button
                    key={item}
                    onClick={() => goToPage(item)}
                    className={clsx(
                      "h-7 w-7 sm:h-8 sm:w-8 rounded-full",
                      "flex items-center justify-center",
                      "transition-colors",
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
