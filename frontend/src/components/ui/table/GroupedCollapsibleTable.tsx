"use client";

import React, { useEffect, useMemo, useState } from "react";

/* ---------------- Types ---------------- */

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
  collapsedCols: LeafCol<RowT>[];
  expandedCols: LeafCol<RowT>[];
};

type LayoutItem<RowT> =
  | { type: "group"; id: string }
  | { type: "single"; key: string };

type Props<RowT> = {
  rows: RowT[];
  getRowKey?: (row: RowT, index: number) => string | number;

  leftCols: LeafCol<RowT>[];
  groups: ColGroup<RowT>[];
  singleCols: LeafCol<RowT>[];

  /** ✅ controls order: group / single / group / single */
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

};

export type SummaryRow<RowT> = {
  id: string;
  label: React.ReactNode;

  /** optional “expanded” value (the 2nd-last column in your current summary table) */
  midValue?: React.ReactNode;

  /** the “total / parent” value (the LAST column) */
  endValue?: React.ReactNode;

  /** optional: customize label alignment */
  labelAlign?: Align;
};

export type SummarySection<RowT> = {
  id: string;
  label: React.ReactNode;

  /** parent row last-column value */
  endValue: React.ReactNode;

  /** child rows shown when expanded */
  children: SummaryRow<RowT>[];

  /** default collapsed/expanded */
  defaultCollapsed?: boolean;
};

export type SummaryBlock<RowT> = {
  /** render summary only when this returns true */
  enabled?: boolean;

  /** collapsible sections like Ads / Other */
  sections?: SummarySection<RowT>[];

  /** always-visible rows like Shipment Charges, CM2 rows, etc. */
  fixedRows?: SummaryRow<RowT>[];

  /**
   * How many columns from the end you want to reserve for values.
   * Your existing summary layout uses:
   * - label spans (visibleCols - 2)
   * - midValue in 2nd-last
   * - endValue in last
   */
  valueCols?: 2; // keep it 2 for your design
};


/* ---------------- Utils ---------------- */

const alignClass = (align?: Align) =>
  align === "left" ? "text-left" : align === "right" ? "text-right" : "text-center";

/* ---------------- Component ---------------- */

export default function GroupedCollapsibleTable<RowT>({
  rows,
  getRowKey,
  leftCols,
  groups,
  singleCols,
  layout,
  initialCollapsed,
  getValue,
  getRowClassName,
  showSignRowInBody = false,
  getSignForCol,
  toggleGroupByColKey,
  onVisibleColCountChange,
  tableClassName = "min-w-[800px] w-full table-auto border-collapse text-[#414042] text-[10px] 2xl:text-xs ",
  headerRow1ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  headerRow2ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  summary
}: Props<RowT>) {
  /* ---------------- State ---------------- */

  const [collapsed, setCollapsed] = useState<Record<string, boolean>>(() => {
    const base: Record<string, boolean> = {};
    groups.forEach((g) => (base[g.id] = true));
    return { ...base, ...(initialCollapsed || {}) };
  });

  const [summaryCollapsed, setSummaryCollapsed] = useState<Record<string, boolean>>(() => {
    const base: Record<string, boolean> = {};
    (summary?.sections || []).forEach((s) => {
      base[s.id] = s.defaultCollapsed ?? true;
    });
    return base;
  });

  const toggleSummary = (id: string) =>
    setSummaryCollapsed((p) => ({ ...p, [id]: !p[id] }));


  const toggleGroup = (id: string) =>
    setCollapsed((p) => ({ ...p, [id]: !p[id] }));

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

  const row2LeafCols = useMemo(() => {
    const out: LeafCol<RowT>[] = [];
    for (const item of resolvedLayout) {
      if (item.type !== "group") continue;
      const g = groupMap.get(item.id);
      if (!g) continue;
      const isCollapsed = collapsed[g.id];
      out.push(...(isCollapsed ? g.collapsedCols : g.expandedCols));
    }
    return out;
  }, [resolvedLayout, collapsed, groupMap]);

  const visibleCount = visibleLeafCols.length;
  const valueCols = summary?.valueCols ?? 2;
  const labelColSpan = Math.max(visibleCount - valueCols, 1);
  const midColSpan = 1;
  const endColSpan = 1;

  const cellPadding = "px-2 sm:px-3 py-3";
  const thBase =
    `whitespace-nowrap border border-gray-300 ${cellPadding}`;

  /* ---------------- Render ---------------- */

  return (
    <table className={tableClassName}>
      <thead className="sticky top-0 z-10 font-bold">
        {/* -------- Header Row 1 -------- */}

        {/* -------- Header Row 1 -------- */}
        <tr className={headerRow1ClassName}>
          {leftCols.map((c) => (
            <th
              key={c.key}
              rowSpan={2}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
            >
              {c.label}
            </th>
          ))}

          {resolvedLayout.map((item) => {
            // ----- GROUP HEADERS (no +/- here) -----
            if (item.type === "group") {
              const g = groupMap.get(item.id);
              if (!g) return null;

              const isCollapsed = collapsed[g.id];
              const cols = isCollapsed ? g.collapsedCols : g.expandedCols;
              if (cols.length === 0) return null;

              return (
                <th
                  key={g.id}
                  colSpan={cols.length}
                  onClick={() => toggleGroup(g.id)}
                  role="button"
                  className={`${thBase} cursor-pointer select-none text-center ${g.headerClassName || ""}`}
                  title="Click to expand/collapse"
                >
                  {g.label}
                </th>
              );
            }

            // ----- SINGLE HEADERS (show +/- if they toggle a group) -----
            const c = singleMap.get(item.key);
            if (!c) return null;

            const targetGroupId = toggleGroupByColKey?.[c.key];
            const isExpandable = Boolean(targetGroupId);
            const isTargetCollapsed = targetGroupId ? collapsed[targetGroupId] : true;

            return (
              <th
                key={c.key}
                rowSpan={2}
                onClick={isExpandable ? () => toggleGroup(targetGroupId!) : undefined}
                role={isExpandable ? "button" : undefined}
                title={isExpandable ? "Click to expand/collapse" : undefined}
                className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""} ${isExpandable ? "cursor-pointer select-none" : ""
                  }`}
              >
                <div className="flex items-center justify-center gap-2 min-w-0">
                  {isExpandable && (
                    <span className="shrink-0 rounded border border-white/60 bg-white/10 px-1 text-xs leading-none">
                      {isTargetCollapsed ? "+" : "−"}
                    </span>
                  )}

                  {/* label */}
                  <span className="min-w-0 truncate">{c.label}</span>
                </div>
              </th>
            );

          })}
        </tr>


        {/* -------- Header Row 2 -------- */}
        <tr className={headerRow2ClassName}>
          {row2LeafCols.map((c) => (
            <th
              key={c.key}
              className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
            >
              {c.label}
            </th>
          ))}
        </tr>
      </thead>

      <tbody>
        {showSignRowInBody && (
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
        )}

        {rows.map((row, idx) => (
          <tr key={getRowKey?.(row, idx) ?? idx} className={getRowClassName?.(row, idx)}>
            {visibleLeafCols.map((c) => (
              <td
                key={c.key}
                className={`border ${cellPadding} ${alignClass(c.align)} ${c.tdClassName || ""}`}
              >

                {getValue(row, c.key, idx)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>


      {summary?.enabled !== false && (summary?.sections?.length || summary?.fixedRows?.length) ? (
        <>
          {/* ---------- Collapsible Sections ---------- */}
          {(summary.sections || []).map((sec) => {
            const isCollapsed = summaryCollapsed[sec.id] ?? true;

            return (
              <React.Fragment key={sec.id}>
                {/* Parent row */}
                <tr
                  onClick={() => toggleSummary(sec.id)}
                  role="button"
                  className="cursor-pointer font-semibold bg-gray-50"
                  title="Click to expand/collapse"
                >
                  <td colSpan={labelColSpan} className="border border-gray-300 px-2 sm:px-3 py-3 text-left">
                    <span className="inline-flex items-center gap-2">
                      <span className="rounded border border-gray-400 px-1 text-xs">
                        {isCollapsed ? "+" : "−"}
                      </span>
                      {sec.label}
                    </span>
                  </td>

                  {/* 2nd-last blank (matches your current layout) */}
                  <td colSpan={midColSpan} className="border border-gray-300 px-2 sm:px-3 py-3" />

                  {/* LAST column value */}
                  <td colSpan={endColSpan} className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center">
                    {sec.endValue}
                  </td>
                </tr>

                {/* Child rows */}
                {!isCollapsed &&
                  sec.children.map((ch) => (
                    <tr key={ch.id}>
                      <td
                        colSpan={labelColSpan}
                        className="border border-gray-300 px-2 sm:px-3 py-3 pl-8 text-right"
                      >
                        {ch.label}
                      </td>

                      {/* 2nd-last value */}
                      <td colSpan={midColSpan} className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center">
                        {ch.midValue ?? ""}
                      </td>

                      {/* last blank */}
                      <td colSpan={endColSpan} className="border border-gray-300 px-2 sm:px-3 py-3" />
                    </tr>
                  ))}
              </React.Fragment>
            );
          })}

          {/* ---------- Fixed rows (always visible) ---------- */}
          {(summary.fixedRows || []).map((r) => (
            <tr key={r.id}>
              <td colSpan={labelColSpan} className="border border-gray-300 px-2 sm:px-3 py-3 text-left">
                {r.label}
              </td>

              {/* keep mid column empty for fixed rows (like your current design) */}
              <td colSpan={midColSpan} className="border border-gray-300 px-2 sm:px-3 py-3" />

              {/* last column value */}
              <td colSpan={endColSpan} className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center">
                {r.endValue ?? ""}
              </td>
            </tr>
          ))}
        </>
      ) : null}

    </table>
  );
}
