"use client";

import React, { useEffect, useMemo, useState } from "react";

/* ---------------- Types ---------------- */

export type Align = "left" | "center" | "right";

// export type LeafCol<RowT> = {
//   key: string;
//   label: React.ReactNode;
//   excelLabel?: string;
//   align?: Align;
//   width?: number | string;      // ✅ ADD
//   tooltip?: React.ReactNode;
//   thClassName?: string;
//   tdClassName?: string;
// };

// export type ColGroup<RowT> = {
//   id: string;
//   label: React.ReactNode;
//   headerClassName?: string;
//   collapsedCols: LeafCol<RowT>[];
//   expandedCols: LeafCol<RowT>[];
// };


export type LeafCol<RowT> = {
  key: string;
  label: React.ReactNode;
  excelLabel?: string;
  align?: Align;
  width?: number | string;
  tooltip?: React.ReactNode;
  info?: React.ReactNode;          // ✅ ADD THIS
  thClassName?: string;
  tdClassName?: string;
  noWrap?: boolean;
};

export type ColGroup<RowT> = {
  id: string;
  label: React.ReactNode;
  info?: React.ReactNode;          // ✅ ADD THIS
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
  getValue,
  getRowClassName,
  showSignRowInBody = false,
  getSignForCol,
  toggleGroupByColKey,
  onVisibleColCountChange,
  tableClassName = "min-w-[800px] w-full table-auto border-collapse bg-white text-[#414042] text-xs 2xl:text-sm",
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

  return (
    <table className={tableClassName}>
      {/* 🔹 Column width controller */}
      <colgroup>
        {visibleLeafCols.map((c) => (
          <col
            key={c.key}
            style={c.width ? { width: c.width } : undefined}
          />
        ))}
      </colgroup>

      <thead className="sticky top-0 z-10 font-bold">


        {/* -------- Header Row 1 -------- */}
        <tr className={headerRow1ClassName}>
          {leftCols.map((c) => (
            <th
              key={c.key}
              // rowSpan={2}
              rowSpan={anyGroupExpanded ? 2 : 1}
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

              const groupRowSpan = anyGroupExpanded ? (isCollapsed ? 2 : 1) : 1;

              return (
                <th
                  key={g.id}
                  colSpan={cols.length}
                  rowSpan={groupRowSpan}
                  onClick={() => toggleGroup(g.id)}
                  role="button"
                  className={`${thBase} cursor-pointer select-none text-center ${g.headerClassName || ""}`}
                  title="Click to expand/collapse"
                >
                  <div className="flex w-full items-center">
                    {/* TEXT + INFO */}
                    <span className="flex-1 inline-flex items-center justify-center gap-1 whitespace-normal break-words leading-tight">
                      <span className="flex items-center">{g.label}</span>

                      {g.info && (
                        <span className="flex items-center shrink-0">
                          {g.info}
                        </span>
                      )}
                    </span>

                    {/* +/- TOGGLE */}
                    <span className="shrink-0 ml-2 rounded border border-white/60 bg-white/10 px-1 text-xs leading-none">
                      {isCollapsed ? "+" : "−"}
                    </span>
                  </div>
                </th>
              );
            }

            // ----- SINGLE HEADERS (show +/- if they toggle a group) -----
            const c = singleMap.get(item.key);
            if (!c) return null;

            const targetGroupId = toggleGroupByColKey?.[c.key];
            const isExpandable = Boolean(targetGroupId);
            const isTargetCollapsed = targetGroupId ? collapsed[targetGroupId] : true;
            const hasLeft = isExpandable;
            const hasRight = Boolean(c.info);
            return (
              <th
                key={c.key}
                // rowSpan={2}
                rowSpan={anyGroupExpanded ? 2 : 1}
                onClick={isExpandable ? () => toggleGroup(targetGroupId!) : undefined}
                role={isExpandable ? "button" : undefined}
                title={isExpandable ? "Click to expand/collapse" : undefined}
                className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""} ${isExpandable ? "cursor-pointer select-none" : ""
                  }`}
              >
                <div className="flex w-full items-center">
                  {/* TEXT + INFO */}
                  <span
                    className={`flex-1 inline-flex items-center justify-center gap-1 leading-tight ${c.noWrap ? "whitespace-nowrap" : "whitespace-normal break-words"
                      }`}
                  >
                    <span className="flex items-center">{c.label}</span>

                    {c.info && (
                      <span className="flex items-center shrink-0">
                        {c.info}
                      </span>
                    )}
                  </span>

                  {/* +/- TOGGLE */}
                  {hasLeft && (
                    <span className="shrink-0 ml-2 rounded border border-white/60 bg-white/10 px-1 text-xs leading-none">
                      {isTargetCollapsed ? "+" : "−"}
                    </span>
                  )}
                </div>
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
                {c.label}
              </th>
            ))}
          </tr>
        )}
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
        <tfoot>
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

                  <td colSpan={midColSpan} className="border border-gray-300 px-2 sm:px-3 py-3" />
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

                      <td colSpan={midColSpan} className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center">
                        {ch.midValue ?? ""}
                      </td>

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

              <td colSpan={midColSpan} className="border border-gray-300 px-2 sm:px-3 py-3" />

              <td colSpan={endColSpan} className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center">
                {r.endValue ?? ""}
              </td>
            </tr>
          ))}
        </tfoot>
      ) : null}

    </table>
  );
}
