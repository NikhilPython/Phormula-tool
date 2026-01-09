"use client";

import React, { useMemo, useState } from "react";

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

  getValue: (row: RowT, colKey: string, rowIndex: number) => React.ReactNode;
  getRowClassName?: (row: RowT, index: number) => string;

  showSignRowInBody?: boolean;
  getSignForCol?: (colKey: string) => { text: string; className?: string } | null;

  toggleGroupByColKey?: Record<string, string>;

  tableClassName?: string;
  headerRow1ClassName?: string;
  headerRow2ClassName?: string;
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
  tableClassName = "min-w-[800px] w-full table-auto border-collapse text-[#414042]",
  headerRow1ClassName = "bg-[#5EA68E] text-[#f8edcf]",
  headerRow2ClassName = "bg-[#5EA68E] text-[#f8edcf]",
}: Props<RowT>) {
  /* ---------------- State ---------------- */

  const [collapsed, setCollapsed] = useState<Record<string, boolean>>(() => {
    const base: Record<string, boolean> = {};
    groups.forEach((g) => (base[g.id] = true));
    return { ...base, ...(initialCollapsed || {}) };
  });

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

  const thBase =
    "whitespace-nowrap border border-gray-300 px-2 py-2 text-xs 2xl:text-sm";

  /* ---------------- Render ---------------- */

  return (
    <table className={tableClassName}>
      <thead className="sticky top-0 z-10 font-bold">
        {/* -------- Header Row 1 -------- */}
        {/* <tr className={headerRow1ClassName}>
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
      className={`${thBase} relative cursor-pointer select-none text-center ${g.headerClassName || ""}`}
      title="Click to expand/collapse"
    >
  
      <span className="absolute left-2 top-1/2 -translate-y-1/2 rounded border border-white/60 bg-white/10 px-1 text-xs leading-none">
        {isCollapsed ? "+" : "−"}
      </span>

      {g.label}
    </th>
  );
}


            const c = singleMap.get(item.key);
            if (!c) return null;
            const target = toggleGroupByColKey?.[c.key];

            return (
              <th
                key={c.key}
                rowSpan={2}
                onClick={target ? () => toggleGroup(target) : undefined}
                role={target ? "button" : undefined}
                className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""}`}
              >
                {c.label}
              </th>
            );
          })}
        </tr> */}


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
                className={`${thBase} ${alignClass(c.align)} ${c.thClassName || ""} ${isExpandable ? "relative cursor-pointer select-none pl-7" : ""
                  }`}
              >
                {c.label}
                {isExpandable && (
                  <span className="absolute left-2 top-1/2 -translate-y-1/2 rounded border border-white/60 bg-white/10 px-1 text-xs leading-none">
                    {isTargetCollapsed ? "+" : "−"}
                  </span>
                )}

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
                  className={`border px-2 py-1 ${sign?.className || ""}`}
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
                className={`border px-2 py-1 ${alignClass(c.align)} ${c.tdClassName || ""}`}
              >
                {getValue(row, c.key, idx)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
