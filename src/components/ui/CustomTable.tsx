// components/ui/CustomTable.tsx
"use client";

import React from "react";

export type CustomTableProps = {
  headers: React.ReactNode[];
  rows: React.ReactNode[][];
  className?: string;
  headerClassName?: string;
  cellClassName?: string;
};

export default function CustomTable({
  headers,
  rows,
  className,
  headerClassName,
  cellClassName,
}: CustomTableProps) {
  const base =
    "w-full border border-neutral-200 rounded-lg overflow-hidden bg-white";
  const th =
    headerClassName ||
    "px-4 py-2 text-center border border-neutral-200 font-semibold text-emerald-600 text-sm md:text-base";
  const td =
    cellClassName ||
    "px-4 py-2 text-center border border-neutral-200 text-sm md:text-base";

  return (
    <div className={className}>
      <table className={`${base} border-collapse`}>
        <thead>
          <tr className="bg-white">
            {headers.map((h, i) => (
              <th key={i} className={th}>
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr className="bg-white" key={i}>
              {r.map((c, j) => (
                <td key={j} className={td}>
                  {c}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
