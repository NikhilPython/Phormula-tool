// components/admin/AdminDataTable.tsx

import React from "react";

type AdminDataTableProps = {
  columns: string[];
  children: React.ReactNode;
  minWidth?: string;
};

export default function AdminDataTable({
  columns,
  children,
  minWidth = "1150px",
}: AdminDataTableProps) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full table-auto" style={{ minWidth }}>
        <thead className="sticky top-0 z-10 bg-white">
          <tr className="border-b border-slate-200">
            {columns.map((column) => (
              <th
                key={column}
                className="px-5 py-4 text-left text-xs font-bold uppercase tracking-wide text-slate-500"
              >
                {column}
              </th>
            ))}
          </tr>
        </thead>

        <tbody className="bg-white">{children}</tbody>
      </table>
    </div>
  );
}