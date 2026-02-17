// "use client";

// import React from "react";
// import PageBreadcrumb from "../common/PageBreadCrumb";
// import DownloadIconButton from "../ui/button/DownloadIconButton";
// import GroupedCollapsibleTable, { LeafCol, ColGroup } from "../ui/table/GroupedCollapsibleTable";

// type TableRow = any;
// type Totals = any;

// type Props = {
//   title: string;
//   currencySymbol: string;

//   loading: boolean;
//   error: string | null;

//   noDataFound: boolean;
//   displayRows: TableRow[];
//   totals: Totals;

//   hideDownloadButton?: boolean;
//   onDownloadExcel: () => void;

//   // These are all straight from your existing table config
//   LEFT_COLS: LeafCol<TableRow>[];
//   groups: ColGroup<TableRow>[];
//   SINGLE_COLS: LeafCol<TableRow>[];

//   getSignForCol: (colKey: string) => { text: string; className: string } | null;
//   formatValue: (value: unknown, key: string) => string | number;
//   getDisplayProductNameFromRow: (row: TableRow) => string;
//   onProductClick: (product: string) => void;

//   onVisibleColCountChange: (n: number) => void;
//   mainColCount: number;

//   // summary parts you already pass to GroupedCollapsibleTable
//   summaryConfig: any;
// };

// const isMissingName = (v: unknown) => {
//   if (v === undefined || v === null) return true;
//   if (typeof v === "number" && Number.isNaN(v)) return true;
//   const s = String(v).trim().toLowerCase();
//   return s === "" || s === "0" || s === "nan" || s === "none" || s === "null" || s === "undefined";
// };

// const SkuMainTable: React.FC<Props> = ({
//   title,
//   currencySymbol,
//   loading,
//   error,
//   noDataFound,
//   displayRows,
//   totals,
//   hideDownloadButton,
//   onDownloadExcel,
//   LEFT_COLS,
//   groups,
//   SINGLE_COLS,
//   getSignForCol,
//   formatValue,
//   getDisplayProductNameFromRow,
//   onProductClick,
//   onVisibleColCountChange,
//   mainColCount,
//   summaryConfig,
// }) => {
//   if (loading) return <div>Loading...</div>;
//   if (error) return <div className="text-red-600">Error: {error}</div>;

//   return (
//     <div className="rounded-xl border border-slate-200 bg-[#D9D9D933] shadow-sm p-4 sm:p-5">
//       <div className="mb-4 flex gap-3 flex-row items-center justify-between">
//         <div className="flex flex-wrap items-baseline gap-0 sm:gap-2 justify-left sm:justify-start">
//           <PageBreadcrumb pageTitle={title} variant="page" textSize="2xl" />
//           <span className="text-[#5EA68E] text-base sm:text-lg lg:text-lg 2xl:text-xl font-bold">
//             ({currencySymbol})
//           </span>
//         </div>

//         <div className="flex justify-center sm:justify-end">
//           {!hideDownloadButton && <DownloadIconButton onClick={onDownloadExcel} />}
//         </div>
//       </div>

//       <div className={`transition-opacity ${noDataFound ? "opacity-30" : "opacity-100"}`}>
//         <div className="w-full overflow-x-auto rounded-xl border border-gray-300">
//           <div className="min-w-full">
//             <GroupedCollapsibleTable<TableRow>
//               rows={displayRows}
//               leftCols={LEFT_COLS}
//               groups={groups}
//               singleCols={SINGLE_COLS}
//               layout={[
//                 { type: "group", id: "units_breakdown" },
//                 { type: "single", key: "asp" },
//                 { type: "group", id: "sales" },
//                 { type: "single", key: "promotional_rebates" },
//                 { type: "single", key: "promotional_rebates_percentage" },
//                 { type: "single", key: "cost_of_unit_sold" },
//                 { type: "group", id: "amazon_breakdown" },
//                 { type: "group", id: "other_transactions_breakdown" },
//                 { type: "group", id: "profit_breakdown" },
//               ]}
//               initialCollapsed={{
//                 units_breakdown: true,
//                 sales: true,
//                 promotions_breakdown: true,
//                 cogs_breakdown: true,
//                 amazon_breakdown: true,
//                 other_transactions_breakdown: true,
//                 profit_breakdown: true,
//               }}
//               toggleGroupByColKey={{
//                 net_units_sold: "units_breakdown",
//                 net_sales: "sales",
//                 amazon_fee: "amazon_breakdown",
//                 other_transactions: "other_transactions_breakdown",
//                 profit: "profit_breakdown",
//               }}
//               onVisibleColCountChange={onVisibleColCountChange}
//               showSignRowInBody
//               getSignForCol={getSignForCol}
//               getRowClassName={(row, index) => {
//                 const name = String((row as any)?.product_name || "").trim().toLowerCase();
//                 if (name === "total") return "bg-[#EFEFEF] font-semibold";
//                 if (name === "others") return "";
//                 return index % 2 === 0 ? "bg-white" : "bg-gray-50";
//               }}
//               getValue={(row, colKey, rowIndex) => {
//                 const name = String((row as any)?.product_name || "").trim().toLowerCase();
//                 const isTotal = name === "total";
//                 const isOthers = name === "others";

//                 if (colKey === "sno") return isTotal ? "" : rowIndex + 1;

//                 if (colKey === "product_name") {
//                   const displayName = getDisplayProductNameFromRow(row);

//                   if (isOthers) {
//                     return (
//                       <span className="inline-block max-w-[220px] truncate text-[#60a68e]">
//                         {displayName}
//                       </span>
//                     );
//                   }

//                   if (!isTotal) {
//                     return (
//                       <span
//                         onClick={() => onProductClick(String(displayName || ""))}
//                         className="inline-block max-w-[220px] cursor-pointer truncate align-middle text-[#60a68e] no-underline"
//                         title={String(displayName || "")}
//                       >
//                         {String(displayName || "-")}
//                       </span>
//                     );
//                   }

//                   return (
//                     <span className="inline-block max-w-[220px] truncate font-semibold">
//                       {String(displayName || "-")}
//                     </span>
//                   );
//                 }

//                 if (colKey === "sku") {
//                   if (isOthers || isTotal) return "-";
//                   return !isMissingName((row as any).sku) ? String((row as any).sku) : "-";
//                 }

//                 return formatValue((row as any)[colKey], colKey);
//               }}
//               summary={summaryConfig(mainColCount, totals)}
//             />
//           </div>
//         </div>
//       </div>
//     </div>
//   );
// };

// export default SkuMainTable;



















"use client";

import React from "react";
import PageBreadcrumb from "../common/PageBreadCrumb";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import GroupedCollapsibleTable, { LeafCol, ColGroup } from "../ui/table/GroupedCollapsibleTable";

import type { TableRow, Totals } from "@/lib/pnl/sku/types";
import { isMissingName } from "@/lib/pnl/sku/utils";

type Props = {
  title: string;
  currencySymbol: string;

  loading: boolean;
  error: string | null;

  noDataFound: boolean;
  displayRows: TableRow[];
  totals: Totals;

  hideDownloadButton?: boolean;
  onDownloadExcel: () => void;

  LEFT_COLS: LeafCol<TableRow>[];
  groups: ColGroup<TableRow>[];
  SINGLE_COLS: LeafCol<TableRow>[];

  getSignForCol: (colKey: string) => { text: string; className?: string } | null;
  formatValue: (value: unknown, key: string) => string | number;
  getDisplayProductNameFromRow: (row: TableRow) => string;
  onProductClick: (product: string) => void;

  onVisibleColCountChange: (n: number) => void;

  // ✅ already-built summary object (from parent)
  summary: any;
};

const SkuMainTable: React.FC<Props> = ({
  title,
  currencySymbol,
  loading,
  error,
  noDataFound,
  displayRows,
  hideDownloadButton,
  onDownloadExcel,
  LEFT_COLS,
  groups,
  SINGLE_COLS,
  getSignForCol,
  formatValue,
  getDisplayProductNameFromRow,
  onProductClick,
  onVisibleColCountChange,
  summary,
}) => {
  if (loading) return <div>Loading...</div>;
  if (error) return <div className="text-red-600">Error: {error}</div>;

  return (
    <div className="rounded-xl border border-slate-200 bg-[#D9D9D933] shadow-sm p-4 sm:p-5">
      <div className="mb-4 flex gap-3 flex-row items-center justify-between">
        <div className="flex flex-wrap items-baseline gap-0 sm:gap-2 justify-left sm:justify-start">
          <PageBreadcrumb pageTitle={title} variant="page" textSize="2xl" />
          <span className="text-[#5EA68E] text-base sm:text-lg lg:text-lg 2xl:text-xl font-bold">
            ({currencySymbol})
          </span>
        </div>

        <div className="flex justify-center sm:justify-end">
          {!hideDownloadButton && <DownloadIconButton onClick={onDownloadExcel} />}
        </div>
      </div>

      <div className={`transition-opacity ${noDataFound ? "opacity-30" : "opacity-100"}`}>
        <div className="w-full overflow-x-auto rounded-xl border border-gray-300">
          <div className="min-w-full">
            <GroupedCollapsibleTable<TableRow>
              rows={displayRows}
              leftCols={LEFT_COLS}
              groups={groups}
              singleCols={SINGLE_COLS}
              layout={[
                { type: "group", id: "units_breakdown" },
                { type: "single", key: "asp" },
                { type: "group", id: "sales" },
                { type: "single", key: "promotional_rebates" },
                { type: "single", key: "promotional_rebates_percentage" },
                { type: "single", key: "cost_of_unit_sold" },
                { type: "group", id: "amazon_breakdown" },
                { type: "group", id: "other_transactions_breakdown" },
                { type: "group", id: "profit_breakdown" },
              ]}
              initialCollapsed={{
                units_breakdown: true,
                sales: true,
                promotions_breakdown: true,
                cogs_breakdown: true,
                amazon_breakdown: true,
                other_transactions_breakdown: true,
                profit_breakdown: true,
              }}
              toggleGroupByColKey={{
                net_units_sold: "units_breakdown",
                net_sales: "sales",
                amazon_fee: "amazon_breakdown",
                other_transactions: "other_transactions_breakdown",
                profit: "profit_breakdown",
              }}
              onVisibleColCountChange={onVisibleColCountChange}
              showSignRowInBody
              getSignForCol={getSignForCol}
              getRowClassName={(row, index) => {
                const name = String((row as any)?.product_name || "").trim().toLowerCase();
                if (name === "total") return "bg-[#EFEFEF] font-semibold";
                if (name === "others") return "";
                return index % 2 === 0 ? "bg-white" : "bg-gray-50";
              }}
              getValue={(row, colKey, rowIndex) => {
                const name = String((row as any)?.product_name || "").trim().toLowerCase();
                const isTotal = name === "total";
                const isOthers = name === "others";

                if (colKey === "sno") return isTotal ? "" : rowIndex + 1;

                if (colKey === "product_name") {
                  const displayName = getDisplayProductNameFromRow(row);

                  if (isOthers) {
                    return <span className="inline-block max-w-[220px] truncate text-[#60a68e]">{displayName}</span>;
                  }

                  if (!isTotal) {
                    return (
                      <span
                        onClick={() => onProductClick(String(displayName || ""))}
                        className="inline-block max-w-[220px] cursor-pointer truncate align-middle text-[#60a68e] no-underline"
                        title={String(displayName || "")}
                      >
                        {String(displayName || "-")}
                      </span>
                    );
                  }

                  return <span className="inline-block max-w-[220px] truncate font-semibold">{String(displayName || "-")}</span>;
                }

                if (colKey === "sku") {
                  if (isOthers || isTotal) return "-";
                  return !isMissingName((row as any).sku) ? String((row as any).sku) : "-";
                }

                return formatValue((row as any)[colKey], colKey);
              }}
              summary={summary}
            />
          </div>
        </div>
      </div>
    </div>
  );
};

export default SkuMainTable;
