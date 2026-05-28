"use client";

import React from "react";
import PageBreadcrumb from "../common/PageBreadCrumb";

type TopBottomRow = {
  product_name: string;
  profit: string;
  profitMix: string;
  salesMix: string;
  cm1_per_unit: string;
};

type TopBottomData = {
  rows: TopBottomRow[];
  totals: {
    profit: string;
    profitMix: string;
    salesMix: string;
    avg_cm1: string;
  };
};

type Props = {
  topData?: TopBottomData | null;
  bottomData?: TopBottomData | null;
  currencySymbol: string;
  previewMode?: boolean;
};

const EMPTY_TOTALS = {
  profit: "0.00",
  profitMix: "0.00",
  salesMix: "0.00",
  avg_cm1: "0.00",
};

const DEMO_TOP_BOTTOM_DATA: TopBottomData = {
  rows: [
    {
      product_name: "Dummy Product 1",
      profit: "0.00",
      profitMix: "0.00",
      salesMix: "0.00",
      cm1_per_unit: "0.00",
    },
    {
      product_name: "Dummy Product 2",
      profit: "0.00",
      profitMix: "0.00",
      salesMix: "0.00",
      cm1_per_unit: "0.00",
    },
    {
      product_name: "Dummy Product 3",
      profit: "0.00",
      profitMix: "0.00",
      salesMix: "0.00",
      cm1_per_unit: "0.00",
    },
    {
      product_name: "Dummy Product 4",
      profit: "0.00",
      profitMix: "0.00",
      salesMix: "0.00",
      cm1_per_unit: "0.00",
    },
    {
      product_name: "Dummy Product 5",
      profit: "0.00",
      profitMix: "0.00",
      salesMix: "0.00",
      cm1_per_unit: "0.00",
    },
  ],
  totals: EMPTY_TOTALS,
};

const SkuTopBottomTables: React.FC<Props> = ({
  topData,
  bottomData,
  currencySymbol,
  previewMode = false,
}) => {
  const safeTopData =
    previewMode || !topData?.rows?.length
      ? DEMO_TOP_BOTTOM_DATA
      : topData;

  const safeBottomData =
    previewMode || !bottomData?.rows?.length
      ? DEMO_TOP_BOTTOM_DATA
      : bottomData;

  const formatRoundedValue = (value: string) => {
    const numberValue = Number(String(value).replace(/[^0-9.-]/g, ""));

    if (Number.isNaN(numberValue)) {
      return "-";
    }

    return Math.round(numberValue).toLocaleString();
  };

  return (
  <div
  className={`rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5 ${
    previewMode ? "opacity-45 pointer-events-none select-none" : ""
  }`}
>
      <div className="flex flex-col justify-between gap-7 md:gap-3 text-[#414042] md:flex-row min-w-0">
        {/* Top 5 */}
        <div className="flex-1 min-w-0">
          <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
            <PageBreadcrumb pageTitle="Most 5 Profitable Products" variant="page" align="left" textSize="2xl" />
          </div>

          <div className="overflow-x-auto rounded-xl border border-gray-300">
            <table className="w-full table-auto border-collapse">
              <thead>
                <tr className="bg-green-500 font-bold text-[#f8edcf]">
                  <th className="min-w-[150px] border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm break-words leading-snug">
                    Product Name
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    CM1 Profit ({currencySymbol})
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    Profit Mix (%)
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    Sales Mix (%)
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    CM1 Profit per Unit ({currencySymbol})
                  </th>
                </tr>
              </thead>

              <tbody>
                {safeTopData.rows.map((item, index) => (
                  <tr key={index} className={index % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                    <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm align-top max-w-[200px]">
                      <span
                        title={item.product_name}
                        className="block truncate whitespace-nowrap overflow-hidden"
                      >
                        {item.product_name || "-"}
                      </span>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {formatRoundedValue(item.profit)}
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {item.profitMix}%
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {item.salesMix}%
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {item.cm1_per_unit}
                    </td>
                  </tr>
                ))}

                <tr className="bg-[#EFEFEF] font-semibold">
                  <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm">
                    <strong>Total</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{formatRoundedValue(safeTopData.totals.profit)}</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{safeTopData.totals.profitMix}%</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{safeTopData.totals.salesMix}%</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{safeTopData.totals.avg_cm1}</strong>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Bottom 5 */}
        <div className="flex-1 min-w-0">
          <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
            <PageBreadcrumb pageTitle="Least 5 Profitable Products" variant="page" align="left" textSize="2xl" />
          </div>

          <div className="overflow-x-auto rounded-xl border border-gray-300">
            <table className="w-full table-auto border-collapse">
              <thead>
                <tr className="bg-[#B75A5A] font-bold text-[#f8edcf]">
                  <th className="min-w-[150px] border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm break-words leading-snug">
                    Product Name
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    CM1 Profit ({currencySymbol})
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    Profit Mix (%)
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    Sales Mix (%)
                  </th>
                  <th className="border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm break-words leading-snug">
                    CM1 Profit per Unit ({currencySymbol})
                  </th>
                </tr>
              </thead>

              <tbody>
                {safeBottomData.rows.map((item, index) => (
                  <tr key={index} className={index % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                    <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm align-top max-w-[200px]">
                      <span
                        title={item.product_name}
                        className="block truncate whitespace-nowrap overflow-hidden"
                      >
                        {item.product_name || "-"}
                      </span>
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {item.profit}
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {item.profitMix}%
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {item.salesMix}%
                    </td>
                    <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                      {item.cm1_per_unit}
                    </td>
                  </tr>
                ))}

                <tr className="bg-[#EFEFEF] font-semibold">
                  <td className="border border-gray-300 px-2 sm:px-3 py-3 text-left text-xs 2xl:text-sm">
                    <strong>Total</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{formatRoundedValue(safeBottomData.totals.profit)}</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{safeBottomData.totals.profitMix}%</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{safeBottomData.totals.salesMix}%</strong>
                  </td>
                  <td className="whitespace-nowrap border border-gray-300 px-2 sm:px-3 py-3 text-center text-xs 2xl:text-sm">
                    <strong>{safeBottomData.totals.avg_cm1}</strong>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SkuTopBottomTables;
