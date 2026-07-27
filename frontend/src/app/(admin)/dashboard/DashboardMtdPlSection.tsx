"use client";

import React from "react";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import DashboardBargraphCard from "@/components/dashboard/DashboardBargraphCard";
import Cm1ProfitBreakdownPie from "@/components/dashboard/Cm1ProfitBreakdownPie";
import { RiCollapseDiagonalFill, RiExpandDiagonalFill } from "react-icons/ri";
import type { CurrencyCode } from "./DashboardTypes";

type DashboardMtdPlSectionProps = {
    isMtdPlExpanded: boolean;
    setIsMtdPlExpanded: React.Dispatch<React.SetStateAction<boolean>>;
    chartRef: React.RefObject<HTMLDivElement | null>;
    countryNameForGraph: string;
    formattedMonthYear: string;
    currencySymbol: string;
    finalBarLabels: string[];
    finalBarValues: number[];
    finalPrevBarValues: number[];
    colors: string[];
    shouldShowDummyUi: boolean;
    loading: boolean;
    finalAllValuesZero: boolean;
    isUsingDummyData: boolean;
    netSalesPieData: any[];
    finalCm1ProfitPieData: any[];
    cm2ProfitPieData: any[];
    displayCurrency: CurrencyCode;
};

export default function DashboardMtdPlSection({
    isMtdPlExpanded,
    setIsMtdPlExpanded,
    chartRef,
    countryNameForGraph,
    formattedMonthYear,
    currencySymbol,
    finalBarLabels,
    finalBarValues,
    finalPrevBarValues,
    colors,
    shouldShowDummyUi,
    loading,
    finalAllValuesZero,
    isUsingDummyData,
    netSalesPieData,
    finalCm1ProfitPieData,
    cm2ProfitPieData,
    displayCurrency,
}: DashboardMtdPlSectionProps) {
    return (
                        <div id="mtd-pl" className="mt-4 scroll-mt-[80px]">
                            <div
                                className={[
                                    "grid grid-cols-1 gap-4 items-stretch",
                                    isMtdPlExpanded ? "lg:grid-cols-1" : "lg:grid-cols-2",
                                ].join(" ")}
                            >
                                <div className="rounded-xl border bg-white p-5 shadow-sm min-w-0">
                                    <div className="2xl:mb-3 flex items-center justify-between">
                                        <div className="text-sm text-charcoal-500">
                                            <div className="flex flex-wrap items-baseline gap-2 text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold">
                                                <PageBreadcrumb pageTitle="MTD P&L" align="left" textSize="2xl" variant="page" />
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            <span className="relative group shrink-0">
                                                <button
                                                    type="button"
                                                    className="
      rounded-md
      border
      border-gray-300
      bg-white
      text-blue-700
      p-1.5
      transition-all
      duration-200
      ease-out
      hover:-translate-y-[2px]
      hover:shadow-lg
      active:translate-y-0
      active:shadow-md
    "
                                                    onClick={() => setIsMtdPlExpanded((s) => !s)}
                                                    aria-label={isMtdPlExpanded ? "Collapse chart" : "Expand chart"}
                                                >
                                                    {isMtdPlExpanded ? (
                                                        <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                                                    ) : (
                                                        <RiExpandDiagonalFill size={18} className="font-extrabold" />
                                                    )}
                                                </button>

                                                {/* Chart.js-like tooltip */}
                                                <span
                                                    className="
      pointer-events-none
      absolute
      left-1/2
      -translate-x-1/2
      -top-9
      z-50
      whitespace-nowrap
      rounded-md
      border
      border-gray-200
      bg-white
      px-2
      py-1
      text-[11px]
      font-medium
      text-[#414042]
      shadow-sm
      opacity-0
      transition-opacity
      duration-150
      group-hover:opacity-100
    "
                                                >
                                                    {isMtdPlExpanded ? "Collapse" : "Expand"}
                                                    <span
                                                        className="
        absolute
        left-1/2
        top-full
        h-2
        w-2
        -translate-x-1/2
        -translate-y-1/2
        rotate-45
        border-r
        border-b
        border-gray-200
        bg-white
      "
                                                    />
                                                </span>
                                            </span>
                                        </div>
                                    </div>

                                    <div ref={chartRef} className="overflow-x-hidden flex-1 min-h-0">
                                        <div className="w-full max-w-full min-w-0 h-full">
                                            <DashboardBargraphCard
                                                countryName={countryNameForGraph}
                                                formattedMonthYear={formattedMonthYear}
                                                currencySymbol={currencySymbol}
                                                labels={finalBarLabels}
                                                values={finalBarValues}
                                                prevValues={finalPrevBarValues}
                                                expanded={isMtdPlExpanded}
                                                colors={colors}
                                                loading={!shouldShowDummyUi && loading}
                                                allValuesZero={finalAllValuesZero}
                                                previewMode={shouldShowDummyUi}
                                            />
                                        </div>
                                    </div>
                                </div>

                                {!isMtdPlExpanded && (
                                    <div
                                        className={
                                            isUsingDummyData
                                                ? "min-w-0 h-full flex flex-col  pointer-events-none select-none transition-opacity duration-300"
                                                : "min-w-0 h-full flex flex-col transition-opacity duration-300"
                                        }
                                    >
                                        <Cm1ProfitBreakdownPie
                                            netSalesData={netSalesPieData}
                                            data={finalCm1ProfitPieData}
                                            cm2Data={cm2ProfitPieData}
                                            currency={displayCurrency}
                                            noDataFound={shouldShowDummyUi}
                                            height={320}
                                        />
                                    </div>
                                )}
                            </div>
                        </div>
    );
}
