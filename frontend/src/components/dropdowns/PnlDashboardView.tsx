"use client";

import React from "react";
import PageBreadcrumb from "../common/PageBreadCrumb";
import PeriodFiltersTable from "../filters/PeriodFiltersTable";
import SummaryMetricCard from "./SummaryMetricCard";
import PerformanceTrendChart from "./PerformanceTrendChart";
import Bargraph from "./BarGraph";
import GraphPage from "./GraphPage";
import CircleChart from "./CircleChart";
import CMchartofsku from "./CMchartofsku";
import SKUtable from "./SKUtable";
import { Modal } from "@/components/ui/modal";
import FileUploadForm from "@/app/(admin)/(ui-elements)/modals/FileUploadForm";
import { IoMdLock } from "react-icons/io";
import { CgPushLeft, CgPushRight } from "react-icons/cg";

// ---- types (loose on purpose) ----
export type RangeType = "monthly" | "quarterly" | "yearly" | "";
export type FocusedChart = "trend" | "pnl" | null;

export type ComparisonRow = {
  label: string;
  valueText: string;
  deltaText: string;
  deltaClassName: string;
};

export type SummaryCardRow = {
  key: string;
  title: string;
  value: React.ReactNode;
  className: string;
  comparisons: ComparisonRow[];
};

export type OverlayBounds = { left: number; width: number };

type Props = {
  // refs
  layoutRef: React.RefObject<HTMLDivElement>;

  // header
  countryName: string;
  range: RangeType;
  selectedMonth: string;
  selectedQuarter: string;
  selectedYear: string;
  yearOptions: string[];
  handleRangeChange: (v: "monthly" | "quarterly" | "yearly") => void;
  handleMonthChange: (v: string) => void;
  handleQuarterChange: (v: string) => void;
  handleYearChange: (v: string) => void;

  // summary
  showSummaryCards: boolean;
  summaryCards: SummaryCardRow[];
  isSummaryZero: boolean;

  // charts / focus
  focusedChart: FocusedChart;
  toggleFocus: (which: Exclude<FocusedChart, null>) => void;
  pnlCollapsed: boolean;
  getTrendWrapperHeight: () => string;

  // chart inputs
  initialCountryName: string;
  globalHomeCurrency?: string;
  currencySymbol: string;

  performanceTrend: any;
  performanceTrendMetric: any;
  setTrendExportApi: (api: any) => void;

  setChartExportApi: (api: any) => void;
  setShowNoDataOverlay: (v: boolean) => void;

  // ai panel
  allDropdownsSelected: boolean;
  aiPanelLoading: boolean;
  aiPanelError: any;
  aiPanel: any;

  // pies
  setExpenseBreakdownPieBase64: (b64: string | null) => void;
  setProductWiseCm1PieBase64: (b64: string | null) => void;

  // sku
  setSkuExportPayload: (p: any) => void;
  handleDownloadSkuSheet1: () => void;

  // overlay
  showNoDataOverlay: boolean;
  overlayBounds: OverlayBounds;
  getTitle: () => string;

  // modal
  showUploadModal: boolean;
  setShowUploadModal: (v: boolean) => void;
  fetchUploadHistory: (
    rangeType: RangeType,
    monthVal: string,
    quarterVal: string,
    yearVal: string,
    country: string
  ) => void;

  // quarter helper
  isQuarter: (q: string) => boolean;
};

export default function PnlDashboardView(p: Props) {
  return (
    <div
      ref={p.layoutRef}
      className="
        space-y-3
        2xl:space-y-6
        relative
      "
    >
      <div
        className="sticky top-0 z-40 bg-white w-full 
        flex flex-col md:flex-row md:items-center md:justify-between 
        gap-4 border-b border-gray-200 pb-3"
      >
        {/* LEFT: Title + Subtitle */}
        <div className="flex flex-col leading-tight w-full md:w-auto md:mb-5">
          <div className="flex items-baseline gap-2">
            <PageBreadcrumb
              pageTitle="Financial Metrics -"
              variant="page"
              align="left"
              textSize="2xl"
            />

            <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
              Amazon{" "}
              {p.countryName?.toLowerCase() === "global"
                ? "Global"
                : p.countryName?.toUpperCase()}
            </span>
          </div>

          <p className="text-xs 2xl:text-sm text-charcoal-500 mt-1">
            Track your profitability and key metrics
          </p>
        </div>

        {/* RIGHT: Filters */}
        <div className="flex w-full md:w-auto justify-start md:justify-end">
          <PeriodFiltersTable
            range={p.range === "" ? "yearly" : (p.range as "monthly" | "quarterly" | "yearly")}
            selectedMonth={p.selectedMonth}
            selectedQuarter={p.selectedQuarter || ""}
            selectedYear={p.selectedYear}
            yearOptions={p.yearOptions}
            onRangeChange={p.handleRangeChange}
            onMonthChange={p.handleMonthChange}
            onQuarterChange={p.handleQuarterChange}
            onYearChange={p.handleYearChange}
          />
        </div>
      </div>

      {/* WRAPPER: stacked layout */}
      <div className="flex flex-col gap-5 w-full mt-4">
        {/* Summary Cards */}
        {p.showSummaryCards && (
          <div
            className={[
              "w-full grid gap-2 2xl:gap-3",
              "grid-cols-2 sm:grid-cols-4 2xl:grid-cols-8",
              p.isSummaryZero ? "opacity-30" : "opacity-100",
            ].join(" ")}
          >
            {p.summaryCards.map((c) => (
              <SummaryMetricCard
                key={c.key}
                title={c.title}
                value={c.value}
                className={c.className}
                comparisons={c.comparisons}
              />
            ))}
          </div>
        )}
      </div>

      {/* ---------------- MONTHLY ---------------- */}
      {p.range === "monthly" && p.selectedMonth && p.selectedYear && (
        <>
          <div className="w-full rounded-xl space-y-4">
            <div
              className={[
                "grid grid-cols-1 gap-4",
                p.focusedChart ? "lg:grid-cols-1" : "lg:grid-cols-2",
              ].join(" ")}
            >
              {/* LEFT card */}
              {(p.focusedChart === null || p.focusedChart === "trend") && (
                <div
                  className={[
                    "rounded-xl border border-gray-300 bg-white p-4",
                    "cursor-default select-none",
                    p.focusedChart === "trend" ? "cursor-default" : "",
                  ].join(" ")}
                  title={p.focusedChart === "trend" ? "Click to exit full view" : "Click to expand"}
                >
                  <div className={p.getTrendWrapperHeight()}>
                    <PerformanceTrendChart
                      range={p.range}
                      month={p.selectedMonth}
                      year={p.selectedYear}
                      countryName={p.initialCountryName}
                      homeCurrency={p.globalHomeCurrency}
                      currencySymbol={p.currencySymbol}
                      data={p.performanceTrend}
                      metric={p.performanceTrendMetric}
                      onExportApiReady={p.setTrendExportApi}
                      isExpanded={p.focusedChart === "trend"}
                      onToggleExpand={() => p.toggleFocus("trend")}
                    />
                  </div>
                </div>
              )}

              {/* RIGHT card */}
              {(p.focusedChart === null || p.focusedChart === "pnl") && (
                <div
                  className={[
                    "rounded-xl border border-gray-300 bg-white p-4",
                    "cursor-default select-none",
                    "min-h-0 overflow-hidden",
                    "flex flex-col",
                    p.focusedChart === "pnl" ? "cursor-default" : "",
                  ].join(" ")}
                  title={p.focusedChart === "pnl" ? "Click to exit full view" : "Click to expand"}
                >
                  {/* Heading */}
                  <div className="shrink-0 flex items-center justify-between gap-3">
                    <div className="flex items-baseline gap-2 min-w-0">
                      <PageBreadcrumb pageTitle="P&L" variant="page" align="left" textSize="2xl" />
                    </div>

                    <button
                      type="button"
                      data-no-expand
                      onClick={(e) => {
                        e.stopPropagation();
                        p.toggleFocus("pnl");
                      }}
                      aria-label={p.focusedChart === "pnl" ? "Collapse P&L chart" : "Expand P&L chart"}
                      title={p.focusedChart === "pnl" ? "Collapse" : "Expand"}
                      className=" hidden lg:inline-flex rounded-md
                        border border-gray-300 bg-white text-blue-700 p-1.5
                        transition-all duration-200 ease-out
                        hover:-translate-y-[2px] hover:shadow-lg
                        active:translate-y-0 active:shadow-md"
                    >
                      {p.focusedChart === "pnl" ? (
                        <CgPushLeft size={18} className="font-extrabold" />
                      ) : (
                        <CgPushRight size={18} className="font-extrabold" />
                      )}
                    </button>
                  </div>

                  <div className="flex-1 min-h-0 overflow-hidden mt-4">
                    <Bargraph
                      range={p.range}
                      selectedMonth={p.selectedMonth}
                      selectedYear={p.selectedYear}
                      countryName={p.initialCountryName}
                      homeCurrency={p.globalHomeCurrency}
                      hideDownloadButton
                      onExportApiReady={p.setChartExportApi}
                      onNoDataChange={(noData: boolean) => p.setShowNoDataOverlay(noData)}
                      isCollapsed={p.pnlCollapsed}
                    />
                  </div>
                </div>
              )}
            </div>
          </div>

          {p.allDropdownsSelected && (
            <div id="business-summary" className="scroll-mt-[80px]">
              {/* <AiSingleInsightCard
                loading={p.aiPanelLoading}
                error={p.aiPanelError}
                summaryBullets={p.aiPanel?.summaryBullets ?? []}
                recommendationBullets={p.aiPanel?.recommendationBullets ?? []}
                skuInsightsBullets={p.aiPanel?.skuInsightsBullets ?? []}
                inventoryBullets={p.aiPanel?.inventoryBullets ?? []}
                recommendationsMap={p.aiPanel?.recommendationsMap}
                objective={p.aiPanel?.objective}
              /> */}
            </div>
          )}

          <div className="flex flex-wrap justify-between gap-6 md:gap-4 mb-4">
            <div className="flex-1 min-w-[300px]">
              <CircleChart
                range={p.range}
                month={p.selectedMonth}
                year={p.selectedYear}
                countryName={p.initialCountryName}
                homeCurrency={p.globalHomeCurrency}
                onExportBase64Ready={p.setExpenseBreakdownPieBase64}
              />
            </div>
            <div className="flex-1 min-w-[300px]">
              <CMchartofsku
                range={p.range}
                month={p.selectedMonth}
                year={p.selectedYear}
                countryName={p.initialCountryName}
                homeCurrency={p.globalHomeCurrency}
                onExportBase64Ready={p.setProductWiseCm1PieBase64}
              />
            </div>
          </div>

          <SKUtable
            range={p.range}
            month={p.selectedMonth}
            year={p.selectedYear}
            countryName={p.initialCountryName}
            homeCurrency={p.globalHomeCurrency}
            hideDownloadButton={false}
            onExportPayloadChange={p.setSkuExportPayload}
            onDownload={p.handleDownloadSkuSheet1}
          />
        </>
      )}

      {/* ---------------- QUARTERLY ---------------- */}
      {p.range === "quarterly" && p.isQuarter(p.selectedQuarter) && p.selectedYear && (
        <>
          <div className="w-full rounded-xl space-y-4">
            <div
              className={[
                "grid grid-cols-1 gap-4",
                p.focusedChart ? "lg:grid-cols-1" : "lg:grid-cols-2",
              ].join(" ")}
            >
              {/* LEFT card (Trend) */}
              {(p.focusedChart === null || p.focusedChart === "trend") && (
                <div
                  className={[
                    "rounded-xl border border-gray-300 bg-white p-4",
                    "cursor-default select-none",
                    p.focusedChart === "trend" ? "cursor-default" : "",
                  ].join(" ")}
                  title={p.focusedChart === "trend" ? "Click to exit full view" : "Click to expand"}
                >
                  <button
                    type="button"
                    data-no-expand
                    onClick={(e) => {
                      e.stopPropagation();
                      p.toggleFocus("trend");
                    }}
                    aria-label={p.focusedChart === "trend" ? "Collapse trend chart" : "Expand trend chart"}
                    title={p.focusedChart === "trend" ? "Collapse" : "Expand"}
                    className="absolute right-3 top-3 z-10 inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-200 bg-white/80 text-gray-700 shadow-sm hover:bg-gray-50"
                  >
                    {p.focusedChart === "trend" ? (
                      <CgPushLeft size={18} className="font-extrabold" />
                    ) : (
                      <CgPushRight size={18} className="font-extrabold" />
                    )}
                  </button>

                  <div className={p.getTrendWrapperHeight()}>
                    <PerformanceTrendChart
                      range={p.range}
                      quarter={p.selectedQuarter}
                      year={p.selectedYear}
                      countryName={p.initialCountryName}
                      homeCurrency={p.globalHomeCurrency}
                      currencySymbol={p.currencySymbol}
                      data={p.performanceTrend}
                      metric={p.performanceTrendMetric}
                      onExportApiReady={p.setTrendExportApi}
                      isExpanded={p.focusedChart === "trend"}
                      onToggleExpand={() => p.toggleFocus("trend")}
                    />
                  </div>
                </div>
              )}

              {/* RIGHT card (PnL) */}
              {(p.focusedChart === null || p.focusedChart === "pnl") && (
                <div
                  className={[
                    "rounded-xl border border-gray-300 bg-white p-4",
                    "cursor-default select-none",
                    "min-h-0 overflow-hidden",
                    "flex flex-col",
                    p.focusedChart === "pnl" ? "cursor-default" : "",
                  ].join(" ")}
                >
                  <div className="shrink-0 flex items-center justify-between gap-3">
                    <div className="flex items-baseline gap-2">
                      <PageBreadcrumb pageTitle="P&L " variant="page" align="left" textSize="2xl" />
                    </div>

                    <button
                      type="button"
                      data-no-expand
                      onClick={(e) => {
                        e.stopPropagation();
                        p.toggleFocus("pnl");
                      }}
                      aria-label={p.focusedChart === "pnl" ? "Collapse P&L chart" : "Expand P&L chart"}
                      title={p.focusedChart === "pnl" ? "Collapse" : "Expand"}
                      className=" hidden lg:inline-flex rounded-md
                        border border-gray-300 bg-white text-blue-700 p-1.5
                        transition-all duration-200 ease-out
                        hover:-translate-y-[2px] hover:shadow-lg
                        active:translate-y-0 active:shadow-md"
                    >
                      {p.focusedChart === "pnl" ? (
                        <CgPushLeft size={18} className="font-extrabold" />
                      ) : (
                        <CgPushRight size={18} className="font-extrabold" />
                      )}
                    </button>
                  </div>

                  <div className="flex-1 min-h-0 overflow-hidden mt-4">
                    <GraphPage
                      range={p.range}
                      selectedQuarter={p.selectedQuarter}
                      selectedYear={p.selectedYear}
                      countryName={p.initialCountryName}
                      homeCurrency={p.globalHomeCurrency}
                      hideDownloadButton
                      onExportApiReady={p.setChartExportApi}
                      onNoDataChange={(noData: boolean) => p.setShowNoDataOverlay(noData)}
                      isCollapsed={p.pnlCollapsed}
                    />
                  </div>
                </div>
              )}
            </div>
          </div>

          {p.allDropdownsSelected && (
            <div id="business-summary" className="scroll-mt-[80px]">
              {/* <AiSingleInsightCard
                loading={p.aiPanelLoading}
                error={p.aiPanelError}
                summaryBullets={p.aiPanel?.summaryBullets ?? []}
                recommendationBullets={p.aiPanel?.recommendationBullets ?? []}
                skuInsightsBullets={p.aiPanel?.skuInsightsBullets ?? []}
                inventoryBullets={p.aiPanel?.inventoryBullets ?? []}
                recommendationsMap={p.aiPanel?.recommendationsMap}
                objective={p.aiPanel?.objective}
              /> */}
            </div>
          )}

          <div className="flex flex-wrap justify-between gap-6 md:gap-4">
            <div className="flex-1 min-w-[300px]">
              <CircleChart
                range={p.range}
                selectedQuarter={p.selectedQuarter}
                year={p.selectedYear}
                countryName={p.initialCountryName}
                homeCurrency={p.globalHomeCurrency}
                onExportBase64Ready={p.setExpenseBreakdownPieBase64}
              />
            </div>
            <div className="flex-1 min-w-[300px]">
              <CMchartofsku
                range={p.range}
                selectedQuarter={p.selectedQuarter}
                year={p.selectedYear}
                countryName={p.initialCountryName}
                homeCurrency={p.globalHomeCurrency}
                onExportBase64Ready={p.setProductWiseCm1PieBase64}
              />
            </div>
          </div>

          <SKUtable
            range={p.range}
            quarter={p.selectedQuarter}
            year={p.selectedYear}
            countryName={p.initialCountryName}
            homeCurrency={p.globalHomeCurrency}
            hideDownloadButton={false}
            onExportPayloadChange={p.setSkuExportPayload}
            onDownload={p.handleDownloadSkuSheet1}
          />
        </>
      )}

      {/* ---------------- YEARLY ---------------- */}
      {p.allDropdownsSelected && p.range === "yearly" && p.selectedYear && (
        <>
          <div className="w-full rounded-xl space-y-4">
            <div
              className={[
                "grid grid-cols-1 gap-4",
                p.focusedChart ? "lg:grid-cols-1" : "lg:grid-cols-2",
              ].join(" ")}
            >
              {/* LEFT card (Trend) */}
              {(p.focusedChart === null || p.focusedChart === "trend") && (
                <div
                  className={[
                    "rounded-xl border border-gray-300 bg-white p-4",
                    "cursor-default select-none",
                    p.focusedChart === "trend" ? "cursor-default" : "",
                  ].join(" ")}
                  title={p.focusedChart === "trend" ? "Click to exit full view" : "Click to expand"}
                >
                  <button
                    type="button"
                    data-no-expand
                    onClick={(e) => {
                      e.stopPropagation();
                      p.toggleFocus("trend");
                    }}
                    aria-label={p.focusedChart === "trend" ? "Collapse trend chart" : "Expand trend chart"}
                    title={p.focusedChart === "trend" ? "Collapse" : "Expand"}
                    className=" absolute right-3 top-3 z-10 inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-200 bg-white/80 text-gray-700 shadow-sm hover:bg-gray-50"
                  >
                    {p.focusedChart === "trend" ? (
                      <CgPushLeft size={18} className="font-extrabold" />
                    ) : (
                      <CgPushRight size={18} className="font-extrabold" />
                    )}
                  </button>

                  <div className={p.getTrendWrapperHeight()}>
                    <PerformanceTrendChart
                      range={p.range}
                      year={p.selectedYear}
                      countryName={p.initialCountryName}
                      homeCurrency={p.globalHomeCurrency}
                      currencySymbol={p.currencySymbol}
                      data={p.performanceTrend}
                      metric={p.performanceTrendMetric}
                      onExportApiReady={p.setTrendExportApi}
                      isExpanded={p.focusedChart === "trend"}
                      onToggleExpand={() => p.toggleFocus("trend")}
                    />
                  </div>
                </div>
              )}

              {/* RIGHT card (PnL) */}
              {(p.focusedChart === null || p.focusedChart === "pnl") && (
                <div
                  className={[
                    "rounded-xl border border-gray-300 bg-white p-4",
                    "cursor-default select-none",
                    "min-h-0 overflow-hidden",
                    "flex flex-col",
                    p.focusedChart === "pnl" ? "cursor-default" : "",
                  ].join(" ")}
                  title={p.focusedChart === "pnl" ? "Click to exit full view" : "Click to expand"}
                >
                  <div className="shrink-0 flex items-center justify-between gap-3">
                    <div className="flex items-baseline gap-2">
                      <PageBreadcrumb pageTitle="P&L " variant="page" align="left" textSize="2xl" />
                    </div>

                    <button
                      type="button"
                      data-no-expand
                      onClick={(e) => {
                        e.stopPropagation();
                        p.toggleFocus("pnl");
                      }}
                      aria-label={p.focusedChart === "pnl" ? "Collapse P&L chart" : "Expand P&L chart"}
                      title={p.focusedChart === "pnl" ? "Collapse" : "Expand"}
                      className=" hidden lg:inline-flex rounded-md
                        border border-gray-300 bg-white text-blue-700 p-1.5
                        transition-all duration-200 ease-out
                        hover:-translate-y-[2px] hover:shadow-lg
                        active:translate-y-0 active:shadow-md"
                    >
                      {p.focusedChart === "pnl" ? (
                        <CgPushLeft size={18} className="font-extrabold" />
                      ) : (
                        <CgPushRight size={18} className="font-extrabold" />
                      )}
                    </button>
                  </div>

                  <div className="flex-1 min-h-0 overflow-hidden mt-4">
                    <GraphPage
                      range={p.range}
                      selectedYear={p.selectedYear}
                      countryName={p.initialCountryName}
                      homeCurrency={p.globalHomeCurrency}
                      hideDownloadButton
                      onExportApiReady={p.setChartExportApi}
                      onNoDataChange={(noData: boolean) => p.setShowNoDataOverlay(noData)}
                      isCollapsed={p.pnlCollapsed}
                    />
                  </div>
                </div>
              )}
            </div>
          </div>

          <div id="business-summary" className="scroll-mt-[80px]">
            {/* <AiSingleInsightCard
              loading={p.aiPanelLoading}
              error={p.aiPanelError}
              summaryBullets={p.aiPanel?.summaryBullets ?? []}
              recommendationBullets={p.aiPanel?.recommendationBullets ?? []}
              skuInsightsBullets={p.aiPanel?.skuInsightsBullets ?? []}
              inventoryBullets={p.aiPanel?.inventoryBullets ?? []}
              recommendationsMap={p.aiPanel?.recommendationsMap}
              objective={p.aiPanel?.objective}
            /> */}
          </div>

          <div className="flex flex-wrap justify-between gap-6 items-stretch md:gap-4">
            <div className="flex-1 min-w-[300px]">
              <CircleChart
                range={p.range}
                year={p.selectedYear}
                countryName={p.initialCountryName}
                homeCurrency={p.globalHomeCurrency}
                onExportBase64Ready={p.setExpenseBreakdownPieBase64}
              />
            </div>

            <div className="flex-1 min-w-[300px]">
              <CMchartofsku
                range={p.range}
                year={p.selectedYear}
                countryName={p.initialCountryName}
                homeCurrency={p.globalHomeCurrency}
                onExportBase64Ready={p.setProductWiseCm1PieBase64}
              />
            </div>
          </div>

          <SKUtable
            range={p.range}
            year={p.selectedYear}
            countryName={p.initialCountryName}
            homeCurrency={p.globalHomeCurrency}
            hideDownloadButton={false}
            onExportPayloadChange={p.setSkuExportPayload}
            onDownload={p.handleDownloadSkuSheet1}
          />
        </>
      )}

      {/* No data overlay */}
      {p.showNoDataOverlay && (
        <div
          className="fixed inset-y-0 z-[9999] flex items-center justify-center pointer-events-none"
          style={{ left: p.overlayBounds.left, width: p.overlayBounds.width || "100%" }}
        >
          <div className="bg-white border border-[#D9D9D9] rounded-xl shadow-xl p-6 max-w-lg w-[90%] text-center pointer-events-auto">
            <div className="mb-4 flex items-center justify-center">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-[#D9D9D9]">
                <IoMdLock className="text-green-500 text-2xl" />
              </div>
            </div>

            <PageBreadcrumb pageTitle="No Data Available" variant="table" align="center" textSize="2xl" />

            <p className="text-charcoal-500 text-xs sm:text-sm leading-relaxed my-4">
              To see performance metrics, you need to upload more files for
              <span className="block mt-0.5">{p.getTitle()}</span>
            </p>
          </div>
        </div>
      )}

      <Modal
        isOpen={p.showUploadModal}
        onClose={() => p.setShowUploadModal(false)}
        className="max-w-3xl w-[90vw] mx-auto p-0 shadow-[6px_6px_7px_0px_#00000026] border border-[#D9D9D9]"
        showCloseButton
      >
        <div className="max-h-[85vh] overflow-y-auto">
          <FileUploadForm
            initialCountry={p.initialCountryName}
            onClose={() => p.setShowUploadModal(false)}
            onComplete={() => {
              p.setShowUploadModal(false);
              p.fetchUploadHistory(
                p.range,
                p.selectedMonth,
                p.selectedQuarter || "",
                p.selectedYear,
                p.initialCountryName
              );
            }}
          />
        </div>
      </Modal>
    </div>
  );
}
