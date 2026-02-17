"use client";

import React, { useMemo, useRef, useState, useCallback } from "react";
import PnlDashboardView, {
  RangeType,
  FocusedChart,
  SummaryCardRow,
  ComparisonRow,
} from "./PnlDashboardView";

// ---- If you already have these types, replace the 'any' with your real types ----
type Summary = any;
type SummaryComparisons = any;
type Quarter = "Q1" | "Q2" | "Q3" | "Q4";

function isQuarter(q: string): q is Quarter {
  return q === "Q1" || q === "Q2" || q === "Q3" || q === "Q4";
}

function monthIndex(monthName: string) {
  const m = monthName.toLowerCase().slice(0, 3);
  const map: Record<string, number> = {
    jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
    jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
  };
  return map[m] ?? 0;
}

function formatMonthLabel(idx: number) {
  const labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  return labels[((idx % 12) + 12) % 12];
}

function getPrevMonthLabel(selectedMonth: string, year: number) {
  const idx = monthIndex(selectedMonth);
  const prevIdx = idx - 1;
  const prevYear = prevIdx < 0 ? year - 1 : year;
  return `${formatMonthLabel(prevIdx)} ${prevYear}`;
}

function getPrevQuarterLabel(selectedQuarter: Quarter, year: number) {
  const order: Quarter[] = ["Q1", "Q2", "Q3", "Q4"];
  const idx = order.indexOf(selectedQuarter);
  const prevIdx = idx - 1;
  const prevQ = prevIdx < 0 ? "Q4" : order[prevIdx];
  const prevYear = prevIdx < 0 ? year - 1 : year;
  return `${prevQ} ${prevYear}`;
}

function getPrevYearLabel(year: number) {
  return `${year - 1}`;
}

export default function PnlDashboardContainer() {
  // ------------------------------
  // ✅ Replace these with your real state + data sources
  // ------------------------------
  const layoutRef = useRef<HTMLDivElement>(null);

  // filters
  const [range, setRange] = useState<RangeType>("monthly");
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  const [selectedQuarter, setSelectedQuarter] = useState<string>("");
  const [selectedYear, setSelectedYear] = useState<string>("");

  const yearOptions = useMemo(() => {
    const now = new Date().getFullYear();
    return Array.from({ length: 6 }, (_, i) => String(now - i));
  }, []);

  // focus / charts
  const [focusedChart, setFocusedChart] = useState<FocusedChart>(null);
  const [pnlCollapsed, setPnlCollapsed] = useState(false);

  // no-data overlay + modal
  const [showNoDataOverlay, setShowNoDataOverlay] = useState(false);
  const [overlayBounds, setOverlayBounds] = useState({ left: 0, width: 0 });
  const [showUploadModal, setShowUploadModal] = useState(false);

  // exports hooks
  const [trendExportApi, setTrendExportApi] = useState<any>(null);
  const [chartExportApi, setChartExportApi] = useState<any>(null);
  const [skuExportPayload, setSkuExportPayload] = useState<any>(null);
  const [expenseBreakdownPieBase64, setExpenseBreakdownPieBase64] = useState<string | null>(null);
  const [productWiseCm1PieBase64, setProductWiseCm1PieBase64] = useState<string | null>(null);

  // AI panel
  const [aiPanelLoading] = useState(false);
  const [aiPanelError] = useState<any>(null);
  const [aiPanel] = useState<any>(null);

  // data placeholders (replace with your real data)
  const countryName = "global";
  const initialCountryName = "global";
  const globalHomeCurrency = "USD";
  const currencySymbol = "$";
  const performanceTrend = [];
  const performanceTrendMetric = "net_sales";

  // Your actual API data object(s)
  const uploadsData: any = {}; // <= replace
  const displayData: Summary = uploadsData?.summary ?? {}; // <= replace logic if needed

  const allDropdownsSelected = Boolean(
    range &&
      selectedYear &&
      (range === "yearly" ||
        (range === "monthly" ? selectedMonth : false) ||
        (range === "quarterly" ? selectedQuarter : false))
  );

  // ------------------------------
  // handlers
  // ------------------------------
  const handleRangeChange = useCallback((v: "monthly" | "quarterly" | "yearly") => {
    setRange(v);
    // optional: reset selection when range changes
    // setSelectedMonth(""); setSelectedQuarter("");
  }, []);

  const handleMonthChange = useCallback((v: string) => setSelectedMonth(v), []);
  const handleQuarterChange = useCallback((v: string) => setSelectedQuarter(v), []);
  const handleYearChange = useCallback((v: string) => setSelectedYear(v), []);

  const toggleFocus = useCallback((which: Exclude<FocusedChart, null>) => {
    setFocusedChart((prev) => (prev === which ? null : which));
  }, []);

  const getTrendWrapperHeight = useCallback(() => {
    // Keep your existing logic if you have one.
    // Example: different heights when expanded
    return focusedChart === "trend" ? "h-[520px] 2xl:h-[700px]" : "h-[420px] 2xl:h-[560px]";
  }, [focusedChart]);

  const getTitle = useCallback(() => {
    if (range === "monthly") return `${selectedMonth} ${selectedYear}`;
    if (range === "quarterly") return `${selectedQuarter} ${selectedYear}`;
    return `${selectedYear}`;
  }, [range, selectedMonth, selectedQuarter, selectedYear]);

  const fetchUploadHistory = useCallback(
    (_range: RangeType, _m: string, _q: string, _y: string, _country: string) => {
      // plug in your real function
      // fetchUploadHistory(range, selectedMonth, selectedQuarter || "", selectedYear, initialCountryName)
    },
    []
  );

  const handleDownloadSkuSheet1 = useCallback(() => {
    // plug in your real download function
  }, []);

  // ------------------------------
  // ✅ Summary cards builder (moved out of View)
  // ------------------------------
  const { showSummaryCards, summaryCards, isSummaryZero } = useMemo(() => {
    if (!uploadsData?.summary) {
      return { showSummaryCards: false, summaryCards: [] as SummaryCardRow[], isSummaryZero: false };
    }

    const summary: Summary = displayData;
    const netSales = summary.total_sales ?? 0;

    const rawComparisons =
      (uploadsData as any).summaryComparisons ?? (uploadsData as any).summary_comparisons;

    const comparisons: SummaryComparisons | undefined = rawComparisons
      ? (rawComparisons as SummaryComparisons)
      : undefined;

    const formatMoney = (val: number, opts?: { showPlus?: boolean }) => {
      const num = Number(val || 0);
      const sign = num < 0 ? "-" : opts?.showPlus && num > 0 ? "+" : "";
      const abs = Math.abs(num);

      return `${sign}${currencySymbol}${abs.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}`;
    };

    const formatUnits = (val: number) =>
      Number(val || 0).toLocaleString(undefined, { maximumFractionDigits: 0 });

    const formatPercent = (val: number) =>
      `${Number(val || 0).toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      })}%`;

    const renderMoneyWithPerUnit = (total: number, units: number) => {
      const totalText = formatMoney(total);

      if (!units) return <span>{totalText}</span>;

      const perUnit = total / units;
      const perUnitText = formatMoney(perUnit);

      return (
        <div className="flex items-baseline gap-1 leading-tight">
          <span className="text-sm 2xl:text-lg font-semibold">{totalText}</span>
          <span className="text-[10px] 2xl:text-xs text-charcoal-400 font-medium">
            ({perUnitText}/unit)
          </span>
        </div>
      );
    };

    const getGrossSales = (s?: Summary) => s?.total_product_sales ?? s?.gross_sales ?? 0;

    // TACoS (your "ROAS" function)
    const costOfAds = summary.advertising_total ?? 0;
    const getRoas = (s?: Summary) => {
      const ns = s?.total_sales ?? 0;
      const ads = s?.advertising_total ?? 0;
      return ns > 0 ? (ads / ns) * 100 : 0;
    };
    const roas = getRoas(summary);
    const formatRoas = (val: number) =>
      `${Number(val || 0).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}%`;

    // cm2 %
    const cm2Percent = netSales > 0 ? ((summary.cm2_profit ?? 0) / netSales) * 100 : 0;

    const isSummaryZero =
      (summary.unit_sold ?? 0) === 0 &&
      (summary.total_sales ?? 0) === 0 &&
      (summary.total_expense ?? 0) === 0 &&
      (summary.cm2_profit ?? 0) === 0;

    // ---- comparisons helper ----
    type ComparisonItem = { label: string; value?: number; diffPct: number | null };

    const getComparisons = (metric: keyof Summary): ComparisonItem[] => {
      const current = Number(summary?.[metric] ?? 0);

      const lm = comparisons?.lastMonth?.[metric];
      const lq = comparisons?.lastQuarter?.[metric];
      const ly = comparisons?.lastYear?.[metric];

      const makeItem = (label: string, prevVal?: number): ComparisonItem => {
        if (typeof prevVal !== "number") return { label, value: undefined, diffPct: null };
        const diffPct = prevVal === 0 ? null : ((current - prevVal) / prevVal) * 100;
        return { label, value: prevVal, diffPct };
      };

      const yNum = Number(selectedYear);

      if (range === "monthly") {
        const label = selectedMonth && yNum ? getPrevMonthLabel(selectedMonth, yNum) : "Prev month";
        return [makeItem(label, typeof lm === "number" ? lm : undefined)];
      }

      if (range === "quarterly") {
        const label =
          selectedQuarter && yNum && isQuarter(selectedQuarter)
            ? getPrevQuarterLabel(selectedQuarter, yNum)
            : "Prev quarter";
        return [makeItem(label, typeof lq === "number" ? lq : undefined)];
      }

      if (range === "yearly") {
        const label = yNum ? getPrevYearLabel(yNum) : "Prev year";
        return [makeItem(label, typeof ly === "number" ? ly : undefined)];
      }

      return [];
    };

    const buildComparisonsRows = (
      metric: keyof Summary,
      formatter: (val: number) => string
    ): ComparisonRow[] => {
      const items = getComparisons(metric);

      return items.map((item) => {
        const hasValue = typeof item.value === "number" && !isNaN(item.value);
        const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

        const deltaClassName = hasDiff
          ? item.diffPct! >= 0
            ? "text-emerald-600"
            : "text-red-600"
          : "text-gray-400";

        const deltaText = hasDiff
          ? `${item.diffPct! >= 0 ? "▲" : "▼"} ${Math.abs(item.diffPct!).toFixed(2)}%`
          : "-";

        return {
          label: item.label,
          valueText: hasValue ? formatter(item.value!) : "-",
          deltaText,
          deltaClassName,
        };
      });
    };

    // gross sales comparisons (special source)
    const getGrossSalesComparisons = (): ComparisonItem[] => {
      const current = getGrossSales(summary);
      const yNum = Number(selectedYear);

      const prevMonth = comparisons?.lastMonth ? getGrossSales(comparisons.lastMonth) : undefined;
      const prevQuarter = comparisons?.lastQuarter ? getGrossSales(comparisons.lastQuarter) : undefined;
      const prevYear = comparisons?.lastYear ? getGrossSales(comparisons.lastYear) : undefined;

      const makeItem = (label: string, prevVal?: number): ComparisonItem => {
        if (typeof prevVal !== "number") return { label, value: undefined, diffPct: null };
        const diffPct = prevVal === 0 ? null : ((current - prevVal) / prevVal) * 100;
        return { label, value: prevVal, diffPct };
      };

      if (range === "monthly") {
        const label = selectedMonth && yNum ? getPrevMonthLabel(selectedMonth, yNum) : "Prev month";
        return [makeItem(label, prevMonth)];
      }
      if (range === "quarterly") {
        const label =
          selectedQuarter && yNum && isQuarter(selectedQuarter)
            ? getPrevQuarterLabel(selectedQuarter, yNum)
            : "Prev quarter";
        return [makeItem(label, prevQuarter)];
      }
      if (range === "yearly") {
        const label = yNum ? getPrevYearLabel(yNum) : "Prev year";
        return [makeItem(label, prevYear)];
      }
      return [];
    };

    const buildGrossSalesComparisonRows = (): ComparisonRow[] => {
      const items = getGrossSalesComparisons();
      return items.map((item) => {
        const hasValue = typeof item.value === "number" && !isNaN(item.value);
        const hasDiff = typeof item.diffPct === "number" && !isNaN(item.diffPct);

        const deltaClassName = hasDiff
          ? item.diffPct! >= 0
            ? "text-emerald-600"
            : "text-red-600"
          : "text-gray-400";

        const deltaText = hasDiff
          ? `${item.diffPct! >= 0 ? "▲" : "▼"} ${Math.abs(item.diffPct!).toFixed(2)}%`
          : "-";

        return {
          label: item.label,
          valueText: hasValue ? formatMoney(item.value!) : "-",
          deltaText,
          deltaClassName,
        };
      });
    };

    // TACoS comparisons
    const buildTacosComparisonRows = (): ComparisonRow[] => {
      const yNum = Number(selectedYear);

      const label =
        range === "monthly"
          ? selectedMonth && yNum
            ? getPrevMonthLabel(selectedMonth, yNum)
            : "Prev month"
          : range === "quarterly"
          ? selectedQuarter && yNum && isQuarter(selectedQuarter)
            ? getPrevQuarterLabel(selectedQuarter, yNum)
            : "Prev quarter"
          : yNum
          ? getPrevYearLabel(yNum)
          : "Prev year";

      const prevVal =
        range === "monthly"
          ? comparisons?.lastMonth
            ? getRoas(comparisons.lastMonth)
            : undefined
          : range === "quarterly"
          ? comparisons?.lastQuarter
            ? getRoas(comparisons.lastQuarter)
            : undefined
          : comparisons?.lastYear
          ? getRoas(comparisons.lastYear)
          : undefined;

      const hasPrev = typeof prevVal === "number" && !isNaN(prevVal);
      const delta = hasPrev ? roas - prevVal! : null;

      const deltaClassName =
        typeof delta === "number"
          ? delta > 0
            ? "text-red-600"
            : delta < 0
            ? "text-emerald-600"
            : "text-gray-400"
          : "text-gray-400";

      const arrow =
        typeof delta === "number" ? (delta > 0 ? "▼" : delta < 0 ? "▲" : "") : "";

      const deltaText =
        typeof delta === "number" ? `${arrow} ${Math.abs(delta).toFixed(2)}%` : "-";

      return [
        {
          label,
          valueText: hasPrev ? formatRoas(prevVal!) : "-",
          deltaText,
          deltaClassName,
        },
      ];
    };

    // CM2% comparisons
    const getCm2Percent = (s?: Summary) =>
      s && (s.total_sales ?? 0) > 0 ? ((s.cm2_profit ?? 0) / (s.total_sales ?? 1)) * 100 : 0;

    const buildCm2PercentComparisonRows = (): ComparisonRow[] => {
      const yNum = Number(selectedYear);

      const label =
        range === "monthly"
          ? selectedMonth && yNum
            ? getPrevMonthLabel(selectedMonth, yNum)
            : "Prev month"
          : range === "quarterly"
          ? selectedQuarter && yNum && isQuarter(selectedQuarter)
            ? getPrevQuarterLabel(selectedQuarter, yNum)
            : "Prev quarter"
          : yNum
          ? getPrevYearLabel(yNum)
          : "Prev year";

      const prevVal =
        range === "monthly"
          ? comparisons?.lastMonth
            ? getCm2Percent(comparisons.lastMonth)
            : undefined
          : range === "quarterly"
          ? comparisons?.lastQuarter
            ? getCm2Percent(comparisons.lastQuarter)
            : undefined
          : comparisons?.lastYear
          ? getCm2Percent(comparisons.lastYear)
          : undefined;

      const hasPrev = typeof prevVal === "number" && !isNaN(prevVal);
      const diffPct = hasPrev && prevVal !== 0 ? ((cm2Percent - prevVal) / prevVal) * 100 : null;

      const deltaClassName =
        typeof diffPct === "number"
          ? diffPct >= 0
            ? "text-emerald-600"
            : "text-red-600"
          : "text-gray-400";

      const deltaText =
        typeof diffPct === "number"
          ? `${diffPct >= 0 ? "▲" : "▼"} ${Math.abs(diffPct).toFixed(1)}%`
          : "-";

      return [
        {
          label,
          valueText: hasPrev ? formatPercent(prevVal!) : "-",
          deltaText,
          deltaClassName,
        },
      ];
    };

    // ---- build cards (same as your current return) ----
    const cards: SummaryCardRow[] = [
      {
        key: "units",
        title: "Units",
        value: formatUnits(summary.unit_sold ?? 0),
        className: "border border-[#FDD36F] bg-[#FDD36F4D]",
        comparisons: buildComparisonsRows("unit_sold", (v) => formatUnits(v)),
      },
      {
        key: "grossSales",
        title: "Gross Sales",
        value: renderMoneyWithPerUnit(getGrossSales(summary), summary.unit_sold ?? 0),
        className: "border border-[#ED9F50] bg-[#ED9F504D]",
        comparisons: buildGrossSalesComparisonRows(),
      },
      {
        key: "netSales",
        title: "Net Sales",
        value: renderMoneyWithPerUnit(netSales, summary.unit_sold ?? 0),
        className: "border border-[#75BBDA] bg-[#75BBDA4D]",
        comparisons: buildComparisonsRows("total_sales", (v) => formatMoney(v)),
      },
      {
        key: "expenses",
        title: "Marketplace Fees",
        value: renderMoneyWithPerUnit(summary.total_expense ?? 0, summary.unit_sold ?? 0),
        className: "border border-[#B75A5A] bg-[#B75A5A4D]",
        comparisons: buildComparisonsRows("total_expense", (v) => formatMoney(v)),
      },
      {
        key: "ads",
        title: "Cost of Advertisement",
        value: renderMoneyWithPerUnit(costOfAds, summary.unit_sold ?? 0),
        className: "border border-[#C49466] bg-[#C494664D]",
        comparisons: buildComparisonsRows("advertising_total", (v) => formatMoney(v)),
      },
      {
        key: "tacos",
        title: "TACoS",
        value: formatRoas(roas),
        className: "border border-[#3A8EA4] bg-[#3A8EA44D]",
        comparisons: buildTacosComparisonRows(),
      },
      {
        key: "cm2",
        title: "CM2 Profit",
        value: renderMoneyWithPerUnit(summary.cm2_profit ?? 0, summary.unit_sold ?? 0),
        className: "border border-[#B8C78C] bg-[#B8C78C4D]",
        comparisons: buildComparisonsRows("cm2_profit", (v) => formatMoney(v)),
      },
      {
        key: "cm2Pct",
        title: "CM2 Profit %",
        value: formatPercent(cm2Percent),
        className: "border border-[#7B9A6D] bg-[#7B9A6D4D]",
        comparisons: buildCm2PercentComparisonRows(),
      },
    ];

    return { showSummaryCards: true, summaryCards: cards, isSummaryZero };
  }, [
    uploadsData,
    displayData,
    currencySymbol,
    range,
    selectedMonth,
    selectedQuarter,
    selectedYear,
  ]);

  // Keep overlay bounds in sync (optional)
  // If you already calculate overlayBounds elsewhere, keep it.
  React.useEffect(() => {
    const el = layoutRef.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    setOverlayBounds({ left: rect.left, width: rect.width });
  }, [range, focusedChart]);

  return (
    <PnlDashboardView
      layoutRef={layoutRef}
      countryName={countryName}
      range={range}
      selectedMonth={selectedMonth}
      selectedQuarter={selectedQuarter}
      selectedYear={selectedYear}
      yearOptions={yearOptions}
      handleRangeChange={handleRangeChange}
      handleMonthChange={handleMonthChange}
      handleQuarterChange={handleQuarterChange}
      handleYearChange={handleYearChange}
      showSummaryCards={showSummaryCards}
      summaryCards={summaryCards}
      isSummaryZero={isSummaryZero}
      focusedChart={focusedChart}
      toggleFocus={toggleFocus}
      pnlCollapsed={pnlCollapsed}
      getTrendWrapperHeight={getTrendWrapperHeight}
      initialCountryName={initialCountryName}
      globalHomeCurrency={globalHomeCurrency}
      currencySymbol={currencySymbol}
      performanceTrend={performanceTrend}
      performanceTrendMetric={performanceTrendMetric}
      setTrendExportApi={setTrendExportApi}
      setChartExportApi={setChartExportApi}
      setShowNoDataOverlay={setShowNoDataOverlay}
      allDropdownsSelected={allDropdownsSelected}
      aiPanelLoading={aiPanelLoading}
      aiPanelError={aiPanelError}
      aiPanel={aiPanel}
      setExpenseBreakdownPieBase64={setExpenseBreakdownPieBase64}
      setProductWiseCm1PieBase64={setProductWiseCm1PieBase64}
      setSkuExportPayload={setSkuExportPayload}
      handleDownloadSkuSheet1={handleDownloadSkuSheet1}
      showNoDataOverlay={showNoDataOverlay}
      overlayBounds={overlayBounds}
      getTitle={getTitle}
      showUploadModal={showUploadModal}
      setShowUploadModal={setShowUploadModal}
      fetchUploadHistory={fetchUploadHistory}
      isQuarter={isQuarter}
    />
  );
}
