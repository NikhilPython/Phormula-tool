"use client";

import React from "react";
import ReactECharts from "echarts-for-react";
import {
  FaBoxArchive,
  FaMoneyBillTrendUp,
  FaTags,
  FaPercent,
  FaAmazon,
  FaLayerGroup,
  FaWallet,
  FaArrowRotateRight,
} from "react-icons/fa6";
import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";


/* ================= TYPES ================= */

type SummaryShape = {
  quantity_total?: number;
  gross_sales?: number;
  net_sales?: number;
  taxncredit?: number;
  amazon_fee?: number;
  advertising_total?: number;
  otherwplatform?: number;
  cashflow?: number;
  rembursement_fee?: number;
  fba_fees?: number;
  selling_fees?: number;
  promotional_rebates?: number;

};

type Props = {
  data: SummaryShape;
  previous_summary?: SummaryShape;
  previousLabel?: string;
  periodType?: "monthly" | "quarterly" | "yearly";
  currency: string;
  isPreviewMode?: boolean;
};

/* ================= COMPONENT ================= */

const CashFlowSankey: React.FC<Props> = ({
  data,
  previous_summary,
  previousLabel,
  currency,
  periodType = "monthly",
  isPreviewMode = false,
}) => {
  /* ---------- helpers ---------- */
  const [screenWidth, setScreenWidth] = React.useState(
    typeof window !== "undefined" ? window.innerWidth : 1920
  );

  const ROUND_CURRENCY_LABELS = [
    "Gross Sales",
    "Net Sales",
    "Promotional Discount",
    "Marketplace Fees",
    "Cash Generated",
    "Net Reimbursement",
    "Others"
  ];

  const formatCurrencyByLabel = (label: string, val?: number) => {
    if (val === undefined || val === null) return "-";

    const shouldRound = ROUND_CURRENCY_LABELS.includes(label);

    const absVal = Math.abs(val).toLocaleString(undefined, {
      minimumFractionDigits: shouldRound ? 0 : 2,
      maximumFractionDigits: shouldRound ? 0 : 2,
    });

    return val < 0 ? `-${currency}${absVal}` : `${currency}${absVal}`;
  };

  const formatCurrencyWithSign = (val?: number) => {
    if (val === undefined || val === null) return "-";

    const absVal = Math.abs(val).toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });

    return val < 0 ? `-${currency}${absVal}` : `${currency}${absVal}`;
  };

  const formatCurrencyRoundedWithSign = (val?: number) => {
    if (val === undefined || val === null || Number.isNaN(Number(val))) return "-";

    const absVal = Math.round(Math.abs(Number(val))).toLocaleString(undefined, {
      maximumFractionDigits: 0,
    });

    return Number(val) < 0 ? `-${currency}${absVal}` : `${currency}${absVal}`;
  };

  const isPreviewSankey =
    !data ||
    Object.values(data).every(
      (v) => v === 0 || v === undefined
    );

  React.useEffect(() => {
    const onResize = () => setScreenWidth(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const is2XL = screenWidth >= 1536;
  const isXL = screenWidth >= 1280 && screenWidth < 1536;
  const isLaptop = screenWidth >= 1024 && screenWidth < 1280;
  const isTablet = screenWidth >= 640 && screenWidth < 1024;
  const isMobile = screenWidth < 640;

  const sankeyCols = {
    label: isMobile
      ? 76
      : isTablet
        ? 92
        : isLaptop
          ? 105
          : isXL
            ? 135
            : 150,
    sign: isMobile ? 14 : isTablet ? 16 : isLaptop ? 18 : 20,
    amount: isMobile
      ? 54
      : isTablet
        ? 70
        : isLaptop
          ? 82
          : isXL
            ? 96
            : 110,
    pct: isMobile ? 0 : isTablet ? 0 : isLaptop ? 0 : isXL ? 42 : 52,
  };

  const formatNumber = (val?: number) =>
    val !== undefined
      ? Number(val).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })
      : "-";

  const formatPrevLabel = (label?: string) => {
    if (!label) return "";

    // Month format: "November 2025" → "Nov'25"
    const monthMatch = label.match(/^([A-Za-z]+)\s(\d{4})$/);
    if (monthMatch) {
      const month = monthMatch[1].slice(0, 3);
      const year = monthMatch[2].slice(-2);
      return `${month}'${year}`;
    }

    // Quarter format: "Q3 2025" → "Q3'25"
    const quarterMatch = label.match(/^(Q\d)\s(\d{4})$/);
    if (quarterMatch) {
      return `${quarterMatch[1]}'${quarterMatch[2].slice(-2)}`;
    }

    // Year format: "2024" → "2024"
    return label;
  };
  const marketplaceFees =
    (data.amazon_fee || 0);

  const prevMarketplaceFees =
    (previous_summary?.amazon_fee || 0);



  const getChangePercent = (curr?: number, prev?: number) => {
    if (curr === undefined || prev === undefined || prev === 0) return undefined;
    return (((curr - prev) / Math.abs(prev)) * 100).toFixed(2);
  };

  const formatInteger = (val?: number) =>
    val !== undefined ? Math.round(val).toLocaleString() : "-";

  const getPerUnitValue = (amount?: number, units?: number) => {
    if (!amount || !units || units === 0) return undefined;
    return (amount / units).toFixed(2);
  };

  /* ---------- CARDS CONFIG ---------- */

  const perUnitCards = [
    "Gross Sales",      // ✅ ADD
    "Net Sales",        // ✅ ADD
    "Marketplace Fees",
    "Others",
    "Cash Generated",
    "Net Reimbursement",
  ];

  const cards = [
    {
      label: "Units",
      value: data.quantity_total,
      prev: previous_summary?.quantity_total,
      icon: <FaBoxArchive size={16} color="#87AD12" />,
      bg: "bg-white",
      border: " border-[#FDD36F] border-t-4 border-t-[#FDD36F] ",
      isCurrency: false,
    },
    {
      label: "Gross Sales",
      value: data.gross_sales,
      prev: previous_summary?.gross_sales,
      icon: <FaMoneyBillTrendUp size={16} />,
      bg: "bg-white",
      border: "border-[#ED9F50] border-t-4 border-t-[#ED9F50]",
      isCurrency: true,
    },
    {
      label: "Net Sales",
      value: data.net_sales,
      prev: previous_summary?.net_sales,
      icon: <FaTags size={16} />,
      bg: "bg-white",
      border: "border-[#75BBDA] border-t-4 border-t-[#75BBDA]",
      isCurrency: true,
    },
    {
      label: "Promotional Discount",
      value: data.promotional_rebates,
      prev: previous_summary?.promotional_rebates,
      icon: <FaPercent size={16} />,
      bg: "bg-white",
      border: "border-[#B8C78C] border-t-4 border-t-[#B8C78C]", // 🔴 red border
      isCurrency: true,
      isDiscount: true,
      isNegative: true, // 👈 ADD THIS
    },

    {
      label: "Marketplace Fees",
      value: marketplaceFees,
      prev: prevMarketplaceFees,
      icon: <FaAmazon size={16} />, // ya FaLayerGroup if you prefer
      bg: "bg-white",
      border: " border-[#B75A5A] border-t-4 border-t-[#B75A5A]",
      isCurrency: true,
    },
    {
      label: "Others",
      value: data.otherwplatform,
      prev: previous_summary?.otherwplatform,
      icon: <FaLayerGroup size={16} />,
      bg: "bg-white",
      border: "border-[#3A8EA4]  border-t-4 border-t-[#3A8EA4]",
      isCurrency: true,
    },
    {
      label: "Cash Generated",
      value: data.cashflow,
      prev: previous_summary?.cashflow,
      icon: <FaWallet size={16} />,
      bg: "bg-white",
      border: "border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]",
      isCurrency: true,
    },
    {
      label: "Net Reimbursement",
      value: data.rembursement_fee,
      prev: previous_summary?.rembursement_fee,
      icon: <FaArrowRotateRight size={16} />,
      bg: "bg-white",
      border: "border-[#C49466] border-t-4 border-t-[#C49466]",
      isCurrency: true,
    },
  ];

  /* ---------- SANKEY ---------- */

  const sankeyColorMap = {
    units: "#FDD36F",
    grossSales: "#75BBDA",         // was orange, now blue
    netSales: "#ED9F50",           // was blue, now orange
    promotionalDiscount: "#B8C78C",
    marketplaceFees: "#B75A5A",
    others: "#3A8EA4",
    cashGenerated: "#7B9A6D",
    netReimbursement: "#C49466",
  };

  const rows = isPreviewSankey
    ? [
      {
        name: "Gross Sales",
        value: 1,
        barColor: sankeyColorMap.grossSales,
      },
      {
        name: "Fees & Costs",
        value: 1,
        barColor: sankeyColorMap.marketplaceFees,
      },
      {
        name: "FBA Fees",
        value: 2,
        sign: "-",
        barColor: sankeyColorMap.marketplaceFees,
        signColor: "#D32F2F",
      },
      {
        name: "Selling Fees",
        value: 2,
        sign: "-",
        barColor: sankeyColorMap.marketplaceFees,
        signColor: "#D32F2F",
      },
      {
        name: "Ads Cost",
        value: 1,
        sign: "-",
        barColor: sankeyColorMap.netReimbursement,
        signColor: "#D32F2F",
      },
      {
        name: "Cash Generated",
        value: 1,
        barColor: sankeyColorMap.cashGenerated,
      },
    ]
    : [
      {
        name: "Gross Sales",
        value: data.gross_sales || 0,
        sign: "+",
        barColor: sankeyColorMap.grossSales,
        signColor: "#2E7D32",
      },
      {
        name: "Tax and Credit",
        value: data.taxncredit || 0,
        sign: "+",
        barColor: sankeyColorMap.netSales,
        signColor: "#2E7D32",
      },
      {
        name: "Discount",
        value: data.promotional_rebates || 0,
        sign: "-",
        barColor: sankeyColorMap.promotionalDiscount,
        signColor: "#D32F2F",
      },
      {
        name: "FBA Fees",
        value: data.fba_fees || 0,
        sign: "-",
        barColor: sankeyColorMap.marketplaceFees,
        signColor: "#D32F2F",
      },
      {
        name: "Selling Fees",
        value: data.selling_fees || 0,
        sign: "-",
        barColor: sankeyColorMap.marketplaceFees,
        signColor: "#D32F2F",
      },
      {
        name: "Ads Cost",
        value: data.advertising_total || 0,
        sign: "-",
        barColor: sankeyColorMap.netReimbursement,
        signColor: "#D32F2F",
      },
      {
        name: "Other",
        value: data.otherwplatform || 0,
        sign: "-",
        barColor: sankeyColorMap.others,
        signColor: "#D32F2F",
      },
      {
        name: "Cash Generated",
        value: data.cashflow || 0,
        sign: "+",
        barColor: sankeyColorMap.cashGenerated,
        signColor: "#2E7D32",
      },
    ];

  const hasPrevious =
    previous_summary &&
    Object.values(previous_summary).some(
      (v) => v !== undefined && v !== 0
    );

  const option = {
    tooltip: {
      formatter: (p: any) => {
        if (p.name === "Summary") return "";

        return `${p.name}<br/>${formatCurrencyRoundedWithSign(
          Number(p.value || 0)
        )}`;
      },
    },
    series: [
      {
        type: "sankey",
        layout: "none",

        left: isMobile ? "1%" : isTablet ? "1%" : isLaptop ? "2%" : "2%",
        right: isMobile ? "30%" : isTablet ? "34%" : isLaptop ? "38%" : isXL ? "28%" : "24%",
        top: "6%",
        bottom: "6%",

        nodeWidth: isMobile ? 10 : isTablet ? 12 : isLaptop ? 14 : 18,
        nodeGap: isMobile ? 10 : isTablet ? 12 : isLaptop ? 14 : 18,
        layoutIterations: 0,
        label: {
          show: true,
          position: "right",
          overflow: "truncate",
          width: isMobile
            ? 120
            : is2XL
              ? 250
              : isLaptop
                ? 128
                : isXL
                  ? 190
                  : 175,
          formatter: (n: any) => {
            if (isPreviewSankey) {
              return `{label|${n.name}}`;
            }

            const row = rows.find(r => r.name === n.name);
            if (!row) return "";

            const base = data.net_sales;
            const pct =
              base && base !== 0
                ? (Math.abs(row.value) / Math.abs(base)) * 100
                : 0;

            const showSign = row.name !== "Cash Generated";
            const signKey = row.sign === "+" ? "signPlus" : "signMinus";

            const formatted = formatCurrencyWithSign(row.value);

            return (
              `{label|${row.name}}` +
              (showSign ? `{${signKey}|(${row.sign})}` : `{signEmpty| }`) +
              `{amount|${formatted}}` +
              `${!isMobile && !isXL && !isLaptop ? `{pct|(${pct.toFixed(2)}%)}` : ""}`
            );
          },
          rich: {
            label: {
              width: sankeyCols.label,
              align: "left",
              fontSize: isMobile ? 9 : isTablet ? 10 : isLaptop ? 10 : 11,
              color: "#374151",
              fontWeight: 500,
            },
            signPlus: {
              width: sankeyCols.sign,
              align: "center",
              fontSize: isMobile ? 9 : isTablet ? 10 : isLaptop ? 10 : 11,
              fontWeight: 700,
              color: "#2E7D32",
            },
            signMinus: {
              width: sankeyCols.sign,
              align: "center",
              fontSize: isMobile ? 9 : isTablet ? 10 : isLaptop ? 10 : 11,
              fontWeight: 700,
              color: "#D32F2F",
            },
            signEmpty: {
              width: sankeyCols.sign,
            },
            amount: {
              width: sankeyCols.amount,
              align: "right",
              fontSize: isMobile ? 9 : isTablet ? 10 : isLaptop ? 10 : 11,
              fontWeight: 700,
              color: "#111827",
            },
            pct: {
              width: sankeyCols.pct,
              align: "right",
              fontSize: isMobile ? 9 : isTablet ? 10 : isLaptop ? 10 : 11,
              fontWeight: 600,
              color: "#6B7280",
            },
          },
        },

        data: [
          { name: "Summary", itemStyle: { color: "transparent" } },
          ...rows.map((r) => ({
            name: r.name,
            value: Math.abs(r.value),
            itemStyle: { color: r.barColor },
          })),
        ],

        links: rows.map((r) => ({
          source: "Summary",
          target: r.name,
          value: Math.abs(r.value),
          lineStyle: {
            color: r.barColor,
            opacity: 0.45,
          },
        })),
      },
    ],
  };

  /* ---------- RENDER ---------- */

  return (
    <div>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {cards.map((c) => {
          const p = getChangePercent(c.value, c.prev);

          const shouldShowPositive =
            c.label === "Marketplace Fees" || c.label === "Promotional Discount";

          const displayValue = shouldShowPositive
            ? Math.abs(c.value || 0)
            : c.value;

          const currentValue =
            c.label === "Units" ? (
              formatInteger(c.value)
            ) : c.isDiscount ? (
              <>
                {formatCurrencyRoundedWithSign(Math.abs(c.value || 0))}
                <span className="ml-1 2xl:text-xs text-[10px] font-medium text-charcoal-500">
                  (
                  {(((Math.abs(c.value || 0)) / (data.gross_sales || 1)) * 100).toFixed(2)}
                  %)
                </span>
              </>
            ) : (
              <>
                {c.isCurrency
                  ? formatCurrencyByLabel(c.label, displayValue)
                  : formatNumber(displayValue)}

                {perUnitCards.includes(c.label) && (
                  <span className="ml-1 2xl:text-xs text-[10px] font-medium text-charcoal-500">
                    (
                    {formatCurrencyRoundedWithSign(
                      Number(getPerUnitValue(displayValue, data.quantity_total))
                    )}{" "}
                    / Unit)
                  </span>
                )}
              </>
            );

          const previousValueText = !hasPrevious
            ? c.label === "Units"
              ? "-"
              : `${currency}-`
            : c.label === "Units"
              ? formatInteger(c.prev)
              : c.isDiscount
                ? formatCurrencyRoundedWithSign(Math.abs(c.prev || 0))
                : `${c.isCurrency
                  ? formatCurrencyByLabel(
                    c.label,
                    c.label === "Marketplace Fees" ? Math.abs(c.prev || 0) : c.prev
                  )
                  : formatNumber(c.prev)
                }${perUnitCards.includes(c.label)
                  ? ` (${formatCurrencyRoundedWithSign(
                    Number(
                      getPerUnitValue(
                        c.label === "Marketplace Fees" ? Math.abs(c.prev || 0) : c.prev,
                        previous_summary?.quantity_total
                      )
                    )
                  )} / Unit)`
                  : ""
                }`;
                
          const comparisons = [
            {
              label: `${formatPrevLabel(previousLabel || "Previous")}`,
              valueText: previousValueText,
              deltaText: hasPrevious && p ? `${Number(p) < 0 ? "▼" : "▲"} ${Math.abs(Number(p))}%` : "-",
              deltaClassName:
                hasPrevious && p
                  ? Number(p) < 0
                    ? "text-red-600"
                    : "text-green-600"
                  : "text-gray-400",
            },
          ];

          return (
            <SummaryMetricCard
              key={c.label}
              title={c.label}
              value={currentValue}
              className={`border ${c.border} ${c.bg} xl:px-4 px-2 py-3`}
              valueClassName="text-charcoal-700"
              comparisons={comparisons}
            />
          );
        })}
      </div>
      <div className="rounded-xl border shadow bg-white p-4">



        {/* SANKEY */}
        <div className="h-[350px] sm:h-[420px] lg:h-[500px] w-full overflow-hidden">
          <ReactECharts
            option={option}
            notMerge={true}
            lazyUpdate={true}
            style={{ height: "100%", width: "100%" }}
          />
        </div>
        {isPreviewMode && (
          <div className="mt-2 text-center text-xs text-gray-400">
            Preview – connect Amazon and fetch data to see cash flow breakdown
          </div>
        )}
      </div>
    </div>

  );
};

export default CashFlowSankey;