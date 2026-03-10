"use client";

import React, { useState, useRef, useEffect } from "react";
import {
  getISTDayInfo,
  getPrevMonthShortLabel,
  getThisMonthShortLabel,
} from "@/lib/dashboard/date";
import type { RegionMetrics } from "@/lib/dashboard/types";

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

type BiAlignedTotalsCard = {
  total_current_advertising: number;
  total_current_net_sales: number;
  total_current_platform_fees: number;
  total_current_profit: number;

  total_previous_advertising: number;
  total_previous_net_sales: number;
  total_previous_net_sales_full_month: number;
  total_previous_platform_fees: number;
  total_previous_profit: number;
  total_current_rembursement_fee: number;
  total_previous_rembursement_fee: number;

};


type Props = {
  data: RegionMetrics; // selected region metrics
  homeCurrency: CurrencyCode;
  convertToHomeCurrency: (value: number, from: CurrencyCode) => number;
  formatHomeK: (value: number) => string;
  todaySales?: number;

  targetHome?: number;
  mtdHome?: number;
  lastMonthTotalHome?: number;
  lastMonthToDateHome?: number;
  decTargetHome?: number;
  currentReimbursement?: number;
  previousReimbursement?: number;
  reimbursementDeltaPct?: number | null;
  biEnabled?: boolean;
  biAlignedTotals?: BiAlignedTotalsCard | null;
  periodCompletedPct?: number; // for BI/range mode
  periodCompletedLabel?: string;
};

const currencySymbolMap: Record<CurrencyCode, string> = {
  USD: "$",
  GBP: "£",
  INR: "₹",
  CAD: "C$",
};

const toApostropheLabel = (s: string) => s.replace(" ", "'");

export default function SalesTargetCard({
  data,
  homeCurrency,
  convertToHomeCurrency,
  formatHomeK,
  todaySales,
  targetHome,
  mtdHome,
  lastMonthTotalHome,
  lastMonthToDateHome,
  decTargetHome,
  currentReimbursement,
  previousReimbursement,
  reimbursementDeltaPct,
  biEnabled,
  biAlignedTotals,
  periodCompletedPct,
  periodCompletedLabel,
}: Props) {
 
  const wrapRef = React.useRef<HTMLDivElement | null>(null);

  const [extraBottom, setExtraBottom] = useState(20);

  useEffect(() => {
    const mq = window.matchMedia("(min-width: 1536px)"); // Tailwind 2xl

    const update = () => {
      setExtraBottom(mq.matches ? 20 : 8);
    };

    update();
    mq.addEventListener("change", update);
    return () => mq.removeEventListener("change", update);
  }, []);

  const computedMtdHome = convertToHomeCurrency(data.mtdUSD ?? 0, homeCurrency);
  const computedLastMonthTotalHome = convertToHomeCurrency(
    data.lastMonthTotalUSD ?? 0,
    homeCurrency
  );
  const computedTargetHome = convertToHomeCurrency(
    data.targetUSD ?? 0,
    homeCurrency
  );
  const computedDecTargetHome = convertToHomeCurrency(
    data.decTargetUSD ?? 0,
    homeCurrency
  );

  // ✅ Always use resolved home values (parent wins)
  const mtdHomeResolved =
    typeof mtdHome === "number" && Number.isFinite(mtdHome)
      ? mtdHome
      : computedMtdHome;

  const lastMonthTotalHomeResolved =
    typeof lastMonthTotalHome === "number" && Number.isFinite(lastMonthTotalHome)
      ? lastMonthTotalHome
      : computedLastMonthTotalHome;

  const targetHomeResolved =
    typeof targetHome === "number" && Number.isFinite(targetHome)
      ? targetHome
      : computedTargetHome;

  // ✅ Dec target resolved (parent wins)
  const decTargetHomeResolved =
    typeof decTargetHome === "number" && Number.isFinite(decTargetHome)
      ? decTargetHome
      : computedDecTargetHome;

  const computedLastMonthToDateHome = convertToHomeCurrency(
    data.lastMonthToDateUSD ?? 0,
    homeCurrency
  );

  const lastMonthToDateHomeResolved =
    typeof lastMonthToDateHome === "number" && Number.isFinite(lastMonthToDateHome)
      ? lastMonthToDateHome
      : computedLastMonthToDateHome;

  // ✅ BI overrides (date-range mode)
  const useBi = !!biEnabled && !!biAlignedTotals;

  const mtdHomeFinal = useBi
    ? biAlignedTotals!.total_current_net_sales
    : mtdHomeResolved;

  // full last month total
  const lastMonthTotalHomeFinal = useBi
    ? biAlignedTotals!.total_previous_net_sales_full_month
    : lastMonthTotalHomeResolved;

  // last month MTD-to-date for the same day-range
  const lastMonthToDateHomeFinal = useBi
    ? biAlignedTotals!.total_previous_net_sales
    : lastMonthToDateHomeResolved;


  // ---- Gauge ratios (all in HOME currency) ----
  // const ratio =
  //   targetHomeResolved > 0 ? mtdHomeResolved / targetHomeResolved : 0;

  // const ratioLast =
  //   targetHomeResolved > 0
  //     ? lastMonthTotalHomeResolved / targetHomeResolved
  //     : 0;

  // const decRatio =
  //   targetHomeResolved > 0 ? decTargetHomeResolved / targetHomeResolved : 0;

  // const greenDraw = Math.min(Math.max(ratio, 0), 1);

  // const OVERFLOW_EMPTY_AT = 2;
  // let orangeDraw = 1;
  // if (ratio > 1) {
  //   const t = (ratio - 1) / (OVERFLOW_EMPTY_AT - 1);
  //   orangeDraw = 1 - Math.min(Math.max(t, 0), 1);
  // }

  // // ✅ Base position of blue marker (where Dec target is on the scale)
  // const decBase = Math.min(Math.max(decRatio, 0), 1);

  // // ✅ Shrink factor when ratio > 1 (same behavior as orange)
  // let decShrink = 1;
  // if (ratio > 1) {
  //   const t = (ratio - 1) / (OVERFLOW_EMPTY_AT - 1);
  //   decShrink = 1 - Math.min(Math.max(t, 0), 1);
  // }

  // // ✅ Final visible blue arc
  // const decDraw = decBase * decShrink;
  // const toDeg_DecTarget = 180 * decDraw;

  // const toDeg_MTD = 180 * greenDraw;
  // const toDeg_Orange = 180 * orangeDraw;

  // const pctDisplay = ratio * 100;


  // ---- Gauge ratios (all in HOME currency) ----

  // const mtdVal = Math.max(0, Number(mtdHomeResolved) || 0);
  const targetVal = Math.max(0, Number(targetHomeResolved) || 0);
  // const prevVal = Math.max(0, Number(lastMonthTotalHomeResolved) || 0);

  const mtdVal = Math.max(0, Number(mtdHomeFinal) || 0);
  const prevVal = Math.max(0, Number(lastMonthTotalHomeFinal) || 0);

  const gaugeMax = Math.max(mtdVal, targetVal, prevVal, 1);

  const mtdNorm = mtdVal / gaugeMax;
  const targetNorm = targetVal / gaugeMax;
  const prevNorm = prevVal / gaugeMax;

  const greenDraw = Math.min(Math.max(mtdNorm, 0), 1);     // MTD
  const decDraw = Math.min(Math.max(targetNorm, 0), 1);    // Target
  const orangeDraw = Math.min(Math.max(prevNorm, 0), 1);   // Prev month sale

  const toDeg_MTD = 180 * greenDraw;
  const toDeg_DecTarget = 180 * decDraw;
  const toDeg_Orange = 180 * orangeDraw;

  const pctDisplay = targetVal > 0 ? (mtdVal / targetVal) * 100 : 0;

  const { todayDay } = getISTDayInfo();
  const todayHomeComputed =
    typeof todaySales === "number" && !Number.isNaN(todaySales)
      ? todaySales
      : todayDay > 0
        ? mtdHomeFinal / todayDay
        : 0;

  const now = new Date();
  const totalDaysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();

  // If you want “month completed so far” including today:
  const monthCompletedPct =
    totalDaysInMonth > 0 ? (todayDay / totalDaysInMonth) * 100 : 0;

  // Compare (positive means you're ahead of pace)
  const paceDeltaPct = pctDisplay - monthCompletedPct;

  const completedPct =
    biEnabled && typeof periodCompletedPct === "number"
      ? periodCompletedPct
      : monthCompletedPct;

  const completedLabel = useBi
    ? (periodCompletedLabel ?? "Period")
    : "Month";


  const prevLabel = getPrevMonthShortLabel();
  const thisMonthLabel = getThisMonthShortLabel();

  // Gauge sizing
  const size = 220;
  const strokeMain = 8; // green
  const strokeDec = 5;   // grey
  const strokeLast = 5;  // orange

  const cx = size / 2;

  // ✅ gaps
  const gapBlueToGreen = 0;   // small gap (tweak 0–6)
  const gapGreenToOrange = 10;

  // ✅ IMPORTANT: base radius must account for blue ring too, since it's outermost
  const rBase = size / 2 - strokeDec;

  // ✅ Outer ring (Blue)
  const rDecTarget = rBase;

  // ✅ Middle ring (Green) - just inside Blue
  const rCurrent =
    rDecTarget - strokeDec / 2 - gapBlueToGreen - strokeMain / 2;

  // ✅ Inner ring (Orange)
  const rLastMTD =
    rCurrent - strokeMain / 2 - gapGreenToOrange - strokeLast / 2;

  const toXYRadius = (angDeg: number, radius: number) => {
    const rad = (Math.PI / 180) * (180 - angDeg);
    return {
      x: cx + radius * Math.cos(rad),
      y: size / 2 - radius * Math.sin(rad),
    };
  };

  const arcPath = (fromDeg: number, toDeg: number, radius: number) => {
    const start = toXYRadius(fromDeg, radius);
    const end = toXYRadius(toDeg, radius);
    const largeArc = toDeg - fromDeg > 180 ? 1 : 0;
    return `M ${start.x} ${start.y} A ${radius} ${radius} 0 ${largeArc} 1 ${end.x} ${end.y}`;
  };

  const fullFrom = 0;

  const knobGreen = toXYRadius(toDeg_MTD, rCurrent);
  const knobYellow = toXYRadius(toDeg_Orange, rLastMTD);
  const knobDec = toXYRadius(toDeg_DecTarget, rDecTarget);

  // const prevToDateVal = Math.max(0, Number(lastMonthToDateHomeResolved) || 0);
  const prevToDateVal = Math.max(0, Number(lastMonthToDateHomeFinal) || 0);
  const prevToDateNorm = prevToDateVal / gaugeMax;
  const toDeg_PrevToDate = 180 * Math.min(Math.max(prevToDateNorm, 0), 1);

  const knobPrevToDate = toXYRadius(toDeg_PrevToDate, rLastMTD);

  const radialDeg =
    (Math.atan2(knobPrevToDate.y - size / 2, knobPrevToDate.x - cx) * 180) / Math.PI;

  const markerDeg = radialDeg + 90;

  // Tooltip
  const TOOLTIP_WIDTH = 70;

  const [tip, setTip] = useState<{
    show: boolean;
    x: number;
    y: number;
    title: string;
    lines: string[];
  }>({ show: false, x: 0, y: 0, title: "", lines: [] });

  const showTip = (e: React.MouseEvent, title: string, lines: string[]) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;

    setTip({
      show: true,
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
      title,
      lines,
    });
  };


  const moveTip = (e: React.MouseEvent) => {
    const rect = wrapRef.current?.getBoundingClientRect();
    if (!rect) return;

    setTip((t) =>
      t.show
        ? { ...t, x: e.clientX - rect.left, y: e.clientY - rect.top }
        : t
    );
  };

  const hideTip = () => setTip((t) => ({ ...t, show: false }));

  const tipTitle = "Sales Snapshot";
  // const tipLines = [
  //   `MTD Sale: ${formatHomeK(mtdHomeResolved)} (${pctDisplay.toFixed(2)}%)`,
  //   `${thisMonthLabel} Target: ${formatHomeK(targetHomeResolved)}`,
  //   `${prevLabel} Sale: ${formatHomeK(lastMonthTotalHomeResolved)}`,
  //   `Last month by today: ${formatHomeK(lastMonthToDateHomeResolved)}`,
  // ];

  const tipLines = [
    `MTD Sale: ${formatHomeK(mtdHomeFinal)} (${pctDisplay.toFixed(2)}%)`,
    `${thisMonthLabel} Target: ${formatHomeK(targetHomeResolved)}`,
    `${prevLabel} Sale: ${formatHomeK(lastMonthTotalHomeFinal)}`,
    `Last month by today: ${formatHomeK(lastMonthToDateHomeFinal)}`,
  ];

  // Reimbursement labels
  const reimbNowLabel = new Intl.DateTimeFormat("en-US", {
    month: "short",
    year: "2-digit",
  }).format(new Date());

  const reimbPrevLabel = new Intl.DateTimeFormat("en-US", {
    month: "short",
    year: "2-digit",
  }).format(new Date(new Date().getFullYear(), new Date().getMonth() - 1, 1));

  const reimbNow =
    biEnabled && biAlignedTotals
      ? (biAlignedTotals.total_current_rembursement_fee ?? 0)
      : (currentReimbursement ?? 0);

  const reimbPrev =
    biEnabled && biAlignedTotals
      ? (biAlignedTotals.total_previous_rembursement_fee ?? 0)
      : (previousReimbursement ?? 0);

  const reimbMax = Math.max(reimbNow, reimbPrev, 1);
  const reimbNowPct = (reimbNow / reimbMax) * 100;
  const reimbPrevPct = (reimbPrev / reimbMax) * 100;

  const homeCurrencySymbol = currencySymbolMap[homeCurrency];

  const reimbNowSalesPct =
    // mtdHomeResolved > 0
    //   ? (reimbNow / mtdHomeResolved) * 100
    //   : 0;
    mtdHomeFinal > 0 ? (reimbNow / mtdHomeFinal) * 100 : 0;

  const reimbPrevSalesPct =
    // lastMonthTotalHomeResolved > 0
    //   ? (reimbPrev / lastMonthTotalHomeResolved) * 100
    // : 0;
    lastMonthTotalHomeFinal > 0 ? (reimbPrev / lastMonthTotalHomeFinal) * 100 : 0;

  const fmtPct = (v: number) => `${v.toFixed(2)}%`;

  const formatWithCurrencySpace = (value: number) => {
    const s = String(formatHomeK(value)).trim();

    // handle sign
    let sign = "";
    let rest = s;

    if (rest.startsWith("-") || rest.startsWith("+")) {
      sign = rest[0];
      rest = rest.slice(1).trim();
    }

    if (rest.startsWith(homeCurrencySymbol)) {
      rest = rest.slice(homeCurrencySymbol.length).trim();
    }

    return `${sign}${homeCurrencySymbol}${rest}`;
  };

  const showReimbDelta =
    typeof reimbursementDeltaPct === "number" &&
    !Number.isNaN(reimbursementDeltaPct);



  return (
    <div className="rounded-xl border p-3 2xl:p-5 shadow-sm h-full flex flex-col bg-white">
      {/* Legend */}
      <div className="mt-2 2xl:mt-2 flex items-center justify-center gap-6 text-[10px] 2xl:text-xs">
        <div className="flex items-center justify-center gap-2 w-[60px]">
          <span
            className="h-2.5 w-2.5 rounded-sm"
            style={{ backgroundColor: "#ED9F50" }}
          />
          <span className="text-charcoal-500">MTD Sale</span>
        </div>

        <div className="flex items-center justify-center gap-2 w-[60px]">
          <span
            className="h-2.5 w-2.5 rounded-sm"
            style={{ backgroundColor: "#5EA68E" }}

          />
          <span className="text-charcoal-500">{thisMonthLabel} Target</span>
        </div>

        <div className="flex items-center justify-center gap-2 w-[60px]">
          <span
            className="h-2.5 w-2.5 rounded-sm"
            style={{ backgroundColor: "#9ca3af" }}
          />
          <span className="text-charcoal-500">{prevLabel} Sale</span>
        </div>

        <div className="flex items-center justify-center gap-2 w-[60px]">
          <span
            className="h-2.5 w-2.5 rounded-sm"
            style={{ backgroundColor: "#B75A5A" }}
          />
          <span className="text-charcoal-500">{prevLabel} MTD</span>
        </div>
      </div>

      {/* Gauge */}
      <div className="mt-3 2xl:mt-4 flex flex-col items-center justify-center">
        <div
          ref={wrapRef}
          className="relative"
          style={{ width: size, height: size / 2 + extraBottom, overflow: "visible" }}
          onMouseMove={moveTip}
          onMouseLeave={hideTip}
        >

          <svg
            width={size}
            height={size / 2 + extraBottom}
            viewBox={`0 0 ${size} ${size / 2 + extraBottom}`}
            style={{ overflow: "visible" }}
            overflow="visible"
          >

            {/* Orange arc (Prev month sale reference) */}
            <path
              d={arcPath(fullFrom, toDeg_Orange, rLastMTD)}
              fill="none"
              // stroke="#f59e0b"
              stroke="#9CA3AF"
              strokeWidth={strokeLast}
              strokeLinecap="round"
              onMouseEnter={(e) => showTip(e, tipTitle, tipLines)}
              onMouseLeave={hideTip}
            />

            {/* ✅ Red rectangular marker, perpendicular to arc (radial) */}
            {/* <rect
              x={knobPrevToDate.x - 2}
              y={knobPrevToDate.y - 8}
              width={6}
              height={8}
              rx={1}
              fill="#FF5C5C"
              stroke="#ffffff"
              strokeWidth={1.5}
              transform={`rotate(${markerDeg}, ${knobPrevToDate.x}, ${knobPrevToDate.y})`}
              onMouseEnter={(e) => showTip(e, tipTitle, tipLines)}
              onMouseLeave={hideTip}
            /> */}

            <circle
              cx={knobPrevToDate.x}
              cy={knobPrevToDate.y}
              r={4}
              fill="#B75A5A"
              stroke="#ffffff"
              strokeWidth={2}
            />


            {/* Dec target arc */}
            <path
              d={arcPath(fullFrom, toDeg_DecTarget, rDecTarget)}
              fill="none"
              // stroke="#9CA3AF"
              stroke="#5EA68E"
              strokeWidth={strokeDec}
              strokeLinecap="round"
              onMouseEnter={(e) => showTip(e, tipTitle, tipLines)}
              onMouseLeave={hideTip}
            />

            {/* Green arc (Current MTD) */}
            <path
              d={arcPath(fullFrom, toDeg_MTD, rCurrent)}
              fill="none"
              stroke="#ED9F50"
              strokeWidth={strokeMain}
              strokeLinecap="round"
              onMouseEnter={(e) => showTip(e, tipTitle, tipLines)}
              onMouseLeave={hideTip}
            />

            {/* Knobs */}
            <circle
              cx={knobYellow.x}
              cy={knobYellow.y}
              r={5}
              // fill="#f59e0b"
              fill="#9CA3AF"
              stroke="#fffbeb"
              strokeWidth={3}
              onMouseEnter={(e) => showTip(e, tipTitle, tipLines)}
              onMouseLeave={hideTip}
            />

            <circle
              cx={knobDec.x}
              cy={knobDec.y}
              r={5}
              // fill="#9CA3AF"
              fill="#5EA68E"
              stroke="#eef2ff"
              strokeWidth={3}
              onMouseEnter={(e) => showTip(e, tipTitle, tipLines)}
              onMouseLeave={hideTip}
            />

            <circle
              cx={knobGreen.x}
              cy={knobGreen.y}
              r={7}
              fill="#ED9F50"
              stroke="#ecfdf3"
              strokeWidth={2}
              onMouseEnter={(e) => showTip(e, tipTitle, tipLines)}
              onMouseLeave={hideTip}
            />
          </svg>

          {/* Tooltip */}
          {tip.show && (
            <div
              className="pointer-events-none absolute z-10 rounded-lg border bg-white px-3 py-2 text-xs shadow-md whitespace-nowrap"
              style={{
                top: tip.y - 12,
                left:
                  tip.x + TOOLTIP_WIDTH + 16 > size
                    ? tip.x - TOOLTIP_WIDTH - 12 // 🔥 shift left
                    : tip.x + 12,                // normal right
              }}
            >

              <div className="font-semibold text-gray-900">{tip.title}</div>
              <div className="mt-1 space-y-0.5 text-gray-600">
                {tip.lines.map((l, i) => (
                  <div key={i}>{l}</div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Percentage */}
        <div className="mt-1 2xl:mt-2 text-center">
          <div className="text-3xl font-semibold">{pctDisplay.toFixed(2)}%</div>
          <div className="text-[10px] 2xl:text-xs text-charcoal-500">Target Achieved</div>
          <div className="mt-1 text-[10px] 2xl:text-xs text-charcoal-500">
            {/* <span className=" text-green-500 font-bold">{monthCompletedPct.toFixed(2)}%</span> of Month Completed vs {" "}
            <span className=" text-green-500 font-bold">{pctDisplay.toFixed(2)}%</span> of Target Achieved */}

            <span className="text-green-500 font-bold">{completedPct.toFixed(2)}%
            </span>
            {" "}of Month Completed vs{" "}
            <span className="text-green-500 font-bold">{pctDisplay.toFixed(2)}%</span>
            {" "}of Target Achieved

            {/* <span
              className={`ml-2 font-medium ${paceDeltaPct >= 0 ? "text-green-700" : "text-rose-700"
                }`}
            >
              ({paceDeltaPct >= 0 ? "+" : "-"}
              {Math.abs(paceDeltaPct).toFixed(2)}% pace)
            </span> */}
          </div>

        </div>
      </div>

      {/* Reimbursement Section */}
      <div className=" max-[1700px]:mt-3 px-3 py-2 max-[1700px]:py-3 2xl:py-1.5">
        <div className="flex items-center justify-center gap-2">
          <div className="text-[10px] 2xl:text-xs text-charcoal-500">
            Monthly Reimbursement
          </div>

          {showReimbDelta && (
            <div
              className={`text-[10px] 2xl:text-xs font-medium px-2 py-0.5 rounded ${reimbursementDeltaPct! >= 0
                ? "bg-green-50 text-green-700"
                : "bg-rose-50 text-rose-700"
                }`}
              title="Current vs previous reimbursement (in home currency)"
            >
              {reimbursementDeltaPct! >= 0 ? "▲" : "▼"}{" "}
              {Math.abs(reimbursementDeltaPct!).toFixed(2)}%
            </div>
          )}
        </div>

        <div className="mt-2">
          <div className="flex items-center justify-between text-[10px] 2xl:text-xs">
            <span className="text-charcoal-500">
              {toApostropheLabel(reimbNowLabel)}{' '}
            </span>
            <span className="font-semibold text-gray-900">
              {formatWithCurrencySpace(reimbNow)}{" "}
              <span className="text-charcoal-500 font-medium">
                ({fmtPct(reimbNowSalesPct)})
              </span>
            </span>

          </div>
          <div className="mt-1 h-2 w-full rounded-full bg-gray-100 overflow-hidden">
            <div
              className="h-full rounded-full"
              style={{ width: `${reimbNowPct}%`, backgroundColor: "#ED9F50" }}
            />
          </div>
        </div>

        <div className="mt-2">
          <div className="flex items-center justify-between text-[10px] 2xl:text-xs">
            <span className="text-charcoal-500">
              {toApostropheLabel(reimbPrevLabel)}{' '}
            </span>
            <span className="font-semibold text-gray-900">
              {formatWithCurrencySpace(reimbPrev)}{" "}
              <span className="text-charcoal-500 font-medium">
                ({fmtPct(reimbPrevSalesPct)})
              </span>
            </span>


          </div>
          <div className="mt-1 h-2 w-full rounded-full bg-gray-100 overflow-hidden">
            <div
              className="h-full rounded-full"
              // style={{ width: `${reimbPrevPct}%`, backgroundColor: "#F59E0B" }}
              style={{ width: `${reimbPrevPct}%`, backgroundColor: "#9CA3AF" }}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
