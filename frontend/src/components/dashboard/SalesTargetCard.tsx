"use client";
import React, { memo, useState, useRef } from "react";
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
  data: RegionMetrics;
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
  periodCompletedPct?: number;
  periodCompletedLabel?: string;

  currentMonthLabel?: string;
  previousMonthLabel?: string;

  // ✅ add these
  completedAt?: number | string | Date | null;
  completedTimeZone?: string;
};

const currencySymbolMap: Record<CurrencyCode, string> = {
  USD: "$",
  GBP: "£",
  INR: "₹",
  CAD: "C$",
};

const toApostropheLabel = (s: string) => s.replace(" ", "'");

function SalesTargetCard({
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
  currentMonthLabel,
  previousMonthLabel,
  completedAt,
  completedTimeZone,
}: Props) {

  const wrapRef = React.useRef<HTMLDivElement | null>(null);

  const extraBottom = 10;

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

  const rawLastMonthTotalHomeFinal =
    typeof lastMonthTotalHome === "number" && Number.isFinite(lastMonthTotalHome)
      ? lastMonthTotalHome
      : useBi
        ? biAlignedTotals!.total_previous_net_sales_full_month
        : lastMonthTotalHomeResolved;

  const rawLastMonthToDateHomeFinal = useBi
    ? biAlignedTotals!.total_previous_net_sales
    : lastMonthToDateHomeResolved;

  const lastMonthTotalHomeFinal = Number(rawLastMonthTotalHomeFinal) || 0;
  const lastMonthToDateHomeFinal = Number(rawLastMonthToDateHomeFinal) || 0;

  // const lastMonthToDateHomeFinal = Number(rawLastMonthToDateHomeFinal) || 0;

  // last month MTD-to-date for the same day-range
  // const lastMonthToDateHomeFinal = useBi
  //   ? biAlignedTotals!.total_previous_net_sales
  //   : lastMonthToDateHomeResolved;

  // ---- Gauge ratios (all in HOME currency) ----

  // const mtdVal = Math.max(0, Number(mtdHomeResolved) || 0);
  // const targetVal = Math.max(0, Number(targetHomeResolved) || 0);
  // const prevVal = Math.max(0, Number(lastMonthTotalHomeResolved) || 0);

  const mtdVal = Math.max(0, Number(mtdHomeFinal) || 0);
  const prevVal = Math.max(0, Number(lastMonthTotalHomeFinal) || 0);

  // ✅ fallback target logic (FIX)
  const targetHomeFinal =
    Number(targetHomeResolved) > 0
      ? Number(targetHomeResolved)
      : Number(lastMonthTotalHomeFinal) || 0;

  const targetVal = Math.max(0, targetHomeFinal);

  // % calculation now uses fallback target
  const pctDisplay = targetVal > 0 ? (mtdVal / targetVal) * 100 : 0;

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

  const getMonthCompletedPctTillYesterday = (
    timestamp?: number | string | Date | null,
    timeZone?: string
  ) => {
    if (!timestamp || !timeZone) return 0;

    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) return 0;

    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(date);

    const get = (type: Intl.DateTimeFormatPartTypes) =>
      parts.find((p) => p.type === type)?.value || "";

    const year = Number(get("year"));
    const month = Number(get("month")); // 1..12
    const day = Number(get("day"));

    if (!year || !month || !day) return 0;

    // ✅ date - 1 because current day is still going on
    const completedDay = Math.max(day - 1, 0);

    const daysInMonth = new Date(year, month, 0).getDate();

    return daysInMonth > 0 ? (completedDay / daysInMonth) * 100 : 0;
  };

  const completedPct =
    completedAt && completedTimeZone
      ? getMonthCompletedPctTillYesterday(completedAt, completedTimeZone)
      : typeof periodCompletedPct === "number" && Number.isFinite(periodCompletedPct)
        ? periodCompletedPct
        : 0;

  const completedLabel = periodCompletedLabel ?? "Month";

  const paceDeltaPct = pctDisplay - completedPct;


  const prevLabel = previousMonthLabel ?? getPrevMonthShortLabel();
  const thisMonthLabel = currentMonthLabel ?? getThisMonthShortLabel();

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

  const tipLines = [
    `MTD Sale: ${formatHomeK(mtdHomeFinal)} (${pctDisplay.toFixed(2)}%)`,
    `${thisMonthLabel} Target: ${formatHomeK(targetHomeFinal)}`,
    `${prevLabel} Sale: ${formatHomeK(lastMonthTotalHomeFinal)}`,
    `Last month by today: ${formatHomeK(lastMonthToDateHomeFinal)}`,
  ];

  const reimbNowLabel = thisMonthLabel;
  const reimbPrevLabel = prevLabel;

  // const reimbNow =
  //   biEnabled && biAlignedTotals
  //     ? (biAlignedTotals.total_current_rembursement_fee ?? 0)
  //     : (currentReimbursement ?? 0);

  // const reimbPrev =
  //   biEnabled && biAlignedTotals
  //     ? (biAlignedTotals.total_previous_rembursement_fee ?? 0)
  //     : (previousReimbursement ?? 0);

  const reimbNowFromProps =
    typeof currentReimbursement === "number" && Number.isFinite(currentReimbursement)
      ? currentReimbursement
      : null;

  const reimbPrevFromProps =
    typeof previousReimbursement === "number" && Number.isFinite(previousReimbursement)
      ? previousReimbursement
      : null;

  const reimbNow =
    reimbNowFromProps !== null
      ? reimbNowFromProps
      : biEnabled && biAlignedTotals
        ? (biAlignedTotals.total_current_rembursement_fee ?? 0)
        : 0;

  const reimbPrev =
    reimbPrevFromProps !== null
      ? reimbPrevFromProps
      : biEnabled && biAlignedTotals
        ? (biAlignedTotals.total_previous_rembursement_fee ?? 0)
        : 0;

  // const reimbMax = Math.max(reimbNow, reimbPrev, 1);
  // const reimbNowPct = (reimbNow / reimbMax) * 100;
  // const reimbPrevPct = (reimbPrev / reimbMax) * 100;

  const reimbScaleMax = Math.max(Math.abs(reimbNow), Math.abs(reimbPrev), 1);

  const getBiFill = (value: number, otherValue: number) => {
    // if both are zero, show nothing
    if (value === 0 && otherValue === 0) {
      return { leftPct: 0, rightPct: 0 };
    }

    // if only this value exists, fill its whole side
    if (value !== 0 && otherValue === 0) {
      return {
        leftPct: value < 0 ? 50 : 0,
        rightPct: value > 0 ? 50 : 0,
      };
    }

    // normal comparison mode: scale by largest absolute value
    const pct = (Math.abs(value) / reimbScaleMax) * 50;

    return {
      leftPct: value < 0 ? pct : 0,
      rightPct: value > 0 ? pct : 0,
    };
  };

  const reimbNowFill = getBiFill(reimbNow, reimbPrev);
  const reimbPrevFill = getBiFill(reimbPrev, reimbNow);

  const homeCurrencySymbol = currencySymbolMap[homeCurrency];

  const reimbNowSalesPct =
    Math.abs(mtdHomeFinal) > 0
      ? (reimbNow / Math.abs(mtdHomeFinal)) * 100
      : 0;

  const reimbPrevSalesPct =
    Math.abs(lastMonthTotalHomeFinal) > 0
      ? (reimbPrev / Math.abs(lastMonthTotalHomeFinal)) * 100
      : 0;

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

  const formatAbsWithCurrencySpace = (value: number) => {
    return formatWithCurrencySpace(Math.abs(value)).replace(/^[-+]/, "");
  };

  const showReimbDelta =
    typeof reimbursementDeltaPct === "number" &&
    !Number.isNaN(reimbursementDeltaPct);

  return (
    <>

      <div className="rounded-xl border p-3 2xl:p-5 shadow-sm min-h-[430px] lg:h-full flex flex-col lg:justify-between min-[1700px]:justify-start bg-white">
        {/* Legend */}
        <div className="mt-2 2xl:mt-2 grid grid-cols-4 gap-4 text-[10px] 2xl:text-xs">
          <div className="flex items-start justify-start gap-2 min-w-0">
            <span
              className="mt-0.5 h-2.5 w-2.5 shrink-0 rounded-sm"
              style={{ backgroundColor: "#ED9F50" }}
            />
            <span className="text-charcoal-500 leading-tight break-words">
              MTD Sale
            </span>
          </div>

          <div className="flex items-start justify-start gap-2 min-w-0">
            <span
              className="mt-0.5 h-2.5 w-2.5 shrink-0 rounded-sm"
              style={{ backgroundColor: "#5EA68E" }}
            />
            <span className="text-charcoal-500 leading-tight break-words">
              {thisMonthLabel} Target
            </span>
          </div>

          <div className="flex items-start justify-start gap-2 min-w-0">
            <span
              className="mt-0.5 h-2.5 w-2.5 shrink-0 rounded-sm"
              style={{ backgroundColor: "#9ca3af" }}
            />
            <span className="text-charcoal-500 leading-tight break-words">
              {prevLabel} Sale
            </span>
          </div>

          <div className="flex items-start justify-start gap-2 min-w-0">
            <span
              className="mt-0.5 h-2.5 w-2.5 shrink-0 rounded-sm"
              style={{ backgroundColor: "#B75A5A" }}
            />
            <span className="text-charcoal-500 leading-tight break-words">
              {prevLabel} MTD
            </span>
          </div>
        </div>

        {/* Gauge */}
        <div className="mt-3 2xl:mt-4 flex flex-col items-center">
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
          <div className="mt-2 text-center">
            <div className="text-3xl font-semibold">{pctDisplay.toFixed(2)}%</div>
            <div className="text-[10px] 2xl:text-xs text-charcoal-500">Target Achieved</div>
            <div className="mt-2 text-[10px] 2xl:text-xs text-charcoal-500">
              <span className="text-green-500 font-bold">{completedPct.toFixed(2)}%</span>
              {" "}of {completedLabel} Completed vs{" "}
              <span className="text-green-500 font-bold">{pctDisplay.toFixed(2)}%</span>
              {" "}of Target Achieved
            </div>
          </div>
        </div>

        {/* Reimbursement Section */}
        <div className="min-[1700px]:mt-7 px-3 pt-2 pb-0">
          <div className="flex items-center justify-center gap-2">
            <div className="text-[10px] 2xl:text-xs text-charcoal-500">
              Monthly Reimbursement
            </div>

            {/* {showReimbDelta && (
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
            )} */}
          </div>

          <div className="mt-2">
            <div className="flex items-center justify-between text-[10px] 2xl:text-xs">
              <span className="text-charcoal-500">
                {reimbNowLabel}
              </span>
              <span className="font-semibold text-gray-900">
                {formatWithCurrencySpace(reimbNow)}{" "}
                <span className="text-charcoal-500 font-medium">
                  ({fmtPct(reimbNowSalesPct)})
                </span>
              </span>
            </div>

            {/* <div className="mt-1 h-2 w-full rounded-full bg-gray-100 overflow-hidden">
            <div
              className="h-full rounded-full"
              style={{ width: `${reimbNowPct}%`, backgroundColor: "#ED9F50" }}
            />
          </div> */}

            <div className="mt-1 relative h-2 w-full rounded-full bg-gray-100 overflow-hidden">

              {/* Center line */}
              <div className="absolute left-1/2 top-0 h-full w-px bg-gray-300 -translate-x-1/2 z-10" />

              {/* 🔴 Center red marker */}
              <span
                className="absolute h-2.5 w-0.5 rounded-full z-20"
                style={{
                  backgroundColor: "#C97A2B",
                  left: "50%",
                  top: "50%",
                  transform: "translate(-50%, -50%)",
                }}
              />

              {reimbNowFill.leftPct > 0 && (
                <div
                  className="absolute right-1/2 top-0 h-full rounded-l-full overflow-hidden"
                  style={{
                    width: `${reimbNowFill.leftPct}%`,
                    backgroundColor: "#ED9F50",
                  }}
                >
                  <span className="bar-shimmer reverse" />
                </div>
              )}

              {reimbNowFill.rightPct > 0 && (
                <div
                  className="absolute left-1/2 top-0 h-full rounded-r-full overflow-hidden"
                  style={{
                    width: `${reimbNowFill.rightPct}%`,
                    backgroundColor: "#ED9F50",
                  }}
                >
                  <span className="bar-shimmer" />
                </div>
              )}
            </div>
          </div>

          <div className="mt-2">
            <div className="flex items-center justify-between text-[10px] 2xl:text-xs">
              <span className="text-charcoal-500">
                {reimbPrevLabel}
              </span>
              <span className="font-semibold text-gray-900">
                {formatWithCurrencySpace(reimbPrev)}{" "}
                <span className="text-charcoal-500 font-medium">
                  ({fmtPct(reimbPrevSalesPct)})
                </span>
              </span>
            </div>
            {/* <div className="mt-1 h-2 w-full rounded-full bg-gray-100 overflow-hidden">
            <div
              className="h-full rounded-full"
              style={{ width: `${reimbPrevPct}%`, backgroundColor: "#9CA3AF" }}
            />
          </div> */}


            <div className="mt-1 relative h-2 w-full rounded-full bg-gray-100 overflow-hidden">

              {/* Center line */}
              <div className="absolute left-1/2 top-0 h-full w-px bg-gray-300 -translate-x-1/2 z-10" />

              {/* 🔴 Center red marker */}
              <span
                className="absolute h-2.5 w-0.5 rounded-full z-20"
                style={{
                  backgroundColor: "#6B7280",
                  left: "50%",
                  top: "50%",
                  transform: "translate(-50%, -50%)",
                }}
              />

              {reimbPrevFill.leftPct > 0 && (
                <div
                  className="absolute right-1/2 top-0 h-full rounded-l-full overflow-hidden"
                  style={{
                    width: `${reimbPrevFill.leftPct}%`,
                    backgroundColor: "#9CA3AF",
                  }}
                >
                  <span className="bar-shimmer reverse" />
                </div>
              )}

              {reimbPrevFill.rightPct > 0 && (
                <div
                  className="absolute left-1/2 top-0 h-full rounded-r-full overflow-hidden"
                  style={{
                    width: `${reimbPrevFill.rightPct}%`,
                    backgroundColor: "#9CA3AF",
                  }}
                >
                  <span className="bar-shimmer" />
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  );
}


export default memo(SalesTargetCard);
