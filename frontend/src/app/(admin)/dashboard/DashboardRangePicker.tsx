"use client";

import React, { useEffect, useRef, useState } from "react";
import { DateRange } from "react-date-range";
import "react-date-range/dist/styles.css";
import "react-date-range/dist/theme/default.css";
import { FaCalendarAlt } from "react-icons/fa";

export default function DashboardRangePicker({
    selectedStartDay,
    selectedEndDay,
    label,
    onSubmit,
    onClear,
    onCloseReset,
}: {
    selectedStartDay: number | null;
    selectedEndDay: number | null;
    label: string;
    onSubmit: (s: number | null, e: number | null) => void;
    onClear: () => void;
    onCloseReset: () => void;
}) {
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);

    const monthStart = new Date(today.getFullYear(), today.getMonth(), 1);
    const monthEnd = new Date(today.getFullYear(), today.getMonth() + 1, 0);
    const maxSelectableDate = yesterday < monthEnd ? yesterday : monthEnd;

    const [shownDate, setShownDate] = useState<Date>(monthStart);
    const [showCalendar, setShowCalendar] = useState(false);
    const [isMtdPlExpanded, setIsMtdPlExpanded] = useState(false);

    const [calendarRange, setCalendarRange] = useState<any>([
        { startDate: null, endDate: null, key: "selection" },
    ]);

    const MS_PER_DAY = 24 * 60 * 60 * 1000;

    const startOfDay = (d: Date) => new Date(d.getFullYear(), d.getMonth(), d.getDate());

    const clampDate = (d: Date, min: Date, max: Date) => {
        const t = d.getTime();
        return new Date(Math.min(Math.max(t, min.getTime()), max.getTime()));
    };

    const calcRangeCompleted = (
        startDate: Date,
        endDate: Date,
        monthStart: Date,
        maxSelectableDate: Date
    ) => {
        const start = clampDate(startOfDay(startDate), startOfDay(monthStart), startOfDay(maxSelectableDate));
        const end = clampDate(startOfDay(endDate), startOfDay(monthStart), startOfDay(maxSelectableDate));

        const rangeDays =
            end.getTime() >= start.getTime()
                ? Math.round((end.getTime() - start.getTime()) / MS_PER_DAY) + 1
                : 0;

        const daysInMonth = new Date(start.getFullYear(), start.getMonth() + 1, 0).getDate();
        const rangeCompletedPct = daysInMonth > 0 ? (rangeDays / daysInMonth) * 100 : 0;

        return { rangeDays, daysInMonth, rangeCompletedPct };
    };

    const [pendingStartDay, setPendingStartDay] = useState<number | null>(null);
    const [pendingEndDay, setPendingEndDay] = useState<number | null>(null);

    // added: store real selected dates locally
    const [pendingStartDate, setPendingStartDate] = useState<Date | null>(null);
    const [pendingEndDate, setPendingEndDate] = useState<Date | null>(null);

    const [rangeCompletedPct, setRangeCompletedPct] = useState(0);
    const [rangeDays, setRangeDays] = useState(0);
    const [daysInMonth, setDaysInMonth] = useState(0);

    const wrapperRef = useRef<HTMLDivElement | null>(null);

    const formatRangeLabel = (startDate: Date | null, endDate: Date | null) => {
        if (!startDate || !endDate) return "Select Date Range";

        const startMonth = startDate.toLocaleString("en-US", { month: "short" });
        const endMonth = endDate.toLocaleString("en-US", { month: "short" });

        if (
            startDate.getFullYear() === endDate.getFullYear() &&
            startDate.getMonth() === endDate.getMonth()
        ) {
            return `${startMonth} ${startDate.getDate()}-${endDate.getDate()}`;
        }

        return `${startMonth} ${startDate.getDate()} - ${endMonth} ${endDate.getDate()}`;
    };

    useEffect(() => {
        if (!showCalendar) return;

        const onPointerDown = (e: PointerEvent) => {
            const el = wrapperRef.current;
            if (!el) return;

            if (!el.contains(e.target as Node)) {
                setShowCalendar(false);
                setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
                setPendingStartDay(null);
                setPendingEndDay(null);
                setPendingStartDate(null);
                setPendingEndDate(null);
                onCloseReset();
            }
        };

        document.addEventListener("pointerdown", onPointerDown, true);
        return () => document.removeEventListener("pointerdown", onPointerDown, true);
    }, [showCalendar, onCloseReset]);

    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === "Escape") setIsMtdPlExpanded(false);
        };
        if (isMtdPlExpanded) window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, [isMtdPlExpanded]);

    const handleCalendarChange = (ranges: any) => {
        const range = ranges.selection;
        setCalendarRange([range]);

        if (range.startDate && range.endDate) {
            setPendingStartDay(range.startDate.getDate());
            setPendingEndDay(range.endDate.getDate());

            // added
            setPendingStartDate(range.startDate);
            setPendingEndDate(range.endDate);

            const { rangeDays, daysInMonth, rangeCompletedPct } = calcRangeCompleted(
                range.startDate,
                range.endDate,
                monthStart,
                maxSelectableDate
            );

            setRangeDays(rangeDays);
            setDaysInMonth(daysInMonth);
            setRangeCompletedPct(rangeCompletedPct);
        } else {
            setPendingStartDay(null);
            setPendingEndDay(null);
            setPendingStartDate(null);
            setPendingEndDate(null);

            setRangeDays(0);
            setDaysInMonth(0);
            setRangeCompletedPct(0);
        }
    };

    const applyRange = () => {
        onSubmit(pendingStartDay, pendingEndDay);
        setShowCalendar(false);
    };

    const clearRange = () => {
        setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
        setPendingStartDay(null);
        setPendingEndDay(null);
        setPendingStartDate(null);
        setPendingEndDate(null);
        onClear();
    };

    const closeAndReset = () => {
        setCalendarRange([{ startDate: null, endDate: null, key: "selection" }]);
        setPendingStartDay(null);
        setPendingEndDay(null);
        setPendingStartDate(null);
        setPendingEndDate(null);
        setShowCalendar(false);
        onCloseReset();
    };

    return (
        <div ref={wrapperRef} className="relative">
            <button
                type="button"
                onClick={() => setShowCalendar((s) => !s)}
                className="flex items-center gap-2 text-xs 2xl:text-sm"
                style={{
                    padding: "6px 10px",
                    borderRadius: 8,
                    border: "1px solid #D9D9D9E5",
                    backgroundColor: "#ffffff",
                }}
            >
                <FaCalendarAlt className="text-sm 2xl:text-md" />
                {label}
            </button>

            {showCalendar && (
                <div
                    style={{
                        position: "absolute",
                        right: 0,
                        top: "110%",
                        zIndex: 50,
                        backgroundColor: "#ffffff",
                        boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
                        padding: 8,
                        borderRadius: 8,
                        minWidth: 320,
                    }}
                >
                    <DateRange
                        ranges={calendarRange}
                        onChange={handleCalendarChange}
                        moveRangeOnFirstSelection={false}
                        showMonthAndYearPickers={false}
                        rangeColors={["#5EA68E"]}
                        minDate={monthStart}
                        maxDate={maxSelectableDate}
                        shownDate={shownDate}
                        onShownDateChange={() => setShownDate(monthStart)}
                        startDatePlaceholder="Start"
                        endDatePlaceholder="End"
                    />

                    <style jsx global>{`
                        .rdrNextPrevButton {
                            display: none !important;
                        }
                    `}</style>

                    <div className="flex justify-between mt-2 gap-2">
                        <button
                            type="button"
                            onClick={clearRange}
                            className="text-xs px-2 py-1 border rounded"
                        >
                            Clear
                        </button>

                        <div className="flex gap-2">
                            <button
                                type="button"
                                onClick={applyRange}
                                disabled={pendingStartDay == null || pendingEndDay == null}
                                className="text-xs px-2 py-1 rounded text-yellow-200"
                                style={{
                                    background: "#37455F",
                                    opacity: pendingStartDay == null ? 0.6 : 1,
                                }}
                            >
                                Submit
                            </button>
                            <button
                                type="button"
                                onClick={closeAndReset}
                                className="text-xs px-2 py-1 rounded text-charcoal-500 border border-charcoal-500"
                            >
                                Close
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
