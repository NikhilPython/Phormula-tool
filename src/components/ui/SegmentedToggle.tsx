"use client";

import React from "react";

type SegmentedValue = string | number;

export interface SegmentedOption<T extends SegmentedValue = SegmentedValue> {
  value: T;
  label?: string;
}

interface SegmentedToggleProps<T extends SegmentedValue = SegmentedValue> {
  value: T;
  options: SegmentedOption<T>[];
  onChange: (val: T) => void;
  className?: string;
  textSizeClass?: string;
  compact?: boolean;
}

/**
 * Responsive segmented toggle:
 * - Natural width by default
 * - No extra empty space on the right
 * - Still supports horizontal scroll if parent is too narrow
 */  
export default function SegmentedToggle<T extends SegmentedValue = SegmentedValue>({
  value,
  options,
  onChange,
  className = "",
  textSizeClass,
  compact = true,
}: SegmentedToggleProps<T>) {
  const containerPad = compact ? "p-1" : "p-1";
  const gap = compact ? "gap-1" : "gap-1";
  const radius = compact ? "rounded-md" : "rounded-lg";
  const btnRadius = compact ? "rounded-md" : "rounded-lg";
  const btnPad = compact ? "px-3 py-1.5" : "px-3 py-1";
  const font = textSizeClass ?? (compact ? "text-[10px] sm:text-xs" : "text-xs");

  return (
    <div className={["inline-block w-fit", className].join(" ")}>
      <div className="inline-block max-w-full overflow-x-auto sm:overflow-visible [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
        <div
          className={[
            "inline-flex w-fit",
            "border border-[#c4c4c4] bg-gray-50",
            containerPad,
            gap,
            radius,
            font,
          ].join(" ")}
        >
          {options.map((opt) => {
            const active = opt.value === value;
            const label = opt.label ?? String(opt.value);

            return (
              <button
                key={String(opt.value)}
                type="button"
                onClick={() => onChange(opt.value)}
                className={[
                  "flex-none",
                  btnRadius,
                  btnPad,
                  "text-center font-medium transition-colors duration-150",
                  "whitespace-nowrap leading-none",
                  active
                    ? "bg-[#5EA68E] text-yellow-200 shadow-sm"
                    : "text-charcoal-500 hover:bg-[#5EA68E40]",
                ].join(" ")}
              >
                {label}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}