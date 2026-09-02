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
  laptopFit?: boolean;
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
  laptopFit = false,
}: SegmentedToggleProps<T>) {
  const containerPad = compact ? "p-1" : "p-1";
  const gap = compact ? "gap-1" : "gap-1";
  const radius = compact ? "rounded-md" : "rounded-lg";
  const btnRadius = compact ? "rounded-md" : "rounded-lg";
  const btnPad = compact ? "px-3 py-1.5" : "px-3 py-1";
  const font = textSizeClass ?? (compact ? "text-[10px] sm:text-xs" : "text-xs");

  return (
    <div
      className={[
        "inline-block w-fit",
        laptopFit ? "lg:max-2xl:block lg:max-2xl:w-full" : "",
        className,
      ].join(" ")}
    >
      <div
        className={[
          "inline-block max-w-full overflow-x-auto sm:overflow-visible [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden",
          laptopFit
            ? "lg:max-2xl:block lg:max-2xl:w-full lg:max-2xl:overflow-hidden"
            : "",
        ].join(" ")}
      >
        <div
          className={[
            "inline-flex w-fit",
            laptopFit
              ? "lg:max-2xl:flex lg:max-2xl:w-full lg:max-2xl:justify-between lg:max-2xl:gap-0.5"
              : "",
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
                  laptopFit
                    ? "lg:max-2xl:min-w-0 lg:max-2xl:flex-1 lg:max-2xl:px-1.5"
                    : "",
                  btnRadius,
                  btnPad,
                  "text-center font-medium transition-colors duration-150",
                  "whitespace-nowrap leading-none",
                  laptopFit
                    ? "lg:max-2xl:whitespace-normal lg:max-2xl:leading-tight"
                    : "",
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
