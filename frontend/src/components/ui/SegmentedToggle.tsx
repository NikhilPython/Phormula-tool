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
}

/**
 * Responsive segmented toggle:
 * - Mobile: full width, each segment equal width
 * - Desktop: auto width by default (unless parent forces width)
 */
export function SegmentedToggle<T extends SegmentedValue = SegmentedValue>({
  value,
  options,
  onChange,
  className = "",
  textSizeClass = "text-xs",
}: SegmentedToggleProps<T>) {
  return (
    <div
      className={[
        // ✅ full width by default, can be overridden by parent className
        "w-full sm:w-auto",
        "flex rounded-lg border border-[#c4c4c4] bg-gray-50 p-1",
        "gap-1",
        textSizeClass,
        className,
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
              "flex-1",
              "rounded-lg px-3 py-1 text-xs 2xl:text-sm",
              "text-center font-medium transition-colors duration-150",
              "whitespace-nowrap",
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
  );
}

export default SegmentedToggle;
