"use client";
import React from "react";

export function StepBadge({
  completed,
  label,
}: {
  completed: boolean;
  label: number | string;
}) {
  return (
    <div
      className={`flex items-center justify-center rounded-full font-bold border-2 w-6 h-6 text-[10px]
        ${completed
          ? "bg-[#5EA68E] border-[#5EA68E] text-white shadow-md"
          : "bg-white text-[#414042] border-[#D9D9D9]"}`}
    >
    {completed ? (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 24 24"
    className="h-8 w-8"
  >
    <circle cx="12" cy="12" r="12" fill="#5EA68E" />
    <path
      d="M16 9l-5.5 5.5L8 12"
      stroke="white"
      strokeWidth="2"
      fill="none"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
) : (
  label
)}

    </div>
  );
}
