"use client";
import { StepBadge } from "@/components/common/StepBadge";
import React from "react";
import { FaCheck } from "react-icons/fa";
import { useRouter } from "next/navigation";

type IntegrationKey = "amazon" | "shopify";

/**
 * Almost-copy of your Step2Integration,
 * but it redirects to /integration?open=amazon|shopify on click.
 */
export function Step2IntegrationCustom({
  locked,
  completed,
  redirectBase = "/integration", // <-- IMPORTANT: your page path
}: {
  locked: boolean;
  completed: boolean;
  redirectBase?: string;
}) {
  const router = useRouter();

  const options: { key: IntegrationKey; title: string; icon: string }[] = [
    { key: "amazon", title: "Amazon Integration", icon: "/amazon.png" },
    { key: "shopify", title: "Shopify Integration", icon: "/shopify.png" },
  ];

  const containerCls = locked
    ? "bg-gray-100 border-gray-200 opacity-60 cursor-not-allowed"
    : completed
    ? "bg-[#5EA68E26] border-[#5EA68E]"
    : "bg-white border-[#5EA68E] hover:bg-gray-50";

  const go = (provider: IntegrationKey) => {
    if (locked) return;
    const sp = new URLSearchParams({ open: provider });
    router.push(`${redirectBase}?${sp.toString()}`);
  };

  return (
    <div className={`p-4 rounded-md border mb-4 transition-all ${containerCls}`}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <StepBadge completed={completed} label={2} />
          <h3 className={`font-semibold text-sm sm:text-base ${locked ? "text-gray-500" : "text-[#5EA68E]"}`}>
            Select Your Integration Method
          </h3>
        </div>
        {locked ? (
          <i className="fa-solid fa-lock text-gray-500 text-xs" />
        ) : completed ? (
          <span className="flex items-center gap-1 text-[10px] font-semibold text-white bg-[#5EA68E] px-3 py-1 rounded-full">
            <FaCheck className="w-3 h-3" /> Completed
          </span>
        ) : (
          <img src="/Favicon.png" alt="icon" className="h-5 sm:h-6" />
        )}
      </div>

      <div className="flex gap-5 ml-8">
        {options.map((opt) => (
          <button
            key={opt.key}
            type="button"
            disabled={locked}
            onClick={() => go(opt.key)}
            title={opt.title}
            className={`aspect-square w-14 rounded-md border border-[#5EA68E] flex items-center justify-center
              ${locked ? "opacity-60 cursor-not-allowed" : "hover:bg-emerald-50"}`}
            aria-label={opt.title}
          >
            <img src={opt.icon} alt={opt.title} className="h-7 w-7 object-contain" />
          </button>
        ))}
      </div>
    </div>
  );
}
