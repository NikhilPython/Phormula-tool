"use client";

import React from "react";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

type InventoryInsightsSectionProps = {
  inventoryBullets?: string[];
  summaryText?: string;
  title?: string;
  className?: string;
};

const InventoryInsightsSection: React.FC<InventoryInsightsSectionProps> = ({
  inventoryBullets = [],
  summaryText = "",
  title = "Inventory Insights",
  className = "",
}) => {
  const hasBullets = inventoryBullets.length > 0;
  const hasSummary = !!summaryText?.trim();

  if (!hasBullets && !hasSummary) return null;

  const detailLines = inventoryBullets.filter((b) => /for detailed/i.test(b));
  const mainLines = inventoryBullets.filter((b) => !/for detailed/i.test(b));

  return (
    <div
      className={`space-y-4 rounded-xl border border-slate-200 bg-white shadow-sm p-4 ${className}`}
    >
      <div className="flex items-center gap-2">
        <PageBreadcrumb pageTitle={title} variant="page" align="left" />
      </div>

      {hasBullets && (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {mainLines.map((b, i) => {
              const raw = String(b || "").trim();

              const isUnfulfillable = /unfulfillable/i.test(raw);
              if (isUnfulfillable) {
                const match = raw.match(/\(([^)]+)\)/);
                const value = match?.[1]?.trim();
                const label = raw.replace(/\([^)]+\)/, "").trim();

                return (
                  <div
                    key={i}
                    className="flex items-start justify-between gap-3 bg-white rounded-lg p-3 border border-amber-100"
                  >
                    <span className="text-sm font-medium text-slate-700">
                      {label}
                    </span>
                    {value ? (
                      <span className="font-bold text-[#414042] text-sm whitespace-nowrap">
                        {value}
                      </span>
                    ) : null}
                  </div>
                );
              }

              const colonIdx = raw.indexOf(":");
              const hasColon = colonIdx > -1;

              const left = hasColon ? raw.slice(0, colonIdx).trim() : raw;
              const right = hasColon ? raw.slice(colonIdx + 1).trim() : "";

              return (
                <div
                  key={i}
                  className="flex justify-between items-center bg-white rounded-lg p-3 border border-amber-100"
                >
                  <span className="text-sm font-medium text-slate-700">
                    {left}
                  </span>
                  {right ? (
                    <span className="font-bold text-[#414042] text-sm whitespace-nowrap">
                      {right}
                    </span>
                  ) : null}
                </div>
              );
            })}
          </div>

          {detailLines.map((line, idx) => (
            <p key={idx} className="text-xs text-slate-500 italic mt-2">
              {line}
            </p>
          ))}
        </>
      )}

      {hasSummary && (
        <div className="text-xs 2xl:text-sm text-charcoal-500 italic border-l-2 border-green-500 pl-3">
          {summaryText}
        </div>
      )}
    </div>
  );
};

export default InventoryInsightsSection;