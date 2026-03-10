"use client";

import React from "react";
import Drawer from "@mui/material/Drawer";
import Productinfoinpopup from "@/components/businessInsight/Productinfoinpopup";

type ObjectiveContext = {
  growth_intent?: string;
  profit_priority?: string;
  inventory_clearance_priority?: boolean;
} | null;

type MetricItem = {
  label: string;
  value: string;
  color?: string;
};

type SelectedRec = {
  productName: string;
  metrics: MetricItem[];
  journeyPoints: string[];
  recommendationPoints: string[];
  advertisingPoints?: string[];
  inventoryPoints?: string[];
  showChart?: boolean;
} | null;

const ObjectiveCards = ({
  objective,
  className = "",
}: {
  objective?: ObjectiveContext;
  className?: string;
}) => {
  const growth = objective?.growth_intent?.replaceAll("_", " ") || "Not Defined";
  const profit = objective?.profit_priority?.replaceAll("_", " ") || "Not Defined";
  const inv = objective?.inventory_clearance_priority ? "Yes" : "No";

  const Card = ({ label, value }: { label: string; value: string }) => (
    <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
      <div className="2xl:text-sm text-xs text-[#7A7A7A]">{label}</div>
      <div className="mt-1 2xl:text-sm text-xs font-semibold text-[#0F172A] capitalize">
        {value}
      </div>
    </div>
  );

  return (
    <div className={`grid grid-cols-1 sm:grid-cols-3 gap-5 ${className}`}>
      <Card label="Primary Focus" value={growth} />
      <Card label="Profit Strategy" value={profit} />
      <Card label="Inventory Dilution" value={inv} />
    </div>
  );
};

type Props = {
  open: boolean;
  onClose: () => void;
  selectedRec: SelectedRec;
  objectiveContext?: ObjectiveContext;
  countryName: string;
};

export default function SkuRecommendationDrawer({
  open,
  onClose,
  selectedRec,
  objectiveContext,
  countryName,
}: Props) {
  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          width: { xs: "100vw", sm: "70vw", md: "50vw", lg: "55vw" },
          maxWidth: 900,
        },
      }}
    >
      <div className="flex flex-col gap-4 h-full">
        <div className="shrink-0 border-b border-slate-200 p-4 flex items-start justify-between gap-3">
          <div>
            <div className="text-sm text-slate-500">Detailed View</div>
            <div className="text-lg font-semibold text-slate-900">
              {selectedRec?.productName || "Details"}
            </div>
          </div>
          <button
            onClick={onClose}
            className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
          >
            ✕
          </button>
        </div>

        <div className="flex-1 overflow-y-auto space-y-4 px-3">
          {objectiveContext && (
            <div className="mt-1">
              <div className="text-sm font-semibold text-charcoal-700 mb-3">Objective</div>
              <ObjectiveCards objective={objectiveContext} />
            </div>
          )}

          <div>
            <div className="text-sm font-semibold text-charcoal-700 mb-3">Metrics</div>

            <div className="grid grid-cols-3 gap-3">
              {(selectedRec?.metrics || []).map((m, i) => (
                <div key={i} className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                  <div className="text-[11px] 2xl:text-xs text-charcoal-400">{m.label}</div>
                  <div className="text-sm 2xl:text-base font-bold flex items-baseline gap-1">
                    {(() => {
                      const match = m.value.match(/^([^\(]+)\s*(\(.+\))?$/);
                      const mainValue = match?.[1]?.trim() || m.value;
                      const percentPart = match?.[2] || "";

                      const isNegative = percentPart.includes("-");
                      const percentColor = isNegative ? "#FF5C5C" : "#5EA68E";

                      return (
                        <>
                          <span style={{ color: "#414042" }}>{mainValue}</span>
                          {percentPart && <span style={{ color: percentColor }}>{percentPart}</span>}
                        </>
                      );
                    })()}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div>
            <div className="text-sm font-semibold text-charcoal-700 mb-3">Recommendation</div>

            {selectedRec?.recommendationPoints?.length ? (
              <div>
                <div className="text-xs font-semibold text-blue-900 mb-1">💡 Action</div>
                <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-600">
                  {selectedRec.recommendationPoints.map((p, i) => (
                    <li key={i}>{p}</li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
            )}

            {selectedRec?.advertisingPoints?.length ? (
              <div className="mt-4">
                <div className="text-xs font-semibold text-purple-900 mb-1">📢 Advertising</div>
                <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-600">
                  {selectedRec.advertisingPoints.map((p, i) => (
                    <li key={i}>{p}</li>
                  ))}
                </ul>
              </div>
            ) : null}

            {selectedRec?.inventoryPoints?.length ? (
              <div className="mt-4">
                <div className="text-xs font-semibold text-amber-900 mb-1">📦 Inventory</div>
                <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-600">
                  {selectedRec.inventoryPoints.map((p, i) => (
                    <li key={i}>{p}</li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>

         {selectedRec?.showChart && selectedRec?.productName && (
              <div className="w-full overflow-hidden rounded-lg border border-slate-200 p-3 bg-white">
                <div className="w-full">
                  <Productinfoinpopup
                    productname={selectedRec.productName}
                    countryName={countryName}
                  />
                </div>
              </div>
            )}

          <div className="pb-4">
            <div className="text-sm font-semibold text-charcoal-700 mb-3">Product Journey</div>

            {selectedRec?.journeyPoints?.length ? (
              <ul className="space-y-1 text-xs 2xl:text-sm text-charcoal-600">
                {selectedRec.journeyPoints.map((p, i) => (
                  <li key={i} className="flex items-start gap-2">
                    <span className="text-charcoal-400">→</span>
                    <span>
                      {p
                        .replace(/^\d+\.\s*-\s*/, "")
                        .replace(/^\d+\.\s*/, "")
                        .replace(/^-+\s*/, "")}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
            )}
          </div>
        </div>
      </div>
    </Drawer>
  );
}