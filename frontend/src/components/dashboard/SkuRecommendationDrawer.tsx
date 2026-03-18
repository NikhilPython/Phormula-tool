// "use client";

// import React from "react";
// import Drawer from "@mui/material/Drawer";
// import Productinfoinpopup from "@/components/businessInsight/Productinfoinpopup";
// import PageBreadcrumb from "../common/PageBreadCrumb";

// type ObjectiveContext = {
//   growth_intent?: string;
//   profit_priority?: string;
//   inventory_clearance_priority?: boolean;
// } | null;

// type MetricItem = {
//   label: string;
//   value: string;
//   color?: string;
// };

// type SelectedRec = {
//   productName: string;
//   metrics: MetricItem[];
//   journeyPoints: string[];
//   recommendationPoints: string[];
//   advertisingPoints?: string[];
//   inventoryPoints?: string[];
//   showChart?: boolean;
// } | null;

// const ObjectiveCards = ({
//   objective,
//   className = "",
// }: {
//   objective?: ObjectiveContext;
//   className?: string;
// }) => {
//   const growth = objective?.growth_intent?.replaceAll("_", " ") || "Not Defined";
//   const profit = objective?.profit_priority?.replaceAll("_", " ") || "Not Defined";
//   const inv = objective?.inventory_clearance_priority ? "Yes" : "No";

//   const Card = ({ label, value }: { label: string; value: string }) => (
//     <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
//       <div className="2xl:text-sm text-xs text-[#7A7A7A]">{label}</div>
//       <div className="mt-1 2xl:text-sm text-xs font-semibold text-[#0F172A] capitalize">
//         {value}
//       </div>
//     </div>
//   );

//   return (
//     <div className={`grid grid-cols-1 sm:grid-cols-3 gap-5 ${className}`}>
//       <Card label="Primary Focus" value={growth} />
//       <Card label="Profit Strategy" value={profit} />
//       <Card label="Inventory Dilution" value={inv} />
//     </div>
//   );
// };

// type Props = {
//   open: boolean;
//   onClose: () => void;
//   selectedRec: SelectedRec;
//   objectiveContext?: ObjectiveContext;
//   countryName: string;
// };

// const metricColors = [
//   "border border-[#FDD36F] border-t-[#FDD36F]",
//   "border border-[#75BBDA] border-t-[#75BBDA]",
//   "border border-[#B75A5A] border-t-[#B75A5A]",
//   "border border-[#7B9A6D] border-t-[#7B9A6D]",
//   "border border-[#C49466] border-t-[#C49466]",
// ];

// const metricOrder = [
//   "units",
//   "net sales",
//   "asp",
//   "cm1 profit",
//   "cm1 profit per unit",
// ];

// export default function SkuRecommendationDrawer({
//   open,
//   onClose,
//   selectedRec,
//   objectiveContext,
//   countryName,
// }: Props) {
//   const sortedMetrics = [...(selectedRec?.metrics || [])].sort((a, b) => {
//     const aIndex = metricOrder.indexOf(a.label.toLowerCase());
//     const bIndex = metricOrder.indexOf(b.label.toLowerCase());

//     const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
//     const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

//     return safeAIndex - safeBIndex;
//   });

//   return (
//     <Drawer
//       anchor="right"
//       open={open}
//       onClose={onClose}
//       PaperProps={{
//         sx: {
//           width: { xs: "100vw", sm: "75vw", md: "60vw", lg: "50vw", "min-[1700px]": "50vw" },
//           maxWidth: 900,
//         },
//       }}
//     >
//       <div className="flex flex-col gap-4 h-full">
//         <div className="shrink-0 border-b border-slate-200 p-3 flex items-center justify-between gap-3">
//           <div>
//             <div className="flex items-center gap-1 flex-wrap">
//               <PageBreadcrumb
//                 pageTitle="Detailed View - "
//                 variant="page"
//                 textSize="2xl"
//               />

//               <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
//                 {selectedRec?.productName || "Details"}
//               </span>
//             </div>
//           </div>

//           <button
//             onClick={onClose}
//             className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
//           >
//             ✕
//           </button>
//         </div>

//         <div className="flex-1 overflow-y-auto space-y-6 px-3">
//           <div>
//             <div className="text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-700 mb-2">Metrics</div>

//             <div className="grid grid-cols-2 sm:grid-cols-3 2xl:grid-cols-5 gap-3">
//               {sortedMetrics.map((m, i) => (
//                 <div
//                   key={i}
//                   className={`rounded-lg border border-t-4 ${metricColors[i % metricColors.length]} bg-slate-50 px-3 py-2`}
//                 >
//                   <div className="text-[10px] 2xl:text-xs text-charcoal-400">
//                     {m.label
//                       .replace(/\b\w/g, (char) => char.toUpperCase())
//                       .replace("Cm1", "CM1")}
//                   </div>

//                   <div className="mt-1 flex flex-col leading-tight">
//                     {(() => {
//                       const match = m.value.match(/^([^\(]+)\s*(\(.+\))?$/);
//                       const mainValue = match?.[1]?.trim() || m.value;
//                       const percentPart = match?.[2] || "";

//                       const isNegative = percentPart.includes("-");
//                       const percentColor = isNegative ? "#FF5C5C" : "#5EA68E";

//                       return (
//                         <>
//                           <span
//                             className="text-sm 2xl:text-lg font-bold"
//                             style={{ color: "#414042" }}
//                           >
//                             {mainValue}
//                           </span>

//                           {percentPart && (
//                             <span
//                               className="text-[10px] 2xl:text-xs font-semibold"
//                               style={{ color: percentColor }}
//                             >
//                               {percentPart}
//                             </span>
//                           )}
//                         </>
//                       );
//                     })()}
//                   </div>
//                 </div>
//               ))}
//             </div>
//           </div>

//           <div>
//             <div className="text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-500 mb-2">
//               Recommendations
//             </div>

//             {selectedRec?.recommendationPoints?.length ? (
//               <div>
//                 <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">Action</div>
//                 <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
//                   {selectedRec.recommendationPoints.map((p, i) => (
//                     <li key={i}>{p}</li>
//                   ))}
//                 </ul>
//               </div>
//             ) : (
//               <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
//             )}

//             {selectedRec?.advertisingPoints?.length ? (
//               <div className="mt-2">
//                 <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
//                   Advertising
//                 </div>
//                 <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
//                   {selectedRec.advertisingPoints.map((p, i) => (
//                     <li key={i}>{p}</li>
//                   ))}
//                 </ul>
//               </div>
//             ) : null}

//             {selectedRec?.inventoryPoints?.length ? (
//               <div className="mt-2">
//                 <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
//                   Inventory
//                 </div>
//                 <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
//                   {selectedRec.inventoryPoints.map((p, i) => (
//                     <li key={i}>{p}</li>
//                   ))}
//                 </ul>
//               </div>
//             ) : null}
//           </div>

//           {selectedRec?.showChart && selectedRec?.productName && (
//             <div className="w-full">
//               <Productinfoinpopup
//                 productname={selectedRec.productName}
//                 countryName={countryName}
//               />
//             </div>
//           )}

//           <div className="pb-4">
//             <div className="flex items-center gap-1 flex-wrap ">
//               <PageBreadcrumb
//                 pageTitle="Product Journey"
//                 variant="page"
//                 textSize="lg"
//               />
//             </div>

//             {selectedRec?.journeyPoints?.length ? (
//               <ul className="space-y-1 text-xs 2xl:text-sm text-charcoal-600">
//                 {selectedRec.journeyPoints.map((p, i) => (
//                   <li key={i} className="flex items-start gap-2">
//                     {/* <span className="text-charcoal-400">→</span> */}
//                     <span >
//                       {p 
//                         .replace(/^\d+\.\s*-\s*/, "")
//                         .replace(/^\d+\.\s*/, "")
//                         .replace(/^-+\s*/, "") }
//                     </span>
//                   </li>
//                 ))}
//               </ul>
//             ) : (
//               <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
//             )}
//           </div>
//         </div>
//       </div>
//     </Drawer>
//   );
// }


















"use client";

import React from "react";
import Drawer from "@mui/material/Drawer";
import Productinfoinpopup from "@/components/businessInsight/Productinfoinpopup";
import PageBreadcrumb from "../common/PageBreadCrumb";

type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

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
  sourceCountryName?: string;
  displayCurrency?: CurrencyCode;
};

const metricColors = [
  "border border-[#FDD36F] border-t-[#FDD36F]",
  "border border-[#75BBDA] border-t-[#75BBDA]",
  "border border-[#B75A5A] border-t-[#B75A5A]",
  "border border-[#7B9A6D] border-t-[#7B9A6D]",
  "border border-[#C49466] border-t-[#C49466]",
];

const metricOrder = [
  "units",
  "net sales",
  "asp",
  "cm1 profit",
  "cm1 profit per unit",
];

export default function SkuRecommendationDrawer({
  open,
  onClose,
  selectedRec,
  objectiveContext,
  countryName,
  sourceCountryName,
  displayCurrency,
}: Props) {
  const sortedMetrics = [...(selectedRec?.metrics || [])].sort((a, b) => {
    const aIndex = metricOrder.indexOf(a.label.toLowerCase());
    const bIndex = metricOrder.indexOf(b.label.toLowerCase());

    const safeAIndex = aIndex === -1 ? Number.MAX_SAFE_INTEGER : aIndex;
    const safeBIndex = bIndex === -1 ? Number.MAX_SAFE_INTEGER : bIndex;

    return safeAIndex - safeBIndex;
  });

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          width: { xs: "100vw", sm: "75vw", md: "60vw", lg: "50vw", "min-[1700px]": "50vw" },
          maxWidth: 900,
        },
      }}
    >
      <div className="flex flex-col gap-4 h-full">
        <div className="shrink-0 border-b border-slate-200 p-3 flex items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-1 flex-wrap">
              <PageBreadcrumb
                pageTitle="Detailed View - "
                variant="page"
                textSize="2xl"
              />

              <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl font-bold text-green-500">
                {selectedRec?.productName || "Details"}
              </span>
            </div>
          </div>

          <button
            onClick={onClose}
            className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
          >
            ✕
          </button>
        </div>

        <div className="flex-1 overflow-y-auto space-y-6 px-3">
          <div>
            <div className="text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-700 mb-2">Metrics</div>

            <div className="grid grid-cols-2 sm:grid-cols-3 2xl:grid-cols-5 gap-3">
              {sortedMetrics.map((m, i) => (
                <div
                  key={i}
                  className={`rounded-lg border border-t-4 ${metricColors[i % metricColors.length]} bg-slate-50 px-3 py-2`}
                >
                  <div className="text-[10px] 2xl:text-xs text-charcoal-400">
                    {m.label
                      .replace(/\b\w/g, (char) => char.toUpperCase())
                      .replace("Cm1", "CM1")}
                  </div>

                  <div className="mt-1 flex flex-col leading-tight">
                    {(() => {
                      const match = m.value.match(/^([^\(]+)\s*(\(.+\))?$/);
                      const mainValue = match?.[1]?.trim() || m.value;
                      const percentPart = match?.[2] || "";

                      const isNegative = percentPart.includes("-");
                      const percentColor = isNegative ? "#FF5C5C" : "#5EA68E";

                      return (
                        <>
                          <span
                            className="text-sm 2xl:text-lg font-bold"
                            style={{ color: "#414042" }}
                          >
                            {mainValue}
                          </span>

                          {percentPart && (
                            <span
                              className="text-[10px] 2xl:text-xs font-semibold"
                              style={{ color: percentColor }}
                            >
                              {percentPart}
                            </span>
                          )}
                        </>
                      );
                    })()}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div>
            <div className="text-xs sm:text-sm 2xl:text-lg font-semibold text-charcoal-500 mb-2">
              Recommendations
            </div>

            {selectedRec?.recommendationPoints?.length ? (
              <div>
                <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">Action</div>
                <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                  {selectedRec.recommendationPoints.map((p, i) => (
                    <li key={i}>{p}</li>
                  ))}
                </ul>
              </div>
            ) : (
              <div className="text-xs 2xl:text-sm text-charcoal-500">—</div>
            )}

            {selectedRec?.advertisingPoints?.length ? (
              <div className="mt-2">
                <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                  Advertising
                </div>
                <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                  {selectedRec.advertisingPoints.map((p, i) => (
                    <li key={i}>{p}</li>
                  ))}
                </ul>
              </div>
            ) : null}

            {selectedRec?.inventoryPoints?.length ? (
              <div className="mt-2">
                <div className="text-xs 2xl:text-sm font-semibold text-charcoal-500">
                  Inventory
                </div>
                <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                  {selectedRec.inventoryPoints.map((p, i) => (
                    <li key={i}>{p}</li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>

          {selectedRec?.showChart && selectedRec?.productName && (
            <div className="w-full">
              <Productinfoinpopup
                productname={selectedRec.productName}
                countryName={countryName}
                sourceCountryName={sourceCountryName}
                displayCurrency={displayCurrency}
              />
            </div>
          )}

          <div className="pb-4">
            <div className="flex items-center gap-1 flex-wrap ">
              <PageBreadcrumb
                pageTitle="Product Journey"
                variant="page"
                textSize="lg"
              />
            </div>

            {selectedRec?.journeyPoints?.length ? (
              <ul className="space-y-1 text-xs 2xl:text-sm text-charcoal-600">
                {selectedRec.journeyPoints.map((p, i) => (
                  <li key={i} className="flex items-start gap-2">
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