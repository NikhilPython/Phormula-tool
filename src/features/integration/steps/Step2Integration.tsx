// "use client";
// import { StepBadge } from "@/components/common/StepBadge";
// import React from "react";
// import { FaCheck } from "react-icons/fa";

// const options = [
//   { key: "amazon", title: "Amazon Integration", icon: "/amazon.png" },
//   { key: "shopify", title: "Shopify Integration", icon: "/shopify.png" },
// ];

// export function Step2Integration({
//   locked,
//   completed,
//   onChoose,
// }: {
//   locked: boolean;
//   completed: boolean;
//   onChoose: (key: "amazon" | "shopify") => void;
// }) {
//   return (
//     <div
//       className={`p-4 rounded-md border mb-4 transition-all ${
//         locked
//           ? "bg-gray-100 border-gray-200 opacity-60 cursor-not-allowed"
//           : completed
//           ? "bg-[#5EA68E26] border-[#5EA68E]"
//           : "bg-white border-[#5EA68E] hover:bg-gray-50"
//       }`}
//     >
//       <div className="flex items-center justify-between mb-3">
//         <div className="flex items-center gap-2">
//           <StepBadge completed={completed} label={2} />
//           <h3 className={`font-semibold text-sm sm:text-base ${locked ? "text-gray-500" : "text-[#5EA68E]"}`}>
//             Select Your Integration Method
//           </h3>
//         </div>
//         {locked ? (
//           <i className="fa-solid fa-lock text-gray-500 text-xs"></i>
//         ) : completed ? (
//           <span className="flex items-center gap-1 text-[10px] font-semibold text-white bg-[#5EA68E] px-3 py-1 rounded-full">
//             <FaCheck className="w-3 h-3" /> Completed
//           </span>
//         ) : (
//           <img src="/Favicon.png" alt="icon" className="h-5 sm:h-6" />
//         )}
//       </div>

//       <div className="flex gap-5 ml-8">
//         {options.map((opt) => (
//           <button
//             key={opt.key}
//             type="button"
//             disabled={locked}
//             onClick={() => onChoose(opt.key as "amazon" | "shopify")}
//             className="aspect-square w-14 rounded-md border border-[#5EA68E] flex items-center justify-center hover:bg-emerald-50"
//           >
//             <img src={opt.icon} alt="" className="h-7 w-7 object-contain" />
//           </button>
//         ))}
//       </div>
//     </div>
//   );
// }





"use client";
import { StepBadge } from "@/components/common/StepBadge";
import React from "react";
import { FaCheck } from "react-icons/fa";

type IntegrationKey = "amazon" | "shopify";

const options: { key: IntegrationKey; title: string; icon: string }[] = [
  { key: "amazon", title: "Amazon Integration", icon: "/amazon.png" },
  { key: "shopify", title: "Shopify Integration", icon: "/shopify.png" },
];

export function Step2Integration({
  locked,
  completed,
  onChoose,
}: {
  locked: boolean;
  completed: boolean;
  onChoose: (key: IntegrationKey) => void;
}) {
  const containerCls = locked
    ? "bg-gray-100 border-gray-200 opacity-60 cursor-not-allowed"
    : completed
    ? "bg-[#5EA68E26] border-[#5EA68E]"
    : "bg-white border-[#5EA68E] hover:bg-gray-50";

  return (
   <div className={`p-4 rounded-md border mb-4 transition-all ${containerCls}`}>
  <div className="flex items-start justify-between gap-2">
    <div className="flex items-center gap-2">
      <StepBadge completed={completed} label={2} />
      <h3
        className={`font-semibold text-sm sm:text-base ${
          locked ? "text-gray-500" : "text-[#5EA68E]"
        }`}
      >
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

  <p className="text-gray-600 text-xs  ml-8 -mt-1 mb-3">
    Choose how you want to connect your data. Each method offers different levels of
    automation and data synchronization.
  </p>

  <div className="flex gap-5 ml-8">
    {options.map((opt) => (
      <button
        key={opt.key}
        type="button"
        disabled={locked}
        onClick={() => !locked && onChoose(opt.key)}
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
