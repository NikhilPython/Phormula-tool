// "use client";
// import { StepBadge } from "@/components/common/StepBadge";
// import React from "react";
// import { FaCheck } from "react-icons/fa";


// export function Step3AmazonFinancial({
//     enabled,
//     completed,
//     onOpen,
// }: {
//     enabled: boolean;
//     completed: boolean;
//     onOpen: () => void;
// }) {
//     return (
//         <div
//             onClick={() => enabled && onOpen()}
//             className={`p-4 z-5 rounded-md border mb-4 transition-all ${enabled
//                     ? completed
//                         ? "bg-[#5EA68E26] border-[#5EA68E] cursor-pointer"
//                         : "bg-white border-[#5EA68E] cursor-pointer hover:bg-gray-50"
//                     : "bg-gray-100 border-gray-200 opacity-60 cursor-not-allowed"
//                 }`}
//         >
//             <div className="flex items-center justify-between mb-1">
//                 <div className="flex items-center gap-2">
//                     <StepBadge completed={completed} label={3} />
//                     <h3 className={`font-semibold text-sm sm:text-base ${enabled ? "text-[#5EA68E]" : "text-gray-500"}`}>
//                         Amazon Financial Dashboard
//                     </h3>
//                 </div>
//                 {completed ? (
//                     <span className="flex items-center gap-1 text-[10px] font-semibold text-white bg-[#5EA68E] px-3 py-1 rounded-full">
//                         <FaCheck className="w-3 h-3" /> Unlocked
//                     </span>
//                 ) : (
//                     <img src="/Favicon.png" alt="icon" className="h-5 sm:h-6" />
//                 )}
//             </div>
//             <p className="text-xs text-slate-600">
//                 Connect Amazon first, then open your financial dashboard to fetch settlements/finances.
//             </p>
//         </div>
//     );
// }




"use client";
import { StepBadge } from "@/components/common/StepBadge";
import React from "react";
import { FaInfinity } from "react-icons/fa";

export function Step3AmazonFinancial({
  enabled,
  completed,
  onOpen,
}: {
  enabled: boolean;
  completed: boolean;
  onOpen: () => void;
}) {
  const containerCls = enabled
    ? completed
      ? "bg-white border-[#1D9BF0] shadow-sm cursor-pointer hover:bg-gray-50"
      : "bg-white border-[#1D9BF0] cursor-pointer hover:bg-gray-50"
    : "bg-gray-100 border-gray-200 opacity-60 cursor-not-allowed";

  return (
    <div
      onClick={() => enabled && onOpen()}
      className={`p-4 rounded-md border mb-4 transition-all ${containerCls}`}
    >
      <div className="flex items-start justify-between gap-2">
        {/* Left: step badge + text */}
        <div>
          <div className="flex items-center gap-2 mb-1">
            <StepBadge completed={completed} label={3} />
            <h3
              className={`font-semibold text-sm sm:text-base ${
                enabled ? "text-[#5EA68E]" : "text-gray-500"
              }`}
            >
              Select Data Fetch Period
            </h3>
          </div>

          <p className="text-gray-600 text-xs  ml-8 -mt-1 mb-3">
            Choose the time period for which you want to fetch historical data
            from Amazon. This will determine how much past data will be
            imported into your dashboard.
          </p>
        </div>

        {/* Right: infinity icon */}
        <div className="mt-1">
          <FaInfinity
            className={`h-5 w-5 ${
              enabled ? "text-[#5EA68E]" : "text-gray-400"
            }`}
          />
        </div>
      </div>
    </div>
  );
}
