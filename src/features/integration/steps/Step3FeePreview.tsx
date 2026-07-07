// "use client";
// import { StepBadge } from "@/components/common/StepBadge";
// import React from "react";
// import { FaCheck } from "react-icons/fa";

// export function Step3FeePreview({
//   enabled,
//   completed,
//   onOpen,
// }: {
//   enabled: boolean;
//   completed: boolean;
//   onOpen: () => void;
// }) {
//   return (
//     <div
//       onClick={() => enabled && onOpen()}
//       className={`p-4 rounded-md border mb-4 transition-all ${
//         enabled
//           ? completed
//             ? "bg-[#5EA68E26] border-[#5EA68E] cursor-pointer"
//             : "bg-white border-[#5EA68E] cursor-pointer hover:bg-gray-50"
//           : "bg-gray-100 border-gray-200 opacity-60 cursor-not-allowed"
//       }`}
//     >
//       <div className="flex items-center justify-between mb-1">
//         <div className="flex items-center gap-2">
//           <StepBadge completed={completed} label={3} />
//           <h3 className={`font-semibold text-sm sm:text-base ${enabled ? "text-[#5EA68E]" : "text-gray-500"}`}>
//             Upload Fee Preview
//           </h3>
//         </div>
//         {completed ? (
//           <span className="flex items-center gap-1 text-[10px] font-semibold text-white bg-[#5EA68E] px-3 py-1 rounded-full">
//             <FaCheck className="w-3 h-3" /> Completed
//           </span>
//         ) : (
//           <img src="/Favicon.png" alt="icon" className="h-5 sm:h-6" />
//         )}
//       </div>
//     </div>
//   );
// }






















"use client";
import { StepBadge } from "@/components/common/StepBadge";
import React from "react";
import { FaCheck } from "react-icons/fa";

export function Step3FeePreview({
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
      ? "bg-[#5EA68E26] border-[#5EA68E] cursor-pointer"
      : "bg-white border-[#5EA68E] cursor-pointer hover:bg-gray-50"
    : "bg-gray-100 border-gray-200 opacity-60 cursor-not-allowed";

  return (
    <div
      onClick={() => enabled && onOpen()}
      className={`p-4 rounded-md border mb-4 transition-all ${containerCls}`}
    >
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2">
          <StepBadge completed={completed} label={3} />
          <h3
            className={`font-semibold text-sm sm:text-base ${
              enabled ? "text-[#5EA68E]" : "text-gray-500"
            }`}
          >
            Upload Fee Preview File
          </h3>
        </div>

        {completed ? (
          <span className="flex items-center gap-1 text-[10px] font-semibold text-white bg-[#5EA68E] px-3 py-1 rounded-full">
            <FaCheck className="w-3 h-3" /> Completed
          </span>
        ) : (
          <img src="/Favicon.png" alt="icon" className="h-5 sm:h-6" />
        )}
      </div>

      {/* 🔹 Description Text (Added as requested) */}
      <p className="text-xs text-gray-600 ml-8 -mt-1">
        This file is available on Amazon. It helps identify product dimensions 
        and category to accurately calculate and verify Amazon fees.
      </p>
    </div>
  );
}
