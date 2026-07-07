// "use client";
// import { StepBadge } from "@/components/common/StepBadge";
// import React from "react";
// import { FaCheck } from "react-icons/fa";

// export function Step1ProductList({ completed, onOpen }: { completed: boolean; onOpen: () => void; }) {
//   return (
//     <div
//       onClick={onOpen}
//       className={`p-4 rounded-md border border-[#5EA68E] cursor-pointer mb-4 transition-all
//         ${completed ? "bg-[#5EA68E26]" : "bg-white hover:bg-gray-50"}`}
//     >
//       <div className="flex items-center justify-between mb-1">
//         <div className="flex items-center gap-2">
//           <StepBadge completed={completed} label={1} />
//           <h3 className="font-semibold text-[#5EA68E] text-sm sm:text-base">
//             Upload Product List with Cost
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
//       <p className="text-[11px] sm:text-xs text-gray-600 ml-7">
//         This helps in calculating COGS for all the products sold. Please include Product Barcode, SKU & Amazon ASIn for calculation.
//       </p>
//     </div>
//   );
// }












"use client";
import { StepBadge } from "@/components/common/StepBadge";
import React from "react";
import { FaCheck } from "react-icons/fa";

export function Step1ProductList({
  completed,
  onOpen,
}: {
  completed: boolean;
  onOpen: () => void;
}) {
  const containerCls = completed
    ? "bg-[#5EA68E26] border-[#5EA68E] cursor-pointer"
    : "bg-white border-[#5EA68E] cursor-pointer hover:bg-gray-50";

  return (
    <div
      onClick={onOpen}
      className={`p-4 rounded-md border mb-4 transition-all ${containerCls}`}
    >
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-2">
          <StepBadge completed={completed} label={1} />
          <h3 className="font-semibold text-sm sm:text-base text-[#5EA68E]">
            Upload Product List with Cost
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

      {/* Description paragraph – matched with Step 2 & 3 */}
      <p className="text-xs  text-gray-600 ml-8 -mt-1">
        Upload a complete product list including Product Barcode, SKU, and Amazon ASIN. 
        This enables accurate COGS calculation for all units sold.
      </p>
    </div>
  );
}
