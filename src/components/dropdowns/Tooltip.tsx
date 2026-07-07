// import React from "react";

// interface TooltipProps {
//   text: string;
// }

// const Tooltip: React.FC<TooltipProps> = ({ text }) => {
//   return (
//     <div className="relative group inline-block ml-1">
//       {/* Icon */}
//       <div className="w-4 h-4 border border-yellow-200
//           text-yellow-200 text-[10px] rounded-full font-bold flex items-center justify-center  cursor-pointer">
//         i
//       </div>

//       {/* Tooltip Box */}
//       <div
//         className="
//           absolute 
//           hidden 
//           group-hover:block 
//           max-w-lg 
//   bg-white 
//   text-charcoal-500
//           text-xs 
//           p-3 
//           rounded 
//           shadow-lg 
//           z-50 
//           top-6 
//           left-1/2 
//           -translate-x-1/2
//           whitespace-normal
//           break-words
//         "
//       >
//         {text}
//       </div>
//     </div>
//   );
// };

// export default Tooltip;















import React from "react";

interface TooltipProps {
    text: string;
}

const Tooltip: React.FC<TooltipProps> = ({ text }) => {
    return (
        <div className="relative group inline-block ml-1">
            {/* Icon */}
            <div className="w-4 h-4 border border-yellow-200
           text-yellow-200 text-[10px] font-bold flex items-center justify-center rounded-full cursor-pointer">
                i
            </div>

            {/* Tooltip */}
            <div
                className="
          absolute 
          hidden 
          group-hover:block 
                  bg-white 
          text-charcoal-500
          text-xs 
          p-3 
          rounded 
          shadow-lg 
          z-50 
          top-6 
          left-1/2 
          -translate-x-1/2

          w-[260px]        /* <-- Forces a horizontal rectangle */
          whitespace-normal /* <-- FIXES vertical stacking */
          break-words
        "
            >
                {text}
            </div>
        </div>
    );
};

export default Tooltip;
