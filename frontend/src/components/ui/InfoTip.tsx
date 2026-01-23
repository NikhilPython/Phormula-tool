// // components/ui/InfoTip.tsx
// "use client";
// import React from "react";

// type InfoTipProps = {
//   text: string;
//   widthClassName?: string; // optional control (e.g. w-64)
// };

// export default function InfoTip({ text, widthClassName = "w-64" }: InfoTipProps) {
//   return (
//     <span className="relative inline-flex items-center group">
//       {/* icon */}
    //   <span
    //     tabIndex={0}
    //     className="ml-1 inline-flex h-4 w-4 items-center justify-center rounded-full border border-slate-400 text-[10px] font-bold text-slate-700
    //                cursor-help select-none
    //                group-hover:bg-slate-100 group-focus-within:bg-slate-100"
    //     aria-label="Info"
    //   >
    //     i
    //   </span>

//       {/* tooltip */}
//       <span
//         className={`pointer-events-none absolute left-1/2 top-full z-50 mt-2 -translate-x-1/2 ${widthClassName}
//                     rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs text-slate-700 shadow-lg
//                     opacity-0 translate-y-1 transition-all
//                     group-hover:opacity-100 group-hover:translate-y-0
//                     group-focus-within:opacity-100 group-focus-within:translate-y-0`}
//         role="tooltip"
//       >
//         {text}
//       </span>
//     </span>
//   );
// }


// components/ui/InfoTip.tsx
"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

type InfoTipProps = {
  text: string;
};

export default function InfoTip({ text }: InfoTipProps) {
  const iconRef = useRef<HTMLSpanElement | null>(null);
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  const updatePos = () => {
    const el = iconRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    // tooltip centered under icon
    setPos({
      top: r.bottom + 10,
      left: r.left + r.width / 2,
    });
  };

  useEffect(() => {
    if (!open) return;
    updatePos();

    const onScroll = () => updatePos();
    const onResize = () => updatePos();

    // capture scroll from any parent (table containers etc.)
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", onResize);

    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", onResize);
    };
  }, [open]);

  const tooltip = useMemo(() => {
    if (!open || !pos) return null;

    return createPortal(
      <div
        style={{ top: pos.top, left: pos.left }}
        className="fixed z-[99999] -translate-x-1/2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs text-slate-700 shadow-lg max-w-[280px]"
        role="tooltip"
      >
        {text}
      </div>,
      document.body
    );
  }, [open, pos, text]);

  return (
    <>
      <span
        ref={iconRef}
          className="ml-1 inline-flex h-3 w-3 items-center justify-center rounded-full border border-yellow-200 text-[10px] font-bold text-green-500 bg-yellow-200
                   cursor-help select-none
                   group-hover:bg-slate-100 group-focus-within:bg-slate-100"
                   style={{backgroundColor:"#F8EDCE"}}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        tabIndex={0}
        aria-label="Info"
      >
       i
      </span>


      {tooltip}
    </>
  );
}
