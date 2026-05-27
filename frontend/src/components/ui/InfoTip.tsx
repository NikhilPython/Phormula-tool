// components/ui/InfoTip.tsx
"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

type InfoTipProps = {
  text: React.ReactNode;
};

export default function InfoTip({ text }: InfoTipProps) {
  const iconRef = useRef<HTMLSpanElement | null>(null);
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  const updatePos = () => {
    const el = iconRef.current;
    if (!el) return;

    const r = el.getBoundingClientRect();

    const tooltipWidth = Math.min(360, window.innerWidth - 32);
    const centerLeft = r.left + r.width / 2;

    const safeLeft = Math.max(
      16 + tooltipWidth / 2,
      Math.min(centerLeft, window.innerWidth - 16 - tooltipWidth / 2)
    );

    setPos({
      top: r.bottom + 10,
      left: safeLeft,
    });
  };

  useEffect(() => {
    if (!open) return;
    updatePos();

    const onScroll = () => updatePos();
    const onResize = () => updatePos();

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
        className="fixed z-[99999] w-[360px] max-w-[calc(100vw-32px)] -translate-x-1/2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs leading-5 text-slate-700 shadow-lg whitespace-pre-line"
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
        className="inline-flex h-3 w-3 items-center justify-center rounded-full border border-yellow-200 bg-yellow-200 text-[10px] font-bold text-green-500 cursor-help select-none group-hover:bg-slate-100 group-focus-within:bg-slate-100"
        style={{ backgroundColor: "#F8EDCE" }}
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