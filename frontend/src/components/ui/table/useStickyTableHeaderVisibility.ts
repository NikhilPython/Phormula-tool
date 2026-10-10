"use client";

import { RefObject, useEffect, useState } from "react";

export default function useStickyTableHeaderVisibility(
  headerRef: RefObject<HTMLElement | null>,
  boundaryRef: RefObject<HTMLElement | null>,
  enabled = true
) {
  const [isVisible, setIsVisible] = useState(true);

  useEffect(() => {
    if (!enabled) {
      setIsVisible(true);
      return;
    }

    let animationFrame = 0;

    const updateVisibility = () => {
      window.cancelAnimationFrame(animationFrame);
      animationFrame = window.requestAnimationFrame(() => {
        const header = headerRef.current;
        const boundary = boundaryRef.current;
        if (!header || !boundary) return;

        const headerRect = header.getBoundingClientRect();
        const boundaryRect = boundary.getBoundingClientRect();
        const totalRow = boundary.querySelector<HTMLElement>(
          '[data-sticky-header-boundary="true"]'
        );
        const boundaryEdge = totalRow
          ? totalRow.getBoundingClientRect().top
          : boundaryRect.bottom;
        const stickyTop = Number.parseFloat(window.getComputedStyle(header).top) || 0;
        const nextVisible = boundaryEdge > stickyTop + headerRect.height;

        setIsVisible((current) =>
          current === nextVisible ? current : nextVisible
        );
      });
    };

    updateVisibility();
    window.addEventListener("scroll", updateVisibility, true);
    window.addEventListener("resize", updateVisibility);

    const resizeObserver =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(updateVisibility)
        : null;

    if (headerRef.current) resizeObserver?.observe(headerRef.current);
    if (boundaryRef.current) resizeObserver?.observe(boundaryRef.current);

    return () => {
      window.cancelAnimationFrame(animationFrame);
      window.removeEventListener("scroll", updateVisibility, true);
      window.removeEventListener("resize", updateVisibility);
      resizeObserver?.disconnect();
    };
  }, [boundaryRef, enabled, headerRef]);

  return isVisible;
}
