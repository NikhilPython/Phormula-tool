"use client";

import { useEffect } from "react";
import { usePathname, useSearchParams } from "next/navigation";

type Props = {
  /** set this to your top header height (px) */
  offset?: number;
};

export default function HashScroll({ offset = 80 }: Props) {
  const pathname = usePathname();
  const searchParams = useSearchParams(); // helps re-run effect when params change

  const scrollToHash = () => {
    const hash = window.location.hash?.replace("#", "");
    if (!hash) return;

    const el = document.getElementById(hash);
    if (!el) return;

    const y = el.getBoundingClientRect().top + window.scrollY - offset;
    window.scrollTo({ top: y, behavior: "smooth" });
  };

  useEffect(() => {
    // when page mounts / route changes
    scrollToHash();

    // when only hash changes (same page)
    window.addEventListener("hashchange", scrollToHash);
    return () => window.removeEventListener("hashchange", scrollToHash);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathname, searchParams]);

  return null;
}
