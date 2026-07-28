import type { ReactNode } from "react";

export const containerClass = "mx-auto w-full max-w-[1560px] px-10 max-md:px-4";

export function Eyebrow({ children, dark = false }: { children: ReactNode; dark?: boolean }) {
  return (
    <div className={`inline-flex items-center gap-2 rounded-full border px-3.5 py-2 text-[12px] font-extrabold uppercase tracking-[0.08em] ${dark ? "border-white/15 bg-white/[0.08] text-[#FDD36F]" : "border-[#5EA68E]/25 bg-[#5EA68E]/10 text-[#4A8A74]"}`}>
      <span className={`h-[7px] w-[7px] rounded-full ${dark ? "bg-[#FDD36F] shadow-[0_0_0_6px_rgba(253,211,111,0.12)]" : "bg-[#5EA68E] shadow-[0_0_0_6px_rgba(94,166,142,0.14)]"}`} />
      {children}
    </div>
  );
}

export function SectionHeading({ children, light = false, className = "" }: { children: ReactNode; light?: boolean; className?: string }) {
  return <h2 className={`mt-4 font-[var(--font-dm-serif)] text-[clamp(2.2rem,4vw,3.4rem)] leading-[1.08] tracking-[-0.035em] ${light ? "text-white" : "text-[#37455F]"} ${className}`}>{children}</h2>;
}

export function SectionCopy({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <p className={`mx-auto mt-[18px] max-w-[650px] text-[1.06rem] leading-[1.7] text-[#5A6272] ${className}`}>{children}</p>;
}
