import React from "react";
import AmazonStatCard, { AmazonStatCardProps } from "@/components/dashboard/AmazonStatCard";

type KpiItem = Pick<
  AmazonStatCardProps,
  | "label"
  | "current"
  | "previous"
  | "loading"
  | "formatter"
  | "previousFormatter"
  | "bottomLabel"
  | "className"
  | "deltaPct"
  | "inverseDelta"
>;

type DashboardStickyKpisProps = {
  items: KpiItem[];
  className?: string;
  stickyClassName?: string;
};


export default function DashboardStickyKpis({
  items,
  className = "",
  stickyClassName = "",
}: DashboardStickyKpisProps) {
  return (
    // <div
    //   className={`sticky max-[480px]:top-[96px] max-[640px]:top-[96px] sm:top-[108px] md:top-[108px] 2xl:top-[124px] z-20 bg-[#F7F7F7] py-2 ${stickyClassName}`}
    // >
      <div className={`w-full ${className}`}>
        <div className="grid grid-cols-2 sm:grid-cols-4 min-[1700px]:grid-cols-8 w-full gap-2 2xl:gap-3 auto-rows-fr mt-2 md:mt-4">
          {items.map((item) => (
            <AmazonStatCard
              key={item.label}
              label={item.label}
              current={item.current}
              previous={item.previous}
              deltaPct={item.deltaPct}
              loading={item.loading}
              formatter={item.formatter}
              previousFormatter={item.previousFormatter}
              bottomLabel={item.bottomLabel}
              className={item.className}
              inverseDelta={item.inverseDelta}
            />
          ))}
        </div>
      </div>
    // </div>
  );
}