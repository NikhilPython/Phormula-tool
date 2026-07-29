import React from "react";
import { containerClass } from "./shared";

const features = [
  {
    title: "Profit clarity",
    description:
      "True contribution margin per SKU, channel, and campaign — not just top-line revenue.",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path
          d="M6 17V9m0 8h12M9 14l3-3 2 2 4-5"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    ),
    iconColor: "#5EA68E",
    iconBg: "#EDF6EF",
  },
  {
    title: "Cashflow control",
    description:
      "Forward-looking cash position, so you see the runway before it gets tight.",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path
          d="M12 5v14m3-11.5c-.8-.7-1.8-1-3-1-1.7 0-3 .9-3 2.2 0 3.3 6 1.6 6 5 0 1.4-1.3 2.3-3.2 2.3-1.3 0-2.5-.4-3.3-1.2"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    ),
    iconColor: "#C6A85C",
    iconBg: "#F7F4E8",
  },
  {
    title: "Inventory forecasting",
    description:
      "Know what to reorder and when, tied directly to margin — not just stock count.",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path
          d="M7 8.5h10l1 9H6l1-9Zm3-1V6.2C10 5 10.9 4 12 4s2 1 2 2.2v1.3"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    ),
    iconColor: "#6E9AB5",
    iconBg: "#EEF4F5",
  },
  {
    title: "Ads & fee intelligence",
    description:
      "Ad spend and marketplace fees reconciled against real margin, automatically.",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <circle
          cx="12"
          cy="12"
          r="6.5"
          stroke="currentColor"
          strokeWidth="1.7"
        />
        <circle cx="12" cy="12" r="2" fill="currentColor" />
      </svg>
    ),
    iconColor: "#6D9C7F",
    iconBg: "#EDF5EE",
  },
] as const;

export default function PlatformDashboardSection() {
  return (
    <section
      id="platform"
      className="
        relative overflow-hidden
        bg-[#FAFAF7]
        py-[64px]
        sm:py-[72px]
        lg:py-[78px]
        xl:py-[86px]
        2xl:py-[104px]
        
      "
    >
      <div className={`${containerClass} relative z-[1]`}>
        <div className="mx-auto max-w-[820px] text-center">
          <div
            className="
              inline-flex items-center justify-center
              rounded-full
              border border-[#5EA68E]/20
              bg-[#EAF5F0]
              px-[13px] py-[6px]
              text-[9px] font-extrabold uppercase
              leading-none tracking-[0.11em]
              text-[#4B9A7D]

              2xl:px-[15px]
              2xl:py-[7px]
              2xl:text-[10px]
            "
          >
            The Platform
          </div>

          <h2
            className="
              mt-[18px]
              font-[var(--font-dm-serif)]
              text-[32px]
              leading-[1.08]
              tracking-[-0.035em]
              text-[#2F3E5B]

              sm:text-[38px]
              lg:text-[42px]
              xl:text-[46px]
              2xl:text-[54px]
            "
          >
            One{" "}
            <span className="text-[#5EA68E]">
              Dashboard
            </span>{" "}
            for Every Number that Matters.
          </h2>

          <p
            className="
              mx-auto mt-[13px]
              max-w-[510px]
              text-[12px]
              leading-[1.55]
              text-[#626B79]

              sm:text-[13px]
              lg:text-[13px]
              2xl:max-w-[560px]
              2xl:text-[14px]
            "
          >
            Phormula pulls sales, ad spend, fees, and inventory into a single
            source of truth — refreshed continuously, not once a month.
          </p>
        </div>

        <div
          className="
            mt-[38px]
            grid grid-cols-1
            gap-[14px]

            sm:grid-cols-2
            lg:mt-[46px]
            lg:grid-cols-4
            lg:gap-[14px]

            xl:gap-[16px]
            2xl:mt-[54px]
            2xl:gap-[20px]
          "
        >
          {features.map((feature) => (
            <article
              key={feature.title}
              className="
                group
                min-h-[176px]
                rounded-[14px]
                border border-[#37455F]/[0.08]
                bg-white
                px-[18px] py-[19px]
                shadow-[0_5px_18px_rgba(55,69,95,0.035)]
                transition-all duration-300

                hover:-translate-y-1
                hover:border-[#37455F]/[0.13]
                hover:shadow-[0_14px_34px_rgba(55,69,95,0.08)]

                lg:min-h-[184px]
                lg:px-[18px]
                lg:py-[20px]

                xl:min-h-[192px]
                xl:px-[20px]
                xl:py-[22px]

                2xl:min-h-[218px]
                2xl:rounded-[17px]
                2xl:px-[24px]
                2xl:py-[25px]
              "
            >
              <div
                className="
                  grid h-[31px] w-[31px]
                  place-items-center
                  rounded-[8px]

                  2xl:h-[36px]
                  2xl:w-[36px]
                  2xl:rounded-[9px]
                "
                style={{
                  backgroundColor: feature.iconBg,
                  color: feature.iconColor,
                }}
              >
                <span className="block h-[16px] w-[16px] 2xl:h-[18px] 2xl:w-[18px]">
                  {feature.icon}
                </span>
              </div>

              <h3
                className="
                  mt-[17px]
                  font-[var(--font-dm-serif)]
                  text-[15px]
                  leading-[1.25]
                  tracking-[-0.015em]
                  text-[#35415A]

                  xl:text-[16px]
                  2xl:mt-[20px]
                  2xl:text-[18px]
                "
              >
                {feature.title}
              </h3>

              <p
                className="
                  mt-[8px]
                  text-[11px]
                  leading-[1.55]
                  text-[#68717E]

                  xl:text-[12px]
                  2xl:mt-[10px]
                  2xl:text-[13px]
                  2xl:leading-[1.6]
                "
              >
                {feature.description}
              </p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}