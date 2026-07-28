import {
  Activity,
  Clock3,
  Sparkles,
  Target,
} from "lucide-react";

import { containerClass } from "./shared";

const items = [
  {
    icon: Target,
    value: "1",
    label: "Source of truth",
    animation: "group-hover:scale-110",
  },
  {
    icon: Activity,
    value: "24/7",
    label: "Live financial visibility",
    animation: "animate-metric-arrow",
  },
  {
    icon: Clock3,
    value: "CM2",
    label: "Profit-level clarity",
    animation: "animate-metric-clock",
  },
  {
    icon: Sparkles,
    value: "∞",
    label: "AI-powered insights",
    animation: "animate-metric-sparkle",
  },
] as const;

export default function MetricStrip() {
  return (
    <section className="bg-[#f7f3ea] py-6 lg:py-8 2xl:py-[42px]">
      <div className={containerClass}>
        <div
          className="
            grid grid-cols-1 overflow-hidden rounded-[18px]
            bg-[radial-gradient(circle_at_15%_20%,rgba(90,116,160,0.35),transparent_32%),linear-gradient(135deg,#07122b_0%,#0d1935_45%,#071126_100%)]
            px-4 py-3
            shadow-[0_20px_45px_rgba(7,18,43,0.22),inset_0_1px_0_rgba(255,255,255,0.12)]

            sm:grid-cols-2
            sm:px-5
            sm:py-4

            lg:grid-cols-4
            lg:rounded-[20px]
            lg:px-3
            lg:py-4

            xl:px-5
            xl:py-5

            2xl:rounded-[22px]
            2xl:px-[34px]
            2xl:py-7
          "
        >
          {items.map(
            ({ icon: Icon, value, label, animation }, index) => (
              <div
                key={label}
                className={`
                  group relative flex min-h-[74px] items-center
                  gap-3 px-3 py-3

                  sm:min-h-[82px]
                  sm:px-4

                  lg:min-h-[76px]
                  lg:gap-3
                  lg:px-4
                  lg:py-2

                  xl:min-h-[82px]
                  xl:gap-4
                  xl:px-5

                  2xl:min-h-[92px]
                  2xl:gap-[18px]
                  2xl:px-[34px]

                  ${
                    index === 0
                      ? "sm:after:absolute sm:after:bottom-3 sm:after:right-0 sm:after:top-3 sm:after:w-px sm:after:bg-white/15"
                      : ""
                  }

                  ${
                    index === 2
                      ? "sm:after:absolute sm:after:bottom-3 sm:after:right-0 sm:after:top-3 sm:after:w-px sm:after:bg-white/15"
                      : ""
                  }

                  ${
                    index < 3
                      ? "lg:after:absolute lg:after:bottom-3 lg:after:right-0 lg:after:top-3 lg:after:w-px lg:after:bg-white/15"
                      : ""
                  }

                  ${
                    index === 1
                      ? "sm:max-lg:after:hidden"
                      : ""
                  }
                `}
              >
                <div
                  className="
                    grid h-11 w-11 shrink-0 place-items-center
                    rounded-[14px]
                    bg-[linear-gradient(135deg,rgba(111,255,233,0.16),rgba(139,92,246,0.12)),rgba(255,255,255,0.07)]
                    text-[#6fffe9]
                    shadow-[inset_0_1px_0_rgba(255,255,255,0.16),0_10px_24px_rgba(0,0,0,0.18)]

                    sm:h-12 sm:w-12
                    lg:h-11 lg:w-11
                    xl:h-[50px] xl:w-[50px]
                    2xl:h-[58px] 2xl:w-[58px]
                    2xl:rounded-[18px]
                  "
                >
                  <Icon
                    strokeWidth={2.2}
                    className={`
                      h-[22px] w-[22px]
                      transition-transform duration-300

                      sm:h-6 sm:w-6
                      lg:h-[22px] lg:w-[22px]
                      xl:h-6 xl:w-6
                      2xl:h-[30px] 2xl:w-[30px]

                      ${animation}
                    `}
                  />
                </div>

                <div className="min-w-0">
                  <div
                    className="
                      whitespace-nowrap text-[26px] font-extrabold
                      leading-none tracking-[-0.04em] text-white

                      sm:text-[28px]
                      lg:text-[27px]
                      xl:text-[31px]
                      2xl:text-[clamp(30px,3vw,46px)]
                    "
                  >
                    {value}
                  </div>

                  <div
                    className="
                      mt-1.5 max-w-[150px]
                      text-[12px] font-semibold leading-[1.3]
                      text-white/80

                      sm:text-[13px]
                      lg:max-w-[132px]
                      lg:text-[12px]

                      xl:max-w-[160px]
                      xl:text-[13px]

                      2xl:mt-2
                      2xl:text-[15px]
                    "
                  >
                    {label}
                  </div>
                </div>
              </div>
            )
          )}
        </div>
      </div>
    </section>
  );
}