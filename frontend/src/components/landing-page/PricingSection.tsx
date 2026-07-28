import Link from "next/link";
import {
  containerClass,
  Eyebrow,
  SectionCopy,
} from "./shared";

const reporting = [
  "Reporting dashboard",
  "Complete P&L dashboard",
  "Sales, fees, and performance tracking",
  "Amazon expense reconciliation",
  "SKU-level financial visibility",
  "Clean monthly reporting view",
];

const growth = [
  "AI business insights",
  "Inventory forecasting",
  "Dispatch and purchase-order reports",
  "Cash-flow visibility",
  "CM2-level profit analysis",
  "Objective-based recommendations",
  "SKU-level growth and risk actions",
];

function PlanCard({ growthPlan = false }: { growthPlan?: boolean }) {
  const list = growthPlan ? growth : reporting;

  return (
    <article
      className={`
        group relative flex h-full flex-col overflow-hidden
        rounded-[20px] p-5 text-left
        transition-all duration-500

        sm:p-6
        lg:rounded-[22px]
        lg:p-6
        xl:p-7
        2xl:rounded-[28px]
        2xl:p-[38px]

        ${
          growthPlan
            ? `
              border border-white/15
              bg-[radial-gradient(circle_at_90%_8%,rgba(94,166,142,0.2),transparent_28%),linear-gradient(145deg,#3c4b68_0%,#2f3c56_55%,#29354d_100%)]
              text-white
              shadow-[0_24px_60px_rgba(38,50,73,0.24),inset_0_1px_0_rgba(255,255,255,0.11)]

              md:-translate-y-1.5
              hover:-translate-y-3

              2xl:-translate-y-3
              2xl:shadow-[0_34px_85px_rgba(38,50,73,0.27),inset_0_1px_0_rgba(255,255,255,0.11)]
              2xl:hover:-translate-y-5
            `
            : `
              border border-[#37455F]/10
              bg-[linear-gradient(145deg,rgba(255,255,255,0.94),rgba(255,255,255,0.7))]
              shadow-[0_18px_45px_rgba(55,69,95,0.08),inset_0_1px_0_rgba(255,255,255,0.9)]
              backdrop-blur-[14px]

              hover:-translate-y-2
              hover:border-[#5EA68E]/30
              hover:shadow-[0_26px_60px_rgba(55,69,95,0.12),inset_0_1px_0_rgba(255,255,255,0.9)]
            `
        }
      `}
    >
      {growthPlan && (
        <div
          className="
            absolute right-3 top-3 z-[5]
            rounded-full border border-white/25
            bg-[#FDD36F]
            px-2.5 py-1.5
            text-[9px] font-black uppercase
            tracking-[0.08em] text-[#37455F]
            shadow-[0_8px_20px_rgba(13,20,34,0.18)]

            sm:right-4
            sm:top-4
            sm:px-3

            2xl:right-[18px]
            2xl:top-[17px]
            2xl:px-[13px]
            2xl:py-2
            2xl:text-[0.65rem]
          "
        >
          ● Most popular
        </div>
      )}

      <div className="flex items-start justify-between gap-4">
        <div className={growthPlan ? "max-w-[70%]" : ""}>
          <div
            className={`
              text-[10px] font-black uppercase
              tracking-[0.12em]

              2xl:text-[0.71rem]

              ${growthPlan ? "text-[#FDD36F]" : "text-[#4A8A74]"}
            `}
          >
            {growthPlan ? "Growth Plan" : "Reporting Plan"}
          </div>

          <h3
            className={`
              mt-1.5
              font-[var(--font-dm-serif)]
              text-[21px] leading-[1.15]
              tracking-[-0.025em]

              xl:text-[22px]
              2xl:mt-2
              2xl:text-[1.55rem]

              ${growthPlan ? "text-white" : "text-[#37455F]"}
            `}
          >
            {growthPlan ? "Growth intelligence" : "Financial visibility"}
          </h3>
        </div>

        <div
          className={`
            grid h-10 w-10 shrink-0 place-items-center
            rounded-[12px] border
            text-[18px]
            transition-all duration-300

            group-hover:-translate-y-1
            group-hover:-rotate-3
            group-hover:scale-105

            xl:h-11 xl:w-11
            2xl:h-[50px]
            2xl:w-[50px]
            2xl:rounded-[15px]

            ${
              growthPlan
                ? "border-white/10 bg-white/[0.08] text-[#FDD36F]"
                : "border-[#5EA68E]/15 bg-[#5EA68E]/10 text-[#4A8A74]"
            }
          `}
        >
          ↗
        </div>
      </div>

      <p
        className={`
          mt-4
          min-h-[62px]
          text-[13px]
          leading-[1.6]

          xl:text-[13.5px]
          2xl:mt-[22px]
          2xl:min-h-[72px]
          2xl:text-[0.91rem]
          2xl:leading-[1.65]

          ${growthPlan ? "text-white/65" : "text-[#5A6272]"}
        `}
      >
        {growthPlan
          ? "Full financial intelligence for founders who want to forecast, protect profitability, and make faster decisions."
          : "Essential reporting for sellers who want clean, reliable numbers without spreadsheet dependency."}
      </p>

      <div className="mt-4 flex items-end gap-1 2xl:mt-[25px]">
        <span
          className={`
            mb-1.5 text-[18px] font-black
            2xl:mb-[9px]
            2xl:text-2xl

            ${growthPlan ? "text-white" : "text-[#37455F]"}
          `}
        >
          $
        </span>

        <span
          className={`
            font-[var(--font-dm-serif)]
            text-[48px]
            leading-[0.88]
            tracking-[-0.06em]

            sm:text-[54px]
            xl:text-[58px]
            2xl:text-[clamp(3.8rem,6vw,4.8rem)]

            ${growthPlan ? "text-white" : "text-[#37455F]"}
          `}
        >
          {growthPlan ? "100" : "50"}
        </span>

        <div className="mb-0.5 flex flex-col">
          <span
            className={`
              text-[12px] font-extrabold
              2xl:text-[0.86rem]

              ${growthPlan ? "text-white/65" : "text-[#5A6272]"}
            `}
          >
            /month
          </span>

          <span
            className={`
              text-[10px] font-bold
              2xl:text-[0.68rem]

              ${growthPlan ? "text-white/40" : "text-[#8A95A3]"}
            `}
          >
            Billed monthly
          </span>
        </div>
      </div>

      <div
        className={`
          my-5 h-px w-full
          2xl:my-[27px]

          ${
            growthPlan
              ? "bg-gradient-to-r from-white/15 to-white/[0.03]"
              : "bg-gradient-to-r from-[#37455F]/15 to-[#37455F]/[0.03]"
          }
        `}
      />

      <div
        className={`
          text-[11px] font-black
          2xl:text-[0.78rem]

          ${growthPlan ? "text-white/85" : "text-[#37455F]"}
        `}
      >
        {growthPlan
          ? "Everything in Reporting, plus"
          : "Everything you need to report"}
      </div>

      <ul
        className="
          my-4 mb-6
          grid list-none gap-2.5

          xl:gap-3
          2xl:my-[19px]
          2xl:mb-[30px]
          2xl:gap-[13px]
        "
      >
        {list.map((item) => (
          <li
            key={item}
            className={`
              flex items-start gap-2.5
              text-[12.5px] font-bold
              leading-[1.45]

              xl:text-[13px]
              2xl:gap-[11px]
              2xl:text-[0.88rem]
              2xl:leading-[1.48]

              ${growthPlan ? "text-white/80" : "text-[#273140]"}
            `}
          >
            <span
              className={`
                mt-px grid h-[19px] w-[19px]
                shrink-0 place-items-center
                rounded-full text-[10px]

                2xl:h-[21px]
                2xl:w-[21px]
                2xl:text-[0.7rem]

                ${
                  growthPlan
                    ? "bg-[#5EA68E]/25 text-[#FDD36F]"
                    : "bg-[#5EA68E]/15 text-[#4A8A74]"
                }
              `}
            >
              ✓
            </span>

            <span>{item}</span>
          </li>
        ))}
      </ul>

      <Link
        href="/signin"
        className={`
          mt-auto inline-flex min-h-[46px] w-full
          items-center justify-center gap-2
          rounded-[12px]
          text-[12px] font-extrabold
          transition-all duration-300

          hover:-translate-y-0.5

          2xl:min-h-[50px]
          2xl:rounded-[13px]
          2xl:text-[0.86rem]

          ${
            growthPlan
              ? "bg-[linear-gradient(135deg,#5EA68E,#4A8A74)] text-white shadow-[0_14px_32px_rgba(20,45,39,0.28)] hover:shadow-[0_18px_38px_rgba(20,45,39,0.34)]"
              : "border-[1.5px] border-[#37455F]/15 bg-white/80 text-[#37455F] shadow-[0_8px_20px_rgba(55,69,95,0.06)] hover:bg-[#37455F] hover:text-white"
          }
        `}
      >
        {growthPlan ? "Choose Growth" : "Start with Reporting"}
        <span className="transition-transform duration-300 group-hover:translate-x-1">
          →
        </span>
      </Link>

      <p
        className={`
          mt-3 min-h-8
          text-center text-[10.5px]
          leading-[1.45]

          2xl:mt-[15px]
          2xl:min-h-9
          2xl:text-[0.72rem]
          2xl:leading-[1.5]

          ${growthPlan ? "text-white/40" : "text-[#8A95A3]"}
        `}
      >
        {growthPlan
          ? "Best for founders actively scaling their Amazon business."
          : "Best for sellers building a reliable reporting foundation."}
      </p>
    </article>
  );
}

export default function PricingSection() {
  return (
    <section
      id="pricing"
      className="
        relative overflow-hidden
        bg-[radial-gradient(circle_at_10%_10%,rgba(94,166,142,0.13),transparent_30%),radial-gradient(circle_at_88%_15%,rgba(253,211,111,0.16),transparent_28%),linear-gradient(180deg,#f7f1e6_0%,#f2eadc_100%)]

        py-[72px]
        lg:py-20
        xl:py-[88px]
        2xl:py-24
      "
    >
      <div className={`${containerClass} relative z-[2] text-center`}>
        <div className="mx-auto max-w-[780px] 2xl:max-w-[830px]">
          <div
              className="
                inline-flex items-center gap-[7px]
                rounded-full border border-[#269770]/20
                bg-[#ebf7f1]/80
                px-[13px] py-[7px]
                text-[10px] font-extrabold uppercase
                leading-none tracking-[0.12em]
                text-[#277d64]
              "
            >
              <span className="h-1.5 w-1.5 rounded-full bg-[#36a47d] shadow-[0_0_0_4px_rgba(54,164,125,0.12)]" />
              Pricing
            </div>

          <h2
            className="
              mx-auto mt-[18px]
              whitespace-nowrap
              font-[var(--font-dm-serif)]
              text-[34px]
              leading-[1.08]
              tracking-[-0.035em]
              text-[#263653]

              sm:text-[38px]
              lg:text-[42px]
              xl:text-[42px]

              2xl:text-[54px]

              max-lg:whitespace-normal
            "
          >
            Simple pricing. Serious business clarity.
          </h2>

          <SectionCopy>
            Start with reliable financial reporting, then upgrade when you need
            forecasting, cash-flow intelligence, and AI-powered recommendations.
          </SectionCopy>

          <div className="mt-5 flex flex-wrap justify-center gap-2 2xl:mt-6 2xl:gap-2.5">
            {[
              "No setup fee",
              "Cancel anytime",
              "Secure Amazon connection",
            ].map((item) => (
              <span
                key={item}
                className="
                  inline-flex items-center gap-1.5
                  rounded-full
                  border border-[#37455F]/10
                  bg-white/55
                  px-3 py-1.5
                  text-[11px] font-bold
                  text-[#5A6272]
                  backdrop-blur-lg

                  2xl:gap-[7px]
                  2xl:px-[13px]
                  2xl:py-2
                  2xl:text-[0.78rem]
                "
              >
                <b
                  className="
                    grid h-4 w-4
                    place-items-center rounded-full
                    bg-[#5EA68E]/15
                    text-[9px] text-[#4A8A74]

                    2xl:h-[17px]
                    2xl:w-[17px]
                    2xl:text-[0.65rem]
                  "
                >
                  ✓
                </b>
                {item}
              </span>
            ))}
          </div>
        </div>

        <div
          className="
            mx-auto mt-10
            grid max-w-[900px]
            grid-cols-2 items-stretch
            gap-5

            max-md:max-w-[520px]
            max-md:grid-cols-1
            max-md:gap-6

            lg:mt-11
            xl:mt-12
            xl:grid-cols-[repeat(2,minmax(0,430px))]
            xl:justify-center
            xl:gap-6

            2xl:mt-[62px]
            2xl:grid-cols-[repeat(2,minmax(0,460px))]
            2xl:gap-7
          "
        >
          <PlanCard />
          <PlanCard growthPlan />
        </div>

        <div
          className="
            mx-auto mt-8
            flex w-fit max-w-[700px]
            items-center gap-2.5
            rounded-[14px]
            border border-[#37455F]/[0.08]
            bg-white/50
            px-4 py-3
            shadow-[0_12px_34px_rgba(55,69,95,0.05)]
            backdrop-blur-lg

            max-sm:items-start

            2xl:mt-[38px]
            2xl:max-w-[730px]
            2xl:gap-[11px]
            2xl:px-[19px]
            2xl:py-[13px]
          "
        >
          <span
            className="
              grid h-6 w-6 shrink-0
              place-items-center rounded-full
              bg-white text-[11px] text-[#4A8A74]
              shadow-[0_5px_16px_rgba(55,69,95,0.1)]

              2xl:h-[27px]
              2xl:w-[27px]
            "
          >
            ✓
          </span>

          <p
            className="
              text-left
              text-[11px] font-semibold
              leading-[1.5] text-[#5A6272]

              2xl:text-[0.78rem]
              2xl:leading-[1.55]
            "
          >
            Both plans include secure data handling, onboarding support, and
            access to your historical business data.
          </p>
        </div>
      </div>
    </section>
  );
}