import React from "react";
import {
  containerClass,
  Eyebrow,
  SectionCopy,
} from "./shared";

const steps = [
  {
    title: "Connect your business data",
    text: "Bring sales, fees, ads, inventory, expenses, purchase orders, and cash-flow inputs into one system.",
    accent: "#5EA68E",
    soft: "rgba(94,166,142,0.13)",
    delay: "0s",
  },
  {
    title: "Reconcile and visualize",
    text: "Phormula turns scattered inputs into dashboards for revenue, CM2 profit, cash flow, inventory, and performance.",
    accent: "#6175A6",
    soft: "rgba(97,117,166,0.13)",
    delay: "0.3s",
  },
  {
    title: "Act on AI recommendations",
    text: "Get clear alerts and next steps when inventory, fees, margin, or cash flow needs attention.",
    accent: "#C5984D",
    soft: "rgba(197,152,77,0.13)",
    delay: "0.6s",
  },
] as const;

function StepArrow() {
  return (
    <div
      className="
        relative flex items-center justify-center
        max-md:h-10
      "
    >
      {/* Desktop line */}
      <span
        className="
          absolute left-0 right-0 top-1/2
          h-px -translate-y-1/2
          bg-[linear-gradient(90deg,transparent,rgba(55,69,95,0.2),transparent)]

          max-md:bottom-0
          max-md:left-1/2
          max-md:right-auto
          max-md:top-0
          max-md:h-auto
          max-md:w-px
          max-md:-translate-x-1/2
          max-md:translate-y-0
          max-md:bg-[linear-gradient(180deg,transparent,rgba(55,69,95,0.2),transparent)]
        "
      />

      <span
        className="
          workflow-arrow relative z-[2]
          grid h-9 w-9 place-items-center
          rounded-full
          border border-[#37455F]/10
          bg-white text-[#53617a]
          shadow-[0_7px_20px_rgba(55,69,95,0.12)]

          max-md:rotate-90
        "
      >
        <svg
          className="h-[17px] w-[17px]"
          viewBox="0 0 24 24"
          fill="none"
        >
          <path
            d="M5 12h13M14 8l4 4-4 4"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </span>
    </div>
  );
}

export default function WorkflowSection() {
  return (
    <section
      id="workflow"
      className="
        relative overflow-hidden
        bg-[radial-gradient(circle_at_15%_10%,rgba(94,166,142,0.12),transparent_32%),radial-gradient(circle_at_88%_75%,rgba(97,117,166,0.10),transparent_30%),#F5EFE0]
        py-[72px]

        lg:py-20
        xl:py-[88px]
        2xl:py-24

        before:pointer-events-none
        before:absolute
        before:-right-32
        before:top-14
        before:h-[310px]
        before:w-[310px]
        before:rounded-full
        before:border
        before:border-[#5EA68E]/10
        before:content-['']

        after:pointer-events-none
        after:absolute
        after:-bottom-36
        after:-left-28
        after:h-[280px]
        after:w-[280px]
        after:rounded-full
        after:border
        after:border-[#6175A6]/10
        after:content-['']
      "
    >
      <div className={`${containerClass} relative z-[1] text-center`}>
        <div className="mx-auto max-w-[780px]">
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
              How it Works
            </div>

          <h2
            className="
              mt-[18px]
              font-[var(--font-dm-serif)]
              text-[34px]
              leading-[1.08]
              tracking-[-0.035em]
              text-[#263653]

              sm:text-[38px]
              lg:text-[42px]
              xl:text-[42px]

              2xl:text-[54px]
            "
          >
            From raw data to{" "}
            <span className="text-[#40577d]">
              founder-ready decisions.
            </span>
          </h2>

          <SectionCopy>
            Phormula helps teams move from manual reporting to an operating
            rhythm built around clean numbers and clear actions.
          </SectionCopy>
        </div>

        <div
          className="
            mt-10 grid items-stretch
            grid-cols-[minmax(0,1fr)_48px_minmax(0,1fr)_48px_minmax(0,1fr)]
            text-left

            max-md:flex
            max-md:flex-col
            max-md:gap-0

            lg:mt-11
            xl:mt-12

            2xl:mt-[52px]
            2xl:grid-cols-[minmax(0,1fr)_58px_minmax(0,1fr)_58px_minmax(0,1fr)]
          "
        >
          {steps.map((step, index) => (
            <React.Fragment key={step.title}>
              <article
                className="
                  group relative
                  flex min-h-[240px] flex-col
                  overflow-hidden
                  rounded-[20px]
                  border border-[#37455F]/[0.08]
                  bg-white/90
                  px-5 py-6
                  shadow-[0_10px_30px_rgba(55,69,95,0.08),0_2px_7px_rgba(55,69,95,0.03)]
                  backdrop-blur-xl
                  transition-all duration-500 ease-out

                  hover:-translate-y-2
                  hover:border-[#37455F]/15
                  hover:shadow-[0_24px_55px_rgba(55,69,95,0.14),0_4px_12px_rgba(55,69,95,0.04)]

                  sm:px-6

                  lg:min-h-[248px]
                  lg:px-5
                  lg:py-6

                  xl:min-h-[258px]
                  xl:px-6
                  xl:py-7

                  2xl:min-h-[280px]
                  2xl:rounded-[26px]
                  2xl:p-[30px]
                "
              >
                {/* Top accent */}
                <span
                  className="
                    absolute left-5 right-5 top-0
                    h-[3px] origin-left
                    scale-x-[0.24] rounded-b-full
                    transition-transform duration-500
                    group-hover:scale-x-100

                    2xl:left-[30px]
                    2xl:right-[30px]
                  "
                  style={{ backgroundColor: step.accent }}
                />

                {/* Decorative glow */}
                <span
                  className="
                    pointer-events-none absolute
                    -right-14 -top-14
                    h-36 w-36 rounded-full
                    opacity-70 blur-[1px]
                    transition-all duration-500

                    group-hover:-right-9
                    group-hover:-top-9
                    group-hover:scale-110
                  "
                  style={{ backgroundColor: step.soft }}
                />

                <div className="relative z-[1] flex items-start justify-between">
                  <div
                    className="
                      workflow-step-number
                      grid h-11 w-11
                      shrink-0 place-items-center
                      rounded-full
                      text-[14px] font-black text-white
                      shadow-[0_10px_24px_rgba(55,69,95,0.16)]

                      2xl:h-[48px]
                      2xl:w-[48px]
                      2xl:text-[15px]
                    "
                    style={{
                      backgroundColor: step.accent,
                      animationDelay: step.delay,
                    }}
                  >
                    {String(index + 1).padStart(2, "0")}
                  </div>

                  <span
                    className="
                      text-[11px] font-extrabold uppercase
                      tracking-[0.12em]
                      opacity-50
                    "
                    style={{ color: step.accent }}
                  >
                    Step {index + 1}
                  </span>
                </div>

                <div className="relative z-[1] mt-8 2xl:mt-9">
                  <h3
                    className="
                      max-w-[270px]
                      text-[17px] font-bold
                      leading-[1.35]
                      tracking-[-0.02em]
                      text-[#37455F]

                      xl:text-[18px]
                      2xl:text-[1.16rem]
                    "
                  >
                    {step.title}
                  </h3>

                  <span
                    className="
                      my-3 block h-0.5 w-8
                      rounded-full
                      transition-all duration-500
                      group-hover:w-14
                    "
                    style={{ backgroundColor: step.accent }}
                  />

                  <p
                    className="
                      text-[13px]
                      leading-[1.65]
                      text-[#5A6272]

                      xl:text-[14px]

                      2xl:text-[0.94rem]
                      2xl:leading-[1.7]
                    "
                  >
                    {step.text}
                  </p>
                </div>

                <div
                  className="
                    relative z-[1] mt-auto
                    flex items-center gap-2
                    pt-5
                    text-[11px] font-bold
                    uppercase tracking-[0.08em]
                    opacity-0
                    transition-all duration-300
                    group-hover:translate-x-1
                    group-hover:opacity-100
                  "
                  style={{ color: step.accent }}
                >
                  Continue
                  <svg
                    className="h-4 w-4"
                    viewBox="0 0 24 24"
                    fill="none"
                  >
                    <path
                      d="M5 12h13M14 8l4 4-4 4"
                      stroke="currentColor"
                      strokeWidth="1.8"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                </div>
              </article>

              {index < steps.length - 1 && <StepArrow />}
            </React.Fragment>
          ))}
        </div>

        <div
          className="
            mx-auto mt-8
            flex w-fit max-w-[650px]
            items-center justify-center gap-3
            rounded-full
            border border-[#37455F]/[0.08]
            bg-white/55
            px-5 py-3
            shadow-[0_10px_30px_rgba(55,69,95,0.06)]
            backdrop-blur-lg

            max-sm:rounded-[16px]
            max-sm:text-left

            2xl:mt-10
          "
        >
          <span className="relative flex h-2.5 w-2.5 shrink-0">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-[#5EA68E] opacity-40" />
            <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-[#5EA68E]" />
          </span>

          <p className="text-[12px] font-semibold leading-5 text-[#4e5c72]">
            One connected workflow from business data to confident action.
          </p>
        </div>
      </div>
    </section>
  );
}