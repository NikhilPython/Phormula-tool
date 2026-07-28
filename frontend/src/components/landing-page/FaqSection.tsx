import FaqAccordion from "@/components/landing-page/FaqAccordion";
import { containerClass, Eyebrow } from "./shared";

export default function FaqSection() {
  return (
    <section
      id="faq"
      className="
        relative overflow-hidden
        bg-[radial-gradient(circle_at_12%_12%,rgba(94,166,142,0.10),transparent_30%),linear-gradient(180deg,#f8f4eb_0%,#f5eee2_100%)]

        py-[72px]
        lg:py-20
        xl:py-[88px]
        2xl:py-24
      "
    >
      <div className={`${containerClass} relative z-[1] text-center`}>
        <div className="mx-auto max-w-[780px] 2xl:max-w-[850px]">
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
              FAQ
            </div>

          <h2
            className="
              mx-auto mt-[18px]
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
            Questions founders ask{" "}
            <span className="text-[#40577d]">
              before switching.
            </span>
          </h2>

          <p
            className="
              mx-auto mt-4 max-w-[590px]
              text-[14px] leading-[1.7]
              text-[#68748a]

              sm:text-[15px]
              2xl:mt-[18px]
              2xl:max-w-[620px]
              2xl:leading-[1.75]
            "
          >
            Everything you need to know about connecting your Amazon business,
            understanding your numbers, and getting started with Phormula.
          </p>
        </div>

        <div
          className="
            mx-auto mt-9 max-w-[850px]
            text-left

            md:mt-10
            xl:mt-11

            2xl:mt-[52px]
            2xl:max-w-[920px]
          "
        >
          <FaqAccordion />
        </div>
      </div>
    </section>
  );
}