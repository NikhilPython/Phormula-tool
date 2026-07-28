"use client";

import { useState } from "react";

const faqs = [
  {
    question: "What exactly does Phormula replace?",
    answer:
      "Phormula replaces manual spreadsheet reporting, disconnected financial files, repeated data follow-ups, and fragmented Amazon business analysis with one centralized workspace.",
  },
  {
    question: "How does Phormula connect with my Amazon business?",
    answer:
      "Phormula securely connects with your Amazon seller data and organizes sales, fees, advertising, inventory, expenses, and profitability into structured dashboards.",
  },
  {
    question: "Can I see SKU-level profitability?",
    answer:
      "Yes. Phormula provides SKU-level visibility across sales, costs, advertising spend, fees, CM1, CM2, and inventory performance.",
  },
  {
    question: "Does Phormula provide inventory forecasting?",
    answer:
      "Yes. The Growth Plan includes inventory forecasting, dispatch planning, purchase-order recommendations, and alerts based on your current stock position and expected demand.",
  },
  {
    question: "How are the AI recommendations generated?",
    answer:
      "Phormula analyzes your financial, advertising, inventory, and operational data to identify changes, explain why they matter, and recommend the next action based on your business objectives.",
  },
  {
    question: "Can I cancel my subscription anytime?",
    answer:
      "Yes. Both plans are billed monthly, with no setup fee, and you can cancel your subscription at any time.",
  },
];

export default function FaqAccordion() {
 const [openIndex, setOpenIndex] = useState<number | null>(0);

const handleToggle = (index: number) => {
  setOpenIndex((currentIndex) =>
    currentIndex === index ? null : index
  );
};

  return (
    <div className="grid gap-3 2xl:gap-4">
      {faqs.map((faq, index) => {
        const isOpen = openIndex === index;

        return (
          <article
            key={faq.question}
            className={`
              group overflow-hidden
              rounded-[16px]
              border
              bg-white/75
              shadow-[0_10px_30px_rgba(55,69,95,0.06)]
              backdrop-blur-xl
              transition-all duration-300

              sm:rounded-[18px]

              2xl:rounded-[20px]

              ${
                isOpen
                  ? "border-[#5EA68E]/30 bg-white shadow-[0_18px_42px_rgba(55,69,95,0.10)]"
                  : "border-[#37455F]/[0.08] hover:border-[#5EA68E]/20 hover:bg-white/90"
              }
            `}
          >
            <button
              type="button"
              onClick={() => handleToggle(index)}
              aria-expanded={isOpen}
              className="
                flex w-full items-center justify-between
                gap-5 px-5 py-[18px]
                text-left

                sm:px-6
                sm:py-5

                2xl:px-7
                2xl:py-[22px]
              "
            >
              <div className="flex min-w-0 items-center gap-3.5 sm:gap-4">
                <span
                  className={`
                    grid h-8 w-8 shrink-0
                    place-items-center
                    rounded-full
                    text-[11px] font-black
                    transition-all duration-300

                    2xl:h-9
                    2xl:w-9
                    2xl:text-[12px]

                    ${
                      isOpen
                        ? "bg-[#5EA68E] text-white shadow-[0_8px_20px_rgba(94,166,142,0.28)]"
                        : "bg-[#5EA68E]/10 text-[#4A8A74]"
                    }
                  `}
                >
                  {String(index + 1).padStart(2, "0")}
                </span>

                <h3
                  className={`
                    text-[15px] font-bold
                    leading-[1.4]
                    tracking-[-0.015em]

                    sm:text-[16px]
                    xl:text-[17px]

                    2xl:text-[18px]

                    ${
                      isOpen
                        ? "text-[#2d3d59]"
                        : "text-[#46536a]"
                    }
                  `}
                >
                  {faq.question}
                </h3>
              </div>

              <span
                className={`
                  relative grid h-8 w-8 shrink-0
                  place-items-center
                  rounded-full
                  border
                  transition-all duration-300

                  2xl:h-9
                  2xl:w-9

                  ${
                    isOpen
                      ? "rotate-45 border-[#5EA68E]/20 bg-[#5EA68E]/10 text-[#4A8A74]"
                      : "border-[#37455F]/10 bg-white text-[#667288] group-hover:border-[#5EA68E]/20"
                  }
                `}
              >
                <svg
                  className="h-4 w-4"
                  viewBox="0 0 24 24"
                  fill="none"
                  aria-hidden="true"
                >
                  <path
                    d="M12 5v14M5 12h14"
                    stroke="currentColor"
                    strokeWidth="1.8"
                    strokeLinecap="round"
                  />
                </svg>
              </span>
            </button>

            <div
              className={`
                grid transition-all duration-300 ease-out

                ${
                  isOpen
                    ? "grid-rows-[1fr] opacity-100"
                    : "grid-rows-[0fr] opacity-0"
                }
              `}
            >
              <div className="overflow-hidden">
                <div
                  className="
                    ml-[66px] mr-5
                    border-t border-[#37455F]/[0.07]
                    pb-5 pt-4

                    sm:ml-[80px]
                    sm:mr-6

                    2xl:ml-[88px]
                    2xl:mr-7
                    2xl:pb-6
                    2xl:pt-[18px]
                  "
                >
                  <p
                    className="
                      max-w-[700px]
                      text-[13px]
                      leading-[1.7]
                      text-[#667288]

                      sm:text-[14px]

                      2xl:text-[15px]
                      2xl:leading-[1.75]
                    "
                  >
                    {faq.answer}
                  </p>
                </div>
              </div>
            </div>
          </article>
        );
      })}
    </div>
  );
}