"use client";

import React from "react";

const faqs = [
  {
    question: "Is Phormula only for Big sellers?",
    answer:
      "Phormula is designed for Amazon and D2C operators who need clear visibility into revenue, fees, ads, inventory, cash flow, and contribution-margin-level profit.",
  },
  {
    question: "Does Phormula replace spreadsheets?",
    answer:
      "Yes. Phormula is built to replace manual spreadsheet tracking with one live dashboard that centralizes business-critical financial and operational data.",
  },
  {
    question: "What does the AI help with?",
    answer:
      "The AI helps identify what changed, why it matters, and which actions can improve profit, stock planning, cash flow, or advertising efficiency according to user's objective.",
  },
  {
    question: "Which plan should I choose?",
    answer:
      "Choose Reporting if you mainly need clean dashboards. Choose Growth if you want AI insights, inventory forecasting, cash-flow visibility, dispatch reports, and CM2-level analysis.",
  },
];

export default function FaqAccordion() {
  const [openFaqs, setOpenFaqs] = React.useState<number[]>(() =>
    faqs.map((_, index) => index)
  );

  const toggleFaq = (index: number) => {
    setOpenFaqs((prev) =>
      prev.includes(index)
        ? prev.filter((item) => item !== index)
        : [...prev, index]
    );
  };

  return (
    <div className="faq-grid">
      {faqs.map((faq, index) => {
        const isOpen = openFaqs.includes(index);

        return (
          <div className={`faq-item ${isOpen ? "active" : ""}`} key={faq.question}>
            <button
              type="button"
              className="faq-question"
              onClick={() => toggleFaq(index)}
              aria-expanded={isOpen}
            >
              <span>{faq.question}</span>
              <span className="faq-icon" aria-hidden="true">
                {isOpen ? "−" : "+"}
              </span>
            </button>

            <div className="faq-answer-wrap">
              <div className="faq-answer-inner">
                <p>{faq.answer}</p>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}