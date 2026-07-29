"use client";

import { FormEvent, useState } from "react";
import { containerClass, Eyebrow } from "./shared";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const inputClass = `
  w-full rounded-[10px] border border-[#37455F]/10 bg-[#FAFAF7]
  px-4 py-3 text-[13px] text-[#273140] outline-none transition
  placeholder:text-[#8A95A3] focus:border-[#5EA68E]
  focus:ring-4 focus:ring-[#5EA68E]/10
`;

type FormStatus = {
  type: "idle" | "success" | "error";
  message: string;
};

export default function DemoSection() {
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [status, setStatus] = useState<FormStatus>({
    type: "idle",
    message: "",
  });

  const benefits = [
    "See true profit beyond revenue",
    "Forecast inventory and cash needs",
    "Get AI recommendations for better decisions",
  ];

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSubmitting(true);
    setStatus({ type: "idle", message: "" });

    const form = event.currentTarget;
    const formData = new FormData(form);

    try {
      const response = await fetch(`${API_BASE_URL}/demo-request`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          full_name: formData.get("full_name"),
          work_email: formData.get("work_email"),
          company_name: formData.get("company_name"),
          monthly_revenue: formData.get("monthly_revenue"),
          manual_tracking: formData.get("manual_tracking"),
          website: formData.get("website"),
        }),
      });

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(result.error || "Unable to submit your request.");
      }

      form.reset();
      setStatus({
        type: "success",
        message: result.message || "Your demo request has been sent.",
      });
    } catch (error) {
      setStatus({
        type: "error",
        message:
          error instanceof Error
            ? error.message
            : "Unable to submit your request.",
      });
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <section id="demo" className="bg-[#37455F] py-16 sm:py-20 lg:py-24">
      <div className={containerClass}>
        <div className="grid grid-cols-[0.9fr_1.1fr] items-center gap-12 overflow-hidden rounded-[28px] border border-white/[0.08] bg-white/6 px-10 py-8 xl:px-12 xl:py-10 max-lg:grid-cols-1 max-lg:gap-9 max-sm:rounded-[22px] max-sm:px-5 max-sm:py-7">
          <div className="max-w-[520px]">
            <Eyebrow>Get started</Eyebrow>
            <h2 className="mt-4 font-[var(--font-dm-serif)] text-[clamp(2.1rem,3.5vw,3.35rem)] leading-[1.02] tracking-[-0.04em] text-white">
              Replace spreadsheet
              <br className="hidden xl:block" /> chaos with financial
              <br className="hidden xl:block" /> clarity.
            </h2>
            <p className="mt-4 max-w-[500px] text-[0.92rem] leading-[1.65] text-white/70">
              Tell us about your business and see how Phormula can help you
              track, reconcile, forecast, and grow from one financial
              intelligence dashboard.
            </p>
            <div className="mt-6 grid gap-3">
              {benefits.map((item) => (
                <div key={item} className="flex items-center gap-2.5 text-[0.86rem] font-bold text-white/90">
                  <span className="grid h-[20px] w-[20px] shrink-0 place-items-center rounded-full bg-[#E5B94B]/15 text-[11px] text-[#E5B94B]">
                    ✓
                  </span>
                  <span>{item}</span>
                </div>
              ))}
            </div>
          </div>

          <form
            className="grid grid-cols-2 gap-3 rounded-[18px] bg-white p-5 shadow-[0_18px_50px_rgba(16,24,40,0.18)] max-sm:grid-cols-1 max-sm:p-4"
            onSubmit={handleSubmit}
          >
            <input className={inputClass} name="full_name" type="text" placeholder="Full name" maxLength={120} required />
            <input className={inputClass} name="work_email" type="email" placeholder="Work email" maxLength={254} required />
            <input className={inputClass} name="company_name" type="text" placeholder="Company name" maxLength={160} />
            <select className={inputClass} name="monthly_revenue" defaultValue="">
              <option value="" disabled>Monthly revenue range</option>
              <option value="Under $50K">Under $50K</option>
              <option value="$50K-$250K">$50K-$250K</option>
              <option value="$250K-$1M">$250K-$1M</option>
              <option value="$1M+">$1M+</option>
            </select>
            <textarea
              className={`${inputClass} col-span-2 min-h-[95px] resize-none max-sm:col-span-1`}
              name="manual_tracking"
              placeholder="What are you currently tracking manually?"
              maxLength={2000}
            />

            <input
              className="hidden"
              name="website"
              type="text"
              tabIndex={-1}
              autoComplete="off"
              aria-hidden="true"
            />

            <button
              className="col-span-2 inline-flex min-h-[46px] items-center justify-center rounded-[9px] bg-[#37455F] px-5 py-3 text-[13px] font-extrabold text-white shadow-[0_8px_22px_rgba(55,69,95,0.2)] transition hover:-translate-y-0.5 hover:bg-[#273248] disabled:cursor-not-allowed disabled:opacity-60 max-sm:col-span-1"
              type="submit"
              disabled={isSubmitting}
            >
              {isSubmitting ? "Sending..." : "Request demo"}
            </button>

            {status.type !== "idle" && (
              <p
                className={`col-span-2 text-center text-[12px] leading-5 max-sm:col-span-1 ${
                  status.type === "success" ? "text-[#3F806C]" : "text-red-600"
                }`}
                role="status"
                aria-live="polite"
              >
                {status.message}
              </p>
            )}

            <p className="col-span-2 text-center text-[10px] leading-4 text-[#8A95A3] max-sm:col-span-1">
              Our team will contact you using the work email provided.
            </p>
          </form>
        </div>
      </div>
    </section>
  );
}
