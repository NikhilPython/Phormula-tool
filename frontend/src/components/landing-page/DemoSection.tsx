import { containerClass, Eyebrow } from "./shared";

const inputClass = `
  w-full
  rounded-[10px]
  border border-[#37455F]/10
  bg-[#FAFAF7]
  px-4 py-3
  text-[13px]
  text-[#273140]
  outline-none
  transition
  placeholder:text-[#8A95A3]
  focus:border-[#5EA68E]
  focus:ring-4
  focus:ring-[#5EA68E]/10
`;

export default function DemoSection() {
  const benefits = [
    "See true profit beyond revenue",
    "Forecast inventory and cash needs",
    "Get AI recommendations for better decisions",
  ];

  return (
    <section
      id="demo"
      className="bg-[#37455F] py-16 sm:py-20 lg:py-24"
    >
      <div className={containerClass}>
        <div
          className="
            grid grid-cols-[0.9fr_1.1fr]
            items-center gap-12
            overflow-hidden
            rounded-[28px]
            border border-white/[0.08]
            bg-white/6
            px-10 py-8

            xl:px-12 xl:py-10

            max-lg:grid-cols-1
            max-lg:gap-9

            max-sm:rounded-[22px]
            max-sm:px-5
            max-sm:py-7
          "
        >
          {/* Left content */}
          <div className="max-w-[520px]">
            <Eyebrow>Get started</Eyebrow>

            <h2
              className="
                mt-4
                font-[var(--font-dm-serif)]
                text-[clamp(2.1rem,3.5vw,3.35rem)]
                leading-[1.02]
                tracking-[-0.04em]
                text-white
              "
            >
              Replace spreadsheet
              <br className="hidden xl:block" /> chaos with financial
              <br className="hidden xl:block" /> clarity.
            </h2>

            <p
              className="
                mt-4
                max-w-[500px]
                text-[0.92rem]
                leading-[1.65]
                text-white/70
              "
            >
              Tell us about your business and see how Phormula can help you
              track, reconcile, forecast, and grow from one financial
              intelligence dashboard.
            </p>

            <div className="mt-6 grid gap-3">
              {benefits.map((item) => (
                <div
                  key={item}
                  className="
                    flex items-center gap-2.5
                    text-[0.86rem]
                    font-bold
                    text-white/90
                  "
                >
                  <span
                    className="
                      grid h-[20px] w-[20px]
                      shrink-0 place-items-center
                      rounded-full
                      bg-[#E5B94B]/15
                      text-[11px]
                      text-[#E5B94B]
                    "
                  >
                    ✓
                  </span>

                  <span>{item}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Form */}
          <form
            className="
              grid grid-cols-2 gap-3
              rounded-[18px]
              bg-white
              p-5
              shadow-[0_18px_50px_rgba(16,24,40,0.18)]

              max-sm:grid-cols-1
              max-sm:p-4
            "
            action="#"
          >
            <input
              className={inputClass}
              type="text"
              placeholder="Full name"
              required
            />

            <input
              className={inputClass}
              type="email"
              placeholder="Work email"
              required
            />

            <input
              className={inputClass}
              type="text"
              placeholder="Company name"
            />

            <select className={inputClass} defaultValue="">
              <option value="" disabled>
                Monthly revenue range
              </option>
              <option>Under $50K</option>
              <option>$50K–$250K</option>
              <option>$250K–$1M</option>
              <option>$1M+</option>
            </select>

            <textarea
              className={`
                ${inputClass}
                col-span-2
                min-h-[95px]
                resize-none
                max-sm:col-span-1
              `}
              placeholder="What are you currently tracking manually?"
            />

            <button
              className="
                col-span-2
                inline-flex min-h-[46px]
                items-center justify-center
                rounded-[9px]
                bg-[#37455F]
                px-5 py-3
                text-[13px]
                font-extrabold
                text-white
                shadow-[0_8px_22px_rgba(55,69,95,0.2)]
                transition
                hover:-translate-y-0.5
                hover:bg-[#273248]

                max-sm:col-span-1
              "
              type="submit"
            >
              Request demo
            </button>

            <p
              className="
                col-span-2
                text-center
                text-[10px]
                leading-4
                text-[#8A95A3]

                max-sm:col-span-1
              "
            >
              This form is ready for connection to your CRM, email tool, or
              backend form handler.
            </p>
          </form>
        </div>
      </div>
    </section>
  );
}