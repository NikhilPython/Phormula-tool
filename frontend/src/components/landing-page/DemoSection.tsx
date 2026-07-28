import { containerClass, Eyebrow } from "./shared";

const benefits = [
  "See true profit beyond revenue",
  "Forecast inventory and cash needs",
  "Get AI recommendations for better decisions",
];

const inputClass = `
  w-full rounded-[11px]
  border border-[#37455F]/15
  bg-[#fbfcfd]
  px-3.5 py-3
  text-[13px] text-[#273140]
  outline-none transition-all duration-300
  placeholder:text-[#8A95A3]

  hover:border-[#37455F]/25
  focus:border-[#5EA68E]
  focus:bg-white
  focus:ring-4 focus:ring-[#5EA68E]/10

  xl:px-4
  xl:py-3.5
  xl:text-[14px]

  2xl:rounded-xl
  2xl:text-[15px]
`;

export default function DemoSection() {
  return (
    <section
      id="demo"
      className="
        relative overflow-hidden bg-white

        py-[72px]
        lg:py-20
        xl:py-[88px]
        2xl:py-24

        before:pointer-events-none
        before:absolute
        before:-left-32
        before:top-16
        before:h-[300px]
        before:w-[300px]
        before:rounded-full
        before:bg-[#5EA68E]/10
        before:blur-[2px]
        before:content-['']

        after:pointer-events-none
        after:absolute
        after:-bottom-32
        after:-right-28
        after:h-[280px]
        after:w-[280px]
        after:rounded-full
        after:bg-[#FDD36F]/10
        after:content-['']
      "
    >
      <div className={`${containerClass} relative z-[1]`}>
        <div
          className="
            relative grid overflow-hidden
            rounded-[22px]
            border border-[#37455F]/[0.07]
            bg-[radial-gradient(circle_at_8%_8%,rgba(94,166,142,0.12),transparent_28%),linear-gradient(135deg,#f8f3e8_0%,#f4ecdf_100%)]
            p-5
            shadow-[0_22px_60px_rgba(55,69,95,0.10)]

            sm:p-7

            lg:grid-cols-[minmax(0,0.92fr)_minmax(420px,1.08fr)]
            lg:gap-8
            lg:p-8

            xl:gap-10
            xl:rounded-[26px]
            xl:p-10

            2xl:grid-cols-2
            2xl:gap-12
            2xl:rounded-[30px]
            2xl:p-12
            2xl:shadow-[0_28px_80px_rgba(55,69,95,0.12)]
          "
        >
          {/* Left side */}
          <div className="relative flex flex-col justify-center">
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
              Get Started
            </div>

            <h2
              className="
                mt-[18px]
                max-w-[600px]
                font-[var(--font-dm-serif)]
                text-[34px]
                leading-[1.08]
                tracking-[-0.035em]
                text-[#37455F]

                sm:text-[38px]
                lg:text-[42px]
                xl:text-[42px]

                2xl:text-[54px]
              "
            >
              Replace spreadsheet chaos with{" "}
              <span className="text-[#40577d]">
                financial clarity.
              </span>
            </h2>

            <p
              className="
                mt-4 max-w-[560px]
                text-[13px]
                leading-[1.7]
                text-[#5A6272]

                sm:text-[14px]
                xl:text-[15px]

                2xl:mt-[18px]
                2xl:text-[1.02rem]
                2xl:leading-[1.75]
              "
            >
              Tell us about your business and see how Phormula can help you
              track, reconcile, forecast, and grow from one financial
              intelligence dashboard.
            </p>

            <div
              className="
                mt-6 grid gap-2.5

                2xl:mt-7
                2xl:gap-3
              "
            >
              {benefits.map((item) => (
                <div
                  key={item}
                  className="
                    group flex items-center gap-2.5
                    text-[13px] font-bold
                    leading-[1.5]
                    text-[#37455F]

                    xl:text-[14px]
                  "
                >
                  <span
                    className="
                      grid h-5 w-5 shrink-0
                      place-items-center
                      rounded-full
                      bg-[#5EA68E]/15
                      text-[10px] text-[#4A8A74]
                      transition-all duration-300

                      group-hover:scale-110
                      group-hover:bg-[#5EA68E]
                      group-hover:text-white

                      2xl:h-[22px]
                      2xl:w-[22px]
                      2xl:text-xs
                    "
                  >
                    ✓
                  </span>

                  {item}
                </div>
              ))}
            </div>

            {/* Small visual cards */}
            <div
              className="
                mt-7 grid grid-cols-2 gap-3

                max-sm:grid-cols-1

                2xl:mt-9
              "
            >
              <div
                className="
                  rounded-[14px]
                  border border-[#5EA68E]/15
                  bg-white/65
                  p-3.5
                  shadow-[0_10px_28px_rgba(55,69,95,0.06)]
                  backdrop-blur-lg
                  transition duration-300
                  hover:-translate-y-1

                  2xl:p-4
                "
              >
                <div className="flex items-center justify-between">
                  <span className="text-[10px] font-extrabold uppercase tracking-[0.1em] text-[#4A8A74]">
                    Financial view
                  </span>

                  <span className="text-[#5EA68E]">↗</span>
                </div>

                <div className="mt-2 text-[18px] font-black text-[#37455F] 2xl:text-[21px]">
                  CM2 clarity
                </div>

                <p className="mt-1 text-[11px] leading-5 text-[#6e7889]">
                  Understand real profit after ads and operational costs.
                </p>
              </div>

              <div
                className="
                  rounded-[14px]
                  border border-[#6175A6]/15
                  bg-white/65
                  p-3.5
                  shadow-[0_10px_28px_rgba(55,69,95,0.06)]
                  backdrop-blur-lg
                  transition duration-300
                  hover:-translate-y-1

                  2xl:p-4
                "
              >
                <div className="flex items-center justify-between">
                  <span className="text-[10px] font-extrabold uppercase tracking-[0.1em] text-[#6175A6]">
                    Smart actions
                  </span>

                  <span className="animate-pulse text-[#6175A6]">✦</span>
                </div>

                <div className="mt-2 text-[18px] font-black text-[#37455F] 2xl:text-[21px]">
                  AI insights
                </div>

                <p className="mt-1 text-[11px] leading-5 text-[#6e7889]">
                  Know what changed and what action to take next.
                </p>
              </div>
            </div>
          </div>

          {/* Form */}
          <form
            className="
              relative mt-7 grid gap-3
              overflow-hidden
              rounded-[18px]
              border border-[#37455F]/[0.07]
              bg-white/95
              p-5
              shadow-[0_16px_44px_rgba(55,69,95,0.09)]

              before:pointer-events-none
              before:absolute
              before:-right-16
              before:-top-16
              before:h-40
              before:w-40
              before:rounded-full
              before:bg-[#5EA68E]/10
              before:content-['']

              sm:p-6

              lg:mt-0
              lg:gap-3
              lg:p-6

              xl:rounded-[20px]
              xl:p-7

              2xl:gap-3.5
              2xl:rounded-[22px]
            "
            action="#"
          >
            <div className="relative z-[1] mb-1">
              <div className="text-[17px] font-black text-[#37455F] xl:text-[18px]">
                Request your demo
              </div>

              <p className="mt-1 text-[11px] leading-5 text-[#7b8595] xl:text-[12px]">
                Share a few details and our team will help you get started.
              </p>
            </div>

            <div className="relative z-[1] grid grid-cols-2 gap-3 max-sm:grid-cols-1">
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
            </div>

            <div className="relative z-[1] grid grid-cols-2 gap-3 max-sm:grid-cols-1">
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
            </div>

            <textarea
              className={`
                ${inputClass}
                relative z-[1]
                min-h-[95px]
                resize-y

                xl:min-h-[105px]
                2xl:min-h-[120px]
              `}
              placeholder="What are you currently tracking manually?"
            />

            <button
              className="
                group relative z-[1]
                inline-flex min-h-[46px]
                items-center justify-center gap-2
                overflow-hidden rounded-[11px]
                bg-[linear-gradient(135deg,#37455F,#27344d)]
                px-5 py-3
                text-[13px] font-extrabold
                text-white
                shadow-[0_10px_28px_rgba(55,69,95,0.22)]
                transition-all duration-300

                hover:-translate-y-0.5
                hover:shadow-[0_16px_34px_rgba(55,69,95,0.28)]

                2xl:min-h-[50px]
                2xl:rounded-xl
                2xl:px-[22px]
                2xl:py-[14px]
                2xl:text-[14px]
              "
              type="submit"
            >
              Request demo

              <span className="transition-transform duration-300 group-hover:translate-x-1">
                →
              </span>
            </button>

            <p
              className="
                relative z-[1]
                text-center
                text-[10px]
                leading-[1.55]
                text-[#8A95A3]

                xl:text-[11px]
                2xl:text-xs
                2xl:leading-5
              "
            >
              Your details stay private and are only used to arrange your
              Phormula demo.
            </p>
          </form>
        </div>
      </div>
    </section>
  );
}