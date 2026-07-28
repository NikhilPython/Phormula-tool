import React from "react";

const beforeItems = [
  "Manual spreadsheet maintenance",
  "Confusion in inventory dispatch",
  "Less time for business, more time spent on calculations",
  "Running after people to gather data",
];

const afterItems = [
  "No more spreadsheet chaos",
  "Proper planning for inventory, dispatch, and POs",
  "AI business insights with objective-based recommendations",
  "No more follow-ups — your business explained in a single screen",
];

function CrossIcon() {
  return (
    <svg
      className="
        h-[17px] w-[17px] stroke-[2.4]
        2xl:h-[19px] 2xl:w-[19px]
      "
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      aria-hidden="true"
    >
      <line x1="18" y1="6" x2="6" y2="18" />
      <line x1="6" y1="6" x2="18" y2="18" />
    </svg>
  );
}

function CheckIcon() {
  return (
    <svg
      className="
        h-[17px] w-[17px] stroke-[2.4]
        2xl:h-[19px] 2xl:w-[19px]
      "
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      aria-hidden="true"
    >
      <polyline points="20 6 9 17 4 12" />
    </svg>
  );
}

export default function BeforeAfterSection() {
  return (
    <section
      id="before-after"
      className="
        relative overflow-hidden
        bg-blue-700! text-white

        py-[72px]
        lg:py-20
        xl:py-[88px]
        2xl:py-24

        before:pointer-events-none
        before:absolute
        before:-right-40
        before:-top-45
        before:h-105
        before:w-105
        before:rounded-full
        before:bg-[rgba(94,166,142,0.16)]
        before:content-['']

        after:pointer-events-none
        after:absolute
        after:-bottom-45
        after:-left-40
        after:h-90
        after:w-90
        after:rounded-full
        after:bg-[rgba(253,211,111,0.10)]
        after:content-['']
      "
    >
      <div
        className="
          relative z-1 mx-auto
          w-[calc(100%-32px)]
          max-w-[1100px]
          text-center

          md:w-[calc(100%-48px)]
          xl:w-[calc(100%-80px)]
          2xl:max-w-[1480px]
        "
      >
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
              The Difference
            </div>

        <h2
  className="
    mx-auto mt-[18px]
    max-w-[820px]
    hero-heading
    !text-[30px]
    !leading-[1.1]
    tracking-[-0.035em]
    text-white

    sm:!text-[36px]
    lg:!text-[42px]

    2xl:max-w-[1000px]
    2xl:!text-[54px]
  "
>
  Move From Manual Chaos to Clear Business Control
</h2>

        <div
          className="
            mt-9 grid grid-cols-1 gap-4

            md:mt-10
            md:grid-cols-2
            md:gap-[18px]

            xl:mt-11
            xl:gap-5

            2xl:mt-12.5
            2xl:gap-5.5
          "
        >
          {/* Before */}
          <article
            className="
              rounded-[20px]
              border border-white/12
              bg-white/6
              p-5 text-left
              shadow-[0_16px_44px_rgba(55,69,95,0.13)]

              sm:p-6

              lg:rounded-[22px]
              lg:p-6

              xl:p-7

              2xl:rounded-[28px]
              2xl:p-8.5
            "
          >
            <div
              className="
                relative z-1 mb-4
                flex items-center gap-2.5
                text-[16px] font-black
                text-white/[0.58]

                xl:text-[17px]

                2xl:mb-5.5
                2xl:text-[1.14rem]
              "
            >
              <svg
                className="
                  h-[18px] w-[18px]
                  2xl:h-5 2xl:w-5
                "
                viewBox="0 0 24 24"
                fill="none"
                stroke="rgba(255,255,255,0.3)"
                strokeWidth="2"
                aria-hidden="true"
              >
                <circle cx="12" cy="12" r="10" />
                <line x1="15" y1="9" x2="9" y2="15" />
                <line x1="9" y1="9" x2="15" y2="15" />
              </svg>

              Before Phormula
            </div>

            <ul className="relative z-1 grid list-none">
              {beforeItems.map((item) => (
                <li
                  key={item}
                  className="
                    flex items-start gap-2.5
                    border-b border-white/8
                    py-[11px]
                    text-[13px] font-bold
                    leading-[1.55]
                    text-white/60
                    last:border-b-0

                    lg:text-[13px]

                    xl:gap-3
                    xl:py-3
                    xl:text-[14px]

                    2xl:py-3.25
                    2xl:text-[0.95rem]
                  "
                >
                  <span
                    className="
                      mt-px inline-flex
                      h-5 w-5 shrink-0
                      items-center justify-center
                      text-white/34

                      2xl:h-5.5
                      2xl:w-5.5
                    "
                  >
                    <CrossIcon />
                  </span>

                  <span>{item}</span>
                </li>
              ))}
            </ul>
          </article>

          {/* After */}
          <article
            className="
              relative overflow-hidden
              rounded-[20px]
              border border-white/20
              bg-[linear-gradient(145deg,#5EA68E,#4A8A74)]
              p-5 text-left
              shadow-[0_16px_44px_rgba(55,69,95,0.13)]

              before:absolute
              before:-right-12
              before:-top-12
              before:h-42.5
              before:w-42.5
              before:rounded-full
              before:bg-white/10
              before:content-['']

              sm:p-6

              lg:rounded-[22px]
              lg:p-6

              xl:p-7

              2xl:rounded-[28px]
              2xl:p-8.5
            "
          >
            <div
              className="
                relative z-1 mb-4
                flex items-center gap-2.5
                text-[16px] font-black
                text-white

                xl:text-[17px]

                2xl:mb-5.5
                2xl:text-[1.14rem]
              "
            >
              <svg
                className="
                  h-[18px] w-[18px]
                  2xl:h-5 2xl:w-5
                "
                viewBox="0 0 24 24"
                fill="none"
                stroke="white"
                strokeWidth="2"
                aria-hidden="true"
              >
                <circle cx="12" cy="12" r="10" />
                <polyline points="9 12 12 15 17 9" />
              </svg>

              After Phormula
            </div>

            <ul className="relative z-1 grid list-none">
              {afterItems.map((item) => (
                <li
                  key={item}
                  className="
                    flex items-start gap-2.5
                    border-b border-white/16
                    py-[11px]
                    text-[13px] font-bold
                    leading-[1.55]
                    text-white/92
                    last:border-b-0

                    lg:text-[13px]

                    xl:gap-3
                    xl:py-3
                    xl:text-[14px]

                    2xl:py-3.25
                    2xl:text-[0.95rem]
                  "
                >
                  <span
                    className="
                      mt-px inline-flex
                      h-5 w-5 shrink-0
                      items-center justify-center
                      text-white

                      2xl:h-5.5
                      2xl:w-5.5
                    "
                  >
                    <CheckIcon />
                  </span>

                  <span>{item}</span>
                </li>
              ))}
            </ul>
          </article>
        </div>
      </div>
    </section>
  );
}