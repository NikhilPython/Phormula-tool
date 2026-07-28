import { containerClass } from "./shared";

type Card = {
  index: string;
  title: string;
  text: string;
  type: "problem" | "solution";
  icon: "file" | "dashboard" | "clock" | "idea";
};

const cards: Card[] = [
  {
    index: "01",
    title: "Scattered Sheets",
    text: "Sales, fees, inventory, ads and expenses live across disconnected files and reports.",
    type: "problem",
    icon: "file",
  },
  {
    index: "01",
    title: "One Live Dashboard",
    text: "Finance, inventory, advertising and leadership stay aligned inside one centralized workspace.",
    type: "solution",
    icon: "dashboard",
  },
  {
    index: "02",
    title: "Delayed Decisions",
    text: "By the time reports are cleaned, the opportunity to protect margin may already be gone.",
    type: "problem",
    icon: "clock",
  },
  {
    index: "02",
    title: "Actionable Insights",
    text: "AI explains what changed, why it matters and what action can improve performance.",
    type: "solution",
    icon: "idea",
  },
];

function Icon({ name }: { name: Card["icon"] }) {
  const iconClass =
    "h-[23px] w-[23px] xl:h-[25px] xl:w-[25px] 2xl:h-[27px] 2xl:w-[27px]";

  if (name === "file") {
    return (
      <svg className={iconClass} viewBox="0 0 24 24" fill="none">
        <path
          d="M7 3.75h7.5L19 8.25v12H7v-16.5Z"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinejoin="round"
        />
        <path
          d="M14.5 3.75v4.5H19M10 12h6M10 15h6"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    );
  }

  if (name === "dashboard") {
    return (
      <svg className={iconClass} viewBox="0 0 24 24" fill="none">
        <rect
          x="3.5"
          y="4.5"
          width="17"
          height="11"
          rx="1.5"
          stroke="currentColor"
          strokeWidth="1.5"
        />
        <path
          d="M8 19.5h8M12 15.5v4"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    );
  }

  if (name === "clock") {
    return (
      <svg className={iconClass} viewBox="0 0 24 24" fill="none">
        <circle
          cx="12"
          cy="13"
          r="7"
          stroke="currentColor"
          strokeWidth="1.5"
        />
        <path
          d="M12 9v4l2.5 1.5M9 3h6M12 3v3"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    );
  }

  return (
    <svg className={iconClass} viewBox="0 0 24 24" fill="none">
      <path
        d="M9 18h6M10 21h4"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
      <path
        d="M8.2 14.5A6 6 0 1 1 15.8 14.5c-.95.7-1.55 1.48-1.8 2.5h-4c-.25-1.02-.85-1.8-1.8-2.5Z"
        stroke="currentColor"
        strokeWidth="1.5"
      />
      <path
        d="m10 11 1.3 1.3L14.5 9"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
    </svg>
  );
}

function CardView({ card }: { card: Card }) {
  const isSolution = card.type === "solution";

  return (
    <article
      className={`
        group relative flex min-h-[142px] items-start
        gap-4 rounded-[16px] border bg-white/90
        px-5 py-5 text-left
        shadow-[0_15px_40px_rgba(56,48,38,0.07),0_2px_8px_rgba(56,48,38,0.03)]
        backdrop-blur-xl
        transition duration-300

        hover:-translate-y-1
        hover:shadow-[0_22px_50px_rgba(56,48,38,0.11),0_4px_12px_rgba(56,48,38,0.04)]

        sm:min-h-[148px]
        sm:px-6
        sm:py-[22px]

        lg:min-h-[150px]
        lg:gap-4
        lg:px-5
        lg:py-5

        xl:min-h-[156px]
        xl:gap-[17px]
        xl:px-6
        xl:py-6

        2xl:min-h-[164px]
        2xl:gap-[18px]
        2xl:rounded-[18px]
        2xl:px-7
        2xl:py-[26px]

        ${
          isSolution
            ? "border-[#39a67e]/20 border-r-4 border-r-[#39a67e]"
            : "border-[#d84c55]/10 hover:border-[#d84c55]/20"
        }
      `}
    >
      <div
        className={`
          grid h-12 w-12 shrink-0 place-items-center
          rounded-[14px]
          transition duration-300

          group-hover:-translate-y-[3px]
          group-hover:-rotate-3
          group-hover:scale-[1.04]

          xl:h-[54px]
          xl:w-[54px]
          xl:rounded-[15px]

          2xl:h-[58px]
          2xl:w-[58px]
          2xl:rounded-2xl

          ${
            isSolution
              ? "bg-[linear-gradient(145deg,rgba(229,246,238,0.98),rgba(218,239,229,0.86))] text-[#2c9571] group-hover:shadow-[0_12px_28px_rgba(44,149,113,0.16)]"
              : "bg-[linear-gradient(145deg,rgba(255,238,238,0.98),rgba(252,229,229,0.82))] text-[#cc5960] group-hover:shadow-[0_12px_28px_rgba(204,89,96,0.14)]"
          }
        `}
      >
        <Icon name={card.icon} />
      </div>

      <div className="relative min-w-0 flex-1 pt-px">
        <span
          className="
            absolute -top-1.5 right-0
            text-[25px] font-extrabold leading-none
            tracking-[-0.05em] text-[#293653]/10

            xl:text-[28px]
            2xl:-top-2.5
            2xl:text-[30px]
          "
        >
          {card.index}
        </span>

        <h3
          className="
            relative z-[1]
            pr-8
            text-[17px] font-semibold leading-[1.3]
            tracking-[-0.02em] text-[#30415f]

            xl:text-[18px]
            2xl:pr-[35px]
            2xl:text-[19px]
          "
        >
          {card.title}
        </h3>

        <span
          className={`
            my-2.5 block h-0.5 w-[28px] rounded-full
            transition-all duration-300
            group-hover:w-[48px]

            2xl:mb-3
            2xl:w-[30px]
            2xl:group-hover:w-[52px]

            ${isSolution ? "bg-[#45aa84]" : "bg-[#e76870]"}
          `}
        />

        <p
          className="
            max-w-full
            text-[12.5px] leading-[1.65]
            text-[#647087]

            sm:text-[13px]
            xl:max-w-[390px]
            2xl:leading-[1.7]
          "
        >
          {card.text}
        </p>
      </div>
    </article>
  );
}

function Connector() {
  return (
    <div
      className="
        relative flex items-center justify-center

        max-md:h-[38px]
        md:min-h-[142px]
        lg:min-h-[150px]
        xl:min-h-[156px]
        2xl:min-h-[164px]
      "
    >
      <span
        className="
          absolute inset-x-0 h-px bg-[#366f5d]/15

          max-md:inset-y-0
          max-md:left-1/2
          max-md:right-auto
          max-md:h-auto
          max-md:w-px
        "
      />

      <span
        className="
          relative z-[2]
          grid h-9 w-9 place-items-center
          rounded-full border border-[#2c455b]/10
          bg-white/95 text-[#5d6c82]
          shadow-[0_8px_24px_rgba(44,57,76,0.1),0_1px_4px_rgba(44,57,76,0.05)]

          max-md:rotate-90

          2xl:h-[38px]
          2xl:w-[38px]
        "
      >
        <svg
          className="h-[18px] w-[18px] 2xl:h-[19px] 2xl:w-[19px]"
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

export default function ProblemSolutionSection() {
  return (
    <section
      id="platform"
      className="
        bg-[radial-gradient(circle_at_50%_35%,rgba(255,255,255,0.95)_0%,rgba(255,255,255,0)_36%),linear-gradient(180deg,#f7f1e6_0%,#f4ecdf_100%)]
        py-[72px]

        lg:py-20
        xl:py-[88px]
        2xl:py-24
      "
    >
      <div className={`${containerClass} text-center`}>
        <div className="mx-auto w-full max-w-[1100px] 2xl:max-w-[1160px]">
          <div
            className="
              mx-auto mb-10 max-w-[720px]

              md:mb-11
              lg:mb-12
              2xl:mb-[54px]
              2xl:max-w-[760px]
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
              The Problem
            </div>

            <h2
              className="
                mt-[18px]
                font-[var(--font-dm-serif)]
                text-[30px]
                leading-[1.08]
                tracking-[-0.035em]
                text-[#263653]

                sm:text-[38px]
                lg:text-[42px]
                xl:text-[42px]
                2xl:mt-[19px]
                2xl:text-[54px]
              "
            >
              From scattered sheets to{" "}
              <span className="text-[#30456e]">
                one source of truth
              </span>
            </h2>

            <p
              className="
                mx-auto mt-4 max-w-[590px]
                text-[14px] leading-[1.7]
                text-[#68748a]

                md:text-[15px]
                2xl:mt-[18px]
                2xl:max-w-[610px]
                2xl:leading-[1.75]
              "
            >
              Phormula replaces fragmented reporting and manual follow-ups with
              one live, intelligent workspace for your business.
            </p>
          </div>

          <div
            className="
              mb-4 grid
              grid-cols-[minmax(0,1fr)_58px_minmax(0,1fr)]

              max-md:hidden

              lg:grid-cols-[minmax(0,1fr)_64px_minmax(0,1fr)]
              xl:grid-cols-[minmax(0,1fr)_70px_minmax(0,1fr)]
              2xl:mb-5
              2xl:grid-cols-[minmax(0,1fr)_76px_minmax(0,1fr)]
            "
          >
            <div className="justify-self-center rounded-full border border-[#d84c55]/20 bg-[#fff1f1]/90 px-[17px] py-2 text-[10px] font-extrabold uppercase tracking-[0.1em] text-[#d84c55]">
              The Problem
            </div>

            <div />

            <div className="justify-self-center rounded-full border border-[#288466]/20 bg-[#e9f7f1]/90 px-[17px] py-2 text-[10px] font-extrabold uppercase tracking-[0.1em] text-[#288466]">
              The Solution
            </div>
          </div>

          <div
            className="
              grid
              grid-cols-[minmax(0,1fr)_58px_minmax(0,1fr)]
              gap-y-4

              max-md:flex
              max-md:flex-col
              max-md:gap-3

              lg:grid-cols-[minmax(0,1fr)_64px_minmax(0,1fr)]
              xl:grid-cols-[minmax(0,1fr)_70px_minmax(0,1fr)]
              2xl:grid-cols-[minmax(0,1fr)_76px_minmax(0,1fr)]
              2xl:gap-y-[18px]
            "
          >
            <CardView card={cards[0]} />
            <Connector />
            <CardView card={cards[1]} />

            <CardView card={cards[2]} />
            <Connector />
            <CardView card={cards[3]} />
          </div>

          <div
            className="
              mx-auto mt-7 flex w-fit max-w-[680px]
              items-center justify-center gap-[10px]
              rounded-[14px]
              border border-[#334157]/[0.08]
              bg-white/60
              px-4 py-3
              shadow-[0_12px_34px_rgba(51,46,37,0.06)]
              backdrop-blur-xl

              sm:px-5
              2xl:mt-9
              2xl:gap-[11px]
              2xl:py-[13px]

              max-sm:items-start
            "
          >
            <span className="grid h-7 w-7 shrink-0 place-items-center rounded-full bg-white text-[#315f74] shadow-[0_5px_16px_rgba(49,95,116,0.12)]">
              ✦
            </span>

            <p className="text-left text-xs font-semibold leading-6 text-[#34435f]">
              Phormula turns operational chaos into clarity, so every decision
              is faster and better informed.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}