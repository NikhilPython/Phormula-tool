import { containerClass } from "./shared";

type FileCardProps = {
  fileName: string;
  rows: string[];
  className?: string;
  rotation?: string;
};

function FileCard({
  fileName,
  rows,
  className = "",
  rotation = "",
}: FileCardProps) {
  return (
    <div
      className={`
        absolute
        w-[175px]
        rounded-[10px]
        border border-[#37455F]/[0.08]
        bg-white
        px-4 py-3
        text-left
        shadow-[0_14px_32px_rgba(55,69,95,0.11)]
        transition duration-300
        hover:-translate-y-1
        hover:shadow-[0_18px_42px_rgba(55,69,95,0.15)]

        lg:w-[190px]
        xl:w-[205px]

        ${rotation}
        ${className}
      `}
    >
      <div className="mb-3 flex items-center gap-1.5">
        <span className="h-[5px] w-[5px] rounded-full bg-[#DF7C68]" />

        <span
          className="
            overflow-hidden text-ellipsis whitespace-nowrap
            font-mono text-[9px] font-medium
            text-[#C66D61]

            lg:text-[10px]
          "
        >
          {fileName}
        </span>
      </div>

      <div className="grid gap-[7px]">
        {rows.map((row, index) => (
          <div
            key={`${row}-${index}`}
            className="flex items-center justify-between gap-3"
          >
            <span className="text-[9px] font-medium text-[#617088] lg:text-[10px]">
              {row}
            </span>

            <span
              className="
                h-px rounded-full bg-[#D8DDE4]
              "
              style={{
                width: index % 2 === 0 ? "42px" : "30px",
              }}
            />
          </div>
        ))}
      </div>
    </div>
  );
}

function MetricCard({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div
      className="
        rounded-[8px]
        bg-[#F0F1EF]
        px-3 py-3
        text-left

        lg:px-4
        lg:py-[14px]
      "
    >
      <p
        className="
          text-[8px] font-semibold
          uppercase tracking-[0.04em]
          text-[#7A8596]

          lg:text-[9px]
        "
      >
        {label}
      </p>

      <p
        className="
          mt-1
          font-mono text-[14px] font-medium
          tracking-[-0.02em]
          text-[#45536B]

          lg:text-[16px]
        "
      >
        {value}
      </p>
    </div>
  );
}

function WorkspaceCard() {
  return (
    <div
      className="
        relative
        w-full max-w-[390px]
        rounded-[12px]
        border border-[#37455F]/[0.08]
        bg-white
        p-5
        text-left
        shadow-[0_18px_44px_rgba(55,69,95,0.13)]

        sm:p-6
        lg:max-w-[430px]
        lg:p-7
      "
    >
      <span
        className="
          absolute inset-y-0 left-0
          w-[3px]
          rounded-l-[12px]
          bg-[#58A78C]
        "
      />

      <div className="flex items-center justify-between gap-4">
        <h3
          className="
            font-[var(--font-dm-serif)]
            text-[17px]
            font-semibold
            text-[#49546A]

            lg:text-[19px]
          "
        >
          Phormula workspace
        </h3>

        <span
          className="
            inline-flex items-center gap-1.5
            rounded-full
            bg-[#E7F3EC]
            px-2.5 py-1
            text-[8px]
            font-bold
            text-[#5B9A82]

            lg:text-[9px]
          "
        >
          <span className="h-1.5 w-1.5 rounded-full bg-[#63A98E]" />
          Live
        </span>
      </div>

      <div className="mt-5 grid grid-cols-2 gap-2.5">
        <MetricCard label="Net Sales" value="£41,452" />
        <MetricCard label="CM2 Profit" value="£12,840" />
        <MetricCard label="Ad Spend" value="£6,120" />
        <MetricCard label="Stock Risk" value="2 SKUs" />
      </div>

      <div className="mt-5 border-t border-[#37455F]/[0.08] pt-4">
        <p className="text-[9px] leading-[1.65] text-[#707B8D] lg:text-[10px]">
          One workspace, always current.{" "}
          <span className="font-bold text-[#4B566C]">
            No exports. No manual joins. No guessing.
          </span>
        </p>
      </div>
    </div>
  );
}

function ReconciledArrow() {
  return (
    <div
      className="
        flex w-full items-center justify-center
        gap-2

        lg:flex-col
        lg:gap-2.5
      "
    >
      <span
        className="
          text-[8px]
          font-extrabold
          uppercase
          tracking-[0.12em]
          text-[#8290A1]
        "
      >
        Reconciled
      </span>

      <div
        className="
          flex w-full max-w-[150px]
          items-center

          lg:max-w-[95px]
        "
      >
        <span className="h-px flex-1 bg-[#68A58F]/40" />

        <span
          className="
            grid h-5 w-5 shrink-0
            place-items-center
            rounded-full
            bg-[#5EA68E]
            text-white
            shadow-[0_5px_14px_rgba(94,166,142,0.25)]
          "
        >
          <svg
            className="h-3 w-3"
            viewBox="0 0 24 24"
            fill="none"
            aria-hidden="true"
          >
            <path
              d="M5 12h13M14 8l4 4-4 4"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </span>
      </div>
    </div>
  );
}

export default function ProblemSolutionSection() {
  return (
    <section
      id="platform"
      className="
        relative overflow-hidden
        bg-[#FCFCFA]
        py-[72px]

        md:py-[84px]
        lg:py-[96px]
        xl:py-[108px]
      "
    >
      <div className={containerClass}>
        <div className="mx-auto max-w-[1180px] text-center">
          {/* Heading */}
          <div className="mx-auto max-w-[780px]">
            <div
              className="
                inline-flex items-center gap-2
                rounded-full
                border border-[#5EA68E]/35
                bg-[#E9F4EF]
                px-4 py-2
                text-[10px]
                font-extrabold
                uppercase
                tracking-[0.07em]
                text-[#5A9D85]

                sm:text-[11px]
              "
            >
              <span className="h-1.5 w-1.5 rounded-full bg-[#5EA68E]" />
              The Problem &amp; The Fix
            </div>

            <h2
              className="
                mt-7
                font-[var(--font-dm-serif)]
                text-[34px]
                leading-[1.04]
                tracking-[-0.035em]
                text-[#37455F]

                sm:text-[42px]
                lg:text-[50px]
                xl:text-[54px]
              "
            >
              From{" "}
              <span className="text-[#5EA68E]">Scattered Sheets</span> to
              <br className="hidden sm:block" /> One Source of Truth
            </h2>

            <p
              className="
                mx-auto mt-5
                max-w-[620px]
                text-[14px]
                leading-[1.65]
                text-[#5F6878]

                sm:text-[15px]
              "
            >
              Phormula replaces disjointed spreadsheets and disconnected tools
              with one live dashboard built for your business.
            </p>
          </div>

          {/* Main visual */}
          <div
            className="
              relative mt-14

              lg:mt-[72px]
            "
          >
            <div
              className="
                absolute left-1/2 top-1/2
                h-[330px] w-[92%]
                -translate-x-1/2 -translate-y-1/2
                rounded-full
                bg-[radial-gradient(circle,rgba(94,166,142,0.08)_0%,rgba(94,166,142,0.03)_35%,transparent_70%)]
                blur-2xl
              "
            />

            <div
              className="
                relative z-[2]
                grid items-center gap-10

                lg:grid-cols-[1fr_130px_1fr]
                lg:gap-4

                xl:grid-cols-[1fr_150px_1fr]
              "
            >
              {/* Scattered spreadsheet files */}
              <div
                className="
                  relative mx-auto
                  h-[310px]
                  w-full max-w-[440px]

                  sm:h-[330px]
                  lg:h-[345px]
                  lg:max-w-[460px]
                "
              >
                <FileCard
                  fileName="sales_Q2.xlsx"
                  rows={["Revenue", "Returns"]}
                  rotation="-rotate-[5deg]"
                  className="
                    left-[5%] top-[15px]
                    sm:left-[8%]
                    lg:left-[2%]
                  "
                />

                <FileCard
                  fileName="ad_spend.csv"
                  rows={["Meta", "Amazon PPC"]}
                  rotation="rotate-[4deg]"
                  className="
                    right-[3%] top-[72px]
                    sm:right-[6%]
                    lg:right-[2%]
                  "
                />

                <FileCard
                  fileName="fees_export.pdf"
                  rows={["FBA fees", "Referral"]}
                  rotation="rotate-[2deg]"
                  className="
                    left-[1%] bottom-[58px]
                    sm:left-[5%]
                    lg:left-0
                  "
                />

                <FileCard
                  fileName="inventory_v3.xlsx"
                  rows={["SKU", "Reorder"]}
                  rotation="-rotate-[4deg]"
                  className="
                    right-[1%] bottom-[18px]
                    sm:right-[5%]
                    lg:right-[1%]
                  "
                />

                <p
                  className="
                    absolute bottom-0 left-1/2
                    -translate-x-1/2
                    whitespace-nowrap
                    text-[9px]
                    text-[#98A1AF]

                    sm:text-[10px]
                  "
                >
                  Four files. Four versions of the truth.
                </p>
              </div>

              {/* Arrow */}
              <ReconciledArrow />

              {/* Phormula workspace */}
              <div className="flex justify-center lg:justify-start">
                <WorkspaceCard />
              </div>
            </div>
          </div>

          {/* Bottom statement */}
          <div
            className="
              relative z-[2]
              mx-auto mt-12
              flex w-fit max-w-[720px]
              items-center gap-3
              rounded-full
              border border-[#37455F]/[0.09]
              bg-white
              px-5 py-3
              shadow-[0_12px_34px_rgba(55,69,95,0.08)]

              sm:px-7
              sm:py-3.5

              max-sm:rounded-[18px]
              max-sm:text-left
            "
          >
            <span
              className="
                grid h-7 w-7 shrink-0
                place-items-center
                rounded-full
                bg-[#377E66]
                text-[11px]
                text-white
                shadow-[0_5px_14px_rgba(55,126,102,0.22)]
              "
            >
              ✦
            </span>

            <p
              className="
                text-[11px]
                font-semibold
                leading-[1.5]
                text-[#435068]

                sm:text-[12px]
              "
            >
              Phormula turns operational chaos into clarity, so every decision
              is faster and better informed.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}