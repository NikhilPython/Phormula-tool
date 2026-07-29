"use client";

import { useCallback, useEffect, useRef, useState } from "react";

const slides = [
  {
    title: "Financial Intelligence.",
    eyebrow: "Live P&L visibility",
    description: "See revenue, costs and contribution margin move together.",
    visual: "financial",
  },
  {
    title: "Profit Clarity.",
    eyebrow: "Margin intelligence",
    description: "Spot the products and cost lines improving or hurting profit.",
    visual: "profit",
  },
  {
    title: "Cashflow Control.",
    eyebrow: "Cash movement",
    description: "Track inflows, outflows and your projected closing balance.",
    visual: "cashflow",
  },
  {
    title: "Inventory Forecasting.",
    eyebrow: "AI demand planning",
    description: "Forecast demand, coverage and the right replenishment window.",
    visual: "inventory",
  },
] as const;

type VisualType = (typeof slides)[number]["visual"];

const SLIDE_DURATION = 4200;
const SLIDE_TRANSITION_DURATION = 400;

export default function AnimatedHeroSection() {
  const [activeIndex, setActiveIndex] = useState(0);
  const [isTransitioning, setIsTransitioning] = useState(false);
  const transitionTimerRef = useRef<number | null>(null);

  const changeSlide = useCallback((nextIndex: number) => {
    if (nextIndex === activeIndex || isTransitioning) return;

    setIsTransitioning(true);

    if (transitionTimerRef.current) {
      window.clearTimeout(transitionTimerRef.current);
    }

    transitionTimerRef.current = window.setTimeout(() => {
      setActiveIndex(nextIndex);
      setIsTransitioning(false);
      transitionTimerRef.current = null;
    }, SLIDE_TRANSITION_DURATION);
  }, [activeIndex, isTransitioning]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      changeSlide((activeIndex + 1) % slides.length);
    }, SLIDE_DURATION);

    return () => window.clearInterval(interval);
  }, [activeIndex, changeSlide]);

  useEffect(() => {
    return () => {
      if (transitionTimerRef.current) {
        window.clearTimeout(transitionTimerRef.current);
      }
    };
  }, []);

  const activeSlide = slides[activeIndex];

 return (
  <section
  className="
    hero
    !mt-0
    !px-4
    sm:!px-6
    lg:!px-8
    xl:!px-10
    2xl:!px-12
    !pb-14
    !pt-[116px]
    sm:!pb-16
    sm:!pt-[132px]
    lg:!pb-16
    lg:!pt-[126px]
    xl:!pt-[96px]
    2xl:!pb-20
    2xl:!pt-[132px]
  "
>
   <div
  className="
    relative
    z-10
    mx-auto
    grid
    w-full
    max-w-[1440px]
    grid-cols-1
    items-center
    gap-12
    lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]
    lg:gap-10
    xl:grid-cols-[minmax(420px,0.9fr)_minmax(560px,1.1fr)]
    xl:gap-12
    2xl:grid-cols-[minmax(500px,0.95fr)_minmax(620px,1.05fr)]
    2xl:gap-14
  "
>
      {/* Left content */}
      <div
        className="
          relative
          z-20
          w-full
          max-w-[620px]
          lg:max-w-[460px]
          xl:max-w-[500px]
          2xl:max-w-[610px]
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
              AI Financial Intelligence
            </div>

       <h1
  className="
    hero-heading
    !mt-5
    !max-w-[500px]
    !text-[36px]
    !leading-[0.98]
    !tracking-[-0.05em]
    sm:!text-[40px]
    lg:!text-[38px]
    xl:!text-[42px]
    2xl:!max-w-[620px]
    2xl:!text-[58px]
    min-[1700px]:!text-[66px]
  "
>
  Empowering D2C <br className="hidden min-[1700px]:block "/> Founders with

  
   
  <span
    className={`
      hero-rotating-copy
      !mt-1
      !min-h-[1.14em]
      !text-[29px]
      !leading-[1.08]
      sm:!text-[33px]
      lg:!text-[31px]
      xl:!text-[35px]
      2xl:!text-[48px]
      min-[1700px]:!text-[54px]
      ${isTransitioning ? "is-changing" : ""}
    `}
  >
    <span
      key={activeSlide.title}
      className="
        hero-rotating-word
        !w-full
        whitespace-normal
        !text-green-500
        lg:whitespace-nowrap
      "
    >
      {activeSlide.title}
    </span>
  </span>
</h1>

       <p
  className="
    !mt-4
    !max-w-[480px]
    !text-[12px]
    !leading-5
    xl:!text-sm
    2xl:!text-base
    2xl:!leading-7
  "
>
  AI-powered financial clarity that helps founders lead, not just
  read reports.
</p>

        <a
  href="#demo"
  className="
    btn
    btn-primary
    !mt-5
    !flex
    !h-[44px]
    !w-[190px]
    !items-center
    !justify-center
    !rounded-[10px]
    !bg-[#37455F]
    !px-5
    !text-[13px]
    !font-extrabold
    !text-[#F8EDCE]
    !shadow-[0_8px_22px_rgba(55,69,95,0.2)]
    transition
    hover:!bg-[#202A3A]
    sm:!w-[200px]
    lg:!w-[205px]
    xl:!h-[46px]
    xl:!w-[215px]
    xl:!text-[14px]
    2xl:!mt-6
    2xl:!h-[56px]
    2xl:!w-[260px]
    2xl:!rounded-xl
    2xl:!text-[16px]
  "
>
  Book a demo
</a>

        <p
  className="
    hero-note
    !mt-4
    !max-w-[480px]
    !text-[12px]
    !leading-[18px]
    xl:!text-sm
    2xl:!mt-5
    2xl:!text-base
    2xl:!leading-6
  "
>
  Built for operators tired of spreadsheet chaos, delayed reports,
  and unclear margins.
</p>
      </div>

      {/* Right dashboard */}
     {/* Right dashboard */}
<div
  className="
    mockup-wrap
    relative
    z-10
    flex
    w-full
    min-w-0
    justify-center
    lg:justify-end
  "
>
  <div
  className="
    w-full
    max-w-[560px]
    sm:max-w-[580px]
    lg:max-w-[520px]
    xl:max-w-[570px]
    2xl:max-w-[660px]
    min-[1700px]:max-w-[720px]
  "
>
    <AnimatedDashboard
      activeIndex={activeIndex}
      activeVisual={activeSlide.visual}
      eyebrow={activeSlide.eyebrow}
      description={activeSlide.description}
      onSelect={changeSlide}
      isTransitioning={isTransitioning}
    />
  </div>
</div>
    </div>
  </section>
);
}

function AnimatedDashboard({
  activeIndex,
  activeVisual,
  eyebrow,
  description,
  onSelect,
  isTransitioning,
}: {
  activeIndex: number;
  activeVisual: VisualType;
  eyebrow: string;
  description: string;
  onSelect: (index: number) => void;
  isTransitioning: boolean;
}) {
  return (
    <div className={`hero-dashboard-shell ${isTransitioning ? "is-changing" : ""}`}>
      <div className="hero-dashboard-glow" />

      <div className="hero-dashboard-window">
        <div className="hero-dashboard-topbar">
          <div className="hero-window-dots" aria-hidden="true">
            <span />
            <span />
            <span />
          </div>
          <span>Phormula AI Command Centre</span>
          <div className="hero-live-pill">
            <i /> Live
          </div>
        </div>

        <div className="hero-dashboard-body">
          <div className="hero-dashboard-heading">
  <div>
    <span>{eyebrow}</span>
    <h3>{slides[activeIndex].title}</h3>
    <p>{description}</p>
  </div>

  <div className="hero-ai-chip">Refresh</div>
</div>

<div
  className="hero-slide-tabs"
  role="tablist"
  aria-label="Hero visual selector"
>
  {slides.map((slide, index) => (
    <button
      key={slide.title}
      type="button"
      className={index === activeIndex ? "active" : ""}
      onClick={() => onSelect(index)}
      aria-selected={index === activeIndex}
      role="tab"
    >
      {slide.title.replace(".", "")}
    </button>
  ))}
</div>

<div className="hero-visual-stage" key={activeVisual}>
            {activeVisual === "financial" && <FinancialVisual />}
            {activeVisual === "profit" && <ProfitVisual />}
            {activeVisual === "cashflow" && <CashflowVisual />}
            {activeVisual === "inventory" && <InventoryVisual />}
          </div>
        </div>
      </div>
    </div>
  );
}

function FinancialVisual() {
  return (
    <div className="hero-visual-grid hero-financial-visual">
      <div className="hero-kpi-row">
        <AnimatedKpi label="Net Sales" value="£41,452" change="+10.08%" delay="0s" />
        <AnimatedKpi label="CM2 Profit" value="£12,840" change="+18.42%" delay=".12s" />
        <AnimatedKpi label="Units" value="4,990" change="+51.81%" delay=".24s" />
      </div>
      <ChartCard title="Performance trend" badge="2026">
        <svg viewBox="0 0 620 220" className="hero-line-chart" role="img" aria-label="Animated financial performance line chart">
          <defs>
            <linearGradient id="financialArea" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#5EA68E" stopOpacity=".32" />
              <stop offset="100%" stopColor="#5EA68E" stopOpacity="0" />
            </linearGradient>
          </defs>
          <GridLines />
          <path className="hero-chart-area" d="M20 174 C92 160 116 133 168 139 C230 146 252 72 324 82 C395 91 425 129 488 102 C540 81 568 42 600 50 L600 205 L20 205 Z" fill="url(#financialArea)" />
          <path className="hero-chart-line primary" pathLength="1" d="M20 174 C92 160 116 133 168 139 C230 146 252 72 324 82 C395 91 425 129 488 102 C540 81 568 42 600 50" />
          {["20,174", "168,139", "324,82", "488,102", "600,50"].map((point, index) => {
            const [cx, cy] = point.split(",");
            return <circle key={point} cx={cx} cy={cy} r="5" className="hero-chart-point" style={{ animationDelay: `${0.6 + index * 0.16}s` }} />;
          })}
        </svg>
      </ChartCard>
    </div>
  );
}

function ProfitVisual() {
  const bars = [72, 48, 84, 62, 91];
  return (
    <div className="hero-visual-grid hero-profit-visual">
      <div className="hero-kpi-row">
        <AnimatedKpi label="Gross Margin" value="61.8%" change="+6.4%" delay="0s" />
        <AnimatedKpi label="CM1 / Unit" value="£4.72" change="+12.1%" delay=".12s" />
        <AnimatedKpi label="Profit Leak" value="-£1,240" change="Fixed" delay=".24s" positive />
      </div>
      <ChartCard title="SKU profit contribution" badge="AI ranked">
        <div className="hero-profit-layout">
          <div className="hero-profit-bars">
            {bars.map((height, index) => (
              <div className="hero-profit-column" key={height}>
                <span style={{ height: `${height}%`, animationDelay: `${index * 0.12}s` }} />
                <small>SKU {index + 1}</small>
              </div>
            ))}
          </div>
          <div className="hero-profit-insight">
            <span>AI insight</span>
            <strong>3 SKUs create 78% of CM2</strong>
            <p>Reallocate ad spend toward high-margin products.</p>
            <div className="hero-profit-ring"><b>78%</b></div>
          </div>
        </div>
      </ChartCard>
    </div>
  );
}

function CashflowVisual() {
  return (
    <div className="hero-visual-grid hero-cashflow-visual">
      <div className="hero-kpi-row">
        <AnimatedKpi label="Cash In" value="£52,400" change="+14.3%" delay="0s" />
        <AnimatedKpi label="Cash Out" value="£31,860" change="-4.8%" delay=".12s" positive />
        <AnimatedKpi label="Closing Cash" value="£20,540" change="Healthy" delay=".24s" positive />
      </div>
      <ChartCard title="13-week cash runway" badge="Forecast">
        <svg viewBox="0 0 620 220" className="hero-cash-chart" role="img" aria-label="Animated cashflow area chart">
          <defs>
            <linearGradient id="cashArea" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#75BBDA" stopOpacity=".4" />
              <stop offset="100%" stopColor="#75BBDA" stopOpacity="0" />
            </linearGradient>
          </defs>
          <GridLines />
          <path className="hero-chart-area cash" d="M20 166 C72 136 104 148 150 126 C198 104 230 122 276 94 C326 64 356 82 402 68 C454 51 505 65 600 33 L600 205 L20 205 Z" fill="url(#cashArea)" />
          <path className="hero-chart-line cash" pathLength="1" d="M20 166 C72 136 104 148 150 126 C198 104 230 122 276 94 C326 64 356 82 402 68 C454 51 505 65 600 33" />
          <line x1="455" y1="25" x2="455" y2="205" className="hero-forecast-divider" />
          <text x="468" y="44" className="hero-forecast-text">forecast</text>
        </svg>
      </ChartCard>
    </div>
  );
}

function InventoryVisual() {
  const coverage = [2.4, 1.1, 3.8, 0.7, 2.9];
  return (
    <div className="hero-visual-grid hero-inventory-visual">
      <div className="hero-kpi-row">
        <AnimatedKpi label="Forecast Units" value="6,420" change="+28.6%" delay="0s" />
        <AnimatedKpi label="Stock Cover" value="2.4 mo" change="Balanced" delay=".12s" positive />
        <AnimatedKpi label="Action Needed" value="2 SKUs" change="Reorder" delay=".24s" />
      </div>
      <ChartCard title="Inventory coverage forecast" badge="Next 6 months">
        <div className="hero-inventory-layout">
          <div className="hero-inventory-list">
            {coverage.map((value, index) => (
              <div className="hero-stock-row" key={value}>
                <span>SKU-{104 + index}</span>
                <div><i style={{ width: `${Math.min(value / 4 * 100, 100)}%`, animationDelay: `${index * 0.12}s` }} /></div>
                <b className={value < 1.5 ? "danger" : ""}>{value} mo</b>
              </div>
            ))}
          </div>
          <div className="hero-reorder-card">
            <span>Suggested PO</span>
            <strong>1,860 units</strong>
            <p>Dispatch by 18 Aug</p>
            <div className="hero-box-animation" aria-hidden="true">
              <i /><i /><i />
            </div>
          </div>
        </div>
      </ChartCard>
    </div>
  );
}

function AnimatedKpi({
  label,
  value,
  change,
  delay,
  positive = false,
}: {
  label: string;
  value: string;
  change: string;
  delay: string;
  positive?: boolean;
}) {
  return (
    <div className="hero-kpi-card" style={{ animationDelay: delay }}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small className={positive ? "positive" : ""}>{change}</small>
      <i className="hero-kpi-shine" />
    </div>
  );
}

function ChartCard({ title, badge, children }: { title: string; badge: string; children: React.ReactNode }) {
  return (
    <div className="hero-chart-card">
      <div className="hero-chart-card-header">
        <strong>{title}</strong>
        <span>{badge}</span>
      </div>
      {children}
    </div>
  );
}

function GridLines() {
  return (
    <g className="hero-grid-lines">
      {[45, 85, 125, 165, 205].map((y) => <line key={y} x1="20" y1={y} x2="600" y2={y} />)}
    </g>
  );
}