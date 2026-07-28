"use client";

import React, { forwardRef, useRef } from "react";
import Image from "next/image";

import { cn } from "@/lib/utils";
import { AnimatedBeam } from "@/components/ui/animated-beam";

type Tone = "green" | "yellow" | "blue";

const Circle = forwardRef<
  HTMLDivElement,
  {
    className?: string;
    children?: React.ReactNode;
  }
>(({ className, children }, ref) => {
  return (
    <div
      ref={ref}
      className={cn(
        "relative z-10 flex items-center justify-center",
        "rounded-2xl border border-[#D9E8E2] bg-white",
        "shadow-[0_16px_45px_rgba(47,62,87,0.12)]",
        className
      )}
    >
      {children}
    </div>
  );
});

Circle.displayName = "Circle";

export function PhormulaBeamHero({
  className,
}: {
  className?: string;
}) {
  const containerRef = useRef<HTMLDivElement>(null);

  const amazonRef = useRef<HTMLDivElement>(null);
  const salesRef = useRef<HTMLDivElement>(null);
  const inventoryRef = useRef<HTMLDivElement>(null);
  const forecastRef = useRef<HTMLDivElement>(null);

  const aiRef = useRef<HTMLDivElement>(null);
  const userRef = useRef<HTMLDivElement>(null);
  const userBeamAnchorRef = useRef<HTMLDivElement>(null);

  return (
    <section
      id="features"
      className={cn(
        "relative w-full overflow-hidden bg-white",
        "py-16 sm:py-20 lg:py-28 lg:pt-60  xl:pt-40 2xl:py-28",
        className
      )}
    >
      {/* Background decorations */}
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute left-1/2 top-[48%] h-[420px] w-[420px] -translate-x-1/2 -translate-y-1/2 rounded-full bg-[#5EA68E]/[0.05] blur-[110px]" />

        <div className="absolute -right-32 top-10 size-72 rounded-full bg-[#5EA68E]/[0.04] blur-[100px]" />

        <div className="absolute -left-32 bottom-0 size-72 rounded-full bg-[#FDD36F]/[0.06] blur-[100px]" />
      </div>

      <div className="relative mx-auto w-full max-w-[1512px] px-5 sm:px-8 lg:px-10 xl:px-12">
        {/* Heading */}
        <div className="mx-auto flex max-w-[920px] flex-col items-center text-center">
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
             Platform
            </div>

          <h2
            className={cn(
              "mt-4 max-w-[820px]",
              "font-[var(--font-dm-serif)]",
              "text-[32px] font-normal leading-[1.1] tracking-[-0.035em]",
              "text-[#37455F]",
              "sm:text-[38px]",
              "md:text-[42px]",
              "lg:text-[48px]",
              "2xl:max-w-[1000px] 2xl:text-[54px]"
            )}
          >
            One connected system for your Amazon business
          </h2>

          <p
            className={cn(
              "mx-auto mt-5 max-w-[620px]",
              "text-[14px] font-medium leading-[1.7] text-[#68748A]",
              "sm:text-[15px]",
              "lg:text-[16px] lg:leading-[1.75]"
            )}
          >
            Phormula connects live sales, historical data, inventory, ads,
            profit metrics and forecasting into one AI-powered decision layer.
          </p>
        </div>

        {/* Mobile layout */}
        <div className="mt-12 md:hidden">
          <MobileBeamLayout />
        </div>

        {/* Desktop and tablet layout */}
        <div className="mt-12 hidden w-full md:block lg:mt-14">
          <div
            ref={containerRef}
            className={cn(
              "relative mx-auto w-full",
              "h-[570px] max-w-[1180px]",
              "lg:h-[620px]",
              "xl:h-[660px]",
              "2xl:h-[680px] 2xl:max-w-[1380px]"
            )}
          >
            <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_52%_48%,rgba(94,166,142,0.08),transparent_39%)]" />

            {/* Left nodes */}
            <BeamNode
              ref={amazonRef}
              className="left-[3%] top-[5%] lg:left-[6%] lg:top-[7%] 2xl:left-[8%]"
              icon="🛒"
              label="Amazon"
              caption="Connect account"
              tone="yellow"
            />

            <BeamNode
              ref={salesRef}
              className="left-[3%] top-[29%] lg:left-[6%] lg:top-[30%] 2xl:left-[8%]"
              icon="📈"
              label="Live Sales"
              caption="Revenue signals"
              tone="green"
            />

            <BeamNode
              ref={inventoryRef}
              className="left-[3%] top-[53%] lg:left-[6%] lg:top-[53%] 2xl:left-[8%]"
              icon="📦"
              label="Inventory"
              caption="Stock visibility"
              tone="green"
            />

            <BeamNode
              ref={forecastRef}
              className="left-[3%] top-[77%] lg:left-[6%] lg:top-[76%] 2xl:left-[8%]"
              icon="🔮"
              label="Forecasting"
              caption="Plan ahead"
              tone="blue"
            />

            {/* Center Phormula engine */}
            <div className="absolute left-[50%] top-1/2 z-20 -translate-x-1/2 -translate-y-1/2">
              <div className="relative flex flex-col items-center">
                <div className="absolute left-1/2 top-[72px] size-44 -translate-x-1/2 -translate-y-1/2 rounded-full bg-[#5EA68E]/10 blur-3xl lg:size-52" />

                <div className="absolute left-1/2 top-[72px] size-36 -translate-x-1/2 -translate-y-1/2 rounded-full border border-[#5EA68E]/20 animate-[phormulaPulse_2.8s_ease-in-out_infinite] lg:size-44" />

                <Circle
                  ref={aiRef}
                  className={cn(
                    "relative size-[92px] rounded-[32px]",
                    "border-[#5EA68E]/35 bg-[#F7FCFA]",
                    "shadow-[0_22px_60px_rgba(94,166,142,0.14)]",
                    "lg:size-[112px] lg:rounded-[38px]",
                    "2xl:size-[120px]"
                  )}
                >
                  <div className="relative size-14 lg:size-16 2xl:size-[72px]">
                    <Image
                      src="/Favicon2.png"
                      alt="Phormula logo"
                      fill
                      priority
                      sizes="72px"
                      className="object-contain animate-[phormulaFloat_2.6s_ease-in-out_infinite]"
                    />
                  </div>
                </Circle>

                <div className="mt-6 text-center">
                  <p className="text-[18px] font-bold leading-none text-[#24324A] lg:text-xl">
                    Phormula
                  </p>

                  <p className="mt-2 text-[11px] font-semibold text-[#66748A] lg:text-xs">
                    AI Insight Engine
                  </p>
                </div>
              </div>
            </div>

            {/* Seller result */}
            <div className="absolute right-[2%] top-1/2 z-20 -translate-y-1/2 lg:right-[4%] 2xl:right-[6%]">
              <div className="relative flex w-[200px] flex-col items-center lg:w-[230px] xl:w-[250px]">
                <div className="absolute left-1/2 top-12 size-36 -translate-x-1/2 rounded-full bg-[#2F3E57]/[0.06] blur-3xl" />

                <div className="relative">
  {/* Beam connection point: icon ke exact left-center par */}
  <div
    ref={userBeamAnchorRef}
    className="
      pointer-events-none
      absolute left-0 top-1/2 z-0
      h-px w-px
      -translate-y-1/2
    "
  />

  <Circle
    ref={userRef}
    className={cn(
      "relative z-20 mx-auto",
      "size-[92px] rounded-[28px]",
      "border-[#D9E8E2] bg-white",
      "shadow-[0_18px_50px_rgba(47,62,87,0.12)]",
      "lg:size-[108px] lg:rounded-[32px]",
      "xl:size-[116px]"
    )}
  >
    <AnimatedIcon
      icon="👤"
      className="text-[34px] lg:text-[42px] xl:text-[46px]"
    />
  </Circle>
</div>

                <div
                  className={cn(
                    "mt-2 w-full rounded-[22px]",
                    "border border-[#37455F]/[0.06] bg-white",
                    "px-4 py-4 text-center",
                    "shadow-[0_14px_40px_rgba(47,62,87,0.07)]",
                    "lg:px-5 lg:py-1"
                  )}
                >
                  <p className="text-xs font-extrabold leading-[1.45] text-[#24324A] lg:text-sm xl:text-base">
                    Clear next steps for the seller
                  </p>
                </div>
              </div>
            </div>

            {/* Animated connections */}
            <AnimatedBeam
              containerRef={containerRef}
              fromRef={amazonRef}
              toRef={aiRef}
            />

            <AnimatedBeam
              containerRef={containerRef}
              fromRef={salesRef}
              toRef={aiRef}
            />

            <AnimatedBeam
              containerRef={containerRef}
              fromRef={inventoryRef}
              toRef={aiRef}
            />

            <AnimatedBeam
              containerRef={containerRef}
              fromRef={forecastRef}
              toRef={aiRef}
            />

            <AnimatedBeam
  containerRef={containerRef}
  fromRef={aiRef}
  toRef={userBeamAnchorRef}
/>
          </div>
        </div>
      </div>
    </section>
  );
}

const BeamNode = forwardRef<
  HTMLDivElement,
  {
    icon: string;
    label: string;
    caption?: string;
    className?: string;
    tone?: Tone;
  }
>(({ icon, label, caption, className, tone = "green" }, ref) => {
  const toneClass =
    tone === "yellow"
      ? "border-[#FDD36F]/70 bg-[#FFF9E8]"
      : tone === "blue"
        ? "border-[#75BBDA]/45 bg-[#F3F9FC]"
        : "border-[#5EA68E]/40 bg-[#F2FBF7]";

  const dotClass =
    tone === "yellow"
      ? "bg-[#F4B83F]"
      : tone === "blue"
        ? "bg-[#75BBDA]"
        : "bg-[#5EA68E]";

  return (
    <div
      className={cn(
        "absolute z-20 flex w-[105px] flex-col items-center gap-2.5",
        "lg:w-[120px] lg:gap-3",
        className
      )}
    >
      <div className="relative">
        <span
          className={cn(
            "absolute -right-1 -top-1 z-20 size-3 rounded-full border-2 border-white",
            dotClass
          )}
        />

        <Circle
          ref={ref}
          className={cn(
            "size-[58px] rounded-[20px]",
            "lg:size-[70px] lg:rounded-[26px]",
            "2xl:size-[74px]",
            toneClass
          )}
        >
          <AnimatedIcon
            icon={icon}
            className="text-[26px] lg:text-[31px] 2xl:text-[34px]"
          />
        </Circle>
      </div>

      <div className="text-center">
        <p className="text-[12px] font-extrabold leading-4 text-[#24324A] lg:text-sm">
          {label}
        </p>

        {caption && (
          <p className="mt-0.5 text-[9px] font-semibold leading-3 text-[#66748A] lg:text-[11px]">
            {caption}
          </p>
        )}
      </div>
    </div>
  );
});

BeamNode.displayName = "BeamNode";

function MobileBeamLayout() {
  const nodes: Array<{
    icon: string;
    label: string;
    caption: string;
    tone: Tone;
  }> = [
    {
      icon: "🛒",
      label: "Amazon",
      caption: "Connect account",
      tone: "yellow",
    },
    {
      icon: "📈",
      label: "Live Sales",
      caption: "Revenue signals",
      tone: "green",
    },
    {
      icon: "📦",
      label: "Inventory",
      caption: "Stock visibility",
      tone: "green",
    },
    {
      icon: "🔮",
      label: "Forecasting",
      caption: "Plan ahead",
      tone: "blue",
    },
  ];

  return (
    <div className="mx-auto max-w-[520px]">
      {/* Input cards */}
      <div className="grid grid-cols-2 gap-3 sm:gap-4">
        {nodes.map((node) => (
          <MobileInputCard key={node.label} {...node} />
        ))}
      </div>

      {/* Connector */}
      <div className="mx-auto flex h-10 w-px items-center justify-center bg-gradient-to-b from-[#5EA68E]/20 to-[#5EA68E]/60">
        <span className="size-2 rounded-full bg-[#5EA68E]" />
      </div>

      {/* Phormula card */}
      <div
        className={cn(
          "relative mx-auto flex w-full max-w-[330px] items-center gap-4",
          "rounded-[24px] border border-[#5EA68E]/25 bg-[#F7FCFA]",
          "p-4 shadow-[0_18px_50px_rgba(94,166,142,0.12)]"
        )}
      >
        <div className="absolute inset-0 rounded-[24px] bg-[#5EA68E]/[0.03]" />

        <Circle className="size-[72px] shrink-0 rounded-[24px] border-[#5EA68E]/35 bg-white">
          <div className="relative size-11">
            <Image
              src="/Favicon2.png"
              alt="Phormula logo"
              fill
              sizes="44px"
              className="object-contain animate-[phormulaFloat_2.6s_ease-in-out_infinite]"
            />
          </div>
        </Circle>

        <div className="relative min-w-0">
          <p className="text-[17px] font-bold text-[#24324A]">
            Phormula
          </p>

          <p className="mt-1 text-[11px] font-semibold text-[#66748A]">
            AI Insight Engine
          </p>

          <p className="mt-2 text-[11px] leading-[1.5] text-[#68748A]">
            Turns connected business data into clear actions.
          </p>
        </div>
      </div>

      {/* Connector */}
      <div className="mx-auto flex h-10 w-px items-center justify-center bg-gradient-to-b from-[#5EA68E]/60 to-[#37455F]/20">
        <span className="size-2 rounded-full bg-[#37455F]" />
      </div>

      {/* Result card */}
      <div
        className={cn(
          "mx-auto flex w-full max-w-[360px] items-center gap-4",
          "rounded-[24px] border border-[#37455F]/10 bg-white",
          "p-4 shadow-[0_18px_50px_rgba(47,62,87,0.09)]"
        )}
      >
        <Circle className="size-[64px] shrink-0 rounded-[21px]">
          <AnimatedIcon icon="👤" className="text-[30px]" />
        </Circle>

        <div className="min-w-0">
          <p className="text-[14px] font-extrabold leading-[1.4] text-[#24324A]">
            Clear next steps for the seller
          </p>

          <p className="mt-1.5 text-[11px] font-medium leading-[1.55] text-[#66748A]">
            What changed, why it matters, and what to do next.
          </p>
        </div>
      </div>
    </div>
  );
}

function MobileInputCard({
  icon,
  label,
  caption,
  tone,
}: {
  icon: string;
  label: string;
  caption: string;
  tone: Tone;
}) {
  const toneClass =
    tone === "yellow"
      ? "border-[#FDD36F]/60 bg-[#FFF9E8]"
      : tone === "blue"
        ? "border-[#75BBDA]/40 bg-[#F3F9FC]"
        : "border-[#5EA68E]/35 bg-[#F2FBF7]";

  return (
    <div
      className={cn(
        "flex min-h-[116px] flex-col items-center justify-center",
        "rounded-[20px] border px-3 py-4 text-center",
        toneClass
      )}
    >
      <AnimatedIcon icon={icon} className="text-[27px]" />

      <p className="mt-2 text-[12px] font-extrabold text-[#24324A]">
        {label}
      </p>

      <p className="mt-1 text-[9px] font-semibold text-[#66748A]">
        {caption}
      </p>
    </div>
  );
}

const AnimatedIcon = ({
  icon,
  className,
}: {
  icon: string;
  className?: string;
}) => {
  return (
    <span
      className={cn(
        "inline-flex items-center justify-center",
        "animate-[phormulaFloat_2.6s_ease-in-out_infinite]",
        "leading-none drop-shadow-sm",
        className
      )}
    >
      {icon}
    </span>
  );
};