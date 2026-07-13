"use client";

import React, { forwardRef, useRef } from "react";
import Image from "next/image";

import { cn } from "@/lib/utils";
import { AnimatedBeam } from "@/components/ui/animated-beam";

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
                "relative z-10 flex items-center justify-center rounded-2xl border border-[#d9e8e2] bg-white shadow-[0_16px_45px_rgba(47,62,87,0.12)]",
                className
            )}
        >
            {children}
        </div>
    );
});

Circle.displayName = "Circle";

export function PhormulaBeamHero({ className }: { className?: string }) {
    const containerRef = useRef<HTMLDivElement>(null);

    const amazonRef = useRef<HTMLDivElement>(null);
    const salesRef = useRef<HTMLDivElement>(null);
    const inventoryRef = useRef<HTMLDivElement>(null);
    const forecastRef = useRef<HTMLDivElement>(null);

    const aiRef = useRef<HTMLDivElement>(null);
    const userRef = useRef<HTMLDivElement>(null);

    return (
        <section
            id="features"
            className={cn(
                " w-full overflow-hidden bg-white ",
                className
            )}
        >
            <div className="container center">
                {/* Heading */}
                <div className="mx-auto  flex w-full flex-col items-center text-center ">
                    <div className="eyebrow">
                        Platform
                    </div>

                    <h2 className="section-heading">
                        One connected system for your Amazon business
                    </h2>

                    <p className="section-copy">
                        Phormula connects live sales, historical data, inventory, ads,
                        profit metrics and forecasting into one AI-powered decision layer.
                    </p>
                </div>

                {/* Main Beam Area */}
                <div className="mx-auto flex w-full justify-center">
                    <div
                        ref={containerRef}
                        className="
              relative mx-auto  w-full max-w-[1180px]
              overflow-hidden h-[620px]
sm:h-[650px]
2xl:h-[680px]
            "
                    >
                        {/* Very soft glow only, no heavy background */}
                        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_52%_48%,rgba(94,166,142,0.08),transparent_38%)]" />
                        <div className="pointer-events-none absolute -right-24 -top-24 size-72 rounded-full bg-[#5EA68E]/5 blur-[90px]" />
                        <div className="pointer-events-none absolute -left-24 bottom-0 size-72 rounded-full bg-[#FDD36F]/7 blur-[90px]" />

                        {/* Left Nodes */}
                       {/* Left Nodes */}
<BeamNode
  ref={amazonRef}
  className="left-[7%] top-[8%]"
  icon="🛒"
  label="Amazon"
  caption="Connect account"
  tone="yellow"
/>

<BeamNode
  ref={salesRef}
  className="left-[7%] top-[30%]"
  icon="📈"
  label="Live Sales"
  caption="Revenue signals"
  tone="green"
/>

<BeamNode
  ref={inventoryRef}
  className="left-[7%] top-[52%]"
  icon="📦"
  label="Inventory"
  caption="Stock visibility"
  tone="green"
/>

<BeamNode
  ref={forecastRef}
  className="left-[7%] top-[74%]"
  icon="🔮"
  label="Forecasting"
  caption="Plan ahead"
  tone="blue"
/>

                        {/* Center Phormula */}
                        <div className="absolute left-1/2 top-1/2 z-20 -translate-x-1/2 -translate-y-1/2">
                            <div className="relative flex flex-col items-center">
                                <div className="absolute left-1/2 top-1/2 size-52 -translate-x-1/2 -translate-y-1/2 rounded-full bg-[#5EA68E]/8 blur-3xl" />
                                <div className="absolute left-1/2 top-1/2 size-44 -translate-x-1/2 -translate-y-1/2 rounded-full border border-[#5EA68E]/20 animate-[phormulaPulse_2.8s_ease-in-out_infinite]" />

                                <Circle
                                    ref={aiRef}
                                    className="
    relative size-20 rounded-[38px]
    border-[#5EA68E]/35 bg-[#f7fcfa]
    shadow-[0_22px_60px_rgba(94,166,142,0.14)]
    sm:size-26
  "
                                >
                                    <div className="flex  items-center justify-center ">
                                        <div className="relative h-12 w-12 sm:h-16 sm:w-16">
                                            <Image
                                                src="/Favicon2.png"
                                                alt="Phormula logo"
                                                fill
                                                priority
                                                className="object-contain animate-[phormulaFloat_2.6s_ease-in-out_infinite]"
                                            />
                                        </div>
                                    </div>
                                </Circle>

                                <div className="mt-20 text-center">
                                    <p className="text-xl font-bold leading-none text-[#24324a]">
                                        Phormula
                                    </p>
                                    <p className="mt-1 text-xs font-semibold text-[#66748a]">
                                        AI Insight Engine
                                    </p>
                                </div>
                            </div>
                        </div>

                        {/* Seller */}
                        <div className="absolute right-[8%] top-[52%] z-20 -translate-y-1/2">
                            <div className="relative flex flex-col items-center">
                                <div className="absolute left-1/2 top-12 size-40 -translate-x-1/2 rounded-full bg-[#2f3e57]/6 blur-3xl" />

                                <Circle
                                    ref={userRef}
                                    className="mx-auto size-32 rounded-[32px] border-[#d9e8e2] bg-white shadow-[0_18px_50px_rgba(47,62,87,0.12)] sm:size-26"
                                >
                                    <AnimatedIcon icon="👤" className="text-3xl sm:text-5xl" />
                                </Circle>

                                <div className="mt-8 w-[260px] rounded-[24px]  bg-white px-7 py-5 text-center ">
                                    <p className="text-base font-black leading-6 text-[#24324a]">
                                        Clear next steps for the seller
                                    </p>
                                    <p className="mt-3 text-xs font-medium leading-5 text-[#66748a]">
                                        What changed, why it matters, and what to do next.
                                    </p>
                                </div>
                            </div>
                        </div>

                        {/* Beams */}
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
                            toRef={userRef}
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
        tone?: "green" | "yellow" | "blue";
    }
>(({ icon, label, caption, className, tone = "green" }, ref) => {
    const toneClass =
        tone === "yellow"
            ? "border-[#fdd36f]/70 bg-[#fff9e8]"
            : tone === "blue"
                ? "border-[#75bbda]/45 bg-[#f3f9fc]"
                : "border-[#5EA68E]/40 bg-[#f2fbf7]";

    const dotClass =
        tone === "yellow"
            ? "bg-[#f4b83f]"
            : tone === "blue"
                ? "bg-[#75bbda]"
                : "bg-[#5EA68E]";

    return (
        <div
            className={cn(
                "absolute z-20 flex w-[800px] flex-col items-center gap-3 sm:w-[100px]",
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
                        "size-[60px] rounded-[26px] sm:size-[70px]",
                        toneClass
                    )}
                >
                    <AnimatedIcon icon={icon} className="text-[38px] sm:text-[32px]" />
                </Circle>
            </div>

            <div className="text-center">
                <p className="text-[13px] font-black leading-4 text-[#24324a] sm:text-sm">
                    {label}
                </p>

                {caption ? (
                    <p className="mt-0.5 text-[10px] font-semibold leading-3 text-[#66748a] sm:text-[11px]">
                        {caption}
                    </p>
                ) : null}
            </div>
        </div>
    );
});

BeamNode.displayName = "BeamNode";

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
                "inline-flex items-center justify-center animate-[phormulaFloat_2.6s_ease-in-out_infinite] text-[34px] leading-none drop-shadow-sm",
                className
            )}
        >
            {icon}
        </span>
    );
};