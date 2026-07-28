'use client';

import { projects } from './data';
import Card from './Card';
import { useScroll } from 'framer-motion';
import { useEffect, useRef } from 'react';
import Lenis from 'lenis';

export default function ScrollSlider() {
  const container = useRef<HTMLElement | null>(null);

  const { scrollYProgress } = useScroll({
    target: container,
    offset: ['start start', 'end end'],
  });

  useEffect(() => {
    const isMobile = window.matchMedia('(max-width: 640px)').matches;
    const isTouchDevice = window.matchMedia('(pointer: coarse)').matches;

    if (isMobile || isTouchDevice) {
      return;
    }

    const lenis = new Lenis();

    let rafId: number;

    function raf(time: number) {
      lenis.raf(time);
      rafId = requestAnimationFrame(raf);
    }

    rafId = requestAnimationFrame(raf);

    return () => {
      cancelAnimationFrame(rafId);
      lenis.destroy();
    };
  }, []);

  return (
    <section ref={container} className="features-scroll-section">
      {/* Sticky heading */}
      <div className="features-heading-wrapper">
        <div className="features-heading-content">
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
              Features
            </div>

          <h2 className="
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
    !text-[#263653]
  ">
            Everything You Need to Run Your Business With Clarity
          </h2>

          <p  className="
                mx-auto mt-4 max-w-[590px]
                text-[14px] leading-[1.7]
                text-[#68748a]

                md:text-[15px]
                2xl:mt-[18px]
                2xl:max-w-[610px]
                2xl:leading-[1.75]
              ">
            Track profitability, understand business performance, and take
            confident actions from one intelligent workspace.
          </p>
        </div>
      </div>

      {/* Existing card flow */}
      <div className="features-cards-wrapper">
        {projects.map((project, i) => {
          const targetScale = 1 - (projects.length - i) * 0.05;

          return (
            <Card
              key={`p_${i}`}
              i={i}
              {...project}
              progress={scrollYProgress}
              range={[i * 0.25, 1]}
              targetScale={targetScale}
            />
          );
        })}
      </div>
    </section>
  );
}