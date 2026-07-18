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
          <div className="eyebrow">Features</div>

          <h2 className="section-heading">
            Everything You Need to Run Your Business With Clarity
          </h2>

          <p className="section-copy">
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