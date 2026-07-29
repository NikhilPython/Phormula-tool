"use client";

import { motion, useReducedMotion } from "framer-motion";
import {
  Link2,
  ChartNoAxesCombined,
  Sparkles,
  ArrowRight,
  ArrowDown,
} from "lucide-react";

import { containerClass, SectionCopy } from "./shared";

const steps = [
  {
    title: "Connect your Business Data",
    text: "Bring sales, fees, ads, inventory, expenses, purchase orders, and cash-flow inputs into one system.",
    icon: Link2,
    iconColor: "#E0A800",
    iconBackground: "#FFF8D9",
    delay: 0,
  },
  {
    title: "Reconcile and Visualize",
    text: "Phormula turns scattered inputs into dashboards for revenue, CM2 profit, cash flow, inventory, and performance.",
    icon: ChartNoAxesCombined,
    iconColor: "#6959C9",
    iconBackground: "#F0EDFF",
    delay: 0.55,
  },
  {
    title: "Act on AI Recommendations",
    text: "Get clear alerts and next steps when inventory, fees, margin, or cash flow needs attention.",
    icon: Sparkles,
    iconColor: "#288F7A",
    iconBackground: "#E9F8F3",
    delay: 1.1,
  },
] as const;

function DesktopConnector({
  delay,
  reduceMotion,
}: {
  delay: number;
  reduceMotion: boolean | null;
}) {
  return (
    <div className="relative hidden min-w-0 items-center md:flex">
      <div className="relative h-[2px] w-full overflow-hidden rounded-full bg-[#DCE1E8]">
        <motion.div
          className="absolute inset-y-0 left-0 w-full origin-left rounded-full bg-[#7AAE9D]"
          initial={reduceMotion ? false : { scaleX: 0 }}
          whileInView={{ scaleX: 1 }}
          viewport={{ once: true, amount: 0.7 }}
          transition={{
            duration: reduceMotion ? 0 : 0.45,
            delay: reduceMotion ? 0 : delay,
            ease: [0.22, 1, 0.36, 1],
          }}
        />
      </div>

      <motion.div
        className="
          absolute right-[-7px] top-1/2 z-10
          grid h-7 w-7 -translate-y-1/2 place-items-center
          rounded-full border border-[#DDE3E8]
          bg-white text-[#6A7A8C]
          shadow-[0_5px_14px_rgba(38,54,83,0.10)]
        "
        initial={reduceMotion ? false : { opacity: 0, scale: 0.5, x: -8 }}
        whileInView={{ opacity: 1, scale: 1, x: 0 }}
        viewport={{ once: true, amount: 0.7 }}
        transition={{
          duration: reduceMotion ? 0 : 0.3,
          delay: reduceMotion ? 0 : delay + 0.28,
          ease: "easeOut",
        }}
      >
        <ArrowRight className="h-3.5 w-3.5" strokeWidth={2} />
      </motion.div>
    </div>
  );
}

function MobileConnector({
  delay,
  reduceMotion,
}: {
  delay: number;
  reduceMotion: boolean | null;
}) {
  return (
    <div className="relative flex h-[54px] justify-center md:hidden">
      <div className="relative h-full w-[2px] overflow-hidden rounded-full bg-[#DCE1E8]">
        <motion.div
          className="absolute inset-x-0 top-0 h-full origin-top rounded-full bg-[#7AAE9D]"
          initial={reduceMotion ? false : { scaleY: 0 }}
          whileInView={{ scaleY: 1 }}
          viewport={{ once: true, amount: 0.7 }}
          transition={{
            duration: reduceMotion ? 0 : 0.4,
            delay: reduceMotion ? 0 : delay,
            ease: [0.22, 1, 0.36, 1],
          }}
        />
      </div>

      <motion.div
        className="
          absolute bottom-[-2px] left-1/2 z-10
          grid h-7 w-7 -translate-x-1/2 place-items-center
          rounded-full border border-[#DDE3E8]
          bg-white text-[#6A7A8C]
          shadow-[0_5px_14px_rgba(38,54,83,0.10)]
        "
        initial={reduceMotion ? false : { opacity: 0, scale: 0.5, y: -8 }}
        whileInView={{ opacity: 1, scale: 1, y: 0 }}
        viewport={{ once: true, amount: 0.7 }}
        transition={{
          duration: reduceMotion ? 0 : 0.3,
          delay: reduceMotion ? 0 : delay + 0.25,
          ease: "easeOut",
        }}
      >
        <ArrowDown className="h-3.5 w-3.5" strokeWidth={2} />
      </motion.div>
    </div>
  );
}

export default function WorkflowSection() {
  const reduceMotion = useReducedMotion();

  return (
    <section
      id="workflow"
      className="
        relative overflow-hidden
        bg-[#F8F8F6]
        py-[72px]
        lg:py-20
        xl:py-[88px]
        2xl:py-24
      "
    >
      <div className={`${containerClass} relative z-10 text-center`}>
        {/* Heading section */}
        <div className="mx-auto max-w-[790px]">
          <motion.div
            initial={
              reduceMotion
                ? false
                : {
                    opacity: 0,
                    y: 12,
                  }
            }
            whileInView={{
              opacity: 1,
              y: 0,
            }}
            viewport={{ once: true, amount: 0.7 }}
            transition={{
              duration: reduceMotion ? 0 : 0.45,
              ease: "easeOut",
            }}
            className="
              inline-flex items-center
              rounded-full
              border border-[#269770]/20
              bg-[#EBF7F1]
              px-[13px] py-[7px]
              text-[10px] font-extrabold uppercase
              leading-none tracking-[0.1em]
              text-[#277D64]
            "
          >
            How It Works
          </motion.div>

          <motion.h2
            initial={
              reduceMotion
                ? false
                : {
                    opacity: 0,
                    y: 16,
                  }
            }
            whileInView={{
              opacity: 1,
              y: 0,
            }}
            viewport={{ once: true, amount: 0.7 }}
            transition={{
              duration: reduceMotion ? 0 : 0.5,
              delay: reduceMotion ? 0 : 0.08,
              ease: "easeOut",
            }}
            className="
              mt-[18px]
              font-[var(--font-dm-serif)]
              text-[34px]
              leading-[1.08]
              tracking-[-0.035em]
              text-[#263653]

              sm:text-[38px]
              lg:text-[42px]
              xl:text-[42px]
              2xl:text-[54px]
            "
          >
            From Raw Data to{" "}
            <span className="text-[#5EA68E]">
              Founder-Ready Decisions.
            </span>
          </motion.h2>

          <motion.div
            initial={
              reduceMotion
                ? false
                : {
                    opacity: 0,
                    y: 14,
                  }
            }
            whileInView={{
              opacity: 1,
              y: 0,
            }}
            viewport={{ once: true, amount: 0.7 }}
            transition={{
              duration: reduceMotion ? 0 : 0.5,
              delay: reduceMotion ? 0 : 0.15,
              ease: "easeOut",
            }}
          >
            <SectionCopy>
              Phormula helps teams move from manual reporting to an operating
              rhythm built around clean numbers and clear actions.
            </SectionCopy>
          </motion.div>
        </div>

        {/* Workflow cards */}
        <div
          className="
            mx-auto mt-10
            grid max-w-[1260px]
            grid-cols-1
            items-center

            md:grid-cols-[minmax(0,1fr)_54px_minmax(0,1fr)_54px_minmax(0,1fr)]

            lg:mt-11
            lg:grid-cols-[minmax(0,1fr)_66px_minmax(0,1fr)_66px_minmax(0,1fr)]

            xl:mt-12
            xl:grid-cols-[minmax(0,1fr)_78px_minmax(0,1fr)_78px_minmax(0,1fr)]

            2xl:mt-[52px]
            2xl:grid-cols-[minmax(0,1fr)_90px_minmax(0,1fr)_90px_minmax(0,1fr)]
          "
        >
          {steps.map((step, index) => {
            const Icon = step.icon;

            return (
              <div
                key={step.title}
                className="contents"
              >
                <motion.article
                  initial={
                    reduceMotion
                      ? false
                      : {
                          opacity: 0,
                          y: 34,
                          scale: 0.96,
                        }
                  }
                  whileInView={{
                    opacity: 1,
                    y: 0,
                    scale: 1,
                  }}
                  viewport={{
                    once: true,
                    amount: 0.45,
                  }}
                  transition={{
                    duration: reduceMotion ? 0 : 0.55,
                    delay: reduceMotion ? 0 : step.delay,
                    ease: [0.22, 1, 0.36, 1],
                  }}
                  whileHover={
                    reduceMotion
                      ? undefined
                      : {
                          y: -7,
                          transition: {
                            duration: 0.25,
                            ease: "easeOut",
                          },
                        }
                  }
                  className="
                    group relative
                    flex min-h-[220px]
                    flex-col items-center justify-center
                    overflow-hidden
                    rounded-[15px]
                    border border-[#273653]/[0.035]
                    bg-white
                    px-6 py-8
                    text-center

                    shadow-[0_8px_28px_rgba(38,54,83,0.035)]
                    transition-shadow duration-300

                    hover:shadow-[0_18px_45px_rgba(38,54,83,0.10)]

                    sm:min-h-[225px]
                    sm:px-7

                    md:min-h-[235px]
                    md:px-4
                    md:py-7

                    lg:min-h-[225px]
                    lg:px-6

                    xl:min-h-[230px]
                    xl:px-8

                    2xl:min-h-[250px]
                    2xl:rounded-[18px]
                    2xl:px-9
                    2xl:py-9
                  "
                >
                  {/* Animated active border */}
                  <motion.span
                    className="
                      absolute inset-x-0 top-0
                      h-[3px] origin-left
                      rounded-t-[15px]
                      bg-[#5EA68E]
                    "
                    initial={reduceMotion ? false : { scaleX: 0 }}
                    whileInView={{ scaleX: 1 }}
                    viewport={{ once: true, amount: 0.5 }}
                    transition={{
                      duration: reduceMotion ? 0 : 0.55,
                      delay: reduceMotion ? 0 : step.delay + 0.18,
                      ease: "easeOut",
                    }}
                  />

                  {/* Step number */}
                  <motion.span
                    initial={
                      reduceMotion
                        ? false
                        : {
                            opacity: 0,
                            scale: 0.6,
                          }
                    }
                    whileInView={{
                      opacity: 1,
                      scale: 1,
                    }}
                    viewport={{ once: true, amount: 0.5 }}
                    transition={{
                      duration: reduceMotion ? 0 : 0.3,
                      delay: reduceMotion ? 0 : step.delay + 0.2,
                    }}
                    className="
                      absolute right-4 top-4
                      grid h-6 w-6 place-items-center
                      rounded-full
                      bg-[#EFF6F3]
                      text-[10px] font-extrabold
                      text-[#4D8D78]
                    "
                  >
                    {index + 1}
                  </motion.span>

                  {/* Icon */}
                  <motion.div
                    initial={
                      reduceMotion
                        ? false
                        : {
                            opacity: 0,
                            rotate: -12,
                            scale: 0.65,
                          }
                    }
                    whileInView={{
                      opacity: 1,
                      rotate: 0,
                      scale: 1,
                    }}
                    viewport={{ once: true, amount: 0.5 }}
                    transition={{
                      duration: reduceMotion ? 0 : 0.45,
                      delay: reduceMotion ? 0 : step.delay + 0.12,
                      type: "spring",
                      stiffness: 170,
                      damping: 15,
                    }}
                    className="
                      mb-5 grid h-[46px] w-[46px]
                      place-items-center rounded-[13px]

                      transition-transform duration-300
                      group-hover:scale-110

                      2xl:mb-6
                      2xl:h-[52px]
                      2xl:w-[52px]
                      2xl:rounded-[15px]
                    "
                    style={{
                      color: step.iconColor,
                      backgroundColor: step.iconBackground,
                    }}
                  >
                    <Icon
                      className="
                        h-[24px] w-[24px]
                        2xl:h-[27px] 2xl:w-[27px]
                      "
                      strokeWidth={2}
                    />
                  </motion.div>

                  <h3
                    className="
                      text-[15px] font-bold
                      leading-[1.35]
                      tracking-[-0.015em]
                      text-[#34425E]

                      lg:text-[15px]
                      xl:text-[16px]
                      2xl:text-[17px]
                    "
                  >
                    {step.title}
                  </h3>

                  <p
                    className="
                      mt-3 max-w-[330px]
                      text-[12px]
                      leading-[1.45]
                      text-[#555B68]

                      lg:text-[12px]
                      xl:text-[13px]
                      xl:leading-[1.5]

                      2xl:mt-3.5
                      2xl:text-[14px]
                      text-left!
                    "
                  >
                    {step.text}
                  </p>

                  {/* Bottom progress dot */}
                  <motion.span
                    initial={
                      reduceMotion
                        ? false
                        : {
                            opacity: 0,
                            scale: 0,
                          }
                    }
                    whileInView={{
                      opacity: 1,
                      scale: 1,
                    }}
                    viewport={{ once: true, amount: 0.5 }}
                    transition={{
                      duration: reduceMotion ? 0 : 0.25,
                      delay: reduceMotion ? 0 : step.delay + 0.42,
                    }}
                    className="
                      absolute bottom-3
                      h-1.5 w-1.5
                      rounded-full bg-[#5EA68E]
                    "
                  />
                </motion.article>

                {index < steps.length - 1 && (
                  <>
                    <DesktopConnector
                      delay={step.delay + 0.45}
                      reduceMotion={reduceMotion}
                    />

                    <MobileConnector
                      delay={step.delay + 0.45}
                      reduceMotion={reduceMotion}
                    />
                  </>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
}