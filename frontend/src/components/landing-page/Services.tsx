"use client";

import React, { useState } from "react";
import Image from "next/image";
import { serviceData } from "./constant";
import ServiceCard from "./ServiceCard";

const circleImage = [
  "/phormula-services/text/frame-full.svg",
  "/phormula-services/text/frame1.svg",
  "/phormula-services/text/frame2.svg",
  "/phormula-services/text/frame3.svg",
  "/phormula-services/text/frame4.svg",
  "/phormula-services/text/frame5.svg",
  "/phormula-services/text/frame6.svg",
];

export default function Services() {
  const [currentCircleImage, setCurrentCircleImage] = useState(0);

  return (
    <section id="platform-services" className="py-24 bg-[#FAFAF7] overflow-visible">
      <div className="mx-auto w-[min(1480px,calc(100%-80px))]">
        <div className="text-center mb-6">
          <div className="inline-flex items-center gap-2 rounded-full border border-[#5EA68E38] bg-[#5EA68E1A] px-4 py-2 text-xs font-extrabold uppercase tracking-[0.12em] text-[#4A8A74]">
            Platform
          </div>

          <h2 className="mt-4 text-5xl font-extrabold tracking-tight text-[#37455F]">
            One operating circle for your business
          </h2>

          <p className="mx-auto mt-4 max-w-2xl text-base leading-7 text-[#5A6272]">
            Phormula brings finance, inventory, fees, cash flow, AI insights, and profit actions into one connected dashboard.
          </p>
        </div>

        <div className="relative flex items-center justify-center">
          <Image
            src="/phormula-services/services-circle-card.svg"
            alt="Phormula services circle"
            width={670}
            height={670}
            className="relative"
          />

          <Image
            src={circleImage[currentCircleImage]}
            alt="circle text"
            width={570}
            height={570}
            className="absolute ml-6 -mt-6 pointer-events-none"
          />

          <div className="absolute h-[670px] w-[670px]">
            <div>
              <Image
                src="/phormula-services/dashboard-analysis.svg"
                alt="Financial analysis"
                width={120}
                height={120}
                className="absolute left-[23%] top-[25%] h-[120px] w-[120px] transition-all duration-700 ease-in-out hover:left-[21%] hover:top-[20%] hover:scale-125 hover:rotate-[-7deg]"
                onMouseLeave={() => setCurrentCircleImage(0)}
                onMouseEnter={() => setCurrentCircleImage(1)}
              />

              {currentCircleImage === 1 && (
                <div className="absolute -left-32 top-20">
                  <ServiceCard
                    description={serviceData[0].description}
                    title={serviceData[0].heading}
                  />
                </div>
              )}
            </div>

            <div>
              <Image
                src="/phormula-services/inventory-forecasting.svg"
                alt="Inventory forecasting"
                width={120}
                height={120}
                className="absolute right-1/3 top-[15%] h-[120px] w-[120px] transition-all duration-700 ease-in-out hover:right-[32.5%] hover:top-[14%] hover:scale-125 hover:rotate-[5deg]"
                onMouseLeave={() => setCurrentCircleImage(0)}
                onMouseEnter={() => setCurrentCircleImage(2)}
              />

              {currentCircleImage === 2 && (
                <div className="absolute right-0 top-0">
                  <ServiceCard
                    description={serviceData[1].description}
                    title={serviceData[1].heading}
                  />
                </div>
              )}
            </div>

            <div>
              <Image
                src="/phormula-services/ai-insights.svg"
                alt="AI business insights"
                width={120}
                height={120}
                className="absolute right-[17%] top-1/3 h-[120px] w-[120px] transition-all duration-700 ease-in-out hover:right-[13.5%] hover:top-[33%] hover:scale-125"
                onMouseLeave={() => setCurrentCircleImage(0)}
                onMouseEnter={() => setCurrentCircleImage(3)}
              />

              {currentCircleImage === 3 && (
                <div className="absolute -right-64 top-1/4">
                  <ServiceCard
                    description={serviceData[2].description}
                    title={serviceData[2].heading}
                  />
                </div>
              )}
            </div>

            <div>
              <Image
                src="/phormula-services/fee-reconciliation.svg"
                alt="Fee reconciliation"
                width={120}
                height={120}
                className="absolute bottom-1/4 right-1/4 h-[120px] w-[120px] transition-all duration-700 ease-in-out hover:right-[20%] hover:bottom-[23%] hover:scale-125 hover:rotate-[-2deg]"
                onMouseLeave={() => setCurrentCircleImage(0)}
                onMouseEnter={() => setCurrentCircleImage(4)}
              />

              {currentCircleImage === 4 && (
                <div className="absolute -right-48 bottom-[20%]">
                  <ServiceCard
                    description={serviceData[3].description}
                    title={serviceData[3].heading}
                  />
                </div>
              )}
            </div>

            <div>
              <Image
                src="/phormula-services/cashflow-visibility.svg"
                alt="Cash-flow visibility"
                width={120}
                height={120}
                className="absolute bottom-[16%] left-[37%] h-[120px] w-[120px] transition-all duration-700 ease-in-out hover:left-[35%] hover:bottom-[13%] hover:scale-125 hover:rotate-[-2deg]"
                onMouseLeave={() => setCurrentCircleImage(0)}
                onMouseEnter={() => setCurrentCircleImage(5)}
              />

              {currentCircleImage === 5 && (
                <div className="absolute -left-24 bottom-0">
                  <ServiceCard
                    description={serviceData[4].description}
                    title={serviceData[4].heading}
                  />
                </div>
              )}
            </div>

            <div>
              <Image
                src="/phormula-services/profit-recommendations.svg"
                alt="Profit recommendations"
                width={120}
                height={120}
                className="absolute bottom-[30%] left-[20%] h-[120px] w-[120px] transition-all duration-700 ease-in-out hover:left-[18%] hover:bottom-[30.5%] hover:scale-125 hover:rotate-[-2deg]"
                onMouseLeave={() => setCurrentCircleImage(0)}
                onMouseEnter={() => setCurrentCircleImage(6)}
              />

              {currentCircleImage === 6 && (
                <div className="absolute -left-36 bottom-1/4">
                  <ServiceCard
                    description={serviceData[5].description}
                    title={serviceData[5].heading}
                  />
                </div>
              )}
            </div>
          </div>

          <div className="absolute flex h-[300px] w-[300px] items-center justify-center rounded-full bg-white shadow-[0_20px_60px_rgba(55,69,95,0.10)]">
            <div className="text-center">
              <p className="text-5xl font-extrabold text-[#37455F]">
                Phormula
              </p>
              <p className="text-4xl font-extrabold text-[#5EA68E]">
                Platform
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}