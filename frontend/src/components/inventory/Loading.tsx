'use client';

import { useEffect, useState } from 'react';
import { FaCogs, FaChartLine, FaCalculator } from 'react-icons/fa';
import Loader from '@/components/loader/Loader';

export default function Loading() {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    let value = 0;

    const timer = setInterval(() => {
      value += Math.random() * 6;

      if (value >= 95) {
        value = 95;
      }

      setProgress(Math.floor(value));
    }, 700);

    return () => clearInterval(timer);
  }, []);

  return (
    <div
      className="
        relative
        flex
        min-h-[calc(100vh-150px)]
        w-full
        items-center
        justify-center
        bg-[#f8f9fa]
        px-4
        py-6
        lg:min-h-[calc(100vh-145px)]
        xl:min-h-[calc(100vh-150px)]
        2xl:min-h-[calc(100vh-165px)]
      "
    >
      <div
        className="
          w-full
          max-w-md
          rounded-2xl
          bg-white
          p-5
          text-center
          font-[Lato]
          shadow-[0px_8px_24px_rgba(0,0,0,0.10)]
          lg:max-w-[390px]
          lg:p-4
          xl:max-w-[420px]
          xl:p-5
          2xl:max-w-md
          2xl:p-6
        "
      >
        <div className="flex justify-center">
          <Loader transparent size={120} />
        </div>

        <div className="my-3 flex justify-center">
          <span className="animate-dot mx-1 h-2 w-2 rounded-full bg-[#5EA68E] delay-[0ms]" />
          <span className="animate-dot mx-1 h-2 w-2 rounded-full bg-[#5EA68E] delay-[200ms]" />
          <span className="animate-dot mx-1 h-2 w-2 rounded-full bg-[#5EA68E] delay-[400ms]" />
          <span className="animate-dot mx-1 h-2 w-2 rounded-full bg-[#5EA68E] delay-[600ms]" />
          <span className="animate-dot mx-1 h-2 w-2 rounded-full bg-[#5EA68E] delay-[800ms]" />
        </div>

        <div className="mt-4">
          <div className="h-2 w-full overflow-hidden rounded-full bg-[#E5E7EB]">
            <div
              className="h-full bg-[#5EA68E] transition-all duration-700"
              style={{ width: `${progress}%` }}
            />
          </div>

          <div className="mt-1 text-xs font-semibold text-[#414042]">
            {progress}% completed
          </div>
        </div>

        <h2 className="mt-3 text-xl text-[#414042] 2xl:text-2xl">
          Processing your data
        </h2>

        <p className="mb-3 text-sm text-[#414042]">
          Analyzing financial information securely
        </p>

        <div className="my-2 flex items-center justify-center rounded-md border border-[#D9D9D9] bg-[#D9D9D926] p-2.5 text-sm text-[#414042] 2xl:p-3">
          <FaCogs className="mr-2 shrink-0" />
          <span>Optimizing results for accuracy</span>
        </div>

        <div className="my-2 flex items-center justify-center rounded-md border border-[#D9D9D9] bg-[#D9D9D926] p-2.5 text-sm text-[#414042] 2xl:p-3">
          <FaChartLine className="mr-2 shrink-0" />
          <span>Processing market data and trends</span>
        </div>

        <div className="my-2 flex items-center justify-center rounded-md border border-[#D9D9D9] bg-[#D9D9D926] p-2.5 text-sm text-[#414042] 2xl:p-3">
          <FaCalculator className="mr-2 shrink-0" />
          <span>Running calculations and data analysis</span>
        </div>

        <p className="mt-4 text-sm text-[#414042]">
          Results will be available shortly.
        </p>

        <style jsx>{`
          @keyframes dotFlow {
            0% {
              opacity: 0.3;
              transform: translateY(0);
            }

            50% {
              opacity: 1;
              transform: translateY(-4px);
            }

            100% {
              opacity: 0.3;
              transform: translateY(0);
            }
          }

          .animate-dot {
            animation: dotFlow 1.4s infinite ease-in-out both;
          }
        `}</style>
      </div>
    </div>
  );
}