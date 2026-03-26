'use client';

import { useEffect, useState } from 'react';
import { FaCogs, FaChartLine, FaCalculator } from 'react-icons/fa';
import Loader from "@/components/loader/Loader";

export default function Loading() {
  const [progress, setProgress] = useState(0);

  // 🔥 Simulated smooth progress
  useEffect(() => {
    let value = 0;

    const timer = setInterval(() => {
      value += Math.random() * 6; // smooth increment
      if (value >= 95) value = 95; // stop at 95%
      setProgress(Math.floor(value));
    }, 700);

    return () => clearInterval(timer);
  }, []);

  const openExploreTab = () => {
    window.open('/', '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="fixed inset-0 flex items-center justify-center bg-[#f8f9fa]">
      <div className="bg-white rounded-2xl shadow-[0px_8px_24px_rgba(0,0,0,0.10)] p-6 w-full max-w-md text-center font-[Lato]">

        {/* Animated Logo */}
                   <Loader transparent size={120} />

        {/* Animated Dots */}
        <div className="flex justify-center my-3">
          <span className="w-2 h-2 mx-1 rounded-full bg-[#5EA68E] animate-dot delay-[0ms]"></span>
          <span className="w-2 h-2 mx-1 rounded-full bg-[#5EA68E] animate-dot delay-[200ms]"></span>
          <span className="w-2 h-2 mx-1 rounded-full bg-[#5EA68E] animate-dot delay-[400ms]"></span>
          <span className="w-2 h-2 mx-1 rounded-full bg-[#5EA68E] animate-dot delay-[600ms]"></span>
          <span className="w-2 h-2 mx-1 rounded-full bg-[#5EA68E] animate-dot delay-[800ms]"></span>
        </div>

        {/* 🔥 Progress Bar (added, layout unchanged) */}
        <div className="mt-4">
          <div className="h-2 w-full bg-[#E5E7EB] rounded-full overflow-hidden">
            <div
              className="h-full bg-[#5EA68E] transition-all duration-700"
              style={{ width: `${progress}%` }}
            />
          </div>
          <div className="text-xs text-[#414042] mt-1 font-semibold">
            {progress}% completed
          </div>
        </div>

        <h2 className="text-[#414042] text-2xl mt-3">
          Processing your data
        </h2>
        <p className="text-[#414042] text-sm mb-3">
          Analyzing financial information securely
        </p>

        <div className="bg-[#D9D9D926] rounded-md p-3 flex items-center justify-center text-[#414042] border border-[#D9D9D9] text-sm my-2">
          <FaCogs className="mr-2" /> Optimizing results for accuracy
        </div>

        <div className="bg-[#D9D9D926] rounded-md p-3 flex items-center justify-center text-[#414042] border border-[#D9D9D9] text-sm my-2">
          <FaChartLine className="mr-2" /> Processing market data and trends
        </div>

        <div className="bg-[#D9D9D926] rounded-md p-3 flex items-center justify-center text-[#414042] border border-[#D9D9D9] text-sm my-2">
          <FaCalculator className="mr-2" /> Running calculations and data analysis
        </div>

        <p className="text-[#414042] text-sm mt-4">
          Results will be available shortly.
        </p>

        {/* Buttons */}
        {/* <div className="flex justify-center gap-4 mt-5">
          <button
            onClick={() => alert('You will be notified.')}
            className="bg-[#37455F] text-[#f8edcf] px-4 py-2 rounded-md text-sm font-semibold hover:bg-[#34495e] shadow-[0px_4px_4px_0px_#00000040]"
          >
            Notify me
          </button>

          <button
            onClick={openExploreTab}
            className="bg-[#37455F] text-[#f8edcf] px-4 py-2 rounded-md text-sm font-semibold hover:bg-[#34495e] shadow-[0px_4px_4px_0px_#00000040]"
          >
            Till then explore tab
          </button>
        </div> */}

        {/* 🔥 Dot animation CSS (safe to keep here or move to globals.css) */}
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
