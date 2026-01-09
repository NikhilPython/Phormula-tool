import React from "react";
import { BsStars } from "react-icons/bs";

type AiButtonProps = {
  onClick?: () => void;
  disabled?: boolean;
  loading?: boolean;
  children?: React.ReactNode;
  className?: string;
};

export const AiButton: React.FC<AiButtonProps> = ({
  onClick,
  disabled = false,
  loading = false,
  children = "AI Insights",
  className = "",
}) => {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={[
        "relative overflow-hidden",
        "bg-custom-effect",
        "rounded-lg p-2", // ✅ updated
        "text-nowrap flex items-center gap-1 justify-end",
        "text-[#F8EDCE]",

        "transition-all duration-200 ease-out",
        "hover:-translate-y-[2px] hover:shadow-lg",
        "active:translate-y-0 active:shadow-md",

        "disabled:opacity-50 disabled:cursor-not-allowed",
        "disabled:transform-none disabled:shadow-none",

        className,
      ].join(" ")}
      // style={{ boxShadow: "0px 4px 4px 0px #00000040" }}
    >
      {/* Base brand gradient */}
      <span className="ai-brand-bg" aria-hidden="true" />

      {/* Animated AI gradient */}
      <span className="ai-animated-bg" aria-hidden="true" />

      {/* Shimmer overlay */}
      <span className="ai-shimmer" aria-hidden="true" />

      {/* Content */}
      <span className="relative z-10 flex items-center gap-1 text-xs 2xl:text-sm">
        <BsStars
          className="text-xs 2xl:text-sm text-yellow-200"
          style={{ color: "#F8EDCE" }}
        />
        {loading ? "Generating..." : children}
      </span>

      <style jsx>{`
        .bg-custom-effect {
          background-image: linear-gradient(99.81deg, #5ea68e 0%, #37455f 97.72%);
        }

        .ai-brand-bg {
          position: absolute;
          inset: 0;
          background-image: linear-gradient(99.81deg, #5ea68e 0%, #37455f 97.72%);
          z-index: 0;
          border-radius: 0.5rem; /* ✅ matches rounded-lg */
        }

        .ai-animated-bg {
          position: absolute;
          inset: 0;
          z-index: 1;
          pointer-events: none;

          background-image: linear-gradient(
            45deg,
            #5ea68e 0%,
            #4f8f7b 22%,
            #37455f 55%,
            #4f8f7b 78%,
            #5ea68e 100%
          );

          background-size: 320% 320%;
          animation: ai-gradient-diagonal 3s linear infinite;

          opacity: 1;
          mix-blend-mode: normal;

          border-radius: 0.5rem; /* ✅ matches rounded-2xl */
        }

        .ai-shimmer {
          position: absolute;
          inset: -40%;
          background: linear-gradient(
            115deg,
            transparent 42%,
            rgba(248, 237, 206, 0.22) 50%,
            transparent 58%
          );
          transform: translateX(-45%) rotate(12deg);
          animation: ai-shimmer-sweep 1.5s ease-in-out infinite;
          mix-blend-mode: overlay;
          pointer-events: none;
          z-index: 2;
        }

        button:hover .ai-animated-bg {
          filter: brightness(1.08) saturate(1.2);
          opacity: 0.45;
        }

        button:disabled .ai-animated-bg,
        button:disabled .ai-shimmer {
          animation: none;
        }

        @keyframes ai-gradient-diagonal {
          0% {
            background-position: 0% 100%;
          }
          100% {
            background-position: 100% 0%;
          }
        }

        @keyframes ai-shimmer-sweep {
          0% {
            transform: translateX(-55%) rotate(12deg);
            opacity: 0.3;
          }
          50% {
            opacity: 0.55;
          }
          100% {
            transform: translateX(55%) rotate(12deg);
            opacity: 0.3;
          }
        }
      `}</style>
    </button>
  );
};
