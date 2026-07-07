// Loader.tsx
import React from "react";

type LoaderProps = {
  src?: string;
  size?: number;
  label?: string;
  transparent?: boolean;
  className?: string;
};

const Loader: React.FC<LoaderProps> = ({
  src = "/loader/infinity-unscreen.gif",
  size = 80,
  label = "Loading…",
  transparent = false,
  className = "",
}) => {
  return (
    <div
      role="status"
      aria-live="polite"
      aria-label={label}
      className={[
        "inline-flex items-center justify-center",
        transparent ? "" : "bg-neutral-100",
        "rounded-2xl shadow-sm border border-black/5",
        className,
      ]
        .filter(Boolean)
        .join(" ")}
      style={{
        width: size,
        height: size,
        minWidth: size,
        minHeight: size,
      }}
    >
      <img
        src={src}
        width={size}
        height={size}
        alt={label}
        draggable={false}
        className="object-contain select-none pointer-events-none"
        style={{ width: size * 0.82, height: size * 0.82 }}
      />
    </div>
  );
};

export default Loader;
