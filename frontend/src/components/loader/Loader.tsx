"use client";

import React, { useEffect, useState } from "react";

type LoaderProps = {
  src?: string;
  size?: number; // add this
  label?: string;
  roundedClass?: string;
  backgroundClass?: string;
  transparent?: boolean;
  className?: string;
  forceFallback?: boolean;
  fullscreen?: boolean;
  zIndex?: number;
  respectReducedMotion?: boolean;
};

export default function Loader({
  src = "/infinityNew.gif",
  size: customSize, // rename prop to avoid clash
  label = "Loading…",
  roundedClass = "rounded-2xl",
  backgroundClass = "",
  transparent = false,
  className = "",
  forceFallback = false,
  fullscreen = false,
  zIndex = 9999,
  respectReducedMotion = false,
}: LoaderProps) {
  const [shouldReduce, setShouldReduce] = useState(false);
  const [loadFailed, setLoadFailed] = useState(false);
  const [size, setSize] = useState(64);

  useEffect(() => {
    if (typeof window !== "undefined" && "matchMedia" in window) {
      const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
      const apply = () => setShouldReduce(mq.matches);
      apply();
      mq.addEventListener?.("change", apply);
      return () => mq.removeEventListener?.("change", apply);
    }
  }, []);

  useEffect(() => {
    if (customSize) {
      setSize(customSize);
      return;
    }

    if (typeof window === "undefined") return;

    const mq = window.matchMedia("(min-width: 1280px)");

    const apply = () => {
      setSize(mq.matches ? 200 : 100);
    };

    apply();
    mq.addEventListener?.("change", apply);
    return () => mq.removeEventListener?.("change", apply);
  }, [customSize]);

  const isVideo =
    typeof src === "string" &&
    (src.endsWith(".mp4") || src.endsWith(".webm") || src.endsWith(".ogg"));

  const showFallback =
    !src || forceFallback || loadFailed || (respectReducedMotion && shouldReduce);

  const Container: React.FC<React.PropsWithChildren> = ({ children }) => (
    <div
      role="status"
      aria-live="polite"
      aria-label={label}
      className={[
        "inline-flex items-center justify-center",
        transparent ? "" : backgroundClass,
        roundedClass,
        className,
      ].join(" ")}
      style={
        fullscreen
          ? {
              position: "fixed",
              inset: 0,
              width: "100vw",
              height: "100vh",
              zIndex,
              background: transparent ? "transparent" : undefined,
            }
          : {
              width: size,
              height: size,
              minWidth: size,
              minHeight: size,
            }
      }
    >
      {children}
    </div>
  );

  return (
    <Container>
      {showFallback ? (
        <div
          className="relative"
          style={{ width: size * 0.55, height: size * 0.55 }}
          aria-hidden
        >
          <div className="box-border w-full h-full rounded-full border-[3px] border-neutral-300 dark:border-neutral-700" />
          {!shouldReduce && !forceFallback && (
            <div className="box-border w-full h-full rounded-full border-[3px] border-transparent border-t-neutral-500 dark:border-t-neutral-200 animate-spin" />
          )}
        </div>
      ) : isVideo ? (
        <video
          src={src}
          width={size}
          height={size}
          muted
          loop
          autoPlay
          playsInline
          aria-hidden
          className={`${roundedClass} object-contain select-none pointer-events-none`}
          style={{ width: size * 0.82, height: size * 0.82 }}
          onError={() => setLoadFailed(true)}
        />
      ) : (
        <img
          src={src}
          width={size}
          height={size}
          alt=""
          draggable={false}
          aria-hidden
          className={`${roundedClass} object-contain select-none pointer-events-none`}
          style={{ width: size * 0.82, height: size * 0.82 }}
          onError={() => setLoadFailed(true)}
        />
      )}
    </Container>
  );
}