// // "use client";

// // import React, { useEffect, useState } from "react";

// // type LoaderProps = {
// //   src?: string;                          // e.g. "/infinity-unscreen.gif" from /public
// //   size?: number;
// //   label?: string;
// //   roundedClass?: string;
// //   backgroundClass?: string;
// //   transparent?: boolean;
// //   className?: string;
// //   forceFallback?: boolean;
// //   fullscreen?: boolean;
// //   zIndex?: number;
// //   /** Show spinner instead of GIF/video when user prefers reduced motion */
// //   respectReducedMotion?: boolean;
// // };

// // export default function Loader({
// //   // src = "/infinity-unscreen.gif",       
// //   src = "/infinitySmall.wmov",       
// //   size = 80,
// //   label = "Loading…",
// //   roundedClass = "rounded-2xl",
// //   backgroundClass = "bg-neutral-100 dark:bg-neutral-900/70 backdrop-blur",
// //   transparent = false,
// //   className = "",
// //   forceFallback = false,
// //   fullscreen = false,
// //   zIndex = 9999,
// //   respectReducedMotion = false,
// // }: LoaderProps) {
// //   const [shouldReduce, setShouldReduce] = useState(false);
// //   const [loadFailed, setLoadFailed] = useState(false);

// //   // Detect reduced motion on the client (avoid hydration mismatch)
// //   useEffect(() => {
// //     if (typeof window !== "undefined" && "matchMedia" in window) {
// //       const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
// //       const apply = () => setShouldReduce(mq.matches);
// //       apply();
// //       mq.addEventListener?.("change", apply);
// //       return () => mq.removeEventListener?.("change", apply);
// //     }
// //   }, []);

// //   const isVideo =
// //     typeof src === "string" &&
// //     (src.endsWith(".mp4") || src.endsWith(".webm") || src.endsWith(".ogg"));

// //   const showFallback =
// //     !src ||
// //     forceFallback ||
// //     loadFailed ||
// //     (respectReducedMotion && shouldReduce);

// //   const Container: React.FC<React.PropsWithChildren> = ({ children }) => (
// //     <div
// //       role="status"
// //       aria-live="polite"
// //       aria-label={label}
// //       className={[
// //         "inline-flex items-center justify-center",
// //         transparent ? "" : backgroundClass,
// //         roundedClass,
// //         // "shadow-sm border border-black/5 dark:border-white/5",
// //         className,
// //       ].join(" ")}
// //       style={
// //         fullscreen
// //           ? {
// //               position: "fixed",
// //               inset: 0,
// //               width: "100vw",
// //               height: "100vh",
// //               minWidth: 0,
// //               minHeight: 0,
// //               zIndex,
// //               background: transparent ? "transparent" : "rgba(0,0,0,0.35)",
// //             }
// //           : {
// //               width: size,
// //               height: size,
// //               minWidth: size,
// //               minHeight: size,
// //             }
// //       }
// //     >
// //       {children}
// //     </div>
// //   );

// //   return (
// //     <Container>
// //       {showFallback ? (
// //         <div
// //           className="relative"
// //           style={{ width: size * 0.55, height: size * 0.55 }}
// //           aria-hidden
// //         >
// //           <div className="box-border w-full h-full rounded-full border-[3px] border-neutral-300 dark:border-neutral-700" />
// //           {/* no spin if shouldReduce */}
// //           {!shouldReduce && !forceFallback && (
// //             <div className="box-border w-full h-full rounded-full border-[3px] border-transparent border-t-neutral-500 dark:border-t-neutral-200 animate-spin" />
// //           )}
// //         </div>
// //       ) : isVideo ? (
// //         <video
// //           src={src}
// //           width={size}
// //           height={size}
// //           muted
// //           loop
// //           autoPlay
// //           playsInline
// //           aria-hidden
// //           className={`${roundedClass} object-contain select-none pointer-events-none`}
// //           style={{ width: size * 0.82, height: size * 0.82 }}
// //           onError={() => setLoadFailed(true)}
// //         />
// //       ) : (
// //         <img
// //           src={src}
// //           width={size}
// //           height={size}
// //           alt=""
// //           draggable={false}
// //           aria-hidden
// //           className={`${roundedClass} object-contain select-none pointer-events-none`}
// //           style={{ width: size * 0.82, height: size * 0.82 }}
// //           onError={() => setLoadFailed(true)} // if GIF fails, show spinner
// //         />
// //       )}
// //     </Container>
// //   );
// // }



























// "use client";

// import React, { useEffect, useMemo, useState } from "react";

// type LoaderProps = {
//   src?: string; // e.g. "/infinity-unscreen.gif" from /public
//   /** Base size (used if responsive sizing is disabled) */
//   size?: number;

//   /** Responsive sizing (laptop vs desktop) */
//   responsive?: boolean;
//   /** px size when viewport is < desktopMinWidth */
//   laptopSize?: number;
//   /** px size when viewport is >= desktopMinWidth */
//   desktopSize?: number;
//   /** breakpoint for desktop in px */
//   desktopMinWidth?: number;

//   label?: string;
//   roundedClass?: string;
//   backgroundClass?: string;
//   transparent?: boolean;
//   className?: string;
//   forceFallback?: boolean;
//   fullscreen?: boolean;
//   zIndex?: number;
//   /** Show spinner instead of GIF/video when user prefers reduced motion */
//   respectReducedMotion?: boolean;
// };

// export default function Loader({
//   // src = "/infinity-unscreen.gif",
//   src = "/infinityNew.gif",
//   size = 80,

//   // responsive sizing defaults
//   responsive = true,
//   laptopSize = 64,
//   desktopSize = 96,
//   desktopMinWidth = 1280,

//   label = "Loading…",
//   roundedClass = "rounded-2xl",
//   backgroundClass = "bg-neutral-100 dark:bg-neutral-900/70 backdrop-blur",
//   transparent = false,
//   className = "",
//   forceFallback = false,
//   fullscreen = false,
//   zIndex = 9999,
//   respectReducedMotion = false,
// }: LoaderProps) {
//   const [shouldReduce, setShouldReduce] = useState(false);
//   const [loadFailed, setLoadFailed] = useState(false);
//   const [effectiveSize, setEffectiveSize] = useState(size);

//   // Detect reduced motion on the client (avoid hydration mismatch)
//   useEffect(() => {
//     if (typeof window !== "undefined" && "matchMedia" in window) {
//       const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
//       const apply = () => setShouldReduce(mq.matches);
//       apply();
//       mq.addEventListener?.("change", apply);
//       return () => mq.removeEventListener?.("change", apply);
//     }
//   }, []);

//   // Responsive sizing: laptop vs desktop
//   useEffect(() => {
//     if (!responsive) {
//       setEffectiveSize(size);
//       return;
//     }

//     if (typeof window === "undefined" || !("matchMedia" in window)) {
//       setEffectiveSize(size);
//       return;
//     }

//     const mq = window.matchMedia(`(min-width: ${desktopMinWidth}px)`);

//     const apply = () => {
//       setEffectiveSize(mq.matches ? desktopSize : laptopSize);
//     };

//     apply();
//     mq.addEventListener?.("change", apply);
//     return () => mq.removeEventListener?.("change", apply);
//   }, [responsive, size, laptopSize, desktopSize, desktopMinWidth]);

//   const isVideo = useMemo(() => {
//     return (
//       typeof src === "string" &&
//       (src.endsWith(".mp4") || src.endsWith(".webm") || src.endsWith(".ogg"))
//     );
//   }, [src]);

//   const showFallback =
//     !src || forceFallback || loadFailed || (respectReducedMotion && shouldReduce);

//   const Container: React.FC<React.PropsWithChildren> = ({ children }) => (
//     <div
//       role="status"
//       aria-live="polite"
//       aria-label={label}
//       className={[
//         "inline-flex items-center justify-center",
//         transparent ? "" : backgroundClass,
//         roundedClass,
//         // "shadow-sm border border-black/5 dark:border-white/5",
//         className,
//       ].join(" ")}
//       style={
//         fullscreen
//           ? {
//               position: "fixed",
//               inset: 0,
//               width: "100vw",
//               height: "100vh",
//               minWidth: 0,
//               minHeight: 0,
//               zIndex,
//               background: transparent ? "transparent" : "rgba(0,0,0,0.35)",
//             }
//           : {
//               width: effectiveSize,
//               height: effectiveSize,
//               minWidth: effectiveSize,
//               minHeight: effectiveSize,
//             }
//       }
//     >
//       {children}
//     </div>
//   );

//   return (
//     <Container>
//       {showFallback ? (
//         <div
//           className="relative"
//           style={{ width: effectiveSize * 0.55, height: effectiveSize * 0.55 }}
//           aria-hidden
//         >
//           <div className="box-border w-full h-full rounded-full border-[3px] border-neutral-300 dark:border-neutral-700" />
//           {/* no spin if shouldReduce */}
//           {!shouldReduce && !forceFallback && (
//             <div className="box-border w-full h-full rounded-full border-[3px] border-transparent border-t-neutral-500 dark:border-t-neutral-200 animate-spin" />
//           )}
//         </div>
//       ) : isVideo ? (
//         <video
//           src={src}
//           width={effectiveSize}
//           height={effectiveSize}
//           muted
//           loop
//           autoPlay
//           playsInline
//           aria-hidden
//           className={`${roundedClass} object-contain select-none pointer-events-none`}
//           style={{ width: effectiveSize * 0.82, height: effectiveSize * 0.82 }}
//           onError={() => setLoadFailed(true)}
//         />
//       ) : (
//         <img
//           src={src}
//           width={effectiveSize}
//           height={effectiveSize}
//           alt=""
//           draggable={false}
//           aria-hidden
//           className={`${roundedClass} object-contain select-none pointer-events-none`}
//           style={{ width: effectiveSize * 0.82, height: effectiveSize * 0.82 }}
//           onError={() => setLoadFailed(true)} // if GIF fails, show spinner
//         />
//       )}
//     </Container>
//   );
// }

























"use client";

import React, { useEffect, useState } from "react";

type LoaderProps = {
  src?: string;
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
  // src = "/infinity-unscreen.gif",
  src = "/infinityNew.gif",

  label = "Loading…",
  roundedClass = "rounded-2xl",
  backgroundClass = "bg-neutral-100 dark:bg-neutral-900/70 backdrop-blur",
  transparent = false,
  className = "",
  forceFallback = false,
  fullscreen = false,
  zIndex = 9999,
  respectReducedMotion = false,
}: LoaderProps) {
  const [shouldReduce, setShouldReduce] = useState(false);
  const [loadFailed, setLoadFailed] = useState(false);
  const [size, setSize] = useState(64); // laptop default

  // Detect reduced motion
  useEffect(() => {
    if (typeof window !== "undefined" && "matchMedia" in window) {
      const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
      const apply = () => setShouldReduce(mq.matches);
      apply();
      mq.addEventListener?.("change", apply);
      return () => mq.removeEventListener?.("change", apply);
    }
  }, []);

  // Laptop vs Desktop sizing
  useEffect(() => {
    if (typeof window === "undefined") return;

    const mq = window.matchMedia("(min-width: 1280px)");

    const apply = () => {
      setSize(mq.matches ? 200 : 100);
    };

    apply();
    mq.addEventListener?.("change", apply);
    return () => mq.removeEventListener?.("change", apply);
  }, []);

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
              background: transparent ? "transparent" : "rgba(0,0,0,0.35)",
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
