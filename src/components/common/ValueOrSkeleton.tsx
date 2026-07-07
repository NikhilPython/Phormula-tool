// components/common/ValueOrSkeleton.tsx
"use client";

import React from "react";
import Loader from "@/components/loader/Loader";

type Props = {
  loading: boolean;
  children: React.ReactNode;
  compact?: boolean;
  mode?: "replace" | "inline";
};

export default function ValueOrSkeleton({
  loading,
  children,
  compact = false,
  mode = "replace",
}: Props) {
  if (mode === "inline") {
    return (
      <span className="inline-flex items-center gap-1">
        {children}
        {/* {loading && (
          <Loader
            size={compact ? 16 : 20}
            transparent
            roundedClass="rounded-full"
            backgroundClass="bg-transparent"
            className="text-gray-400"
            forceFallback
          />
        )} */}
      </span>
    );
  }

  // if (loading) {
  //   return (
  //     <div className="inline-flex items-center justify-center">
  //       <Loader
  //         size={compact ? 28 : 36}
  //         transparent
  //         roundedClass="rounded-full"
  //         backgroundClass="bg-transparent"
  //         className="text-gray-400"
  //         forceFallback
  //       />
  //     </div>
  //   );
  // }
  return <>{children}</>;
}
