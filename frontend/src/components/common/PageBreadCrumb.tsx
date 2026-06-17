import React from "react";

interface BreadcrumbProps {
  pageTitle: React.ReactNode;
  variant?: "page" | "table" | "superadmin";
  align?: "center" | "left" | "right" | "center2";
  className?: string;
  textSize?: "sm" | "base" | "lg" | "xl" | "2xl" | "3xl";
}

const PageBreadcrumb: React.FC<BreadcrumbProps> = ({
  pageTitle,
  variant = "page",
  align = "center",
  className = "",
  textSize = "2xl",
}) => {
  const colorByVariant: Record<NonNullable<BreadcrumbProps["variant"]>, string> =
    {
      page: "text-charcoal-500 dark:text-white/90",
      table: "text-green-500 dark:text-[#cbd5e1]",

      // New variant for Super Admin dark theme
      superadmin: "text-white",
    };

  const alignClassMap: Record<NonNullable<BreadcrumbProps["align"]>, string> = {
    center: "text-left sm:text-center",
    center2: "text-center",
    left: "text-left",
    right: "text-right",
  };

  const responsiveSizeMap: Record<
    NonNullable<BreadcrumbProps["textSize"]>,
    string
  > = {
    sm: "text-[11px] sm:text-xs lg:text-xs 2xl:text-sm",
    base: "text-xs sm:text-sm lg:text-sm 2xl:text-base",
    lg: "text-sm sm:text-base lg:text-base 2xl:text-lg",
    xl: "text-base sm:text-lg lg:text-lg 2xl:text-xl",
    "2xl": "text-base sm:text-xl lg:text-lg 2xl:text-2xl",
    "3xl": "text-lg sm:text-2xl lg:text-xl 2xl:text-3xl",
  };

  return (
    <div className={alignClassMap[align]}>
      <h2
        className={`
          inline-block
          font-bold
          break-words
          whitespace-normal
          max-w-full
          ${responsiveSizeMap[textSize]}
          ${colorByVariant[variant]}
          ${className}
        `}
      >
        {pageTitle}
      </h2>
    </div>
  );
};

export default PageBreadcrumb;