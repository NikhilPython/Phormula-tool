// // import React from "react";

// // interface BreadcrumbProps {
// //   pageTitle: string;
// //   variant?: "page" | "table";
// //   align?: "center" | "left" | "right";
// //   className?: string;
// //   textSize?: "sm" | "base" | "lg" | "xl" | "2xl" | "3xl";
// // }

// // const PageBreadcrumb: React.FC<BreadcrumbProps> = ({
// //   pageTitle,
// //   variant = "page",
// //   align = "center",
// //   className = "",
// //   textSize = "2xl",
// // }) => {
// //   const colorByVariant: Record<NonNullable<BreadcrumbProps["variant"]>, string> = {
// //     page: "text-charcoal-500 dark:text-white/90",
// //     table: "text-green-500 dark:text-[#cbd5e1]",
// //   };

// //   const alignWrap: Record<NonNullable<BreadcrumbProps["align"]>, string> = {
// //     center: "items-center justify-center text-center",
// //     left: "items-start justify-start text-left",
// //     right: "items-end justify-end text-right",
// //   };

// //   const sizeMap: Record<NonNullable<BreadcrumbProps["textSize"]>, string> = {
// //     sm: "text-sm",
// //     base: "text-base",
// //     lg: "text-lg",
// //     xl: "text-xl",
// //     "2xl": "text-2xl",
// //     "3xl": "text-3xl",
// //   };

// //   return (
// //     <div className={`flex flex-col ${alignWrap[align]} mb-4`}>
// //       <h2
// //         className={`font-bold ${sizeMap[textSize]} ${colorByVariant[variant]} ${className}`}
// //         dangerouslySetInnerHTML={{ __html: pageTitle }}
// //       />
// //     </div>
// //   );
// // };

// // export default PageBreadcrumb;



// import React from "react";

// interface BreadcrumbProps {
//   pageTitle: string;
//   variant?: "page" | "table";
//   align?: "center" | "left" | "right";
//   className?: string;
//   textSize?: "sm" | "base" | "lg" | "xl" | "2xl" | "3xl";
// }

// const PageBreadcrumb: React.FC<BreadcrumbProps> = ({
//   pageTitle,
//   variant = "page",
//   align = "center",
//   className = "",
//   textSize = "2xl",
// }) => {
//   const colorByVariant: Record<
//     NonNullable<BreadcrumbProps["variant"]>,
//     string
//   > = {
//     page: "text-charcoal-500 dark:text-white/90",
//     table: "text-green-500 dark:text-[#cbd5e1]",
//   };

//   // use plain text-align classes, no flex
//   const alignClassMap: Record<
//     NonNullable<BreadcrumbProps["align"]>,
//     string
//   > = {
//     center: "text-center",
//     left: "text-left",
//     right: "text-right",
//   };

//   // const responsiveSizeMap: Record<
//   //   NonNullable<BreadcrumbProps["textSize"]>,
//   //   string
//   // > = {
//   //   sm: "text-xs sm:text-sm md:text-sm",
//   //   base: "text-sm sm:text-base md:text-base",
//   //   lg: "text-base sm:text-lg md:text-lg",
//   //   xl: "text-lg sm:text-xl md:text-xl",
//   //   "2xl": "text-lg sm:text-2xl md:text-2xl ",
//   //   "3xl": "text-lg sm:text-2xl md:text-3xl",
//   // };

//    const responsiveSizeMap: Record<
//   NonNullable<BreadcrumbProps["textSize"]>,
//   string
// > = {
//   sm:  "text-[11px] sm:text-xs lg:text-xs 2xl:text-sm",
//   base:"text-xs sm:text-sm lg:text-sm 2xl:text-base",
//   lg:  "text-sm sm:text-base lg:text-base 2xl:text-lg",
//   xl:  "text-base sm:text-lg lg:text-lg 2xl:text-xl",
//   "2xl":"text-base sm:text-xl lg:text-lg 2xl:text-2xl",
//   "3xl":"text-lg sm:text-2xl lg:text-xl 2xl:text-3xl",
// };

//   return (
//     <div className={`${alignClassMap[align]}`}>
//       <h2
//         className={`
//           inline-block          /* don’t stretch full width */
//           font-bold 
//           break-words 
//           whitespace-normal 
//           max-w-full 
//           ${responsiveSizeMap[textSize]} 
//           ${colorByVariant[variant]} 
//           ${className}
//         `}
//         dangerouslySetInnerHTML={{ __html: pageTitle }}
//       />
//     </div>
//   );
// };

// export default PageBreadcrumb;




import React from "react";

interface BreadcrumbProps {
  pageTitle: React.ReactNode; // ✅ changed from string
  variant?: "page" | "table";
  align?: "center" | "left" | "right";
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
    };

  const alignClassMap: Record<NonNullable<BreadcrumbProps["align"]>, string> = {
    center: "text-center",
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
        {pageTitle} {/* ✅ render normally */}
      </h2>
    </div>
  );
};

export default PageBreadcrumb;
