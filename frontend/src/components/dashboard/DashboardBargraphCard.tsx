// // "use client";

// // import React, { useMemo } from "react";
// // import SimpleBarChart from "@/components/charts/SimpleBarChart";

// // type DashboardBargraphCardProps = {
// //   countryName: string;
// //   formattedMonthYear: string;
// //   currencySymbol: string;

// //   labels: string[];
// //   values: number[];
// //   prevValues: number[];
// //   colors?: string[];
// //   expanded?: boolean;
// //   loading: boolean;
// //   allValuesZero?: boolean;
// // };

// // const DashboardBargraphCard: React.FC<DashboardBargraphCardProps> = ({
// //   countryName,
// //   formattedMonthYear,
// //   currencySymbol,
// //   labels,
// //   values,
// //   prevValues,
// //   colors,
// //   loading,
// //   expanded,
// //   allValuesZero = false,
// // }) => {
// //   const titleCountry = useMemo(() => {
// //     const c = (countryName || "").toLowerCase();
// //     if (!c) return "";
// //     if (c === "global") return "Global";
// //     return c.toUpperCase(); // UK / US / CA
// //   }, [countryName]);

// //   const titleMonth = useMemo(() => {
// //     return (formattedMonthYear || "").trim();
// //   }, [formattedMonthYear]);

// //   const prevColors = useMemo(() => {
// //     if (!colors || colors.length !== labels.length) return [];

// //     // Add ~50% opacity to each hex color: #RRGGBB -> #RRGGBB80
// //     return colors.map((c) => (c.startsWith("#") && c.length === 7 ? `${c}4D` : c));
// //   }, [colors, labels.length]);


// //   return (
// //     <div className="relative w-full rounded-xl">
// //       <div
// //         className={
// //           allValuesZero && !loading
// //             ? "opacity-30 pointer-events-none"
// //             : "opacity-100"
// //         }
// //       >
// //         <div className="w-full h-[46vh] sm:h-[48vh] md:h-[50vh] 
// //                 transition-opacity duration-300
// //                 text-[10px] 2xl:text-xs">
// //           {loading ? (
// //             <div className="flex h-full items-center justify-center">
// //               <div className="flex items-center justify-center text-sm text-gray-500">
// //                 Loading chart…
// //               </div>
// //             </div>
// //           ) : (
// //             // <SimpleBarChart
// //             //   labels={labels}
// //             //   values={values}
// //             //   colors={colors}
// //             //   // xTitle={formattedMonthYear}
// //             //   yTitle={`Amount (${currencySymbol})`}
// //             // />

// //             <SimpleBarChart
// //               labels={labels}
// //               values={values}
// //               prevValues={prevValues}
// //               colors={colors}
// //               prevColors={prevColors}
// //               currentLabel="MTD"
// //               prevLabel="Last month till date"
// //               yTitle={`Amount (${currencySymbol})`}
// //               showPrev={expanded}
// //             />
// //           )}
// //         </div>
// //       </div>
// //     </div>
// //   );
// // };

// // export default DashboardBargraphCard;











// "use client";

// import React, { useMemo } from "react";
// import SimpleBarChart from "@/components/charts/SimpleBarChart";

// type DashboardBargraphCardProps = {
//   countryName: string;
//   formattedMonthYear: string;
//   currencySymbol: string;

//   labels: string[];
//   values: number[];
//   prevValues: number[];
//   colors?: string[];
//   expanded?: boolean;
//   loading: boolean;
//   allValuesZero?: boolean;
//   previewMode?: boolean;
// };

// const DashboardBargraphCard: React.FC<DashboardBargraphCardProps> = ({
//   countryName,
//   formattedMonthYear,
//   currencySymbol,
//   labels,
//   values,
//   prevValues,
//   colors,
//   loading,
//   expanded,
//   allValuesZero = false,
//   previewMode = false,
// }) => {
//   const titleCountry = useMemo(() => {
//     const c = (countryName || "").toLowerCase();
//     if (!c) return "";
//     if (c === "global") return "Global";
//     return c.toUpperCase();
//   }, [countryName]);

//   const titleMonth = useMemo(() => {
//     return (formattedMonthYear || "").trim();
//   }, [formattedMonthYear]);

//   const prevColors = useMemo(() => {
//     if (!colors || colors.length !== labels.length) return [];
//     return colors.map((c) => (c.startsWith("#") && c.length === 7 ? `${c}4D` : c));
//   }, [colors, labels.length]);

//   const chartValues = useMemo(() => {
//     if (!previewMode) return values;
//     return labels.map(() => 0);
//   }, [previewMode, values, labels]);

//   const chartPrevValues = useMemo(() => {
//     if (!previewMode) return prevValues;
//     return labels.map(() => 0);
//   }, [previewMode, prevValues, labels]);

//   const isZeroState = previewMode || allValuesZero;

//   console.log("DashboardBargraphCard props:", {
//     countryName,
//     formattedMonthYear,
//     currencySymbol,
//     labels,
//     values,
//     prevValues,
//     colors,
//     loading,
//     expanded,
//     allValuesZero,
//     previewMode
//   });

//   return (
//     <div className="relative w-full rounded-xl">
//       <div
//         className={
//           isZeroState && !loading
//             ? "opacity-30 pointer-events-none"
//             : "opacity-100"
//         }
//       >
//         <div
//           className="w-full h-[46vh] sm:h-[48vh] md:h-[50vh]
//                 transition-opacity duration-300
//                 text-[10px] 2xl:text-xs"
//         >
//           {loading ? (
//             <div className="flex h-full items-center justify-center">
//               <div className="flex items-center justify-center text-sm text-gray-500">
//                 Loading chart…
//               </div>
//             </div>
//           ) : (
//             <SimpleBarChart
//               labels={labels}
//               values={chartValues}
//               prevValues={chartPrevValues}
//               colors={colors}
//               prevColors={prevColors}
//               currentLabel="MTD"
//               prevLabel="Last month till date"
//               yTitle={`Amount (${currencySymbol})`}
//               showPrev={expanded}
//             />
//           )}
//         </div>
//       </div>
//     </div>
//   );
// };

// export default DashboardBargraphCard;


















"use client";

import React, { useMemo } from "react";
import SimpleBarChart from "@/components/charts/SimpleBarChart";

type DashboardBargraphCardProps = {
  countryName: string;
  formattedMonthYear: string;
  currencySymbol: string;
  labels: string[];
  values: number[];
  prevValues: number[];
  colors?: string[];
  expanded?: boolean;
  loading: boolean;
  allValuesZero?: boolean;
  previewMode?: boolean;
};

const DashboardBargraphCard: React.FC<DashboardBargraphCardProps> = ({
  countryName,
  formattedMonthYear,
  currencySymbol,
  labels = [],
  values = [],
  prevValues = [],
  colors = [],
  loading,
  expanded = false,
  allValuesZero = false,
  previewMode = false,
}) => {
  const normalizedLabels = useMemo(() => labels ?? [], [labels]);

  const normalizedValues = useMemo(() => {
    return normalizedLabels.map((_, i) => Number(values?.[i] ?? 0));
  }, [normalizedLabels, values]);

  const normalizedPrevValues = useMemo(() => {
    return normalizedLabels.map((_, i) => Number(prevValues?.[i] ?? 0));
  }, [normalizedLabels, prevValues]);

  const normalizedColors = useMemo(() => {
    return normalizedLabels.map((_, i) => colors?.[i] ?? "#75BBDA");
  }, [normalizedLabels, colors]);

  const prevColors = useMemo(() => {
    return normalizedColors.map((c) =>
      c.startsWith("#") && c.length === 7 ? `${c}4D` : c
    );
  }, [normalizedColors]);

  const chartValues = useMemo(() => {
    if (previewMode) return normalizedLabels.map(() => 0);
    return normalizedValues;
  }, [previewMode, normalizedLabels, normalizedValues]);

  const chartPrevValues = useMemo(() => {
    if (previewMode) return normalizedLabels.map(() => 0);
    return normalizedPrevValues;
  }, [previewMode, normalizedLabels, normalizedPrevValues]);

  // derive zero state from actual values instead of trusting parent blindly
  const derivedAllValuesZero = useMemo(() => {
    return (
      chartValues.every((v) => !Number(v)) &&
      chartPrevValues.every((v) => !Number(v))
    );
  }, [chartValues, chartPrevValues]);

  const isZeroState = previewMode || derivedAllValuesZero;

  console.log("DashboardBargraphCard props:", {
    countryName,
    formattedMonthYear,
    currencySymbol,
    labels: normalizedLabels,
    values: normalizedValues,
    prevValues: normalizedPrevValues,
    colors: normalizedColors,
    loading,
    expanded,
    allValuesZeroFromParent: allValuesZero,
    derivedAllValuesZero,
    previewMode,
  });

  return (
    <div className="relative w-full rounded-xl">
      <div className={isZeroState && !loading ? "opacity-30" : "opacity-100"}>
        <div
          className="w-full h-[46vh] sm:h-[48vh] md:h-[50vh]
          transition-opacity duration-300 text-[10px] 2xl:text-xs"
        >
          {loading ? (
            <div className="flex h-full items-center justify-center">
              <div className="flex items-center justify-center text-sm text-gray-500">
                Loading chart…
              </div>
            </div>
          ) : (
            <SimpleBarChart
              labels={normalizedLabels}
              values={chartValues}
              prevValues={chartPrevValues}
              colors={normalizedColors}
              prevColors={prevColors}
              currentLabel="MTD"
              prevLabel="Last month till date"
              yTitle={`Amount (${currencySymbol})`}
              showPrev={expanded}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default DashboardBargraphCard;