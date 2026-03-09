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
};


const DashboardBargraphCard: React.FC<DashboardBargraphCardProps> = ({
  countryName,
  formattedMonthYear,
  currencySymbol,
  labels,
  values,
  prevValues,
  colors,
  loading,
  expanded,
  allValuesZero = false,
}) => {
  const titleCountry = useMemo(() => {
    const c = (countryName || "").toLowerCase();
    if (!c) return "";
    if (c === "global") return "Global";
    return c.toUpperCase(); // UK / US / CA
  }, [countryName]);

  const titleMonth = useMemo(() => {
    return (formattedMonthYear || "").trim();
  }, [formattedMonthYear]);

  const prevColors = useMemo(() => {
    if (!colors || colors.length !== labels.length) return [];

    // Add ~50% opacity to each hex color: #RRGGBB -> #RRGGBB80
    return colors.map((c) => (c.startsWith("#") && c.length === 7 ? `${c}4D` : c));
  }, [colors, labels.length]);


  return (
    <div className="relative w-full rounded-xl">
      <div
        className={
          allValuesZero && !loading
            ? "opacity-30 pointer-events-none"
            : "opacity-100"
        }
      >
        <div className="w-full h-[46vh] sm:h-[48vh] md:h-[50vh] 
                transition-opacity duration-300
                text-[10px] 2xl:text-xs">
          {loading ? (
            <div className="flex h-full items-center justify-center">
              <div className="flex items-center justify-center text-sm text-gray-500">
                Loading chart…
              </div>
            </div>
          ) : (
            // <SimpleBarChart
            //   labels={labels}
            //   values={values}
            //   colors={colors}
            //   // xTitle={formattedMonthYear}
            //   yTitle={`Amount (${currencySymbol})`}
            // />

            <SimpleBarChart
              labels={labels}
              values={values}
              prevValues={prevValues}
              colors={colors}
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
