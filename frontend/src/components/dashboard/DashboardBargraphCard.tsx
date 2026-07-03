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

  const shouldShowAsPositiveBar = (label: string) => {
    return ["Tax & Credits", "Others", "Other Charges"].includes(label);
  };

  const normalizeBarValue = (label: string, value: unknown) => {
    const n = Math.round(Number(value ?? 0));

    return shouldShowAsPositiveBar(label)
      ? Math.abs(n)
      : n;
  };

  const normalizedValues = useMemo(() => {
    return normalizedLabels.map((label, i) =>
      normalizeBarValue(label, values?.[i])
    );
  }, [normalizedLabels, values]);

  const normalizedPrevValues = useMemo(() => {
    return normalizedLabels.map((label, i) =>
      normalizeBarValue(label, prevValues?.[i])
    );
  }, [normalizedLabels, prevValues]);

  const normalizedColors = useMemo(() => {
    return normalizedLabels.map((_, i) => colors?.[i] ?? "#75BBDA");
  }, [normalizedLabels, colors]);

  const prevColors = useMemo(() => {
  return normalizedLabels.map(() => "#D9D9D9");
}, [normalizedLabels]);

  const chartValues = useMemo(() => {
    if (previewMode) return normalizedLabels.map(() => 0);
    return normalizedValues;
  }, [previewMode, normalizedLabels, normalizedValues]);

  const chartPrevValues = useMemo(() => {
    if (previewMode) return normalizedLabels.map(() => 0);
    return normalizedPrevValues;
  }, [previewMode, normalizedLabels, normalizedPrevValues]);

  const derivedAllValuesZero = useMemo(() => {
    const currentAllZero = chartValues.every((v) => !Number(v));

    if (!expanded) {
      return currentAllZero;
    }

    const previousAllZero = chartPrevValues.every((v) => !Number(v));

    return currentAllZero && previousAllZero;
  }, [chartValues, chartPrevValues, expanded]);

  const isZeroState = previewMode || derivedAllValuesZero;

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
              currentLabel={`${formattedMonthYear} MTD`}
              prevLabel="Previous MTD"
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