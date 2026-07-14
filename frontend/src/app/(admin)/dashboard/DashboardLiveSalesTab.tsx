"use client";

import React from "react";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import AmazonStatCard from "@/components/dashboard/AmazonStatCard";
import SalesTargetCard from "@/components/dashboard/SalesTargetCard";
import SalesTargetStatsCard from "@/components/dashboard/SalesTargetStatsCard";
import LiveBiLineGraph from "@/components/businessInsight/LiveBiLineChartPanel";
import DashboardRangePicker from "./DashboardRangePicker";

type GlobalMtdView = "global" | "uk" | "us";
type DashboardLiveSalesTabProps = Record<string, any>;

export default function DashboardLiveSalesTab({
    hasAmazonCard,
    platform,
    globalMtdViewOptions,
    globalMtdView,
    setGlobalMtdView,
    showLiveBI,
    isCountryMode,
    selectedStartDay,
    selectedEndDay,
    formatAppliedRangeLabel,
    isRangeChangeRef,
    setSelectedStartDay,
    setSelectedEndDay,
    setBiError,
    loadRangeFromCache,
    fetchLiveBiPayload,
    shouldShowDummyUi,
    dummyStatData,
    globalMtdCardData,
    safeDeltaPct,
    loading,
    shopifyLoading,
    biLoading,
    previousSkuwiseGlobalLoading,
    fmtInt,
    formatDisplayAmount,
    prevLabel,
    fmtPct2,
    deltaPctPoints,
    renderCountryMtdCards,
    useBiForAmazonCards,
    biCardKpis,
    mtdUnitsCurrent,
    mtdUnitsPrevious,
    mtdUnitsDelta,
    rangeActive,
    convertToDisplayCurrency,
    uk,
    amazonDataCurrency,
    prev,
    deltas,
    mtdCostOfAdsCurrentDisplay,
    mtdCostOfAdsPreviousDisplay,
    mtdCostOfAdsDelta,
    mtdTacosCurrent,
    mtdTacosPrevious,
    mtdTacosDelta,
    mtdCm2ProfitCurrentDisplay,
    mtdCm2ProfitPreviousDisplay,
    mtdCm2ProfitDelta,
    mtdCm2ProfitPctCurrent,
    mtdCm2ProfitPctPrevious,
    mtdCm2ProfitPctDelta,
    hasShopifyCard,
    shopify,
    shopifyDeriv,
    shopifyPrevDeriv,
    finalBiDailySeriesHome,
    finalBiPeriods,
    dashboardAllowedEndISO,
    biUiLoading,
    biError,
    currencySymbol,
    regions,
    targetRegion,
    setTargetRegion,
    displayCurrency,
    formatDisplayK,
    finalTargetsTodayHome,
    finalTargetsMtdHome,
    finalStatsTargetHome,
    finalTargetsLastMonthTotalHome,
    targets_lastMonthToDateHome,
    finalStatsSalesTrendPct,
    finalStatsTargetTrendPct,
    finalTargetsReimbursement,
    biAlignedTotalsHome,
    biCardsReady,
    formattedMonthYear,
    currentDisplayMonth,
    todaySalesRaw,
    stickyTargetHome,
    targets_mtdHome,
    targets_lastMonthTotalHome,
    targets_reimbursement,
    globalTargetCardTotals,
    salesTargetBiAlignedTotals,
    finalRangeCompletedPct,
    lastRefreshAt,
    dashboardCompletedTimeZone,
}: DashboardLiveSalesTabProps) {
    const RangePicker = DashboardRangePicker;

    return (
                    <div
                        id="live-sales"
                        className="grid grid-cols-12 gap-4 mt-2 md:mt-4 scroll-mt-[80px] items-start"
                    >
                        {/* LEFT COLUMN */}
                        <div
                            className="col-span-12 lg:col-span-8 order-2 lg:order-1 flex flex-col gap-4 min-w-0 h-auto min-h-0"
                        >
                            {/* AMAZON SECTION */}
                            {hasAmazonCard && (

                                <div className="flex flex-col gap-4 2xl:gap-4">
                                    {/* Amazon KPI Box */}
                                    <div className="w-full rounded-xl border bg-white p-3 2xl:p-5 shadow-sm">
                                        <div className="mb-3 lg:mb-2 2xl:mb-4 flex items-center justify-between gap-2 sm:gap-3">
                                            <div className="min-w-0">
                                                <PageBreadcrumb pageTitle="MTD Sales" variant="page" align="left" />
                                            </div>

                                            {/* RIGHT: TOGGLE (only for global) */}
                                            {platform === "global" && globalMtdViewOptions.length > 1 && (
                                                <SegmentedToggle<GlobalMtdView>
                                                    value={globalMtdView}
                                                    options={globalMtdViewOptions}
                                                    onChange={setGlobalMtdView}
                                                    compact
                                                    textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
                                                />
                                            )}

                                            {showLiveBI && isCountryMode && (
                                                <div className="ml-auto shrink-0">
                                                    <RangePicker
                                                        selectedStartDay={selectedStartDay}
                                                        selectedEndDay={selectedEndDay}
                                                        label={formatAppliedRangeLabel(selectedStartDay, selectedEndDay)}
                                                        onSubmit={async (s, e) => {

                                                            isRangeChangeRef.current = true;

                                                            setSelectedStartDay(s);
                                                            setSelectedEndDay(e);
                                                            setBiError(null);


                                                            const found = await loadRangeFromCache(
                                                                s,
                                                                e
                                                            );


                                                            if (found) {
                                                                return;
                                                            }


                                                            await fetchLiveBiPayload({
                                                                startDay: s,
                                                                endDay: e,
                                                                generateInsights: false,
                                                                skipLoader: true,
                                                                dataOnlyRefresh: false,
                                                            });

                                                        }}
                                                        onClear={() => {
                                                            setSelectedStartDay(null);
                                                            setSelectedEndDay(null);
                                                            setBiError(null);
                                                        }}
                                                        onCloseReset={() => {
                                                            setSelectedStartDay(null);
                                                            setSelectedEndDay(null);
                                                            setBiError(null);
                                                        }}
                                                    />
                                                </div>
                                            )}
                                        </div>

                                        {platform === "global" ? (
                                            globalMtdView === "global" ? (
                                                <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-2 lg:gap-2 2xl:gap-3 auto-rows-fr">
                                                    <AmazonStatCard
                                                        label="Units"
                                                        current={shouldShowDummyUi ? dummyStatData.units.current : globalMtdCardData.units}
                                                        previous={shouldShowDummyUi ? dummyStatData.units.previous : globalMtdCardData.prevUnits}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.units.current, dummyStatData.units.previous)
                                                                : safeDeltaPct(globalMtdCardData.units, globalMtdCardData.prevUnits)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={fmtInt}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                                                    />

                                                    <AmazonStatCard
                                                        label="ASP"
                                                        current={shouldShowDummyUi ? dummyStatData.asp.current : globalMtdCardData.asp}
                                                        previous={shouldShowDummyUi ? dummyStatData.asp.previous : globalMtdCardData.prevAsp}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.asp.current, dummyStatData.asp.previous)
                                                                : safeDeltaPct(globalMtdCardData.asp, globalMtdCardData.prevAsp)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={formatDisplayAmount}
                                                        previousFormatter={formatDisplayAmount}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                                    />


                                                    <AmazonStatCard
                                                        label="Gross Sales"
                                                        current={shouldShowDummyUi ? dummyStatData.grossSales.current : globalMtdCardData.grossSales}
                                                        previous={shouldShowDummyUi ? dummyStatData.grossSales.previous : globalMtdCardData.prevGrossSales}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.grossSales.current, dummyStatData.grossSales.previous)
                                                                : safeDeltaPct(globalMtdCardData.grossSales, globalMtdCardData.prevGrossSales)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                                                    />

                                                    <AmazonStatCard
                                                        label="Net Sales"
                                                        current={shouldShowDummyUi ? dummyStatData.netSales.current : globalMtdCardData.netSales}
                                                        previous={shouldShowDummyUi ? dummyStatData.netSales.previous : globalMtdCardData.prevNetSales}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.netSales.current, dummyStatData.netSales.previous)
                                                                : safeDeltaPct(globalMtdCardData.netSales, globalMtdCardData.prevNetSales)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"
                                                    />


                                                    <AmazonStatCard
                                                        label="Cost of Ads"
                                                        current={shouldShowDummyUi ? dummyStatData.costOfAds.current : globalMtdCardData.ads}
                                                        previous={shouldShowDummyUi ? dummyStatData.costOfAds.previous : globalMtdCardData.prevAds}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.costOfAds.current, dummyStatData.costOfAds.previous)
                                                                : safeDeltaPct(globalMtdCardData.ads, globalMtdCardData.prevAds)
                                                        }
                                                        inverseDelta
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                                    />

                                                    <AmazonStatCard
                                                        label="TACoS"
                                                        current={shouldShowDummyUi ? dummyStatData.tacos.current : globalMtdCardData.tacos}
                                                        previous={shouldShowDummyUi ? dummyStatData.tacos.previous : globalMtdCardData.prevTacos}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.tacos.current, dummyStatData.tacos.previous)
                                                                : safeDeltaPct(globalMtdCardData.tacos, globalMtdCardData.prevTacos)
                                                        }
                                                        inverseDelta
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={fmtPct2}
                                                        previousFormatter={fmtPct2}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                                    />

                                                    <AmazonStatCard
                                                        label="CM2 Profit"
                                                        current={shouldShowDummyUi ? dummyStatData.cm2Profit.current : globalMtdCardData.cm2Profit}
                                                        previous={shouldShowDummyUi ? dummyStatData.cm2Profit.previous : globalMtdCardData.prevCm2Profit}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? safeDeltaPct(dummyStatData.cm2Profit.current, dummyStatData.cm2Profit.previous)
                                                                : safeDeltaPct(globalMtdCardData.cm2Profit, globalMtdCardData.prevCm2Profit)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                        previousFormatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#A8BF7A] border-t-4 border-t-[#A8BF7A]"
                                                    />

                                                    <AmazonStatCard
                                                        label="CM2 Profit %"
                                                        current={shouldShowDummyUi ? dummyStatData.cm2ProfitPct.current : globalMtdCardData.cm2Pct}
                                                        previous={shouldShowDummyUi ? dummyStatData.cm2ProfitPct.previous : globalMtdCardData.prevCm2Pct}
                                                        deltaPct={
                                                            shouldShowDummyUi
                                                                ? deltaPctPoints(dummyStatData.cm2ProfitPct.current, dummyStatData.cm2ProfitPct.previous)
                                                                : deltaPctPoints(globalMtdCardData.cm2Pct, globalMtdCardData.prevCm2Pct)
                                                        }
                                                        loading={!shouldShowDummyUi && (loading || shopifyLoading || biLoading || previousSkuwiseGlobalLoading)}
                                                        formatter={fmtPct2}
                                                        previousFormatter={fmtPct2}
                                                        bottomLabel={prevLabel}
                                                        className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                                                    />
                                                </div>
                                            ) : (
                                                <div className="space-y-4">
                                                    {renderCountryMtdCards(globalMtdView)}
                                                </div>
                                            )
                                        ) : (
                                            <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-4 2xl:grid-cols-4 gap-2 lg:gap-2 2xl:gap-3 auto-rows-fr">

                                                <AmazonStatCard
                                                    label="Units"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.units.current
                                                            : (useBiForAmazonCards ? biCardKpis.curr.units : mtdUnitsCurrent)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.units.previous
                                                            : (useBiForAmazonCards ? biCardKpis.prev.units : mtdUnitsPrevious)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.units.deltaPct
                                                            : (useBiForAmazonCards ? biCardKpis.deltas.units : mtdUnitsDelta)
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={fmtInt}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                                                />

                                                <AmazonStatCard
                                                    label="ASP"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.asp.current
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.curr.asp
                                                                : convertToDisplayCurrency(uk.aspGBP ?? 0, amazonDataCurrency)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.asp.previous
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.prev.asp
                                                                : convertToDisplayCurrency(prev.asp, amazonDataCurrency)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.asp.deltaPct
                                                            : (useBiForAmazonCards ? biCardKpis.deltas.asp : deltas.aspPct)
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={formatDisplayAmount}
                                                    previousFormatter={formatDisplayAmount}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                                />

                                                <AmazonStatCard
                                                    label="Gross Sales"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.grossSales.current
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.curr.grossSales
                                                                : convertToDisplayCurrency(uk.grossSalesGBP ?? 0, amazonDataCurrency)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.grossSales.previous
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.prev.grossSales
                                                                : convertToDisplayCurrency(prev.grossSales ?? 0, amazonDataCurrency)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.grossSales.deltaPct
                                                            : (useBiForAmazonCards
                                                                ? biCardKpis.deltas.grossSales
                                                                : safeDeltaPct(uk.grossSalesGBP ?? 0, prev.grossSales ?? 0))
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "Gross Sales")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#ED9F50] border-t-4 border-t-[#ED9F50]"
                                                />

                                                <AmazonStatCard
                                                    label="Net Sales"
                                                    current={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.netSales.current
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.curr.netSales
                                                                : convertToDisplayCurrency(uk.netSalesGBP ?? 0, amazonDataCurrency)
                                                    }
                                                    previous={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.netSales.previous
                                                            : showLiveBI && rangeActive
                                                                ? biCardKpis.prev.netSales
                                                                : convertToDisplayCurrency(prev.netSales, amazonDataCurrency)
                                                    }
                                                    deltaPct={
                                                        shouldShowDummyUi
                                                            ? dummyStatData.netSales.deltaPct
                                                            : (useBiForAmazonCards ? biCardKpis.deltas.netSales : deltas.netSalesPct)
                                                    }
                                                    loading={!shouldShowDummyUi && (loading || biLoading)}
                                                    formatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "Net Sales")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"
                                                />



                                                <AmazonStatCard
                                                    label="Cost of Ads"
                                                    current={mtdCostOfAdsCurrentDisplay}
                                                    previous={mtdCostOfAdsPreviousDisplay}
                                                    deltaPct={mtdCostOfAdsDelta}
                                                    inverseDelta
                                                    loading={!shouldShowDummyUi && (loading || (rangeActive ? biLoading : false))}
                                                    formatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "Cost of Ads")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#C49466] border-t-4 border-t-[#C49466]"
                                                />

                                                <AmazonStatCard
                                                    label="TACoS"
                                                    current={mtdTacosCurrent}
                                                    previous={mtdTacosPrevious}
                                                    deltaPct={mtdTacosDelta}
                                                    inverseDelta
                                                    loading={!shouldShowDummyUi && (loading || (rangeActive ? biLoading : false))}
                                                    formatter={fmtPct2}
                                                    previousFormatter={fmtPct2}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#3A8EA4] border-t-4 border-t-[#3A8EA4]"
                                                />

                                                <AmazonStatCard
                                                    label="CM2 Profit"
                                                    current={mtdCm2ProfitCurrentDisplay}
                                                    previous={mtdCm2ProfitPreviousDisplay}
                                                    deltaPct={mtdCm2ProfitDelta}
                                                    loading={!shouldShowDummyUi && loading}
                                                    formatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                    previousFormatter={(val) => formatDisplayAmount(val, "CM2 Profit")}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#B8C78C] border-t-4 border-t-[#B8C78C]"
                                                />

                                                <AmazonStatCard
                                                    label="CM2 Profit %"
                                                    current={mtdCm2ProfitPctCurrent}
                                                    previous={mtdCm2ProfitPctPrevious}
                                                    deltaPct={mtdCm2ProfitPctDelta}
                                                    loading={!shouldShowDummyUi && loading}
                                                    formatter={fmtPct2}
                                                    previousFormatter={fmtPct2}
                                                    bottomLabel={prevLabel}
                                                    className="border-[#7B9A6D] border-t-4 border-t-[#7B9A6D]"
                                                />
                                            </div>
                                        )}



                                    </div>



                                    {/* Live BI graph */}
                                    {/* {showLiveBI && isCountryMode && (
                                        <div className="w-full rounded-xl border bg-white p-3 lg:p-3 2xl:p-5 shadow-sm overflow-x-hidden">
                                            <div className="w-full max-w-full min-w-0">

                                                {!shouldShowDummyUi && biStatus === "processing" && (
                                                    <div className="flex justify-center items-center py-10">
                                                        <Loader className="bg-transparent" />
                                                    </div>
                                                )}

                                                {!shouldShowDummyUi && biStatus === "error" && (
                                                    <div className="text-center py-10 text-sm text-red-500">
                                                        Taking longer than expected. Please refresh once.
                                                    </div>
                                                )}

                                                {!shouldShowDummyUi && biStatus === "ready" && !biDailySeriesHome && (
                                                    <div className="text-center py-10 text-sm text-gray-500">
                                                        No data available for the selected period
                                                    </div>
                                                )}

                                              
                                                {(shouldShowDummyUi || biStatus === "ready") && finalBiDailySeriesHome && (

                                                    <LiveBiLineGraph
                                                        dailySeries={finalBiDailySeriesHome}
                                                        periods={finalBiPeriods}
                                                        loading={liveBiGraphLoading}
                                                        isRefreshing={!shouldShowDummyUi && biUiLoading && !!finalBiDailySeriesHome}
                                                        error={shouldShowDummyUi ? null : biError}
                                                        selectedStartDay={selectedStartDay}
                                                        selectedEndDay={selectedEndDay}
                                                        currentDataEndDate={dashboardAllowedEndISO}
                                                        currencySymbol={currencySymbol}
                                                    />

                                                )}

                                            </div>
                                        </div>
                                    )} */}
                                </div>

                            )}

                            {/* Shopify Block */}
                            {!isCountryMode && hasShopifyCard && (
                                <div className="flex lg:flex-1">
                                    <div className="w-full rounded-xl border bg-white p-5 shadow-sm">
                                        <div className="mb-4 flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                                            <div className="flex flex-col">
                                                <div className="flex items-baseline gap-2">
                                                    <PageBreadcrumb
                                                        pageTitle="Shopify"
                                                        variant="page"
                                                        align="left"
                                                        textSize="2xl"
                                                    />
                                                </div>
                                            </div>
                                        </div>
                                        {/* Shopify Block */}
                                        {!isCountryMode && hasShopifyCard && (
                                            <div className="flex lg:flex-1">
                                                <div className="w-full rounded-xl border bg-white p-5 shadow-sm">
                                                    <div className="mb-4 flex flex-col gap-4 md:flex-row md:items-start md:justify-between ">
                                                        <div className="flex flex-col">
                                                            <div className="flex items-baseline gap-2">
                                                                <PageBreadcrumb
                                                                    pageTitle="Shopify"
                                                                    variant="page"
                                                                    align="left"
                                                                    textSize="2xl"
                                                                />
                                                            </div>
                                                        </div>
                                                    </div>

                                                    {shopifyLoading ? (
                                                        <div className="mt-3 text-sm text-gray-500">Loading Shopify…</div>
                                                    ) : shopify ? (
                                                        <div className="grid grid-cols-2 gap-3 sm:grid-cols-2 lg:grid-cols-3">

                                                            <AmazonStatCard
                                                                label="Units"
                                                                current={shopifyDeriv?.totalOrders ?? 0}
                                                                previous={shopifyPrevDeriv?.totalOrders ?? 0}
                                                                loading={shopifyLoading}
                                                                formatter={fmtInt}
                                                                bottomLabel={prevLabel}
                                                                // className="border-[#FDD36F] bg-[#FDD36F4D]"
                                                                className="border-[#FDD36F] border-t-4 border-t-[#FDD36F]"
                                                            />
                                                            <AmazonStatCard
                                                                label="Sales"
                                                                current={convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR")}
                                                                previous={convertToDisplayCurrency(shopifyPrevDeriv?.netSales ?? 0, "INR")}
                                                                loading={shopifyLoading}
                                                                formatter={formatDisplayAmount}
                                                                bottomLabel={prevLabel}
                                                                // className="border-[#75BBDA] bg-[#75BBDA4D]"
                                                                className="border-[#75BBDA] border-t-4 border-t-[#75BBDA]"

                                                            />
                                                            <AmazonStatCard
                                                                label="ASP"
                                                                current={(() => {
                                                                    const units = shopifyDeriv?.totalOrders ?? 0;
                                                                    if (!units) return 0;
                                                                    const net = convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR");
                                                                    return net / units;
                                                                })()}
                                                                previous={0}
                                                                loading={shopifyLoading}
                                                                formatter={formatDisplayAmount}
                                                                bottomLabel={prevLabel}
                                                                // className="border-[#B75A5A] bg-[#B75A5A4D]"
                                                                className="border-[#B75A5A] border-t-4 border-t-[#B75A5A]"
                                                            />
                                                        </div>
                                                    ) : (
                                                        <div className="mt-2 text-sm text-gray-500">
                                                            No Shopify data for the current month.
                                                        </div>
                                                    )}
                                                </div>
                                            </div>
                                        )}
                                    </div>

                                    <AmazonStatCard
                                        label="ASP"
                                        current={(() => {
                                            const units = shopifyDeriv?.totalOrders ?? 0;
                                            if (!units) return 0;
                                            const net = convertToDisplayCurrency(shopifyDeriv?.netSales ?? 0, "INR");
                                            return net / units;
                                        })()}
                                        previous={0}
                                        loading={shopifyLoading}
                                        formatter={formatDisplayAmount}
                                        bottomLabel={prevLabel}
                                        // className="border-[#B75A5A] bg-[#B75A5A4D] "
                                        className="border-[#B75A5A] bg-[#B75A5A4D] "

                                    />
                                </div>
                            )}

                            {showLiveBI && (
                                <div
                                    id="ai-insights"
                                    className="w-full max-w-full min-w-0 rounded-xl border bg-white p-4 sm:p-5 shadow-sm overflow-x-hidden scroll-mt-[80px]"
                                >
                                    <div className="w-full max-w-full min-w-0 h-full">
                                        <LiveBiLineGraph
                                            dailySeries={finalBiDailySeriesHome}
                                            periods={finalBiPeriods}
                                            loading={!shouldShowDummyUi && biUiLoading}
                                            error={shouldShowDummyUi ? null : biError}
                                            selectedStartDay={selectedStartDay}
                                            selectedEndDay={selectedEndDay}
                                            currentDataEndDate={dashboardAllowedEndISO}
                                            currencySymbol={currencySymbol}
                                        />
                                    </div>
                                </div>
                            )}
                        </div>

                        {/* RIGHT COLUMN – Sales Target */}
                        <aside className="col-span-12 lg:col-span-4 order-1 lg:order-2 h-auto min-h-0 self-start">
                            <div className="grid gap-4 h-auto">

                                {/* Top card */}
                                <div className="w-full self-start">

                                    <SalesTargetStatsCard
                                        regions={regions}
                                        value={targetRegion}
                                        onChange={setTargetRegion}
                                        hideTabs={isCountryMode}
                                        homeCurrency={displayCurrency}
                                        formatHomeK={formatDisplayK}
                                        todayHome={finalTargetsTodayHome}
                                        mtdHome={finalTargetsMtdHome}
                                        targetHome={finalStatsTargetHome}
                                        lastMonthTotalHome={finalTargetsLastMonthTotalHome}
                                        lastMonthToDateHome={targets_lastMonthToDateHome}
                                        salesTrendPct={finalStatsSalesTrendPct}
                                        targetTrendPct={finalStatsTargetTrendPct}
                                        currentReimbursement={finalTargetsReimbursement.current}
                                        previousReimbursement={finalTargetsReimbursement.previous}
                                        biAlignedTotals={shouldShowDummyUi ? null : biAlignedTotalsHome}
                                        biEnabled={shouldShowDummyUi ? false : biCardsReady}
                                        currentMonthLabel={formattedMonthYear}
                                        previousMonthLabel={prevLabel}
                                        currentMonthName={currentDisplayMonth.monthName}
                                        currentYear={currentDisplayMonth.year}
                                    />

                                </div>

                                {/* Bottom card */}
                                <div className="h-auto lg:h-full lg:sticky lg:top-4 2xl:top-6">

                                    <SalesTargetCard
                                        data={regions[targetRegion]}
                                        homeCurrency={displayCurrency}
                                        convertToHomeCurrency={(value, from) => convertToDisplayCurrency(value, from)}
                                        formatHomeK={formatDisplayK}
                                        todaySales={todaySalesRaw}
                                        targetHome={stickyTargetHome}
                                        mtdHome={targets_mtdHome}
                                        lastMonthTotalHome={targets_lastMonthTotalHome}
                                        lastMonthToDateHome={targets_lastMonthToDateHome}
                                        currentMonthLabel={formattedMonthYear}
                                        previousMonthLabel={prevLabel}
                                        currentReimbursement={targets_reimbursement.current}
                                        previousReimbursement={targets_reimbursement.previous}
                                        reimbursementDeltaPct={
                                            platform === "global"
                                                ? globalTargetCardTotals.reimbursementDeltaPct
                                                : safeDeltaPct(
                                                    targets_reimbursement.current,
                                                    targets_reimbursement.previous
                                                )
                                        }
                                        biAlignedTotals={shouldShowDummyUi ? null : salesTargetBiAlignedTotals}
                                        biEnabled={
                                            shouldShowDummyUi
                                                ? false
                                                : platform === "global"
                                                    ? true
                                                    : biCardsReady
                                        }
                                        periodCompletedPct={finalRangeCompletedPct}
                                        periodCompletedLabel="Month"
                                        completedAt={lastRefreshAt}
                                        completedTimeZone={dashboardCompletedTimeZone}
                                    />
                                </div>
                            </div>
                        </aside>
                    </div >
    );
}
