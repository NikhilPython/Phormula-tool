"use client";

import React from "react";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Loader from "@/components/loader/Loader";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import InventoryInsightsSection from "@/components/common/inventory/InventoryInsightsSection";

type DashboardInventoryInsightsTabProps = Record<string, any>;

export default function DashboardInventoryInsightsTab({
    inventoryInsightsLoading,
    inventoryInsightsError,
    inventoryInsightsData,
    platform,
    selectedGlobalInventoryCountry,
    setSelectedGlobalInventoryCountry,
    selectedAgeingTrendBucket,
    handleAgeingTrendBucketChange,
    handleInventoryInsightsExcelDownload,
    canDownloadInventoryInsightsExcel,
    handleHeatmapProductClick,
    countryName,
}: DashboardInventoryInsightsTabProps) {
    return (
                    <div
                        id="inventory-insights"
                        className="mt-4 scroll-mt-[80px] space-y-6"
                    >
                        {inventoryInsightsLoading ? (
                            <div className="min-h-[420px] flex items-center justify-center">
                                <Loader fullscreen={false} transparent />
                            </div>
                        ) : inventoryInsightsError ? (
                            <div className="w-full rounded-2xl border-2 border-red-200 bg-red-50 p-6 space-y-2">
                                <div className="flex items-center gap-2">
                                    <span className="text-lg">⚠️</span>
                                    <p className="font-semibold text-red-700">
                                        Unable to Load Inventory Insights
                                    </p>
                                </div>

                                <p className="text-sm text-red-600">
                                    {inventoryInsightsError}
                                </p>
                            </div>
                        ) : inventoryInsightsData ? (
                            <>
                                <div className="mb-4 flex items-center justify-between gap-4">
                                    <PageBreadcrumb pageTitle="Inventory Insights" variant="page" align="left" textSize="2xl" />

                                    {platform === "global" && (
                                        <SegmentedToggle
                                            value={selectedGlobalInventoryCountry}
                                            onChange={(val) =>
                                                setSelectedGlobalInventoryCountry(String(val) as "uk" | "us")
                                            }
                                            options={[
                                                { value: "uk", label: "UK" },
                                                { value: "us", label: "US" },
                                            ]}
                                            compact
                                            textSizeClass="text-xs"
                                        />
                                    )}
                                </div>

                                <InventoryInsightsSection
                                    heatmapBuckets={inventoryInsightsData.heatmapBuckets}
                                    heatmapData={inventoryInsightsData.heatmapData}
                                    donutData={inventoryInsightsData.donutData}
                                    donutTotalUnits={inventoryInsightsData.donutTotalUnits}
                                    trendSelectedBucket={selectedAgeingTrendBucket}
                                    trendData={inventoryInsightsData.trendData}
                                    trendAllSeriesData={inventoryInsightsData.trendAllSeriesData}
                                    trendLineColor={inventoryInsightsData.trendLineColor}
                                    trendBucketOptions={inventoryInsightsData.trendBucketOptions}
                                    onTrendBucketChange={handleAgeingTrendBucketChange}
                                    actions={inventoryInsightsData.actions}
                                    actionLogic={inventoryInsightsData.actionLogic}
                                    onDownloadInventoryExcel={handleInventoryInsightsExcelDownload}
                                    canDownloadInventoryExcel={canDownloadInventoryInsightsExcel}
                                    onHeatmapProductClick={handleHeatmapProductClick}
                                    heatmapExcelCountryLabel={
                                        platform === "global"
                                            ? selectedGlobalInventoryCountry.toUpperCase()
                                            : countryName.toUpperCase()
                                    }
                                    heatmapExcelPlatformLabel="Phormula"
                                    heatmapExcelTitleLine="Inventory Insights Report"
                                />
                            </>
                        ) : (
                            <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-600">
                                No inventory insights found for the selected period.
                            </div>
                        )}
                    </div>
    );
}
