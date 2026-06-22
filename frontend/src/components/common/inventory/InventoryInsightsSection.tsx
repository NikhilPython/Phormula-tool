"use client";

import React from "react";
import AgeingRiskHeatmap, {
    AgeingBucket,
    AgeingRiskHeatmapRow,
} from "@/components/common/inventory/AgeingRiskHeatmap";

import SkuAgeingDonutChart, {
    DonutChartItem,
} from "@/components/common/inventory/SkuAgeingDonutChart";

import AgeingTrendChart, {
    AgeingTrendItem,
    AgeingTrendBucketOption,
} from "@/components/common/inventory/AgeingTrendChart";

import ActionBasedDashboard, {
    ActionCardItem,
    ActionLogicItem,
} from "@/components/common/inventory/ActionBasedDashboard";


type InventoryInsightsSectionProps = {
    heatmapBuckets: AgeingBucket[];
    heatmapData: AgeingRiskHeatmapRow[];

    donutData: DonutChartItem[];
    donutTotalUnits?: number;

    trendSelectedBucket: string;
    trendData: AgeingTrendItem[];
    trendLineColor: string;

    trendBucketOptions?: AgeingTrendBucketOption[];
    onTrendBucketChange?: (bucketValue: string) => void;

    actions: ActionCardItem[];
    actionLogic: ActionLogicItem[];

    onActionViewDetails?: (action: ActionCardItem) => void;

    // ✅ new
    onDownloadInventoryExcel?: () => void;
    canDownloadInventoryExcel?: boolean;
};

const InventoryInsightsSection: React.FC<InventoryInsightsSectionProps> = ({
    heatmapBuckets,
    heatmapData,
    donutData,
    donutTotalUnits,
    trendSelectedBucket,
    trendData,
    trendLineColor,
    trendBucketOptions = [],
    onTrendBucketChange,
    actions,
    actionLogic,
    onActionViewDetails,
    onDownloadInventoryExcel,
    canDownloadInventoryExcel = false,
}) => {
    const hasHeatmap = heatmapBuckets.length > 0 && heatmapData.length > 0;
    const hasDonut = donutData.length > 0;
    const hasTrend = trendSelectedBucket && trendData.length > 0;
    const hasActions = actions.length > 0;

    if (!hasHeatmap && !hasDonut && !hasTrend && !hasActions) {
        return null;
    }

    return (
        <div className="space-y-5">
            <div className="space-y-5">
                {hasActions && (
                    <ActionBasedDashboard
                        title="Action-Based Dashboard"
                        subtitle=""
                        actions={actions}
                        actionLogic={actionLogic}
                        onViewDetails={onActionViewDetails}
                        onDownloadInventoryExcel={onDownloadInventoryExcel}
                        canDownloadInventoryExcel={canDownloadInventoryExcel}
                    />
                )}

                {hasHeatmap && (
                    <AgeingRiskHeatmap
                        title="Ageing Risk Heatmap"
                        subtitle="Quickly identify products with old inventory"
                        data={heatmapData}
                        buckets={heatmapBuckets}
                    />
                )}

               {(hasTrend || hasDonut) && (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2 lg:items-stretch">
        {hasTrend && (
            <AgeingTrendChart
                title="Ageing Trend Over Time"
                subtitle="Track how old inventory is increasing or decreasing"
                selectedBucket={trendSelectedBucket}
                data={trendData}
                lineColor={trendLineColor}
                bucketOptions={trendBucketOptions}
                onBucketChange={onTrendBucketChange}
            />
        )}

        {hasDonut && (
            <SkuAgeingDonutChart
                title="Ageing Donut Chart"
                subtitle="Overall inventory ageing distribution across all SKUs"
                data={donutData}
                totalUnits={donutTotalUnits}
            />
        )}
    </div>
)}
            </div>
        </div>
    );
};

export default InventoryInsightsSection;