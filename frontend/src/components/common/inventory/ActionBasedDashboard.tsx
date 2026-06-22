import React from "react";
import PageBreadcrumb from "../PageBreadCrumb";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";

export type ActionCardItem = {
    key: string;
    label: string;
    description: string;
    count: number;
    color: string;
    backgroundColor: string;

    displayValue?: string | number;
    valueSuffix?: string;

    unitCount?: number;
    skuCount?: number;

    deltaValue?: string | number;
    deltaPercentage?: number | null;
};

export type ActionLogicItem = {
    key: string;
    label: string;
    description: string;
    color: string;
};

type ActionBasedDashboardProps = {
    title?: string;
    subtitle?: string;
    actions: ActionCardItem[];
    actionLogic: ActionLogicItem[];
    onViewDetails?: (action: ActionCardItem) => void;
    onDownloadInventoryExcel?: () => void;
    canDownloadInventoryExcel?: boolean;
};

const ActionBasedDashboard: React.FC<ActionBasedDashboardProps> = ({
    title = "Action-Based Dashboard",
    subtitle = "Group SKUs by recommended action",
    actions,
    onDownloadInventoryExcel,
    canDownloadInventoryExcel = false,
}) => {
    return (
        <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
            <div className="mb-3 flex items-start justify-between gap-3">
                <div>
                    <PageBreadcrumb
                        pageTitle={title}
                        variant="page"
                        align="left"
                        textSize="xl"
                    />
                </div>

                {onDownloadInventoryExcel && (
                    <DownloadIconButton
                        onClick={onDownloadInventoryExcel}
                        disabled={!canDownloadInventoryExcel}
                    />
                )}
            </div>

            <div className="grid grid-cols-1 gap-4 sm:grid-cols-3 min-[1700px]:grid-cols-6">
                {actions.map((action) => (
                    <div
                        key={action.key}
                        className="rounded-lg border border-t-4 px-3 py-2.5 text-center"
                        style={{
                            backgroundColor: action.backgroundColor || "#ffffff",
                            borderColor: action.color,
                            borderTopColor: action.color,
                        }}
                    >
                        <h4 className="truncate text-sm font-bold text-charcoal-500">
                            {action.label}
                        </h4>

                        <p className="mx-auto mt-1 h-8 max-w-[210px] overflow-hidden text-xs leading-4 text-charcoal-500">
                            {action.description}
                        </p>

                        {action.key === "estimated_storage_cost" ? (
                            <div className="flex flex-col items-center justify-center gap-1 text-charcoal-500">
                                <span className="text-xl font-extrabold leading-none">
                                    {action.displayValue ?? action.count}
                                </span>

                                {typeof action.deltaPercentage === "number" && (
                                    <span
                                        className={
                                            action.deltaPercentage <= 0
                                                ? "text-xs font-bold leading-none text-emerald-600"
                                                : "text-xs font-bold leading-none text-red-600"
                                        }
                                        title={
                                            action.deltaValue
                                                ? `Change vs previous month: ${action.deltaValue}`
                                                : "Change vs previous month"
                                        }
                                    >
                                        {action.deltaPercentage <= 0 ? "▼" : "▲"}{" "}
                                        {Math.abs(action.deltaPercentage).toFixed(2)}%
                                    </span>
                                )}
                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center gap-1 text-charcoal-500">
                                <span className="text-xl font-extrabold leading-none">
                                    {typeof action.skuCount === "number"
                                        ? `${action.skuCount.toLocaleString()} SKUs`
                                        : `${action.count.toLocaleString()} SKUs`}
                                </span>

                                {typeof action.unitCount === "number" && (
                                    <span className="text-xs font-semibold leading-none">
                                        {action.unitCount.toLocaleString()} Units
                                    </span>
                                )}
                            </div>
                        )}

                        {action.key === "estimated_storage_cost" && (
                            <span className="mt-1 block text-xs font-bold leading-none text-charcoal-500">
                                {/* Storage Cost */}
                            </span>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
};

export default ActionBasedDashboard;