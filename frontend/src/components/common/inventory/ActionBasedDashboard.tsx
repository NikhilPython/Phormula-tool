import React from "react";
import PageBreadcrumb from "../PageBreadCrumb";

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

    // ✅ ADD THIS
    avgCoverageRatio?: number;

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
};

const ActionBasedDashboard: React.FC<ActionBasedDashboardProps> = ({
    title = "Action-Based Dashboard",
    subtitle = "Group SKUs by recommended action",
    actions,
}) => {
    return (
        <>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-5 min-[1700px]:grid-cols-5">
                {actions.map((action) => (
                    <div
                        key={action.key}
                        className="rounded-lg border border-t-4 p-2.5 sm:p-3 text-center"
                        style={{
                            backgroundColor: "#ffffff",
                            borderColor: action.color,
                            borderTopColor: action.color,
                        }}
                    >
                        <h4 className="truncate text-[10px] sm:text-[10px] 2xl:text-xs font-medium text-charcoal-500">
                            {action.label}
                        </h4>

                        <p className="mx-auto mt-1 h-4 max-w-[210px] overflow-hidden text-[10px] sm:text-[10px] 2xl:text-xs leading-4 text-charcoal-500">
                            {action.description}
                        </p>

                        {action.key === "estimated_storage_cost" ? (
                            <div className="mt-2 flex flex-col items-center justify-center gap-2 text-charcoal-500">
                                <span className="text-sm 2xl:text-lg font-semibold leading-none">
                                    {action.displayValue ?? action.count}
                                </span>

                                {typeof action.deltaPercentage === "number" && (
                                    <span
                                        className={
                                            action.deltaPercentage <= 0
                                                ? "text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold leading-none text-emerald-600"
                                                : "text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold leading-none text-red-600"
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
                            <div className="mt-2 flex flex-col items-center justify-center gap-2 text-charcoal-500">
                                <span className="text-sm 2xl:text-lg font-semibold leading-none">
                                    {typeof action.skuCount === "number"
                                        ? `${action.skuCount.toLocaleString()} SKUs`
                                        : `${action.count.toLocaleString()} SKUs`}
                                </span>

                                {action.key === "high_alert" ? (
                                    typeof action.avgCoverageRatio === "number" && (
                                        <span className="text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold leading-none">
                                            Avg Coverage Ratio: {action.avgCoverageRatio.toFixed(2)}
                                        </span>
                                    )
                                ) : (
                                    typeof action.unitCount === "number" && (
                                        <span className="text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold leading-none">
                                            {action.unitCount.toLocaleString()} Units
                                        </span>
                                    )
                                )}

                                {/* {action.key === "high_alert" ? (
                                    <span className="text-xl font-extrabold leading-none">
                                        {typeof action.skuCount === "number"
                                            ? `${action.skuCount.toLocaleString()} SKUs`
                                            : `${action.count.toLocaleString()} SKUs`}
                                    </span>
                                ) : (
                                    <>
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
                                    </>
                                )} */}
                            </div>
                        )}

                    </div>
                ))}
            </div>
        </>
    );
};

export default ActionBasedDashboard;

