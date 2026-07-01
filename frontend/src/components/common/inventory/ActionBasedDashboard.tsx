// import React from "react";
// import PageBreadcrumb from "../PageBreadCrumb";

// export type ActionCardItem = {
//     key: string;
//     label: string;
//     description: string;
//     count: number;
//     color: string;
//     backgroundColor: string;

//     displayValue?: string | number;
//     valueSuffix?: string;

//     unitCount?: number;
//     skuCount?: number;

//     // ✅ ADD THIS
//     avgCoverageRatio?: number;

//     deltaValue?: string | number;
//     deltaPercentage?: number | null;
// };

// export type ActionLogicItem = {
//     key: string;
//     label: string;
//     description: string;
//     color: string;
// };

// type ActionBasedDashboardProps = {
//     title?: string;
//     subtitle?: string;
//     actions: ActionCardItem[];
//     actionLogic: ActionLogicItem[];
//     onViewDetails?: (action: ActionCardItem) => void;
// };

// const ActionBasedDashboard: React.FC<ActionBasedDashboardProps> = ({
//     title = "Action-Based Dashboard",
//     subtitle = "Group SKUs by recommended action",
//     actions,
// }) => {
//     return (
//         <>
//             <div className="grid grid-cols-1 gap-3 sm:grid-cols-6 min-[1700px]:grid-cols-6">
//                 {actions.map((action) => (
//                     <div
//                         key={action.key}
//                         className="rounded-lg border border-t-4 p-2.5 sm:p-3 text-center"
//                         style={{
//                             backgroundColor: "#ffffff",
//                             borderColor: action.color,
//                             borderTopColor: action.color,
//                         }}
//                     >
//                         <h4 className="truncate text-[10px] sm:text-[10px] 2xl:text-xs font-medium text-charcoal-500">
//                             {action.label}
//                         </h4>

//                         <p className="mx-auto mt-1 h-6 max-w-[210px] overflow-hidden text-[10px] sm:text-[10px] 2xl:text-xs leading-4 text-charcoal-500">
//                             {action.description}
//                         </p>

//                         {action.key === "estimated_storage_cost" ? (
//                             <div className="flex flex-col items-center justify-center gap-1 text-charcoal-500">
//                                 <span className="text-sm 2xl:text-lg  font-extrabold leading-none">
//                                     {action.displayValue ?? action.count}
//                                 </span>

//                                 {typeof action.deltaPercentage === "number" && (
//                                     <span
//                                         className={
//                                             action.deltaPercentage <= 0
//                                                 ? "text-[9.5px] sm:text-[10px] 2xl:text-xs font-bold leading-none text-emerald-600"
//                                                 : "text-[9.5px] sm:text-[10px] 2xl:text-xs font-bold leading-none text-red-600"
//                                         }
//                                         title={
//                                             action.deltaValue
//                                                 ? `Change vs previous month: ${action.deltaValue}`
//                                                 : "Change vs previous month"
//                                         }
//                                     >
//                                         {action.deltaPercentage <= 0 ? "▼" : "▲"}{" "}
//                                         {Math.abs(action.deltaPercentage).toFixed(2)}%
//                                     </span>
//                                 )}
//                             </div>
//                         ) : (
//                             <div className="flex flex-col items-center justify-center gap-1 text-charcoal-500">
//                                 <span className="text-sm 2xl:text-lg font-extrabold leading-none">
//                                     {typeof action.skuCount === "number"
//                                         ? `${action.skuCount.toLocaleString()} SKUs`
//                                         : `${action.count.toLocaleString()} SKUs`}
//                                 </span>

//                                 {action.key === "high_alert" ? (
//                                     typeof action.avgCoverageRatio === "number" && (
//                                         <span className="text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold leading-none">
//                                             Avg Coverage Ratio: {action.avgCoverageRatio.toFixed(2)}
//                                         </span>
//                                     )
//                                 ) : (
//                                     typeof action.unitCount === "number" && (
//                                         <span className="text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold leading-none">
//                                             {action.unitCount.toLocaleString()} Units
//                                         </span>
//                                     )
//                                 )}

//                                 {/* {action.key === "high_alert" ? (
//                                     <span className="text-xl font-extrabold leading-none">
//                                         {typeof action.skuCount === "number"
//                                             ? `${action.skuCount.toLocaleString()} SKUs`
//                                             : `${action.count.toLocaleString()} SKUs`}
//                                     </span>
//                                 ) : (
//                                     <>
//                                         <span className="text-xl font-extrabold leading-none">
//                                             {typeof action.skuCount === "number"
//                                                 ? `${action.skuCount.toLocaleString()} SKUs`
//                                                 : `${action.count.toLocaleString()} SKUs`}
//                                         </span>

//                                         {typeof action.unitCount === "number" && (
//                                             <span className="text-xs font-semibold leading-none">
//                                                 {action.unitCount.toLocaleString()} Units
//                                             </span>
//                                         )}
//                                     </>
//                                 )} */}
//                             </div>
//                         )}

//                     </div>
//                 ))}
//             </div>
//         </>
//     );
// };

// export default ActionBasedDashboard;














import React from "react";

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
    actions,
    onViewDetails,
}) => {
    const getMainValue = (action: ActionCardItem) => {
        if (action.key === "estimated_storage_cost") {
            return action.displayValue ?? action.count;
        }

        const skuValue =
            typeof action.skuCount === "number" ? action.skuCount : action.count;

        return `${skuValue.toLocaleString()} SKUs`;
    };

    const getSubValue = (action: ActionCardItem) => {
        if (action.key === "estimated_storage_cost") {
            if (typeof action.deltaPercentage === "number") {
                return `${action.deltaPercentage <= 0 ? "▼" : "▲"} ${Math.abs(
                    action.deltaPercentage
                ).toFixed(2)}%`;
            }

            return "—";
        }

        if (action.key === "high_alert") {
            if (typeof action.avgCoverageRatio === "number") {
                return `Avg Coverage Ratio: ${action.avgCoverageRatio.toFixed(2)}`;
            }

            return "";
        }

        if (typeof action.unitCount === "number") {
            return `${action.unitCount.toLocaleString()} Units`;
        }

        return "";
    };

    const getSubValueClass = (action: ActionCardItem) => {
        if (
            action.key === "estimated_storage_cost" &&
            typeof action.deltaPercentage === "number"
        ) {
            return action.deltaPercentage <= 0
                ? "text-emerald-600"
                : "text-red-600";
        }

        return "text-charcoal-500";
    };

    return (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-6">
            {actions.map((action) => (
                <button
                    key={action.key}
                    type="button"
                    onClick={() => onViewDetails?.(action)}
                    className="group rounded-xl border border-t-4 bg-white px-3.5 py-3 text-left shadow-sm transition-all duration-200"
                    style={{
                        borderColor: action.color,
                        borderTopColor: action.color,
                    }}
                >
                    {/* Title */}
                    <h4 className="truncate text-[10px] sm:text-[10px] 2xl:text-xs leading-tight font-medium text-charcoal-500 ">
                        {action.label}
                    </h4>

                    {/* Sub Title */}
                    <p className="mt-1 line-clamp-2 min-h-[30px] text-[9.5px] sm:text-[10px] 2xl:text-xs leading-tight font-medium text-charcoal-500">
                        {action.description}
                    </p>

                    {/* Value + Sub Value */}
                    <div className="mt-3 flex items-end justify-between gap-3">
                        <span className="truncate text-sm 2xl:text-lg font-semibold leading-none text-charcoal-500">
                            {getMainValue(action)}
                        </span>

                        <span
                            className={`shrink-0 text-right text-[9.5px] sm:text-[10px] 2xl:text-xs font-semibold leading-tight text-charcoal-500 ${getSubValueClass(
                                action
                            )}`}
                            title={
                                action.deltaValue
                                    ? `Change vs previous month: ${action.deltaValue}`
                                    : undefined
                            }
                        >
                            {getSubValue(action)}
                        </span>
                    </div>
                </button>
            ))}
        </div>
    );
};

export default ActionBasedDashboard;