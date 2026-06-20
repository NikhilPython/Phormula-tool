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
    actionLogic,
    onDownloadInventoryExcel,
    canDownloadInventoryExcel = false,
}) => {
    return (
        <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                    <PageBreadcrumb
                        pageTitle={title}
                        variant="page"
                        align="left"
                        textSize="2xl"
                    />

                    {subtitle && (
                        <p className="mt-1 text-xs 2xl:text-sm text-charcoal-400">
                            {subtitle}
                        </p>
                    )}
                </div>

                {onDownloadInventoryExcel && (
                    <DownloadIconButton
                        onClick={onDownloadInventoryExcel}
                        disabled={!canDownloadInventoryExcel}
                    />
                )}
            </div>


            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-5">
                {actions.map((action) => (
                    <div
                        key={action.key}
                        className="rounded-xl border border-t-4 p-4 text-center"
                        style={{
                            backgroundColor: "#ffffff",
                            borderColor: action.color,
                            borderTopColor: action.color,
                        }}
                    >
                        <h4 className="text-base font-bold text-slate-900">
                            {action.label}
                        </h4>

                        <p className="my-2 min-h-9 text-xs text-slate-900">
                            {action.description}
                        </p>

                        <strong className="block text-3xl font-extrabold text-slate-900">
                            {action.displayValue ?? action.count}
                        </strong>

                        <span className="mb-2 block text-xs font-bold text-slate-900">
                            {action.valueSuffix ?? "SKUs"}
                        </span>
                    </div>
                ))}
            </div>

        </div>
    );
};

export default ActionBasedDashboard;