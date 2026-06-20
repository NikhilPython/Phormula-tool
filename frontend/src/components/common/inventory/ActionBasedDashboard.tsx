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

                    {subtitle && (
                        <p className="mt-0.5 text-xs text-charcoal-400">
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

            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
                {actions.map((action) => (
                    <div
                        key={action.key}
                        className="rounded-lg border border-t-4 px-3 py-2.5 text-center"
                        style={{
                            backgroundColor: "#ffffff",
                            borderColor: action.color,
                            borderTopColor: action.color,
                        }}
                    >
                        <h4 className="truncate text-sm font-bold text-slate-900">
                            {action.label}
                        </h4>

                        <p className="mx-auto mt-1 h-8 max-w-[210px] overflow-hidden text-[11px] leading-4 text-slate-700">
                            {action.description}
                        </p>

                        <strong className="mt-2 block text-2xl font-extrabold leading-none text-slate-900">
                            {action.displayValue ?? action.count}
                        </strong>

                        <span className="mt-1 block text-[11px] font-bold leading-none text-slate-900">
                            {action.valueSuffix ?? "SKUs"}
                        </span>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default ActionBasedDashboard;