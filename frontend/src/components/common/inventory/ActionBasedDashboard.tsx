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
    actionLogic,
}) => {
    return (
        <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="mb-4">
                <h3 className="text-lg font-extrabold uppercase text-slate-900">
                    {title}
                </h3>
                <p className="mt-1 text-sm text-slate-900">{subtitle}</p>
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-5">
                {actions.map((action) => (
                    <div
                        key={action.key}
                        className="rounded-xl border border-t-4 p-4 text-center"
                        style={{
                            backgroundColor: action.backgroundColor,
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

            <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 p-4">
                <h4 className="mb-3 font-bold text-slate-900">Action Logic</h4>

                <div className="grid grid-cols-1 gap-x-5 gap-y-2 text-xs lg:grid-cols-2">
                    {actions
                        .map((action) =>
                            actionLogic.find((logic) => logic.key === action.key)
                        )
                        .filter((logic): logic is ActionLogicItem => Boolean(logic))
                        .map((logic) => (
                            <p key={logic.key} className="m-0 text-slate-900">
                                <span
                                    className="mr-2 inline-block h-2.5 w-2.5 rounded-full"
                                    style={{ backgroundColor: logic.color }}
                                />
                                <b>{logic.label}:</b> {logic.description}
                            </p>
                        ))}
                </div>
            </div>
        </div>
    );
};

export default ActionBasedDashboard;