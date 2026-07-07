"use client";

import React from "react";
import PageBreadcrumb from "../common/PageBreadCrumb";

type Props = {
    loading: boolean;
    error?: string | null;
    summaryBullets?: string[];
    skuInsightsBullets?: string[];
};

const MonthEndBusinessSummaryCard: React.FC<Props> = ({
    loading,
    error,
    summaryBullets = [],
    skuInsightsBullets = [],
}) => {
    return (
        <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
            <PageBreadcrumb
                pageTitle="Month-end Business Summary"
                variant="page"
                align="left"
                textSize="2xl"
                 className="mb-4"
            />

            {loading ? (
                <div className="text-xs text-charcoal-500">Loading…</div>
            ) : error ? (
                <div className="text-xs text-red-600">{error}</div>
            ) : summaryBullets.length ? (
                <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
                    {summaryBullets.map((b, i) => (
                        <li key={i}>{b}</li>
                    ))}
                </ul>
            ) : (
                <div className="text-xs 2xl:text-sm text-charcoal-500">No summary available.</div>
            )}

            {skuInsightsBullets.length ? (
                <>
                    <div className="mt-4 text-xs 2xl:text-lg font-semibold text-charcoal-500">
                        SKU Insights
                    </div>
                    <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500 mt-2">
                        {skuInsightsBullets.map((b, i) => (
                            <li key={i}>{b}</li>
                        ))}
                    </ul>
                </>
            ) : null}
        </div>
    );
};

export default MonthEndBusinessSummaryCard;
