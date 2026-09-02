"use client";

import React from "react";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SegmentedToggle from "@/components/ui/SegmentedToggle";
import { AiButton } from "@/components/ui/button/AiButton";

type DashboardPageHeaderProps<TTab extends string = string> = {
    brandName: React.ReactNode;
    countryName: string;
    formattedMonthYear: string;
    handleHardRefresh: () => void;
    pageLoading: boolean;
    lastRefreshAt: string | number | Date | null | undefined;
    lastUpdatedTimeText: string;
    activeDateRegion: string;
    formatUSTime12hr: (timestamp: string | number | Date | null | undefined) => string;
    formatLastUpdatedDateTime: (
        timestamp: string | number | Date | null | undefined,
        timeZone: string
    ) => string;
    formatUKTime12hr: (timestamp: string | number | Date | null | undefined) => string;
    activeTab: TTab;
    TOP_TABS: { id: TTab; label: string }[];
    shouldScrollTabTopRef: React.MutableRefObject<boolean>;
    setActiveTab: (tab: TTab) => void;
    syncTabToHash: (tab: TTab) => void;
};

export default function DashboardPageHeader<TTab extends string = string>({
    brandName,
    countryName,
    formattedMonthYear,
    handleHardRefresh,
    pageLoading,
    lastRefreshAt,
    lastUpdatedTimeText,
    activeDateRegion,
    formatUSTime12hr,
    formatLastUpdatedDateTime,
    formatUKTime12hr,
    activeTab,
    TOP_TABS,
    shouldScrollTabTopRef,
    setActiveTab,
    syncTabToHash,
}: DashboardPageHeaderProps<TTab>) {
    return (
        <div className="sticky top-0 z-40 bg-[#F7F7F7]">
            <div className="bg-[#F7F7F7]">
               <div className="flex flex-col items-start gap-2 lg:flex-row lg:items-center lg:justify-between">

    {/* LEFT SIDE */}
    <div className="flex w-full min-w-0 flex-col leading-tight lg:w-auto">
        <p className="mb-1 truncate text-xs text-charcoal-500 sm:text-sm 2xl:text-lg">
            Let&apos;s get started,{" "}
            <span className="text-green-500">{brandName}!</span>
        </p>

        <div className="flex w-full flex-wrap items-center gap-1 lg:w-auto">
            <PageBreadcrumb
                pageTitle="Sales Dashboard - Amazon"
                variant="page"
                textSize="2xl"
            />

            <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                {countryName === "global"
                    ? "Global"
                    : countryName.toUpperCase()}
            </span>

            <span className="text-base font-bold text-green-500 sm:text-xl lg:text-lg 2xl:text-2xl">
                - {formattedMonthYear}
            </span>
        </div>
    </div>

    {/* RIGHT SIDE */}
    <div className="flex w-full flex-wrap items-center justify-between gap-2 lg:w-auto lg:flex-col lg:items-end lg:justify-start lg:gap-1">
        <AiButton
            onClick={handleHardRefresh}
            disabled={pageLoading}
            className={`shrink-0 rounded-md border shadow-sm
                px-2 py-1 text-[10px]
                sm:px-3 sm:py-1.5 sm:text-xs
                2xl:text-sm
                ${
                    pageLoading
                        ? "cursor-not-allowed border-gray-200 bg-gray-100 text-gray-400"
                        : "border-gray-300 bg-white hover:bg-gray-50"
                }`}
        >
            {pageLoading ? "Refreshing…" : "Refresh"}
        </AiButton>

        {lastRefreshAt != null && (
            <span className="whitespace-nowrap text-xs text-gray-500 2xl:text-sm">
                Last Updated at{" "}
                {lastUpdatedTimeText ||
                    (activeDateRegion === "US"
                        ? formatUSTime12hr(lastRefreshAt)
                        : activeDateRegion === "CA"
                            ? formatLastUpdatedDateTime(
                                  lastRefreshAt,
                                  "America/Toronto"
                              )
                            : formatUKTime12hr(lastRefreshAt))}
            </span>
        )}
    </div>
</div>
            </div>

            <div className="bg-[#F7F7F7] border-b border-gray-200 
    max-[480px]:py-1 max-[640px]:pb-2 sm:py-2">
                <SegmentedToggle<TTab>
                    value={activeTab}
                    options={TOP_TABS.map((t) => ({ value: t.id, label: t.label }))}
                    onChange={(tab) => {
                        shouldScrollTabTopRef.current = true;

                        setActiveTab(tab);
                        syncTabToHash(tab);
                    }}
                    className="mt-2 w-full"
                    compact
                    textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
                />
            </div>
        </div>
    );
}
