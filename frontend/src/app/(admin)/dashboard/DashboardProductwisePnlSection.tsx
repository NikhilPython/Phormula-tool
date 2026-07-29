"use client";

import React from "react";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import GroupedCollapsibleTable from "@/components/ui/table/GroupedCollapsibleTable";
import { fmtInt } from "@/lib/dashboard/format";
import {
    RiCollapseDiagonalFill,
    RiExpandDiagonalFill,
    RiLayoutColumnFill,
    RiLayoutColumnLine,
} from "react-icons/ri";

type DashboardProductwisePnlSectionProps = Record<string, any>;

const getOptionalSummaryNumber = (source: unknown, keys: string[]) => {
    const record =
        source && typeof source === "object" ? (source as Record<string, unknown>) : null;

    for (const key of keys) {
        if (!record || !Object.prototype.hasOwnProperty.call(record, key)) continue;

        const value = record[key];
        if (value === undefined || value === null || value === "") continue;

        const normalized =
            typeof value === "number"
                ? value
                : String(value).replace(/,/g, "").trim();

        if (
            typeof normalized === "string" &&
            (normalized === "-" ||
                (normalized.length === 1 &&
                    (normalized.charCodeAt(0) === 8211 || normalized.charCodeAt(0) === 8212)))
        ) {
            continue;
        }

        const n = typeof normalized === "number" ? normalized : Number(normalized);
        if (Number.isFinite(n)) return n;
    }

    return null;
};

const nonZeroOrNull = (value: unknown) => {
    const n = Number(String(value ?? "").replace(/,/g, "").trim());
    return Number.isFinite(n) && n !== 0 ? n : null;
};

const sumKnownNumbers = (...values: Array<number | null>) => {
    const known = values.filter((value): value is number => value !== null);
    return known.length ? known.reduce((sum, value) => sum + value, 0) : null;
};

export default function DashboardProductwisePnlSection({
    currencySymbol,
    adsLoading,
    showAllMtdProductwiseRows,
    setShowAllMtdProductwiseRows,
    productwiseAllColumnsExpanded,
    handleToggleProductwiseAllColumns,
    handleDownloadPlProductwiseMtd,
    shouldShowDummyUi,
    loading,
    monthlySkuwiseRows,
    finalMonthlySkuwiseRowsForTable,
    shouldScrollMtdProductwiseTable,
    productwiseHasExpandedGroups,
    setProductwiseAnyGroupExpanded,
    SKUWISE_LEFT_COLS,
    SKUWISE_GROUPS,
    SKUWISE_SINGLE_COLS,
    productwiseInitialCollapsed,
    productwiseCollapsed,
    setProductwiseCollapsed,
    setProductwiseAllColumnsExpanded,
    PRODUCTWISE_GROUP_IDS,
    plSortConfig,
    setPlSortConfig,
    getAdsSignForCol,
    mtdProductwiseTableScrollHeight,
    toNumber,
    getSkuwiseDisplayProductName,
    openPnlSkuDrawer,
    renderLiveNetSalesDelta,
    formatAdsNumber,
    formatAdType,
    costOfAds,
    formatSummaryRounded,
    sponsoredBrandSpend,
    dealVouchers,
    platformFee,
    plSummaryTotals,
    formatSummaryValue,
    lost_inventory_total,
    otherPlatformFee,
    countryName,
    isUsPnlSkuLayout,
    getProductwiseOtherTransactionsTotal,
    totalRowCm2Profit,
    totalRowCm2Margins,
    tacosFromDisplayedCardsForSummary,
    reimbursementForSummary,
    reimbursementVsCm2PctForSummary,
    reimbursementVsSalesPctForSummary,
}: DashboardProductwisePnlSectionProps) {
    const summaryTotalRow = React.useMemo(() => {
        const rows = Array.isArray(finalMonthlySkuwiseRowsForTable) ? finalMonthlySkuwiseRowsForTable : [];

        return (
            rows.find((row: any) => {
                const sku = String(row?.sku || "").trim().toUpperCase();
                const name = String(row?.product_name || "").trim().toLowerCase();

                return (
                    !!row?.isTotal ||
                    sku === "GRAND_TOTAL" ||
                    sku === "TOTAL" ||
                    name === "grand total" ||
                    name === "total"
                );
            }) ||
            rows[rows.length - 1] ||
            {}
        );
    }, [finalMonthlySkuwiseRowsForTable]);

    const getSummaryNumber = React.useCallback(
        (keys: string[]) =>
            getOptionalSummaryNumber(summaryTotalRow, keys) ??
            getOptionalSummaryNumber(plSummaryTotals, keys),
        [plSummaryTotals, summaryTotalRow]
    );

    const placementFees = getSummaryNumber(["placement_fee"]);
    const shippingCharges = getSummaryNumber(["shipment_fees"]);
    const customsFees = getSummaryNumber(["customs_fee"]);
    const shippingChargesTotal = getSummaryNumber(["shipping_charges"]);

    const shortTermStorage =
        getSummaryNumber(["short_term_storage_fee", "short_term_storage"]) ??
        nonZeroOrNull(plSummaryTotals?.short_term_storage_fee);
    const longTermStorage =
        getSummaryNumber(["long_term_storage_fee", "long_term_storage"]) ??
        nonZeroOrNull(plSummaryTotals?.long_term_storage_fee);
    const storageFees = getSummaryNumber(["storage_fee"]);

    const inventoryCharges =
        getSummaryNumber([
            "inventory_charges",
            "inventory_charge",
            "inventory_fees",
            "inventory_fee",
            "fba_disposal",
        ]) ?? nonZeroOrNull(plSummaryTotals?.fba_disposal);
    const reimbursementForLostInventory =
        getSummaryNumber([
            "reimbursement_lost_inventory_amount",
            "lost_inventory_reimbursement",
            "lost_total",
        ]) ?? nonZeroOrNull(lost_inventory_total);
    const inventoryChargesAndReimbursement =
        getSummaryNumber([
            "inventory_charges_and_reimbursement",
            "inventory_charges_reimbursement",
            "inventory_charges_reimbursement_total",
        ]) ??
        (inventoryCharges !== null && reimbursementForLostInventory !== null
            ? inventoryCharges - reimbursementForLostInventory
            : null);

    const platformManagementFees = getSummaryNumber(["platform_management_fees"]);
    const others = getSummaryNumber(["other_adjustment"]);

    const formatSummaryValueOrDash = (value: number | null, key: string) => {
        return value === null ? "-" : formatSummaryValue(value, key);
    };

    const productwiseSummaryRows = isUsPnlSkuLayout
        ? [
            {
                type: "section" as const,
                id: "ads",
                label: <>Cost of Advertisement <strong className="text-[#ff5c5c]">(-)</strong></>,
                endValue: formatSummaryRounded(costOfAds),
                defaultCollapsed: true,
                children: [
                    {
                        id: "ads_1",
                        label: <>Visibility - Ads <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryRounded(sponsoredBrandSpend),
                    },
                    {
                        id: "ads_3",
                        label: <>Visibility - Deals, Vouchers and Reviews <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryRounded(dealVouchers),
                    },
                ],
            },
            {
                type: "section" as const,
                id: "shipping_charges",
                label: <>Shipping Charges <strong className="text-[#ff5c5c]">(-)</strong></>,
                endValue: formatSummaryValueOrDash(shippingChargesTotal, "shipping_charges"),
                defaultCollapsed: true,
                children: [
                    {
                        id: "placement_fees",
                        label: <>Placement Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValueOrDash(placementFees, "placement_fee"),
                    },
                    {
                        id: "shipping_charges_child",
                        label: <>Shipping Charges <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValueOrDash(shippingCharges, "shipment_fees"),
                    },
                    {
                        id: "customs_fees",
                        label: <>Customs Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValueOrDash(customsFees, "customs_fee"),
                    },
                ],
            },
            {
                type: "section" as const,
                id: "storage_fee",
                label: <>Storage Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                endValue: formatSummaryValueOrDash(storageFees, "storage_fee"),
                defaultCollapsed: true,
                children: [
                    {
                        id: "short_term_storage_fee",
                        label: <>Short Term Storage <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValueOrDash(shortTermStorage, "short_term_storage_fee"),
                    },
                    {
                        id: "long_term_storage_fee",
                        label: <>Long Term Storage <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValueOrDash(longTermStorage, "long_term_storage_fee"),
                    },
                ],
            },
            {
                type: "section" as const,
                id: "inventory_charges_reimbursement",
                label: "Inventory Charges and Reimbursement",
                endValue: formatSummaryValueOrDash(
                    inventoryChargesAndReimbursement,
                    "inventory_charges_and_reimbursement"
                ),
                defaultCollapsed: true,
                children: [
                    {
                        id: "inventory_charges",
                        label: <>Inventory Charges <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValueOrDash(inventoryCharges, "inventory_charges"),
                    },
                    {
                        id: "lost_inventory_reimbursement",
                        label: (
                            <>
                                Reimbursement for lost Inventory
                                {plSummaryTotals?.reimbursement_lost_inventory_units
                                    ? ` - ${plSummaryTotals.reimbursement_lost_inventory_units} Units `
                                    : " "}
                                <strong className="text-green-500">(+)</strong>
                            </>
                        ),
                        midValue: formatSummaryValueOrDash(
                            reimbursementForLostInventory,
                            "reimbursement_lost_inventory_amount"
                        ),
                    },
                ],
            },
            {
                type: "fixed" as const,
                id: "platform_management_fees",
                label: "Platform Management Fees",
                endValue: formatSummaryValueOrDash(
                    platformManagementFees,
                    "platform_management_fees"
                ),
            },
            {
                type: "fixed" as const,
                id: "others",
                label: "Others",
                endValue: formatSummaryValueOrDash(others, "other_adjustment"),
            },
            {
                type: "fixed" as const,
                id: "cm2_profit",
                label: "CM2 Profit",
                endValue: Math.round(totalRowCm2Profit).toLocaleString(),
            },
            {
                type: "fixed" as const,
                id: "cm2_profit_percentage",
                label: "CM2 Profit %",
                endValue: `${formatSummaryValue(totalRowCm2Margins, "cm2_margins")}%`,
            },
            {
                type: "fixed" as const,
                id: "tacos",
                label: "TACoS (Total Advertising Cost of Sale)",
                endValue: `${formatSummaryValue(
                    tacosFromDisplayedCardsForSummary,
                    "acos"
                )}%`,
            },
            {
                type: "section" as const,
                id: "net_reimbursement",
                label: "Net Reimbursement",
                endValue: formatSummaryValue(
                    reimbursementForSummary,
                    "net_reimbursement"
                ),
                defaultCollapsed: true,
                children: [
                    {
                        id: "net_reimbursement_debt_payment",
                        label: (
                            <>
                                Charged <strong className="text-[#ff5c5c]">(-)</strong>
                            </>
                        ),
                        midValue: formatSummaryValue(
                            plSummaryTotals.debt_payment,
                            "debt_payment"
                        ),
                    },
                    {
                        id: "net_reimbursement_disbursement",
                        label: (
                            <>
                                Disbursement <strong className="text-green-500">(+)</strong>
                            </>
                        ),
                        midValue: formatSummaryValue(
                            plSummaryTotals.disbursement,
                            "disbursement"
                        ),
                    },
                ],
            },
            {
                type: "fixed" as const,
                id: "rv_sales",
                label: "Reimbursement vs Sales",
                endValue: `${formatSummaryValue(
                    reimbursementVsSalesPctForSummary,
                    "reimbursement_vs_sales"
                )}%`,
            },
            {
                type: "fixed" as const,
                id: "rv_cm2",
                label: "Reimbursement vs CM2 Margins",
                endValue: `${formatSummaryValue(
                    reimbursementVsCm2PctForSummary,
                    "rembursment_vs_cm2_margins"
                )}%`,
            },
        ]
        : [
            {
                type: "section" as const,
                id: "ads",
                label: <>Cost of Advertisement <strong className="text-[#ff5c5c]">(-)</strong></>,
                endValue: formatSummaryRounded(costOfAds),
                defaultCollapsed: true,
                children: [
                    {
                        id: "ads_1",
                        label: <>Visibility - Ads <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryRounded(sponsoredBrandSpend),
                    },
                    {
                        id: "ads_3",
                        label: <>Visibility - Deals, Vouchers and Reviews <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryRounded(dealVouchers),
                    },
                ],
            },
            {
                type: "section" as const,
                id: "other",
                label: "Other Transactions",
                endValue: formatSummaryRounded(platformFee),
                defaultCollapsed: true,
                children: [
                    {
                        id: "short_term_storage_fee",
                        label: <>Short Term Storage Fee <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValue(
                            plSummaryTotals.short_term_storage_fee,
                            "short_term_storage_fee"
                        ),
                    },
                    {
                        id: "long_term_storage_fee",
                        label: <>Long Term Storage Fee <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValue(
                            plSummaryTotals.long_term_storage_fee,
                            "long_term_storage_fee"
                        ),
                    },
                    {
                        id: "fba_disposal",
                        label: <>FBA Disposal <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValue(
                            plSummaryTotals.fba_disposal,
                            "fba_disposal"
                        ),
                    },
                    {
                        id: "other_3",
                        label: (
                            <>
                                Reimbursement for lost Inventory{" "}
                                <strong className="text-green-500">(+)</strong>
                            </>
                        ),
                        midValue: formatSummaryValue(lost_inventory_total, "lost_total"),
                    },
                    {
                        id: "other_misc",
                        label: <>Misc. Transactions <strong className="text-green-500">(+)</strong></>,
                        midValue: formatSummaryValue(
                            plSummaryTotals.misc_transaction,
                            "misc_transaction"
                        ),
                    },
                    {
                        id: "other_1",
                        label: <>Other Platform Fees <strong className="text-[#ff5c5c]">(-)</strong></>,
                        midValue: formatSummaryValue(otherPlatformFee, "platformfeenew"),
                    },
                ],
            },
            ...(countryName === "us" || countryName === "global"
                ? [
                    {
                        type: "fixed" as const,
                        id: "ship",
                        label: (
                            <>
                                Shipment Charges <strong className="text-[#ff5c5c]">(-)</strong>
                            </>
                        ),
                        endValue: formatSummaryValue(
                            plSummaryTotals.shipment_charges,
                            "shipment_charges"
                        ),
                    },
                ]
                : []),
            {
                type: "fixed" as const,
                id: "cm2_profit",
                label: "CM2 Profit/Loss",
                endValue: Math.round(totalRowCm2Profit).toLocaleString(),
            },
            {
                type: "fixed" as const,
                id: "cm2_margins",
                label: "CM2 Margins",
                endValue: `${formatSummaryValue(totalRowCm2Margins, "cm2_margins")}%`,
            },
            {
                type: "fixed" as const,
                id: "tacos",
                label: "TACoS (Total Advertising Cost of Sale)",
                endValue: `${formatSummaryValue(
                    tacosFromDisplayedCardsForSummary,
                    "acos"
                )}%`,
            },
            {
                type: "section" as const,
                id: "net_reimbursement",
                label: "Net Reimbursement",
                endValue: formatSummaryValue(
                    reimbursementForSummary,
                    "net_reimbursement"
                ),
                defaultCollapsed: true,
                children: [
                    {
                        id: "net_reimbursement_debt_payment",
                        label: (
                            <>
                                Charged <strong className="text-[#ff5c5c]">(-)</strong>
                            </>
                        ),
                        midValue: formatSummaryValue(
                            plSummaryTotals.debt_payment,
                            "debt_payment"
                        ),
                    },
                    {
                        id: "net_reimbursement_disbursement",
                        label: (
                            <>
                                Disbursement <strong className="text-green-500">(+)</strong>
                            </>
                        ),
                        midValue: formatSummaryValue(
                            plSummaryTotals.disbursement,
                            "disbursement"
                        ),
                    },
                ],
            },
            {
                type: "fixed" as const,
                id: "rv_cm2",
                label: "Reimbursement vs CM2 Margins",
                endValue: `${formatSummaryValue(
                    reimbursementVsCm2PctForSummary,
                    "rembursment_vs_cm2_margins"
                )}%`,
            },
            {
                type: "fixed" as const,
                id: "rv_sales",
                label: "Reimbursement vs Sales",
                endValue: `${formatSummaryValue(
                    reimbursementVsSalesPctForSummary,
                    "reimbursement_vs_sales"
                )}%`,
            },
        ];

    return (
                        <div id="pnl-mtd" className="scroll-mt-[10px] mt-2 md:mt-4 w-full rounded-xl border bg-white p-4 sm:p-5 shadow-sm overflow-hidden">
                            <div className="mb-3 relative flex items-center justify-between gap-3">
                                {/* LEFT: Title */}
                                <div className="flex items-center gap-2">
                                    <PageBreadcrumb
                                        pageTitle="P&L Productwise Breakdown"
                                        variant="page"
                                        align="left"
                                        textSize="2xl"
                                    />
                                    <span className="text-base sm:text-xl lg:text-lg 2xl:text-2xl text-green-500 font-semibold">
                                        ({currencySymbol})
                                    </span>
                                </div>
                                {/* CENTER: Ads loading message */}
                                {adsLoading && (
                                    <div className="absolute left-1/2 -translate-x-1/2 flex items-center gap-2 text-sm 2xl:text-base text-charcoal-700 font-medium">
                                        <span className="inline-block h-2.5 w-2.5 rounded-full bg-charcoal-500 animate-pulse" />
                                        Ads data is being fetched, please wait…
                                    </div>
                                )}

                                <div className="flex items-center gap-2">
                                    {/* Expand / collapse rows */}
                                    <button
                                        type="button"
                                        onClick={() => setShowAllMtdProductwiseRows((prev: boolean) => !prev)}
                                        title={showAllMtdProductwiseRows ? "Collapse rows" : "Expand all rows"}
                                        aria-label={showAllMtdProductwiseRows ? "Collapse rows" : "Expand all rows"}
                                        className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                    >
                                        {showAllMtdProductwiseRows ? (
                                            <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                                        ) : (
                                            <RiExpandDiagonalFill size={18} className="font-extrabold" />
                                        )}
                                    </button>

                                    {/* Expand / collapse all columns */}
                                    <button
                                        type="button"
                                        onClick={handleToggleProductwiseAllColumns}
                                        title={
                                            productwiseAllColumnsExpanded
                                                ? "Collapse all columns"
                                                : "Expand all columns"
                                        }
                                        aria-label={
                                            productwiseAllColumnsExpanded
                                                ? "Collapse all columns"
                                                : "Expand all columns"
                                        }
                                        className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                    >
                                        {productwiseAllColumnsExpanded ? (
                                            <RiLayoutColumnLine size={18} className="font-extrabold" />
                                        ) : (
                                            <RiLayoutColumnFill size={18} className="font-extrabold" />
                                        )}
                                    </button>

                                    <DownloadIconButton
                                        onClick={handleDownloadPlProductwiseMtd}
                                        aria-label="Download P&L Productwise Breakdown MTD"
                                        className="transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md"
                                    />
                                </div>
                            </div>

                            {!shouldShowDummyUi && loading && monthlySkuwiseRows.length === 0 ? (
                                <div className="text-sm text-gray-500">Loading…</div>
                            ) : finalMonthlySkuwiseRowsForTable.length === 0 ? (
                                <div className="text-sm text-red-600">
                                    No P&L productwise rows available for this period.
                                </div>
                            ) : (
                                <div
                                    className={[
                                        "w-full max-w-full rounded-xl border border-gray-300",
                                        shouldScrollMtdProductwiseTable
                                            ? "overflow-hidden"
                                            : "overflow-x-auto overflow-y-hidden",
                                    ].join(" ")}
                                >
                                    <div className="w-full max-w-full">
                                        <GroupedCollapsibleTable<any>
                                            rows={finalMonthlySkuwiseRowsForTable}
                                            onAnyGroupExpandedChange={setProductwiseAnyGroupExpanded}
                                            tableClassName={[
                                                "border-collapse bg-white text-[#414042] text-[12px] lg:text-[12px] min-[1700px]:text-[14px]",
                                                productwiseHasExpandedGroups ? "table-fixed" : "w-full table-fixed",
                                            ].join(" ")}
                                            getRowKey={(row, idx) =>
                                                row.isTotal
                                                    ? "TOTAL"
                                                    : row.isOthers
                                                        ? "OTHERS"
                                                        : row.sku || String(idx)
                                            }
                                            leftCols={SKUWISE_LEFT_COLS}
                                            groups={SKUWISE_GROUPS}
                                            singleCols={SKUWISE_SINGLE_COLS}
                                            initialCollapsed={productwiseInitialCollapsed}
                                            collapsedState={productwiseCollapsed}
                                            onCollapsedChange={(next) => {
                                                setProductwiseCollapsed(next);

                                                setProductwiseAllColumnsExpanded(
                                                    PRODUCTWISE_GROUP_IDS.length > 0 &&
                                                    PRODUCTWISE_GROUP_IDS.every((groupId: string) => next[groupId] === false)
                                                );
                                            }}
                                            defaultSort={plSortConfig}
                                            onSortChange={setPlSortConfig}
                                            showSignRowInBody
                                            getSignForCol={getAdsSignForCol}
                                            bodyMaxHeight={
                                                shouldScrollMtdProductwiseTable
                                                    ? mtdProductwiseTableScrollHeight
                                                    : undefined
                                            }
                                            preserveColumnWidths="responsive"
                                            isTotalRow={(row) => {
                                                const name = String(row?.product_name || "").trim().toLowerCase();
                                                const sku = String(row?.sku || "").trim().toUpperCase();

                                                return (
                                                    !!row.isTotal ||
                                                    sku === "GRAND_TOTAL" ||
                                                    sku === "TOTAL" ||
                                                    name === "grand total" ||
                                                    name === "total"
                                                );
                                            }}

                                            layout={[
                                                { type: "group", id: "quantity" },
                                                { type: "single", key: "asp" },
                                                { type: "group", id: "net_sales" },
                                                ...(isUsPnlSkuLayout
                                                    ? [{ type: "single" as const, key: "promotional_rebates_percentage" }]
                                                    : [{ type: "group" as const, id: "promotions" }]),
                                                { type: "single", key: "cogs" },
                                                { type: "group", id: "marketplace_fees" },
                                                { type: "group", id: "other_transactions" },
                                                { type: "group", id: "profit" },
                                                { type: "group", id: "ads_spend" },
                                                { type: "single", key: "acos" },
                                                { type: "group", id: "cm2_profit" },
                                            ]}

                                            // initialCollapsed={{ marketplace_fees: false }}
                                            getRowClassName={(row, index) => {
                                                if (row.isTotal) return "bg-[#EFEFEF] font-semibold";
                                                if (row.isOthers) return "bg-white";
                                                return index % 2 === 0 ? "bg-white" : "bg-gray-50";
                                            }}
                                            getValue={(row, colKey) => {
                                                if (colKey === "sno") return row.isTotal ? "" : row.sno ?? "";
                                                if (colKey === "sku") {
                                                    if (row.isOthers || row.isTotal) return "-";
                                                    return row.sku || "-";
                                                }
                                                if (colKey === "quantity") {
                                                    return fmtInt(toNumber((row as any).quantity));
                                                }

                                                if (colKey === "return_quantity") {
                                                    return fmtInt(toNumber((row as any).return_quantity));
                                                }

                                                if (colKey === "total_quantity") {
                                                    return fmtInt(
                                                        toNumber(
                                                            (row as any).total_quantity ??
                                                            (toNumber((row as any).quantity) - toNumber((row as any).return_quantity))
                                                        )
                                                    );
                                                }
                                                if (colKey === "product_name") {
                                                    if (row.isTotal) {
                                                        return (
                                                            <span className="inline-block w-full truncate font-semibold">
                                                                Total
                                                            </span>
                                                        );
                                                    }

                                                    if (row.isOthers) {
                                                        return (
                                                            <span
                                                                className="inline-block w-full truncate text-green-500"
                                                                title="Aggregated remaining products"
                                                            >
                                                                Others
                                                            </span>
                                                        );
                                                    }


                                                    const displayName = getSkuwiseDisplayProductName(row);

                                                    return (
                                                        <button
                                                            type="button"
                                                            onClick={() =>
                                                                openPnlSkuDrawer({
                                                                    ...row,
                                                                    product_name: displayName,
                                                                })
                                                            }
                                                            className="flex w-full items-center justify-between gap-3 text-left text-green-500"
                                                            title={String(displayName || "")}
                                                        >
                                                            <span className="min-w-0 truncate">
                                                                {displayName}
                                                            </span>

                                                            {renderLiveNetSalesDelta({
                                                                ...row,
                                                                product_name: displayName,
                                                            })}
                                                        </button>
                                                    );
                                                }

                                                if (colKey === "quantity")
                                                    return Math.round(Number(row.quantity || 0)).toLocaleString();

                                                if (colKey === "asp") return formatAdsNumber(row.asp);
                                                if (colKey === "net_sales") return Math.round(Number(row.net_sales || 0)).toLocaleString();

                                                if (colKey === "other_transactions") {
                                                    const v = Number(
                                                        typeof getProductwiseOtherTransactionsTotal === "function"
                                                            ? getProductwiseOtherTransactionsTotal(row)
                                                            : row.other_transactions ?? row.tax_and_credits ?? 0
                                                    );

                                                    return Math.round(Number.isFinite(v) ? v : 0).toLocaleString();
                                                }

                                                if (colKey === "misc_transaction") {
                                                    const v = Number((row as any)[colKey] ?? 0);
                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString();
                                                }

                                                if (colKey === "tax" || colKey === "credits" || colKey === "tax_and_credits") {
                                                    const v = Number((row as any)[colKey] ?? 0);
                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString();
                                                }
                                                if (colKey === "cm1_profit_per") {
                                                    const v = Number(row.cm1_profit_per ?? 0);
                                                    return `${formatAdsNumber(Math.abs(v))}%`;
                                                }

                                                if (colKey === "cm1_profit_per_unit") {
                                                    const v = Number(row.cm1_profit_per_unit ?? 0);
                                                    return formatAdsNumber(Math.abs(v));
                                                }

                                                if (colKey === "cm2_profit_per") {
                                                    const v = Number(row.cm2_profit_per ?? 0);
                                                    return `${formatAdsNumber(v)}%`;
                                                }

                                                // CM2 per unit (no %)
                                                if (colKey === "cm2_profit_per_unit") {
                                                    const v = Number(row.cm2_profit_per_unit ?? 0);
                                                    return formatAdsNumber(v);
                                                }
                                                if (colKey === "ad_type") {
                                                    if (row.isOthers || row.isTotal) return "-";
                                                    return formatAdType((row as any).ad_type);
                                                }
                                                if (
                                                    colKey === "product_spend" ||
                                                    colKey === "display_spend" ||
                                                    colKey === "brand_spend"
                                                ) {
                                                    const v = Number((row as any)[colKey] ?? 0);

                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString("en-GB", {
                                                        minimumFractionDigits: 0,
                                                        maximumFractionDigits: 0,
                                                    });
                                                }

                                                if (colKey === "ads_spend") {
                                                    const v = Number(row.ads_spend ?? 0);

                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString("en-GB", {
                                                        minimumFractionDigits: 0,
                                                        maximumFractionDigits: 0,
                                                    });
                                                }

                                                if (colKey === "ads_spend")
                                                    return Math.round(Math.abs(Number(row.ads_spend || 0))).toLocaleString();
                                                if (colKey === "acos") {
                                                    const v = Number(row.acos ?? 0);
                                                    return `${formatAdsNumber(v)}%`;
                                                }
                                                if (colKey === "cogs")
                                                    return Math.round(Math.abs(Number(row.cogs || 0))).toLocaleString();

                                                if (colKey === "fba_fees")
                                                    return Math.round(Math.abs(Number(row.fba_fees || 0))).toLocaleString();

                                                if (colKey === "selling_fees")
                                                    return Math.round(Math.abs(Number(row.selling_fees || 0))).toLocaleString();

                                                if (colKey === "marketplace_total")
                                                    return Math.round(
                                                        Math.abs(Number(row.fba_fees || 0)) + Math.abs(Number(row.selling_fees || 0))
                                                    ).toLocaleString();
                                                if (colKey === "cm2_profit")
                                                    return Math.round(Number(row.cm2_profit || 0)).toLocaleString();
                                                if (colKey === "profit")
                                                    return Math.round(Number(row.profit || 0)).toLocaleString();
                                                if (colKey === "promotional_rebates") {
                                                    const v = Number((row as any)[colKey] ?? 0);

                                                    return Math.round(Math.abs(Number.isFinite(v) ? v : 0)).toLocaleString("en-GB", {
                                                        minimumFractionDigits: 0,
                                                        maximumFractionDigits: 0,
                                                    });
                                                }

                                                if (colKey === "promotional_rebates_percentage") {
                                                    const v = Number((row as any)[colKey] ?? 0);

                                                    const value = Math.abs(Number.isFinite(v) ? v : 0).toLocaleString("en-GB", {
                                                        minimumFractionDigits: 2,
                                                        maximumFractionDigits: 2,
                                                    });

                                                    return `${value}%`;
                                                }
                                                if (
                                                    colKey === "gross_sales" ||
                                                    colKey === "refund_sales" ||
                                                    colKey === "net_sales_tax_and_credits" ||
                                                    colKey === "net_sales"
                                                ) {
                                                    const v = Number((row as any)[colKey] ?? 0);

                                                    return Math.round(Number.isFinite(v) ? v : 0).toLocaleString("en-GB", {
                                                        minimumFractionDigits: 0,
                                                        maximumFractionDigits: 0,
                                                    });
                                                }

                                                return (row as any)[colKey] ?? "";
                                            }}
                                            summary={{
                                                enabled: finalMonthlySkuwiseRowsForTable.length > 0,
                                                rows: productwiseSummaryRows,

                                                valueCols: 2,
                                                boldSectionsByDefault: false,
                                            }}
                                        />
                                    </div>
                                </div>
                            )}

                        </div>
    );
}
