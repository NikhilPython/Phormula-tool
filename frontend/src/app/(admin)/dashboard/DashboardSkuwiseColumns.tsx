import React from "react";
import InfoTip from "@/components/ui/InfoTip";
import type { ColGroup, LeafCol } from "@/components/ui/table/GroupedCollapsibleTable";
import { toNumberSafe } from "@/lib/dashboard/format";
import type { MonthlySkuwiseTableRow } from "./DashboardTypes";

const TERM_DEFINITIONS: Record<string, string> = {
    product_name: "Product Name. The delta represents the change compared to the previous period.",
    asp: "Average Selling Price",
    ads_spend: "Ads Spend",
    acos: "ACos",
    cogs: "Cogs",
    net_units_sold: "Net Units Sold",
    net_sales: "Net Sales",
    net_taxes: "Net Taxes",
    net_credits: "Net Credits",
    tex_and_credits: "Taxes & Credits = combined effect of taxes and credits applied to orders (used to reconcile from gross to net).",
    marketplace_fees: "Marketplace Fees = total fees charged by Amazon (e.g., referral + FBA fees).",
    amazon_fee: "Marketplace Fees = total fees charged by Amazon (e.g., referral + FBA fees).",
    selling_fees: "Selling Fees = Amazon referral/commission and selling-related fees (non-FBA components).",
    fba_fees: "FBA Fees = fulfillment, storage-related and FBA service fees (as mapped in reports).",
    promotional_rebates: "Promotions = promotional rebates/discounts applied (coupons/deals) that reduce profitability.",
    promotional_rebates_percentage: "Promotions % = Promotions ÷ Net Sales × 100.",
    cost_of_unit_sold: "COGS = Cost of goods sold for the units sold in the period (as provided/derived).",
    cm1_profit: "CM1 Profit",
    cm2_profit: "CM2 Profit",
    profit_percentage: "CM1 Profit % = CM1 Profit ÷ Net Sales × 100.",
    unit_wise_profitability: "CM1 Profit Per Unit = CM1 Profit ÷ Net Units Sold.",
};

type FormatDisplayAmount = (value: any, label?: string) => React.ReactNode;

export function buildSkuwiseTableColumns(
    formatDisplayAmount: FormatDisplayAmount,
    options: { isUsSkuLayout?: boolean } = {}
) {
    const isUsSkuLayout = Boolean(options.isUsSkuLayout);

    const SKUWISE_LEFT_COLS: LeafCol<MonthlySkuwiseTableRow>[] = [
        {
            key: "sno",
            label: "S.No",
            align: "center",
            width: "6%",
        },
        {
            key: "product_name",
            label: "Product Name",
            align: "left",
            info: <InfoTip text={TERM_DEFINITIONS.product_name} />,
            width: "14%",
        },
    ];

    const SKUWISE_GROUPS = [
        {
            id: "marketplace_fees",
            label: isUsSkuLayout ? "Amazon Fees" : "Marketplace Fees",
            // info: <InfoTip text={TERM_DEFINITIONS.marketplace_fees} />,
            collapsedCols: [
                {
                    key: "marketplace_total",
                    label: isUsSkuLayout ? "Total Fees" : "Total",
                    width: "7%",
                    align: "center" as const,
                },
            ],

            expandedCols: [
                { key: "selling_fees", label: "Selling Fees", align: "center" as const },
                { key: "fba_fees", label: "FBA Fees", align: "center" as const },
                {
                    key: "marketplace_total",
                    label: isUsSkuLayout ? "Total Fees" : "Total",
                    align: "center" as const,
                },
            ],
        },

        {
            id: "quantity",
            label: isUsSkuLayout ? "Units" : "Net Units Sold",
            info: <InfoTip text={TERM_DEFINITIONS.net_units_sold} />,

            collapsedCols: [
                {
                    key: "total_quantity",
                    label: isUsSkuLayout ? "Net Units Sold" : "Total",
                    align: "center" as const,
                    width: "8%",
                    sortable: true,
                },
            ],

            expandedCols: [
                {
                    key: "sku",
                    label: "SKU",
                    align: "left" as const,
                    width: "7%",
                },
                {
                    key: "quantity",
                    label: "Units Sold",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
                {
                    key: "return_quantity",
                    label: "Return",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
                {
                    key: "total_quantity",
                    label: isUsSkuLayout ? "Net Units Sold" : "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
            ],
        },
        {
            id: "net_sales",
            label: isUsSkuLayout ? "Sales" : "Net Sales",
            info: <InfoTip text={TERM_DEFINITIONS.net_sales} />,

            collapsedCols: [
                {
                    key: "net_sales",
                    label: isUsSkuLayout ? "Net Sales" : "Total",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                },
            ],

            expandedCols: isUsSkuLayout
                ? [
                    {
                        key: "gross_sales",
                        label: "Gross Sales",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                        info: <InfoTip text="Gross Sales" />,
                    },
                    {
                        key: "refund_sales",
                        label: "Sales - Refund",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                        info: <InfoTip text="Sales refunded during this period." />,
                    },
                    {
                        key: "promotional_rebates",
                        label: "Promotions",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                        info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates} />,
                    },
                    {
                        key: "net_sales",
                        label: "Net Sales",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                    },
                ]
                : [
                    {
                        key: "gross_sales",
                        label: "Gross Sales",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                        info: <InfoTip text="Gross Sales" />,
                    },
                    {
                        key: "refund_sales",
                        label: "Sales - Refund",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                        info: <InfoTip text="Sales refunded during this period." />,
                    },
                    {
                        key: "net_sales_tax_and_credits",
                        label: "Taxes and Credits",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                        info: <InfoTip text={TERM_DEFINITIONS.tex_and_credits} />,
                    },
                    {
                        key: "net_sales",
                        label: "Total",
                        sortable: true,
                        align: "center" as const,
                        width: "7%",
                    },
                ],
        },
        {
            id: "promotions",
            label: "Promotions",
            info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates} />,

            collapsedCols: [
                {
                    key: "promotional_rebates",
                    label: "Promotions",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                    info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates} />,
                },
            ],

            expandedCols: [
                {
                    key: "promotional_rebates",
                    label: "Promotions",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                    info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates} />,
                },
                {
                    key: "promotional_rebates_percentage",
                    label: "Promotions %",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                    info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates_percentage} />,
                },
            ],
        },
        {
            id: "other_transactions",
            label: "Other Transactions",
            info: <InfoTip text={TERM_DEFINITIONS.tex_and_credits} />,

            collapsedCols: [
                {
                    key: isUsSkuLayout ? "other_transactions" : "tax_and_credits",
                    label: "Total",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                },
            ],

            expandedCols: [
                {
                    key: "tax",
                    label: "Net Taxes",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                    info: <InfoTip text={TERM_DEFINITIONS.net_taxes} />,
                },
                {
                    key: "credits",
                    label: "Net Credits",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                    info: <InfoTip text={TERM_DEFINITIONS.net_credits} />,
                },
                ...(isUsSkuLayout
                    ? [
                        {
                            key: "misc_transaction",
                            label: "Misc. Transactions",
                            sortable: true,
                            align: "center" as const,
                            width: "7%",
                        },
                    ]
                    : []),
                {
                    key: isUsSkuLayout ? "other_transactions" : "tax_and_credits",
                    label: "Total",
                    sortable: true,
                    align: "center" as const,
                    width: "7%",
                },
            ],
        },
        {
            id: "profit",
            label: "CM1 Profit",
            // info: <InfoTip text={TERM_DEFINITIONS.cm1_profit} />,

            collapsedCols: [
                {
                    key: "profit",
                    label: isUsSkuLayout ? "Margin" : "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
            ],

            expandedCols: isUsSkuLayout
                ? [
                    { key: "cm1_profit_per_unit", label: "Per Unit", align: "center" as const },
                    { key: "cm1_profit_per", label: "%", align: "center" as const },
                    { key: "profit", label: "Margin", align: "center" as const },
                ]
                : [
                    { key: "cm1_profit_per_unit", label: "Per Unit", align: "center" as const },
                    { key: "cm1_profit_per", label: "%", align: "center" as const },
                    {
                        key: "profit",
                        label: "Total",
                        align: "center" as const,
                    },
                ],
        },
        {
            id: "tax_and_credits",
            label: "Other Transactions",

            collapsedCols: [
                {
                    key: "tax_and_credits",
                    label: "Total",
                    align: "center" as const,
                    width: "10%",
                },
            ],

            expandedCols: [
                {
                    key: "tax",
                    label: "Net Taxes",
                    info: <InfoTip text={TERM_DEFINITIONS.net_taxes} />,
                    align: "center" as const
                },
                {
                    key: "credits",
                    label: "Net Credits",
                    info: <InfoTip text={TERM_DEFINITIONS.net_credits} />,
                    align: "center" as const
                },
                {
                    key: "tax_and_credits",
                    label: "Total",
                    align: "center" as const,
                },
            ],
        },
        {
            id: "ads_spend",
            label: "Ads Spend",
            info: <InfoTip text={TERM_DEFINITIONS.ads_spend} />,

            collapsedCols: [
                {
                    key: "ads_spend",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                    render: (row: MonthlySkuwiseTableRow) =>
                        formatDisplayAmount(toNumberSafe(row.ads_spend)),
                },
            ],

            expandedCols: [
                {
                    key: "product_spend",
                    label: "Sponsored Product",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                },
                {
                    key: "display_spend",
                    label: "Sponsored Display",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                },
                {
                    key: "ads_spend",
                    label: "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "8%",
                },
            ],
        },

        {
            id: "cm2_profit",
            label: "CM2 Profit",
            info: <InfoTip text={TERM_DEFINITIONS.cm2_profit} />,

            collapsedCols: [
                {
                    key: "cm2_profit",
                    label: isUsSkuLayout ? "Margin" : "Total",
                    align: "center" as const,
                    sortable: true,
                    width: "7%",
                },
            ],

            expandedCols: isUsSkuLayout
                ? [
                    { key: "cm2_profit_per_unit", label: "Per Unit", align: "center" as const },
                    { key: "cm2_profit_per", label: "%", align: "center" as const },
                    { key: "cm2_profit", label: "Margin", align: "center" as const, sortable: true },
                ]
                : [
                    { key: "cm2_profit_per_unit", label: "Per Unit", align: "center" as const },
                    { key: "cm2_profit_per", label: "%", align: "center" as const },
                    {
                        key: "cm2_profit",
                        label: "Total",
                        align: "center" as const,
                        sortable: true,
                    },
                ],
        },
    ];

    const SKUWISE_SINGLE_COLS: LeafCol<MonthlySkuwiseTableRow>[] = [
        { key: "quantity", label: "Net Units Sold", align: "center" as const },
        {
            key: "asp",
            label: "ASP",
            info: <InfoTip text={TERM_DEFINITIONS.asp} />,
            align: "center" as const,
            width: "7%",
        },
        { key: "cogs", label: "COGS", align: "center" as const, },
        {
            key: "promotional_rebates_percentage",
            label: "Promotions %",
            align: "center" as const,
            width: "10%",
            noWrap: true,
            thClassName: "whitespace-nowrap",
            info: <InfoTip text={TERM_DEFINITIONS.promotional_rebates_percentage} />,
        },
        { key: "profit", label: "CM1 Profit", align: "center" as const },
        { key: "ads_spend", label: "Ads Spend", align: "center" as const, },
        { key: "acos", label: "ACoS %", align: "center" as const, },
        { key: "cm2_profit", label: "CM2 Profit", align: "center" as const },
        { key: "cm1_profit_per", label: "CM1 Profit Per Unit", align: "center" as const },
        { key: "cm1_profit_per_unit", label: "CM1 Profit %", align: "center" as const },
        { key: "cm2_profit_per", label: "CM2 Profit Per Unit", align: "center" as const },
        { key: "cm2_profit_per_unit", label: "CM2 Profit %", align: "center" as const }

    ];

    return {
        SKUWISE_LEFT_COLS,
        SKUWISE_GROUPS,
        SKUWISE_SINGLE_COLS,
    };
}
