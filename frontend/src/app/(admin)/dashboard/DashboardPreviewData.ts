import type { ApiResponse } from "@/components/businessInsight/types";
import type { InventoryRow } from "@/lib/inventory/fetchCurrentInventoryData";
import type {
    Cm1PieSlice,
    GraphDailySeries,
    InventoryAlertRecord,
    MonthlySkuwiseTableRow,
} from "./DashboardTypes";

export const dummyStatData = {
    units: { current: 0, previous: 0, deltaPct: 0 },
    grossSales: { current: 0, previous: 0, deltaPct: 0 },
    netSales: { current: 0, previous: 0, deltaPct: 0 },
    asp: { current: 0, previous: 0, deltaPct: 0 },
    costOfAds: { current: 0, previous: 0, deltaPct: 0 },
    tacos: { current: 0, previous: 0, deltaPct: 0 },
    cm2Profit: { current: 0, previous: 0, deltaPct: 0 },
    cm2ProfitPct: { current: 0, previous: 0, deltaPct: 0 },
};

export const dummyLiveBusinessClientData: ApiResponse & {
    portfolio_recommendation?: string;
    objective_context?: {
        growth_intent: string;
        inventory_clearance_priority: boolean;
        profit_priority: string;
    };
    ads_recommendation?: string;
    inventory_summary?: {
        alert_bullets: string[];
        summary_text: string;
    };
    recommended_actions_mtd?: Record<string, string>;
    remaining_skus_block?: string;
    categorized_growth?: any;
    ai_insights?: Record<string, any>;
} = {
    periods: {
        previous: {
            label: "Feb MTD",
            start_date: "2026-02-01",
            end_date: "2026-02-05",
        },
        current_mtd: {
            label: "Mar MTD",
            start_date: "2026-03-01",
            end_date: "2026-03-05",
        },
    },

    overall_summary: [
        "Business delivered healthy month-to-date growth driven by stronger unit movement, better sales conversion, and stable contribution margins across core SKUs.",
    ],

    overall_actions: [
        "Increase focus on top-performing SKUs with strong unit velocity and stable profitability.",
        "Reduce inefficient ad spend on low-contribution SKUs.",
        "Monitor low-stock products to avoid losing momentum during the current period.",
    ],

    portfolio_recommendation:
        "Keep scaling winning SKUs while protecting margin on mid-tier products and correcting slow-moving inventory pockets.",

    objective_context: {
        growth_intent: "aggressive",
        inventory_clearance_priority: true,
        profit_priority: "protect_growth",
    },

    ads_recommendation:
        "Shift budget toward high-conversion SKUs and reduce exposure on products with weak sales-to-spend efficiency.",

    inventory_summary: {
        alert_bullets: [
            "Dummy Product 2 is approaching low stock threshold.",
            "Dummy Product 3 is in high alert and may require urgent replenishment.",
        ],
        summary_text:
            "Inventory health is mixed. A few hero SKUs are healthy, but 2 products need replenishment attention to avoid sales loss.",
    },

    recommended_actions_mtd: {
        "1": `Dummy Product 1
Units: 0
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
Dummy Product 1 has maintained steady momentum and gained share through stronger conversion and stable pricing.

Recommendation:
Continue scaling this SKU with controlled inventory support and sustained ad investment.

Advertising:
Increase spend moderately on top-performing keywords and branded placements.

Inventory:
Maintain healthy stock cover and avoid under-ordering during high velocity weeks.`,

        "2": `Dummy Product 2
Units: 0 (0%)
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
Dummy Product 2 is growing, though ASP softness indicates price pressure.

Recommendation:
Protect profitability by limiting discount dependency and improving listing conversion.

Advertising:
Reduce non-performing spend and prioritize campaigns with stronger ROAS.

Inventory:
Replenish conservatively and align purchase planning with recent sell-through.`,

        "3": `Dummy Product 3
Units: 0 (0%)
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
The remaining SKU portfolio is contributing moderate growth but with weaker profitability consistency.

Recommendation:
Clean up weak SKUs, focus on margin-positive products, and improve assortment efficiency.

Advertising:
Consolidate spend toward better converting products and pause weak ad groups.

Inventory:
Use tighter replenishment rules on slow-moving items to avoid inventory drag.`,
    },


    remaining_skus_block: `Other SKUs
Units: 0 (0%)
ASP: £0.00 (0%)
Net sales: £0.00 (0%)
CM1 profit per unit: £0.00 (0%)
CM1 profit: £0.00 (0%)

Product Journey:
The remaining SKU portfolio is contributing moderate growth but with weaker profitability consistency.

Recommendation:
Clean up weak SKUs, focus on margin-positive products, and improve assortment efficiency.

Advertising:
Consolidate spend toward better converting products and pause weak ad groups.

Inventory:
Use tighter replenishment rules on slow-moving items to avoid inventory drag.`,

    categorized_growth: {
        top_80_skus: [
            {
                product_name: "Dummy Product 1",
                sku: "DUMMY-SKU-001",
                quantity_month1: 0,
                quantity_month2: 0,
                asp_month1: 0,
                asp_month2: 0,
                product_sales_month1: 0,
                product_sales_month2: 0,
                net_sales_month1: 0,
                net_sales_month2: 0,
                sales_mix_month1: 0,
                sales_mix_month2: 0,
                unit_wise_profitability_month1: 0,
                unit_wise_profitability_month2: 0,
                profit_month1: 0,
                profit_month2: 0,
                profit_percentage_month1: 0,
                profit_percentage_month2: 0,
                "Sales Mix (Month2)": 0,
                "Unit Growth": { category: "growth", value: 0 },
                "ASP Growth": { category: "growth", value: 0 },
                "Sales Growth": { category: "growth", value: 0 },
                "Net Sales Growth": { category: "growth", value: 0 },
                "Sales Mix Change": { category: "growth", value: 0 },
                "Profit Per Unit": { category: "growth", value: 0 },
                "CM1 Profit Impact": { category: "growth", value: 0 },
            },
            {
                product_name: "Dummy Product 2",
                sku: "DUMMY-SKU-002",
                quantity_month1: 0,
                quantity_month2: 0,
                asp_month1: 0,
                asp_month2: 0,
                product_sales_month1: 0,
                product_sales_month2: 0,
                net_sales_month1: 0,
                net_sales_month2: 0,
                sales_mix_month1: 0,
                sales_mix_month2: 0,
                unit_wise_profitability_month1: 0,
                unit_wise_profitability_month2: 0,
                profit_month1: 0,
                profit_month2: 0,
                profit_percentage_month1: 0,
                profit_percentage_month2: 0,
                "Sales Mix (Month2)": 0,
                "Unit Growth": { category: "growth", value: 0 },
                "ASP Growth": { category: "growth", value: 0 },
                "Sales Growth": { category: "growth", value: 0 },
                "Net Sales Growth": { category: "growth", value: 0 },
                "Sales Mix Change": { category: "growth", value: 0 },
                "Profit Per Unit": { category: "growth", value: 0 },
                "CM1 Profit Impact": { category: "growth", value: 0 },
            },
        ],

        new_or_reviving_skus: [
            {
                product_name: "Dummy Product 3",
                sku: "DUMMY-SKU-003",
                quantity_month1: 0,
                quantity_month2: 0,
                asp_month1: 0,
                asp_month2: 0,
                product_sales_month1: 0,
                product_sales_month2: 0,
                net_sales_month1: 0,
                net_sales_month2: 0,
                sales_mix_month1: 0,
                sales_mix_month2: 0,
                unit_wise_profitability_month1: 0,
                unit_wise_profitability_month2: 0,
                profit_month1: 0,
                profit_month2: 0,
                profit_percentage_month1: 0,
                profit_percentage_month2: 0,
                "Sales Mix (Month2)": 0,
            },
        ],

        other_skus: [],

        top_80_total: {
            product_name: "Total",
            "Sales Mix (Month2)": 0,
            "Unit Growth": { category: "growth", value: 0 },
            "ASP Growth": { category: "growth", value: 0 },
            "Sales Growth": { category: "growth", value: 0 },
            "Net Sales Growth": { category: "growth", value: 0 },
            "Sales Mix Change": { category: "growth", value: 0 },
            "Profit Per Unit": { category: "growth", value: 0 },
            "CM1 Profit Impact": { category: "growth", value: 0 },
        },

        new_or_reviving_total: {
            product_name: "Total",
            "Sales Mix (Month2)": 0,
        },

        other_total: {
            product_name: "Total",
            "Sales Mix (Month2)": 0,
            "Unit Growth": { category: "growth", value: 0 },
            "ASP Growth": { category: "growth", value: 0 },
            "Sales Growth": { category: "growth", value: 0 },
            "Net Sales Growth": { category: "growth", value: 0 },
            "Sales Mix Change": { category: "growth", value: 0 },
            "Profit Per Unit": { category: "growth", value: 0 },
            "CM1 Profit Impact": { category: "growth", value: 0 },
        },

        all_skus_total: {
            product_name: "Total",
            "Sales Mix (Month2)": 0,
            "Unit Growth": { category: "growth", value: 0 },
            "ASP Growth": { category: "growth", value: 0 },
            "Sales Growth": { category: "growth", value: 0 },
            "Net Sales Growth": { category: "growth", value: 0 },
            "Sales Mix Change": { category: "growth", value: 0 },
            "Profit Per Unit": { category: "growth", value: 0 },
            "CM1 Profit Impact": { category: "growth", value: 0 },
        },
    },

    ai_insights: {
        "DUMMY-SKU-001": {
            product_name: "Dummy Product 1",
            sku: "DUMMY-SKU-001",
            insight: `Product Journey:
Dummy Product 1 has shown strong and stable growth with better unit movement and improved contribution.

Recommendation:
Scale this SKU further with controlled aggressiveness and maintain inventory readiness.

Advertising:
Increase bids on best-performing search terms and keep branded traffic protected.

Inventory:
Maintain healthy stock cover and monitor weekly velocity.`,
        },

        "DUMMY-SKU-002": {
            product_name: "Dummy Product 2",
            sku: "DUMMY-SKU-002",
            insight: `Product Journey:
Dummy Product 2 continues to grow, though ASP pressure suggests pricing sensitivity in the current period.

Recommendation:
Improve listing efficiency and protect margin instead of chasing aggressive discount-led growth.

Advertising:
Reduce weak campaign spend and reallocate to higher-conversion traffic.

Inventory:
Plan replenishment conservatively with close watch on stock aging.`,
        },

        "DUMMY-SKU-003": {
            product_name: "Dummy Product 3",
            sku: "DUMMY-SKU-003",
            insight: `Product Journey:
Dummy Product 3 is a new or reviving SKU and has started contributing positively to sales and profit.

Recommendation:
Support early momentum but validate consistency before scaling too aggressively.

Advertising:
Test campaigns in a measured way and double down only where conversion quality is stable.

Inventory:
Keep enough stock for validation but avoid over-committing too early.`,
        },
    },
};

export const dummyBiDailySeriesHome: GraphDailySeries = {
    previous: [
        { date: "2026-03-01", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-02", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-03", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-04", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-05", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
    ],
    current_mtd: [
        { date: "2026-03-01", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-02", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-03", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-04", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
        { date: "2026-03-05", net_sales: 0, gross_sales: 0, quantity: 0, profit: 0, cm2_profit: 0 },
    ],
};

export const dummyBiPeriods = {
    previous: {
        label: "Feb MTD",
        start_date: "2026-02-01",
        end_date: "2026-02-05",
    },
    current_mtd: {
        label: "Mar MTD",
        start_date: "2026-03-01",
        end_date: "2026-03-05",
    },
};

export const dummyMonthlySkuwiseRowsForTable: MonthlySkuwiseTableRow[] = [
    {
        sno: 1,
        sku: "SKU-001",
        product_name: "Dummy Product 1",
        quantity: 0,
        asp: 0,
        net_sales: 0,
        cogs: 0,
        fba_fees: 0,
        selling_fees: 0,
        tax: 0,
        credits: 0,
        tax_and_credits: 0,
        cm1_profit_per: 0,
        cm1_profit_per_unit: 0,
        cm2_profit_per: 0,
        cm2_profit_per_unit: 0,
        ads_spend: 0,
        acos: 0,
        cm2_profit: 0,
        profit: 0,
    },
    {
        sno: 2,
        sku: "SKU-002",
        product_name: "Dummy Product 2",
        quantity: 0,
        asp: 0,
        net_sales: 0,
        cogs: 0,
        fba_fees: 0,
        selling_fees: 0,
        tax: 0,
        credits: 0,
        tax_and_credits: 0,
        cm1_profit_per: 0,
        cm1_profit_per_unit: 0,
        cm2_profit_per: 0,
        cm2_profit_per_unit: 0,
        ads_spend: 0,
        acos: 0,
        cm2_profit: 0,
        profit: 0,
    },
    {
        sno: 3,
        sku: "SKU-003",
        product_name: "Dummy Product 3",
        quantity: 0,
        asp: 0,
        net_sales: 0,
        cogs: 0,
        fba_fees: 0,
        selling_fees: 0,
        tax: 0,
        credits: 0,
        tax_and_credits: 0,
        cm1_profit_per: 0,
        cm1_profit_per_unit: 0,
        cm2_profit_per: 0,
        cm2_profit_per_unit: 0,
        ads_spend: 0,
        acos: 0,
        cm2_profit: 0,
        profit: 0,
    },
    {
        sno: 4,
        sku: "SKU-004",
        product_name: "Dummy Product 4",
        quantity: 0,
        asp: 0,
        net_sales: 0,
        cogs: 0,
        fba_fees: 0,
        selling_fees: 0,
        tax: 0,
        credits: 0,
        tax_and_credits: 0,
        cm1_profit_per: 0,
        cm1_profit_per_unit: 0,
        cm2_profit_per: 0,
        cm2_profit_per_unit: 0,
        ads_spend: 0,
        acos: 0,
        cm2_profit: 0,
        profit: 0,
    },
    {
        sno: 5,
        sku: "SKU-005",
        product_name: "Dummy Product 5",
        quantity: 0,
        asp: 0,
        net_sales: 0,
        cogs: 0,
        fba_fees: 0,
        selling_fees: 0,
        tax: 0,
        credits: 0,
        tax_and_credits: 0,
        cm1_profit_per: 0,
        cm1_profit_per_unit: 0,
        cm2_profit_per: 0,
        cm2_profit_per_unit: 0,
        ads_spend: 0,
        acos: 0,
        cm2_profit: 0,
        profit: 0,
    },
    {
        sno: undefined,
        sku: "GRAND_TOTAL",
        product_name: "Total",
        quantity: 0,
        asp: 0,
        net_sales: 0,
        cogs: 0,
        fba_fees: 0,
        selling_fees: 0,
        tax: 0,
        credits: 0,
        tax_and_credits: 0,
        cm1_profit_per: 0,
        cm1_profit_per_unit: 0,
        cm2_profit_per: 0,
        cm2_profit_per_unit: 0,
        ads_spend: 0,
        acos: 0,
        cm2_profit: 0,
        profit: 0,
        isTotal: true,
    },
];

export const dummySalesTargetStats = {
    todayHome: 0,
    mtdHome: 0,
    targetHome: 0,
    lastMonthTotalHome: 0,
    lastMonthToDateHome: 0,
    salesTrendPct: 0,
    targetTrendPct: 0,
    reimbursement: {
        current: 0,
        previous: 0,
    },
    periodCompletedPct: 0,
};

export const dummyTargetData = [
    { label: "Amazon US", target: 0, achieved: 0 },
    { label: "Amazon UK", target: 0, achieved: 0 },
    { label: "Shopify", target: 0, achieved: 0 },
];

export const dummyCm1ProfitPieData: Cm1PieSlice[] = [
    { name: "Product A", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
    { name: "Product B", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
    { name: "Product C", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
    { name: "Product D", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
    { name: "Others", value: 0, prevValue: 0, pct: 0, deltaPct: 0 },
];

export const dummyInventoryRows: InventoryRow[] = [
    {
        "S.No": 1,
        "Product Name": "Dummy Product 1",
        "SKU": "DUMMY-SKU-001",
        "MTD Sales": 0,
        "Sales Last 30 Days": 0,
        "Sales Rank": 0,
        "Current Inventory": 0,
        "Inventory 180+ Days": 0,
        "Estimated Storage Cost ($)": 0,
        "Coverage Ratio (In Months)": 0,
        "Inventory Alerts": "High alert",
    } as InventoryRow,
    {
        "S.No": 2,
        "Product Name": "Dummy Product 2",
        "SKU": "DUMMY-SKU-002",
        "MTD Sales": 0,
        "Sales Last 30 Days": 0,
        "Sales Rank": 0,
        "Current Inventory": 0,
        "Inventory 180+ Days": 0,
        "Estimated Storage Cost ($)": 0,
        "Coverage Ratio (In Months)": 0,
        "Inventory Alerts": "High alert",
    } as InventoryRow,
    {
        "S.No": 3,
        "Product Name": "Dummy Product 3",
        "SKU": "DUMMY-SKU-003",
        "MTD Sales": 0,
        "Sales Last 30 Days": 0,
        "Sales Rank": 0,
        "Current Inventory": 0,
        "Inventory 180+ Days": 0,
        "Estimated Storage Cost ($)": 0,
        "Coverage Ratio (In Months)": 0,
        "Inventory Alerts": "High alert",
    } as InventoryRow,
    {
        "S.No": 4,
        "Product Name": "Dummy Product 4",
        "SKU": "DUMMY-SKU-004",
        "MTD Sales": 0,
        "Sales Last 30 Days": 0,
        "Sales Rank": 0,
        "Current Inventory": 0,
        "Inventory 180+ Days": 0,
        "Estimated Storage Cost ($)": 0,
        "Coverage Ratio (In Months)": 0,
        "Inventory Alerts": "High alert",
    } as InventoryRow,
    {
        "S.No": 5,
        "Product Name": "Dummy Product 5",
        "SKU": "DUMMY-SKU-005",
        "MTD Sales": 0,
        "Sales Last 30 Days": 0,
        "Sales Rank": 0,
        "Current Inventory": 0,
        "Inventory 180+ Days": 0,
        "Estimated Storage Cost ($)": 0,
        "Coverage Ratio (In Months)": 0,
        "Inventory Alerts": "High alert",
    } as InventoryRow,
];

export const dummyInventoryAlerts: InventoryAlertRecord = {
    "DUMMY-SKU-001": { alert: "High alert", alert_type: "warning" },
    "DUMMY-SKU-002": { alert: "High alert", alert_type: "warning" },
    "DUMMY-SKU-003": { alert: "High alert", alert_type: "error" },
    "DUMMY-SKU-004": { alert: "High alert", alert_type: "error" },
    "DUMMY-SKU-005": { alert: "High alert", alert_type: "error" },
};

export const dummyBarLabels = [
    "Net Sales",
    "COGS",
    "Marketplace Fees",
    "Tax & Credits",
    "CM1 Profit",
    "Advertisements",
    "Others",
    "CM2 Profit",
];

export const dummyBarValues = [16200, 6200, 2100, 420, 7480, 2100, 530, 3250];
export const dummyPrevBarValues = [13100, 5400, 1800, 360, 5540, 1760, 470, 2480];
