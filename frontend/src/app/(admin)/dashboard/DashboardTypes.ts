import type {
    AgeingBucket,
    AgeingRiskHeatmapRow,
} from "@/components/common/inventory/AgeingRiskHeatmap";
import type { DonutChartItem } from "@/components/common/inventory/SkuAgeingDonutChart";
import type {
    AgeingTrendItem,
    AgeingTrendAllSeriesItem,
} from "@/components/common/inventory/AgeingTrendChart";
import type {
    ActionCardItem,
    ActionLogicItem,
} from "@/components/common/inventory/ActionBasedDashboard";

export type CurrencyCode = "USD" | "GBP" | "INR" | "CAD";

export type Cm1PieSlice = {
    name: string;
    value: number;
    prevValue: number;
    pct: number;
    deltaPct: number | null;
};

export type MonthlySpRow = {
    sno: number | null;
    products: string | null;
    spend: number | null;
};

export type MonthlyAdsSpentRow = {
    sno?: number | null;
    sku: string;
    ad_spend: number;
    isTotal?: boolean;
    isOthers?: boolean;
};

export type MonthlySkuwiseRow = {
    sno?: number;
    sku: string;
    product_name: string;
    ad_type?: string;
    quantity: number;
    return_quantity?: number;
    total_quantity?: number;
    asp: number;
    net_sales: number;
    gross_sales?: number;
    refund_sales?: number;
    promotional_rebates?: number;
    promotional_rebates_percentage?: number;
    debt_payment?: number;
    disbursement?: number;
    net_taxes?: number;
    other_transactions?: number;
    misc_transaction?: number;
    cogs: number;
    fba_fees: number;
    selling_fees: number;
    ads_spend: number;
    acos: number;
    cm2_profit: number;
    tax: number;
    credits: number;
    tax_and_credits: number;
    cm1_profit_per: number;
    cm1_profit_per_unit: number;
    cm2_profit_per: number;
    cm2_profit_per_unit: number;
    profit: number;
    net_sales_tax_and_credits?: number;
    platform_fee?: number;
    platform_fee_inventory_storage?: number;
    lost_total?: number;
    other?: number;
    product_spend?: number;
    display_spend?: number;
    brand_spend?: number;
    dealsvouchar_ads?: number;
    platformfeenew?: number;
    short_term_storage_fee?: number;
    long_term_storage_fee?: number;
    fba_disposal?: number;

    previous_net_sales?: number;
    net_sales_delta?: number;
    net_sales_delta_percentage?: number | null;

    total_cm2_profit?: number;
    total_cm2_margins?: number;
    tacos_total_advertising_cost_of_sale?: number;
    current_net_reimbursement?: number;
    reimbursement_vs_sales?: number;
    reimbursement_vs_cm2_margins?: number;
    shipment_fees?: number;
    total_ads?: number;
    advertising_fees?: number;
    amazon_fees?: number;
    isTotal?: boolean;
    isOthers?: boolean;
};

export type GrandTotalSkuwiseRow = Partial<MonthlySkuwiseRow> & {
    gross_sales?: number;
    total_ads?: number;
    advertising_total?: number;
    amazon_fees?: number;
    advertising_fees?: number;
    tacos_total_advertising_cost_of_sale?: number;
    total_cm2_profit?: number;
    total_cm2_margins?: number;
    profit_percentage?: number;
    shipment_fees?: number;
    debt_payment?: number;
    disbursement?: number;
    current_net_reimbursement?: number;
    reimbursement_vs_sales?: number;
    reimbursement_vs_cm2_margins?: number;
    marketplace_fees?: number;
};

export type MonthlySkuwiseTableRow = MonthlySkuwiseRow & {
    isOthers?: boolean;
    isTotal?: boolean;
};

export type FetchLiveBiPayloadArgs = {
    startDay?: number | null;
    endDay?: number | null;
    generateInsights?: boolean;
    skipLoader?: boolean;
    dataOnlyRefresh?: boolean;

    // ✅ ADD THIS
    manualAiRefresh?: boolean;
};

export type ProductwiseMoneyKey =
    | "asp"
    | "gross_sales"
    | "refund_sales"
    | "net_sales"
    | "net_taxes"
    | "promotional_rebates"
    | "promotional_rebates_percentage"
    | "other_transactions"
    | "cogs"
    | "fba_fees"
    | "selling_fees"
    | "ads_spend"
    | "cm2_profit"
    | "tax"
    | "credits"
    | "tax_and_credits"
    | "cm1_profit_per_unit"
    | "cm2_profit_per_unit"
    | "profit"
    | "platformfeenew"
    | "debt_payment"
    | "disbursement"
    | "platform_fee"
    | "platform_fee_inventory_storage"
    | "lost_total"
    | "other"
    | "misc_transaction"
    | "product_spend"
    | "display_spend"
    | "brand_spend"
    | "dealsvouchar_ads"
    | "platformfeenew";

export type CountryTimezoneResponse = {
    country: "uk" | "us";
    country_label: string;
    india: {
        timezone: string;
        abbreviation: string;
        datetime: string;
        date: string;
        time: string;
    };
    selected_country: {
        timezone: string;
        abbreviation: string;
        datetime: string;
        date: string;
        time: string;
    };
};

export type ChartMetric = "net_sales" | "quantity";

export type DailyPoint = {
    date: string;
    quantity?: number;
    net_sales?: number;
    gross_sales?: number;
    profit?: number;
    cm2_profit?: number;
};


export type ApiDailySeries = {
    previous?: DailyPoint[];
    current_mtd?: DailyPoint[];

    previous_global?: DailyPoint[];
    current_mtd_global?: DailyPoint[];

    previous_uk?: DailyPoint[];
    current_mtd_uk?: DailyPoint[];

    previous_us?: DailyPoint[];
    current_mtd_us?: DailyPoint[];

    previous_ca?: DailyPoint[];
    current_mtd_ca?: DailyPoint[];
};

export type GraphDailySeries = {
    previous: DailyPoint[];
    current_mtd: DailyPoint[];
};

export type PeriodInfo = {
    label: string;
    start_date: string;
    end_date: string;
};

export type Cm1ProfitPieApiSlice = {
    name: string;
    profit_curr: number;
    profit_prev: number;
    pct: number;
    delta_pct: number;
};

export type Cm1ProfitPieApi = {
    min_named?: number;
    pareto_threshold?: number;
    total_profit_curr?: number;
    slices: Cm1ProfitPieApiSlice[];
};

export type BiApiResponse = {
    message?: string;
    periods?: {
        previous?: PeriodInfo;
        current_mtd?: PeriodInfo;
    };
    daily_series?: ApiDailySeries;

    aligned_totals?: BiAlignedTotals;

    categorized_growth?: any;
    insights?: Record<string, any>;
    ai_insights?: Record<string, any>;
    overall_summary?: string;
    overall_actions?: string[];
    cm1_profit_pie?: Cm1ProfitPieApi;
};

export type BiAlignedTotals = {
    current_cm2_profit?: number;
    previous_cm2_profit?: number;

    total_current_profit_cm2?: number;
    total_previous_profit_cm2?: number;

    total_current_profit_percentage?: number;
    total_previous_profit_percentage?: number;

    total_previous_net_sales_full_month?: number;
    total_previous_net_sales?: number;
    total_current_net_sales?: number;

    total_current_advertising?: number;
    total_previous_advertising?: number;

    total_current_platform_fees?: number;
    total_previous_platform_fees?: number;

    total_current_profit?: number;
    total_previous_profit?: number;

    total_current_rembursement_fee: number;
    total_previous_rembursement_fee: number;
};

export type InventoryAlertRecord = Record<string, { alert?: string; alert_type?: string }>;

export type UiAlert = {
    id: string;
    title: string;
    message: string;
    variant: "success" | "error" | "warning" | "info";
};

export type InventoryCurrentRow = Record<string, any>;

export type InventoryCurrentApiResponse = {
    success: boolean;
    table_name?: string;
    columns?: string[];
    rows?: InventoryCurrentRow[];
    total_rows?: number;
    categories?: Record<
        string,
        {
            items?: any[];
            product_count?: number;
            sku_count?: number;
            name?: string;
            label?: string;
            description?: string;
        }
    >;
    category_counts?: Record<string, number>;

    high_alert_coverage_summary?: {
        average_coverage_ratio?: number;
        high_alert_sku_count?: number;
        high_alert_threshold?: number;
        items?: {
            alert?: string;
            coverage_ratio_months?: number;
            high_alert_threshold?: number;
            product_name?: string;
            sku?: string;
        }[];
    };

    month?: string;
    year?: number;
    country_key?: string;

    combined_countries?: string[];
    country_results?: Record<string, InventoryCurrentApiResponse>;

    inventory_age_summary?: {
        total?: number;
        current_month_units_sold_total?: number;
        percentage_base_total?: number;
        sellable_total?: number;
        unfulfillable_total?: number;
        total_units_summary?: {
            current_month_units_sold?: {
                total?: number;
                percentage_share?: number;
            };
            sellable?: {
                total?: number;
                percentage_share?: number;
            };
            unfulfillable?: {
                total?: number;
                percentage_share?: number;
            };
        };
        columns?: Record<
            string,
            {
                total?: number;
                percentage_share?: number;
            }
        >;
    };
    message?: string;
};

export type InventoryAgeSummaryItem = {
    month: string;
    month_number?: number;
    year: number;
    age_bucket: string;
    column: string;
    units: number;
};

export type InventoryAgeMonthSummaryItem = {
    month: string;
    month_number: number;
    year: number;
    source?: string;
    totals: Record<string, number>;
};

export type InventoryAgeSummaryApiResponse = {
    success: boolean;
    table_name?: string;
    month?: string;
    year?: number;
    country_key?: string;
    totals?: Record<string, number>;
    age_summary?: InventoryAgeSummaryItem[];
    month_summary?: InventoryAgeMonthSummaryItem[];
    message?: string;
    combined_countries?: string[];
    country_results?: Record<string, InventoryAgeSummaryApiResponse>;
};

export type InventoryInsightsData = {
    heatmapBuckets: AgeingBucket[];
    heatmapData: AgeingRiskHeatmapRow[];
    donutSku: string;
    donutData: DonutChartItem[];
    donutTotalUnits: number;
    trendSelectedBucket: string;
    trendData: AgeingTrendItem[];
    trendLineColor: string;

    trendAllSeriesData: AgeingTrendAllSeriesItem[];

    trendBucketOptions: {
        label: string;
        value: string;
        color: string;
    }[];

    actions: ActionCardItem[];
    actionLogic: ActionLogicItem[];
    inventoryAgeSummary?: InventoryCurrentApiResponse["inventory_age_summary"];
};

export type PlSummaryTotals = {
    advertising_total: number;
    visible_ads: number;
    dealsvouchar_ads: number;

    other_transactions: number;
    platform_fee: number;
    inventory_storage_fees: number;
    platform_fee_inventory_storage: number;

    // Inventory Storage Fees breakup
    short_term_storage_fee: number;
    long_term_storage_fee: number;
    fba_disposal: number;

    misc_transaction: number;

    reimbursement_lost_inventory_amount: number;
    reimbursement_lost_inventory_units: number;
    lost_total: number;

    shipment_charges: number;
    reimbursement_vs_sales: number;

    cm2_profit: number;
    cm2_margins: number;
    acos: number;

    rembursment_vs_cm2_margins: number;
    net_reimbursement: number;
    debt_payment: number;
    disbursement: number;

    profit: number;
    net_sales: number;
};
