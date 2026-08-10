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
import type {
    DailyPoint,
    InventoryCurrentApiResponse,
    InventoryCurrentRow,
    InventoryAgeSummaryApiResponse,
    InventoryInsightsData,
} from "./DashboardTypes";

const getFirstPositiveInventoryValue = (...values: any[]) => {
    for (const value of values) {
        const n = inventoryToNum(value);
        if (n > 0) return n;
    }

    return 0;
};

export const sliceByDayRange = (
    points: DailyPoint[] = [],
    startDay: number | null,
    endDay: number | null
) => {
    if (startDay == null || endDay == null) return points;

    const s = Math.min(startDay, endDay);
    const e = Math.max(startDay, endDay);

    return points.filter((p) => {
        const day = Number(p.date?.slice(8, 10));
        return day >= s && day <= e;
    });
};


export const inventoryMonthIndexMap: Record<string, number> = {
    january: 0,
    february: 1,
    march: 2,
    april: 3,
    may: 4,
    june: 5,
    july: 6,
    august: 7,
    september: 8,
    october: 9,
    november: 10,
    december: 11,
};

const INVENTORY_BUCKETS: AgeingBucket[] = [
    { key: "zeroToOneEighty", label: "0–180 Days", color: "#7B9A6D" },
    { key: "oneEightyOneToTwoSeventy", label: "181–270 Days", color: "#ED9F50" },
    { key: "twoSeventyOneToThreeSixtyFive", label: "271–365 Days", color: "#C49466" },
    { key: "threeSixtyFivePlus", label: "365+ Days", color: "#B75A5A" },
];

const SPLIT_FIRST_180_INVENTORY_BUCKETS: AgeingBucket[] = [
    { key: "zeroToNinety", label: "0–90 Days", color: "#7B9A6D" },
    { key: "ninetyOneToOneEighty", label: "91–180 Days", color: "#FDD36F" },
    { key: "oneEightyOneToTwoSeventy", label: "181–270 Days", color: "#ED9F50" },
    { key: "twoSeventyOneToThreeSixtyFive", label: "271–365 Days", color: "#C49466" },
    { key: "threeSixtyFivePlus", label: "365+ Days", color: "#B75A5A" },
];

const COMBINED_FIRST_180_INVENTORY_BUCKETS: AgeingBucket[] = [
    { key: "zeroToOneEighty", label: "0–180 Days", color: "#7B9A6D" },
    { key: "oneEightyOneToTwoSeventy", label: "181–270 Days", color: "#ED9F50" },
    { key: "twoSeventyOneToThreeSixtyFive", label: "271–365 Days", color: "#C49466" },
    { key: "threeSixtyFivePlus", label: "365+ Days", color: "#B75A5A" },
];

const getDynamicInventoryBuckets = (
    rows: InventoryCurrentRow[]
): AgeingBucket[] => {
    const splitFirst180Total = rows.reduce((sum, row) => {
        return (
            sum +
            getInventoryAgeValue(row, "inv-age-0-to-90-days") +
            getInventoryAgeValue(row, "inv-age-91-to-180-days")
        );
    }, 0);

    const combinedFirst180Total = rows.reduce((sum, row) => {
        return sum + getInventoryAgeValue(row, "inv-age-0-to-180-days");
    }, 0);

    if (splitFirst180Total > 0) {
        return SPLIT_FIRST_180_INVENTORY_BUCKETS;
    }

    if (combinedFirst180Total > 0) {
        return COMBINED_FIRST_180_INVENTORY_BUCKETS;
    }

    return COMBINED_FIRST_180_INVENTORY_BUCKETS;
};

export const AGEING_TREND_BUCKET_OPTIONS = [
    {
        label: "0–180 Days",
        value: "0-180 days",
        column: "inv-age-0-to-180-days",
        color: "#7B9A6D",
    },
    {
        label: "181–270 Days",
        value: "181-270 days",
        column: "inv-age-181-to-270-days",
        color: "#ED9F50",
    },
    {
        label: "271–365 Days",
        value: "271-365 days",
        column: "inv-age-271-to-365-days",
        color: "#C49466",
    },
    {
        label: "365+ Days",
        value: "365+ days",
        column: "inv-age-365-plus-days",
        color: "#B75A5A",
    },
    {
        label: "Unsellable",
        value: "unsellable",
        column: "unfulfillable-quantity",
        color: "#3A8EA4",
    },
];

const INVENTORY_ACTION_LOGIC: ActionLogicItem[] = [
    {
        key: "healthy",
        label: "Healthy",
        description: "Stock covers 0–180 days",
        color: "#7B9A6D",
    },
    {
        key: "high_alert",
        label: "High Alert",
        description: "Shipment Required",
        color: "#B75A5A",
    },
    // {
    //     key: "discount",
    //     label: "Discount",
    //     description: "Stock aged 91–180 days",
    //     color: "#FDD36F",
    // },
    {
        key: "liquidate",
        label: "Liquidate",
        description: "Stock older than 180 days",
        color: "#ED9F50",
    },
    {
        key: "unfulfillable",
        label: "Unfulfillable",
        description: "Remove or dispose stock",
        color: "#3A8EA4",
    },
    {
        key: "estimated_storage_cost",
        label: "Estimate Storage",
        description: "Monthly storage estimate",
        color: "#C49466",
    },
];

const INVENTORY_ACTION_META: Record<
    string,
    {
        label: string;
        description: string;
        color: string;
        backgroundColor: string;
    }
> = {
    healthy: {
        label: "Healthy",
        description: "Stock covers 0–180 days",
        color: "#7B9A6D",
        backgroundColor: "#ffffff",
    },
    high_alert: {
        label: "High Alert",
        description: "Shipment Required",
        color: "#B75A5A",
        backgroundColor: "#ffffff",
    },
    // discount: {
    //     label: "Discount",
    //     description: "Stock aged 91–180 days",
    //     color: "#FDD36F",
    //     backgroundColor: "#ffffff",
    // },
    liquidate: {
        label: "Liquidate",
        description: "Stock older than 180 days",
        color: "#ED9F50",
        backgroundColor: "#ffffff",
    },
    unfulfillable: {
        label: "Unfulfillable",
        description: "Remove or dispose stock",
        color: "#3A8EA4",
        backgroundColor: "#ffffff",
    },
    estimated_storage_cost: {
        label: "Estimate Storage",
        description: "Monthly storage estimate",
        color: "#C49466",
        backgroundColor: "#ffffff",
    },
};

const inventoryToNum = (v: any) => {
    if (v === null || v === undefined) return 0;
    if (typeof v === "number") return Number.isFinite(v) ? v : 0;

    const n = Number(String(v).replace(/,/g, "").trim());
    return Number.isFinite(n) ? n : 0;
};

const getInventoryRowSku = (row: InventoryCurrentRow) =>
    String(row?.SKU ?? row?.sku ?? "").trim();

const getInventoryRowProductName = (row: InventoryCurrentRow) =>
    String(row?.["Product Name"] ?? row?.product_name ?? "").trim();

const isInventoryInsightsTotalRow = (row: InventoryCurrentRow) => {
    const sku = getInventoryRowSku(row).toLowerCase();
    const product = getInventoryRowProductName(row).toLowerCase();

    return sku === "total" || product === "total" || sku === "grand total" || product === "grand total";
};

const isInventoryInsightsPercentageRow = (row: InventoryCurrentRow) => {
    const product = getInventoryRowProductName(row).toLowerCase();
    const sku = getInventoryRowSku(row).toLowerCase();
    const rowType = String(row?.row_type || "").trim().toLowerCase();

    return (
        row?.is_percentage_row === true ||
        rowType === "percentage" ||
        product === "percentage" ||
        product === "% of total" ||
        sku === "percentage" ||
        sku === "% of total"
    );
};

const getInventoryAgeValue = (row: InventoryCurrentRow, key: string) =>
    inventoryToNum(row?.[key]);

const hasAnyAgeingBucketValue = (row: InventoryCurrentRow) => {
    return (
        getInventoryAgeValue(row, "inv-age-0-to-90-days") > 0 ||
        getInventoryAgeValue(row, "inv-age-91-to-180-days") > 0 ||
        getInventoryAgeValue(row, "inv-age-0-to-180-days") > 0 ||
        getInventoryAgeValue(row, "inv-age-181-to-270-days") > 0 ||
        getInventoryAgeValue(row, "inv-age-271-to-365-days") > 0 ||
        getInventoryAgeValue(row, "inv-age-365-plus-days") > 0
    );
};

const normalizeInventoryKey = (key: string) =>
    String(key || "")
        .toLowerCase()
        .trim()
        .replace(/[%()]/g, "")
        .replace(/[_\-\s]+/g, "_")
        .replace(/__+/g, "_");

const firstInventoryNumberValue = (
    row: InventoryCurrentRow,
    keys: string[]
) => {
    if (!row) return 0;

    for (const key of keys) {
        if (!key) continue;

        const value = row?.[key];

        if (value !== null && value !== undefined && value !== "") {
            return inventoryToNum(value);
        }
    }

    const normalizedTargets = keys.map(normalizeInventoryKey);

    for (const [rowKey, rowValue] of Object.entries(row)) {
        if (normalizedTargets.includes(normalizeInventoryKey(rowKey))) {
            return inventoryToNum(rowValue);
        }
    }

    return 0;
};

const getCurrentMonthUnitsSold = (row: InventoryCurrentRow) => {
    const directKey = Object.keys(row || {}).find((key) =>
        normalizeInventoryKey(key).startsWith("current_month_units_sold")
    );

    return firstInventoryNumberValue(row, [
        directKey || "",
        "Current Month Units Sold",
        "current_month_units_sold",
        "currentMonthUnitsSold",
    ]);
};

const getSalesLast30DaysValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "Sales Last 30 Days",
        "sales_last_30_days",
        "sales-last-30-days",
        "salesLast30Days",
        "last_30_days_sales",
        "last-30-days-sales",
        "Last 30 Days Sales",
    ]);

const getInventoryCurrentFbaValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "Sellable Units",
        "available",
        "Current Inventory FBA",
        "current_inventory_fba",
        "current-fba",
        "fulfillable_quantity",
        "available_quantity",
    ]);

const getInventoryCurrentAwdValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "total_onhand_quantity",
        "Current Inventory AWD",
        "current_inventory_awd",
        "current-awd",
        "available_awd",
        "awd_available",
    ]);

const getInventoryTransitFbaValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "inbound-shipped\r",
        "inbound-shipped",
        "In Transit FBA",
        "in_transit_fba",
        "in-transit-fba",
        "fc-transfer",
        "fc_transfer",
        "reserved_fc_transfer",
        "inbound-working",
    ]);

const getInventoryTransitAwdValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "total_inbound_quantity",
        "In Transit AWD",
        "in_transit_awd",
        "in-transit-awd",
        "inbound_quantity",
        "inbound-quantity",
        "Inbound Units",
        "inbound_units",
        "inbound-shipped",
    ]);

const getInventoryUnsellableFbaValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "Unsellable Inventory FBA",
        "Unsellable FBA",
        "unfulfillable-quantity",
        "unfulfillable_quantity",
        "Unfulfillable Units",
    ]);

const getInventoryUnsellableAwdValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "Unsellable Inventory AWD",
        "Unsellable AWD",
        "unsellable_awd",
        "unfulfillable_awd",
    ]);

const getInventoryStorageCostValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "Storage Cost (Est) - in USD",
        "Storage Cost (Est) in USD",
        "estimated-storage-cost-next-month",
        "estimated_storage_cost_next_month",
        "estimatedStorageCostNextMonth",
        "Estimated Storage Cost",
        "storage_cost_est",
        "storage_cost",
    ]);

const getInventoryCoverageCurrentAndTransitValue = (row: InventoryCurrentRow) =>
    firstInventoryNumberValue(row, [
        "Coverage Ratio (Current + In Transit)",
        "Coverage Ratio (Current + In transit)",
        "Coverage Ratio (Current + Intransit)",
        "Coverage Ratio (Current + Inventory)",
        "coverage_ratio_current_in_transit",
        "coverage_ratio_current_intransit",
        "coverage_ratio_current_plus_in_transit",
    ]);

const mergeInventoryRowsBySku = (
    rows: InventoryCurrentRow[]
): InventoryCurrentRow[] => {
    const unique = new Map<string, InventoryCurrentRow>();

    rows.forEach((row) => {
        const sku = getInventoryRowSku(row);
        const productName = getInventoryRowProductName(row);
        const key = `${sku || productName}`.trim().toLowerCase();

        if (
            !key ||
            key === "total" ||
            key === "grand total" ||
            key === "percentage" ||
            key === "% of total" ||
            isInventoryInsightsTotalRow(row) ||
            isInventoryInsightsPercentageRow(row)
        ) {
            return;
        }

        const next: InventoryCurrentRow = { ...(unique.get(key) || {}) };

        Object.entries(row).forEach(([fieldKey, fieldValue]) => {
            const isEmpty =
                fieldValue === null ||
                fieldValue === undefined ||
                fieldValue === "" ||
                String(fieldValue).trim().toLowerCase() === "nan" ||
                String(fieldValue).trim().toLowerCase() === "none" ||
                String(fieldValue).trim().toLowerCase() === "null";

            if (!isEmpty) {
                next[fieldKey] = fieldValue;
            }
        });

        unique.set(key, next);
    });

    return Array.from(unique.values());
};

const getShortMonthLabel = (monthName?: string) => {
    const clean = String(monthName || "").trim();
    return clean ? clean.slice(0, 3) : "-";
};

const getMonthYearFromInventoryTableName = (tableName?: string) => {
    const match = String(tableName || "").match(/_([a-z]+)(\d{4})_table$/i);

    if (!match) {
        return {
            month: "",
            year: 0,
            month_number: 0,
        };
    }

    const month = match[1].toLowerCase();
    const year = Number(match[2]);
    const month_number = (inventoryMonthIndexMap[month] ?? -1) + 1;

    return {
        month,
        year,
        month_number,
    };
};

const getInventoryCurrencySymbol = (countryName: string, homeCurrency?: string) => {
    const v =
        String(countryName || "").toLowerCase() === "global"
            ? String(homeCurrency || "usd").toLowerCase()
            : String(countryName || "").toLowerCase();

    switch (v) {
        case "usd":
        case "us":
        case "global":
            return "$";
        case "gbp":
        case "uk":
            return "£";
        case "cad":
        case "ca":
        case "canada":
            return "C$";
        case "inr":
        case "india":
            return "₹";
        default:
            return "¤";
    }
};

const formatInventoryStorageCost = (
    value: number,
    countryName: string,
    homeCurrency?: string
) => {
    const symbol = getInventoryCurrencySymbol(countryName, homeCurrency);

    return `${symbol}${value.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    })}`;
};

const getEstimatedStorageCostTotal = (
    latestResponse?: InventoryCurrentApiResponse
) => {
    const storageItems =
        latestResponse?.categories?.estimated_storage_cost?.items ?? [];

    const totalRow = storageItems.find((item: any) => {
        const productName = String(item?.product_name || "")
            .trim()
            .toLowerCase();

        const sku = String(item?.sku || "")
            .trim()
            .toLowerCase();

        return productName === "total" || sku === "total" || sku === "";
    });

    if (totalRow) {
        return inventoryToNum(totalRow?.["estimated-storage-cost-next-month"]);
    }

    return storageItems.reduce(
        (sum: number, item: any) =>
            sum + inventoryToNum(item?.["estimated-storage-cost-next-month"]),
        0
    );
};

export const buildAgeingTrendDataFromSummary = (
    ageSummaryResponses: InventoryAgeSummaryApiResponse[],
    bucketColumn: string
): AgeingTrendItem[] => {
    const monthMap = new Map<
        string,
        {
            month: string;
            month_number: number;
            year: number;
            value: number;
        }
    >();
    let foundColumnData = false;

    for (const res of ageSummaryResponses || []) {
        if (!res?.success) continue;

        if (Array.isArray(res.month_summary) && res.month_summary.length > 0) {
            let foundMonthSummaryColumn = false;

            for (const item of res.month_summary) {
                if (
                    !item.totals ||
                    !Object.prototype.hasOwnProperty.call(item.totals, bucketColumn)
                ) {
                    continue;
                }

                const key = `${item.year}-${item.month_number}`;
                foundColumnData = true;
                foundMonthSummaryColumn = true;

                monthMap.set(key, {
                    month: item.month,
                    month_number: item.month_number,
                    year: item.year,
                    value: inventoryToNum(item.totals?.[bucketColumn]),
                });
            }

            if (foundMonthSummaryColumn) continue;
        }

        if (Array.isArray(res.age_summary) && res.age_summary.length > 0) {
            for (const item of res.age_summary) {
                if (item.column !== bucketColumn) continue;

                const monthNumber =
                    item.month_number ??
                    inventoryMonthIndexMap[item.month.toLowerCase()] + 1;

                const key = `${item.year}-${monthNumber}`;
                foundColumnData = true;

                monthMap.set(key, {
                    month: item.month,
                    month_number: monthNumber,
                    year: item.year,
                    value: inventoryToNum(item.units),
                });
            }

            continue;
        }

        if (res.month && res.year && res.totals) {
            if (!Object.prototype.hasOwnProperty.call(res.totals, bucketColumn)) {
                continue;
            }

            const monthNumber = inventoryMonthIndexMap[res.month.toLowerCase()] + 1;
            const key = `${res.year}-${monthNumber}`;
            foundColumnData = true;

            monthMap.set(key, {
                month: res.month,
                month_number: monthNumber,
                year: res.year,
                value: inventoryToNum(res.totals?.[bucketColumn]),
            });
        }
    }

    if (!foundColumnData) return [];

    return Array.from(monthMap.values())
        .sort((a, b) => {
            if (a.year !== b.year) return a.year - b.year;
            return a.month_number - b.month_number;
        })
        .map((item) => ({
            label: getShortMonthLabel(item.month),
            value: item.value,
        }));
};

export const buildAgeingTrendDataFromInventoryCurrent = (
    inventoryResponses: InventoryCurrentApiResponse[],
    bucketColumn: string
): AgeingTrendItem[] => {
    const getTrendValue = (res: InventoryCurrentApiResponse) => {
        if (bucketColumn === "unfulfillable-quantity") {
            const summaryValue =
                res.inventory_age_summary?.unfulfillable_total ??
                res.inventory_age_summary?.total_units_summary?.unfulfillable?.total;

            if (summaryValue !== null && summaryValue !== undefined) {
                return inventoryToNum(summaryValue);
            }

            const categoryRows = res?.categories
                ? Object.values(res.categories).flatMap((category) =>
                    Array.isArray(category?.items) ? category.items : []
                )
                : [];
            const rows = Array.isArray(res.rows) && res.rows.length > 0
                ? res.rows
                : categoryRows;

            return rows
                .filter(
                    (row) =>
                        !isInventoryInsightsTotalRow(row) &&
                        !isInventoryInsightsPercentageRow(row)
                )
                .reduce(
                    (sum, row) =>
                        sum +
                        getInventoryUnsellableFbaValue(row) +
                        getInventoryUnsellableAwdValue(row),
                    0
                );
        }

        return inventoryToNum(
            res.inventory_age_summary?.columns?.[bucketColumn]?.total
        );
    };

    return (inventoryResponses || [])
        .filter((res) => res?.success)
        .map((res) => {
            const parsed = getMonthYearFromInventoryTableName(res.table_name);

            const month = String(res.month || "").toLowerCase() || parsed.month;
            const year = Number(res.year || 0) || parsed.year;
            const month_number =
                (inventoryMonthIndexMap[month] ?? -1) + 1 || parsed.month_number;

            return {
                month,
                year,
                month_number,
                value: getTrendValue(res),
            };
        })
        .filter((item) => item.month && item.year && item.month_number)
        .sort((a, b) => {
            if (a.year !== b.year) return a.year - b.year;
            return a.month_number - b.month_number;
        })
        .map((item) => ({
            label: getShortMonthLabel(item.month),
            value: item.value,
        }));
};

const isGlobalInventoryResponse = (res?: InventoryCurrentApiResponse) => {
    return (
        String(res?.country_key || "").toLowerCase() === "global" &&
        !!res?.country_results &&
        typeof res.country_results === "object"
    );
};

export const getSelectedCountryInventoryResponse = (
    res: InventoryCurrentApiResponse | undefined,
    selectedCountry: string
): InventoryCurrentApiResponse | undefined => {
    if (!res) return undefined;

    if (!isGlobalInventoryResponse(res)) return res;

    const selectedKey = String(selectedCountry || "uk").toLowerCase();

    const countryRes =
        res.country_results?.[selectedKey] ||
        res.country_results?.uk ||
        Object.values(res.country_results || {})[0];

    if (!countryRes) return undefined;

    return {
        ...countryRes,
        country_key: countryRes.country_key || selectedKey,
    };
};

export const getSelectedCountryAgeSummaryResponses = (
    responses: InventoryAgeSummaryApiResponse[],
    selectedCountry: string
): InventoryAgeSummaryApiResponse[] => {
    const selectedKey = String(selectedCountry || "uk").toLowerCase();

    return (responses || []).map((res: any) => {
        if (
            String(res?.country_key || "").toLowerCase() === "global" &&
            res?.country_results &&
            typeof res.country_results === "object"
        ) {
            const countryRes =
                res.country_results?.[selectedKey] ||
                res.country_results?.uk ||
                Object.values(res.country_results || {})[0];

            return {
                ...(countryRes || {}),
                country_key: selectedKey,
            };
        }

        return res;
    });
};

const buildDonutDataFromInventoryAgeSummary = (
    inventoryAgeSummary?: InventoryCurrentApiResponse["inventory_age_summary"]
): DonutChartItem[] => {
    if (!inventoryAgeSummary) return [];

    const columns = inventoryAgeSummary?.columns || {};
    const unfulfillableUnits = inventoryToNum(
        inventoryAgeSummary?.unfulfillable_total ??
        inventoryAgeSummary?.total_units_summary?.unfulfillable?.total
    );
    const sellableUnits = inventoryToNum(
        inventoryAgeSummary?.sellable_total ??
        inventoryAgeSummary?.total_units_summary?.sellable?.total
    );
    const totalUnits = getFirstPositiveInventoryValue(
        inventoryAgeSummary?.percentage_base_total,
        inventoryAgeSummary?.total,
        sellableUnits + unfulfillableUnits
    );
    const hasSplitFirst180 =
        inventoryToNum(columns["inv-age-0-to-90-days"]?.total) > 0 ||
        inventoryToNum(columns["inv-age-91-to-180-days"]?.total) > 0;

    const first180Buckets = hasSplitFirst180
        ? [
            {
                bucket: "0–90 Days",
                column: "inv-age-0-to-90-days",
                color: "#7B9A6D",
            },
            {
                bucket: "91–180 Days",
                column: "inv-age-91-to-180-days",
                color: "#FDD36F",
            },
        ]
        : [
            {
                bucket: "0–180 Days",
                column: "inv-age-0-to-180-days",
                color: "#7B9A6D",
            },
        ];

    const summaryBuckets = [
        ...first180Buckets,
        {
            bucket: "181–270 Days",
            column: "inv-age-181-to-270-days",
            color: "#ED9F50",
        },
        {
            bucket: "271–365 Days",
            column: "inv-age-271-to-365-days",
            color: "#C49466",
        },
        {
            bucket: "365+ Days",
            column: "inv-age-365-plus-days",
            color: "#B75A5A",
        },
    ];

    const computedTotalUnits =
        summaryBuckets.reduce(
            (sum, bucket) => sum + inventoryToNum(columns[bucket.column]?.total),
            0
        ) + unfulfillableUnits;
    const finalTotalUnits = getFirstPositiveInventoryValue(
        computedTotalUnits,
        totalUnits
    );

    const ageingItems = summaryBuckets.map((bucket) => {
        const item = columns[bucket.column];
        const units = inventoryToNum(item?.total);

        return {
            bucket: bucket.bucket,
            units,
            percentageShare:
                finalTotalUnits > 0
                    ? (units / finalTotalUnits) * 100
                    : undefined,
            color: bucket.color,
        };
    });

    return [
        ...ageingItems,
        {
            bucket: "Unsellable",
            units: unfulfillableUnits,
            percentageShare:
                finalTotalUnits > 0
                    ? (unfulfillableUnits / finalTotalUnits) * 100
                    : undefined,
            color: "#3A8EA4",
        },
    ];
};

const buildBackendPercentageHeatmapRow = (
    row: InventoryCurrentRow | undefined,
    isUsingSplitFirst180: boolean
): AgeingRiskHeatmapRow | null => {
    if (!row) return null;

    return {
        productName: "% of Total",
        sku: "-",

        zeroToNinety: inventoryToNum(row?.["inv-age-0-to-90-days"]),
        ninetyOneToOneEighty: inventoryToNum(row?.["inv-age-91-to-180-days"]),
        zeroToOneEighty: inventoryToNum(row?.["inv-age-0-to-180-days"]),

        oneEightyOneToTwoSeventy: inventoryToNum(row?.["inv-age-181-to-270-days"]),
        twoSeventyOneToThreeSixtyFive: inventoryToNum(row?.["inv-age-271-to-365-days"]),
        threeSixtyFivePlus: inventoryToNum(row?.["inv-age-365-plus-days"]),

        available: inventoryToNum(row?.["Sellable Units"]),
        totalUnits: inventoryToNum(row?.["Sellable Units"]),

        unsellableUnits: inventoryToNum(row?.["unfulfillable-quantity"]),

        inboundUnits: undefined,
        unitsSold: undefined,
        coverageRatio: undefined,
        inventoryAlert: "",

        isPercentageRow: true,
    };
};

export const buildInventoryInsightsFromResponses = (
    responses: InventoryCurrentApiResponse[],
    ageSummaryResponses: InventoryAgeSummaryApiResponse[] = [],
    countryName: string,
    homeCurrency?: string,
    selectedTrendBucketValue: string = "365+ days",
    selectedGlobalInventoryCountry: string = "uk"
): InventoryInsightsData => {
    const validResponses = responses.filter((res) => res?.success);
    const latestRawResponse = validResponses[validResponses.length - 1];

    const isGlobalInventory = isGlobalInventoryResponse(latestRawResponse);

    const latestResponse = getSelectedCountryInventoryResponse(
        latestRawResponse,
        selectedGlobalInventoryCountry
    );

    const rawRows = latestResponse?.rows ?? [];
    const categoryRows = latestResponse?.categories
        ? Object.values(latestResponse.categories).flatMap((category) =>
            Array.isArray(category?.items) ? category.items : []
        )
        : [];
    const mergedRows = mergeInventoryRowsBySku([...rawRows, ...categoryRows]);

    const backendPercentageRawRow = rawRows.find((row) =>
        isInventoryInsightsPercentageRow(row)
    );

    const backendTotalRawRow = rawRows.find((row) =>
        isInventoryInsightsTotalRow(row)
    );

    const latestRows = mergedRows.filter(
        (row) =>
            !isInventoryInsightsTotalRow(row) &&
            !isInventoryInsightsPercentageRow(row) &&
            hasAnyAgeingBucketValue(row)
    );

    const dynamicHeatmapBuckets = getDynamicInventoryBuckets(latestRows);

    const isUsingSplitFirst180 = dynamicHeatmapBuckets.some(
        (bucket) => bucket.key === "zeroToNinety"
    );

    const selectedAgeSummaryResponses = isGlobalInventory
        ? getSelectedCountryAgeSummaryResponses(
            ageSummaryResponses,
            selectedGlobalInventoryCountry
        )
        : ageSummaryResponses;

    const selectedInventoryResponses = isGlobalInventory
        ? validResponses
            .map((res) =>
                getSelectedCountryInventoryResponse(
                    res,
                    selectedGlobalInventoryCountry
                )
            )
            .filter(Boolean) as InventoryCurrentApiResponse[]
        : validResponses;

    const heatmapData: AgeingRiskHeatmapRow[] = latestRows.map((row) => {
        const sku = getInventoryRowSku(row);
        const productName = getInventoryRowProductName(row);

        const zeroToNinety = getInventoryAgeValue(row, "inv-age-0-to-90-days");

        const ninetyOneToOneEighty = getInventoryAgeValue(
            row,
            "inv-age-91-to-180-days"
        );

        const zeroToOneEighty = getInventoryAgeValue(
            row,
            "inv-age-0-to-180-days"
        );

        const oneEightyOneToTwoSeventy = getInventoryAgeValue(
            row,
            "inv-age-181-to-270-days"
        );

        const twoSeventyOneToThreeSixtyFive = getInventoryAgeValue(
            row,
            "inv-age-271-to-365-days"
        );

        const threeSixtyFivePlus = getInventoryAgeValue(
            row,
            "inv-age-365-plus-days"
        );

        const first180Total = isUsingSplitFirst180
            ? zeroToNinety + ninetyOneToOneEighty
            : zeroToOneEighty;

        const bucketTotal =
            first180Total +
            oneEightyOneToTwoSeventy +
            twoSeventyOneToThreeSixtyFive +
            threeSixtyFivePlus;

        const available = inventoryToNum(
            row?.available ??
            row?.available_quantity ??
            row?.fulfillable_quantity
        );

        const fcTransfer = inventoryToNum(
            row?.["fc-transfer"] ??
            row?.fc_transfer ??
            row?.reserved_fc_transfer
        );

        const sellableUnits =
            inventoryToNum(
                row?.["Sellable Units"] ??
                row?.sellable_units ??
                row?.sellableUnits
            ) || available + fcTransfer;

        const inboundUnits = getFirstPositiveInventoryValue(
            row?.inbound_quantity,
            row?.["inbound_quantity"],
            row?.["inbound-quantity"],
            row?.inboundQuantity,
            row?.["Inbound Quantity"],
            row?.["Inbound Units"],
            row?.inbound_units,
            row?.inboundUnits,
            row?.transit_total,
            row?.["inbound-shipped"],
            row?.["inbound-working"],
            row?.["inbound-received"]
        );

        const unsellableUnits = inventoryToNum(
            row?.["unfulfillable-quantity"] ??
            row?.unfulfillable_quantity
        );

        const salesLast30Days = getSalesLast30DaysValue(row);
        const unitsSold = getCurrentMonthUnitsSold(row) || salesLast30Days;
        const dashboardSalesValue = salesLast30Days || unitsSold;
        const currentFba = getInventoryCurrentFbaValue(row);
        const currentAwd = getInventoryCurrentAwdValue(row);
        const transitFba = getInventoryTransitFbaValue(row);
        const transitAwd = getInventoryTransitAwdValue(row);
        const totalInStock =
            firstInventoryNumberValue(row, [
                "total_stock",
                "Total Sellable Inventory In Stock",
                "Total Sellable In Stock",
            ]) ||
            currentFba + currentAwd;
        const totalInTransit =
            firstInventoryNumberValue(row, [
                "total_transit",
                "Total Sellable Inventory In Transit",
                "Total Sellable In Transit",
            ]) ||
            transitFba + transitAwd;
        const coverageCurrentAndTransit =
            getInventoryCoverageCurrentAndTransitValue(row) ||
            (dashboardSalesValue > 0
                ? (totalInStock + totalInTransit) / dashboardSalesValue
                : 0);

        const previousSalesRankKey = Object.keys(row || {}).find((key) =>
            String(key).toLowerCase().startsWith("previous month sales rank")
        );

        const previousSalesRank = previousSalesRankKey
            ? row?.[previousSalesRankKey]
            : row?.previous_sales_rank ??
            row?.previousSalesRank ??
            row?.["Previous Month Sales Rank"] ??
            "";

        return {
            productName: productName || sku || "-",
            sku,

            zeroToNinety,
            ninetyOneToOneEighty,
            zeroToOneEighty,

            oneEightyOneToTwoSeventy,
            twoSeventyOneToThreeSixtyFive,
            threeSixtyFivePlus,

            available,
            fcTransfer,
            totalUnits: sellableUnits,
            inboundUnits,
            unsellableUnits,
            currentFba,
            currentAwd,
            transitFba,
            transitAwd,
            totalInStock,
            totalInTransit,
            unsellableFba: getInventoryUnsellableFbaValue(row),
            unsellableAwd: getInventoryUnsellableAwdValue(row),
            storageCostUsd: getInventoryStorageCostValue(row),
            coverageCurrentAndTransit,

            unitsSold,

            salesRank:
                row?.["sales-rank"] ??
                row?.sales_rank ??
                row?.salesRank ??
                row?.["Sales Rank"] ??
                row?.["sales rank"] ??
                "",

            previousSalesRank,

            salesLast30Days,
            coverageRatio: inventoryToNum(row?.["Coverage Ratio (In Months)"]),
            inventoryAlert: String(row?.["Inventory Alerts"] || "").trim(),
        };
    });

    const backendTotalHeatmapRow: AgeingRiskHeatmapRow | null = backendTotalRawRow
        ? {
            productName: "Total",
            sku: "-",

            zeroToNinety: inventoryToNum(backendTotalRawRow?.["inv-age-0-to-90-days"]),
            ninetyOneToOneEighty: inventoryToNum(backendTotalRawRow?.["inv-age-91-to-180-days"]),
            zeroToOneEighty: inventoryToNum(backendTotalRawRow?.["inv-age-0-to-180-days"]),

            oneEightyOneToTwoSeventy: inventoryToNum(backendTotalRawRow?.["inv-age-181-to-270-days"]),
            twoSeventyOneToThreeSixtyFive: inventoryToNum(backendTotalRawRow?.["inv-age-271-to-365-days"]),
            threeSixtyFivePlus: inventoryToNum(backendTotalRawRow?.["inv-age-365-plus-days"]),

            available: inventoryToNum(backendTotalRawRow?.available),
            fcTransfer: inventoryToNum(backendTotalRawRow?.["fc-transfer"]),
            totalUnits: inventoryToNum(backendTotalRawRow?.["Sellable Units"]),
            inboundUnits: inventoryToNum(backendTotalRawRow?.["Inbound Units"]),
            unsellableUnits: inventoryToNum(backendTotalRawRow?.["unfulfillable-quantity"]),
            currentFba: getInventoryCurrentFbaValue(backendTotalRawRow),
            currentAwd: getInventoryCurrentAwdValue(backendTotalRawRow),
            transitFba: getInventoryTransitFbaValue(backendTotalRawRow),
            transitAwd: getInventoryTransitAwdValue(backendTotalRawRow),
            totalInStock:
                firstInventoryNumberValue(backendTotalRawRow, [
                    "total_stock",
                    "Total Sellable Inventory In Stock",
                    "Total Sellable In Stock",
                ]) ||
                getInventoryCurrentFbaValue(backendTotalRawRow) +
                getInventoryCurrentAwdValue(backendTotalRawRow),
            totalInTransit:
                firstInventoryNumberValue(backendTotalRawRow, [
                    "total_transit",
                    "Total Sellable Inventory In Transit",
                    "Total Sellable In Transit",
                ]) ||
                getInventoryTransitFbaValue(backendTotalRawRow) +
                getInventoryTransitAwdValue(backendTotalRawRow),
            unsellableFba: getInventoryUnsellableFbaValue(backendTotalRawRow),
            unsellableAwd: getInventoryUnsellableAwdValue(backendTotalRawRow),
            storageCostUsd:
                getInventoryStorageCostValue(backendTotalRawRow) ||
                getEstimatedStorageCostTotal(latestResponse),

            unitsSold:
                getCurrentMonthUnitsSold(backendTotalRawRow) ||
                getSalesLast30DaysValue(backendTotalRawRow),

            // ✅ Add this
            salesLast30Days: getSalesLast30DaysValue(backendTotalRawRow),

            // ✅ backend value only
            coverageRatio: inventoryToNum(
                backendTotalRawRow?.["Coverage Ratio (In Months)"] ??
                backendTotalRawRow?.inventory_coverage_ratio
            ),
            coverageCurrentAndTransit:
                getInventoryCoverageCurrentAndTransitValue(backendTotalRawRow) ||
                (() => {
                    const unitsSold =
                        getCurrentMonthUnitsSold(backendTotalRawRow) ||
                        getSalesLast30DaysValue(backendTotalRawRow);
                    const dashboardSalesValue =
                        getSalesLast30DaysValue(backendTotalRawRow) || unitsSold;
                    const totalInStock =
                        firstInventoryNumberValue(backendTotalRawRow, [
                            "total_stock",
                            "Total Sellable Inventory In Stock",
                            "Total Sellable In Stock",
                        ]) ||
                        getInventoryCurrentFbaValue(backendTotalRawRow) +
                        getInventoryCurrentAwdValue(backendTotalRawRow);
                    const totalInTransit =
                        firstInventoryNumberValue(backendTotalRawRow, [
                            "total_transit",
                            "Total Sellable Inventory In Transit",
                            "Total Sellable In Transit",
                        ]) ||
                        getInventoryTransitFbaValue(backendTotalRawRow) +
                        getInventoryTransitAwdValue(backendTotalRawRow);

                    return dashboardSalesValue > 0
                        ? (totalInStock + totalInTransit) / dashboardSalesValue
                        : 0;
                })(),

            inventoryAlert: "",
            salesRank: "",
            previousSalesRank: "",

            isTotalRow: true,
        }
        : null;

    const backendPercentageHeatmapRow = buildBackendPercentageHeatmapRow(
        backendPercentageRawRow,
        isUsingSplitFirst180
    );

    const finalHeatmapData = [
        ...heatmapData,
        ...(backendTotalHeatmapRow ? [backendTotalHeatmapRow] : []),
        ...(backendPercentageHeatmapRow ? [backendPercentageHeatmapRow] : []),
    ];

    const overallAgeing = latestRows.reduce(
        (acc, row) => {
            acc.zeroToNinety += getInventoryAgeValue(
                row,
                "inv-age-0-to-90-days"
            );

            acc.ninetyOneToOneEighty += getInventoryAgeValue(
                row,
                "inv-age-91-to-180-days"
            );

            acc.zeroToOneEighty += getInventoryAgeValue(
                row,
                "inv-age-0-to-180-days"
            );

            acc.oneEightyOneToTwoSeventy += getInventoryAgeValue(
                row,
                "inv-age-181-to-270-days"
            );

            acc.twoSeventyOneToThreeSixtyFive += getInventoryAgeValue(
                row,
                "inv-age-271-to-365-days"
            );

            acc.threeSixtyFivePlus += getInventoryAgeValue(
                row,
                "inv-age-365-plus-days"
            );

            return acc;
        },
        {
            zeroToNinety: 0,
            ninetyOneToOneEighty: 0,
            zeroToOneEighty: 0,
            oneEightyOneToTwoSeventy: 0,
            twoSeventyOneToThreeSixtyFive: 0,
            threeSixtyFivePlus: 0,
        }
    );

    const backendSummaryDonutData = buildDonutDataFromInventoryAgeSummary(
        latestResponse?.inventory_age_summary
    );
    const fallbackUnfulfillableUnits = mergedRows.reduce(
        (sum, row) =>
            sum +
            getInventoryUnsellableFbaValue(row) +
            getInventoryUnsellableAwdValue(row),
        0
    );

    const fallbackDonutData: DonutChartItem[] = isUsingSplitFirst180
        ? [
            {
                bucket: "0–90 Days",
                units: overallAgeing.zeroToNinety,
                color: "#7B9A6D",
            },
            {
                bucket: "91–180 Days",
                units: overallAgeing.ninetyOneToOneEighty,
                color: "#FDD36F",
            },
            {
                bucket: "181–270 Days",
                units: overallAgeing.oneEightyOneToTwoSeventy,
                color: "#ED9F50",
            },
            {
                bucket: "271–365 Days",
                units: overallAgeing.twoSeventyOneToThreeSixtyFive,
                color: "#C49466",
            },
            {
                bucket: "365+ Days",
                units: overallAgeing.threeSixtyFivePlus,
                color: "#B75A5A",
            },
        ]
        : [
            {
                bucket: "0–180 Days",
                units: overallAgeing.zeroToOneEighty,
                color: "#7B9A6D",
            },
            {
                bucket: "181–270 Days",
                units: overallAgeing.oneEightyOneToTwoSeventy,
                color: "#ED9F50",
            },
            {
                bucket: "271–365 Days",
                units: overallAgeing.twoSeventyOneToThreeSixtyFive,
                color: "#C49466",
            },
            {
                bucket: "365+ Days",
                units: overallAgeing.threeSixtyFivePlus,
                color: "#B75A5A",
            },
        ];

    const fallbackDonutTotalUnits =
        fallbackDonutData.reduce(
            (sum, item) => sum + inventoryToNum(item.units),
            0
        ) + fallbackUnfulfillableUnits;

    const fallbackDonutDataWithUnsellable: DonutChartItem[] = [
        ...fallbackDonutData,
        {
            bucket: "Unsellable",
            units: fallbackUnfulfillableUnits,
            percentageShare:
                fallbackDonutTotalUnits > 0
                    ? (fallbackUnfulfillableUnits / fallbackDonutTotalUnits) * 100
                    : undefined,
            color: "#3A8EA4",
        },
    ];

    const donutData: DonutChartItem[] =
        backendSummaryDonutData.length > 0
            ? backendSummaryDonutData
            : fallbackDonutDataWithUnsellable;

    const donutTotalUnits = getFirstPositiveInventoryValue(
        donutData.reduce((sum, item) => sum + inventoryToNum(item.units), 0),
        latestResponse?.inventory_age_summary?.percentage_base_total,
        latestResponse?.inventory_age_summary?.total
    );

    const isAllTrendSelected = selectedTrendBucketValue === "all";

    const selectedTrendBucket =
        AGEING_TREND_BUCKET_OPTIONS.find(
            (bucket) => bucket.value === selectedTrendBucketValue
        ) ||
        AGEING_TREND_BUCKET_OPTIONS.find(
            (bucket) => bucket.value === "365+ days"
        ) ||
        AGEING_TREND_BUCKET_OPTIONS[0];

    const trendDataFromSummary = isAllTrendSelected
        ? []
        : buildAgeingTrendDataFromSummary(
            selectedAgeSummaryResponses,
            selectedTrendBucket.column
        );

    const trendDataFromInventoryCurrent = isAllTrendSelected
        ? []
        : buildAgeingTrendDataFromInventoryCurrent(
            selectedInventoryResponses,
            selectedTrendBucket.column
        );

    const trendData =
        trendDataFromSummary.length > 0
            ? trendDataFromSummary
            : trendDataFromInventoryCurrent;

    const trendAllSeriesData: AgeingTrendAllSeriesItem[] =
        AGEING_TREND_BUCKET_OPTIONS.map((bucket) => {
            const dataFromSummary = buildAgeingTrendDataFromSummary(
                selectedAgeSummaryResponses,
                bucket.column
            );

            const dataFromInventoryCurrent = buildAgeingTrendDataFromInventoryCurrent(
                selectedInventoryResponses,
                bucket.column
            );

            return {
                bucketValue: bucket.value,
                bucketLabel: bucket.label,
                color: bucket.color,
                data:
                    dataFromSummary.length > 0
                        ? dataFromSummary
                        : dataFromInventoryCurrent,
            };
        });

    const healthyRows = latestRows.filter((row) =>
        hasInventoryValue(row, "inv-age-0-to-180-days") ||
        hasInventoryValue(row, "inv-age-0-to-90-days") ||
        hasInventoryValue(row, "inv-age-91-to-180-days")
    );

    const highAlertRows = latestRows.filter((row) => hasHighAlert(row));

    const highAlertAvgCoverageRatio =
        typeof latestResponse?.high_alert_coverage_summary?.average_coverage_ratio === "number"
            ? latestResponse.high_alert_coverage_summary.average_coverage_ratio
            : (() => {
                const validCoverageRows = highAlertRows
                    .map((row) => inventoryToNum(row?.["Coverage Ratio (In Months)"]))
                    .filter((value) => value > 0);

                if (!validCoverageRows.length) return 0;

                return (
                    validCoverageRows.reduce((sum, value) => sum + value, 0) /
                    validCoverageRows.length
                );
            })();

    const discountRows = latestRows.filter((row) =>
        hasInventoryValue(row, "inv-age-0-to-180-days")
    );

    const liquidateRows = latestRows.filter((row) =>
        hasOlderThan180Inventory(row)
    );

    const unfulfillableRows = latestRows.filter((row) =>
        inventoryToNum(row?.["unfulfillable-quantity"]) > 0
    );

    const estimatedStorageCategory =
        latestResponse?.categories?.estimated_storage_cost as any;

    const storageCostTotal =
        getEstimatedStorageCostTotal(latestResponse) ||
        latestRows.reduce(
            (sum, row) =>
                sum + inventoryToNum(row?.["estimated-storage-cost-next-month"]),
            0
        );

    const previousStorageCostTotal = inventoryToNum(
        estimatedStorageCategory?.previous_storage_cost
    );

    const storageCostDelta = storageCostTotal - previousStorageCostTotal;

    const storageCostDeltaPercentage =
        previousStorageCostTotal > 0
            ? (storageCostDelta / Math.abs(previousStorageCostTotal)) * 100
            : null;

    const storageCostRows = latestRows.filter((row) =>
        inventoryToNum(row?.["estimated-storage-cost-next-month"]) > 0
    );

    const getSkuCountForAgeColumn = (
        rows: InventoryCurrentRow[],
        column: string
    ) => {
        return rows.filter((row) => {
            const sku = getInventoryRowSku(row);
            const productName = getInventoryRowProductName(row);

            if (!sku && !productName) return false;

            return getInventoryAgeValue(row, column) > 0;
        }).length;
    };

    const getUnitCountForAgeColumn = (
        rows: InventoryCurrentRow[],
        column: string
    ) => {
        return rows.reduce((sum, row) => {
            return sum + getInventoryAgeValue(row, column);
        }, 0);
    };

    const actions: ActionCardItem[] = [
        ...(isUsingSplitFirst180
            ? [
                {
                    key: "age_0_90",
                    label: "Healthy",
                    description: "Stock aged 0–90 days",
                    count: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-90-days"),
                    displayValue: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-90-days"),
                    skuCount: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-90-days"),
                    unitCount: getUnitCountForAgeColumn(latestRows, "inv-age-0-to-90-days"),
                    color: "#7B9A6D",
                    backgroundColor: "#ffffff",
                },

                // ✅ High Alert comes before 91–180 Days
                {
                    key: "high_alert",
                    label: INVENTORY_ACTION_META.high_alert.label,
                    description: INVENTORY_ACTION_META.high_alert.description,
                    count:
                        latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
                        getUniqueSkuCount(highAlertRows),
                    displayValue:
                        latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
                        getUniqueSkuCount(highAlertRows),
                    skuCount:
                        latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
                        getUniqueSkuCount(highAlertRows),

                    avgCoverageRatio: highAlertAvgCoverageRatio,

                    color: INVENTORY_ACTION_META.high_alert.color,
                    backgroundColor: INVENTORY_ACTION_META.high_alert.backgroundColor,
                },

                {
                    key: "age_91_180",
                    label: "Discount",
                    description: "Stock aged 91–180 days",
                    count: getSkuCountForAgeColumn(latestRows, "inv-age-91-to-180-days"),
                    displayValue: getSkuCountForAgeColumn(latestRows, "inv-age-91-to-180-days"),
                    skuCount: getSkuCountForAgeColumn(latestRows, "inv-age-91-to-180-days"),
                    unitCount: getUnitCountForAgeColumn(latestRows, "inv-age-91-to-180-days"),
                    color: "#FDD36F",
                    backgroundColor: "#ffffff",
                },
            ]
            : [
                {
                    key: "healthy",
                    label: INVENTORY_ACTION_META.healthy.label,
                    description: INVENTORY_ACTION_META.healthy.description,
                    count: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-180-days"),
                    displayValue: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-180-days"),
                    skuCount: getSkuCountForAgeColumn(latestRows, "inv-age-0-to-180-days"),
                    unitCount: getUnitCountForAgeColumn(latestRows, "inv-age-0-to-180-days"),
                    color: INVENTORY_ACTION_META.healthy.color,
                    backgroundColor: INVENTORY_ACTION_META.healthy.backgroundColor,
                },

                // ✅ High Alert comes after Healthy when 0–180 combined exists
                {
                    key: "high_alert",
                    label: INVENTORY_ACTION_META.high_alert.label,
                    description: INVENTORY_ACTION_META.high_alert.description,
                    count:
                        latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
                        getUniqueSkuCount(highAlertRows),
                    displayValue:
                        latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
                        getUniqueSkuCount(highAlertRows),
                    skuCount:
                        latestResponse?.high_alert_coverage_summary?.high_alert_sku_count ??
                        getUniqueSkuCount(highAlertRows),

                    avgCoverageRatio: highAlertAvgCoverageRatio,

                    color: INVENTORY_ACTION_META.high_alert.color,
                    backgroundColor: INVENTORY_ACTION_META.high_alert.backgroundColor,
                },
            ]),

        {
            key: "liquidate",
            label: INVENTORY_ACTION_META.liquidate.label,
            description: INVENTORY_ACTION_META.liquidate.description,
            count: getUniqueSkuCount(liquidateRows),
            displayValue: getUniqueSkuCount(liquidateRows),
            skuCount: getUniqueSkuCount(liquidateRows),
            unitCount: sumInventoryUnitsByKeys(liquidateRows, [
                "inv-age-181-to-270-days",
                "inv-age-271-to-365-days",
                "inv-age-365-plus-days",
            ]),
            color: INVENTORY_ACTION_META.liquidate.color,
            backgroundColor: INVENTORY_ACTION_META.liquidate.backgroundColor,
        },

        {
            key: "unfulfillable",
            label: INVENTORY_ACTION_META.unfulfillable.label,
            description: INVENTORY_ACTION_META.unfulfillable.description,
            count: getUniqueSkuCount(unfulfillableRows),
            displayValue: getUniqueSkuCount(unfulfillableRows),
            skuCount: getUniqueSkuCount(unfulfillableRows),
            unitCount: unfulfillableRows.reduce(
                (sum, row) => sum + inventoryToNum(row?.["unfulfillable-quantity"]),
                0
            ),
            color: INVENTORY_ACTION_META.unfulfillable.color,
            backgroundColor: INVENTORY_ACTION_META.unfulfillable.backgroundColor,
        },

        {
            key: "estimated_storage_cost",
            label: INVENTORY_ACTION_META.estimated_storage_cost.label,
            description: INVENTORY_ACTION_META.estimated_storage_cost.description,
            count: getUniqueSkuCount(storageCostRows),
            displayValue: formatInventoryStorageCost(
                storageCostTotal,
                countryName,
                homeCurrency
            ),
            deltaValue:
                previousStorageCostTotal > 0
                    ? formatInventoryStorageCost(
                        storageCostDelta,
                        countryName,
                        homeCurrency
                    )
                    : undefined,
            deltaPercentage: storageCostDeltaPercentage,
            skuCount: getUniqueSkuCount(storageCostRows),
            unitCount: storageCostRows.reduce(
                (sum, row) => sum + getRowAgeingTotalUnits(row),
                0
            ),
            color: INVENTORY_ACTION_META.estimated_storage_cost.color,
            backgroundColor: INVENTORY_ACTION_META.estimated_storage_cost.backgroundColor,
        },
    ];

    return {
        heatmapBuckets: dynamicHeatmapBuckets,
        heatmapData: finalHeatmapData,
        donutSku: "Overall",
        donutData,
        donutTotalUnits,
        trendSelectedBucket: isAllTrendSelected
            ? "all"
            : selectedTrendBucket.value,

        trendData,

        trendLineColor: isAllTrendSelected
            ? "#B75A5A"
            : selectedTrendBucket.color,

        trendAllSeriesData,

        trendBucketOptions: AGEING_TREND_BUCKET_OPTIONS.map((bucket) => ({
            label: bucket.label,
            value: bucket.value,
            color: bucket.color,
        })),

        actions,
        actionLogic: INVENTORY_ACTION_LOGIC,
        inventoryAgeSummary: latestResponse?.inventory_age_summary,
    };
};

const bucketKeyToApiColumnForExcel: Record<string, string> = {
    zeroToNinety: "inv-age-0-to-90-days",
    ninetyOneToOneEighty: "inv-age-91-to-180-days",
    zeroToOneEighty: "inv-age-0-to-180-days",
    oneEightyOneToTwoSeventy: "inv-age-181-to-270-days",
    twoSeventyOneToThreeSixtyFive: "inv-age-271-to-365-days",
    threeSixtyFivePlus: "inv-age-365-plus-days",
};

export const buildInventoryInsightsExcelRows = (
    rows: AgeingRiskHeatmapRow[],
    buckets: AgeingBucket[],
    inventoryAgeSummary?: InventoryCurrentApiResponse["inventory_age_summary"]
) => {
    const percentageRow = rows.find((row) => row.isPercentageRow);
    const hasAnyExcelBucketValue = (row: AgeingRiskHeatmapRow) => {
        return buckets.some((bucket) => Number(row[bucket.key] || 0) > 0);
    };

    const productRows = rows.filter(
        (row) =>
            !row.isPercentageRow &&
            !row.isTotalRow &&
            hasAnyExcelBucketValue(row)
    );

    const sortedRows = [...productRows].sort((a, b) => {
        const aUnitsSold = Number(a.unitsSold || 0);
        const bUnitsSold = Number(b.unitsSold || 0);
        return bUnitsSold - aUnitsSold;
    });

    const totalRow: AgeingRiskHeatmapRow = {
        productName: "Total",
        sku: "-",
        isTotalRow: true,
        zeroToNinety: 0,
        ninetyOneToOneEighty: 0,
        zeroToOneEighty: 0,
        oneEightyOneToTwoSeventy: 0,
        twoSeventyOneToThreeSixtyFive: 0,
        threeSixtyFivePlus: 0,
        available: 0,
        fcTransfer: 0,
        totalUnits: 0,
        inboundUnits: 0,
        unsellableUnits: 0,

        // keep current month sold separately
        unitsSold: 0,

        // Legacy fallback sales value for older inventory responses.
        salesLast30Days: 0,

        salesRank: "",
        coverageRatio: undefined,
        inventoryAlert: "",
    };

    buckets.forEach((bucket) => {
        const apiColumn = bucketKeyToApiColumnForExcel[bucket.key];

        totalRow[bucket.key] =
            inventoryAgeSummary?.columns?.[apiColumn]?.total ??
            sortedRows.reduce(
                (sum, row) => sum + Number(row[bucket.key] || 0),
                0
            );
    });

    totalRow.available =
        inventoryAgeSummary?.sellable_total ??
        sortedRows.reduce((sum, row) => sum + Number(row.available || 0), 0);

    totalRow.totalUnits = totalRow.available;

    totalRow.inboundUnits = sortedRows.reduce(
        (sum, row) => sum + Number(row.inboundUnits || 0),
        0
    );

    totalRow.unsellableUnits =
        inventoryAgeSummary?.unfulfillable_total ??
        sortedRows.reduce(
            (sum, row) => sum + Number(row.unsellableUnits || 0),
            0
        );

    // ✅ Current month units sold stays separate
    totalRow.unitsSold =
        inventoryAgeSummary?.current_month_units_sold_total ??
        sortedRows.reduce((sum, row) => sum + Number(row.unitsSold || 0), 0);

    // Keep the legacy sales value separate from current-month units sold.
    totalRow.salesLast30Days =
        sortedRows.reduce((sum, row) => sum + Number(row.salesLast30Days || 0), 0);

    return percentageRow
        ? [...sortedRows, totalRow, percentageRow]
        : [...sortedRows, totalRow];
};

const getUniqueSkuCount = (rows: InventoryCurrentRow[]) => {
    const skus = new Set<string>();

    rows.forEach((row) => {
        const sku = getInventoryRowSku(row);
        if (sku) skus.add(sku);
    });

    return skus.size;
};

const sumInventoryUnitsByKeys = (
    rows: InventoryCurrentRow[],
    keys: string[]
) => {
    return rows.reduce((sum, row) => {
        return (
            sum +
            keys.reduce(
                (rowSum, key) => rowSum + inventoryToNum(row?.[key]),
                0
            )
        );
    }, 0);
};

const getRowAgeingTotalUnits = (row: InventoryCurrentRow) => {
    const available = inventoryToNum(row?.available);

    if (available > 0) return available;

    const splitFirst180 =
        inventoryToNum(row?.["inv-age-0-to-90-days"]) +
        inventoryToNum(row?.["inv-age-91-to-180-days"]);

    const combinedFirst180 = inventoryToNum(row?.["inv-age-0-to-180-days"]);

    return (
        (splitFirst180 > 0 ? splitFirst180 : combinedFirst180) +
        inventoryToNum(row?.["inv-age-181-to-270-days"]) +
        inventoryToNum(row?.["inv-age-271-to-365-days"]) +
        inventoryToNum(row?.["inv-age-365-plus-days"])
    );
};

const hasInventoryValue = (row: InventoryCurrentRow, key: string) => {
    return inventoryToNum(row?.[key]) > 0;
};

const hasOlderThan180Inventory = (row: InventoryCurrentRow) => {
    return (
        inventoryToNum(row?.["inv-age-181-to-270-days"]) > 0 ||
        inventoryToNum(row?.["inv-age-271-to-365-days"]) > 0 ||
        inventoryToNum(row?.["inv-age-365-plus-days"]) > 0
    );
};

const hasHighAlert = (row: InventoryCurrentRow) => {
    return String(row?.["Inventory Alerts"] || "")
        .trim()
        .toLowerCase() === "high alert";
};
