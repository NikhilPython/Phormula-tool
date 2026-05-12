// /src/lib/inventory/fetchCurrentInventoryData.ts

export type InventoryRow = Record<string, string | number | null>;

const normalizeSku = (v: any) =>
  String(v || "")
    .trim()
    .toUpperCase();

type FetchArgs = {
  baseURL: string;
  token: string;
  country: string; // "global" | "uk" | "us" | "ca"
  month: string;   // "february" etc
  year: string;    // "2026" etc
  XLSX?: any;      // no longer required for new response, kept to avoid caller changes
};

async function hitAgedInventoryOnce(baseURL: string, jwtToken: string) {
  const res = await fetch(`${baseURL}/amazon_api/inventory/aged`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${jwtToken}`,
      "Content-Type": "application/json",
    },
  });

  if (!res.ok) {
    let msg = `Aged Inventory API Error: ${res.status}`;
    try {
      const j = await res.json();
      if (j?.error) msg = j.error;
    } catch { }
    throw new Error(msg);
  }

  return res.json().catch(() => ({}));
}

function getCurrentInventoryEndpoint(baseURL: string) {
  return `${baseURL}/current_inventory`;
}

function toNumberOrZero(v: any): number {
  if (v === null || v === undefined || v === "" || v === "-") return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;

  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
}

function firstValue(row: any, keys: string[], fallback: any = 0) {
  for (const key of keys) {
    if (row?.[key] !== undefined && row?.[key] !== null && row?.[key] !== "") {
      return row[key];
    }
  }
  return fallback;
}

function getCurrentMonthUnits(row: any) {
  const key = Object.keys(row || {}).find((k) =>
    k.toLowerCase().startsWith("current month units sold")
  );

  return key ? row[key] : 0;
}

function normalizeInventoryRow(row: InventoryRow): InventoryRow {
  const sku = normalizeSku(row["SKU"] ?? row["sku"]);

  const currentInventory = firstValue(row, [
    "Current Inventory",
    "Inventory at the end of the month",
    "available",
    "available_quantity",
    "fulfillable_quantity",
    "total_quantity",
  ]);

  const inventory180Plus =
    toNumberOrZero(row["Inventory 180+ Days"]) ||
    toNumberOrZero(row["inv-age-181-to-270-days"]) +
    toNumberOrZero(row["inv-age-271-to-365-days"]) +
    toNumberOrZero(row["inv-age-365-plus-days"]);

  return {
    ...row,

    // UI-friendly aliases
    "S.No": firstValue(row, ["S.No", "Sno.", "sno"], ""),
    "SKU": sku,
    "Product Name": firstValue(row, ["Product Name", "product_name"], ""),
    "MTD Sales": toNumberOrZero(
      firstValue(row, ["MTD Sales"], getCurrentMonthUnits(row))
    ),
    "Sales Last 30 Days": toNumberOrZero(
      firstValue(row, ["Sales Last 30 Days", "last_30_days_units"], 0)
    ),
    "Sales Rank": toNumberOrZero(
      firstValue(row, ["Sales Rank", "Sales rank", "sales-rank", "sales_rank"], 0)
    ),
    "Current Inventory": toNumberOrZero(currentInventory),
    "Inventory 180+ Days": toNumberOrZero(inventory180Plus),
    "Estimated Storage Cost ($)": toNumberOrZero(
      firstValue(
        row,
        [
          "Estimated Storage Cost ($)",
          "Estimated Storage Cost",
          "estimated-storage-cost-next-month",
          "estimated_storage_cost",
          "Estimated Storage Cost ($ )",
        ],
        0
      )
    ),
    "Coverage Ratio (In Months)": toNumberOrZero(
      firstValue(
        row,
        ["Coverage Ratio (In Months)", "Coverage Ratio", "coverage_ratio"],
        0
      )
    ),
    "Inventory Alerts": String(
      firstValue(row, ["Inventory Alerts", "inventory_alert"], "")
    ),
  };
}

function normalizeAlertsMap(rawAlerts: any = {}) {
  const normalized: Record<string, { alert?: string; alert_type?: string }> = {};

  Object.keys(rawAlerts || {}).forEach((k) => {
    normalized[normalizeSku(k)] = rawAlerts[k];
  });

  return normalized;
}

function addCountryToRows(rows: any[] = [], countryName: "uk" | "us" | "ca") {
  return rows.map((row) => ({
    ...row,
    Country: countryName.toUpperCase(),
    country: countryName,
  }));
}

function getDummyInventoryRows(): InventoryRow[] {
  return [
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
    },
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
    },
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
    },
  ].map(normalizeInventoryRow);
}

function getDummyInventoryAlerts() {
  return {
    "DUMMY-SKU-001": { alert: "High alert", alert_type: "error" },
    "DUMMY-SKU-002": { alert: "High alert", alert_type: "error" },
    "DUMMY-SKU-003": { alert: "High alert", alert_type: "error" },
  };
}

export async function fetchCurrentInventoryData(args: FetchArgs): Promise<{
  rows: InventoryRow[];
  alerts: Record<string, { alert?: string; alert_type?: string }>;
  filename?: string;
  excelBase64?: string;
  warnings?: string[];
  meta?: any;
}> {
  const { baseURL, token, country, month, year } = args;

  await hitAgedInventoryOnce(baseURL, token);

  const res = await fetch(getCurrentInventoryEndpoint(baseURL), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ month, year, country }),
  });

  console.log("Current Inventory", res)

  if (!res.ok) {
    const errJson = await res.json().catch(() => ({}));
    throw new Error(errJson?.error || "Failed to fetch CurrentInventory data");
  }

  const json = await res.json().catch(() => ({}));

  const isGlobal = String(country || "").toLowerCase() === "global";

  const pickArray = (...values: any[]): any[] => {
    for (const value of values) {
      if (Array.isArray(value)) return value;
    }
    return [];
  };

  const pickString = (...values: any[]): string | undefined => {
    for (const value of values) {
      if (typeof value === "string" && value.trim()) return value;
    }
    return undefined;
  };

  const responseData = json?.data;

  let normalizedAlerts: Record<string, { alert?: string; alert_type?: string }> = {};
  let apiRows: any[] = [];
  let warnings: string[] = [];

  if (isGlobal) {
    const ukRows = addCountryToRows(
      Array.isArray(json?.skuwise_items_uk) ? json.skuwise_items_uk : [],
      "uk"
    );

    const usRows = addCountryToRows(
      Array.isArray(json?.skuwise_items_us) ? json.skuwise_items_us : [],
      "us"
    );

    apiRows = [...ukRows, ...usRows];

    const ukAlerts = normalizeAlertsMap(json?.inventory_alerts_uk);
    const usAlerts = normalizeAlertsMap(json?.inventory_alerts_us);

    normalizedAlerts = {
      ...Object.fromEntries(
        Object.entries(ukAlerts).map(([sku, alert]) => [`UK-${sku}`, alert])
      ),
      ...Object.fromEntries(
        Object.entries(usAlerts).map(([sku, alert]) => [`US-${sku}`, alert])
      ),
    };

    warnings = [
      ...(Array.isArray(json?.warnings_uk) ? json.warnings_uk : []),
      ...(Array.isArray(json?.warnings_us) ? json.warnings_us : []),
    ];
  } else {
    // Countrywise response: keep existing behavior
    normalizedAlerts = normalizeAlertsMap(json?.inventory_alerts || {});

    apiRows = pickArray(
      json?.skuwise_items,
      responseData?.skuwise_items,
      responseData?.rows,
      responseData?.items,
      json?.rows,
      json?.items,
      json?.table_data,
      responseData?.table_data
    );

    warnings = json?.warnings || responseData?.warnings || [];
  }

  console.log("[current_inventory raw response]", {
    isGlobal,
    topLevelKeys: Object.keys(json || {}),
    dataType: typeof responseData,
    dataKeys:
      responseData && typeof responseData === "object" && !Array.isArray(responseData)
        ? Object.keys(responseData)
        : [],
    skuwiseTopLevelCount: Array.isArray(json?.skuwise_items)
      ? json.skuwise_items.length
      : null,
    skuwiseUkCount: Array.isArray(json?.skuwise_items_uk)
      ? json.skuwise_items_uk.length
      : null,
    skuwiseUsCount: Array.isArray(json?.skuwise_items_us)
      ? json.skuwise_items_us.length
      : null,
    mappedRowsCount: apiRows.length,
    firstMappedRow: apiRows[0],
  });

  if (!apiRows.length) {
    console.warn("[current_inventory] No inventory rows found in JSON response", json);

    return {
      rows: [],
      alerts: normalizedAlerts,
      filename: json?.filename ?? responseData?.filename,
      excelBase64: typeof responseData === "string" ? responseData : json?.excelBase64,
      warnings: json?.warnings || responseData?.warnings || [],
      meta: json?.meta || responseData?.meta,
    };
  }

  return {
    rows: apiRows.map((row) => {
      const normalized = normalizeInventoryRow(row);

      if (!isGlobal) {
        // Countrywise rows stay exactly as before
        return normalized;
      }

      const rowCountry = String((row as any)?.country || "").toUpperCase();
      const sku = normalizeSku((row as any)?.SKU ?? (row as any)?.sku);

      return {
        ...normalized,
        Country: rowCountry,
        country: rowCountry.toLowerCase(),
        alertKey: `${rowCountry}-${sku}`,
      };
    }),
    alerts: normalizedAlerts,
    filename: json?.filename ?? responseData?.filename,
    excelBase64: pickString(
      typeof responseData === "string" ? responseData : undefined,
      json?.excelBase64,
      responseData?.excelBase64,
      responseData?.data
    ),
    warnings,
    meta: json?.meta || responseData?.meta,
  };
}