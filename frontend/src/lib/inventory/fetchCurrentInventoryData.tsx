// /src/lib/inventory/fetchCurrentInventoryData.ts
export type InventoryRow = Record<string, string | number>;

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
  XLSX: any;       // pass xlsx-js-style instance from caller
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
    } catch {}
    throw new Error(msg);
  }

  return res.json().catch(() => ({}));
}

function getCurrentInventoryEndpoint(baseURL: string) {
  return `${baseURL}/current_inventory`;
}

function base64ToArrayBuffer(base64: string) {
  const byteCharacters = atob(base64);
  const buffers: ArrayBuffer[] = [];

  for (let offset = 0; offset < byteCharacters.length; offset += 1024) {
    const slice = byteCharacters.slice(offset, offset + 1024);
    const byteNumbers = new Array(slice.length);
    for (let i = 0; i < slice.length; i++) byteNumbers[i] = slice.charCodeAt(i);
    buffers.push(new Uint8Array(byteNumbers).buffer as ArrayBuffer);
  }

  const totalLen = buffers.reduce((sum, b) => sum + b.byteLength, 0);
  const merged = new Uint8Array(totalLen);
  let pos = 0;

  for (const b of buffers) {
    merged.set(new Uint8Array(b), pos);
    pos += b.byteLength;
  }

  return merged.buffer;
}

function toNumberOrZero(v: any): number {
  if (v === null || v === undefined || v === "" || v === "-") return 0;
  if (typeof v === "number") return Number.isFinite(v) ? v : 0;

  const n = Number(String(v).replace(/,/g, "").trim());
  return Number.isFinite(n) ? n : 0;
}

function normalizeInventoryRow(row: InventoryRow): InventoryRow {
  return {
    ...row,

    // force exact keys used by the UI table
    "Sales Rank": toNumberOrZero(
      row["Sales Rank"] ??
      row["Sales rank"] ??
      row["sales_rank"]
    ),

    "Estimated Storage Cost ($)": toNumberOrZero(
      row["Estimated Storage Cost ($)"] ??
      row["Estimated Storage Cost"] ??
      row["estimated_storage_cost"] ??
      row["Estimated Storage Cost ($ )"]
    ),

    "Coverage Ratio (In Months)": toNumberOrZero(
      row["Coverage Ratio (In Months)"] ??
      row["Coverage Ratio"] ??
      row["coverage_ratio"]
    ),
  };
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
}> {
  const { baseURL, token, country, month, year, XLSX } = args;

  await hitAgedInventoryOnce(baseURL, token);

  const endpoint = getCurrentInventoryEndpoint(baseURL);

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ month, year, country }),
  });

  if (!res.ok) {
    const errJson = await res.json().catch(() => ({}));
    throw new Error(errJson?.error || "Failed to fetch CurrentInventory data");
  }

  const json = await res.json().catch(() => ({}));

  const rawAlerts = json?.inventory_alerts || {};
  const normalizedAlerts: Record<string, { alert?: string; alert_type?: string }> = {};

  Object.keys(rawAlerts).forEach((k) => {
    normalizedAlerts[normalizeSku(k)] = rawAlerts[k];
  });

  const fileData: string | undefined = json?.data;

  // fallback dummy rows
  if (!fileData) {
    return {
      rows: getDummyInventoryRows(),
      alerts: getDummyInventoryAlerts(),
    };
  }

  const ab = base64ToArrayBuffer(fileData);
  const arr = new Uint8Array(ab);

  const wb = XLSX.read(arr, { type: "array" });
  const sheetName = wb.SheetNames[0];
  const sheet = wb.Sheets[sheetName];

  const rawRows = (XLSX.utils.sheet_to_json as <T>(sheet: any, opts?: any) => T[])<InventoryRow>(
    sheet,
    { defval: "" }
  );

  const rows = rawRows.map(normalizeInventoryRow);

  return { rows, alerts: normalizedAlerts };
}