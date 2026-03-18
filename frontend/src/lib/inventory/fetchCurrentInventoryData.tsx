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
    } catch { }
    throw new Error(msg);
  }

  return res.json().catch(() => ({}));
}

// function getCurrentInventoryEndpoint(baseURL: string, inventoryCountry: string) {
//   return inventoryCountry === "global"
//     ? `${baseURL}/current_inventory`
//     : `${baseURL}/current_inventory`;
// }

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

  // merge chunks
  const totalLen = buffers.reduce((sum, b) => sum + b.byteLength, 0);
  const merged = new Uint8Array(totalLen);
  let pos = 0;
  for (const b of buffers) {
    merged.set(new Uint8Array(b), pos);
    pos += b.byteLength;
  }

  return merged.buffer;
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

  // alerts
  const rawAlerts = json?.inventory_alerts || {};
  const normalizedAlerts: Record<string, { alert?: string; alert_type?: string }> = {};
  Object.keys(rawAlerts).forEach((k) => {
    normalizedAlerts[normalizeSku(k)] = rawAlerts[k];
  });

  // file
  const fileData: string | undefined = json?.data;
  if (!fileData) throw new Error(json?.message || "Empty file received from server");

  const ab = base64ToArrayBuffer(fileData);
  const arr = new Uint8Array(ab);

  const wb = XLSX.read(arr, { type: "array" });
  const sheetName = wb.SheetNames[0];
  const sheet = wb.Sheets[sheetName];

  const rows = (XLSX.utils.sheet_to_json as <T>(sheet: any, opts?: any) => T[])<InventoryRow>(sheet, { defval: "" });

  return { rows, alerts: normalizedAlerts };
}
