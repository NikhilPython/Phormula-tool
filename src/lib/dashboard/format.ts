// lib/dashboard/format.ts

export const fmtCurrency = (val: any, ccy = "GBP") => {
  if (val === null || val === undefined || val === "" || isNaN(Number(val)))
    return "—";
  return new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: ccy,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(val));
};

export const fmtGBP = (val: any) => fmtCurrency(val, "GBP");

export const fmtUSD = (val: any) => {
  if (val === null || val === undefined || val === "" || isNaN(Number(val)))
    return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(val));
};

export const fmtShopify = (val: any) => {
  if (val === null || val === undefined || val === "" || isNaN(Number(val)))
    return "—";
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(val));
};

export const fmtNum = (val: any) =>
  val === null || val === undefined || val === "" || isNaN(Number(val))
    ? "—"
    : new Intl.NumberFormat("en-GB", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      }).format(Number(val));

export const fmtPct = (val: any) =>
  val === null || val === undefined || isNaN(Number(val))
    ? "—"
    : `${Number(val).toFixed(2)}%`;

export const fmtUSDk = (val: any) => {
  if (val === null || val === undefined || val === "" || isNaN(Number(val)))
    return "—";
  const n = Number(val);
  const abs = Math.abs(n);

  if (abs < 1000) {
    return fmtUSD(n);
  }

  const k = n / 1000;
  const base = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  }).format(k);

  return `${base}k`;
};

export const fmtInt = (val: any) =>
  val === null || val === undefined || val === "" || isNaN(Number(val))
    ? "—"
    : new Intl.NumberFormat("en-GB", {
        maximumFractionDigits: 0,
      }).format(Math.round(Number(val)));

export const toNumberSafe = (v: any) => {
  if (v === null || v === undefined) return 0;
  if (typeof v === "number") return v;
  const s = String(v).replace(/[, ]+/g, "");
  const n = Number(s);
  return isNaN(n) ? 0 : n;
};

export const calcDeltaPct = (
  current: number,
  previous: number | null | undefined
) => {
  const prev = Number(previous ?? 0);
  const curr = Number(current ?? 0);

  if (!prev || !Number.isFinite(prev)) return null; // avoid divide-by-zero
  const pct = ((curr - prev) / prev) * 100;
  return pct;
};
