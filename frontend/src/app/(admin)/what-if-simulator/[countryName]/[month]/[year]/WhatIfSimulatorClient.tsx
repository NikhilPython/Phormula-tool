"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import { useHomeCurrencyFromProfile } from "@/lib/utils/useHomeCurrencyFromProfile";

type CostKey =
  | "cogs"
  | "amazonFees"
  | "otherTransactionFees"
  | "inventoryCharges"
  | "shortTermStorage"
  | "longTermStorage"
  | "otherFees"
  | "shipping";
type CostInputs = Record<CostKey, number>;
type ScenarioInput = { units: number; asp: number };
type SliderDirection = "up" | "down" | null;
type AiScenarioInsight = { signature: string; text: string };
type InventoryPosition = { onHand: number; inbound: number; monthLabel: string; partial: boolean };
type HistoricalSku = {
  key: string;
  sku: string;
  name: string;
  units: number;
  asp: number;
  netSales: number;
  costAmounts: CostInputs;
  totalAds: number;
  hasTotalAds: boolean;
  netTaxes: number;
  netCredits: number;
};
type HistoricalData = {
  rows: HistoricalSku[];
  monthLabel: string;
  units: number;
  sales: number;
  totalAds: number;
  netTaxes: number;
  netCredits: number;
  costAmounts: CostInputs;
  sourceAvailable: boolean;
};

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];
const normalizeKey = (value: string) => value.toLowerCase().replace(/[^a-z0-9]/g, "");
const numberValue = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  const cleaned = String(value).replace(/,/g, "").replace(/[^\d.-]/g, "");
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};
const readField = (row: Record<string, unknown>, aliases: string[]) => {
  const fields = new Map<string, unknown>();
  Object.entries(row).forEach(([key, value]) => fields.set(normalizeKey(key), value));
  let zeroValue: { value: number; found: true } | null = null;
  for (const alias of aliases) {
    const value = numberValue(fields.get(normalizeKey(alias)));
    if (value === null) continue;
    if (value !== 0) return { value, found: true };
    zeroValue = { value: 0, found: true };
  }
  return zeroValue ?? { value: 0, found: false };
};

function inventoryPositionFromResponse(payload: any, monthLabel: string): InventoryPosition | null {
  const countryResponses = payload?.country_results && typeof payload.country_results === "object"
    ? Object.values(payload.country_results)
    : [payload];
  let onHand = 0;
  let inbound = 0;
  let foundInventory = false;
  let partial = false;

  countryResponses.forEach((countryResponse: any) => {
    const rows: Record<string, unknown>[] = Array.isArray(countryResponse?.rows)
      ? countryResponse.rows.filter((row: unknown) => row && typeof row === "object")
      : [];
    if (countryResponses.length > 1 && (countryResponse?.success === false || rows.length === 0)) {
      partial = true;
    }
    const isSummaryRow = (row: Record<string, unknown>) => {
      const sku = String(row.sku ?? "").trim().toLowerCase();
      const name = String(row.product_name ?? row.product ?? "").trim().toLowerCase();
      return [sku, name].some((value) => ["total", "grand total", "grand_total", "percentage", "% of total"].includes(value));
    };
    const productRows = rows.filter((row) => !isSummaryRow(row));
    const sourceRows = productRows.length ? productRows : rows.filter(isSummaryRow).slice(0, 1);

    sourceRows.forEach((row) => {
      const stockTotal = readField(row, ["total_stock", "Total Sellable Inventory In Stock", "Total Sellable In Stock"]);
      const sellable = readField(row, ["Sellable Units", "sellable_units", "sellableUnits", "sellable_sum_last"]);
      const available = readField(row, ["available", "available_quantity", "fulfillable_quantity", "afn_fulfillable_quantity"]);
      const fcTransfer = readField(row, ["fc-transfer", "fc_transfer", "fcTransfer", "reserved_fc_transfer"]);
      const awdOnHand = readField(row, ["total_onhand_quantity", "Current Inventory AWD", "current_inventory_awd"]);
      const transitTotal = readField(row, ["total_transit", "Total Sellable Inventory In Transit", "Total Sellable In Transit"]);
      const fbaInbound = readField(row, ["inbound-shipped", "In Transit FBA", "in_transit_fba", "inbound_quantity"]);
      const awdInbound = readField(row, ["total_inbound_quantity", "In Transit AWD", "in_transit_awd", "inbound_awd"]);

      const rowOnHand = stockTotal.found
        ? stockTotal.value
        : (sellable.found ? sellable.value : available.value + fcTransfer.value) + awdOnHand.value;
      const rowInbound = transitTotal.found ? transitTotal.value : fbaInbound.value + awdInbound.value;
      const hasInventoryFields = [stockTotal, sellable, available, fcTransfer, awdOnHand, transitTotal, fbaInbound, awdInbound]
        .some((field) => field.found);
      if (hasInventoryFields) {
        onHand += Math.max(0, rowOnHand);
        inbound += Math.max(0, rowInbound);
        foundInventory = true;
      }
    });
  });

  return foundInventory ? { onHand, inbound, monthLabel, partial } : null;
}

const absolute = (value: number) => Math.abs(value);
const isTotalRow = (row: Record<string, unknown>) => {
  const value = String(row.sku || row.product_name || row.product || "").trim().toLowerCase();
  return ["total", "grand_total", "grand total"].includes(value);
};

function previousMonth(month: string, year: string) {
  const monthIndex = MONTHS.findIndex((candidate) => candidate.toLowerCase() === decodeURIComponent(month).toLowerCase());
  if (monthIndex < 0) return null;
  const date = new Date(Number(year), monthIndex - 1, 1);
  return { month: MONTHS[date.getMonth()], year: date.getFullYear() };
}

function recentMonthsBefore(month: string, year: string, count: number) {
  const months: { month: string; year: number }[] = [];
  let cursor = previousMonth(month, year);
  while (cursor && months.length < count) {
    months.push(cursor);
    cursor = previousMonth(cursor.month, String(cursor.year));
  }
  return months;
}

function formatCurrency(value: number, currency: string, precision = 0) {
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency,
      minimumFractionDigits: precision,
      maximumFractionDigits: precision,
    }).format(Number.isFinite(value) ? value : 0);
  } catch {
    return `${currency} ${Number(value || 0).toFixed(precision)}`;
  }
}
const formatNumber = (value: number) => Math.round(Number(value) || 0).toLocaleString();
const emptyCosts = (): CostInputs => ({
  cogs: 0,
  amazonFees: 0,
  otherTransactionFees: 0,
  inventoryCharges: 0,
  shortTermStorage: 0,
  longTermStorage: 0,
  otherFees: 0,
  shipping: 0,
});
function costAmountsFromRow(row: Record<string, unknown>, isUkFormula: boolean, isUsFormula: boolean) {
  const cogs = readField(row, ["cost_of_unit_sold", "cogs", "cost_of_goods_sold", "product_cost"]);
  const directAmazon = readField(row, ["amazon_fee", "amazon_fees", "amazon_expense", "marketplace_fees"]);
  const fbaFees = readField(row, ["fba_fees", "fba_fee"]);
  const sellingFees = readField(row, ["selling_fees", "referral_fees", "selling_fee"]);
  const otherTransactions = readField(row, ["other_transaction_fees", "other_transactions"]);
  const shortStorage = readField(row, ["short_term_storage_fee", "short_term_storage"]);
  const longStorage = readField(row, ["long_term_storage_fee", "long_term_storage"]);
  const inventoryDirect = readField(row, [
    "inventory_charges_andd_reimbursement",
    "inventory_charges_and_reimbursement",
    "inventory_charges_reimbursement",
    "inventory_charges_reimbursement_total",
  ]);
  const inventoryCharges = readField(row, ["inventory_charges", "inventory_charge", "inventory_fees", "inventory_fee", "fba_disposal"]);
  const lostInventoryReimbursement = readField(row, [
    "reimbursement_lost_inventory_amount",
    "lost_inventory_reimbursement",
    "lost_total",
  ]);
  const inventoryValue = inventoryDirect.found
    ? inventoryDirect.value
    : absolute(lostInventoryReimbursement.value) - absolute(inventoryCharges.value);
  const hasInventory = inventoryDirect.found || inventoryCharges.found || lostInventoryReimbursement.found;
  const platformNew = readField(row, ["platformfeenew"]);
  const platformManagement = readField(row, ["platform_management_fees", "platform_management_fee"]);
  const otherAdjustment = readField(row, ["other_adjustment"]);
  const shipping = readField(row, ["shipping_charges", "shipment_fees", "shipment_charges"]);
  const amazonValue = directAmazon.found
    ? directAmazon.value
    : fbaFees.value + sellingFees.value;
  const hasAmazon = directAmazon.found || fbaFees.found || sellingFees.found;
  const otherFees = isUkFormula
    ? absolute(platformNew.value)
    : isUsFormula
      ? absolute(platformManagement.value - otherAdjustment.value)
      : 0;
  const hasOtherFees = isUkFormula
    ? platformNew.found
    : isUsFormula
      ? platformManagement.found || otherAdjustment.found
      : false;

  const amounts: CostInputs = {
    cogs: cogs.found ? absolute(cogs.value) : 0,
    amazonFees: hasAmazon ? absolute(amazonValue) : 0,
    otherTransactionFees: otherTransactions.found ? otherTransactions.value : 0,
    inventoryCharges: hasInventory ? inventoryValue : 0,
    shortTermStorage: shortStorage.found ? absolute(shortStorage.value) : 0,
    longTermStorage: longStorage.found ? absolute(longStorage.value) : 0,
    otherFees: hasOtherFees ? otherFees : 0,
    shipping: shipping.found ? absolute(shipping.value) : 0,
  };
  const found: Record<CostKey, boolean> = {
    cogs: cogs.found,
    amazonFees: hasAmazon,
    otherTransactionFees: otherTransactions.found,
    inventoryCharges: hasInventory,
    shortTermStorage: shortStorage.found,
    longTermStorage: longStorage.found,
    otherFees: hasOtherFees,
    shipping: shipping.found,
  };
  return { amounts, found };
}

function normalizeHistory(payload: any, monthLabel: string, country: string): HistoricalData {
  const normalizedCountry = country.trim().toLowerCase();
  const isUkFormula = ["uk", "gb", "united kingdom", "great britain"].includes(normalizedCountry);
  const isUsFormula = ["us", "usa", "united states", "global"].includes(normalizedCountry);
  const rawRows: Record<string, unknown>[] = Array.isArray(payload?.data)
    ? payload.data.filter((row: unknown) => row && typeof row === "object")
    : [];
  const totalRow = rawRows.find(isTotalRow) ?? {};
  const rows = rawRows.filter((row) => !isTotalRow(row)).map((row, index) => {
    const sku = String(row.sku ?? row.asin ?? row.product_id ?? "").trim();
    const name = String(row.product_name ?? row.product ?? row.title ?? sku ?? `Product ${index + 1}`).trim();
    const unitsRead = readField(row, ["total_quantity", "net_quantity", "quantity", "units", "unit_sold", "units_sold"]);
    const units = Math.max(0, unitsRead.value);
    const salesRead = readField(row, ["net_sales", "netsales", "total_sales", "sales", "revenue"]);
    const aspRead = readField(row, ["asp", "avg_sales_price", "average_selling_price", "selling_price"]);
    const netSales = absolute(salesRead.value || (aspRead.value * units));
    const asp = absolute(aspRead.found ? aspRead.value : (units > 0 ? netSales / units : 0));
    const { amounts } = costAmountsFromRow(row, isUkFormula, isUsFormula);
    const totalAds = readField(row, ["total_ads"]);
    const netTaxes = readField(row, ["net_taxes", "tax"]);
    const netCredits = readField(row, ["net_credits", "credits"]);

    return {
      key: sku || `${normalizeKey(name)}-${index}`,
      sku: sku || "—",
      name: name || sku || `Product ${index + 1}`,
      units,
      asp,
      netSales,
      costAmounts: amounts,
      totalAds: totalAds.found ? absolute(totalAds.value) : 0,
      hasTotalAds: totalAds.found,
      netTaxes: absolute(netTaxes.value),
      netCredits: netCredits.value,
    } satisfies HistoricalSku;
  }).filter((row) => row.sku !== "—" || row.name);

  const totalRecord = costAmountsFromRow(totalRow, isUkFormula, isUsFormula);
  const unitsRead = readField(totalRow, ["total_quantity", "net_quantity", "quantity", "units", "unit_sold", "units_sold"]);
  const rowUnits = rows.reduce((sum, row) => sum + row.units, 0);
  const units = rowUnits || Math.max(0, unitsRead.value);
  const rowSales = rows.reduce((sum, row) => sum + row.netSales, 0);
  const totalSalesRead = readField(totalRow, ["net_sales", "netsales", "total_sales", "sales", "revenue"]);
  const sales = absolute(totalSalesRead.found ? totalSalesRead.value : rowSales);
  const rowAds = readField(totalRow, ["total_ads"]);
  const totalAds = rowAds.found
    ? absolute(rowAds.value)
    : rows.filter((row) => row.hasTotalAds).reduce((sum, row) => sum + row.totalAds, 0);
  const totalNetTaxesRead = readField(totalRow, ["net_taxes", "tax"]);
  const totalNetCreditsRead = readField(totalRow, ["net_credits", "credits"]);
  const netTaxes = totalNetTaxesRead.found
    ? absolute(totalNetTaxesRead.value)
    : rows.reduce((sum, row) => sum + row.netTaxes, 0);
  const netCredits = totalNetCreditsRead.found
    ? totalNetCreditsRead.value
    : rows.reduce((sum, row) => sum + row.netCredits, 0);
  const costAmounts = emptyCosts();
  (Object.keys(costAmounts) as CostKey[]).forEach((key) => {
    const productTotal = rows.reduce((sum, row) => sum + row.costAmounts[key], 0);
    const totalValue = totalRecord.amounts[key];
    const totalFound = totalRecord.found[key];
    costAmounts[key] = totalFound && totalValue !== 0 ? totalValue : productTotal || (totalFound ? totalValue : 0);
  });
  const hasData = rows.length > 0 || sales > 0 || totalAds > 0 || netTaxes !== 0 || netCredits !== 0 || Object.values(costAmounts).some((value) => value !== 0);
  return { rows, monthLabel, units, sales, totalAds, netTaxes, netCredits, costAmounts, sourceAvailable: hasData };
}

function MetricCard({
  label,
  value,
  hint,
  accent = "border-slate-200",
  highlighted = false,
}: {
  label: string;
  value: React.ReactNode;
  hint?: string;
  accent?: string;
  highlighted?: boolean;
}) {
  return (
    <div className={`rounded-xl border border-t-4 p-4 shadow-sm ${accent} ${highlighted ? "bg-[#F7F7F7]" : "bg-white"}`}>
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</p>
      <div className="mt-2 text-xl font-bold text-slate-900">{value}</div>
      {hint && <p className="mt-1 text-xs text-slate-500">{hint}</p>}
    </div>
  );
}

function RangeSlider({
  label,
  value,
  baseline,
  max,
  step,
  format,
  currencyPrefix,
  accent = "border-[#FDD36F]",
  onChange,
}: {
  label: string;
  value: number;
  baseline: number;
  max: number;
  step: number;
  format: (value: number) => string;
  currencyPrefix?: string;
  accent?: string;
  onChange: (value: number) => void;
}) {
  const [draftValue, setDraftValue] = useState(String(value));
  const sliderMax = Math.max(max, value, step);
  const normalizeSliderValue = (number: number) => step >= 1 ? Math.round(number) : Number(number.toFixed(2));
  useEffect(() => setDraftValue(String(value)), [value]);
  const commitDraftValue = () => {
    const parsed = Number(draftValue);
    if (draftValue.trim() !== "" && Number.isFinite(parsed) && parsed >= 0) {
      if (parsed === value) {
        setDraftValue(String(value));
        return;
      }
      const committed = normalizeSliderValue(parsed);
      onChange(committed);
      setDraftValue(String(committed));
    } else {
      setDraftValue(String(value));
    }
  };

  const percentage = sliderMax > 0 ? (Math.min(value, sliderMax) / sliderMax) * 100 : 0;

  return (
    <div className={`block rounded-xl border border-t-4 bg-white p-4 shadow-sm ${accent}`}>
      <div className="flex items-center justify-between gap-3">
        <span className="text-sm font-medium text-charcoal-500">{label}</span>
      </div>
      <div className="relative mt-4 flex h-5 items-center">
        <input
          type="range"
          min={0}
          max={sliderMax}
          step={step}
          value={Math.min(value, sliderMax)}
          onChange={(event) => onChange(Number(event.target.value))}
          aria-label={`${label} slider`}
          style={{ "--slider-progress": `${percentage}%` } as React.CSSProperties}
          className="thin-range-slider"
        />
      </div>
      <span className="mt-1 flex justify-between gap-2 text-[11px] text-slate-400">
        <span>0</span>
        <span>Previous month: {format(baseline)}</span>
        <span>{format(sliderMax)}</span>
      </span>
      <div className="mt-3 flex items-center justify-between gap-3">
        <span className="text-xs text-charcoal-400">Scenario value</span>
        <label className="flex w-36 items-center gap-1.5 rounded-lg border border-slate-200 bg-slate-50 px-2 py-1.5 focus-within:border-[#5EA68E] focus-within:ring-2 focus-within:ring-[#5EA68E]/15">
          {currencyPrefix && <span className="text-xs text-slate-500">{currencyPrefix}</span>}
          <input
            type="number"
            min={0}
            step={step}
            value={draftValue}
            onChange={(event) => {
              const nextValue = event.target.value;
              setDraftValue(nextValue);
              if (nextValue.trim() === "" || /[.\-]$/.test(nextValue)) return;
              const parsed = Number(nextValue);
              if (Number.isFinite(parsed) && parsed >= 0) onChange(normalizeSliderValue(parsed));
            }}
            onBlur={commitDraftValue}
            onKeyDown={(event) => {
              if (event.key === "Enter") event.currentTarget.blur();
            }}
            aria-label={`${label} exact value`}
            className="w-full min-w-0 bg-transparent text-right text-sm font-semibold text-slate-900 outline-none"
          />
        </label>
      </div>
    </div>
  );
}

export default function WhatIfSimulatorClient() {
  const params = useParams<{ countryName: string; month: string; year: string }>();
  const countryName = decodeURIComponent(String(params?.countryName ?? "global"));
  const month = decodeURIComponent(String(params?.month ?? ""));
  const year = String(params?.year ?? "");
  const previous = useMemo(() => previousMonth(month, year), [month, year]);
  const previousMonthName = previous?.month;
  const previousYear = previous?.year;
  const { profileHomeCurrency } = useHomeCurrencyFromProfile();
  const country = countryName.toLowerCase();
  const currency = country === "global"
    ? String(profileHomeCurrency || "USD").toUpperCase()
    : country === "india" ? "INR" : ["uk", "gb", "united kingdom"].includes(country) ? "GBP" : ["eu", "europe"].includes(country) ? "EUR" : country === "ca" ? "CAD" : "USD";

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [history, setHistory] = useState<HistoricalData | null>(null);
  const [historicalMonths, setHistoricalMonths] = useState<HistoricalData[]>([]);
  const [benchmarkLoading, setBenchmarkLoading] = useState(true);
  const [benchmarkError, setBenchmarkError] = useState("");
  const [inventoryPosition, setInventoryPosition] = useState<InventoryPosition | null>(null);
  const [inventoryLoading, setInventoryLoading] = useState(false);
  const [inventoryError, setInventoryError] = useState("");
  const [overallInput, setOverallInput] = useState<ScenarioInput>({ units: 0, asp: 0 });
  const [changedInputs, setChangedInputs] = useState<Record<keyof ScenarioInput, boolean>>({ units: false, asp: false });
  const [inputDirections, setInputDirections] = useState<Record<keyof ScenarioInput, SliderDirection>>({ units: null, asp: null });
  const [targetTacosGoal, setTargetTacosGoal] = useState("");
  const [targetTacosChanged, setTargetTacosChanged] = useState(false);
  const [additionalExpense, setAdditionalExpense] = useState(0);
  const [additionalExpenseChanged, setAdditionalExpenseChanged] = useState(false);
  const [overallCosts, setOverallCosts] = useState<CostInputs>(emptyCosts());
  const [aiInsight, setAiInsight] = useState<AiScenarioInsight | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiError, setAiError] = useState("");

  useEffect(() => {
    if (!previousMonthName || previousYear === undefined || !Number.isFinite(previousYear)) {
      setLoading(false);
      setHistory(null);
      setHistoricalMonths([]);
      setBenchmarkLoading(false);
      setBenchmarkError("");
      setInventoryPosition(null);
      setInventoryLoading(false);
      setInventoryError("");
      setChangedInputs({ units: false, asp: false });
      setInputDirections({ units: null, asp: null });
      setTargetTacosGoal("");
      setTargetTacosChanged(false);
      setAdditionalExpense(0);
      setAdditionalExpenseChanged(false);
      setError("Select a valid month and year to load the previous month’s data.");
      return;
    }
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError("");
      setHistory(null);
      setHistoricalMonths([]);
      setBenchmarkLoading(true);
      setBenchmarkError("");
      setInventoryPosition(null);
      setInventoryLoading(true);
      setInventoryError("");
      setChangedInputs({ units: false, asp: false });
      setInputDirections({ units: null, asp: null });
      setTargetTacosGoal("");
      setTargetTacosChanged(false);
      setAdditionalExpense(0);
      setAdditionalExpenseChanged(false);
      try {
        const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
        if (!token) throw new Error("Sign in to load previous-month data.");
        const baseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000").replace(/\/$/, "");
        const periods = recentMonthsBefore(month, year, 12);
        const fetchPeriod = async (period: { month: string; year: number }) => {
          const query = new URLSearchParams({ month: period.month.toLowerCase(), year: String(period.year), country, benchmarkOnly: "true" });
          if (country === "global" && profileHomeCurrency) {
            query.set("homeCurrency", profileHomeCurrency.toLowerCase());
          }
          const response = await fetch(`${baseUrl}/skutableprofit?${query.toString()}`, {
            headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
            signal: controller.signal,
          });
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(payload?.error || `Could not load ${period.month} ${period.year} data.`);
          const normalized = normalizeHistory({ data: payload?.current_data ?? [] }, `${period.month} ${period.year}`, country);
          return { ...normalized, rows: [] };
        };

        const normalized = await fetchPeriod(periods[0]);
        setHistory(normalized);
        setHistoricalMonths([normalized]);
        setOverallInput({
          units: Math.round(normalized.units),
          asp: Number((normalized.units > 0 ? normalized.sales / normalized.units : 0).toFixed(2)),
        });
        setOverallCosts({ ...normalized.costAmounts });
        const initialTargetTacos =
          normalized.sales > 0
            ? (absolute(normalized.totalAds) / normalized.sales) * 100
            : null;

        setTargetTacosGoal(
          initialTargetTacos !== null && Number.isFinite(initialTargetTacos)
            ? initialTargetTacos.toFixed(2)
            : ""
        );
        setTargetTacosChanged(false);

        const loadInventoryPosition = async () => {
          const supportedCountries = ["uk", "gb", "united kingdom", "us", "usa", "united states", "global"];
          if (!supportedCountries.includes(country)) {
            setInventoryError("Inventory comparison is available for UK and US marketplaces.");
            setInventoryLoading(false);
            return;
          }

          const inventoryCountry = ["gb", "united kingdom"].includes(country)
            ? "uk"
            : ["usa", "united states"].includes(country) ? "us" : country;
          try {
            const query = new URLSearchParams({
              country_key: inventoryCountry,
              range_type: "monthly",
              month_name: periods[0].month.toLowerCase(),
              year: String(periods[0].year),
            });
            const response = await fetch(`${baseUrl}/inventory_current?${query.toString()}`, {
              headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
              cache: "no-store",
              signal: controller.signal,
            });
            const payload = await response.json().catch(() => ({}));
            if (!response.ok || payload?.success === false) {
              throw new Error(payload?.message || payload?.error || "Inventory data is unavailable.");
            }
            const selectedMonth = String(payload?.selected_month || periods[0].month);
            const monthLabel = `${selectedMonth.charAt(0).toUpperCase()}${selectedMonth.slice(1)} ${payload?.year || periods[0].year}`;
            const position = inventoryPositionFromResponse(payload, monthLabel);
            if (!position) throw new Error("No inventory quantities were returned.");
            if (!controller.signal.aborted) setInventoryPosition(position);
          } catch (inventoryLoadError: any) {
            if (inventoryLoadError?.name !== "AbortError" && !controller.signal.aborted) {
              setInventoryError(inventoryLoadError?.message || "Inventory data is unavailable.");
            }
          } finally {
            if (!controller.signal.aborted) setInventoryLoading(false);
          }
        };
        void loadInventoryPosition();

        const loadRemainingHistory = async () => {
          const loaded = [normalized];
          try {
            for (let index = 1; index < periods.length; index += 3) {
              const batch = await Promise.all(periods.slice(index, index + 3).map(fetchPeriod));
              loaded.push(...batch);
              if (!controller.signal.aborted) setHistoricalMonths([...loaded]);
            }
          } catch (historyError: any) {
            if (historyError?.name !== "AbortError" && !controller.signal.aborted) {
              setBenchmarkError(historyError?.message || "Some monthly history could not be loaded.");
            }
          } finally {
            if (!controller.signal.aborted) setBenchmarkLoading(false);
          }
        };
        if (periods.length > 1) void loadRemainingHistory();
        else setBenchmarkLoading(false);
      } catch (loadError: any) {
        if (loadError?.name !== "AbortError") {
          setError(loadError?.message || "Could not load previous-month data.");
          setHistory(null);
          setBenchmarkLoading(false);
          setInventoryLoading(false);
        }
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    };
    void load();
    return () => controller.abort();
  }, [country, month, year, previousMonthName, previousYear, profileHomeCurrency]);

  const calculateProfit = (
    sales: number,
    ads: number,
    costs: CostInputs,
    netTaxes = 0,
    netCredits = 0,
    additional = 0,
  ) => {
    const cm1 = sales - costs.amazonFees - absolute(netTaxes) - netCredits;
    const cm2 = cm1 - absolute(ads) - absolute(additional);
    return { cm1, cm2 };
  };

  const historicalBenchmarkRows = historicalMonths
    .filter((monthData) => monthData.sourceAvailable)
    .map((monthData) => ({
      month: monthData.monthLabel,
      units: monthData.units,
      sales: monthData.sales,
      asp: monthData.units > 0 ? monthData.sales / monthData.units : 0,
      ads: monthData.totalAds,
      netTaxes: monthData.netTaxes,
      netCredits: monthData.netCredits,
      cm2: calculateProfit(monthData.sales, monthData.totalAds, monthData.costAmounts, monthData.netTaxes, monthData.netCredits).cm2,
    }));
  const unitRecords = historicalBenchmarkRows;
  const adsRecords = historicalBenchmarkRows;
  const aspRecords = historicalBenchmarkRows.filter((record) => record.units > 0 && record.asp > 0);
  const cm2Records = historicalBenchmarkRows.filter((record) => record.units > 0 && record.sales > 0);
  const lowestUnitsMonth = unitRecords.length ? unitRecords.reduce((low, record) => record.units < low.units ? record : low) : null;
  const highestUnitsMonth = unitRecords.length ? unitRecords.reduce((high, record) => record.units > high.units ? record : high) : null;
  const lowestAspMonth = aspRecords.length ? aspRecords.reduce((low, record) => record.asp < low.asp ? record : low) : null;
  const highestAspMonth = aspRecords.length ? aspRecords.reduce((high, record) => record.asp > high.asp ? record : high) : null;
  const lowestAdsMonth = adsRecords.length ? adsRecords.reduce((low, record) => record.ads < low.ads ? record : low) : null;
  const highestAdsMonth = adsRecords.length ? adsRecords.reduce((high, record) => record.ads > high.ads ? record : high) : null;
  const lowestCm2Month = cm2Records.length ? cm2Records.reduce((low, record) => record.cm2 < low.cm2 ? record : low) : null;
  const highestCm2Month = cm2Records.length ? cm2Records.reduce((high, record) => record.cm2 > high.cm2 ? record : high) : null;
  const baselineAsp = history && history.units > 0 ? history.sales / history.units : overallInput.asp;
  const unitsSliderMax = Math.max(100, Math.ceil(Math.max(history?.units || overallInput.units || 1, highestUnitsMonth?.units || 0) * 3));
  const aspSliderMax = Math.max(1, baselineAsp * 3, (highestAspMonth?.asp || 0) * 3);
  const overallSales = overallInput.units * overallInput.asp;
  const parsedTargetTacosGoal = targetTacosGoal.trim() === "" ? null : numberValue(targetTacosGoal);
  const targetTacosGoalValue = parsedTargetTacosGoal !== null && parsedTargetTacosGoal >= 0
    ? parsedTargetTacosGoal
    : null;
  const adsBudget = targetTacosGoalValue !== null ? overallSales * targetTacosGoalValue / 100 : 0;
  const baselineAds = history?.totalAds ?? 0;
  const unitDirection = inputDirections.units ?? "up";
  const unitRecordMonth = unitDirection === "down" ? lowestUnitsMonth : highestUnitsMonth;
  const unitCm2Month = unitDirection === "down" ? lowestCm2Month : highestCm2Month;
  const unitsMatchesCm2Month = unitCm2Month && Math.abs(overallInput.units - Math.round(unitCm2Month.units)) <= 1;
  const unitComparison = unitRecordMonth
    ? unitDirection === "down"
      ? overallInput.units < unitRecordMonth.units ? "Your plan is below that low." : overallInput.units === unitRecordMonth.units ? "Your plan matches that low." : "Your plan is still above that low."
      : overallInput.units > unitRecordMonth.units ? "You’ve exceeded that high." : overallInput.units === unitRecordMonth.units ? "Your plan matches that high." : "Your plan is still below that high."
    : "";
  const inventoryInsight = inventoryPosition
    ? inventoryPosition.partial
      ? `Available inventory data for ${inventoryPosition.monthLabel} shows ${formatNumber(inventoryPosition.onHand)} units on hand and ${formatNumber(inventoryPosition.inbound)} inbound. Another marketplace is missing data, so confirm its stock before deciding dispatch quantities.`
      : overallInput.units > inventoryPosition.onHand + inventoryPosition.inbound
        ? `At ${inventoryPosition.monthLabel}, you had ${formatNumber(inventoryPosition.onHand)} units on hand and ${formatNumber(inventoryPosition.inbound)} inbound. This plan is short by about ${formatNumber(overallInput.units - inventoryPosition.onHand - inventoryPosition.inbound)} units; prepare dispatch or replenishment.`
        : overallInput.units > inventoryPosition.onHand
          ? `At ${inventoryPosition.monthLabel}, you had ${formatNumber(inventoryPosition.onHand)} units on hand and ${formatNumber(inventoryPosition.inbound)} inbound. Incoming stock can cover this plan’s ${formatNumber(overallInput.units - inventoryPosition.onHand)}-unit gap; confirm transfer/arrival.`
          : `At ${inventoryPosition.monthLabel}, you had ${formatNumber(inventoryPosition.onHand)} units on hand. This plan leaves about ${formatNumber(inventoryPosition.onHand - overallInput.units)} units before inbound stock; if demand supports it, you have room to plan more.`
    : inventoryError ? inventoryError : "";
  const unitsHistoryInsight = unitRecordMonth
    ? `Your ${unitDirection === "down" ? "lowest" : "highest"} monthly units were ${formatNumber(unitRecordMonth.units)} in ${unitRecordMonth.month}. ${unitComparison}${unitsMatchesCm2Month && unitCm2Month ? ` At about ${formatNumber(unitCm2Month.units)} units, CM2 was ${unitDirection === "down" ? "lowest" : "highest"} at ${formatCurrency(unitCm2Month.cm2, currency)} in ${unitCm2Month.month}.` : ""}`.trim()
    : benchmarkError ? "Past unit data could not be loaded for comparison." : "No past unit data was found for comparison.";
  const aspDirection = inputDirections.asp ?? "up";
  const aspRecordMonth = aspDirection === "down" ? lowestCm2Month : highestCm2Month;
  const aspMatchesCm2Month = aspRecordMonth && overallInput.asp.toFixed(2) === aspRecordMonth.asp.toFixed(2);
  const aspHistoryInsight = aspRecordMonth
    ? `CM2 was ${aspDirection === "down" ? "lowest" : "highest"} at ${formatCurrency(aspRecordMonth.cm2, currency)} in ${aspRecordMonth.month}, when ASP was ${formatCurrency(aspRecordMonth.asp, currency, 2)}. ${aspMatchesCm2Month ? "Your scenario matches that ASP." : overallInput.asp > aspRecordMonth.asp ? "Your scenario ASP is above that level." : "Your scenario ASP is below that level."}`
    : benchmarkError ? "Past sales data could not be loaded to compare ASP and CM2." : "No past sales data was found to compare ASP and CM2.";
  const adsDirection = adsBudget >= baselineAds ? "up" : "down";
  const adsRecordMonth = adsDirection === "down" ? lowestAdsMonth : highestAdsMonth;
  const adsHistoryInsight = adsRecordMonth
    ? `Your ${adsDirection === "down" ? "lowest" : "highest"} monthly ads spend was ${formatCurrency(adsRecordMonth.ads, currency)} in ${adsRecordMonth.month}. ${adsBudget > adsRecordMonth.ads ? "This budget is above that level." : adsBudget === adsRecordMonth.ads ? "This budget matches that level." : "This budget is below that level."}`
    : benchmarkError ? "Past ads data could not be loaded for comparison." : "No past ads data was found for comparison.";
  const costScale = history && history.units > 0 ? overallInput.units / history.units : 1;
  const overallCostTotals = Object.fromEntries(
    (Object.keys(overallCosts) as CostKey[]).map((key) => [key, overallCosts[key] * costScale]),
  ) as CostInputs;
  const projectedNetTaxes = (history?.netTaxes ?? 0) * costScale;
  const projectedNetCredits = (history?.netCredits ?? 0) * costScale;
  const overallProfit = calculateProfit(
    overallSales,
    adsBudget,
    overallCostTotals,
    projectedNetTaxes,
    projectedNetCredits,
    additionalExpense,
  );
  const result = {
    units: overallInput.units,
    sales: overallSales,
    cm1: overallProfit.cm1,
    cm2: overallProfit.cm2,
  };
  const margin = result.sales > 0 ? (result.cm2 / result.sales) * 100 : 0;
  const baselineProfit = history
    ? calculateProfit(history.sales, history.totalAds, history.costAmounts, history.netTaxes, history.netCredits)
    : { cm1: 0, cm2: 0 };
  const cm1Margin = result.sales > 0 ? (result.cm1 / result.sales) * 100 : 0;
  const baselineMargin = history && history.sales > 0 ? (baselineProfit.cm2 / history.sales) * 100 : 0;
  const scenarioInputsChanged =
    Object.values(changedInputs).some(Boolean) ||
    targetTacosChanged ||
    additionalExpenseChanged;
  const scenarioInsightsLoading = scenarioInputsChanged && (benchmarkLoading || (changedInputs.units && inventoryLoading));
  const signedCurrency = (value: number) => `${value >= 0 ? "+" : "−"}${formatCurrency(Math.abs(value), currency)}`;
  const aspChangeImpact = overallInput.units * (overallInput.asp - baselineAsp);
  const aspAdsBudgetImpact = aspChangeImpact * (targetTacosGoalValue ?? 0) / 100;
  const aspCm2Impact = aspChangeImpact - aspAdsBudgetImpact;
  const aspImpactInsight = `At ${formatNumber(overallInput.units)} units, ASP ${formatCurrency(baselineAsp, currency, 2)} → ${formatCurrency(overallInput.asp, currency, 2)} changes projected sales and CM1 by ${signedCurrency(aspChangeImpact)}. The ads budget changes by ${signedCurrency(aspAdsBudgetImpact)}, so CM2 changes by ${signedCurrency(aspCm2Impact)}.`;
  const adsChangeImpact = -(adsBudget - baselineAds);
  const adsImpactInsight = `Target TACoS sets ads budget to ${formatCurrency(adsBudget, currency)}. Compared with previous-month ads of ${formatCurrency(baselineAds, currency)}, this changes CM2 by ${signedCurrency(adsChangeImpact)}.`;
  const targetTacosGoalInsight = targetTacosGoalValue !== null
    ? `Target TACoS ${targetTacosGoalValue.toFixed(2)}% calculates an ads budget of ${formatCurrency(adsBudget, currency)} from projected sales.`
    : "Enter a Target TACoS to calculate the ads budget from projected sales.";
  const additionalExpenseInsight = `Additional expense of ${formatCurrency(additionalExpense, currency)} is deducted from CM2.`;

  const scenarioInsightPoints = [
    changedInputs.units ? unitsHistoryInsight : null,
    changedInputs.units ? inventoryInsight || null : null,
    changedInputs.asp ? aspHistoryInsight : null,
    changedInputs.asp ? aspImpactInsight : null,
    (changedInputs.units || changedInputs.asp || targetTacosChanged) ? adsHistoryInsight : null,
    (changedInputs.units || changedInputs.asp || targetTacosChanged) ? adsImpactInsight : null,
    targetTacosChanged ? targetTacosGoalInsight : null,
    additionalExpenseChanged ? additionalExpenseInsight : null,
  ].filter((point): point is string => Boolean(point));
  const scenarioSignature = JSON.stringify({
    country,
    month,
    year,
    currency,
    inputs: overallInput,
    targetTacosGoal: targetTacosGoalValue,
    additionalExpense,
    costs: overallCosts,
    history: history
      ? {
        month: history.monthLabel,
        units: history.units,
        sales: history.sales,
        totalAds: history.totalAds,
        netTaxes: history.netTaxes,
        netCredits: history.netCredits,
        costAmounts: history.costAmounts,
      }
      : null,
  });
  const aiInsightIsCurrent = aiInsight?.signature === scenarioSignature;

  const updateOverallInput = (field: keyof ScenarioInput, value: number) => {
    if (value !== overallInput[field]) {
      setInputDirections((current) => ({ ...current, [field]: value > overallInput[field] ? "up" : "down" }));
    }
    setOverallInput((current) => ({ ...current, [field]: value }));
    setChangedInputs((current) => ({ ...current, [field]: true }));
    setAiError("");
  };

  const generateAiAnalysis = async () => {
    setAiLoading(true);
    setAiError("");
    const requestSignature = scenarioSignature;

    try {
      const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
      if (!token) throw new Error("Sign in to generate an AI scenario analysis.");

      const baseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000").replace(/\/$/, "");
      const response = await fetch(`${baseUrl}/api/agent/what-if-analysis`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          country,
          currency,
          baseline: {
            month: history?.monthLabel ?? (previous ? `${previous.month} ${previous.year}` : "Previous month"),
            units: history?.units ?? 0,
            asp: baselineAsp,
            ads: history?.totalAds ?? 0,
            net_sales: history?.sales ?? 0,
            cm1: baselineProfit.cm1,
            cm2: baselineProfit.cm2,
            cm2_margin: baselineMargin,
            target_tacos_goal: history && history.sales > 0 ? (history.totalAds / history.sales) * 100 : null,
          },
          scenario: {
            month: "Current scenario",
            units: result.units,
            asp: overallInput.asp,
            ads: adsBudget,
            net_sales: result.sales,
            cm1: result.cm1,
            cm2: result.cm2,
            cm2_margin: margin,
            target_tacos_goal: targetTacosGoalValue,
            additional_expense: additionalExpense,
          },
          internal_cost_context: {
            baseline: {
              amazon_fees: history?.costAmounts.amazonFees ?? 0,
              net_taxes: history?.netTaxes ?? 0,
              net_credits: history?.netCredits ?? 0,
              additional_expense: 0,
            },
            scenario: {
              amazon_fees: overallCostTotals.amazonFees,
              net_taxes: projectedNetTaxes,
              net_credits: projectedNetCredits,
              additional_expense: additionalExpense,
            },
          },
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload?.error || "AI scenario analysis failed.");
      if (typeof payload?.analysis !== "string" || !payload.analysis.trim()) {
        throw new Error("The AI did not return a scenario analysis.");
      }
      setAiInsight({ signature: requestSignature, text: payload.analysis.trim() });
    } catch (requestError: any) {
      setAiError(requestError?.message || "AI scenario analysis is temporarily unavailable.");
    } finally {
      setAiLoading(false);
    }
  };

  return (
    <div className="relative w-full space-y-3 pb-10">
      <section className="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
        <div className="border-b border-slate-200 bg-[#F7F7F7] px-4 py-4 md:px-6">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-end">
            <div>
              <p className="text-xs font-bold uppercase tracking-[0.18em] text-green-500">Scenario planning</p>
              <h1 className="mt-2 text-2xl font-bold text-charcoal-500 md:text-3xl">What-if Simulator</h1>
              <p className="mt-2 max-w-3xl text-sm text-charcoal-500">Set units, ASP, Target TACoS and any additional expense.</p>
            </div>
            <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm shadow-sm">
              <span className="text-charcoal-400">Historical baseline</span>
              <p className="mt-0.5 font-semibold text-charcoal-500">{history?.monthLabel ?? (previous ? `${previous.month} ${previous.year}` : "Previous month")} · {countryName.toUpperCase()}</p>
            </div>
          </div>
        </div>

        {error && <div className="mx-5 mt-5 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900 md:mx-7">{error} The simulator will use zero historical cost assumptions until previous-month data loads.</div>}
        {!error && !loading && history && !history.sourceAvailable && <div className="mx-5 mt-5 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900 md:mx-7">No previous-month data was found, so historical cost assumptions are zero.</div>}

        <div className="space-y-5 p-4 md:p-6">
          <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
            <div><h2 className="font-semibold text-charcoal-500">Scenario inputs</h2><p className="mt-1 text-xs text-charcoal-400">Use sliders or enter exact values. Historical comparisons appear in scenario insights after you change an input.</p></div>
            {loading && <p className="text-sm text-slate-500">Loading previous-month values…</p>}
          </div>

          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <RangeSlider
              label="Units"
              value={overallInput.units}
              baseline={history?.units ?? 0}
              max={unitsSliderMax}
              step={1}
              format={formatNumber}
              accent="border-[#FDD36F]"
              onChange={(value) => updateOverallInput("units", value)}
            />
            <RangeSlider
              label="ASP"
              value={overallInput.asp}
              baseline={baselineAsp}
              max={aspSliderMax}
              step={0.01}
              format={(value) => formatCurrency(value, currency, 2)}
              currencyPrefix={currency}
              accent="border-[#B75A5A]"
              onChange={(value) => updateOverallInput("asp", value)}
            />
            <div className="rounded-xl border border-t-4 border-[#3A8EA4] bg-white p-4 shadow-sm">
              <label
                htmlFor="target-tacos-goal"
                className="text-sm font-medium text-slate-700"
              >
                Target TACoS
              </label>

              <div className="mt-3 flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 focus-within:border-[#5EA68E] focus-within:ring-2 focus-within:ring-[#5EA68E]/15">
                <input
                  id="target-tacos-goal"
                  type="number"
                  min={0}
                  step={0.01}
                  value={targetTacosGoal}
                  onChange={(event) => {
                    setTargetTacosGoal(event.target.value);
                    setTargetTacosChanged(true);
                    setAiError("");
                  }}
                  onBlur={() => {
                    const value = Number(targetTacosGoal);

                    if (
                      targetTacosGoal.trim() !== "" &&
                      Number.isFinite(value) &&
                      value >= 0
                    ) {
                      setTargetTacosGoal(value.toFixed(2));
                    }
                  }}
                  aria-label="Target TACoS goal"
                  className="w-full bg-transparent text-right text-sm font-semibold text-slate-900 outline-none"
                  placeholder="e.g. 25.00"
                />
                <span className="text-sm text-slate-500">%</span>
              </div>

              <p className="mt-2 text-[11px] leading-4 text-slate-500">Ads budget ÷ projected sales. Sets the ads budget for this scenario.</p>
            </div>

            <div className="rounded-xl border border-t-4 border-[#B8C78C] bg-white p-4 shadow-sm">
              <label htmlFor="additional-expense" className="text-sm font-medium text-slate-700">Additional expense</label>
              <div className="mt-3 flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 focus-within:border-[#5EA68E] focus-within:ring-2 focus-within:ring-[#5EA68E]/15">
                <span className="text-xs text-slate-500">{currency}</span>
                <input
                  id="additional-expense"
                  type="number"
                  min={0}
                  step={1}
                  value={additionalExpense}
                  onChange={(event) => {
                    const value = Number(event.target.value);
                    setAdditionalExpense(Number.isFinite(value) && value > 0 ? Math.round(value) : 0);
                    setAdditionalExpenseChanged(true);
                    setAiError("");
                  }}
                  aria-label="Additional expense"
                  className="w-full bg-transparent text-right text-sm font-semibold text-slate-900 outline-none"
                  placeholder="0"
                />
              </div>
              <p className="mt-2 text-[11px] leading-4 text-slate-500">Deducted from projected CM2.</p>
            </div>

          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Projected sales" value={formatCurrency(result.sales, currency)} hint={`${formatNumber(result.units)} units × ASP`} accent="border-[#75BBDA]" />
            <MetricCard label="Ads budget" value={formatCurrency(adsBudget, currency)} hint="Projected sales × Target TACoS" accent="border-[#C49466]" />
            <MetricCard
              label="Projected CM1"
              value={
                <span className="flex items-baseline flex-wrap gap-1.5">
                  <span>{formatCurrency(result.cm1, currency)}</span>
                  <span className="text-xs font-normal text-slate-500">
                    ({cm1Margin.toFixed(1)}%)
                  </span>
                </span>
              }
              hint="Sales − previous Amazon fees − net taxes − net credits"
              accent="border-[#7B9A6D]"
            />
            <MetricCard
              label="Projected CM2"
              value={
                <span className="flex items-baseline flex-wrap gap-1.5">
                  <span>{formatCurrency(result.cm2, currency)}</span>
                  <span className="text-xs font-normal text-slate-500">
                    ({margin.toFixed(1)}%)
                  </span>
                </span>
              }
              hint="CM1 − ads budget − additional expense"
              accent="border-[#B8C78C]"
            />
          </div>

          {scenarioInputsChanged && (
            <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
              <h2 className="font-semibold text-charcoal-500">Scenario insights</h2>
              <p className="mt-1 text-xs text-charcoal-400">How your changed inputs compare with past results and affect this projection.</p>
              {scenarioInsightsLoading
                ? <p className="mt-4 text-sm text-slate-500">Comparing this scenario with your past results…</p>
                : <ul className="mt-4 list-disc space-y-2 pl-5 text-sm leading-6 text-charcoal-500">
                  {scenarioInsightPoints.map((point, index) => <li key={`${index}-${point}`}>{point}</li>)}
                </ul>}
            </section>
          )}

          <section className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
            <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
              <div>
                <div className="flex items-center gap-2">
                  <h2 className="font-semibold text-charcoal-500">AI scenario analysis</h2>
                  <span className="rounded-full bg-[#F8EDCE] px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide text-[#37455F]">AI</span>
                </div>
                <p className="mt-1 text-xs text-charcoal-400">Get an explanation of this projection compared with the previous-month baseline.</p>
              </div>
              <button
                type="button"
                onClick={generateAiAnalysis}
                disabled={aiLoading || loading}
                className="inline-flex min-w-[170px] items-center justify-center rounded-lg bg-[#37455F] px-4 py-2.5 text-sm font-semibold text-[#F8EDCE] shadow-sm transition hover:bg-[#2F3B52] focus:outline-none focus:ring-2 focus:ring-[#5EA68E] focus:ring-offset-2 disabled:cursor-wait disabled:opacity-60"
              >
                {aiLoading ? "Analyzing scenario…" : aiInsightIsCurrent ? "Refresh AI analysis" : "Generate AI analysis"}
              </button>
            </div>
            {aiError && <p role="alert" className="mt-4 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{aiError}</p>}
            {aiLoading && <p className="mt-4 text-sm text-charcoal-500">AI is reviewing the current units, ASP, ad budget and projected CM2…</p>}
            {aiInsightIsCurrent && <p className="mt-4 whitespace-pre-line text-sm leading-6 text-charcoal-500">{aiInsight?.text}</p>}
            {!aiLoading && aiInsight && !aiInsightIsCurrent && <p className="mt-4 text-sm text-charcoal-400">Inputs changed. Generate a fresh AI analysis for this scenario.</p>}
            {!aiLoading && !aiInsight && !aiError && <p className="mt-4 text-sm text-charcoal-400">AI will explain the projected CM2 and margin movement and share up to two practical observations.</p>}
            <p className="mt-3 text-[11px] text-slate-400">CM1 and CM2 figures above come from the simulator formulas; AI only explains the results.</p>
          </section>

          {/* <section className="rounded-2xl border border-slate-200 bg-white p-5">
            <div className="flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
              <div>
                <h2 className="font-semibold text-slate-900">Scenario outlook</h2>
                <p className="mt-1 text-xs text-slate-500">Projected CM2 compared with {history?.monthLabel ?? "the previous-month baseline"}.</p>
              </div>
              <div className={`text-sm font-semibold ${cm2Change >= 0 ? "text-emerald-700" : "text-rose-600"}`}>
                {cm2Change >= 0 ? "+" : "−"}{formatCurrency(Math.abs(cm2Change), currency)} CM2 · {marginChange >= 0 ? "+" : "−"}{Math.abs(marginChange).toFixed(1)} pp margin
              </div>
            </div>
            <div className="mt-5 space-y-4">
              {[
                { label: history?.monthLabel ?? "Previous-month baseline", value: baselineProfit.cm2, color: "bg-slate-300" },
                { label: "Current scenario", value: result.cm2, color: result.cm2 >= 0 ? "bg-[#5EA68E]" : "bg-rose-400" },
              ].map((item) => (
                <div key={item.label}>
                  <div className="mb-1.5 flex justify-between gap-3 text-xs">
                    <span className="text-slate-600">{item.label}</span>
                    <strong className="text-slate-800">{formatCurrency(item.value, currency)}</strong>
                  </div>
                  <div className="h-2 overflow-hidden rounded-full bg-slate-100">
                    <div className={`h-full rounded-full ${item.color}`} style={{ width: `${Math.abs(item.value) / comparisonMax * 100}%` }} />
                  </div>
                </div>
              ))}
            </div>
          </section> */}
        </div>
      </section>
    </div>
  );
}
