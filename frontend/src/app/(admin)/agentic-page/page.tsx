"use client";

import { useMemo, useState } from "react";

type EventForm = {
  name: string;
  month: number;
  year: number;
};

type SKUPlan = {
  sku: string;
  product_name: string;
  base_forecast: string[];
  uplift_percentage: string[];
  event_forecast: string[];
  history_story: string[];
  current_status: string[];
  actions: string[];
  pricing_decision: string[];
  range_impact_overview: string[];
  recommended_price_range: string[];
  profitability_guardrail: string[];
  target_sales_note: string[];
  summary: string[];
};

type ApiResponse = {
  result: SKUPlan[];
  status: string;
  total_skus: number;
  user_id: number;
};

const months = [
  { label: "January", value: 1 },
  { label: "February", value: 2 },
  { label: "March", value: 3 },
  { label: "April", value: 4 },
  { label: "May", value: 5 },
  { label: "June", value: 6 },
  { label: "July", value: 7 },
  { label: "August", value: 8 },
  { label: "September", value: 9 },
  { label: "October", value: 10 },
  { label: "November", value: 11 },
  { label: "December", value: 12 },
];

function monthName(month: number) {
  return months.find((m) => m.value === month)?.label ?? `Month ${month}`;
}

function getSectionTone(title: string) {
  if (title === "Actions") return "border-red-200 bg-red-50";
  if (title === "Pricing Decision") return "border-purple-200 bg-purple-50";
  if (title === "Summary") return "border-blue-200 bg-blue-50";
  if (title === "Current Status") return "border-amber-200 bg-amber-50";
  return "border-gray-200 bg-gray-50";
}

function detectPriceText(lines: string[]) {
  const combined = lines.join(" ");
  const match = combined.match(/£\d+(\.\d+)?\s*(?:to|–|-)\s*£\d+(\.\d+)?/);
  return match?.[0] ?? null;
}

export default function EventPlanPage() {
  const [lastEvent, setLastEvent] = useState<EventForm>({
    name: "Black Friday",
    month: 12,
    year: 2025,
  });

  const [futureEvent, setFutureEvent] = useState<EventForm>({
    name: "Black Friday",
    month: 5,
    year: 2026,
  });

  const [targetSales, setTargetSales] = useState<number>(15000);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [apiData, setApiData] = useState<ApiResponse | null>(null);
  const [search, setSearch] = useState("");

  const requestBody = useMemo(
    () => ({
      last_event: lastEvent,
      future_event: futureEvent,
      target_sales: targetSales,
    }),
    [lastEvent, futureEvent, targetSales]
  );

  const handleSubmit = async () => {
    setLoading(true);
    setError("");

    try {
      const token = localStorage.getItem("jwtToken");

      if (!token) {
        setError("No token found. Please login first.");
        return;
      }

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000"}/api/event-plan`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify(requestBody),
        }
      );

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data?.message || "Failed to fetch event plan.");
      }

      setApiData(data);
    } catch (err: any) {
      setError(err?.message || "Something went wrong.");
    } finally {
      setLoading(false);
    }
  };

  const filteredResults = useMemo(() => {
    const items = apiData?.result ?? [];
    if (!search.trim()) return items;

    const q = search.toLowerCase();
    return items.filter(
      (item) =>
        item.product_name?.toLowerCase().includes(q) ||
        item.sku?.toLowerCase().includes(q)
    );
  }, [apiData, search]);

  const inputClass =
    "w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm text-gray-900 outline-none transition focus:border-black focus:ring-4 focus:ring-black/5";

  return (
    <div className="min-h-screen bg-[#f6f7fb]">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <div className="mb-6 rounded-[28px] border border-gray-200 bg-white p-6 shadow-sm">
          <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <h1 className="text-3xl font-semibold tracking-tight text-gray-900">
                Event Plan SKU Report
              </h1>
              <p className="mt-2 text-sm text-gray-600">
                Each SKU is shown as a full planning story: forecast, uplift, pricing,
                profitability and actions.
              </p>
            </div>

            <div className="rounded-2xl bg-gray-900 px-4 py-3 text-white">
              <p className="text-xs uppercase tracking-wide text-gray-300">
                Target Sales
              </p>
              <p className="mt-1 text-2xl font-semibold">£{targetSales}</p>
            </div>
          </div>

          <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div>
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Last Event Name
              </label>
              <input
                type="text"
                className={inputClass}
                value={lastEvent.name}
                onChange={(e) => setLastEvent({ ...lastEvent, name: e.target.value })}
                placeholder="Enter last event name"
              />
            </div>

            <div>
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Last Event Month
              </label>
              <select
                className={inputClass}
                value={lastEvent.month}
                onChange={(e) =>
                  setLastEvent({ ...lastEvent, month: Number(e.target.value) })
                }
              >
                {months.map((month) => (
                  <option key={month.value} value={month.value}>
                    {month.label}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Last Event Year
              </label>
              <input
                type="number"
                className={inputClass}
                value={lastEvent.year}
                onChange={(e) =>
                  setLastEvent({ ...lastEvent, year: Number(e.target.value) })
                }
              />
            </div>

            <div>
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Future Event Name
              </label>
              <input
                type="text"
                className={inputClass}
                value={futureEvent.name}
                onChange={(e) =>
                  setFutureEvent({ ...futureEvent, name: e.target.value })
                }
                placeholder="Enter future event name"
              />
            </div>

            <div>
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Future Event Month
              </label>
              <select
                className={inputClass}
                value={futureEvent.month}
                onChange={(e) =>
                  setFutureEvent({ ...futureEvent, month: Number(e.target.value) })
                }
              >
                {months.map((month) => (
                  <option key={month.value} value={month.value}>
                    {month.label}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Future Event Year
              </label>
              <input
                type="number"
                className={inputClass}
                value={futureEvent.year}
                onChange={(e) =>
                  setFutureEvent({ ...futureEvent, year: Number(e.target.value) })
                }
              />
            </div>

            <div className="xl:col-span-2">
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Target Sales
              </label>
              <div className="relative">
                <span className="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2 text-gray-500">
                  £
                </span>
                <input
                  type="number"
                  className="w-full rounded-2xl border border-gray-200 bg-white py-3 pl-8 pr-4 text-sm text-gray-900 outline-none transition focus:border-black focus:ring-4 focus:ring-black/5"
                  value={targetSales}
                  onChange={(e) => setTargetSales(Number(e.target.value))}
                />
              </div>
            </div>
          </div>

          <div className="mt-5 flex flex-wrap items-center gap-3">
            <button
              onClick={handleSubmit}
              disabled={loading}
              className="rounded-2xl bg-black px-5 py-3 text-sm font-medium text-white transition hover:bg-gray-800 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {loading ? "Generating..." : "Generate Plan"}
            </button>

            <div className="text-xs text-gray-500">
              JWT token sent in Authorization header
            </div>
          </div>

          {error && (
            <div className="mt-4 rounded-2xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {error}
            </div>
          )}
        </div>

        <div className="mb-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <div className="rounded-[28px] border border-gray-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-gray-500">Status</p>
            <p className="mt-2 text-3xl font-semibold text-gray-900">
              {apiData?.status || "-"}
            </p>
          </div>

          <div className="rounded-[28px] border border-gray-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-gray-500">Total SKUs</p>
            <p className="mt-2 text-3xl font-semibold text-gray-900">
              {apiData?.total_skus || 0}
            </p>
          </div>

          <div className="rounded-[28px] border border-gray-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-gray-500">Visible SKUs</p>
            <p className="mt-2 text-3xl font-semibold text-gray-900">
              {filteredResults.length}
            </p>
          </div>

          <div className="rounded-[28px] border border-gray-200 bg-white p-5 shadow-sm">
            <p className="text-sm text-gray-500">User ID</p>
            <p className="mt-2 text-3xl font-semibold text-gray-900">
              {apiData?.user_id || "-"}
            </p>
          </div>
        </div>

        <div className="mb-6 rounded-[28px] border border-gray-200 bg-white p-4 shadow-sm">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <h2 className="text-lg font-semibold text-gray-900">SKU Stories</h2>
              <p className="text-sm text-gray-500">
                The page follows the exact section order from the new API response. :contentReference[oaicite:1]
              </p>
            </div>

            <input
              type="text"
              placeholder="Search by product name or SKU..."
              className="w-full rounded-2xl border border-gray-200 bg-white px-4 py-3 text-sm outline-none focus:border-black focus:ring-4 focus:ring-black/5 lg:max-w-sm"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
        </div>

        <div className="space-y-8">
          {filteredResults.map((item, index) => {
            const recommendedRange = detectPriceText(item.recommended_price_range);
            const pricingRange = detectPriceText(item.pricing_decision);

            return (
              <article
                key={item.sku}
                className="overflow-hidden rounded-[32px] border border-gray-200 bg-white shadow-sm"
              >
                <div className="border-b border-gray-200 bg-gradient-to-r from-gray-950 to-gray-800 px-6 py-6 text-white">
                  <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                      <div className="mb-2 inline-flex rounded-full bg-white/10 px-3 py-1 text-xs font-medium uppercase tracking-wide text-white/90">
                        SKU #{index + 1}
                      </div>
                      <h2 className="text-2xl font-semibold">{item.product_name}</h2>
                      <p className="mt-1 text-sm text-gray-300">SKU: {item.sku}</p>
                    </div>

                    <div className="grid gap-3 sm:grid-cols-2">
                      <div className="rounded-2xl bg-white/10 px-4 py-3 backdrop-blur">
                        <p className="text-xs uppercase tracking-wide text-gray-300">
                          Recommended Price
                        </p>
                        <p className="mt-1 text-base font-semibold text-white">
                          {recommendedRange || pricingRange || "See pricing section"}
                        </p>
                      </div>

                      <div className="rounded-2xl bg-white/10 px-4 py-3 backdrop-blur">
                        <p className="text-xs uppercase tracking-wide text-gray-300">
                          Future Event
                        </p>
                        <p className="mt-1 text-base font-semibold text-white">
                          {futureEvent.name} · {monthName(futureEvent.month)} {futureEvent.year}
                        </p>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="grid gap-4 p-6 lg:grid-cols-3">
                  <SectionCard title="Base Forecast" items={item.base_forecast} />
                  <SectionCard title="Uplift Percentage" items={item.uplift_percentage} />
                  <SectionCard title="Event Forecast" items={item.event_forecast} />

                  <SectionCard title="History Story" items={item.history_story} className="lg:col-span-2" />
                  <SectionCard title="Current Status" items={item.current_status} />

                  <SectionCard title="Actions" items={item.actions} className="lg:col-span-2" />
                  <SectionCard title="Pricing Decision" items={item.pricing_decision} />

                  <SectionCard
                    title="Range Impact Overview"
                    items={item.range_impact_overview}
                    className="lg:col-span-2"
                  />
                  <SectionCard title="Recommended Price Range" items={item.recommended_price_range} />

                  <SectionCard
                    title="Profitability Guardrail"
                    items={item.profitability_guardrail}
                  />
                  <SectionCard
                    title="Target Sales Note"
                    items={item.target_sales_note}
                  />
                  <SectionCard title="Summary" items={item.summary} />
                </div>
              </article>
            );
          })}
        </div>

        {apiData && filteredResults.length === 0 && (
          <div className="mt-6 rounded-[28px] border border-gray-200 bg-white p-10 text-center shadow-sm">
            <p className="text-sm text-gray-500">No SKU matched your search.</p>
          </div>
        )}
      </div>
    </div>
  );
}

function SectionCard({
  title,
  items,
  className = "",
}: {
  title: string;
  items: string[];
  className?: string;
}) {
  return (
    <section
      className={`rounded-[24px] border p-5 ${getSectionTone(title)} ${className}`}
    >
      <h3 className="text-sm font-semibold uppercase tracking-wide text-gray-700">
        {title}
      </h3>

      <div className="mt-4 space-y-2">
        {items.map((item, index) => (
          <div
            key={`${title}-${index}`}
            className="rounded-2xl border border-white/70 bg-white/70 px-4 py-3 text-sm leading-6 text-gray-800"
          >
            {item}
          </div>
        ))}
      </div>
    </section>
  );
}