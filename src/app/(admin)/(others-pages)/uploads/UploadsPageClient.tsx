"use client";

import * as React from "react";
import Image from "next/image";
import { useRouter } from "next/navigation";
import { useGetUploadHistoryQuery, type UploadItem } from "@/lib/api/feePreviewApi";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import ComponentCard from "@/components/common/ComponentCard"; // ✅ use your card
import Button from "@/components/ui/button/Button";

const monthMap: Record<string, number> = {
  january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
  july: 6, august: 7, september: 8, october: 9, november: 10, december: 11,
};

const countryFlags: Record<string, string> = {
  us: "/usFlag.png",
  uk: "/ukFlag.png",
  canada: "/canadaflag.png",
  global: "/Global.png",
};

type Grouped = Record<string, Record<string, UploadItem[]>>; // country -> quarter -> uploads

export default function UploadsPage() {
  const router = useRouter();
  const { data, isLoading, isError } = useGetUploadHistoryQuery();

  const [expandedCountry, setExpandedCountry] = React.useState<string | null>(null);
  const [expandedQuarter, setExpandedQuarter] = React.useState<Record<string, boolean>>({});

  const uploads = data?.uploads ?? [];

  const sorted = React.useMemo(() => {
    return [...uploads].sort((a, b) => {
      const da = new Date(a.year, monthMap[a.month.toLowerCase()] ?? 0).getTime();
      const db = new Date(b.year, monthMap[b.month.toLowerCase()] ?? 0).getTime();
      return db - da;
    });
  }, [uploads]);

  const grouped: Grouped = React.useMemo(() => {
    const out: Grouped = {};
    for (const u of sorted) {
      const c = (u.country ?? "").toLowerCase();
      if (!c) continue;
      const m = (u.month ?? "").toLowerCase();
      const monthIdx = monthMap[m] ?? 0;
      const q = Math.floor(monthIdx / 3) + 1;
      const key = `Q${q}_FY ${u.year}`;
      out[c] ??= {};
      out[c][key] ??= [];
      out[c][key].push(u);
    }
    return out;
  }, [sorted]);

  const toggleCountry = (country: string) =>
    setExpandedCountry((prev) => (prev === country ? null : country));

  const toggleQuarter = (country: string, quarter: string) => {
    const k = `${country}-${quarter}`;
    setExpandedQuarter((prev) => ({ ...prev, [k]: !prev[k] }));
  };

  const handleViewPerformance = (upload: UploadItem) => {
    const { month, year, country } = upload;
    router.push(`/pnl-dashboard/MTD/${country}/${month}/${year}`);
  };

  const EmptyState = () => (
    <div className="rounded-xl border border-dashed border-gray-300 bg-white p-10 text-center">
      <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-emerald-50 ring-1 ring-emerald-200">
        <span className="text-green-500">↑</span>
      </div>
      <h3 className="text-base font-semibold text-[var(--color-charcoal-500)]">No uploads yet</h3>
      <p className="mt-1 text-sm text-gray-500">
        When you upload MTD files, they’ll appear here grouped by country and quarter.
      </p>
      <div className="mt-4">
        <Button variant="primary" size="sm" onClick={() => router.push("/uploads/new")}>
          Upload a file
        </Button>
      </div>
    </div>
  );

  const Skeleton = () => (
    <div className="space-y-4">
      {Array.from({ length: 3 }).map((_, i) => (
        <div key={i} className="animate-pulse rounded-xl border border-gray-200 bg-white p-4">
          <div className="h-4 w-40 rounded bg-gray-200" />
          <div className="mt-3 h-3 w-full rounded bg-gray-100" />
          <div className="mt-2 h-3 w-4/5 rounded bg-gray-100" />
        </div>
      ))}
    </div>
  );

  return (
    <div className="w-full">
      {/* Page title (32px) */}
      <PageBreadcrumb pageTitle="Your Upload History" variant="page" align="left" />

      <ComponentCard
        title="Recently Uploaded Files"
        desc="Grouped by Country → Quarter. Click a country to expand, then a quarter to see its files."
        className="mt-4"
      >
        {/* Meta row */}
        <div className="mb-2 flex flex-wrap items-center justify-between gap-3">
          <div className="text-sm text-gray-600">
            {isLoading
              ? "Loading upload history…"
              : isError
              ? "Failed to load upload history."
              : `Total uploads: ${uploads.length}`}
          </div>
          {!isLoading && !isError && uploads.length > 0 && (
            <div className="text-xs text-gray-500">
              Using theme colors — blue for actions, green for counts.
            </div>
          )}
        </div>

        {/* States */}
        {isLoading && <Skeleton />}
        {!isLoading && !isError && uploads.length === 0 && <EmptyState />}

        {/* Content */}
        {!isLoading && !isError && uploads.length > 0 && (
          <div className="space-y-5">
            {Object.entries(grouped).map(([country, quarters]) => {
              if (country === "global") return null;
              const isOpen = expandedCountry === country;
              const totalForCountry = Object.values(quarters).reduce((acc, arr) => acc + arr.length, 0);

              return (
                <div
                  key={country}
                  className="rounded-xl border border-gray-200 bg-white shadow-sm transition hover:shadow-md"
                >
                  {/* Country header */}
                  <button
                    type="button"
                    onClick={() => toggleCountry(country)}
                    className="flex w-full items-center justify-between rounded-t-xl px-4 py-3"
                  >
                    <span className="flex items-center gap-3">
                      <span className="relative h-5 w-7 overflow-hidden rounded-sm ring-1 ring-gray-200">
                        <Image
                          src={countryFlags[country] ?? "/defaultFlag.png"}
                          alt={`${country} flag`}
                          fill
                          className="object-cover"
                        />
                      </span>
                      <span className="text-sm font-semibold tracking-wide text-[var(--color-charcoal-500)]">
                        {country.toUpperCase()}
                      </span>
                      <span className="rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-medium text-green-600 ring-1 ring-emerald-100">
                        {totalForCountry}
                      </span>
                    </span>
                    <i
                      className={`fa-solid ${isOpen ? "fa-chevron-up" : "fa-chevron-down"} text-gray-500`}
                    />
                  </button>

                  {/* Divider */}
                  <div className="h-px w-full bg-gray-100" />

                  {/* Quarters */}
                  {isOpen && (
                    <div className="p-3">
                      <div className="space-y-4">
                        {Object.entries(quarters).map(([quarter, quarterUploads]) => {
                          const key = `${country}-${quarter}`;
                          const openQ = !!expandedQuarter[key];

                          return (
                            <div key={quarter} className="rounded-lg border border-gray-200 bg-white">
                              {/* Quarter header */}
                              <button
                                type="button"
                                onClick={() => toggleQuarter(country, quarter)}
                                className="flex w-full items-center justify-between rounded-lg px-3 py-2 hover:bg-gray-50"
                              >
                                <span className="inline-flex items-center gap-2 text-sm font-semibold text-[var(--color-charcoal-500)]">
                                  <i className="fa-solid fa-calendar" style={{ color: "#5ea68e" }} />
                                  {quarter}
                                  <span className="rounded-full bg-emerald-50 px-1.5 py-0.5 text-[10px] font-medium text-green-600 ring-1 ring-emerald-100">
                                    {quarterUploads.length}
                                  </span>
                                </span>
                                <i
                                  className={`fa-solid ${openQ ? "fa-chevron-up" : "fa-chevron-down"} text-gray-500`}
                                />
                              </button>

                              {/* Upload rows */}
                              {openQ && (
                                <div className="divide-y divide-gray-200">
                                  {quarterUploads.map((u) => (
                                    <div
                                      key={u.id}
                                      className="flex flex-col items-start gap-3 px-3 py-3 md:flex-row md:items-center md:justify-between"
                                    >
                                      <div className="min-w-0">
                                        <div className="truncate text-sm font-semibold text-neutral-900">
                                          MTD_{u.month.charAt(0).toUpperCase() + u.month.slice(1).substring(0, 2)}
                                          &apos;{String(u.year).slice(-2)} — {u.country.toUpperCase()}
                                        </div>
                                        <div className="text-xs text-gray-600">
                                          Country: {u.country.toUpperCase()} &middot; Month: {u.month} &middot; Year: {u.year}
                                        </div>
                                      </div>

                                      <div className="flex shrink-0 items-center gap-2">
                                        <Button
                                          variant="primary"
                                          size="sm"
                                          onClick={() => handleViewPerformance(u)}
                                        >
                                          View Monthly Performance
                                        </Button>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </ComponentCard>
    </div>
  );
}
