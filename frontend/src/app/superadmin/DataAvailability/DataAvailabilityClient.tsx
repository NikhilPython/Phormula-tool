"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Database,
  Eye,
  RefreshCw,
  Search,
  XCircle,
} from "lucide-react";
import { toast } from "sonner";

import Loader from "@/components/loader/Loader";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

const MONTHS = [
  { value: "january", label: "January" },
  { value: "february", label: "February" },
  { value: "march", label: "March" },
  { value: "april", label: "April" },
  { value: "may", label: "May" },
  { value: "june", label: "June" },
  { value: "july", label: "July" },
  { value: "august", label: "August" },
  { value: "september", label: "September" },
  { value: "october", label: "October" },
  { value: "november", label: "November" },
  { value: "december", label: "December" },
];

const STATUS_OPTIONS = ["All", "Complete", "Missing"] as const;

type AvailabilityStatus = "Complete" | "Missing" | "Stale" | "Failed";

type AvailabilityRow = {
  user_id: number | string;
  name?: string;
  email: string;
  brand_name?: string;
  company_name?: string;
  country: string;
  country_key: string;
  marketplace_id?: string | null;
  checks: {
    sku_data: boolean;
    pnl_data: boolean;
    ads_connected: boolean;
    inventory_connected: boolean;
    currency_global_data: boolean;
  };
  counts: {
    sku_rows: number;
    pnl_rows: number;
    inventory_rows: number;
    monthwise_inventory_rows: number;
    current_inventory_rows: number;
    currency_rows: number;
    global_rows: number;
  };
  status: AvailabilityStatus;
  missing: string[];
};

type AvailabilityResponse = {
  period?: {
    month: string;
    month_number: number;
    year: number;
  };
  summary?: {
    total: number;
    complete: number;
    missing: number;
    stale: number;
    failed: number;
  };
  rows?: AvailabilityRow[];
  message?: string;
};

type IssueSummary = {
  open: number;
  resolved: number;
  critical: number;
  high: number;
};

type IssueDetectionResponse = {
  success?: boolean;
  created?: number;
  updated?: number;
  resolved?: number;
  open_count?: number;
  message?: string;
};

type IssuesResponse = {
  success?: boolean;
  summary?: IssueSummary;
  message?: string;
};

const currentMonth = () => MONTHS[new Date().getMonth()]?.value || "january";
const currentYear = () => new Date().getFullYear();

const statusStyles: Record<AvailabilityStatus, string> = {
  Complete: "border-emerald-300/25 bg-emerald-500/10 text-emerald-200",
  Missing: "border-amber-300/25 bg-amber-500/10 text-amber-100",
  Stale: "border-sky-300/25 bg-sky-500/10 text-sky-100",
  Failed: "border-red-300/25 bg-red-500/10 text-red-100",
};

const checkLabels: Array<{
  key: keyof AvailabilityRow["checks"];
  label: string;
  countKey?: keyof AvailabilityRow["counts"];
}> = [
  { key: "sku_data", label: "SKU", countKey: "sku_rows" },
  { key: "pnl_data", label: "P&L", countKey: "pnl_rows" },
  { key: "ads_connected", label: "Ads" },
  { key: "inventory_connected", label: "Inventory", countKey: "inventory_rows" },
  {
    key: "currency_global_data",
    label: "Currency/Global",
    countKey: "global_rows",
  },
];

const getVisibleMissing = (row: AvailabilityRow) =>
  (row.missing || []).filter(
    (item) => item.trim().toLowerCase() !== "transactions"
  );

const hasAllVisibleChecks = (row: AvailabilityRow) =>
  checkLabels.every((check) => row.checks[check.key]);

const getVisibleStatus = (row: AvailabilityRow): AvailabilityStatus => {
  if (hasAllVisibleChecks(row)) return "Complete";
  if (getVisibleMissing(row).length > 0) return "Missing";
  return row.status === "Failed" ? "Missing" : row.status;
};

const normalizeAvailabilityRow = (row: AvailabilityRow): AvailabilityRow => {
  const status = getVisibleStatus(row);

  return {
    ...row,
    status,
    missing: hasAllVisibleChecks(row) ? [] : getVisibleMissing(row),
  };
};

export default function DataAvailabilityClient() {
  const router = useRouter();

  const [rows, setRows] = useState<AvailabilityRow[]>([]);
  const [summary, setSummary] = useState<AvailabilityResponse["summary"]>();
  const [month, setMonth] = useState(currentMonth);
  const [year, setYear] = useState(currentYear);
  const [statusFilter, setStatusFilter] =
    useState<(typeof STATUS_OPTIONS)[number]>("All");
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [issueSummary, setIssueSummary] = useState<IssueSummary>({
    open: 0,
    resolved: 0,
    critical: 0,
    high: 0,
  });

  const fetchAvailability = useCallback(async () => {
    setLoading(true);

    try {
      const token = localStorage.getItem("superadmin_token");

      if (!token) {
        toast.error("No authentication token found");
        router.push("/superadmin/CDPAdminConsole");
        return;
      }

      const params = new URLSearchParams({
        month,
        year: String(year),
      });

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/data_availability?${params.toString()}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        }
      );

      const data = (await response.json().catch(() => ({}))) as AvailabilityResponse;

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem("superadmin_token");
          router.push("/superadmin/CDPAdminConsole");
          return;
        }

        throw new Error(data.message || "Failed to load data availability.");
      }

      setRows((data.rows || []).map(normalizeAvailabilityRow));
      setSummary(data.summary);

      try {
        const issueParams = new URLSearchParams({
          month,
          year: String(year),
        });

        const issueRunResponse = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/issue_detection/run?${issueParams.toString()}`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );

        const issueRunData = (await issueRunResponse
          .json()
          .catch(() => ({}))) as IssueDetectionResponse;

        if (!issueRunResponse.ok) {
          throw new Error(
            issueRunData.message || "Failed to run automated issue detection."
          );
        }

        const issuesResponse = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/issues?${issueParams.toString()}&status=open`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );

        const issuesData = (await issuesResponse
          .json()
          .catch(() => ({}))) as IssuesResponse;

        if (!issuesResponse.ok) {
          throw new Error(issuesData.message || "Failed to load automated issues.");
        }

        setIssueSummary(
          issuesData.summary || {
            open: issueRunData.open_count || 0,
            resolved: 0,
            critical: 0,
            high: 0,
          }
        );
      } catch (issueError) {
        const issueMessage =
          issueError instanceof Error
            ? issueError.message
            : "Automated issue detection failed.";
        toast.error(issueMessage);
        setIssueSummary({
          open: 0,
          resolved: 0,
          critical: 0,
          high: 0,
        });
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Failed to load data availability.";
      toast.error(message);
      setRows([]);
      setSummary(undefined);
      setIssueSummary({
        open: 0,
        resolved: 0,
        critical: 0,
        high: 0,
      });
    } finally {
      setLoading(false);
    }
  }, [month, router, year]);

  useEffect(() => {
    fetchAvailability();
  }, [fetchAvailability]);

  const filteredRows = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();

    return rows.filter((row) => {
      const visibleStatus = getVisibleStatus(row);
      const matchesStatus =
        statusFilter === "All" ? true : visibleStatus === statusFilter;

      const searchable = [
        row.name,
        row.email,
        row.brand_name,
        row.company_name,
        row.country,
        row.marketplace_id,
        visibleStatus,
        getVisibleMissing(row).join(", "),
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      return matchesStatus && (!query || searchable.includes(query));
    });
  }, [rows, searchQuery, statusFilter]);

  const visibleSummary = useMemo(() => {
    const counts = {
      total: rows.length,
      complete: 0,
      missing: 0,
      stale: 0,
      failed: 0,
    };

    rows.forEach((row) => {
      const status = getVisibleStatus(row).toLowerCase() as
        | "complete"
        | "missing"
        | "stale"
        | "failed";
      counts[status] += 1;
    });

    return counts;
  }, [rows]);

  const summaryCards: Array<{
    label: string;
    value: number;
    icon: typeof Database;
    className: string;
    onClick?: () => void;
  }> = [
    {
      label: "Total Checks",
      value: visibleSummary.total,
      icon: Database,
      className: "border-t-[#31d9e5]",
    },
    {
      label: "Complete",
      value: visibleSummary.complete,
      icon: CheckCircle2,
      className: "border-t-emerald-400",
    },
    {
      label: "Missing",
      value: visibleSummary.missing,
      icon: XCircle,
      className: "border-t-amber-300",
    },
    {
      label: "Open Issues",
      value: issueSummary.open,
      icon: AlertTriangle,
      className: "border-t-red-300",
      onClick: () => router.push(`/superadmin/Issues?month=${month}&year=${year}`),
    },
  ];

  return (
    <div className="w-full">
      <div className="space-y-6">
        <div className="rounded-2xl border border-white/10 bg-[#484962] px-5 py-5 text-white shadow-[0_18px_40px_rgba(20,22,45,0.25)]">
          <div className="flex items-start gap-3">
            <button
              type="button"
              onClick={() => {
                if (window.history.length > 1) {
                  router.back();
                } else {
                  router.push("/superadmin/SuperAdminDashboard");
                }
              }}
              className="mt-1 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-white/10 bg-white/[0.06] text-white/80 shadow-sm transition hover:bg-white/10 hover:text-[#31d9e5]"
              aria-label="Go back"
              title="Back"
            >
              <ArrowLeft size={17} />
            </button>

            <div className="flex flex-col leading-tight">
              <PageBreadcrumb
                pageTitle="Data Availability"
                variant="superadmin"
                align="left"
                textSize="2xl"
              />

              <p className="mt-2 text-sm text-white/60">
                Check user data readiness by country, period, source, and sync status.
              </p>
            </div>
          </div>
        </div>

        <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {summaryCards.map((card) => {
            const Icon = card.icon;

            return (
              <button
                type="button"
                key={card.label}
                onClick={card.onClick}
                className={`rounded-2xl border border-t-4 border-white/10 bg-[#484962] p-5 text-left text-white shadow-[0_18px_40px_rgba(20,22,45,0.22)] transition ${
                  card.onClick
                    ? "hover:-translate-y-0.5 hover:border-red-200/35 hover:bg-[#50516d]"
                    : ""
                } ${card.className}`}
              >
                <div className="flex items-center gap-4">
                  <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5]">
                    <Icon size={22} />
                  </span>

                  <div>
                    <p className="text-sm font-medium text-white/60">{card.label}</p>
                    <h3 className="mt-1 text-xl font-bold text-white">
                      {card.value.toLocaleString()}
                    </h3>
                  </div>
                </div>
              </button>
            );
          })}
        </section>

        <section className="space-y-4">
          <div className="flex flex-col gap-3 rounded-2xl border border-white/10 bg-[#484962] p-4 shadow-[0_18px_40px_rgba(20,22,45,0.22)] xl:flex-row xl:items-center xl:justify-between">
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3 xl:w-[560px]">
              <select
                value={month}
                onChange={(event) => setMonth(event.target.value)}
                className="h-11 rounded-xl border border-white/10 bg-white/[0.06] px-3 text-sm text-white outline-none focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
              >
                {MONTHS.map((option) => (
                  <option
                    key={option.value}
                    value={option.value}
                    className="bg-[#37384f] text-white"
                  >
                    {option.label}
                  </option>
                ))}
              </select>

              <input
                type="number"
                value={year}
                min={2020}
                max={2100}
                onChange={(event) => setYear(Number(event.target.value))}
                className="h-11 rounded-xl border border-white/10 bg-white/[0.06] px-3 text-sm text-white outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
              />

              <select
                value={statusFilter}
                onChange={(event) =>
                  setStatusFilter(event.target.value as typeof statusFilter)
                }
                className="h-11 rounded-xl border border-white/10 bg-white/[0.06] px-3 text-sm text-white outline-none focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
              >
                {STATUS_OPTIONS.map((option) => (
                  <option
                    key={option}
                    value={option}
                    className="bg-[#37384f] text-white"
                  >
                    {option}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
              <div className="relative w-full sm:w-[360px]">
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(event) => setSearchQuery(event.target.value)}
                  placeholder="Search user, brand, country or status..."
                  className="h-11 w-full rounded-xl border border-white/10 bg-white/[0.06] px-4 pr-11 text-sm text-white shadow-sm outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
                />

                <span className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]">
                  <Search size={18} />
                </span>
              </div>

              <button
                type="button"
                onClick={fetchAvailability}
                disabled={loading}
                className="inline-flex h-11 items-center justify-center gap-2 rounded-xl bg-[#31d9e5] px-4 text-sm font-semibold text-[#303247] transition hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:opacity-60"
              >
                <RefreshCw size={17} className={loading ? "animate-spin" : ""} />
                Refresh
              </button>
            </div>
          </div>

          <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#484962] text-white shadow-[0_18px_40px_rgba(20,22,45,0.22)]">
            {loading ? (
              <Loader fullscreen backgroundClass="bg-[#37384f]/80" />
            ) : (
              <div className="max-w-full overflow-x-auto">
                <table className="min-w-[1260px] w-full border-collapse">
                  <thead className="border-b border-white/10 bg-white/[0.04]">
                    <tr>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        User
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Country
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Marketplace
                      </th>
                      {checkLabels.map((check) => (
                        <th
                          key={check.key}
                          className="px-4 py-4 text-center text-xs font-semibold uppercase tracking-wide text-white/55"
                        >
                          {check.label}
                        </th>
                      ))}
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Status
                      </th>
                      <th className="px-5 py-4 text-center text-xs font-semibold uppercase tracking-wide text-white/55">
                        Action
                      </th>
                    </tr>
                  </thead>

                  <tbody className="divide-y divide-white/10">
                    {filteredRows.length > 0 ? (
                      filteredRows.map((row) => {
                        const visibleStatus = getVisibleStatus(row);
                        const visibleMissing = getVisibleMissing(row);

                        return (
                          <tr
                            key={`${row.user_id}-${row.country_key}-${row.marketplace_id || "none"}`}
                            className="transition hover:bg-white/[0.04]"
                          >
                          <td className="px-5 py-4">
                            <div className="max-w-[260px]">
                              <p className="font-semibold text-white">
                                {row.name || "Not added"}
                              </p>
                              <p className="mt-1 text-xs text-white/55">
                                {row.brand_name || "Brand not added"} ·{" "}
                                {row.company_name || "Company not added"}
                              </p>
                              <p className="mt-1 break-all text-xs text-white/45">
                                {row.email}
                              </p>
                            </div>
                          </td>

                          <td className="px-5 py-4">
                            <span className="inline-flex rounded-full border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-3 py-1 text-xs font-semibold text-[#31d9e5]">
                              {row.country || "-"}
                            </span>
                          </td>

                          <td className="px-5 py-4 text-sm text-white/65">
                            {row.marketplace_id || "-"}
                          </td>

                          {checkLabels.map((check) => {
                            const ok = row.checks[check.key];

                            return (
                              <td
                                key={check.key}
                                className="px-4 py-4 text-center"
                              >
                                <span
                                  className={`inline-flex min-w-[74px] items-center justify-center rounded-full border px-3 py-1 text-xs font-semibold ${
                                    ok
                                      ? "border-emerald-300/25 bg-emerald-500/10 text-emerald-200"
                                      : "border-red-300/25 bg-red-500/10 text-red-100"
                                  }`}
                                >
                                  {ok ? "Yes" : "No"}
                                </span>
                              </td>
                            );
                          })}

                          <td className="px-5 py-4">
                            <div className="max-w-[220px]">
                              <span
                                className={`inline-flex rounded-full border px-3 py-1 text-xs font-semibold ${statusStyles[visibleStatus]}`}
                              >
                                {visibleStatus}
                              </span>

                              {visibleMissing.length > 0 && !hasAllVisibleChecks(row) && (
                                <p className="mt-2 text-xs leading-5 text-white/45">
                                  {visibleMissing.join(", ")}
                                </p>
                              )}
                            </div>
                          </td>

                          <td className="px-5 py-4 text-center">
                            <button
                              type="button"
                              onClick={() =>
                                router.push(
                                  `/superadmin/ViewUserPage/${encodeURIComponent(row.email)}`
                                )
                              }
                              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 shadow-sm transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5]"
                              title="View user"
                              aria-label={`View ${row.email}`}
                            >
                              <Eye size={17} />
                            </button>
                          </td>
                          </tr>
                        );
                      })
                    ) : (
                      <tr>
                        <td
                          colSpan={10}
                          className="px-5 py-16 text-center text-sm text-white/60"
                        >
                          No availability rows found for the selected filters.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
