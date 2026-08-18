"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Eye,
  RefreshCw,
  Search,
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

const STATUS_OPTIONS = ["open", "resolved", "all"] as const;

type IssueStatus = (typeof STATUS_OPTIONS)[number];

type Issue = {
  id: number;
  user_id: number;
  email?: string;
  name?: string;
  brand_name?: string;
  company_name?: string;
  country?: string;
  marketplace_id?: string | null;
  issue_type: string;
  severity: "critical" | "high" | "medium" | "low" | string;
  status: "open" | "resolved" | string;
  title: string;
  description?: string;
  evidence?: Record<string, unknown>;
  occurrences?: number;
  last_seen_at?: string | null;
  resolved_at?: string | null;
  period?: {
    month?: string;
    year?: number;
  };
};

type IssueSummary = {
  open: number;
  resolved: number;
  critical: number;
  high: number;
};

type IssuesResponse = {
  success?: boolean;
  summary?: IssueSummary;
  issues?: Issue[];
  message?: string;
};

const currentMonth = () => MONTHS[new Date().getMonth()]?.value || "january";
const currentYear = () => new Date().getFullYear();

const severityStyles: Record<string, string> = {
  critical: "border-red-300/25 bg-red-500/10 text-red-100",
  high: "border-orange-300/25 bg-orange-500/10 text-orange-100",
  medium: "border-amber-300/25 bg-amber-500/10 text-amber-100",
  low: "border-sky-300/25 bg-sky-500/10 text-sky-100",
};

const statusStyles: Record<string, string> = {
  open: "border-red-300/25 bg-red-500/10 text-red-100",
  resolved: "border-emerald-300/25 bg-emerald-500/10 text-emerald-100",
};

const prettify = (value?: string | null) =>
  String(value || "")
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());

const formatDate = (value?: string | null) => {
  if (!value) return "-";

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "-";

  return parsed.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
};

export default function IssuesClient() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [issues, setIssues] = useState<Issue[]>([]);
  const [summary, setSummary] = useState<IssueSummary>({
    open: 0,
    resolved: 0,
    critical: 0,
    high: 0,
  });
  const [month, setMonth] = useState(
    searchParams.get("month") || currentMonth()
  );
  const [year, setYear] = useState(
    Number(searchParams.get("year")) || currentYear()
  );
  const [status, setStatus] = useState<IssueStatus>("open");
  const [searchQuery, setSearchQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const fetchIssues = useCallback(async () => {
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
        status,
      });

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/issues?${params.toString()}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        }
      );

      const data = (await response.json().catch(() => ({}))) as IssuesResponse;

      if (!response.ok) {
        if (response.status === 401) {
          localStorage.removeItem("superadmin_token");
          router.push("/superadmin/CDPAdminConsole");
          return;
        }

        throw new Error(data.message || "Failed to load issues.");
      }

      setIssues(data.issues || []);
      setSummary(
        data.summary || {
          open: 0,
          resolved: 0,
          critical: 0,
          high: 0,
        }
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load issues.";
      toast.error(message);
      setIssues([]);
    } finally {
      setLoading(false);
    }
  }, [month, router, status, year]);

  const runDetection = useCallback(async () => {
    setRefreshing(true);

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
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/superadmin/issue_detection/run?${params.toString()}`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        }
      );

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(data.message || "Failed to refresh issue detection.");
      }

      toast.success("Issue detection refreshed.");
      await fetchIssues();
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Failed to refresh issue detection.";
      toast.error(message);
    } finally {
      setRefreshing(false);
    }
  }, [fetchIssues, month, router, year]);

  useEffect(() => {
    fetchIssues();
  }, [fetchIssues]);

  const filteredIssues = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();

    if (!query) return issues;

    return issues.filter((issue) =>
      [
        issue.name,
        issue.email,
        issue.brand_name,
        issue.company_name,
        issue.country,
        issue.marketplace_id,
        issue.title,
        issue.description,
        prettify(issue.issue_type),
        prettify(issue.severity),
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(query)
    );
  }, [issues, searchQuery]);

  const summaryCards = [
    {
      label: "Open",
      value: summary.open,
      icon: AlertTriangle,
      className: "border-t-red-300",
    },
    {
      label: "Critical",
      value: summary.critical,
      icon: AlertTriangle,
      className: "border-t-red-400",
    },
    {
      label: "High",
      value: summary.high,
      icon: AlertTriangle,
      className: "border-t-orange-300",
    },
    {
      label: "Resolved",
      value: summary.resolved,
      icon: CheckCircle2,
      className: "border-t-emerald-400",
    },
  ];

  return (
    <div className="w-full">
      <div className="space-y-6">
        <div className="rounded-2xl border border-white/10 bg-[#484962] px-5 py-5 text-white shadow-[0_18px_40px_rgba(20,22,45,0.25)]">
          <div className="flex items-start gap-3">
            <button
              type="button"
              onClick={() => router.push("/superadmin/DataAvailability")}
              className="mt-1 inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-white/10 bg-white/[0.06] text-white/80 shadow-sm transition hover:bg-white/10 hover:text-[#31d9e5]"
              aria-label="Go back"
              title="Back"
            >
              <ArrowLeft size={17} />
            </button>

            <div className="flex flex-col leading-tight">
              <PageBreadcrumb
                pageTitle="Issues"
                variant="superadmin"
                align="left"
                textSize="2xl"
              />

              <p className="mt-2 text-sm text-white/60">
                Review automated issue detection results by user, country, and period.
              </p>
            </div>
          </div>
        </div>

        <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {summaryCards.map((card) => {
            const Icon = card.icon;

            return (
              <div
                key={card.label}
                className={`rounded-2xl border border-t-4 border-white/10 bg-[#484962] p-5 text-white shadow-[0_18px_40px_rgba(20,22,45,0.22)] ${card.className}`}
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
              </div>
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
                value={status}
                onChange={(event) => setStatus(event.target.value as IssueStatus)}
                className="h-11 rounded-xl border border-white/10 bg-white/[0.06] px-3 text-sm text-white outline-none focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
              >
                {STATUS_OPTIONS.map((option) => (
                  <option
                    key={option}
                    value={option}
                    className="bg-[#37384f] text-white"
                  >
                    {prettify(option)}
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
                  placeholder="Search user, brand, country or issue..."
                  className="h-11 w-full rounded-xl border border-white/10 bg-white/[0.06] px-4 pr-11 text-sm text-white shadow-sm outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-4 focus:ring-[#31d9e5]/15"
                />

                <span className="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-[#31d9e5]">
                  <Search size={18} />
                </span>
              </div>

              <button
                type="button"
                onClick={runDetection}
                disabled={refreshing || loading}
                className="inline-flex h-11 items-center justify-center gap-2 rounded-xl bg-[#31d9e5] px-4 text-sm font-semibold text-[#303247] transition hover:bg-[#28cbd6] disabled:cursor-not-allowed disabled:opacity-60"
              >
                <RefreshCw size={17} className={refreshing ? "animate-spin" : ""} />
                Refresh
              </button>
            </div>
          </div>

          <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#484962] text-white shadow-[0_18px_40px_rgba(20,22,45,0.22)]">
            {loading ? (
              <Loader fullscreen backgroundClass="bg-[#37384f]/80" />
            ) : (
              <div className="max-w-full overflow-x-auto">
                <table className="min-w-[1320px] w-full border-collapse">
                  <thead className="border-b border-white/10 bg-white/[0.04]">
                    <tr>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        User
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Issue
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Country
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Marketplace
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Severity
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Status
                      </th>
                      <th className="px-5 py-4 text-left text-xs font-semibold uppercase tracking-wide text-white/55">
                        Last Seen
                      </th>
                      <th className="px-5 py-4 text-center text-xs font-semibold uppercase tracking-wide text-white/55">
                        Action
                      </th>
                    </tr>
                  </thead>

                  <tbody className="divide-y divide-white/10">
                    {filteredIssues.length > 0 ? (
                      filteredIssues.map((issue) => (
                        <tr key={issue.id} className="transition hover:bg-white/[0.04]">
                          <td className="px-5 py-4">
                            <div className="max-w-[250px]">
                              <p className="font-semibold text-white">
                                {issue.name || "Not added"}
                              </p>
                              <p className="mt-1 text-xs text-white/55">
                                {issue.brand_name || "Brand not added"} -{" "}
                                {issue.company_name || "Company not added"}
                              </p>
                              <p className="mt-1 break-all text-xs text-white/45">
                                {issue.email || "-"}
                              </p>
                            </div>
                          </td>

                          <td className="px-5 py-4">
                            <div className="max-w-[360px]">
                              <p className="font-semibold text-white">{issue.title}</p>
                              <p className="mt-1 text-xs text-white/50">
                                {prettify(issue.issue_type)}
                              </p>
                              {issue.description && (
                                <p className="mt-2 text-xs leading-5 text-white/45">
                                  {issue.description}
                                </p>
                              )}
                            </div>
                          </td>

                          <td className="px-5 py-4">
                            <span className="inline-flex rounded-full border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-3 py-1 text-xs font-semibold text-[#31d9e5]">
                              {issue.country || "-"}
                            </span>
                          </td>

                          <td className="px-5 py-4 text-sm text-white/65">
                            {issue.marketplace_id || "-"}
                          </td>

                          <td className="px-5 py-4">
                            <span
                              className={`inline-flex rounded-full border px-3 py-1 text-xs font-semibold ${
                                severityStyles[issue.severity] ||
                                "border-white/15 bg-white/[0.06] text-white/75"
                              }`}
                            >
                              {prettify(issue.severity)}
                            </span>
                          </td>

                          <td className="px-5 py-4">
                            <span
                              className={`inline-flex rounded-full border px-3 py-1 text-xs font-semibold ${
                                statusStyles[issue.status] ||
                                "border-white/15 bg-white/[0.06] text-white/75"
                              }`}
                            >
                              {prettify(issue.status)}
                            </span>
                          </td>

                          <td className="px-5 py-4 text-sm text-white/60">
                            {formatDate(issue.last_seen_at)}
                          </td>

                          <td className="px-5 py-4 text-center">
                            <button
                              type="button"
                              onClick={() => {
                                if (issue.email) {
                                  router.push(
                                    `/superadmin/ViewUserPage/${encodeURIComponent(
                                      issue.email
                                    )}`
                                  );
                                }
                              }}
                              disabled={!issue.email}
                              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] text-white/75 shadow-sm transition hover:border-[#31d9e5]/40 hover:bg-[#31d9e5]/10 hover:text-[#31d9e5] disabled:cursor-not-allowed disabled:opacity-50"
                              title="View user"
                              aria-label={`View ${issue.email || "user"}`}
                            >
                              <Eye size={17} />
                            </button>
                          </td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td
                          colSpan={8}
                          className="px-5 py-16 text-center text-sm text-white/60"
                        >
                          No issues found for the selected filters.
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
