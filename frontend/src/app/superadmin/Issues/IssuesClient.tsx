"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Eye,
  Search,
} from "lucide-react";
import { toast } from "sonner";

import Loader from "@/components/loader/Loader";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SuperAdminUsersTable from "@/components/admin/table/SuperAdminUsersTable";
import SummaryMetricCard from "@/components/dropdowns/SummaryMetricCard";

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
  const [status, setStatus] = useState<IssueStatus>("all");
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
      label: "Open Issues",
      value: summary.open,
      icon: AlertTriangle,
      detail: "Currently unresolved issues",
    },
    {
      label: "Critical",
      value: summary.critical,
      icon: AlertTriangle,
      detail: "Critical issues requiring attention",
    },
    {
      label: "High Priority",
      value: summary.high,
      icon: AlertTriangle,
      detail: "High severity issues",
    },
    {
      label: "Resolved",
      value: summary.resolved,
      icon: CheckCircle2,
      detail: "Issues resolved successfully",
    },
  ];

  const issueColumns = [
    {
      key: "serial_no",
      label: "S.No.",
      render: (issue: Issue) => {
        const index = filteredIssues.findIndex(
          (item) => item.id === issue.id
        );

        return (
          <span className="font-medium text-white/65">
            {index >= 0 ? index + 1 : "-"}
          </span>
        );
      },
    },

    {
      key: "brand",
      label: "Brand",
      cellClassName: "font-semibold text-white",
      render: (issue: Issue) =>
        issue.brand_name || "Not added",
    },

    {
      key: "admin",
      label: "Admin Name",
      render: (issue: Issue) => (
        <div className="min-w-[150px]">
          <p className="font-medium text-white/75">
            {issue.name || "Not added"}
          </p>

          {/* <p className="mt-1 break-all text-[10px] text-white/40 2xl:text-xs">
            {issue.email || "-"}
          </p> */}
        </div>
      ),
    },

    {
      key: "issue",
      label: "Issue",
      render: (issue: Issue) => (
        <div className="max-w-[220px] text-center">
          <p className="font-medium text-white/80">
            {/* {issue.title} */}
            {prettify(issue.issue_type)}
          </p>

          {/* <p className="mt-1 text-[10px] text-white/45 2xl:text-xs">
            {prettify(issue.issue_type)}
          </p> */}
        </div>
      ),
    },

    {
      key: "description",
      label: "Description",
      render: (issue: Issue) => (
        <div className="flex w-full justify-center">
          <p className="max-w-[300px] whitespace-normal text-center text-[10px] leading-4 text-white/50 2xl:text-xs">
            {issue.description || "-"}
          </p>
        </div>
      ),
    },

    {
      key: "country",
      label: "Country",
      render: (issue: Issue) => (
        <span className="inline-flex rounded-full border border-[#31d9e5]/25 bg-[#31d9e5]/10 px-2.5 py-1 text-[10px] font-semibold uppercase text-[#31d9e5] 2xl:text-xs">
          {issue.country || "-"}
        </span>
      ),
    },

    {
      key: "severity",
      label: "Severity",
      render: (issue: Issue) => (
        <span
          className={`inline-flex rounded-full border px-3 py-1 text-[10px] font-semibold 2xl:text-xs ${severityStyles[issue.severity] ||
            "border-white/15 bg-white/[0.06] text-white/75"
            }`}
        >
          {prettify(issue.severity)}
        </span>
      ),
    },

    {
      key: "status",
      label: "Status",
      render: (issue: Issue) => (
        <span
          className={`inline-flex rounded-full border px-3 py-1 text-[10px] font-semibold 2xl:text-xs ${statusStyles[issue.status] ||
            "border-white/15 bg-white/[0.06] text-white/75"
            }`}
        >
          {prettify(issue.status)}
        </span>
      ),
    },
  ];

  return (
    <div className="w-full text-white">
      <div className="space-y-5 2xl:space-y-6">

        {/* PAGE HEADER */}
        <PageBreadcrumb
          pageTitle="Issues"
          variant="superadmin"
          align="left"
          textSize="2xl"
        />

        {/* SUMMARY CARDS */}
        <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {summaryCards.map((card) => {
            const Icon = card.icon;

            return (
              <div
                key={card.label}
                className="relative min-h-[88px] overflow-hidden rounded-xl border-t-4 border-[#31D9E5] bg-[#484962] p-3 shadow-[0_14px_32px_rgba(20,22,45,0.20)] 2xl:min-h-[96px] 2xl:p-4"
              >
                <div className="flex h-full items-center gap-3">
                  <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#31d9e5]/15 text-[#31d9e5] 2xl:h-11 2xl:w-11">
                    <Icon size={20} strokeWidth={2.1} />
                  </span>

                  <div className="min-w-0">
                    <SummaryMetricCard
                      title={card.label}
                      value={card.value.toLocaleString()}
                      className="!bg-transparent !p-0 !shadow-none"
                      titleClassName="!text-white/70"
                      valueClassName="!text-white"
                    />

                    <p className="mt-1 text-[11px] text-white/45 2xl:text-xs">
                      {card.detail}
                    </p>
                  </div>
                </div>
              </div>
            );
          })}
        </section>

        {/* FILTERS */}
        <section className="rounded-xl border border-white/10 bg-[#484962] p-4 shadow-[0_16px_38px_rgba(20,22,45,0.20)]">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
            <select
              value={status}
              onChange={(event) =>
                setStatus(event.target.value as IssueStatus)
              }
              className="h-10 w-full rounded-lg border border-white/10 bg-white/[0.06] px-3 text-xs text-white outline-none focus:border-[#31d9e5] focus:ring-2 focus:ring-[#31d9e5]/15 sm:w-[180px] 2xl:text-sm"
            >
              <option disabled className="bg-[#37384f] text-white/50">
                Select Status
              </option>

              <option value="all" className="bg-[#37384f] text-white">
                All
              </option>

              <option value="open" className="bg-[#37384f] text-white">
                Open
              </option>

              <option value="resolved" className="bg-[#37384f] text-white">
                Resolved
              </option>
            </select>

            <div className="relative w-full sm:w-[300px] 2xl:w-[360px]">
              <input
                type="text"
                value={searchQuery}
                onChange={(event) =>
                  setSearchQuery(event.target.value)
                }
                placeholder="Search issues..."
                className="h-10 w-full rounded-lg border border-white/10 bg-white/[0.06] px-3 pr-9 text-xs text-white outline-none placeholder:text-white/40 focus:border-[#31d9e5] focus:ring-2 focus:ring-[#31d9e5]/15 2xl:text-sm"
              />

              <Search
                size={14}
                className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-[#31d9e5]"
              />
            </div>
          </div>
        </section>

        {/* ISSUES TABLE */}
        {loading ? (
          <div className="relative min-h-[260px] overflow-hidden rounded-xl border border-white/10 bg-[#484962]">
            <Loader
              fullscreen
              backgroundClass="bg-[#37384f]/65"
              roundedClass="rounded-xl"
            />
          </div>
        ) : (
          <SuperAdminUsersTable
            data={filteredIssues}
            columns={issueColumns}
            minWidth="1380px"
            emptyTitle="No issues found"
            emptyDescription={
              searchQuery.trim()
                ? "Try changing your search or selected filters."
                : "No issues are available for the selected period."
            }
          />
        )}
      </div>
    </div>
  );
}
