"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useSelector } from "react-redux";
import { FiEdit, FiCheck, FiX } from "react-icons/fi";

import Button from "@/components/ui/button/Button";
import DataTable, { type ColumnDef, type Row } from "@/components/ui/table/DataTable";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import { useAppDispatch } from "@/lib/hooks";
import { setUser } from "@/lib/features/auth/authSlice";
import { useGetUserDataQuery, useUpdateProfileMutation } from "@/lib/api/profileApi";
import { platformToCurrencyCode } from "@/lib/utils/currency";
import { useConnectedPlatforms } from "@/lib/utils/useConnectedPlatforms";
import type { PlatformId } from "@/lib/utils/platforms";
import SegmentedToggle from "../ui/SegmentedToggle";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import TargetVsSalesChart from "../objectives/TargetVsSalesChart";
import ObjectiveMoMChart from "../objectives/ObjectiveMoMChart";


type ObjectivesPageClientProps = {
  country?: string;
};

type UserObjectiveForm = {
  growth_intent: "conservative" | "balanced" | "aggressive";
  profit_priority: "high" | "protect_growth" | "sacrifice_short_term";
  inventory_clearance_priority: boolean;
  business_context: string;
  country: string;
  time_horizon: "1_month";
};

type CurrencyRateRow = {
  user_currency: string;
  country: string;
  selected_currency: string;
  conversion_rate: number;
  month: string;
  year: number;
};

type TargetRow = Row & {
  sno: React.ReactNode;
  marketplace: React.ReactNode;
  targetNative: React.ReactNode;
  conversion: React.ReactNode;
  targetHome: React.ReactNode;
  __isTotal?: boolean;
  __pid?: PlatformId | "global";
};

type SummaryTab =
  | "business_summary"
  | "target_achieved"
  | "objective_mom"
  | "output";

const SUMMARY_TABS: Array<{ key: SummaryTab; label: string }> = [
  { key: "business_summary", label: "Business Summary and Journey" },
  { key: "target_achieved", label: "Targets" },
  { key: "objective_mom", label: "Objective MoM" },
  { key: "output", label: "Output" },
];

const PLATFORM_TARGET_META: Partial<
  Record<PlatformId, { marketplace: string; currencySymbol: string }>
> = {
  "amazon-us": { marketplace: "Amazon US", currencySymbol: "$" },
  "amazon-uk": { marketplace: "Amazon UK", currencySymbol: "£" },
  "amazon-ca": { marketplace: "Amazon CA", currencySymbol: "C$" },
  shopify: { marketplace: "Shopify", currencySymbol: "" },
};

// Target vs Slaes graph Dummy Data - To be removed when real API is integrated
const dummyTargetVsSalesData = [
  { month: "Jan", target: 12000, sales: 9800 },
  { month: "Feb", target: 12000, sales: 10450 },
  { month: "Mar", target: 12000, sales: 11700 },
  { month: "Apr", target: 14000, sales: 12600 },
  { month: "May", target: 14000, sales: 13250 },
  { month: "Jun", target: 14000, sales: 13800 },
  { month: "Jul", target: 16000, sales: 14900 },
  { month: "Aug", target: 16000, sales: 15400 },
  { month: "Sep", target: 16000, sales: 15150 },
  { month: "Oct", target: 18000, sales: 16900 },
  { month: "Nov", target: 18000, sales: 17600 },
  { month: "Dec", target: 18000, sales: 18500 },
];

const shortMoney = (value: number, currency = "USD") =>
  new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);


const prettifyObjectiveValue = (v?: string | null) => {
  const s = (v ?? "").trim();
  if (!s) return "-";

  const pretty = s
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());

  return pretty.replace(/\b(Us|Uk|Uae|Eu)\b/g, (m) => m.toUpperCase());
};

const money = (amount: number, currency: string) =>
  new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount);

const platformToCountry = (pid: PlatformId) => {
  if (pid === "amazon-us") return "us";
  if (pid === "amazon-uk") return "uk";
  if (pid === "amazon-ca") return "ca";
  return "global";
};

function InfoCard({
  title,
  children,
  action,
}: {
  title: React.ReactNode;
  children: React.ReactNode;
  action?: React.ReactNode;
}) {
  return (
    <div className="h-full rounded-2xl border border-gray-200 bg-white dark:border-gray-800 dark:bg-gray-900">
      <div className="flex items-center justify-between px-4 py-3">
        <h3 className="text-sm font-semibold text-gray-800 dark:text-white/90">{title}</h3>
        {action}
      </div>
      <div className="h-px w-full bg-gray-200 dark:bg-gray-800" />
      <div className="p-4">{children}</div>
    </div>
  );
}

function InfoItem({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div>
      <p className="mb-1 text-xs text-gray-500 dark:text-gray-400">{label}</p>
      <div className="text-sm font-medium text-gray-800 dark:text-white/90">{value}</div>
    </div>
  );
}

function SummaryTabs({
  activeTab,
  onChange,
}: {
  activeTab: SummaryTab;
  onChange: (tab: SummaryTab) => void;
}) {
  return (
    <div className="mb-4">
      <SegmentedToggle<SummaryTab>
        value={activeTab}
        onChange={onChange}
        options={SUMMARY_TABS.map((tab) => ({
          value: tab.key,
          label: tab.label,
        }))}
        className="mt-2"
        compact
        textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
      />
    </div>
  );
}

function PlaceholderPanel({ title }: { title: string }) {
  return (
    <div className="rounded-2xl ">


      <div className="space-y-5 text-sm text-charcoal-500 dark:text-gray-300">
        <div>
          <h4 className="font-semibold ">
            1. Business Foundation
          </h4>
          <ul className="mt-1 list-disc space-y-1 pl-5">
            <li>
              Built around premium skincare products focusing on quality,
              repeat purchases, and strong customer trust.
            </li>
            <li>
              Established presence across Amazon US and Shopify to balance
              marketplace reach with direct customer ownership.
            </li>
          </ul>
        </div>

        <div>
          <h4 className="font-semibold">
            2. Early Growth Phase
          </h4>
          <ul className="mt-1 list-disc space-y-1 pl-5">
            <li>
              Initial growth was driven by product-market fit within core
              skincare categories.
            </li>
            <li>
              Amazon helped generate traffic and sales volume while Shopify
              strengthened direct brand relationships.
            </li>
          </ul>
        </div>

        <div>
          <h4 className="font-semibold ">
            3. Revenue Expansion Strategy
          </h4>
          <ul className="mt-1 list-disc space-y-1 pl-5">
            <li>
              Growth focused on paid advertising, organic ranking improvement,
              and stronger marketplace visibility.
            </li>
            <li>
              Efforts prioritized scalable sales growth while maintaining brand
              positioning and customer experience.
            </li>
          </ul>
        </div>

        <div>
          <h4 className="font-semibold ">
            4. Inventory Management Evolution
          </h4>
          <ul className="mt-1 list-disc space-y-1 pl-5">
            <li>
              Inventory turnover became critical due to shelf-life sensitivity
              and aging stock risks.
            </li>
            <li>
              Focus shifted to clearing slow-moving SKUs while maintaining
              healthy stock levels of best-selling products.
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
}

export default function ObjectivesPageClient({
  country,
}: ObjectivesPageClientProps) {
  const dispatch = useAppDispatch();
  const token = useSelector((state: any) => state.auth?.token);

  const [activeTab, setActiveTab] = useState<SummaryTab>("business_summary");
  const [currencyRates, setCurrencyRates] = useState<CurrencyRateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);

  const [editingPid, setEditingPid] = useState<PlatformId | null>(null);
  const [draftTarget, setDraftTarget] = useState<string>("");
  const [isTargetEditMode, setIsTargetEditMode] = useState(false);
  const [isObjectiveEditMode, setIsObjectiveEditMode] = useState(false);
  const [targetLocked, setTargetLocked] = useState(false);
  const [objective, setObjective] = useState<UserObjectiveForm>({
    growth_intent: "aggressive",
    profit_priority: "protect_growth",
    inventory_clearance_priority: false,
    business_context: "",
    country: "",
    time_horizon: "1_month",
  });

  const [objectiveDraft, setObjectiveDraft] = useState<UserObjectiveForm>(objective);

  const { data, isLoading, isError } = useGetUserDataQuery();
  const [updateProfile, { isLoading: isSaving }] = useUpdateProfileMutation();

  const connected = useConnectedPlatforms();

  const connectedPlatformsForTargets = useMemo(() => {
    const ids: PlatformId[] = [];
    if (connected.amazonUs) ids.push("amazon-us");
    if (connected.amazonUk) ids.push("amazon-uk");
    if (connected.amazonCa) ids.push("amazon-ca");
    if (connected.shopify) ids.push("shopify");
    return ids;
  }, [connected.amazonUs, connected.amazonUk, connected.amazonCa, connected.shopify]);

  const integratedCountries = useMemo(() => {
    const countries: string[] = [];
    if (connected.amazonUs) countries.push("us");
    if (connected.amazonUk) countries.push("uk");
    if (connected.amazonCa) countries.push("ca");
    if (connected.shopify) countries.push("global");
    return countries;
  }, [connected]);

  const pagePlatform: PlatformId = useMemo(() => {
    const c = (country || "").toLowerCase();

    if (c === "uk") return "amazon-uk";
    if (c === "us") return "amazon-us";
    if (c === "ca") return "amazon-ca";
    if (c === "global") return "shopify";

    const amazonConnectedCount = [
      connected.amazonUk,
      connected.amazonUs,
      connected.amazonCa,
    ].filter(Boolean).length;

    if (amazonConnectedCount === 1) {
      if (connected.amazonUk) return "amazon-uk";
      if (connected.amazonUs) return "amazon-us";
      if (connected.amazonCa) return "amazon-ca";
    }

    return "shopify";
  }, [country, connected.amazonUk, connected.amazonUs, connected.amazonCa]);

  const pageCurrency = useMemo(() => platformToCurrencyCode(pagePlatform), [pagePlatform]);

  const homeCurrencyCode = ((data as any)?.homeCurrency || pageCurrency || "USD").toUpperCase();

  const baseNativeTarget = Number((data as any)?.target_sales ?? 0);

  const GROWTH_OPTIONS = ["conservative", "balanced", "aggressive"] as const;

  const PROFIT_OPTIONS: Array<{
    label: string;
    value: UserObjectiveForm["profit_priority"];
  }> = [
      { label: "Yes Profit is high priority", value: "high" },
      {
        label: "I'm Okay with current profit if it helps me grow my sales",
        value: "protect_growth",
      },
      {
        label: "I'm Okay with short term losses if it helps in high growth numbers",
        value: "sacrifice_short_term",
      },
    ];

  useEffect(() => {
    if (!token) return;

    const fetchRates = async () => {
      try {
        setRatesLoading(true);
        const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`, {
          headers: { Authorization: `Bearer ${token}` },
        });

        if (!res.ok) {
          const errText = await res.text();
          throw new Error(`Failed to fetch rates: ${res.status} ${errText}`);
        }

        const json = await res.json();
        setCurrencyRates(json);
      } catch (e) {
        console.error(e);
        setCurrencyRates([]);
      } finally {
        setRatesLoading(false);
      }
    };

    fetchRates();
  }, [token]);

  const rateMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const r of currencyRates) {
      const key = `${r.user_currency}|${r.selected_currency}|${r.country}`;
      map.set(key, Number(r.conversion_rate));
    }
    return map;
  }, [currencyRates]);

  const getFxDb = (from: string, to: string, country: string) => {
    const f = (from || "").toLowerCase();
    const t = (to || "").toLowerCase();
    const c = (country || "").toLowerCase();

    if (f === t) return 1;

    const direct = rateMap.get(`${f}|${t}|${c}`);
    if (direct != null) return direct;

    const inv = rateMap.get(`${t}|${f}|${c}`);
    if (inv != null && inv !== 0) return 1 / inv;

    return 1;
  };

  const startEditTarget = (pid: PlatformId) => {
    setEditingPid(pid);
    setDraftTarget(String(baseNativeTarget || ""));
  };

  const cancelEditTarget = () => {
    setEditingPid(null);
    setDraftTarget("");
  };

  const openTargetEditMode = () => {
    setIsTargetEditMode(true);
    const firstPid = connectedPlatformsForTargets[0];
    if (firstPid) startEditTarget(firstPid);
  };

  const closeTargetEditMode = () => {
    setIsTargetEditMode(false);
    cancelEditTarget();
  };

  const startObjectiveEdit = () => {
    setObjectiveDraft(objective);
    setIsObjectiveEditMode(true);
  };

  const cancelObjectiveEdit = () => {
    setObjectiveDraft(objective);
    setIsObjectiveEditMode(false);
  };

  const getCurrentMonthYear = () => {
    const now = new Date();

    return {
      month: now.toLocaleString("en-US", { month: "long" }),
      year: now.getFullYear(),
    };
  };

  const resolvedTargetCountry = useMemo(() => {
    if (objectiveDraft.country) return objectiveDraft.country.toLowerCase();
    if (objective.country) return objective.country.toLowerCase();
    if (country) return country.toLowerCase();
    return "uk";
  }, [objectiveDraft.country, objective.country, country]);

  useEffect(() => {
    if (!token) return;

    const fetchTargetStatus = async () => {
      try {
        const now = new Date();
        const month = now.toLocaleString("en-US", { month: "long" });
        const year = now.getFullYear();

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/target-summary?month=${month}&year=${year}&country=${resolvedTargetCountry}`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          }
        );

        if (res.ok) {
          const data = await res.json();
          if (data?.data?.target_sales) {
            setTargetLocked(true);
          }
        }
      } catch (err) {
        console.error("Target status check failed", err);
      }
    };

    fetchTargetStatus();
  }, [token, resolvedTargetCountry]);

  const saveInlineTarget = async () => {
    const next = Number(draftTarget);

    if (!Number.isFinite(next) || next < 0) {
      alert("Please enter a valid number.");
      return;
    }

    const { month, year } = getCurrentMonthYear();

    const payload = {
      month,
      year,
      country: resolvedTargetCountry,
      target_sales: next,
    };

    try {
      await updateProfile({ target_sales: next } as any).unwrap();
      dispatch(setUser({ target_sales: next } as any));

      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/target-summary`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(payload),
      });

      const result = await res.json();

      if (!res.ok) {
        throw new Error(result?.error || "Failed to save monthly target summary.");
      }

      console.log("Monthly target saved:", result);

      setIsTargetEditMode(false);
      setEditingPid(null);
      setDraftTarget("");
      setTargetLocked(true);
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to update target.");
    }
  };

  const handleInlineObjectiveSave = async () => {
    try {
      const payload = {
        ...objectiveDraft,
        country: objectiveDraft.country.toLowerCase(),
      };

      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/objective`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(payload),
      });

      if (!res.ok) throw new Error("Failed to save objective");

      setObjective(objectiveDraft);
      setIsObjectiveEditMode(false);

      localStorage.setItem("user_objective", JSON.stringify(objectiveDraft));
      localStorage.setItem("user_objective_backend", JSON.stringify(payload));
    } catch (err) {
      console.error(err);
      alert("Failed to save objective");
    }
  };

  useEffect(() => {
    const saved = localStorage.getItem("user_objective");
    if (!saved) return;

    try {
      const parsed = JSON.parse(saved);
      setObjective((prev) => ({
        ...prev,
        ...parsed,
        profit_priority: parsed.profit_priority ?? parsed.primary_goal ?? prev.profit_priority,
        growth_intent: parsed.growth_intent ?? parsed.risk_level ?? prev.growth_intent,
      }));
    } catch (e) {
      console.error("Failed to parse objective from localStorage");
    }
  }, []);

  useEffect(() => {
    if (!objective.country) {
      if (country) {
        setObjective((prev) => ({ ...prev, country: country.toLowerCase() }));
      } else if (integratedCountries.length) {
        setObjective((prev) => ({ ...prev, country: integratedCountries[0] }));
      }
    }
  }, [country, integratedCountries, objective.country]);

  const monthlyTargetData: TargetRow[] = useMemo(() => {
    const currentTarget = Number((data as any)?.target_sales ?? 0);

    const rows: TargetRow[] = connectedPlatformsForTargets.map((pid, idx) => {
      const meta = PLATFORM_TARGET_META[pid] ?? {
        marketplace: String(pid),
        currencySymbol: "",
      };

      const nativeCurrency = platformToCurrencyCode(pid) || homeCurrencyCode;
      const country = platformToCountry(pid);
      const nativeToHome = getFxDb(nativeCurrency, homeCurrencyCode, country);
      const homeTarget = currentTarget * nativeToHome;

      return {
        __pid: pid,
        sno: `${idx + 1}.`,
        marketplace: meta.marketplace,
        targetNative: money(currentTarget, nativeCurrency),
        conversion: nativeCurrency === homeCurrencyCode ? "-" : nativeToHome.toFixed(3),
        targetHome: money(homeTarget, homeCurrencyCode),
      };
    });

    if (rows.length) {
      const totalHome = rows.reduce((sum, row: any) => {
        const num = Number(String(row.targetHome).replace(/[^0-9.-]+/g, ""));
        return sum + (Number.isFinite(num) ? num : 0);
      }, 0);

      rows.push({
        __pid: "global",
        sno: "",
        marketplace: "Total",
        targetNative: "",
        conversion: "",
        targetHome: money(totalHome, homeCurrencyCode),
        __isTotal: true,
      });
    }

    return rows;
  }, [connectedPlatformsForTargets, data, homeCurrencyCode, rateMap]);

  const monthlyTargetColumns: ColumnDef<TargetRow>[] = useMemo(
    () => [
      { key: "sno", header: "S.No.", width: "60px" },
      { key: "marketplace", header: "Marketplace", width: "180px" },
      {
        key: "targetNative",
        header: "Target (Native Currency)",
        width: "220px",
        render: (row: any) => {
          if (row.__isTotal) return row.targetNative;

          const pid = row.__pid as PlatformId;
          const nativeCurrency = platformToCurrencyCode(pid) || homeCurrencyCode;
          const currentTarget = Number((data as any)?.target_sales ?? 0);

          if (editingPid === pid) {
            return (
              <input
                autoFocus
                type="number"
                inputMode="numeric"
                min={0}
                step={1}
                value={draftTarget}
                onChange={(e) => setDraftTarget(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") saveInlineTarget();
                  if (e.key === "Escape") cancelEditTarget();
                }}
                className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-center text-sm outline-none focus:border-gray-400"
              />
            );
          }

          const displayValue = money(currentTarget, nativeCurrency);

          if (!isTargetEditMode) {
            return <span className="block w-full text-center">{displayValue}</span>;
          }

          return (
            <button
              type="button"
              onClick={() => startEditTarget(pid)}
              className="w-full cursor-text rounded-md text-center hover:bg-gray-50"
              title="Click to edit"
            >
              {displayValue}
            </button>
          );
        },
      },
      {
        key: "conversion",
        header: `Conversion Rate (${homeCurrencyCode})`,
        width: "210px",
      },
      {
        key: "targetHome",
        header: `Target (${homeCurrencyCode})`,
        width: "200px",
      },
    ],
    [homeCurrencyCode, editingPid, draftTarget, isTargetEditMode, data]
  );

  return (
    <div className="w-full">
      {(isLoading || ratesLoading) && (
        <div className="mb-4 text-sm text-gray-500 dark:text-gray-400">Loading…</div>
      )}

      {isError && (
        <div className="mb-4 text-sm text-red-500">Failed to load objectives page.</div>
      )}

      <div className="mb-1">
        <PageBreadcrumb pageTitle="Business Overview" variant="page" align="left" textSize="2xl" />
      </div>

      <SummaryTabs activeTab={activeTab} onChange={setActiveTab} />

      {activeTab === "business_summary" && (
        <div className="grid grid-cols-1 gap-4">
          <InfoCard
            title={<PageBreadcrumb pageTitle="Business Summary" variant="table" align="left" />}
            action={
              !isObjectiveEditMode ? (
                <button
                  onClick={startObjectiveEdit}
                  className="h-9 w-9 text-gray-700"
                  type="button"
                >
                  <FiEdit className="text-lg" />
                </button>
              ) : (
                <div className="flex items-center gap-2">
                  <Button size="icon" onClick={handleInlineObjectiveSave}>
                    <FiCheck />
                  </Button>
                  <Button size="icon" variant="outline" onClick={cancelObjectiveEdit}>
                    <FiX />
                  </Button>
                </div>
              )
            }
          >
            <div className="grid grid-cols-1 gap-4">
              <div>
                <div className="mb-1 flex items-center justify-end">
                  {/* <p className="text-xs text-gray-500 dark:text-gray-400">Business Summary</p> */}
                  {isObjectiveEditMode && <p className="text-xs text-gray-500">Max 250 characters</p>}
                </div>

                {isObjectiveEditMode ? (
                  <textarea
                    rows={5}
                    maxLength={250}
                    value={objectiveDraft.business_context || ""}
                    onChange={(e) =>
                      setObjectiveDraft((prev) => ({
                        ...prev,
                        business_context: e.target.value,
                      }))
                    }
                    className="w-full resize-none rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                    placeholder="Describe your business context..."
                  />
                ) : objective.business_context ? (
                  <p className="whitespace-pre-wrap text-sm text-gray-800 dark:text-white/90">
                    {objective.business_context}
                  </p>
                ) : (
                  <p className="whitespace-pre-wrap text-sm text-gray-400 dark:text-gray-500 italic leading-relaxed">
                    Example:
                    Our business primarily sells premium skincare products across Amazon US and Shopify.
                    We focus on maintaining strong margins while scaling revenue through ads and organic ranking.
                    Inventory turnover is critical for us due to product shelf life, so clearing slow-moving SKUs
                    while maintaining bestseller stock is a key priority.
                  </p>
                )}
              </div>
            </div>
          </InfoCard>

          <InfoCard
            title={<PageBreadcrumb pageTitle="Business Journey" variant="table" align="left" />}
          >
            <PlaceholderPanel title="Business Journey" />
          </InfoCard>
        </div>
      )}



      {activeTab === "target_achieved" && (
        <div className="grid grid-cols-1 gap-4">

          <InfoCard
            title={<PageBreadcrumb pageTitle="Monthly Targets" variant="table" align="left" />}
            action={
              <div className="flex items-center gap-2">
                {!isTargetEditMode ? (
                  !targetLocked && (
                    <button
                      type="button"
                      onClick={openTargetEditMode}
                      className="inline-flex h-9 w-9 items-center justify-center text-gray-700"
                      aria-label="Enable edit mode"
                      title="Edit targets"
                    >
                      <FiEdit className="text-lg" />
                    </button>
                  )
                ) : (
                  <>
                    <Button
                      type="button"
                      onClick={saveInlineTarget}
                      size="icon"
                      disabled={isSaving || !editingPid}
                      title="Save"
                    >
                      <FiCheck />
                    </Button>

                    <Button
                      type="button"
                      onClick={closeTargetEditMode}
                      size="icon"
                      variant="outline"
                      disabled={isSaving}
                      title="Cancel"
                    >
                      <FiX />
                    </Button>
                  </>
                )}
              </div>
            }
          >
            {isTargetEditMode && (
              <div className="mb-3 rounded-md border border-yellow-300 bg-yellow-50 px-3 py-2 text-sm text-yellow-800">
                ⚠️ Please enter your target carefully. Once saved, you will not be able to
                change it until the 1st of next month.
              </div>
            )}

            <DataTable
              columns={monthlyTargetColumns}
              data={monthlyTargetData}
              paginate={false}
              scrollY={false}
              stickyHeader={false}
              emptyMessage={
                ratesLoading ? "Loading currency rates..." : "No connected marketplaces."
              }
              className="rounded-xl"
              rowClassName={(row) => (row.__isTotal ? "bg-[#D9D9D933] font-semibold" : "")}
            />
          </InfoCard>
          
          <InfoCard
            title={<PageBreadcrumb pageTitle="Target vs Monthwise Sales" variant="table" align="left" />}
          >
            <div className="h-[420px] w-full">
              <TargetVsSalesChart currencySymbol={homeCurrencyCode === "GBP" ? "£" : homeCurrencyCode === "USD" ? "$" : homeCurrencyCode === "EUR" ? "€" : homeCurrencyCode} />
            </div>
          </InfoCard>
        </div>
      )}

      {activeTab === "objective_mom" && (
        <div className="grid grid-cols-1 gap-4">
          <InfoCard
            title={<PageBreadcrumb pageTitle="Strategic Objectives" variant="table" align="left" />}
            action={
              !isObjectiveEditMode ? (
                <button
                  onClick={startObjectiveEdit}
                  className="h-9 w-9 text-gray-700"
                  type="button"
                >
                  <FiEdit className="text-lg" />
                </button>
              ) : (
                <div className="flex items-center gap-2">
                  <Button size="icon" onClick={handleInlineObjectiveSave}>
                    <FiCheck />
                  </Button>
                  <Button size="icon" variant="outline" onClick={cancelObjectiveEdit}>
                    <FiX />
                  </Button>
                </div>
              )
            }
          >
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <InfoItem
                label="Growth"
                value={
                  isObjectiveEditMode ? (
                    <select
                      value={objectiveDraft.growth_intent}
                      onChange={(e) =>
                        setObjectiveDraft((prev) => ({
                          ...prev,
                          growth_intent: e.target.value as UserObjectiveForm["growth_intent"],
                        }))
                      }
                      className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                    >
                      {GROWTH_OPTIONS.map((v) => (
                        <option key={v} value={v}>
                          {v.charAt(0).toUpperCase() + v.slice(1)}
                        </option>
                      ))}
                    </select>
                  ) : (
                    prettifyObjectiveValue(objective.growth_intent)
                  )
                }
              />

              <InfoItem
                label="Profit"
                value={
                  isObjectiveEditMode ? (
                    <select
                      value={objectiveDraft.profit_priority}
                      onChange={(e) =>
                        setObjectiveDraft((prev) => ({
                          ...prev,
                          profit_priority: e.target.value as UserObjectiveForm["profit_priority"],
                        }))
                      }
                      className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                    >
                      {PROFIT_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  ) : (
                    prettifyObjectiveValue(objective.profit_priority)
                  )
                }
              />

              <InfoItem
                label="Inventory Dilution"
                value={
                  isObjectiveEditMode ? (
                    <select
                      value={objectiveDraft.inventory_clearance_priority ? "yes" : "no"}
                      onChange={(e) =>
                        setObjectiveDraft((prev) => ({
                          ...prev,
                          inventory_clearance_priority: e.target.value === "yes",
                        }))
                      }
                      className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                    >
                      <option value="yes">Yes</option>
                      <option value="no">No</option>
                    </select>
                  ) : (
                    objective.inventory_clearance_priority ? "Yes" : "No"
                  )
                }
              />

              <InfoItem
                label="Country"
                value={
                  isObjectiveEditMode ? (
                    <select
                      value={objectiveDraft.country}
                      onChange={(e) =>
                        setObjectiveDraft((prev) => ({
                          ...prev,
                          country: e.target.value,
                        }))
                      }
                      className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                    >
                      <option value="" disabled>
                        Select Country
                      </option>
                      {integratedCountries.map((c) => (
                        <option key={c} value={c}>
                          {c.toUpperCase()}
                        </option>
                      ))}
                    </select>
                  ) : (
                    objective.country?.toUpperCase() || "-"
                  )
                }
              />
            </div>
          </InfoCard>

          <InfoCard
            title={<PageBreadcrumb pageTitle="Objective MoM (Month on Month)" variant="table" align="left" />}
          >
            <div className="h-[420px] w-full">
              <ObjectiveMoMChart title="Objective MoM Trend" />
            </div>
          </InfoCard>
        </div>
      )}
    </div>
  );
}