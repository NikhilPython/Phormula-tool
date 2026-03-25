"use client";
import React, { useEffect, useMemo, useState, useRef } from "react";
import { useSearchParams } from "next/navigation";
import { useSelector } from "react-redux";
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
import TargetVsSalesChart from "../objectives/TargetVsSalesChart";
import ObjectiveMoMChart from "../objectives/ObjectiveMoMChart";
import {
  FiEdit,
  FiCheck,
  FiX,
  FiGlobe,
  FiFileText,
  FiTrash2,
} from "react-icons/fi";
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import JSZip from "jszip";
import mammoth from "mammoth";
import Loader from "@/components/loader/Loader";
import { FiChevronDown, FiChevronRight } from "react-icons/fi";
import { IoMdLock } from "react-icons/io";
import { useRouter } from 'next/navigation';

type ObjectivesPageClientProps = {
  country?: string;
  month?: string;
  year?: string;
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
  | "targets_and_objectives"
  | "output";

type UploadedSummaryFile = {
  id: string;
  name: string;
  size: number;
  type: string;
  extractedText: string;
  uploadStatus: "ready" | "processing" | "error";
  error?: string;
  rawFile?: File;
};

type UserObjectiveForm = {
  growth_intent: "conservative" | "balanced" | "aggressive";
  profit_priority: "high" | "protect_growth" | "sacrifice_short_term";
  inventory_clearance_priority: boolean;
  business_context: string;
  country: string;
  time_horizon: "1_month";
  website: string;
  uploaded_files: UploadedSummaryFile[];
};

type BusinessJourneySection = {
  title: string;
  points: string[];
};

type BusinessJourneyResponse =
  | string
  | string[]
  | BusinessJourneySection[]
  | null;

const SUMMARY_TABS: Array<{ key: SummaryTab; label: string }> = [
  { key: "business_summary", label: "Business Summary" },
  { key: "targets_and_objectives", label: "Targets and Objectives" },
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

const dummyTargetVsSalesData = [
  { month: "Jan'25", target: 12000, sales: 9800, target_units: 900, monthwise_units: 760 },
  { month: "Feb'25", target: 12000, sales: 10450, target_units: 900, monthwise_units: 810 },
  { month: "Mar'25", target: 13000, sales: 11700, target_units: 950, monthwise_units: 870 },
  { month: "Apr'25", target: 14000, sales: 12600, target_units: 980, monthwise_units: 910 },
  { month: "May'25", target: 14500, sales: 13250, target_units: 1020, monthwise_units: 940 },
  { month: "Jun'25", target: 15000, sales: 13800, target_units: 1080, monthwise_units: 990 },
];

const dummyMonthlyTargetData: TargetRow[] = [
  {
    __pid: "amazon-us",
    sno: "1.",
    marketplace: "Amazon US",
    targetNative: "$12,000.00",
    conversion: "1.000",
    targetHome: "$12,000.00",
  },
  {
    __pid: "amazon-uk",
    sno: "2.",
    marketplace: "Amazon UK",
    targetNative: "£8,500.00",
    conversion: "1.270",
    targetHome: "$10,795.00",
  },
  {
    __pid: "shopify",
    sno: "3.",
    marketplace: "Shopify",
    targetNative: "$6,000.00",
    conversion: "-",
    targetHome: "$6,000.00",
  },
  {
    __pid: "global",
    sno: "",
    marketplace: "Total",
    targetNative: "",
    conversion: "",
    targetHome: "$28,795.00",
    __isTotal: true,
  },
];

type JourneySection = {
  title: string;
  content: Array<
    | { type: "paragraph"; text: string }
    | { type: "bullet"; text: string }
  >;
};

function JourneyAccordionSection({
  title,
  isOpen,
  onToggle,
  children,
}: {
  title: string;
  isOpen: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  const contentRef = useRef<HTMLDivElement | null>(null);
  const [height, setHeight] = useState(0);

  useEffect(() => {
    if (!contentRef.current) return;

    if (isOpen) {
      setHeight(contentRef.current.scrollHeight);
    } else {
      setHeight(0);
    }
  }, [isOpen, children]);

  return (
    <div className="overflow-hidden rounded-xl border border-gray-200 bg-white dark:border-gray-800 dark:bg-gray-900">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center justify-between px-4 py-3 text-left transition-colors duration-200 hover:bg-gray-50 dark:hover:bg-gray-800/60"
      >
        <div className="flex items-center gap-3">
          <span
            className={`text-charcoal-500 transition-transform duration-300 ease-in-out dark:text-gray-400 ${isOpen ? "rotate-90" : "rotate-0"
              }`}
          >
            <FiChevronRight size={18} />
          </span>

          <h4 className="text-sm font-semibold text-charcoal-500 dark:text-white/90">
            {title}
          </h4>
        </div>
      </button>

      <div
        style={{ height }}
        className="overflow-hidden transition-all duration-300 ease-in-out"
      >
        <div
          ref={contentRef}
          className={`border-t border-gray-200 px-4 py-4 transition-opacity duration-300 ease-in-out dark:border-gray-800 ${isOpen ? "opacity-100" : "opacity-0"
            }`}
        >
          {children}
        </div>
      </div>
    </div>
  );
}
function parseBusinessJourneySections(input: string): JourneySection[] {
  if (!input) return [];

  const lines = input
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const sections: JourneySection[] = [];
  let currentSection: JourneySection | null = null;

  for (const line of lines) {
    const isHeading = /^\d+\.\s+/.test(line);
    const isBullet = /^[-•]\s+/.test(line);

    if (isHeading) {
      if (currentSection) sections.push(currentSection);

      currentSection = {
        title: line.replace(/^\d+\.\s+/, "").trim(),
        content: [],
      };
      continue;
    }

    if (!currentSection) {
      currentSection = {
        title: "Overview",
        content: [],
      };
    }

    if (isBullet) {
      currentSection.content.push({
        type: "bullet",
        text: line.replace(/^[-•]\s+/, "").trim(),
      });
    } else {
      currentSection.content.push({
        type: "paragraph",
        text: line,
      });
    }
  }

  if (currentSection) sections.push(currentSection);

  return sections;
}


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

const countryToPlatform = (country: string): PlatformId => {
  const c = (country || "").toLowerCase();

  if (c === "uk") return "amazon-uk";
  if (c === "us") return "amazon-us";
  if (c === "ca") return "amazon-ca";
  return "shopify";
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
    <div className=" relative h-full rounded-2xl border border-gray-200 bg-white dark:border-gray-800 dark:bg-gray-900">
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

type ParsedJourneyBlock =
  | { type: "heading"; text: string }
  | { type: "paragraph"; text: string }
  | { type: "bullet"; text: string };

function BusinessJourneyPanel({
  journey,
  loading,
  error,
}: {
  journey: BusinessJourneyResponse;
  loading: boolean;
  error: string | null;
}) {
  const [openSectionKey, setOpenSectionKey] = useState<string | null>(null);

  const toggleSection = (key: string) => {
    setOpenSectionKey((prev) => (prev === key ? null : key));
  };

  const hasJourneyContent =
    !!journey &&
    !(
      typeof journey === "string" && !journey.trim()
    ) &&
    !(
      Array.isArray(journey) && journey.length === 0
    );

  return (
    <div
      className={`relative overflow-hidden rounded-2xl ${loading && !hasJourneyContent ? "min-h-[220px]" : ""
        }`}
    >
      {loading && (
        <div className="absolute inset-0 z-20 flex items-center justify-center rounded-2xl bg-white/80 dark:bg-gray-900/80">
          <Loader backgroundClass="bg-transparent" />
        </div>
      )}

      <div className={loading ? "pointer-events-none opacity-60" : ""}>
        {error ? (
          <div className="rounded-2xl">
            <p className="text-sm text-red-500">{error}</p>
          </div>
        ) : !journey ? (
          <div className="rounded-2xl">
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Business Journey will generate after Business Summary is available.
            </p>
          </div>
        ) : typeof journey === "string" ? (
          <div className="space-y-3">
            {parseBusinessJourneySections(journey).map((section, idx) => {
              const key = `${section.title}-${idx}`;
              const isOpen = openSectionKey === key;

              return (
                <JourneyAccordionSection
                  key={key}
                  title={section.title}
                  isOpen={isOpen}
                  onToggle={() => toggleSection(key)}
                >
                  <div className="space-y-2">
                    {section.content.map((item, itemIdx) => {
                      if (item.type === "bullet") {
                        return (
                          <div key={itemIdx} className="flex items-start gap-2 pl-2">
                            <span className="mt-2 min-h-1.5 min-w-1.5 rounded-full bg-charcoal-500 dark:bg-gray-400" />
                            <p className="text-sm leading-5 text-charcoal-500 dark:text-gray-300">
                              {item.text}
                            </p>
                          </div>
                        );
                      }

                      return (
                        <p
                          key={itemIdx}
                          className="text-sm leading-6 text-charcoal-500 dark:text-gray-300"
                        >
                          {item.text}
                        </p>
                      );
                    })}
                  </div>
                </JourneyAccordionSection>
              );
            })}
          </div>
        ) : Array.isArray(journey) &&
          journey.every(
            (item) =>
              typeof item === "object" &&
              item !== null &&
              "title" in item &&
              "points" in item
          ) ? (
          <div className="space-y-3">
            {(journey as BusinessJourneySection[]).map((section, idx) => {
              const key = `${section.title}-${idx}`;
              const isOpen = openSectionKey === key;

              return (
                <JourneyAccordionSection
                  key={key}
                  title={section.title}
                  isOpen={isOpen}
                  onToggle={() => toggleSection(key)}
                >
                  <div className="space-y-3">
                    {section.points?.map((point, pointIdx) => (
                      <div key={pointIdx} className="flex items-start gap-2 pl-2">
                        <span className="mt-2 h-1.5 w-1.5 rounded-full bg-gray-500 dark:bg-gray-400" />
                        <p className="text-sm leading-6 text-gray-700 dark:text-gray-300">
                          {point}
                        </p>
                      </div>
                    ))}
                  </div>
                </JourneyAccordionSection>
              );
            })}
          </div>
        ) : (
          <div className="rounded-2xl">
            <pre className="whitespace-pre-wrap text-sm text-charcoal-500 dark:text-white/90">
              {JSON.stringify(journey, null, 2)}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}

const PreviewLockedSection = ({
  enabled,
  children,
  title,
  description,
  buttonText,
  onAction,
}: {
  enabled: boolean;
  children: React.ReactNode;
  title?: string;
  description?: string;
  buttonText?: string;
  onAction?: () => void;
}) => {
  return (
    <div className="relative w-full">
      <div
        className={
          enabled
            ? "pointer-events-none select-none opacity-45 transition-all duration-300"
            : "opacity-100 transition-all duration-300"
        }
      >
        {children}
      </div>

      {enabled && (
        <>
          <div className="absolute inset-0 z-10 rounded-xl bg-white/45" />

          <div className="absolute inset-0 z-20 pointer-events-none">
            <div className="sticky top-[20vh] flex justify-center px-4">
              <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 text-center">
                <div className="mb-4 flex justify-center">
                  <div className="flex h-16 w-16 items-center justify-center rounded-full  bg-[#37455F]">
                    <IoMdLock className="text-3xl text-[#F8EDCE]" />
                  </div>
                </div>

                <h3 className="text-lg font-semibold text-[#414042]">
                  {title}
                </h3>

                <p className="mt-2 text-sm text-gray-600 leading-6">
                  {description}
                </p>

                <button
                  onClick={onAction}
                  className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE]"
                >
                  {buttonText}
                </button>

                <p className="mt-3 text-xs text-gray-500">
                  Demo data is shown for preview only.
                </p>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
};

const readFileAsArrayBuffer = (file: File) =>
  new Promise<ArrayBuffer>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as ArrayBuffer);
    reader.onerror = reject;
    reader.readAsArrayBuffer(file);
  });

const stripXmlTags = (xml: string) =>
  xml
    .replace(/<a:br\s*\/>/g, "\n")
    .replace(/<\/a:p>/g, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\s+\n/g, "\n")
    .replace(/\n\s+/g, "\n")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n\n")
    .trim();

const extractTextFromDocx = async (file: File) => {
  const buffer = await readFileAsArrayBuffer(file);
  const result = await mammoth.extractRawText({ arrayBuffer: buffer });
  return (result.value || "").trim();
};

const extractTextFromPptx = async (file: File) => {
  const buffer = await readFileAsArrayBuffer(file);
  const zip = await JSZip.loadAsync(buffer);

  const slideFiles = Object.keys(zip.files)
    .filter((name) => /^ppt\/slides\/slide\d+\.xml$/.test(name))
    .sort((a, b) => {
      const getNum = (s: string) => Number(s.match(/slide(\d+)\.xml/)?.[1] || 0);
      return getNum(a) - getNum(b);
    });

  const texts: string[] = [];

  for (const slidePath of slideFiles) {
    const xml = await zip.files[slidePath].async("string");
    const slideText = stripXmlTags(xml);
    if (slideText) {
      texts.push(slideText);
    }
  }

  return texts.join("\n\n---\n\n").trim();
};

const extractTextFromSupportedFile = async (file: File) => {
  const name = file.name.toLowerCase();

  if (name.endsWith(".docx")) {
    return extractTextFromDocx(file);
  }

  if (name.endsWith(".pptx")) {
    return extractTextFromPptx(file);
  }

  throw new Error("Only .docx and .pptx text extraction is supported in the frontend.");
};


export default function ObjectivesPageClient({
  country,
  month,
  year
}: ObjectivesPageClientProps) {
  const dispatch = useAppDispatch();
  const searchParams = useSearchParams();
  const token = useSelector((state: any) => state.auth?.token);

  const isPreviewMode = month === "NA" || year === "NA";

  const [activeTab, setActiveTab] = useState<SummaryTab>("business_summary");
  useEffect(() => {
    const tab = searchParams.get("tab");

    if (
      tab === "business_summary" ||
      tab === "targets_and_objectives" ||
      tab === "output"
    ) {
      setActiveTab(tab);
    }
  }, [searchParams]);

  const [currencyRates, setCurrencyRates] = useState<CurrencyRateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);

  const [editingPid, setEditingPid] = useState<PlatformId | null>(null);
  const [draftTarget, setDraftTarget] = useState<string>("");
  const [isTargetEditMode, setIsTargetEditMode] = useState(false);

  const [isBusinessSummaryEditMode, setIsBusinessSummaryEditMode] = useState(false);
  const [isStrategicEditMode, setIsStrategicEditMode] = useState(false);

  const [businessJourney, setBusinessJourney] = useState<BusinessJourneyResponse>(null);
  const [isFetchingBusinessJourney, setIsFetchingBusinessJourney] = useState(false);
  const [businessJourneyError, setBusinessJourneyError] = useState<string | null>(null);

  const [objective, setObjective] = useState<UserObjectiveForm>({
    growth_intent: "balanced",
    profit_priority: "protect_growth",
    inventory_clearance_priority: false,
    business_context: "",
    country: "",
    time_horizon: "1_month",
    website: "",
    uploaded_files: [],
  });

  const [objectiveDraft, setObjectiveDraft] = useState<UserObjectiveForm>(objective);
  useEffect(() => {
    if (!isPreviewMode) return;

    const dummyObjective: UserObjectiveForm = {
      growth_intent: "aggressive",
      profit_priority: "protect_growth",
      inventory_clearance_priority: true,
      business_context:
        "This is a dummy business summary preview. The business focuses on premium skincare products across marketplace and D2C channels, with emphasis on growth, margin discipline, and healthy inventory turnover.",
      country: "global",
      time_horizon: "1_month",
      website: "",
      uploaded_files: [],
    };

    setObjective(dummyObjective);
    setObjectiveDraft(dummyObjective);
  }, [isPreviewMode]);
  const [expandedChart, setExpandedChart] = useState<"targetSales" | "objectiveMoM" | null>(null);

  const [objectiveTargetDraft, setObjectiveTargetDraft] = useState<string>("");
  const [objectiveEditingPid, setObjectiveEditingPid] = useState<PlatformId | null>(null);
  const [isGeneratingSummary, setIsGeneratingSummary] = useState(false);
  const [isFetchingBusinessSummary, setIsFetchingBusinessSummary] = useState(false);
  const [isFetchingObjective, setIsFetchingObjective] = useState(false);

  const toggleChartExpand = (chart: "targetSales" | "objectiveMoM") => {
    setExpandedChart((prev) => (prev === chart ? null : chart));
  };

  const { data, isLoading, isError } = useGetUserDataQuery();
  const [updateProfile, { isLoading: isSaving }] = useUpdateProfileMutation();

  const connected = useConnectedPlatforms();

  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [isExtractingFiles, setIsExtractingFiles] = useState(false);

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

  const nonGlobalIntegratedCountries = useMemo(() => {
    return integratedCountries.filter((c) => c !== "global");
  }, [integratedCountries]);

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


  const fetchBusinessJourney = async ({
    countryName,
    month,
    year,
  }: {
    countryName: string;
    month: string;
    year: number;
  }) => {
    if (!token) return;
    if (!objective.business_context?.trim() && !objectiveDraft.business_context?.trim()) return;

    try {
      setIsFetchingBusinessJourney(true);
      setBusinessJourneyError(null);

      const params = new URLSearchParams({
        countryName: (countryName || "uk").toLowerCase(),
        month: (month || "february").toLowerCase(),
        year: String(year),
      });

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/generate?${params.toString()}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );

      const json = await res.json().catch(() => null);

      if (!res.ok) {
        throw new Error(json?.error || json?.details || "Failed to generate business journey");
      }

      setBusinessJourney(json?.business_journey ?? null);
    } catch (error: any) {
      console.error("Failed to fetch business journey:", error);
      setBusinessJourney(null);
      setBusinessJourneyError(error?.message || "Failed to load business journey");
    } finally {
      setIsFetchingBusinessJourney(false);
    }
  };

  useEffect(() => {
    if (isPreviewMode) return;
    if (!token) return;
    if (!objective.business_context?.trim()) return;

    const now = new Date();
    const monthName = now.toLocaleString("en-US", { month: "long" }).toLowerCase();
    const currentYear = now.getFullYear();

    const countryToFetch =
      (country || objective.country || integratedCountries[0] || "uk").toLowerCase();

    fetchBusinessJourney({
      countryName: countryToFetch,
      month: monthName,
      year: currentYear,
    });
  }, [
    token,
    isPreviewMode,
    objective.business_context,
    objective.country,
    country,
    integratedCountries,
  ]);

  useEffect(() => {
    if (!token || isPreviewMode) return;

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

  const monthName = (date = new Date()) =>
    date.toLocaleString("en-US", { month: "long" }).toLowerCase();

  const currentYear = (date = new Date()) => date.getFullYear();

  const rateMap = useMemo(() => {
    const map = new Map<string, number>();

    for (const r of currencyRates) {
      const key = [
        (r.user_currency || "").toLowerCase(),
        (r.selected_currency || "").toLowerCase(),
        (r.country || "").toLowerCase(),
        (r.month || "").toLowerCase(),
        Number(r.year),
      ].join("|");

      map.set(key, Number(r.conversion_rate));
    }

    return map;
  }, [currencyRates]);

  const getFxDb = (
    from: string,
    to: string,
    country: string,
    month: string = monthName(),
    year: number = currentYear()
  ) => {
    const f = (from || "").toLowerCase();
    const t = (to || "").toLowerCase();
    const c = (country || "").toLowerCase();
    const m = (month || "").toLowerCase();
    const y = Number(year);

    if (f === t) return 1;

    const directKey = `${f}|${t}|${c}|${m}|${y}`;
    const inverseKey = `${t}|${f}|${c}|${m}|${y}`;

    const direct = rateMap.get(directKey);
    if (direct != null) return direct;

    const inv = rateMap.get(inverseKey);
    if (inv != null && inv !== 0) return 1 / inv;

    return 1;
  };

  const router = useRouter();

  const resolvedTargetCountry = useMemo(() => {
    if (isPreviewMode) return "global";
    if (objectiveDraft.country) return objectiveDraft.country.toLowerCase();
    if (objective.country) return objective.country.toLowerCase();
    if (country) return country.toLowerCase();
    return "uk";
  }, [isPreviewMode, objectiveDraft.country, objective.country, country]);

  const isGlobalPage = resolvedTargetCountry === "global";



  const getCurrencySymbol = (code: string) => {
    switch ((code || "").toUpperCase()) {
      case "GBP":
        return "£";
      case "USD":
        return "$";
      case "EUR":
        return "€";
      case "CAD":
        return "C$";
      default:
        return code;
    }
  };

  const chartCurrencyCode = isPreviewMode
    ? homeCurrencyCode
    : resolvedTargetCountry === "global"
      ? homeCurrencyCode
      : pageCurrency;


  const startEditTarget = (pid: PlatformId) => {
    setEditingPid(pid);
    setDraftTarget(String(baseNativeTarget || ""));
  };

  const cancelEditTarget = () => {
    setEditingPid(null);
    setDraftTarget("");
  };


  const startBusinessSummaryEdit = () => {
    setObjectiveDraft(objective);
    setIsBusinessSummaryEditMode(true);
  };

  const cancelBusinessSummaryEdit = () => {
    setObjectiveDraft(objective);
    setIsBusinessSummaryEditMode(false);
  };

  const startStrategicEdit = () => {
    setObjectiveDraft(objective);
    setObjectiveTargetDraft(String(Number((data as any)?.target_sales ?? 0)));
    setObjectiveEditingPid(pagePlatform);
    setIsStrategicEditMode(true);
  };

  const cancelStrategicEdit = () => {
    setObjectiveDraft(objective);
    setObjectiveTargetDraft("");
    setObjectiveEditingPid(null);
    setIsStrategicEditMode(false);
  };

  const strategicTargetPid = objectiveEditingPid || pagePlatform;

  const targetSourceCountry = useMemo(() => {
    if (!isGlobalPage) {
      return resolvedTargetCountry;
    }

    if (objectiveDraft.country && objectiveDraft.country.toLowerCase() !== "global") {
      return objectiveDraft.country.toLowerCase();
    }

    if (objective.country && objective.country.toLowerCase() !== "global") {
      return objective.country.toLowerCase();
    }

    return nonGlobalIntegratedCountries[0] || "uk";
  }, [
    isGlobalPage,
    objectiveDraft.country,
    objective.country,
    resolvedTargetCountry,
    nonGlobalIntegratedCountries,
  ]);

  const targetSourcePlatform = useMemo(() => {
    return countryToPlatform(targetSourceCountry);
  }, [targetSourceCountry]);

  const targetSourceCurrency = useMemo(() => {
    return platformToCurrencyCode(targetSourcePlatform) || homeCurrencyCode;
  }, [targetSourcePlatform, homeCurrencyCode]);

  const strategicDisplayCurrency = useMemo(() => {
    if (isGlobalPage) {
      return homeCurrencyCode;
    }

    return platformToCurrencyCode(strategicTargetPid) || homeCurrencyCode;
  }, [isGlobalPage, strategicTargetPid, homeCurrencyCode]);

  const homeCountryForFx = useMemo(() => {
    if (homeCurrencyCode === "USD") return "us";
    if (homeCurrencyCode === "GBP") return "uk";
    if (homeCurrencyCode === "CAD") return "ca";
    if (homeCurrencyCode === "INR") return "india";
    return resolvedTargetCountry;
  }, [homeCurrencyCode, resolvedTargetCountry]);

  const strategicConversionRate = useMemo(() => {
    if (!isGlobalPage) return null;

    return getFxDb(
      targetSourceCurrency,
      homeCurrencyCode,
      homeCountryForFx,
      monthName(),
      currentYear()
    );
  }, [
    isGlobalPage,
    targetSourceCurrency,
    homeCurrencyCode,
    homeCountryForFx,
    rateMap,
  ]);

  const strategicDisplayTargetValue = useMemo(() => {
    const rawTarget = Number((data as any)?.target_sales ?? 0);

    if (!isGlobalPage) {
      return rawTarget;
    }

    const fx = strategicConversionRate ?? 1;
    return rawTarget * fx;
  }, [isGlobalPage, strategicConversionRate, data]);

  const getObjectiveMonth = () => {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, "0");
    return `${year}-${month}`;
  };

  const getCurrentMonthYear = () => {
    const now = new Date();

    return {
      month: now.toLocaleString("en-US", { month: "long" }),
      year: now.getFullYear(),
    };
  };



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
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to update target.");
    }
  };

  const handleBusinessSummarySave = async () => {
    try {
      const website = objectiveDraft.website?.trim();
      const businessContext = objectiveDraft.business_context?.trim();

      const pptFile = objectiveDraft.uploaded_files.find(
        (f) =>
          f.uploadStatus === "ready" &&
          f.name.toLowerCase().endsWith(".pptx")
      );

      const hasUploadedPpt = !!pptFile?.rawFile;
      const hasSavedPptWithoutRawFile = !!pptFile && !pptFile.rawFile;

      if (!businessContext && !website && !pptFile) {
        alert("Please provide at least one: Business Summary, Website, or PPT file.");
        return;
      }

      if (!businessContext && !website && hasSavedPptWithoutRawFile) {
        alert("Please re-upload the PPT file before saving.");
        return;
      }

      let nextBusinessContext = businessContext || "";

      if (!nextBusinessContext && (website || hasUploadedPpt)) {
        setIsGeneratingSummary(true);

        const formData = new FormData();

        if (website) {
          formData.append("website", website);
        }

        if (hasUploadedPpt && pptFile?.rawFile) {
          formData.append("ppt", pptFile.rawFile);
        }

        const analyzeRes = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/analyze-website`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${token}`,
            },
            body: formData,
          }
        );

        const analyzeJson = await analyzeRes.json().catch(() => null);

        if (!analyzeRes.ok) {
          throw new Error(
            analyzeJson?.details ||
            analyzeJson?.error ||
            "Failed to analyze input"
          );
        }

        nextBusinessContext = analyzeJson?.data?.overview || "";
      }

      const objectivePayload = {
        country: (objectiveDraft.country || resolvedTargetCountry).toLowerCase(),
        month: getObjectiveMonth(),
        business_context: nextBusinessContext || "",
        website_url: website || null,
        ppt_file_name: pptFile?.name || null,
      };

      const objectiveRes = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/objective`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify(objectivePayload),
        }
      );

      const objectiveJson = await objectiveRes.json().catch(() => null);

      if (!objectiveRes.ok) {
        throw new Error(objectiveJson?.error || "Failed to save business summary");
      }

      const finalObjective = {
        ...objective,
        ...objectiveDraft,
        business_context: nextBusinessContext,
      };

      setObjective(finalObjective);
      setObjectiveDraft(finalObjective);
      setIsBusinessSummaryEditMode(false);

      const now = new Date();
      await fetchBusinessJourney({
        countryName: (objectiveDraft.country || resolvedTargetCountry).toLowerCase(),
        month: now.toLocaleString("en-US", { month: "long" }).toLowerCase(),
        year: now.getFullYear(),
      });
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to save business summary");
    } finally {
      setIsGeneratingSummary(false);
    }
  };
  const handleStrategicObjectivesSave = async () => {
    try {
      const nextTarget = Number(objectiveTargetDraft);

      if (!Number.isFinite(nextTarget) || nextTarget < 0) {
        alert("Please enter a valid target.");
        return;
      }

      const countryToSave = (objectiveDraft.country || resolvedTargetCountry).toLowerCase();

      const objectivePayload = {
        country: countryToSave,
        month: getObjectiveMonth(),
        growth_intent: objectiveDraft.growth_intent,
        profit_priority: objectiveDraft.profit_priority,
        inventory_clearance_priority: objectiveDraft.inventory_clearance_priority,
      };

      const targetPayload = {
        month: new Date().toLocaleString("en-US", { month: "long" }),
        year: new Date().getFullYear(),
        country: countryToSave,
        target_sales: nextTarget,
      };

      const [objectiveRes, targetSummaryRes] = await Promise.all([
        fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/objective`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify(objectivePayload),
        }),
        fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/target-summary`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
          body: JSON.stringify(targetPayload),
        }),
      ]);

      const objectiveJson = await objectiveRes.json().catch(() => null);
      const targetSummaryJson = await targetSummaryRes.json().catch(() => null);

      if (!objectiveRes.ok) {
        throw new Error(objectiveJson?.error || "Failed to save objective");
      }

      if (!targetSummaryRes.ok) {
        throw new Error(
          targetSummaryJson?.error || "Failed to save monthly target summary."
        );
      }

      await updateProfile({ target_sales: nextTarget } as any).unwrap();
      dispatch(setUser({ target_sales: nextTarget } as any));

      const finalObjective = {
        ...objective,
        growth_intent: objectiveDraft.growth_intent,
        profit_priority: objectiveDraft.profit_priority,
        inventory_clearance_priority: objectiveDraft.inventory_clearance_priority,
        country: objectiveDraft.country,
      };

      setObjective(finalObjective);
      setObjectiveDraft(finalObjective);
      setIsStrategicEditMode(false);
      setObjectiveTargetDraft("");
      setObjectiveEditingPid(null);
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to save section");
    }
  };

  useEffect(() => {
    if (isPreviewMode) return;
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
    if (isPreviewMode) return dummyMonthlyTargetData;

    const currentTarget = Number((data as any)?.target_sales ?? 0);

    const rows: TargetRow[] = connectedPlatformsForTargets.map((pid, idx) => {
      const meta = PLATFORM_TARGET_META[pid] ?? {
        marketplace: String(pid),
        currencySymbol: "",
      };

      const nativeCurrency = platformToCurrencyCode(pid) || homeCurrencyCode;
      const rowCountry = platformToCountry(pid);
      const nativeToHome = getFxDb(nativeCurrency, homeCurrencyCode, rowCountry);
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
  }, [isPreviewMode, connectedPlatformsForTargets, data, homeCurrencyCode, rateMap]);

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

  const handleRemoveUploadedFile = (id: string) => {
    setObjectiveDraft((prev) => ({
      ...prev,
      uploaded_files: prev.uploaded_files.filter((file) => file.id !== id),
    }));
  };

  const handleFilesSelected = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setIsExtractingFiles(true);

    try {
      const id = `${file.name}-${file.size}-${Date.now()}-${Math.random()
        .toString(36)
        .slice(2, 8)}`;

      let processed: UploadedSummaryFile;

      try {
        const extractedText = await extractTextFromSupportedFile(file);

        processed = {
          id,
          name: file.name,
          size: file.size,
          type: file.type || "application/octet-stream",
          extractedText,
          uploadStatus: "ready",
          rawFile: file,
        };
      } catch (error: any) {
        processed = {
          id,
          name: file.name,
          size: file.size,
          type: file.type || "application/octet-stream",
          extractedText: "",
          uploadStatus: "error",
          error: error?.message || "Failed to extract text",
          rawFile: file,
        };
      }

      setObjectiveDraft((prev) => ({
        ...prev,
        uploaded_files: [processed],
      }));
    } finally {
      setIsExtractingFiles(false);
      e.target.value = "";
    }
  };

  useEffect(() => {
    const fetchBusinessSummary = async () => {
      if (!token || isPreviewMode) return;

      try {
        setIsFetchingBusinessSummary(true);

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/analyze-website`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
            },
          }
        );

        const json = await res.json().catch(() => null);

        if (!res.ok) {
          if (res.status !== 404) {
            throw new Error(json?.error || "Failed to fetch business summary");
          }
          return;
        }

        const serverFiles = json?.ppt_file_name
          ? [
            {
              id: `server-${json.ppt_file_name}`,
              name: json.ppt_file_name,
              size: 0,
              type: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
              extractedText: "",
              uploadStatus: "ready" as const,
            },
          ]
          : [];

        setObjective((prev) => ({
          ...prev,
          business_context: json?.overview || "",
          website: json?.website || "",
          uploaded_files: serverFiles,
        }));

        setObjectiveDraft((prev) => ({
          ...prev,
          business_context: json?.overview || "",
          website: json?.website || "",
          uploaded_files: serverFiles,
        }));
      } catch (error) {
        console.error(error);
      } finally {
        setIsFetchingBusinessSummary(false);
      }
    };

    fetchBusinessSummary();
  }, [token]);

  useEffect(() => {
    const fetchObjective = async () => {
      if (!token) return;

      const countryToFetch =
        (country || objective.country || integratedCountries[0] || "").toLowerCase();

      if (!countryToFetch) return;

      try {
        setIsFetchingObjective(true);

        const month = getObjectiveMonth();

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/objective?country=${encodeURIComponent(
            countryToFetch
          )}&month=${encodeURIComponent(month)}`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
            },
          }
        );

        const json = await res.json().catch(() => null);

        if (!res.ok) {
          if (res.status !== 404) {
            throw new Error(json?.error || "Failed to fetch objective");
          }

          setObjective((prev) => ({
            ...prev,
            country: countryToFetch,
          }));

          setObjectiveDraft((prev) => ({
            ...prev,
            country: countryToFetch,
          }));

          return;
        }

        const serverObjective = json?.objective;
        if (!serverObjective) return;

        setObjective((prev) => ({
          ...prev,
          country: countryToFetch,
          growth_intent: serverObjective.growth_intent ?? prev.growth_intent,
          profit_priority: serverObjective.profit_priority ?? prev.profit_priority,
          inventory_clearance_priority:
            serverObjective.inventory_clearance_priority ?? prev.inventory_clearance_priority,
          business_context: serverObjective.business_context ?? prev.business_context,
        }));

        setObjectiveDraft((prev) => ({
          ...prev,
          country: countryToFetch,
          growth_intent: serverObjective.growth_intent ?? prev.growth_intent,
          profit_priority: serverObjective.profit_priority ?? prev.profit_priority,
          inventory_clearance_priority:
            serverObjective.inventory_clearance_priority ?? prev.inventory_clearance_priority,
          business_context: serverObjective.business_context ?? prev.business_context,
        }));
      } catch (error) {
        console.error("Failed to fetch objective:", error);
      } finally {
        setIsFetchingObjective(false);
      }
    };

    fetchObjective();
  }, [token, country, integratedCountries]);

  const handleConnectAmazonPreview = () => {
    const connectCountry = country === "global" ? "uk" : country;
    router.push(`/integration-dashboard/${connectCountry}/NA/NA`);
  };

  return (
    <div className="w-full">


      {!isPreviewMode && (isLoading || ratesLoading || isFetchingBusinessSummary) && (
        <div className="mb-4 text-sm text-gray-500 dark:text-gray-400">Loading…</div>
      )}


      {isError && (
        <div className="mb-4 text-sm text-red-500">Failed to load objectives page.</div>
      )}

      <div className="mb-1">
        <PageBreadcrumb pageTitle="Business Overview" variant="page" align="left" textSize="2xl" />
      </div>


      <SummaryTabs activeTab={activeTab} onChange={setActiveTab} />

      <PreviewLockedSection
        enabled={isPreviewMode}
        title="Preview Mode"
        description="You're not seeing your real data yet.Connect your Amazon account now to unlock complete visibility into your business performance."
        buttonText="Connect Amazon"
        onAction={handleConnectAmazonPreview}
      >
        {activeTab === "business_summary" && (
          <div className="grid grid-cols-1 gap-4">
            <InfoCard
              title={<PageBreadcrumb pageTitle="Business Summary" variant="table" align="left" />}
              action={
                !isBusinessSummaryEditMode ? (
                  <button
                    type="button"
                    onClick={startBusinessSummaryEdit}
                    disabled={isPreviewMode}
                    className="inline-flex h-9 w-9 items-center justify-center text-gray-700 disabled:opacity-50 disabled:cursor-not-allowed"
                    aria-label="Enable business summary edit mode"
                    title="Edit business summary"
                  >
                    <FiEdit className="text-lg" />
                  </button>
                ) : (
                  <div className="flex items-center gap-2">
                    <Button
                      type="button"
                      onClick={handleBusinessSummarySave}
                      size="icon"
                      title="Save"
                      disabled={isGeneratingSummary || isSaving}
                    >
                      <FiCheck />
                    </Button>

                    <Button
                      type="button"
                      onClick={cancelBusinessSummaryEdit}
                      size="icon"
                      variant="outline"
                      title="Cancel"
                      disabled={isGeneratingSummary || isSaving}
                    >
                      <FiX />
                    </Button>
                  </div>
                )
              }
            >
              <>
                {isGeneratingSummary && (
                  <div className="absolute inset-0 z-50 flex items-center justify-center rounded-2xl bg-white/80 ">
                    <Loader backgroundClass="bg-transparent" />
                  </div>
                )}

                <div className="grid grid-cols-1 gap-5">
                  <div>
                    <div className="mb-1 flex items-center justify-end">
                      {isBusinessSummaryEditMode && (
                        <p className="text-xs text-gray-500">Max 250 characters</p>
                      )}
                    </div>

                    {isBusinessSummaryEditMode ? (
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
                        className="w-full resize-none rounded-md border border-gray-300 bg-white px-4 py-3 text-sm text-gray-800 outline-none focus:border-gray-400 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                        placeholder="Describe your business context..."
                      />
                    ) : objective.business_context ? (
                      <p className="whitespace-pre-wrap text-sm text-charcoal-500 dark:text-white/90">
                        {objective.business_context}
                      </p>
                    ) : (
                      <p className="whitespace-pre-wrap italic leading-relaxed text-sm text-gray-400 dark:text-gray-500">
                        Example:
                        Our business primarily sells premium skincare products across Amazon US and Shopify.
                        We focus on maintaining strong margins while scaling revenue through ads and organic ranking.
                        Inventory turnover is critical for us due to product shelf life, so clearing slow-moving SKUs
                        while maintaining bestseller stock is a key priority.
                      </p>
                    )}
                  </div>

                  {isBusinessSummaryEditMode && (
                    <div className="flex items-center gap-3 py-3">
                      <div className="h-px flex-1 bg-gray-200 dark:bg-gray-700" />

                      <span className="whitespace-nowrap text-[10px] sm:text-xs font-medium tracking-widest text-gray-400 dark:text-gray-500">
                        OR AUTO-EXTRACT YOUR BUSINESS SUMMARY
                      </span>

                      <div className="h-px flex-1 bg-gray-200 dark:bg-gray-700" />
                    </div>
                  )}

                  {isBusinessSummaryEditMode && (
                    <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                      <div>
                        <p className="mb-2 text-xs text-charcoal-500 dark:text-gray-400">Website</p>

                        <div className="relative">
                          <FiGlobe className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                          <input
                            type="url"
                            placeholder="https://yourwebsite.com"
                            value={objectiveDraft.website || ""}
                            onChange={(e) =>
                              setObjectiveDraft((prev) => ({
                                ...prev,
                                website: e.target.value,
                              }))
                            }
                            className="w-full rounded-md border border-gray-300 bg-white py-3 pl-10 pr-4 text-sm text-gray-800 outline-none focus:border-gray-400 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                          />
                        </div>
                      </div>

                      <div>
                        <p className="mb-2 text-xs text-charcoal-500 dark:text-gray-400">Files</p>

                        <div className="w-full rounded-md border border-gray-300 bg-white px-2 py-2 dark:border-gray-700 dark:bg-gray-800">
                          <div className="flex items-center gap-2">
                            <label
                              htmlFor="business-summary-file"
                              className="shrink-0 cursor-pointer rounded-md bg-gray-100 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-200 dark:bg-gray-700 dark:text-gray-200 dark:hover:bg-gray-600"
                            >
                              Upload File
                            </label>

                            <input
                              id="business-summary-file"
                              ref={fileInputRef}
                              type="file"
                              accept=".docx,.pptx"
                              onChange={handleFilesSelected}
                              className="hidden"
                            />

                            <div className="min-w-0 flex-1">
                              {objectiveDraft.uploaded_files?.[0] ? (
                                <div className="inline-flex max-w-full items-center gap-2 rounded-md bg-gray-50 px-2 py-1 dark:bg-gray-700/60">
                                  <FiFileText className="shrink-0 text-gray-500 dark:text-gray-300" />
                                  <span className="truncate text-xs text-gray-600 dark:text-gray-300 max-w-[180px] sm:max-w-[220px]">
                                    {objectiveDraft.uploaded_files[0].name}
                                  </span>
                                  <button
                                    type="button"
                                    onClick={() => handleRemoveUploadedFile(objectiveDraft.uploaded_files[0].id)}
                                    className="shrink-0 text-gray-400 hover:text-red-500"
                                    title="Remove file"
                                  >
                                    <FiTrash2 size={14} />
                                  </button>
                                </div>
                              ) : (
                                <span className="block truncate px-2 text-xs text-gray-500 dark:text-gray-400">
                                  No file selected
                                </span>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  {isExtractingFiles && (
                    <div className="rounded-2xl border border-gray-200 bg-gray-50 px-4 py-3 text-sm text-gray-600 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300">
                      Extracting text from files...
                    </div>
                  )}
                </div>
              </>
            </InfoCard>

            <InfoCard
              title={<PageBreadcrumb pageTitle="Business Journey" variant="table" align="left" />}
            >
              <BusinessJourneyPanel
                journey={businessJourney}
                loading={isFetchingBusinessJourney}
                error={businessJourneyError}
              />
            </InfoCard>

            <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
              {(expandedChart === null || expandedChart === "targetSales") && (
                <div className={expandedChart === "targetSales" ? "lg:col-span-2" : ""}>
                  <InfoCard
                    title={
                      <PageBreadcrumb
                        pageTitle="Target vs Monthwise Sales"
                        variant="table"
                        align="left"
                      />
                    }
                    action={
                      <button
                        type="button"
                        data-no-expand
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleChartExpand("targetSales");
                        }}
                        aria-label={
                          expandedChart === "targetSales"
                            ? "Collapse Target vs Monthwise Sales chart"
                            : "Expand Target vs Monthwise Sales chart"
                        }
                        title={expandedChart === "targetSales" ? "Collapse" : "Expand"}
                        className="hidden lg:inline-flex rounded-md border border-gray-300 bg-white text-blue-700 p-1.5 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md dark:bg-gray-800 dark:border-gray-700 dark:text-blue-400"
                      >
                        {expandedChart === "targetSales" ? (
                          <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                        ) : (
                          <RiExpandDiagonalFill size={18} className="font-extrabold" />
                        )}
                      </button>
                    }
                  >
                    <div className="h-[420px] w-full">
                      <TargetVsSalesChart
                        country={isPreviewMode ? "global" : resolvedTargetCountry}
                        token={token || ""}
                        apiBaseUrl={process.env.NEXT_PUBLIC_API_BASE_URL || ""}
                        monthsToLoad={12}
                        className="h-full w-full"
                        isPreviewMode={isPreviewMode}
                        dummyData={dummyTargetVsSalesData}
                        currencySymbol={getCurrencySymbol(chartCurrencyCode)}
                      />
                    </div>
                  </InfoCard>
                </div>
              )}

              {(expandedChart === null || expandedChart === "objectiveMoM") && (
                <div className={expandedChart === "objectiveMoM" ? "lg:col-span-2" : ""}>
                  <InfoCard
                    title={
                      <PageBreadcrumb
                        pageTitle="Objective MoM"
                        variant="table"
                        align="left"
                      />
                    }
                    action={
                      <button
                        type="button"
                        data-no-expand
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleChartExpand("objectiveMoM");
                        }}
                        aria-label={
                          expandedChart === "objectiveMoM"
                            ? "Collapse Objective MoM chart"
                            : "Expand Objective MoM chart"
                        }
                        title={expandedChart === "objectiveMoM" ? "Collapse" : "Expand"}
                        className="hidden lg:inline-flex rounded-md border border-gray-300 bg-white text-blue-700 p-1.5 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md dark:bg-gray-800 dark:border-gray-700 dark:text-blue-400"
                      >
                        {expandedChart === "objectiveMoM" ? (
                          <RiCollapseDiagonalFill size={18} className="font-extrabold" />
                        ) : (
                          <RiExpandDiagonalFill size={18} className="font-extrabold" />
                        )}
                      </button>
                    }
                  >
                    <div className="h-[420px] w-full">
                      <ObjectiveMoMChart
                        title="Objective MoM Trend"
                        country={isPreviewMode ? "global" : resolvedTargetCountry}
                        token={token || ""}
                        apiBaseUrl={process.env.NEXT_PUBLIC_API_BASE_URL || ""}
                        className="h-full w-full"
                        isPreviewMode={isPreviewMode}
                        dummyData={[
                          { month: "2025-01", growth_intent: "balanced", profit_priority: "protect_growth" },
                          { month: "2025-02", growth_intent: "aggressive", profit_priority: "protect_growth" },
                          { month: "2025-03", growth_intent: "balanced", profit_priority: "high" },
                          { month: "2025-04", growth_intent: "aggressive", profit_priority: "high" },
                          { month: "2025-05", growth_intent: "balanced", profit_priority: "sacrifice_short_term" },
                          { month: "2025-06", growth_intent: "aggressive", profit_priority: "protect_growth" },
                        ]}
                      />
                    </div>
                  </InfoCard>
                </div>
              )}
            </div>

          </div>
        )}


        {activeTab === "targets_and_objectives" && (

          <div className="grid grid-cols-1 gap-4">
            <InfoCard
              title={
                <PageBreadcrumb
                  pageTitle="Strategic Objectives and Monthly Targets"
                  variant="table"
                  align="left"
                />
              }
              action={
                !isStrategicEditMode ? (
                  <button
                    onClick={startStrategicEdit}
                    className="h-9 w-9 text-gray-700 disabled:opacity-50 disabled:cursor-not-allowed"
                    type="button"
                    disabled={isPreviewMode}
                  >
                    <FiEdit className="text-lg" />
                  </button>
                ) : (
                  <div className="flex items-center gap-2">
                    <Button size="icon" onClick={handleStrategicObjectivesSave}>
                      <FiCheck />
                    </Button>
                    <Button size="icon" variant="outline" onClick={cancelStrategicEdit}>
                      <FiX />
                    </Button>
                  </div>
                )
              }
            >
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
                <InfoItem
                  label="Growth"
                  value={
                    isStrategicEditMode ? (
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
                    isStrategicEditMode ? (
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
                    isStrategicEditMode ? (
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
                    isStrategicEditMode ? (
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

                <InfoItem
                  label={`Target (${strategicDisplayCurrency})`}
                  value={
                    isStrategicEditMode ? (
                      <input
                        type="number"
                        min={0}
                        step={1}
                        value={objectiveTargetDraft}
                        onChange={(e) => setObjectiveTargetDraft(e.target.value)}
                        className="w-full rounded-md border border-gray-300 bg-white px-2 py-1 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                        placeholder="Enter target"
                      />
                    ) : (
                      money(strategicDisplayTargetValue, strategicDisplayCurrency)
                    )
                  }
                />

                {isGlobalPage && (
                  <InfoItem
                    label={`Conversion Rate (${targetSourceCurrency} → ${homeCurrencyCode})`}
                    value={
                      <span>
                        {strategicConversionRate == null ? "-" : strategicConversionRate.toFixed(3)}
                      </span>
                    }
                  />
                )}
              </div>
            </InfoCard>
          </div>

        )}
      </PreviewLockedSection>
    </div>
  );
}