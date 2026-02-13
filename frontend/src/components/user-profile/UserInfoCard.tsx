
"use client";
import React, { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import { useModal } from "../../hooks/useModal";
import { Modal } from "../ui/modal";
import Button from "../ui/button/Button";
import Input from "../form/input/InputField";
import Label from "../form/Label";
import {
  useForgotPasswordMutation,
  useGetUserDataQuery,
  useUpdateProfileMutation,
} from "@/lib/api/profileApi";
import { FiEdit } from "react-icons/fi";
import Link from "next/link";
import { platformToCurrencyCode } from "@/lib/utils/currency";
import { useConnectedPlatforms } from "@/lib/utils/useConnectedPlatforms";
import { ALL_PLATFORM_DEFS, type PlatformId } from "@/lib/utils/platforms";
import ReactCountryFlag from "react-country-flag";
import { FaPlus } from "react-icons/fa6";
import DataTable, { type ColumnDef, type Row } from "@/components/ui/table/DataTable";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import { useSelector } from "react-redux";
import { useAppDispatch } from "@/lib/hooks";
import { setUser } from "@/lib/features/auth/authSlice";

const safeJson = (val: any) => {
  try {
    if (!val) return null;
    if (typeof val === "object") return val;
    return JSON.parse(val);
  } catch {
    return null;
  }
};

type FormState = {
  name: string;
  brand_name: string;
  company_name: string;
  annual_sales_range: string;
  email: string;
  phone_number: string;
  homeCurrency: string;
  target_sales: string;

  gst_no: string;
  pan_no: string;

  address_building: string;
  address_city: string;
  address_country: string;
  address_state: string;
  address_zipcode: string;
};

type UserObjectiveForm = {
  growth_intent: "conservative" | "balanced" | "aggressive";
  profit_priority: "high" | "protect_growth" | "sacrifice_short_term";
  inventory_clearance_priority: boolean; // yes/no -> true/false
  business_context: string; // mandatory in UI
  country: string; // mandatory in UI
  time_horizon: "1_month"; // fixed for now (or make dropdown later)
};


type Section = "personal" | "company" | "targets" | "objective";


type CurrencyRateRow = {
  user_currency: string;       // normalized lower in backend
  country: string;             // normalized lower in backend
  selected_currency: string;   // normalized lower in backend
  conversion_rate: number;
  month: string;
  year: number;
};


const REVENUE_OPTIONS = [
  "",
  "$0 - $50K",
  "$50K - $100K",
  "$100K - $500K",
  "$500K - $1M",
  "$1M+",
];

// ✅ Currency dropdown options
const CURRENCY_OPTIONS = ["USD", "GBP", "INR", "CAD"];


function platformIsConnected(
  platform: PlatformId,
  connected: {
    amazonUk: boolean;
    amazonUs: boolean;
    amazonCa: boolean;
    shopify: boolean;
  }
) {
  switch (platform) {
    case "global":
      return false; // show Global Snapshot always (change to false if you don't want it)
    case "amazon-uk":
      return connected.amazonUk;
    case "amazon-us":
      return connected.amazonUs;
    case "amazon-ca":
      return connected.amazonCa;
    case "shopify":
      return connected.shopify;
    default:
      return false;
  }
}

const PLATFORM_FLAG_META: Partial<
  Record<PlatformId, { label: string; countryCode?: string }>
> = {
  "amazon-us": { label: "Amazon US", countryCode: "US" },
  "amazon-uk": { label: "Amazon UK", countryCode: "GB" },
  "amazon-ca": { label: "Amazon CA", countryCode: "CA" },
  shopify: { label: "Shopify" }, // no flag
};

const PLATFORM_TARGET_META: Partial<
  Record<PlatformId, { marketplace: string; currencySymbol: string }>
> = {
  "amazon-us": { marketplace: "Amazon US", currencySymbol: "$" },
  "amazon-uk": { marketplace: "Amazon UK", currencySymbol: "£" },
  "amazon-ca": { marketplace: "Amazon CA", currencySymbol: "C$" },
  shopify: { marketplace: "Shopify", currencySymbol: "" },
};



const formatK = (n: number, symbol = "") => `${symbol}${Math.round(n / 1000)}K`;


type TargetRow = Row & {
  sno: React.ReactNode;
  marketplace: React.ReactNode;
  targetNative: React.ReactNode; // ✅ native currency
  conversion: React.ReactNode;   // ✅ native -> home
  targetHome: React.ReactNode;   // ✅ home currency (replaces "global")
  __isTotal?: boolean;
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
    <div className="rounded-2xl border border-gray-200 bg-white p-4 dark:border-gray-800 dark:bg-gray-900">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-800 dark:text-white/90">
          {title}
        </h3>
        {action}
      </div>
      {children}
    </div>
  );
}

function InfoItem({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div>
      <p className="mb-1 text-xs text-gray-500 dark:text-gray-400">{label}</p>

      {/* Use div so value can be anything (text, <p>, <span>, etc.) safely */}
      <div className="text-sm font-medium text-gray-800 dark:text-white/90">
        {value}
      </div>
    </div>
  );
}






export default function UserInfoCard() {
  const dispatch = useAppDispatch();
  const [currencyRates, setCurrencyRates] = useState<CurrencyRateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);
  const { isOpen, openModal, closeModal } = useModal();

  // ✅ route param from /country/{range}/{country}/{month}/{year} (may be undefined on profile pages)
  const params = useParams() as { country?: string };

  // ✅ connected marketplaces (from your existing hook)
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
    const c = (params?.country || "").toLowerCase();

    if (c === "uk") return "amazon-uk";
    if (c === "us") return "amazon-us";
    if (c === "ca") return "amazon-ca";
    if (c === "global") return "global";

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

    return "global";
  }, [
    params?.country,
    connected.amazonUk,
    connected.amazonUs,
    connected.amazonCa,
  ]);

  const pageCurrency = useMemo(
    () => platformToCurrencyCode(pagePlatform),
    [pagePlatform]
  );

  const { data, isLoading, isError } = useGetUserDataQuery();


  console.log("User data123:", data);

  const token = useSelector((state: any) => state.auth?.token);

  useEffect(() => {
    if (!token) return; // ✅ wait for redux token

    const fetchRates = async () => {
      try {
        setRatesLoading(true);

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          }
        );

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
    // key: "usd|gb|uk"
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

    // direct
    const direct = rateMap.get(`${f}|${t}|${c}`);
    if (direct != null) return direct;

    // inverse (if only opposite exists)
    const inv = rateMap.get(`${t}|${f}|${c}`);
    if (inv != null && inv !== 0) return 1 / inv;

    // fallback 1 if missing
    return 1;
  };


  const [updateProfile, { isLoading: isSaving }] = useUpdateProfileMutation();

  const [form, setForm] = useState<FormState>({
    name: "",
    brand_name: "",
    company_name: "",
    annual_sales_range: "",
    email: "",
    phone_number: "",
    homeCurrency: "",
    target_sales: "",

    gst_no: "",
    pan_no: "",

    address_building: "",
    address_city: "",
    address_country: "",
    address_state: "",
    address_zipcode: "",
  });

  const [objective, setObjective] = useState<UserObjectiveForm>({
    growth_intent: "aggressive",
    profit_priority: "protect_growth",
    inventory_clearance_priority: false,
    business_context: "",
    country: "",
    time_horizon: "1_month",
  });


  const GROWTH_OPTIONS = ["conservative", "balanced", "aggressive"] as const;

  const PROFIT_OPTIONS: Array<{ label: string; value: UserObjectiveForm["profit_priority"] }> = [
    { label: "Yes Profit is high priority", value: "high" },
    { label: "I'm Okay with current profit if it helps me grow my sales", value: "protect_growth" },
    { label: "I'm Okay with short term losses if it helps in high growth numbers", value: "sacrifice_short_term" },
  ];

  const INVENTORY_DILUTION_OPTIONS = ["yes", "no"] as const;


  // ✅ per-section edit state
  const [activeSection, setActiveSection] = useState<Section>("personal");
  const openSection = (s: Section) => {
    setActiveSection(s);
    openModal();
  };

  const homeCurrencyCode = (
    (data as any)?.homeCurrency ||
    form.homeCurrency ||
    pageCurrency ||
    "USD"
  ).toUpperCase();

  const baseNativeTarget = Number((data as any)?.target_sales ?? 0);

  const monthlyTargetData: TargetRow[] = useMemo(() => {
    const rows: TargetRow[] = connectedPlatformsForTargets.map((pid, idx) => {
      const meta =
        PLATFORM_TARGET_META[pid] ?? { marketplace: String(pid), currencySymbol: "" };

      const nativeCurrency = platformToCurrencyCode(pid) || homeCurrencyCode;
      const country = platformToCountry(pid);

      // ✅ conversion rate (native -> home)
      const nativeToHome = getFxDb(nativeCurrency, homeCurrencyCode, country);

      // ✅ home target = native target * rate
      const homeTarget = baseNativeTarget * nativeToHome;

      return {
        sno: `${idx + 1}.`,
        marketplace: meta.marketplace,
        targetNative: money(baseNativeTarget, nativeCurrency),
        conversion: nativeCurrency === homeCurrencyCode ? "-" : nativeToHome.toFixed(3),
        targetHome: money(homeTarget, homeCurrencyCode),
      };
    });

    // ✅ Total row = sum of all home targets
    if (rows.length) {
      const totalHome = rows.reduce((sum, row: any) => {
        const num = Number(String(row.targetHome).replace(/[^0-9.-]+/g, ""));
        return sum + (Number.isFinite(num) ? num : 0);
      }, 0);

      rows.push({
        sno: "",
        marketplace: "Total",
        targetNative: "",
        conversion: "",
        targetHome: money(totalHome, homeCurrencyCode),
        __isTotal: true,
      });
    }

    return rows;
  }, [connectedPlatformsForTargets, baseNativeTarget, homeCurrencyCode, rateMap]);

  const monthlyTargetColumns: ColumnDef<TargetRow>[] = useMemo(
    () => [
      { key: "sno", header: "S.No.", width: "60px" },
      { key: "marketplace", header: "Marketplace", width: "180px" },
      { key: "targetNative", header: "Target (Native Currency)", width: "220px" },
      { key: "conversion", header: `Conversion Rate ${homeCurrencyCode})`, width: "210px" },
      // ✅ REPLACES "Target (Global Currency)"
      { key: "targetHome", header: `Target (${homeCurrencyCode})`, width: "200px" },
    ],
    [homeCurrencyCode]
  );


  // ✅ compute effective currency AFTER data + form exist
  const effectiveCurrency =
    pageCurrency || form.homeCurrency || (data as any)?.homeCurrency || "USD";

  useEffect(() => {
    if (!data) return;

    const tax = (data as any)?.tax_id ?? {};
    const addr = (data as any)?.address ?? {};

    setForm({
      name: (data as any)?.name ?? "",
      brand_name: data.brand_name ?? "",
      company_name: data.company_name ?? "",
      annual_sales_range: data.annual_sales_range ?? "",
      email: data.email ?? "",
      phone_number: data.phone_number ?? "",
      homeCurrency: (data as any).homeCurrency ?? "",
      target_sales:
        (data as any).target_sales != null
          ? String((data as any).target_sales)
          : "",

      gst_no: tax.gst_no ?? "",
      pan_no: tax.pan_no ?? "",

      address_building: addr.building ?? "",
      address_city: addr.city ?? "",
      address_country: addr.country ?? "",
      address_state: addr.state ?? "",
      address_zipcode: addr.zipcode ?? "",
    });
  }, [data]);




  useEffect(() => {
    if (pageCurrency && !form.homeCurrency) {
      setForm((prev) => ({ ...prev, homeCurrency: pageCurrency }));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pageCurrency]);

  const show = (v?: string | null) => (v && v.trim().length ? v : "-");

  const handleInput =
    (key: keyof FormState) =>
      (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
        setForm((prev) => ({ ...prev, [key]: e.target.value }));
      };

  const buildPayloadBySection = () => {
    if (activeSection === "personal") {
      return {
        name: form.name,
        phone_number: form.phone_number,
      };
    }

    if (activeSection === "company") {
      return {
        brand_name: form.brand_name,
        company_name: form.company_name,
        annual_sales_range: form.annual_sales_range,
        homeCurrency: form.homeCurrency,

        tax_id: {
          gst_no: form.gst_no,
          pan_no: form.pan_no,
        },
        address: {
          building: form.address_building,
          city: form.address_city,
          country: form.address_country,
          state: form.address_state,
          zipcode: form.address_zipcode,
        },

      };
    }

    // ✅ targets
    return {
      target_sales: form.target_sales === "" ? null : Number(form.target_sales),
    };
  };

  const handleSave = async () => {
    try {
      const payload = buildPayloadBySection();
      await updateProfile(payload as any).unwrap();
      dispatch(setUser(payload as any));
      closeModal();
    } catch (err: any) {
      console.error(err);
      alert(err?.data?.message ?? "Failed to update profile.");
    }
  };

  const handleSaveObjective = async () => {
    try {
      // basic validation to prevent backend crash on country None
      if (!objective.country?.trim()) {
        alert("Please select Country.");
        return;
      }
      if (!objective.profit_priority) {
        alert("Please select Profit.");
        return;
      }

      if (!token) {
        alert("Token missing. Please login again.");
        return;
      }

      const payload = {
        country: objective.country.trim().toLowerCase(),
        growth_intent: objective.growth_intent,
        profit_priority: objective.profit_priority,
        inventory_clearance_priority: objective.inventory_clearance_priority,
        business_context: objective.business_context?.trim() || null,
        time_horizon: objective.time_horizon, 
      };


      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/objective`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err?.error || err?.message || "Failed to save objective");
      }

      // store BOTH: backend payload + ui state (so you can show it later)
      localStorage.setItem("user_objective", JSON.stringify(objective));
      localStorage.setItem("user_objective_backend", JSON.stringify(payload));

      closeModal();
    } catch (e: any) {
      console.error(e);
      alert(e?.message || "Failed to save objective");
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
        // ✅ if older/newer payload shapes exist, normalize them:
        profit_priority: parsed.profit_priority ?? parsed.primary_goal ?? prev.profit_priority,
        growth_intent: parsed.growth_intent ?? parsed.risk_level ?? prev.growth_intent,
      }));
    } catch (e) {
      console.error("Failed to parse objective from localStorage");
    }
  }, []);

  useEffect(() => {
    if (!objective.country && integratedCountries.length) {
      setObjective((prev) => ({
        ...prev,
        country: integratedCountries[0],
      }));
    }
  }, [integratedCountries, objective.country]);



  const [forgotPassword, { isLoading: isSending, isSuccess }] =
    useForgotPasswordMutation();

  const handleForgotPassword = async () => {
    if (!data?.email) return;
    try {
      await forgotPassword({ email: data.email }).unwrap();
    } catch (err: any) {
      console.error(err);
      alert(err?.data?.message || "Failed to send reset email.");
    }
  };

  const formatCurrency = (
    value: string | number | null | undefined,
    currency: string
  ) => {
    const n = typeof value === "string" ? Number(value) : value ?? NaN;
    if (!currency || !Number.isFinite(n)) return "-";
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency,
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(n);
  };

  const RISK_LEVELS = ["conservative", "balanced", "aggressive"] as const;

  const riskMeta = {
    conservative: {
      label: "Conservative",
      color: "text-green-600",
      dot: "bg-green-600",
    },
    balanced: {
      label: "Balanced",
      color: "text-yellow-500",
      dot: "bg-yellow-500",
    },
    aggressive: {
      label: "Aggressive",
      color: "text-red-600",
      dot: "bg-red-600",
    },
  };

  const modalTitle =
    activeSection === "personal"
      ? "Edit Personal Info"
      : activeSection === "company"
        ? "Edit Company Info"
        : activeSection === "targets"
          ? "Edit Monthly Targets"
          : "Edit Objective";

  const modalSubtitle =
    activeSection === "personal"
      ? "Update your contact and password settings."
      : activeSection === "company"
        ? "Update your company and business details."
        : activeSection === "targets"
          ? "Update your marketplace targets."
          : "For AI to help and create insights for you, we require you to answer a few questions.";


  return (
    <div className="">
      <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
        <div className="w-full">
          {isLoading && (
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Loading…
            </div>
          )}
          {isError && (
            <div className="text-sm text-red-500">Failed to load profile.</div>
          )}

          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <InfoCard
              title={
                <PageBreadcrumb
                  pageTitle="Personal Info"
                  variant="table"
                  align="left"
                // textSize="base"
                />
              }
              action={
                <button
                  onClick={() => openSection("personal")}
                  className="
      inline-flex items-center justify-center
      h-9 w-9
      text-gray-700
      
    "
                  aria-label="Edit"
                >
                  <FiEdit className="text-lg" />
                </button>
              }
            >
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                <InfoItem label="Name" value={show((data as any)?.name)} />
                <InfoItem label="Email" value={show(data?.email)} />
                <InfoItem label="Phone" value={show(data?.phone_number)} />
                <InfoItem
                  label="Reset Password"
                  value={
                    <span
                      onClick={handleForgotPassword}
                      className={`cursor-pointer ${isSuccess
                        ? "text-green-600 dark:text-green-400"
                        : "text-blue-600 hover:underline"
                        }`}
                    >
                      {isSending
                        ? "Sending..."
                        : isSuccess
                          ? "Email sent for password reset"
                          : "Click here to change password"}
                    </span>
                  }
                />
              </div>
            </InfoCard>

            {/* Company Info */}
            <InfoCard
              title={
                <PageBreadcrumb
                  pageTitle="Company Info"
                  variant="table"
                  align="left"
                // textSize="base"
                />
              }
              action={
                <button
                  onClick={() => openSection("company")}
                  className="
      inline-flex items-center justify-center
      h-9 w-9
      text-gray-700
      
    "
                  aria-label="Edit"
                >
                  <FiEdit className="text-lg" />
                </button>
              }
            >
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
                <InfoItem label="Company Name" value={show(data?.company_name)} />
                <InfoItem label="Brand Name" value={show(data?.brand_name)} />
                <InfoItem
                  label="Home Currency"
                  value={show((data as any)?.homeCurrency)}
                />
                <InfoItem
                  label="Revenue"
                  value={show(data?.annual_sales_range)}
                />
                <InfoItem label="GST No." value={show((data as any)?.tax_id?.gst_no)} />
                <InfoItem label="PAN No." value={show((data as any)?.tax_id?.pan_no)} />
                <InfoItem
                  label="Address"
                  value={(() => {
                    const a = (data as any)?.address || {};
                    const full = [a.building, a.city, a.state, a.country, a.zipcode]
                      .filter(Boolean)
                      .join(", ");
                    return show(full);
                  })()}
                />

              </div>
            </InfoCard>

            {/* Integrations (placeholder) */}
            <div className="lg:col-span-2">
              <InfoCard
                title={
                  <PageBreadcrumb
                    pageTitle="Integrations"
                    variant="table"
                    align="left"
                  // textSize="base"
                  />
                }>
                {(() => {
                  const connectedPlatforms = ALL_PLATFORM_DEFS.filter((p) =>
                    platformIsConnected(p.id, connected)
                  );

                  if (connectedPlatforms.length === 0) {
                    return (
                      <p className="text-sm text-gray-500 dark:text-gray-400">
                        No platforms connected yet.
                      </p>
                    );
                  }

                  return (
                    <>
                      {/* Connected platforms */}
                      <div className="space-y-3">
                        {connectedPlatforms.map((p) => {
                          const meta = PLATFORM_FLAG_META[p.id] ?? { label: p.label };

                          return (
                            <div key={p.id} className="flex items-center gap-3">
                              {meta.countryCode && (
                                <ReactCountryFlag
                                  svg
                                  countryCode={meta.countryCode as any}
                                  className="text-[22px] leading-none"
                                  aria-label={meta.label}
                                />
                              )}

                              <span className="text-sm font-semibold text-gray-800 dark:text-white/90">
                                {meta.label}
                              </span>
                            </div>
                          );
                        })}
                      </div>



                      {/* + Integrate more marketplaces */}
                      <Link
                        href=""
                        className="
    inline-flex items-center gap-2
    text-sm font-semibold text-green-500 dark:text-emerald-400
    mt-4 whitespace-nowrap
    border-b border-transparent
    hover:border-green-500 dark:hover:border-emerald-400
  "
                      >
                        <FaPlus size={12} />
                        <span>Integrate more marketplaces</span>
                      </Link>

                    </>
                  );
                })()}
              </InfoCard>

            </div>

            {/* Row 3 - full width */}
            <div className="lg:col-span-2">
              <InfoCard
                title={
                  <PageBreadcrumb
                    pageTitle="Monthly Targets"
                    variant="table"
                    align="left"
                  // textSize="base"
                  />
                }
                action={
                  <button
                    onClick={() => openSection("targets")}
                    className="
      inline-flex items-center justify-center
      h-9 w-9
      text-gray-700
      
    "
                    aria-label="Edit"
                  >
                    <FiEdit className="text-lg" />
                  </button>
                }

              >
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
                  rowClassName={(row) =>
                    row.__isTotal ? "font-semibold bg-[#D9D9D933]" : ""
                  }
                />
              </InfoCard>
            </div>

            <div className="lg:col-span-2">
              <InfoCard
                title={
                  <PageBreadcrumb
                    pageTitle="Objective"
                    variant="table"
                    align="left"
                  />
                }
                action={
                  <button
                    onClick={() => openSection("objective")}
                    className="h-9 w-9 text-gray-700"
                    aria-label="Edit"
                  >
                    <FiEdit className="text-lg" />
                  </button>
                }
              >
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                  <InfoItem label="Growth" value={objective.growth_intent} />
                  <InfoItem label="Profit" value={objective.profit_priority} />
                  <InfoItem label="Inventory Dilution" value={objective.inventory_clearance_priority ? "Yes" : "No"} />
                  <InfoItem label="Country" value={objective.country?.toUpperCase() || "-"} />
                  <InfoItem
                    label="Business Context"
                    value={
                      objective.business_context ? (
                        <p className="line-clamp-1 text-sm text-gray-800 dark:text-white/90">
                          {objective.business_context}
                        </p>
                      ) : (
                        "-"
                      )
                    }
                  />

                </div>
              </InfoCard>
            </div>


          </div>
        </div>
      </div>

      {/* ---- MODAL ---- */}
      <Modal isOpen={isOpen} onClose={closeModal} className="max-w-[700px] m-4">
        <div className="no-scrollbar relative w-full max-w-[700px] overflow-y-auto  rounded-2xl border border-gray-200 bg-white p-4 dark:bg-gray-900 lg:p-11">
          <div className="px-2 pr-14">
            {/* <h4 className="mb-2 text-2xl font-semibold text-gray-800 dark:text-white/90">
              {modalTitle}
            </h4> */}

            <PageBreadcrumb
              pageTitle={modalTitle}
              variant="table"
              align="left"
              textSize="2xl"
            />

            <p className="mb-6 text-sm text-charcoal-500 ">
              {modalSubtitle}
            </p>
          </div>

          <form className="flex flex-col" onSubmit={(e) => e.preventDefault()}>
            <div className="custom-scrollbar h-full overflow-y-auto px-2 pb-3">
              <div className="mt-2">
                <div className="grid grid-cols-1 gap-x-6 gap-y-5 lg:grid-cols-2">
                  {/* PERSONAL */}
                  {activeSection === "personal" && (
                    <>
                      <div className="col-span-2 lg:col-span-1">
                        <Label>Name</Label>
                        <Input
                          type="text"
                          value={form.name}
                          onChange={handleInput("name")}
                        />
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Email (read-only)</Label>
                        <Input type="text" value={form.email} disabled />
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Phone</Label>
                        <Input
                          type="text"
                          value={form.phone_number}
                          onChange={handleInput("phone_number")}
                        />
                      </div>

                      <div className="col-span-2">
                        <Label>Reset Password</Label>
                        <p
                          onClick={handleForgotPassword}
                          className={`text-sm font-medium cursor-pointer ${isSuccess
                            ? "text-green-600 dark:text-green-400"
                            : "text-blue-600 hover:underline"
                            }`}
                        >
                          {isSending
                            ? "Sending..."
                            : isSuccess
                              ? "Email sent for password reset"
                              : "Click here to change password"}
                        </p>
                      </div>
                    </>
                  )}

                  {/* COMPANY */}
                  {activeSection === "company" && (
                    <>
                      <div className="col-span-2 lg:col-span-1">
                        <Label>Brand Name</Label>
                        <Input
                          type="text"
                          value={form.brand_name}
                          onChange={handleInput("brand_name")}
                        />
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Company Name</Label>
                        <Input
                          type="text"
                          value={form.company_name}
                          onChange={handleInput("company_name")}
                        />
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Revenue</Label>
                        <select
                          value={form.annual_sales_range}
                          onChange={handleInput("annual_sales_range")}
                          className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-800 dark:bg-gray-800 dark:text-gray-200"
                        >
                          <option value="">Select Revenue Range</option>
                          {REVENUE_OPTIONS.filter(Boolean).map((opt) => (
                            <option key={opt} value={opt}>
                              {opt}
                            </option>
                          ))}
                        </select>
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Home Currency</Label>
                        <select
                          value={form.homeCurrency}
                          onChange={handleInput("homeCurrency")}
                          className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-800 dark:bg-gray-800 dark:text-gray-200"
                        >
                          <option value="">Select Currency</option>
                          {CURRENCY_OPTIONS.map((cur) => (
                            <option key={cur} value={cur}>
                              {cur}
                            </option>
                          ))}
                        </select>
                      </div>
                      <div className="col-span-2 lg:col-span-1">
                        <Label>GST No.</Label>
                        <Input type="text" value={form.gst_no} onChange={handleInput("gst_no")} />
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>PAN No.</Label>
                        <Input type="text" value={form.pan_no} onChange={handleInput("pan_no")} />
                      </div>

                      {/* Address block */}
                      <div className="col-span-2">
                        <Label>Address</Label>

                        <div className="grid grid-cols-1 gap-4">
                          {/* Building */}
                          <Input
                            type="text"
                            placeholder="Building No."
                            value={form.address_building}
                            onChange={handleInput("address_building")}
                          />

                          {/* City */}
                          <Input
                            type="text"
                            placeholder="City"
                            value={form.address_city}
                            onChange={handleInput("address_city")}
                          />

                          {/* ✅ 3 columns row: Country / State / Zipcode */}
                          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
                            <Input
                              type="text"
                              placeholder="Country/Region"
                              value={form.address_country}
                              onChange={handleInput("address_country")}
                            />

                            <Input
                              type="text"
                              placeholder="State"
                              value={form.address_state}
                              onChange={handleInput("address_state")}
                            />

                            <Input
                              type="text"
                              placeholder="Zipcode"
                              value={form.address_zipcode}
                              onChange={handleInput("address_zipcode")}
                            />
                          </div>
                        </div>
                      </div>


                    </>
                  )}

                  {activeSection === "targets" && (
                    <>
                      {/* ✅ Edit Monthly Target (same input moved from Company) */}
                      <div className="col-span-2 lg:col-span-1">
                        <Label>Monthly Target ({homeCurrencyCode})</Label>
                        <Input
                          type="number"
                          inputMode="numeric"
                          step={1}
                          min="0"
                          value={form.target_sales}
                          onChange={handleInput("target_sales")}
                        />
                      </div>
                    </>
                  )}
                  {activeSection === "objective" && (
                    <>
                      {/* 1) Country (full width) */}
                      <div className="col-span-2">
                        <Label>Country</Label>
                        <select
                          value={objective.country}
                          onChange={(e) =>
                            setObjective((prev) => ({ ...prev, country: e.target.value }))
                          }
                          className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                          required
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
                      </div>

                      {/* 2) Growth + Inventory in single row on lg (half + half) */}
                      <div className="col-span-2 lg:col-span-1">
                        <Label>Growth</Label>
                        <p className="mb-2 text-xs text-gray-500 dark:text-gray-400">(sub heading)</p>
                        <select
                          value={objective.growth_intent}
                          onChange={(e) =>
                            setObjective((prev) => ({
                              ...prev,
                              growth_intent: e.target.value as UserObjectiveForm["growth_intent"],
                            }))
                          }
                          className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                          required
                        >
                          {GROWTH_OPTIONS.map((v) => (
                            <option key={v} value={v}>
                              {v.charAt(0).toUpperCase() + v.slice(1)}
                            </option>
                          ))}
                        </select>
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Inventory Dilution</Label>
                        <p className="mb-2 text-xs text-gray-500 dark:text-gray-400">(subheading)</p>
                        <select
                          value={objective.inventory_clearance_priority ? "yes" : "no"}
                          onChange={(e) =>
                            setObjective((prev) => ({
                              ...prev,
                              inventory_clearance_priority: e.target.value === "yes",
                            }))
                          }
                          className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                          required
                        >
                          <option value="yes">Yes</option>
                          <option value="no">No</option>
                        </select>
                      </div>

                      {/* 3) Profit (full width) */}
                      <div className="col-span-2">
                        <Label>Profit</Label>
                        <p className="mb-2 text-xs text-gray-500 dark:text-gray-400">(subheading)</p>
                        <select
                          value={objective.profit_priority}
                          onChange={(e) =>
                            setObjective((prev) => ({
                              ...prev,
                              profit_priority: e.target.value as UserObjectiveForm["profit_priority"],
                            }))
                          }
                          className="w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm text-gray-800 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-200"
                          required
                        >
                          {PROFIT_OPTIONS.map((opt) => (
                            <option key={opt.value} value={opt.value}>
                              {opt.label}
                            </option>
                          ))}
                        </select>
                      </div>

                      {/* 4) Business Overview (full width) */}
                      <div className="col-span-2">
                        <Label>Business Overview</Label>
                        <p className="mb-2 text-xs text-gray-500 dark:text-gray-400">(user input for text)</p>
                        <Input
                          type="text"
                          value={objective.business_context}
                          onChange={(e) =>
                            setObjective((prev) => ({ ...prev, business_context: e.target.value }))
                          }
                          required
                        />
                      </div>
                    </>
                  )}


                </div>
              </div>
            </div>

            <div className="flex items-center gap-3 px-2 mt-6 lg:justify-end">
              <Button
                size="sm"
                variant="outline"
                onClick={closeModal}
                disabled={isSaving}
              >
                Close
              </Button>
              <Button
                size="sm"
                onClick={
                  activeSection === "objective" ? handleSaveObjective : handleSave
                }
                disabled={isSaving}
              >

                {isSaving ? "Saving…" : "Save Changes"}
              </Button>
            </div>
          </form>
        </div>
      </Modal>
    </div>
  );
}
