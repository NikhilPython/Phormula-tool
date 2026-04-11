"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { useModal } from "../../hooks/useModal";
import { Modal } from "../ui/modal";
import Button from "../ui/button/Button";
import Input from "../form/input/InputField";
import Label from "../form/Label";
import {
  useForgotPasswordMutation,
  useGetUserDataQuery,
  useUpdateProfileMutation,
  useGetCountriesQuery,
} from "@/lib/api/profileApi";
import { FiEdit, FiCheck, FiX, FiLock } from "react-icons/fi";
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
import { TiUpload } from "react-icons/ti";
import SkuMultiCountryUpload from "../ui/modal/SkuMultiCountryUpload";
import FeepreviewUpload from "../ui/modal/FeepreviewUpload";
import {
  companyInfoSchema,
  getCompanyInfoFieldErrors,
  getPersonalInfoFieldErrors,
  personalInfoSchema,
  sanitizeAlphaNumSpace,
  sanitizeAlphaSpace,
  sanitizePhoneLoose,
  sanitizeUpperAlphaNum,
  type CompanyInfoFormErrors,
  type PersonalInfoFormErrors,
} from "@/lib/validations/authValidation";
import IntegrationToggleButton from "@/features/integration/IntegrationToggleButton";
import { Steps } from "intro.js-react";
import "intro.js/introjs.css";
import { IoMdLock } from "react-icons/io";

type ProfileTab = "personal" | "objectives" | "integrations";

type FormState = {
  name: string;
  brand_name: string;
  company_name: string;
  // annual_sales_range: string;
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
  inventory_clearance_priority: boolean;
  business_context: string;
  country: string;
  time_horizon: "1_month";
};

type Section = "personal" | "company" | "targets" | "objective";

type CurrencyRateRow = {
  user_currency: string;
  country: string;
  selected_currency: string;
  conversion_rate: number;
  month: string;
  year: number;
};

// const REVENUE_OPTIONS = ["", "$0 - $50K", "$50K - $100K", "$100K - $500K", "$500K - $1M", "$1M+"];
const CURRENCY_OPTIONS = ["USD"];

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// function platformIsConnected(
//   platform: PlatformId,
//   connected: {
//     amazonUk: boolean;
//     amazonUs: boolean;
//     amazonCa: boolean;
//     shopify: boolean;
//   }
// ) {
//   switch (platform) {
//     case "global":
//       return false;
//     case "amazon-uk":
//       return connected.amazonUk;
//     case "amazon-us":
//       return connected.amazonUs;
//     case "amazon-ca":
//       return connected.amazonCa;
//     case "shopify":
//       return connected.shopify;
//     default:
//       return false;
//   }
// }

function platformIsConnected(
  platform: PlatformId,
  connected: {
    amazonUk: boolean;
    amazonUs: boolean;
    amazonCa: boolean;
    amazonAds: boolean;
    shopify: boolean;
  }
) {
  switch (platform) {
    case "global":
      return false;
    case "amazon-uk":
      return connected.amazonUk;
    case "amazon-us":
      return connected.amazonUs;
    case "amazon-ca":
      return connected.amazonCa;
    case "amazon-ads":
      return connected.amazonAds;
    case "shopify":
      return connected.shopify;
    default:
      return false;
  }
}

const PLATFORM_FLAG_META: Partial<
  Record<
    PlatformId,
    { label: string; countryCode?: string; image?: string }
  >
> = {
  "amazon-us": { label: "Amazon US", countryCode: "US" },
  "amazon-uk": { label: "Amazon UK", countryCode: "GB" },
  "amazon-ca": { label: "Amazon CA", countryCode: "CA" },
  "amazon-ads": {
    label: "Amazon Ads",
    image: "/images/AmazonAds.jpg",
  },
  shopify: { label: "Shopify" },
};


const PLATFORM_TARGET_META: Partial<Record<PlatformId, { marketplace: string; currencySymbol: string }>> = {
  "amazon-us": { marketplace: "Amazon US", currencySymbol: "$" },
  "amazon-uk": { marketplace: "Amazon UK", currencySymbol: "£" },
  "amazon-ca": { marketplace: "Amazon CA", currencySymbol: "C$" },
  shopify: { marketplace: "Shopify", currencySymbol: "" },
};

type TargetRow = Row & {
  sno: React.ReactNode;
  marketplace: React.ReactNode;
  targetNative: React.ReactNode;
  conversion: React.ReactNode;
  targetHome: React.ReactNode;
  __isTotal?: boolean;
  __pid?: PlatformId;
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
  id,
  title,
  children,
  action,
  disabled = false,
  disabledMessage,
  hideDisabledOverlay = false,
}: {
  id?: string;
  title: React.ReactNode;
  children: React.ReactNode;
  action?: React.ReactNode;
  disabled?: boolean;
  disabledMessage?: string;
  hideDisabledOverlay?: boolean;
}) {
  return (
    <div
      id={id}
      className="relative h-full overflow-hidden rounded-2xl border border-gray-200 bg-white dark:border-gray-800 dark:bg-gray-900"
    >
      <div className="flex items-center justify-between px-4 py-3">
        <h3 className="text-sm font-semibold text-gray-800 dark:text-white/90">{title}</h3>
        {action}
      </div>
      <div className="h-px w-full bg-gray-200 dark:bg-gray-800" />
      <div className="p-4">{children}</div>

      {disabled && !hideDisabledOverlay && (
        <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/45 dark:bg-black/30">
          {disabledMessage ? (
            <div className="flex items-center gap-2 rounded-md bg-white/90 px-3 py-2 text-sm font-medium text-gray-700 shadow-sm dark:bg-gray-800/90 dark:text-gray-200">
              {/* <FiLock className="h-4 w-4 text-gray-600 dark:text-gray-300" /> */}
              <div className="flex justify-center">
                <div className="flex h-6 w-6 items-center justify-center rounded-full  bg-[#37455F]">
                  <IoMdLock className="h-4 w-4 text-[#F8EDCE]" />
                </div>
              </div>
              <span>{disabledMessage}</span>
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}

// function InfoItem({
//   id,
//   label,
//   value,
// }: {
//   id?: string;
//   label: string;
//   value: React.ReactNode;
// }) {
//   return (
//     <div id={id}>
//       <p className="mb-1 text-xs text-gray-500 dark:text-gray-400">{label}</p>
//       <div className="text-sm font-medium text-gray-800 dark:text-white/90">{value}</div>
//     </div>
//   );
// }

interface InfoItemProps {
  id?: string;
  label: string;
  value: React.ReactNode;
  required?: boolean;
}

function InfoItem({ id, label, value, required }: InfoItemProps) {
  return (
    <div id={id}>
      <p className="mb-1 text-xs text-gray-500 dark:text-gray-400">
        {label}
        {required && <span className="ml-1 text-red-500">*</span>}
      </p>
      <div className="text-sm font-medium text-gray-800 dark:text-white/90">{value}</div>
    </div>
  );
}

export default function UserInfoCard({ activeTab = "personal" }: { activeTab?: ProfileTab }) {
  const dispatch = useAppDispatch();
  const suppressTourOnceRef = useRef(false);
  const hasShownTourRef = useRef(false);

  const [isOnboarding, setIsOnboarding] = useState(false);
  const [onboardingStep, setOnboardingStep] = useState<"personal" | "company" | "done">("personal");
  const [currencyRates, setCurrencyRates] = useState<CurrencyRateRow[]>([]);
  const [ratesLoading, setRatesLoading] = useState(false);
  const { isOpen, openModal, closeModal } = useModal();
  const [editingPid, setEditingPid] = useState<PlatformId | null>(null);
  const [draftTarget, setDraftTarget] = useState<string>("");
  const [isTargetEditMode, setIsTargetEditMode] = useState(false);
  const [isObjectiveEditMode, setIsObjectiveEditMode] = useState(false);
  const [isPersonalEditMode, setIsPersonalEditMode] = useState(false);
  const [isCompanyEditMode, setIsCompanyEditMode] = useState(false);
  const [isSkuUploaded, setIsSkuUploaded] = useState(false);
  const [tourEnabled, setTourEnabled] = useState(false);
  const [tourStarted, setTourStarted] = useState(false);
  const [isMarkingStepsSeen, setIsMarkingStepsSeen] = useState(false);

  // const PROFILE_TOUR_SEEN_KEY = "profile_intro_seen";

  const [objective, setObjective] = useState<UserObjectiveForm>({
    growth_intent: "aggressive",
    profit_priority: "protect_growth",
    inventory_clearance_priority: false,
    business_context: "",
    country: "",
    time_horizon: "1_month",
  });

  const [objectiveDraft, setObjectiveDraft] = useState<UserObjectiveForm>(objective);

  const searchParams = useSearchParams();

  const countryName = searchParams.get("countryName") || "global";
  const month = searchParams.get("month") || "NA";
  const year = searchParams.get("year") || "NA";
  const connected = useConnectedPlatforms();

  const connectedPlatformsForTargets = useMemo(() => {
    const ids: PlatformId[] = [];
    if (connected.amazonUs) ids.push("amazon-us");
    if (connected.amazonUk) ids.push("amazon-uk");
    if (connected.amazonCa) ids.push("amazon-ca");
    if (connected.shopify) ids.push("shopify");
    return ids;
  }, [connected.amazonUs, connected.amazonUk, connected.amazonCa, connected.shopify]);

  const [personalTouched, setPersonalTouched] = useState({
    name: false,
    phone_number: false,
  });

  const [companyTouched, setCompanyTouched] = useState({
    brand_name: false,
    company_name: false,
    // annual_sales_range: false,
    homeCurrency: false,
    gst_no: false,
    pan_no: false,
    address_building: false,
    address_city: false,
    address_country: false,
    address_state: false,
    address_zipcode: false,
  });

  const [personalErrors, setPersonalErrors] = useState<PersonalInfoFormErrors>({});
  const [companyErrors, setCompanyErrors] = useState<CompanyInfoFormErrors>({});
  const [showMemberPasswordForm, setShowMemberPasswordForm] = useState(false);
  const [memberPasswordForm, setMemberPasswordForm] = useState({
    old_password: "",
    new_password: "",
    confirm_password: "",
  });

  const [memberPasswordLoading, setMemberPasswordLoading] = useState(false);
  const [memberPasswordMessage, setMemberPasswordMessage] = useState("");
  const [memberPasswordError, setMemberPasswordError] = useState("");

  const handleMemberChangePassword = async () => {
    setMemberPasswordError("");
    setMemberPasswordMessage("");

    const oldPassword = memberPasswordForm.old_password.trim();
    const newPassword = memberPasswordForm.new_password.trim();
    const confirmPassword = memberPasswordForm.confirm_password.trim();

    if (!oldPassword || !newPassword || !confirmPassword) {
      setMemberPasswordError("All password fields are required.");
      return;
    }

    if (newPassword.length < 6) {
      setMemberPasswordError("New password must be at least 6 characters.");
      return;
    }

    if (newPassword !== confirmPassword) {
      setMemberPasswordError("New password and confirm password do not match.");
      return;
    }

    try {
      setMemberPasswordLoading(true);

      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/member_change_password`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          body: JSON.stringify({
            old_password: oldPassword,
            new_password: newPassword,
          }),
        }
      );

      const result = await res.json().catch(() => ({}));

      if (!res.ok) {
        setMemberPasswordError(
          result?.error || result?.message || "Failed to change password."
        );
        return;
      }

      setMemberPasswordMessage(result?.message || "Password changed successfully.");
      setMemberPasswordForm({
        old_password: "",
        new_password: "",
        confirm_password: "",
      });
    } catch (error: any) {
      setMemberPasswordError(error?.message || "Something went wrong.");
    } finally {
      setMemberPasswordLoading(false);
    }
  };

  const integratedCountries = useMemo(() => {
    const countries: string[] = [];
    if (connected.amazonUs) countries.push("us");
    if (connected.amazonUk) countries.push("uk");
    if (connected.amazonCa) countries.push("ca");
    if (connected.shopify) countries.push("global");
    return countries;
  }, [connected]);

  const COUNTRY_DIAL_CODE: Record<string, string> = {
    india: "+91",
    in: "+91",
    united_states: "+1",
    us: "+1",
    usa: "+1",
    united_kingdom: "+44",
    uk: "+44",
    gb: "+44",
    canada: "+1",
    ca: "+1",
  };

  const normalizeCountryKey = (value?: string | null) =>
    String(value || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_");

  const getSelectedDialCode = (country?: string | null) => {
    const key = normalizeCountryKey(country);
    return COUNTRY_DIAL_CODE[key] || "";
  };

  const stripDialCode = (phone?: string) =>
    String(phone || "")
      .replace(/^\+\d+\s*/, "")
      .trim();

  const pagePlatform: PlatformId = useMemo(() => {
    const c = countryName.toLowerCase();

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
  }, [countryName, connected.amazonUk, connected.amazonUs, connected.amazonCa]);

  const pageCurrency = useMemo(() => platformToCurrencyCode(pagePlatform), [pagePlatform]);

  // const { data, isLoading, isError } = useGetUserDataQuery();
  // const { data, isLoading, isError, refetch } = useGetUserDataQuery();

  const [data, setData] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isError, setIsError] = useState(false);
  const [isSaving, setIsSaving] = useState(false);

  const isPersonalComplete =
    !!String((data as any)?.name || "").trim() &&
    !!String((data as any)?.phone_number || "").trim();

  const addr = (data as any)?.address ?? {};
  const isCompanyComplete =
    !!String((data as any)?.company_name || "").trim() &&
    !!String((data as any)?.brand_name || "").trim() &&
    !!String((data as any)?.homeCurrency || "").trim() &&
    !!String(addr.building || "").trim() &&
    !!String(addr.city || "").trim() &&
    !!String(addr.state || "").trim() &&
    !!String(addr.country || "").trim() &&
    !!String(addr.zipcode || "").trim();

  const isAmazonConnected =
    connected.amazonUk || connected.amazonUs || connected.amazonCa;

  const hasSkuSheet = Boolean(data?.sku_sheet_exists);
  const hasIntegration =
    connected.amazonUk || connected.amazonUs || connected.amazonCa || connected.shopify;

  const canAccessProductControls = isCompanyComplete || hasSkuSheet;
  const canAccessIntegrations = hasSkuSheet || hasIntegration;

  // const canAccessProductControls = isCompanyComplete;
  // const hasSkuSheet = Boolean(data?.sku_sheet_exists);
  // const canAccessIntegrations = isCompanyComplete && hasSkuSheet;

  const isMemberUser = Boolean((data as any)?.is_member);
  const token = useSelector((state: any) => state.auth?.token);
  // const [updateProfile, { isLoading: isSaving }] = useUpdateProfileMutation();
  // const [forgotPassword, { isLoading: isSending, isSuccess }] = useForgotPasswordMutation();

  // const feeModal = useModal();
  // const skuModal = useModal();
  // const [selectedCountry, setSelectedCountry] = React.useState<string | null>(null);

  // const { data: countriesRes } = useGetCountriesQuery();
  // const countries: string[] = countriesRes?.countries ?? [];

  const fetchUserData = async () => {
    if (!token) return;

    try {
      setIsLoading(true);
      setIsError(false);

      const res = await fetch(`${API_BASE}/get_user_data`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      const result = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(result?.error || result?.message || "Failed to load profile");
      }

      if (!result || typeof result !== "object") {
        throw new Error("Invalid profile response");
      }

      setData(result);
      setIsSkuUploaded(Boolean(result?.sku_sheet_exists));

      const tax = result?.tax_id ?? {};
      const addr = result?.address ?? {};

      setForm((prev) => ({
        ...prev,
        name: result?.name ?? prev.name,
        brand_name: result?.brand_name ?? prev.brand_name,
        company_name: result?.company_name ?? prev.company_name,
        email: result?.email ?? prev.email,
        phone_number: result?.phone_number ?? prev.phone_number,
        homeCurrency: result?.homeCurrency ?? prev.homeCurrency,
        target_sales:
          result?.target_sales != null ? String(result.target_sales) : prev.target_sales,
        gst_no: tax?.gst_no ?? prev.gst_no,
        pan_no: tax?.pan_no ?? prev.pan_no,
        address_building: addr?.building ?? prev.address_building,
        address_city: addr?.city ?? prev.address_city,
        address_country: addr?.country ?? prev.address_country,
        address_state: addr?.state ?? prev.address_state,
        address_zipcode: addr?.zipcode ?? prev.address_zipcode,
      }));
    } catch (error) {
      console.error(error);
      setIsError(true);
    } finally {
      setIsLoading(false);
    }
  };

  const postProfileUpdate = async (payload: any) => {
    if (!token) {
      throw new Error("Token missing. Please login again.");
    }

    const res = await fetch(`${API_BASE}/profileupdate`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(payload),
    });

    const result = await res.json().catch(() => ({}));

    if (!res.ok) {
      throw new Error(result?.error || result?.message || "Failed to update profile");
    }

    return result;
  };

  useEffect(() => {
    fetchUserData();
  }, [token]);

  const [forgotPassword, { isLoading: isSending, isSuccess }] = useForgotPasswordMutation();

  const feeModal = useModal();
  const skuModal = useModal();
  const [selectedCountry, setSelectedCountry] = React.useState<string | null>(null);

  const [form, setForm] = useState<FormState>({
    name: "",
    brand_name: "",
    company_name: "",
    // annual_sales_range: "",
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

  // const selectedCountryForPhone =
  //   form.address_country || data?.address?.country || "";

  // const selectedDialCode = getSelectedDialCode(selectedCountryForPhone);

  // const displayedPhone = form.phone_number
  //   ? selectedDialCode
  //     ? `${selectedDialCode} ${stripDialCode(form.phone_number)}`
  //     : form.phone_number
  //   : "";

  const getDialCodeFromPhone = (phone?: string | null) => {
    const value = String(phone || "").trim();

    if (value.startsWith("+1")) return "+1";
    if (value.startsWith("+91")) return "+91";
    if (value.startsWith("+44")) return "+44";
    if (value.startsWith("+61")) return "+61";

    return "";
  };

  const stripKnownDialCode = (phone?: string | null) => {
    const value = String(phone || "").trim();

    return value
      .replace(/^\+(1|91|44|61)\s*/, "") // remove country code
      .replace(/\s+/g, ""); // ❗ remove ALL spaces
  };

  const selectedDialCode = getDialCodeFromPhone(form.phone_number);

  const displayedPhone = form.phone_number
    ? selectedDialCode
      ? `${selectedDialCode} ${stripKnownDialCode(form.phone_number)}`
      : form.phone_number.replace(/\s+/g, "")
    : "";

  const GROWTH_OPTIONS = ["conservative", "balanced", "aggressive"] as const;

  const PROFIT_OPTIONS: Array<{ label: string; value: UserObjectiveForm["profit_priority"] }> = [
    { label: "Yes Profit is high priority", value: "high" },
    { label: "I'm Okay with current profit if it helps me grow my sales", value: "protect_growth" },
    { label: "I'm Okay with short term losses if it helps in high growth numbers", value: "sacrifice_short_term" },
  ];

  const [activeSection, setActiveSection] = useState<Section>("personal");
  const [tourKey, setTourKey] = useState(0);

  const [tourPhase, setTourPhase] = useState<"overview" | "company-form">("overview");

  const homeCurrencyCode = ((data as any)?.homeCurrency || form.homeCurrency || pageCurrency || "USD").toUpperCase();

  const baseNativeTarget = Number(
    form.target_sales !== "" ? form.target_sales : (data as any)?.target_sales ?? 0
  );

  const show = (v?: string | null) => (v && v.trim().length ? v : "-");

  const startPersonalEdit = () => setIsPersonalEditMode(true);

  const cancelPersonalEdit = () => {
    setIsPersonalEditMode(false);
    setPersonalErrors({});
    setPersonalTouched({
      name: false,
      phone_number: false,
    });

    if (data) {
      setForm((prev) => ({
        ...prev,
        name: data?.name ?? "",
        phone_number: data?.phone_number ?? "",
      }));
    }
  };

  useEffect(() => {
    if (!data || activeTab !== "personal") return;

    const alreadySeen = data?.steps_exists === true;

    // stop forever once Amazon is connected
    if (isAmazonConnected) return;

    // backend already marked steps as done
    if (alreadySeen) return;

    // do not retrigger after edit/save in same session
    if (suppressTourOnceRef.current) return;

    // only once per page load / login
    if (hasShownTourRef.current) return;

    const timer = setTimeout(() => {
      const personalEl = document.querySelector("#tour-personal-info");
      const companyEl = document.querySelector("#tour-company-info");
      const productEl = document.querySelector("#tour-product-controls");
      const skuUploadEl = document.querySelector("#tour-sku-upload-icon");
      const integrationsEl = document.querySelector("#tour-integrations");
      const integrationIconEl = document.querySelector("#tour-integration-icon");

      if (
        !personalEl ||
        !companyEl ||
        !productEl ||
        !skuUploadEl ||
        !integrationsEl ||
        !integrationIconEl
      ) {
        return;
      }

      hasShownTourRef.current = true;

      setTourEnabled(false);
      setTourPhase("overview");
      setTourKey((k) => k + 1);

      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          setTourStarted(true);
          setTourEnabled(true);
        });
      });
    }, 700);

    return () => clearTimeout(timer);
  }, [data, activeTab, isAmazonConnected]);

  useEffect(() => {
    if (!data) return;
    if (!isAmazonConnected) return;
    if (data?.steps_exists) return;

    const syncStepsSeen = async () => {
      try {
        await postProfileUpdate({ steps_exists: true });

        setData((prev: any) => ({
          ...prev,
          steps_exists: true,
        }));

        setTourEnabled(false);
        setTourStarted(false);
      } catch (error) {
        console.error("Failed to update steps_exists after Amazon connection", error);
      }
    };

    syncStepsSeen();
  }, [data, isAmazonConnected]);

  // const startCompanyEdit = () => {
  //   setIsCompanyEditMode(true);

  //   setTourEnabled(false);
  //   setTourPhase("company-form");
  //   setTourKey((k) => k + 1);

  //   setTimeout(() => {
  //     setTourEnabled(true);
  //   }, 150);
  // };

  const startCompanyEdit = () => {
    setIsCompanyEditMode(true);
    setTourEnabled(false);
  };

  // const introSteps = useMemo(() => {
  //   if (tourPhase === "overview") {
  //     return [
  //       {
  //         element: "#tour-personal-edit",
  //         intro: "Click here if you want to update your personal details.",
  //         position: "left",
  //       },
  //       {
  //         element: "#tour-personal-info",
  //         intro: "You can view and edit your personal details here in Personal Info.",
  //         position: "bottom",
  //       },
  //       {
  //         element: "#tour-personal-edit",
  //         intro: "Click here if you want to update your personal details.",
  //         position: "left",
  //       },
  //       {
  //         element: "#tour-company-info",
  //         intro: "This is your Company Info section. You need to complete these details before moving to the next setup steps.",
  //         position: "bottom",
  //       },
  //       {
  //         element: "#tour-company-edit",
  //         intro: "Click this edit icon to start filling in your company details.",
  //         position: "left",
  //       },
  //       {
  //         element: "#tour-product-controls",
  //         intro: "This is Product & Inventory Controls.",
  //         position: "top",
  //       },
  //       {
  //         element: "#tour-sku-upload-icon",
  //         intro: "Click on this icon to upload your SKU sheet.",
  //         position: "bottom",
  //       },
  //       {
  //         element: "#tour-integrations",
  //         intro: "Integrations: here you can see your integrated platforms.",
  //         position: "top",
  //       },
  //       {
  //         element: "#tour-integration-icon",
  //         intro: "Click on the integration icon to connect your platform to our tool.",
  //         position: "left",
  //       },
  //     ];
  //   }

  //   // if (tourPhase === "company-form") {
  //   //   return [
  //   //     {
  //   //       element: "#tour-company-name",
  //   //       intro: "Enter your company name here.",
  //   //       position: "bottom",
  //   //     },
  //   //     {
  //   //       element: "#tour-brand-name",
  //   //       intro: "Enter your brand name here.",
  //   //       position: "bottom",
  //   //     },
  //   //     {
  //   //       element: "#tour-home-currency",
  //   //       intro: "Select your home currency here.",
  //   //       position: "bottom",
  //   //     },
  //   //   ];
  //   // }

  //   return [];
  // }, [tourPhase]);

  const baseOverviewSteps = [
    {
      element: "#tour-personal-edit",
      intro: "Click here if you want to update your personal details.",
      position: "left",
    },
    {
      element: "#tour-personal-info",
      intro: "You can view and edit your personal details here in Personal Info.",
      position: "bottom",
    },
    // {
    //   element: "#tour-personal-edit",
    //   intro: "Click here if you want to update your personal details.",
    //   position: "left",
    // },
    {
      element: "#tour-company-info",
      intro: "This is your Company Info section. You need to complete these details before moving to the next setup steps.",
      position: "bottom",
    },
    // {
    //   element: "#tour-company-edit",
    //   intro: "Click this edit icon to start filling in your company details.",
    //   position: "left",
    // },
    {
      element: "#tour-product-controls",
      intro: "This is Product & Inventory Controls.",
      position: "top",
    },
    // {
    //   element: "#tour-sku-upload-icon",
    //   intro: "Click on this icon to upload your SKU sheet.",
    //   position: "bottom",
    // },
    {
      element: "#tour-integrations",
      intro: "Integrations: here you can see your integrated platforms.",
      position: "top",
    },
    // {
    //   element: "#tour-integration-icon",
    //   intro: "Click on the integration icon to connect your platform to our tool.",
    //   position: "left",
    // },
  ];

  const introSteps = useMemo(() => {
    if (tourPhase === "overview") {
      const total = baseOverviewSteps.length;

      return baseOverviewSteps.map((step, index) => ({
        ...step,
        intro: `
        <div class="mb-2 text-sm font-semibold text-charcoal-500">
          Step ${index} of ${total - 1}
        </div>
        <div>${step.intro}</div>
      `,
      }));
    }

    return [];
  }, [tourPhase]);

  const cancelCompanyEdit = () => {
    setIsCompanyEditMode(false);
    setCompanyErrors({});
    setCompanyTouched({
      brand_name: false,
      company_name: false,
      homeCurrency: false,
      gst_no: false,
      pan_no: false,
      address_building: false,
      address_city: false,
      address_country: false,
      address_state: false,
      address_zipcode: false,
    });

    if (data) {
      const tax = data?.tax_id ?? {};
      const addr = data?.address ?? {};

      setForm((prev) => ({
        ...prev,
        brand_name: data?.brand_name ?? "",
        company_name: data?.company_name ?? "",
        homeCurrency: data?.homeCurrency ?? "",
        gst_no: tax?.gst_no ?? "",
        pan_no: tax?.pan_no ?? "",
        address_building: addr?.building ?? "",
        address_city: addr?.city ?? "",
        address_country: addr?.country ?? "",
        address_state: addr?.state ?? "",
        address_zipcode: addr?.zipcode ?? "",
      }));
    }
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

  const startEditTarget = (pid: PlatformId) => {
    setEditingPid(pid);
    setDraftTarget(String(baseNativeTarget || ""));
  };

  const cancelEditTarget = () => {
    setEditingPid(null);
    setDraftTarget("");
  };

  const openFeePreview = (country: string) => {
    setSelectedCountry(country);
    feeModal.openModal();
  };

  const closeFeePreview = () => {
    setSelectedCountry(null);
    feeModal.closeModal();
  };

  const openSection = (s: Section) => {
    if (isMemberUser) return;
    setActiveSection(s);
    openModal();
  };

  const handleInput =
    (key: keyof FormState) =>
      (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
        setForm((prev) => ({ ...prev, [key]: e.target.value }));
      };

  const handlePersonalChange =
    (key: "name" | "phone_number") =>
      (e: React.ChangeEvent<HTMLInputElement>) => {
        let value = e.target.value;

        if (key === "name") value = sanitizeAlphaSpace(value);
        if (key === "phone_number") value = sanitizePhoneLoose(value);

        setForm((prev) => ({ ...prev, [key]: value }));

        if (personalTouched[key]) {
          validatePersonalField(key, { [key]: value });
        }
      };

  const handleCompanyChange =
    (
      key:
        | "brand_name"
        | "company_name"
        | "gst_no"
        | "pan_no"
        | "address_building"
        | "address_city"
        | "address_country"
        | "address_state"
        | "address_zipcode"
        // | "annual_sales_range"
        | "homeCurrency"
    ) =>
      (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
        let value = e.target.value;

        if (key === "brand_name" || key === "company_name") {
          value = sanitizeAlphaNumSpace(value);
        }

        if (key === "gst_no" || key === "pan_no") {
          value = sanitizeUpperAlphaNum(value);
        }

        if (
          key === "address_building" ||
          key === "address_city" ||
          key === "address_country" ||
          key === "address_state"
        ) {
          value = sanitizeAlphaNumSpace(value);
        }

        if (key === "address_zipcode") {
          value = value.replace(/[^A-Za-z0-9\- ]/g, "");
        }

        setForm((prev) => ({ ...prev, [key]: value }));

        if (companyTouched[key]) {
          validateCompanyField(key, { [key]: value });
        }
      };

  const validatePersonal = (values = { name: form.name, phone_number: form.phone_number }) => {
    const result = personalInfoSchema.safeParse(values);

    if (result.success) {
      setPersonalErrors({});
      return { success: true as const };
    }

    const errs = getPersonalInfoFieldErrors(result.error);
    setPersonalErrors(errs);
    return { success: false as const, errors: errs };
  };

  const validatePersonalField = (
    field: keyof PersonalInfoFormErrors,
    nextValues?: Partial<{ name: string; phone_number: string }>
  ) => {
    const merged = {
      name: form.name,
      phone_number: form.phone_number,
      ...nextValues,
    };

    const result = personalInfoSchema.safeParse(merged);

    if (result.success) {
      setPersonalErrors((prev) => ({ ...prev, [field]: undefined }));
      return;
    }

    const errs = getPersonalInfoFieldErrors(result.error);
    setPersonalErrors((prev) => ({ ...prev, [field]: errs[field] }));
  };

  const validateCompany = (
    values = {
      brand_name: form.brand_name,
      company_name: form.company_name,
      // annual_sales_range: form.annual_sales_range,
      homeCurrency: form.homeCurrency,
      gst_no: form.gst_no,
      pan_no: form.pan_no,
      address_building: form.address_building,
      address_city: form.address_city,
      address_country: form.address_country,
      address_state: form.address_state,
      address_zipcode: form.address_zipcode,
    }
  ) => {
    const result = companyInfoSchema.safeParse(values);

    if (result.success) {
      setCompanyErrors({});
      return { success: true as const };
    }

    const errs = getCompanyInfoFieldErrors(result.error);
    setCompanyErrors(errs);
    return { success: false as const, errors: errs };
  };

  const validateCompanyField = (
    field: keyof CompanyInfoFormErrors,
    nextValues?: Partial<{
      brand_name: string;
      company_name: string;
      // annual_sales_range: string;
      homeCurrency: string;
      gst_no: string;
      pan_no: string;
      address_building: string;
      address_city: string;
      address_country: string;
      address_state: string;
      address_zipcode: string;
    }>
  ) => {
    const merged = {
      brand_name: form.brand_name,
      company_name: form.company_name,
      // annual_sales_range: form.annual_sales_range,
      homeCurrency: form.homeCurrency,
      gst_no: form.gst_no,
      pan_no: form.pan_no,
      address_building: form.address_building,
      address_city: form.address_city,
      address_country: form.address_country,
      address_state: form.address_state,
      address_zipcode: form.address_zipcode,
      ...nextValues,
    };

    const result = companyInfoSchema.safeParse(merged);

    if (result.success) {
      setCompanyErrors((prev) => ({ ...prev, [field]: undefined }));
      return;
    }

    const errs = getCompanyInfoFieldErrors(result.error);
    setCompanyErrors((prev) => ({ ...prev, [field]: errs[field] }));
  };

  const buildPayloadBySection = () => {
    if (activeSection === "personal") {
      return { name: form.name, phone_number: form.phone_number };
    }

    if (activeSection === "company") {
      return {
        brand_name: form.brand_name,
        company_name: form.company_name,
        // annual_sales_range: form.annual_sales_range,
        homeCurrency: form.homeCurrency,
        tax_id: { gst_no: form.gst_no, pan_no: form.pan_no },
        address: {
          building: form.address_building,
          city: form.address_city,
          country: form.address_country,
          state: form.address_state,
          zipcode: form.address_zipcode,
        },
      };
    }

    return { target_sales: form.target_sales === "" ? null : Number(form.target_sales) };
  };

  // const handleSave = async () => {
  //   try {
  //     const payload = buildPayloadBySection();
  //     await updateProfile(payload as any).unwrap();
  //     await refetch();
  //     closeModal();
  //   } catch (err: any) {
  //     console.error(err);
  //     alert(err?.data?.message ?? "Failed to update profile.");
  //   }
  // };

  const handleSave = async () => {
    try {
      setIsSaving(true);
      const payload = buildPayloadBySection();
      await postProfileUpdate(payload);
      await fetchUserData();
      closeModal();
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to update profile.");
    } finally {
      setIsSaving(false);
    }
  };

  // const handleSavePersonal = async () => {
  //   setPersonalTouched({
  //     name: true,
  //     phone_number: true,
  //   });

  //   const result = validatePersonal();
  //   if (!result.success) return;

  //   try {
  //     await updateProfile({
  //       name: form.name.trim(),
  //       phone_number: form.phone_number.trim(),
  //     }).unwrap();

  //     await refetch();

  //     setIsPersonalEditMode(false);

  //     if (isOnboarding) {
  //       setOnboardingStep("company");
  //       setIsCompanyEditMode(true);
  //     }
  //   } catch (err: any) {
  //     console.error(err);
  //     alert(err?.data?.error || err?.data?.message || "Failed to update personal info");
  //   }
  // };

  // const handleSaveCompany = async () => {
  //   setCompanyTouched({
  //     brand_name: true,
  //     company_name: true,
  //     homeCurrency: true,
  //     gst_no: true,
  //     pan_no: true,
  //     address_building: true,
  //     address_city: true,
  //     address_country: true,
  //     address_state: true,
  //     address_zipcode: true,
  //   });

  //   const result = validateCompany();
  //   if (!result.success) return;

  //   try {
  //     const payload = {
  //       brand_name: form.brand_name.trim(),
  //       company_name: form.company_name.trim(),
  //       homeCurrency: form.homeCurrency,
  //       tax_id: {
  //         gst_no: form.gst_no.trim(),
  //         pan_no: form.pan_no.trim(),
  //       },
  //       address: {
  //         building: form.address_building.trim(),
  //         city: form.address_city.trim(),
  //         country: form.address_country.trim(),
  //         state: form.address_state.trim(),
  //         zipcode: form.address_zipcode.trim(),
  //       },
  //     };

  //     await updateProfile(payload as any).unwrap();
  //     dispatch(setUser(payload as any));

  //     setIsCompanyEditMode(false);

  //     if (isOnboarding) {
  //       setIsOnboarding(false);
  //       setOnboardingStep("done");
  //       localStorage.setItem("profile_onboarding_complete", "true");
  //     }
  //   } catch (err: any) {
  //     console.error(err);
  //     alert("Failed to update company info");
  //   }
  // };

  const handleSavePersonal = async () => {
    setPersonalTouched({
      name: true,
      phone_number: true,
    });

    const result = validatePersonal();
    if (!result.success) return;

    try {
      setIsSaving(true);
      suppressTourOnceRef.current = true;

      await postProfileUpdate({
        name: form.name.trim(),
        phone_number: form.phone_number.trim(),
      });

      await fetchUserData();
      setIsPersonalEditMode(false);

      if (isOnboarding) {
        setOnboardingStep("company");
        setIsCompanyEditMode(true);
      }
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to update personal info");
    } finally {
      setIsSaving(false);
    }
  };

  const handleSaveCompany = async () => {
    setCompanyTouched({
      brand_name: true,
      company_name: true,
      homeCurrency: true,
      gst_no: true,
      pan_no: true,
      address_building: true,
      address_city: true,
      address_country: true,
      address_state: true,
      address_zipcode: true,
    });

    const result = validateCompany();
    if (!result.success) return;

    try {
      setIsSaving(true);
      suppressTourOnceRef.current = true;

      const payload = {
        brand_name: form.brand_name.trim(),
        company_name: form.company_name.trim(),
        homeCurrency: form.homeCurrency,
        tax_id: {
          gst_no: form.gst_no.trim(),
          pan_no: form.pan_no.trim(),
        },
        address: {
          building: form.address_building.trim(),
          city: form.address_city.trim(),
          country: form.address_country.trim(),
          state: form.address_state.trim(),
          zipcode: form.address_zipcode.trim(),
        },
      };

      await postProfileUpdate(payload);
      await fetchUserData();
      setIsCompanyEditMode(false);

      if (isOnboarding) {
        setIsOnboarding(false);
        setOnboardingStep("done");
        localStorage.setItem("profile_onboarding_complete", "true");
      }
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to update company info");
    } finally {
      setIsSaving(false);
    }
  };

  const saveInlineTarget = async () => {
    const next = Number(draftTarget);

    if (!Number.isFinite(next) || next < 0) {
      alert("Please enter a valid number.");
      return;
    }

    try {
      setIsSaving(true);
      setForm((prev) => ({ ...prev, target_sales: String(next) }));
      await postProfileUpdate({ target_sales: next });
      await fetchUserData();
      setIsTargetEditMode(false);
      setEditingPid(null);
      setDraftTarget("");
    } catch (err: any) {
      console.error(err);
      alert(err?.message || "Failed to update target.");
    } finally {
      setIsSaving(false);
    }
  };

  const handleSaveObjective = async () => {
    try {
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

      localStorage.setItem("user_objective", JSON.stringify(objective));
      localStorage.setItem("user_objective_backend", JSON.stringify(payload));

      closeModal();
    } catch (e: any) {
      console.error(e);
      alert(e?.message || "Failed to save objective");
    }
  };

  const handleForgotPassword = async () => {
    if (!(data as any)?.email) return;
    try {
      await forgotPassword({ email: (data as any).email }).unwrap();
    } catch (err: any) {
      console.error(err);
      alert(err?.data?.message || "Failed to send reset email.");
    }
  };

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

  const monthlyTargetData: TargetRow[] = useMemo(() => {
    const rows: TargetRow[] = connectedPlatformsForTargets.map((pid, idx) => {
      const meta = PLATFORM_TARGET_META[pid] ?? { marketplace: String(pid), currencySymbol: "" };
      const nativeCurrency = platformToCurrencyCode(pid) || homeCurrencyCode;
      const country = platformToCountry(pid);
      const nativeToHome = getFxDb(nativeCurrency, homeCurrencyCode, country);
      const homeTarget = baseNativeTarget * nativeToHome;

      return {
        __pid: pid,
        sno: `${idx + 1}.`,
        marketplace: meta.marketplace,
        targetNative: money(baseNativeTarget, nativeCurrency),
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
  }, [connectedPlatformsForTargets, baseNativeTarget, homeCurrencyCode, rateMap]);


  // useEffect(() => {
  //   if (!data) return;

  //   const tax = (data as any)?.tax_id ?? {};
  //   const addr = (data as any)?.address ?? {};

  //   setForm({
  //     name: (data as any)?.name ?? "",
  //     brand_name: (data as any)?.brand_name ?? "",
  //     company_name: (data as any)?.company_name ?? "",
  //     // annual_sales_range: (data as any)?.annual_sales_range ?? "",
  //     email: (data as any)?.email ?? "",
  //     phone_number: (data as any)?.phone_number ?? "",
  //     homeCurrency: (data as any)?.homeCurrency ?? "",
  //     target_sales: (data as any)?.target_sales != null ? String((data as any)?.target_sales) : "",
  //     gst_no: tax.gst_no ?? "",
  //     pan_no: tax.pan_no ?? "",
  //     address_building: addr.building ?? "",
  //     address_city: addr.city ?? "",
  //     address_country: addr.country ?? "",
  //     address_state: addr.state ?? "",
  //     address_zipcode: addr.zipcode ?? "",
  //   });
  // }, [data]);

  useEffect(() => {
    if (pageCurrency && !form.homeCurrency) {
      setForm((prev) => ({ ...prev, homeCurrency: pageCurrency }));
    }
  }, [pageCurrency, form.homeCurrency]);

  useEffect(() => {
    if (isMemberUser) {
      setIsPersonalEditMode(false);
      setIsCompanyEditMode(false);
      setIsTargetEditMode(false);
      setIsObjectiveEditMode(false);
      setEditingPid(null);
    }
  }, [isMemberUser]);

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
    } catch {
      console.error("Failed to parse objective from localStorage");
    }
  }, []);

  useEffect(() => {
    if (!objective.country && integratedCountries.length) {
      setObjective((prev) => ({ ...prev, country: integratedCountries[0] }));
    }
  }, [integratedCountries, objective.country]);

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

  const markStepsSeen = async () => {
    if (isMarkingStepsSeen || data?.steps_exists) return;

    try {
      setIsMarkingStepsSeen(true);

      await postProfileUpdate({ steps_exists: true });

      setData((prev: any) => ({
        ...prev,
        steps_exists: true,
      }));

      setTourStarted(false);
    } catch (e) {
      console.error("Failed to update steps_exists", e);
    } finally {
      setIsMarkingStepsSeen(false);
    }
  };

  return (
    <div className="">
      <Steps
        key={`${tourPhase}-${tourKey}`}
        enabled={tourEnabled}
        steps={introSteps}
        initialStep={0}
        onExit={() => {
          setTourEnabled(false);
        }}

        onComplete={() => {
          setTourEnabled(false);
        }}
        options={{
          showProgress: true,
          showBullets: false,
          exitOnOverlayClick: false,
          exitOnEsc: false,
          nextLabel: "Next",
          prevLabel: "Back",
          doneLabel: "Done",
          progress: "Step {{current}} of {{total}}",
        }}
      />
      <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
        <div className="w-full">
          {isLoading && <div className="text-sm text-gray-500 dark:text-gray-400">Loading…</div>}
          {isError && <div className="text-sm text-red-500">Failed to load profile.</div>}

          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 lg:items-stretch">
            {activeTab === "personal" && (
              <>
                <div >
                  <InfoCard
                    id="tour-personal-info"
                    title={<PageBreadcrumb pageTitle="Personal Info" variant="table" align="left" />}
                    action={
                      isMemberUser ? null : !isPersonalEditMode ? (
                        <button
                          id="tour-personal-edit"
                          onClick={startPersonalEdit}
                          type="button"
                          className="flex h-9 w-9 items-center justify-center rounded-md text-gray-700 hover:bg-gray-100"
                        >
                          <FiEdit className="h-4 w-4" />
                        </button>
                      ) : (
                        <div className="flex items-center gap-2">
                          <Button size="icon" onClick={handleSavePersonal}>
                            <FiCheck />
                          </Button>
                          <Button size="icon" variant="outline" onClick={cancelPersonalEdit}>
                            <FiX />
                          </Button>
                        </div>
                      )
                    }
                  >
                    <div className="grid grid-cols-2 gap-4">
                      <InfoItem
                        label="Name"
                        required
                        value={
                          isPersonalEditMode ? (
                            <div>
                              <Input
                                type="text"
                                value={form.name}
                                onChange={handlePersonalChange("name")}
                                onBlur={() => {
                                  setPersonalTouched((prev) => ({ ...prev, name: true }));
                                  validatePersonalField("name");
                                }}
                                maxLength={50}
                                error={!!(personalTouched.name && personalErrors.name)}
                              />
                              {personalTouched.name && personalErrors.name && (
                                <p className="mt-1.5 text-xs text-red-500">{personalErrors.name}</p>
                              )}
                            </div>
                          ) : (
                            show(form.name)
                          )
                        }
                      />

                      <InfoItem label="Email" value={show(form.email)} />

                      <InfoItem
                        label="Phone"
                        required
                        value={
                          isPersonalEditMode ? (
                            <div>
                              <Input
                                type="text"
                                value={displayedPhone}
                                onChange={(e) => {
                                  const rawValue = e.target.value.trim();

                                  let nextValue = rawValue;

                                  if (selectedDialCode && rawValue.startsWith(selectedDialCode)) {
                                    const localNumber = rawValue.slice(selectedDialCode.length).trim();
                                    nextValue = `${selectedDialCode}${localNumber}`;
                                  }

                                  handlePersonalChange("phone_number")({
                                    target: { value: nextValue },
                                  } as React.ChangeEvent<HTMLInputElement>);
                                }}
                                onBlur={() => {
                                  setPersonalTouched((prev) => ({ ...prev, phone_number: true }));
                                  validatePersonalField("phone_number");
                                }}
                                maxLength={20}
                                inputMode="tel"
                                error={!!(personalTouched.phone_number && personalErrors.phone_number)}
                              />
                              {personalTouched.phone_number && personalErrors.phone_number && (
                                <p className="mt-1.5 text-xs text-red-500">{personalErrors.phone_number}</p>
                              )}
                            </div>
                          ) : (
                            show(displayedPhone)
                          )
                        }
                      />

                      <div className="">
                        <p className="mb-1 text-xs text-gray-500 dark:text-gray-400">
                          {isMemberUser ? "Change Password" : "Reset Password"}
                        </p>

                        {!isMemberUser ? (
                          <button
                            type="button"
                            onClick={handleForgotPassword}
                            disabled={isSending}
                            className={`text-sm font-medium ${isSuccess
                              ? "text-green-600 dark:text-green-400"
                              : "text-blue-600 hover:underline dark:text-blue-400"
                              } ${isSending ? "cursor-not-allowed opacity-60" : ""}`}
                          >
                            {isSending
                              ? "Sending..."
                              : isSuccess
                                ? "Email sent for password reset"
                                : "Click here to change password"}
                          </button>
                        ) : !showMemberPasswordForm ? (
                          <button
                            type="button"
                            onClick={() => {
                              setShowMemberPasswordForm(true);
                              setMemberPasswordError("");
                              setMemberPasswordMessage("");
                            }}
                            className="text-sm font-medium text-blue-600 hover:underline dark:text-blue-400"
                          >
                            Click here to change password
                          </button>
                        ) : (
                          <div className="mt-2 space-y-3">
                            <Input
                              type="password"
                              placeholder="Old Password"
                              value={memberPasswordForm.old_password}
                              onChange={(e) =>
                                setMemberPasswordForm((prev) => ({
                                  ...prev,
                                  old_password: e.target.value,
                                }))
                              }
                            />

                            <Input
                              type="password"
                              placeholder="New Password"
                              value={memberPasswordForm.new_password}
                              onChange={(e) =>
                                setMemberPasswordForm((prev) => ({
                                  ...prev,
                                  new_password: e.target.value,
                                }))
                              }
                            />

                            <Input
                              type="password"
                              placeholder="Confirm New Password"
                              value={memberPasswordForm.confirm_password}
                              onChange={(e) =>
                                setMemberPasswordForm((prev) => ({
                                  ...prev,
                                  confirm_password: e.target.value,
                                }))
                              }
                            />

                            {memberPasswordError && (
                              <p className="text-xs text-red-500">{memberPasswordError}</p>
                            )}

                            {memberPasswordMessage && (
                              <p className="text-xs text-green-600 dark:text-green-400">
                                {memberPasswordMessage}
                              </p>
                            )}

                            <div className="flex items-center gap-2">
                              <Button
                                type="button"
                                size="sm"
                                onClick={handleMemberChangePassword}
                                disabled={memberPasswordLoading}
                              >
                                {memberPasswordLoading ? "Updating..." : "Update Password"}
                              </Button>

                              <Button
                                type="button"
                                size="sm"
                                variant="outline"
                                onClick={() => {
                                  setShowMemberPasswordForm(false);
                                  setMemberPasswordError("");
                                  setMemberPasswordMessage("");
                                  setMemberPasswordForm({
                                    old_password: "",
                                    new_password: "",
                                    confirm_password: "",
                                  });
                                }}
                              >
                                Cancel
                              </Button>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </InfoCard>
                </div>

                <div id="tour-company-info">
                  <InfoCard
                    title={<PageBreadcrumb pageTitle="Company Info" variant="table" align="left" />}
                    action={
                      isMemberUser ? null : !isCompanyEditMode ? (
                        <button
                          id="tour-company-edit"
                          onClick={startCompanyEdit}
                          type="button"
                          className="flex h-9 w-9 items-center justify-center rounded-md text-gray-700 hover:bg-gray-100"
                        >
                          <FiEdit className="h-4 w-4" />
                        </button>
                      ) : (
                        <div className="flex items-center gap-2">
                          <Button size="icon" onClick={handleSaveCompany}>
                            <FiCheck />
                          </Button>
                          <Button size="icon" variant="outline" onClick={cancelCompanyEdit}>
                            <FiX />
                          </Button>
                        </div>
                      )
                    }
                  >
                    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
                      <InfoItem
                        required
                        label="Company Name"
                        value={
                          isCompanyEditMode ? (
                            <div>
                              <Input
                                type="text"
                                value={form.company_name}
                                onChange={handleCompanyChange("company_name")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, company_name: true }));
                                  validateCompanyField("company_name");
                                }}
                                maxLength={80}
                                error={!!(companyTouched.company_name && companyErrors.company_name)}
                              />
                              {companyTouched.company_name && companyErrors.company_name && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.company_name}</p>
                              )}
                            </div>
                          ) : (
                            show(form.company_name)
                          )
                        }
                      />

                      <InfoItem
                        required
                        label="Brand Name"
                        value={
                          isCompanyEditMode ? (
                            <div>
                              <Input
                                type="text"
                                value={form.brand_name}
                                onChange={handleCompanyChange("brand_name")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, brand_name: true }));
                                  validateCompanyField("brand_name");
                                }}
                                maxLength={80}
                                error={!!(companyTouched.brand_name && companyErrors.brand_name)}
                              />
                              {companyTouched.brand_name && companyErrors.brand_name && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.brand_name}</p>
                              )}
                            </div>
                          ) : (
                            show(form.brand_name)
                          )
                        }
                      />

                      {/* <InfoItem
                      label="Revenue"
                      value={
                        isCompanyEditMode ? (
                          <div>
                            <select
                              value={form.annual_sales_range}
                              onChange={handleCompanyChange("annual_sales_range")}
                              onBlur={() => {
                                setCompanyTouched((prev) => ({ ...prev, annual_sales_range: true }));
                                validateCompanyField("annual_sales_range");
                              }}
                              className={`h-11 w-full rounded-md border bg-white px-3 pr-10 text-sm text-gray-800 dark:bg-gray-800 dark:text-gray-200 ${companyTouched.annual_sales_range && companyErrors.annual_sales_range
                                ? "border-red-500"
                                : "border-gray-300"
                                }`}
                            >
                              <option value="">Select Revenue Range</option>
                              {REVENUE_OPTIONS.filter(Boolean).map((opt) => (
                                <option key={opt} value={opt}>
                                  {opt}
                                </option>
                              ))}
                            </select>
                            {companyTouched.annual_sales_range && companyErrors.annual_sales_range && (
                              <p className="mt-1.5 text-xs text-red-500">{companyErrors.annual_sales_range}</p>
                            )}
                          </div>
                        ) : (
                          show(form.annual_sales_range)
                        )
                      }
                    /> */}

                      <InfoItem
                        id="tour-home-currency"
                        label="Home Currency"
                        value={
                          isCompanyEditMode ? (
                            <div>
                              <select
                                value={form.homeCurrency}
                                onChange={handleCompanyChange("homeCurrency")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, homeCurrency: true }));
                                  validateCompanyField("homeCurrency");
                                }}
                                className={`h-11 w-full rounded-md border bg-white px-3 pr-10 text-sm text-gray-800 dark:bg-gray-800 dark:text-gray-200 ${companyTouched.homeCurrency && companyErrors.homeCurrency
                                  ? "border-red-500"
                                  : "border-gray-300"
                                  }`}
                              >
                                <option value="">Select Currency</option>
                                {CURRENCY_OPTIONS.map((cur) => (
                                  <option key={cur} value={cur}>
                                    {cur}
                                  </option>
                                ))}
                              </select>
                              {companyTouched.homeCurrency && companyErrors.homeCurrency && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.homeCurrency}</p>
                              )}
                            </div>
                          ) : (
                            show(form.homeCurrency)
                          )
                        }
                      />

                      <InfoItem
                        label="GST No."
                        value={
                          isCompanyEditMode ? (
                            <div>
                              <Input
                                type="text"
                                value={form.gst_no}
                                onChange={handleCompanyChange("gst_no")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, gst_no: true }));
                                  validateCompanyField("gst_no");
                                }}
                                maxLength={15}
                                error={!!(companyTouched.gst_no && companyErrors.gst_no)}
                              />
                              {companyTouched.gst_no && companyErrors.gst_no && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.gst_no}</p>
                              )}
                            </div>
                          ) : (
                            show(form.gst_no)
                          )
                        }
                      />

                      <InfoItem
                        label="PAN No."
                        value={
                          isCompanyEditMode ? (
                            <div>
                              <Input
                                type="text"
                                value={form.pan_no}
                                onChange={handleCompanyChange("pan_no")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, pan_no: true }));
                                  validateCompanyField("pan_no");
                                }}
                                maxLength={10}
                                error={!!(companyTouched.pan_no && companyErrors.pan_no)}
                              />
                              {companyTouched.pan_no && companyErrors.pan_no && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.pan_no}</p>
                              )}
                            </div>
                          ) : (
                            show(form.pan_no)
                          )
                        }
                      />

                      <div className="sm:col-span-2 lg:col-span-3">
                        <InfoItem
                          required
                          label="Address"
                          value={
                            isCompanyEditMode ? (
                              <div className="grid grid-cols-1 gap-3">
                                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                                  <div>
                                    <Input
                                      type="text"
                                      placeholder="Building No."
                                      value={form.address_building}
                                      onChange={handleCompanyChange("address_building")}
                                      onBlur={() => {
                                        setCompanyTouched((prev) => ({ ...prev, address_building: true }));
                                        validateCompanyField("address_building");
                                      }}
                                      maxLength={120}
                                      error={!!(
                                        companyTouched.address_building && companyErrors.address_building
                                      )}
                                    />
                                    {companyTouched.address_building && companyErrors.address_building && (
                                      <p className="mt-1.5 text-xs text-red-500">
                                        {companyErrors.address_building}
                                      </p>
                                    )}
                                  </div>

                                  <div>
                                    <Input
                                      type="text"
                                      placeholder="City"
                                      value={form.address_city}
                                      onChange={handleCompanyChange("address_city")}
                                      onBlur={() => {
                                        setCompanyTouched((prev) => ({ ...prev, address_city: true }));
                                        validateCompanyField("address_city");
                                      }}
                                      maxLength={60}
                                      error={!!(companyTouched.address_city && companyErrors.address_city)}
                                    />
                                    {companyTouched.address_city && companyErrors.address_city && (
                                      <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_city}</p>
                                    )}
                                  </div>
                                </div>

                                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                                  <div>
                                    <Input
                                      type="text"
                                      placeholder="State"
                                      value={form.address_state}
                                      onChange={handleCompanyChange("address_state")}
                                      onBlur={() => {
                                        setCompanyTouched((prev) => ({ ...prev, address_state: true }));
                                        validateCompanyField("address_state");
                                      }}
                                      maxLength={60}
                                      error={!!(companyTouched.address_state && companyErrors.address_state)}
                                    />
                                    {companyTouched.address_state && companyErrors.address_state && (
                                      <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_state}</p>
                                    )}
                                  </div>

                                  <div>
                                    <Input
                                      type="text"
                                      placeholder="Zipcode"
                                      value={form.address_zipcode}
                                      onChange={handleCompanyChange("address_zipcode")}
                                      onBlur={() => {
                                        setCompanyTouched((prev) => ({ ...prev, address_zipcode: true }));
                                        validateCompanyField("address_zipcode");
                                      }}
                                      maxLength={12}
                                      error={!!(companyTouched.address_zipcode && companyErrors.address_zipcode)}
                                    />
                                    {companyTouched.address_zipcode && companyErrors.address_zipcode && (
                                      <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_zipcode}</p>
                                    )}
                                  </div>

                                  <div>
                                    <Input
                                      type="text"
                                      placeholder="Country/Region"
                                      value={form.address_country}
                                      onChange={handleCompanyChange("address_country")}
                                      onBlur={() => {
                                        setCompanyTouched((prev) => ({ ...prev, address_country: true }));
                                        validateCompanyField("address_country");
                                      }}
                                      maxLength={60}
                                      error={!!(companyTouched.address_country && companyErrors.address_country)}
                                    />
                                    {companyTouched.address_country && companyErrors.address_country && (
                                      <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_country}</p>
                                    )}
                                  </div>
                                </div>
                              </div>
                            ) : (
                              <div className="text-sm font-medium text-gray-800 dark:text-white/90">
                                {[
                                  form.address_building,
                                  form.address_city,
                                  form.address_state,
                                  form.address_country,
                                  form.address_zipcode,
                                ]
                                  .map((x) => (x ?? "").trim())
                                  .filter(Boolean)
                                  .join(", ") || "-"}
                              </div>
                            )
                          }
                        />
                      </div>
                    </div>
                  </InfoCard>
                </div>
              </>
            )}

            {activeTab === "personal" && (
              <>
                {!isMemberUser && (
                  <div id="tour-product-controls" className="lg:col-span-1 h-full">
                    <InfoCard
                      title={
                        <PageBreadcrumb
                          pageTitle="Product & Inventory Controls"
                          variant="table"
                          align="left"
                        />
                      }
                      disabled={!canAccessProductControls}
                      disabledMessage="Complete Company Info to unlock this section"
                      hideDisabledOverlay={tourEnabled}
                    >
                      <div className="grid grid-cols-1 gap-4">
                        <div className="flex items-center justify-start gap-2">
                          <p className="text-sm font-semibold text-charcoal-500">SKU Information</p>

                          <button
                            id="tour-sku-upload-icon"
                            onClick={canAccessProductControls ? skuModal.openModal : undefined}
                            disabled={!canAccessProductControls}
                            className="inline-flex items-center rounded-md p-1 text-gray-700 dark:text-gray-200"
                            aria-label="Upload SKU"
                            title="Upload SKU"
                            type="button"
                          >
                            <TiUpload size={16} />
                          </button>
                        </div>
                      </div>

                      <Modal
                        isOpen={feeModal.isOpen}
                        onClose={closeFeePreview}
                        className="m-4 max-w-[800px] border border-[#D9D9D9] shadow-[6px_6px_7px_0px_#00000026]"
                      >
                        <div className="relative w-full rounded-3xl bg-white p-4 no-scrollbar dark:bg-gray-900 lg:p-11">
                          {selectedCountry ? (
                            <FeepreviewUpload country={selectedCountry} onClose={closeFeePreview} />
                          ) : (
                            <p className="text-center text-gray-500 dark:text-gray-400">
                              No country selected
                            </p>
                          )}
                        </div>
                      </Modal>

                      <Modal
                        isOpen={skuModal.isOpen}
                        onClose={skuModal.closeModal}
                        className="m-4 max-w-[500px] border border-[#D9D9D9] shadow-[6px_6px_7px_0px_#00000026]"
                      >
                        <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar dark:bg-gray-900 lg:p-9">
                          <SkuMultiCountryUpload
                            onClose={skuModal.closeModal}
                            onComplete={() => {
                              setIsSkuUploaded(true);
                              setData((prev: any) => ({
                                ...prev,
                                sku_sheet_exists: true,
                              }));
                              skuModal.closeModal();
                            }}
                          />
                        </div>
                      </Modal>
                    </InfoCard>
                  </div>
                )}



                {!isMemberUser && (
                  <div className="lg:col-span-1 h-full">
                    <InfoCard
                      id="tour-integrations"
                      title={<PageBreadcrumb pageTitle="Integrations" variant="table" align="left" />}
                      action={
                        <div
                          id="tour-integration-icon"
                          className={!canAccessIntegrations ? "pointer-events-none opacity-60" : ""}
                        >
                          <IntegrationToggleButton />
                        </div>
                      }
                      disabled={!canAccessIntegrations}
                      disabledMessage="Upload your SKU sheet to unlock this section"
                      hideDisabledOverlay={tourEnabled}
                    >
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
                          <div className="flex flex-wrap items-center justify-between gap-4">
                            <div className="flex flex-wrap items-center gap-4">
                              {connectedPlatforms.map((p) => {
                                const meta = PLATFORM_FLAG_META[p.id] ?? { label: p.label };

                                return (
                                  <div key={p.id} className="flex items-center gap-3">
                                    {meta.countryCode ? (
                                      <ReactCountryFlag
                                        svg
                                        countryCode={meta.countryCode as any}
                                        className="text-[22px] leading-none"
                                        aria-label={meta.label}
                                      />
                                    ) : meta.image ? (
                                      <img
                                        src={meta.image}
                                        alt={meta.label}
                                        className="h-8 w-8 object-contain"
                                      />
                                    ) : null}

                                    <span className="text-sm font-semibold text-gray-800 dark:text-white/90">
                                      {meta.label}
                                    </span>
                                  </div>
                                );
                              })}
                            </div>

                            {/* <Link
                            href=""
                            className="inline-flex items-center gap-2 whitespace-nowrap text-sm font-semibold text-green-500 hover:underline dark:text-emerald-400"
                          >
                            <FaPlus size={12} />
                            <span>Integrate more marketplaces</span>
                          </Link> */}
                          </div>
                        );
                      })()}
                    </InfoCard>
                  </div>
                )}

              </>
            )}
          </div>
        </div>
      </div >

      <Modal isOpen={isOpen} onClose={closeModal} className="m-4 max-w-[700px]">
        <div className="no-scrollbar relative w-full max-w-[700px] overflow-y-auto rounded-2xl border border-gray-200 bg-white p-4 dark:bg-gray-900 lg:p-11">
          <div className="px-2 pr-14">
            <PageBreadcrumb pageTitle={modalTitle} variant="table" align="left" textSize="2xl" />
            <p className="text-sm text-charcoal-500">{modalSubtitle}</p>
          </div>

          <form className="flex flex-col" onSubmit={(e) => e.preventDefault()}>
            <div className="custom-scrollbar h-full overflow-y-auto px-2 pb-3">
              <div className="mt-2">
                <div className="grid grid-cols-1 gap-x-6 gap-y-5 lg:grid-cols-2">
                  {activeSection === "personal" && (
                    <>
                      <div className="col-span-2 lg:col-span-1">
                        <Label >Name</Label>
                        <Input
                          type="text"
                          value={form.name}
                          onChange={handlePersonalChange("name")}
                          onBlur={() => {
                            setPersonalTouched((prev) => ({ ...prev, name: true }));
                            validatePersonalField("name");
                          }}
                          maxLength={50}
                          error={!!(personalTouched.name && personalErrors.name)}
                        />
                        {personalTouched.name && personalErrors.name && (
                          <p className="mt-1.5 text-xs text-red-500">{personalErrors.name}</p>
                        )}
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Email (read-only)</Label>
                        <Input type="text" value={form.email} disabled />
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Phone</Label>
                        <Input
                          type="text"
                          value={displayedPhone}
                          onChange={(e) => {
                            const rawValue = e.target.value.trim();

                            let nextValue = rawValue;

                            if (selectedDialCode && rawValue.startsWith(selectedDialCode)) {
                              const localNumber = rawValue.slice(selectedDialCode.length).trim();
                              nextValue = `${selectedDialCode}${localNumber}`;
                            }

                            handlePersonalChange("phone_number")({
                              target: { value: nextValue },
                            } as React.ChangeEvent<HTMLInputElement>);
                          }}
                          onBlur={() => {
                            setPersonalTouched((prev) => ({ ...prev, phone_number: true }));
                            validatePersonalField("phone_number");
                          }}
                          maxLength={20}
                          inputMode="tel"
                          error={!!(personalTouched.phone_number && personalErrors.phone_number)}
                        />
                        {personalTouched.phone_number && personalErrors.phone_number && (
                          <p className="mt-1.5 text-xs text-red-500">{personalErrors.phone_number}</p>
                        )}
                      </div>

                      <div className="col-span-2">
                        <Label>Reset Password</Label>
                        <p
                          onClick={handleForgotPassword}
                          className={`cursor-pointer text-sm font-medium ${isSuccess ? "text-green-600 dark:text-green-400" : "text-blue-600 hover:underline"
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

                  {activeSection === "company" && (
                    <>
                      <div id="tour-brand-name" className="col-span-2 lg:col-span-1">
                        <Label >Brand Name</Label>
                        <Input
                          type="text"
                          value={form.brand_name}
                          onChange={handleCompanyChange("brand_name")}
                          onBlur={() => {
                            setCompanyTouched((prev) => ({ ...prev, brand_name: true }));
                            validateCompanyField("brand_name");
                          }}
                          maxLength={80}
                          error={!!(companyTouched.brand_name && companyErrors.brand_name)}
                        />
                        {companyTouched.brand_name && companyErrors.brand_name && (
                          <p className="mt-1.5 text-xs text-red-500">{companyErrors.brand_name}</p>
                        )}
                      </div>

                      <div id="tour-company-name" className="col-span-2 lg:col-span-1">
                        <Label >Company Name</Label>
                        <Input
                          type="text"
                          value={form.company_name}
                          onChange={handleCompanyChange("company_name")}
                          onBlur={() => {
                            setCompanyTouched((prev) => ({ ...prev, company_name: true }));
                            validateCompanyField("company_name");
                          }}
                          maxLength={80}
                          error={!!(companyTouched.company_name && companyErrors.company_name)}
                        />
                        {companyTouched.company_name && companyErrors.company_name && (
                          <p className="mt-1.5 text-xs text-red-500">{companyErrors.company_name}</p>
                        )}
                      </div>

                      {/* <div className="col-span-2 lg:col-span-1">
                        <Label>Revenue</Label>
                        <select
                          value={form.annual_sales_range}
                          onChange={handleCompanyChange("annual_sales_range")}
                          onBlur={() => {
                            setCompanyTouched((prev) => ({ ...prev, annual_sales_range: true }));
                            validateCompanyField("annual_sales_range");
                          }}
                          className={`h-11 w-full rounded-md border bg-white px-3 pr-10 text-sm text-gray-800 dark:bg-gray-800 dark:text-gray-200 ${companyTouched.annual_sales_range && companyErrors.annual_sales_range
                            ? "border-red-500"
                            : "border-gray-300"
                            }`}
                        >
                          <option value="">Select Revenue Range</option>
                          {REVENUE_OPTIONS.filter(Boolean).map((opt) => (
                            <option key={opt} value={opt}>
                              {opt}
                            </option>
                          ))}
                        </select>
                        {companyTouched.annual_sales_range && companyErrors.annual_sales_range && (
                          <p className="mt-1.5 text-xs text-red-500">{companyErrors.annual_sales_range}</p>
                        )}
                      </div> */}

                      <div id="tour-home-currency" className="col-span-2 lg:col-span-1">
                        <Label >Home Currency</Label>
                        <select
                          value={form.homeCurrency}
                          onChange={handleCompanyChange("homeCurrency")}
                          onBlur={() => {
                            setCompanyTouched((prev) => ({ ...prev, homeCurrency: true }));
                            validateCompanyField("homeCurrency");
                          }}
                          className={`h-11 w-full rounded-md border bg-white px-3 pr-10 text-sm text-gray-800 dark:bg-gray-800 dark:text-gray-200 ${companyTouched.homeCurrency && companyErrors.homeCurrency
                            ? "border-red-500"
                            : "border-gray-300"
                            }`}
                        >
                          <option value="">Select Currency</option>
                          {CURRENCY_OPTIONS.map((cur) => (
                            <option key={cur} value={cur}>
                              {cur}
                            </option>
                          ))}
                        </select>
                        {companyTouched.homeCurrency && companyErrors.homeCurrency && (
                          <p className="mt-1.5 text-xs text-red-500">{companyErrors.homeCurrency}</p>
                        )}
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>GST No.</Label>
                        <Input
                          type="text"
                          value={form.gst_no}
                          onChange={handleCompanyChange("gst_no")}
                          onBlur={() => {
                            setCompanyTouched((prev) => ({ ...prev, gst_no: true }));
                            validateCompanyField("gst_no");
                          }}
                          maxLength={15}
                          error={!!(companyTouched.gst_no && companyErrors.gst_no)}
                        />
                        {companyTouched.gst_no && companyErrors.gst_no && (
                          <p className="mt-1.5 text-xs text-red-500">{companyErrors.gst_no}</p>
                        )}
                      </div>

                      <div className="col-span-2 lg:col-span-1">
                        <Label>PAN No.</Label>
                        <Input
                          type="text"
                          value={form.pan_no}
                          onChange={handleCompanyChange("pan_no")}
                          onBlur={() => {
                            setCompanyTouched((prev) => ({ ...prev, pan_no: true }));
                            validateCompanyField("pan_no");
                          }}
                          maxLength={10}
                          error={!!(companyTouched.pan_no && companyErrors.pan_no)}
                        />
                        {companyTouched.pan_no && companyErrors.pan_no && (
                          <p className="mt-1.5 text-xs text-red-500">{companyErrors.pan_no}</p>
                        )}
                      </div>

                      <div className="col-span-2">
                        <Label >Address</Label>
                        <div className="grid grid-cols-1 gap-4">
                          <div>
                            <Input
                              type="text"
                              placeholder="Building No."
                              value={form.address_building}
                              onChange={handleCompanyChange("address_building")}
                              onBlur={() => {
                                setCompanyTouched((prev) => ({ ...prev, address_building: true }));
                                validateCompanyField("address_building");
                              }}
                              maxLength={120}
                              error={!!(companyTouched.address_building && companyErrors.address_building)}
                            />
                            {companyTouched.address_building && companyErrors.address_building && (
                              <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_building}</p>
                            )}
                          </div>

                          <div>
                            <Input
                              type="text"
                              placeholder="City"
                              value={form.address_city}
                              onChange={handleCompanyChange("address_city")}
                              onBlur={() => {
                                setCompanyTouched((prev) => ({ ...prev, address_city: true }));
                                validateCompanyField("address_city");
                              }}
                              maxLength={60}
                              error={!!(companyTouched.address_city && companyErrors.address_city)}
                            />
                            {companyTouched.address_city && companyErrors.address_city && (
                              <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_city}</p>
                            )}
                          </div>

                          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
                            <div>
                              <Input
                                type="text"
                                placeholder="Country/Region"
                                value={form.address_country}
                                onChange={handleCompanyChange("address_country")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, address_country: true }));
                                  validateCompanyField("address_country");
                                }}
                                maxLength={60}
                                error={!!(companyTouched.address_country && companyErrors.address_country)}
                              />
                              {companyTouched.address_country && companyErrors.address_country && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_country}</p>
                              )}
                            </div>

                            <div>
                              <Input
                                type="text"
                                placeholder="State"
                                value={form.address_state}
                                onChange={handleCompanyChange("address_state")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, address_state: true }));
                                  validateCompanyField("address_state");
                                }}
                                maxLength={60}
                                error={!!(companyTouched.address_state && companyErrors.address_state)}
                              />
                              {companyTouched.address_state && companyErrors.address_state && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_state}</p>
                              )}
                            </div>

                            <div>
                              <Input
                                type="text"
                                placeholder="Zipcode"
                                value={form.address_zipcode}
                                onChange={handleCompanyChange("address_zipcode")}
                                onBlur={() => {
                                  setCompanyTouched((prev) => ({ ...prev, address_zipcode: true }));
                                  validateCompanyField("address_zipcode");
                                }}
                                maxLength={12}
                                error={!!(companyTouched.address_zipcode && companyErrors.address_zipcode)}
                              />
                              {companyTouched.address_zipcode && companyErrors.address_zipcode && (
                                <p className="mt-1.5 text-xs text-red-500">{companyErrors.address_zipcode}</p>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                    </>
                  )}

                  {activeSection === "targets" && (
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
                  )}

                  {activeSection === "objective" && (
                    <>
                      <div className="col-span-2">
                        <Label>Country</Label>
                        <select
                          value={objective.country}
                          onChange={(e) => setObjective((prev) => ({ ...prev, country: e.target.value }))}
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

                      <div className="col-span-2 lg:col-span-1">
                        <Label>Growth</Label>
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

                      <div className="col-span-2">
                        <Label>Profit</Label>
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

                      <div className="col-span-2">
                        <Label>Business Overview</Label>
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

            <div className="mt-6 flex items-center gap-3 px-2 lg:justify-end">
              <Button size="sm" variant="outline" onClick={closeModal} disabled={isSaving}>
                Close
              </Button>
              <Button
                size="sm"
                onClick={
                  activeSection === "objective"
                    ? handleSaveObjective
                    : activeSection === "personal"
                      ? handleSavePersonal
                      : activeSection === "company"
                        ? handleSaveCompany
                        : handleSave
                }
                disabled={isSaving}
              >
                {isSaving ? "Saving…" : "Save Changes"}
              </Button>
            </div>
          </form>
        </div>
      </Modal>
    </div >
  );
}