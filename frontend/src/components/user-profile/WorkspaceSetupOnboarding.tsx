"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSelector } from "react-redux";
import ReactCountryFlag from "react-country-flag";
import {
  FiCheck,
  FiCloud,
  FiDownload,
  FiEdit3,
  FiEye,
  FiLock,
  FiPlus,
  FiUser,
  FiX,
} from "react-icons/fi";
import { SiAmazon } from "react-icons/si";

import Button from "@/components/ui/button/Button";
import Input from "@/components/form/input/InputField";
import Label from "@/components/form/Label";
import { Modal } from "@/components/ui/modal";
import SkuMultiCountryUpload from "@/components/ui/modal/SkuMultiCountryUpload";
import DataTable, { Row as TableRow, ColumnDef } from "@/components/ui/table/DataTable";
import IntegrationToggleButton from "@/features/integration/IntegrationToggleButton";
import { useModal } from "@/hooks/useModal";
import { useConnectedPlatforms } from "@/lib/utils/useConnectedPlatforms";
import { useForgotPasswordMutation } from "@/lib/api/profileApi";
import {
  useLazyGetCurrentSkuSheetQuery,
  useDownloadCurrentSkuSheetMutation,
} from "@/lib/api/skuApi";
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

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

const CURRENCY_OPTIONS = ["USD"];

const inputClass =
  "!h-9 2xl:!h-11 w-full rounded-xl border border-[#DDE5E1] bg-white px-3 !text-[12px] 2xl:!text-[14px] text-[#273140] outline-none transition placeholder:text-[#9AA5A0] focus:border-green-500 focus:ring-4 focus:ring-green-500/10 disabled:bg-[#F5F7F6] disabled:text-[#7E8984]";


const SKU_VIEW_COLUMNS: ColumnDef<TableRow>[] = [
  { key: "s_no", header: "S. No." },
  { key: "product_name", header: "Product Name" },
  { key: "product_barcode", header: "Product Barcode" },
  { key: "asin", header: "ASIN" },
  { key: "sku_uk", header: "SKU_UK" },
  { key: "sku_us", header: "SKU_US" },
  { key: "sku_canada", header: "SKU_CANADA" },
  { key: "landing_cost", header: "Landing Cost" },
  { key: "currency", header: "Currency" },
  { key: "date", header: "Date" },
  { key: "local_stock", header: "Local Stock" },
  { key: "in_transit_units", header: "In Transit Units" },
];

type FormState = {
  name: string;
  phone_number: string;
  email: string;
  company_name: string;
  brand_name: string;
  homeCurrency: string;
  gst_no: string;
  pan_no: string;
  address_building: string;
  address_city: string;
  address_state: string;
  address_country: string;
  address_zipcode: string;
};

export type WorkspaceActivity = {
  label: string;
  at: string;
  currency: string;
};

type WorkspaceSetupOnboardingProps = {
  dashboardHref?: string;
  onSetupComplete?: () => void;
  onActivityChange?: (activity: WorkspaceActivity | null) => void;
  className?: string;
};

const emptyForm: FormState = {
  name: "",
  phone_number: "",
  email: "",
  company_name: "",
  brand_name: "",
  homeCurrency: "USD",
  gst_no: "",
  pan_no: "",
  address_building: "",
  address_city: "",
  address_state: "",
  address_country: "",
  address_zipcode: "",
};

function getLatestWorkspaceActivity(userData: any): WorkspaceActivity | null {
  if (!userData) return null;

  const candidates = [
    { label: "Company information", at: userData?.company_updated_at },
    { label: "SKU sheet", at: userData?.sku_updated_at },
    { label: "Marketplace integration", at: userData?.integration_updated_at },
  ]
    .filter((item) => item.at)
    .map((item) => ({ ...item, time: new Date(item.at).getTime() }))
    .filter((item) => !Number.isNaN(item.time));

  if (!candidates.length) return null;

  const latest = candidates.reduce((current, item) =>
    item.time > current.time ? item : current
  );

  return {
    label: latest.label,
    at: latest.at,
    currency: String(userData?.homeCurrency || "USD").toUpperCase(),
  };
}

function cx(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function StatusPill({
  children,
  tone = "green",
}: {
  children: React.ReactNode;
  tone?: "green" | "blue" | "gray";
}) {
  const styles = {
    green: "bg-[#EAF7F1] text-[#0E8558]",
    blue: "bg-[#EEF5FA] text-[#41708F]",
    gray: "bg-[#F1F3F2] text-[#74807A]",
  };

  return (
    <span className={cx("rounded-full px-2.5 py-1 text-[11px] font-semibold", styles[tone])}>
      {children}
    </span>
  );
}

function Panel({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section
      className={cx(
        "h-full rounded-2xl border border-[#E3E8E5] bg-white shadow-[0_12px_34px_rgba(39,49,64,0.055)]",
        className
      )}
    >
      {children}
    </section>
  );
}

function ReadOnlyField({
  label,
  value,
  className,
}: {
  label: string;
  value: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cx("min-w-0", className)}>
      <p className="text-[10px] 2xl:text-[11px] font-semibold uppercase tracking-[0.08em] text-[#8A948F]">
        {label}
      </p>
      <div className="mt-1 break-words whitespace-normal text-xs 2xl:text-sm font-semibold leading-5 2xl:leading-6 text-[#33413A]">
        {value || "-"}
      </div>
    </div>
  );
}

export default function WorkspaceSetupOnboarding({
  onActivityChange,
  className,
}: WorkspaceSetupOnboardingProps) {
  const skuModal = useModal();
  const skuViewModal = useModal();
  const connected = useConnectedPlatforms();

  const [
    getCurrentSkuSheet,
    {
      data: currentSkuSheet,
      isFetching: isSkuSheetLoading,
      error: skuSheetError,
    },
  ] = useLazyGetCurrentSkuSheetQuery();

  const [
    downloadCurrentSkuSheet,
    { isLoading: isDownloadingSkuSheet },
  ] = useDownloadCurrentSkuSheetMutation();
  const reduxToken = useSelector((state: any) => state.auth?.token);

  const [data, setData] = useState<any>(null);
  const [form, setForm] = useState<FormState>(emptyForm);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [loadError, setLoadError] = useState("");

  const [isPersonalEditing, setIsPersonalEditing] = useState(false);
  const [personalErrors, setPersonalErrors] = useState<PersonalInfoFormErrors>({});
  const [personalTouched, setPersonalTouched] = useState({
    name: false,
    phone_number: false,
  });

  const [companyErrors, setCompanyErrors] = useState<CompanyInfoFormErrors>({});
  const [companyTouched, setCompanyTouched] = useState<Record<keyof Omit<FormState, "name" | "phone_number" | "email">, boolean>>({
    company_name: false,
    brand_name: false,
    homeCurrency: false,
    gst_no: false,
    pan_no: false,
    address_building: false,
    address_city: false,
    address_state: false,
    address_country: false,
    address_zipcode: false,
  });
  const [isCompanyEditing, setIsCompanyEditing] = useState(false);
  const [uploadSuccess, setUploadSuccess] = useState(false);

  const [forgotPassword, { isLoading: isSendingReset, isSuccess: resetEmailSent }] =
    useForgotPasswordMutation();

  const companySectionRef = useRef<HTMLDivElement | null>(null);
  const productSectionRef = useRef<HTMLDivElement | null>(null);
  const integrationSectionRef = useRef<HTMLDivElement | null>(null);
  const integrationTrackingReadyRef = useRef(false);
  const previousIntegrationSnapshotRef = useRef("");

  const token =
    reduxToken ||
    (typeof window !== "undefined" ? localStorage.getItem("jwtToken") || "" : "");

  const fetchUserData = useCallback(async () => {
    if (!token) {
      setIsLoading(false);
      setLoadError("Authentication token is missing.");
      return;
    }

    try {
      setIsLoading(true);
      setLoadError("");

      const response = await fetch(`${API_BASE}/get_user_data`, {
        headers: { Authorization: `Bearer ${token}` },
        cache: "no-store",
      });

      const result = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(result?.error || result?.message || "Failed to load profile");
      }

      const tax = result?.tax_id || {};
      const address = result?.address || {};

      setData(result);
      setForm({
        name: result?.name || "",
        phone_number: result?.phone_number || "",
        email: result?.email || "",
        company_name: result?.company_name || "",
        brand_name: result?.brand_name || "",
        homeCurrency: result?.homeCurrency || "USD",
        gst_no: tax?.gst_no || "",
        pan_no: tax?.pan_no || "",
        address_building: address?.building || "",
        address_city: address?.city || "",
        address_state: address?.state || "",
        address_country: address?.country || "",
        address_zipcode: address?.zipcode || "",
      });

      onActivityChange?.(getLatestWorkspaceActivity(result));
    } catch (error: any) {
      setLoadError(error?.message || "Failed to load profile");
    } finally {
      setIsLoading(false);
    }
  }, [token, onActivityChange]);

  useEffect(() => {
    void fetchUserData();
  }, [fetchUserData]);

  useEffect(() => {
    const snapshot = JSON.stringify({
      amazonUs: Boolean(connected.amazonUs),
      amazonUk: Boolean(connected.amazonUk),
    });

    if (!integrationTrackingReadyRef.current) {
      previousIntegrationSnapshotRef.current = snapshot;
      const timer = window.setTimeout(() => {
        previousIntegrationSnapshotRef.current = JSON.stringify({
          amazonUs: Boolean(connected.amazonUs),
          amazonUk: Boolean(connected.amazonUk),
        });
        integrationTrackingReadyRef.current = true;
      }, 1200);
      return () => window.clearTimeout(timer);
    }

    if (previousIntegrationSnapshotRef.current !== snapshot) {
      previousIntegrationSnapshotRef.current = snapshot;
      const timer = window.setTimeout(() => {
        void fetchUserData();
      }, 300);
      return () => window.clearTimeout(timer);
    }
  }, [connected.amazonUk, connected.amazonUs, fetchUserData]);

  const isPersonalComplete = Boolean(
    String(data?.name || "").trim() && String(data?.phone_number || "").trim()
  );

  const address = data?.address || {};
  const isCompanyComplete = Boolean(
    String(data?.company_name || "").trim() &&
    String(data?.brand_name || "").trim() &&
    String(data?.homeCurrency || "").trim() &&
    String(address?.building || "").trim() &&
    String(address?.city || "").trim() &&
    String(address?.state || "").trim() &&
    String(address?.country || "").trim() &&
    String(address?.zipcode || "").trim()
  );

  const hasSkuSheet = Boolean(data?.sku_sheet_exists || uploadSuccess);
  const hasIntegration = Boolean(
    connected.amazonUk || connected.amazonUs || connected.amazonAds
  );

  const canAccessProductControls = isCompanyComplete || hasSkuSheet;
  const canAccessIntegrations = hasSkuSheet || hasIntegration;

  // Progressive setup guide:
  // Company Info -> SKU Sheet -> Integrations
  const guideStep: "company" | "sku" | "integration" | "done" =
    !isCompanyComplete
      ? "company"
      : !hasSkuSheet
        ? "sku"
        : !hasIntegration
          ? "integration"
          : "done";

  const handlePersonalChange =
    (key: "name" | "phone_number") =>
      (event: React.ChangeEvent<HTMLInputElement>) => {
        let value = event.target.value;

        if (key === "name") value = sanitizeAlphaSpace(value);
        if (key === "phone_number") value = sanitizePhoneLoose(value);

        setForm((previous) => ({ ...previous, [key]: value }));

        if (personalTouched[key]) {
          validatePersonalField(key, { [key]: value });
        }
      };

  const validatePersonalField = (
    field: keyof PersonalInfoFormErrors,
    nextValues?: Partial<{ name: string; phone_number: string }>
  ) => {
    const result = personalInfoSchema.safeParse({
      name: form.name,
      phone_number: form.phone_number,
      ...nextValues,
    });

    if (result.success) {
      setPersonalErrors((previous) => ({ ...previous, [field]: undefined }));
      return;
    }

    const errors = getPersonalInfoFieldErrors(result.error);
    setPersonalErrors((previous) => ({ ...previous, [field]: errors[field] }));
  };

  const handleSavePersonal = async () => {
    setPersonalTouched({ name: true, phone_number: true });

    const validation = personalInfoSchema.safeParse({
      name: form.name,
      phone_number: form.phone_number,
    });

    if (!validation.success) {
      setPersonalErrors(getPersonalInfoFieldErrors(validation.error));
      return;
    }

    if (!token) return;

    try {
      setIsSaving(true);

      const response = await fetch(`${API_BASE}/profileupdate`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          name: form.name.trim(),
          phone_number: form.phone_number.trim(),
        }),
      });

      const result = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(result?.error || result?.message || "Failed to update personal info");
      }

      await fetchUserData();
      setIsPersonalEditing(false);
    } catch (error: any) {
      window.alert(error?.message || "Failed to update personal info");
    } finally {
      setIsSaving(false);
    }
  };

  const cancelPersonalEdit = () => {
    setIsPersonalEditing(false);
    setPersonalErrors({});
    setPersonalTouched({ name: false, phone_number: false });
    setForm((previous) => ({
      ...previous,
      name: data?.name || "",
      phone_number: data?.phone_number || "",
    }));
  };

  const handleForgotPassword = async () => {
    if (!data?.email) {
      window.alert("Email address is not available for this account.");
      return;
    }

    try {
      await forgotPassword({ email: data.email }).unwrap();
    } catch (error: any) {
      console.error(error);
      window.alert(error?.data?.message || "Failed to send reset email.");
    }
  };

  const handleDownloadSkuTemplate = () => {
    const link = document.createElement("a");
    link.href = "/sku-information-template.xlsx";
    link.download = "SKU Information.xlsx";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleViewSkuSheet = async () => {
    skuViewModal.openModal();

    try {
      await getCurrentSkuSheet().unwrap();
    } catch (error) {
      console.error("Failed to load SKU sheet:", error);
    }
  };

  const handleDownloadCurrentSkuSheet = async () => {
    try {
      const blob = await downloadCurrentSkuSheet().unwrap();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");

      link.href = url;
      link.download = "SKU Information.xlsx";
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
    } catch (error) {
      console.error("Failed to download SKU sheet:", error);
      window.alert("Failed to download SKU sheet.");
    }
  };

  const handleCompanyChange =
    (key: keyof Omit<FormState, "name" | "phone_number" | "email">) =>
      (event: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
        let value = event.target.value;

        if (key === "company_name" || key === "brand_name") {
          value = sanitizeAlphaNumSpace(value);
        }

        if (key === "gst_no" || key === "pan_no") {
          value = sanitizeUpperAlphaNum(value);
        }

        if (
          ["address_building", "address_city", "address_state", "address_country"].includes(
            key
          )
        ) {
          value = sanitizeAlphaNumSpace(value);
        }

        if (key === "address_zipcode") {
          value = value.replace(/[^A-Za-z0-9\- ]/g, "");
        }

        setForm((previous) => ({ ...previous, [key]: value }));

        if (companyTouched[key]) {
          validateCompanyField(key, { [key]: value });
        }
      };

  const validateCompanyField = (
    field: keyof Omit<FormState, "name" | "phone_number" | "email">,
    nextValues?: Partial<FormState>
  ) => {
    const values = { ...form, ...nextValues };
    const result = companyInfoSchema.safeParse({
      company_name: values.company_name,
      brand_name: values.brand_name,
      homeCurrency: values.homeCurrency,
      gst_no: values.gst_no,
      pan_no: values.pan_no,
      address_building: values.address_building,
      address_city: values.address_city,
      address_state: values.address_state,
      address_country: values.address_country,
      address_zipcode: values.address_zipcode,
    });

    if (result.success) {
      setCompanyErrors((previous) => ({ ...previous, [field]: undefined }));
      return;
    }

    const errors = getCompanyInfoFieldErrors(result.error);
    setCompanyErrors((previous) => ({ ...previous, [field]: errors[field] }));
  };

  const markCompanyTouched = (key: keyof Omit<FormState, "name" | "phone_number" | "email">) => {
    setCompanyTouched((previous) => ({ ...previous, [key]: true }));
    validateCompanyField(key);
  };

  const handleSaveCompany = async () => {
    const companyKeys = Object.keys(companyTouched) as Array<
      keyof Omit<FormState, "name" | "phone_number" | "email">
    >;

    setCompanyTouched(
      companyKeys.reduce(
        (accumulator, key) => ({ ...accumulator, [key]: true }),
        {} as Record<keyof Omit<FormState, "name" | "phone_number" | "email">, boolean>
      )
    );

    const validation = companyInfoSchema.safeParse({
      company_name: form.company_name,
      brand_name: form.brand_name,
      homeCurrency: form.homeCurrency,
      gst_no: form.gst_no,
      pan_no: form.pan_no,
      address_building: form.address_building,
      address_city: form.address_city,
      address_state: form.address_state,
      address_country: form.address_country,
      address_zipcode: form.address_zipcode,
    });

    if (!validation.success) {
      setCompanyErrors(getCompanyInfoFieldErrors(validation.error));
      return;
    }

    if (!token) return;

    try {
      setIsSaving(true);

      const response = await fetch(`${API_BASE}/profileupdate`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          company_name: form.company_name.trim(),
          brand_name: form.brand_name.trim(),
          homeCurrency: form.homeCurrency,
          tax_id: {
            gst_no: form.gst_no.trim(),
            pan_no: form.pan_no.trim(),
          },
          address: {
            building: form.address_building.trim(),
            city: form.address_city.trim(),
            state: form.address_state.trim(),
            country: form.address_country.trim(),
            zipcode: form.address_zipcode.trim(),
          },
        }),
      });

      const result = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(
          result?.error || result?.message || "Failed to save company information"
        );
      }

      await fetchUserData();
      setIsCompanyEditing(false);

      // Guide the user to the next required step after company info is saved.
      if (!hasSkuSheet) {
        window.setTimeout(() => {
          productSectionRef.current?.scrollIntoView({
            behavior: "smooth",
            block: "center",
          });
        }, 250);
      }
    } catch (error: any) {
      window.alert(error?.message || "Failed to save company information");
    } finally {
      setIsSaving(false);
    }
  };

  const connectedPlatforms = useMemo(
    () => [
      {
        id: "amazon-us",
        label: "Amazon US",
        countryCode: "US",
        connected: connected.amazonUs,
      },
      {
        id: "amazon-uk",
        label: "Amazon UK",
        countryCode: "GB",
        connected: connected.amazonUk,
      },
      {
        id: "amazon-ads",
        label: "Amazon Ads",
        connected: connected.amazonAds,
      },
    ],
    [connected.amazonUs, connected.amazonUk, connected.amazonAds]
  );

  const showCompanyError = (key: keyof Omit<FormState, "name" | "phone_number" | "email">) =>
    companyTouched[key] && companyErrors[key] ? (
      <p className="mt-1.5 text-xs text-[#B75A5A]">{companyErrors[key]}</p>
    ) : null;

  return (
    <div className={cx("min-h-full", className)}>
      {loadError && (
        <div className="mb-4 rounded-xl border border-[#EAC9C9] bg-[#FFF6F6] px-4 py-3 text-xs 2xl:text-sm text-[#A94E4E]">
          {loadError}
        </div>
      )}

      <div className="space-y-5">
        {/* Row 1: Personal + Company, exact 50 / 50 */}
        <div
          className={cx(
            "grid grid-cols-1 gap-5 xl:grid-cols-2",
            isCompanyEditing ? "xl:items-start" : "xl:items-stretch"
          )}
        >
          <Panel className="overflow-hidden">
            <div className="flex items-start justify-between gap-3 border-b border-[#E8ECEA] px-5 py-4">
              <div>
                <div className="flex flex-wrap items-center gap-2">
                  <h2 className="text-sm 2xl:text-base font-bold text-[#37455F]">1. Personal details</h2>
                  <StatusPill tone={isPersonalComplete ? "green" : "blue"}>
                    {isPersonalComplete ? "Completed" : "In progress"}
                  </StatusPill>
                </div>
                <p className="mt-1 text-[11px] 2xl:text-xs text-[#78847E]">
                  Manage your name, phone number and account password.
                </p>
              </div>

              {!isPersonalEditing ? (
                <button
                  type="button"
                  onClick={() => setIsPersonalEditing(true)}
                  className="inline-flex h-9 shrink-0 items-center gap-2 rounded-lg border border-[#DDE5E1] px-3 text-xs font-semibold text-[#37455F] hover:bg-[#F5F8F6]"
                >
                  <FiEdit3 size={14} /> Edit details
                </button>
              ) : (
                <div className="flex shrink-0 items-center gap-2">
                  <Button type="button" size="sm" onClick={handleSavePersonal} disabled={isSaving}>
                    <FiCheck className="mr-1" /> {isSaving ? "Saving…" : "Save"}
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={cancelPersonalEdit}
                    disabled={isSaving}
                  >
                    <FiX className="mr-1" /> Cancel
                  </Button>
                </div>
              )}
            </div>

            <div className="p-4 2xl:p-5">
              {isLoading ? (
                <div className="grid min-h-[220px] 2xl:min-h-[250px] place-items-center text-xs 2xl:text-sm text-[#78847E]">
                  Loading profile…
                </div>
              ) : isPersonalEditing ? (
                <div className="space-y-5">
                  <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 [&_label]:!text-[12px] 2xl:[&_label]:!text-[14px]">
                    <div>
                      <Label>
                        Name <span className="text-[#B75A5A]">*</span>
                      </Label>
                      <Input
                        type="text"
                        value={form.name}
                        onChange={handlePersonalChange("name")}
                        onBlur={() => {
                          setPersonalTouched((previous) => ({ ...previous, name: true }));
                          validatePersonalField("name");
                        }}
                        maxLength={50}
                        className="!h-9 !text-[12px] 2xl:!h-11 2xl:!text-[14px]"
                        error={Boolean(personalTouched.name && personalErrors.name)}
                      />
                      {personalTouched.name && personalErrors.name && (
                        <p className="mt-1.5 text-xs text-[#B75A5A]">{personalErrors.name}</p>
                      )}
                    </div>

                    <div>
                      <Label>
                        Phone Number <span className="text-[#B75A5A]">*</span>
                      </Label>
                      <Input
                        type="text"
                        value={form.phone_number}
                        onChange={handlePersonalChange("phone_number")}
                        onBlur={() => {
                          setPersonalTouched((previous) => ({
                            ...previous,
                            phone_number: true,
                          }));
                          validatePersonalField("phone_number");
                        }}
                        maxLength={20}
                        className="!h-9 !text-[12px] 2xl:!h-11 2xl:!text-[14px]"
                        inputMode="tel"
                        error={Boolean(
                          personalTouched.phone_number && personalErrors.phone_number
                        )}
                      />
                      {personalTouched.phone_number && personalErrors.phone_number && (
                        <p className="mt-1.5 text-xs text-[#B75A5A]">
                          {personalErrors.phone_number}
                        </p>
                      )}
                    </div>
                  </div>

                  <div className="rounded-2xl border border-[#DDEBE4] bg-[#F8FCFA] p-4">
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                      <div>
                        <p className="text-xs font-bold uppercase tracking-[0.08em] text-[#8A948F]">
                          Password
                        </p>
                        <p className="mt-1 text-xs 2xl:text-sm font-semibold text-[#33413A]">••••••••••</p>
                        <p className="mt-1 text-[11px] text-[#78847E]">
                          We will send a secure password reset link to your registered email.
                        </p>
                      </div>
                      <button
                        type="button"
                        onClick={handleForgotPassword}
                        disabled={isSendingReset || resetEmailSent}
                        className={cx(
                          "inline-flex h-9 items-center justify-center rounded-lg border px-3 text-xs font-semibold transition",
                          resetEmailSent
                            ? "border-[#B9D8C9] bg-[#EAF7F1] text-[#0E8558]"
                            : "border-[#B9D8C9] bg-white text-[#0E8558] hover:bg-[#F2FAF6]",
                          isSendingReset && "cursor-not-allowed opacity-60"
                        )}
                      >
                        {isSendingReset
                          ? "Sending…"
                          : resetEmailSent
                            ? "Reset email sent"
                            : "Reset password"}
                      </button>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="space-y-5">
                  <div className="rounded-2xl bg-[#FBFDFC] p-4">
                    <div className="grid grid-cols-1 gap-x-6 gap-y-4 sm:grid-cols-2">
                      {/* Name */}
                      <div className="min-w-0">
                        <p className="text-[10px] 2xl:text-[11px] font-semibold uppercase tracking-[0.08em] text-[#8A948F]">
                          Name
                        </p>

                        <p className="mt-1 truncate text-sm 2xl:text-[15px] font-bold text-[#26322C]">
                          {form.name || "-"}
                        </p>
                      </div>

                      {/* Email */}
                      <div className="min-w-0">
                        <p className="text-[10px] 2xl:text-[11px] font-semibold uppercase tracking-[0.08em] text-[#8A948F]">
                          Email
                        </p>

                        <p
                          title={data?.email || ""}
                          className="mt-1 truncate text-xs 2xl:text-sm font-semibold text-[#33413A]"
                        >
                          {data?.email || "-"}
                        </p>
                      </div>

                      {/* Phone Number */}
                      <div className="min-w-0">
                        <p className="text-[10px] 2xl:text-[11px] font-semibold uppercase tracking-[0.08em] text-[#8A948F]">
                          Phone number
                        </p>

                        <p className="mt-1 text-xs 2xl:text-sm font-semibold text-[#33413A]">
                          {form.phone_number || "-"}
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-2xl border border-[#DDEBE4] bg-[#F8FCFA] p-4">
                    <div className="flex items-center gap-3">
                      <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-white text-green-500 shadow-sm">
                        <FiLock size={18} />
                      </span>
                      <div>
                        <p className="text-xs 2xl:text-sm font-bold text-[#33413A]">Password</p>
                        <p className="mt-0.5 text-xs text-[#78847E]">{"\u2022".repeat(10)}</p>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </Panel>

          <div ref={companySectionRef} className="h-full scroll-mt-24">
            <Panel
              className={cx(
                "overflow-hidden transition-all duration-300",
                guideStep === "company" &&
                  "border-green-500 ring-4 ring-green-500/10 shadow-[0_14px_38px_rgba(21,154,103,0.12)]"
              )}
            >
            <div className="flex flex-col gap-3 border-b border-[#E8ECEA] px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <div className="flex flex-wrap items-center gap-2">
                  <h2 className="text-sm 2xl:text-base font-bold text-[#37455F]">2. Company info</h2>

                  {guideStep === "company" ? (
                    <span className="rounded-full bg-green-500 px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.08em] text-white shadow-sm">
                      Step 1 · Start here
                    </span>
                  ) : (
                    <StatusPill tone={isCompanyComplete ? "green" : "blue"}>
                      {isCompanyComplete ? "Completed" : "In progress"}
                    </StatusPill>
                  )}
                </div>

                <p className="mt-1 text-[11px] 2xl:text-xs text-[#78847E]">
                  {guideStep === "company"
                    ? "Complete your company information first. This will unlock SKU sheet upload."
                    : "Tell us about your business."}
                </p>
              </div>

              {!isCompanyEditing ? (
                <button
                  type="button"
                  onClick={() => setIsCompanyEditing(true)}
                  className="inline-flex h-9 items-center gap-2 rounded-lg border border-[#DDE5E1] px-3 text-xs font-semibold text-[#37455F] hover:bg-[#F5F8F6]"
                >
                  <FiEdit3 size={14} /> {isCompanyComplete ? "Edit details" : "Add details"}
                </button>
              ) : (
                <div className="flex shrink-0 items-center gap-2">
                  <Button
                    type="button"
                    size="sm"
                    onClick={handleSaveCompany}
                    disabled={isSaving}
                  >
                    <FiCheck className="mr-1" />
                    {isSaving ? "Saving…" : "Save"}
                  </Button>

                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      setIsCompanyEditing(false);
                      void fetchUserData();
                    }}
                    disabled={isSaving}
                  >
                    <FiX className="mr-1" /> Cancel
                  </Button>
                </div>
              )}
            </div>

            <div className="p-4 2xl:p-5">
              {isLoading ? (
                <div className="grid min-h-[220px] 2xl:min-h-[250px] place-items-center text-xs 2xl:text-sm text-[#78847E]">
                  Loading profile…
                </div>
              ) : isCompanyEditing ? (
                <>
                  <div className="grid grid-cols-1 gap-x-3 gap-y-3 sm:grid-cols-3 2xl:gap-x-5 2xl:gap-y-4 [&_label]:!text-[12px] 2xl:[&_label]:!text-[14px] [&_input]:!h-9 [&_input]:!text-[12px] 2xl:[&_input]:!h-11 2xl:[&_input]:!text-[14px]">
                    <div>
                      <Label>Company Name <span className="text-[#B75A5A]">*</span></Label>
                      <Input type="text" value={form.company_name} onChange={handleCompanyChange("company_name")} onBlur={() => markCompanyTouched("company_name")} maxLength={80} error={Boolean(companyTouched.company_name && companyErrors.company_name)} />
                      {showCompanyError("company_name")}
                    </div>
                    <div>
                      <Label>Brand Name <span className="text-[#B75A5A]">*</span></Label>
                      <Input type="text" value={form.brand_name} onChange={handleCompanyChange("brand_name")} onBlur={() => markCompanyTouched("brand_name")} maxLength={80} error={Boolean(companyTouched.brand_name && companyErrors.brand_name)} />
                      {showCompanyError("brand_name")}
                    </div>
                    <div>
                      <Label>Home Currency <span className="text-[#B75A5A]">*</span></Label>
                      <select value={form.homeCurrency} onChange={handleCompanyChange("homeCurrency")} onBlur={() => markCompanyTouched("homeCurrency")} className={inputClass}>
                        <option value="">Select Currency</option>
                        {CURRENCY_OPTIONS.map((currency) => <option key={currency} value={currency}>{currency}</option>)}
                      </select>
                      {showCompanyError("homeCurrency")}
                    </div>
                    <div>
                      <Label>GST No.</Label>
                      <Input type="text" value={form.gst_no} onChange={handleCompanyChange("gst_no")} onBlur={() => markCompanyTouched("gst_no")} maxLength={15} error={Boolean(companyTouched.gst_no && companyErrors.gst_no)} />
                      {showCompanyError("gst_no")}
                    </div>
                    <div>
                      <Label>PAN No.</Label>
                      <Input type="text" value={form.pan_no} onChange={handleCompanyChange("pan_no")} onBlur={() => markCompanyTouched("pan_no")} maxLength={10} error={Boolean(companyTouched.pan_no && companyErrors.pan_no)} />
                      {showCompanyError("pan_no")}
                    </div>
                    <div>
                      <Label>Building No. <span className="text-[#B75A5A]">*</span></Label>
                      <Input type="text" value={form.address_building} onChange={handleCompanyChange("address_building")} onBlur={() => markCompanyTouched("address_building")} maxLength={120} error={Boolean(companyTouched.address_building && companyErrors.address_building)} />
                      {showCompanyError("address_building")}
                    </div>
                    <div>
                      <Label>City <span className="text-[#B75A5A]">*</span></Label>
                      <Input type="text" value={form.address_city} onChange={handleCompanyChange("address_city")} onBlur={() => markCompanyTouched("address_city")} maxLength={60} error={Boolean(companyTouched.address_city && companyErrors.address_city)} />
                      {showCompanyError("address_city")}
                    </div>
                    <div>
                      <Label>State <span className="text-[#B75A5A]">*</span></Label>
                      <Input type="text" value={form.address_state} onChange={handleCompanyChange("address_state")} onBlur={() => markCompanyTouched("address_state")} maxLength={60} error={Boolean(companyTouched.address_state && companyErrors.address_state)} />
                      {showCompanyError("address_state")}
                    </div>
                    <div>
                      <Label>Country / Region <span className="text-[#B75A5A]">*</span></Label>
                      <Input type="text" value={form.address_country} onChange={handleCompanyChange("address_country")} onBlur={() => markCompanyTouched("address_country")} maxLength={60} error={Boolean(companyTouched.address_country && companyErrors.address_country)} />
                      {showCompanyError("address_country")}
                    </div>
                    <div>
                      <Label>Zipcode <span className="text-[#B75A5A]">*</span></Label>
                      <Input type="text" value={form.address_zipcode} onChange={handleCompanyChange("address_zipcode")} onBlur={() => markCompanyTouched("address_zipcode")} maxLength={12} error={Boolean(companyTouched.address_zipcode && companyErrors.address_zipcode)} />
                      {showCompanyError("address_zipcode")}
                    </div>
                  </div>


                </>
              ) : (
                <div className="rounded-2xl px-1 py-1">
                  <div className="grid grid-cols-1 gap-x-8 gap-y-6 sm:grid-cols-2 xl:grid-cols-3">
                    <ReadOnlyField label="Company name" value={form.company_name} />
                    <ReadOnlyField label="Brand name" value={form.brand_name} />
                    <ReadOnlyField label="Home currency" value={form.homeCurrency} />
                    <ReadOnlyField label="GST No." value={form.gst_no || "-"} />
                    <ReadOnlyField label="PAN No." value={form.pan_no || "-"} />
                    <ReadOnlyField
                      label="Business address"
                      className="min-w-0"
                      value={[
                        form.address_building,
                        form.address_city,
                        form.address_state,
                        form.address_country,
                        form.address_zipcode,
                      ]
                        .filter(Boolean)
                        .join(", ")}
                    />
                  </div>
                </div>
              )}
            </div>
            </Panel>
          </div>
        </div>

        {/* Row 2: Product + Integrations, exact 50 / 50 */}
        <div className="grid grid-cols-1 gap-5 xl:grid-cols-2 xl:items-stretch">
          <div ref={productSectionRef} className="h-full scroll-mt-24">
            <Panel
              className={cx(
                "relative overflow-hidden transition-all duration-300",
                guideStep === "sku" &&
                  "border-green-500 ring-4 ring-green-500/10 shadow-[0_14px_38px_rgba(21,154,103,0.12)]",
                !canAccessProductControls && "bg-[#FAFBFA]"
              )}
            >
            <div className="flex h-full flex-col p-5">
              <div className="flex gap-4">
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <h2 className="text-sm 2xl:text-base font-bold text-[#37455F]">3. Product & inventory controls</h2>

                    {guideStep === "sku" ? (
                      <span className="rounded-full bg-green-500 px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.08em] text-white shadow-sm">
                        Step 2 · Next step
                      </span>
                    ) : (
                      <StatusPill tone={hasSkuSheet ? "green" : canAccessProductControls ? "blue" : "gray"}>
                        {hasSkuSheet ? "SKU uploaded" : canAccessProductControls ? "Ready" : "Locked"}
                      </StatusPill>
                    )}
                  </div>

                  <p className="mt-2 text-xs 2xl:text-sm font-bold text-[#33413A]">
                    {hasSkuSheet
                      ? "Your SKU sheet is ready"
                      : guideStep === "sku"
                        ? "Upload your SKU sheet to continue"
                        : "SKU upload is locked"}
                  </p>

                  <p className="mt-1 text-[11px] 2xl:text-xs leading-4 2xl:leading-5 text-[#78847E]">
                    {hasSkuSheet
                      ? "Your product data is available. You can replace the sheet whenever you need to update your catalog."
                      : guideStep === "sku"
                        ? "Company information is complete. Upload your product sheet now to unlock marketplace integrations."
                        : "Complete Company Info first. SKU upload will unlock automatically after that."}
                  </p>
                </div>
              </div>

              <button
                type="button"
                disabled={!canAccessProductControls}
                onClick={skuModal.openModal}
                className={cx(
                  "mt-5 flex min-h-[150px] flex-1 w-full flex-col items-center justify-center rounded-2xl border border-dashed px-5 py-5 text-center transition",
                  canAccessProductControls
                    ? "border-[#B9D8C9] bg-[#FBFEFC] hover:border-green-500 hover:bg-[#F4FBF7]"
                    : "cursor-not-allowed border-[#DDE2DF] bg-[#F5F6F5] opacity-70"
                )}
              >
                {canAccessProductControls ? (
                  <FiCloud className="text-green-500" size={28} />
                ) : (
                  <FiLock className="text-[#8D9792]" size={23} />
                )}
                <span className="mt-2 text-[12px] 2xl:text-[14px] font-bold text-[#37455F]">
                  {hasSkuSheet
                    ? "Replace or update your SKU sheet"
                    : canAccessProductControls
                      ? "Upload SKU sheet"
                      : "SKU upload locked"}
                </span>
                <span className="mt-1 text-[11px] 2xl:text-xs text-[#78847E]">
                  {canAccessProductControls
                    ? "Click to browse · .xlsx, .xls, .csv"
                    : "Complete Company Info to unlock"}
                </span>

                {!canAccessProductControls && (
                  <span className="mt-2 inline-flex items-center gap-1.5 rounded-full bg-[#F1F3F2] px-3 py-1 text-[10px] font-bold text-[#74807A]">
                    <FiLock size={11} /> Locked until Step 1 is complete
                  </span>
                )}
              </button>

              <div className="mt-3 flex flex-wrap items-center justify-center gap-4">
                <button
                  type="button"
                  onClick={handleDownloadSkuTemplate}
                  className="inline-flex items-center gap-1.5 text-[12px] 2xl:text-[13px] font-semibold text-[#5EA68E] transition hover:text-[#4a907a]"
                >
                  Download format here
                  <FiDownload size={14} />
                </button>

                {hasSkuSheet && (
                  <>
                    <span className="h-4 w-px bg-[#DDE5E1]" />

                    <button
                      type="button"
                      onClick={handleViewSkuSheet}
                      className="inline-flex items-center gap-1.5 text-[12px] 2xl:text-[13px] font-semibold text-[#41708F] transition hover:text-[#315c78]"
                    >
                      View your sheet
                      <FiEye size={14} />
                    </button>
                  </>
                )}
              </div>
            </div>
            </Panel>
          </div>

          <div ref={integrationSectionRef} className="h-full scroll-mt-24">
            <Panel
              className={cx(
                "relative overflow-hidden transition-all duration-300",
                guideStep === "integration" &&
                  "border-green-500 ring-4 ring-green-500/10 shadow-[0_14px_38px_rgba(21,154,103,0.12)]",
                !canAccessIntegrations && "bg-[#FAFBFA]"
              )}
            >
              <div className="flex h-full flex-col p-5">
                <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                  <div className="flex gap-4">
                    <div>
                      <div className="flex flex-wrap items-center gap-2">
                        <h2 className="text-sm 2xl:text-base font-bold text-[#37455F]">4. Integrations</h2>

                        {guideStep === "integration" ? (
                          <span className="rounded-full bg-green-500 px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.08em] text-white shadow-sm">
                            Step 3 · Final step
                          </span>
                        ) : (
                          <StatusPill tone={hasIntegration ? "green" : canAccessIntegrations ? "blue" : "gray"}>
                            {hasIntegration ? "Connected" : canAccessIntegrations ? "Ready to connect" : "Locked"}
                          </StatusPill>
                        )}
                      </div>

                      <p className="mt-2 text-xs 2xl:text-sm font-bold text-[#33413A]">
                        {hasIntegration
                          ? "Marketplace connection status"
                          : guideStep === "integration"
                            ? "Connect your marketplace to finish setup"
                            : "Marketplace integrations are locked"}
                      </p>

                      <p className="mt-1 text-[11px] 2xl:text-xs leading-4 2xl:leading-5 text-[#78847E]">
                        {hasIntegration
                          ? "Connect or manage your marketplace integrations below."
                          : guideStep === "integration"
                            ? "Your SKU sheet is ready. Connect Amazon or Amazon Ads to complete workspace setup."
                            : "Upload your SKU sheet first. Integrations will unlock automatically after Step 2."}
                      </p>
                    </div>
                  </div>

                  <div className={cx(!canAccessIntegrations && "pointer-events-none opacity-50")}>
                    <IntegrationToggleButton />
                  </div>
                </div>

                <div className="mt-5 border-t border-[#EDF0EE] pt-4">
                  <p className="text-[11px] 2xl:text-xs font-bold text-[#37455F]">Marketplace status</p>
                  <p className="mt-0.5 text-[10px] 2xl:text-[11px] text-[#8A948F]">
                    These cards show the current connection status.
                  </p>
                </div>

                <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-3">
                  {connectedPlatforms.map((platform) => (
                    <div
                      key={platform.id}
                      className="rounded-2xl border border-[#E1E7E3] bg-[#FCFDFC] p-4"
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="flex h-10 w-10 items-center justify-center overflow-hidden rounded-xl bg-white shadow-sm">
                          {platform.id === "amazon-ads" ? (
                            <img
                              src="/images/AmazonAds.jpg"
                              alt="Amazon Ads"
                              className="h-7 w-7 object-contain"
                            />
                          ) : (
                            <SiAmazon className="text-[#202A38]" size={24} />
                          )}
                        </div>
                        {platform.countryCode && (
                          <ReactCountryFlag svg countryCode={platform.countryCode} className="text-lg" />
                        )}
                      </div>

                      <p className="mt-3 text-xs 2xl:text-sm font-bold text-[#33413A]">{platform.label}</p>
                      <span
                        className={cx(
                          "mt-2 inline-flex rounded-full px-2 py-1 text-[10px] font-semibold",
                          platform.connected
                            ? "bg-[#EAF7F1] text-green-500"
                            : "bg-[#F1F3F2] text-[#8A948F]"
                        )}
                      >
                        {platform.connected ? "Connected" : "Not connected"}
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              {!canAccessIntegrations && (
                <div className="absolute inset-0 flex items-center justify-center bg-white/60 backdrop-blur-[1px]">
                  <div className="max-w-[330px] rounded-2xl border border-[#E1E6E3] bg-white px-5 py-4 text-center shadow-sm">
                    <span className="mx-auto flex h-9 w-9 items-center justify-center rounded-full bg-[#F1F3F2] text-[#74807A]">
                      <FiLock size={16} />
                    </span>
                    <p className="mt-2 text-[12px] font-bold text-[#56615C] 2xl:text-[14px]">
                      Step 3 is locked
                    </p>
                    <p className="mt-1 text-[10px] leading-4 text-[#8A948F] 2xl:text-[11px]">
                      Upload your SKU sheet first to unlock marketplace integrations.
                    </p>
                  </div>
                </div>
              )}
            </Panel>
          </div>
        </div>
      </div>

      <Modal
        isOpen={skuModal.isOpen}
        onClose={skuModal.closeModal}
        className="m-4 max-w-[520px] border border-[#D9D9D9] shadow-[6px_6px_7px_0px_#00000026]"
      >
        <div className="relative w-full rounded-2xl bg-white p-4 dark:bg-gray-900 lg:p-8">
          <SkuMultiCountryUpload
            onClose={skuModal.closeModal}
            onComplete={() => {
              setUploadSuccess(true);
              setData((previous: any) => ({ ...previous, sku_sheet_exists: true }));
              void fetchUserData();
              skuModal.closeModal();

              window.setTimeout(() => {
                integrationSectionRef.current?.scrollIntoView({
                  behavior: "smooth",
                  block: "center",
                });
              }, 300);
            }}
          />
        </div>
      </Modal>

      <Modal
        isOpen={skuViewModal.isOpen}
        onClose={skuViewModal.closeModal}
        className="m-4 max-w-[1200px] border border-[#D9D9D9] shadow-[6px_6px_7px_0px_#00000026]"
      >
        <div className="relative w-full rounded-2xl bg-white p-5 dark:bg-gray-900 lg:p-6">
          <div className="flex items-start justify-between gap-4 border-b border-[#E8ECEA] pb-4">
            <div>
              <h2 className="text-base font-bold text-[#37455F] dark:text-white">
                Your SKU Sheet
              </h2>
              <p className="mt-1 text-xs text-[#78847E] dark:text-gray-400">
                This is your currently uploaded SKU information.
              </p>
            </div>

            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={handleDownloadCurrentSkuSheet}
                disabled={isDownloadingSkuSheet || !currentSkuSheet?.rows?.length}
                className="inline-flex h-9 items-center gap-2 rounded-lg border border-[#B9D8C9] bg-[#F8FCFA] px-3 text-xs font-semibold text-[#0E8558] transition hover:bg-[#EFF9F4] disabled:cursor-not-allowed disabled:opacity-60"
              >
                <FiDownload size={14} />
                {isDownloadingSkuSheet ? "Downloading..." : "Download Sheet"}
              </button>

              <button
                type="button"
                onClick={skuViewModal.closeModal}
                className="flex h-9 w-9 items-center justify-center rounded-lg border border-[#DDE5E1] text-[#66746D] transition hover:bg-[#F5F8F6]"
                aria-label="Close SKU sheet"
              >
                <FiX size={16} />
              </button>
            </div>
          </div>

          {isSkuSheetLoading ? (
            <div className="grid min-h-[350px] place-items-center">
              <div className="text-center">
                <div className="mx-auto h-8 w-8 animate-spin rounded-full border-2 border-[#DDE5E1] border-t-green-500" />
                <p className="mt-3 text-xs font-medium text-[#78847E]">
                  Loading your SKU sheet...
                </p>
              </div>
            </div>
          ) : skuSheetError ? (
            <div className="grid min-h-[300px] place-items-center text-center">
              <div>
                <p className="text-sm font-semibold text-[#B75A5A]">
                  Could not load your SKU sheet
                </p>
                <button
                  type="button"
                  onClick={() => void getCurrentSkuSheet()}
                  className="mt-3 text-xs font-semibold text-green-500"
                >
                  Try again
                </button>
              </div>
            </div>
          ) : !currentSkuSheet?.rows?.length ? (
            <div className="grid min-h-[300px] place-items-center">
              <p className="text-sm text-[#78847E]">No SKU data found.</p>
            </div>
          ) : (
            <div className="mt-5 min-w-0 w-full">
              <DataTable
                columns={SKU_VIEW_COLUMNS}
                data={currentSkuSheet.rows as unknown as TableRow[]}
                pageSize={10}
                maxHeight="60vh"
                stickyHeader
                zebra
                emptyMessage="No SKU data found."
                className="my-4 w-full max-w-full min-w-0"
                tableClassName="
                  [&_th]:whitespace-nowrap
                  [&_td]:whitespace-nowrap
                  [&_th]:overflow-hidden
                  [&_th]:text-ellipsis
                  [&_td]:overflow-hidden
                  [&_td]:text-ellipsis
                "
              />
            </div>
          )}
        </div>
      </Modal>
    </div>
  );
}
