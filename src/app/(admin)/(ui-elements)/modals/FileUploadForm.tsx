
"use client";

import React, { useEffect, useMemo, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { Modal } from "@/components/ui/modal";

interface FileUploadFormProps {
  initialCountry: string;
  onClose: () => void;
  onComplete: () => void;
}

const FileUploadForm = ({ initialCountry, onClose, onComplete }: FileUploadFormProps) => {
  // ---------------- Routing ----------------
  const router = useRouter();
  const params = useParams<{ countryName?: string }>();
  const countryName = params?.countryName ?? "";

  // ---------------- Profile from localStorage ----------------
  const profile = useMemo(() => {
    try {
      if (typeof window === "undefined") return {};
      const raw = localStorage.getItem("profile");
      return raw ? JSON.parse(raw) : {};
    } catch {
      return {};
    }
  }, []);

  // ---------------- UI State ----------------
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState("");
  const [modalMessage, setModalMessage] = useState<React.ReactNode>("");
  const [showModal, setShowModal] = useState(false);
  const [modalPromise, setModalPromise] = useState<null | ((value: boolean) => void)>(null);
  const [file1Month, setFile1Month] = useState("");
  const [file2Month, setFile2Month] = useState("");
  const [file1Year, setFile1Year] = useState("");
  const [file2Year, setFile2Year] = useState("");

  const currentYear = new Date().getFullYear();
  const years = Array.from({ length: 2 }, (_, i) => currentYear - 1 + i);

  const [file1, setFile1] = useState<File | null>(null);
  const [file2, setFile2] = useState<File | null>(null);
  const [transitTime] = useState<string>((profile as any).transitTime || "");
  const [stockUnit] = useState<string>((profile as any).stockUnit || "");
  const [country] = useState<string>((profile as any).country || "");
  const [category, setCategory] = useState<string>((profile as any).category || "");
  const [subcategory, setSubcategory] = useState<string>((profile as any).subcategory || "");
  const [categories, setCategories] = useState<string[]>([]);
  const [subcategories, setSubcategories] = useState<string[]>([]);
  const [year, setYear] = useState<string>("");
  const [month, setMonth] = useState<string>("");

  // ---------------- Derived ----------------
  const effectiveCountry = useMemo(
    () => (country || countryName || "").toLowerCase(),
    [country, countryName]
  );

  // ---------------- Helpers ----------------
  const capitalizeFirstLetter = (str: unknown) =>
    typeof str === "string"
      ? str.charAt(0).toUpperCase() + str.slice(1).toLowerCase()
      : "";

  const getAvailableMonths = () => [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
  ];

  const safeMonthIndexValue = (m: string) => {
    if (!m) return "";
    const idx = getAvailableMonths().findIndex(
      (mon) => mon.toLowerCase() === m.toLowerCase()
    );
    return idx >= 0 ? String(idx + 1) : "";
  };

  // ---------------- File Inputs ----------------
  const handleFileChange1 = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    setFile1(file || null);
  };

  const handleFileChange2 = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    setFile2(file || null);
  };

  // ---------------- Selects ----------------
  const handleMonthChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const numeric = Number(e.target.value);
    if (!numeric) {
      setMonth("");
      return;
    }
    const idx = numeric - 1;
    const months = getAvailableMonths();
    setMonth(months[idx]);
  };

  const handleYearChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const selectedYear = e.target.value;
    setYear(selectedYear);
  };

  const updateCategories = () => {
    let options: string[] = [];
    if ((effectiveCountry || "").toUpperCase() === "INDIA") {
      options = ["Health", "Beauty"];
    } else {
      options = ["Select Category"];
    }
    setCategories(options);
    setCategory(options.includes(category) ? category : "");
  };

  const updateSubcategories = () => {
    let options: string[] = [];
    if (category === "Health") {
      options = ["Lubricants", "Intimate Hygiene"];
    } else if (category === "Beauty") {
      options = ["Shampoo", "Soap"];
    } else {
      options = ["Select Subcategory"];
    }
    setSubcategories(options);
    setSubcategory(options.includes(subcategory) ? subcategory : "");
  };

  useEffect(() => {
    updateCategories();
  }, [effectiveCountry]);

  useEffect(() => {
    updateSubcategories();
  }, [category]);

  useEffect(() => {
  if (isUploading) {
    // Disable scroll
    document.body.style.overflow = "hidden";
  } else {
    // Restore scroll
    document.body.style.overflow = "";
  }

  // Cleanup on unmount
  return () => {
    document.body.style.overflow = "";
  };
}, [isUploading]);


  // ---------------- Modal ----------------
  const confirmWithModal = (message: React.ReactNode) =>
    new Promise<boolean>((resolve) => {
      setModalMessage(message);
      setShowModal(true);
      setModalPromise(() => resolve);
    });

  const handleCombinedSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    // 1) Basic file/country guards
    if (!file1 || !file2) {
      setError("Please upload both files.");
      return;
    }
    if (!effectiveCountry) {
      setError("Country is missing. Please open this page with a country selected.");
      return;
    }

    // 2) Ensure month/year (fallback to parsed CSV hints if user didn't choose)
    let finalMonth = month?.toLowerCase() || "";
    let finalYear = (year || "").toString();

    if (!finalMonth) {
      finalMonth = (file1Month || file2Month || "").toLowerCase();
    }
    if (!finalYear) {
      finalYear = (file1Year || file2Year || "").toString();
    }

    if (!finalMonth || !finalYear) {
      setError("Please select a Month and Year (or upload files that contain them).");
      return;
    }

    // 3) Optional: normalize month spelling just in case
    const allowedMonths = [
      "january", "february", "march", "april", "may", "june",
      "july", "august", "september", "october", "november", "december"
    ];
    if (!allowedMonths.includes(finalMonth)) {
      setError(`Invalid month: ${finalMonth}. Please re-select.`);
      return;
    }

    // 4) Token guard (your API requires it)
    const token = localStorage.getItem("jwtToken");
    if (!token) {
      setError("You are not logged in. Please log in and try again.");
      return;
    }

    try {
      // 5) Check if the period already exists (only if we have M/Y)
      let existingUpload: any = null;
      try {
        const historyResponse = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload_history`, {
          method: "GET",
          headers: { Authorization: `Bearer ${token}` },
        });
        // tolerate non-200 here; we'll just skip the replace-confirm
        if (historyResponse.ok) {
          const historyData = await historyResponse.json();
          existingUpload = Array.isArray(historyData?.uploads)
            ? historyData.uploads.find(
              (u: any) =>
                String(u?.year) === String(finalYear) &&
                String(u?.month || "").toLowerCase() === finalMonth &&
                String(u?.country || "").toLowerCase() === effectiveCountry
            )
            : null;
        }
      } catch {
        // ignore history errors; continue with upload
      }

      if (existingUpload) {
        const confirmed = await confirmWithModal(
          <>
            You have already uploaded data for {capitalizeFirstLetter(finalMonth)}/{finalYear} in{" "}
            {effectiveCountry.toUpperCase()}.
            <br />
            Do you want to replace the previous file?
          </>
        );
        if (!confirmed) {
          // User cancelled — keep the page as is
          return;
        }
      }

      // 6) Call your existing upload
      setIsUploading(true);
      // Make sure your submitForm uses the component's current state (month/year)
      // If submitForm reads from state, sync it before calling:
      if (finalMonth !== month) setMonth(finalMonth);
      if (finalYear !== year) setYear(finalYear);

      const responseData = await submitForm(); // <-- your working upload function

      // 7) Redirect to the stats page
      const ranged = "MTD"; // or "QTD" if that's the active tab in your UI
      await router.push(`/pnl-dashboard/${ranged}/${effectiveCountry}/${finalMonth}/${finalYear}`);
    } catch (err) {
      console.error("There was a problem with the file upload:", err);
      setError("Upload failed. Please try again.");
    } finally {
      setIsUploading(false);
    }
  };


  // ---------------- SubmitForm ----------------
  const submitForm = async () => {
    if (!file1 || !file2) throw new Error("Both files are required");

    // --- Find a profile_id robustly ---
    const safeGetJwtPayload = () => {
      try {
        const token = localStorage.getItem("jwtToken");
        if (!token) return null;
        const [, payloadB64] = token.split(".");
        if (!payloadB64) return null;
        const json = atob(payloadB64.replace(/-/g, "+").replace(/_/g, "/"));
        return JSON.parse(json);
      } catch {
        return null;
      }
    };

    const jwtPayload = safeGetJwtPayload();
    const profileIdFromProfile = (profile as any)?.id;
    const profileIdFromJwt =
      (jwtPayload && (jwtPayload.profile_id ?? jwtPayload.user_id)) || null;

    // Final fallback so backend never KeyErrors
    const finalProfileId = String(
      profileIdFromProfile ?? profileIdFromJwt ?? "0"
    );

    const formData = new FormData();
    formData.append("file1", file1);
    formData.append("file2", file2);
    formData.append("transit_time", String(transitTime));
    formData.append("stock_unit", String(stockUnit));
    formData.append("country", effectiveCountry);
    formData.append("category", category);
    formData.append("subcategory", subcategory);
    formData.append("year", year);
    formData.append("month", month);
    formData.append("profile_id", finalProfileId);

    const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/upload`, {
        method: "POST",
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        body: formData,
      });

      const contentType = response.headers.get("Content-Type") || "";
      if (contentType.includes("application/json")) {
        const responseData = await response.json();

        localStorage.setItem("excelFileData", responseData.excel_file ?? "");
        localStorage.setItem("pnlReport", responseData.pnl_report ?? "");
        localStorage.setItem("totalSales", responseData.total_sales ?? "");
        localStorage.setItem("totalProfit", responseData.total_profit ?? "");
        localStorage.setItem("totalFbaFees", responseData.total_fba_fees ?? "");
        localStorage.setItem("totalExpense", responseData.total_expense ?? "");
        localStorage.setItem(
          "platformfee",
          responseData.platform_fee ?? responseData.otherwplatform ?? ""
        );
        localStorage.setItem("expenseChart", responseData.expense_chart_img ?? "");
        localStorage.setItem("salesChart", responseData.sales_chart_img ?? "");

        if (countryName) {
          localStorage.removeItem(`forecast-${countryName}`);
          localStorage.removeItem(`forecast-time-${countryName}`);
        }
        localStorage.removeItem("mergedInventoryData");

        return responseData;
      }

      const text = await response.text();
      throw new Error(text || "Unexpected response from server");
    } catch (err) {
      console.error("There was a problem with the file upload:", err);
      throw err;
    }
  };


  // ---------------- Render ----------------
  return (
    <>
      <div className="w-full h-full overflow-y-auto flex flex-col items-center shadow-[6px_6px_7px_0px_#00000026] ">
        <div className="w-full flex justify-center ">
          <div className="w-full   rounded-xl bg-white  p-4 md:p-5 lg:p-6 text-[13px] md:text-[14px] border border-[#D9D9D9]">
            <h2 className="text-center text-2xl md:text-3xl font-semibold text-[#5EA68E] my-3 md:my-4">
              Upload File <i className="fa-solid fa-cloud-arrow-up" />
            </h2>

            <form onSubmit={handleCombinedSubmit} encType="multipart/form-data" className="space-y-5 md:space-y-6">
              {/* File inputs */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-5">
                <div className="space-y-1.5">
                  <label className="block text-xs font-medium">Month to Date Amazon Report:</label>
                  <div
                    className={`relative h-[160px] md:h-[170px] w-full border border-neutral-700 rounded-xl bg-white flex items-center justify-center overflow-hidden ${file1 ? "ring-2 ring-emerald-500" : ""
                      }`}
                  >
                    <input
                      type="file"
                      id="file1"
                      name="file1"
                      onChange={handleFileChange1}
                      accept=".xls,.xlsx,.csv"
                      required
                      className="absolute inset-0 h-full w-full opacity-0 cursor-pointer z-20"
                    />
                    <img
                      src="/uploadbox.png"
                      alt="file-icon"
                      className="pointer-events-none w-[36vw] max-w-[180px] min-w-[90px] opacity-70"
                    />
                    {file1 && (
                      <p className="pointer-events-none absolute text-center px-2 text-neutral-800 font-medium text-xs md:text-sm break-words">
                        {file1?.name || "Choose File"}
                      </p>
                    )}
                  </div>
                  <p className="text-[#5EA68E] font-semibold text-[11px] md:text-xs m-0">
                    Amazon → Seller Central → Payments → Reports Repository → Report Type Transactions → Select Month
                  </p>
                </div>

                <div className="space-y-1.5">
                  <label className="block text-xs font-medium">Monthly End Inventory File:</label>
                  <div
                    className={`relative h-[160px] md:h-[170px] w-full border border-neutral-700 rounded-xl bg-white flex items-center justify-center overflow-hidden ${file2 ? "ring-2 ring-emerald-500" : ""
                      }`}
                  >
                    <input
                      type="file"
                      id="file2"
                      name="file2"
                      onChange={handleFileChange2}
                      accept=".xls,.xlsx,.csv"
                      required
                      className="absolute inset-0 h-full w-full opacity-0 cursor-pointer z-20"
                    />
                    <img
                      src="/uploadbox.png"
                      alt="file-icon"
                      className="pointer-events-none w-[36vw] max-w-[180px] min-w-[90px] opacity-70"
                    />
                    {file2 && (
                      <p className="pointer-events-none absolute text-center px-2 text-neutral-800 font-medium text-xs md:text-sm break-words">
                        {file2?.name || 'Choose File'}
                      </p>
                    )}
                  </div>
                  <p className="text-[#5EA68E] font-semibold text-[11px] md:text-xs m-0">
                    Amazon → Seller Central → Reports → Fulfilment by amazon → Inventory Ledger → Download
                  </p>
                  <p className="italic text-neutral-600 text-[11px] md:text-xs m-0">
                    *Summary View - Aggregate report by Country. Select last day of the previous month and download in .csv format
                  </p>
                </div>
              </div>

              {error && <p className="text-red-600 text-xs">{error}</p>}

              <div className="relative">
                <input
                  type="text"
                  name="country"
                  id="country"
                  value={(effectiveCountry || "").toUpperCase()}
                  readOnly
                  className="mt-2 w-full rounded-xl border border-neutral-700 px-3 py-2.5 text-sm md:text-[15px] focus:outline-none"
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5 md:gap-4">
                <select
                  id="year"
                  name="year"
                  value={year}
                  onChange={handleYearChange}
                  required
                  className="w-full rounded-xl border border-neutral-700 py-2.5 text-sm md:text-[15px] focus:outline-none"
                >
                  <option value="">Select Year</option>
                  {years.map((yy) => (
                    <option key={yy} value={yy}>
                      {yy}
                    </option>
                  ))}
                </select>

                <select
                  id="month"
                  name="month"
                  value={safeMonthIndexValue(month)}
                  onChange={handleMonthChange}
                  required
                  className="w-full rounded-xl border border-neutral-700 py-2.5 text-sm md:text-[15px] focus:outline-none"
                >
                  <option value="">Select Month</option>
                  {getAvailableMonths().map((m, index) => (
                    <option key={m} value={index + 1}>
                      {capitalizeFirstLetter(m)}
                    </option>
                  ))}
                </select>
              </div>

              <button
                type="submit"
                className="w-full rounded-md bg-slate-700 text-[#f8edcf] shadow-md py-2.5 md:py-3 text-sm md:text-[15px] font-medium hover:bg-slate-800 transition disabled:opacity-60"
                disabled={isUploading}
              >
                Upload
              </button>

              {/* {isUploading && (
                <div className="absolute inset-0 z-20 flex flex-col items-center justify-center backdrop-blur-md bg-black/30">
                  <video
                    src="/infinity2.webm"
                    autoPlay
                    muted
                    loop
                    playsInline
                    className="w-[120px] md:w-[140px]"
                  />
                  <div className="mt-4 text-white text-sm md:text-base">Uploading...</div>
                </div>
              )} */}
              {isUploading && (
                <div className="fixed inset-0 z-[9999] flex flex-col items-center justify-center bg-black/40 backdrop-blur-md">
                  <video
                    src="/infinity2.webm"
                    autoPlay
                    muted
                    loop
                    playsInline
                    className="w-[120px] md:w-[140px]"
                  />
                  <div className="mt-4 text-white text-sm md:text-base">
                    Uploading...
                  </div>
                </div>
              )}

            </form>
          </div>
        </div>

        {/* Modal Component */}
        <Modal
          isOpen={showModal}
          onClose={() => {
            // closing the modal by clicking backdrop / close icon should be treated as cancel
            setShowModal(false);
            if (modalPromise) modalPromise(false);
          }}
          className="max-w-md mx-auto p-5"
          showCloseButton
        >
          <div className="p-2">
            <h3 className="text-lg font-semibold text-gray-800 dark:text-white/90 mb-2">Replace previous upload?</h3>
            <div className="text-sm text-gray-600 dark:text-gray-300">{modalMessage}</div>

            <div className="mt-5 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => {
                  setShowModal(false);
                  if (modalPromise) modalPromise(false);
                }}
                className="inline-flex items-center rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm font-medium text-gray-700 hover:bg-gray-50 dark:border-gray-700 dark:bg-gray-800 dark:text-gray-300"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowModal(false);
                  if (modalPromise) modalPromise(true);
                }}
                className="inline-flex items-center rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-semibold text-white hover:bg-emerald-700"
              >
                Replace
              </button>
            </div>
          </div>
        </Modal>
      </div>
    </>
  );
};

export default FileUploadForm;
