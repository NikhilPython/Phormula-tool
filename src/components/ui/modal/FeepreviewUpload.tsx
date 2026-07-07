"use client";
import React from "react";
import { useGetCountriesQuery, useGetCountryProfileQuery } from "@/lib/api/feePreviewApi";
import ConfirmationFeepreview from "@/components/ui/modal/ConfirmationFeepreview";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import Button from "../button/Button";
import { Globe, Store, Clock, Boxes, Upload } from "lucide-react";

type FeepreviewUploadProps = {
  country: string;
  onClose: () => void;
};

export default function FeepreviewUpload({ country: initialCountry, onClose }: FeepreviewUploadProps) {
  const [country, setCountry] = React.useState<string>(initialCountry || "");
  const [file, setFile] = React.useState<File | null>(null);
  const [transitTime, setTransitTime] = React.useState<string>("");
  const [stockUnit, setStockUnit] = React.useState<string>("");
  const [showConfirm, setShowConfirm] = React.useState<boolean>(false);
  const marketplace = "Amazon";

  const { data: countriesData, isLoading: loadingCountries, isError: isCountriesError } =
    useGetCountriesQuery();
  const countries: string[] = countriesData?.countries ?? [];

  const { data: profileData, isFetching: loadingProfile } = useGetCountryProfileQuery(country, {
    skip: !country || country === "NA" || country === "global",
  });

  React.useEffect(() => {
    if (!profileData) return;
    if (profileData.transit_time != null) setTransitTime(String(profileData.transit_time));
    if (profileData.stock_unit != null) setStockUnit(String(profileData.stock_unit));
  }, [profileData]);

  const onCountryChange = (e: React.ChangeEvent<HTMLSelectElement>) => setCountry(e.target.value);
  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) =>
    setFile(e.target.files?.[0] ?? null);

  const onSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setShowConfirm(true); // 🔁 switch the whole modal to the confirmation/table view
  };

  // ✅ Replace the form with the confirmation view
  if (showConfirm) {
    return (
      <div className="w-full">
        <ConfirmationFeepreview
          country={country}
          marketplace={marketplace}
          file={file}
          transitTime={transitTime}
          stockUnit={stockUnit}
          onBack={() => setShowConfirm(false)}
        />
      </div>
    );
  }

  // ⬇️ Form view (shown until "Next")
  return (
    <div className="w-full">
      <PageBreadcrumb pageTitle="Fee Preview Upload" variant="table" />

     <form onSubmit={onSubmit} className="space-y-4" encType="multipart/form-data">
  {/* Country */}
  <div>
    <label className="mb-1 block text-sm font-medium text-gray-700">Country</label>
    <div className="relative">
      <Globe
        size={18}
        className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none"
      />
      <select
        name="country"
        id="country"
        value={country}
        onChange={onCountryChange}
        required
        className="w-full rounded-lg border border-gray-400 pl-10 pr-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
      >
        <option value="" disabled>
          {loadingCountries ? "Loading…" : "Select Country"}
        </option>
        {!loadingCountries &&
          !isCountriesError &&
          countries.map((c) => (
            <option key={c} value={c.toLowerCase()}>
              {c.toUpperCase()}
            </option>
          ))}
      </select>
    </div>
    {isCountriesError && (
      <p className="mt-1 text-xs text-red-500">Failed to load countries.</p>
    )}
  </div>

  {/* Marketplace */}
  <div>
    <label className="mb-1 block text-sm font-medium text-gray-700">Marketplace</label>
    <div className="relative">
      <Store
        size={18}
        className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none"
      />
      <input
        type="text"
        value="Amazon"
        readOnly
        className="w-full rounded-lg border border-gray-400 bg-gray-50 pl-10 pr-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
      />
    </div>
  </div>

  {/* Transit Time */}
  <div>
    <label htmlFor="transitTime" className="mb-1 block text-sm font-medium text-gray-700">
      Transit Time (in months)
    </label>
    <div className="relative">
      <Clock
        size={18}
        className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none"
      />
      <input
        id="transitTime"
        name="transitTime"
        type="number"
        inputMode="numeric"
        value={transitTime}
        onChange={(e) => setTransitTime(e.target.value)}
        required
        className="w-full rounded-lg border border-gray-400 pl-10 pr-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
      />
    </div>
    {loadingProfile && (
      <p className="mt-1 text-xs text-gray-500">Loading existing profile…</p>
    )}
  </div>

  {/* Stock Unit */}
  <div>
    <label htmlFor="stockUnit" className="mb-1 block text-sm font-medium text-gray-700">
      Stock Keeping Unit (in months)
    </label>
    <div className="relative">
      <Boxes
        size={18}
        className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none"
      />
      <input
        id="stockUnit"
        name="stockUnit"
        type="number"
        inputMode="numeric"
        value={stockUnit}
        onChange={(e) => setStockUnit(e.target.value)}
        required
        className="w-full rounded-lg border border-gray-400 pl-10 pr-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
      />
    </div>
  </div>

  {/* File upload */}
  <div>
    <label className="mb-1 block text-sm font-medium text-gray-700">
      Fee Preview Details File
    </label>

    <div className="relative">
      <Upload
        size={18}
        className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500 pointer-events-none z-10"
      />
      <input
        type="file"
        name="file"
        id="file"
        accept=".xlsx,.xls"
        onChange={onFileChange}
        required
        className="block w-full cursor-pointer rounded-lg border border-gray-400 bg-white text-sm pl-10
          file:mr-3 file:rounded-md file:border-0 file:bg-gray-100 file:px-3 file:py-2 file:text-sm file:font-medium hover:file:bg-gray-200"
      />
    </div>

    <p className="mt-2 text-sm text-[#5EA68E] font-semibold">
      Amazon → Seller Central → Reports → Fulfillment → Fee Preview
      <br />{" "}
      <span className="text-[#414042]">
        Download <b>.xlsx</b> (or .xls) for current FBA inventory
      </span>
    </p>
  </div>

  {/* Submit */}
  <div className="flex items-center justify-center gap-3 w-full">
    <button
      type="submit"
      className="w-full justify-center rounded-lg bg-[#2c3854] px-4 py-2 text-sm font-semibold text-[#f8edcf] hover:opacity-95 disabled:opacity-60"
    >
      Next
    </button>
  </div>
</form>

    </div>
  );
}









// "use client";

// import React from "react";
// import {
//   useGetCountriesQuery,
//   useGetCountryProfileQuery,
//   useUploadFeePreviewMutation,
// } from "@/lib/api/feePreviewApi";
// import PageBreadcrumb from "@/components/common/PageBreadCrumb";
// import Button from "../button/Button";
// import { Modal } from ".";
// import { AiOutlineCheckCircle } from "react-icons/ai";

// type FeepreviewUploadProps = {
//   country: string;
//   onClose: () => void;
// };

// export default function FeepreviewUpload({
//   country: initialCountry,
//   onClose,
// }: FeepreviewUploadProps) {
//   const [country, setCountry] = React.useState<string>(initialCountry || "");
//   const [transitTime, setTransitTime] = React.useState<string>("");
//   const [stockUnit, setStockUnit] = React.useState<string>("");
//   const [error, setError] = React.useState<string>("");
//   const [showSuccess, setShowSuccess] = React.useState<boolean>(false);
//   const marketplace = "Amazon";

//   // queries
//   const { data: countriesData, isLoading: loadingCountries, isError: isCountriesError } =
//     useGetCountriesQuery();
//   const countries: string[] = countriesData?.countries ?? [];

//   const { data: profileData, isFetching: loadingProfile } = useGetCountryProfileQuery(country, {
//     skip: !country || country === "NA" || country === "global",
//   });

//   React.useEffect(() => {
//     if (!profileData) return;
//     if (profileData.transit_time != null) setTransitTime(String(profileData.transit_time));
//     if (profileData.stock_unit != null) setStockUnit(String(profileData.stock_unit));
//   }, [profileData]);

//   // ✅ use your existing upload mutation
//   const [uploadFeePreview, { isLoading: isSaving }] = useUploadFeePreviewMutation();

//   const onCountryChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
//     setCountry(e.target.value);
//     setError("");
//   };

//   // const onSubmit = async (e: React.FormEvent) => {
//   //   e.preventDefault();
//   //   setError("");

//   //   const t = Number.parseInt(String(transitTime), 10);
//   //   const s = Number.parseInt(String(stockUnit), 10);

//   //   if (!country) return setError("Country is required.");
//   //   if (!Number.isFinite(t) || t <= 0) return setError("Enter a valid Transit Time (months).");
//   //   if (!Number.isFinite(s) || s <= 0) return setError("Enter a valid Stock Keeping Unit (months).");

//   //   try {
//   //     // 🔁 Send the SAME endpoint payload but without file
//   //     // If your backend expects multipart, sending FormData is safest.
//   //     const form = new FormData();
//   //     form.append("country", country);
//   //     form.append("marketplace", marketplace);
//   //     form.append("transit_time", String(t));
//   //     form.append("stock_unit", String(s));
//   //     // let the backend know there is intentionally no file
//   //     form.append("skip_file", "true");

//   //     await uploadFeePreview(form as any).unwrap();

//   //     // optional local caching
//   //     localStorage.setItem("country", String(country));
//   //     localStorage.setItem("transitTime", String(t));
//   //     localStorage.setItem("stockUnit", String(s));

//   //     setShowSuccess(true);
//   //   } catch (err: any) {
//   //     console.error("Save failed:", err);
//   //     const msg =
//   //       err?.data?.message || err?.error || err?.message || "Failed to save. Please try again.";
//   //     setError(msg);
//   //   }
//   // };

// const onSubmit = async (e: React.FormEvent) => {
//   e.preventDefault();
//   setError("");

//   const t = Number.parseInt(String(transitTime), 10);
//   const s = Number.parseInt(String(stockUnit), 10);

//   if (!country) return setError("Country is required.");
//   if (!Number.isFinite(t) || t <= 0) return setError("Enter a valid Transit Time (months).");
//   if (!Number.isFinite(s) || s <= 0) return setError("Enter a valid Stock Keeping Unit (months).");

//   try {
//     // ✅ Call the RTK Query mutation with a JSON body (no file)
//     await uploadFeePreview({
//       country,
//       marketplace,          // e.g. "Amazon"
//       transit_time: t,
//       stock_unit: s,
//       // file: undefined    // (omit)
//     }).unwrap();

//     // Optional local cache
//     localStorage.setItem("country", String(country));
//     localStorage.setItem("transitTime", String(t));
//     localStorage.setItem("stockUnit", String(s));

//     setShowSuccess(true);
//   } catch (err: any) {
//     console.error("Save failed:", err);
//     const msg =
//       err?.data?.message ||
//       err?.data?.error ||
//       err?.error ||
//       err?.message ||
//       "Failed to save. Please try again.";
//     setError(msg);
//   }
// };

//   const closeSuccess = () => {
//     setShowSuccess(false);
//     onClose(); // close parent modal after success
//   };

//   return (
//     <div className="w-full">
//       <PageBreadcrumb pageTitle="Fee Preview" variant="table" />

//       <form onSubmit={onSubmit} className="space-y-4">
//         <div>
//           <label className="mb-1 block text-sm font-medium text-gray-700">Country</label>
//           <select
//             name="country"
//             id="country"
//             value={country}
//             onChange={onCountryChange}
//             required
//             className="w-full rounded-lg border border-gray-400 px-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
//           >
//             <option value="" disabled>
//               {loadingCountries ? "Loading…" : "Select Country"}
//             </option>
//             {!loadingCountries &&
//               !isCountriesError &&
//               countries.map((c) => (
//                 <option key={c} value={c.toLowerCase()}>
//                   {c.toUpperCase()}
//                 </option>
//               ))}
//           </select>
//           {isCountriesError && (
//             <p className="mt-1 text-xs text-red-500">Failed to load countries.</p>
//           )}
//         </div>

//         <div>
//           <label className="mb-1 block text-sm font-medium text-gray-700">Marketplace</label>
//           <input
//             type="text"
//             value={marketplace}
//             readOnly
//             className="w-full rounded-lg border border-gray-400 bg-gray-50 px-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
//           />
//         </div>

//         <div>
//           <label htmlFor="transitTime" className="mb-1 block text-sm font-medium text-gray-700">
//             Transit Time (in months)
//           </label>
//           <input
//             id="transitTime"
//             name="transitTime"
//             type="number"
//             inputMode="numeric"
//             value={transitTime}
//             onChange={(e) => setTransitTime(e.target.value)}
//             required
//             className="w-full rounded-lg border border-gray-400 px-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
//           />
//           {loadingProfile && (
//             <p className="mt-1 text-xs text-gray-500">Loading existing profile…</p>
//           )}
//         </div>

//         <div>
//           <label htmlFor="stockUnit" className="mb-1 block text-sm font-medium text-gray-700">
//             Stock Keeping Unit (in months)
//           </label>
//           <input
//             id="stockUnit"
//             name="stockUnit"
//             type="number"
//             inputMode="numeric"
//             value={stockUnit}
//             onChange={(e) => setStockUnit(e.target.value)}
//             required
//             className="w-full rounded-lg border border-gray-400 px-4 py-2.5 text-sm focus:border-green-500 focus:outline-hidden focus:ring-2 focus:ring-emerald-500/10"
//           />
//         </div>

//         {error && <p className="text-sm text-red-600">{error}</p>}

//         <div className="flex items-center justify-end gap-3 pt-2">
//           <Button variant="outline" size="sm" onClick={onClose} disabled={isSaving}>
//             Cancel
//           </Button>
//           <Button
//             variant="primary"
//             size="sm"
//             type="submit"
//             disabled={isSaving || !country || !transitTime || !stockUnit}
//           >
//             {isSaving ? "Saving…" : "Submit"}
//           </Button>
//         </div>
//       </form>

//       {/* Success Modal */}
//       <Modal isOpen={showSuccess} onClose={closeSuccess} className="m-4 max-w-[520px]">
//         <div className="relative w-full rounded-3xl bg-white p-8 text-center dark:bg-gray-900">
//           <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-full bg-emerald-50">
//             <AiOutlineCheckCircle className="h-8 w-8 text-emerald-600" />
//           </div>
//         <h3 className="mb-2 text-lg font-semibold text-gray-800 dark:text-white/90">
//             Successfully saved
//           </h3>
//           <p className="mb-6 text-sm text-gray-600 dark:text-gray-400">
//             Your fee preview settings for <b>{country?.toUpperCase()}</b> have been saved.
//           </p>
//           <div className="flex justify-center">
//             <Button variant="primary" size="sm" onClick={closeSuccess}>
//               Done
//             </Button>
//           </div>
//         </div>
//       </Modal>
//     </div>
//   );
// }
