// src/lib/utils/region.ts
import type { UploadItem } from "@/lib/api/feePreviewApi";

export const monthMap: Record<string, number> = {
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
};

/** Case-insensitive "latest" finder for a given country's uploads
 *  (or across all uploads if you pass the whole list).
 */
export function getMostRecentUpload(uploads: UploadItem[]): UploadItem {
  return uploads.reduce((latest, upload) => {
    const latestMonthNum = monthMap[(latest.month ?? "").toLowerCase()] ?? 0;
    const uploadMonthNum = monthMap[(upload.month ?? "").toLowerCase()] ?? 0;

    if (
      upload.year > latest.year ||
      (upload.year === latest.year && uploadMonthNum > latestMonthNum)
    ) {
      return upload;
    }
    return latest;
  }, uploads[0]);
}

/** Build dropdown options from countries list + special items */
export function buildRegionOptions(countryList: string[]) {
  const base = countryList.map((c) => ({
    value: c.toLowerCase(),
    label: c.toUpperCase(),
  }));

  return [
    ...base,
    { value: "global", valueUpper: "GLOBAL", label: "Global Snapshot" },
    { value: "add_more", label: "Add more Countries" },
  ].map(({ value, label }) => ({ value, label }));
}

/** Compute the default “home” path for a country (QTD by default).
 *
 * For `global`, we pick the most recent upload across *all* uploads and route to:
 *   /country/${range}/global/${mostRecent.month}/${mostRecent.year}
 */
export function computeHomePath(
  selectedCountry: string,
  ranged: string | undefined,
  uploads: UploadItem[]
) {
  const range = ranged || "QTD";
  const country = (selectedCountry || "").toLowerCase();

  // --- GLOBAL: pick most recent across ALL uploads ---
  if (country === "global") {
    if (uploads.length > 0) {
      const mostRecent = getMostRecentUpload(uploads);
      return `/pnl-dashboard/${range}/global/${mostRecent.month}/${mostRecent.year}`;
    }
    return `/pnl-dashboard/${range}/global/NA/NA`;
  }

  // --- Normal country logic ---
  const countryUploads = uploads.filter(
    (u) => (u.country || "").toLowerCase() === country
  );

  if (countryUploads.length > 0) {
    const mostRecent = getMostRecentUpload(countryUploads);
    return `/pnl-dashboard/${range}/${country}/${mostRecent.month}/${mostRecent.year}`;
  }

  return `/pnl-dashboard/${range}/${country}/NA/NA`;
}
