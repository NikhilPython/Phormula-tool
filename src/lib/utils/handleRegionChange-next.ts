// src/lib/utils/handleRegionChange-next.ts
import type { UploadItem } from "@/lib/api/feePreviewApi";
import { getMostRecentUpload } from "@/lib/utils/region";

type Args = {
  value: string;
  ranged?: string;
  uploadHistory: UploadItem[];
  push: (href: string) => void; // from next/navigation's router.push
  onAddMore: () => void; // e.g. push("/settings/countries")
  onBeforeNavigate?: (country: string) => void;
};

export function handleRegionChangeNext({
  value,
  ranged,
  uploadHistory,
  push,
  onAddMore,
  onBeforeNavigate,
}: Args) {
  const country = (value || "").toLowerCase();

  if (country === "add_more") {
    onAddMore();
    return;
  }

  onBeforeNavigate?.(country);

  const range = ranged || "QTD";

  // --- GLOBAL: pick most recent across ALL uploads ---
  if (country === "global") {
    if (uploadHistory.length > 0) {
      const mostRecent = getMostRecentUpload(uploadHistory);
      push(`/pnl-dashboard/${range}/global/${mostRecent.month}/${mostRecent.year}`);
    } else {
      push(`/pnl-dashboard/${range}/global/NA/NA`);
    }
    return;
  }

  // --- Normal countries ---
  const countryUploads = uploadHistory.filter(
    (u) => (u.country || "").toLowerCase() === country
  );

  if (countryUploads.length > 0) {
    const mostRecent = getMostRecentUpload(countryUploads);
    push(`/pnl-dashboard/${range}/${country}/${mostRecent.month}/${mostRecent.year}`);
  } else {
    push(`/pnl-dashboard/${range}/${country}/NA/NA`);
  }
}
