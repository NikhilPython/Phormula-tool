// src/lib/utils/handleRegionChange-react-router.ts
import type { UploadItem } from "@/lib/api/feePreviewApi";
import { getMostRecentUpload } from "@/lib/utils/region";

type Args = {
  value: string; // new country selected
  ranged?: string; // QTD/MTD/etc
  uploadHistory: UploadItem[];
  navigate: (to: string) => void; // from react-router
  onAddMore: () => void; // e.g. navigate("/Choosecountry")
  onBeforeNavigate?: (country: string) => void; // e.g. clear caches
};

export function handleRegionChangeReactRouter({
  value,
  ranged,
  uploadHistory,
  navigate,
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

  // --- GLOBAL: most recent across ALL uploads ---
  if (country === "global") {
    if (uploadHistory.length > 0) {
      const mostRecent = getMostRecentUpload(uploadHistory);
      navigate(`/pnl-dashboard/${range}/global/${mostRecent.month}/${mostRecent.year}`);
    } else {
      navigate(`/pnl-dashboard/${range}/global/NA/NA`);
    }
    // keep your existing reload behavior
    setTimeout(() => window.location.reload(), 100);
    return;
  }

  // --- Normal countries ---
  const countryUploads = uploadHistory.filter(
    (u) => (u.country || "").toLowerCase() === country
  );

  if (countryUploads.length > 0) {
    const mostRecent = getMostRecentUpload(countryUploads);
    navigate(`/pnl-dashboard/${range}/${country}/${mostRecent.month}/${mostRecent.year}`);
    setTimeout(() => window.location.reload(), 100);
  } else {
    navigate(`/pnl-dashboard/${range}/${country}/NA/NA`);
  }
}
