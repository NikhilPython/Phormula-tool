// import { useEffect, useState } from "react";
// import { useFileUploadStatusQuery, useGetCountryProfileQuery } from "@/lib/api/feePreviewApi";
// export const LS_KEYS = {
//   integrationMethod: "integrationMethod",
//   feePreviewDone: (country?: string) => `feePreviewDone_${country ?? "global"}`,
//   mtdDone: (country?: string) => `mtdDone_${country ?? "global"}`,
//   amazonRefreshToken: (country?: string) => `amazonRefreshToken_${country ?? "global"}`,
// } as const;

// export function useIntegrationProgress(countryName?: string) {
//   const country = (countryName || "").toLowerCase();

//   const { data: fileStatus } = useFileUploadStatusQuery();
//   const fileUploaded = !!fileStatus?.file_uploaded;

//   const { data: profile } = useGetCountryProfileQuery(country, { skip: !country });
//   const profileExists = !!profile?.exists;

//   const [integrationMethod, setIntegrationMethod] = useState<string | null>(
//     () => localStorage.getItem(LS_KEYS.integrationMethod)
//   );

//   const [mtdUploaded, setMtdUploaded] = useState<boolean>(() =>
//     localStorage.getItem(LS_KEYS.mtdDone(country)) === "true"
//   );

//   // ✅ FIX: make amazonConnected a true React state
//   const [amazonConnected, setAmazonConnected] = useState<boolean>(() =>
//     !!localStorage.getItem(LS_KEYS.amazonRefreshToken(country))
//   );

//   useEffect(() => {
//     localStorage.setItem(LS_KEYS.feePreviewDone(country), String(profileExists));
//   }, [country, profileExists]);

//   return {
//     country,
//     fileUploaded,
//     profileExists,
//     integrationMethod,
//     setIntegrationMethod,

//     amazonConnected,
//     setAmazonConnected,  // ✅ available now

//     mtdUploaded,
//     setMtdUploaded,
//   };
// }


























import { useEffect, useState } from "react";
import {
  useFileUploadStatusQuery,
  useGetCountryProfileQuery,
} from "@/lib/api/feePreviewApi";

export const LS_KEYS = {
  integrationMethod: "integrationMethod",
  feePreviewDone: (country?: string) => `feePreviewDone_${country ?? "global"}`,
  mtdDone: (country?: string) => `mtdDone_${country ?? "global"}`,
  amazonRefreshToken: (country?: string) =>
    `amazonRefreshToken_${country ?? "global"}`,
} as const;

export function useIntegrationProgress(countryName?: string) {
  const country = (countryName || "").toLowerCase();

  // ---- API-driven state ----
  const {
    data: fileStatus,
    refetch: refetchFileStatus,        // ⬅️ NEW
  } = useFileUploadStatusQuery();

  const fileUploaded = !!fileStatus?.file_uploaded;

  const { data: profile } = useGetCountryProfileQuery(country, {
    skip: !country,
  });
  const profileExists = !!profile?.exists;

  // ---- LocalStorage + React state ----

  const [integrationMethod, setIntegrationMethodState] = useState<string | null>(
    () => {
      if (typeof window === "undefined") return null;
      return localStorage.getItem(LS_KEYS.integrationMethod);
    }
  );

  const [mtdUploaded, setMtdUploadedState] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return localStorage.getItem(LS_KEYS.mtdDone(country)) === "true";
  });

  const [amazonConnected, setAmazonConnectedState] = useState<boolean>(() => {
    if (typeof window === "undefined") return false;
    return !!localStorage.getItem(LS_KEYS.amazonRefreshToken(country));
  });

  useEffect(() => {
    if (typeof window === "undefined") return;

    setMtdUploadedState(localStorage.getItem(LS_KEYS.mtdDone(country)) === "true");
    setAmazonConnectedState(
      !!localStorage.getItem(LS_KEYS.amazonRefreshToken(country))
    );
  }, [country]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    localStorage.setItem(
      LS_KEYS.feePreviewDone(country),
      String(profileExists)
    );
  }, [country, profileExists]);

  const setIntegrationMethod = (method: string | null) => {
    setIntegrationMethodState(method);
    if (typeof window !== "undefined" && method) {
      localStorage.setItem(LS_KEYS.integrationMethod, method);
    }
  };

  const setMtdUploaded = (value: boolean) => {
    setMtdUploadedState(value);
    if (typeof window !== "undefined") {
      localStorage.setItem(LS_KEYS.mtdDone(country), String(value));
    }
  };

  const setAmazonConnected = (value: boolean) => {
    setAmazonConnectedState(value);
  };

  return {
    country,
    fileUploaded,
    profileExists,

    integrationMethod,
    setIntegrationMethod,

    amazonConnected,
    setAmazonConnected,

    mtdUploaded,
    setMtdUploaded,

    refetchFileStatus,          // ⬅️ EXPORTED
  };
}
