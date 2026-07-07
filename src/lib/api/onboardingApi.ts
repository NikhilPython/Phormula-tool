// src/lib/api/onboardingApi.ts
import { baseApi } from "./baseApi";

/** ===== Types ===== */
export type SelectFormReq = {
  country: string;             // comma-separated codes: "us, uk"
  company_name?: string;
  brand_name?: string;
  homeCurrency?: string;
  /** added to support Revenue step */
  annual_sales_range?: string; // e.g. "$0 - $50K"
};

export type SelectFormRes = {
  success: boolean;
  message?: string;
};

export type MarkOnboardingCompleteReq = {
  onboarding_complete: boolean; // usually true
};

export type MarkOnboardingCompleteRes = {
  success: boolean;
  message?: string;
};

/** ===== API ===== */
export const onboardingApi = baseApi.injectEndpoints({
  endpoints: (build) => ({
    submitSelectForm: build.mutation<SelectFormRes, SelectFormReq>({
      query: (body) => ({
        url: "/selectform",
        method: "POST",
        body,
      }),
      invalidatesTags: ["Profile", "User"],
    }),

    /** NEW: mark onboarding complete */
    markOnboardingComplete: build.mutation<
      MarkOnboardingCompleteRes,
      MarkOnboardingCompleteReq
    >({
      query: (body) => ({
        url: "/onboarding_complete",
        method: "POST",
        body,
      }),
      invalidatesTags: ["User", "Profile"],
    }),
  }),
});

export const {
  useSubmitSelectFormMutation,
  useMarkOnboardingCompleteMutation, 
} = onboardingApi;
