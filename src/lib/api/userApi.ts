import { baseApi } from "./baseApi";

export type User = {
  id?: number;
  name?: string;
  email?: string;
  phone_number?: string;
  brand_name?: string;
  company_name?: string;
  annual_sales_range?: string;
  homeCurrency?: string;
  target_sales?: number | null;
  tax_id?: any;
  address?: any;
  member_name?: string;     // agar user member hai to ye field aayegi (client profile tab ke liye)

  // ✅ CLIENT onboarding / profile fields (agar backend bhejta hai)
  country?: string;          // e.g. "UK" or "US,UK"
  marketplace_id?: string;   // current selected marketplace id (client)

  // ✅ MEMBER SUPPORT (token/member_login response se store karne ke liye)
  is_member?: boolean;
  member_id?: number;
  owner_user_id?: number;
  modules?: string[];        // allowed sidebar modules
  marketplaces?: string[];   // member marketplace_ids
  countries?: string[];      // member countries array
};


type UploadHistoryRes = { uploads: Array<any> };

type SelectFormReq = {
  country?: string;         // "us, uk"
  company_name?: string;
  brand_name?: string;
  homeCurrency?: string;
  annual_sales_range?: string;
};

type SelectFormRes = { success: boolean; message?: string };

type ResendReq = { email: string };
type ResendRes = { success?: boolean; message?: string };

type PassCountryRes = { countries: string[] };

export const userApi = baseApi.injectEndpoints({
  endpoints: (build) => ({
    getUser: build.query<User, void>({
      query: () => ({ url: "/get_user_data", method: "GET" }),
      providesTags: ["User"],
    }),

    getUploadHistory: build.query<UploadHistoryRes, void>({
      query: () => ({ url: "/upload_history", method: "GET" }),
      providesTags: ["Uploads"],
    }),

    selectForm: build.mutation<SelectFormRes, SelectFormReq>({
      query: (body) => ({
        url: "/selectform",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
      invalidatesTags: ["User", "Profile"],
    }),

    resendVerification: build.mutation<ResendRes, ResendReq>({
      query: (body) => ({
        url: "/resend_verification",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
    }),

    // passCountryFromProfiles: build.query<PassCountryRes, void>({
    //   query: () => ({ url: "/passcountryfromprofiles", method: "GET" }),
    // }),
  }),
});

export const {
  useGetUserQuery,
  useGetUploadHistoryQuery,
  useSelectFormMutation,
  useResendVerificationMutation,
  // usePassCountryFromProfilesQuery,
} = userApi;
