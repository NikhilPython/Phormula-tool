import { baseApi } from "./baseApi";

export type UserData = {
  marketplace_id?: string;
  marketplace_ids?: string[];

  country?: string;
  countries?: string[];

  id?: string;
  name?: string;
  email?: string;
  phone_number?: string;
  company_name?: string;
  brand_name?: string;
  annual_sales_range?: string;
  onboarding_complete?: boolean;
  homeCurrency?: string;
  target_sales?: number;

  amazon_user_exists?: boolean;
  amazon_ads_exists?: boolean;
  amazon_connected?: boolean;
  connected_marketplaces_count?: number;

  tax_id?: {
    gst_no?: string;
    pan_no?: string;
  };

  address?: {
    building?: string;
    city?: string;
    country?: string;
    state?: string;
    zipcode?: string;
  };
};

export type CountriesResponse = {
  countries: string[];
};

export type ForgotPasswordRequest = {
  email: string;
};

export type ForgotPasswordResponse = {
  success?: boolean;
  message?: string;
  email?: string;
  otp_expires_in_seconds?: number;
  resend_available_in_seconds?: number;
};

export type ResetPasswordOtpRequest = {
  email: string;
  otp: string;
  password: string;
};

export type ResetPasswordOtpResponse = {
  success?: boolean;
  message?: string;
};

export type UpdateProfileResponse = {
  message: string;
};

export const profileApi = baseApi.injectEndpoints({
  overrideExisting: true,

  endpoints: (build) => ({
    getUserData: build.query<UserData, void>({
      query: () => ({
        url: "/get_user_data",
        method: "GET",
      }),
      providesTags: ["User"],
    }),

    updateProfile: build.mutation<
      UpdateProfileResponse,
      Partial<UserData>
    >({
      query: (body) => ({
        url: "/profileupdate",
        method: "POST",
        body,
      }),
      invalidatesTags: ["User"],
    }),

    getCountries: build.query<CountriesResponse, void>({
      query: () => ({
        url: "/passcountryfromprofiles",
        method: "GET",
      }),
      providesTags: ["Profile"],
    }),

    // Send password reset OTP
    forgotPassword: build.mutation<
      ForgotPasswordResponse,
      ForgotPasswordRequest
    >({
      query: (body) => ({
        url: "/forgot_password",
        method: "POST",
        body,
      }),
    }),

    // Verify OTP + update password
    resetPasswordOtp: build.mutation<
      ResetPasswordOtpResponse,
      ResetPasswordOtpRequest
    >({
      query: (body) => ({
        url: "/reset-password-otp",
        method: "POST",
        body,
      }),
    }),
  }),
});

export const {
  useGetUserDataQuery,
  useLazyGetUserDataQuery,
  useUpdateProfileMutation,
  useGetCountriesQuery,
  useForgotPasswordMutation,
  useResetPasswordOtpMutation,
} = profileApi;