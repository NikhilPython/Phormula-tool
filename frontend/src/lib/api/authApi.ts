import { baseApi } from "./baseApi";

export type LoginReq = { email: string; password: string };

export type LoginRes = {
  token: string;
  message?: string;
  is_member?: boolean;
  member_id?: number;
  owner_user_id?: number;
  modules?: string[];
  marketplaces?: string[];
  countries?: string[];
};

export type RegisterReq = {
  name: string;
  email: string;
  password: string;
  phone_number: string;
  phone_number_raw?: string;
};

export type RegisterRes = {
  success: boolean;
  message: string;
  requires_verification?: boolean;
  email?: string;
  user_id?: number;
  token_name?: string;
  otp_expires_in_seconds?: number;
  resend_available_in_seconds?: number;
};

export type VerifyEmailOtpReq = { email: string; otp: string };
export type VerifyEmailOtpRes = {
  token: any;
  success: boolean;
  message: string;
  user_id?: number;
  email?: string;
  already_verified?: boolean;
};

export type ResendVerificationOtpReq = { email: string };
export type ResendVerificationOtpRes = {
  success: boolean;
  message: string;
  already_verified?: boolean;
  otp_expires_in_seconds?: number;
  resend_available_in_seconds?: number;
  retry_after_seconds?: number;
};

export const authApi = baseApi.injectEndpoints({
  endpoints: (build) => ({
    login: build.mutation<LoginRes, LoginReq>({
      query: (body) => ({
        url: "/login",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
      invalidatesTags: ["User"],
    }),

    memberLogin: build.mutation<LoginRes, LoginReq>({
      query: (body) => ({
        url: "/member_login",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
      invalidatesTags: ["User"],
    }),

    register: build.mutation<RegisterRes, RegisterReq>({
      query: (body) => ({
        url: "/register",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
    }),

    verifyEmailOtp: build.mutation<VerifyEmailOtpRes, VerifyEmailOtpReq>({
      query: (body) => ({
        url: "/verify-email-otp",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
    }),

    resendVerificationOtp: build.mutation<
      ResendVerificationOtpRes,
      ResendVerificationOtpReq
    >({
      query: (body) => ({
        url: "/resend-verification-otp",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
    }),

    resetPassword: build.mutation<any, { token: string; password: string }>({
      query: ({ token, password }) => ({
        url: `/reset_password/${encodeURIComponent(token)}`,
        method: "POST",
        body: { password },
        headers: { "Content-Type": "application/json" },
      }),
    }),
  }),
  overrideExisting: false,
});

export const {
  useLoginMutation,
  useMemberLoginMutation,
  useRegisterMutation,
  useVerifyEmailOtpMutation,
  useResendVerificationOtpMutation,
  useResetPasswordMutation,
} = authApi;
