import { baseApi } from "./baseApi";

export type LoginReq = { email: string; password: string };

// ✅ common response (client + member)
export type LoginRes = {
  token: string;
  message?: string;

  // member props (backend member_login returns these)
  is_member?: boolean;
  member_id?: number;
  owner_user_id?: number;
  modules?: string[];
  marketplaces?: string[];
  countries?: string[];
};

export const authApi = baseApi.injectEndpoints({
  endpoints: (build) => ({
    // 🔹 CLIENT LOGIN
    login: build.mutation<LoginRes, LoginReq>({
      query: (body) => ({
        url: "/login",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
      invalidatesTags: ["User"],
    }),

    // ✅ MEMBER LOGIN
    memberLogin: build.mutation<LoginRes, LoginReq>({
      query: (body) => ({
        url: "/member_login",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
      invalidatesTags: ["User"],
    }),

    // 🔹 REGISTER
    register: build.mutation<any, any>({
      query: (body) => ({
        url: "/register",
        method: "POST",
        body,
        headers: { "Content-Type": "application/json" },
      }),
      invalidatesTags: ["User"],
    }),

    // 🔹 RESET PASSWORD
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
  useMemberLoginMutation, // ✅ export
  useRegisterMutation,
  useResetPasswordMutation,
} = authApi;