import { baseApi } from "./baseApi";

export type UserData = {
  id?: string;
  email?: string;
  phone_number?: string;
  company_name?: string;
  brand_name?: string;
  annual_sales_range?: string;
  onboarding_complete?: boolean;
  homeCurrency?: string;
};



export type CountriesResponse = {
  countries: string[];
};

export type ForgotPasswordRequest = {
  email: string;
};

export const profileApi = baseApi.injectEndpoints({
  endpoints: (build) => ({
    getUserData: build.query<UserData, void>({
      query: () => ({ url: "/get_user_data", method: "GET" }),
      providesTags: ["User"],        // OK: 'User' exists in baseApi.tagTypes
    }),

    updateProfile: build.mutation<{ success?: boolean } | unknown, Partial<UserData>>({
      query: (body) => ({
        url: "/profileupdate",
        method: "POST",
        body,
      }),
      invalidatesTags: ["User"],     // OK
    }),

    getCountries: build.query<CountriesResponse, void>({
      query: () => ({ url: "/passcountryfromprofiles", method: "GET" }),
      providesTags: ["Profile"],     
    }),

    forgotPassword: build.mutation<{ message?: string } | unknown, ForgotPasswordRequest>({
      query: (body) => ({
        url: "/forgot_password",
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
} = profileApi;
