import { baseApi } from "./baseApi";

export type UserData = {
  marketplace_id: any;
  country?: string;
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
  amazon_user_exist?: boolean;
  amazon_ads_exists?: boolean;

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

    updateProfile: build.mutation<UpdateProfileResponse, Partial<UserData>>({
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

    forgotPassword: build.mutation<{ message?: string }, ForgotPasswordRequest>({
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