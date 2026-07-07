// src/lib/api/skuApi.ts
import { baseApi } from "./baseApi";

export type UploadSkuMultiCountryReq = {
  file: File;
};

export type UploadSkuMultiCountryRes = {
  success?: boolean;
  message?: string;
  // include any fields your API returns if you need them later
};

export const skuApi = baseApi.injectEndpoints({
  endpoints: (build) => ({
    uploadSkuMultiCountry: build.mutation<
      UploadSkuMultiCountryRes,
      UploadSkuMultiCountryReq
    >({
      query: ({ file }) => {
        const fd = new FormData();
        fd.append("file", file);

        return {
          url: "/multiCountry",
          method: "POST",
          body: fd,
          // DO NOT set Content-Type for FormData; the browser will add the boundary
        };
      },
      invalidatesTags: ["Uploads"],
    }),
  }),
});

export const { useUploadSkuMultiCountryMutation } = skuApi;
