// src/lib/api/skuApi.ts
import { baseApi } from "./baseApi";

export type UploadSkuMultiCountryReq = {
  file: File;
};

export type UploadSkuMultiCountryRes = {
  success?: boolean;
  message?: string;
  sync_result?: unknown;
  sku_updated_at?: string | null;
};

export type SkuSheetRow = {
  s_no?: number | null;
  product_name?: string | null;
  product_barcode?: string | null;
  asin?: string | null;
  sku_uk?: string | null;
  sku_us?: string | null;
  sku_canada?: string | null;
  landing_cost?: number | null;
  currency?: string | null;
  date?: string | null;
  local_stock?: number | null;
  in_transit_units?: number | null;
};

export type CurrentSkuSheetRes = {
  success: boolean;
  rows: SkuSheetRow[];
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
          // DO NOT set Content-Type for FormData; the browser adds the boundary.
        };
      },
      invalidatesTags: ["Uploads"],
    }),

    getCurrentSkuSheet: build.query<CurrentSkuSheetRes, void>({
      query: () => ({
        url: "/multiCountry/current",
        method: "GET",
      }),
      providesTags: ["Uploads"],
    }),

    downloadCurrentSkuSheet: build.mutation<Blob, void>({
      query: () => ({
        url: "/multiCountry/current/download",
        method: "GET",
        responseHandler: (response) => response.blob(),
      }),
    }),
  }),
});

export const {
  useUploadSkuMultiCountryMutation,
  useLazyGetCurrentSkuSheetQuery,
  useDownloadCurrentSkuSheetMutation,
} = skuApi;
