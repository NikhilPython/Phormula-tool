import { baseApi } from "./baseApi";

export type UploadHistory = {
  uploads?: Array<any>;
};

export const uploadsApi = baseApi.injectEndpoints({
  overrideExisting: true,
  endpoints: (build) => ({
    getUploadHistory: build.query<UploadHistory, void>({
      query: () => ({ url: "/upload_history", method: "GET" }),
      providesTags: ["Uploads"],
    }),

    // IMPORTANT: send FormData directly; do NOT set Content-Type headers.
    uploadFiles: build.mutation<any, FormData>({
      query: (formData) => ({
        url: "/upload",
        method: "POST",
        body: formData,
      }),
      invalidatesTags: ["Uploads"],
    }),
  }),
});

export const {
  useGetUploadHistoryQuery,
  useLazyGetUploadHistoryQuery,
  useUploadFilesMutation,
} = uploadsApi;
