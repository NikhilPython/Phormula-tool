// // import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";
// // import type { RootState } from "../store";
// // import { API_BASE } from "@/config/env";

// // export const baseApi = createApi({
// //   reducerPath: "api",
// //   baseQuery: fetchBaseQuery({
// //     baseUrl: API_BASE,
// //     prepareHeaders: (headers, { getState }) => {
// //       const token =
// //         (getState() as RootState).auth.token || (typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null);

// //       if (token) {
// //         headers.set("authorization", `Bearer ${token}`);
// //       }

// //       // default JSON content-type for all requests; individual endpoints can override
// //       if (!headers.has("Content-Type")) {
// //         headers.set("Content-Type", "application/json");
// //       }

// //       return headers;
// //     },
// //     credentials: "include",
// //   }),
// //   tagTypes: ["User", "Uploads", "Profile"],
// //   endpoints: () => ({}),
// // });









// // src/lib/api/baseApi.ts
// import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";
// import type { RootState } from "../store";
// import { API_BASE } from "@/config/env";

// export const baseApi = createApi({
//   reducerPath: "api",
//   baseQuery: fetchBaseQuery({
//     baseUrl: API_BASE,
//     credentials: "include",
//     prepareHeaders: (headers, { getState }) => {
//       const token =
//         (getState() as RootState).auth.token ||
//         (typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null);

//       if (token) {
//         headers.set("authorization", `Bearer ${token}`);
//       }
//       return headers;
//     },
//   }),
//   tagTypes: ["User", "Uploads", "Profile"],
//   endpoints: () => ({}),
// });







import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";
import type { RootState } from "../store";
import { API_BASE } from "@/config/env";

export const baseApi = createApi({
  reducerPath: "api",
  baseQuery: fetchBaseQuery({
    baseUrl: API_BASE,            // e.g. "http://127.0.0.1:5000"
    credentials: "include",
    prepareHeaders: (headers, { getState }) => {
      const token =
        (getState() as RootState).auth.token ||
        (typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null);

      if (token) {
        headers.set("authorization", `Bearer ${token}`);
      }
      // DO NOT set Content-Type here; fetch will set the multipart boundary automatically for FormData.
      return headers;
    },
  }),
  tagTypes: ["User", "Uploads", "Profile", "Forecast", "ManualForecast"],
  endpoints: () => ({}),
});
