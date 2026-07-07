// // src/lib/utils/startAmazonLogin.ts
// "use client";

// import { PlatformId } from "./platforms";
// import { getAmazonConfigForPlatform } from "./amazonMarketplaces";
// import { useSelector } from "react-redux";
// import type { RootState } from "@/lib/store"; // adjust to your store setup

// const API_BASE =
//   process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:5000";

// export const startAmazonLogin = async (platform: PlatformId) => {

//     const reduxToken = useSelector(
//         (state: RootState) => state.auth.token 
//       );

//   const config = getAmazonConfigForPlatform(platform);
//   if (!config) {
//     console.error("No Amazon config for platform:", platform);
//     return;
//   }

//   const token = reduxToken;

//   if (!token) {
//     console.error("No JWT token found in localStorage for Amazon login");
//     return;
//   }

//   const params = new URLSearchParams({
//     region: config.region,
//     marketplace_id: config.marketplaceId,
//   });

//   const res = await fetch(
//     `${API_BASE}/amazon_api/login?${params.toString()}`,
//     {
//       method: "GET",
//       headers: {
//         Authorization: `Bearer ${token}`,
//       },
//     }
//   );

//   const contentType = res.headers.get("content-type") || "";
//   if (!contentType.includes("application/json")) {
//     const text = await res.text();
//     console.error("Non-JSON response from /amazon_api/login:", text);
//     return;
//   }

//   const data = await res.json();
//   console.log("Amazon login response:", data);

//   if (!res.ok || !data?.success || !data?.auth_url) {
//     console.error("Amazon login failed:", data);
//     return;
//   }

//   // Redirect user to Amazon OAuth
//   window.location.href = data.auth_url;
// };
