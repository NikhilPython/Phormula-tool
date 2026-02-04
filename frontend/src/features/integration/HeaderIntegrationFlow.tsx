// // "use client";

// // import React, { useEffect, useState } from "react";
// // import { Modal } from "@/components/ui/modal";
// // import AmazonConnectLegacy from "./AmazonConnectLegacy";
// // import { LS_KEYS } from "./useIntegrationProgress";
// // import { useParams } from "next/navigation";
// // import IntegrationsModal from "./IntegrationsModal";

// // // Keep types in sync with your existing code
// // type Provider = "amazon" | "shopify";
// // type Origin = "header" | "page";

// // export default function HeaderIntegrationFlow() {
// //   const [openIntegrationModal, setOpenIntegrationModal] = useState(false);
// //   const [showAmazonLegacy, setShowAmazonLegacy] = useState(false);

// //   // if you need country in AmazonConnectLegacy logic
// //   const { countryName } = useParams<{ countryName: string }>();
// //   const selectedCountry = (countryName || "").toLowerCase();

// //   useEffect(() => {
// //     const handler = (e: Event) => {
// //       const custom = e as CustomEvent<{ provider: Provider; origin?: Origin }>;
// //       const { provider, origin } = custom.detail || {};
// //       if (!provider) return;

// //       // 🟢 Only handle header-origin events here
// //       if (origin !== "header") return;

// //       if (provider === "amazon") {
// //         // Directly open AmazonConnectLegacy
// //         setShowAmazonLegacy(true);
// //       }

// //       // You can also handle Shopify header flow here if needed later
// //       // if (provider === "shopify") { ... }
// //     };

// //     window.addEventListener("integration:choose", handler as EventListener);
// //     return () => {
// //       window.removeEventListener("integration:choose", handler as EventListener);
// //     };
// //   }, []);

// //   return (
// //     <>
// //       {/* This button is just an example – use whatever triggers your header modal */}
// //       {/* You might already have a button in your Header – wrap this there */}
// //       <button
// //         type="button"
// //         onClick={() => setOpenIntegrationModal(true)}
// //         className="flex items-center gap-2"
// //       >
// //         Integrations
// //       </button>

// //       {/* Header Integration Modal (your existing one) */}
// //       <IntegrationsModal
// //         open={openIntegrationModal}
// //         onClose={() => setOpenIntegrationModal(false)}
// //       />

// //       {/* Amazon Legacy modal for header flow */}
// //       {showAmazonLegacy && (
// //         <Modal
// //           isOpen
// //           onClose={() => setShowAmazonLegacy(false)}
// //           className="m-4 max-w-xl"
// //           showCloseButton
// //         >
// //           <AmazonConnectLegacy
// //             onClose={() => setShowAmazonLegacy(false)}
// //             onConnected={(refreshToken?: string) => {
// //               if (typeof window !== "undefined") {
// //                 localStorage.setItem(
// //                   LS_KEYS.amazonRefreshToken(selectedCountry),
// //                   String(refreshToken ?? "")
// //                 );
// //               }
// //               setShowAmazonLegacy(false);
// //             }}
// //           />
// //         </Modal>
// //       )}
// //     </>
// //   );
// // }










// // features/integration/HeaderIntegrationFlow.tsx
// "use client";

// import React, { useEffect, useState } from "react";
// import { Modal } from "@/components/ui/modal";
// import AmazonConnectLegacy from "./AmazonConnectLegacy";
// import { LS_KEYS } from "./useIntegrationProgress";
// import { useParams } from "next/navigation";
// import IntegrationsModal from "./IntegrationsModal";

// type Provider = "amazon" | "shopify";
// type Origin = "header" | "page";

// export default function HeaderIntegrationFlow() {
//   const [openIntegrationModal, setOpenIntegrationModal] = useState(false);
//   const [showAmazonLegacy, setShowAmazonLegacy] = useState(false);

//   const { countryName } = useParams<{ countryName: string }>();
//   const selectedCountry = (countryName || "").toLowerCase();

//   useEffect(() => {
//     const handler = (e: Event) => {
//       const custom = e as CustomEvent<{ provider: Provider; origin?: Origin }>;
//       const { provider, origin } = custom.detail || {};
//       if (!provider) return;

//       // 🟢 Only handle header-origin events here
//       if (origin !== "header") return;

//       if (provider === "amazon") {
//         // Directly open AmazonConnectLegacy
//         setShowAmazonLegacy(true);
//       }
//     };

//     window.addEventListener("integration:choose", handler as EventListener);
//     return () => {
//       window.removeEventListener("integration:choose", handler as EventListener);
//     };
//   }, []);

//   return (
//     <>
//       {/* This button is just an example – use whatever triggers your header modal */}
//       {/* You might already have a button in your Header – wrap this there */}
//       <button
//         type="button"
//         onClick={() => setOpenIntegrationModal(true)}
//         className="flex items-center gap-2"
//       >
//         Integrations
//       </button>

//       {/* Header Integration Modal (your existing one) */}
//       <IntegrationsModal
//         open={openIntegrationModal}
//         onClose={() => setOpenIntegrationModal(false)}
//       />

//       {/* Amazon Legacy modal for header flow */}
//       {showAmazonLegacy && (
//         <Modal
//           isOpen
//           onClose={() => setShowAmazonLegacy(false)}
//           className="m-4 max-w-xl"
//           showCloseButton
//         >
//           <AmazonConnectLegacy
//             onClose={() => setShowAmazonLegacy(false)}
//             onConnected={(refreshToken?: string) => {
//               if (typeof window !== "undefined") {
//                 localStorage.setItem(
//                   LS_KEYS.amazonRefreshToken(selectedCountry),
//                   String(refreshToken ?? "")
//                 );
//               }
//               setShowAmazonLegacy(false);
//             }}
//           />
//         </Modal>
//       )}
//     </>
//   );
// }



















"use client";

import React, { useEffect, useState } from "react";
import { Modal } from "@/components/ui/modal";
import AmazonConnectLegacy from "./AmazonConnectLegacy";
import { LS_KEYS } from "./useIntegrationProgress";
import { useParams } from "next/navigation";
import IntegrationsModal from "./IntegrationsModal";

// ✅ NEW
import AmazonAdsConnectModal from "./AmazonAdsConnectModal";
import AmazonAdsIntegrationFlow from "./AmazonAdsIntegrationFlow";
import AmazonAdsConnectLegacy from "./AmazonAdsConnectLegacy";

type Provider = "amazon" | "shopify" | "amazon_ads";
type Origin = "header" | "page";

export default function HeaderIntegrationFlow() {
  const [openIntegrationModal, setOpenIntegrationModal] = useState(false);
  const [showAmazonLegacy, setShowAmazonLegacy] = useState(false);
  const [showAmazonAds, setShowAmazonAds] = useState(false);

  // Minimal props required by AmazonAdsConnectModal
  const [adsStatusLoading] = useState(false);
  const [adsStatus] = useState<any | null>(null);
  const [adsConnecting, setAdsConnecting] = useState(false);
  const [adsError, setAdsError] = useState<string | null>(null);

  const { countryName } = useParams<{ countryName: string }>();
  const selectedCountry = (countryName || "").toLowerCase();

  useEffect(() => {
    const handler = (e: Event) => {
      const custom = e as CustomEvent<{ provider: Provider; origin?: Origin }>;
      const { provider, origin } = custom.detail || {};
      if (!provider) return;
      console.log("Header flow got event:", custom.detail);
      console.log("integration:choose received", custom.detail);

      // 🟢 Only handle header-origin events here
      if (origin !== "header") return;

      if (provider === "amazon") {
        setShowAmazonLegacy(true);
        return;
      }

      if (origin !== "header") return;
      if (provider === "amazon_ads") setShowAmazonAds(true);
    };

    window.addEventListener("integration:choose", handler as EventListener);
    return () => {
      window.removeEventListener("integration:choose", handler as EventListener);
    };
  }, []);

  const onConnectOrSyncAds = async () => {
    // Safe stub: keeps UI working; wire to backend later without affecting anything else
    try {
      setAdsConnecting(true);
      setAdsError(null);
      console.log("Amazon Ads connect/sync clicked");
    } catch (err) {
      console.error(err);
      setAdsError("Amazon Ads connect failed");
    } finally {
      setAdsConnecting(false);
    }
  };

  return (
    <>
      <button
        type="button"
        onClick={() => setOpenIntegrationModal(true)}
        className="flex items-center gap-2"
      >
        Integrations
      </button>

      <IntegrationsModal
        open={openIntegrationModal}
        onClose={() => setOpenIntegrationModal(false)}
      />

      <AmazonAdsIntegrationFlow />

      {showAmazonLegacy && (
        <Modal
          isOpen
          onClose={() => setShowAmazonLegacy(false)}
          className="m-4 max-w-xl"
          showCloseButton
        >
          <AmazonConnectLegacy
            onClose={() => setShowAmazonLegacy(false)}
            onConnected={(refreshToken?: string) => {
              if (typeof window !== "undefined") {
                localStorage.setItem(
                  LS_KEYS.amazonRefreshToken(selectedCountry),
                  String(refreshToken ?? "")
                );
              }
              setShowAmazonLegacy(false);
            }}
          />
        </Modal>
      )}

      {/* ✅ NEW */}
      {showAmazonAds && (
        <AmazonAdsConnectLegacy
          onClose={() => setShowAmazonAds(false)}
          onConnected={() => setShowAmazonAds(false)}
        />
      )}


    </>
  );
}
