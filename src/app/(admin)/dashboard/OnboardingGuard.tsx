"use client";

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import { useAppSelector } from "@/lib/store";

const ONBOARDING_ROUTES = [
  "/choose-country",
  "/brand",
  "/chooserevenue",
];

export function OnboardingGuard({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();

  const { user } = useAppSelector((s: any) => s.auth);

  useEffect(() => {
    // Wait for user to be loaded
    if (!user) return;

    // If already on one of the onboarding routes, don't re-redirect
    if (ONBOARDING_ROUTES.some((route) => pathname.startsWith(route))) {
      return;
    }

    // 👇 Adjust these field names to match your API shape
    if (!user.country) {
      router.replace("/choose-country");
      return;
    }

    if (!user.brand_name) {
      router.replace("/brand");
      return;
    }

    if (!user.revenue) {
      router.replace("/chooserevenue");
      return;
    }

    // If all three are filled, do nothing – user can see normal app
  }, [user, pathname, router]);

  return <>{children}</>;
}
