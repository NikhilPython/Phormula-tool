"use client";

import { useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { useAppSelector } from "@/lib/hooks"; // typed hooks that read from the store


export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const token = useAppSelector((s) => s.auth.token);

  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  useEffect(() => {
    if (!mounted) return;
    // If there's no token, send to signin (and keep redirect param so you can bounce back after login)
    if (!token) {
      const redirect = encodeURIComponent(pathname || "/");
      router.replace(`/signin?redirect=${redirect}`);
    }
  }, [mounted, token, pathname, router]);

  // Avoid rendering protected UI until we know auth state client-side
  if (!mounted) return null;
  if (!token) return null; // while redirecting

  return <>{children}</>;
}
