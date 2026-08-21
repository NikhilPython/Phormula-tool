import type { RegisterRes } from "@/lib/api/authApi";

/**
 * Call this immediately after `useRegisterMutation().unwrap()` succeeds.
 * If you prefer not to import AppRouterInstance, remove that type and use
 * `{ push: (href: string) => void }` instead.
 */
export function goToOtpVerification(
  router: { push: (href: string) => void },
  result: RegisterRes,
  submittedEmail: string
) {
  const email = (result.email || submittedEmail).trim().toLowerCase();

  if (result.requires_verification && email) {
    router.push(`/verify-email?email=${encodeURIComponent(email)}`);
    return true;
  }

  return false;
}
